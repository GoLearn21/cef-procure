use cef::{args::Args, rc::*, *};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, Condvar};
use serde::{Deserialize, Serialize};

// Supplier URLs for testing navigation
const SUPPLIER_URLS: &[(&str, &str)] = &[
    ("Lowes", "https://www.lowes.com"),
    ("HD Supply", "https://www.hdsupplysolutions.com"),
    ("Grainger", "https://www.grainger.com"),
    ("Lowes Pro", "https://www.lowesprosupply.com"),
    ("Chadwell", "https://www.chadwellsupply.com"),
];

// ============================================================================
// IPC Protocol Types (matches comet-procure-cdp/src-tauri/src/cef_bridge/ipc.rs)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum MessageType {
    // === Tab Management ===
    CreateTab { tab_id: String, supplier_id: String, url: String },
    Navigate { tab_id: String, url: String },
    ExecuteScript { tab_id: String, script: String },
    ExtractProducts { tab_id: String, supplier_id: String },
    GetPageInfo { tab_id: String },
    CheckLoginStatus { tab_id: String, supplier_id: String },
    CloseTab { tab_id: String },
    ListTabs,
    Shutdown,
    Ping,

    // === Native Input Events (Human-Like Automation) ===
    SendMouseMove {
        tab_id: String,
        x: i32,
        y: i32,
        modifiers: u32,
    },
    SendMouseClick {
        tab_id: String,
        x: i32,
        y: i32,
        button: MouseButton,
        mouse_up: bool,
        click_count: i32,
        modifiers: u32,
    },
    SendMouseWheel {
        tab_id: String,
        x: i32,
        y: i32,
        delta_x: i32,
        delta_y: i32,
        modifiers: u32,
    },
    SendKeyEvent {
        tab_id: String,
        event_type: KeyEventKind,
        windows_key_code: i32,
        native_key_code: i32,
        character: u16,
        modifiers: u32,
    },

    // === DOM Queries ===
    GetElementBounds {
        tab_id: String,
        selector: String,
    },
    WaitForSelector {
        tab_id: String,
        selector: String,
        timeout_ms: u64,
        visible: bool,
    },
    WaitForLoad {
        tab_id: String,
        timeout_ms: u64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MouseButton {
    Left,
    Middle,
    Right,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum KeyEventKind {
    RawKeyDown,
    KeyDown,
    KeyUp,
    Char,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CefMessage {
    pub id: String,
    pub message: MessageType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum CefResponse {
    Success { id: String, data: Option<serde_json::Value> },
    Error { id: String, error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductData {
    pub name: String,
    pub price: Option<f64>,
    pub price_text: String,
    pub sku: Option<String>,
    pub url: String,
    pub image_url: Option<String>,
    pub availability: Option<String>,
    pub supplier_id: String,
}

// ============================================================================
// Tab Management
// ============================================================================

struct TabInfo {
    tab_id: String,
    supplier_id: String,
    browser_view: BrowserView,
    url: String,
}

type TabStore = Arc<Mutex<HashMap<String, TabInfo>>>;

// ============================================================================
// Browser View Creation Task (for thread-safe creation on UI thread)
// ============================================================================

// Result holder for browser view creation
struct BrowserViewResult {
    browser_view: Option<BrowserView>,
    completed: bool,
}

type BrowserViewResultHolder = Arc<(Mutex<BrowserViewResult>, Condvar)>;

// Task to create a browser view on the UI thread
// IMPORTANT: Chrome-style CEF only allows ONE BrowserView per Window.
// So we create a NEW window for each tab's BrowserView.
wrap_task! {
    struct CreateBrowserViewTask {
        tab_id: String,
        url: CefString,
        tabs: TabStore,
        window: Arc<Mutex<Option<Window>>>,  // Not used for adding - kept for compatibility
        result: BrowserViewResultHolder,
    }

    impl Task {
        fn execute(&self) {
            eprintln!("[PROCUREMENT][TASK] Executing CreateBrowserViewTask on UI thread for tab: {}", self.tab_id);

            let mut client = ProcurementClient::new(Some(self.tabs.clone()), self.tab_id.clone());

            let browser_view = browser_view_create(
                Some(&mut client),
                Some(&self.url),
                Some(&Default::default()),
                Option::<&mut DictionaryValue>::None,
                Option::<&mut RequestContext>::None,
                Option::<&mut BrowserViewDelegate>::None,
            );

            if let Some(ref bv) = browser_view {
                eprintln!("[PROCUREMENT][TASK] BrowserView created successfully");

                // Create a NEW window for this BrowserView (Chrome-style CEF requires 1 BrowserView per Window)
                let mut delegate = ProcurementWindowDelegate::new(bv.clone());
                match window_create_top_level(Some(&mut delegate)) {
                    Some(_win) => {
                        eprintln!("[PROCUREMENT][TASK] Created new window for tab: {}", self.tab_id);

                        // Wait for browser to be created (it's async after window creation)
                        // Poll for up to 5 seconds
                        let start = std::time::Instant::now();
                        let timeout = std::time::Duration::from_secs(5);
                        loop {
                            if bv.browser().is_some() {
                                eprintln!("[PROCUREMENT][TASK] Browser is now ready for tab: {}", self.tab_id);
                                break;
                            }
                            if start.elapsed() > timeout {
                                eprintln!("[PROCUREMENT][TASK] WARNING: Browser not ready after 5s for tab: {}", self.tab_id);
                                break;
                            }
                            // Process CEF messages to allow browser creation
                            do_message_loop_work();
                            std::thread::sleep(std::time::Duration::from_millis(50));
                        }
                    }
                    None => {
                        eprintln!("[PROCUREMENT][TASK] ERROR: Failed to create window for tab: {}", self.tab_id);
                    }
                }
            } else {
                eprintln!("[PROCUREMENT][TASK] ERROR: browser_view_create returned None");
            }

            // Store result and signal completion
            let (lock, cvar) = &*self.result;
            if let Ok(mut result) = lock.lock() {
                result.browser_view = browser_view;
                result.completed = true;
            }
            cvar.notify_all();
            eprintln!("[PROCUREMENT][TASK] CreateBrowserViewTask completed");
        }
    }
}

// Helper function to create browser view on UI thread from any thread
fn create_browser_view_on_ui_thread(
    tab_id: String,
    url: &str,
    tabs: &TabStore,
    window: &Arc<Mutex<Option<Window>>>,
) -> Option<BrowserView> {
    eprintln!("[PROCUREMENT] Creating browser view on UI thread for: {}", tab_id);

    // Check if we're already on the UI thread
    let on_ui_thread = currently_on(ThreadId::UI) != 0;
    eprintln!("[PROCUREMENT] Currently on UI thread: {}", on_ui_thread);

    if on_ui_thread {
        // Direct creation on UI thread
        let mut client = ProcurementClient::new(Some(tabs.clone()), tab_id.clone());
        let url_cef = CefString::from(url);

        return browser_view_create(
            Some(&mut client),
            Some(&url_cef),
            Some(&Default::default()),
            Option::<&mut DictionaryValue>::None,
            Option::<&mut RequestContext>::None,
            Option::<&mut BrowserViewDelegate>::None,
        );
    }

    // Create result holder
    let result: BrowserViewResultHolder = Arc::new((
        Mutex::new(BrowserViewResult {
            browser_view: None,
            completed: false,
        }),
        Condvar::new(),
    ));

    // Create and post task
    let mut task = CreateBrowserViewTask::new(
        tab_id,
        CefString::from(url),
        tabs.clone(),
        window.clone(),
        result.clone(),
    );

    let post_result = post_task(ThreadId::UI, Some(&mut task));
    eprintln!("[PROCUREMENT] post_task result: {}", post_result);

    if post_result == 0 {
        eprintln!("[PROCUREMENT] ERROR: Failed to post task to UI thread");
        return None;
    }

    // Wait for completion with timeout
    let (lock, cvar) = &*result;
    let mut result_guard = lock.lock().unwrap();
    let timeout = std::time::Duration::from_secs(10);

    while !result_guard.completed {
        let wait_result = cvar.wait_timeout(result_guard, timeout).unwrap();
        result_guard = wait_result.0;
        if wait_result.1.timed_out() {
            eprintln!("[PROCUREMENT] ERROR: Timeout waiting for browser view creation");
            return None;
        }
    }

    eprintln!("[PROCUREMENT] Browser view creation completed, has view: {}", result_guard.browser_view.is_some());
    result_guard.browser_view.take()
}

// ============================================================================
// Navigate Task (for thread-safe navigation on UI thread)
// ============================================================================

// Result holder for navigation
struct NavigateResult {
    success: bool,
    error: Option<String>,
    completed: bool,
}

type NavigateResultHolder = Arc<(Mutex<NavigateResult>, Condvar)>;

// Task to navigate a browser tab on the UI thread
wrap_task! {
    struct NavigateTask {
        tabs: TabStore,
        tab_id: String,
        url: CefString,
        result: NavigateResultHolder,
    }

    impl Task {
        fn execute(&self) {
            eprintln!("[PROCUREMENT][NAV_TASK] Executing NavigateTask on UI thread for tab: {}", self.tab_id);

            let nav_result = if let Ok(tabs_guard) = self.tabs.lock() {
                if let Some(tab) = tabs_guard.get(&self.tab_id) {
                    eprintln!("[PROCUREMENT][NAV_TASK] Tab found, getting browser...");
                    if let Some(browser) = tab.browser_view.browser() {
                        eprintln!("[PROCUREMENT][NAV_TASK] Browser found!");
                        if let Some(frame) = browser.main_frame() {
                            frame.load_url(Some(&self.url));
                            eprintln!("[PROCUREMENT][NAV_TASK] load_url called successfully");
                            (true, None)
                        } else {
                            eprintln!("[PROCUREMENT][NAV_TASK] ERROR: No main frame");
                            (false, Some("No main frame".to_string()))
                        }
                    } else {
                        eprintln!("[PROCUREMENT][NAV_TASK] ERROR: No browser object on UI thread");
                        (false, Some("No browser".to_string()))
                    }
                } else {
                    eprintln!("[PROCUREMENT][NAV_TASK] ERROR: Tab not found");
                    (false, Some(format!("Tab not found: {}", self.tab_id)))
                }
            } else {
                eprintln!("[PROCUREMENT][NAV_TASK] ERROR: Failed to lock tabs");
                (false, Some("Failed to lock tabs".to_string()))
            };

            // Store result and signal completion
            let (lock, cvar) = &*self.result;
            if let Ok(mut result) = lock.lock() {
                result.success = nav_result.0;
                result.error = nav_result.1;
                result.completed = true;
            }
            cvar.notify_all();
            eprintln!("[PROCUREMENT][NAV_TASK] NavigateTask completed");
        }
    }
}

// Helper function to navigate on UI thread from any thread
fn navigate_on_ui_thread(tabs: &TabStore, tab_id: &str, url: &str) -> Result<(), String> {
    eprintln!("[PROCUREMENT] Navigating on UI thread: {} -> {}", tab_id, url);

    // Create result holder
    let result: NavigateResultHolder = Arc::new((
        Mutex::new(NavigateResult {
            success: false,
            error: None,
            completed: false,
        }),
        Condvar::new(),
    ));

    // Create and post task
    let mut task = NavigateTask::new(
        tabs.clone(),
        tab_id.to_string(),
        CefString::from(url),
        result.clone(),
    );

    let post_result = post_task(ThreadId::UI, Some(&mut task));
    eprintln!("[PROCUREMENT] Navigate post_task result: {}", post_result);

    if post_result == 0 {
        return Err("Failed to post navigate task to UI thread".to_string());
    }

    // Wait for completion with timeout
    let (lock, cvar) = &*result;
    let mut result_guard = lock.lock().unwrap();
    let timeout = std::time::Duration::from_secs(10);

    while !result_guard.completed {
        let wait_result = cvar.wait_timeout(result_guard, timeout).unwrap();
        result_guard = wait_result.0;
        if wait_result.1.timed_out() {
            return Err("Timeout waiting for navigation".to_string());
        }
    }

    if result_guard.success {
        Ok(())
    } else {
        Err(result_guard.error.clone().unwrap_or_else(|| "Unknown error".to_string()))
    }
}

// ============================================================================
// ExecuteScript Task (for thread-safe script execution on UI thread)
// ============================================================================

// Task to execute JavaScript on a browser tab on the UI thread
wrap_task! {
    struct ExecuteScriptTask {
        tabs: TabStore,
        tab_id: String,
        script: CefString,
        result: NavigateResultHolder,  // Reuse the same result type
    }

    impl Task {
        fn execute(&self) {
            eprintln!("[PROCUREMENT][SCRIPT_TASK] Executing script on UI thread for tab: {}", self.tab_id);

            let exec_result = if let Ok(tabs_guard) = self.tabs.lock() {
                if let Some(tab) = tabs_guard.get(&self.tab_id) {
                    if let Some(browser) = tab.browser_view.browser() {
                        if let Some(frame) = browser.main_frame() {
                            frame.execute_java_script(Some(&self.script), None, 0);
                            eprintln!("[PROCUREMENT][SCRIPT_TASK] JavaScript executed successfully");
                            (true, None)
                        } else {
                            eprintln!("[PROCUREMENT][SCRIPT_TASK] ERROR: No main frame");
                            (false, Some("No main frame".to_string()))
                        }
                    } else {
                        eprintln!("[PROCUREMENT][SCRIPT_TASK] ERROR: No browser object");
                        (false, Some("No browser".to_string()))
                    }
                } else {
                    eprintln!("[PROCUREMENT][SCRIPT_TASK] ERROR: Tab not found");
                    (false, Some(format!("Tab not found: {}", self.tab_id)))
                }
            } else {
                (false, Some("Failed to lock tabs".to_string()))
            };

            // Store result and signal completion
            let (lock, cvar) = &*self.result;
            if let Ok(mut result) = lock.lock() {
                result.success = exec_result.0;
                result.error = exec_result.1;
                result.completed = true;
            }
            cvar.notify_all();
        }
    }
}

// Helper function to execute script on UI thread from any thread
fn execute_script_on_ui_thread(tabs: &TabStore, tab_id: &str, script: &str) -> Result<(), String> {
    eprintln!("[PROCUREMENT] Executing script on UI thread for: {}", tab_id);

    let result: NavigateResultHolder = Arc::new((
        Mutex::new(NavigateResult {
            success: false,
            error: None,
            completed: false,
        }),
        Condvar::new(),
    ));

    let mut task = ExecuteScriptTask::new(
        tabs.clone(),
        tab_id.to_string(),
        CefString::from(script),
        result.clone(),
    );

    let post_result = post_task(ThreadId::UI, Some(&mut task));
    if post_result == 0 {
        return Err("Failed to post script task to UI thread".to_string());
    }

    let (lock, cvar) = &*result;
    let mut result_guard = lock.lock().unwrap();
    let timeout = std::time::Duration::from_secs(10);

    while !result_guard.completed {
        let wait_result = cvar.wait_timeout(result_guard, timeout).unwrap();
        result_guard = wait_result.0;
        if wait_result.1.timed_out() {
            return Err("Timeout waiting for script execution".to_string());
        }
    }

    if result_guard.success {
        Ok(())
    } else {
        Err(result_guard.error.clone().unwrap_or_else(|| "Unknown error".to_string()))
    }
}

// ============================================================================
// IPC Handler State
// ============================================================================

struct IpcState {
    tabs: TabStore,
    is_ipc_mode: bool,
}

impl IpcState {
    fn new(is_ipc_mode: bool) -> Self {
        Self {
            tabs: Arc::new(Mutex::new(HashMap::new())),
            is_ipc_mode,
        }
    }
}

// ============================================================================
// CEF App Implementation
// ============================================================================

wrap_app! {
    struct ProcurementApp {
        window: Arc<Mutex<Option<Window>>>,
        ipc_state: Arc<Mutex<IpcState>>,
    }

    impl App {
        fn browser_process_handler(&self) -> Option<BrowserProcessHandler> {
            Some(ProcurementBrowserProcessHandler::new(
                self.window.clone(),
                self.ipc_state.clone(),
            ))
        }
    }
}

wrap_browser_process_handler! {
    struct ProcurementBrowserProcessHandler {
        window: Arc<Mutex<Option<Window>>>,
        ipc_state: Arc<Mutex<IpcState>>,
    }

    impl BrowserProcessHandler {
        fn on_context_initialized(&self) {
            println!("[PROCUREMENT] CEF context initialized");

            let is_ipc_mode = {
                let state = self.ipc_state.lock().unwrap();
                state.is_ipc_mode
            };

            if is_ipc_mode {
                // IPC mode: Create a hidden window that browser views can attach to
                // This is CRITICAL - browser views need a parent window to render properly
                println!("[PROCUREMENT] Running in IPC mode - creating hidden host window for browser views");

                // Create a minimal browser view for the hidden window
                // Using about:blank as it's fast and doesn't require network
                let mut client = ProcurementClient::new(None, "ipc-host".to_string());
                let url = CefString::from("about:blank");

                let browser_view = browser_view_create(
                    Some(&mut client),
                    Some(&url),
                    Some(&Default::default()),
                    Option::<&mut DictionaryValue>::None,
                    Option::<&mut RequestContext>::None,
                    Option::<&mut BrowserViewDelegate>::None,
                )
                .expect("Failed to create host browser view for IPC mode");

                // Create the hidden window - this hosts all IPC-created browser views
                let mut delegate = ProcurementWindowDelegate::new(browser_view);
                if let Ok(mut window) = self.window.lock() {
                    match window_create_top_level(Some(&mut delegate)) {
                        Some(win) => {
                            println!("[PROCUREMENT] Hidden host window created successfully");
                            // Note: window.show() is called in WindowDelegate::on_window_created
                            // For IPC mode, we could optionally hide it, but visible is OK for debugging
                            *window = Some(win);
                        }
                        None => {
                            eprintln!("[PROCUREMENT] ERROR: Failed to create hidden host window!");
                        }
                    }
                } else {
                    eprintln!("[PROCUREMENT] ERROR: Failed to lock window mutex");
                }

                // Send context initialized event
                let response = serde_json::json!({
                    "status": "Event",
                    "event_type": { "type": "ContextInitialized" },
                    "data": {}
                });
                println!("{}", serde_json::to_string(&response).unwrap());
            } else {
                // Standalone mode: Create default window with first supplier
                println!("[PROCUREMENT] Starting with supplier: {}", SUPPLIER_URLS[0].0);

                let mut client = ProcurementClient::new(None, String::new());
                let url = CefString::from(SUPPLIER_URLS[0].1);

                let browser_view = browser_view_create(
                    Some(&mut client),
                    Some(&url),
                    Some(&Default::default()),
                    Option::<&mut DictionaryValue>::None,
                    Option::<&mut RequestContext>::None,
                    Option::<&mut BrowserViewDelegate>::None,
                )
                .expect("Failed to create browser view");

                let mut delegate = ProcurementWindowDelegate::new(browser_view);
                if let Ok(mut window) = self.window.lock() {
                    *window = Some(
                        window_create_top_level(Some(&mut delegate)).expect("Failed to create window"),
                    );
                }
            }
        }
    }
}

// ============================================================================
// CEF Client Implementation
// ============================================================================

wrap_client! {
    struct ProcurementClient {
        tabs: Option<TabStore>,
        tab_id: String,
    }

    impl Client {
        fn life_span_handler(&self) -> Option<LifeSpanHandler> {
            Some(ProcurementLifeSpanHandler::new(
                self.tabs.clone(),
                self.tab_id.clone(),
            ))
        }

        fn load_handler(&self) -> Option<LoadHandler> {
            Some(ProcurementLoadHandler::new(
                self.tab_id.clone(),
            ))
        }
    }
}

wrap_life_span_handler! {
    struct ProcurementLifeSpanHandler {
        tabs: Option<TabStore>,
        tab_id: String,
    }

    impl LifeSpanHandler {
        fn on_before_close(&self, _browser: Option<&mut Browser>) {
            // Remove tab from store when browser closes
            if let Some(tabs) = &self.tabs {
                if let Ok(mut tabs_guard) = tabs.lock() {
                    tabs_guard.remove(&self.tab_id);
                    eprintln!("[PROCUREMENT] Tab closed: {}", self.tab_id);
                }
            }
        }
    }
}

wrap_load_handler! {
    struct ProcurementLoadHandler {
        tab_id: String,
    }

    impl LoadHandler {
        fn on_loading_state_change(
            &self,
            browser: Option<&mut Browser>,
            is_loading: ::std::os::raw::c_int,
            _can_go_back: ::std::os::raw::c_int,
            _can_go_forward: ::std::os::raw::c_int,
        ) {
            // Wrap in catch_unwind to prevent panics from crashing CEF
            let tab_id = self.tab_id.clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if is_loading == 0 {
                    // Page finished loading
                    if let Some(browser) = browser {
                        if let Some(frame) = browser.main_frame() {
                            // Safely get URL - convert through CefString
                            let frame_url = frame.url();
                            let url_cef = CefString::from(&frame_url);
                            let url_str = url_cef.to_string();
                            eprintln!("[PROCUREMENT] Page loaded: {}", url_str);

                            // Emit LoadCompleted event via stdout JSON for Tauri to capture
                            // This enables event-driven waiting instead of polling
                            if !tab_id.is_empty() && tab_id != "ipc-host" {
                                let event = serde_json::json!({
                                    "status": "Event",
                                    "event_type": {
                                        "type": "LoadCompleted",
                                        "tab_id": tab_id,
                                        "url": url_str
                                    },
                                    "data": {
                                        "tab_id": tab_id,
                                        "url": url_str,
                                        "timestamp": std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .map(|d| d.as_millis())
                                            .unwrap_or(0)
                                    }
                                });
                                // Print to stdout for Tauri to capture (safely, don't panic)
                                use std::io::Write;
                                if let Ok(event_str) = serde_json::to_string(&event) {
                                    let mut stdout = std::io::stdout().lock();
                                    if let Err(e) = writeln!(stdout, "{}", event_str) {
                                        eprintln!("[PROCUREMENT] Warning: stdout write failed: {}", e);
                                    }
                                    let _ = stdout.flush();
                                }
                                eprintln!("[PROCUREMENT] LoadCompleted event emitted for tab: {}", tab_id);
                            }

                            // Inject product extraction script
                            eprintln!("[PROCUREMENT] Injecting product extraction script...");
                            let js = CefString::from(PRODUCT_EXTRACTION_JS);
                            frame.execute_java_script(Some(&js), None, 0);
                            eprintln!("[PROCUREMENT] JavaScript injection complete");
                        }
                    }
                }
            }));

            if let Err(e) = result {
                eprintln!("[PROCUREMENT] Warning: on_loading_state_change caught panic: {:?}", e);
            }
        }
    }
}

// JavaScript for product extraction
const PRODUCT_EXTRACTION_JS: &str = r#"
(function() {
    const pageInfo = {
        title: document.title,
        url: window.location.href,
        supplier: window.location.hostname
    };
    const products = [];
    const selectors = [
        '[data-product-id]',
        '.product-item',
        '.product-card',
        '[class*="product"]'
    ];
    for (const selector of selectors) {
        const elements = document.querySelectorAll(selector);
        if (elements.length > 0) {
            elements.forEach((el, index) => {
                if (index < 10) {
                    const nameEl = el.querySelector('h2, h3, h4, [class*="title"], [class*="name"]');
                    const priceEl = el.querySelector('[class*="price"]');
                    const imgEl = el.querySelector('img');
                    const linkEl = el.querySelector('a');
                    products.push({
                        name: nameEl?.textContent?.trim() || '',
                        priceText: priceEl?.textContent?.trim() || '',
                        imageUrl: imgEl?.src || null,
                        url: linkEl?.href || window.location.href
                    });
                }
            });
            break;
        }
    }
    console.log('[PROCUREMENT] Page analyzed:', JSON.stringify({
        ...pageInfo,
        productCount: products.length
    }));
    return JSON.stringify({ pageInfo, products });
})();
"#;

// ============================================================================
// Window Delegate
// ============================================================================

wrap_window_delegate! {
    struct ProcurementWindowDelegate {
        browser_view: BrowserView,
    }

    impl ViewDelegate {
        fn on_child_view_changed(
            &self,
            _view: Option<&mut View>,
            _added: ::std::os::raw::c_int,
            _child: Option<&mut View>,
        ) {
        }
    }

    impl PanelDelegate {}

    impl WindowDelegate {
        fn on_window_created(&self, window: Option<&mut Window>) {
            if let Some(window) = window {
                let view = self.browser_view.clone();
                window.add_child_view(Some(&mut (&view).into()));
                window.show();
                eprintln!("[PROCUREMENT] Window created and shown");
            }
        }

        fn on_window_destroyed(&self, _window: Option<&mut Window>) {
            eprintln!("[PROCUREMENT] Window destroyed, shutting down...");
            quit_message_loop();
        }

        fn with_standard_window_buttons(&self, _window: Option<&mut Window>) -> ::std::os::raw::c_int {
            1
        }

        fn can_resize(&self, _window: Option<&mut Window>) -> ::std::os::raw::c_int {
            1
        }

        fn can_maximize(&self, _window: Option<&mut Window>) -> ::std::os::raw::c_int {
            1
        }

        fn can_minimize(&self, _window: Option<&mut Window>) -> ::std::os::raw::c_int {
            1
        }

        fn can_close(&self, _window: Option<&mut Window>) -> ::std::os::raw::c_int {
            1
        }
    }
}

// ============================================================================
// TCP Connection Handler
// ============================================================================

fn handle_tcp_connection(
    stream: TcpStream,
    tabs: &TabStore,
    window: &Arc<Mutex<Option<Window>>>,
) {
    let peer_addr = stream.peer_addr().ok();
    eprintln!("[PROCUREMENT] TCP connection from {:?}", peer_addr);

    let reader = BufReader::new(stream.try_clone().expect("Failed to clone stream"));
    let mut writer = stream;

    for line in reader.lines() {
        match line {
            Ok(line) if !line.is_empty() => {
                match serde_json::from_str::<CefMessage>(&line) {
                    Ok(msg) => {
                        let response = handle_ipc_message(msg, tabs, window);
                        if let Ok(response_json) = serde_json::to_string(&response) {
                            if let Err(e) = writeln!(writer, "{}", response_json) {
                                eprintln!("[PROCUREMENT] TCP write error: {}", e);
                                break;
                            }
                            writer.flush().ok();
                        }
                    }
                    Err(e) => {
                        let error_response = CefResponse::Error {
                            id: "unknown".to_string(),
                            error: format!("Failed to parse message: {}", e),
                        };
                        if let Ok(response_json) = serde_json::to_string(&error_response) {
                            if let Err(e) = writeln!(writer, "{}", response_json) {
                                eprintln!("[PROCUREMENT] TCP write error: {}", e);
                                break;
                            }
                            writer.flush().ok();
                        }
                    }
                }
            }
            Ok(_) => {} // Empty line
            Err(e) => {
                eprintln!("[PROCUREMENT] TCP read error: {}", e);
                break;
            }
        }
    }

    eprintln!("[PROCUREMENT] TCP connection closed from {:?}", peer_addr);
}

// ============================================================================
// IPC Message Handler
// ============================================================================

fn handle_ipc_message(
    msg: CefMessage,
    tabs: &TabStore,
    window: &Arc<Mutex<Option<Window>>>,
) -> CefResponse {
    eprintln!("[PROCUREMENT][IPC] Received message: {:?}", msg.message);

    match msg.message {
        MessageType::Ping => {
            eprintln!("[PROCUREMENT][IPC] Handling Ping");
            CefResponse::Success {
                id: msg.id,
                data: Some(serde_json::json!({"pong": true})),
            }
        }

        MessageType::CreateTab { tab_id, supplier_id, url } => {
            eprintln!("[PROCUREMENT][IPC] === CreateTab START ===");
            eprintln!("[PROCUREMENT][IPC] tab_id: {}", tab_id);
            eprintln!("[PROCUREMENT][IPC] supplier_id: {}", supplier_id);
            eprintln!("[PROCUREMENT][IPC] url: {}", url);

            // Create browser view on UI thread (CRITICAL: browser_view_create must run on UI thread)
            eprintln!("[PROCUREMENT][IPC] Creating browser_view on UI thread...");
            match create_browser_view_on_ui_thread(tab_id.clone(), &url, tabs, window) {
                Some(browser_view) => {
                    eprintln!("[PROCUREMENT][IPC] BrowserView created successfully via UI thread");

                    // Check if browser object exists
                    let has_browser = browser_view.browser().is_some();
                    eprintln!("[PROCUREMENT][IPC] Browser object exists: {}", has_browser);

                    // Store tab info
                    let tab_info = TabInfo {
                        tab_id: tab_id.clone(),
                        supplier_id: supplier_id.clone(),
                        browser_view,
                        url: url.clone(),
                    };

                    if let Ok(mut tabs_guard) = tabs.lock() {
                        tabs_guard.insert(tab_id.clone(), tab_info);
                        eprintln!("[PROCUREMENT][IPC] Tab stored. Total tabs: {}", tabs_guard.len());
                    }

                    eprintln!("[PROCUREMENT][IPC] === CreateTab SUCCESS ===");
                    CefResponse::Success {
                        id: msg.id,
                        data: Some(serde_json::json!({
                            "tab_id": tab_id,
                            "supplier_id": supplier_id,
                            "url": url,
                            "browser_created": has_browser
                        })),
                    }
                }
                None => {
                    eprintln!("[PROCUREMENT][IPC] ERROR: browser_view_create returned None");
                    eprintln!("[PROCUREMENT][IPC] === CreateTab FAILED ===");
                    CefResponse::Error {
                        id: msg.id,
                        error: "Failed to create browser view".to_string(),
                    }
                }
            }
        }

        MessageType::Navigate { tab_id, url } => {
            eprintln!("[PROCUREMENT][IPC] === Navigate START ===");
            eprintln!("[PROCUREMENT][IPC] tab_id: {}", tab_id);
            eprintln!("[PROCUREMENT][IPC] url: {}", url);

            // Navigate on UI thread (browser object is only accessible on UI thread)
            let result = navigate_on_ui_thread(tabs, &tab_id, &url);

            match result {
                Ok(()) => {
                    eprintln!("[PROCUREMENT][IPC] === Navigate SUCCESS ===");
                    CefResponse::Success {
                        id: msg.id,
                        data: Some(serde_json::json!({"navigated": true})),
                    }
                }
                Err(e) => {
                    eprintln!("[PROCUREMENT][IPC] === Navigate FAILED: {} ===", e);
                    CefResponse::Error { id: msg.id, error: e }
                }
            }
        }

        MessageType::ExecuteScript { tab_id, script } => {
            eprintln!("[PROCUREMENT][IPC] === ExecuteScript START ===");
            eprintln!("[PROCUREMENT][IPC] tab_id: {}", tab_id);
            eprintln!("[PROCUREMENT][IPC] script length: {} chars", script.len());

            // Execute script on UI thread (browser object only accessible on UI thread)
            let result = execute_script_on_ui_thread(tabs, &tab_id, &script);

            match &result {
                Ok(()) => eprintln!("[PROCUREMENT][IPC] === ExecuteScript SUCCESS ==="),
                Err(e) => eprintln!("[PROCUREMENT][IPC] === ExecuteScript FAILED: {} ===", e),
            }

            match result {
                Ok(()) => CefResponse::Success {
                    id: msg.id,
                    data: Some(serde_json::json!({"executed": true})),
                },
                Err(e) => CefResponse::Error { id: msg.id, error: e },
            }
        }

        MessageType::ExtractProducts { tab_id, supplier_id } => {
            eprintln!("[PROCUREMENT][IPC] === ExtractProducts START ===");
            eprintln!("[PROCUREMENT][IPC] tab_id: {}", tab_id);
            eprintln!("[PROCUREMENT][IPC] supplier_id: {}", supplier_id);

            // Execute product extraction script on UI thread
            let result = execute_script_on_ui_thread(tabs, &tab_id, PRODUCT_EXTRACTION_JS);

            match &result {
                Ok(()) => eprintln!("[PROCUREMENT][IPC] === ExtractProducts SUCCESS (supplier: {}) ===", supplier_id),
                Err(e) => eprintln!("[PROCUREMENT][IPC] === ExtractProducts FAILED: {} ===", e),
            }

            match result {
                Ok(()) => CefResponse::Success {
                    id: msg.id,
                    data: Some(serde_json::json!({
                        "extraction_started": true,
                        "supplier_id": supplier_id
                    })),
                },
                Err(e) => CefResponse::Error { id: msg.id, error: e },
            }
        }

        MessageType::GetPageInfo { tab_id } => {
            eprintln!("[PROCUREMENT][IPC] === GetPageInfo START ===");
            eprintln!("[PROCUREMENT][IPC] tab_id: {}", tab_id);

            let result = if let Ok(tabs_guard) = tabs.lock() {
                eprintln!("[PROCUREMENT][IPC] Tabs locked, looking for tab...");
                if let Some(tab) = tabs_guard.get(&tab_id) {
                    eprintln!("[PROCUREMENT][IPC] Tab found (supplier: {})", tab.supplier_id);
                    if let Some(browser) = tab.browser_view.browser() {
                        let is_loading = browser.is_loading() != 0;
                        eprintln!("[PROCUREMENT][IPC] Browser found, is_loading: {}", is_loading);
                        if let Some(frame) = browser.main_frame() {
                            let url = CefString::from(&frame.url());
                            let url_str = url.to_string();
                            eprintln!("[PROCUREMENT][IPC] Main frame URL: {}", url_str);
                            let info = serde_json::json!({
                                "tab_id": tab_id,
                                "url": url_str,
                                "title": "", // Would need V8 callback for title
                                "is_loading": is_loading,
                                "supplier_id": tab.supplier_id
                            });
                            eprintln!("[PROCUREMENT][IPC] === GetPageInfo SUCCESS ===");
                            Ok(info)
                        } else {
                            eprintln!("[PROCUREMENT][IPC] ERROR: No main frame");
                            Err("No main frame".to_string())
                        }
                    } else {
                        eprintln!("[PROCUREMENT][IPC] ERROR: No browser (browser view not attached?)");
                        Err("No browser".to_string())
                    }
                } else {
                    eprintln!("[PROCUREMENT][IPC] ERROR: Tab not found");
                    eprintln!("[PROCUREMENT][IPC] Available tabs: {:?}", tabs_guard.keys().collect::<Vec<_>>());
                    Err(format!("Tab not found: {}", tab_id))
                }
            } else {
                eprintln!("[PROCUREMENT][IPC] ERROR: Failed to lock tabs mutex");
                Err("Failed to lock tabs".to_string())
            };

            match result {
                Ok(info) => CefResponse::Success {
                    id: msg.id,
                    data: Some(info),
                },
                Err(e) => {
                    eprintln!("[PROCUREMENT][IPC] === GetPageInfo FAILED: {} ===", e);
                    CefResponse::Error { id: msg.id, error: e }
                }
            }
        }

        MessageType::CheckLoginStatus { tab_id, supplier_id } => {
            // Login detection would require executing JS and checking for login indicators
            // For now, return a placeholder
            CefResponse::Success {
                id: msg.id,
                data: Some(serde_json::json!({
                    "tab_id": tab_id,
                    "supplier_id": supplier_id,
                    "is_logged_in": false,
                    "message": "Login detection not yet implemented"
                })),
            }
        }

        MessageType::CloseTab { tab_id } => {
            let result = if let Ok(mut tabs_guard) = tabs.lock() {
                if let Some(tab) = tabs_guard.remove(&tab_id) {
                    if let Some(browser) = tab.browser_view.browser() {
                        if let Some(host) = browser.host() {
                            host.close_browser(true as _);
                        }
                    }
                    Ok(())
                } else {
                    Err(format!("Tab not found: {}", tab_id))
                }
            } else {
                Err("Failed to lock tabs".to_string())
            };

            match result {
                Ok(()) => CefResponse::Success {
                    id: msg.id,
                    data: Some(serde_json::json!({"closed": true})),
                },
                Err(e) => CefResponse::Error { id: msg.id, error: e },
            }
        }

        MessageType::ListTabs => {
            let tab_list = if let Ok(tabs_guard) = tabs.lock() {
                tabs_guard.iter().map(|(id, tab)| {
                    serde_json::json!({
                        "tab_id": id,
                        "supplier_id": tab.supplier_id,
                        "url": tab.url
                    })
                }).collect::<Vec<_>>()
            } else {
                vec![]
            };

            CefResponse::Success {
                id: msg.id,
                data: Some(serde_json::json!({"tabs": tab_list})),
            }
        }

        MessageType::Shutdown => {
            // Close all tabs and shutdown
            if let Ok(mut tabs_guard) = tabs.lock() {
                for (_, tab) in tabs_guard.drain() {
                    if let Some(browser) = tab.browser_view.browser() {
                        if let Some(host) = browser.host() {
                            host.close_browser(true as _);
                        }
                    }
                }
            }

            // Queue shutdown
            quit_message_loop();

            CefResponse::Success {
                id: msg.id,
                data: Some(serde_json::json!({"shutdown": true})),
            }
        }

        // === Native Input Event Handlers ===

        MessageType::SendMouseMove { tab_id, x, y, modifiers } => {
            let result = if let Ok(tabs_guard) = tabs.lock() {
                if let Some(tab) = tabs_guard.get(&tab_id) {
                    if let Some(browser) = tab.browser_view.browser() {
                        if let Some(host) = browser.host() {
                            let event = MouseEvent { x, y, modifiers };
                            host.send_mouse_move_event(Some(&event), 0);
                            Ok(())
                        } else {
                            Err("No browser host".to_string())
                        }
                    } else {
                        Err("No browser".to_string())
                    }
                } else {
                    Err(format!("Tab not found: {}", tab_id))
                }
            } else {
                Err("Failed to lock tabs".to_string())
            };

            match result {
                Ok(()) => CefResponse::Success {
                    id: msg.id,
                    data: Some(serde_json::json!({"sent": true, "event": "mouse_move", "x": x, "y": y})),
                },
                Err(e) => CefResponse::Error { id: msg.id, error: e },
            }
        }

        MessageType::SendMouseClick { tab_id, x, y, button, mouse_up, click_count, modifiers } => {
            let result = if let Ok(tabs_guard) = tabs.lock() {
                if let Some(tab) = tabs_guard.get(&tab_id) {
                    if let Some(browser) = tab.browser_view.browser() {
                        if let Some(host) = browser.host() {
                            let event = MouseEvent { x, y, modifiers };
                            let button_type = match button {
                                MouseButton::Left => MouseButtonType::LEFT,
                                MouseButton::Middle => MouseButtonType::MIDDLE,
                                MouseButton::Right => MouseButtonType::RIGHT,
                            };
                            host.send_mouse_click_event(
                                Some(&event),
                                button_type,
                                mouse_up as i32,
                                click_count,
                            );
                            Ok(())
                        } else {
                            Err("No browser host".to_string())
                        }
                    } else {
                        Err("No browser".to_string())
                    }
                } else {
                    Err(format!("Tab not found: {}", tab_id))
                }
            } else {
                Err("Failed to lock tabs".to_string())
            };

            match result {
                Ok(()) => CefResponse::Success {
                    id: msg.id,
                    data: Some(serde_json::json!({
                        "sent": true,
                        "event": "mouse_click",
                        "x": x,
                        "y": y,
                        "mouse_up": mouse_up
                    })),
                },
                Err(e) => CefResponse::Error { id: msg.id, error: e },
            }
        }

        MessageType::SendMouseWheel { tab_id, x, y, delta_x, delta_y, modifiers } => {
            let result = if let Ok(tabs_guard) = tabs.lock() {
                if let Some(tab) = tabs_guard.get(&tab_id) {
                    if let Some(browser) = tab.browser_view.browser() {
                        if let Some(host) = browser.host() {
                            let event = MouseEvent { x, y, modifiers };
                            host.send_mouse_wheel_event(Some(&event), delta_x, delta_y);
                            Ok(())
                        } else {
                            Err("No browser host".to_string())
                        }
                    } else {
                        Err("No browser".to_string())
                    }
                } else {
                    Err(format!("Tab not found: {}", tab_id))
                }
            } else {
                Err("Failed to lock tabs".to_string())
            };

            match result {
                Ok(()) => CefResponse::Success {
                    id: msg.id,
                    data: Some(serde_json::json!({
                        "sent": true,
                        "event": "mouse_wheel",
                        "delta_x": delta_x,
                        "delta_y": delta_y
                    })),
                },
                Err(e) => CefResponse::Error { id: msg.id, error: e },
            }
        }

        MessageType::SendKeyEvent { tab_id, event_type, windows_key_code, native_key_code, character, modifiers } => {
            let result = if let Ok(tabs_guard) = tabs.lock() {
                if let Some(tab) = tabs_guard.get(&tab_id) {
                    if let Some(browser) = tab.browser_view.browser() {
                        if let Some(host) = browser.host() {
                            let key_type = match event_type {
                                KeyEventKind::RawKeyDown => KeyEventType::RAWKEYDOWN,
                                KeyEventKind::KeyDown => KeyEventType::KEYDOWN,
                                KeyEventKind::KeyUp => KeyEventType::KEYUP,
                                KeyEventKind::Char => KeyEventType::CHAR,
                            };
                            let event = KeyEvent {
                                size: std::mem::size_of::<KeyEvent>(),
                                type_: key_type,
                                modifiers,
                                windows_key_code,
                                native_key_code,
                                is_system_key: 0,
                                character,
                                unmodified_character: character,
                                focus_on_editable_field: 1,
                            };
                            host.send_key_event(Some(&event));
                            Ok(())
                        } else {
                            Err("No browser host".to_string())
                        }
                    } else {
                        Err("No browser".to_string())
                    }
                } else {
                    Err(format!("Tab not found: {}", tab_id))
                }
            } else {
                Err("Failed to lock tabs".to_string())
            };

            match result {
                Ok(()) => CefResponse::Success {
                    id: msg.id,
                    data: Some(serde_json::json!({
                        "sent": true,
                        "event": "key",
                        "character": character
                    })),
                },
                Err(e) => CefResponse::Error { id: msg.id, error: e },
            }
        }

        // === DOM Query Handlers ===

        MessageType::GetElementBounds { tab_id, selector } => {
            // Execute JavaScript to get element bounds
            let js = format!(r#"
                (function() {{
                    const el = document.querySelector('{}');
                    if (!el) return JSON.stringify({{ found: false }});
                    const rect = el.getBoundingClientRect();
                    return JSON.stringify({{
                        found: true,
                        x: rect.x,
                        y: rect.y,
                        width: rect.width,
                        height: rect.height,
                        centerX: rect.x + rect.width / 2,
                        centerY: rect.y + rect.height / 2,
                        visible: rect.width > 0 && rect.height > 0
                    }});
                }})();
            "#, selector.replace("'", "\\'"));

            let result = if let Ok(tabs_guard) = tabs.lock() {
                if let Some(tab) = tabs_guard.get(&tab_id) {
                    if let Some(browser) = tab.browser_view.browser() {
                        if let Some(frame) = browser.main_frame() {
                            let js_cef = CefString::from(js.as_str());
                            frame.execute_java_script(Some(&js_cef), None, 0);
                            Ok(selector.clone())
                        } else {
                            Err("No main frame".to_string())
                        }
                    } else {
                        Err("No browser".to_string())
                    }
                } else {
                    Err(format!("Tab not found: {}", tab_id))
                }
            } else {
                Err("Failed to lock tabs".to_string())
            };

            match result {
                Ok(sel) => CefResponse::Success {
                    id: msg.id,
                    data: Some(serde_json::json!({
                        "query_started": true,
                        "selector": sel,
                        "note": "Result will be logged to console; use V8 handler for sync results"
                    })),
                },
                Err(e) => CefResponse::Error { id: msg.id, error: e },
            }
        }

        MessageType::WaitForSelector { tab_id, selector, timeout_ms, visible } => {
            // Execute JavaScript to wait for element
            let js = format!(r#"
                (function() {{
                    const selector = '{}';
                    const timeout = {};
                    const checkVisible = {};
                    const startTime = Date.now();

                    function check() {{
                        const el = document.querySelector(selector);
                        if (el) {{
                            if (checkVisible) {{
                                const rect = el.getBoundingClientRect();
                                if (rect.width > 0 && rect.height > 0) {{
                                    console.log(JSON.stringify({{
                                        found: true,
                                        selector: selector,
                                        elapsed: Date.now() - startTime
                                    }}));
                                    return;
                                }}
                            }} else {{
                                console.log(JSON.stringify({{
                                    found: true,
                                    selector: selector,
                                    elapsed: Date.now() - startTime
                                }}));
                                return;
                            }}
                        }}

                        if (Date.now() - startTime < timeout) {{
                            setTimeout(check, 100);
                        }} else {{
                            console.log(JSON.stringify({{
                                found: false,
                                selector: selector,
                                timeout: true
                            }}));
                        }}
                    }}

                    check();
                }})();
            "#, selector.replace("'", "\\'"), timeout_ms, visible);

            let result = if let Ok(tabs_guard) = tabs.lock() {
                if let Some(tab) = tabs_guard.get(&tab_id) {
                    if let Some(browser) = tab.browser_view.browser() {
                        if let Some(frame) = browser.main_frame() {
                            let js_cef = CefString::from(js.as_str());
                            frame.execute_java_script(Some(&js_cef), None, 0);
                            Ok(())
                        } else {
                            Err("No main frame".to_string())
                        }
                    } else {
                        Err("No browser".to_string())
                    }
                } else {
                    Err(format!("Tab not found: {}", tab_id))
                }
            } else {
                Err("Failed to lock tabs".to_string())
            };

            match result {
                Ok(()) => CefResponse::Success {
                    id: msg.id,
                    data: Some(serde_json::json!({
                        "wait_started": true,
                        "selector": selector,
                        "timeout_ms": timeout_ms,
                        "visible": visible
                    })),
                },
                Err(e) => CefResponse::Error { id: msg.id, error: e },
            }
        }

        MessageType::WaitForLoad { tab_id, timeout_ms } => {
            eprintln!("[PROCUREMENT][IPC] === WaitForLoad START ===");
            eprintln!("[PROCUREMENT][IPC] tab_id: {}", tab_id);
            eprintln!("[PROCUREMENT][IPC] timeout_ms: {}", timeout_ms);

            let start = std::time::Instant::now();
            let timeout = std::time::Duration::from_millis(timeout_ms);

            loop {
                let is_loading = if let Ok(tabs_guard) = tabs.lock() {
                    if let Some(tab) = tabs_guard.get(&tab_id) {
                        if let Some(browser) = tab.browser_view.browser() {
                            browser.is_loading() != 0
                        } else {
                            // No browser yet - still loading/initializing
                            true
                        }
                    } else {
                        eprintln!("[PROCUREMENT][IPC] WaitForLoad: Tab not found: {}", tab_id);
                        return CefResponse::Error {
                            id: msg.id,
                            error: format!("Tab not found: {}", tab_id),
                        };
                    }
                } else {
                    eprintln!("[PROCUREMENT][IPC] WaitForLoad: Failed to lock tabs");
                    return CefResponse::Error {
                        id: msg.id,
                        error: "Failed to lock tabs".to_string(),
                    };
                };

                if !is_loading {
                    let elapsed = start.elapsed().as_millis();
                    eprintln!("[PROCUREMENT][IPC] === WaitForLoad SUCCESS ({}ms) ===", elapsed);
                    return CefResponse::Success {
                        id: msg.id,
                        data: Some(serde_json::json!({
                            "loaded": true,
                            "elapsed_ms": elapsed
                        })),
                    };
                }

                if start.elapsed() > timeout {
                    eprintln!("[PROCUREMENT][IPC] === WaitForLoad TIMEOUT ({}ms) ===", timeout_ms);
                    return CefResponse::Error {
                        id: msg.id,
                        error: format!("Timeout waiting for page load after {}ms", timeout_ms),
                    };
                }

                // Poll every 100ms
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }
}

fn send_response(response: &CefResponse) {
    if let Ok(json) = serde_json::to_string(response) {
        // Use writeln! to handle broken pipe gracefully (when Tauri stops reading stdout)
        use std::io::Write;
        let _ = writeln!(std::io::stdout(), "{}", json);
        let _ = std::io::stdout().flush();
    }
}

// ============================================================================
// Main Entry Point
// ============================================================================

fn main() {
    println!("[PROCUREMENT] Comet Procure CEF Browser Starting...");
    println!("[PROCUREMENT] Configured suppliers:");
    for (name, url) in SUPPLIER_URLS {
        println!("  - {}: {}", name, url);
    }

    // Check for IPC mode flag and TCP port
    let cli_args: Vec<String> = std::env::args().collect();
    let is_ipc_mode = cli_args.iter().any(|a| a == "--ipc-mode");
    let tcp_port: Option<u16> = cli_args.iter()
        .find(|a| a.starts_with("--tcp-port="))
        .and_then(|a| a.strip_prefix("--tcp-port="))
        .and_then(|p| p.parse().ok());

    if is_ipc_mode {
        if let Some(port) = tcp_port {
            println!("[PROCUREMENT] IPC mode enabled via TCP on port {}", port);
        } else {
            println!("[PROCUREMENT] IPC mode enabled via stdin/stdout");
        }
    }

    #[cfg(target_os = "macos")]
    let _loader = {
        let loader = library_loader::LibraryLoader::new(&std::env::current_exe().unwrap(), false);
        assert!(loader.load());
        loader
    };

    #[cfg(target_os = "macos")]
    {
        use objc2::{
            ClassType, MainThreadMarker, msg_send,
            rc::Retained,
            runtime::{AnyObject, NSObjectProtocol},
        };
        use objc2_app_kit::NSApp;

        use application::SimpleApplication;

        let mtm = MainThreadMarker::new().unwrap();

        unsafe {
            let _: Retained<AnyObject> = msg_send![SimpleApplication::class(), sharedApplication];
        }

        assert!(NSApp(mtm).isKindOfClass(SimpleApplication::class()));
    }

    let _ = api_hash(sys::CEF_API_VERSION_LAST, 0);

    let args = Args::new();
    let cmd = args.as_cmd_line().unwrap();

    let switch = CefString::from("type");
    let is_browser_process = cmd.has_switch(Some(&switch)) != 1;

    let window = Arc::new(Mutex::new(None));
    let ipc_state = Arc::new(Mutex::new(IpcState::new(is_ipc_mode)));
    let mut app = ProcurementApp::new(window.clone(), ipc_state.clone());

    let ret = execute_process(
        Some(args.as_main_args()),
        Some(&mut app),
        std::ptr::null_mut(),
    );

    if is_browser_process {
        println!("[PROCUREMENT] Launching browser process");
        assert!(ret == -1, "cannot execute browser process");
    } else {
        let process_type = CefString::from(&cmd.switch_value(Some(&switch)));
        println!("[PROCUREMENT] Launching helper process: {process_type}");
        assert!(ret >= 0, "cannot execute non-browser process");
        return;
    }

    // Configure cache path (can be overridden via --cache-path)
    let cache_path = cli_args.iter()
        .find(|a| a.starts_with("--cache-path="))
        .map(|a| a.strip_prefix("--cache-path=").unwrap().to_string())
        .unwrap_or_else(|| {
            std::env::temp_dir()
                .join("comet_procure_cache")
                .to_string_lossy()
                .to_string()
        });

    std::fs::create_dir_all(&cache_path).ok();
    println!("[PROCUREMENT] Using cache path: {}", cache_path);
    std::io::stdout().flush().ok();

    eprintln!("[PROCUREMENT] Creating CEF settings...");

    let settings = Settings {
        no_sandbox: !cfg!(feature = "sandbox") as _,
        root_cache_path: CefString::from(cache_path.as_str()),
        ..Default::default()
    };

    eprintln!("[PROCUREMENT] About to call cef::initialize...");
    let init_result = initialize(
        Some(args.as_main_args()),
        Some(&settings),
        Some(&mut app),
        std::ptr::null_mut(),
    );
    eprintln!("[PROCUREMENT] cef::initialize returned: {}", init_result);

    if init_result != 1 {
        eprintln!("[PROCUREMENT] CEF initialization failed with code: {}", init_result);
        eprintln!("[PROCUREMENT] This may happen when running as a subprocess with piped I/O");
        eprintln!("[PROCUREMENT] Ensure CEF framework is properly loaded and app has correct permissions");

        // Send error response to Tauri if in IPC mode
        if is_ipc_mode {
            let error_response = CefResponse::Error {
                id: "init".to_string(),
                error: format!("CEF initialization failed with code: {}", init_result),
            };
            send_response(&error_response);
        }

        std::process::exit(1);
    }

    println!("[PROCUREMENT] CEF initialized successfully");

    // Start IPC listener thread if in IPC mode
    if is_ipc_mode {
        let tabs = {
            let state = ipc_state.lock().unwrap();
            state.tabs.clone()
        };
        let window_clone = window.clone();

        if let Some(port) = tcp_port {
            // TCP-based IPC (preferred for macOS subprocess launching)
            std::thread::spawn(move || {
                let addr = format!("127.0.0.1:{}", port);
                match TcpListener::bind(&addr) {
                    Ok(listener) => {
                        println!("[PROCUREMENT] TCP server listening on {}", addr);
                        // Print ready signal that Tauri can detect
                        println!("{{\"status\":\"Ready\",\"port\":{}}}", port);
                        std::io::stdout().flush().ok();

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    let tabs_clone = tabs.clone();
                                    let window_clone2 = window_clone.clone();
                                    std::thread::spawn(move || {
                                        handle_tcp_connection(stream, &tabs_clone, &window_clone2);
                                    });
                                }
                                Err(e) => {
                                    eprintln!("[PROCUREMENT] TCP accept error: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[PROCUREMENT] Failed to bind TCP listener: {}", e);
                        let error_response = CefResponse::Error {
                            id: "init".to_string(),
                            error: format!("Failed to bind TCP port {}: {}", port, e),
                        };
                        send_response(&error_response);
                    }
                }
                eprintln!("[PROCUREMENT] TCP IPC listener ended");
            });
        } else {
            // stdin/stdout-based IPC (fallback, may not work on macOS subprocess)
            std::thread::spawn(move || {
                let stdin = std::io::stdin();
                let reader = BufReader::new(stdin.lock());

                for line in reader.lines() {
                    match line {
                        Ok(line) if !line.is_empty() => {
                            match serde_json::from_str::<CefMessage>(&line) {
                                Ok(msg) => {
                                    let response = handle_ipc_message(msg, &tabs, &window_clone);
                                    send_response(&response);
                                }
                                Err(e) => {
                                    let error_response = CefResponse::Error {
                                        id: "unknown".to_string(),
                                        error: format!("Failed to parse message: {}", e),
                                    };
                                    send_response(&error_response);
                                }
                            }
                        }
                        Ok(_) => {} // Empty line
                        Err(e) => {
                            eprintln!("[PROCUREMENT] Error reading stdin: {}", e);
                            break;
                        }
                    }
                }

                eprintln!("[PROCUREMENT] IPC listener ended");
            });
        }
    }

    eprintln!("[PROCUREMENT] Running message loop...");
    run_message_loop();

    if !is_ipc_mode {
        let window = window.lock().expect("Failed to lock window");
        let window = window.as_ref().expect("Window is None");
        assert!(window.has_one_ref());
    }

    eprintln!("[PROCUREMENT] Shutting down CEF...");
    shutdown();
    eprintln!("[PROCUREMENT] Goodbye!");
}

#[cfg(target_os = "macos")]
mod application {
    use std::cell::Cell;

    use cef::application_mac::{CefAppProtocol, CrAppControlProtocol, CrAppProtocol};
    use objc2::{DefinedClass, define_class, runtime::Bool};
    use objc2_app_kit::NSApplication;

    pub struct SimpleApplicationIvars {
        handling_send_event: Cell<Bool>,
    }

    define_class!(
        #[unsafe(super(NSApplication))]
        #[ivars = SimpleApplicationIvars]
        pub struct SimpleApplication;

        unsafe impl CrAppControlProtocol for SimpleApplication {
            #[unsafe(method(setHandlingSendEvent:))]
            unsafe fn set_handling_send_event(&self, handling_send_event: Bool) {
                self.ivars().handling_send_event.set(handling_send_event);
            }
        }

        unsafe impl CrAppProtocol for SimpleApplication {
            #[unsafe(method(isHandlingSendEvent))]
            unsafe fn is_handling_send_event(&self) -> Bool {
                self.ivars().handling_send_event.get()
            }
        }

        unsafe impl CefAppProtocol for SimpleApplication {}
    );
}
