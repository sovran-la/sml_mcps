//! MCP Server
//!
//! Core server implementation with generic context support.

use crate::pagination::{DEFAULT_PAGE_SIZE, PageState, paginate};
use crate::transport::Transport;
use crate::types::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

//
// Tool Environment - passed to tools during execution
//

/// Environment provided to tools during execution
///
/// Gives tools access to notifications, progress reporting, and resources.
pub struct ToolEnv<'a> {
    /// `None` when the server was driven without a transport (direct dispatch
    /// in tests, for example). Sending is then a no-op rather than a panic.
    transport: Option<&'a Arc<Mutex<dyn Transport>>>,
    resources: &'a HashMap<String, Box<dyn Resource>>,
    /// Minimum severity the client asked for via `logging/setLevel`.
    log_level: LogLevel,
    /// What to do with log records that the client will not see.
    stderr_logging: StderrLogging,
    /// Default `logger` name on emitted log records.
    logger: &'a str,
}

impl<'a> ToolEnv<'a> {
    /// Send a notification to the client
    ///
    /// Returns `Ok(())` without doing anything when the server has no
    /// transport attached.
    pub fn send_notification(&self, method: &str, params: Option<Value>) -> Result<()> {
        let Some(transport) = self.transport else {
            return Ok(());
        };
        let notification = JsonRpcMessage::notification(method, params);
        let mut transport = transport
            .lock()
            .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
        transport.write(&notification)
    }

    /// Send a log message to the client, falling back to stderr.
    ///
    /// The record reaches the client as a `notifications/message` only when
    /// `level` meets the threshold the client set via `logging/setLevel` (the
    /// server's configured default until then). Anything the client will not
    /// see is written to stderr instead, so a suppressed log is still
    /// diagnosable - the stdio transport explicitly permits stderr for
    /// "any logging purposes including informational, debug, and error
    /// messages."
    ///
    /// This never fails the calling tool: a transport write error is reported
    /// on stderr and swallowed. Logging is diagnostics, not business logic.
    pub fn log(&self, level: LogLevel, message: impl Into<String>) -> Result<()> {
        self.log_data(level, None, Value::String(message.into()));
        Ok(())
    }

    /// Send a structured log record with an optional logger name.
    ///
    /// `data` may be any JSON value; the spec places no constraint on it
    /// beyond being JSON-serializable. `logger` defaults to the server name.
    pub fn log_data(&self, level: LogLevel, logger: Option<&str>, data: Value) {
        let logger = logger.unwrap_or(self.logger);
        let delivered = if level >= self.log_level {
            self.send_notification(
                "notifications/message",
                Some(serde_json::json!({
                    "level": level.as_str(),
                    "logger": logger,
                    "data": data,
                })),
            )
            .inspect_err(|e| eprintln!("[{}] failed to deliver log record: {}", logger, e))
            .is_ok()
                && self.transport.is_some()
        } else {
            false
        };

        let write_stderr = match self.stderr_logging {
            StderrLogging::Never => false,
            StderrLogging::Fallback => !delivered,
            StderrLogging::Always => true,
        };

        if write_stderr {
            match data.as_str() {
                // Strings are the common case; print them unquoted.
                Some(text) => eprintln!("[{}] {}: {}", logger, level.as_str(), text),
                None => eprintln!("[{}] {}: {}", logger, level.as_str(), data),
            }
        }
    }

    /// The minimum log severity the client is currently accepting.
    pub fn log_level(&self) -> LogLevel {
        self.log_level
    }

    /// Would a record at `level` be delivered to the client?
    ///
    /// Useful to skip building an expensive log payload that would be dropped.
    pub fn log_enabled(&self, level: LogLevel) -> bool {
        level >= self.log_level
    }

    /// Send progress update for long-running operations
    pub fn send_progress(&self, token: &str, progress: f64, total: Option<f64>) -> Result<()> {
        let mut params = serde_json::json!({
            "progressToken": token,
            "progress": progress
        });
        if let Some(t) = total {
            params["total"] = serde_json::json!(t);
        }
        self.send_notification("notifications/progress", Some(params))
    }

    /// List all resource URIs
    pub fn list_resources(&self) -> Vec<String> {
        self.resources.keys().cloned().collect()
    }

    /// Get a resource by URI
    pub fn get_resource(&self, uri: &str) -> Option<&dyn Resource> {
        self.resources.get(uri).map(|r| r.as_ref())
    }
}

/// Where log records go when the client will not receive them.
///
/// The MCP stdio transport permits servers to write anything to stderr, and
/// that is the sanctioned escape hatch for diagnostics a client did not
/// subscribe to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StderrLogging {
    /// Drop suppressed records entirely.
    Never,
    /// Write to stderr only when the record did not reach the client, because
    /// it was below the client's threshold, there was no transport, or the
    /// write failed. This is the default: it guarantees no log ever vanishes
    /// silently, without duplicating records the client already has.
    #[default]
    Fallback,
    /// Write every record to stderr, in addition to delivering it.
    Always,
}

/// Log severity levels
///
/// These are the eight syslog severities from RFC 5424, which is what MCP's
/// logging utility uses. Ordering is by increasing severity, so a record is
/// emitted when `record_level >= configured_minimum`.
///
/// Marked `#[non_exhaustive]`: match on it with a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[non_exhaustive]
pub enum LogLevel {
    /// Detailed debugging information (function entry/exit points).
    Debug,
    /// General informational messages (operation progress updates).
    #[default]
    Info,
    /// Normal but significant events (configuration changes).
    Notice,
    /// Warning conditions (deprecated feature usage).
    Warning,
    /// Error conditions (operation failures).
    Error,
    /// Critical conditions (system component failures).
    Critical,
    /// Action must be taken immediately (data corruption detected).
    Alert,
    /// System is unusable (complete system failure).
    Emergency,
}

impl LogLevel {
    /// The wire name for this level.
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Notice => "notice",
            LogLevel::Warning => "warning",
            LogLevel::Error => "error",
            LogLevel::Critical => "critical",
            LogLevel::Alert => "alert",
            LogLevel::Emergency => "emergency",
        }
    }

    /// Parse a wire level name, case-insensitively.
    ///
    /// Returns `None` for anything not in RFC 5424, which callers turn into
    /// `-32602 Invalid params`.
    pub fn from_wire(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "debug" => Some(LogLevel::Debug),
            "info" => Some(LogLevel::Info),
            "notice" => Some(LogLevel::Notice),
            "warning" => Some(LogLevel::Warning),
            "error" => Some(LogLevel::Error),
            "critical" => Some(LogLevel::Critical),
            "alert" => Some(LogLevel::Alert),
            "emergency" => Some(LogLevel::Emergency),
            _ => None,
        }
    }

    /// Every level, ascending by severity.
    pub const ALL: [LogLevel; 8] = [
        LogLevel::Debug,
        LogLevel::Info,
        LogLevel::Notice,
        LogLevel::Warning,
        LogLevel::Error,
        LogLevel::Critical,
        LogLevel::Alert,
        LogLevel::Emergency,
    ];
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for LogLevel {
    type Err = McpError;

    fn from_str(s: &str) -> Result<Self> {
        LogLevel::from_wire(s)
            .ok_or_else(|| McpError::InvalidParams(format!("Unknown log level: {}", s)))
    }
}

//
// Tool trait
//

/// Tool implementation trait
///
/// Generic over context type `C` for shared state between tools.
pub trait Tool<C>: Send + Sync {
    /// Tool name (must be unique)
    fn name(&self) -> &str;

    /// Human-readable description
    fn description(&self) -> &str;

    /// JSON Schema for input arguments
    ///
    /// Interpreted as JSON Schema 2020-12 unless it carries a `$schema` key.
    fn schema(&self) -> Value;

    /// Human-readable display name, distinct from the programmatic `name`.
    ///
    /// Default: `None`, so clients fall back to `name`.
    fn title(&self) -> Option<String> {
        None
    }

    /// JSON Schema describing the tool's `structuredContent`.
    ///
    /// Declaring this is a promise: every successful result **MUST** carry
    /// `structuredContent` conforming to the schema, and the server validates
    /// that promise before responding.
    ///
    /// Default: `None` (unstructured output only).
    fn output_schema(&self) -> Option<Value> {
        None
    }

    /// Icons for display in user interfaces.
    ///
    /// Default: empty.
    fn icons(&self) -> Vec<Icon> {
        Vec::new()
    }

    /// Whether this tool can be invoked as a task.
    ///
    /// Default: [`TaskSupport::Forbidden`]. Returning `Optional` or `Required`
    /// only has an effect once the server has tasks enabled via
    /// [`Server::enable_tasks`].
    fn task_support(&self) -> TaskSupport {
        TaskSupport::Forbidden
    }

    /// Tool behavior annotations (hints for clients)
    ///
    /// Override this to provide hints about tool behavior:
    /// - `read_only_hint`: Tool only reads, never modifies
    /// - `idempotent_hint`: Safe to retry (same result on repeat calls)
    /// - `destructive_hint`: May overwrite or heavily mutate data
    /// - `open_world_hint`: Tool touches an open world (network, shared store)
    ///
    /// Default returns None (no hints).
    fn annotations(&self) -> Option<ToolAnnotations> {
        None
    }

    /// Protocol metadata for this tool.
    ///
    /// Default: `None`.
    fn meta(&self) -> Option<Meta> {
        None
    }

    /// Execute the tool
    fn execute(&self, args: Value, context: &mut C, env: &ToolEnv) -> Result<CallToolResult>;

    /// Build the wire representation of this tool.
    fn as_protocol_tool(&self) -> crate::types::Tool {
        let task_support = self.task_support();
        crate::types::Tool {
            name: self.name().to_string(),
            title: self.title(),
            description: Some(self.description().to_string()),
            input_schema: self.schema(),
            output_schema: self.output_schema(),
            annotations: self.annotations(),
            icons: self.icons(),
            // `forbidden` is the default; omitting it keeps `tools/list`
            // compact and means the same thing to a client.
            execution: (task_support != TaskSupport::Forbidden).then_some(ToolExecution {
                task_support: Some(task_support),
            }),
            meta: self.meta(),
        }
    }
}

//
// Resource trait
//

/// Resource implementation trait
pub trait Resource: Send + Sync {
    /// Resource URI (must be unique)
    fn uri(&self) -> String;

    /// Human-readable name
    fn name(&self) -> String;

    /// Description
    fn description(&self) -> String;

    /// MIME type
    fn mime_type(&self) -> String;

    /// Get resource content
    fn content(&self) -> Vec<ResourceContent>;

    /// Human-readable display name, distinct from `name`. Default: `None`.
    fn title(&self) -> Option<String> {
        None
    }

    /// Size in bytes, when cheaply known. Default: `None`.
    fn size(&self) -> Option<u64> {
        None
    }

    /// Icons for display in user interfaces. Default: empty.
    fn icons(&self) -> Vec<Icon> {
        Vec::new()
    }

    /// Audience / priority / last-modified hints. Default: `None`.
    fn annotations(&self) -> Option<Annotations> {
        None
    }

    /// Protocol metadata. Default: `None`.
    fn meta(&self) -> Option<Meta> {
        None
    }

    /// Convert to protocol Resource type
    fn as_protocol_resource(&self) -> crate::types::Resource {
        crate::types::Resource {
            uri: self.uri(),
            name: self.name(),
            title: self.title(),
            description: Some(self.description()),
            mime_type: Some(self.mime_type()),
            size: self.size(),
            icons: self.icons(),
            annotations: self.annotations(),
            meta: self.meta(),
        }
    }
}

//
// Prompt trait
//

/// Prompt implementation trait
pub trait PromptDef: Send + Sync {
    /// Prompt name (must be unique)
    fn name(&self) -> &str;

    /// Human-readable description
    fn description(&self) -> Option<&str>;

    /// Argument definitions
    fn arguments(&self) -> Vec<PromptArgument>;

    /// Generate prompt messages
    fn get_messages(&self, args: &HashMap<String, String>) -> Result<Vec<PromptMessage>>;

    /// Human-readable display name, distinct from `name`. Default: `None`.
    fn title(&self) -> Option<String> {
        None
    }

    /// Icons for display in user interfaces. Default: empty.
    fn icons(&self) -> Vec<Icon> {
        Vec::new()
    }

    /// Protocol metadata. Default: `None`.
    fn meta(&self) -> Option<Meta> {
        None
    }

    /// Convert to protocol Prompt type
    fn as_protocol_prompt(&self) -> Prompt {
        Prompt {
            name: self.name().to_string(),
            title: self.title(),
            description: self.description().map(String::from),
            arguments: self.arguments(),
            icons: self.icons(),
            meta: self.meta(),
        }
    }
}

//
// Server configuration
//

/// Server configuration
#[derive(Clone)]
pub struct ServerConfig {
    pub name: String,
    pub version: String,
    pub instructions: Option<String>,
    /// Human-readable display name for this server (2025-06-18).
    pub title: Option<String>,
    /// Human-readable description of what this server does (2025-11-25).
    pub description: Option<String>,
    /// Project or documentation URL.
    pub website_url: Option<String>,
    /// Icons identifying this server in user interfaces (2025-11-25).
    pub icons: Vec<Icon>,
    /// Page size for list operations (tools, resources, prompts)
    pub page_size: usize,
    /// Minimum log severity delivered to clients that never call
    /// `logging/setLevel`.
    ///
    /// The spec leaves the pre-`setLevel` default to the server. `Info` keeps
    /// useful records flowing without drowning clients in `debug`.
    pub default_log_level: LogLevel,
    /// Where log records go when the client will not receive them
    /// (default: [`StderrLogging::Fallback`]).
    pub stderr_logging: StderrLogging,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            name: "sml_mcps".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            instructions: None,
            title: None,
            description: None,
            website_url: None,
            icons: Vec::new(),
            page_size: DEFAULT_PAGE_SIZE,
            default_log_level: LogLevel::Info,
            stderr_logging: StderrLogging::default(),
        }
    }
}

//
// MCP Server
//

/// MCP Server - generic over context type
pub struct Server<C> {
    config: ServerConfig,
    tools: HashMap<String, Box<dyn Tool<C>>>,
    resources: HashMap<String, Box<dyn Resource>>,
    resource_templates: Vec<ResourceTemplate>,
    prompts: HashMap<String, Box<dyn PromptDef>>,
    transport: Option<Arc<Mutex<dyn Transport>>>,
    initialized: bool,
    /// Minimum log severity, as last set by `logging/setLevel`.
    log_level: LogLevel,
}

impl<C: Send + Sync + 'static> Server<C> {
    /// Create a new server with the given configuration
    pub fn new(config: ServerConfig) -> Self {
        let log_level = config.default_log_level;
        Self {
            config,
            tools: HashMap::new(),
            resources: HashMap::new(),
            resource_templates: Vec::new(),
            prompts: HashMap::new(),
            transport: None,
            initialized: false,
            log_level,
        }
    }

    /// This server's identity as advertised to clients.
    fn server_info(&self) -> Implementation {
        Implementation {
            name: self.config.name.clone(),
            version: self.config.version.clone(),
            title: self.config.title.clone(),
            description: self.config.description.clone(),
            icons: self.config.icons.clone(),
            website_url: self.config.website_url.clone(),
        }
    }

    /// Build the environment handed to a tool for one invocation.
    fn tool_env(&self) -> ToolEnv<'_> {
        ToolEnv {
            transport: self.transport.as_ref(),
            resources: &self.resources,
            log_level: self.log_level,
            stderr_logging: self.config.stderr_logging,
            logger: &self.config.name,
        }
    }

    /// Add a tool to the server
    pub fn add_tool(&mut self, tool: impl Tool<C> + 'static) -> Result<()> {
        let name = tool.name().to_string();
        if self.tools.contains_key(&name) {
            return Err(McpError::Internal(format!("Duplicate tool: {}", name)));
        }
        self.tools.insert(name, Box::new(tool));
        Ok(())
    }

    /// Add a resource to the server
    pub fn add_resource(&mut self, resource: impl Resource + 'static) -> Result<()> {
        let uri = resource.uri();
        if self.resources.contains_key(&uri) {
            return Err(McpError::Internal(format!("Duplicate resource: {}", uri)));
        }
        self.resources.insert(uri, Box::new(resource));
        Ok(())
    }

    /// Add a resource template, exposed via `resources/templates/list`
    ///
    /// Templates are static metadata (an RFC 6570 URI template plus display
    /// fields); reads still go through `resources/read` against a concrete URI.
    pub fn add_resource_template(&mut self, template: ResourceTemplate) -> Result<()> {
        if self
            .resource_templates
            .iter()
            .any(|t| t.uri_template == template.uri_template)
        {
            return Err(McpError::Internal(format!(
                "Duplicate resource template: {}",
                template.uri_template
            )));
        }
        self.resource_templates.push(template);
        Ok(())
    }

    /// Add a prompt to the server
    pub fn add_prompt(&mut self, prompt: impl PromptDef + 'static) -> Result<()> {
        let name = prompt.name().to_string();
        if self.prompts.contains_key(&name) {
            return Err(McpError::Internal(format!("Duplicate prompt: {}", name)));
        }
        self.prompts.insert(name, Box::new(prompt));
        Ok(())
    }

    /// Run the server with the given transport and context (for stdio - continuous loop)
    pub fn start<T: Transport + 'static>(&mut self, transport: T, mut context: C) -> Result<()> {
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(transport));
        self.transport = Some(transport.clone());

        eprintln!(
            "MCP Server `{}` started, version {}",
            self.config.name, self.config.version
        );

        loop {
            // Read message
            let message = {
                let mut t = transport
                    .lock()
                    .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
                match t.read() {
                    Ok(msg) => msg,
                    Err(McpError::TransportClosed) => break,
                    Err(e) => return Err(e),
                }
            };

            // Handle message
            if let Some(response) = self.handle_message(message, &mut context)? {
                let mut t = transport
                    .lock()
                    .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
                t.write(&response)?;
            }
        }

        Ok(())
    }

    /// Process a single request (for HTTP - single request/response)
    ///
    /// Pass an Arc<Mutex<T>> so that:
    /// 1. ToolEnv can write notifications to the transport during execution
    /// 2. You can extract buffered messages after processing
    ///
    /// For HttpTransport, use `transport.take_sse_response()` after this call.
    pub fn process_one<T: Transport + 'static>(
        &mut self,
        transport: Arc<Mutex<T>>,
        context: &mut C,
    ) -> Result<()> {
        // Store as dyn Transport for ToolEnv
        self.transport = Some(transport.clone() as Arc<Mutex<dyn Transport>>);

        // Read message
        let message = {
            let mut t = transport
                .lock()
                .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
            t.read()?
        };

        // Handle and write response
        if let Some(response) = self.handle_message(message, context)? {
            let mut t = transport
                .lock()
                .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
            t.write(&response)?;
        }

        Ok(())
    }

    /// Handle a single message
    fn handle_message(
        &mut self,
        message: JsonRpcMessage,
        context: &mut C,
    ) -> Result<Option<JsonRpcMessage>> {
        match message {
            JsonRpcMessage::Request(request) => {
                let response = self.handle_request(request, context);
                Ok(Some(response))
            }
            JsonRpcMessage::Notification(notification) => {
                self.handle_notification(notification)?;
                Ok(None)
            }
            JsonRpcMessage::Response(_) => Ok(None),
        }
    }

    /// Handle a request and return a response
    fn handle_request(&mut self, request: JsonRpcRequest, context: &mut C) -> JsonRpcMessage {
        let id = request.id.clone();

        match self.dispatch_request(&request, context) {
            Ok(result) => JsonRpcMessage::response(id, result),
            Err(e) => JsonRpcMessage::error(id, e.to_jsonrpc_error()),
        }
    }

    /// Dispatch a request to the appropriate handler
    fn dispatch_request(&mut self, request: &JsonRpcRequest, context: &mut C) -> Result<Value> {
        match request.method.as_str() {
            "initialize" => self.handle_initialize(request),
            "ping" => self.handle_ping(),
            "tools/list" => self.handle_list_tools(request),
            "tools/call" => self.handle_call_tool(request, context),
            "resources/list" => self.handle_list_resources(request),
            "resources/templates/list" => self.handle_list_resource_templates(request),
            "resources/read" => self.handle_read_resource(request),
            "prompts/list" => self.handle_list_prompts(request),
            "prompts/get" => self.handle_get_prompt(request),
            "logging/setLevel" => self.handle_set_level(request),
            method => Err(McpError::MethodNotFound(method.to_string())),
        }
    }

    /// `logging/setLevel` - set the minimum severity delivered to this client.
    fn handle_set_level(&mut self, request: &JsonRpcRequest) -> Result<Value> {
        let params: SetLevelParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())
                .map_err(|e| McpError::InvalidParams(format!("Invalid setLevel params: {}", e)))?,
            None => return Err(McpError::InvalidParams("Missing params".into())),
        };

        // Unknown levels are Invalid params, per the logging spec.
        self.log_level = LogLevel::from_wire(&params.level).ok_or_else(|| {
            McpError::InvalidParams(format!(
                "Unknown log level `{}`; expected one of: {}",
                params.level,
                LogLevel::ALL
                    .iter()
                    .map(|l| l.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;

        Ok(serde_json::json!({}))
    }

    fn handle_initialize(&mut self, request: &JsonRpcRequest) -> Result<Value> {
        let _params: InitializeParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())?,
            None => InitializeParams::default(),
        };

        self.initialized = true;

        let result = InitializeResult {
            protocol_version: PROTOCOL_VERSION.to_string(),
            capabilities: ServerCapabilities {
                tools: if self.tools.is_empty() {
                    None
                } else {
                    Some(ToolsCapability::default())
                },
                resources: if self.resources.is_empty() && self.resource_templates.is_empty() {
                    None
                } else {
                    Some(ResourcesCapability::default())
                },
                prompts: if self.prompts.is_empty() {
                    None
                } else {
                    Some(PromptsCapability::default())
                },
                // The framework always implements `logging/setLevel` and can
                // emit `notifications/message`, and a server that emits them
                // MUST declare this. We used to emit without declaring.
                logging: Some(serde_json::json!({})),
                experimental: None,
            },
            server_info: self.server_info(),
            instructions: self.config.instructions.clone(),
        };

        Ok(serde_json::to_value(result)?)
    }

    fn handle_ping(&self) -> Result<Value> {
        Ok(serde_json::to_value(PingResult {})?)
    }

    fn handle_list_tools(&self, request: &JsonRpcRequest) -> Result<Value> {
        let params: ListToolsParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())?,
            None => ListToolsParams::default(),
        };

        // Collect all tools (sorted for consistent pagination)
        let mut all_tools: Vec<crate::types::Tool> =
            self.tools.values().map(|t| t.as_protocol_tool()).collect();
        all_tools.sort_by(|a, b| a.name.cmp(&b.name));

        // Apply pagination
        let state = PageState::from_cursor(params.cursor.as_deref(), self.config.page_size);
        let (tools, next_cursor) = paginate(&all_tools, &state);

        Ok(serde_json::to_value(ListToolsResult {
            tools,
            next_cursor,
        })?)
    }

    fn handle_call_tool(&mut self, request: &JsonRpcRequest, context: &mut C) -> Result<Value> {
        let params: CallToolParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())?,
            None => return Err(McpError::InvalidParams("Missing params".into())),
        };

        // An unknown tool is a protocol error, and the spec's own example gives
        // it -32602. It is not something a model can fix by retrying with
        // different arguments, so it must not be an `isError` result.
        let tool = self
            .tools
            .get(&params.name)
            .ok_or_else(|| McpError::InvalidParams(format!("Unknown tool: {}", params.name)))?;

        let env = self.tool_env();

        let outcome = tool.execute(
            params.arguments.unwrap_or(serde_json::json!({})),
            context,
            &env,
        );

        // Tool *execution* failures are results, not JSON-RPC errors: the spec
        // wants the model to see actionable text and self-correct. Only
        // genuinely protocol-level failures propagate as errors.
        let result = match outcome {
            Ok(result) => result,
            Err(McpError::ToolError(message)) => CallToolResult::error(message),
            Err(McpError::InvalidParams(message)) => {
                // SEP-1303: input validation errors are Tool Execution Errors.
                CallToolResult::error(format!("Invalid arguments: {}", message))
            }
            Err(other) => return Err(other),
        };

        // Declaring an outputSchema is a promise that every successful result
        // carries conforming structured content. Catch a broken promise here
        // rather than shipping malformed data to the client.
        if !result.is_error {
            if let Some(schema) = tool.output_schema() {
                if let Err(problem) = crate::schema_check::validate_structured_output(
                    &schema,
                    result.structured_content.as_ref(),
                ) {
                    return Err(McpError::Internal(format!(
                        "Tool `{}` declares an outputSchema but returned {}",
                        params.name, problem
                    )));
                }
            }
        }

        Ok(serde_json::to_value(result)?)
    }

    fn handle_list_resources(&self, request: &JsonRpcRequest) -> Result<Value> {
        let params: ListResourcesParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())?,
            None => ListResourcesParams::default(),
        };

        // Collect all resources (sorted for consistent pagination)
        let mut all_resources: Vec<crate::types::Resource> = self
            .resources
            .values()
            .map(|r| r.as_protocol_resource())
            .collect();
        all_resources.sort_by(|a, b| a.uri.cmp(&b.uri));

        // Apply pagination
        let state = PageState::from_cursor(params.cursor.as_deref(), self.config.page_size);
        let (resources, next_cursor) = paginate(&all_resources, &state);

        Ok(serde_json::to_value(ListResourcesResult {
            resources,
            next_cursor,
        })?)
    }

    fn handle_list_resource_templates(&self, request: &JsonRpcRequest) -> Result<Value> {
        let params: ListResourceTemplatesParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())?,
            None => ListResourceTemplatesParams::default(),
        };

        // Sorted for stable pagination, same as the other list handlers.
        let mut all: Vec<ResourceTemplate> = self.resource_templates.clone();
        all.sort_by(|a, b| a.uri_template.cmp(&b.uri_template));

        let state = PageState::from_cursor(params.cursor.as_deref(), self.config.page_size);
        let (resource_templates, next_cursor) = paginate(&all, &state);

        Ok(serde_json::to_value(ListResourceTemplatesResult {
            resource_templates,
            next_cursor,
        })?)
    }

    fn handle_read_resource(&self, request: &JsonRpcRequest) -> Result<Value> {
        let params: ReadResourceParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())?,
            None => return Err(McpError::InvalidParams("Missing params".into())),
        };

        let resource = self
            .resources
            .get(&params.uri)
            .ok_or_else(|| McpError::ResourceNotFound(params.uri.clone()))?;

        let contents = resource.content();
        Ok(serde_json::to_value(ReadResourceResult { contents })?)
    }

    fn handle_list_prompts(&self, request: &JsonRpcRequest) -> Result<Value> {
        let params: ListPromptsParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())?,
            None => ListPromptsParams::default(),
        };

        // Collect all prompts (sorted for consistent pagination)
        let mut all_prompts: Vec<Prompt> = self
            .prompts
            .values()
            .map(|p| p.as_protocol_prompt())
            .collect();
        all_prompts.sort_by(|a, b| a.name.cmp(&b.name));

        // Apply pagination
        let state = PageState::from_cursor(params.cursor.as_deref(), self.config.page_size);
        let (prompts, next_cursor) = paginate(&all_prompts, &state);

        Ok(serde_json::to_value(ListPromptsResult {
            prompts,
            next_cursor,
        })?)
    }

    fn handle_get_prompt(&self, request: &JsonRpcRequest) -> Result<Value> {
        let params: GetPromptParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())?,
            None => return Err(McpError::InvalidParams("Missing params".into())),
        };

        let prompt = self
            .prompts
            .get(&params.name)
            .ok_or_else(|| McpError::PromptNotFound(params.name.clone()))?;

        let messages = prompt.get_messages(&params.arguments)?;
        let result = GetPromptResult {
            description: prompt.description().map(String::from),
            messages,
        };

        Ok(serde_json::to_value(result)?)
    }

    fn handle_notification(&mut self, notification: JsonRpcNotification) -> Result<()> {
        match notification.method.as_str() {
            "notifications/initialized" => Ok(()),
            "notifications/cancelled" => Ok(()),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::Transport;

    // Test context
    struct TestContext {
        counter: i32,
    }

    // Test tool
    struct IncrementTool;

    impl Tool<TestContext> for IncrementTool {
        fn name(&self) -> &str {
            "increment"
        }
        fn description(&self) -> &str {
            "Increment the counter"
        }
        fn schema(&self) -> Value {
            serde_json::json!({
                "type": "object",
                "properties": {
                    "amount": { "type": "integer" }
                }
            })
        }
        fn execute(
            &self,
            args: Value,
            ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            let amount = args.get("amount").and_then(|a| a.as_i64()).unwrap_or(1) as i32;
            ctx.counter += amount;
            Ok(CallToolResult::text(format!(
                "Counter is now: {}",
                ctx.counter
            )))
        }
    }

    // Tool that uses env to send notifications
    struct NotifyTool;

    impl Tool<TestContext> for NotifyTool {
        fn name(&self) -> &str {
            "notify"
        }
        fn description(&self) -> &str {
            "Send a notification"
        }
        fn schema(&self) -> Value {
            serde_json::json!({"type": "object"})
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            env: &ToolEnv,
        ) -> Result<CallToolResult> {
            env.log(LogLevel::Info, "test notification")?;
            env.send_progress("token", 0.5, Some(1.0))?;
            Ok(CallToolResult::text("done"))
        }
    }

    // Tool that fails
    struct FailingTool;

    impl Tool<TestContext> for FailingTool {
        fn name(&self) -> &str {
            "fail"
        }
        fn description(&self) -> &str {
            "Always fails"
        }
        fn schema(&self) -> Value {
            serde_json::json!({"type": "object"})
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            Err(McpError::ToolError("intentional failure".into()))
        }
    }

    // Test resource
    struct TestResource {
        uri: String,
        data: String,
    }

    impl Resource for TestResource {
        fn uri(&self) -> String {
            self.uri.clone()
        }
        fn name(&self) -> String {
            "test-resource".into()
        }
        fn description(&self) -> String {
            "A test resource".into()
        }
        fn mime_type(&self) -> String {
            "text/plain".into()
        }
        fn content(&self) -> Vec<ResourceContent> {
            vec![ResourceContent::Text {
                uri: self.uri.clone(),
                text: self.data.clone(),
                mime_type: Some("text/plain".into()),
            }]
        }
    }

    // Test prompt
    struct TestPrompt;

    impl PromptDef for TestPrompt {
        fn name(&self) -> &str {
            "test-prompt"
        }
        fn description(&self) -> Option<&str> {
            Some("A test prompt")
        }
        fn arguments(&self) -> Vec<PromptArgument> {
            vec![PromptArgument {
                name: "name".into(),
                description: Some("Your name".into()),
                required: true,
                ..Default::default()
            }]
        }
        fn get_messages(&self, args: &HashMap<String, String>) -> Result<Vec<PromptMessage>> {
            let name = args.get("name").cloned().unwrap_or_else(|| "world".into());
            Ok(vec![PromptMessage {
                role: Role::User,
                content: Content::text(format!("Hello, {}!", name)),
            }])
        }
    }

    // Mock transport for testing
    struct MockTransport {
        messages: Vec<JsonRpcMessage>,
        responses: Vec<JsonRpcMessage>,
        read_index: usize,
    }

    impl MockTransport {
        fn new(messages: Vec<JsonRpcMessage>) -> Self {
            Self {
                messages,
                responses: Vec::new(),
                read_index: 0,
            }
        }

        fn get_responses(&self) -> &[JsonRpcMessage] {
            &self.responses
        }
    }

    impl Transport for MockTransport {
        fn read(&mut self) -> Result<JsonRpcMessage> {
            if self.read_index >= self.messages.len() {
                return Err(McpError::TransportClosed);
            }
            let msg = self.messages[self.read_index].clone();
            self.read_index += 1;
            Ok(msg)
        }

        fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
            self.responses.push(message.clone());
            Ok(())
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    // Transport that records writes into a handle the test still owns, so
    // messages can be inspected after the transport is erased to `dyn Transport`.
    #[derive(Clone, Default)]
    struct RecordingTransport {
        written: Arc<Mutex<Vec<JsonRpcMessage>>>,
    }

    impl RecordingTransport {
        fn written(&self) -> Vec<JsonRpcMessage> {
            self.written.lock().unwrap().clone()
        }
    }

    impl Transport for RecordingTransport {
        fn read(&mut self) -> Result<JsonRpcMessage> {
            Err(McpError::TransportClosed)
        }

        fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
            self.written.lock().unwrap().push(message.clone());
            Ok(())
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    // Transport whose writes always fail, to prove logging swallows errors.
    struct FailingTransport;

    impl Transport for FailingTransport {
        fn read(&mut self) -> Result<JsonRpcMessage> {
            Err(McpError::TransportClosed)
        }

        fn write(&mut self, _message: &JsonRpcMessage) -> Result<()> {
            Err(McpError::Internal("write failed".into()))
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    // Helper to create a request message
    fn make_request(id: i64, method: &str, params: Option<Value>) -> JsonRpcMessage {
        JsonRpcMessage::Request(JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(id),
            method: method.to_string(),
            params,
        })
    }

    // Helper to create a notification message
    fn make_notification(method: &str, params: Option<Value>) -> JsonRpcMessage {
        JsonRpcMessage::Notification(JsonRpcNotification {
            jsonrpc: Default::default(),
            method: method.to_string(),
            params,
        })
    }

    #[test]
    fn test_add_tool() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(IncrementTool).unwrap();
        assert!(server.tools.contains_key("increment"));
    }

    #[test]
    fn test_duplicate_tool_error() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(IncrementTool).unwrap();
        let result = server.add_tool(IncrementTool);
        assert!(result.is_err());
    }

    #[test]
    fn test_add_resource() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let resource = TestResource {
            uri: "test://data".into(),
            data: "hello".into(),
        };
        server.add_resource(resource).unwrap();
        assert!(server.resources.contains_key("test://data"));
    }

    #[test]
    fn test_duplicate_resource_error() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let r1 = TestResource {
            uri: "test://data".into(),
            data: "hello".into(),
        };
        let r2 = TestResource {
            uri: "test://data".into(),
            data: "world".into(),
        };
        server.add_resource(r1).unwrap();
        let result = server.add_resource(r2);
        assert!(result.is_err());
    }

    #[test]
    fn test_add_prompt() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_prompt(TestPrompt).unwrap();
        assert!(server.prompts.contains_key("test-prompt"));
    }

    #[test]
    fn test_duplicate_prompt_error() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_prompt(TestPrompt).unwrap();
        let result = server.add_prompt(TestPrompt);
        assert!(result.is_err());
    }

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.name, "sml_mcps");
        assert!(config.instructions.is_none());
    }

    #[test]
    fn test_log_level_as_str() {
        assert_eq!(LogLevel::Debug.as_str(), "debug");
        assert_eq!(LogLevel::Info.as_str(), "info");
        assert_eq!(LogLevel::Notice.as_str(), "notice");
        assert_eq!(LogLevel::Warning.as_str(), "warning");
        assert_eq!(LogLevel::Error.as_str(), "error");
        assert_eq!(LogLevel::Critical.as_str(), "critical");
        assert_eq!(LogLevel::Alert.as_str(), "alert");
        assert_eq!(LogLevel::Emergency.as_str(), "emergency");
    }

    #[test]
    fn test_log_level_covers_all_rfc5424_severities() {
        // MCP's logging utility is exactly the eight syslog severities.
        assert_eq!(LogLevel::ALL.len(), 8);
        let names: Vec<_> = LogLevel::ALL.iter().map(|l| l.as_str()).collect();
        assert_eq!(
            names,
            [
                "debug",
                "info",
                "notice",
                "warning",
                "error",
                "critical",
                "alert",
                "emergency"
            ]
        );
    }

    #[test]
    fn test_log_level_ordering_is_by_severity() {
        // The threshold check is `record >= minimum`, so ordering must be
        // ascending by severity across the whole set.
        for pair in LogLevel::ALL.windows(2) {
            assert!(pair[0] < pair[1], "{:?} should be < {:?}", pair[0], pair[1]);
        }
        assert!(LogLevel::Error > LogLevel::Debug);
        assert!(LogLevel::Emergency > LogLevel::Warning);
    }

    #[test]
    fn test_log_level_round_trips_through_wire_names() {
        for level in LogLevel::ALL {
            assert_eq!(LogLevel::from_wire(level.as_str()), Some(level));
            assert_eq!(level.to_string(), level.as_str());
        }
    }

    #[test]
    fn test_log_level_from_wire_is_case_insensitive() {
        assert_eq!(LogLevel::from_wire("ERROR"), Some(LogLevel::Error));
        assert_eq!(LogLevel::from_wire("Warning"), Some(LogLevel::Warning));
        assert_eq!(LogLevel::from_wire("bogus"), None);
        assert_eq!(LogLevel::from_wire(""), None);
        assert!("nonsense".parse::<LogLevel>().is_err());
        assert_eq!("debug".parse::<LogLevel>().unwrap(), LogLevel::Debug);
    }

    #[test]
    fn test_logging_capability_is_declared() {
        // A server that emits notifications/message MUST declare `logging`.
        // We emit from ToolEnv::log, so we always declare it.
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let req = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "initialize".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&req, &mut ctx).unwrap();
        assert!(!result["capabilities"]["logging"].is_null());
    }

    #[test]
    fn test_set_level_updates_threshold() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        assert_eq!(server.log_level, LogLevel::Info);

        let req = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "logging/setLevel".to_string(),
            params: Some(serde_json::json!({ "level": "error" })),
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&req, &mut ctx).unwrap();

        assert_eq!(result, serde_json::json!({}));
        assert_eq!(server.log_level, LogLevel::Error);
    }

    #[test]
    fn test_set_level_rejects_unknown_level() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let req = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "logging/setLevel".to_string(),
            params: Some(serde_json::json!({ "level": "verbose" })),
        };
        let mut ctx = TestContext { counter: 0 };
        let err = server.dispatch_request(&req, &mut ctx).unwrap_err();

        // Invalid log level: -32602 (Invalid params).
        assert_eq!(err.to_jsonrpc_error().code, -32602);
        // The threshold must be left alone on a rejected request.
        assert_eq!(server.log_level, LogLevel::Info);
    }

    #[test]
    fn test_set_level_rejects_missing_params() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let req = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "logging/setLevel".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        assert!(matches!(
            server.dispatch_request(&req, &mut ctx),
            Err(McpError::InvalidParams(_))
        ));
    }

    /// Build a standalone ToolEnv for direct logging tests.
    fn log_env<'a>(
        transport: Option<&'a Arc<Mutex<dyn Transport>>>,
        resources: &'a HashMap<String, Box<dyn Resource>>,
        log_level: LogLevel,
        stderr_logging: StderrLogging,
    ) -> ToolEnv<'a> {
        ToolEnv {
            transport,
            resources,
            log_level,
            stderr_logging,
            logger: "test-logger",
        }
    }

    #[test]
    fn test_log_respects_threshold() {
        let resources: HashMap<String, Box<dyn Resource>> = HashMap::new();
        let recorder = RecordingTransport::default();
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(recorder.clone()));

        let env = log_env(
            Some(&transport),
            &resources,
            LogLevel::Warning,
            StderrLogging::Never,
        );

        // Below threshold: dropped.
        env.log(LogLevel::Debug, "quiet").unwrap();
        env.log(LogLevel::Info, "also quiet").unwrap();
        // At or above threshold: delivered.
        env.log(LogLevel::Warning, "loud").unwrap();
        env.log(LogLevel::Error, "louder").unwrap();

        let sent = recorder.written();
        assert_eq!(sent.len(), 2);
        for msg in &sent {
            let JsonRpcMessage::Notification(n) = msg else {
                panic!("expected notification");
            };
            assert_eq!(n.method, "notifications/message");
        }
    }

    #[test]
    fn test_log_emits_spec_shaped_notification() {
        let resources: HashMap<String, Box<dyn Resource>> = HashMap::new();
        let recorder = RecordingTransport::default();
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(recorder.clone()));
        let env = log_env(
            Some(&transport),
            &resources,
            LogLevel::Debug,
            StderrLogging::Never,
        );

        env.log(LogLevel::Error, "boom").unwrap();

        let sent = recorder.written();
        let JsonRpcMessage::Notification(n) = &sent[0] else {
            panic!("expected notification");
        };
        let params = n.params.as_ref().unwrap();
        assert_eq!(params["level"], "error");
        assert_eq!(params["logger"], "test-logger");
        assert_eq!(params["data"], "boom");
    }

    #[test]
    fn test_log_data_carries_structured_payload_and_logger() {
        let resources: HashMap<String, Box<dyn Resource>> = HashMap::new();
        let recorder = RecordingTransport::default();
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(recorder.clone()));
        let env = log_env(
            Some(&transport),
            &resources,
            LogLevel::Debug,
            StderrLogging::Never,
        );

        env.log_data(
            LogLevel::Critical,
            Some("database"),
            serde_json::json!({ "error": "Connection failed", "port": 5432 }),
        );

        let sent = recorder.written();
        let JsonRpcMessage::Notification(n) = &sent[0] else {
            panic!("expected notification");
        };
        let params = n.params.as_ref().unwrap();
        assert_eq!(params["level"], "critical");
        assert_eq!(params["logger"], "database");
        assert_eq!(params["data"]["error"], "Connection failed");
        assert_eq!(params["data"]["port"], 5432);
    }

    #[test]
    fn test_log_never_fails_the_tool() {
        // The whole point of the stderr fallback: logging must not be able to
        // turn a working tool into a failing one.
        let resources: HashMap<String, Box<dyn Resource>> = HashMap::new();

        // No transport at all.
        let env = log_env(None, &resources, LogLevel::Debug, StderrLogging::Fallback);
        assert!(env.log(LogLevel::Error, "no transport").is_ok());

        // Transport that errors on every write.
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(FailingTransport));
        let env = log_env(
            Some(&transport),
            &resources,
            LogLevel::Debug,
            StderrLogging::Fallback,
        );
        assert!(env.log(LogLevel::Error, "write fails").is_ok());
    }

    #[test]
    fn test_log_enabled_matches_delivery() {
        let resources: HashMap<String, Box<dyn Resource>> = HashMap::new();
        let recorder = RecordingTransport::default();
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(recorder.clone()));
        let env = log_env(
            Some(&transport),
            &resources,
            LogLevel::Warning,
            StderrLogging::Never,
        );

        assert_eq!(env.log_level(), LogLevel::Warning);
        assert!(!env.log_enabled(LogLevel::Debug));
        assert!(!env.log_enabled(LogLevel::Info));
        assert!(!env.log_enabled(LogLevel::Notice));
        assert!(env.log_enabled(LogLevel::Warning));
        assert!(env.log_enabled(LogLevel::Emergency));

        for level in LogLevel::ALL {
            env.log(level, "x").unwrap();
        }
        let sent = recorder.written().len();
        let expected = LogLevel::ALL
            .iter()
            .filter(|l| env.log_enabled(**l))
            .count();
        assert_eq!(sent, expected);
    }

    #[test]
    fn test_stderr_logging_never_still_delivers_to_client() {
        // StderrLogging::Never suppresses only the *fallback*, not delivery.
        let resources: HashMap<String, Box<dyn Resource>> = HashMap::new();
        let recorder = RecordingTransport::default();
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(recorder.clone()));
        let env = log_env(
            Some(&transport),
            &resources,
            LogLevel::Debug,
            StderrLogging::Never,
        );

        env.log(LogLevel::Info, "delivered").unwrap();
        assert_eq!(recorder.written().len(), 1);
    }

    #[test]
    fn test_server_default_log_level_is_configurable() {
        let server: Server<TestContext> = Server::new(ServerConfig {
            default_log_level: LogLevel::Error,
            ..Default::default()
        });
        assert_eq!(server.log_level, LogLevel::Error);
    }

    #[test]
    fn test_stderr_logging_default_is_fallback() {
        assert_eq!(StderrLogging::default(), StderrLogging::Fallback);
        assert_eq!(
            ServerConfig::default().stderr_logging,
            StderrLogging::Fallback
        );
        assert_eq!(ServerConfig::default().default_log_level, LogLevel::Info);
    }

    #[test]
    fn test_tool_call_without_transport_does_not_panic() {
        // ToolEnv used to unwrap the transport; dispatching a tool call
        // directly (no start/process_one) panicked.
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(NotifyTool).unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/call".to_string(),
            params: Some(serde_json::json!({ "name": "notify" })),
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();
        assert_eq!(result["content"][0]["text"], "done");
    }

    #[test]
    fn test_handle_initialize() {
        let mut server: Server<TestContext> = Server::new(ServerConfig {
            name: "test-server".into(),
            version: "1.0.0".into(),
            instructions: Some("test instructions".into()),
            ..Default::default()
        });
        server.add_tool(IncrementTool).unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "initialize".to_string(),
            params: Some(serde_json::json!({
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "1.0" }
            })),
        };

        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx);
        assert!(result.is_ok());

        let value = result.unwrap();
        assert_eq!(value["serverInfo"]["name"], "test-server");
        assert!(server.initialized);
    }

    #[test]
    fn test_handle_initialize_no_params() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "initialize".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_ping() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "ping".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_tools_list() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(IncrementTool).unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/list".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();

        let tools = result["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "increment");
    }

    #[test]
    fn test_handle_tools_call() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(IncrementTool).unwrap();

        // Need to set up transport for ToolEnv
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(MockTransport::new(vec![])));
        server.transport = Some(transport);

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/call".to_string(),
            params: Some(serde_json::json!({
                "name": "increment",
                "arguments": { "amount": 5 }
            })),
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();

        assert_eq!(ctx.counter, 5);
        assert!(result["content"][0]["text"].as_str().unwrap().contains("5"));
    }

    #[test]
    fn test_handle_tools_call_no_params() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(IncrementTool).unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/call".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_tools_call_unknown_tool() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(MockTransport::new(vec![])));
        server.transport = Some(transport);

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/call".to_string(),
            params: Some(serde_json::json!({ "name": "nonexistent" })),
        };
        let mut ctx = TestContext { counter: 0 };
        // An unknown tool is a protocol error (Invalid params), not a tool
        // execution error: no argument change can fix it.
        let result = server.dispatch_request(&request, &mut ctx);
        assert!(matches!(result, Err(McpError::InvalidParams(_))));
        assert_eq!(result.unwrap_err().to_jsonrpc_error().code, -32602);
    }

    #[test]
    fn test_handle_tools_call_tool_failure() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(FailingTool).unwrap();
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(MockTransport::new(vec![])));
        server.transport = Some(transport);

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/call".to_string(),
            params: Some(serde_json::json!({ "name": "fail" })),
        };
        let mut ctx = TestContext { counter: 0 };
        // A tool that returns McpError::ToolError produced a JSON-RPC error
        // before; the spec wants it as an `isError` result so the model can
        // read the message and self-correct.
        let result = server.dispatch_request(&request, &mut ctx).unwrap();
        assert_eq!(result["isError"], true);
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("intentional failure")
        );
    }

    #[test]
    fn test_handle_resources_list() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server
            .add_resource(TestResource {
                uri: "test://data".into(),
                data: "hello".into(),
            })
            .unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "resources/list".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();

        let resources = result["resources"].as_array().unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0]["uri"], "test://data");
    }

    #[test]
    fn test_handle_resources_read() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server
            .add_resource(TestResource {
                uri: "test://data".into(),
                data: "hello world".into(),
            })
            .unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "resources/read".to_string(),
            params: Some(serde_json::json!({ "uri": "test://data" })),
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();

        let contents = result["contents"].as_array().unwrap();
        assert_eq!(contents[0]["text"], "hello world");
    }

    #[test]
    fn test_handle_resources_read_not_found() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "resources/read".to_string(),
            params: Some(serde_json::json!({ "uri": "nonexistent://uri" })),
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx);
        assert!(matches!(result, Err(McpError::ResourceNotFound(_))));
    }

    #[test]
    fn test_handle_resources_read_no_params() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "resources/read".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx);
        assert!(matches!(result, Err(McpError::InvalidParams(_))));
    }

    #[test]
    fn test_handle_prompts_list() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_prompt(TestPrompt).unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "prompts/list".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();

        let prompts = result["prompts"].as_array().unwrap();
        assert_eq!(prompts.len(), 1);
        assert_eq!(prompts[0]["name"], "test-prompt");
    }

    #[test]
    fn test_handle_prompts_get() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_prompt(TestPrompt).unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "prompts/get".to_string(),
            params: Some(serde_json::json!({
                "name": "test-prompt",
                "arguments": { "name": "Claude" }
            })),
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();

        let messages = result["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]["content"]["text"]
                .as_str()
                .unwrap()
                .contains("Claude")
        );
    }

    #[test]
    fn test_handle_prompts_get_not_found() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "prompts/get".to_string(),
            params: Some(serde_json::json!({ "name": "nonexistent", "arguments": {} })),
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx);
        assert!(matches!(result, Err(McpError::PromptNotFound(_))));
    }

    #[test]
    fn test_handle_prompts_get_no_params() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "prompts/get".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx);
        assert!(matches!(result, Err(McpError::InvalidParams(_))));
    }

    #[test]
    fn test_handle_unknown_method() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "unknown/method".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx);
        assert!(matches!(result, Err(McpError::MethodNotFound(_))));
    }

    #[test]
    fn test_handle_notification() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());

        let init_notification = JsonRpcNotification {
            jsonrpc: Default::default(),
            method: "notifications/initialized".to_string(),
            params: None,
        };
        let result = server.handle_notification(init_notification);
        assert!(result.is_ok());

        let cancel_notification = JsonRpcNotification {
            jsonrpc: Default::default(),
            method: "notifications/cancelled".to_string(),
            params: Some(serde_json::json!({ "requestId": 1 })),
        };
        let result = server.handle_notification(cancel_notification);
        assert!(result.is_ok());

        let unknown_notification = JsonRpcNotification {
            jsonrpc: Default::default(),
            method: "unknown/notification".to_string(),
            params: None,
        };
        let result = server.handle_notification(unknown_notification);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_message_request() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(MockTransport::new(vec![])));
        server.transport = Some(transport);

        let message = make_request(1, "ping", None);
        let mut ctx = TestContext { counter: 0 };
        let result = server.handle_message(message, &mut ctx).unwrap();

        assert!(result.is_some());
        if let Some(JsonRpcMessage::Response(resp)) = result {
            assert!(resp.result.is_some());
        } else {
            panic!("Expected response");
        }
    }

    #[test]
    fn test_handle_message_notification() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let message = make_notification("notifications/initialized", None);
        let mut ctx = TestContext { counter: 0 };
        let result = server.handle_message(message, &mut ctx).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_message_response() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let message = JsonRpcMessage::Response(JsonRpcResponse {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            result: Some(serde_json::json!({})),
            error: None,
        });
        let mut ctx = TestContext { counter: 0 };
        let result = server.handle_message(message, &mut ctx).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_request_error_response() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(99),
            method: "unknown".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let response = server.handle_request(request, &mut ctx);

        if let JsonRpcMessage::Response(resp) = response {
            assert!(resp.error.is_some());
            assert_eq!(resp.id, RequestId::Number(99));
        } else {
            panic!("Expected response");
        }
    }

    #[test]
    fn test_process_one() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(IncrementTool).unwrap();

        let messages = vec![make_request(
            1,
            "tools/call",
            Some(serde_json::json!({
                "name": "increment",
                "arguments": { "amount": 10 }
            })),
        )];

        let transport = Arc::new(Mutex::new(MockTransport::new(messages)));
        let mut ctx = TestContext { counter: 0 };

        server.process_one(transport.clone(), &mut ctx).unwrap();

        assert_eq!(ctx.counter, 10);

        let t = transport.lock().unwrap();
        let responses = t.get_responses();
        assert_eq!(responses.len(), 1);
    }

    #[test]
    fn test_start_exits_on_transport_closed() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());

        // Empty transport will return TransportClosed immediately
        let transport = MockTransport::new(vec![]);
        let ctx = TestContext { counter: 0 };

        let result = server.start(transport, ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn test_start_processes_messages() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(IncrementTool).unwrap();

        let messages = vec![
            make_request(
                1,
                "initialize",
                Some(serde_json::json!({
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": { "name": "test", "version": "1.0" }
                })),
            ),
            make_request(
                2,
                "tools/call",
                Some(serde_json::json!({
                    "name": "increment",
                    "arguments": { "amount": 7 }
                })),
            ),
        ];

        let transport = MockTransport::new(messages);
        let ctx = TestContext { counter: 0 };

        // start will process messages until transport closes
        let result = server.start(transport, ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn test_tool_env_list_resources() {
        let mut resources: HashMap<String, Box<dyn Resource>> = HashMap::new();
        resources.insert(
            "test://a".into(),
            Box::new(TestResource {
                uri: "test://a".into(),
                data: "a".into(),
            }),
        );
        resources.insert(
            "test://b".into(),
            Box::new(TestResource {
                uri: "test://b".into(),
                data: "b".into(),
            }),
        );

        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(MockTransport::new(vec![])));
        let env = log_env(
            Some(&transport),
            &resources,
            LogLevel::Info,
            StderrLogging::Never,
        );

        let uris = env.list_resources();
        assert_eq!(uris.len(), 2);
        assert!(uris.contains(&"test://a".to_string()));
        assert!(uris.contains(&"test://b".to_string()));
    }

    #[test]
    fn test_tool_env_get_resource() {
        let mut resources: HashMap<String, Box<dyn Resource>> = HashMap::new();
        resources.insert(
            "test://data".into(),
            Box::new(TestResource {
                uri: "test://data".into(),
                data: "hello".into(),
            }),
        );

        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(MockTransport::new(vec![])));
        let env = log_env(
            Some(&transport),
            &resources,
            LogLevel::Info,
            StderrLogging::Never,
        );

        let found = env.get_resource("test://data");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), "test-resource");

        let not_found = env.get_resource("nonexistent");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_tool_with_notifications() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(NotifyTool).unwrap();

        let messages = vec![make_request(
            1,
            "tools/call",
            Some(serde_json::json!({ "name": "notify" })),
        )];

        let transport = Arc::new(Mutex::new(MockTransport::new(messages)));
        let mut ctx = TestContext { counter: 0 };

        server.process_one(transport.clone(), &mut ctx).unwrap();

        let t = transport.lock().unwrap();
        let responses = t.get_responses();
        // Should have: notification, progress, response
        assert!(responses.len() >= 2);
    }

    #[test]
    fn test_resource_as_protocol_resource() {
        let resource = TestResource {
            uri: "test://uri".into(),
            data: "data".into(),
        };
        let proto = resource.as_protocol_resource();

        assert_eq!(proto.uri, "test://uri");
        assert_eq!(proto.name, "test-resource");
        assert_eq!(proto.description, Some("A test resource".into()));
        assert_eq!(proto.mime_type, Some("text/plain".into()));
    }

    #[test]
    fn test_prompt_as_protocol_prompt() {
        let prompt = TestPrompt;
        let proto = prompt.as_protocol_prompt();

        assert_eq!(proto.name, "test-prompt");
        assert_eq!(proto.description, Some("A test prompt".into()));
        assert_eq!(proto.arguments.len(), 1);
        assert_eq!(proto.arguments[0].name, "name");
    }

    #[test]
    fn test_capabilities_reflect_registered_items() {
        // Server with nothing
        let mut empty_server: Server<TestContext> = Server::new(ServerConfig::default());
        let req = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "initialize".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = empty_server.dispatch_request(&req, &mut ctx).unwrap();
        assert!(result["capabilities"]["tools"].is_null());
        assert!(result["capabilities"]["resources"].is_null());
        assert!(result["capabilities"]["prompts"].is_null());

        // Server with everything
        let mut full_server: Server<TestContext> = Server::new(ServerConfig::default());
        full_server.add_tool(IncrementTool).unwrap();
        full_server
            .add_resource(TestResource {
                uri: "test://x".into(),
                data: "x".into(),
            })
            .unwrap();
        full_server.add_prompt(TestPrompt).unwrap();

        let result = full_server.dispatch_request(&req, &mut ctx).unwrap();
        assert!(!result["capabilities"]["tools"].is_null());
        assert!(!result["capabilities"]["resources"].is_null());
        assert!(!result["capabilities"]["prompts"].is_null());
    }

    // Helper tool for pagination tests - creates many tools
    struct NumberedTool(u32);

    impl Tool<TestContext> for NumberedTool {
        fn name(&self) -> &str {
            // This is a bit hacky but works for tests
            Box::leak(format!("tool_{:03}", self.0).into_boxed_str())
        }
        fn description(&self) -> &str {
            "A numbered tool"
        }
        fn schema(&self) -> Value {
            serde_json::json!({"type": "object"})
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            Ok(CallToolResult::text(format!("Tool {}", self.0)))
        }
    }

    #[test]
    fn test_pagination_tools_list() {
        // Create server with small page size
        let mut server: Server<TestContext> = Server::new(ServerConfig {
            page_size: 3,
            ..Default::default()
        });

        // Add 10 tools
        for i in 0..10 {
            server.add_tool(NumberedTool(i)).unwrap();
        }

        let mut ctx = TestContext { counter: 0 };

        // First page - no cursor
        let req = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/list".to_string(),
            params: None,
        };
        let result = server.dispatch_request(&req, &mut ctx).unwrap();
        let tools = result["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 3);
        assert_eq!(tools[0]["name"], "tool_000");
        assert_eq!(tools[2]["name"], "tool_002");
        let next_cursor = result["nextCursor"].as_str().unwrap();

        // Second page - with cursor
        let req2 = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(2),
            method: "tools/list".to_string(),
            params: Some(serde_json::json!({ "cursor": next_cursor })),
        };
        let result2 = server.dispatch_request(&req2, &mut ctx).unwrap();
        let tools2 = result2["tools"].as_array().unwrap();
        assert_eq!(tools2.len(), 3);
        assert_eq!(tools2[0]["name"], "tool_003");
        assert_eq!(tools2[2]["name"], "tool_005");
        let next_cursor2 = result2["nextCursor"].as_str().unwrap();

        // Third page
        let req3 = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(3),
            method: "tools/list".to_string(),
            params: Some(serde_json::json!({ "cursor": next_cursor2 })),
        };
        let result3 = server.dispatch_request(&req3, &mut ctx).unwrap();
        let tools3 = result3["tools"].as_array().unwrap();
        assert_eq!(tools3.len(), 3);
        assert_eq!(tools3[0]["name"], "tool_006");
        let next_cursor3 = result3["nextCursor"].as_str().unwrap();

        // Fourth (last) page - only 1 item left
        let req4 = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(4),
            method: "tools/list".to_string(),
            params: Some(serde_json::json!({ "cursor": next_cursor3 })),
        };
        let result4 = server.dispatch_request(&req4, &mut ctx).unwrap();
        let tools4 = result4["tools"].as_array().unwrap();
        assert_eq!(tools4.len(), 1);
        assert_eq!(tools4[0]["name"], "tool_009");
        assert!(result4["nextCursor"].is_null()); // No more pages
    }

    //
    // Structured output (2025-06-18)
    //

    /// Tool that declares an output schema and honors it.
    struct StructuredTool;

    impl Tool<TestContext> for StructuredTool {
        fn name(&self) -> &str {
            "structured"
        }
        fn description(&self) -> &str {
            "Returns structured data"
        }
        fn title(&self) -> Option<String> {
            Some("Structured Thing".into())
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object" })
        }
        fn output_schema(&self) -> Option<Value> {
            Some(serde_json::json!({
                "type": "object",
                "properties": { "count": { "type": "integer" } },
                "required": ["count"]
            }))
        }
        fn icons(&self) -> Vec<Icon> {
            vec![Icon::new("https://example.com/i.png")]
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            Ok(CallToolResult::structured(
                serde_json::json!({ "count": 3 }),
            )?)
        }
    }

    /// Tool that declares an output schema and then breaks the promise.
    struct BrokenSchemaTool;

    impl Tool<TestContext> for BrokenSchemaTool {
        fn name(&self) -> &str {
            "broken"
        }
        fn description(&self) -> &str {
            "Declares a schema it does not honor"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object" })
        }
        fn output_schema(&self) -> Option<Value> {
            Some(serde_json::json!({
                "type": "object",
                "properties": { "count": { "type": "integer" } },
                "required": ["count"]
            }))
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            // No structuredContent at all.
            Ok(CallToolResult::text("just text"))
        }
    }

    /// Tool that reports an execution failure the model could act on.
    struct BadInputTool;

    impl Tool<TestContext> for BadInputTool {
        fn name(&self) -> &str {
            "bad_input"
        }
        fn description(&self) -> &str {
            "Rejects its arguments"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object" })
        }
        fn output_schema(&self) -> Option<Value> {
            Some(serde_json::json!({ "type": "object", "required": ["x"] }))
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            Err(McpError::InvalidParams("date must be in the future".into()))
        }
    }

    fn call(server: &mut Server<TestContext>, name: &str) -> Result<Value> {
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/call".to_string(),
            params: Some(serde_json::json!({ "name": name })),
        };
        let mut ctx = TestContext { counter: 0 };
        server.dispatch_request(&request, &mut ctx)
    }

    #[test]
    fn test_structured_tool_result_carries_structured_content() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(StructuredTool).unwrap();

        let result = call(&mut server, "structured").unwrap();
        assert_eq!(result["structuredContent"]["count"], 3);
        // The spec asks for a text mirror for pre-2025-06-18 clients.
        assert!(result["content"][0]["text"].as_str().unwrap().contains("3"));
    }

    #[test]
    fn test_output_schema_violation_is_an_internal_error() {
        // Declaring outputSchema is a MUST-level promise. Breaking it is a
        // server bug, so it must not be silently shipped to the client.
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(BrokenSchemaTool).unwrap();

        let err = call(&mut server, "broken").unwrap_err();
        assert!(matches!(err, McpError::Internal(_)));
        assert!(err.to_string().contains("outputSchema"), "{err}");
    }

    #[test]
    fn test_output_schema_not_enforced_on_error_results() {
        // An isError result has nothing structured to validate; enforcing the
        // schema there would turn a reportable failure into an internal error.
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(BadInputTool).unwrap();

        let result = call(&mut server, "bad_input").unwrap();
        assert_eq!(result["isError"], true);
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("date must be in the future")
        );
    }

    #[test]
    fn test_invalid_params_from_tool_becomes_execution_error() {
        // SEP-1303: input validation errors are Tool Execution Errors, so the
        // model sees the message and can retry with corrected arguments.
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(BadInputTool).unwrap();

        let result = call(&mut server, "bad_input").unwrap();
        assert_eq!(result["isError"], true);
    }

    #[test]
    fn test_tools_list_exposes_title_output_schema_and_icons() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(StructuredTool).unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/list".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();
        let tool = &result["tools"][0];

        assert_eq!(tool["name"], "structured");
        assert_eq!(tool["title"], "Structured Thing");
        assert_eq!(tool["outputSchema"]["required"][0], "count");
        assert_eq!(tool["icons"][0]["src"], "https://example.com/i.png");
        // taskSupport defaults to forbidden and is omitted.
        assert!(tool.get("execution").is_none());
    }

    #[test]
    fn test_tools_list_omits_optional_fields_for_plain_tools() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(IncrementTool).unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/list".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();
        let tool = &result["tools"][0];

        for absent in ["title", "outputSchema", "icons", "execution", "_meta"] {
            assert!(tool.get(absent).is_none(), "{absent} should be omitted");
        }
    }

    //
    // Server identity (2025-06-18 / 2025-11-25)
    //

    #[test]
    fn test_initialize_reports_configured_server_identity() {
        let mut server: Server<TestContext> = Server::new(ServerConfig {
            name: "srv".into(),
            version: "2.0.0".into(),
            title: Some("My Server".into()),
            description: Some("Does things".into()),
            website_url: Some("https://example.com".into()),
            icons: vec![Icon::new("https://example.com/s.png")],
            ..Default::default()
        });

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "initialize".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();
        let info = &result["serverInfo"];

        assert_eq!(info["name"], "srv");
        assert_eq!(info["title"], "My Server");
        assert_eq!(info["description"], "Does things");
        assert_eq!(info["websiteUrl"], "https://example.com");
        assert_eq!(info["icons"][0]["src"], "https://example.com/s.png");
    }

    //
    // Resource templates
    //

    fn template(uri: &str, name: &str) -> ResourceTemplate {
        ResourceTemplate {
            uri_template: uri.into(),
            name: name.into(),
            ..Default::default()
        }
    }

    #[test]
    fn test_resource_templates_list() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server
            .add_resource_template(template("file:///{path}", "Project Files"))
            .unwrap();
        server
            .add_resource_template(template("db:///{table}", "Tables"))
            .unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "resources/templates/list".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();

        let templates = result["resourceTemplates"].as_array().unwrap();
        assert_eq!(templates.len(), 2);
        // Sorted by uriTemplate for stable pagination.
        assert_eq!(templates[0]["uriTemplate"], "db:///{table}");
        assert_eq!(templates[1]["uriTemplate"], "file:///{path}");
    }

    #[test]
    fn test_resource_templates_list_empty_by_default() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "resources/templates/list".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();
        assert_eq!(result["resourceTemplates"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_duplicate_resource_template_error() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server
            .add_resource_template(template("file:///{path}", "A"))
            .unwrap();
        assert!(
            server
                .add_resource_template(template("file:///{path}", "B"))
                .is_err()
        );
    }

    #[test]
    fn test_templates_alone_declare_the_resources_capability() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server
            .add_resource_template(template("file:///{path}", "A"))
            .unwrap();

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "initialize".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();
        assert!(!result["capabilities"]["resources"].is_null());
    }

    #[test]
    fn test_resource_templates_paginate() {
        let mut server: Server<TestContext> = Server::new(ServerConfig {
            page_size: 2,
            ..Default::default()
        });
        for i in 0..5 {
            server
                .add_resource_template(template(&format!("x:///{:02}", i), "t"))
                .unwrap();
        }

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "resources/templates/list".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();
        assert_eq!(result["resourceTemplates"].as_array().unwrap().len(), 2);
        assert!(result["nextCursor"].is_string());
    }

    //
    // JSON-RPC batching removal (2025-06-18)
    //

    #[test]
    fn test_batch_messages_are_rejected() {
        // "Remove support for JSON-RPC batching" - a top-level array is not a
        // valid message and must be reported as Invalid Request, not as an
        // opaque parse failure.
        let batch = r#"[{"jsonrpc":"2.0","id":1,"method":"ping"},
                        {"jsonrpc":"2.0","id":2,"method":"ping"}]"#;
        let err = JsonRpcMessage::parse(batch).unwrap_err();

        assert!(matches!(err, McpError::InvalidMessage(_)));
        assert_eq!(err.to_jsonrpc_error().code, -32600);
        assert!(err.to_string().to_lowercase().contains("batch"), "{err}");
    }

    #[test]
    fn test_batch_rejection_tolerates_leading_whitespace() {
        let err = JsonRpcMessage::parse("   \n\t[{\"jsonrpc\":\"2.0\"}]").unwrap_err();
        assert!(matches!(err, McpError::InvalidMessage(_)));
    }

    #[test]
    fn test_single_messages_still_parse() {
        let request = JsonRpcMessage::parse(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#).unwrap();
        assert!(matches!(request, JsonRpcMessage::Request(_)));

        let notification =
            JsonRpcMessage::parse(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
                .unwrap();
        assert!(matches!(notification, JsonRpcMessage::Notification(_)));

        let response = JsonRpcMessage::parse(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#).unwrap();
        assert!(matches!(response, JsonRpcMessage::Response(_)));
    }

    #[test]
    fn test_params_structs_tolerate_unknown_fields() {
        // Forward compatibility: params must never gain deny_unknown_fields,
        // or every future spec addition becomes a hard parse failure here.
        let with_extras = serde_json::json!({
            "name": "increment",
            "arguments": {},
            "_meta": { "io.modelcontextprotocol/progressToken": 1 },
            "task": { "ttl": 60000 },
            "somethingFromTheFuture": true
        });
        assert!(serde_json::from_value::<CallToolParams>(with_extras).is_ok());

        let list = serde_json::json!({ "cursor": "abc", "_meta": {}, "future": 1 });
        assert!(serde_json::from_value::<ListToolsParams>(list).is_ok());
    }

    #[test]
    fn test_pagination_invalid_cursor() {
        let mut server: Server<TestContext> = Server::new(ServerConfig {
            page_size: 3,
            ..Default::default()
        });
        server.add_tool(IncrementTool).unwrap();

        let mut ctx = TestContext { counter: 0 };

        // Invalid cursor should default to first page
        let req = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "tools/list".to_string(),
            params: Some(serde_json::json!({ "cursor": "garbage" })),
        };
        let result = server.dispatch_request(&req, &mut ctx).unwrap();
        let tools = result["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1); // Should get the one tool
    }
}
