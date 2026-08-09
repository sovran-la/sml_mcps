//! MCP Server
//!
//! Core server implementation with generic context support.

use crate::broker::RequestBroker;
use crate::pagination::{DEFAULT_PAGE_SIZE, PageState, paginate};
use crate::tasks::{
    CreateTaskResult, ListTasksParams, ListTasksResult, TaskConfig, TaskIdParams, TaskOutcome,
    TaskParams, TaskStatus, TaskStore, TasksCapability, related_task_meta,
};
use crate::transport::Transport;
use crate::types::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
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
    resources: &'a HashMap<String, Arc<dyn Resource>>,
    /// Minimum severity the client asked for via `logging/setLevel`.
    log_level: LogLevel,
    /// What to do with log records that the client will not see.
    stderr_logging: StderrLogging,
    /// Default `logger` name on emitted log records.
    logger: &'a str,
    /// What the client said it can do, from `initialize`.
    client_capabilities: &'a ClientCapabilities,
    /// Routing for server-initiated request/response round trips.
    broker: &'a RequestBroker,
    /// Whether this environment may block waiting on the client.
    ///
    /// False inside task workers: nothing would be reading the transport while
    /// the worker blocked, so the round trip could never complete.
    can_request: bool,
    /// Set inside a task worker; flipped when the task is cancelled.
    cancelled: Option<&'a Arc<AtomicBool>>,
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

    /// Has the task running this tool been cancelled?
    ///
    /// Cancellation is cooperative: the server flips this flag and moves the
    /// task to `cancelled`, but only the tool can actually stop. Long-running
    /// tools should poll this and return early.
    ///
    /// Always `false` outside a task.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
    }

    /// Is this tool running inside a task?
    pub fn is_task(&self) -> bool {
        self.cancelled.is_some()
    }

    /// What the client declared it supports during `initialize`.
    ///
    /// Check this before doing anything that depends on a client feature; the
    /// spec forbids using a capability the peer did not declare.
    pub fn client_capabilities(&self) -> &ClientCapabilities {
        self.client_capabilities
    }

    //
    // Server-initiated requests
    //

    /// Send a JSON-RPC request to the client and block until it answers.
    ///
    /// Only usable on a bidirectional transport (stdio, Unix socket) from the
    /// thread running the server loop. Inside a task worker, or on the HTTP
    /// transport where a request carries one message and has no back-channel,
    /// this returns an error rather than hanging.
    ///
    /// There is no timeout: `Transport::read` blocks, and interrupting it
    /// would need a second thread per call. A client that never answers will
    /// stall this tool, which is also true of a client that never answers any
    /// other request.
    pub fn send_request(&self, method: &str, params: Value) -> Result<Value> {
        if !self.can_request {
            return Err(McpError::Internal(format!(
                "`{}` needs a response from the client, which is not possible here: \
                 nothing is reading the transport on this thread",
                method
            )));
        }
        let Some(transport) = self.transport else {
            return Err(McpError::Internal(format!(
                "`{}` needs a transport to reach the client",
                method
            )));
        };

        let id = self.broker.next_request_id();
        let request = JsonRpcMessage::request(id.clone(), method, Some(params));

        {
            let mut guard = transport
                .lock()
                .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
            guard.write(&request)?;
        }

        // We are now the only reader. Everything that comes off the wire is
        // ours to route: our response, someone else's response, or a client
        // message the main loop still needs to see.
        loop {
            if let Some(response) = self.broker.take_parked(&id) {
                return RequestBroker::into_result(response);
            }

            let message = {
                let mut guard = transport
                    .lock()
                    .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
                guard.read()?
            };

            match message {
                JsonRpcMessage::Response(response) if response.id == id => {
                    return RequestBroker::into_result(response);
                }
                JsonRpcMessage::Response(response) => self.broker.park(response),
                other => self.broker.defer(other),
            }
        }
    }

    //
    // Elicitation
    //

    /// Ask the user for input via the client.
    ///
    /// Refuses to send a mode the client did not declare, which the spec
    /// requires, and validates the request shape before it goes out.
    pub fn elicit(&self, params: ElicitParams) -> Result<ElicitResult> {
        let mode = params.resolved_mode();
        if !self.client_capabilities.supports_elicitation(mode) {
            return Err(McpError::Internal(format!(
                "client did not declare support for {:?} mode elicitation",
                mode
            )));
        }
        params.validate().map_err(McpError::InvalidParams)?;

        let value = self.send_request("elicitation/create", serde_json::to_value(&params)?)?;
        Ok(serde_json::from_value(value)?)
    }

    /// Ask for structured data with a form.
    ///
    /// Convenience over [`ToolEnv::elicit`] for the common case. Build the
    /// schema with [`ElicitSchema`].
    pub fn elicit_form(&self, message: impl Into<String>, schema: Value) -> Result<ElicitResult> {
        self.elicit(ElicitParams::form(message, schema))
    }

    /// Send the user out to a URL for an interaction the client must not see.
    ///
    /// This is the required mode for credentials, payments, and third-party
    /// OAuth. An `accept` response means the user consented to navigate, *not*
    /// that the interaction finished - that happens out of band.
    pub fn elicit_url(
        &self,
        message: impl Into<String>,
        url: impl Into<String>,
        elicitation_id: impl Into<String>,
    ) -> Result<ElicitResult> {
        self.elicit(ElicitParams::url(message, url, elicitation_id))
    }

    /// Tell the client an out-of-band URL elicitation finished.
    ///
    /// Fire-and-forget: clients **MUST** ignore unknown or already-completed
    /// ids, and **SHOULD** offer manual retry in case this never arrives.
    pub fn notify_elicitation_complete(&self, elicitation_id: impl Into<String>) -> Result<()> {
        self.send_notification(
            "notifications/elicitation/complete",
            Some(serde_json::to_value(ElicitationCompleteParams {
                elicitation_id: elicitation_id.into(),
            })?),
        )
    }

    //
    // Sampling
    //

    /// Ask the client's LLM for a completion.
    ///
    /// Refuses to send tools to a client that did not declare
    /// `sampling.tools`, and validates the tool-use/tool-result structure the
    /// spec requires before spending a round trip on it.
    pub fn create_message(&self, params: CreateMessageParams) -> Result<CreateMessageResult> {
        if !self.client_capabilities.supports_sampling() {
            return Err(McpError::Internal(
                "client did not declare support for sampling".into(),
            ));
        }
        if !params.tools.is_empty() && !self.client_capabilities.supports_sampling_tools() {
            return Err(McpError::Internal(
                "client did not declare `sampling.tools`, so tool-enabled sampling \
                 requests must not be sent to it"
                    .into(),
            ));
        }
        if params
            .include_context
            .as_deref()
            .is_some_and(|context| context != "none")
            && !self
                .client_capabilities
                .sampling
                .as_ref()
                .is_some_and(|s| s.supports_context())
        {
            return Err(McpError::Internal(
                "client did not declare `sampling.context`, so `includeContext` must \
                 be omitted or \"none\""
                    .into(),
            ));
        }
        params.validate().map_err(McpError::InvalidParams)?;

        let value = self.send_request("sampling/createMessage", serde_json::to_value(&params)?)?;
        Ok(serde_json::from_value(value)?)
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

/// Parse a request's params, treating an absent `params` as invalid.
fn parse_params<T: serde::de::DeserializeOwned>(request: &JsonRpcRequest) -> Result<T> {
    let params = request
        .params
        .as_ref()
        .ok_or_else(|| McpError::InvalidParams("Missing params".into()))?;
    serde_json::from_value(params.clone())
        .map_err(|e| McpError::InvalidParams(format!("Invalid params: {e}")))
}

//
// MCP Server
//

/// MCP Server - generic over context type
pub struct Server<C> {
    config: ServerConfig,
    tools: HashMap<String, Arc<dyn Tool<C>>>,
    resources: Arc<HashMap<String, Arc<dyn Resource>>>,
    resource_templates: Vec<ResourceTemplate>,
    prompts: HashMap<String, Box<dyn PromptDef>>,
    transport: Option<Arc<Mutex<dyn Transport>>>,
    initialized: bool,
    /// Minimum log severity, as last set by `logging/setLevel`.
    log_level: LogLevel,
    /// What the client declared during `initialize`.
    ///
    /// Defaults to "nothing declared", so a server that never saw an
    /// `initialize` will refuse to send elicitation or sampling requests -
    /// which is correct, since it has no evidence the client can handle them.
    client_capabilities: ClientCapabilities,
    /// Routing for server-initiated request/response round trips.
    broker: Arc<RequestBroker>,
    /// Present only once [`Server::enable_tasks`] has been called.
    tasks: Option<TaskRuntime<C>>,
}

/// Task workers cannot make server-initiated requests, so they need no client
/// capabilities - but `ToolEnv` borrows them, so they need somewhere to point.
static NO_CLIENT_CAPABILITIES: std::sync::LazyLock<ClientCapabilities> =
    std::sync::LazyLock::new(ClientCapabilities::default);

/// Everything needed to run tools on a background thread.
struct TaskRuntime<C> {
    store: Arc<TaskStore>,
    /// Builds a fresh context per task.
    ///
    /// A worker cannot borrow the `&mut C` the server loop is holding, so it
    /// makes its own. That also means a task sees no in-flight mutations from
    /// the request that spawned it - which the stateless model wants anyway.
    context_factory: Arc<dyn Fn() -> C + Send + Sync>,
}

impl<C: Send + Sync + 'static> Server<C> {
    /// Create a new server with the given configuration
    pub fn new(config: ServerConfig) -> Self {
        let log_level = config.default_log_level;
        Self {
            config,
            tools: HashMap::new(),
            resources: Arc::new(HashMap::new()),
            resource_templates: Vec::new(),
            prompts: HashMap::new(),
            transport: None,
            initialized: false,
            log_level,
            client_capabilities: ClientCapabilities::default(),
            broker: Arc::new(RequestBroker::new()),
            tasks: None,
        }
    }

    /// Enable task-augmented `tools/call`, plus `tasks/get`, `tasks/result`,
    /// `tasks/list`, and `tasks/cancel`.
    ///
    /// Opt-in, because the spec is strict about what declaring the capability
    /// commits you to: "Receivers that do not declare the task capability for a
    /// request type **MUST** process requests of that type normally, ignoring
    /// any task-augmentation metadata if present." A server that never calls
    /// this behaves exactly as before, ignoring any `task` field it is sent.
    ///
    /// `context_factory` builds a fresh context for each task, since a worker
    /// thread cannot borrow the context the server loop holds.
    ///
    /// Individual tools still opt in through [`Tool::task_support`]; enabling
    /// the runtime alone does not make any tool task-callable.
    ///
    /// ```ignore
    /// let mut server: Server<AppContext> = Server::new(config);
    /// server.enable_tasks(TaskConfig::default(), || AppContext::new());
    /// ```
    pub fn enable_tasks<F>(&mut self, config: TaskConfig, context_factory: F)
    where
        F: Fn() -> C + Send + Sync + 'static,
    {
        self.tasks = Some(TaskRuntime {
            store: Arc::new(TaskStore::new(config)),
            context_factory: Arc::new(context_factory),
        });
    }

    /// The task store, once tasks are enabled.
    ///
    /// Useful for inspecting state in tests or an admin surface.
    pub fn task_store(&self) -> Option<&Arc<TaskStore>> {
        self.tasks.as_ref().map(|runtime| &runtime.store)
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
            client_capabilities: &self.client_capabilities,
            broker: &self.broker,
            can_request: true,
            cancelled: None,
        }
    }

    /// Add a tool to the server
    pub fn add_tool(&mut self, tool: impl Tool<C> + 'static) -> Result<()> {
        let name = tool.name().to_string();
        if self.tools.contains_key(&name) {
            return Err(McpError::Internal(format!("Duplicate tool: {}", name)));
        }
        self.tools.insert(name, Arc::new(tool));
        Ok(())
    }

    /// Add a resource to the server
    pub fn add_resource(&mut self, resource: impl Resource + 'static) -> Result<()> {
        let uri = resource.uri();
        if self.resources.contains_key(&uri) {
            return Err(McpError::Internal(format!("Duplicate resource: {}", uri)));
        }
        // Copy-on-write: the clone only happens if a task worker is already
        // holding a handle to the map, which is rare and cheap either way.
        Arc::make_mut(&mut self.resources).insert(uri, Arc::new(resource));
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
            // A tool awaiting a server-initiated response reads from the same
            // transport, and hands back anything that was not its own. Drain
            // that first so those messages are handled in arrival order.
            let message = match self.broker.next_deferred() {
                Some(message) => message,
                None => {
                    let mut t = transport
                        .lock()
                        .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
                    match t.read() {
                        Ok(msg) => msg,
                        Err(McpError::TransportClosed) => break,
                        Err(e) => return Err(e),
                    }
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
            "tasks/get" => self.handle_task_get(request),
            "tasks/result" => self.handle_task_result(request),
            "tasks/list" => self.handle_task_list(request),
            "tasks/cancel" => self.handle_task_cancel(request),
            method => Err(McpError::MethodNotFound(method.to_string())),
        }
    }

    //
    // Tasks
    //

    /// The task store, or `-32601` if tasks were never enabled.
    fn task_store_or_unsupported(&self) -> Result<&Arc<TaskStore>> {
        self.task_store().ok_or_else(|| {
            McpError::MethodNotFound("tasks are not supported by this server".into())
        })
    }

    fn handle_task_get(&self, request: &JsonRpcRequest) -> Result<Value> {
        let store = self.task_store_or_unsupported()?;
        let params: TaskIdParams = parse_params(request)?;
        Ok(serde_json::to_value(store.get(&params.task_id)?)?)
    }

    /// `tasks/result` - block until terminal, then return exactly what the
    /// underlying request would have returned.
    fn handle_task_result(&self, request: &JsonRpcRequest) -> Result<Value> {
        let store = self.task_store_or_unsupported()?;
        let params: TaskIdParams = parse_params(request)?;

        match store.await_result(&params.task_id)? {
            // "For tasks in a terminal status, receivers MUST return from
            // tasks/result exactly what the underlying request would have
            // returned, whether that is a successful result or a JSON-RPC
            // error." The related-task metadata is required here specifically,
            // because the result shape carries no task id of its own.
            TaskOutcome::Value(mut value) => {
                if let Some(object) = value.as_object_mut() {
                    object.insert(
                        "_meta".to_string(),
                        Value::Object(related_task_meta(&params.task_id)),
                    );
                }
                Ok(value)
            }
            TaskOutcome::Error(error) => Err(McpError::Passthrough(error)),
        }
    }

    fn handle_task_list(&self, request: &JsonRpcRequest) -> Result<Value> {
        let store = self.task_store_or_unsupported()?;
        let params: ListTasksParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())?,
            None => ListTasksParams::default(),
        };

        let all = store.list()?;
        let state = PageState::from_cursor(params.cursor.as_deref(), self.config.page_size);
        let (tasks, next_cursor) = paginate(&all, &state);

        Ok(serde_json::to_value(ListTasksResult {
            tasks,
            next_cursor,
        })?)
    }

    fn handle_task_cancel(&self, request: &JsonRpcRequest) -> Result<Value> {
        let store = self.task_store_or_unsupported()?;
        let params: TaskIdParams = parse_params(request)?;
        Ok(serde_json::to_value(store.cancel(&params.task_id)?)?)
    }

    /// Start a task-augmented tool call and answer with a `CreateTaskResult`.
    ///
    /// The task record is created before the response goes out, so a client
    /// that polls immediately always finds it.
    fn start_tool_task(
        &self,
        tool: Arc<dyn Tool<C>>,
        arguments: Value,
        task_params: TaskParams,
    ) -> Result<Value> {
        let runtime = self
            .tasks
            .as_ref()
            .ok_or_else(|| McpError::Internal("tasks are not enabled".into()))?;

        let (task, cancelled) = runtime.store.create(task_params.ttl)?;

        let store = runtime.store.clone();
        let context_factory = runtime.context_factory.clone();
        let transport = self.transport.clone();
        let resources = self.resources.clone();
        let broker = self.broker.clone();
        let log_level = self.log_level;
        let stderr_logging = self.config.stderr_logging;
        let logger = self.config.name.clone();
        let task_id = task.task_id.clone();

        // std::thread, not a runtime: the work really does run alongside the
        // server loop, so polling returns live answers.
        let spawned = std::thread::Builder::new()
            .name(format!("sml-task-{}", &task_id[..8.min(task_id.len())]))
            .spawn(move || {
                let mut context = context_factory();
                let env = ToolEnv {
                    transport: transport.as_ref(),
                    resources: &resources,
                    log_level,
                    stderr_logging,
                    logger: &logger,
                    client_capabilities: &NO_CLIENT_CAPABILITIES,
                    broker: &broker,
                    // Nothing is reading the transport on this thread, and the
                    // server loop may itself be blocked in tasks/result on this
                    // very task. A round trip from here could never complete,
                    // so it is refused rather than deadlocked.
                    can_request: false,
                    cancelled: Some(&cancelled),
                };

                let (status, outcome) = match tool.execute(arguments, &mut context, &env) {
                    // "For tool calls specifically, this includes cases where
                    // the tool call result has isError set to true" -> failed.
                    Ok(result) if result.is_error => {
                        let message = result
                            .content
                            .first()
                            .and_then(Content::as_text)
                            .unwrap_or("tool reported an error")
                            .to_string();
                        match serde_json::to_value(result) {
                            Ok(value) => (TaskStatus::Failed, TaskOutcome::Value(value)),
                            Err(e) => (
                                TaskStatus::Failed,
                                TaskOutcome::Error(JsonRpcError::internal_error(format!(
                                    "{message} (and the result could not be serialized: {e})"
                                ))),
                            ),
                        }
                    }
                    Ok(result) => match serde_json::to_value(result) {
                        Ok(value) => (TaskStatus::Completed, TaskOutcome::Value(value)),
                        Err(e) => (
                            TaskStatus::Failed,
                            TaskOutcome::Error(JsonRpcError::internal_error(e.to_string())),
                        ),
                    },
                    Err(McpError::ToolError(message)) | Err(McpError::InvalidParams(message)) => {
                        // Same classification as a synchronous call: a tool
                        // execution failure is an isError result.
                        match serde_json::to_value(CallToolResult::error(&message)) {
                            Ok(value) => (TaskStatus::Failed, TaskOutcome::Value(value)),
                            Err(e) => (
                                TaskStatus::Failed,
                                TaskOutcome::Error(JsonRpcError::internal_error(e.to_string())),
                            ),
                        }
                    }
                    Err(other) => (
                        TaskStatus::Failed,
                        TaskOutcome::Error(other.to_jsonrpc_error()),
                    ),
                };

                // Ignored if the task was cancelled or swept meanwhile, which
                // is exactly the required behavior.
                let _ = store.finish(&task_id, status, outcome);

                // Optional per the spec, but it lets a client stop polling
                // early when it is listening.
                if let Ok(task) = store.get(&task_id) {
                    if let Some(transport) = &transport {
                        if let Ok(params) = serde_json::to_value(&task) {
                            let notification = JsonRpcMessage::notification(
                                "notifications/tasks/status",
                                Some(params),
                            );
                            if let Ok(mut guard) = transport.lock() {
                                let _ = guard.write(&notification);
                            }
                        }
                    }
                }
            });

        if let Err(e) = spawned {
            // The task exists but has no worker; fail it rather than leaving
            // it working forever.
            let _ = runtime.store.finish(
                &task.task_id,
                TaskStatus::Failed,
                TaskOutcome::Error(JsonRpcError::internal_error(format!(
                    "failed to spawn task worker: {e}"
                ))),
            );
            return Err(McpError::Internal(format!(
                "failed to spawn task worker: {e}"
            )));
        }

        Ok(serde_json::to_value(CreateTaskResult { task, meta: None })?)
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
        let params: InitializeParams = match &request.params {
            Some(p) => serde_json::from_value(p.clone())
                .map_err(|e| McpError::InvalidParams(format!("Invalid initialize params: {e}")))?,
            None => InitializeParams::default(),
        };

        // Remembered so ToolEnv can refuse to use a capability the client
        // never declared, which the spec requires of both parties.
        self.client_capabilities = params.capabilities;
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
                // Only declared when a runtime is actually installed, since
                // declaring it commits us to honoring task-augmented requests.
                tasks: self.tasks.as_ref().map(|_| TasksCapability {
                    list: Some(serde_json::json!({})),
                    cancel: Some(serde_json::json!({})),
                    requests: Some(serde_json::json!({ "tools": { "call": {} } })),
                }),
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
            .ok_or_else(|| McpError::InvalidParams(format!("Unknown tool: {}", params.name)))?
            .clone();

        // Task-augmentation negotiation happens on two levels: the server
        // capability, then the per-tool `execution.taskSupport`.
        let augmented = params.task.is_some();
        let support = tool.task_support();

        if self.tasks.is_some() {
            match (augmented, support) {
                (true, TaskSupport::Forbidden) => {
                    // "Servers SHOULD return a -32601 error if a client
                    // attempts to [invoke a forbidden tool as a task]."
                    return Err(McpError::MethodNotFound(format!(
                        "tool `{}` cannot be invoked as a task",
                        params.name
                    )));
                }
                (false, TaskSupport::Required) => {
                    // "Servers MUST return a -32601 error if a client does not
                    // [invoke a required tool as a task]."
                    return Err(McpError::MethodNotFound(format!(
                        "tool `{}` must be invoked as a task",
                        params.name
                    )));
                }
                (true, _) => {
                    return self.start_tool_task(
                        tool,
                        params.arguments.unwrap_or(serde_json::json!({})),
                        params.task.unwrap_or_default(),
                    );
                }
                (false, _) => {}
            }
        }
        // With tasks disabled we "MUST process requests of that type normally,
        // ignoring any task-augmentation metadata if present" - so the `task`
        // field simply falls on the floor.

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
    use std::collections::VecDeque;
    use std::time::Duration;

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

    // Borrowed by log_env, which needs somewhere for the &-fields to point.
    static NO_CAPABILITIES: std::sync::LazyLock<ClientCapabilities> =
        std::sync::LazyLock::new(ClientCapabilities::default);
    static TEST_BROKER: std::sync::LazyLock<RequestBroker> =
        std::sync::LazyLock::new(RequestBroker::new);

    /// Build a standalone ToolEnv for direct logging tests.
    fn log_env<'a>(
        transport: Option<&'a Arc<Mutex<dyn Transport>>>,
        resources: &'a HashMap<String, Arc<dyn Resource>>,
        log_level: LogLevel,
        stderr_logging: StderrLogging,
    ) -> ToolEnv<'a> {
        ToolEnv {
            transport,
            resources,
            log_level,
            stderr_logging,
            logger: "test-logger",
            client_capabilities: &NO_CAPABILITIES,
            broker: &TEST_BROKER,
            can_request: true,
            cancelled: None,
        }
    }

    #[test]
    fn test_log_respects_threshold() {
        let resources: HashMap<String, Arc<dyn Resource>> = HashMap::new();
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
        let resources: HashMap<String, Arc<dyn Resource>> = HashMap::new();
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
        let resources: HashMap<String, Arc<dyn Resource>> = HashMap::new();
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
        let resources: HashMap<String, Arc<dyn Resource>> = HashMap::new();

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
        let resources: HashMap<String, Arc<dyn Resource>> = HashMap::new();
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
        let resources: HashMap<String, Arc<dyn Resource>> = HashMap::new();
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
        let mut resources: HashMap<String, Arc<dyn Resource>> = HashMap::new();
        resources.insert(
            "test://a".into(),
            Arc::new(TestResource {
                uri: "test://a".into(),
                data: "a".into(),
            }),
        );
        resources.insert(
            "test://b".into(),
            Arc::new(TestResource {
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
        let mut resources: HashMap<String, Arc<dyn Resource>> = HashMap::new();
        resources.insert(
            "test://data".into(),
            Arc::new(TestResource {
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

    //
    // Server-initiated requests: elicitation and sampling
    //

    /// Transport that answers every server-initiated request from a queue of
    /// canned responses, and records what was sent.
    struct ScriptedTransport {
        /// Responses to hand back, in order, as raw result values.
        replies: VecDeque<Value>,
        /// Messages the server wrote.
        written: Arc<Mutex<Vec<JsonRpcMessage>>>,
        /// Messages to inject that are *not* the awaited response.
        inject: VecDeque<JsonRpcMessage>,
    }

    impl ScriptedTransport {
        fn new(replies: Vec<Value>) -> Self {
            Self {
                replies: replies.into(),
                written: Arc::new(Mutex::new(Vec::new())),
                inject: VecDeque::new(),
            }
        }

        fn with_injected(mut self, messages: Vec<JsonRpcMessage>) -> Self {
            self.inject = messages.into();
            self
        }

        /// The id of the most recent request the server wrote.
        fn last_request_id(&self) -> Option<RequestId> {
            self.written
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find_map(|m| match m {
                    JsonRpcMessage::Request(r) => Some(r.id.clone()),
                    _ => None,
                })
        }
    }

    impl Transport for ScriptedTransport {
        fn read(&mut self) -> Result<JsonRpcMessage> {
            // Anything queued for injection comes first, so the waiter has to
            // route it rather than mistake it for its own response.
            if let Some(message) = self.inject.pop_front() {
                return Ok(message);
            }
            let Some(result) = self.replies.pop_front() else {
                return Err(McpError::TransportClosed);
            };
            let id = self
                .last_request_id()
                .ok_or_else(|| McpError::Internal("no request to answer".into()))?;
            Ok(JsonRpcMessage::Response(JsonRpcResponse {
                jsonrpc: Default::default(),
                id,
                result: Some(result),
                error: None,
            }))
        }

        fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
            self.written.lock().unwrap().push(message.clone());
            Ok(())
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// A server, the log of what it wrote, and a handle keeping the transport
    /// alive for the duration of the test.
    type ScriptedServer = (
        Server<TestContext>,
        Arc<Mutex<Vec<JsonRpcMessage>>>,
        Arc<Mutex<dyn Transport>>,
    );

    /// Build a server wired to a scripted transport, with the given client
    /// capabilities already negotiated.
    fn scripted_server(capabilities: Value, transport: ScriptedTransport) -> ScriptedServer {
        let written = transport.written.clone();
        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(transport));

        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.transport = Some(transport.clone());
        server.client_capabilities = serde_json::from_value(capabilities).unwrap();

        (server, written, transport)
    }

    fn written_requests(written: &Arc<Mutex<Vec<JsonRpcMessage>>>) -> Vec<JsonRpcRequest> {
        written
            .lock()
            .unwrap()
            .iter()
            .filter_map(|m| match m {
                JsonRpcMessage::Request(r) => Some(r.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn test_elicit_form_round_trip() {
        let transport = ScriptedTransport::new(vec![serde_json::json!({
            "action": "accept",
            "content": { "name": "octocat" }
        })]);
        let (server, written, _t) = scripted_server(
            serde_json::json!({ "elicitation": { "form": {} } }),
            transport,
        );

        let env = server.tool_env();
        let result = env
            .elicit_form(
                "Please provide your GitHub username",
                ElicitSchema::new()
                    .string("name", |f| f)
                    .required("name")
                    .build(),
            )
            .unwrap();

        assert!(result.accepted());
        assert_eq!(result.accepted_content().unwrap()["name"], "octocat");

        let requests = written_requests(&written);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "elicitation/create");
        let params = requests[0].params.as_ref().unwrap();
        assert_eq!(params["mode"], "form");
        assert_eq!(params["requestedSchema"]["required"][0], "name");
    }

    #[test]
    fn test_elicit_url_round_trip() {
        let transport = ScriptedTransport::new(vec![serde_json::json!({ "action": "accept" })]);
        let (server, written, _t) = scripted_server(
            serde_json::json!({ "elicitation": { "url": {} } }),
            transport,
        );

        let env = server.tool_env();
        let result = env
            .elicit_url(
                "Please provide your API key to continue.",
                "https://mcp.example.com/ui/set_api_key",
                "550e8400-e29b-41d4-a716-446655440000",
            )
            .unwrap();

        assert!(result.accepted());
        // URL mode accept means consent to navigate, not completion.
        assert!(result.accepted_content().is_none());

        let params = written_requests(&written)[0].params.clone().unwrap();
        assert_eq!(params["mode"], "url");
        assert_eq!(
            params["elicitationId"],
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_elicit_refuses_mode_the_client_did_not_declare() {
        // "Servers MUST NOT send elicitation requests with modes that are not
        // supported by the client."
        let transport = ScriptedTransport::new(vec![serde_json::json!({ "action": "accept" })]);
        let (server, written, _t) = scripted_server(
            serde_json::json!({ "elicitation": { "form": {} } }),
            transport,
        );

        let env = server.tool_env();
        let err = env
            .elicit_url("m", "https://example.com/x", "id-1")
            .unwrap_err();

        assert!(err.to_string().contains("Url"), "{err}");
        // Nothing may go out on the wire.
        assert!(written_requests(&written).is_empty());
    }

    #[test]
    fn test_elicit_refuses_when_no_elicitation_capability() {
        let transport = ScriptedTransport::new(vec![]);
        let (server, written, _t) = scripted_server(serde_json::json!({}), transport);

        let env = server.tool_env();
        assert!(env.elicit_form("m", serde_json::json!({})).is_err());
        assert!(written_requests(&written).is_empty());
    }

    #[test]
    fn test_elicit_validates_before_sending() {
        let transport = ScriptedTransport::new(vec![]);
        let (server, written, _t) = scripted_server(
            serde_json::json!({ "elicitation": { "form": {}, "url": {} } }),
            transport,
        );

        let env = server.tool_env();
        let mut bad = ElicitParams::url("m", "not-a-url", "id");
        bad.url = Some("not-a-url".into());

        assert!(matches!(env.elicit(bad), Err(McpError::InvalidParams(_))));
        assert!(written_requests(&written).is_empty());
    }

    #[test]
    fn test_elicit_surfaces_client_errors() {
        // A client that got a mode it does not support answers -32602.
        struct ErroringTransport(Arc<Mutex<Vec<JsonRpcMessage>>>);
        impl Transport for ErroringTransport {
            fn read(&mut self) -> Result<JsonRpcMessage> {
                let id = self
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .rev()
                    .find_map(|m| match m {
                        JsonRpcMessage::Request(r) => Some(r.id.clone()),
                        _ => None,
                    })
                    .unwrap();
                Ok(JsonRpcMessage::error(
                    id,
                    JsonRpcError::invalid_params("unsupported mode"),
                ))
            }
            fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
                self.0.lock().unwrap().push(message.clone());
                Ok(())
            }
            fn close(&mut self) -> Result<()> {
                Ok(())
            }
        }

        let written = Arc::new(Mutex::new(Vec::new()));
        let transport: Arc<Mutex<dyn Transport>> =
            Arc::new(Mutex::new(ErroringTransport(written.clone())));
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.transport = Some(transport);
        server.client_capabilities =
            serde_json::from_value(serde_json::json!({ "elicitation": { "form": {} } })).unwrap();

        let env = server.tool_env();
        let err = env.elicit_form("m", serde_json::json!({})).unwrap_err();
        assert!(err.to_string().contains("unsupported mode"), "{err}");
    }

    #[test]
    fn test_round_trip_defers_client_messages_it_reads() {
        // While awaiting its response, the waiter is the only reader, so it
        // must hand back anything that is not its own answer.
        let injected = vec![
            JsonRpcMessage::notification("notifications/cancelled", None),
            JsonRpcMessage::request(99i64, "ping", None),
        ];
        let transport = ScriptedTransport::new(vec![serde_json::json!({ "action": "decline" })])
            .with_injected(injected);
        let (server, _w, _t) = scripted_server(
            serde_json::json!({ "elicitation": { "form": {} } }),
            transport,
        );

        let env = server.tool_env();
        let result = env.elicit_form("m", serde_json::json!({})).unwrap();
        assert_eq!(result.action, ElicitAction::Decline);

        // Both injected messages are queued for the main loop, in order.
        let first = server.broker.next_deferred().unwrap();
        let second = server.broker.next_deferred().unwrap();
        assert!(
            matches!(first, JsonRpcMessage::Notification(ref n) if n.method == "notifications/cancelled")
        );
        assert!(matches!(second, JsonRpcMessage::Request(ref r) if r.method == "ping"));
        assert!(server.broker.next_deferred().is_none());
    }

    #[test]
    fn test_round_trip_parks_responses_for_other_requests() {
        // A response for a different in-flight id must be kept, not dropped.
        let stray = JsonRpcMessage::Response(JsonRpcResponse {
            jsonrpc: Default::default(),
            id: RequestId::String("sml-999".into()),
            result: Some(serde_json::json!({ "action": "accept" })),
            error: None,
        });
        let transport = ScriptedTransport::new(vec![serde_json::json!({ "action": "cancel" })])
            .with_injected(vec![stray]);
        let (server, _w, _t) = scripted_server(
            serde_json::json!({ "elicitation": { "form": {} } }),
            transport,
        );

        let env = server.tool_env();
        let result = env.elicit_form("m", serde_json::json!({})).unwrap();
        assert_eq!(result.action, ElicitAction::Cancel);

        // The stray response is parked, not discarded.
        assert!(
            server
                .broker
                .take_parked(&RequestId::String("sml-999".into()))
                .is_some()
        );
    }

    #[test]
    fn test_round_trip_uses_prefixed_string_ids() {
        // Server ids must not collide with the numeric ids clients use.
        let transport = ScriptedTransport::new(vec![serde_json::json!({ "action": "accept" })]);
        let (server, written, _t) = scripted_server(
            serde_json::json!({ "elicitation": { "form": {} } }),
            transport,
        );

        server
            .tool_env()
            .elicit_form("m", serde_json::json!({}))
            .unwrap();

        let RequestId::String(id) = &written_requests(&written)[0].id else {
            panic!("expected a string id");
        };
        assert!(id.starts_with("sml-"), "{id}");
    }

    #[test]
    fn test_round_trip_fails_cleanly_without_a_transport() {
        // Rather than hanging, a round trip with nowhere to go errors out.
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.client_capabilities =
            serde_json::from_value(serde_json::json!({ "elicitation": { "form": {} } })).unwrap();

        let err = server
            .tool_env()
            .elicit_form("m", serde_json::json!({}))
            .unwrap_err();
        assert!(err.to_string().contains("transport"), "{err}");
    }

    #[test]
    fn test_round_trip_fails_cleanly_when_the_transport_closes() {
        let transport = ScriptedTransport::new(vec![]);
        let (server, _w, _t) = scripted_server(
            serde_json::json!({ "elicitation": { "form": {} } }),
            transport,
        );

        assert!(matches!(
            server.tool_env().elicit_form("m", serde_json::json!({})),
            Err(McpError::TransportClosed)
        ));
    }

    #[test]
    fn test_elicitation_complete_notification() {
        let transport = ScriptedTransport::new(vec![]);
        let (server, written, _t) = scripted_server(
            serde_json::json!({ "elicitation": { "url": {} } }),
            transport,
        );

        server
            .tool_env()
            .notify_elicitation_complete("550e8400-e29b-41d4-a716-446655440000")
            .unwrap();

        let sent = written.lock().unwrap().clone();
        let JsonRpcMessage::Notification(n) = &sent[0] else {
            panic!("expected notification");
        };
        assert_eq!(n.method, "notifications/elicitation/complete");
        assert_eq!(
            n.params.as_ref().unwrap()["elicitationId"],
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_sampling_round_trip() {
        let transport = ScriptedTransport::new(vec![serde_json::json!({
            "role": "assistant",
            "content": { "type": "text", "text": "The capital of France is Paris." },
            "model": "claude-3-sonnet-20240307",
            "stopReason": "endTurn"
        })]);
        let (server, written, _t) =
            scripted_server(serde_json::json!({ "sampling": {} }), transport);

        let result = server
            .tool_env()
            .create_message(CreateMessageParams::new(
                vec![SamplingMessage::user(SamplingContent::text(
                    "What is the capital of France?",
                ))],
                100,
            ))
            .unwrap();

        assert_eq!(result.text(), "The capital of France is Paris.");
        assert!(!result.wants_tools());
        assert_eq!(
            written_requests(&written)[0].method,
            "sampling/createMessage"
        );
    }

    #[test]
    fn test_sampling_with_tools_round_trip() {
        let transport = ScriptedTransport::new(vec![serde_json::json!({
            "role": "assistant",
            "content": [
                { "type": "tool_use", "id": "call_abc123", "name": "get_weather",
                  "input": { "city": "Paris" } }
            ],
            "model": "claude-3-sonnet-20240307",
            "stopReason": "toolUse"
        })]);
        let (server, written, _t) = scripted_server(
            serde_json::json!({ "sampling": { "tools": {} } }),
            transport,
        );

        let result = server
            .tool_env()
            .create_message(
                CreateMessageParams::new(
                    vec![SamplingMessage::user(SamplingContent::text("weather?"))],
                    1000,
                )
                .with_tools([SamplingTool::new(
                    "get_weather",
                    serde_json::json!({ "type": "object" }),
                )])
                .with_tool_choice(ToolChoice::auto()),
            )
            .unwrap();

        assert!(result.wants_tools());
        assert_eq!(result.tool_uses()[0].1, "get_weather");

        let params = written_requests(&written)[0].params.clone().unwrap();
        assert_eq!(params["tools"][0]["name"], "get_weather");
        assert_eq!(params["toolChoice"]["mode"], "auto");
    }

    #[test]
    fn test_sampling_refuses_tools_without_the_capability() {
        // "Servers MUST NOT send tool-enabled sampling requests to Clients
        // that have not declared support via the sampling.tools capability."
        let transport = ScriptedTransport::new(vec![]);
        let (server, written, _t) =
            scripted_server(serde_json::json!({ "sampling": {} }), transport);

        let err = server
            .tool_env()
            .create_message(
                CreateMessageParams::new(
                    vec![SamplingMessage::user(SamplingContent::text("hi"))],
                    100,
                )
                .with_tools([SamplingTool::new("t", serde_json::json!({}))]),
            )
            .unwrap_err();

        assert!(err.to_string().contains("sampling.tools"), "{err}");
        assert!(written_requests(&written).is_empty());
    }

    #[test]
    fn test_sampling_refuses_without_the_sampling_capability() {
        let transport = ScriptedTransport::new(vec![]);
        let (server, written, _t) = scripted_server(serde_json::json!({}), transport);

        let err = server
            .tool_env()
            .create_message(CreateMessageParams::new(
                vec![SamplingMessage::user(SamplingContent::text("hi"))],
                100,
            ))
            .unwrap_err();

        assert!(err.to_string().contains("sampling"), "{err}");
        assert!(written_requests(&written).is_empty());
    }

    #[test]
    fn test_sampling_refuses_include_context_without_the_capability() {
        let transport = ScriptedTransport::new(vec![]);
        let (server, written, _t) =
            scripted_server(serde_json::json!({ "sampling": {} }), transport);

        let mut params = CreateMessageParams::new(
            vec![SamplingMessage::user(SamplingContent::text("hi"))],
            100,
        );
        params.include_context = Some("thisServer".into());

        let err = server.tool_env().create_message(params).unwrap_err();
        assert!(err.to_string().contains("includeContext"), "{err}");
        assert!(written_requests(&written).is_empty());

        // "none" needs no capability.
        let transport = ScriptedTransport::new(vec![serde_json::json!({
            "role": "assistant",
            "content": { "type": "text", "text": "ok" },
            "model": "m"
        })]);
        let (server, _w, _t) = scripted_server(serde_json::json!({ "sampling": {} }), transport);
        let mut params = CreateMessageParams::new(
            vec![SamplingMessage::user(SamplingContent::text("hi"))],
            100,
        );
        params.include_context = Some("none".into());
        assert!(server.tool_env().create_message(params).is_ok());
    }

    #[test]
    fn test_sampling_validates_the_tool_loop_before_sending() {
        let transport = ScriptedTransport::new(vec![]);
        let (server, written, _t) = scripted_server(
            serde_json::json!({ "sampling": { "tools": {} } }),
            transport,
        );

        // An assistant turn with an unanswered tool use.
        let params = CreateMessageParams::new(
            vec![SamplingMessage::assistant(vec![SamplingContent::ToolUse {
                id: "a".into(),
                name: "t".into(),
                input: serde_json::json!({}),
            }])],
            100,
        );

        assert!(matches!(
            server.tool_env().create_message(params),
            Err(McpError::InvalidParams(_))
        ));
        assert!(written_requests(&written).is_empty());
    }

    #[test]
    fn test_client_capabilities_recorded_at_initialize() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        assert!(!server.client_capabilities.supports_sampling());

        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "initialize".to_string(),
            params: Some(serde_json::json!({
                "protocolVersion": "2025-11-25",
                "capabilities": {
                    "sampling": { "tools": {} },
                    "elicitation": { "form": {}, "url": {} }
                },
                "clientInfo": { "name": "test", "version": "1.0" }
            })),
        };
        let mut ctx = TestContext { counter: 0 };
        server.dispatch_request(&request, &mut ctx).unwrap();

        assert!(server.client_capabilities.supports_sampling());
        assert!(server.client_capabilities.supports_sampling_tools());
        assert!(
            server
                .client_capabilities
                .supports_elicitation(ElicitationMode::Url)
        );
        assert!(
            server
                .client_capabilities
                .supports_elicitation(ElicitationMode::Form)
        );
    }

    #[test]
    fn test_uninitialized_server_declares_no_client_capabilities() {
        // Without an initialize, we have no evidence the client can handle
        // server-initiated requests, so we must not send any.
        let server: Server<TestContext> = Server::new(ServerConfig::default());
        assert!(!server.client_capabilities.supports_sampling());
        assert!(!server.client_capabilities.supports_sampling_tools());
        assert!(
            !server
                .client_capabilities
                .supports_elicitation(ElicitationMode::Form)
        );
    }

    //
    // Tasks (2025-11-25)
    //

    /// Tool that blocks until released, so a task can be observed mid-flight.
    struct SlowTool {
        release: Arc<(Mutex<bool>, std::sync::Condvar)>,
    }

    impl SlowTool {
        fn new() -> (Self, Arc<(Mutex<bool>, std::sync::Condvar)>) {
            let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
            (
                Self {
                    release: release.clone(),
                },
                release,
            )
        }

        fn release(gate: &Arc<(Mutex<bool>, std::sync::Condvar)>) {
            *gate.0.lock().unwrap() = true;
            gate.1.notify_all();
        }
    }

    impl Tool<TestContext> for SlowTool {
        fn name(&self) -> &str {
            "slow"
        }
        fn description(&self) -> &str {
            "Blocks until released"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object" })
        }
        fn task_support(&self) -> TaskSupport {
            TaskSupport::Optional
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            env: &ToolEnv,
        ) -> Result<CallToolResult> {
            let (lock, condvar) = &*self.release;
            let mut released = lock.lock().unwrap();
            while !*released && !env.is_cancelled() {
                let (guard, timeout) = condvar
                    .wait_timeout(released, Duration::from_millis(20))
                    .unwrap();
                released = guard;
                if timeout.timed_out() && env.is_cancelled() {
                    break;
                }
            }
            if env.is_cancelled() {
                return Ok(CallToolResult::text("stopped early"));
            }
            Ok(CallToolResult::text("finished"))
        }
    }

    /// Tool that must be invoked as a task.
    struct TaskOnlyTool;

    impl Tool<TestContext> for TaskOnlyTool {
        fn name(&self) -> &str {
            "task_only"
        }
        fn description(&self) -> &str {
            "Only callable as a task"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object" })
        }
        fn task_support(&self) -> TaskSupport {
            TaskSupport::Required
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            env: &ToolEnv,
        ) -> Result<CallToolResult> {
            // Proves the worker's environment knows it is inside a task.
            assert!(env.is_task());
            Ok(CallToolResult::text("ran as task"))
        }
    }

    fn task_server() -> Server<TestContext> {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.enable_tasks(TaskConfig::default(), || TestContext { counter: 0 });
        server
    }

    fn dispatch(server: &mut Server<TestContext>, method: &str, params: Value) -> Result<Value> {
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: method.to_string(),
            params: Some(params),
        };
        let mut ctx = TestContext { counter: 0 };
        server.dispatch_request(&request, &mut ctx)
    }

    /// Poll tasks/get until the status is terminal, or give up.
    fn await_terminal(server: &mut Server<TestContext>, task_id: &str) -> Value {
        for _ in 0..500 {
            let task = dispatch(
                server,
                "tasks/get",
                serde_json::json!({ "taskId": task_id }),
            )
            .expect("tasks/get should succeed");
            let status = task["status"].as_str().unwrap();
            if matches!(status, "completed" | "failed" | "cancelled") {
                return task;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("task {task_id} never reached a terminal status");
    }

    #[test]
    fn test_tasks_capability_only_declared_when_enabled() {
        let mut plain: Server<TestContext> = Server::new(ServerConfig::default());
        let result = dispatch(&mut plain, "initialize", serde_json::json!({})).unwrap();
        assert!(result["capabilities"]["tasks"].is_null());

        let mut enabled = task_server();
        let result = dispatch(&mut enabled, "initialize", serde_json::json!({})).unwrap();
        let tasks = &result["capabilities"]["tasks"];
        assert!(!tasks.is_null());
        assert!(!tasks["list"].is_null());
        assert!(!tasks["cancel"].is_null());
        assert!(!tasks["requests"]["tools"]["call"].is_null());
    }

    #[test]
    fn test_task_methods_are_method_not_found_when_disabled() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        for method in ["tasks/get", "tasks/result", "tasks/list", "tasks/cancel"] {
            let err =
                dispatch(&mut server, method, serde_json::json!({ "taskId": "x" })).unwrap_err();
            assert_eq!(err.to_jsonrpc_error().code, -32601, "{method}");
        }
    }

    #[test]
    fn test_task_augmentation_ignored_when_tasks_disabled() {
        // "Receivers that do not declare the task capability for a request type
        // MUST process requests of that type normally, ignoring any
        // task-augmentation metadata if present."
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(IncrementTool).unwrap();

        let result = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({
                "name": "increment",
                "arguments": { "amount": 2 },
                "task": { "ttl": 60000 }
            }),
        )
        .unwrap();

        // Ordinary result, not a CreateTaskResult.
        assert!(result.get("task").is_none());
        assert!(result["content"][0]["text"].as_str().unwrap().contains('2'));
    }

    #[test]
    fn test_task_augmented_call_returns_create_task_result() {
        let mut server = task_server();
        let (tool, gate) = SlowTool::new();
        server.add_tool(tool).unwrap();

        let result = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "slow", "task": { "ttl": 60000 } }),
        )
        .unwrap();

        // "Tasks MUST begin in the working status when created."
        assert_eq!(result["task"]["status"], "working");
        let task_id = result["task"]["taskId"].as_str().unwrap().to_string();
        assert_eq!(result["task"]["ttl"], 60000);
        assert!(result["task"]["createdAt"].is_string());
        assert!(result["task"]["lastUpdatedAt"].is_string());
        // The actual result is NOT in the create response.
        assert!(result.get("content").is_none());

        // The task is visible immediately, before the work is done.
        let polled = dispatch(
            &mut server,
            "tasks/get",
            serde_json::json!({ "taskId": task_id }),
        )
        .unwrap();
        assert_eq!(polled["taskId"], task_id.as_str());

        SlowTool::release(&gate);
        let terminal = await_terminal(&mut server, &task_id);
        assert_eq!(terminal["status"], "completed");
    }

    #[test]
    fn test_task_result_returns_the_underlying_result() {
        let mut server = task_server();
        let (tool, gate) = SlowTool::new();
        server.add_tool(tool).unwrap();

        let created = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "slow", "task": {} }),
        )
        .unwrap();
        let task_id = created["task"]["taskId"].as_str().unwrap().to_string();

        SlowTool::release(&gate);
        await_terminal(&mut server, &task_id);

        let result = dispatch(
            &mut server,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        )
        .unwrap();

        assert_eq!(result["content"][0]["text"], "finished");
        // "The tasks/result operation MUST include this metadata in its
        // response, as the result structure itself does not contain the task
        // id."
        assert_eq!(
            result["_meta"]["io.modelcontextprotocol/related-task"]["taskId"],
            task_id.as_str()
        );
    }

    #[test]
    fn test_task_result_blocks_until_the_task_finishes() {
        let mut server = task_server();
        let (tool, gate) = SlowTool::new();
        server.add_tool(tool).unwrap();

        let created = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "slow", "task": {} }),
        )
        .unwrap();
        let task_id = created["task"]["taskId"].as_str().unwrap().to_string();

        // Release from another thread after a delay; tasks/result must wait.
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(120));
            SlowTool::release(&gate);
        });

        let started = std::time::Instant::now();
        let result = dispatch(
            &mut server,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        )
        .unwrap();

        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "tasks/result returned before the task finished"
        );
        assert_eq!(result["content"][0]["text"], "finished");
        releaser.join().unwrap();
    }

    #[test]
    fn test_task_cancel_moves_to_cancelled_and_stops_the_tool() {
        let mut server = task_server();
        let (tool, _gate) = SlowTool::new();
        server.add_tool(tool).unwrap();

        let created = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "slow", "task": {} }),
        )
        .unwrap();
        let task_id = created["task"]["taskId"].as_str().unwrap().to_string();

        let cancelled = dispatch(
            &mut server,
            "tasks/cancel",
            serde_json::json!({ "taskId": task_id }),
        )
        .unwrap();

        // The status moves before the response is sent.
        assert_eq!(cancelled["status"], "cancelled");
        assert_eq!(
            dispatch(
                &mut server,
                "tasks/get",
                serde_json::json!({ "taskId": task_id })
            )
            .unwrap()["status"],
            "cancelled"
        );
    }

    #[test]
    fn test_cancelling_a_terminal_task_is_invalid_params() {
        let mut server = task_server();
        let (tool, gate) = SlowTool::new();
        server.add_tool(tool).unwrap();

        let created = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "slow", "task": {} }),
        )
        .unwrap();
        let task_id = created["task"]["taskId"].as_str().unwrap().to_string();

        SlowTool::release(&gate);
        await_terminal(&mut server, &task_id);

        let err = dispatch(
            &mut server,
            "tasks/cancel",
            serde_json::json!({ "taskId": task_id }),
        )
        .unwrap_err();
        assert_eq!(err.to_jsonrpc_error().code, -32602);
    }

    #[test]
    fn test_unknown_task_ids_are_invalid_params() {
        let mut server = task_server();
        for method in ["tasks/get", "tasks/result", "tasks/cancel"] {
            let err = dispatch(
                &mut server,
                method,
                serde_json::json!({ "taskId": "nonexistent" }),
            )
            .unwrap_err();
            assert_eq!(err.to_jsonrpc_error().code, -32602, "{method}");
        }
    }

    #[test]
    fn test_tasks_list_paginates() {
        let mut server = Server::new(ServerConfig {
            page_size: 2,
            ..Default::default()
        });
        server.enable_tasks(TaskConfig::default(), || TestContext { counter: 0 });
        let (tool, gate) = SlowTool::new();
        server.add_tool(tool).unwrap();

        let mut ids = Vec::new();
        for _ in 0..3 {
            let created = dispatch(
                &mut server,
                "tools/call",
                serde_json::json!({ "name": "slow", "task": {} }),
            )
            .unwrap();
            ids.push(created["task"]["taskId"].as_str().unwrap().to_string());
        }

        let page = dispatch(&mut server, "tasks/list", serde_json::json!({})).unwrap();
        assert_eq!(page["tasks"].as_array().unwrap().len(), 2);
        assert!(page["nextCursor"].is_string());

        let next = dispatch(
            &mut server,
            "tasks/list",
            serde_json::json!({ "cursor": page["nextCursor"] }),
        )
        .unwrap();
        assert_eq!(next["tasks"].as_array().unwrap().len(), 1);
        assert!(next["nextCursor"].is_null());

        SlowTool::release(&gate);
        for id in &ids {
            await_terminal(&mut server, id);
        }
    }

    #[test]
    fn test_forbidden_tool_rejects_task_augmentation() {
        // "If execution.taskSupport is not present or forbidden, clients MUST
        // NOT attempt to invoke the tool as a task. Servers SHOULD return a
        // -32601 error if a client attempts to do so."
        let mut server = task_server();
        server.add_tool(IncrementTool).unwrap();

        let err = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "increment", "task": {} }),
        )
        .unwrap_err();
        assert_eq!(err.to_jsonrpc_error().code, -32601);
        assert!(err.to_string().contains("cannot be invoked as a task"));
    }

    #[test]
    fn test_required_tool_rejects_a_plain_call() {
        // "If execution.taskSupport is required, clients MUST invoke the tool
        // as a task. Servers MUST return a -32601 error if a client does not."
        let mut server = task_server();
        server.add_tool(TaskOnlyTool).unwrap();

        let err = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "task_only" }),
        )
        .unwrap_err();
        assert_eq!(err.to_jsonrpc_error().code, -32601);
        assert!(err.to_string().contains("must be invoked as a task"));
    }

    #[test]
    fn test_required_tool_runs_when_augmented() {
        let mut server = task_server();
        server.add_tool(TaskOnlyTool).unwrap();

        let created = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "task_only", "task": {} }),
        )
        .unwrap();
        let task_id = created["task"]["taskId"].as_str().unwrap().to_string();

        await_terminal(&mut server, &task_id);
        let result = dispatch(
            &mut server,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        )
        .unwrap();
        assert_eq!(result["content"][0]["text"], "ran as task");
    }

    #[test]
    fn test_task_support_appears_in_tools_list() {
        let mut server = task_server();
        server.add_tool(TaskOnlyTool).unwrap();
        server.add_tool(IncrementTool).unwrap();

        let result = dispatch(&mut server, "tools/list", serde_json::json!({})).unwrap();
        let tools = result["tools"].as_array().unwrap();

        let task_only = tools.iter().find(|t| t["name"] == "task_only").unwrap();
        assert_eq!(task_only["execution"]["taskSupport"], "required");

        // `forbidden` is the default and stays off the wire.
        let increment = tools.iter().find(|t| t["name"] == "increment").unwrap();
        assert!(increment.get("execution").is_none());
    }

    #[test]
    fn test_failing_tool_in_a_task_reaches_failed() {
        // "For tool calls specifically, this includes cases where the tool call
        // result has isError set to true."
        struct FailingTaskTool;
        impl Tool<TestContext> for FailingTaskTool {
            fn name(&self) -> &str {
                "fails"
            }
            fn description(&self) -> &str {
                "Always fails"
            }
            fn schema(&self) -> Value {
                serde_json::json!({ "type": "object" })
            }
            fn task_support(&self) -> TaskSupport {
                TaskSupport::Optional
            }
            fn execute(
                &self,
                _args: Value,
                _ctx: &mut TestContext,
                _env: &ToolEnv,
            ) -> Result<CallToolResult> {
                Err(McpError::ToolError("upstream exploded".into()))
            }
        }

        let mut server = task_server();
        server.add_tool(FailingTaskTool).unwrap();

        let created = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "fails", "task": {} }),
        )
        .unwrap();
        let task_id = created["task"]["taskId"].as_str().unwrap().to_string();

        let terminal = await_terminal(&mut server, &task_id);
        assert_eq!(terminal["status"], "failed");

        // tasks/result returns exactly what the call would have returned.
        let result = dispatch(
            &mut server,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        )
        .unwrap();
        assert_eq!(result["isError"], true);
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("upstream exploded")
        );
    }

    #[test]
    fn test_task_worker_cannot_make_server_initiated_requests() {
        // Nothing would be reading the transport while a worker blocked, and
        // the server loop may be inside tasks/result on this very task, so a
        // round trip must fail fast rather than deadlock.
        struct EliciterTool;
        impl Tool<TestContext> for EliciterTool {
            fn name(&self) -> &str {
                "elicits"
            }
            fn description(&self) -> &str {
                "Tries to elicit from inside a task"
            }
            fn schema(&self) -> Value {
                serde_json::json!({ "type": "object" })
            }
            fn task_support(&self) -> TaskSupport {
                TaskSupport::Optional
            }
            fn execute(
                &self,
                _args: Value,
                _ctx: &mut TestContext,
                env: &ToolEnv,
            ) -> Result<CallToolResult> {
                match env.send_request("elicitation/create", serde_json::json!({})) {
                    Ok(_) => Ok(CallToolResult::text("unexpectedly succeeded")),
                    Err(e) => Ok(CallToolResult::text(format!("refused: {e}"))),
                }
            }
        }

        let mut server = task_server();
        server.add_tool(EliciterTool).unwrap();

        let created = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "elicits", "task": {} }),
        )
        .unwrap();
        let task_id = created["task"]["taskId"].as_str().unwrap().to_string();

        await_terminal(&mut server, &task_id);
        let result = dispatch(
            &mut server,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        )
        .unwrap();

        let text = result["content"][0]["text"].as_str().unwrap();
        assert!(text.starts_with("refused:"), "{text}");
        assert!(text.contains("nothing is reading the transport"), "{text}");
    }

    #[test]
    fn test_task_ids_are_unpredictable() {
        let mut server = task_server();
        let (tool, gate) = SlowTool::new();
        server.add_tool(tool).unwrap();

        let mut ids = std::collections::HashSet::new();
        for _ in 0..5 {
            let created = dispatch(
                &mut server,
                "tools/call",
                serde_json::json!({ "name": "slow", "task": {} }),
            )
            .unwrap();
            let id = created["task"]["taskId"].as_str().unwrap().to_string();
            assert_eq!(id.len(), 32, "task ids carry 128 bits of entropy");
            ids.insert(id);
        }
        assert_eq!(ids.len(), 5);

        SlowTool::release(&gate);
        for id in &ids {
            await_terminal(&mut server, id);
        }
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
