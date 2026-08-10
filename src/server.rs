//! MCP Server
//!
//! Core server implementation with generic context support.

use crate::broker::RequestBroker;
use crate::pagination::{DEFAULT_PAGE_SIZE, PageState, paginate};
use crate::tasks::{
    CreateTaskResult, ListTasksParams, ListTasksResult, RELATED_TASK, TaskConfig, TaskIdParams,
    TaskOutcome, TaskParams, TaskStatus, TaskStore, TasksCapability, related_task_meta,
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
    /// Where outgoing messages go.
    ///
    /// `None` when the server was driven without a transport (direct dispatch
    /// in tests, for example). Sending is then a no-op rather than a panic.
    ///
    /// Independent of the read handle wherever the transport can be split, so a
    /// task worker can write while the server loop sits blocked in `read`.
    transport: Option<&'a Arc<Mutex<dyn Transport>>>,
    /// Where a [`RequestMode::Direct`] waiter reads its own response from.
    reader: Option<&'a Arc<Mutex<dyn Transport>>>,
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
    /// How this environment gets an answer out of the client.
    mode: RequestMode,
    /// How long a server-initiated request waits before giving up.
    request_timeout: Option<std::time::Duration>,
    /// Set inside a task worker.
    task: Option<TaskEnv<'a>>,
}

/// How a [`ToolEnv`] obtains the answer to a server-initiated request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestMode {
    /// Read the transport directly. Only valid on the thread running the
    /// server loop, which is the thread that would otherwise be reading.
    Direct,
    /// Write the request, then wait for whoever *is* reading to hand the
    /// response over. Used by task workers, which have no reader of their own.
    Delegated,
    /// Impossible here: nothing will ever read the answer.
    Forbidden,
}

/// The task a tool is running inside, when it is running inside one.
struct TaskEnv<'a> {
    store: &'a Arc<TaskStore>,
    task_id: &'a str,
    /// Flipped when the task is cancelled.
    cancelled: &'a Arc<AtomicBool>,
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
        let notification = JsonRpcMessage::notification(method, self.tag_with_task(params));
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

    /// Associate an outgoing message with the task it belongs to.
    ///
    /// "All requests, notifications, and responses related to a task **MUST**
    /// include the `io.modelcontextprotocol/related-task` key in their `_meta`
    /// field... For example, an elicitation that a task-augmented tool call
    /// depends on **MUST** share the same related task ID with that tool call's
    /// task."
    ///
    /// It is not decoration. A client sitting in `tasks/result` for task X that
    /// receives an out-of-band `elicitation/create` has no protocol-level way
    /// to know the elicitation belongs to X without it, and a conformant client
    /// may refuse to route it back.
    ///
    /// Outside a task this is the identity function. `notifications/tasks/status`
    /// is the one message that **SHOULD NOT** carry the key, and it is written
    /// directly by the worker rather than through here.
    fn tag_with_task(&self, params: Option<Value>) -> Option<Value> {
        let Some(task) = self.task.as_ref() else {
            return params;
        };

        let mut params = params.unwrap_or_else(|| Value::Object(serde_json::Map::new()));
        let Some(object) = params.as_object_mut() else {
            return Some(params);
        };

        let meta = object
            .entry("_meta")
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if let Some(meta) = meta.as_object_mut() {
            meta.insert(
                RELATED_TASK.to_string(),
                serde_json::json!({ "taskId": task.task_id }),
            );
        }
        Some(params)
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
        self.task
            .as_ref()
            .is_some_and(|task| task.cancelled.load(Ordering::SeqCst))
    }

    /// Is this tool running inside a task?
    pub fn is_task(&self) -> bool {
        self.task.is_some()
    }

    /// The id of the task running this tool, if any.
    pub fn task_id(&self) -> Option<&str> {
        self.task.as_ref().map(|task| task.task_id)
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
    /// Waits up to [`ServerConfig::request_timeout`] (2 minutes by default),
    /// then gives up with [`McpError::Timeout`]. Use
    /// [`send_request_with_timeout`](Self::send_request_with_timeout) for a
    /// different budget on one call.
    pub fn send_request(&self, method: &str, params: Value) -> Result<Value> {
        self.send_request_with_timeout(method, params, self.request_timeout)
    }

    /// [`send_request`](Self::send_request) with an explicit deadline.
    ///
    /// `None` waits indefinitely, which is only reasonable when something else
    /// guarantees the client answers.
    ///
    /// On expiry the request is abandoned - a late answer is discarded rather
    /// than mistaken for the next one - and the client is told with
    /// `notifications/cancelled`, which the spec asks of a party that times
    /// out. The transport is left exactly as it was found.
    pub fn send_request_with_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Option<std::time::Duration>,
    ) -> Result<Value> {
        if self.mode == RequestMode::Forbidden {
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
        let request = JsonRpcMessage::request(id.clone(), method, self.tag_with_task(Some(params)));
        let deadline = timeout.map(|t| std::time::Instant::now() + t);

        // Registered *before* the request goes out: the answer can come back
        // the instant it lands, and a waiter that had not registered yet would
        // miss it.
        let delegated = match self.mode {
            RequestMode::Delegated => Some(self.broker.register_waiter(&id)?),
            _ => None,
        };

        {
            let mut guard = transport
                .lock()
                .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
            guard.write(&request)?;
        }

        let outcome = match delegated {
            // Someone else is reading. Say the task is waiting on the user, then
            // block until they hand the answer over.
            Some(receiver) => {
                let _ = self.set_task_status(TaskStatus::InputRequired);
                let outcome = self.await_delegated_response(&id, receiver, deadline);
                let _ = self.set_task_status(TaskStatus::Working);
                outcome
            }
            None => {
                let reader = self.reader.unwrap_or(transport);
                let outcome = self.await_response(reader, &id, deadline);
                // However this ended, the transport goes back to blocking
                // reads: the server loop after us must not inherit a deadline
                // it never asked for.
                if deadline.is_some() {
                    if let Ok(mut guard) = reader.lock() {
                        let _ = guard.set_read_timeout(None);
                    }
                }
                outcome
            }
        };

        if let Err(McpError::Timeout(_)) = &outcome {
            self.broker.abandon(&id);
            // Best-effort: the client SHOULD stop working on something nobody
            // is waiting for. A failure here changes nothing for us.
            let _ = self.send_notification(
                "notifications/cancelled",
                Some(serde_json::json!({
                    "requestId": id,
                    "reason": format!("no response within {:?}", timeout.unwrap_or_default()),
                })),
            );
        }

        outcome
    }

    /// Wait for the reading thread to hand our response over.
    ///
    /// Polled rather than blocked outright so a cancelled task stops waiting on
    /// an answer whose result will be thrown away regardless.
    fn await_delegated_response(
        &self,
        id: &RequestId,
        receiver: std::sync::mpsc::Receiver<JsonRpcResponse>,
        deadline: Option<std::time::Instant>,
    ) -> Result<Value> {
        /// How often to look up from the channel to check for cancellation.
        const POLL: std::time::Duration = std::time::Duration::from_millis(100);

        loop {
            if self.is_cancelled() {
                return Err(McpError::Internal(format!(
                    "`{id:?}` abandoned: the task was cancelled"
                )));
            }

            let wait = match deadline {
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    if left.is_zero() {
                        return Err(McpError::Timeout(format!("no response to request {id:?}")));
                    }
                    left.min(POLL)
                }
                None => POLL,
            };

            match receiver.recv_timeout(wait) {
                Ok(response) => return RequestBroker::into_result(response),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                // The sender was dropped, which only happens if the broker
                // handed our slot to someone else - it cannot arrive now.
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(McpError::Internal(format!(
                        "the response to {id:?} can no longer be delivered"
                    )));
                }
            }
        }
    }

    /// Move the surrounding task between `working` and `input_required`.
    ///
    /// A no-op outside a task, and deliberately quiet: failing to announce that
    /// a task is waiting on input is not a reason to fail the request that is
    /// waiting.
    fn set_task_status(&self, status: TaskStatus) -> Result<bool> {
        match &self.task {
            Some(task) => task.store.set_status(task.task_id, status),
            None => Ok(false),
        }
    }

    /// Read until our response turns up, routing everything else on the way.
    ///
    /// We are the only reader while this runs, so everything that comes off the
    /// wire is ours to place: our response, someone else's response, or a
    /// client message the main loop still needs to see.
    fn await_response(
        &self,
        transport: &Arc<Mutex<dyn Transport>>,
        id: &RequestId,
        deadline: Option<std::time::Instant>,
    ) -> Result<Value> {
        loop {
            if let Some(response) = self.broker.take_parked(id) {
                return RequestBroker::into_result(response);
            }

            // Checked here as well as inside the transport, because a stream of
            // messages meant for someone else would otherwise keep resetting
            // the wait.
            let remaining = match deadline {
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    if left.is_zero() {
                        return Err(McpError::Timeout(format!("no response to request {id:?}")));
                    }
                    Some(left)
                }
                None => None,
            };

            let message = {
                let mut guard = transport
                    .lock()
                    .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
                // A transport that cannot honor deadlines says so; the loop's
                // own check still bounds the wait between messages, but a
                // single read may outlast it.
                guard.set_read_timeout(remaining)?;
                guard.read()?
            };

            match message {
                JsonRpcMessage::Response(response) if response.id == *id => {
                    return RequestBroker::into_result(response);
                }
                // Might belong to a task worker blocked on a channel, which
                // would never come back to collect a parked response.
                JsonRpcMessage::Response(response) => self.broker.deliver(response),
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
    // Roots
    //

    /// Ask the client which filesystem roots it has exposed.
    ///
    /// Returns an error if the client declared no `roots` capability, since
    /// both parties may "only use capabilities that were successfully
    /// negotiated".
    pub fn list_roots(&self) -> Result<Vec<Root>> {
        if !self.client_capabilities.supports_roots() {
            return Err(McpError::Internal(
                "client did not declare support for roots".into(),
            ));
        }
        let value = self.send_request("roots/list", serde_json::json!({}))?;
        let result: ListRootsResult = serde_json::from_value(value)?;
        Ok(result.roots)
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
    /// Protocol revisions this server will negotiate, newest first.
    ///
    /// Defaults to [`SUPPORTED_PROTOCOL_VERSIONS`]. Narrow it to drop support
    /// for older clients; the first entry is what an unrecognized request
    /// negotiates down to.
    pub supported_versions: Vec<String>,
    /// Minimum log severity delivered to clients that never call
    /// `logging/setLevel`.
    ///
    /// The spec leaves the pre-`setLevel` default to the server. `Info` keeps
    /// useful records flowing without drowning clients in `debug`.
    pub default_log_level: LogLevel,
    /// Where log records go when the client will not receive them
    /// (default: [`StderrLogging::Fallback`]).
    pub stderr_logging: StderrLogging,
    /// How long a server-initiated request waits for the client to answer
    /// (default: 2 minutes). `None` waits forever.
    ///
    /// Elicitation and sampling block the tool that started them, so a client
    /// that never answers holds that tool - and, on a single-threaded stdio
    /// server, the whole loop - open indefinitely. The spec asks senders to
    /// "establish timeouts for all sent requests, to prevent hung connections
    /// and resource exhaustion".
    ///
    /// Two minutes is chosen for the harder case: elicitation puts a form in
    /// front of a person, and people are slow. Sampling usually answers in
    /// seconds, so tune it per call with
    /// [`ToolEnv::send_request_with_timeout`] rather than lowering this.
    pub request_timeout: Option<std::time::Duration>,
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
            supported_versions: SUPPORTED_PROTOCOL_VERSIONS
                .iter()
                .map(|v| v.to_string())
                .collect(),
            default_log_level: LogLevel::Info,
            stderr_logging: StderrLogging::default(),
            request_timeout: Some(std::time::Duration::from_secs(120)),
        }
    }
}

/// Note the task an error response belongs to, in `data`.
///
/// Left alone when `data` is present but not an object: a task that failed with
/// structured diagnostics must still return "exactly what the underlying
/// request would have returned", and mangling that to make room is a worse
/// trade than omitting the annotation.
fn with_related_task_data(mut error: JsonRpcError, task_id: &str) -> JsonRpcError {
    let data = error
        .data
        .get_or_insert_with(|| Value::Object(serde_json::Map::new()));
    if let Some(object) = data.as_object_mut() {
        object.insert(
            RELATED_TASK.to_string(),
            serde_json::json!({ "taskId": task_id }),
        );
    }
    error
}

/// The message a panic carried, for reporting it as an error instead.
///
/// `panic!("text")` and `panic!("{fmt}")` produce a `&str` and a `String`
/// respectively; anything else is a custom payload nobody can render.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        return (*text).to_string();
    }
    if let Some(text) = payload.downcast_ref::<String>() {
        return text.clone();
    }
    "no message".to_string()
}

/// Is this read failure the peer sending something unreadable, rather than the
/// connection itself failing?
///
/// The distinction decides whether the session survives: a message we could not
/// parse gets an error response and the loop continues, while a broken socket
/// ends it.
fn is_malformed(error: &McpError) -> bool {
    matches!(error, McpError::Json(_) | McpError::InvalidMessage(_))
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

/// Parse a request's params where every field is optional.
///
/// The failure has to be `-32602`, not the `-32700` a bare `?` produced:
/// JSON-RPC reserves parse error for "invalid JSON was received by the server",
/// and a type-mismatched parameter in well-formed JSON is not that. The tools
/// page uses `-32602` for exactly this ("requests that fail to satisfy
/// CallToolRequest schema").
fn parse_optional_params<T: serde::de::DeserializeOwned + Default>(
    request: &JsonRpcRequest,
) -> Result<T> {
    match &request.params {
        Some(params) => serde_json::from_value(params.clone())
            .map_err(|e| McpError::InvalidParams(format!("Invalid params: {e}"))),
        None => Ok(T::default()),
    }
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
    /// An independent handle for writing, where the transport can be split.
    ///
    /// The read handle is held for the whole of a blocking `read`, so anything
    /// writing through it waits for the loop to wake - which, for a task worker
    /// wanting to elicit, is the very thing it is waiting to cause. A separate
    /// sink removes that circularity. Falls back to `transport` where splitting
    /// is not possible (HTTP), which is also where nothing needs it.
    writer: Option<Arc<Mutex<dyn Transport>>>,
    /// Whether the transport can be pumped while `tasks/result` blocks.
    ///
    /// Requires read deadlines: without them the pump could not notice the task
    /// finishing. False keeps task workers from making requests at all, which
    /// is the old behaviour and is deadlock-free.
    can_pump: bool,
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
    /// Workers held until the response announcing their task has been written.
    ///
    /// A worker writes through an independent handle on its own thread, so
    /// without this its first message - an elicitation, a log record, a status
    /// notification - can beat the `CreateTaskResult` onto the wire, and the
    /// client is told about a task it has never heard of.
    task_starts: Mutex<Vec<Arc<TaskGate>>>,
}

/// One-shot gate holding a task worker until its `CreateTaskResult` is out.
#[derive(Debug, Default)]
struct TaskGate {
    open: Mutex<bool>,
    opened: std::sync::Condvar,
}

impl TaskGate {
    /// Block until the gate opens, or give up after `limit`.
    ///
    /// Giving up rather than waiting forever: a caller that dispatches without
    /// ever writing (a test driving `dispatch_request` directly) would
    /// otherwise strand the worker.
    fn wait(&self, limit: std::time::Duration) {
        let deadline = std::time::Instant::now() + limit;
        let Ok(mut open) = self.open.lock() else {
            return;
        };
        while !*open {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return;
            }
            let Ok((guard, _)) = self.opened.wait_timeout(open, left) else {
                return;
            };
            open = guard;
        }
    }

    fn open(&self) {
        if let Ok(mut open) = self.open.lock() {
            *open = true;
        }
        self.opened.notify_all();
    }
}

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
            writer: None,
            can_pump: false,
            initialized: false,
            log_level,
            client_capabilities: ClientCapabilities::default(),
            broker: Arc::new(RequestBroker::new()),
            tasks: None,
            task_starts: Mutex::new(Vec::new()),
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
            transport: self.writer.as_ref().or(self.transport.as_ref()),
            reader: self.transport.as_ref(),
            resources: &self.resources,
            log_level: self.log_level,
            stderr_logging: self.config.stderr_logging,
            logger: &self.config.name,
            client_capabilities: &self.client_capabilities,
            broker: &self.broker,
            // This runs on the loop's own thread, so it can read for itself.
            mode: RequestMode::Direct,
            request_timeout: self.config.request_timeout,
            task: None,
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
        let mut transport = transport;

        // A second handle on the same sink, so a task worker can write while
        // this loop is parked inside `read` holding the read handle.
        let writer: Option<Arc<Mutex<dyn Transport>>> = transport
            .try_clone_writer()
            .map(|w| Arc::new(Mutex::new(w)) as Arc<Mutex<dyn Transport>>);

        // Deadlines are what let `tasks/result` pump instead of blocking, so
        // they decide whether a task may ask the client anything. Probing with
        // `None` reports support without arming anything.
        let honors_deadlines = transport.set_read_timeout(None).unwrap_or(false);

        let transport: Arc<Mutex<dyn Transport>> = Arc::new(Mutex::new(transport));
        self.can_pump = honors_deadlines && writer.is_some();
        self.writer = Some(writer.unwrap_or_else(|| transport.clone()));
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
                    // The guard is released before anything is written: on a
                    // transport that cannot be split, the writer is this very
                    // mutex.
                    let read = {
                        let mut t = transport
                            .lock()
                            .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
                        t.read()
                    };
                    match read {
                        Ok(msg) => msg,
                        Err(McpError::TransportClosed) => break,
                        // A deadline someone else armed and did not clear. The
                        // loop itself never sets one, so this is not our
                        // timeout to report; just read again.
                        Err(McpError::Timeout(_)) => continue,
                        // Garbage on the wire is the client's problem, not a
                        // reason to hang up on it. JSON-RPC wants an error
                        // response, with a null id when the id could not be
                        // read, and the session carries on.
                        Err(e) if is_malformed(&e) => {
                            self.write_message(&JsonRpcMessage::error(
                                RequestId::Null,
                                e.to_jsonrpc_error(),
                            ))?;
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                }
            };

            // Handle message
            if let Some(response) = self.handle_message(message, &mut context)? {
                self.write_message(&response)?;
            }
        }

        Ok(())
    }

    /// Send a message to the client.
    ///
    /// Everything the server writes goes through here, on one handle behind one
    /// lock. That matters now that task workers write too: two handles to the
    /// same socket are two *different* mutexes, so concurrent writes interleave
    /// and produce a line that is not valid JSON.
    fn write_message(&self, message: &JsonRpcMessage) -> Result<()> {
        let Some(writer) = self.writer.as_ref().or(self.transport.as_ref()) else {
            return Ok(());
        };
        let outcome = writer
            .lock()
            .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?
            .write(message);

        // Whatever we just wrote, any task started while handling this request
        // has now been announced, so its worker may speak.
        self.release_task_workers();
        outcome
    }

    /// Let every worker parked at its start gate run.
    fn release_task_workers(&self) {
        let Ok(mut gates) = self.task_starts.lock() else {
            return;
        };
        for gate in gates.drain(..) {
            gate.open();
        }
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
        let shared = transport.clone() as Arc<Mutex<dyn Transport>>;
        // One message in, one message out: there is no back-channel to split
        // and nothing to pump.
        self.writer = Some(shared.clone());
        self.can_pump = false;
        self.transport = Some(shared);

        // Read message
        let message = {
            let mut t = transport
                .lock()
                .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
            t.read()?
        };

        // Handle and write response
        if let Some(response) = self.handle_message(message, context)? {
            self.write_message(&response)?;
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
            // An answer to something *we* asked. Route it to whoever is
            // waiting - a task worker on another thread, or a parked slot for a
            // waiter further down this call stack. Dropping it here used to be
            // safe only because task workers could not ask anything.
            JsonRpcMessage::Response(response) => {
                self.broker.deliver(response);
                Ok(None)
            }
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

        match self.await_task(store, &params.task_id)? {
            // "For tasks in a terminal status, receivers MUST return from
            // tasks/result exactly what the underlying request would have
            // returned, whether that is a successful result or a JSON-RPC
            // error." The related-task metadata is required here specifically,
            // because the result shape carries no task id of its own.
            TaskOutcome::Value(mut value) => {
                if let Some(object) = value.as_object_mut() {
                    // Merged, not substituted. Overwriting `_meta` wholesale
                    // threw away whatever the tool put there, and "receivers
                    // MUST return from tasks/result exactly what the underlying
                    // request would have returned" - plus this key, not instead
                    // of the caller's metadata.
                    let meta = object
                        .entry("_meta")
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));
                    match meta.as_object_mut() {
                        Some(meta) => {
                            meta.insert(
                                RELATED_TASK.to_string(),
                                serde_json::json!({ "taskId": params.task_id }),
                            );
                        }
                        // A tool that put a non-object under `_meta` produced
                        // something no client can read anyway; the required key
                        // wins.
                        None => *meta = Value::Object(related_task_meta(&params.task_id)),
                    }
                }
                Ok(value)
            }
            // "The `tasks/result` operation MUST include this metadata in its
            // response, as the result structure itself does not contain the
            // task ID." An error response has no result object to carry it, so
            // it goes in `data` - which is where a client would look for
            // anything the error knows beyond its code and message.
            TaskOutcome::Error(error) => Err(McpError::Passthrough(with_related_task_data(
                error,
                &params.task_id,
            ))),
        }
    }

    /// Wait for a task to finish, keeping the transport moving while we do.
    ///
    /// The spec says `tasks/result` "**MUST** block the response until the task
    /// reaches a terminal status". Blocking on the store alone satisfies that
    /// but strands anything the task needs from the client: this thread is the
    /// only reader, and it is asleep. A task that asked for input would wait for
    /// an answer that could not arrive, and the client would wait for a result
    /// that would never come.
    ///
    /// So instead of sleeping, this reads. Client traffic is deferred to the
    /// main loop exactly as it is during any other server-initiated request,
    /// and responses reach the worker that is waiting on them. Reads are given a
    /// short deadline so the task finishing is noticed promptly - nothing on the
    /// wire announces it.
    ///
    /// Where the transport cannot do deadlines there is nothing to pump with,
    /// so this falls back to the plain block. That is safe because such a
    /// server also refuses task workers any server-initiated request.
    fn await_task(&self, store: &Arc<TaskStore>, task_id: &str) -> Result<TaskOutcome> {
        /// How long a pumped read waits before re-checking the task.
        const PUMP_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

        let (Some(transport), true) = (self.transport.as_ref(), self.can_pump) else {
            return store.await_result(task_id);
        };

        loop {
            if let Some(outcome) = store.try_result(task_id)? {
                return Ok(outcome);
            }

            let message = {
                let mut guard = transport
                    .lock()
                    .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;
                guard.set_read_timeout(Some(PUMP_INTERVAL))?;
                let message = guard.read();
                // Restored before anything else can read, so the main loop does
                // not inherit a deadline it never asked for.
                let _ = guard.set_read_timeout(None);
                message
            };

            match message {
                // Nothing arrived in this slice; look at the task again.
                Err(McpError::Timeout(_)) => continue,
                // The client hung up. Fall back to waiting on the task alone -
                // it is still running, and its result may still be wanted.
                Err(McpError::TransportClosed) => return store.await_result(task_id),
                // Unreadable input is the client's problem, not a reason to
                // abandon the task. Answer it and keep pumping.
                Err(e) if is_malformed(&e) => {
                    self.write_message(&JsonRpcMessage::error(
                        RequestId::Null,
                        e.to_jsonrpc_error(),
                    ))?;
                }
                Err(e) => return Err(e),
                Ok(JsonRpcMessage::Response(response)) => self.broker.deliver(response),
                Ok(JsonRpcMessage::Request(request)) => match self.answer_inline(&request) {
                    Some(response) => self.write_message(&response)?,
                    None => self.broker.defer(JsonRpcMessage::Request(request)),
                },
                Ok(other) => self.broker.defer(other),
            }
        }
    }

    /// Answer a request from inside the `tasks/result` pump, if it is one of
    /// the few that can safely be handled re-entrantly.
    ///
    /// Deferring everything is what made `tasks/result` a trap: deferred
    /// messages are only replayed by the main loop, which cannot run until
    /// `tasks/result` returns, so a client that followed the spec - "receivers
    /// **MUST** transition the task to `cancelled` status before sending the
    /// response", "requestors can continue polling via `tasks/get` in parallel"
    /// - was answered with silence. Worse, the flow the spec *prescribes* for
    ///   `input_required` (call `tasks/result`, then decide) is exactly the one
    ///   that trapped it.
    ///
    /// These four are `&self`, touch only the task store or nothing at all, and
    /// cannot re-enter this function. Everything else - including a second
    /// `tasks/result` - keeps being deferred.
    fn answer_inline(&self, request: &JsonRpcRequest) -> Option<JsonRpcMessage> {
        let outcome = match request.method.as_str() {
            "ping" => self.handle_ping(),
            "tasks/get" => self.handle_task_get(request),
            "tasks/list" => self.handle_task_list(request),
            "tasks/cancel" => self.handle_task_cancel(request),
            _ => return None,
        };

        Some(match outcome {
            Ok(result) => JsonRpcMessage::response(request.id.clone(), result),
            Err(e) => JsonRpcMessage::error(request.id.clone(), e.to_jsonrpc_error()),
        })
    }

    fn handle_task_list(&self, request: &JsonRpcRequest) -> Result<Value> {
        let store = self.task_store_or_unsupported()?;
        let params: ListTasksParams = parse_optional_params(request)?;

        let all = store.list()?;
        let state = PageState::from_cursor(params.cursor.as_deref(), self.config.page_size)?;

        // "Invalid or nonexistent cursor in `tasks/list`: -32602 (Invalid
        // params)" - a MUST, and one an expiring store can genuinely hit, since
        // a cursor issued a minute ago may now point past the end.
        if state.offset > 0 && state.offset >= all.len() {
            return Err(McpError::InvalidParams(format!(
                "Invalid cursor: no task at offset {}",
                state.offset
            )));
        }

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
        // Writes go through the split handle, so the worker never waits on the
        // server loop's blocking read to release the transport.
        let transport = self.writer.clone().or_else(|| self.transport.clone());
        let resources = self.resources.clone();
        let broker = self.broker.clone();
        let log_level = self.log_level;
        let stderr_logging = self.config.stderr_logging;
        let logger = self.config.name.clone();
        let request_timeout = self.config.request_timeout;
        let can_pump = self.can_pump;
        // A worker that may elicit needs to know what the client supports, or
        // `elicit` refuses before it ever reaches the broker.
        let client_capabilities = if can_pump {
            self.client_capabilities.clone()
        } else {
            ClientCapabilities::default()
        };
        let task_id = task.task_id.clone();

        // Held until this call's `CreateTaskResult` is on the wire. Only
        // installed when there is something to write through: a caller
        // dispatching without a transport has no response to wait for.
        let gate = transport.is_some().then(|| {
            let gate = Arc::new(TaskGate::default());
            if let Ok(mut gates) = self.task_starts.lock() {
                gates.push(gate.clone());
            }
            gate
        });

        // std::thread, not a runtime: the work really does run alongside the
        // server loop, so polling returns live answers.
        let spawned = std::thread::Builder::new()
            .name(format!("sml-task-{}", &task_id[..8.min(task_id.len())]))
            .spawn(move || {
                // The client cannot make sense of a message about a task it has
                // not been told about yet.
                if let Some(gate) = &gate {
                    gate.wait(std::time::Duration::from_secs(5));
                }

                let mut context = context_factory();
                let env = ToolEnv {
                    transport: transport.as_ref(),
                    // Nothing reads on this thread, so a Direct wait is never
                    // an option here.
                    reader: None,
                    resources: &resources,
                    log_level,
                    stderr_logging,
                    logger: &logger,
                    client_capabilities: &client_capabilities,
                    broker: &broker,
                    // Delegated where the transport can be pumped: the worker
                    // writes its own request and waits for whichever thread is
                    // reading to hand the answer over. Where it cannot be
                    // pumped, asking would deadlock against `tasks/result`, so
                    // it is refused outright.
                    mode: if can_pump {
                        RequestMode::Delegated
                    } else {
                        RequestMode::Forbidden
                    },
                    request_timeout,
                    task: Some(TaskEnv {
                        store: &store,
                        task_id: &task_id,
                        cancelled: &cancelled,
                    }),
                };

                // A panic here used to unwind the thread with `finish` never
                // reached: the task sat in `working` until its TTL, `tasks/get`
                // reported `working` forever, and `tasks/result` - which runs
                // on the server loop thread - blocked indefinitely, taking
                // every other request down with it. A panic is a failed task,
                // not a wedged server.
                let executed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    match tool.execute(arguments, &mut context, &env) {
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
                        Err(McpError::ToolError(message))
                        | Err(McpError::InvalidParams(message)) => {
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
                    }
                }));

                let (status, outcome) = match executed {
                    Ok(finished) => finished,
                    Err(payload) => (
                        TaskStatus::Failed,
                        TaskOutcome::Error(JsonRpcError::internal_error(format!(
                            "task worker panicked: {}",
                            panic_message(payload.as_ref())
                        ))),
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

        // "If the server supports the requested protocol version, it MUST
        // respond with the same version. Otherwise, the server MUST respond
        // with another protocol version it supports [...] the latest."
        let negotiated = self
            .config
            .supported_versions
            .iter()
            .find(|version| **version == params.protocol_version)
            .cloned()
            .unwrap_or_else(|| {
                self.config
                    .supported_versions
                    .first()
                    .cloned()
                    .unwrap_or_else(|| PROTOCOL_VERSION.to_string())
            });

        let result = InitializeResult {
            protocol_version: negotiated,
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
        let params: ListToolsParams = parse_optional_params(request)?;

        // Collect all tools (sorted for consistent pagination)
        let mut all_tools: Vec<crate::types::Tool> =
            self.tools.values().map(|t| t.as_protocol_tool()).collect();
        all_tools.sort_by(|a, b| a.name.cmp(&b.name));

        // Apply pagination
        let state = PageState::from_cursor(params.cursor.as_deref(), self.config.page_size)?;
        let (tools, next_cursor) = paginate(&all_tools, &state);

        Ok(serde_json::to_value(ListToolsResult {
            tools,
            next_cursor,
        })?)
    }

    fn handle_call_tool(&mut self, request: &JsonRpcRequest, context: &mut C) -> Result<Value> {
        let params: CallToolParams = parse_params(request)?;

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

        // A panicking tool is a broken tool, not a broken server. Unwinding
        // from here kills the process on stdio, the connection thread on a
        // Unix daemon, and the whole accept loop on HTTP.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tool.execute(
                params.arguments.unwrap_or(serde_json::json!({})),
                context,
                &env,
            )
        }))
        .unwrap_or_else(|payload| {
            Err(McpError::Internal(format!(
                "tool `{}` panicked: {}",
                params.name,
                panic_message(payload.as_ref())
            )))
        });

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
        let params: ListResourcesParams = parse_optional_params(request)?;

        // Collect all resources (sorted for consistent pagination)
        let mut all_resources: Vec<crate::types::Resource> = self
            .resources
            .values()
            .map(|r| r.as_protocol_resource())
            .collect();
        all_resources.sort_by(|a, b| a.uri.cmp(&b.uri));

        // Apply pagination
        let state = PageState::from_cursor(params.cursor.as_deref(), self.config.page_size)?;
        let (resources, next_cursor) = paginate(&all_resources, &state);

        Ok(serde_json::to_value(ListResourcesResult {
            resources,
            next_cursor,
        })?)
    }

    fn handle_list_resource_templates(&self, request: &JsonRpcRequest) -> Result<Value> {
        let params: ListResourceTemplatesParams = parse_optional_params(request)?;

        // Sorted for stable pagination, same as the other list handlers.
        let mut all: Vec<ResourceTemplate> = self.resource_templates.clone();
        all.sort_by(|a, b| a.uri_template.cmp(&b.uri_template));

        let state = PageState::from_cursor(params.cursor.as_deref(), self.config.page_size)?;
        let (resource_templates, next_cursor) = paginate(&all, &state);

        Ok(serde_json::to_value(ListResourceTemplatesResult {
            resource_templates,
            next_cursor,
        })?)
    }

    fn handle_read_resource(&self, request: &JsonRpcRequest) -> Result<Value> {
        let params: ReadResourceParams = parse_params(request)?;

        let resource = self
            .resources
            .get(&params.uri)
            .ok_or_else(|| McpError::ResourceNotFound(params.uri.clone()))?;

        let contents = resource.content();
        Ok(serde_json::to_value(ReadResourceResult { contents })?)
    }

    fn handle_list_prompts(&self, request: &JsonRpcRequest) -> Result<Value> {
        let params: ListPromptsParams = parse_optional_params(request)?;

        // Collect all prompts (sorted for consistent pagination)
        let mut all_prompts: Vec<Prompt> = self
            .prompts
            .values()
            .map(|p| p.as_protocol_prompt())
            .collect();
        all_prompts.sort_by(|a, b| a.name.cmp(&b.name));

        // Apply pagination
        let state = PageState::from_cursor(params.cursor.as_deref(), self.config.page_size)?;
        let (prompts, next_cursor) = paginate(&all_prompts, &state);

        Ok(serde_json::to_value(ListPromptsResult {
            prompts,
            next_cursor,
        })?)
    }

    fn handle_get_prompt(&self, request: &JsonRpcRequest) -> Result<Value> {
        let params: GetPromptParams = parse_params(request)?;

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
            reader: transport,
            resources,
            log_level,
            stderr_logging,
            logger: "test-logger",
            client_capabilities: &NO_CAPABILITIES,
            broker: &TEST_BROKER,
            mode: RequestMode::Direct,
            request_timeout: None,
            task: None,
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

    //
    // Server-initiated request timeouts
    //

    /// What a scripted client does on each read.
    enum Step {
        /// Say nothing until the deadline runs out.
        Silence,
        /// Answer the `n`th request the server wrote (0-based).
        Answer(usize, Value),
        /// Hang up.
        Close,
    }

    /// A client whose every read is scripted, and which honors deadlines.
    struct TimeoutTransport {
        steps: VecDeque<Step>,
        written: Arc<Mutex<Vec<JsonRpcMessage>>>,
        /// The deadline currently in force.
        timeout: Arc<Mutex<Option<std::time::Duration>>>,
        /// Every deadline this transport was handed, in order.
        seen: Arc<Mutex<Vec<Option<std::time::Duration>>>>,
        /// Whether to claim deadline support.
        honors_timeouts: bool,
    }

    impl TimeoutTransport {
        fn new(steps: Vec<Step>) -> Self {
            Self {
                steps: steps.into(),
                written: Arc::new(Mutex::new(Vec::new())),
                timeout: Arc::new(Mutex::new(None)),
                seen: Arc::new(Mutex::new(Vec::new())),
                honors_timeouts: true,
            }
        }

        /// A transport that ignores deadlines, like HTTP or a custom one that
        /// never implemented them.
        fn without_timeout_support(mut self) -> Self {
            self.honors_timeouts = false;
            self
        }

        fn request_id(&self, index: usize) -> RequestId {
            self.written
                .lock()
                .unwrap()
                .iter()
                .filter_map(|m| match m {
                    JsonRpcMessage::Request(r) => Some(r.id.clone()),
                    _ => None,
                })
                .nth(index)
                .expect("no such request")
        }
    }

    impl Transport for TimeoutTransport {
        fn read(&mut self) -> Result<JsonRpcMessage> {
            match self.steps.pop_front() {
                Some(Step::Silence) | None => {
                    let timeout = *self.timeout.lock().unwrap();
                    match timeout {
                        Some(t) => {
                            std::thread::sleep(t);
                            Err(McpError::Timeout(format!("no message within {t:?}")))
                        }
                        // No deadline and nothing to say. A real transport
                        // would block here; a test must not.
                        None => Err(McpError::TransportClosed),
                    }
                }
                Some(Step::Answer(index, result)) => {
                    Ok(JsonRpcMessage::Response(JsonRpcResponse {
                        jsonrpc: Default::default(),
                        id: self.request_id(index),
                        result: Some(result),
                        error: None,
                    }))
                }
                Some(Step::Close) => Err(McpError::TransportClosed),
            }
        }

        fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
            self.written.lock().unwrap().push(message.clone());
            Ok(())
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }

        fn set_read_timeout(&mut self, timeout: Option<std::time::Duration>) -> Result<bool> {
            self.seen.lock().unwrap().push(timeout);
            if self.honors_timeouts {
                *self.timeout.lock().unwrap() = timeout;
            }
            Ok(self.honors_timeouts)
        }
    }

    /// A server wired to a `TimeoutTransport`, plus the handles a test needs to
    /// see what the transport was told.
    struct TimeoutFixture {
        server: Server<TestContext>,
        /// Everything the server wrote.
        written: Arc<Mutex<Vec<JsonRpcMessage>>>,
        /// The deadline currently in force on the transport.
        timeout: Arc<Mutex<Option<std::time::Duration>>>,
        /// Every deadline the transport was handed, in order.
        seen: Arc<Mutex<Vec<Option<std::time::Duration>>>>,
    }

    /// Wire a `TimeoutTransport` to a server with elicitation negotiated.
    fn timeout_server(transport: TimeoutTransport) -> TimeoutFixture {
        let written = transport.written.clone();
        let timeout = transport.timeout.clone();
        let seen = transport.seen.clone();

        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.transport = Some(Arc::new(Mutex::new(transport)));
        server.client_capabilities =
            serde_json::from_value(serde_json::json!({ "elicitation": { "form": {} } })).unwrap();

        TimeoutFixture {
            server,
            written,
            timeout,
            seen,
        }
    }

    #[test]
    fn test_the_default_request_timeout_is_two_minutes() {
        assert_eq!(
            ServerConfig::default().request_timeout,
            Some(std::time::Duration::from_secs(120))
        );
    }

    #[test]
    fn test_a_silent_client_times_out_instead_of_stalling_the_tool() {
        let TimeoutFixture { server, .. } =
            timeout_server(TimeoutTransport::new(vec![Step::Silence]));

        let env = server.tool_env();
        let started = std::time::Instant::now();
        let result = env.send_request_with_timeout(
            "elicitation/create",
            serde_json::json!({}),
            Some(std::time::Duration::from_millis(50)),
        );

        assert!(matches!(result, Err(McpError::Timeout(_))), "{result:?}");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_a_timeout_tells_the_client_to_stop() {
        // The spec asks a party that times out to issue a cancellation, so the
        // peer is not left working on an answer nobody wants.
        let TimeoutFixture {
            server, written, ..
        } = timeout_server(TimeoutTransport::new(vec![Step::Silence]));

        let env = server.tool_env();
        let _ = env.send_request_with_timeout(
            "elicitation/create",
            serde_json::json!({}),
            Some(std::time::Duration::from_millis(30)),
        );

        let request_id = written
            .lock()
            .unwrap()
            .iter()
            .find_map(|m| match m {
                JsonRpcMessage::Request(r) => Some(r.id.clone()),
                _ => None,
            })
            .expect("a request should have gone out");

        let cancelled = written
            .lock()
            .unwrap()
            .iter()
            .find_map(|m| match m {
                JsonRpcMessage::Notification(n) if n.method == "notifications/cancelled" => {
                    Some(n.params.clone())
                }
                _ => None,
            })
            .expect("a cancellation should follow the timeout");

        let params = cancelled.expect("cancellation carries params");
        assert_eq!(
            params["requestId"],
            serde_json::to_value(&request_id).unwrap()
        );
        assert!(params["reason"].as_str().unwrap().contains("30ms"));
    }

    #[test]
    fn test_a_client_that_hangs_up_is_a_close_not_a_timeout() {
        // A disconnect is final; retrying would be pointless, and the caller
        // needs to be able to tell the two apart.
        let TimeoutFixture { server, .. } =
            timeout_server(TimeoutTransport::new(vec![Step::Close]));

        let env = server.tool_env();
        let result = env.send_request_with_timeout(
            "elicitation/create",
            serde_json::json!({}),
            Some(std::time::Duration::from_secs(30)),
        );

        assert!(
            matches!(result, Err(McpError::TransportClosed)),
            "{result:?}"
        );
    }

    #[test]
    fn test_a_timeout_leaves_the_transport_blocking_again() {
        // The server loop reads next, and it never asked for a deadline. One
        // left armed would turn its idle wait into a spurious failure.
        let TimeoutFixture {
            server, timeout, ..
        } = timeout_server(TimeoutTransport::new(vec![Step::Silence]));

        let env = server.tool_env();
        let _ = env.send_request_with_timeout(
            "elicitation/create",
            serde_json::json!({}),
            Some(std::time::Duration::from_millis(20)),
        );

        assert_eq!(*timeout.lock().unwrap(), None);
    }

    #[test]
    fn test_a_successful_request_also_clears_the_deadline() {
        let TimeoutFixture {
            server, timeout, ..
        } = timeout_server(TimeoutTransport::new(vec![Step::Answer(
            0,
            serde_json::json!({ "action": "decline" }),
        )]));

        let env = server.tool_env();
        env.send_request_with_timeout(
            "elicitation/create",
            serde_json::json!({}),
            Some(std::time::Duration::from_secs(30)),
        )
        .unwrap();

        assert_eq!(*timeout.lock().unwrap(), None);
    }

    #[test]
    fn test_a_late_answer_is_not_mistaken_for_the_next_one() {
        // The first request times out, then its answer finally arrives while a
        // second request is outstanding. Handing that stale answer to the
        // second caller would be silent data corruption.
        let TimeoutFixture { server, .. } = timeout_server(TimeoutTransport::new(vec![
            Step::Silence,
            // The late answer to request 0, then the real answer to request 1.
            Step::Answer(
                0,
                serde_json::json!({ "action": "accept", "content": { "n": 1 } }),
            ),
            Step::Answer(
                1,
                serde_json::json!({ "action": "accept", "content": { "n": 2 } }),
            ),
        ]));

        let env = server.tool_env();
        assert!(matches!(
            env.send_request_with_timeout(
                "elicitation/create",
                serde_json::json!({}),
                Some(std::time::Duration::from_millis(20)),
            ),
            Err(McpError::Timeout(_))
        ));

        let second = env
            .send_request_with_timeout(
                "elicitation/create",
                serde_json::json!({}),
                Some(std::time::Duration::from_secs(30)),
            )
            .unwrap();

        assert_eq!(second["content"]["n"], 2, "got the abandoned answer");
    }

    #[test]
    fn test_no_timeout_means_no_deadline_on_the_transport() {
        let TimeoutFixture { server, seen, .. } =
            timeout_server(TimeoutTransport::new(vec![Step::Answer(
                0,
                serde_json::json!({ "action": "cancel" }),
            )]));

        let env = server.tool_env();
        env.send_request_with_timeout("elicitation/create", serde_json::json!({}), None)
            .unwrap();

        assert!(
            seen.lock().unwrap().iter().all(|t| t.is_none()),
            "an untimed request must not arm a deadline"
        );
    }

    #[test]
    fn test_the_budget_shrinks_across_unrelated_messages() {
        // Traffic for someone else must not keep resetting the wait, or a busy
        // session would never time out.
        let TimeoutFixture { server, seen, .. } = timeout_server(TimeoutTransport::new(vec![
            Step::Answer(0, serde_json::json!({ "not": "ours" })),
            Step::Silence,
        ]));

        // Answer(0) hands back a response with request 0's id, which *is* ours,
        // so use a second request to make the first answer land elsewhere.
        let env = server.tool_env();
        let _ = env.send_request_with_timeout(
            "elicitation/create",
            serde_json::json!({}),
            Some(std::time::Duration::from_millis(60)),
        );

        let budgets: Vec<_> = seen.lock().unwrap().iter().filter_map(|t| *t).collect();
        assert!(!budgets.is_empty(), "a deadline should have been armed");
        for budget in &budgets {
            assert!(
                *budget <= std::time::Duration::from_millis(60),
                "budget {budget:?} exceeds what was asked for"
            );
        }
    }

    #[test]
    fn test_a_transport_without_deadline_support_still_reports_a_timeout() {
        // It cannot interrupt a read in progress, but the wait between messages
        // is still bounded - and the caller was told support was missing.
        let transport =
            TimeoutTransport::new(vec![Step::Answer(0, serde_json::json!({ "ok": true }))])
                .without_timeout_support();
        let TimeoutFixture { server, .. } = timeout_server(transport);

        let env = server.tool_env();
        // Zero budget: the loop's own check fires before any read happens.
        let result = env.send_request_with_timeout(
            "elicitation/create",
            serde_json::json!({}),
            Some(std::time::Duration::ZERO),
        );
        assert!(matches!(result, Err(McpError::Timeout(_))), "{result:?}");
    }

    #[test]
    fn test_elicit_uses_the_configured_timeout() {
        let TimeoutFixture {
            mut server, seen, ..
        } = timeout_server(TimeoutTransport::new(vec![Step::Silence]));
        server.config.request_timeout = Some(std::time::Duration::from_millis(40));

        let env = server.tool_env();
        let result = env.elicit_form("pick one", ElicitSchema::new().build());

        assert!(matches!(result, Err(McpError::Timeout(_))), "{result:?}");
        assert!(
            seen.lock()
                .unwrap()
                .iter()
                .any(|t| matches!(t, Some(d) if *d <= std::time::Duration::from_millis(40))),
            "elicit should inherit ServerConfig::request_timeout"
        );
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

    /// Tool that panics, to prove a broken tool cannot wedge the server.
    struct PanickingTool;

    impl Tool<TestContext> for PanickingTool {
        fn name(&self) -> &str {
            "panics"
        }
        fn description(&self) -> &str {
            "Panics on purpose"
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
            panic!("tool exploded");
        }
    }

    #[test]
    fn test_a_panicking_task_worker_fails_the_task_instead_of_wedging_it() {
        // Before: the worker thread unwound, `store.finish` was never reached,
        // the task sat in `working` until its TTL, and `tasks/result` - handled
        // on the server loop thread - blocked forever, so the server stopped
        // answering everything else too.
        let mut server = task_server();
        server.add_tool(PanickingTool).unwrap();

        let created = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "panics", "task": {} }),
        )
        .unwrap();
        let task_id = created["task"]["taskId"].as_str().unwrap().to_string();

        let task = await_terminal(&mut server, &task_id);
        assert_eq!(task["status"], "failed");
        assert!(
            task["statusMessage"]
                .as_str()
                .unwrap()
                .contains("tool exploded"),
            "{task}"
        );

        // And the result is retrievable rather than blocking forever.
        let err = dispatch(
            &mut server,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        )
        .unwrap_err();
        let error = err.to_jsonrpc_error();
        assert_eq!(error.code, -32603);
        assert!(error.message.contains("panicked"), "{}", error.message);
    }

    #[test]
    fn test_a_panicking_tool_is_an_internal_error_not_a_crash() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        server.add_tool(PanickingTool).unwrap();
        server.add_tool(IncrementTool).unwrap();

        let err = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "panics" }),
        )
        .unwrap_err();
        assert_eq!(err.to_jsonrpc_error().code, -32603);
        assert!(err.to_string().contains("panicked"), "{err}");

        // The server is still usable afterwards.
        let ok = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "increment" }),
        )
        .unwrap();
        assert_eq!(ok["content"][0]["text"], "Counter is now: 1");
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

    /// A task-callable tool that puts its own key in the result's `_meta`.
    struct TracingTool;

    impl Tool<TestContext> for TracingTool {
        fn name(&self) -> &str {
            "tracing"
        }
        fn description(&self) -> &str {
            "Returns metadata of its own"
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
            Ok(CallToolResult::text("done")
                .with_meta_key("com.example/trace", serde_json::json!("keep-me")))
        }
    }

    #[test]
    fn test_task_result_adds_related_task_without_destroying_tool_meta() {
        // "Receivers MUST return from tasks/result exactly what the underlying
        // request would have returned." Overwriting `_meta` wholesale deleted
        // whatever the tool put there.
        let mut server = task_server();
        server.add_tool(TracingTool).unwrap();

        let created = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "tracing", "task": {} }),
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

        assert_eq!(result["_meta"]["com.example/trace"], "keep-me", "{result}");
        assert_eq!(
            result["_meta"][crate::tasks::RELATED_TASK]["taskId"],
            task_id.as_str()
        );
    }

    #[test]
    fn test_task_result_errors_name_their_task_too() {
        let mut server = task_server();
        server.add_tool(PanickingTool).unwrap();

        let created = dispatch(
            &mut server,
            "tools/call",
            serde_json::json!({ "name": "panics", "task": {} }),
        )
        .unwrap();
        let task_id = created["task"]["taskId"].as_str().unwrap().to_string();
        await_terminal(&mut server, &task_id);

        let error = dispatch(
            &mut server,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        )
        .unwrap_err()
        .to_jsonrpc_error();

        assert_eq!(
            error.data.expect("error data")[crate::tasks::RELATED_TASK]["taskId"],
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
    fn test_task_worker_cannot_make_server_initiated_requests_without_a_pump() {
        // Delegating a request needs someone reading the transport, and that
        // needs read deadlines so `tasks/result` can pump instead of sleeping.
        // This server has no transport at all, so the round trip must fail fast
        // rather than deadlock.
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

    //
    // input_required: a task asking the client something
    //

    /// A task-callable tool that elicits once and reports what came back.
    #[cfg(unix)]
    struct AskingTool;

    #[cfg(unix)]
    impl Tool<TestContext> for AskingTool {
        fn name(&self) -> &str {
            "asks"
        }
        fn description(&self) -> &str {
            "Elicits from inside a task"
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
            let result = env.elicit_form("your name?", ElicitSchema::new().build())?;
            let name = result
                .accepted_content()
                .and_then(|content| content.get("name").cloned())
                .and_then(|value| value.as_str().map(String::from))
                .unwrap_or_else(|| "<none>".to_string());
            Ok(CallToolResult::text(format!("hello {name}")))
        }
    }

    /// A task-callable tool that logs and reports progress, so the `_meta` on
    /// task-related *notifications* can be inspected on the wire.
    #[cfg(unix)]
    struct ChattyTool;

    #[cfg(unix)]
    impl Tool<TestContext> for ChattyTool {
        fn name(&self) -> &str {
            "chatty"
        }
        fn description(&self) -> &str {
            "Logs from inside a task"
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
            env.log(LogLevel::Info, "working on it")?;
            env.send_progress("tok", 0.5, Some(1.0))?;
            Ok(CallToolResult::text("done"))
        }
    }

    /// A task-enabled server running over one end of a socket pair.
    ///
    /// Returns the client end. The server thread ends when the client drops.
    #[cfg(unix)]
    fn task_server_on_a_socket() -> (crate::transport::UnixTransport, std::thread::JoinHandle<()>) {
        use crate::transport::UnixTransport;
        use std::os::unix::net::UnixStream;

        let (server_end, client_end) = UnixStream::pair().unwrap();
        let handle = std::thread::spawn(move || {
            let mut server = task_server();
            server.add_tool(AskingTool).unwrap();
            server.add_tool(ChattyTool).unwrap();
            let _ = server.start(
                UnixTransport::from_stream(server_end),
                TestContext { counter: 0 },
            );
        });

        let mut client = UnixTransport::from_stream(client_end);
        // Every read in these tests is bounded, so a regression fails the test
        // instead of hanging the suite.
        client
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        (client, handle)
    }

    #[cfg(unix)]
    fn send(client: &mut crate::transport::UnixTransport, id: i64, method: &str, params: Value) {
        client
            .write(&JsonRpcMessage::request(id, method, Some(params)))
            .unwrap();
    }

    /// Read until a response to `id` arrives, handing anything else to
    /// `on_other`.
    ///
    /// A finishing task announces itself with `notifications/tasks/status`,
    /// which can land at any point and is never what a caller is waiting for,
    /// so it is absorbed here rather than in every handler.
    #[cfg(unix)]
    fn read_until_response(
        client: &mut crate::transport::UnixTransport,
        id: i64,
        mut on_other: impl FnMut(&mut crate::transport::UnixTransport, JsonRpcMessage),
    ) -> JsonRpcResponse {
        for _ in 0..100 {
            match client.read().expect("client read") {
                JsonRpcMessage::Response(r) if r.id == RequestId::Number(id) => return r,
                JsonRpcMessage::Notification(n) if n.method == "notifications/tasks/status" => {}
                other => on_other(client, other),
            }
        }
        panic!("no response to request {id}");
    }

    #[cfg(unix)]
    fn initialize_with_elicitation(client: &mut crate::transport::UnixTransport) {
        send(
            client,
            1,
            "initialize",
            serde_json::json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "elicitation": { "form": {} } },
                "clientInfo": { "name": "test", "version": "1.0" }
            }),
        );
        read_until_response(client, 1, |_, other| panic!("unexpected {other:?}"));
    }

    #[cfg(unix)]
    #[test]
    fn test_a_task_can_elicit_and_reaches_input_required() {
        let (mut client, _server) = task_server_on_a_socket();
        initialize_with_elicitation(&mut client);

        send(
            &mut client,
            2,
            "tools/call",
            serde_json::json!({ "name": "asks", "task": {} }),
        );
        let created =
            read_until_response(&mut client, 2, |_, other| panic!("unexpected {other:?}"));
        let task_id = created.result.unwrap()["task"]["taskId"]
            .as_str()
            .unwrap()
            .to_string();

        // The worker's elicitation should arrive without anyone prompting it.
        let elicitation = loop {
            match client.read().expect("client read") {
                JsonRpcMessage::Request(r) if r.method == "elicitation/create" => break r,
                JsonRpcMessage::Notification(n) if n.method == "notifications/tasks/status" => {}
                other => panic!("expected an elicitation, got {other:?}"),
            }
        };

        // While it is outstanding, the task reports that it is waiting on us.
        send(
            &mut client,
            3,
            "tasks/get",
            serde_json::json!({ "taskId": task_id }),
        );
        let status = read_until_response(&mut client, 3, |_, other| panic!("unexpected {other:?}"));
        assert_eq!(
            status.result.unwrap()["status"],
            "input_required",
            "a task blocked on elicitation should say so"
        );

        // Answer, and the task should finish with what we said.
        client
            .write(&JsonRpcMessage::response(
                elicitation.id,
                serde_json::json!({ "action": "accept", "content": { "name": "octocat" } }),
            ))
            .unwrap();

        send(
            &mut client,
            4,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        );
        let result = read_until_response(&mut client, 4, |_, other| panic!("unexpected {other:?}"));
        assert_eq!(
            result.result.unwrap()["content"][0]["text"],
            "hello octocat"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_tasks_result_pumps_so_an_eliciting_task_can_finish() {
        // The deadlock this exists to prevent: `tasks/result` blocks, so the
        // only reader is asleep; the task asks the client something; the answer
        // can never be read; the task never finishes; `tasks/result` never
        // returns. Both sides wait forever.
        //
        // `tasks/result` therefore reads while it waits, which is what lets the
        // elicitation below get through at all.
        let (mut client, _server) = task_server_on_a_socket();
        initialize_with_elicitation(&mut client);

        send(
            &mut client,
            2,
            "tools/call",
            serde_json::json!({ "name": "asks", "task": {} }),
        );
        let created =
            read_until_response(&mut client, 2, |_, other| panic!("unexpected {other:?}"));
        let task_id = created.result.unwrap()["task"]["taskId"]
            .as_str()
            .unwrap()
            .to_string();

        // Ask for the result immediately, before answering anything.
        send(
            &mut client,
            3,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        );

        // The elicitation must still reach us, and answering it must let
        // `tasks/result` complete.
        let result = read_until_response(&mut client, 3, |client, other| match other {
            JsonRpcMessage::Request(r) if r.method == "elicitation/create" => {
                client
                    .write(&JsonRpcMessage::response(
                        r.id,
                        serde_json::json!({ "action": "accept", "content": { "name": "nobody" } }),
                    ))
                    .unwrap();
            }
            other => panic!("unexpected {other:?}"),
        });

        assert_eq!(result.result.unwrap()["content"][0]["text"], "hello nobody");
    }

    #[cfg(unix)]
    #[test]
    fn test_a_declined_elicitation_still_finishes_the_task() {
        // Declining is an answer. The task must come back to `working` and run
        // to completion rather than sitting in `input_required`.
        let (mut client, _server) = task_server_on_a_socket();
        initialize_with_elicitation(&mut client);

        send(
            &mut client,
            2,
            "tools/call",
            serde_json::json!({ "name": "asks", "task": {} }),
        );
        let created =
            read_until_response(&mut client, 2, |_, other| panic!("unexpected {other:?}"));
        let task_id = created.result.unwrap()["task"]["taskId"]
            .as_str()
            .unwrap()
            .to_string();

        send(
            &mut client,
            3,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        );

        let result = read_until_response(&mut client, 3, |client, other| match other {
            JsonRpcMessage::Request(r) if r.method == "elicitation/create" => {
                client
                    .write(&JsonRpcMessage::response(
                        r.id,
                        serde_json::json!({ "action": "decline" }),
                    ))
                    .unwrap();
            }
            other => panic!("unexpected {other:?}"),
        });

        // `accepted_content` withholds content on anything but accept, so the
        // tool sees no name.
        assert_eq!(result.result.unwrap()["content"][0]["text"], "hello <none>");
    }

    /// Drive the flow the spec prescribes for `input_required`: start a task,
    /// let it elicit, then open `tasks/result` without answering. Returns the
    /// client and the task id, with the elicitation still outstanding.
    #[cfg(unix)]
    fn task_blocked_on_input(
        client: &mut crate::transport::UnixTransport,
    ) -> (String, JsonRpcRequest) {
        initialize_with_elicitation(client);

        send(
            client,
            2,
            "tools/call",
            serde_json::json!({ "name": "asks", "task": {} }),
        );
        let created = read_until_response(client, 2, |_, other| panic!("unexpected {other:?}"));
        let task_id = created.result.unwrap()["task"]["taskId"]
            .as_str()
            .unwrap()
            .to_string();

        let elicitation = loop {
            match client.read().expect("client read") {
                JsonRpcMessage::Request(r) if r.method == "elicitation/create" => break r,
                JsonRpcMessage::Notification(n) if n.method == "notifications/tasks/status" => {}
                other => panic!("expected an elicitation, got {other:?}"),
            }
        };

        (task_id, elicitation)
    }

    /// The task id a message claims to be related to, if it says.
    #[cfg(unix)]
    fn related_task_of(params: Option<&Value>) -> Option<String> {
        params?
            .get("_meta")?
            .get(crate::tasks::RELATED_TASK)?
            .get("taskId")?
            .as_str()
            .map(String::from)
    }

    #[cfg(unix)]
    #[test]
    fn test_a_task_elicitation_carries_related_task_meta() {
        // "The receiver MUST include the `io.modelcontextprotocol/related-task`
        // metadata in the request to associate it with the task." Without it
        // the client, which is sitting in `tasks/result` for this very task,
        // cannot tell what the elicitation belongs to.
        let (mut client, _server) = task_server_on_a_socket();
        let (task_id, elicitation) = task_blocked_on_input(&mut client);

        assert_eq!(
            related_task_of(elicitation.params.as_ref()).as_deref(),
            Some(task_id.as_str()),
            "elicitation params: {:?}",
            elicitation.params
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_task_notifications_carry_related_task_meta_except_status() {
        let (mut client, _server) = task_server_on_a_socket();
        initialize_with_elicitation(&mut client);

        send(
            &mut client,
            2,
            "tools/call",
            serde_json::json!({ "name": "chatty", "task": {} }),
        );

        let mut logs = 0;
        let mut progress = 0;
        let mut status = 0;

        // Read until the task announces it finished, checking everything on
        // the way.
        for _ in 0..20 {
            match client.read().expect("client read") {
                JsonRpcMessage::Response(_) => {}
                JsonRpcMessage::Request(r) => panic!("unexpected request {r:?}"),
                JsonRpcMessage::Notification(n) => {
                    let related = related_task_of(n.params.as_ref());
                    match n.method.as_str() {
                        // "Task status notifications SHOULD NOT include the
                        // `io.modelcontextprotocol/related-task` metadata" -
                        // the params already carry the whole task.
                        "notifications/tasks/status" => {
                            assert_eq!(related, None, "status params: {:?}", n.params);
                            status += 1;
                            break;
                        }
                        "notifications/message" => {
                            assert!(related.is_some(), "log params: {:?}", n.params);
                            logs += 1;
                        }
                        "notifications/progress" => {
                            assert!(related.is_some(), "progress params: {:?}", n.params);
                            progress += 1;
                        }
                        other => panic!("unexpected notification {other}"),
                    }
                }
            }
        }

        assert_eq!((logs, progress, status), (1, 1, 1));
    }

    #[cfg(unix)]
    #[test]
    fn test_a_worker_never_speaks_before_its_task_is_announced() {
        // The worker writes through an independent handle on its own thread, so
        // without a start gate its elicitation can win the race to the writer
        // and the client is told about a task it has never heard of. Repeated
        // because losing that race is a matter of scheduling luck.
        for _ in 0..5 {
            let (mut client, _server) = task_server_on_a_socket();
            initialize_with_elicitation(&mut client);

            send(
                &mut client,
                2,
                "tools/call",
                serde_json::json!({ "name": "asks", "task": {} }),
            );

            match client.read().expect("client read") {
                JsonRpcMessage::Response(r) => assert_eq!(r.id, RequestId::Number(2)),
                other => panic!("the task must be announced first, got {other:?}"),
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_tasks_cancel_is_answered_while_tasks_result_blocks() {
        // The deadlock: the client is told to call `tasks/result` on
        // `input_required`, then decides to cancel instead. Every request sent
        // during `tasks/result` used to be deferred to a main loop that could
        // not run until `tasks/result` returned - which it never would.
        let (mut client, _server) = task_server_on_a_socket();
        let (task_id, _elicitation) = task_blocked_on_input(&mut client);

        send(
            &mut client,
            3,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        );
        send(
            &mut client,
            4,
            "tasks/cancel",
            serde_json::json!({ "taskId": task_id }),
        );

        // "Upon receiving a valid cancellation request, receivers ... MUST
        // transition the task to `cancelled` status before sending the
        // response."
        let mut seen = Vec::new();
        let cancelled = read_until_response(&mut client, 4, |_, other| seen.push(other));
        assert_eq!(cancelled.result.unwrap()["status"], "cancelled");

        // And the blocked `tasks/result` unblocks with the cancellation.
        let result = read_until_response(&mut client, 3, |_, other| seen.push(other));
        assert!(result.error.is_some(), "{result:?}");
    }

    #[cfg(unix)]
    #[test]
    fn test_tasks_get_and_ping_are_answered_while_tasks_result_blocks() {
        // "While `tasks/result` blocks until the task reaches a terminal
        // status, requestors can continue polling via `tasks/get` in parallel."
        let (mut client, _server) = task_server_on_a_socket();
        let (task_id, elicitation) = task_blocked_on_input(&mut client);

        send(
            &mut client,
            3,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        );
        send(
            &mut client,
            4,
            "tasks/get",
            serde_json::json!({ "taskId": task_id }),
        );
        send(&mut client, 5, "ping", serde_json::json!({}));

        let mut seen = Vec::new();
        let polled = read_until_response(&mut client, 4, |_, other| seen.push(other));
        assert_eq!(polled.result.unwrap()["status"], "input_required");

        let pong = read_until_response(&mut client, 5, |_, other| seen.push(other));
        assert!(pong.result.is_some());

        // The task is still live and finishes normally once answered.
        client
            .write(&JsonRpcMessage::response(
                elicitation.id,
                serde_json::json!({ "action": "accept", "content": { "name": "octocat" } }),
            ))
            .unwrap();

        let result = read_until_response(&mut client, 3, |_, other| seen.push(other));
        assert_eq!(
            result.result.unwrap()["content"][0]["text"],
            "hello octocat"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_a_second_tasks_result_is_deferred_not_answered_re_entrantly() {
        // Only the safe read-only methods are answered inline. A nested
        // `tasks/result` would recurse into the pump, so it waits for the main
        // loop like everything else - and is answered once the first returns.
        let (mut client, _server) = task_server_on_a_socket();
        let (task_id, elicitation) = task_blocked_on_input(&mut client);

        send(
            &mut client,
            3,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        );
        send(
            &mut client,
            4,
            "tasks/result",
            serde_json::json!({ "taskId": task_id }),
        );

        client
            .write(&JsonRpcMessage::response(
                elicitation.id,
                serde_json::json!({ "action": "accept", "content": { "name": "twice" } }),
            ))
            .unwrap();

        let mut seen = Vec::new();
        let first = read_until_response(&mut client, 3, |_, other| seen.push(other));
        assert_eq!(first.result.unwrap()["content"][0]["text"], "hello twice");

        let second = read_until_response(&mut client, 4, |_, other| seen.push(other));
        assert_eq!(second.result.unwrap()["content"][0]["text"], "hello twice");
    }

    #[test]
    fn test_set_status_honors_the_state_machine() {
        let store = TaskStore::new(TaskConfig::default());
        let (task, _cancelled) = store.create(None).unwrap();

        // working -> input_required -> working
        assert!(
            store
                .set_status(&task.task_id, TaskStatus::InputRequired)
                .unwrap()
        );
        assert_eq!(
            store.get(&task.task_id).unwrap().status,
            TaskStatus::InputRequired
        );
        assert!(
            store
                .set_status(&task.task_id, TaskStatus::Working)
                .unwrap()
        );

        // Same-status moves are not transitions.
        assert!(
            !store
                .set_status(&task.task_id, TaskStatus::Working)
                .unwrap()
        );

        // Terminal states carry an outcome, so they go through finish().
        assert!(
            store
                .set_status(&task.task_id, TaskStatus::Completed)
                .is_err()
        );

        // An unknown task is not an error - it may simply have expired.
        assert!(!store.set_status("nope", TaskStatus::InputRequired).unwrap());
    }

    #[test]
    fn test_a_finished_task_cannot_be_moved_back_to_input_required() {
        let store = TaskStore::new(TaskConfig::default());
        let (task, _cancelled) = store.create(None).unwrap();
        store
            .finish(
                &task.task_id,
                TaskStatus::Completed,
                TaskOutcome::Value(serde_json::json!({})),
            )
            .unwrap();

        assert!(
            !store
                .set_status(&task.task_id, TaskStatus::InputRequired)
                .unwrap()
        );
        assert_eq!(
            store.get(&task.task_id).unwrap().status,
            TaskStatus::Completed
        );
    }

    #[test]
    fn test_try_result_does_not_block() {
        let store = TaskStore::new(TaskConfig::default());
        let (task, _cancelled) = store.create(None).unwrap();

        assert!(store.try_result(&task.task_id).unwrap().is_none());

        store
            .finish(
                &task.task_id,
                TaskStatus::Completed,
                TaskOutcome::Value(serde_json::json!({ "done": true })),
            )
            .unwrap();

        let TaskOutcome::Value(value) = store.try_result(&task.task_id).unwrap().unwrap() else {
            panic!("expected a value");
        };
        assert_eq!(value["done"], true);

        // An unknown task is an error, matching await_result.
        assert!(store.try_result("nope").is_err());
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

    //
    // Version negotiation
    //

    fn initialize_with(server: &mut Server<TestContext>, version: &str) -> Value {
        dispatch(
            server,
            "initialize",
            serde_json::json!({
                "protocolVersion": version,
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "1.0" }
            }),
        )
        .unwrap()
    }

    #[test]
    fn test_protocol_version_is_2025_11_25() {
        assert_eq!(PROTOCOL_VERSION, "2025-11-25");
        assert_eq!(SUPPORTED_PROTOCOL_VERSIONS[0], PROTOCOL_VERSION);
        assert!(is_supported_protocol_version("2025-11-25"));
        assert!(is_supported_protocol_version("2025-06-18"));
        assert!(is_supported_protocol_version("2025-03-26"));
        assert!(!is_supported_protocol_version("2024-11-05"));
        assert!(!is_supported_protocol_version(""));
    }

    #[test]
    fn test_supported_version_is_echoed_back() {
        // "If the server supports the requested protocol version, it MUST
        // respond with the same version."
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        for version in SUPPORTED_PROTOCOL_VERSIONS {
            let result = initialize_with(&mut server, version);
            assert_eq!(result["protocolVersion"], *version);
        }
    }

    #[test]
    fn test_unsupported_version_negotiates_down_to_the_latest() {
        // "Otherwise, the server MUST respond with another protocol version it
        // supports. This SHOULD be the latest version supported by the server."
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        for version in ["2024-11-05", "1.0.0", "", "not-a-version"] {
            let result = initialize_with(&mut server, version);
            assert_eq!(
                result["protocolVersion"], PROTOCOL_VERSION,
                "for {version:?}"
            );
        }
    }

    #[test]
    fn test_supported_versions_are_configurable() {
        let mut server: Server<TestContext> = Server::new(ServerConfig {
            supported_versions: vec!["2025-11-25".into()],
            ..Default::default()
        });

        assert_eq!(
            initialize_with(&mut server, "2025-11-25")["protocolVersion"],
            "2025-11-25"
        );
        // A dropped revision negotiates up to the only one on offer.
        assert_eq!(
            initialize_with(&mut server, "2025-03-26")["protocolVersion"],
            "2025-11-25"
        );
    }

    #[test]
    fn test_initialize_without_params_still_negotiates() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let request = JsonRpcRequest {
            jsonrpc: Default::default(),
            id: RequestId::Number(1),
            method: "initialize".to_string(),
            params: None,
        };
        let mut ctx = TestContext { counter: 0 };
        let result = server.dispatch_request(&request, &mut ctx).unwrap();
        assert_eq!(result["protocolVersion"], PROTOCOL_VERSION);
    }

    #[test]
    fn test_malformed_initialize_is_invalid_params_not_a_parse_error() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        let err = dispatch(
            &mut server,
            "initialize",
            serde_json::json!({ "protocolVersion": 42 }),
        )
        .unwrap_err();
        assert_eq!(err.to_jsonrpc_error().code, -32602);
    }

    //
    // Roots
    //

    #[test]
    fn test_list_roots_round_trip() {
        let transport = ScriptedTransport::new(vec![serde_json::json!({
            "roots": [
                { "uri": "file:///home/user/project", "name": "My Project" },
                { "uri": "file:///tmp" }
            ]
        })]);
        let (server, written, _t) = scripted_server(
            serde_json::json!({ "roots": { "listChanged": true } }),
            transport,
        );

        let roots = server.tool_env().list_roots().unwrap();
        assert_eq!(roots.len(), 2);
        assert_eq!(roots[0].uri, "file:///home/user/project");
        assert_eq!(roots[0].name.as_deref(), Some("My Project"));
        assert!(roots[1].name.is_none());

        assert_eq!(written_requests(&written)[0].method, "roots/list");
    }

    #[test]
    fn test_list_roots_refuses_without_the_capability() {
        let transport = ScriptedTransport::new(vec![]);
        let (server, written, _t) = scripted_server(serde_json::json!({}), transport);

        let err = server.tool_env().list_roots().unwrap_err();
        assert!(err.to_string().contains("roots"), "{err}");
        assert!(written_requests(&written).is_empty());
    }

    #[test]
    fn test_roots_capability_is_recorded_at_initialize() {
        let mut server: Server<TestContext> = Server::new(ServerConfig::default());
        assert!(!server.client_capabilities.supports_roots());

        dispatch(
            &mut server,
            "initialize",
            serde_json::json!({
                "protocolVersion": "2025-11-25",
                "capabilities": { "roots": { "listChanged": true } },
                "clientInfo": { "name": "test", "version": "1.0" }
            }),
        )
        .unwrap();

        assert!(server.client_capabilities.supports_roots());
    }

    #[test]
    fn test_an_invalid_cursor_is_invalid_params_on_every_list_operation() {
        // "Invalid cursors SHOULD result in an error with code -32602 (Invalid
        // params)" - and for `tasks/list` it is a MUST. Restarting at page one
        // instead gave a client that mangled a cursor an infinite loop over the
        // first page.
        let mut server = task_server();
        server.add_tool(IncrementTool).unwrap();

        for method in [
            "tools/list",
            "resources/list",
            "resources/templates/list",
            "prompts/list",
            "tasks/list",
        ] {
            let error = dispatch(
                &mut server,
                method,
                serde_json::json!({ "cursor": "!!!not-a-cursor!!!" }),
            )
            .unwrap_err();
            assert_eq!(error.to_jsonrpc_error().code, -32602, "{method}");
        }
    }

    #[test]
    fn test_a_cursor_past_the_end_of_tasks_list_is_invalid_params() {
        // "Invalid or nonexistent cursor in tasks/list: -32602". Tasks expire,
        // so a cursor handed out a minute ago really can point past the end.
        let mut server = task_server();

        let past_the_end = crate::pagination::encode_cursor_for_test(99);
        let error = dispatch(
            &mut server,
            "tasks/list",
            serde_json::json!({ "cursor": past_the_end }),
        )
        .unwrap_err();
        assert_eq!(error.to_jsonrpc_error().code, -32602);
    }

    #[test]
    fn test_type_mismatched_params_are_invalid_params_not_a_parse_error() {
        // JSON-RPC reserves -32700 for "invalid JSON was received by the
        // server". Well-formed JSON with a wrong-typed field is -32602, which
        // the tools page's own example uses.
        let mut server = task_server();
        server.add_tool(IncrementTool).unwrap();

        let cases: [(&str, Value); 6] = [
            ("tools/list", serde_json::json!({ "cursor": 12345 })),
            ("resources/list", serde_json::json!({ "cursor": 12345 })),
            ("prompts/list", serde_json::json!({ "cursor": 12345 })),
            ("tasks/list", serde_json::json!({ "cursor": 12345 })),
            ("tools/call", serde_json::json!({ "name": 42 })),
            ("resources/read", serde_json::json!({ "uri": [] })),
        ];

        for (method, params) in cases {
            let error = dispatch(&mut server, method, params).unwrap_err();
            assert_eq!(error.to_jsonrpc_error().code, -32602, "{method}");
        }
    }

    //
    // Malformed input: answered, never fatal
    //
    // Every case here used to end the session with nothing written back. On
    // stdio that is a one-line remote kill; on a Unix daemon it drops the
    // client. JSON-RPC wants an error response and MCP explicitly allows a
    // null id for "error cases where the ID could not be read".
    //

    /// A plain server on one end of a socket pair, with the *raw* other end so
    /// a test can put bytes on the wire that no `Transport` would produce.
    #[cfg(unix)]
    fn server_on_a_raw_socket() -> (std::os::unix::net::UnixStream, std::thread::JoinHandle<()>) {
        use crate::transport::UnixTransport;
        use std::os::unix::net::UnixStream;

        let (server_end, client_end) = UnixStream::pair().unwrap();
        let handle = std::thread::spawn(move || {
            let mut server: Server<TestContext> = Server::new(ServerConfig::default());
            server.add_tool(IncrementTool).unwrap();
            let _ = server.start(
                UnixTransport::from_stream(server_end),
                TestContext { counter: 0 },
            );
        });

        client_end
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        (client_end, handle)
    }

    /// Write raw bytes plus a newline, then read one line back.
    #[cfg(unix)]
    fn exchange_raw(client: &mut std::os::unix::net::UnixStream, line: &[u8]) -> Value {
        use std::io::{BufRead, BufReader, Write};

        client.write_all(line).unwrap();
        client.write_all(b"\n").unwrap();
        client.flush().unwrap();

        let mut reader = BufReader::new(client.try_clone().unwrap());
        let mut response = String::new();
        reader.read_line(&mut response).expect("a response line");
        assert!(!response.is_empty(), "server hung up instead of answering");
        serde_json::from_str(&response).expect("response is JSON")
    }

    /// The session still works after whatever we just sent it.
    #[cfg(unix)]
    fn assert_still_alive(client: &mut std::os::unix::net::UnixStream) {
        let pong = exchange_raw(
            client,
            br#"{"jsonrpc":"2.0","id":99,"method":"ping"}"#.as_ref(),
        );
        assert_eq!(pong["id"], 99, "session did not survive: {pong}");
        assert!(pong["result"].is_object(), "{pong}");
    }

    #[cfg(unix)]
    #[test]
    fn test_a_json_syntax_error_is_answered_with_parse_error() {
        let (mut client, _server) = server_on_a_raw_socket();

        let error = exchange_raw(&mut client, b"{not json");
        assert_eq!(error["error"]["code"], -32700, "{error}");
        assert!(error["id"].is_null(), "id must be null when unreadable");

        assert_still_alive(&mut client);
    }

    #[cfg(unix)]
    #[test]
    fn test_a_batch_array_is_answered_with_invalid_request() {
        // The migration notes claimed this already worked. It did in
        // `JsonRpcMessage::parse`; on the wire the client saw a closed socket.
        let (mut client, _server) = server_on_a_raw_socket();

        let error = exchange_raw(
            &mut client,
            br#"[{"jsonrpc":"2.0","id":1,"method":"ping"}]"#,
        );
        assert_eq!(error["error"]["code"], -32600, "{error}");
        assert!(
            error["error"]["message"]
                .as_str()
                .unwrap()
                .contains("batching"),
            "{error}"
        );

        assert_still_alive(&mut client);
    }

    #[cfg(unix)]
    #[test]
    fn test_a_null_id_is_answered_with_invalid_request() {
        let (mut client, _server) = server_on_a_raw_socket();

        let error = exchange_raw(
            &mut client,
            br#"{"jsonrpc":"2.0","id":null,"method":"ping"}"#,
        );
        assert_eq!(error["error"]["code"], -32600, "{error}");
        assert!(
            error["error"]["message"].as_str().unwrap().contains("null"),
            "{error}"
        );

        assert_still_alive(&mut client);
    }

    #[cfg(unix)]
    #[test]
    fn test_a_float_id_is_answered_rather_than_silently_reclassified() {
        // Dropping `deny_unknown_fields` would make an untagged parse read this
        // as a *notification* (id ignored), so the client would wait forever
        // for a response nobody owes it.
        let (mut client, _server) = server_on_a_raw_socket();

        let error = exchange_raw(
            &mut client,
            br#"{"jsonrpc":"2.0","id":1.5,"method":"ping"}"#,
        );
        assert_eq!(error["error"]["code"], -32600, "{error}");

        assert_still_alive(&mut client);
    }

    #[cfg(unix)]
    #[test]
    fn test_non_utf8_bytes_are_answered_with_invalid_request() {
        let (mut client, _server) = server_on_a_raw_socket();

        let error = exchange_raw(&mut client, &[0x7b, 0xff, 0xfe, 0x7d]);
        assert_eq!(error["error"]["code"], -32600, "{error}");
        assert!(
            error["error"]["message"]
                .as_str()
                .unwrap()
                .contains("UTF-8"),
            "{error}"
        );

        assert_still_alive(&mut client);
    }

    #[cfg(unix)]
    #[test]
    fn test_an_unknown_top_level_key_is_not_fatal() {
        // The nastiest of the set: one unrecognised key used to take the whole
        // server down. Forward compatibility says ignore it and answer.
        let (mut client, _server) = server_on_a_raw_socket();

        let response = exchange_raw(
            &mut client,
            br#"{"jsonrpc":"2.0","id":7,"method":"ping","extra":true}"#,
        );
        assert_eq!(response["id"], 7, "{response}");
        assert!(response["result"].is_object(), "{response}");

        assert_still_alive(&mut client);
    }

    #[cfg(unix)]
    #[test]
    fn test_a_stray_response_with_a_null_id_is_not_fatal() {
        // What a conformant client sends back when *it* could not read an id.
        // It answers nothing, so the session must simply carry on.
        let (mut client, _server) = server_on_a_raw_socket();

        use std::io::Write;
        client
            .write_all(br#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"nope"}}"#)
            .unwrap();
        client.write_all(b"\n").unwrap();
        client.flush().unwrap();

        assert_still_alive(&mut client);
    }

    #[test]
    fn test_null_request_id_serializes_as_json_null() {
        let message = JsonRpcMessage::error(RequestId::Null, JsonRpcError::parse_error("bad"));
        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains("\"id\":null"), "{json}");
    }
}
