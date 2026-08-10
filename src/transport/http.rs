//! HTTP Transport
//!
//! Streamable HTTP transport for remote MCP servers.
//! Returns SSE stream to support notifications and progress.

use crate::transport::Transport;
use crate::types::{JsonRpcMessage, McpError, Result};
use std::sync::{Arc, Mutex};

/// What one HTTP request has produced so far.
///
/// The two halves are kept apart, and the response *seals* the buffer, because
/// one HTTP request carries exactly one answer: everything written before the
/// response belongs in the body with it, and anything written afterwards has
/// nowhere to go. Keeping a flat list instead made the content type a race - a
/// task worker's `notifications/tasks/status` landing between the response
/// being buffered and being read turned the same call from
/// `application/json` with one message into `text/event-stream` with two.
#[derive(Default)]
struct Buffered {
    /// Notifications written before the response, in order.
    notifications: Vec<String>,
    /// The response, once one has been written.
    response: Option<String>,
}

/// HTTP request/response transport with SSE support
///
/// Buffers all outgoing messages and returns them as an SSE stream.
pub struct HttpTransport {
    /// The request body (JSON-RPC message)
    request: Option<String>,
    /// Buffered messages to return as SSE stream
    messages: Arc<Mutex<Buffered>>,
}

impl HttpTransport {
    /// Create a new HTTP transport from a request body
    pub fn new(request_body: String) -> Self {
        Self {
            request: Some(request_body),
            messages: Arc::new(Mutex::new(Buffered::default())),
        }
    }

    /// The buffered messages, tolerating a poisoned lock.
    ///
    /// Poisoning is exactly what a panic mid-request produces, and it is what
    /// the caught panic then needs to report on. A pair of buffers has no
    /// invariant an interrupted write can break, so there is nothing to protect
    /// by refusing to look at it.
    fn buffered(&self) -> std::sync::MutexGuard<'_, Buffered> {
        self.messages.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Take the response as an SSE stream body
    ///
    /// Returns the messages formatted as SSE events:
    /// ```text
    /// data: {"jsonrpc":"2.0",...}
    ///
    /// data: {"jsonrpc":"2.0",...}
    ///
    /// ```
    pub fn take_sse_response(&mut self) -> String {
        let buffered = self.buffered();
        buffered
            .notifications
            .iter()
            .chain(buffered.response.iter())
            .map(|msg| format!("data: {}\n\n", msg))
            .collect()
    }

    /// Take response as plain JSON (for single response, no notifications)
    ///
    /// `None` when the server wrote no response - which is what a notification
    /// or a client response looks like, and is answered with `202 Accepted`.
    pub fn take_response(&mut self) -> Option<String> {
        self.buffered().response.clone()
    }

    /// Whether anything besides the response needs an SSE stream to carry it.
    pub fn has_notifications(&self) -> bool {
        !self.buffered().notifications.is_empty()
    }
}

impl Transport for HttpTransport {
    fn read(&mut self) -> Result<JsonRpcMessage> {
        let body = self.request.take().ok_or(McpError::TransportClosed)?;

        JsonRpcMessage::parse(&body)
    }

    /// Buffer a message for the response body.
    ///
    /// Writing the response closes this transport: a task worker outlives the
    /// request that started it, and anything it writes afterwards - its
    /// `notifications/tasks/status`, an `env.log()` - has no response left to
    /// travel in. Saying so with [`McpError::TransportClosed`] rather than
    /// swallowing it is what lets `env.log()` fall back to stderr, where a
    /// downstream author can actually find it.
    fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
        let json = serde_json::to_string(message)?;
        let mut buffered = self.buffered();

        if buffered.response.is_some() {
            return Err(McpError::TransportClosed);
        }
        match message {
            JsonRpcMessage::Response(_) => buffered.response = Some(json),
            _ => buffered.notifications.push(json),
        }
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
//
// HttpServer - high-level server wrapper
//

use crate::server::{LogLevel, Server, ServerConfig, TaskContext};
use crate::tasks::{TaskStore, random_hex_id};
use crate::transport::OriginPolicy;
use crate::transport::pool::WorkerPool;
use crate::types::{ASSUMED_PROTOCOL_VERSION, ClientCapabilities};
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::time::{Duration, Instant};
use tiny_http::{Header, Method, Request, Response, Server as TinyServer};

#[cfg(feature = "auth")]
use crate::auth::{Claims, JwtValidator, ProtectedResourceMetadata, unauthorized_challenge};

/// Setup function type for configuring tools on each request
type SetupFn<C> = Box<dyn Fn(&mut Server<C>) -> Result<()> + Send + Sync>;

/// A ready-to-send HTTP response with an owned body.
///
/// Owned, rather than borrowing from the request, because the response travels
/// to a worker thread and outlives whatever built it.
type HttpResponse = Response<Cursor<Vec<u8>>>;

/// The header carrying the session id, per transports §Session Management.
const SESSION_HEADER: &str = "Mcp-Session-Id";

/// How many sessions to remember.
///
/// Anyone who can reach the endpoint can start one, so this is bounded like
/// everything else a peer controls; the least recently used goes first.
const MAX_SESSIONS: usize = 256;

/// How long a session survives without being used.
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// The path of a request URL, without any query string or fragment.
///
/// `tiny_http` hands back the raw request target, so `/mcp?sessionId=abc` is
/// not `/mcp` by string comparison - and the spec asks for "a single HTTP
/// endpoint *path*". The well-known metadata route has the same problem, and it
/// is a URL intermediaries append cache busters to.
fn path_of(url: &str) -> &str {
    url.split(['?', '#']).next().unwrap_or("")
}

/// Look up a request header, case-insensitively as HTTP requires.
fn header_value<'a>(request: &'a Request, name: &'static str) -> Option<&'a str> {
    request
        .headers()
        .iter()
        .find(|header| header.field.equiv(name))
        .map(|header| header.value.as_str())
}

/// Attach a header, or leave the response alone if the value cannot be one.
///
/// Nothing here builds a header value a client controls, so the failing arm is
/// unreachable in practice - but dropping one header beats refusing to answer.
fn with_header(response: HttpResponse, name: &'static str, value: impl AsRef<str>) -> HttpResponse {
    match Header::from_bytes(name, value.as_ref()) {
        Ok(header) => response.with_header(header),
        Err(()) => response,
    }
}

/// A `200 OK` carrying this body, as this content type.
fn body_response(content_type: &'static str, body: String) -> HttpResponse {
    with_header(
        Response::from_data(body.into_bytes()),
        "Content-Type",
        content_type,
    )
}

/// A response with no body at all, for the statuses that must not carry one.
fn empty_response(status: u16) -> HttpResponse {
    Response::from_data(Vec::new()).with_status_code(status)
}

/// State that belongs to one client's session rather than to one request.
///
/// A `Server` is built per request here, so anything the client established
/// earlier - the log level it asked for, what it said it could do, the revision
/// it negotiated, the tasks it started - is gone by the time the next request
/// arrives unless it is kept somewhere that outlives the request.
///
/// Kept per *session*, not per process. Hoisting this state onto the
/// `HttpServer` fixed the losing half and created a worse one: every client on
/// the listener read and wrote the same state, so a client that had never sent
/// `initialize` inherited another client's declared capabilities, and one
/// client's `logging/setLevel` changed what an unrelated client received. Under
/// `serve_with_auth` that was one tenant's session driving another tenant's
/// request.
#[derive(Default)]
struct Session {
    /// As last set by `logging/setLevel`.
    log_level: Option<LogLevel>,
    /// As declared at `initialize`.
    client_capabilities: ClientCapabilities,
    /// As agreed at `initialize`.
    negotiated_version: Option<String>,
    /// Whether the handshake has happened, for `require_initialization`.
    initialized: bool,
    /// The store tasks live in, once a request has enabled tasks.
    tasks: Option<Arc<TaskStore>>,
}

impl Session {
    /// Give a freshly built server everything the session already knows.
    fn restore<C: Send + Sync + 'static>(&mut self, server: &mut Server<C>) {
        if let Some(level) = self.log_level {
            server.set_log_level(level);
        }
        server.set_client_capabilities(self.client_capabilities.clone());
        if let Some(version) = &self.negotiated_version {
            server.set_negotiated_version(version.clone());
        }
        server.set_initialized(self.initialized);

        // The first request that enables tasks donates its store; every later
        // one runs against that same store instead of its own.
        match self.tasks.clone() {
            Some(store) => server.share_task_store(store),
            None => self.tasks = server.task_store().cloned(),
        }
    }

    /// Take back whatever the request changed.
    ///
    /// The log level is only recorded when it differs from what a fresh server
    /// starts at, so a request that never touched `logging/setLevel` does not
    /// look like one that did - which is what decides whether this session is
    /// worth remembering at all.
    fn absorb<C: Send + Sync + 'static>(&mut self, server: &Server<C>, default_level: LogLevel) {
        if server.log_level() != default_level {
            self.log_level = Some(server.log_level());
        }
        self.client_capabilities = server.client_capabilities().clone();
        if let Some(version) = server.negotiated_version() {
            self.negotiated_version = Some(version.to_string());
        }
        self.initialized |= server.is_initialized();
    }

    /// Is there anything here worth another request finding?
    ///
    /// A session that only ever answered a `ping` holds nothing, and keeping
    /// one per request would let anyone who can reach the endpoint churn the
    /// table. A handshake, a log level, or a live task is worth an entry; the
    /// bare task *store* is not, since every request on a tasks-enabled server
    /// donates one whether or not a task was ever created.
    fn worth_keeping(&self) -> bool {
        self.initialized
            || self.log_level.is_some()
            || self.negotiated_version.is_some()
            || self
                .tasks
                .as_ref()
                .is_some_and(|store| !store.is_empty().unwrap_or(true))
    }
}

/// One entry in the session table.
struct SessionEntry {
    state: Arc<Mutex<Session>>,
    last_used: Instant,
}

/// The live sessions, keyed by the id handed out in `Mcp-Session-Id`.
///
/// Each session is behind its own lock, so two requests in the *same* session
/// serialize only while state is handed over and taken back - never while a
/// tool runs - and two requests in different sessions never contend at all.
#[derive(Default)]
struct Sessions {
    live: Mutex<HashMap<String, SessionEntry>>,
}

/// A request naming a session this server does not have.
struct UnknownSession;

impl Sessions {
    /// The table, tolerating a poisoned lock.
    ///
    /// A `HashMap` of `Arc`s has no invariant an interrupted write can break,
    /// and refusing to look at it would turn one panic into a permanently
    /// broken server - the same reasoning `HttpTransport::buffered` uses.
    fn table(&self) -> std::sync::MutexGuard<'_, HashMap<String, SessionEntry>> {
        self.live.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The session this request belongs to.
    ///
    /// A request that names a session gets that one, or `Err` if it has been
    /// terminated or expired - which the spec answers with `404`. A request
    /// that names none gets a brand-new one rather than sharing anybody's:
    /// a client that does not participate in session management then behaves
    /// exactly as it did before sessions existed, with no state carried and
    /// nothing inherited.
    fn checkout(
        &self,
        id: Option<&str>,
    ) -> std::result::Result<(String, Arc<Mutex<Session>>), UnknownSession> {
        let mut table = self.table();
        Self::sweep(&mut table);

        match id {
            Some(id) => {
                let entry = table.get_mut(id).ok_or(UnknownSession)?;
                entry.last_used = Instant::now();
                Ok((id.to_string(), entry.state.clone()))
            }
            None => Ok((random_hex_id(), Arc::new(Mutex::new(Session::default())))),
        }
    }

    /// Put the session back, and say whether it is worth telling the client
    /// about.
    fn check_in(&self, id: String, state: Arc<Mutex<Session>>) -> Option<String> {
        let keep = state
            .lock()
            .map(|session| session.worth_keeping())
            .unwrap_or(false);
        if !keep {
            self.table().remove(&id);
            return None;
        }

        let mut table = self.table();
        table.insert(
            id.clone(),
            SessionEntry {
                state,
                last_used: Instant::now(),
            },
        );
        Self::evict_oldest(&mut table);
        Some(id)
    }

    /// Forget a session, as `DELETE` asks.
    fn terminate(&self, id: &str) -> bool {
        self.table().remove(id).is_some()
    }

    /// Drop sessions nobody has touched in a while.
    fn sweep(table: &mut HashMap<String, SessionEntry>) {
        let now = Instant::now();
        table.retain(|_, entry| now.duration_since(entry.last_used) < SESSION_IDLE_TIMEOUT);
    }

    /// Keep the table under its ceiling, least recently used first.
    fn evict_oldest(table: &mut HashMap<String, SessionEntry>) {
        while table.len() > MAX_SESSIONS {
            let Some(oldest) = table
                .iter()
                .min_by_key(|(id, entry)| (entry.last_used, (*id).clone()))
                .map(|(id, _)| id.clone())
            else {
                return;
            };
            table.remove(&oldest);
        }
    }
}

/// What a processed request produced.
enum Processed {
    /// A body to send with `200 OK`.
    Body(String, &'static str),
    /// Nothing to send. The input was a JSON-RPC response or notification, and
    /// the spec is specific: "If the server accepts the input, the server
    /// **MUST** return HTTP status code 202 Accepted with no body."
    Accepted,
}

/// Build an error response whose body is a JSON-RPC error object.
///
/// The spec permits (and dual-era clients benefit from) HTTP error responses
/// carrying a JSON-RPC *error response* with no `id` - a plain-text body forces
/// clients to guess why the request failed.
fn error_response(status: u16, code: i32, message: impl AsRef<str>) -> HttpResponse {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": message.as_ref() },
    })
    .to_string();

    body_response("application/json", body).with_status_code(status)
}

/// How the listening socket is set up.
enum Binding {
    Plain,
    /// PEM certificate chain and private key, served over TLS by rustls.
    #[cfg(feature = "tls")]
    Tls {
        certificate: Vec<u8>,
        private_key: Vec<u8>,
    },
}

/// Threads to keep for serving requests.
///
/// It wants to be comfortably larger than the number of clients that might sit
/// in a blocking `tasks/result` at once - that call is bounded by
/// [`ServerConfig::task_result_timeout`], but while it waits it holds one of
/// these.
fn default_pool_size() -> usize {
    8 * std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// The answer to a request that arrived with nowhere to run.
///
/// Every worker busy *and* the queue behind them full is the one case a bounded
/// pool has to have an answer for. Queueing without limit would let a peer make
/// the server hold arbitrarily many accepted requests; blocking the accept loop
/// would make the whole listener as slow as its slowest request, which is the
/// thing this design exists to prevent. So it is said out loud, in the shape
/// clients already parse.
fn overloaded() -> HttpResponse {
    error_response(
        503,
        -32603,
        "Service Unavailable: every request thread is busy; retry shortly",
    )
}

/// Bind, then serve forever: accept on this thread, answer on the pool.
///
/// The accept loop does exactly two things - take a request and hand it to a
/// worker - so how long any one request takes cannot affect when the next is
/// picked up. That is the whole difference from the loop this replaced.
fn listen<H>(addr: &str, pool_size: usize, binding: Binding, handle: H) -> Result<()>
where
    H: Fn(Request) + Send + Sync + 'static,
{
    let server = match binding {
        Binding::Plain => TinyServer::http(addr),
        #[cfg(feature = "tls")]
        Binding::Tls {
            certificate,
            private_key,
        } => TinyServer::https(
            addr,
            tiny_http::SslConfig {
                certificate,
                private_key,
            },
        ),
    }
    .map_err(|e| McpError::Internal(format!("Failed to start HTTP server: {}", e)))?;

    // Room for one waiting request per thread. Big enough that a burst of short
    // requests queues instead of being refused, small enough that "the server
    // is saturated" is answered rather than hidden behind an ever-growing
    // backlog nobody is getting to.
    let threads = pool_size.max(1);
    let pool = WorkerPool::new(threads, threads, handle);

    for request in server.incoming_requests() {
        if let Err(request) = pool.dispatch(request) {
            let _ = request.respond(overloaded());
        }
    }

    Ok(())
}

/// High-level HTTP MCP server
///
/// Wraps the request loop boilerplate for serving MCP over HTTP. `tiny_http`
/// accepts; a fixed pool of threads answers, one request each, so a client
/// blocked in `tasks/result` cannot hold up anybody else.
///
/// The pool is bounded in both directions - [`pool_size`](Self::pool_size)
/// threads, and one waiting request per thread behind them. A request arriving
/// when both are full is answered `503` rather than queued, because a queue
/// with no ceiling is a peer deciding how much this server holds, and blocking
/// the accept loop instead would make the listener as slow as its slowest
/// request.
///
/// # Example (no auth)
/// ```ignore
/// HttpServer::new(config)
///     .with_tools(|s| {
///         s.add_tool(EchoTool)?;
///         s.add_tool(CounterTool)?;
///         Ok(())
///     })
///     .serve("127.0.0.1:3000", || AppContext::new())?;
/// ```
///
/// # Example (with JWT auth)
/// ```ignore
/// HttpServer::new(config)
///     .with_tools(|s| {
///         s.add_tool(WhoamiTool)?;
///         Ok(())
///     })
///     .serve_with_auth(
///         "127.0.0.1:3001",
///         // Bound to this server's resource: a token minted for anyone else
///         // MUST be rejected, and an unbound validator is refused at startup.
///         JwtValidator::hs256(SECRET).for_resource(&resource),
///         |claims| AuthContext {
///             user_id: claims.user_id().to_string(),
///             tenant_id: claims.tenant_id().to_string(),
///         },
///     )?;
/// ```
pub struct HttpServer<C> {
    config: ServerConfig,
    endpoint: String,
    setup: Option<SetupFn<C>>,
    origin_policy: OriginPolicy,
    /// Served at the RFC 9728 well-known path, and pointed at by the
    /// `WWW-Authenticate` challenge on a 401.
    #[cfg(feature = "auth")]
    protected_resource: Option<ProtectedResourceMetadata>,
    /// Scopes every authenticated request must carry.
    #[cfg(feature = "auth")]
    required_scopes: Vec<String>,
    /// Everything that outlives a single request, per client.
    sessions: Sessions,
    /// Threads serving requests; `None` picks a default from the CPU count.
    pool_size: Option<usize>,
}

impl<C: Send + Sync + 'static> HttpServer<C> {
    /// Create a new HTTP server with the given configuration
    ///
    /// The `Origin` policy defaults to [`OriginPolicy::Loopback`]. Bind to
    /// `127.0.0.1` unless you specifically intend to be reachable off-host.
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            endpoint: "/mcp".to_string(),
            setup: None,
            origin_policy: OriginPolicy::default(),
            #[cfg(feature = "auth")]
            protected_resource: None,
            #[cfg(feature = "auth")]
            required_scopes: Vec::new(),
            sessions: Sessions::default(),
            pool_size: None,
        }
    }

    /// Set the endpoint path (default: "/mcp")
    pub fn endpoint(mut self, path: impl Into<String>) -> Self {
        self.endpoint = path.into();
        self
    }

    /// How many threads serve requests (default: `8 ×` the CPU count).
    ///
    /// Each in-flight request occupies one for its whole duration, including a
    /// `tasks/result` that is waiting for a task to finish - bounded by
    /// [`ServerConfig::task_result_timeout`], but a wait all the same. Size
    /// this above the number of clients you expect to be blocked at once.
    ///
    /// It also sets how deep the queue behind those threads runs: one waiting
    /// request per thread, so a burst queues rather than being refused. A
    /// request that arrives with every thread busy *and* that queue full is
    /// answered `503` - see [`HttpServer`] for why that beats queueing without
    /// limit.
    pub fn pool_size(mut self, threads: usize) -> Self {
        self.pool_size = Some(threads.max(1));
        self
    }

    /// Set which `Origin` headers are accepted (default:
    /// [`OriginPolicy::Loopback`]).
    ///
    /// Requests carrying an `Origin` the policy rejects are answered with
    /// HTTP 403, as the spec requires. Requests with no `Origin` header at all
    /// - which is every non-browser client - are unaffected.
    ///
    /// ```ignore
    /// HttpServer::new(config)
    ///     .origin_policy(OriginPolicy::allowlist(["https://app.example.com"]))
    ///     .serve("127.0.0.1:3000", || Ctx::new())?;
    /// ```
    pub fn origin_policy(mut self, policy: OriginPolicy) -> Self {
        self.origin_policy = policy;
        self
    }

    /// Publish OAuth 2.0 Protected Resource Metadata (RFC 9728).
    ///
    /// The spec makes this a MUST for protected servers: "MCP servers **MUST**
    /// implement OAuth 2.0 Protected Resource Metadata to indicate the
    /// locations of authorization servers."
    ///
    /// Setting it does two things:
    ///
    /// - serves the document at the well-known path derived from the resource
    ///   URI, over GET, with no authentication
    /// - adds `WWW-Authenticate: Bearer resource_metadata="...", scope="..."`
    ///   to every 401, which is the discovery path clients try first
    ///
    /// ```ignore
    /// let resource = ResourceUri::parse("https://mcp.example.com/mcp")?;
    /// HttpServer::new(config)
    ///     .protected_resource(
    ///         ProtectedResourceMetadata::new(resource.clone(), ["https://auth.example.com"])
    ///             .with_scopes(["files:read"]),
    ///     )
    ///     .serve_with_auth(addr, JwtValidator::rs256_pem(key)?.for_resource(&resource), ..)?;
    /// ```
    #[cfg(feature = "auth")]
    pub fn protected_resource(mut self, metadata: ProtectedResourceMetadata) -> Self {
        self.protected_resource = Some(metadata);
        self
    }

    /// Require these scopes on every authenticated request.
    ///
    /// A token missing any of them is answered with `403` and a
    /// `WWW-Authenticate: Bearer error="insufficient_scope"` challenge naming
    /// what would have been enough - RFC 6750 §3.1, and the spec's own
    /// "principle of least privilege" guidance.
    ///
    /// ```ignore
    /// HttpServer::new(config)
    ///     .require_scopes(["files:read"])
    ///     .serve_with_auth(addr, validator, ..)?;
    /// ```
    #[cfg(feature = "auth")]
    pub fn require_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required_scopes = scopes.into_iter().map(Into::into).collect();
        self
    }

    /// A 403 for a token that authenticated but is not permitted here.
    #[cfg(feature = "auth")]
    fn forbidden(&self, missing: &[&str]) -> HttpResponse {
        let response = error_response(
            403,
            -32600,
            format!("Forbidden: missing scope(s): {}", missing.join(" ")),
        );

        // The challenge names the metadata document, so it can only be built
        // where one is published.
        let challenge = self.protected_resource.as_ref().and_then(|metadata| {
            let url = metadata.resource_uri().ok()?.metadata_url();
            Some(crate::auth::insufficient_scope_challenge(
                &url,
                &self.required_scopes,
                Some("the token does not carry the scopes this request needs"),
            ))
        });

        match challenge {
            Some(challenge) => with_header(response, "WWW-Authenticate", challenge),
            None => response,
        }
    }

    /// The `WWW-Authenticate` challenge to attach to a 401, if metadata is
    /// published.
    #[cfg(feature = "auth")]
    fn challenge(&self) -> Option<String> {
        let metadata = self.protected_resource.as_ref()?;
        let url = metadata.resource_uri().ok()?.metadata_url();
        Some(unauthorized_challenge(&url, &metadata.scopes_supported))
    }

    /// Serve the metadata document when this request is asking for it.
    #[cfg(feature = "auth")]
    fn protected_resource_response(&self, request: &Request) -> Option<HttpResponse> {
        let metadata = self.protected_resource.as_ref()?;
        let path = metadata.resource_uri().ok()?.metadata_path();
        if path_of(request.url()) != path {
            return None;
        }

        // Discovery must work before the client has a token, so this endpoint
        // is deliberately unauthenticated - it contains nothing secret.
        if !matches!(request.method(), Method::Get) {
            return Some(error_response(405, -32600, "Method Not Allowed"));
        }

        let body = serde_json::to_string(metadata).ok()?;
        Some(body_response("application/json", body))
    }

    /// Configure tools via a setup closure
    ///
    /// The closure is called for each request to set up a fresh server.
    pub fn with_tools<F>(mut self, setup: F) -> Self
    where
        F: Fn(&mut Server<C>) -> Result<()> + Send + Sync + 'static,
    {
        self.setup = Some(Box::new(setup));
        self
    }

    /// Run the transport-level checks every request must pass, regardless of
    /// authentication: endpoint path, HTTP method, and `Origin`.
    ///
    /// Returns `Some(response)` when the request must be rejected.
    fn precheck(&self, request: &Request) -> Option<HttpResponse> {
        // Discovery is served before the endpoint check, since it lives at a
        // different path by design.
        #[cfg(feature = "auth")]
        if let Some(response) = self.protected_resource_response(request) {
            return Some(response);
        }

        if path_of(request.url()) != self.endpoint {
            return Some(error_response(404, -32600, "Not Found"));
        }

        // POST carries messages; DELETE ends a session.
        if !matches!(request.method(), Method::Post | Method::Delete) {
            return Some(error_response(405, -32600, "Method Not Allowed"));
        }

        // "If the server receives a request with an invalid or unsupported
        // MCP-Protocol-Version, it MUST respond with 400 Bad Request." An
        // absent header is not an error: it means 2025-03-26, which we speak.
        let version =
            header_value(request, "MCP-Protocol-Version").unwrap_or(ASSUMED_PROTOCOL_VERSION);
        if !self.config.supported_versions.iter().any(|v| v == version) {
            self.trace(|| format!("  ✗ Unsupported MCP-Protocol-Version: {}", version));
            return Some(error_response(
                400,
                -32600,
                format!(
                    "Unsupported MCP-Protocol-Version `{}`; this server supports: {}",
                    version,
                    self.config.supported_versions.join(", ")
                ),
            ));
        }

        // Only a *present* Origin is validated. Non-browser clients omit it,
        // and DNS rebinding requires a browser, which always sends it.
        if let Some(origin) = header_value(request, "Origin") {
            if !self.origin_policy.is_allowed(origin) {
                eprintln!("  ✗ Rejected Origin: {}", origin);
                return Some(error_response(
                    403,
                    -32600,
                    format!("Forbidden: disallowed Origin `{}`", origin),
                ));
            }
        }

        None
    }

    /// A 401 carrying the `WWW-Authenticate` challenge, when metadata is
    /// published for clients to discover the authorization server from.
    #[cfg(feature = "auth")]
    fn unauthorized(&self, message: impl AsRef<str>) -> HttpResponse {
        let response = error_response(401, -32600, message);
        match self.challenge() {
            Some(challenge) => with_header(response, "WWW-Authenticate", challenge),
            None => response,
        }
    }

    /// Read the request body, or produce the response to send instead.
    ///
    /// Bounded by [`ServerConfig::max_message_bytes`], the same ceiling the
    /// line transports use. It had none: 48 MiB was accepted and echoed back,
    /// so the transport actually exposed to the network was the one *without* a
    /// cap. The declared length is checked before a byte is read, and the read
    /// itself is capped as well, since a chunked body declares no length and a
    /// `Content-Length` can lie.
    fn read_body(&self, request: &mut Request) -> std::result::Result<String, HttpResponse> {
        let limit = self.config.max_message_bytes;

        // `body_length` is the declared `Content-Length`, and `None` for a
        // chunked body - which declares nothing and is exactly why the read
        // below is capped too.
        if request
            .body_length()
            .is_some_and(|declared| declared > limit)
        {
            return Err(self.too_large(limit));
        }

        let mut body = String::new();
        // One byte over the limit is enough to know it was exceeded, and is all
        // that is ever read past it.
        if let Err(e) = request
            .as_reader()
            .take(limit as u64 + 1)
            .read_to_string(&mut body)
        {
            eprintln!("  Failed to read body: {}", e);
            return Err(error_response(400, -32700, "Bad Request: unreadable body"));
        }
        if body.len() > limit {
            return Err(self.too_large(limit));
        }

        Ok(body)
    }

    /// The answer to a body bigger than this server will accept.
    fn too_large(&self, limit: usize) -> HttpResponse {
        error_response(
            413,
            -32600,
            format!("Payload Too Large: the body exceeds the {limit} byte limit"),
        )
    }

    /// Write a diagnostic that may contain request or response bodies.
    ///
    /// Logging §Security: "Log messages **MUST NOT** contain: Credentials or
    /// secrets; Personal identifying information". Tool arguments routinely
    /// carry both, so full bodies are only written when the server was
    /// configured for `debug` in the first place - and so are request targets,
    /// which carry query strings, and the identity behind a token.
    fn trace(&self, message: impl FnOnce() -> String) {
        if self.logs_bodies() {
            eprintln!("{}", message());
        }
    }

    /// Whether this server was configured verbosely enough to print bodies.
    fn logs_bodies(&self) -> bool {
        self.config.default_log_level == LogLevel::Debug
    }

    /// Turn a processed result into the HTTP response to send.
    fn finish(&self, outcome: Result<Processed>) -> HttpResponse {
        match outcome {
            Ok(Processed::Body(response_body, content_type)) => {
                self.trace(|| format!("  Response ({}): {}", content_type, response_body));
                body_response(content_type, response_body)
            }
            // 202 with *no body*: `{}` is not a valid JSON-RPC message, so a
            // strict client parsing it fails.
            Ok(Processed::Accepted) => empty_response(202),
            Err(e) => {
                eprintln!("  Error: {}", e);
                // A client's syntax error is not the server's internal error.
                // The -32600 the batch path carefully builds used to be thrown
                // away by this wrapper.
                let (status, code) = match &e {
                    McpError::Json(_) => (400, -32700),
                    McpError::InvalidMessage(_) | McpError::InvalidRequest { .. } => (400, -32600),
                    _ => (500, -32603),
                };
                error_response(status, code, e.to_string())
            }
        }
    }

    /// Serve without authentication
    ///
    /// The context factory is called for each request, on that request's own
    /// thread - so it must be callable from several at once.
    pub fn serve<F>(self, addr: &str, context_factory: F) -> Result<()>
    where
        F: Fn() -> C + Send + Sync + 'static,
    {
        self.serve_on(addr, Binding::Plain, context_factory)
    }

    /// [`serve`](Self::serve) over TLS, with a PEM certificate chain and key.
    ///
    /// Terminating TLS here is for deployments with nothing in front of them;
    /// behind a reverse proxy, let the proxy do it and bind to loopback.
    #[cfg(feature = "tls")]
    pub fn serve_tls<F>(
        self,
        addr: &str,
        certificate: Vec<u8>,
        private_key: Vec<u8>,
        context_factory: F,
    ) -> Result<()>
    where
        F: Fn() -> C + Send + Sync + 'static,
    {
        self.serve_on(
            addr,
            Binding::Tls {
                certificate,
                private_key,
            },
            context_factory,
        )
    }

    fn serve_on<F>(self, addr: &str, binding: Binding, context_factory: F) -> Result<()>
    where
        F: Fn() -> C + Send + Sync + 'static,
    {
        eprintln!(
            "MCP HTTP server `{}` listening on {}{}",
            self.config.name, addr, self.endpoint
        );

        let pool_size = self.pool_size.unwrap_or_else(default_pool_size);
        listen(addr, pool_size, binding, move |mut request| {
            self.trace(|| format!("{} {}", request.method(), request.url()));

            let response = if let Some(rejection) = self.precheck(&request) {
                rejection
            } else if matches!(request.method(), Method::Delete) {
                self.end_session(&request)
            } else {
                // No authorization context, so requestors cannot be told apart:
                // tasks stay reachable by id but are not listable.
                self.respond(&mut request, TaskContext::Anonymous, &context_factory)
            };

            self.send(request, response);
        })
    }

    /// Serve with JWT authentication
    ///
    /// The context factory receives validated claims and creates a context.
    #[cfg(feature = "auth")]
    pub fn serve_with_auth<F>(
        self,
        addr: &str,
        validator: JwtValidator,
        context_factory: F,
    ) -> Result<()>
    where
        F: Fn(&Claims) -> C + Send + Sync + 'static,
    {
        self.serve_with_auth_on(addr, Binding::Plain, validator, context_factory)
    }

    /// [`serve_with_auth`](Self::serve_with_auth) over TLS.
    #[cfg(all(feature = "auth", feature = "tls"))]
    pub fn serve_with_auth_tls<F>(
        self,
        addr: &str,
        certificate: Vec<u8>,
        private_key: Vec<u8>,
        validator: JwtValidator,
        context_factory: F,
    ) -> Result<()>
    where
        F: Fn(&Claims) -> C + Send + Sync + 'static,
    {
        self.serve_with_auth_on(
            addr,
            Binding::Tls {
                certificate,
                private_key,
            },
            validator,
            context_factory,
        )
    }

    #[cfg(feature = "auth")]
    fn serve_with_auth_on<F>(
        self,
        addr: &str,
        binding: Binding,
        validator: JwtValidator,
        context_factory: F,
    ) -> Result<()>
    where
        F: Fn(&Claims) -> C + Send + Sync + 'static,
    {
        // "MCP servers MUST validate that access tokens were issued
        // specifically for them as the intended audience" - so a validator that
        // checks no audience cannot protect anything, and starting with one
        // would quietly accept tokens minted for other services.
        if !validator.binds_audience() {
            return Err(McpError::Internal(
                "serve_with_auth needs a validator bound to an audience: MCP servers MUST \
                 reject tokens that do not name them in the `aud` claim. Use \
                 `.for_resource(&resource)` (preferred) or `.with_audience(..)`."
                    .into(),
            ));
        }

        eprintln!(
            "MCP HTTP server `{}` (authenticated) listening on {}{}",
            self.config.name, addr, self.endpoint
        );

        let pool_size = self.pool_size.unwrap_or_else(default_pool_size);
        listen(addr, pool_size, binding, move |mut request| {
            self.trace(|| format!("{} {}", request.method(), request.url()));

            let response = self.authenticated_response(&mut request, &validator, &context_factory);
            self.send(request, response);
        })
    }

    /// Authenticate, authorize, and answer - or say why not.
    ///
    /// Split out of the accept closure only so the early returns can stay early
    /// returns while the caller still ends up holding the request to answer on.
    #[cfg(feature = "auth")]
    fn authenticated_response<F>(
        &self,
        request: &mut Request,
        validator: &JwtValidator,
        context_factory: &F,
    ) -> HttpResponse
    where
        F: Fn(&Claims) -> C,
    {
        if let Some(rejection) = self.precheck(request) {
            return rejection;
        }

        // JWT Authentication
        let claims = match header_value(request, "Authorization") {
            Some(header) => match validator.validate_header(header) {
                Ok(claims) => {
                    // The subject and tenant identify a person; they go behind
                    // the same gate as request bodies.
                    self.trace(|| {
                        format!(
                            "  ✓ Authenticated: user={}, tenant={}",
                            claims.user_id(),
                            claims.tenant_id()
                        )
                    });
                    claims
                }
                Err(e) => {
                    eprintln!("  ✗ Auth failed: {}", e);
                    return self.unauthorized(format!("Unauthorized: {}", e));
                }
            },
            None => {
                eprintln!("  ✗ No Authorization header");
                return self.unauthorized("Unauthorized: Missing Authorization header");
            }
        };

        // Authenticated is not authorized.
        let missing = claims.missing_scopes(&self.required_scopes);
        if !missing.is_empty() {
            eprintln!("  ✗ Missing scope(s): {}", missing.join(" "));
            return self.forbidden(&missing);
        }

        if matches!(request.method(), Method::Delete) {
            return self.end_session(request);
        }

        // Tasks bind to the identity the token carries, which is what makes
        // them isolatable.
        let owner = format!("{}\u{1}{}", claims.user_id(), claims.tenant_id());
        self.respond(request, TaskContext::Owner(owner), || {
            context_factory(&claims)
        })
    }

    /// Write the answer, and note it if the peer is no longer there to read it.
    ///
    /// A peer hanging up mid-answer is its business, not an incident - and it
    /// is peer-controlled, so it does not get to write to stderr on demand.
    fn send(&self, request: Request, response: HttpResponse) {
        if let Err(e) = request.respond(response) {
            self.trace(|| format!("  Failed to send response: {}", e));
        }
    }

    /// `DELETE` on the endpoint: "Clients that no longer need a particular
    /// session **SHOULD** send an HTTP DELETE ... to explicitly terminate it."
    fn end_session(&self, request: &Request) -> HttpResponse {
        match header_value(request, SESSION_HEADER) {
            Some(id) if self.sessions.terminate(id) => empty_response(204),
            Some(_) => error_response(404, -32600, "Not Found: no such session"),
            None => error_response(
                400,
                -32600,
                format!("Bad Request: DELETE needs an `{SESSION_HEADER}` header"),
            ),
        }
    }

    /// Answer one message, in the session it belongs to.
    fn respond(
        &self,
        request: &mut Request,
        task_context: TaskContext,
        make_context: impl FnOnce() -> C,
    ) -> HttpResponse {
        let Ok((id, session)) = self
            .sessions
            .checkout(header_value(request, SESSION_HEADER))
        else {
            // "The server MAY terminate the session at any time, after which it
            // MUST respond to requests containing that session ID with HTTP 404
            // Not Found."
            return error_response(404, -32600, "Not Found: unknown or terminated session");
        };

        let body = match self.read_body(request) {
            Ok(body) => body,
            Err(rejection) => return rejection,
        };
        self.trace(|| format!("  Request: {}", body));

        let response = self.finish(self.guarded(body, task_context, make_context, &session));

        // A session is only advertised once it holds something a later request
        // could want, so a client that only ever pings is never handed an id.
        match self.sessions.check_in(id, session) {
            Some(id) => with_header(response, SESSION_HEADER, id),
            None => response,
        }
    }

    /// [`process_request`](Self::process_request) with the request's panics
    /// contained.
    ///
    /// An unwinding tool, setup closure, or context factory used to take the
    /// accept loop with it: the client saw a dropped connection and the server
    /// stopped serving *everyone*. Thread-per-request narrows the blast radius
    /// to one worker, but a panic still has to come back as a JSON-RPC error
    /// rather than the bare `500` a dropped `Request` produces.
    fn guarded(
        &self,
        body: String,
        task_context: TaskContext,
        make_context: impl FnOnce() -> C,
        session: &Mutex<Session>,
    ) -> Result<Processed> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut ctx = make_context();
            self.process_request(body, &mut ctx, task_context, session)
        }))
        .unwrap_or_else(|payload| {
            Err(McpError::Internal(format!(
                "request handler panicked: {}",
                crate::server::panic_message(payload.as_ref())
            )))
        })
    }

    /// Process a single request and return the body to send, if any.
    fn process_request(
        &self,
        body: String,
        ctx: &mut C,
        task_context: TaskContext,
        session: &Mutex<Session>,
    ) -> Result<Processed> {
        // Create fresh server
        let mut server: Server<C> = Server::new(self.config.clone());

        // Set up tools
        if let Some(ref setup) = self.setup {
            setup(&mut server)?;
        }

        server.set_task_context(task_context);

        // Hand it the session's state before it sees the request, and take back
        // whatever the request changed afterwards. The lock is held across
        // neither the dispatch nor the transport.
        {
            let mut session = session
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            session.restore(&mut server);
        }

        // Create transport and process
        let transport = Arc::new(Mutex::new(HttpTransport::new(body)));
        let outcome = server.process_one(transport.clone(), ctx);

        {
            let mut session = session
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            session.absorb(&server, self.config.default_log_level);
        }
        outcome?;

        // Extract response
        let mut transport_guard = transport
            .lock()
            .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;

        if transport_guard.has_notifications() {
            return Ok(Processed::Body(
                transport_guard.take_sse_response(),
                "text/event-stream",
            ));
        }

        // Nothing written means the input was a notification or a response,
        // which is the 202 case. It used to be answered `200 {}`, and `{}` is
        // not a JSON-RPC message.
        match transport_guard.take_response() {
            Some(body) => Ok(Processed::Body(body, "application/json")),
            None => Ok(Processed::Accepted),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::JsonRpcMessage;

    #[test]
    fn test_http_transport_roundtrip() {
        let request = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let mut transport = HttpTransport::new(request.to_string());

        // Read the request
        let msg = transport.read().unwrap();
        if let JsonRpcMessage::Request(req) = msg {
            assert_eq!(req.method, "ping");
        } else {
            panic!("Expected request");
        }

        // Write a response
        let response = JsonRpcMessage::response(1i64, serde_json::json!({}));
        transport.write(&response).unwrap();

        // Get the response body
        let body = transport.take_response().unwrap();
        assert!(body.contains("\"result\":{}"));
    }

    #[test]
    fn test_http_transport_sse_multiple_messages() {
        let request = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;
        let mut transport = HttpTransport::new(request.to_string());

        // Simulate notification + response
        let notification = JsonRpcMessage::notification(
            "notifications/message",
            Some(serde_json::json!({"level": "info", "data": "hello"})),
        );
        transport.write(&notification).unwrap();

        let response = JsonRpcMessage::response(1i64, serde_json::json!({"result": "done"}));
        transport.write(&response).unwrap();

        assert!(transport.has_notifications());

        let sse = transport.take_sse_response();
        assert!(sse.contains("data: "));
        assert!(sse.contains("notifications/message"));
        assert!(sse.contains("\"result\":\"done\""));
    }

    #[test]
    fn the_response_seals_the_buffer() {
        // One HTTP request carries one answer. A task worker outlives the
        // request that started it, so anything it writes afterwards used to
        // land in a buffer that had already been - or was about to be - read:
        // either dropped silently, or racing the content-type decision.
        let mut transport =
            HttpTransport::new(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#.into());

        transport
            .write(&JsonRpcMessage::response(1i64, serde_json::json!({})))
            .unwrap();

        let late = JsonRpcMessage::notification(
            "notifications/tasks/status",
            Some(serde_json::json!({ "taskId": "t" })),
        );
        assert!(
            matches!(transport.write(&late), Err(McpError::TransportClosed)),
            "a late write is refused, so `env.log()` can fall back to stderr"
        );

        assert!(
            !transport.has_notifications(),
            "the content type cannot be changed after the response is decided"
        );
        assert!(transport.take_response().unwrap().contains("\"result\""));
    }

    #[test]
    fn notifications_before_the_response_still_make_an_sse_stream() {
        let mut transport =
            HttpTransport::new(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#.into());

        transport
            .write(&JsonRpcMessage::notification("notifications/message", None))
            .unwrap();
        transport
            .write(&JsonRpcMessage::response(1i64, serde_json::json!({})))
            .unwrap();

        assert!(transport.has_notifications());
        let sse = transport.take_sse_response();
        // Order is preserved: what happened during the call, then the answer.
        let first = sse.find("notifications/message").unwrap();
        let second = sse.find("\"result\"").unwrap();
        assert!(first < second, "{sse}");
    }
}

/// Integration tests for HttpServer
/// These spawn actual servers and make HTTP requests
#[cfg(test)]
mod http_server_tests {
    use super::*;
    use crate::server::{Server, ServerConfig, Tool, ToolEnv};
    use crate::types::{CallToolResult, Result};
    use serde_json::Value;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::atomic::{AtomicI64, AtomicU16, Ordering};
    use std::thread;
    use std::time::Duration;

    // Port counter to avoid conflicts between tests
    static PORT: AtomicU16 = AtomicU16::new(13000);

    fn next_port() -> u16 {
        PORT.fetch_add(1, Ordering::SeqCst)
    }

    // Test context
    struct TestContext {
        counter: Arc<AtomicI64>,
    }

    // Simple echo tool (no notifications)
    struct EchoTool;
    impl Tool<TestContext> for EchoTool {
        fn name(&self) -> &str {
            "echo"
        }
        fn description(&self) -> &str {
            "Echo a message"
        }
        fn schema(&self) -> Value {
            serde_json::json!({
                "type": "object",
                "properties": { "message": { "type": "string" } },
                "required": ["message"]
            })
        }
        fn execute(
            &self,
            args: Value,
            _ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            let msg = args.get("message").and_then(|m| m.as_str()).unwrap_or("");
            Ok(CallToolResult::text(format!("Echo: {}", msg)))
        }
    }

    // Tool that sends notifications (triggers SSE)
    struct NotifyTool;
    impl Tool<TestContext> for NotifyTool {
        fn name(&self) -> &str {
            "notify"
        }
        fn description(&self) -> &str {
            "Tool that sends notifications"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            env: &ToolEnv,
        ) -> Result<CallToolResult> {
            use crate::server::LogLevel;
            env.log(LogLevel::Info, "notification from tool")?;
            Ok(CallToolResult::text("done with notification"))
        }
    }

    // Tool that panics, to prove one bad call cannot end the accept loop
    struct PanicTool;
    impl Tool<TestContext> for PanicTool {
        fn name(&self) -> &str {
            "panic"
        }
        fn description(&self) -> &str {
            "Panics on purpose"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            panic!("http tool exploded");
        }
    }

    // Task-callable tool, to prove a task survives the request that made it
    struct SlowTaskTool;
    impl Tool<TestContext> for SlowTaskTool {
        fn name(&self) -> &str {
            "slow"
        }
        fn description(&self) -> &str {
            "Runs as a task"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn task_support(&self) -> crate::types::TaskSupport {
            crate::types::TaskSupport::Optional
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            Ok(CallToolResult::text("finished"))
        }
    }

    // Tool that reports the log level it can see, to prove setLevel sticks
    struct LogLevelTool;
    impl Tool<TestContext> for LogLevelTool {
        fn name(&self) -> &str {
            "level"
        }
        fn description(&self) -> &str {
            "Reports the client's log threshold"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            env: &ToolEnv,
        ) -> Result<CallToolResult> {
            Ok(CallToolResult::text(env.log_level().as_str()))
        }
    }

    // Task-callable tool that takes its time, so a blocking `tasks/result` is
    // observable
    struct BlockingTaskTool;
    impl Tool<TestContext> for BlockingTaskTool {
        fn name(&self) -> &str {
            "blocking"
        }
        fn description(&self) -> &str {
            "Runs as a task, slowly"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn task_support(&self) -> crate::types::TaskSupport {
            crate::types::TaskSupport::Optional
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            thread::sleep(Duration::from_millis(1500));
            Ok(CallToolResult::text("eventually"))
        }
    }

    /// A latch a test holds request threads on, and can count arrivals at.
    ///
    /// The pool is bounded, so "every worker is busy" has to be reachable on
    /// purpose rather than by hoping a sleep is long enough.
    #[derive(Default)]
    struct Gate {
        open: Mutex<bool>,
        changed: std::sync::Condvar,
        arrived: AtomicI64,
    }

    impl Gate {
        fn wait(&self) {
            self.arrived.fetch_add(1, Ordering::SeqCst);
            let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
            while !*open {
                open = self.changed.wait(open).unwrap_or_else(|e| e.into_inner());
            }
        }

        fn open(&self) {
            *self.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
            self.changed.notify_all();
        }

        fn arrivals(&self) -> i64 {
            self.arrived.load(Ordering::SeqCst)
        }
    }

    /// Tool that parks the request thread it runs on until the test says
    /// otherwise, so saturation is a fact rather than a race.
    struct GateTool {
        gate: Arc<Gate>,
    }

    impl Tool<TestContext> for GateTool {
        fn name(&self) -> &str {
            "gate"
        }
        fn description(&self) -> &str {
            "Blocks until the test releases it"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            self.gate.wait();
            Ok(CallToolResult::text("released"))
        }
    }

    // Tool that reports what the client said it could do, to prove one
    // client's declaration never reaches another
    struct CapsTool;
    impl Tool<TestContext> for CapsTool {
        fn name(&self) -> &str {
            "caps"
        }
        fn description(&self) -> &str {
            "Reports the client's declared capabilities"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _args: Value,
            _ctx: &mut TestContext,
            env: &ToolEnv,
        ) -> Result<CallToolResult> {
            Ok(CallToolResult::text(format!(
                "elicitation={}",
                env.client_capabilities().elicitation.is_some()
            )))
        }
    }

    // Counter tool that uses context
    struct CounterTool;
    impl Tool<TestContext> for CounterTool {
        fn name(&self) -> &str {
            "counter"
        }
        fn description(&self) -> &str {
            "Increment shared counter"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _args: Value,
            ctx: &mut TestContext,
            _env: &ToolEnv,
        ) -> Result<CallToolResult> {
            let val = ctx.counter.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(CallToolResult::text(format!("Counter: {}", val)))
        }
    }

    /// A client that carries its session id the way a conformant one does.
    ///
    /// Continuity across requests is a *session* property now, not a property
    /// of the listener: a client that never echoes `Mcp-Session-Id` gets a
    /// fresh session every time and inherits nothing from anybody.
    struct SessionClient {
        addr: String,
        id: Option<String>,
    }

    impl SessionClient {
        fn new(addr: &str) -> Self {
            Self {
                addr: addr.to_string(),
                id: None,
            }
        }

        fn post(&mut self, body: &str) -> (u16, String, String) {
            self.post_with(body, &[])
        }

        fn post_with(&mut self, body: &str, extra: &[(&str, &str)]) -> (u16, String, String) {
            let carried = self.id.clone();
            let mut headers: Vec<(&str, &str)> = extra.to_vec();
            if let Some(id) = carried.as_deref() {
                headers.push((SESSION_HEADER, id));
            }

            let (status, content_type, body, session) =
                http_request_full(&self.addr, "POST", "/mcp", body, &headers).unwrap();
            if let Some(session) = session {
                self.id = Some(session);
            }
            (status, content_type, body)
        }

        fn session_id(&self) -> Option<&str> {
            self.id.as_deref()
        }
    }

    /// Helper to make a raw HTTP request with an arbitrary method and headers
    fn http_request(
        addr: &str,
        method: &str,
        path: &str,
        body: &str,
        extra_headers: &[(&str, &str)],
    ) -> std::io::Result<(u16, String, String)> {
        http_request_full(addr, method, path, body, extra_headers)
            .map(|(status, content_type, body, _)| (status, content_type, body))
    }

    /// As [`http_request`], plus any `Mcp-Session-Id` the server handed back.
    fn http_request_full(
        addr: &str,
        method: &str,
        path: &str,
        body: &str,
        extra_headers: &[(&str, &str)],
    ) -> std::io::Result<(u16, String, String, Option<String>)> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;

        let extra: String = extra_headers
            .iter()
            .map(|(k, v)| format!("{}: {}\r\n", k, v))
            .collect();

        let request = format!(
            "{} {} HTTP/1.1\r\n\
             Host: {}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\
             {}Connection: close\r\n\
             \r\n\
             {}",
            method,
            path,
            addr,
            body.len(),
            extra,
            body
        );

        stream.write_all(request.as_bytes())?;
        stream.flush()?;

        let mut response = String::new();
        stream.read_to_string(&mut response)?;

        // Parse status code
        let status_line = response.lines().next().unwrap_or("");
        let status_code: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Parse content-type
        let content_type = response
            .lines()
            .find(|l| l.to_lowercase().starts_with("content-type:"))
            .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
            .unwrap_or_default();

        // Parse body (after empty line)
        let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();

        let session = response
            .lines()
            .find(|l| {
                l.to_lowercase()
                    .starts_with(&format!("{}:", SESSION_HEADER.to_lowercase()))
            })
            .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string());

        Ok((status_code, content_type, body, session))
    }

    /// Read exactly one HTTP response off a connection that is staying open.
    ///
    /// `read_to_string` works everywhere else because those requests ask for the
    /// connection to be closed, which is what ends the read. A reused connection
    /// never closes, so the response has to be framed by its own
    /// `Content-Length` instead.
    fn read_one_response(stream: &mut TcpStream) -> (u16, String) {
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            assert_eq!(
                stream.read(&mut byte).expect("reading response headers"),
                1,
                "the connection closed mid-response"
            );
            head.push(byte[0]);
        }

        let head = String::from_utf8(head).expect("headers are text");
        let status: u16 = head
            .lines()
            .next()
            .unwrap_or("")
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let length: usize = head
            .lines()
            .find(|l| l.to_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1)?.trim().parse().ok())
            .expect("a framed response carries its length");

        let mut body = vec![0u8; length];
        stream.read_exact(&mut body).expect("reading response body");
        (status, String::from_utf8(body).expect("the body is text"))
    }

    /// Helper to make raw HTTP POST request
    fn http_post(addr: &str, path: &str, body: &str) -> std::io::Result<(u16, String, String)> {
        http_request(addr, "POST", path, body, &[])
    }

    /// Helper to POST with an `Origin` header
    fn http_post_with_origin(
        addr: &str,
        path: &str,
        body: &str,
        origin: &str,
    ) -> std::io::Result<(u16, String, String)> {
        http_request(addr, "POST", path, body, &[("Origin", origin)])
    }

    /// Spin up a server on a fresh port with the given origin policy and return
    /// its address. The server thread is detached; it dies with the test binary.
    fn spawn_server(policy: OriginPolicy) -> String {
        spawn_server_with(
            policy,
            ServerConfig {
                name: "test-server".into(),
                version: "1.0.0".into(),
                instructions: None,
                ..Default::default()
            },
        )
    }

    /// [`spawn_server`] with the configuration under test.
    fn spawn_server_with(policy: OriginPolicy, config: ServerConfig) -> String {
        let addr = format!("127.0.0.1:{}", next_port());

        let server_addr = addr.clone();
        thread::spawn(move || {
            let counter = Arc::new(AtomicI64::new(0));
            let _ = HttpServer::new(config)
                .origin_policy(policy)
                .with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(EchoTool)?;
                    s.add_tool(PanicTool)?;
                    s.add_tool(LogLevelTool)?;
                    s.add_tool(CapsTool)?;
                    s.add_tool(SlowTaskTool)?;
                    s.add_tool(BlockingTaskTool)?;
                    s.enable_tasks(Default::default(), || TestContext {
                        counter: Arc::new(AtomicI64::new(0)),
                    });
                    Ok(())
                })
                .serve(&server_addr, move || TestContext {
                    counter: counter.clone(),
                });
        });

        thread::sleep(Duration::from_millis(100));
        addr
    }

    const PING: &str = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;

    //
    // Transport-level conformance
    //

    #[test]
    fn test_a_notification_is_202_with_no_body() {
        // "If the input is a JSON-RPC response or notification: If the server
        // accepts the input, the server MUST return HTTP status code 202
        // Accepted with no body." It used to answer `200 {}`, and `{}` is not a
        // JSON-RPC message, so a strict client fails parsing it.
        let addr = spawn_server(OriginPolicy::Loopback);

        let (status, _, body) = http_post(
            &addr,
            "/mcp",
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        )
        .unwrap();

        assert_eq!(status, 202);
        assert!(body.is_empty(), "202 must have no body, got {body:?}");
    }

    #[test]
    fn test_a_client_response_is_202_with_no_body() {
        let addr = spawn_server(OriginPolicy::Loopback);

        let (status, _, body) =
            http_post(&addr, "/mcp", r#"{"jsonrpc":"2.0","id":7,"result":{}}"#).unwrap();

        assert_eq!(status, 202);
        assert!(body.is_empty(), "202 must have no body, got {body:?}");
    }

    #[test]
    fn test_an_unparseable_body_is_400_parse_error() {
        // A client's syntax error is not the server's internal error.
        let addr = spawn_server(OriginPolicy::Loopback);

        let (status, _, body) = http_post(&addr, "/mcp", "{not json").unwrap();

        assert_eq!(status, 400);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32700);
    }

    #[test]
    fn test_a_body_less_post_is_400_parse_error() {
        // There is no request with no body at this layer - there is a request
        // whose body is empty, and an empty body is not a JSON-RPC message.
        // Worth pinning because the check that used to catch it lived in the
        // HTTP layer and now lives in the parser; the answer must not change.
        let addr = spawn_server(OriginPolicy::Loopback);

        let (status, _, body) = http_post(&addr, "/mcp", "").unwrap();

        assert_eq!(status, 400, "{body}");
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32700, "{body}");
    }

    #[test]
    fn test_a_batch_body_is_400_invalid_request() {
        let addr = spawn_server(OriginPolicy::Loopback);

        let (status, _, body) = http_post(
            &addr,
            "/mcp",
            r#"[{"jsonrpc":"2.0","id":1,"method":"ping"}]"#,
        )
        .unwrap();

        assert_eq!(status, 400);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32600);
        assert!(
            parsed["error"]["message"]
                .as_str()
                .unwrap()
                .contains("batching"),
            "{body}"
        );
    }

    #[test]
    fn test_the_endpoint_is_a_path_not_a_request_target() {
        // "The server MUST provide a single HTTP endpoint *path*". `tiny_http`
        // hands back the raw target, so `/mcp?sessionId=abc` used to 404.
        let addr = spawn_server(OriginPolicy::Loopback);

        for target in ["/mcp?sessionId=abc", "/mcp?", "/mcp#frag"] {
            let (status, _, body) = http_post(&addr, target, PING).unwrap();
            assert_eq!(status, 200, "{target} should reach the endpoint: {body}");
        }
    }

    #[test]
    fn test_a_panicking_tool_does_not_kill_the_accept_loop() {
        let addr = spawn_server(OriginPolicy::Loopback);

        // Caught at the dispatch layer, so the client gets a well-formed
        // JSON-RPC error rather than a dropped connection.
        let call = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"panic"}}"#;
        let (status, _, body) = http_post(&addr, "/mcp", call).unwrap();
        assert_eq!(status, 200);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32603, "{body}");
        assert!(
            parsed["error"]["message"]
                .as_str()
                .unwrap()
                .contains("panicked"),
            "{body}"
        );

        // The listener must still be there for everyone else.
        let (status, _, _) = http_post(&addr, "/mcp", PING).unwrap();
        assert_eq!(status, 200, "the accept loop died with the tool");
    }

    #[test]
    fn test_a_panic_outside_dispatch_does_not_kill_the_accept_loop() {
        // The outer guard's job: anything that unwinds through
        // `process_request` - a setup closure, a context factory, a `schema()`
        // implementation - used to terminate the listener thread.
        let addr = format!("127.0.0.1:{}", next_port());

        let server_addr = addr.clone();
        thread::spawn(move || {
            let counter = Arc::new(AtomicI64::new(0));
            let _ = HttpServer::new(ServerConfig::default())
                .with_tools(|_: &mut Server<TestContext>| panic!("setup exploded"))
                .serve(&server_addr, move || TestContext {
                    counter: counter.clone(),
                });
        });
        thread::sleep(Duration::from_millis(100));

        let (status, _, body) = http_post(&addr, "/mcp", PING).unwrap();
        assert_eq!(status, 500);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32603);

        let (status, _, _) = http_post(&addr, "/mcp", PING).unwrap();
        assert_eq!(status, 500, "the accept loop died with the setup closure");
    }

    //
    // Per-connection state
    //

    #[test]
    fn test_set_level_survives_the_request_that_set_it() {
        // logging §Message Flow shows `setLevel(error)` followed by "Only sends
        // error level and above" for subsequent activity. A fresh `Server` per
        // request meant the setting was acknowledged and then dropped.
        let addr = spawn_server(OriginPolicy::Loopback);
        let mut client = SessionClient::new(&addr);

        let call = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"level"}}"#;
        let (_, _, before) = client.post(call);
        let parsed: Value = serde_json::from_str(&before).unwrap();
        assert_eq!(parsed["result"]["content"][0]["text"], "info");

        let (status, _, _) = client.post(
            r#"{"jsonrpc":"2.0","id":2,"method":"logging/setLevel","params":{"level":"error"}}"#,
        );
        assert_eq!(status, 200);
        assert!(
            client.session_id().is_some(),
            "a request that established state is told which session holds it"
        );

        let (_, _, after) = client.post(call);
        let parsed: Value = serde_json::from_str(&after).unwrap();
        assert_eq!(parsed["result"]["content"][0]["text"], "error");
    }

    #[test]
    fn test_one_clients_log_level_does_not_reach_another() {
        // The state that S5 hoisted onto the `HttpServer` was shared by every
        // client on the listener, so `logging/setLevel` from one changed what
        // an unrelated one received - on `serve_with_auth`, across tenants.
        let addr = spawn_server(OriginPolicy::Loopback);
        let call = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"level"}}"#;

        let mut noisy = SessionClient::new(&addr);
        let (status, _, _) = noisy.post(
            r#"{"jsonrpc":"2.0","id":2,"method":"logging/setLevel","params":{"level":"error"}}"#,
        );
        assert_eq!(status, 200);

        let mut bystander = SessionClient::new(&addr);
        let (_, _, body) = bystander.post(call);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            parsed["result"]["content"][0]["text"], "info",
            "a client that set nothing keeps the server default: {body}"
        );

        // And the client that did set it still has it.
        let (_, _, body) = noisy.post(call);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["result"]["content"][0]["text"], "error", "{body}");
    }

    #[test]
    fn test_one_clients_capabilities_do_not_reach_another() {
        // Same bleed, worse consequence: the server believed a client had
        // declared capabilities it never sent. "Servers MUST NOT send
        // elicitation requests with modes that are not supported by the
        // client", and `ToolEnv::client_capabilities()` is public API that
        // downstream tools branch on.
        let addr = spawn_server(OriginPolicy::Loopback);
        let call = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"caps"}}"#;

        let mut declared = SessionClient::new(&addr);
        let (status, _, _) = declared.post(
            r#"{"jsonrpc":"2.0","id":9,"method":"initialize","params":{
                "protocolVersion":"2025-06-18",
                "capabilities":{"elicitation":{},"sampling":{}},
                "clientInfo":{"name":"a","version":"1"}
            }}"#,
        );
        assert_eq!(status, 200);
        let (_, _, body) = declared.post(call);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["result"]["content"][0]["text"], "elicitation=true");

        let mut bystander = SessionClient::new(&addr);
        let (_, _, body) = bystander.post(call);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            parsed["result"]["content"][0]["text"], "elicitation=false",
            "this client has never sent initialize: {body}"
        );
    }

    #[test]
    fn test_an_unknown_session_id_is_404() {
        // "The server MAY terminate the session at any time, after which it
        // MUST respond to requests containing that session ID with HTTP 404."
        let addr = spawn_server(OriginPolicy::Loopback);

        let (status, _, _) =
            http_request(&addr, "POST", "/mcp", PING, &[(SESSION_HEADER, "nope")]).unwrap();

        assert_eq!(status, 404);
    }

    #[test]
    fn test_delete_terminates_a_session() {
        // "Clients that no longer need a particular session SHOULD send an
        // HTTP DELETE ... to explicitly terminate it."
        let addr = spawn_server(OriginPolicy::Loopback);
        let mut client = SessionClient::new(&addr);

        client.post(
            r#"{"jsonrpc":"2.0","id":1,"method":"logging/setLevel","params":{"level":"error"}}"#,
        );
        let id = client
            .session_id()
            .expect("a session was established")
            .to_string();

        let (status, _, _) =
            http_request(&addr, "DELETE", "/mcp", "", &[(SESSION_HEADER, &id)]).unwrap();
        assert_eq!(status, 204);

        // Gone means gone.
        let (status, _, _) =
            http_request(&addr, "POST", "/mcp", PING, &[(SESSION_HEADER, &id)]).unwrap();
        assert_eq!(status, 404);

        let (status, _, _) =
            http_request(&addr, "DELETE", "/mcp", "", &[(SESSION_HEADER, &id)]).unwrap();
        assert_eq!(status, 404, "a second DELETE has nothing to terminate");
    }

    #[test]
    fn test_a_blocking_tasks_result_does_not_hold_up_anybody_else() {
        // The shipping blocker: `HttpServer` processed requests one at a time
        // on the accept loop, so a `tasks/result` held that thread - and with
        // it every other client - until the task terminated or its TTL elapsed.
        // A `ping` sent on another connection waited exactly as long. TTL is
        // client-requested and capped at an hour by default, so one well-formed,
        // fully authorized request was a complete denial of service.
        let addr = spawn_server(OriginPolicy::Loopback);
        let mut client = SessionClient::new(&addr);

        let (_, _, body) = client.post(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"blocking","task":{}}}"#,
        );
        let parsed: Value = serde_json::from_str(&body).unwrap();
        let task_id = parsed["result"]["task"]["taskId"]
            .as_str()
            .unwrap()
            .to_string();
        let session = client.session_id().unwrap().to_string();

        let blocked_addr = addr.clone();
        let blocked = thread::spawn(move || {
            let started = Instant::now();
            let (body, _) = {
                let request = format!(
                    r#"{{"jsonrpc":"2.0","id":2,"method":"tasks/result","params":{{"taskId":"{task_id}"}}}}"#
                );
                let (_, _, body, session) = http_request_full(
                    &blocked_addr,
                    "POST",
                    "/mcp",
                    &request,
                    &[(SESSION_HEADER, &session)],
                )
                .unwrap();
                (body, session)
            };
            (started.elapsed(), body)
        });

        // Give the blocking call time to actually be in flight.
        thread::sleep(Duration::from_millis(200));

        let started = Instant::now();
        let (status, _, _) = http_post(&addr, "/mcp", PING).unwrap();
        let ping_took = started.elapsed();

        assert_eq!(status, 200);
        assert!(
            ping_took < Duration::from_millis(600),
            "an unrelated ping waited {ping_took:?} on somebody else's tasks/result"
        );

        let (blocked_took, body) = blocked.join().unwrap();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            parsed["result"]["content"][0]["text"], "eventually",
            "{body}"
        );
        assert!(
            blocked_took > ping_took,
            "the blocking call really did block: {blocked_took:?}"
        );
    }

    #[test]
    fn test_a_saturated_server_says_so_rather_than_queueing_without_limit() {
        // The pool is bounded in both directions, so there is a state where a
        // request arrives with nowhere to run. The two alternatives are worse:
        // an unbounded queue is a peer-controlled amount of memory and a
        // growing pile of requests nobody is getting to, and blocking the
        // accept loop makes the listener as slow as its slowest request, which
        // is the failure this whole design exists to prevent.
        let gate = Arc::new(Gate::default());
        let addr = format!("127.0.0.1:{}", next_port());

        let server_addr = addr.clone();
        let their_gate = Arc::clone(&gate);
        thread::spawn(move || {
            let counter = Arc::new(AtomicI64::new(0));
            let _ = HttpServer::new(ServerConfig::default())
                // One thread, and room for one more request behind it.
                .pool_size(1)
                .with_tools(move |s: &mut Server<TestContext>| {
                    s.add_tool(GateTool {
                        gate: Arc::clone(&their_gate),
                    })?;
                    Ok(())
                })
                .serve(&server_addr, move || TestContext {
                    counter: counter.clone(),
                });
        });
        thread::sleep(Duration::from_millis(100));

        // Occupy the only worker, and wait until it really is occupied.
        let blocked_addr = addr.clone();
        let blocked = thread::spawn(move || {
            http_post(
                &blocked_addr,
                "/mcp",
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"gate"}}"#,
            )
            .unwrap()
        });
        for _ in 0..500 {
            if gate.arrivals() == 1 {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(gate.arrivals(), 1, "the worker never picked up the request");

        // Four more, against one worker and one queue slot.
        let pings: Vec<_> = (0..4)
            .map(|_| {
                let addr = addr.clone();
                thread::spawn(move || http_post(&addr, "/mcp", PING).unwrap())
            })
            .collect();
        thread::sleep(Duration::from_millis(300));
        gate.open();

        let answers: Vec<(u16, String, String)> =
            pings.into_iter().map(|p| p.join().unwrap()).collect();
        let statuses: Vec<u16> = answers.iter().map(|(status, ..)| *status).collect();
        let shed: Vec<&(u16, String, String)> = answers
            .iter()
            .filter(|(status, ..)| *status == 503)
            .collect();
        let served = statuses.iter().filter(|status| **status == 200).count();

        assert!(
            shed.len() >= 2,
            "a saturated server must refuse rather than absorb: {statuses:?}"
        );
        assert_eq!(
            shed.len() + served,
            4,
            "every request got one of the two answers: {statuses:?}"
        );
        assert!(
            served >= 1,
            "the queued request was still served once a worker freed up: {statuses:?}"
        );

        // The refusal is a JSON-RPC body like every other rejection here, not a
        // dropped connection or a bare status line.
        let (_, content_type, body) = shed[0];
        assert_eq!(content_type, "application/json");
        let parsed: Value = serde_json::from_str(body).unwrap();
        assert_eq!(parsed["error"]["code"], -32603, "{body}");
        assert!(
            parsed["error"]["message"]
                .as_str()
                .unwrap()
                .contains("busy"),
            "{body}"
        );

        // And the server is unharmed by having refused.
        let (status, _, body) = http_post(&addr, "/mcp", PING).unwrap();
        assert_eq!(status, 200, "{body}");

        let (_, _, body) = blocked.join().unwrap();
        assert!(body.contains("released"), "{body}");
    }

    #[test]
    fn test_two_requests_on_one_connection_are_both_answered() {
        // Every other test here closes the connection after one request. A
        // request now travels to a worker thread to be answered, and `tiny_http`
        // only reads the next request on a connection once the previous one has
        // been responded to - so a handoff that lost the request, or answered
        // out of band, would wedge the connection rather than fail loudly.
        let addr = spawn_server(OriginPolicy::Loopback);

        let mut stream = TcpStream::connect(&addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        for id in 1..=2 {
            let body = format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"ping"}}"#);
            let request = format!(
                "POST /mcp HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\n\r\n{}",
                addr,
                body.len(),
                body
            );
            stream.write_all(request.as_bytes()).unwrap();
            stream.flush().unwrap();

            let (status, response_body) = read_one_response(&mut stream);
            assert_eq!(status, 200, "request {id} on a reused connection");
            let parsed: Value = serde_json::from_str(&response_body).unwrap();
            assert_eq!(parsed["id"], id, "answers stay matched to their requests");
        }
    }

    #[test]
    fn test_a_chunked_body_over_the_limit_is_refused() {
        // A chunked body declares no length, so the `Content-Length` check has
        // nothing to look at and the cap on the read itself is the only thing
        // standing between a peer and an unbounded allocation.
        let addr = spawn_server_with(
            OriginPolicy::Loopback,
            ServerConfig {
                name: "test-server".into(),
                max_message_bytes: 1024,
                ..Default::default()
            },
        );

        let mut stream = TcpStream::connect(&addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let chunk = "x".repeat(4096);
        let request = format!(
            "POST /mcp HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
             Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
            addr,
            chunk.len(),
            chunk
        );
        stream.write_all(request.as_bytes()).unwrap();
        stream.flush().unwrap();

        let mut response = String::new();
        let _ = stream.read_to_string(&mut response);
        let status: u16 = response
            .lines()
            .next()
            .unwrap_or("")
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        assert_eq!(status, 413, "{response}");
        assert!(response.contains("1024"), "{response}");
    }

    #[cfg(feature = "tls")]
    #[test]
    fn test_serve_tls_refuses_a_certificate_it_cannot_use() {
        // The one thing a mis-wired TLS path would do silently is bind *plain*
        // HTTP on the HTTPS port. An unusable certificate has to be an error,
        // which means the request never reached a listener at all.
        let addr = format!("127.0.0.1:{}", next_port());

        let error = HttpServer::new(ServerConfig::default())
            .with_tools(|s: &mut Server<TestContext>| {
                s.add_tool(EchoTool)?;
                Ok(())
            })
            .serve_tls(
                &addr,
                b"-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----\n"
                    .to_vec(),
                b"-----BEGIN PRIVATE KEY-----\nnot a key\n-----END PRIVATE KEY-----\n".to_vec(),
                || TestContext {
                    counter: Arc::new(AtomicI64::new(0)),
                },
            )
            .unwrap_err();

        assert!(
            error.to_string().contains("Failed to start HTTP server"),
            "{error}"
        );
        assert!(
            TcpStream::connect(&addr).is_err(),
            "nothing may be listening after a refused certificate"
        );
    }

    #[test]
    fn test_a_body_over_the_limit_is_refused_before_it_is_read() {
        // stdio and Unix capped a message at 8 MiB while HTTP - the transport
        // actually exposed to the network - accepted 48 MiB and echoed it back,
        // which made it an amplifier as well as a sink.
        let addr = spawn_server_with(
            OriginPolicy::Loopback,
            ServerConfig {
                name: "test-server".into(),
                max_message_bytes: 4096,
                ..Default::default()
            },
        );

        let oversized = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"echo","arguments":{{"message":"{}"}}}}}}"#,
            "x".repeat(8192)
        );
        let (status, _, body) = http_post(&addr, "/mcp", &oversized).unwrap();

        assert_eq!(status, 413, "{body}");
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32600);
        assert!(
            parsed["error"]["message"]
                .as_str()
                .unwrap()
                .contains("4096"),
            "{body}"
        );

        // And a body under the limit still works, so the cap is a cap and not
        // a wall.
        let (status, _, _) = http_post(&addr, "/mcp", PING).unwrap();
        assert_eq!(status, 200);
    }

    #[test]
    fn test_a_lying_content_length_cannot_get_past_the_cap() {
        // The declared length is checked first, but a chunked body declares
        // none and a `Content-Length` can simply be wrong, so the read is
        // capped as well.
        let addr = spawn_server_with(
            OriginPolicy::Loopback,
            ServerConfig {
                name: "test-server".into(),
                max_message_bytes: 1024,
                ..Default::default()
            },
        );

        let mut stream = TcpStream::connect(&addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let body = "x".repeat(4096);
        let request = format!(
            "POST /mcp HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{}",
            addr,
            body.len(),
            body
        );
        // Announce a small body, send a large one.
        let request = request.replace(
            &format!("Content-Length: {}", body.len()),
            "Content-Length: 4096",
        );
        stream.write_all(request.as_bytes()).unwrap();
        stream.flush().unwrap();

        let mut response = String::new();
        let _ = stream.read_to_string(&mut response);
        let status: u16 = response
            .lines()
            .next()
            .unwrap_or("")
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        assert_eq!(status, 413, "{response}");
    }

    #[test]
    fn test_require_initialization_works_over_http() {
        // M10's fix was per-`Server` state, and HTTP rebuilds the `Server` for
        // every request without carrying `initialized` across - so the flag
        // made the transport permanently unusable: `initialize` was answered,
        // and every request after it, for every client, forever, was refused.
        let addr = spawn_server_with(
            OriginPolicy::Loopback,
            ServerConfig {
                name: "test-server".into(),
                require_initialization: true,
                ..Default::default()
            },
        );
        let mut client = SessionClient::new(&addr);

        // Before the handshake, refused - which is the flag doing its job.
        let (_, _, body) = client.post(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32600, "{body}");

        let (status, _, _) = client.post(
            r#"{"jsonrpc":"2.0","id":2,"method":"initialize","params":{
                "protocolVersion":"2025-06-18",
                "capabilities":{},
                "clientInfo":{"name":"a","version":"1"}
            }}"#,
        );
        assert_eq!(status, 200);

        // After it, allowed - on the same session.
        let (_, _, body) = client.post(r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert!(parsed["result"]["tools"].is_array(), "{body}");

        // A different client has not initialized, and is still refused.
        let mut stranger = SessionClient::new(&addr);
        let (_, _, body) = stranger.post(r#"{"jsonrpc":"2.0","id":4,"method":"tools/list"}"#);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32600, "{body}");
    }

    #[test]
    fn test_a_task_augmented_call_has_a_deterministic_content_type() {
        // The task gate used to open when the response was *buffered*, not when
        // it was read, so a worker's `notifications/tasks/status` landing in
        // between turned the same call from `application/json` with one message
        // into `text/event-stream` with two.
        let addr = spawn_server(OriginPolicy::Loopback);
        let call =
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"slow","task":{}}}"#;

        for _ in 0..20 {
            let mut client = SessionClient::new(&addr);
            let (status, content_type, body) = client.post(call);
            assert_eq!(status, 200);
            assert_eq!(content_type, "application/json", "{body}");
        }
    }

    #[test]
    fn test_a_panic_does_not_break_the_session_it_happened_in() {
        // Nothing panic-capable runs under the session lock, but a lock that
        // answers `-32603 Session lock poisoned` forever if anything ever did
        // is a worse failure than the panic it is reacting to.
        let addr = spawn_server(OriginPolicy::Loopback);
        let mut client = SessionClient::new(&addr);

        client.post(
            r#"{"jsonrpc":"2.0","id":1,"method":"logging/setLevel","params":{"level":"error"}}"#,
        );
        let (_, _, body) = client
            .post(r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"panic"}}"#);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32603, "{body}");

        let (status, _, body) = client
            .post(r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"level"}}"#);
        assert_eq!(status, 200);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            parsed["result"]["content"][0]["text"], "error",
            "the session survived the panic intact: {body}"
        );
    }

    #[test]
    fn test_a_stateless_request_is_not_handed_a_session_id() {
        // Anyone who can reach the endpoint could otherwise churn the session
        // table one ping at a time.
        let addr = spawn_server(OriginPolicy::Loopback);

        let (status, _, _, session) = http_request_full(&addr, "POST", "/mcp", PING, &[]).unwrap();

        assert_eq!(status, 200);
        assert!(session.is_none(), "a ping establishes nothing: {session:?}");
    }

    #[test]
    fn test_client_capabilities_survive_the_initialize_request() {
        let addr = spawn_server(OriginPolicy::Loopback);

        let init = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{"elicitation":{"form":{}}},"clientInfo":{"name":"t","version":"1"}}}"#;
        let (status, _, body) = http_post(&addr, "/mcp", init).unwrap();
        assert_eq!(status, 200);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["result"]["protocolVersion"], "2025-11-25");

        // A second `initialize` on the same connection must see what the first
        // negotiated rather than starting from nothing.
        let (_, _, body) = http_post(&addr, "/mcp", init).unwrap();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["result"]["protocolVersion"], "2025-11-25");
    }

    #[test]
    fn test_a_task_is_still_there_on_the_next_request() {
        // The store used to go out of scope with the request that created it,
        // so `tools/call` handed back a `taskId` and every later `tasks/get`,
        // `tasks/result` and `tasks/cancel` answered -32602 "Task not found".
        let addr = spawn_server(OriginPolicy::Loopback);
        let mut client = SessionClient::new(&addr);

        let call =
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"slow","task":{}}}"#;
        let (status, _, body) = client.post(call);
        assert_eq!(status, 200);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        let task_id = parsed["result"]["task"]["taskId"]
            .as_str()
            .expect("a task id")
            .to_string();
        assert!(
            client.session_id().is_some(),
            "the response says which session the task lives in"
        );

        let result = format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"tasks/result","params":{{"taskId":"{task_id}"}}}}"#
        );
        let (status, _, body) = client.post(&result);
        assert_eq!(status, 200);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["result"]["content"][0]["text"], "finished", "{body}");
    }

    #[test]
    fn test_a_task_belongs_to_the_session_that_created_it() {
        // The store is per session, not per process: another client cannot
        // resolve a task id it was never given a session for.
        let addr = spawn_server(OriginPolicy::Loopback);
        let mut owner = SessionClient::new(&addr);

        let (_, _, body) = owner.post(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"slow","task":{}}}"#,
        );
        let parsed: Value = serde_json::from_str(&body).unwrap();
        let task_id = parsed["result"]["task"]["taskId"]
            .as_str()
            .unwrap()
            .to_string();

        let mut stranger = SessionClient::new(&addr);
        let (_, _, body) = stranger.post(&format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"tasks/get","params":{{"taskId":"{task_id}"}}}}"#
        ));
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32602, "{body}");
    }

    #[test]
    fn test_an_unauthenticated_http_server_does_not_declare_tasks_list() {
        // "Receivers that cannot identify requestors SHOULD NOT declare the
        // `tasks.list` capability" - several clients share this endpoint and
        // nothing tells them apart.
        let addr = spawn_server(OriginPolicy::Loopback);

        let init = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}"#;
        let (_, _, body) = http_post(&addr, "/mcp", init).unwrap();
        let parsed: Value = serde_json::from_str(&body).unwrap();

        let tasks = &parsed["result"]["capabilities"]["tasks"];
        assert!(!tasks.is_null(), "{body}");
        assert!(tasks["list"].is_null(), "{body}");
        assert!(!tasks["cancel"].is_null(), "{body}");

        // And the method itself refuses rather than listing everyone's work.
        let (_, _, body) = http_post(
            &addr,
            "/mcp",
            r#"{"jsonrpc":"2.0","id":2,"method":"tasks/list"}"#,
        )
        .unwrap();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32601, "{body}");
    }

    #[test]
    fn test_request_bodies_are_only_logged_at_debug() {
        // Logging §Security: log messages MUST NOT contain credentials or PII,
        // and tool arguments routinely carry both.
        let quiet: HttpServer<TestContext> = HttpServer::new(ServerConfig::default());
        assert!(!quiet.logs_bodies());

        let verbose: HttpServer<TestContext> = HttpServer::new(ServerConfig {
            default_log_level: crate::server::LogLevel::Debug,
            ..Default::default()
        });
        assert!(verbose.logs_bodies());
    }

    #[test]
    fn test_origin_absent_is_allowed() {
        // Non-browser clients send no Origin at all. They must keep working.
        let addr = spawn_server(OriginPolicy::Loopback);
        let (status, _, _) = http_post(&addr, "/mcp", PING).unwrap();
        assert_eq!(status, 200);
    }

    #[test]
    fn test_origin_loopback_allowed() {
        let addr = spawn_server(OriginPolicy::Loopback);
        for origin in [
            "http://localhost",
            "http://localhost:5173",
            "http://127.0.0.1:8080",
            "https://[::1]:3000",
        ] {
            let (status, _, body) = http_post_with_origin(&addr, "/mcp", PING, origin).unwrap();
            assert_eq!(status, 200, "origin {origin} should be allowed: {body}");
        }
    }

    #[test]
    fn test_origin_remote_rejected_with_403() {
        let addr = spawn_server(OriginPolicy::Loopback);

        // The DNS rebinding shape: attacker page on a public origin.
        let (status, content_type, body) =
            http_post_with_origin(&addr, "/mcp", PING, "https://evil.example.com").unwrap();

        assert_eq!(status, 403);
        assert_eq!(content_type, "application/json");

        // Body is a JSON-RPC error response with no `id`.
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["jsonrpc"], "2.0");
        assert!(parsed.get("id").is_none());
        assert_eq!(parsed["error"]["code"], -32600);
        assert!(
            parsed["error"]["message"]
                .as_str()
                .unwrap()
                .contains("evil.example.com")
        );
    }

    #[test]
    fn test_origin_lookalike_hostname_rejected() {
        let addr = spawn_server(OriginPolicy::Loopback);
        for origin in [
            "http://localhost.evil.example.com",
            "http://notlocalhost",
            "http://192.168.1.10:3000",
            "null",
        ] {
            let (status, _, _) = http_post_with_origin(&addr, "/mcp", PING, origin).unwrap();
            assert_eq!(status, 403, "origin {origin} should be rejected");
        }
    }

    #[test]
    fn test_origin_allowlist_policy() {
        let addr = spawn_server(OriginPolicy::allowlist(["https://app.example.com"]));

        let (status, _, _) =
            http_post_with_origin(&addr, "/mcp", PING, "https://app.example.com").unwrap();
        assert_eq!(status, 200);

        // Loopback is not implicitly allowed once an allowlist is set.
        let (status, _, _) =
            http_post_with_origin(&addr, "/mcp", PING, "http://localhost:3000").unwrap();
        assert_eq!(status, 403);
    }

    #[test]
    fn test_origin_any_policy_allows_everything() {
        let addr = spawn_server(OriginPolicy::Any);
        let (status, _, _) =
            http_post_with_origin(&addr, "/mcp", PING, "https://evil.example.com").unwrap();
        assert_eq!(status, 200);
    }

    #[test]
    fn test_origin_checked_before_body_is_read() {
        // A rejected Origin must not reach tool dispatch.
        let addr = spawn_server(OriginPolicy::Loopback);
        let call = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"message":"pwned"}}}"#;
        let (status, _, body) =
            http_post_with_origin(&addr, "/mcp", call, "https://evil.example.com").unwrap();

        assert_eq!(status, 403);
        assert!(!body.contains("pwned"));
    }

    #[test]
    fn test_non_post_methods_rejected_with_405() {
        // DELETE is the one exception: transports §Session Management gives it
        // a meaning, so it is answered rather than refused.
        let addr = spawn_server(OriginPolicy::Loopback);
        for method in ["GET", "PUT", "PATCH", "HEAD"] {
            let (status, _, _) = http_request(&addr, method, "/mcp", "", &[]).unwrap();
            assert_eq!(status, 405, "{method} should be rejected");
        }
    }

    #[test]
    fn test_protocol_version_header_accepted_when_supported() {
        let addr = spawn_server(OriginPolicy::Loopback);
        for version in ["2025-11-25", "2025-06-18", "2025-03-26"] {
            let (status, _, _) = http_request(
                &addr,
                "POST",
                "/mcp",
                PING,
                &[("MCP-Protocol-Version", version)],
            )
            .unwrap();
            assert_eq!(status, 200, "{version} should be accepted");
        }
    }

    #[test]
    fn test_absent_protocol_version_header_is_allowed() {
        // "if the server does not receive an MCP-Protocol-Version header [...]
        // the server SHOULD assume protocol version 2025-03-26" - which we
        // still support, so the request proceeds.
        let addr = spawn_server(OriginPolicy::Loopback);
        let (status, _, _) = http_post(&addr, "/mcp", PING).unwrap();
        assert_eq!(status, 200);
    }

    #[test]
    fn test_unsupported_protocol_version_header_is_400() {
        // "If the server receives a request with an invalid or unsupported
        // MCP-Protocol-Version, it MUST respond with 400 Bad Request."
        let addr = spawn_server(OriginPolicy::Loopback);
        for version in ["2024-11-05", "2026-07-28", "garbage"] {
            let (status, _, body) = http_request(
                &addr,
                "POST",
                "/mcp",
                PING,
                &[("MCP-Protocol-Version", version)],
            )
            .unwrap();

            assert_eq!(status, 400, "{version} should be rejected");
            let parsed: Value = serde_json::from_str(&body).unwrap();
            let message = parsed["error"]["message"].as_str().unwrap();
            assert!(message.contains(version), "{message}");
            // The error names what we do support, so the client can retry.
            assert!(message.contains("2025-11-25"), "{message}");
        }
    }

    #[test]
    fn test_error_bodies_are_jsonrpc() {
        let addr = spawn_server(OriginPolicy::Loopback);

        let (status, content_type, body) = http_post(&addr, "/nope", PING).unwrap();
        assert_eq!(status, 404);
        assert_eq!(content_type, "application/json");
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"]["message"], "Not Found");
    }

    #[test]
    fn test_http_server_ping() {
        let port = next_port();
        let addr = format!("127.0.0.1:{}", port);

        let config = ServerConfig {
            name: "test-server".into(),
            version: "1.0.0".into(),
            instructions: None,
            ..Default::default()
        };

        let server_addr = addr.clone();
        let handle = thread::spawn(move || {
            let counter = Arc::new(AtomicI64::new(0));
            let _ = HttpServer::new(config)
                .with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(EchoTool)?;
                    Ok(())
                })
                .serve(&server_addr, move || TestContext {
                    counter: counter.clone(),
                });
        });

        // Give server time to start
        thread::sleep(Duration::from_millis(100));

        // Test ping
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let (status, content_type, response) = http_post(&addr, "/mcp", body).unwrap();

        assert_eq!(status, 200);
        assert_eq!(content_type, "application/json");
        assert!(response.contains("\"result\":{}"));

        drop(handle); // Server will stop when test ends
    }

    #[test]
    fn test_http_server_tool_call_json_response() {
        let port = next_port();
        let addr = format!("127.0.0.1:{}", port);

        let config = ServerConfig {
            name: "test-server".into(),
            version: "1.0.0".into(),
            instructions: None,
            ..Default::default()
        };

        let server_addr = addr.clone();
        let handle = thread::spawn(move || {
            let counter = Arc::new(AtomicI64::new(0));
            let _ = HttpServer::new(config)
                .with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(EchoTool)?;
                    Ok(())
                })
                .serve(&server_addr, move || TestContext {
                    counter: counter.clone(),
                });
        });

        thread::sleep(Duration::from_millis(100));

        // Call echo tool (no notifications -> JSON response)
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"message":"hello"}}}"#;
        let (status, content_type, response) = http_post(&addr, "/mcp", body).unwrap();

        assert_eq!(status, 200);
        assert_eq!(content_type, "application/json");
        assert!(response.contains("Echo: hello"));

        drop(handle);
    }

    #[test]
    fn test_http_server_tool_call_sse_response() {
        let port = next_port();
        let addr = format!("127.0.0.1:{}", port);

        let config = ServerConfig {
            name: "test-server".into(),
            version: "1.0.0".into(),
            instructions: None,
            ..Default::default()
        };

        let server_addr = addr.clone();
        let handle = thread::spawn(move || {
            let counter = Arc::new(AtomicI64::new(0));
            let _ = HttpServer::new(config)
                .with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(NotifyTool)?;
                    Ok(())
                })
                .serve(&server_addr, move || TestContext {
                    counter: counter.clone(),
                });
        });

        thread::sleep(Duration::from_millis(100));

        // Call notify tool (has notifications -> SSE response)
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"notify"}}"#;
        let (status, content_type, response) = http_post(&addr, "/mcp", body).unwrap();

        assert_eq!(status, 200);
        assert_eq!(content_type, "text/event-stream");
        assert!(response.contains("data: "));
        assert!(response.contains("notification from tool"));
        assert!(response.contains("done with notification"));

        drop(handle);
    }

    #[test]
    fn test_http_server_wrong_endpoint_404() {
        let port = next_port();
        let addr = format!("127.0.0.1:{}", port);

        let config = ServerConfig {
            name: "test-server".into(),
            version: "1.0.0".into(),
            instructions: None,
            ..Default::default()
        };

        let server_addr = addr.clone();
        let handle = thread::spawn(move || {
            let counter = Arc::new(AtomicI64::new(0));
            let _ = HttpServer::new(config)
                .with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(EchoTool)?;
                    Ok(())
                })
                .serve(&server_addr, move || TestContext {
                    counter: counter.clone(),
                });
        });

        thread::sleep(Duration::from_millis(100));

        // Wrong endpoint
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let (status, _, response) = http_post(&addr, "/wrong", body).unwrap();

        assert_eq!(status, 404);
        assert!(response.contains("Not Found"));

        drop(handle);
    }

    #[test]
    fn test_http_server_custom_endpoint() {
        let port = next_port();
        let addr = format!("127.0.0.1:{}", port);

        let config = ServerConfig {
            name: "test-server".into(),
            version: "1.0.0".into(),
            instructions: None,
            ..Default::default()
        };

        let server_addr = addr.clone();
        let handle = thread::spawn(move || {
            let counter = Arc::new(AtomicI64::new(0));
            let _ = HttpServer::new(config)
                .endpoint("/custom/path")
                .with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(EchoTool)?;
                    Ok(())
                })
                .serve(&server_addr, move || TestContext {
                    counter: counter.clone(),
                });
        });

        thread::sleep(Duration::from_millis(100));

        // Custom endpoint should work
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let (status, _, _) = http_post(&addr, "/custom/path", body).unwrap();
        assert_eq!(status, 200);

        // Default endpoint should 404
        let (status, _, _) = http_post(&addr, "/mcp", body).unwrap();
        assert_eq!(status, 404);

        drop(handle);
    }

    #[test]
    fn test_http_server_shared_context() {
        let port = next_port();
        let addr = format!("127.0.0.1:{}", port);

        let config = ServerConfig {
            name: "test-server".into(),
            version: "1.0.0".into(),
            instructions: None,
            ..Default::default()
        };

        // Shared counter
        let shared_counter = Arc::new(AtomicI64::new(0));
        let counter_for_server = shared_counter.clone();

        let server_addr = addr.clone();
        let handle = thread::spawn(move || {
            let _ = HttpServer::new(config)
                .with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(CounterTool)?;
                    Ok(())
                })
                .serve(&server_addr, move || TestContext {
                    counter: counter_for_server.clone(),
                });
        });

        thread::sleep(Duration::from_millis(100));

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"counter"}}"#;

        // First call
        let (_, _, response) = http_post(&addr, "/mcp", body).unwrap();
        assert!(response.contains("Counter: 1"));

        // Second call - should increment
        let (_, _, response) = http_post(&addr, "/mcp", body).unwrap();
        assert!(response.contains("Counter: 2"));

        // Verify shared counter
        assert_eq!(shared_counter.load(Ordering::SeqCst), 2);

        drop(handle);
    }

    #[test]
    fn test_http_server_tools_list() {
        let port = next_port();
        let addr = format!("127.0.0.1:{}", port);

        let config = ServerConfig {
            name: "test-server".into(),
            version: "1.0.0".into(),
            instructions: None,
            ..Default::default()
        };

        let server_addr = addr.clone();
        let handle = thread::spawn(move || {
            let counter = Arc::new(AtomicI64::new(0));
            let _ = HttpServer::new(config)
                .with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(EchoTool)?;
                    s.add_tool(NotifyTool)?;
                    s.add_tool(CounterTool)?;
                    Ok(())
                })
                .serve(&server_addr, move || TestContext {
                    counter: counter.clone(),
                });
        });

        thread::sleep(Duration::from_millis(100));

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
        let (status, _, response) = http_post(&addr, "/mcp", body).unwrap();

        assert_eq!(status, 200);
        assert!(response.contains("\"name\":\"echo\""));
        assert!(response.contains("\"name\":\"notify\""));
        assert!(response.contains("\"name\":\"counter\""));

        drop(handle);
    }

    #[cfg(feature = "auth")]
    mod auth_tests {
        use super::*;
        use crate::auth::JwtValidator;
        use jsonwebtoken::{Algorithm, EncodingKey, Header as JwtHeader, encode};
        use serde::Serialize;

        const SECRET: &[u8] = b"test-secret-key";

        struct AuthContext {
            user_id: String,
        }

        struct WhoamiTool;
        impl Tool<AuthContext> for WhoamiTool {
            fn name(&self) -> &str {
                "whoami"
            }
            fn description(&self) -> &str {
                "Return user info"
            }
            fn schema(&self) -> Value {
                serde_json::json!({ "type": "object", "properties": {} })
            }
            fn execute(
                &self,
                _args: Value,
                ctx: &mut AuthContext,
                _env: &ToolEnv,
            ) -> Result<CallToolResult> {
                Ok(CallToolResult::text(format!("User: {}", ctx.user_id)))
            }
        }

        fn make_token(user_id: &str, tenant_id: &str) -> String {
            make_token_with(user_id, tenant_id, Some(RESOURCE), None)
        }

        /// A token for this server, with optional audience and scopes.
        fn make_token_with(
            user_id: &str,
            tenant_id: &str,
            audience: Option<&str>,
            scope: Option<&str>,
        ) -> String {
            #[derive(Serialize)]
            struct Claims {
                sub: String,
                tenant_id: String,
                exp: u64,
                #[serde(skip_serializing_if = "Option::is_none")]
                aud: Option<String>,
                #[serde(skip_serializing_if = "Option::is_none")]
                scope: Option<String>,
            }

            let claims = Claims {
                sub: user_id.into(),
                tenant_id: tenant_id.into(),
                exp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
                aud: audience.map(String::from),
                scope: scope.map(String::from),
            };

            encode(
                &JwtHeader::new(Algorithm::HS256),
                &claims,
                &EncodingKey::from_secret(SECRET),
            )
            .unwrap()
        }

        /// A validator bound to this server's resource, which is the only kind
        /// `serve_with_auth` will start with.
        fn bound_validator() -> JwtValidator {
            JwtValidator::hs256(SECRET).for_resource(&ResourceUri::parse(RESOURCE).unwrap())
        }

        /// [`http_post_with_auth`] that also carries and reports a session id.
        fn http_post_in_session(
            addr: &str,
            body: &str,
            token: Option<&str>,
            session: Option<&str>,
        ) -> (String, Option<String>) {
            let mut headers: Vec<(&str, &str)> = Vec::new();
            let bearer;
            if let Some(token) = token {
                bearer = format!("Bearer {token}");
                headers.push(("Authorization", &bearer));
            }
            if let Some(session) = session {
                headers.push((super::SESSION_HEADER, session));
            }

            let (_, _, body, session) =
                http_request_full(addr, "POST", "/mcp", body, &headers).unwrap();
            (body, session)
        }

        fn http_post_with_auth(
            addr: &str,
            path: &str,
            body: &str,
            token: Option<&str>,
        ) -> std::io::Result<(u16, String, String)> {
            let mut stream = TcpStream::connect(addr)?;
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;

            let auth_header = token
                .map(|t| format!("Authorization: Bearer {}\r\n", t))
                .unwrap_or_default();

            let request = format!(
                "POST {} HTTP/1.1\r\n\
                 Host: {}\r\n\
                 Content-Type: application/json\r\n\
                 Content-Length: {}\r\n\
                 {}Connection: close\r\n\
                 \r\n\
                 {}",
                path,
                addr,
                body.len(),
                auth_header,
                body
            );

            stream.write_all(request.as_bytes())?;
            stream.flush()?;

            let mut response = String::new();
            stream.read_to_string(&mut response)?;

            let status_line = response.lines().next().unwrap_or("");
            let status_code: u16 = status_line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let content_type = response
                .lines()
                .find(|l| l.to_lowercase().starts_with("content-type:"))
                .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
                .unwrap_or_default();

            let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();

            Ok((status_code, content_type, body))
        }

        #[test]
        fn test_http_server_auth_missing_header() {
            let port = next_port();
            let addr = format!("127.0.0.1:{}", port);

            let config = ServerConfig {
                name: "test-auth".into(),
                version: "1.0.0".into(),
                instructions: None,
                ..Default::default()
            };

            let server_addr = addr.clone();
            let handle = thread::spawn(move || {
                let _ = HttpServer::new(config)
                    .with_tools(|s: &mut Server<AuthContext>| {
                        s.add_tool(WhoamiTool)?;
                        Ok(())
                    })
                    .serve_with_auth(&server_addr, bound_validator(), |claims| AuthContext {
                        user_id: claims.user_id().to_string(),
                    });
            });

            thread::sleep(Duration::from_millis(100));

            let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
            let (status, _, response) = http_post_with_auth(&addr, "/mcp", body, None).unwrap();

            assert_eq!(status, 401);
            assert!(response.contains("Missing Authorization"));

            drop(handle);
        }

        #[test]
        fn test_http_server_auth_invalid_token() {
            let port = next_port();
            let addr = format!("127.0.0.1:{}", port);

            let config = ServerConfig {
                name: "test-auth".into(),
                version: "1.0.0".into(),
                instructions: None,
                ..Default::default()
            };

            let server_addr = addr.clone();
            let handle = thread::spawn(move || {
                let _ = HttpServer::new(config)
                    .with_tools(|s: &mut Server<AuthContext>| {
                        s.add_tool(WhoamiTool)?;
                        Ok(())
                    })
                    .serve_with_auth(&server_addr, bound_validator(), |claims| AuthContext {
                        user_id: claims.user_id().to_string(),
                    });
            });

            thread::sleep(Duration::from_millis(100));

            let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
            let (status, _, _) =
                http_post_with_auth(&addr, "/mcp", body, Some("invalid-token")).unwrap();

            assert_eq!(status, 401);

            drop(handle);
        }

        //
        // RFC 8707 / RFC 9728
        //

        use crate::auth::{ProtectedResourceMetadata, ResourceUri};

        const RESOURCE: &str = "https://mcp.example.com/mcp";

        fn metadata() -> ProtectedResourceMetadata {
            ProtectedResourceMetadata::new(
                ResourceUri::parse(RESOURCE).unwrap(),
                ["https://auth.example.com"],
            )
            .with_scopes(["files:read", "files:write"])
        }

        /// Token with an explicit audience, so RFC 8707 validation has
        /// something to check.
        fn make_token_for(audience: Option<&str>) -> String {
            #[derive(Serialize)]
            struct AudClaims {
                sub: String,
                tenant_id: String,
                exp: u64,
                #[serde(skip_serializing_if = "Option::is_none")]
                aud: Option<String>,
            }

            let claims = AudClaims {
                sub: "alice".into(),
                tenant_id: "tenant-1".into(),
                exp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
                aud: audience.map(String::from),
            };

            encode(
                &JwtHeader::new(Algorithm::HS256),
                &claims,
                &EncodingKey::from_secret(SECRET),
            )
            .unwrap()
        }

        /// Spawn an authenticated server bound to the canonical resource URI.
        fn spawn_protected_server() -> String {
            let addr = format!("127.0.0.1:{}", next_port());
            let server_addr = addr.clone();

            thread::spawn(move || {
                let resource = ResourceUri::parse(RESOURCE).unwrap();
                let _ = HttpServer::new(ServerConfig {
                    name: "protected".into(),
                    ..Default::default()
                })
                .protected_resource(metadata())
                .with_tools(|s: &mut Server<AuthContext>| {
                    s.add_tool(WhoamiTool)?;
                    Ok(())
                })
                .serve_with_auth(
                    &server_addr,
                    JwtValidator::hs256(SECRET).for_resource(&resource),
                    |claims| AuthContext {
                        user_id: claims.user_id().to_string(),
                    },
                );
            });

            thread::sleep(Duration::from_millis(100));
            addr
        }

        #[test]
        fn test_serve_with_auth_refuses_an_unbound_validator() {
            // "MCP servers MUST validate that access tokens were issued
            // specifically for them as the intended audience." A validator with
            // no audience cannot, so it cannot protect anything - and the crate
            // used to document exactly this call as the way to do it.
            let addr = format!("127.0.0.1:{}", next_port());
            let error = HttpServer::new(ServerConfig::default())
                .with_tools(|s: &mut Server<AuthContext>| {
                    s.add_tool(WhoamiTool)?;
                    Ok(())
                })
                .serve_with_auth(&addr, JwtValidator::hs256(SECRET), |claims| AuthContext {
                    user_id: claims.user_id().to_string(),
                })
                .unwrap_err();

            assert!(error.to_string().contains("aud"), "{error}");

            // Nothing is listening: it refused before binding.
            assert!(TcpStream::connect(&addr).is_err());
        }

        #[test]
        fn test_missing_scopes_are_403_with_an_insufficient_scope_challenge() {
            let addr = format!("127.0.0.1:{}", next_port());
            let server_addr = addr.clone();

            thread::spawn(move || {
                let _ = HttpServer::new(ServerConfig::default())
                    .protected_resource(metadata())
                    .require_scopes(["files:write"])
                    .with_tools(|s: &mut Server<AuthContext>| {
                        s.add_tool(WhoamiTool)?;
                        Ok(())
                    })
                    .serve_with_auth(&server_addr, bound_validator(), |claims| AuthContext {
                        user_id: claims.user_id().to_string(),
                    });
            });
            thread::sleep(Duration::from_millis(100));

            // Authenticated, but read-only.
            let token = make_token_with("alice", "t", Some(RESOURCE), Some("files:read"));
            let mut stream = TcpStream::connect(&addr).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
            let request = format!(
                "POST /mcp HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                addr,
                token,
                body.len(),
                body
            );
            stream.write_all(request.as_bytes()).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();

            assert!(response.starts_with("HTTP/1.1 403"), "{response}");
            let challenge = response
                .lines()
                .find(|l| l.to_lowercase().starts_with("www-authenticate:"))
                .expect("403 must say what would have been enough");
            assert!(
                challenge.contains("error=\"insufficient_scope\""),
                "{challenge}"
            );
            assert!(challenge.contains("scope=\"files:write\""), "{challenge}");

            // With the scope, the same request goes through.
            let token =
                make_token_with("alice", "t", Some(RESOURCE), Some("files:read files:write"));
            let (status, _, _) = http_post_with_auth(&addr, "/mcp", body, Some(&token)).unwrap();
            assert_eq!(status, 200);
        }

        #[test]
        fn test_a_lowercase_bearer_scheme_is_accepted() {
            let addr = spawn_protected_server();
            let token = make_token("alice", "tenant-1");

            let mut stream = TcpStream::connect(&addr).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
            let request = format!(
                "POST /mcp HTTP/1.1\r\nHost: {}\r\nAuthorization: bearer {}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                addr,
                token,
                body.len(),
                body
            );
            stream.write_all(request.as_bytes()).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();

            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        }

        #[test]
        fn test_tasks_are_bound_to_the_authorization_context() {
            // "When an authorization context is provided, receivers MUST bind
            // tasks to said context" and MUST reject `tasks/get`,
            // `tasks/result` and `tasks/cancel` for tasks belonging to another
            // one. Masked before only because a task never survived its
            // request at all.
            struct TaskTool;
            impl Tool<AuthContext> for TaskTool {
                fn name(&self) -> &str {
                    "work"
                }
                fn description(&self) -> &str {
                    "Runs as a task"
                }
                fn schema(&self) -> Value {
                    serde_json::json!({ "type": "object", "properties": {} })
                }
                fn task_support(&self) -> crate::types::TaskSupport {
                    crate::types::TaskSupport::Optional
                }
                fn execute(
                    &self,
                    _args: Value,
                    ctx: &mut AuthContext,
                    _env: &ToolEnv,
                ) -> Result<CallToolResult> {
                    Ok(CallToolResult::text(format!("done for {}", ctx.user_id)))
                }
            }

            let addr = format!("127.0.0.1:{}", next_port());
            let server_addr = addr.clone();
            thread::spawn(move || {
                let _ = HttpServer::new(ServerConfig::default())
                    .with_tools(|s: &mut Server<AuthContext>| {
                        s.add_tool(TaskTool)?;
                        s.enable_tasks(Default::default(), || AuthContext {
                            user_id: "worker".into(),
                        });
                        Ok(())
                    })
                    .serve_with_auth(&server_addr, bound_validator(), |claims| AuthContext {
                        user_id: claims.user_id().to_string(),
                    });
            });
            thread::sleep(Duration::from_millis(100));

            let alice = make_token_with("alice", "tenant-a", Some(RESOURCE), None);
            let mallory = make_token_with("mallory", "tenant-b", Some(RESOURCE), None);

            let call = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"work","task":{}}}"#;
            let (body, session) = http_post_in_session(&addr, call, Some(&alice), None);
            let parsed: Value = serde_json::from_str(&body).unwrap();
            let task_id = parsed["result"]["task"]["taskId"]
                .as_str()
                .expect("a task id")
                .to_string();

            // Both of them speak into the *same* session, so the shared store
            // is real and the authorization context is the only thing between
            // Mallory and Alice's task. Nothing about session keying is being
            // leaned on here.
            let session = session.expect("the task's session");
            let alice_says =
                |body: &str| http_post_in_session(&addr, body, Some(&alice), Some(&session)).0;
            let mallory_says =
                |body: &str| http_post_in_session(&addr, body, Some(&mallory), Some(&session)).0;

            // Mallory knows the id and is perfectly well authenticated.
            let get = format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"tasks/get","params":{{"taskId":"{task_id}"}}}}"#
            );
            let body = mallory_says(&get);
            let parsed: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(parsed["error"]["code"], -32602, "{body}");

            let cancel = format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"tasks/cancel","params":{{"taskId":"{task_id}"}}}}"#
            );
            let body = mallory_says(&cancel);
            let parsed: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(parsed["error"]["code"], -32602, "{body}");

            // "For tasks/list requests, receivers MUST ensure the returned task
            // list includes only tasks associated with the requestor's
            // authorization context."
            let list = r#"{"jsonrpc":"2.0","id":4,"method":"tasks/list"}"#;
            let body = mallory_says(list);
            let parsed: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(
                parsed["result"]["tasks"].as_array().unwrap().len(),
                0,
                "{body}"
            );

            // Alice still has hers.
            let body = alice_says(&get);
            let parsed: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(parsed["result"]["taskId"], task_id.as_str(), "{body}");

            let body = alice_says(list);
            let parsed: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(
                parsed["result"]["tasks"].as_array().unwrap().len(),
                1,
                "{body}"
            );
        }

        #[test]
        fn test_an_authenticated_server_does_declare_tasks_list() {
            // It can identify requestors, so listing is safe - and filtered.
            let addr = format!("127.0.0.1:{}", next_port());
            let server_addr = addr.clone();
            thread::spawn(move || {
                let _ = HttpServer::new(ServerConfig::default())
                    .with_tools(|s: &mut Server<AuthContext>| {
                        s.add_tool(WhoamiTool)?;
                        s.enable_tasks(Default::default(), || AuthContext {
                            user_id: "worker".into(),
                        });
                        Ok(())
                    })
                    .serve_with_auth(&server_addr, bound_validator(), |claims| AuthContext {
                        user_id: claims.user_id().to_string(),
                    });
            });
            thread::sleep(Duration::from_millis(100));

            let token = make_token("alice", "tenant-a");
            let init = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}"#;
            let (_, _, body) = http_post_with_auth(&addr, "/mcp", init, Some(&token)).unwrap();
            let parsed: Value = serde_json::from_str(&body).unwrap();
            assert!(
                !parsed["result"]["capabilities"]["tasks"]["list"].is_null(),
                "{body}"
            );
        }

        #[test]
        fn test_protected_resource_metadata_is_served_unauthenticated() {
            // Discovery has to work before the client has a token.
            let addr = spawn_protected_server();
            let (status, content_type, body) = http_request(
                &addr,
                "GET",
                "/.well-known/oauth-protected-resource/mcp",
                "",
                &[],
            )
            .unwrap();

            assert_eq!(status, 200);
            assert_eq!(content_type, "application/json");

            let parsed: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(parsed["resource"], RESOURCE);
            assert_eq!(
                parsed["authorization_servers"][0],
                "https://auth.example.com"
            );
            assert_eq!(parsed["scopes_supported"][0], "files:read");
            assert_eq!(parsed["bearer_methods_supported"][0], "header");
        }

        #[test]
        fn test_unauthorized_response_carries_the_discovery_challenge() {
            let addr = spawn_protected_server();
            let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;

            let mut stream = TcpStream::connect(&addr).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request = format!(
                "POST /mcp HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                addr,
                body.len(),
                body
            );
            stream.write_all(request.as_bytes()).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();

            assert!(response.starts_with("HTTP/1.1 401"), "{response}");

            let challenge = response
                .lines()
                .find(|l| l.to_lowercase().starts_with("www-authenticate:"))
                .expect("401 must carry a WWW-Authenticate challenge");

            assert!(challenge.contains(
                "resource_metadata=\"https://mcp.example.com/.well-known/oauth-protected-resource/mcp\""
            ));
            // Naming the scopes is a SHOULD, and saves the client a guess.
            assert!(challenge.contains("scope=\"files:read files:write\""));
        }

        #[test]
        fn test_token_for_another_audience_is_rejected() {
            // The RFC 8707 MUST: a token minted for some other service must not
            // be usable here.
            let addr = spawn_protected_server();
            let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;

            let foreign = make_token_for(Some("https://other.example.com/mcp"));
            let (status, _, _) = http_post_with_auth(&addr, "/mcp", body, Some(&foreign)).unwrap();
            assert_eq!(status, 401, "foreign-audience token must be rejected");

            // A token with no audience at all is equally unusable.
            let audienceless = make_token_for(None);
            let (status, _, _) =
                http_post_with_auth(&addr, "/mcp", body, Some(&audienceless)).unwrap();
            assert_eq!(status, 401, "audience-less token must be rejected");
        }

        #[test]
        fn test_token_for_this_resource_is_accepted() {
            let addr = spawn_protected_server();
            let token = make_token_for(Some(RESOURCE));
            let body =
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"whoami"}}"#;

            let (status, _, response) =
                http_post_with_auth(&addr, "/mcp", body, Some(&token)).unwrap();

            assert_eq!(status, 200);
            assert!(response.contains("User: alice"));
        }

        #[test]
        fn test_metadata_endpoint_rejects_non_get() {
            let addr = spawn_protected_server();
            let (status, _, _) = http_request(
                &addr,
                "POST",
                "/.well-known/oauth-protected-resource/mcp",
                "",
                &[],
            )
            .unwrap();
            assert_eq!(status, 405);
        }

        #[test]
        fn test_no_metadata_means_no_challenge() {
            // A server that publishes nothing still 401s, just without the
            // discovery hint.
            let addr = format!("127.0.0.1:{}", next_port());
            let server_addr = addr.clone();
            thread::spawn(move || {
                let _ = HttpServer::new(ServerConfig::default())
                    .with_tools(|s: &mut Server<AuthContext>| {
                        s.add_tool(WhoamiTool)?;
                        Ok(())
                    })
                    .serve_with_auth(&server_addr, bound_validator(), |claims| AuthContext {
                        user_id: claims.user_id().to_string(),
                    });
            });
            thread::sleep(Duration::from_millis(100));

            let (status, _, _) = http_request(
                &addr,
                "GET",
                "/.well-known/oauth-protected-resource",
                "",
                &[],
            )
            .unwrap();
            assert_eq!(status, 404, "nothing is published at the well-known path");
        }

        #[test]
        fn test_http_server_auth_valid_token() {
            let port = next_port();
            let addr = format!("127.0.0.1:{}", port);

            let config = ServerConfig {
                name: "test-auth".into(),
                version: "1.0.0".into(),
                instructions: None,
                ..Default::default()
            };

            let server_addr = addr.clone();
            let handle = thread::spawn(move || {
                let _ = HttpServer::new(config)
                    .with_tools(|s: &mut Server<AuthContext>| {
                        s.add_tool(WhoamiTool)?;
                        Ok(())
                    })
                    .serve_with_auth(&server_addr, bound_validator(), |claims| AuthContext {
                        user_id: claims.user_id().to_string(),
                    });
            });

            thread::sleep(Duration::from_millis(100));

            let token = make_token("alice", "tenant-1");
            let body =
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"whoami"}}"#;
            let (status, _, response) =
                http_post_with_auth(&addr, "/mcp", body, Some(&token)).unwrap();

            assert_eq!(status, 200);
            assert!(response.contains("User: alice"));

            drop(handle);
        }
    }
}
