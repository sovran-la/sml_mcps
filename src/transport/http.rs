//! HTTP Transport
//!
//! Streamable HTTP transport for remote MCP servers.
//! Returns SSE stream to support notifications and progress.

use crate::transport::Transport;
use crate::types::{JsonRpcMessage, McpError, Result};
use std::sync::{Arc, Mutex};

/// HTTP request/response transport with SSE support
///
/// Buffers all outgoing messages and returns them as an SSE stream.
pub struct HttpTransport {
    /// The request body (JSON-RPC message)
    request: Option<String>,
    /// Buffered messages to return as SSE stream
    messages: Arc<Mutex<Vec<String>>>,
}

impl HttpTransport {
    /// Create a new HTTP transport from a request body
    pub fn new(request_body: String) -> Self {
        Self {
            request: Some(request_body),
            messages: Arc::new(Mutex::new(Vec::new())),
        }
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
        let messages = self.messages.lock().unwrap();
        messages
            .iter()
            .map(|msg| format!("data: {}\n\n", msg))
            .collect()
    }

    /// Take response as plain JSON (for single response, no notifications)
    ///
    /// Returns just the last message (the actual response), or empty if none.
    pub fn take_response(&mut self) -> Option<String> {
        let messages = self.messages.lock().unwrap();
        messages.last().cloned()
    }

    /// Check if there are multiple messages (notifications + response)
    pub fn has_notifications(&self) -> bool {
        let messages = self.messages.lock().unwrap();
        messages.len() > 1
    }
}

impl Transport for HttpTransport {
    fn read(&mut self) -> Result<JsonRpcMessage> {
        let body = self.request.take().ok_or(McpError::TransportClosed)?;

        JsonRpcMessage::parse(&body)
    }

    fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
        let json = serde_json::to_string(message)?;
        let mut messages = self
            .messages
            .lock()
            .map_err(|_| McpError::Internal("Lock poisoned".into()))?;
        messages.push(json);
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

//
// HttpServer - high-level server wrapper
//

use crate::server::{Server, ServerConfig};
use crate::transport::OriginPolicy;
use crate::types::ASSUMED_PROTOCOL_VERSION;
use std::io::Cursor;
use tiny_http::{Header, Method, Request as TinyRequest, Response, Server as TinyServer};

#[cfg(feature = "auth")]
use crate::auth::{Claims, JwtValidator, ProtectedResourceMetadata, unauthorized_challenge};

/// Setup function type for configuring tools on each request
type SetupFn<C> = Box<dyn Fn(&mut Server<C>) -> Result<()> + Send + Sync>;

/// A ready-to-send HTTP response with an owned body.
type HttpResponse = Response<Cursor<Vec<u8>>>;

/// Look up a request header case-insensitively.
fn header_value<'a>(request: &'a TinyRequest, name: &'static str) -> Option<&'a str> {
    request
        .headers()
        .iter()
        .find(|h| h.field.equiv(name))
        .map(|h| h.value.as_str())
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

    Response::from_string(body)
        .with_status_code(status)
        .with_header(
            // Constructed from a string literal that is always a valid header.
            Header::from_bytes("Content-Type", "application/json")
                .expect("static Content-Type header is valid"),
        )
}

/// High-level HTTP MCP server
///
/// Wraps the request loop boilerplate for serving MCP over HTTP.
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
///         JwtValidator::hs256(SECRET),
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
        }
    }

    /// Set the endpoint path (default: "/mcp")
    pub fn endpoint(mut self, path: impl Into<String>) -> Self {
        self.endpoint = path.into();
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
    fn protected_resource_response(&self, request: &TinyRequest) -> Option<HttpResponse> {
        let metadata = self.protected_resource.as_ref()?;
        let path = metadata.resource_uri().ok()?.metadata_path();
        if request.url() != path {
            return None;
        }

        // Discovery must work before the client has a token, so this endpoint
        // is deliberately unauthenticated - it contains nothing secret.
        if request.method() != &Method::Get {
            return Some(error_response(405, -32600, "Method Not Allowed"));
        }

        let body = serde_json::to_string(metadata).ok()?;
        Some(
            Response::from_string(body).with_header(
                Header::from_bytes("Content-Type", "application/json")
                    .expect("static Content-Type header is valid"),
            ),
        )
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
    fn precheck(&self, request: &TinyRequest) -> Option<HttpResponse> {
        // Discovery is served before the endpoint check, since it lives at a
        // different path by design.
        #[cfg(feature = "auth")]
        if let Some(response) = self.protected_resource_response(request) {
            return Some(response);
        }

        if request.url() != self.endpoint {
            return Some(error_response(404, -32600, "Not Found"));
        }

        if request.method() != &Method::Post {
            return Some(error_response(405, -32600, "Method Not Allowed"));
        }

        // "If the server receives a request with an invalid or unsupported
        // MCP-Protocol-Version, it MUST respond with 400 Bad Request." An
        // absent header is not an error: it means 2025-03-26, which we speak.
        let version =
            header_value(request, "MCP-Protocol-Version").unwrap_or(ASSUMED_PROTOCOL_VERSION);
        if !self.config.supported_versions.iter().any(|v| v == version) {
            eprintln!("  ✗ Unsupported MCP-Protocol-Version: {}", version);
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
            Some(challenge) => match Header::from_bytes("WWW-Authenticate", challenge) {
                Ok(header) => response.with_header(header),
                Err(_) => response,
            },
            None => response,
        }
    }

    /// Read the request body, or produce the 400 response to send instead.
    fn read_body(request: &mut TinyRequest) -> std::result::Result<String, HttpResponse> {
        let mut body = String::new();
        match request.as_reader().read_to_string(&mut body) {
            Ok(_) => Ok(body),
            Err(e) => {
                eprintln!("  Failed to read body: {}", e);
                Err(error_response(400, -32700, "Bad Request: unreadable body"))
            }
        }
    }

    /// Turn a processed result into the HTTP response to send.
    fn finish(&self, outcome: Result<(String, &'static str)>) -> HttpResponse {
        match outcome {
            Ok((response_body, content_type)) => {
                eprintln!("  Response ({}): {}", content_type, response_body);
                let header = Header::from_bytes("Content-Type", content_type)
                    .expect("static Content-Type header is valid");
                Response::from_data(response_body.into_bytes()).with_header(header)
            }
            Err(e) => {
                eprintln!("  Error: {}", e);
                error_response(500, -32603, format!("Internal error: {}", e))
            }
        }
    }

    /// Serve without authentication
    ///
    /// The context factory is called for each request.
    pub fn serve<F>(self, addr: &str, context_factory: F) -> Result<()>
    where
        F: Fn() -> C,
    {
        let http_server = TinyServer::http(addr)
            .map_err(|e| McpError::Internal(format!("Failed to start HTTP server: {}", e)))?;

        eprintln!(
            "MCP HTTP server `{}` listening on http://{}{}",
            self.config.name, addr, self.endpoint
        );

        for mut request in http_server.incoming_requests() {
            eprintln!("{} {}", request.method(), request.url());

            if let Some(rejection) = self.precheck(&request) {
                let _ = request.respond(rejection);
                continue;
            }

            let body = match Self::read_body(&mut request) {
                Ok(body) => body,
                Err(rejection) => {
                    let _ = request.respond(rejection);
                    continue;
                }
            };

            eprintln!("  Request: {}", body);

            let mut ctx = context_factory();
            let response = self.finish(self.process_request(body, &mut ctx));
            if let Err(e) = request.respond(response) {
                eprintln!("  Failed to send response: {}", e);
            }
        }

        Ok(())
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
        F: Fn(&Claims) -> C,
    {
        let http_server = TinyServer::http(addr)
            .map_err(|e| McpError::Internal(format!("Failed to start HTTP server: {}", e)))?;

        eprintln!(
            "MCP HTTP server `{}` (authenticated) listening on http://{}{}",
            self.config.name, addr, self.endpoint
        );

        for mut request in http_server.incoming_requests() {
            eprintln!("{} {}", request.method(), request.url());

            if let Some(rejection) = self.precheck(&request) {
                let _ = request.respond(rejection);
                continue;
            }

            // JWT Authentication
            let auth_header = header_value(&request, "Authorization");

            let claims = match auth_header {
                Some(header) => match validator.validate_header(header) {
                    Ok(claims) => {
                        eprintln!(
                            "  ✓ Authenticated: user={}, tenant={}",
                            claims.user_id(),
                            claims.tenant_id()
                        );
                        claims
                    }
                    Err(e) => {
                        eprintln!("  ✗ Auth failed: {}", e);
                        let _ = request.respond(self.unauthorized(format!("Unauthorized: {}", e)));
                        continue;
                    }
                },
                None => {
                    eprintln!("  ✗ No Authorization header");
                    let _ = request
                        .respond(self.unauthorized("Unauthorized: Missing Authorization header"));
                    continue;
                }
            };

            let body = match Self::read_body(&mut request) {
                Ok(body) => body,
                Err(rejection) => {
                    let _ = request.respond(rejection);
                    continue;
                }
            };

            eprintln!("  Request: {}", body);

            // Process request with auth context
            let mut ctx = context_factory(&claims);
            let response = self.finish(self.process_request(body, &mut ctx));
            if let Err(e) = request.respond(response) {
                eprintln!("  Failed to send response: {}", e);
            }
        }

        Ok(())
    }

    /// Process a single request and return (body, content_type)
    fn process_request(&self, body: String, ctx: &mut C) -> Result<(String, &'static str)> {
        // Create fresh server
        let mut server: Server<C> = Server::new(self.config.clone());

        // Set up tools
        if let Some(ref setup) = self.setup {
            setup(&mut server)?;
        }

        // Create transport and process
        let transport = Arc::new(Mutex::new(HttpTransport::new(body)));
        server.process_one(transport.clone(), ctx)?;

        // Extract response
        let mut transport_guard = transport
            .lock()
            .map_err(|_| McpError::Internal("Transport lock poisoned".into()))?;

        if transport_guard.has_notifications() {
            Ok((transport_guard.take_sse_response(), "text/event-stream"))
        } else {
            Ok((
                transport_guard
                    .take_response()
                    .unwrap_or_else(|| "{}".to_string()),
                "application/json",
            ))
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

    /// Helper to make a raw HTTP request with an arbitrary method and headers
    fn http_request(
        addr: &str,
        method: &str,
        path: &str,
        body: &str,
        extra_headers: &[(&str, &str)],
    ) -> std::io::Result<(u16, String, String)> {
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

        Ok((status_code, content_type, body))
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
        let addr = format!("127.0.0.1:{}", next_port());

        let config = ServerConfig {
            name: "test-server".into(),
            version: "1.0.0".into(),
            instructions: None,
            ..Default::default()
        };

        let server_addr = addr.clone();
        thread::spawn(move || {
            let counter = Arc::new(AtomicI64::new(0));
            let _ = HttpServer::new(config)
                .origin_policy(policy)
                .with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(EchoTool)?;
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
        let addr = spawn_server(OriginPolicy::Loopback);
        for method in ["GET", "DELETE", "PUT"] {
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
            #[derive(Serialize)]
            struct Claims {
                sub: String,
                tenant_id: String,
                exp: u64,
            }

            let claims = Claims {
                sub: user_id.into(),
                tenant_id: tenant_id.into(),
                exp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            };

            encode(
                &JwtHeader::new(Algorithm::HS256),
                &claims,
                &EncodingKey::from_secret(SECRET),
            )
            .unwrap()
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
                    .serve_with_auth(&server_addr, JwtValidator::hs256(SECRET), |claims| {
                        AuthContext {
                            user_id: claims.user_id().to_string(),
                        }
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
                    .serve_with_auth(&server_addr, JwtValidator::hs256(SECRET), |claims| {
                        AuthContext {
                            user_id: claims.user_id().to_string(),
                        }
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
                    .serve_with_auth(&server_addr, JwtValidator::hs256(SECRET), |claims| {
                        AuthContext {
                            user_id: claims.user_id().to_string(),
                        }
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
                    .serve_with_auth(&server_addr, JwtValidator::hs256(SECRET), |claims| {
                        AuthContext {
                            user_id: claims.user_id().to_string(),
                        }
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
