//! HTTP server with JWT authentication and SSE responses
//!
//! Run with: cargo run --example http_auth --features hosted

use serde_json::Value;
use sml_mcps::{
    CallToolResult, HttpServer, LogLevel, Result, ServerConfig, Tool, ToolEnv,
    auth::{JwtValidator, ResourceUri},
};

// For demo: generate test tokens
use jsonwebtoken::{Algorithm, EncodingKey, Header as JwtHeader, encode};
use serde::Serialize;

const SECRET: &[u8] = b"super-secret-key-for-testing-only";

/// Context that includes auth info
struct AuthContext {
    user_id: String,
    tenant_id: String,
}

struct WhoamiTool;
impl Tool<AuthContext> for WhoamiTool {
    fn name(&self) -> &str {
        "whoami"
    }
    fn description(&self) -> &str {
        "Returns info about the authenticated user"
    }
    fn schema(&self) -> Value {
        serde_json::json!({ "type": "object", "properties": {} })
    }
    fn execute(
        &self,
        _args: Value,
        ctx: &mut AuthContext,
        env: &ToolEnv,
    ) -> Result<CallToolResult> {
        env.log(
            LogLevel::Info,
            format!("User {} checking identity", ctx.user_id),
        )?;
        Ok(CallToolResult::text(format!(
            "You are user '{}' in tenant '{}'",
            ctx.user_id, ctx.tenant_id
        )))
    }
}

struct EchoTool;
impl Tool<AuthContext> for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Echo back a message"
    }
    fn schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": { "message": { "type": "string" } },
            "required": ["message"]
        })
    }
    fn execute(&self, args: Value, ctx: &mut AuthContext, env: &ToolEnv) -> Result<CallToolResult> {
        let msg = args
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("(no message)");
        env.log(
            LogLevel::Debug,
            format!("Echo from {}: {}", ctx.user_id, msg),
        )?;
        Ok(CallToolResult::text(format!("Echo: {}", msg)))
    }
}

/// The canonical URI clients are told to ask for tokens for, and the one this
/// server accepts tokens for. RFC 8707: a token minted for anything else MUST
/// be rejected.
const RESOURCE: &str = "http://127.0.0.1:3001/mcp";

fn generate_test_token(user_id: &str, tenant_id: &str) -> String {
    #[derive(Serialize)]
    struct TestClaims {
        sub: String,
        exp: u64,
        aud: String,
        tenant_id: String,
        scope: String,
    }

    let claims = TestClaims {
        sub: user_id.to_string(),
        exp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600,
        aud: RESOURCE.to_string(),
        tenant_id: tenant_id.to_string(),
        scope: "read write".to_string(),
    };

    encode(
        &JwtHeader::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

fn main() -> Result<()> {
    let addr = "127.0.0.1:3001";

    let test_token = generate_test_token("user-123", "tenant-456");
    eprintln!("\n=== TEST TOKEN (valid for 1 hour) ===");
    eprintln!("{}", test_token);
    eprintln!("\nTest with:");
    eprintln!("curl -X POST http://{}/mcp \\", addr);
    eprintln!("  -H \"Content-Type: application/json\" \\");
    eprintln!("  -H \"Authorization: Bearer {}\" \\", test_token);
    eprintln!(
        "  -d '{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{{\"name\":\"whoami\"}}}}'"
    );
    eprintln!("=====================================\n");

    let config = ServerConfig {
        name: "authenticated-http".to_string(),
        version: "1.0.0".to_string(),
        instructions: None,
        ..Default::default()
    };

    let resource = ResourceUri::parse(RESOURCE)
        .map_err(|e| sml_mcps::McpError::Internal(format!("bad resource URI: {e}")))?;

    HttpServer::new(config)
        .with_tools(|server| {
            server.add_tool(WhoamiTool)?;
            server.add_tool(EchoTool)?;
            Ok(())
        })
        .serve_with_auth(
            addr,
            // Bound to this server: `serve_with_auth` refuses to start with a
            // validator that would accept tokens minted for someone else.
            JwtValidator::hs256(SECRET).for_resource(&resource),
            |claims| AuthContext {
                user_id: claims.user_id().to_string(),
                tenant_id: claims.tenant_id().to_string(),
            },
        )
}
