//! # sml_mcps - Small MCP Server
//!
//! A minimal, sync MCP server implementation. No tokio, no async, just works.
//!
//! ## Features
//!
//! - `schema` (default) - JSON Schema generation for tools via schemars
//! - `http` - Streamable HTTP transport via tiny_http
//! - `auth` - JWT validation for hosted deployments
//! - `hosted` - Enables both `http` and `auth`

pub mod pagination;
pub mod server;
pub mod transport;
pub mod types;

#[cfg(feature = "auth")]
pub mod auth;

#[cfg(unix)]
pub mod bridge;

// Re-export commonly used types
pub use pagination::{DEFAULT_PAGE_SIZE, PageState, paginate};
pub use server::{
    LogLevel, PromptDef, Resource, Server, ServerConfig, StderrLogging, Tool, ToolEnv,
};
pub use transport::{OriginPolicy, StdioTransport, Transport};
pub use types::*;

#[cfg(feature = "http")]
pub use transport::{HttpServer, HttpTransport};

#[cfg(unix)]
pub use bridge::Bridge;

#[cfg(unix)]
pub use transport::{UnixServer, UnixTransport};
