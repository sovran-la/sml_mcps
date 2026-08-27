//! # sml_mcps - Small MCP Server
//!
//! A minimal, sync MCP server implementation. No tokio, no async, just works.
//!
//! ## Features
//!
//! - `schema` (default) - JSON Schema generation for tools via schemars
//! - `http` - Streamable HTTP transport on an in-tree HTTP/1.1 server
//!   (thread-per-connection; the request grammar is `httparse`'s)
//! - `auth` - JWT validation for hosted deployments
//! - `hosted` - Enables both `http` and `auth`
//! - `tls` - HTTPS for the HTTP transport, via rustls
//! - `cli` - `install`/`uninstall`/`serve`/`health` subcommands, and the
//!   registry of MCP clients they configure

/// What a composed identity is joined with, and so what neither half of one may
/// contain.
///
/// Sessions and tasks are keyed on `<user>␁<tenant>␁<id>`, which is what
/// security_best_practices §Session Hijacking asks for - and it only tells two
/// identities apart while the separator cannot appear in what it separates. A
/// control character rather than a `:` because it cannot occur in a URI, an
/// email address, a UUID or any other shape a subject realistically takes, so
/// refusing one costs no legitimate token anything.
///
/// Composed by the HTTP transport and refused by the JWT validator, so it is
/// only a thing when one of them is here.
#[cfg(any(feature = "http", feature = "auth"))]
pub(crate) const IDENTITY_SEPARATOR: char = '\u{1}';

mod broker;
pub mod pagination;
pub mod schema_check;
pub mod server;
pub mod tasks;
pub mod transport;
pub mod types;

#[cfg(feature = "auth")]
pub mod auth;

/// The subcommands every MCP server ends up writing, and the MCP client
/// registry behind `install`.
#[cfg(feature = "cli")]
pub mod cli;

#[cfg(unix)]
pub mod bridge;

/// Adds [`Server::serve_daemon`], which needs no import - the module exports
/// nothing, it only extends `Server`.
#[cfg(unix)]
mod daemon;

/// Where a daemon's socket goes, and who is allowed to be behind it.
#[cfg(unix)]
mod socket;

// Re-export commonly used types
pub use pagination::{DEFAULT_PAGE_SIZE, PageState, paginate};
pub use server::{
    LogLevel, PromptDef, Resource, Server, ServerConfig, StderrLogging, Tool, ToolEnv,
};
pub use tasks::{Task, TaskConfig, TaskStatus, TaskStore};
pub use transport::{OriginPolicy, StdioTransport, StreamTransport, TcpTransport, Transport};
pub use types::*;

#[cfg(feature = "http")]
pub use transport::{HttpServer, HttpTransport};

#[cfg(unix)]
pub use bridge::Bridge;

#[cfg(unix)]
pub use socket::user_socket_path;

#[cfg(unix)]
pub use transport::{UnixServer, UnixTransport};
