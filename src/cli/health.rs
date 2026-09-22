//! `health --connect <host:port>`: is there an MCP daemon at that address?
//!
//! The author's `health` handler checks whatever "healthy" means on *this*
//! machine. Pointed at another machine with `--connect`, none of that applies -
//! there is no local socket, no local config, nothing local to look at - and
//! what is worth knowing is whether the far end answers as an MCP server. So
//! the harness answers that itself, the same way for every server, and the
//! author's handler is not called.
//!
//! The probe is the client half of a session: `initialize`, the `initialized`
//! notification, one `ping`. Reachability alone would be a TCP connect; that
//! something MCP is behind the port, and keeps answering after the handshake,
//! takes the round-trips.

use crate::remote::RemoteAddr;
#[cfg(test)]
use crate::transport::TcpTransport;
use crate::transport::Transport;
use crate::types::{JsonRpcMessage, McpError, Result};
use serde_json::Value;
use std::fmt;
use std::time::Duration;

/// How long the probe waits for each answer from the daemon.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// What a probe found on the far end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProbeReport {
    /// The address that was dialed, as it was given.
    pub address: String,
    /// The daemon's `serverInfo.name`.
    pub name: String,
    /// The daemon's `serverInfo.version`.
    pub version: String,
}

impl fmt::Display for ProbeReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ok - {} is serving {} {}",
            self.address, self.name, self.version
        )
    }
}

/// Ask the daemon at `addr` who it is.
pub(super) fn probe(addr: &RemoteAddr) -> Result<ProbeReport> {
    probe_transport(addr, Box::new(addr.connect()?))
}

pub(super) fn probe_transport(
    addr: &RemoteAddr,
    mut transport: Box<dyn Transport>,
) -> Result<ProbeReport> {
    if !transport.set_read_timeout(Some(PROBE_TIMEOUT))? {
        return Err(McpError::Internal(
            "remote health transport must support read deadlines".into(),
        ));
    }

    let initialize = JsonRpcMessage::request(
        1i64,
        "initialize",
        Some(serde_json::json!({
            "protocolVersion": crate::types::PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {
                "name": "sml_mcps health",
                "version": env!("CARGO_PKG_VERSION"),
            },
        })),
    );
    send(&mut transport, addr, &initialize, "initialize")?;
    let handshake = answer(&mut transport, addr, "initialize")?;

    send(
        &mut transport,
        addr,
        &JsonRpcMessage::notification("notifications/initialized", None),
        "initialized",
    )?;
    send(
        &mut transport,
        addr,
        &JsonRpcMessage::request(2i64, "ping", None),
        "ping",
    )?;
    answer(&mut transport, addr, "ping")?;

    let _ = transport.close();

    let info = &handshake["serverInfo"];
    Ok(ProbeReport {
        address: addr.to_string(),
        name: info["name"].as_str().unwrap_or("?").to_string(),
        version: info["version"].as_str().unwrap_or("?").to_string(),
    })
}

/// Write one message, naming the address if the far end will not take it.
///
/// A peer that closes the moment it accepts is seen here as often as in the
/// read that follows - `EPIPE` on the write, or EOF on the read, depending on
/// which side the kernel notices first - and both have to say the same thing.
fn send(
    transport: &mut dyn Transport,
    addr: &RemoteAddr,
    message: &JsonRpcMessage,
    method: &str,
) -> Result<()> {
    transport.write(message).map_err(|error| {
        McpError::Internal(format!(
            "{addr} hung up while being sent `{method}`: {error}"
        ))
    })
}

/// The result of the request just sent, skipping any notification the daemon
/// volunteers in between.
fn answer(transport: &mut dyn Transport, addr: &RemoteAddr, method: &str) -> Result<Value> {
    loop {
        match transport.read() {
            Ok(JsonRpcMessage::Response(response)) => {
                return match (response.result, response.error) {
                    (Some(result), _) => Ok(result),
                    (None, Some(error)) => Err(McpError::Internal(format!(
                        "{addr} answered `{method}` with an error: {}",
                        error.message
                    ))),
                    (None, None) => Ok(Value::Null),
                };
            }
            Ok(JsonRpcMessage::Notification(_)) => continue,
            Ok(other) => {
                return Err(McpError::Internal(format!(
                    "{addr} answered `{method}` with something that is not a response: {other:?}"
                )));
            }
            Err(McpError::TransportClosed) => {
                return Err(McpError::Internal(format!(
                    "{addr} hung up before answering `{method}`"
                )));
            }
            Err(McpError::Timeout(_)) => {
                return Err(McpError::Internal(format!(
                    "{addr} did not answer `{method}` within {PROBE_TIMEOUT:?}"
                )));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{Server, ServerConfig};
    use std::net::TcpListener;
    use std::thread;

    fn addr_of(listener: &TcpListener) -> RemoteAddr {
        RemoteAddr::parse(&listener.local_addr().unwrap().to_string()).unwrap()
    }

    /// Serve one connection of a real `Server` over `listener`, in a thread.
    fn serve_one(listener: TcpListener) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut server: Server<()> = Server::new(ServerConfig {
                name: "probed".into(),
                version: "3.2.1".into(),
                ..Default::default()
            });
            let _ = server.start(TcpTransport::from_stream(stream), ());
        })
    }

    #[test]
    fn a_real_server_is_reported_by_name_and_version() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = addr_of(&listener);
        let server = serve_one(listener);

        let report = probe(&addr).unwrap();
        assert_eq!(report.name, "probed");
        assert_eq!(report.version, "3.2.1");
        assert_eq!(report.address, addr.to_string());
        assert_eq!(
            report.to_string(),
            format!("ok - {addr} is serving probed 3.2.1")
        );
        server.join().unwrap();
    }

    #[test]
    fn a_real_server_is_reached_by_name_as_well_as_by_literal() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = RemoteAddr::parse(&format!("localhost:{port}")).unwrap();
        let server = serve_one(listener);

        let report = probe(&addr).unwrap();
        assert_eq!(report.name, "probed");
        assert_eq!(report.address, format!("localhost:{port}"));
        server.join().unwrap();
    }

    #[test]
    fn nothing_listening_is_an_error_naming_the_address() {
        let addr = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            addr_of(&listener)
        };

        let Err(err) = probe(&addr) else {
            panic!("nothing is listening there");
        };
        let text = err.to_string();
        assert!(text.contains("cannot connect to"), "{text}");
        assert!(text.contains(&addr.to_string()), "{text}");
    }

    #[test]
    fn a_name_that_does_not_resolve_is_an_error_naming_the_address() {
        let addr = RemoteAddr::parse("no-such-host.invalid:7211").unwrap();

        let Err(err) = probe(&addr) else {
            panic!("a reserved-invalid name must not resolve");
        };
        assert!(
            err.to_string()
                .contains("cannot connect to no-such-host.invalid:7211"),
            "{err}"
        );
    }

    #[test]
    fn something_that_hangs_up_is_an_error_not_a_hang() {
        // A listener that accepts, reads the handshake, and closes:
        // TCP-reachable, but not MCP.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = addr_of(&listener);
        let acceptor = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut t = TcpTransport::from_stream(stream);
            let _ = t.read().unwrap();
            drop(t);
        });

        let Err(err) = probe(&addr) else {
            panic!("a peer that hangs up is not healthy");
        };
        assert!(
            err.to_string()
                .contains(&format!("{addr} hung up before answering `initialize`")),
            "{err}"
        );
        acceptor.join().unwrap();
    }

    #[test]
    fn something_that_closes_without_reading_is_an_error_naming_the_address() {
        // Closes before reading anything, so the probe's write may fail
        // (`EPIPE`) or its read may (EOF), depending on timing. Either way:
        // an error, the address in it, and no hang.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = addr_of(&listener);
        let acceptor = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            drop(stream);
        });

        let Err(err) = probe(&addr) else {
            panic!("a peer that hangs up is not healthy");
        };
        let text = err.to_string();
        assert!(text.contains("hung up"), "{text}");
        assert!(text.contains(&addr.to_string()), "{text}");
        acceptor.join().unwrap();
    }

    #[test]
    fn something_that_answers_with_an_error_reports_it() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = addr_of(&listener);
        let acceptor = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut t = TcpTransport::from_stream(stream);
            let _ = t.read().unwrap();
            t.write(&JsonRpcMessage::error(
                1i64,
                crate::types::JsonRpcError::internal_error("not today"),
            ))
            .unwrap();
        });

        let Err(err) = probe(&addr) else {
            panic!("an error response is not healthy");
        };
        assert!(err.to_string().contains("not today"), "{err}");
        acceptor.join().unwrap();
    }

    #[test]
    fn a_notification_before_the_answer_is_skipped() {
        // A daemon may volunteer a log message between the request and its
        // response; that is the daemon working, not the probe failing.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = addr_of(&listener);
        let acceptor = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut t = TcpTransport::from_stream(stream);
            let _ = t.read().unwrap();
            t.write(&JsonRpcMessage::notification(
                "notifications/message",
                Some(serde_json::json!({ "level": "info", "data": "hi" })),
            ))
            .unwrap();
            t.write(&JsonRpcMessage::response(
                1i64,
                serde_json::json!({ "serverInfo": { "name": "n", "version": "v" } }),
            ))
            .unwrap();
            let _ = t.read().unwrap(); // initialized
            let _ = t.read().unwrap(); // ping
            t.write(&JsonRpcMessage::response(2i64, serde_json::json!({})))
                .unwrap();
        });

        let report = probe(&addr).unwrap();
        assert_eq!(report.name, "n");
        acceptor.join().unwrap();
    }

    #[test]
    fn a_handshake_that_answers_and_then_stops_is_an_error() {
        // `initialize` answered, `ping` not: the far end speaks just enough
        // MCP to be mistaken for a server, and the probe must not be fooled.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = addr_of(&listener);
        let acceptor = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut t = TcpTransport::from_stream(stream);
            let _ = t.read().unwrap();
            t.write(&JsonRpcMessage::response(
                1i64,
                serde_json::json!({ "serverInfo": { "name": "n", "version": "v" } }),
            ))
            .unwrap();
            // Hang up instead of answering the ping.
        });

        let Err(err) = probe(&addr) else {
            panic!("a server that stops answering after the handshake is not healthy");
        };
        // Which step notices - the `initialized` write, the `ping` write, or
        // the `ping` read - depends on when the kernel sees the close. What
        // does not depend on timing: it is a hang-up, and it names the peer.
        let text = err.to_string();
        assert!(text.contains("hung up"), "{text}");
        assert!(text.contains(&addr.to_string()), "{text}");
        assert!(
            !text.contains("`initialize`"),
            "the handshake itself was answered: {text}"
        );
        acceptor.join().unwrap();
    }
}
