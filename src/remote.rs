//! The remote shim: `--connect <host:port>`.
//!
//! A shim that dials a daemon on *another* machine over the TCP listener it
//! opened with [`TcpSocketListener`](crate::TcpSocketListener), instead of the
//! Unix socket on this one. The flag, what counts as a valid value for it, and
//! the connection it makes all live here - shared between the daemon dispatch
//! and the CLI harness so the two cannot disagree about what `--connect`
//! means.
//!
//! What this is not: a remote daemon manager. A remote shim never spawns a
//! daemon, never looks at a socket path or a PID file, and never reads local
//! configuration. The daemon on the other machine owns all of that, and a shim
//! that consulted a local `config.toml` about a remote daemon would be
//! answering a question nobody asked.
//!
//! **TCP is in the clear.** Everything the daemon says goes over the wire
//! unencrypted and unauthenticated. Bind the daemon's TCP listener to an
//! interface only the people you trust can reach - a Tailscale address, a
//! loopback behind an SSH tunnel - never a public one. See
//! [`TcpTransport`](crate::TcpTransport).

use crate::transport::TcpTransport;
use crate::types::{McpError, Result};
use std::fmt;
use std::time::Duration;

/// Dial a daemon over TCP rather than looking for one on the Unix socket.
///
/// Sits beside `--daemon`, `--foreground` and `--socket` in the daemon's
/// `argv` dispatch, and is the same flag `install --connect` writes into a
/// client's config and `health --connect` probes with.
pub(crate) const CONNECT_FLAG: &str = "--connect";

/// How long a remote shim waits for a connection before calling the daemon
/// unreachable.
///
/// Long enough for a Tailscale peer to wake from a relay handshake; short
/// enough that an MCP client whose daemon is down finds out before it gives
/// up on the shim.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A `host:port` the shim was told to dial.
///
/// Validated for shape only - a non-empty host and a numeric port - and never
/// pre-parsed as a socket address. The real use is a name like
/// `jetson-memory:7211`, which only the resolver can turn into anything, and a
/// parser that insisted on an IP literal would refuse exactly the value the
/// flag exists for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteAddr(String);

impl RemoteAddr {
    /// The address a `--connect` value names, or what is wrong with it.
    pub(crate) fn parse(value: &str) -> std::result::Result<Self, String> {
        if value.chars().any(char::is_whitespace) {
            return Err(format!(
                "`{value}` is not a host:port - it contains whitespace"
            ));
        }

        let Some((host, port)) = value.rsplit_once(':') else {
            return Err(format!("`{value}` is not a host:port - there is no port"));
        };
        if host.is_empty() {
            return Err(format!("`{value}` is not a host:port - there is no host"));
        }
        if !matches!(port.parse::<u16>(), Ok(1..)) {
            return Err(format!(
                "`{value}` is not a host:port - `{port}` is not a port number"
            ));
        }

        Ok(Self(value.to_string()))
    }

    /// Dial the daemon, or say which address could not be reached and why.
    ///
    /// No retry and no respawn: there is nothing on this machine to respawn,
    /// and a shim that retried a remote daemon would be hiding the outage
    /// from the one person able to fix it.
    pub(crate) fn connect(&self) -> Result<TcpTransport> {
        TcpTransport::connect_timeout(self.0.as_str(), CONNECT_TIMEOUT)
            .map_err(|error| McpError::Internal(format!("cannot connect to {}: {}", self.0, error)))
    }
}

impl fmt::Display for RemoteAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The `--connect` value in `args`, if there is one, or what is wrong with it.
///
/// Reads `--connect <value>` and `--connect=<value>`. The program name is not
/// skipped here - callers hand in whatever slice is theirs to read - and every
/// other argument is left alone, because what else is on the line is the
/// caller's business.
///
/// A second `--connect` is an error rather than a last-one-wins: two
/// addresses is a command line that does not know where it is going.
pub(crate) fn connect_from_args<I>(args: I) -> std::result::Result<Option<RemoteAddr>, String>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut found: Option<RemoteAddr> = None;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        let arg = arg.as_ref();
        let value = if arg == CONNECT_FLAG {
            match args.next() {
                Some(value) => value.as_ref().to_string(),
                None => return Err(format!("`{CONNECT_FLAG}` needs a host:port")),
            }
        } else if let Some(value) = arg
            .strip_prefix(CONNECT_FLAG)
            .and_then(|rest| rest.strip_prefix('='))
        {
            value.to_string()
        } else {
            continue;
        };

        if value.is_empty() {
            return Err(format!("`{CONNECT_FLAG}` needs a host:port"));
        }
        let addr = RemoteAddr::parse(&value)?;
        if found.is_some() {
            return Err(format!("`{CONNECT_FLAG}` was given more than once"));
        }
        found = Some(addr);
    }

    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- the address -----------------------------------------------------

    #[test]
    fn an_ip_literal_and_port_is_an_address() {
        assert_eq!(
            RemoteAddr::parse("127.0.0.1:7211").unwrap().to_string(),
            "127.0.0.1:7211"
        );
    }

    #[test]
    fn a_hostname_and_port_is_an_address() {
        // The real use: a MagicDNS name. Not resolved here - that would make
        // parsing depend on the network - just accepted as given.
        for name in [
            "jetson-memory:7211",
            "localhost:7211",
            "jetson-memory.tail1234.ts.net:7211",
            "no-such-host.invalid:7211",
        ] {
            assert_eq!(RemoteAddr::parse(name).unwrap().to_string(), name, "{name}");
        }
    }

    #[test]
    fn an_ipv6_literal_in_brackets_is_an_address() {
        assert_eq!(
            RemoteAddr::parse("[::1]:7211").unwrap().to_string(),
            "[::1]:7211"
        );
    }

    #[test]
    fn a_value_with_no_port_is_refused() {
        for value in ["jetson-memory", "127.0.0.1", "jetson-memory:", "host:port"] {
            let err = RemoteAddr::parse(value).unwrap_err();
            assert!(err.contains("not a host:port"), "{value}: {err}");
            assert!(
                err.contains(value),
                "the message must name the value: {err}"
            );
        }
    }

    #[test]
    fn a_value_with_no_host_is_refused() {
        let err = RemoteAddr::parse(":7211").unwrap_err();
        assert!(err.contains("no host"), "{err}");
    }

    #[test]
    fn a_port_outside_the_range_is_refused() {
        for value in ["host:0", "host:65536", "host:-1", "host:7211a"] {
            let err = RemoteAddr::parse(value).unwrap_err();
            assert!(err.contains("not a port number"), "{value}: {err}");
        }
    }

    #[test]
    fn whitespace_is_refused() {
        for value in ["host :7211", " host:7211", "host:7211 ", "ho st:7211"] {
            assert!(RemoteAddr::parse(value).is_err(), "{value:?}");
        }
    }

    #[test]
    fn an_empty_value_is_refused() {
        assert!(RemoteAddr::parse("").is_err());
    }

    #[test]
    fn the_address_displays_as_it_was_given() {
        assert_eq!(
            RemoteAddr::parse("jetson-memory:7211").unwrap().to_string(),
            "jetson-memory:7211"
        );
    }

    // ---- the flag --------------------------------------------------------

    #[test]
    fn no_connect_flag_is_none() {
        assert_eq!(
            connect_from_args(["--daemon", "--socket", "/x"]).unwrap(),
            None
        );
        assert_eq!(connect_from_args(Vec::<String>::new()).unwrap(), None);
    }

    #[test]
    fn the_flag_and_a_value_are_read() {
        assert_eq!(
            connect_from_args(["--connect", "jetson-memory:7211"]).unwrap(),
            Some(RemoteAddr::parse("jetson-memory:7211").unwrap())
        );
    }

    #[test]
    fn the_flag_is_found_anywhere_in_the_line() {
        assert_eq!(
            connect_from_args(["serve", "-v", "--connect", "h:1", "--other"])
                .unwrap()
                .unwrap()
                .to_string(),
            "h:1"
        );
    }

    #[test]
    fn the_equals_form_is_read() {
        assert_eq!(
            connect_from_args(["--connect=jetson-memory:7211"])
                .unwrap()
                .unwrap()
                .to_string(),
            "jetson-memory:7211"
        );
    }

    #[test]
    fn the_flag_with_nothing_after_it_is_an_error() {
        let err = connect_from_args(["--connect"]).unwrap_err();
        assert!(err.contains("needs a host:port"), "{err}");

        let err = connect_from_args(["--connect="]).unwrap_err();
        assert!(err.contains("needs a host:port"), "{err}");
    }

    #[test]
    fn the_flag_with_a_bad_value_is_an_error() {
        let err = connect_from_args(["--connect", "jetson-memory"]).unwrap_err();
        assert!(err.contains("not a host:port"), "{err}");

        // The next flag is not a value, and must not be swallowed as one.
        let err = connect_from_args(["--connect", "--daemon"]).unwrap_err();
        assert!(err.contains("not a host:port"), "{err}");
    }

    #[test]
    fn the_flag_twice_is_an_error() {
        let err = connect_from_args(["--connect", "a:1", "--connect", "b:2"]).unwrap_err();
        assert!(err.contains("more than once"), "{err}");
    }

    #[test]
    fn only_the_exact_flag_counts() {
        for near_miss in [
            "--connection",
            "-connect",
            "connect",
            "--CONNECT",
            "--connect-to",
        ] {
            assert_eq!(
                connect_from_args([near_miss, "h:1"]).unwrap(),
                None,
                "`{near_miss}` should not be the flag"
            );
        }
    }

    // ---- connecting ------------------------------------------------------

    #[test]
    fn connecting_reaches_a_listener_by_literal_and_by_name() {
        use crate::transport::Transport;
        use crate::types::JsonRpcMessage;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        for addr in [format!("127.0.0.1:{port}"), format!("localhost:{port}")] {
            let mut client = RemoteAddr::parse(&addr).unwrap().connect().unwrap();
            let (server, _) = listener.accept().unwrap();
            let mut server = TcpTransport::from_stream(server);

            let msg = JsonRpcMessage::request(1i64, "ping", None);
            client.write(&msg).unwrap();
            assert_eq!(server.read().unwrap(), msg, "{addr}");
        }
    }

    #[test]
    fn connecting_to_nothing_names_the_address() {
        let addr = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().to_string()
        };

        let Err(err) = RemoteAddr::parse(&addr).unwrap().connect() else {
            panic!("nothing is listening there");
        };
        let text = err.to_string();
        assert!(text.contains("cannot connect to"), "{text}");
        assert!(text.contains(&addr), "the address must be named: {text}");
    }

    #[test]
    fn connecting_to_a_name_that_does_not_resolve_names_the_address() {
        let Err(err) = RemoteAddr::parse("no-such-host.invalid:7211")
            .unwrap()
            .connect()
        else {
            panic!("a reserved-invalid name must not resolve");
        };
        let text = err.to_string();
        assert!(
            text.contains("cannot connect to no-such-host.invalid:7211"),
            "{text}"
        );
    }
}
