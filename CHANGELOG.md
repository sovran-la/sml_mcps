# Changelog

Versioning is `BREAKING.FEATURE.FIX`: a change that breaks an existing
consumer bumps the first component, a compatible addition the second, and a
fix the third. History before 0.7.0 is in `git log`.

## 0.7.0

### Remote mode: `--connect <host:port>`

An MCP client on one machine can now reach a daemon on another, over the
`TcpSocketListener` the daemon already opens. The same binary, launched with
`--connect <host:port>`, is a shim that dials that listener instead of looking
for a Unix socket.

- **`Server::serve_daemon` / `serve_daemon_with`** gain a fourth `argv` mode.
  `--connect` takes a hostname or an IP literal (`jetson-memory:7211`; the
  resolver decides), validated only as `host:port` with a numeric port. The
  remote shim proxies stdio through the same `Bridge::run` the Unix shim uses.
  It never spawns a daemon, never touches the socket path or its directory or a
  PID file, never reads local configuration, and never retries: an unreachable
  daemon is an error naming the address and a non-zero exit, within a bounded
  connect timeout (10s per resolved address). `--connect` combined with
  `--daemon`, `--foreground` or `--socket` is an argument error.
- **`TcpTransport::connect_timeout(addr, Duration)`** - `connect` with a
  deadline per resolved address, for hosts that drop rather than refuse.
- **`cli`: `install --connect <host:port>`** writes an entry whose arguments
  end in `--connect <host:port>`, in every registry format (the five JSON
  clients and Codex's TOML), so the client launches the binary as a remote
  shim. An existing local entry is reported *updated*. `uninstall` removes the
  entry by name whatever address it carries; it accepts `--connect` so the
  install line uninstalls with one word changed.
- **`cli`: `health --connect <host:port>`** probes the remote daemon - an
  `initialize` and a `ping` over TCP - and reports `ok - <addr> is serving
  <name> <version>`, instead of calling the author's `on_health` handler. A
  bad value exits 2; an unreachable or non-MCP peer exits 1 naming the address.
- **`cli`: `ServerEntry::with_connect(addr)`** and the public field
  `ServerEntry::connect: Option<String>`. `arguments()` is now the one place
  the argument list is assembled, and both config writers and both
  `check_existing` comparisons go through it.
- A bare-entry server (`ServerEntry::new("x", &[])`) launched as
  `<binary> --connect host:port` dispatches to `serve` with the flag, as it
  does when launched bare.

### Source break

`ServerEntry` has a new public field, `connect`. Code that builds the struct as
a literal must add `connect: None`; `ServerEntry::new` and the builders are
unchanged. This is the break that puts the release at 0.7.0 rather than 0.6.2;
nothing else in the public API changed shape.

### Security note

TCP is in the clear. The daemon's TCP listener must be bound to an interface
only trusted machines can reach - a Tailscale address - never a public one.
This release adds no encryption and no authentication; it adds a client for the
listener that was already there.
