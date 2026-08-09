# MCP 2025-11-25 Migration Notes

Decision log for the jump from `2025-03-26` to `2025-11-25`, which means
adopting **two** revisions at once: `2025-06-18` and `2025-11-25`.

Read this if you maintain a downstream server, or if you want to know why
something was built the way it was rather than the obvious way.

Confidence is flagged per decision. Anything marked **REVIEW** is a judgement
call that deserves a second opinion; anything marked **RESOLVED** was one, and
has since been settled — the original reasoning is kept alongside what replaced
it, because a decision that got reversed is worth more than one that was never
questioned.

Everything the first pass flagged for review has now been resolved: full JSON
Schema validation (§3.3), timeouts on server-initiated requests (§3.4), task
`input_required` (§3.5), and cross-platform task-ID entropy (§3.7). The known
flake in §5 is fixed, along with two others found while confirming it.

---

## 1. Breaking changes

Every break is listed here with its fix. The whole set is mechanical; none of
it requires rethinking a server's design.

### 1.1 `Content` variants gained `annotations`, and `Content::Resource` changed shape

**Confidence: high.**

Content blocks carry optional `annotations` (audience / priority /
lastModified) in every revision from 2025-06-18 on, and there is no way to add
an optional field to a struct-variant without breaking struct-literal
construction — Rust's default field values are still unstable.

Separately, `Content::Resource` was flat:

```rust
Content::Resource { uri, mime_type, text }   // serialized {"type":"resource","uri":...}
```

The spec nests it:

```json
{ "type": "resource", "resource": { "uri": ..., "mimeType": ..., "text": ... } }
```

We were emitting something no conformant client would read. That is a bug fix
that happens to be a break.

**Fix:** use the constructors instead of struct literals.

```rust
Content::Text { text: msg }              ->  Content::text(msg)
Content::Image { data, mime_type: m }    ->  Content::image(data, m)
Content::Resource { uri, .. }            ->  Content::embedded(ResourceContent::text(uri, body))
```

Verified against the four downstream servers: 10 call sites total, all
`Content::Text` or `Content::Image`. No downstream code constructs
`Content::Resource`.

### 1.2 Structs gained fields; use `..Default::default()`

**Confidence: high.**

`CallToolResult`, `Tool`, `Prompt`, `PromptArgument`, and `Resource` all gained
fields (`structuredContent`, `title`, `outputSchema`, `icons`, `_meta`, …).
They all derive `Default` now, so:

```rust
CallToolResult { content: vec![...], is_error: false }
// becomes
CallToolResult { content: vec![...], is_error: false, ..Default::default() }
// or better
CallToolResult::content([...])
```

9 downstream call sites for `CallToolResult`, 1 for `PromptArgument`.

### 1.3 `#[non_exhaustive]` on `Content`, `ResourceContent`, `McpError`, `LogLevel`

**Confidence: high.**

Matching on these now needs a wildcard arm. Done deliberately and all at once,
so future spec revisions can add variants without another break. `LogLevel`
went from 4 variants to the 8 RFC 5424 severities MCP actually specifies, which
would otherwise have been a break on its own.

These are enums, not structs, so downstream can still *construct* every
variant. Only exhaustive `match` is affected.

### 1.4 `McpError::ToolError` / `InvalidParams` from a tool are now results, not errors

**Confidence: high. This is a runtime behavior change, not a compile break.**

Returning `Err(McpError::ToolError(msg))` from `Tool::execute` used to produce a
JSON-RPC error. It now produces `CallToolResult { is_error: true }`.

This is what the spec asks for, and SEP-1303 sharpened it in 2025-11-25:

> Clarify that input validation errors should be returned as Tool Execution
> Errors rather than Protocol Errors to enable model self-correction.

The model now *sees* the failure text and can retry with corrected arguments,
instead of the host swallowing an opaque protocol error. Downstream servers
need no change — 81 `McpError::ToolError` sites across the fleet all improve.

Unknown-tool moved from `-32000` to `-32602`, which the tools spec's own
example uses.

### 1.5 `ClientCapabilities` field types changed

**Confidence: high.**

```rust
sampling: HashMap<String, Value>   ->  Option<SamplingCapability>
roots:    RootCapabilities         ->  Option<RootCapabilities>
```

Absent and present-but-empty mean different things — `{}` means "basic
sampling", absent means "do not send me sampling requests at all" — and a
`HashMap` cannot express that distinction. This type is only ever deserialized
by the framework; nothing downstream constructs it.

### 1.6 Error codes moved

**Confidence: high.** See §3.1.

---

## 2. Non-breaking additions worth knowing about

- `ServerConfig` gained `title`, `description`, `website_url`, `icons`,
  `default_log_level`, `stderr_logging`, `supported_versions`. All four
  downstream servers build it with `..Default::default()`, so this costs
  nothing.
- `Tool` gained defaulted trait methods: `title`, `output_schema`, `icons`,
  `task_support`, `meta`, `as_protocol_tool`. Existing impls compile untouched.
- `Resource` and `PromptDef` likewise.
- `ToolEnv` gained `elicit*`, `create_message`, `list_roots`, `send_request`,
  `send_request_with_timeout`, `log_data`, `log_enabled`, `is_cancelled`,
  `is_task`, `task_id`, `client_capabilities`. Its fields are private with no
  public constructor, so additions are free.
- `Transport` gained `set_read_timeout`, defaulted to "not supported". A custom
  transport compiles untouched and keeps blocking forever, which is what it did
  before; implement it to get elicitation timeouts and task `input_required`.
- `ServerConfig` also gained `request_timeout`. It builds with
  `..Default::default()` downstream, so this costs nothing.
- `McpError` gained `Timeout`. The enum is `#[non_exhaustive]`, so downstream
  matches already carry a wildcard arm.

---

## 3. Decisions that were not obvious

### 3.1 Error codes: `-32002` belongs to resources

**Confidence: high.**

We used `-32002` for prompt-not-found. It is the one implementation-defined
code MCP assigns a meaning to, and that meaning is *resource* not found. A
conformant client asking for a missing prompt was told a resource was missing.

- `PromptNotFound`: `-32002` → `-32602`. The prompts spec classifies an invalid
  prompt name as Invalid params, with no dedicated code.
- `ResourceNotFound`: `-32001` → `-32002`. `-32001` was never a spec code
  either; moving onto `-32002` is only possible now that prompts have vacated.

Both now carry `data` (`uri` / `name`).

**Downstream impact:** a client that special-cased `-32001`/`-32002` from an
sml_mcps server sees different codes. No Rust API changed.

### 3.2 `env.log()` swallows errors and falls back to stderr

**Confidence: high.**

Three problems were tangled together: we emitted `notifications/message`
without declaring the `logging` capability (a MUST), never implemented
`logging/setLevel` so clients could not turn the volume down, and — once
gating existed — records would silently vanish.

The fix declares the capability, implements `setLevel`, gates on the threshold,
and routes anything the client will *not* see to stderr. `StderrLogging` picks
the policy; `Fallback` is the default, so nothing is ever lost and nothing is
duplicated.

`env.log()` also stopped propagating transport errors. **Logging must not be
able to fail an otherwise-working tool.** A failed write is reported on stderr
and swallowed. The signature is unchanged, so this is invisible at compile
time — noted here because it is a real semantic change.

Default threshold is `Info`. The spec does not define a pre-`setLevel` default,
so this is our choice; `ServerConfig::default_log_level` overrides it.

### 3.3 Output-schema validation is full JSON Schema

**Confidence: high. RESOLVED** — was "deliberately partial, REVIEW if you plan
to lean on it".

Declaring `outputSchema` is a MUST-level promise: "Servers MUST provide
structured results that conform to this schema." The first cut of
`src/schema_check.rs` checked only presence, the top-level `type`, `required`,
and one level of property types, and explicitly declined to judge `pattern`,
`minimum`, `format`, `additionalProperties`, `$ref`, or `oneOf`/`anyOf`/`allOf`.
Anything it accepted could still be rejected by a strict client, which made it
close to worthless as a guarantee.

It now validates properly, through `boon` (draft 2020-12).

**Why `boon` over `jsonschema`:** `jsonschema`'s default features pull in
reqwest and rustls, and it offers a tokio-backed resolver. `boon` has no async
anywhere in its tree. It costs ~25 transitive crates, mostly the `url`/`idna`
chain. That is a real cost and a deliberate one — the crate is small because it
excludes tokio, not because it refuses useful dependencies.

Two behaviours are deliberate:

- **An uncompilable schema is skipped, not fatal.** A typo in a server's own
  `outputSchema` should not fail every call to a tool that works. The presence
  half of the promise is still enforced.
- **External `$ref`s are refused, not resolved.** boon's default loader reads
  `file://` URLs, which would turn validation into a file read as a side
  effect. A no-op loader closes that off. Internal `#/$defs/...` refs are
  unaffected.

`format` stays an annotation, per 2020-12 and boon's default.

`CompiledSchema` is available for servers that want to compile once and reuse;
`validate_structured_output` keeps its old signature and compiles per call
(sub-millisecond for typical tool schemas).

**Downstream impact:** a server whose `structuredContent` did not really match
its declared schema now gets an error where it previously got silence. That is
the point, but it is worth knowing about before upgrading.

### 3.4 Server-initiated requests: one reader, bounded wait

**Confidence: high on the design. The timeout question is settled — see
below.**

Elicitation and sampling need the server to send a request and block for a
response. In a sync server with no runtime, the thing reading the transport is
the main loop, and the code wanting the answer is a tool inside that loop.

The invariant that makes it work (`src/broker.rs`): **at most one thing reads
at a time.** Whoever is reading owns everything it pulls off the wire — its own
response is returned, a response for another id is parked for that waiter, and
anything else is deferred and replayed to the main loop before it reads again.
Nothing is dropped, nothing is delivered twice.

**Alternatives considered:**

- *A dedicated reader thread with channels.* Cleaner in the abstract, but it
  restructures the entire server loop and forces `Transport` to be split into
  independent read/write halves, which not every transport supports.
- *smol.* Introduces async through the whole call chain for two features, and
  `Tool::execute` would have to become async. Rejected outright.

**Timeouts: RESOLVED** — was "no timeout, REVIEW the timeout question".

The original reasoning was that interrupting `Transport::read` needs a second
thread per call. It does not; it needs the transport to be able to expire a
read, which both bidirectional transports can do. `UnixTransport` sets
`SO_RCVTIMEO`; `StdioTransport` polls stdin.

`Transport::set_read_timeout` reports whether the deadline was honored rather
than pretending, and defaults to `Ok(false)` — a transport that has not thought
about deadlines cannot deliver them.

`ServerConfig::request_timeout` defaults to **2 minutes**, chosen for
elicitation, which waits on a person rather than a machine.
`ToolEnv::send_request_with_timeout` overrides it per call; `None` restores the
old unbounded wait.

On expiry the request is abandoned — a late answer is discarded rather than
handed to whoever asks next, which would be silent data corruption — and
`notifications/cancelled` goes out, which the spec asks of a party that times
out.

Framing under a deadline lives in `src/transport/line.rs`, shared by both
transports. Three things it gets right that `BufRead::read_line` does not: an
expired read keeps the bytes it already had (losing them desyncs framing
permanently), the deadline covers the whole message rather than each syscall,
and bytes are decoded only once a full line exists, so a multi-byte character
split across reads survives.

Polling stdin meant giving up `io::Stdin`, whose private buffer is invisible to
`poll`: a client that pipelined two messages into one write would have looked
idle while holding the answer. `StdioTransport` reads fd 0 directly now,
borrowing the descriptor rather than owning it. **If your server also reads
stdin itself, that is now a private buffer it cannot see** — no downstream
server does.

One platform note, now a regression test: on macOS `setsockopt(SO_RCVTIMEO)`
fails with `EINVAL` once the peer has closed, so clearing a deadline that was
never armed turned an ordinary hangup into an IO error.

**Server-initiated request ids** are strings prefixed `sml-`. JSON-RPC shares
one id namespace across both directions; a numeric id could collide with a
client's. The prefix makes the two spaces disjoint by construction.

### 3.5 Tasks reach `input_required`

**Confidence: high. RESOLVED** — was "tasks never reach `input_required`,
REVIEW if a downstream server wants elicitation inside a long task".

The original reasoning was sound: a task worker runs on its own thread, nothing
reads the transport there, and the server loop may itself be blocked inside
`tasks/result` waiting on that very task. A round trip could never complete, so
workers got `can_request: false` and a clear refusal rather than a deadlock.

Both halves are now fixed, and it did not take the dedicated-reader-thread
redesign this section originally predicted.

**The worker does not read.** It registers as a waiter in the broker, writes its
request, and blocks on a channel until whichever thread *is* reading hands the
answer over. Responses the server loop used to drop — safe only while nothing
could be waiting for one — are routed to that waiter. The task sits in
`input_required` for exactly as long as it waits, then returns to `working`.

**`tasks/result` pumps.** It reads the transport while it blocks instead of only
sleeping on the store, so an elicitation issued by the task it is waiting for
can still get through. Client traffic is deferred to the main loop exactly as
during any other server-initiated request. Reads carry a short deadline, because
nothing on the wire announces a task finishing.

**Writes moved to a second handle.** The server loop holds the transport lock
for the whole of a blocking read, so a worker writing through it would wait for
the loop to wake — the very thing it is trying to cause. `try_clone_writer`
already existed for the bridge; the server uses it the same way. Every server
write then had to move onto that one handle: two handles to one socket are two
different mutexes, and the loop's responses interleaved with a worker's requests
into a line that was not valid JSON. This incidentally fixes a task worker's log
records being stuck behind the loop's read.

**Where it does not apply.** Splitting the transport and bounding a read are
both required. The HTTP transport can do neither, so workers there are refused
exactly as before, and tasks move `working -> completed | failed | cancelled`.
The same holds for any custom transport that has not implemented
`set_read_timeout`.

**Downstream impact:** none required. A server that never elicits inside a task
is unaffected; one that wants to now can.

### 3.6 Tasks are opt-in

**Confidence: high.**

`Server::enable_tasks()` must be called explicitly. The spec is strict about
what declaring the capability commits you to, and equally clear about the
alternative:

> Receivers that do not declare the task capability for a request type MUST
> process requests of that type normally, ignoring any task-augmentation
> metadata if present.

A server that never calls it behaves exactly as before and drops any `task`
field on the floor. Correct, and zero downstream churn.

The context factory exists because a worker thread cannot borrow the `&mut C`
the server loop holds. A task therefore sees a *fresh* context, not the one
from the request that spawned it. For servers whose context is a handle to
shared state (an `Arc<Db>`, a config) this is invisible. For one holding
per-request mutable state it is a real semantic difference — **document it in
your server if that applies.**

### 3.7 Task IDs use the platform CSPRNG

**Confidence: high. RESOLVED — the non-unix path no longer differs.**

The spec is blunt:

> If context-binding is unavailable, receivers MUST generate cryptographically
> secure task IDs with enough entropy to prevent guessing.

That is every stdio server — the task ID is the only thing protecting a task's
results. So this is not decoration.

**RESOLVED** — was "the unix path is solid, the fallback is best-effort, REVIEW
the non-unix path".

The original code read `/dev/urandom` on unix and fell back to `RandomState`
mixed with a counter and the clock elsewhere. Unpredictable in practice, but not
a CSPRNG, and not something to rest a security property on.

Task ids now come from `getrandom` on every target, which is the platform source
directly: `getrandom(2)` on Linux, `arc4random_buf` on the BSDs and macOS,
`ProcessPrng` on Windows. That deletes the `cfg(unix)` split and the hand-rolled
`/dev/urandom` read. It is the same crate `rand` uses for this, has no async in
its tree, and adds `cfg-if` next to the `libc` we already depend on.

The `RandomState` mixer survives only for a platform with no entropy source at
all, since panicking inside a tool call is worse than a degraded id. It says so
on stderr if it is ever reached.

Entropy is tested per-bit now rather than only for uniqueness — a source stuck
in its high bytes still produces unique-looking ids.

We also do **not** declare `tasks.list` conditionally on auth context, though
the spec suggests receivers that cannot identify requestors `SHOULD NOT`
declare it. **REVIEW:** for a stdio server there is exactly one requestor, so
listing exposes nothing the requestor did not create. For a future HTTP
deployment with real auth, this needs revisiting alongside per-context task
binding, which is not implemented.

### 3.8 Elicitation schema builder can only express legal schemas

**Confidence: high.**

Form-mode `requestedSchema` is restricted to a flat object of primitives so
clients can render a form without implementing JSON Schema. `ElicitSchema`
can only express that subset, so anything it produces is valid by
construction — including all four enum shapes SEP-1330 defines.

`required()` silently ignores names that were never added: a typo would
otherwise produce a schema demanding a field with no corresponding form
control, which no user could ever satisfy.

`ElicitResult::accepted_content()` returns data only on `accept`, so a client
that wrongly attaches content to a `decline` cannot get the server to act on it.

### 3.9 Origin validation defaults to loopback-only

**Confidence: high.**

Only a *present* `Origin` is checked, matching the spec's exact wording ("If
the `Origin` header is present and invalid"). Every non-browser client sends
none, and the attack requires a browser, which always sends one. Rejecting
absent origins would break every real deployment for zero security.

Default is `OriginPolicy::Loopback`, which is what actually stops rebinding:
the attacker's page is served from a public origin, so its `Origin` never looks
like loopback. `Allowlist` and `Any` are available; `Any` is an explicit
opt-out.

### 3.10 Audience validation is enforced by us, not by `jsonwebtoken`

**Confidence: high — a test caught this.**

`jsonwebtoken` only compares `aud` when the claim is *present*. A token with no
`aud` at all passes its check. That is exactly the token the spec says to
refuse:

> MCP servers MUST only accept tokens specifically intended for themselves and
> MUST reject tokens that do not include them in the audience claim.

`JwtValidator::for_resource` therefore re-checks after decoding rather than
trusting the library's edge-case semantics.

### 3.11 HTTP error bodies are JSON-RPC

**Confidence: high.**

`HttpServer` returned plain-text bodies (`Internal Error: ...`) with bare
status codes. The spec permits a JSON-RPC error response with no `id`, and it
lets a client tell *why* a request failed instead of guessing from the status.

The duplicated request loops in `serve` / `serve_with_auth` were factored into
shared `precheck` / `read_body` / `finish` helpers so the two paths cannot
drift apart again — which they already had.

### 3.12 `initialize` params are all `#[serde(default)]`

**Confidence: medium. REVIEW.**

The spec marks `protocolVersion`, `capabilities`, and `clientInfo` as required.
We accept a sparse `initialize` and negotiate down instead of rejecting.

**Rationale:** the existing code already treated `params: None` as
`InitializeParams::default()`, so rejecting `{}` while accepting *nothing* was
incoherent. Being liberal in what we accept costs nothing — an empty version
matches no supported version and negotiates to the latest anyway.

**Counter-argument:** a strict server would surface client bugs earlier. If you
prefer that, make the fields required again; the negotiation logic is unchanged
either way. A *malformed* initialize (wrong types) is `-32602`, not a parse
error.

---

## 4. Deliberately not implemented

| Feature | Why |
|---|---|
| `completion/complete` | Optional; we declare no `completions` capability, which is compliant. Nothing downstream has completable arguments. |
| `resources/subscribe` | Optional; we declare no `subscribe`. No downstream resource changes after registration. |
| `notifications/*/list_changed` | Optional; we declare no `listChanged`. Tool/resource/prompt sets are fixed at startup. |
| Per-auth-context task binding | Meaningful only for a hosted HTTP deployment. See §3.7. |
| Acting on `notifications/cancelled` | Accepted and ignored. The server is single-threaded per request, so there is nothing to interrupt; task cancellation goes through `tasks/cancel`, which is implemented. |
| SSE resumability (`Last-Event-ID`) | Our HTTP transport buffers a whole response and returns it as one body. There is no long-lived stream to resume. |
| DCR / CIMD / OIDC discovery | Client-and-authorization-server concerns. The server-side obligation is Protected Resource Metadata, which *is* implemented. |
| Anything from 2026-07-28 | Out of scope by instruction. |

---

## 5. Test flakes (all fixed)

Three, all pre-existing. The first was the one on the list; the other two
surfaced while stress-running the suite to confirm it was gone.

### `bridge::tests::test_auto_start_connects_to_running_daemon`

Failed roughly one run in four. Not a test bug — a real one in `auto_start`.

It treated "socket file present, PID file absent" as an orphaned socket and
deleted it. That is also exactly what a healthy `UnixServer::serve` daemon looks
like, because only `serve_daemon` writes a PID file. A daemon that finished
binding in the window between the opening connect attempt and the `exists()`
check had its socket unlinked out from under it: still listening, on an inode
with no name, unreachable until its idle timeout.

Removal now requires proof. `probe_socket` connects, and only `ECONNREFUSED`,
`ENOENT`, or `ENOTSOCK` count as "dead". `EACCES` or a backlog-full `EAGAIN`
prove nothing and leave the file alone. A single `ECONNREFUSED` is ambiguous on
the BSDs — a saturated listener refuses identically — so the probe repeats, and
any successful connect ends it early and hands back that live connection instead
of a deletion.

The test dropped its retry loop: it waits for the daemon as setup, then calls
`auto_start` once, which is the behaviour under test.

### `test_sigterm_clean_shutdown` / `test_sigint_clean_shutdown`

Waited for the socket and PID file to disappear, then immediately asserted the
process was dead. The daemon removes those files and *then* unwinds and exits —
separate moments, and a loaded machine deschedules it in between. Both now wait
for the exit on the same 5s budget as every other wait, so a daemon that
genuinely hangs still fails the test.

### `test_daemon_survives_parent_exit`

`example_binary()` ran `cargo build --example unix_server` from inside each of
the five integration tests, concurrently. Cargo publishes the example by
unlinking and re-linking `target/debug/examples/unix_server`, so a test that had
just checked `exists()` could spawn a path another build had removed — `ENOENT`
from `spawn`, roughly one run in ten. The build happens once behind a `LazyLock`
now.

## 6. Verification

- 472 tests, all passing, `--all-features` (465 unit + 5 integration + 2 doc)
- `cargo clippy --all-features --all-targets`: clean
- `cargo fmt --check`: clean
- suite run 12+ consecutive times to confirm the flakes above are gone

Conformance checks that exist specifically as regression guards:

- prompt-not-found can never collide with resource-not-found again
- param structs never gain `deny_unknown_fields` (forward compatibility)
- JSON-RPC batch arrays are rejected with `-32600`, not an opaque parse error
- task IDs are unique, 128-bit, and non-sequential
- `tasks/result` genuinely blocks until the task finishes
- a task worker's elicitation completes, including while `tasks/result` is
  outstanding on that same task (the deadlock this design exists to avoid)
- a task worker is still refused where the transport cannot be pumped
- an expired server-initiated request cannot have its late answer mistaken for
  the next request's
- a read that times out mid-message keeps the bytes it already had
- `auto_start` never unlinks a socket a daemon is listening on
- an audience-less token is rejected
