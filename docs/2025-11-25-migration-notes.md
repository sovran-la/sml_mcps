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

An independent review of this branch then found 37 issues — 7 critical, 14
significant, 16 minor. All are fixed; §7 lists each one with what was wrong and
what changed. Two claims made in earlier revisions of this document were
falsified by that review and are corrected in place: batch arrays were *not*
answered on the wire (§6), and per-auth-context task binding is no longer
"deliberately not implemented" (§4, §3.7).

A **second** independent review then verified those 37 fixes — all real, none
faked — and found that three of them had moved the problem rather than removed
it, plus 22 issues of its own. All 22 are fixed; §8 lists each one. A third
falsified claim from this document is corrected in place: HTTP kept
per-*process* state, not per-connection (§6, §3.14). The two shipping blockers
it named were a remote OOM reachable before any handshake (§8, N1) and a
total-server denial of service from one `tasks/result` (§8, N2); the second is
why the HTTP transport now serves every request on its own thread (§3.13).

A **third** independent review then verified those 22, agreed with all of them,
and found 14 issues of its own — two of them ways to kill the process with a
request carrying no body, no token and no handshake. All 14 are fixed; §9 lists
each one. Its framing is worth keeping, because it is the same framing as last
time: this was "the third round in a row where a fix moved the problem one
layer outward rather than removing it… the ceiling was put where the review
pointed, and the thing on the other side of the ceiling was not looked at." The
thing on the other side was `tiny_http`'s body reader, and neither blocker was
fixable above it — which is why the HTTP/1.1 layer is now ours (§3.13). A
**fourth** claim from this document is falsified and corrected in place: an
idle pool did not queue a burst, it refused half of one (§3.13).

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

### 1.7 `HttpServer` context factories must be `Send + Sync + 'static`

**Confidence: high.**

`serve` and `serve_with_auth` used to call the factory on the one accept-loop
thread. Requests are served concurrently now (§3.13), so it is called on each
request's own thread:

```rust
pub fn serve<F>(self, addr: &str, context_factory: F) -> Result<()>
where F: Fn() -> C + Send + Sync + 'static     // was: F: Fn() -> C
```

A factory that closes over an `Arc` — which is what all four downstream servers
do — needs no change. One that closes over an `Rc`, a `RefCell`, or a borrow of
a local does, and could not have been correct under concurrency anyway.

`C` itself already had to be `Send + Sync + 'static`.

### 1.8 `paginate` returns a `Result`

**Confidence: high.**

```rust
let (page, next) = paginate(&items, &state);      // was
let (page, next) = paginate(&items, &state)?;     // now
```

A cursor past the end of the list is `-32602` rather than an empty page; see
§8, N16. Only relevant to a downstream server that paginates its own lists —
the built-in handlers do this internally.

### 1.9 `TaskStore::await_result` takes a patience

```rust
store.await_result(&id, requestor)                     // was
store.await_result(&id, requestor, Some(timeout))      // now, `None` waits for the TTL
```

See §8, N2. `Server` passes `ServerConfig::task_result_timeout`.

### 1.10 `TaskConfig` and `ServerConfig` gained fields

`TaskConfig::max_records`, `ServerConfig::task_result_timeout`,
`ServerConfig::max_message_bytes`. Both structs derive `Default`, so
`..Default::default()` construction (§1.2) is unaffected.

### 1.11 The `http` feature has no dependencies, and `tls` moved again

`http = []`. The HTTP/1.1 layer is ours now (§3.13), so the feature costs
nothing. Nothing here re-exported `tiny_http`, so a downstream `Cargo.toml` that
named it for its own reasons is unaffected — but it will no longer arrive by
way of this crate.

The one thing to change is a hand-written `tls` line: it is
`tls = ["http", "dep:rustls", "dep:rustls-pemfile"]` now, not
`tiny_http/ssl-rustls` and not `rouille/rustls`. Depending on the `tls`
*feature* rather than on the crate behind it needs no change.

### 1.12 `HttpServer::pool_size` is deprecated in favour of `max_connections`

**Confidence: high. Deprecated, not removed — it still compiles and still does
the sensible thing.**

There is no pool any more; each connection gets a thread of its own (§3.13), so
the number to give is a ceiling on live connections rather than a count of
pre-spawned threads:

```rust
HttpServer::new(config).pool_size(64)          // deprecated, forwards to:
HttpServer::new(config).max_connections(64)
```

The default moved from `8 × CPU` threads spawned at `serve()` time to 512
connections spawned on arrival. Two knobs are new alongside it, and both are
what make direct exposure supportable: `HttpServer::read_timeout` (30s, how long
a peer may take over a request head or body) and `HttpServer::idle_timeout` (2
minutes, how long a kept-alive connection may sit between requests).

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
server-initiated requests exactly as before, and tasks move
`working -> completed | failed | cancelled`. The same holds for any custom
transport that has not implemented `set_read_timeout`.

This section used to stop there, which read as though HTTP tasks were otherwise
fine. They were not: the task store went out of scope with the request that
created it, so the `taskId` in a `CreateTaskResult` resolved to nothing on every
subsequent request. Fixed in §7, S6 — the store outlives the request now.

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

**RESOLVED — tasks are now bound to an authorization context.** The original
text said `tasks.list` was declared unconditionally, with "for a stdio server
there is exactly one requestor" as the argument. That is right for stdio and
wrong for `serve_with_auth`, where an authorization context *is* provided and
the MUST bites:

> When an authorization context is provided, receivers **MUST** bind tasks to
> said context. […] receivers that cannot identify requestors **SHOULD NOT**
> declare the `tasks.list` capability.

It was masked by tasks not surviving a request on HTTP at all (§7, S6). Fixing
that made it a live cross-tenant leak, so both halves landed together.
`TaskContext` now says how requestors are identified — `SingleRequestor`
(stdio, one Unix connection), `Owner` (a token's subject and tenant), or
`Anonymous` (unauthenticated HTTP) — task records carry an owner, every task
operation filters on it, and `tasks.list` is declared only where requestors can
be told apart.

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

### 3.10 Audience validation is enforced by us, and is not optional

**Confidence: high. RESOLVED** — was "enforced by us, not by `jsonwebtoken`",
which was true and insufficient.

`jsonwebtoken`'s semantics are the wrong way round for this. With an audience
configured it *ignores* a token that carries no `aud` at all — exactly the token
the spec says to refuse:

> MCP servers MUST only accept tokens specifically intended for themselves and
> MUST reject tokens that do not include them in the audience claim.

And with none configured, `Validation::new` leaves `validate_aud: true, aud:
None`, which *rejects* every token that does carry an `aud` — i.e. every RFC
8707 conformant one. Backwards in both directions.

So the library's audience machinery is switched off entirely and the check lives
in `JwtValidator::validate`, driven by `for_resource` or `with_audience`. The
original text was correct about `for_resource` and missed the larger hole:
`serve_with_auth(addr, JwtValidator::hs256(SECRET), ..)` — the form this
document and the crate's own doc example both recommended — enforced no
audience at all. `serve_with_auth` now refuses to start with a validator that
binds none. See §7, C7.

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

### 3.13 The HTTP/1.1 layer is ours

**Confidence: high. Rewritten twice — read the two claims it replaces.**

The transport accepts on one thread and answers on one thread per connection.
Both halves of that sentence changed this cycle, and the reason is the same
reason each previous revision of this section changed: the ceiling kept being
put where the review pointed, and the thing on the other side of it was not
looked at.

**Why it is not somebody else's HTTP server.** The N4 fix refused an over-large
body before reading a byte of it. That is the right instinct, and against
`tiny_http` it was the wrong outcome, because the reader it hands back promised
to consume `Content-Length` bytes and its destructor keeps that promise:

```rust
// tiny_http-0.12.0 src/util/equal_reader.rs
fn drop(&mut self) {
    let mut remaining_to_read = self.size;
    while remaining_to_read > 0 {
        let mut buf = vec![0; remaining_to_read];   // the peer chose this number
        match self.reader.read(&mut buf) { ... }    // and this read has no deadline
    }
}
```

Two consequences, both measured on the wire before the fix, both reachable with
no token and no handshake:

- `Content-Length: 200000000000000000` and no body — 81 bytes — printed
  `memory allocation of 200000000000000000 bytes failed` and died with
  `SIGABRT`. The client got its `413` first, and then the server was gone.
- `Content-Length: 100000000` and no body — 72 bytes — was answered `413`, and
  the next request went unanswered. The drain has no deadline, so the worker
  never came back. Repeat it `pool_size + backlog + 1` times and the accept
  loop drains one itself and never iterates again: a permanently deaf server,
  every worker idle, for about 8 KiB of traffic.

Neither is fixable above `tiny_http`'s API, and this was checked rather than
assumed. The allocation happens *before* the destructor's first read, so
draining, responding, or dropping differently beforehand does not avoid it;
`Request` exposes no way to hand back its reader; and the drain cannot be
bounded because 0.12 sets no socket timeout and offers no way to set one. The
same layer is why connection concurrency was unbounded — `TaskPool::spawn`
starts a thread per connection with no ceiling, so a peer decided how many
threads this process has.

So the layer is ours: `src/transport/http1.rs`, `std::net`, no dependencies.
The subset is the subset MCP needs — `POST`/`GET`/`DELETE` on one path,
`Content-Length` and `chunked` bodies, keep-alive, one buffered response. No
pipelining, no compression, no ranges, no upgrade. Three properties are the
whole point, and each one is a finding:

- **Nothing is sized from a declared length.** A `Content-Length` is compared
  against a ceiling and thrown away. `read_exactly` grows a buffer as bytes
  arrive; a declared length no machine could hold costs one comparison.
- **Every read has a deadline.** `read_timeout` (30s) covers a request head, or
  a body once one has started. `idle_timeout` (2m) covers a kept-alive
  connection between requests. A body that never arrives costs one connection
  until the deadline.
- **Live connections are capped** at `max_connections` (512), one thread each,
  spawned on arrival rather than up front. A connection over the ceiling is
  answered `503` by a dedicated refusal thread, never by the accept loop —
  writing to a peer is peer-paced, and the accept loop is the one thread that
  must never wait on one.

An over-cap body is refused without being read *and without being drained*, and
its connection is closed. That is deliberate on both counts: the bytes are
still coming (or never will), and guessing where the next request starts on a
connection whose framing was abandoned is how a desynchronised connection
becomes a request-smuggling primitive. Closing is bounded on the way out — a
half-close, then a discard capped at 64 KiB and 250 ms, so the peer sees the
`413` instead of a reset.

Owning the parser also meant owning the framing hygiene that comes with it:
`Content-Length` together with `Transfer-Encoding` is `400`, conflicting
lengths are `400`, obsolete line folding is `400`, an extra space in the
request line is `400`, and an unknown transfer coding is `501`.

**The first claim this replaces.** An earlier revision said the transport ran
on `rouille`, and blamed `chrono`, `time`, `url`, `percent-encoding`,
`multipart`, `threadpool`, `filetime`, `sha1_smol`, `rand` and "its own older
`base64`" on it. Four of those were never rouille's: `time` arrives through
`jsonwebtoken` → `simple_asn1`, `url` and `percent-encoding` through `boon`,
and `base64 0.13` through `rustls-pemfile` on the TLS path.

**The second claim this replaces.** The revision after that said "the queue
holds one waiting request per thread, so a burst still queues; only sustained
overload is refused." That is false, and the review measured it. `WorkerPool`
used `sync_channel(backlog)`, which admits `backlog + 1` outstanding items no
matter how many workers are idle, because only one worker is ever inside `recv`
at a time. On a pool with **four idle threads** and a tool that does nothing:

| simultaneous pings | 200 | 503 |
|---|---|---|
| 8 | 8 | 0 |
| 32 | 16 | **16** |
| 128 | 46 | **82** |

Those were `ping`s against an idle server, and the `503` carries no `id`, so a
client could not even correlate the refusal with the request it lost.

**Where the pool went.** It is gone, and that is the fix for the table above. A
connection carries one request at a time, so bounding connections bounds work;
a second ceiling behind the first was one queue too many, and it was the wrong
size. Nothing is queued now — a connection either gets a thread or is told
there is no room — so a burst of 32 on an idle server is 32 threads and no
refusals, which is a test. Two other findings dissolve with it: `WorkerPool`'s
`Drop` joined every worker unconditionally, so one wedged handler wedged
shutdown forever, and the default of `8 × available_parallelism()` pre-spawned
512 threads on a 64-core host before a single request arrived. Threads are
spawned per connection now, so an idle server has two.

Measured after, on the same machine and with the same probes:

```
Content-Length: 200000000000000000, no body  -> 413, then ping -> 200 OK
Content-Length: 100000000, no body           -> 413, next request answered in 92µs
200 idle keep-alive connections, cap 512     -> +200 threads, 200 answered
200 idle keep-alive connections, cap 8       -> +8 threads, 8 answered, 192 × 503
                                                and back to baseline on close
```

**Dependencies.** Unique crates, `cargo tree -e normal`:

| | before | after |
|---|---|---|
| default | 64 | 64 |
| `--features http` | 69 | **64** |
| `--features hosted` | 84 | 79 |
| `--all-features` | 92 | 86 |

The `http` feature now costs nothing at all. What went: `tiny_http`, `ascii`,
`chunked_transfer`, `httpdate`, `log`, and on the TLS path the whole `rustls
0.20` / `webpki` / `sct` / `ring 0.16` / `rustls-pemfile 0.2` chain.

**TLS.** `rustls` directly, 0.23 rather than the unmaintained 0.20 that arrived
through `tiny_http/ssl-rustls`, with `ring` as the provider. The handshake runs
on the connection's own thread under the connection's own deadlines, never on
the accept loop. `serve_tls` still refuses a certificate it cannot use *before*
binding, so a mis-wired TLS path cannot serve plaintext on the HTTPS port — and
there is now a test that drives a real handshake and a real `ping` through it,
rather than only testing the refusal.

**Exposure.** This is a supported configuration now, which it was not before:
with no request deadlines anywhere in the stack, a peer that declared a body
and never sent it held a thread forever. Behind a reverse proxy, terminate TLS
there and bind to loopback; in front of nothing, keep the defaults and set
`origin_policy` deliberately.

**Sizing.** `max_connections` is the knob to think about alongside
`ServerConfig::task_result_timeout`: an in-flight request holds its connection
for its whole duration, including a `tasks/result` that is waiting, so the
ceiling has to be comfortably above the number of clients expected to be
blocked at once.

### 3.14 HTTP state is per *session*, not per process

**Confidence: medium-high.**

**The claim this replaces.** An earlier revision of this document said the HTTP
transport "keeps **per-connection** state across requests." It kept
per-*process* state: one `Session` on the `HttpServer`, not keyed by anything.
Every client on the listener read and wrote it. Confirmed on the wire — a client
that had never sent `initialize` was reported as supporting elicitation and
sampling because a *different* client had, and `logging/setLevel` from one
client changed what an unrelated one received. Under `serve_with_auth` that was
one tenant's session state driving another tenant's request. That claim is now
corrected, and this is the third falsified statement this document has had to
retract in place.

**What it is now.** State is keyed on `Mcp-Session-Id`, which transports
§Session Management makes a MAY, with each session behind its own lock. The id
is 128 bits from the platform CSPRNG, the same source task ids use — the spec
asks for "globally unique and cryptographically secure".

**The decision worth arguing with.** The review suggested requiring the header
on every request after `initialize`. This does not: a request that carries no
session id gets a **private, throwaway session** instead of an error. The
reasoning is that a client which ignores session management then behaves exactly
as it did *before* sessions existed — nothing carried, nothing inherited, no
bleed — where requiring the header would break every such client at once for a
feature the spec marks optional. A client that *does* echo the id gets full
continuity, including its tasks.

The consequences are worth stating plainly:

- an id this server does not know is `404`, per the spec's rule for a
  terminated session, and `DELETE` on the endpoint terminates one
- the id is only handed out once a request establishes something worth keeping
  (a handshake, a log level, a live task), so a client that only pings never
  churns the table
- **tasks are reachable only within the session that created them.** A client
  that wants to resolve a `taskId` on a later request must send the session id
  it was given. This is a behavior change from the S6 fix, which made the store
  process-wide; process-wide is exactly what N3 was about.
- the table is swept at 30 minutes idle and bounded at 256 sessions,
  least-recently-used first

---

## 4. Deliberately not implemented

| Feature | Why |
|---|---|
| `completion/complete` | Optional; we declare no `completions` capability, which is compliant. Nothing downstream has completable arguments. |
| `resources/subscribe` | Optional; we declare no `subscribe`. No downstream resource changes after registration. |
| `notifications/*/list_changed` | Optional; we declare no `listChanged`. Tool/resource/prompt sets are fixed at startup. |
| ~~Per-auth-context task binding~~ | **Now implemented.** See §3.7 and §7, S8. |
| Acting on `notifications/cancelled` | Accepted and ignored. The server is single-threaded per request, so there is nothing to interrupt; task cancellation goes through `tasks/cancel`, which is implemented. |
| SSE resumability (`Last-Event-ID`) | Our HTTP transport buffers a whole response and returns it as one body. There is no long-lived stream to resume. That also disposes of the neighbouring SHOULD — "the server SHOULD immediately send an SSE event consisting of an event ID and an empty `data` field in order to prime the client to reconnect" — since priming a client to reconnect to a stream that is already complete when it is sent would achieve nothing. Worth revisiting together if the transport ever streams incrementally. |
| The `GET`-opened SSE stream | A separate rule from resumability, and separately a MAY: transports §Listening lets a server open a stream on `GET` to push server-initiated messages, and lets one that does not "return HTTP 405". We return `405`, with `Allow: POST, DELETE`. The reason is the same as the row above — there is no long-lived stream here — but it is its own decision and was previously only implied by that one. |
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

- 626 tests, all passing, `--all-features` (619 unit + 5 integration + 2 doc)
- `cargo clippy --all-features --all-targets -- -D warnings`: clean
- `cargo fmt --check`: clean
- `cargo clippy` clean for every feature combination: none, `http`, `hosted`,
  `tls`
- `cargo build --all-features`: no future-incompatibility warnings, which was
  not true while `multipart` and `buf_redux` were in the tree (§3.13)
- suite run repeatedly to confirm the flakes above are gone, and to shake out
  the timing-sensitive concurrency guards added for §8 and §9
- §9's two critical findings were re-driven against the *previous* commit
  before the fix and against this one after it, with the same probe: the
  allocator abort and the wedged listener are both reproducible there and
  neither is here. The probes were deleted; the guards that replaced them are
  in the list below

Conformance checks that exist specifically as regression guards:

- prompt-not-found can never collide with resource-not-found again
- param structs never gain `deny_unknown_fields` (forward compatibility)
- JSON-RPC batch arrays are rejected with `-32600` **on the wire**, driven
  through a real server over a socket rather than by calling the parser — the
  earlier version of this claim tested only the parser, and the server hung up
  on the client without writing anything (§7, C1)
- every other malformed input is answered rather than fatal: bad JSON, a null
  id, a float id, non-UTF-8 bytes, an unknown top-level key
- task IDs are unique, 128-bit, and non-sequential
- `tasks/result` genuinely blocks until the task finishes
- a task worker's elicitation completes, including while `tasks/result` is
  outstanding on that same task (the deadlock this design exists to avoid)
- a task worker is still refused where the transport cannot be pumped
- an expired server-initiated request cannot have its late answer mistaken for
  the next request's
- a read that times out mid-message keeps the bytes it already had
- `auto_start` never unlinks a socket a daemon is listening on
- an audience-less token is rejected, and a validator that enforces no audience
  cannot be used to serve
- a panicking tool fails its task or its call, and never the server
- `tasks/cancel` and `tasks/get` are answered while `tasks/result` blocks
- a task's elicitations, logs and progress carry `related-task` metadata, and
  its status notifications do not
- HTTP answers 202 with no body to a notification, 400 to a malformed body, and
  keeps **per-session** state across requests — the earlier version of this
  claim said "per-connection", and the state was in fact shared by every client
  on the listener (§8, N3)
- one requestor's tasks are invisible to another
- a blocking `tasks/result` over HTTP does not delay an unrelated `ping` on
  another connection, measured (§8, N2) — re-measured against the `rouille`,
  the `tiny_http` + pool, and the thread-per-connection implementations (§3.13)
- an absurd `Content-Length` with no body behind it is answered `413` and the
  **process is still there afterwards** — it aborted out of the allocator
  before (§9, C1)
- a declared body that never arrives costs one connection and not the listener,
  once per slot and one over, with every attacker socket still open (§9, C2)
- a body *under* the cap that never arrives expires on the deadline rather than
  waiting for a peer that has stopped talking
- both of those, unauthenticated, against `serve_with_auth` — which is where
  they were reachable from
- a burst of 32 simultaneous pings against an idle server is answered 32 times,
  which the pool this replaced did not manage (§9, S1)
- a saturated HTTP server answers `503` with a JSON-RPC body and is unharmed by
  having refused: the ceiling lets go when the connection does
- two requests on one reused connection are both answered, in order, and a
  request whose body the handler never read does not desynchronise the next one
- an HTTP body over the cap is refused with `413` even when it is *chunked* and
  declares no length at all
- framing a proxy and this server could read differently is refused rather than
  guessed at: `Content-Length` with `Transfer-Encoding`, two different
  `Content-Length`s, obsolete line folding, an extra space in the request line,
  an unknown transfer coding
- a header value carrying a newline cannot forge a second response header
- `100 Continue` is sent for a body that will be accepted and withheld from one
  already refused
- a session id issued to one identity is not usable by another, and neither is
  a `DELETE` of it (§9, S2)
- `serve_tls` refuses a certificate it cannot use and binds nothing, so a
  mis-wired TLS path cannot serve plaintext on the HTTPS port — and a
  certificate it *can* use serves a real `ping` over a real handshake
- one client's `logging/setLevel` and declared capabilities do not reach another
- a response to an id the server never issued is dropped rather than retained
- the deferred queue refuses rather than growing, and says so with
  `-32603 reason = "overloaded"`
- the bridge answers unreadable input and keeps the session, including a message
  over the frame cap
- an unreadable request is answered with the id it carried, when it carried one
- an HTTP body over the cap is refused with `413`, including one whose
  `Content-Length` lies
- `require_initialization` works over HTTP, and per client

---

## 7. Review findings (all fixed)

An independent review of this branch against both revisions found 37 issues.
Each is listed with the commit-level summary of what was wrong; the fix always
came with a test that fails without it.

### Critical

| # | What was wrong |
|---|---|
| C1 | Any message the server could not parse ended the session with nothing written back — a one-line remote kill on stdio. Reachable by a batch array, `"id": null`, a float id, non-UTF-8, bad JSON, or one unknown top-level key (the envelope structs carried `deny_unknown_fields` under an untagged enum). Now: `RequestId::Null`, shape-based classification in `JsonRpcMessage::from_value`, and a read loop that answers and carries on. |
| C2 | A panicking task worker unwound past `store.finish`, so the task stayed `working` until its TTL and `tasks/result` — handled on the loop thread — blocked forever, taking every other request with it. Now: `catch_unwind` on both call paths, plus a bounded `await_result` that re-sweeps. |
| C3 | Every request sent while `tasks/result` was outstanding was deferred to a loop that could not run until it returned. Permanent deadlock with `request_timeout: None`, on exactly the flow the spec prescribes for `input_required`. Now: `ping`, `tasks/get`, `tasks/list` and `tasks/cancel` are dispatched inline from the pump. |
| C4 | Task-related requests and notifications carried no `io.modelcontextprotocol/related-task` metadata, so a client in `tasks/result` could not tell what an out-of-band elicitation belonged to. |
| C5 | HTTP answered a notification or client response with `200 {}` instead of `202 Accepted` and no body — and `{}` is not a JSON-RPC message. |
| C6 | `tasks/result` overwrote the result's `_meta` instead of merging, deleting whatever the tool had put there. |
| C7 | Audience validation was opt-in, and the documented example opted out — accepting tokens minted for anyone. See §3.10. |

### Significant

| # | What was wrong |
|---|---|
| S1 | Type-mismatched params were reported as `-32700` instead of `-32602`. |
| S2 | HTTP reported a client's syntax error as `500 -32603 Internal error`. |
| S3 | An invalid cursor silently returned page one, which is an infinite loop for a client that follows cursors. `-32602` now, and `tasks/list` also rejects a cursor past the end. |
| S4 | `outputSchema` was enforced on the synchronous path only, so the same tool succeeded as a task and failed as a call. |
| S5 | `logging/setLevel` was acknowledged and dropped on HTTP, along with client capabilities and the negotiated version, because the server was rebuilt per request. |
| S6 | Tasks over HTTP handed back a `taskId` no later request could resolve, while the capability was advertised as working. |
| S7 | `tools/call` arguments were never checked against `inputSchema`. |
| S8 | Tasks were not bound to an authorization context. See §3.7. |
| S9 | `LineReader::partial` grew without a cap: a peer that never sends a newline could OOM the process, once per connection. |
| S10 | `execution.taskSupport` was advertised without the `tasks` capability, which contradicts tool-level negotiation rule 1. |
| S11 | A panic anywhere in a request killed the HTTP accept loop, and three `messages.lock().unwrap()`s panicked on the poison that panic produced. |
| S12 | `POST /mcp?sessionId=abc` 404'd, because the endpoint was compared against the raw request target. |
| S13 | Broader form of C3: nothing at all was answered while `tasks/result` blocked. |
| S14 | A task cancelled mid-elicitation leaked its broker waiter and sent no `notifications/cancelled`. |

### Minor

| # | What was wrong |
|---|---|
| M1 | `Bearer` was matched case-sensitively (RFC 7235 §2.1: it is case-insensitive). |
| M2 | An uncompilable `outputSchema` was skipped silently, so a typo bought permanent invisible non-enforcement. Reported once, at registration. |
| M3 | The output schema was recompiled — regexes and all — on every call. Compiled once now, with the input schema. |
| M4 | `send_progress` accepted only a string token, and nothing plumbed the client's `_meta.progressToken` through, which made progress unusable. |
| M5 | `is_valid_tool_name`, `insufficient_scope_challenge`, `Claims::missing_scopes` and `MODEL_IMMEDIATE_RESPONSE` were built, tested, and wired to nothing. All four are now used. |
| M6 | The README still listed task `input_required` as not included. |
| M7 | `page_size: 0` produced an empty page plus a cursor to the same offset — an infinite loop. Clamped to 1. |
| M8 | `set_status_message` could overwrite a cancelled task's diagnostic. |
| M9 | HTTP logged full request and response bodies unconditionally; tool arguments routinely carry credentials and PII. Gated on `debug`. |
| M10 | No initialization ordering was enforced. Available as `ServerConfig::require_initialization`, off by default since the relevant rules are client-side SHOULDs. |
| M11 | The `tools` capability was omitted for an empty tool map even when `tasks.requests.tools.call` was declared. |
| M12 | The SSE priming SHOULD was skipped without saying so. Now stated in §4. |
| M13 | `McpError::Timeout` was indistinguishable from a generic internal error. It carries `data.reason = "timeout"`. |
| M14 | `nbf` was not validated, so a not-yet-valid token was accepted. |
| M15 | The negotiated protocol version was computed and discarded. Kept now, and a test backs the claim that everything newer is additive and optional. |
| M16 | `tasks/result` error responses carried no related-task metadata. It goes in `error.data`. |

---

## 8. Second review findings (all fixed)

A second independent review verified the 37 fixes in §7 — all of them real, none
faked, each with a genuine regression guard — and found 22 issues of its own,
two of them shipping blockers. Its framing is worth keeping: the first round's
fixes were applied to the places the findings *named* and not to the
structurally identical places they didn't, and three of them moved the problem
rather than removing it.

Every one of the 22 is fixed. Each fix came with a test that fails without it.

### Shipping blockers

| # | What was wrong | Resolution |
|---|---|---|
| N1 | `RequestBroker::parked` was unbounded and never swept. The server has no in-flight requests of its own until a tool elicits or samples, so *every* response a peer sends before that is unmatched by construction — ~48 messages of 8 MiB reached a gigabyte, pre-handshake, while the server went on answering pings. | Fixed. A response to an id the broker never issued is dropped outright; what survives is capped at 16, oldest evicted, and a repeated answer to one id replaces rather than accumulates. |
| N2 | `tasks/result` over HTTP wedged the whole server. Requests were processed one at a time on the accept loop, so the mandated block held every other client for up to the task's TTL — client-requested, an hour by default. A regression the S6 fix introduced. | Fixed in two halves. The wait is bounded by `ServerConfig::task_result_timeout` (30s) on any transport that cannot be pumped, answering `-32603 reason = "timeout"` so a client can poll and ask again. And requests are served concurrently, one worker thread each (§3.13), so a blocked call costs one pool slot rather than the server. |

### Significant

| # | What was wrong | Resolution |
|---|---|---|
| N3 | The HTTP `Session` was process-wide, so capabilities and log level bled between clients — and, under `serve_with_auth`, between tenants. | Fixed: state is keyed on `Mcp-Session-Id`, one lock per session. See §3.14 for the design and the one place it deliberately departs from the review's suggestion. |
| N4 | The HTTP body had no cap: 48 MiB accepted and echoed back at the same size, while stdio and Unix capped at 8 MiB — the wrong way round relative to exposure. | Fixed: `ServerConfig::max_message_bytes` applies to HTTP too, checked against `Content-Length` before reading and enforced on the read itself. `413`. |
| N5 | `Bridge::pump` still died on one malformed line and returned `Ok(())`, so the client's session vanished with nothing written and no diagnostic. S9's new 8 MiB cap gave that a fresh trigger on a *legitimate* large message the shim used to forward. | Fixed: the shim answers and carries on, exactly as the server does, with teardown reserved for a connection that has actually failed. |
| N6 | Error responses carried `id: null` even when the id was readable, so a client that forgot `jsonrpc` got an error it could not correlate and blocked until its own timeout. | Fixed: `McpError::InvalidRequest` carries the id through classification. A genuinely unreadable id — a float, an object — still answers `null`, which is what the spec's exception is for. |
| N7 | `require_initialization` made the HTTP transport permanently unusable, for every client, forever. | Fixed: `Server::set_initialized`, carried in the session. Tested over HTTP, including that one client's handshake does not open the gate for another. |
| N8 | `RequestBroker::deferred` was unbounded while anything blocked the loop, and every queued `tools/call` was then executed serially on release. | Fixed: capped at 256, shedding the newest so the queue keeps its arrival order, and a shed *request* is answered `-32603` with `data.reason = "overloaded"` rather than dropped. |

### Minor

| # | What was wrong | Resolution |
|---|---|---|
| N9 | The `jsonrpc` value was never checked while a *missing* one was fatal — strictness exactly backwards. | Fixed: absent defaults to `"2.0"`, a wrong value is `-32600` (with its id, per N6). |
| N10 | A broker waiter leaked if the write of a server-initiated request failed. | Fixed: every non-success exit unregisters, one exit earlier than S14's. |
| N11 | `MAX_MESSAGE_BYTES` was a `pub(crate) const` with no escape hatch, so a server whose clients legitimately send a base64 attachment had to fork. | Fixed: `ServerConfig::max_message_bytes`, pushed down through a new `Transport::set_max_message_bytes`. |
| N12 | The cap could be overshot by one buffer refill, because the branch that found the newline appended without re-checking. | Fixed: checked on both branches. The constant means what it says. |
| N13 | `max_concurrent` counted the whole store, so one tenant on the shared HTTP store denied every other. | Fixed: counted per requestor, per tasks §Resource Management. |
| N14 | The HTTP `session` mutex was not poison-tolerant, so one panic would have answered `-32603 Session lock poisoned` forever. | Fixed: `unwrap_or_else(|e| e.into_inner())`, matching `HttpTransport::buffered`. |
| N15 | Terminal task records accumulated for their full TTL and counted toward nothing, bounded only by the client's request rate. | Fixed: `TaskConfig::max_records` (1024). Eviction only ever takes finished records; a store full of running ones refuses rather than discarding work. |
| N16 | A past-the-end cursor returned an empty page on four of the five list operations — indistinguishable from "that was everything". | Fixed: the check is in `paginate`, so `tasks/list` keeps its MUST-level wording and the rest get the SHOULD. Offset zero stays exempt: an empty list is not an error. |
| N17 | This document's "per-connection state" claim was not true. | Corrected in place, in §6 and §3.14, and the `Session` doc comment says what it actually is. |
| N18 | `answer_inline` bypassed `require_initialization`. Unreachable in practice, but an invariant enforced on one path is not an invariant. | Fixed: one `check_initialized`, called from every path that dispatches. |
| N19 | The content type of a task-augmented `tools/call` was a race between the worker's status notification and the response being read. | Fixed at the source: `HttpTransport` keeps notifications and the response apart, and the response seals the buffer. |
| N20 | `await_task` held the connection thread after the client hung up, waiting out the task's TTL on behalf of nobody. | Fixed: returns `TransportClosed`. The worker finishes on its own and its result waits in the store. |
| N21 | An HTTP task worker's notifications went into a buffer nobody drains, so `env.log()` from inside a task went nowhere. | Fixed rather than documented: a sealed transport refuses the write with `TransportClosed`, which is what makes `env.log()` fall back to stderr. |
| N22 | Two `eprintln!`s survived the M9 fix — the full request target, query string included, and `user=`/`tenant=` on every authenticated request. | Fixed: both behind the same `debug` gate as bodies. |

### Residuals the second review flagged on §7's fixes

- **C1** — the two gaps are N6 (the id) and N5 (the bridge). Both closed.
- **S3** — the residual is N16. Closed.
- **S13** — closed as far as it goes, and worth saying out loud rather than
  leaving implied: what `answer_inline` provides is the *narrow* form. `ping`,
  `tasks/get`, `tasks/list` and `tasks/cancel` are answered while a
  `tasks/result` blocks; `tools/list`, `tools/call`, `logging/setLevel`,
  `resources/*`, `prompts/*` and a second `tasks/result` are still deferred for
  its duration. That is deliberate — those four are the ones that are `&self`,
  touch only the store, and cannot re-enter `await_task` — and it covers what
  the spec actually promises, which is parallel `tasks/get` polling. The general
  form is *narrowed*, not closed. What has changed is that the queue behind it
  is now bounded and answers instead of growing (N8), and that on HTTP the block
  itself is bounded and concurrent (N2), so the narrowing is no longer load
  bearing.
- **M9** — the residual is N22. Closed.

### Two places this departs from the review's suggested fix

Both are judgement calls, recorded here so the next reviewer can disagree with
the reasoning rather than reverse-engineer it.

**N8, which end of the queue to shed.** The review suggested answering the
*oldest* queued request on overflow. This sheds the *newest* — the one being
deferred at that moment. Shedding at the door keeps whatever is already queued
moving in arrival order and gives the client its backpressure signal
immediately, where shedding the oldest fails requests that have already waited
longest and, under sustained load, can starve the queue of anything that ever
gets served. Same bound, same honesty, better ordering.

**N3, whether the session header is required.** The review suggested requiring
`Mcp-Session-Id` on every request after `initialize`. A request without one gets
a private throwaway session instead. See §3.14 — the short version is that
requiring it breaks every client that ignores an optional feature, while a
throwaway session gives exactly the pre-session behavior with none of the bleed.

---

## 9. Third review findings (all fixed)

A third independent review verified the 22 fixes in §8 — all real, each with a
genuine regression guard — re-measured the two properties §3.13 rests on rather
than trusting them, and found 14 issues of its own. Two were ways to kill the
process with a request carrying no body, no token and no handshake.

Every one of the 14 is fixed. Each fix came with a test that fails without it.

Its assessment of the pool is worth recording even though the pool is gone:
the concurrency reasoning in it was correct, the two edges it identified as
load-bearing were the two that mattered, and the property N2 needed — a
blocking `tasks/result` costs one slot, not the server — held on the wire, at a
worst-case 610 µs ping against a 4-second block. What it got wrong was its
*size*, and what it did not cover was the layer underneath it.

### Shipping blockers

| # | What was wrong | Resolution |
|---|---|---|
| C1 | One unauthenticated request, headers only, aborted the process. The N4 fix refused an over-large body before reading it; `tiny_http`'s `EqualReader::drop` then ran `vec![0; content_length]` on our own worker thread, and `alloc_zeroed` of a peer-chosen 200000000000000000 calls `handle_alloc_error`, which aborts. 81 bytes. The client got its `413` first. | Fixed, by owning the HTTP layer — see §3.13 for why it could not be fixed above `tiny_http`, which was checked rather than assumed. Nothing is sized from a declared length now: it is compared against a ceiling and thrown away, and the body of a refused request is never read *and never drained*. |
| C2 | The same destructor's drain was a blocking read with no deadline, and `tiny_http` 0.12 sets no socket timeout and exposes no way to. `Content-Length: 100000000` with no body held a worker forever; `pool_size + backlog + 1` of them — 65 requests, under 8 KiB, no token — reached the accept loop, which drained one itself and never iterated again. A permanently deaf server with every worker idle. | Fixed. Every read has a deadline (`read_timeout`, `idle_timeout`), an over-cap body is refused without a drain at all, and a refused *connection* is answered by a dedicated thread so the accept loop never waits on a peer. Measured after: the next request was answered in 92 µs, with the attacker's socket still open. |

### Significant

| # | What was wrong | Resolution |
|---|---|---|
| S1 | An **idle** four-thread pool refused 16 of 32 simultaneous pings, because `sync_channel(backlog)` admits `backlog + 1` outstanding items however many workers are free. This falsified §3.13's "a burst still queues; only sustained overload is refused", and the `503` carries no `id`, so a client could not correlate the refusal with the request it lost. | Fixed by removing the queue rather than resizing it. A connection carries one request at a time, so bounding connections bounds work; nothing waits behind a full queue because there is no queue. 32 simultaneous pings against an idle server are answered 32 times, which is now a test. The falsified claim is corrected in place in §3.13. |
| S2 | Sessions were keyed on the header value alone — one global map, shared across tenants. security_best_practices §Session Hijacking names the exact mitigation: "combine the session ID with information unique to the authorized user… Use a key format like `<user_id>:<session_id>`." Guessing or capturing an id got you another tenant's negotiated version, `initialized`, log level and a write handle to their `client_capabilities`. | Fixed: keyed `<owner>\u{1}<id>` under `serve_with_auth` and on the bare id under `serve`, where there is no identity to bind to. A mismatched id falls out as the `404` the spec already prescribes, and `DELETE` is scoped the same way, so one tenant cannot terminate another's session. |
| S3 | `tiny_http`'s `TaskPool` spawns an OS thread per connection with no ceiling: 200 idle keep-alive connections took the process from 694 to 890 threads, and `pool_size` did not enter into it. Past the platform's thread limit `thread::spawn` panics on the accept thread, which is C2's terminal state by another road. | Fixed: `HttpServer::max_connections` (512) is a real ceiling, one thread per live connection, spawned on arrival rather than up front. Measured at a cap of 8: 200 connections became +8 threads, 8 answered, 192 refused `503`, and the thread count returned to baseline when they closed. A thread the OS refuses closes that one connection instead of ending the listener. |

### Minor

| # | What was wrong | Resolution |
|---|---|---|
| m1 | `Sessions::check_in` was not poison-tolerant, and failed by *deleting* the session: a poisoned lock was indistinguishable from "nothing worth keeping", so the client's next request got `404`. N14 chose `into_inner()` for exactly this reasoning, and two of the three call sites followed it. | Fixed: all three are consistent now. |
| m2 | `Session::absorb` assigned `client_capabilities` wholesale while merging everything else, so two concurrent requests in one session could lose a handshake's declaration — `initialize` absorbs the real set, a concurrent `ping` absorbs an empty one over the top. | Fixed: merged, not assigned. A request whose capability set says nothing at all has nothing to say about capabilities. |
| m3 | `405` responses carried no `Allow` header. RFC 9110 §15.5.6 makes it a MUST, and clients probing for the old HTTP+SSE transport read 405s. | Fixed: `Allow: POST, DELETE` on the endpoint, `Allow: GET` on the metadata path. |
| m4 | An over-cap *chunked* body left the decoder mid-stream, so the next request on that connection got a bare `400` from `tiny_http` with no body — and behind a proxy pooling upstream connections, a desync is a smuggling primitive rather than an inconvenience. | Fixed at the source: an over-cap body is never read past the ceiling and its connection is closed, with `Connection: close` on the `413` saying so. Closing is bounded on the way out so the peer still sees the answer rather than a reset. |
| m5 | Six unconditional `eprintln!`s on remotely reachable paths, so any peer could write one stderr line per request without authenticating — and one of them echoed the peer's own `Origin` verbatim. | Fixed: the `Origin` echo and the body/parse diagnostics are behind the same `debug` gate as bodies. The three auth-failure lines stay ungated, which is the review's own recommendation: they are security events, and none of them interpolates peer text. |
| m6 | `guarded` covered the context factory and dispatch. A panic in `precheck`, `read_body`, `finish` or `send` was caught one layer out, and the `Request` was then dropped without a response — a bare `500` with an empty body, the one answer shape §3.11 exists to eliminate. | Fixed in two layers: the whole per-request answer is inside `catch_unwind` and comes back as a JSON-RPC `-32603`, and the connection loop has a backstop of its own so a handler that somehow unwinds past that still produces a response rather than a dropped connection. |
| m7 | A float `id` was answered `-32600 "invalid request: data did not match any variant of untagged enum RequestId"` — correct code, correct `id: null`, and a message naming a private Rust type. | Fixed: "request id must be a string or an integer", caught where it is noticed rather than inherited from a whole-struct parse. |
| m8 | A session could vanish without a `DELETE`: once a never-initialized session's tasks expire it holds nothing, so `check_in` drops it and the client's next request is `404`. Spec-legal, and worth saying rather than fixing. | Documented on `worth_keeping`, including which client it bites — one that creates tasks without ever initializing. |
| m9 | `evict_oldest` used `min_by_key` with a tuple containing a cloned `String`, so it allocated once per entry scanned, per eviction, to break a tie between identical instants. | Fixed: `min_by` with a comparison, which reads the same and allocates nothing. |

### Where this departs from the review's suggested fix

**C1/C2, drain versus own the layer.** The review's recommendation was to drain
the refused body through a fixed-size buffer now, file the two-line upstream fix
on `EqualReader::drop`, and pin or vendor `tiny_http` until it lands — noting
that the drain converts C1 into C2 rather than fixing it, and that
`Expect: 100-continue` defeats the drain and aborts anyway.

That reasoning is right, and it is why this does not drain. Draining is a read
on a peer's schedule, which is C2; not draining is an abort, which is C1; and
the third option — bounding the drain — needs a socket deadline `tiny_http` 0.12
does not have. Every route out of the pair requires changing the layer, and the
review lists "a different HTTP server" among the options for C2 and S3 both. A
vendored reader would have fixed C1 alone; a vendored `RefinedTcpStream` would
have added C2; neither touches S3. Owning ~1000 lines of `std::net` fixes all
three, removes five dependencies, and puts the ceilings where the rest of this
crate already keeps them. It is also the same trade §3.13 already made once, for
the same reason: the thread pool was cheaper to own than 27 crates.

The cost is honest and worth stating: this is a hand-written HTTP parser, and
hand-written HTTP parsers are where request smuggling lives. So the framing
rules a smuggler needs are refused rather than guessed at — `Content-Length`
with `Transfer-Encoding`, conflicting lengths, obsolete folding, an extra space
in the request line, an unknown transfer coding — and a connection whose body
was abandoned is closed rather than reused. Each has a test.

**S1, an independent backlog versus no backlog.** The review suggested giving
the queue its own depth and adding a short `send_timeout`. This removes the
queue instead. With one request in flight per connection, `max_connections` is
already the ceiling on outstanding work, and a second one behind it can only be
wrong in one of two directions — too small refuses healthy bursts, too large is
the unbounded queue the ceiling existed to prevent. A connection either gets a
thread now or is told there is no room, which is the same backpressure signal
with nothing to tune.
