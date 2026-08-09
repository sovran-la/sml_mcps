# MCP 2025-11-25 Migration Notes

Decision log for the jump from `2025-03-26` to `2025-11-25`, which means
adopting **two** revisions at once: `2025-06-18` and `2025-11-25`.

Read this if you maintain a downstream server, or if you want to know why
something was built the way it was rather than the obvious way.

Confidence is flagged per decision. Anything marked **REVIEW** is a judgement
call that deserves a second opinion.

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
  `log_data`, `log_enabled`, `is_cancelled`, `is_task`, `client_capabilities`.
  Its fields are private with no public constructor, so additions are free.

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

### 3.3 Output-schema validation is deliberately partial

**Confidence: medium. REVIEW if you plan to lean on it.**

Declaring `outputSchema` is a MUST-level promise: "Servers MUST provide
structured results that conform to this schema." Shipping a broken promise to
the client silently seemed worse than checking.

But a full JSON Schema 2020-12 implementation is a large dependency for a crate
whose entire pitch is being small. `src/schema_check.rs` therefore checks:

- `structuredContent` is present at all
- the top-level `type` matches
- every entry in `required` is present
- declared property types match, recursing one level

and explicitly declines to judge `pattern`, `minimum`, `format`,
`additionalProperties`, `$ref`, or `oneOf`/`anyOf`/`allOf`.

It errs **permissive**: a false rejection breaks a working tool, while a false
acceptance only leaves the client exactly where it would have been with no
check at all.

**Alternative considered:** depend on the `jsonschema` crate. Rejected — it
pulls in a substantial tree, and this crate's whole value proposition is not
doing that. **If a downstream server needs strict validation, validate before
returning.**

### 3.4 Server-initiated requests: one reader, no timeout

**Confidence: medium-high on the design, medium on the no-timeout call.
REVIEW the timeout question.**

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

**No timeout.** `Transport::read` blocks; interrupting it needs a second thread
per call. A client that never answers an elicitation stalls that tool — which
is no worse than a client that never answers any other request. If this turns
out to matter, the fix is a reader thread (above), not a timeout bolted onto
the current shape.

**Server-initiated request ids** are strings prefixed `sml-`. JSON-RPC shares
one id namespace across both directions; a numeric id could collide with a
client's. The prefix makes the two spaces disjoint by construction.

### 3.5 Tasks never reach `input_required`

**Confidence: high on the reasoning, medium on whether it matters.
REVIEW if a downstream server wants elicitation inside a long task.**

The spec describes `input_required` as a task pausing to elicit. We cannot do
that safely:

- the task runs on a worker thread
- nothing is reading the transport on that thread
- the *server loop* may itself be blocked inside `tasks/result` waiting on that
  very task

So a round trip from a worker could never complete. Rather than deadlock, task
workers get a `ToolEnv` with `can_request: false`, which refuses server-initiated
requests with a clear message.

Tasks therefore move `working -> completed | failed | cancelled`, a legal
subset of the state machine. `input_required` is a SHOULD ("when the task
receiver has messages for the requestor…"), and we never have such messages.

**To lift this** would take the dedicated-reader-thread design from §3.4.

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

### 3.7 Task IDs use OS entropy, with a fallback

**Confidence: medium-high. REVIEW the non-unix path.**

The spec is blunt:

> If context-binding is unavailable, receivers MUST generate cryptographically
> secure task IDs with enough entropy to prevent guessing.

That is every stdio server — the task ID is the only thing protecting a task's
results. So this is not decoration.

We take no `getrandom`/`rand` dependency, so: on unix, read 16 bytes from
`/dev/urandom`. Elsewhere, fall back to `RandomState`, whose keys the standard
library seeds from the platform CSPRNG, mixed with a counter and the clock.

**The unix path is solid. The fallback is best-effort** — `RandomState`
increments its key per call within a thread after the first, so the mixing is
doing real work there. Every deployment target for this crate today is unix. If
Windows becomes a target, add `getrandom`.

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
| Task `input_required` | See §3.5. |
| Per-auth-context task binding | Meaningful only for a hosted HTTP deployment. See §3.7. |
| Acting on `notifications/cancelled` | Accepted and ignored. The server is single-threaded per request, so there is nothing to interrupt; task cancellation goes through `tasks/cancel`, which is implemented. |
| SSE resumability (`Last-Event-ID`) | Our HTTP transport buffers a whole response and returns it as one body. There is no long-lived stream to resume. |
| DCR / CIMD / OIDC discovery | Client-and-authorization-server concerns. The server-side obligation is Protected Resource Metadata, which *is* implemented. |
| Anything from 2026-07-28 | Out of scope by instruction. |

---

## 5. Known test flake (pre-existing)

`bridge::tests::test_auto_start_connects_to_running_daemon` fails
intermittently, roughly 1 run in 4, and only on the first run after a fresh
build. It races a real daemon spawn against a 5-second deadline.

**This predates these changes** — verified by running it on the unmodified base
commit (`4378095`) in a clean worktree, where it fails the same way. The larger
test suite here makes it slightly more likely by loading the machine. Worth
fixing separately; not touched.

---

## 6. Verification

- 396 tests, all passing, `--all-features`
- `cargo clippy --all-features --all-targets`: clean
- `cargo fmt --check`: clean

Conformance checks that exist specifically as regression guards:

- prompt-not-found can never collide with resource-not-found again
- param structs never gain `deny_unknown_fields` (forward compatibility)
- JSON-RPC batch arrays are rejected with `-32600`, not an opaque parse error
- task IDs are unique, 128-bit, and non-sequential
- `tasks/result` genuinely blocks until the task finishes
- a task worker cannot make a server-initiated request (deadlock guard)
- an audience-less token is rejected
