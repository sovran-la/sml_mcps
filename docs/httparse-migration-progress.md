# httparse migration — progress report

**Branch:** `feat/2025-11-25-compliance`
**Commit:** `aad8d71 feat(http): hand the request grammar to httparse`
**Status:** the swap itself is **done, green, and committed**. What is left is
tests and docs, not code. Details below.

---

## State at handoff

| | |
|---|---|
| `cargo test --all-features` | **621 unit + 5 integration + 2 doctests, all pass** (identical to the pre-swap baseline) |
| `cargo clippy --all-features --all-targets` | **clean**, no warnings |
| `cargo tree --all-features` | **113 → 114 crates.** The diff is exactly one line: `> httparse v1.10.1`. Zero transitive deps (confirmed: no `dependencies` key in its `Cargo.lock` entry, and it is a leaf in the tree). |
| Ran the suite twice? | **No — only once after the final edit.** See "What's left" #1. |

Nothing is half-edited. The tree builds and passes as committed.

---

## What was done

### 1. `Cargo.toml`
`httparse = { version = "1.10", optional = true }`, and the `http` feature
became `http = ["dep:httparse"]`. The comment claiming the feature "costs no
dependencies at all" was no longer true and was rewritten rather than left to
rot — it now says one crate, no transitive deps.

### 2. `read_head` — the real change
Replaced the line-by-line reader + `parse_request_line` + header-splitting loop
with an incremental `httparse` loop:

- Accumulate into a `Vec<u8>` bounded by `limits.max_head_bytes` (32 KiB), which
  grows **only as bytes actually arrive** — the module's "nothing is sized from
  a peer's declared length" property is preserved.
- Copy out of the `BufReader` with `fill_buf()`, and `consume()` only the bytes
  `httparse` says belong to the head. **Everything past the head stays in the
  reader's buffer**, which is why the body path needed no changes at all.
- On `Complete(n)`: `io.consume(n - consumed)`. On `Partial`: consume the round
  and loop. On `Err`: map to a status via the new `rejection()`.

**Why incremental re-parse rather than "scan for `\r\n\r\n`, then parse once":**
a hand-written terminator scan has to agree with `httparse` about what ends a
head (it accepts bare `\n` too). If it ever disagrees, that *is* a
desynchronised connection. `httparse` is the only thing that decides where the
head ends. This is the single most important design decision in the change —
do not "optimise" it into a pre-scan.

**The one optimisation that is safe** and is in the code: only call `parse()` on
a round that delivered a `\n`. A head always ends with a newline, so a round
without one cannot have completed a head. This cannot disagree with `httparse`
about completeness — it only declines to ask, and it keeps a 1-byte-per-packet
slowloris from forcing 32 K full re-parses.

### 3. `head_from()` (new)
Turns `httparse::Request` into our owned `Head`. Header values arrive as
`&[u8]`; non-UTF-8 (obs-text, legal per RFC 9110 §5.5) is **refused with 400**,
which matches the pre-swap behaviour exactly — everything downstream compares,
logs or echoes these as text.

### 4. `rejection()` (new) — `httparse::Error` → status
Mapping was chosen from **measured** behaviour, not the docs:

| httparse error | Status | Reached by |
|---|---|---|
| `Version` | **505** | `HTTP/2.0`, `HTTP/1.9`, garbage 3rd token |
| `TooManyHeaders` | **431** | more fields than `max_headers` |
| `HeaderName` | **400** | bad name token, **obs-fold**, **space before colon** |
| `HeaderValue` | **400** | NUL / DEL / CTL in a value |
| `Token` | **400** | **double space in request line**, bad method, CTL in target |
| `NewLine` | **400** | bare CR mid-head, trailing space after version |

### 5. `parse_chunk_size` — delegated, with a load-bearing guard
Now calls `httparse::parse_chunk_size`. **The guard on top is not optional.**
Measured: httparse answers `Complete((_, 0))` — i.e. *"this is the last
chunk"* — to all three of `"\r\n"`, `" \r\n"` and `";x\r\n"`. Reading a blank
line as end-of-body while a stricter front-end rejects it is a smuggling
primitive. So: **a chunk size must start with an ASCII hex digit.**

### 6. Deleted
`parse_request_line` (~24 lines), the header loop in `read_head` (~35 lines),
the hand-rolled hex in `parse_chunk_size` (~10 lines), and `is_timeout` (only
the old `read_head` used it). Net: less hand-rolled parsing, which was the goal.

### 7. Kept, deliberately — `httparse` reports headers, it does not adjudicate them
`framing_of()` **in full**: `Content-Length` + `Transfer-Encoding` → 400,
conflicting `Content-Length` → 400, duplicate `Transfer-Encoding` → 400, unknown
coding → 501. Both ceilings. The entire body path (deadlines, chunked decoder,
`read_exactly`, `drain_if_cheap`, trailers). Connection management, threading,
sessions, origin validation, JWT, TLS, response rendering, 503-on-saturation —
**all untouched**. No tokio.

---

## Three things this tightened (each verified against httparse first)

1. **Space before a header's colon** (`X-T : v`) is now **400**. It was accepted
   before. Classic way to slip a field past a proxy that reads it differently.
2. **`Content-Length: +5`** is now **400**. `usize::from_str` accepts a leading
   `+`; RFC 9112 §6.2 is `1*DIGIT`. We read 5 where a strict front-end rejects
   the field outright — two answers to where the request ends.
3. **Chunk-size guard**, above.

**One deliberate behaviour change:** `POST /a b HTTP/1.1` (space in target) now
answers **505** instead of 400, because httparse reports it as `Error::Version`
and httparse does not expose the offending token. Both close the connection; no
test covered it. Judged not worth reintroducing hand-rolled request-line
scanning to recover a status code.

**Deliberately unchanged:** bare-LF line endings are still accepted. Both the
old hand-rolled reader and httparse accept them, and RFC 9112 §2.2 permits it
("a recipient MAY recognize a single LF"). No regression — but if you want to
tighten it, that is a separate, arguable change.

---

## What's left

1. **Run the suite a second time to check for flakes.** The task asked for at
   least two runs; only one full run happened after the final edit. Just
   `cargo test --all-features` twice.
2. **Add the new parser-boundary tests.** Existing coverage was preserved and
   all 621 still pass, but these edges are now *load-bearing* and have no test
   of their own. In priority order, all in `src/transport/http1.rs` `mod tests`
   (use the existing `drive()` / `echo_body()` helpers — they need no changes):
   - `parse_chunk_size` guard: `""`, `" "`, `";x"` must be `None`. **Highest
     value — this one is a smuggling guard against a measured httparse quirk.**
   - Space before a header colon → 400.
   - `Content-Length: +5` → 400.
   - Header value with an invalid UTF-8 byte (`\xff`) → 400.
   - **A head split across reads.** The `Fake` socket in `mod tests` is a single
     `Cursor`, so it always delivers the whole head in one `fill_buf`. A socket
     that hands back N bytes per `read()` would exercise the incremental loop —
     the `consumed` bookkeeping and the "body bytes stay in the reader" property
     are the parts most worth a test, and nothing currently covers them.
     Suggested: a `Dribble` socket wrapping a `Cursor` that returns
     `min(n, chunk)` per read, driven at chunk sizes 1, 3 and 7.
   - `HeaderValue` CTL rejection (NUL in a value) → 400.
3. **`docs/2025-11-25-migration-notes.md` is NOT yet updated.** §3.13 ("The
   HTTP/1.1 layer is ours") still says the layer has no dependencies and
   describes owning the parser. It needs a subsection saying the *parser*
   specifically is now httparse and why: battle-tested vs. hand-rolled, request
   smuggling is a grammar problem, one crate with zero transitive deps, and
   that the three safety properties (nothing sized from a declared length, every
   read has a deadline, concurrency capped) are unchanged and are still the
   reason the *server* is ours. Also update §1.11 which states the `http`
   feature has no dependencies. The prose style there is specific and
   argumentative — match it; read §3.13 before writing.
4. **`README.md` may repeat the "no dependencies" claim** for the `http`
   feature — grep for it. Not checked.

---

## What the next agent needs to know

- **Cargo.lock is gitignored in this repo.** `git add Cargo.lock` fails. The
  commit contains `Cargo.toml` + `src/transport/http1.rs` only. This is normal
  here, not a mistake.
- `/Users/bsneed/.cargo/bin/cargo` — full path required, tilde does not expand.
- **A scratch probe crate is at `/tmp/hp`.** It is how every claim above about
  httparse behaviour was measured rather than assumed. If you need to check
  another edge, edit `/tmp/hp/src/main.rs` and `cargo run -q` — much faster than
  reasoning about the docs, which were wrong in at least one place (they imply
  obs-fold config applies to requests; it does not, and requests reject it
  outright).
- The most important verified property, worth re-checking if you change the
  loop: **every prefix of a valid request parses to `Partial`, never `Err`.** If
  that were false, requests split across TCP segments would be rejected at
  random. It was checked over every prefix of six different requests.
- `max_headers` / `max_head_bytes` are **not** publicly settable — `http.rs`
  only overrides `max_body_bytes` and takes `..Limits::default()` for the rest.
  So the `vec![EMPTY_HEADER; max_headers]` scratch is a fixed 3.2 KiB
  (100 × 32 B) per parse attempt, entirely under our control, never a peer's.
- That scratch `Vec` **must** be allocated inside the loop. Hoisting it out
  does not borrow-check: `Header<'buf>` ties the header slice's lifetime to the
  parse buffer, which is mutated between rounds. Do not try to defeat this with
  unsafe; the allocation is 3.2 KiB and the newline gate keeps the attempt count
  down to roughly the number of lines.
