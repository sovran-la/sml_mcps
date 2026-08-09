//! Tasks - durable, pollable execution of long-running requests.
//!
//! Introduced in 2025-11-25 and marked experimental there. A client augments a
//! `tools/call` with a `task` field; the server records the task, answers
//! immediately with a `CreateTaskResult`, runs the work on a background thread,
//! and the client polls `tasks/get` until the status is terminal, then fetches
//! the value with `tasks/result`.
//!
//! Concurrency here is `std::thread` and a `Condvar`, not a runtime. The work
//! genuinely runs in parallel with the server loop, so a polling client gets
//! real answers while a task is in flight. `tasks/result` blocks until the task
//! finishes, which the spec requires ("it **MUST** block the response until the
//! task reaches a terminal status").
//!
//! ## Asking the requestor something mid-task
//!
//! A worker that elicits moves its task to `input_required` for as long as it
//! waits, then back to `working`. Two things make that possible without
//! deadlocking:
//!
//! - The worker does not read for itself. It writes its request through a
//!   write handle independent of the one the server loop is blocked on, and
//!   waits on a channel for whichever thread is reading to hand the answer over.
//! - `tasks/result` pumps the transport while it blocks, instead of only
//!   sleeping on the store. Otherwise the one reader would be asleep waiting for
//!   a task that was waiting for an answer that could not be read.
//!
//! Both need a transport that can be split and can bound a read. Where that is
//! not true - the HTTP transport, notably - task workers are refused
//! server-initiated requests outright, and tasks move
//! `working -> completed | failed | cancelled`, a legal subset of the state
//! machine.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::types::{JsonRpcError, McpError, Result};

/// `_meta` key associating a message with the task it belongs to.
pub const RELATED_TASK: &str = "io.modelcontextprotocol/related-task";

/// `_meta` key on a `CreateTaskResult` carrying text to hand the model while
/// the task runs. Non-binding guidance in the spec.
pub const MODEL_IMMEDIATE_RESPONSE: &str = "io.modelcontextprotocol/model-immediate-response";

/// Where a task is in its lifecycle.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Being processed. Every task starts here.
    Working,
    /// The receiver needs input from the requestor.
    ///
    /// Entered while a task worker waits on an elicitation or sampling
    /// request, and left again as soon as it has an answer.
    InputRequired,
    /// Finished successfully; the result is available.
    Completed,
    /// Did not finish successfully. For a tool call this includes a result
    /// with `isError: true`.
    Failed,
    /// Cancelled before completion.
    Cancelled,
}

impl TaskStatus {
    /// Is this a terminal status?
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        )
    }

    /// May a task move from `self` to `next`?
    ///
    /// working -> input_required | terminal
    /// input_required -> working | terminal
    /// terminal -> nothing
    pub fn can_transition_to(&self, next: TaskStatus) -> bool {
        match self {
            TaskStatus::Working => next != TaskStatus::Working,
            TaskStatus::InputRequired => next != TaskStatus::InputRequired,
            _ => false,
        }
    }
}

/// The wire representation of a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub task_id: String,
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// ISO 8601 creation time.
    pub created_at: String,
    /// ISO 8601 time of the last status change.
    pub last_updated_at: String,
    /// Milliseconds from creation before the task may be deleted. `null` means
    /// unlimited, so the field is always present.
    pub ttl: Option<u64>,
    /// Suggested milliseconds between `tasks/get` calls.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_interval: Option<u64>,
}

/// Result of a task-augmented request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskResult {
    pub task: Task,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<crate::types::Meta>,
}

/// The `task` field a requestor adds to augment a request.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskParams {
    /// Requested lifetime in milliseconds. The receiver may override it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,
}

/// Params of `tasks/get`, `tasks/result`, and `tasks/cancel`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskIdParams {
    pub task_id: String,
}

/// Params of `tasks/list`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Result of `tasks/list`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksResult {
    pub tasks: Vec<Task>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Which task-augmented requests this server accepts.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TasksCapability {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests: Option<Value>,
}

/// Tuning for the task subsystem.
#[derive(Debug, Clone)]
pub struct TaskConfig {
    /// Lifetime applied when the requestor asks for none (default: 5 minutes).
    pub default_ttl_ms: u64,
    /// Ceiling on a requested lifetime (default: 1 hour).
    ///
    /// The spec lets receivers override a requested `ttl` and asks them to
    /// bound it, so unbounded client-chosen retention cannot exhaust memory.
    pub max_ttl_ms: u64,
    /// Polling interval suggested to requestors (default: 1s).
    pub poll_interval_ms: u64,
    /// Ceiling on simultaneously running tasks (default: 16).
    pub max_concurrent: usize,
}

impl Default for TaskConfig {
    fn default() -> Self {
        Self {
            default_ttl_ms: 300_000,
            max_ttl_ms: 3_600_000,
            poll_interval_ms: 1_000,
            max_concurrent: 16,
        }
    }
}

/// What a finished task produced.
#[derive(Debug, Clone)]
pub enum TaskOutcome {
    /// A successful JSON-RPC result, ready to return from `tasks/result`.
    Value(Value),
    /// A JSON-RPC error. `tasks/result` **MUST** return exactly what the
    /// underlying request would have returned, errors included.
    Error(JsonRpcError),
}

/// One task's full state.
#[derive(Debug)]
struct TaskRecord {
    task: Task,
    outcome: Option<TaskOutcome>,
    /// Set when creation happened, for TTL expiry.
    created: SystemTime,
    /// Cooperative cancellation flag, observed by the running tool.
    cancelled: Arc<AtomicBool>,
}

impl TaskRecord {
    fn expired(&self, now: SystemTime) -> bool {
        let Some(ttl) = self.task.ttl else {
            return false; // null ttl means unlimited
        };
        now.duration_since(self.created)
            .map(|elapsed| elapsed >= Duration::from_millis(ttl))
            .unwrap_or(false)
    }
}

/// Durable-enough store of tasks, with blocking result retrieval.
///
/// "Durable" here means in-process and surviving for the task's TTL. A store
/// backed by disk would survive a restart; nothing in `sml_mcps` outlives its
/// process, and a restarted stdio server has no client to serve the old task
/// to anyway.
#[derive(Debug)]
pub struct TaskStore {
    inner: Mutex<HashMap<String, TaskRecord>>,
    /// Signalled on every status change, so `tasks/result` can wait.
    changed: Condvar,
    config: TaskConfig,
}

impl TaskStore {
    pub fn new(config: TaskConfig) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            changed: Condvar::new(),
            config,
        }
    }

    /// The configured limits.
    pub fn config(&self) -> &TaskConfig {
        &self.config
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, HashMap<String, TaskRecord>>> {
        self.inner
            .lock()
            .map_err(|_| McpError::Internal("Task store lock poisoned".into()))
    }

    /// Drop tasks whose TTL has elapsed.
    ///
    /// Called at the start of every operation, which keeps cleanup free of a
    /// reaper thread while still bounding retention.
    fn sweep(tasks: &mut HashMap<String, TaskRecord>, now: SystemTime) {
        tasks.retain(|_, record| !record.expired(now));
    }

    /// How many tasks are currently running.
    pub fn running(&self) -> Result<usize> {
        let mut tasks = self.lock()?;
        Self::sweep(&mut tasks, SystemTime::now());
        Ok(tasks
            .values()
            .filter(|record| !record.task.status.is_terminal())
            .count())
    }

    /// Create a task in `working` status and return its wire form.
    ///
    /// The record exists before this returns, which is what lets the caller
    /// answer `CreateTaskResult` knowing a later `tasks/get` will find it.
    pub fn create(&self, requested_ttl: Option<u64>) -> Result<(Task, Arc<AtomicBool>)> {
        let now = SystemTime::now();
        let mut tasks = self.lock()?;
        Self::sweep(&mut tasks, now);

        let running = tasks
            .values()
            .filter(|record| !record.task.status.is_terminal())
            .count();
        if running >= self.config.max_concurrent {
            return Err(McpError::Internal(format!(
                "too many tasks in flight ({}/{})",
                running, self.config.max_concurrent
            )));
        }

        // Receivers MAY override the requested ttl; we clamp rather than
        // reject, so a greedy request degrades instead of failing.
        let ttl = requested_ttl
            .unwrap_or(self.config.default_ttl_ms)
            .min(self.config.max_ttl_ms);

        let timestamp = iso8601(now);
        let task = Task {
            task_id: new_task_id(),
            status: TaskStatus::Working,
            status_message: None,
            created_at: timestamp.clone(),
            last_updated_at: timestamp,
            ttl: Some(ttl),
            poll_interval: Some(self.config.poll_interval_ms),
        };

        let cancelled = Arc::new(AtomicBool::new(false));
        tasks.insert(
            task.task_id.clone(),
            TaskRecord {
                task: task.clone(),
                outcome: None,
                created: now,
                cancelled: cancelled.clone(),
            },
        );

        Ok((task, cancelled))
    }

    /// Fetch a task's current state.
    pub fn get(&self, task_id: &str) -> Result<Task> {
        let mut tasks = self.lock()?;
        Self::sweep(&mut tasks, SystemTime::now());
        tasks
            .get(task_id)
            .map(|record| record.task.clone())
            .ok_or_else(|| not_found(task_id))
    }

    /// Every task, newest first.
    pub fn list(&self) -> Result<Vec<Task>> {
        let mut tasks = self.lock()?;
        Self::sweep(&mut tasks, SystemTime::now());
        let mut all: Vec<Task> = tasks.values().map(|record| record.task.clone()).collect();
        // Stable order so cursor pagination stays coherent between pages.
        all.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.task_id.cmp(&b.task_id))
        });
        Ok(all)
    }

    /// Record a task's outcome and move it to a terminal status.
    ///
    /// Ignored if the task is already terminal - a cancelled task stays
    /// cancelled "even if execution continues to completion or fails."
    pub fn finish(&self, task_id: &str, status: TaskStatus, outcome: TaskOutcome) -> Result<()> {
        let mut tasks = self.lock()?;
        let Some(record) = tasks.get_mut(task_id) else {
            return Ok(()); // expired and swept while running
        };
        if record.task.status.is_terminal() {
            return Ok(());
        }

        record.task.status = status;
        record.task.last_updated_at = iso8601(SystemTime::now());
        if let TaskOutcome::Error(error) = &outcome {
            record.task.status_message = Some(error.message.clone());
        }
        record.outcome = Some(outcome);

        drop(tasks);
        self.changed.notify_all();
        Ok(())
    }

    /// Move a task between non-terminal states, honoring the state machine.
    ///
    /// This is how a task reaches `input_required`: a worker that needs an
    /// answer from the requestor says so before it blocks, and says `working`
    /// again once it has one. Use [`finish`](Self::finish) for terminal states,
    /// which carry an outcome.
    ///
    /// Returns whether the move happened. An illegal transition is reported
    /// rather than forced, and a task that has already finished or been
    /// cancelled stays that way - the worker asking for input has simply not
    /// noticed yet.
    pub fn set_status(&self, task_id: &str, status: TaskStatus) -> Result<bool> {
        if status.is_terminal() {
            return Err(McpError::Internal(format!(
                "use finish() to move task {task_id} to a terminal status"
            )));
        }

        let mut tasks = self.lock()?;
        let Some(record) = tasks.get_mut(task_id) else {
            return Ok(false); // expired and swept while running
        };
        if !record.task.status.can_transition_to(status) {
            return Ok(false);
        }

        record.task.status = status;
        record.task.last_updated_at = iso8601(SystemTime::now());
        drop(tasks);
        self.changed.notify_all();
        Ok(true)
    }

    /// Attach a human-readable note without changing status.
    pub fn set_status_message(&self, task_id: &str, message: impl Into<String>) -> Result<()> {
        let mut tasks = self.lock()?;
        if let Some(record) = tasks.get_mut(task_id) {
            record.task.status_message = Some(message.into());
            record.task.last_updated_at = iso8601(SystemTime::now());
        }
        drop(tasks);
        self.changed.notify_all();
        Ok(())
    }

    /// Cancel a task, cooperatively.
    ///
    /// Returns Invalid params for an unknown task or one already terminal,
    /// both of which the spec names explicitly.
    pub fn cancel(&self, task_id: &str) -> Result<Task> {
        let mut tasks = self.lock()?;
        Self::sweep(&mut tasks, SystemTime::now());

        let record = tasks.get_mut(task_id).ok_or_else(|| not_found(task_id))?;
        if record.task.status.is_terminal() {
            return Err(McpError::InvalidParams(format!(
                "Cannot cancel task: already in terminal status '{}'",
                status_name(record.task.status)
            )));
        }

        // Signal the worker, then move to cancelled *before* responding, as
        // the spec requires.
        record.cancelled.store(true, Ordering::SeqCst);
        record.task.status = TaskStatus::Cancelled;
        record.task.status_message = Some("The task was cancelled by request.".into());
        record.task.last_updated_at = iso8601(SystemTime::now());
        record.outcome = Some(TaskOutcome::Error(JsonRpcError::new(
            -32603,
            "Task was cancelled",
        )));

        let task = record.task.clone();
        drop(tasks);
        self.changed.notify_all();
        Ok(task)
    }

    /// The task's outcome if it has finished, or `None` while it runs.
    ///
    /// The non-blocking half of [`await_result`](Self::await_result), for a
    /// caller that has something else to do between checks - such as reading
    /// the transport, so a task waiting on `input_required` can be answered.
    pub fn try_result(&self, task_id: &str) -> Result<Option<TaskOutcome>> {
        let mut tasks = self.lock()?;
        Self::sweep(&mut tasks, SystemTime::now());

        let record = tasks.get(task_id).ok_or_else(|| not_found(task_id))?;
        if !record.task.status.is_terminal() {
            return Ok(None);
        }
        record
            .outcome
            .clone()
            .ok_or_else(|| {
                McpError::Internal(format!("task {task_id} is terminal but has no result"))
            })
            .map(Some)
    }

    /// Block until the task is terminal, then return its outcome.
    ///
    /// The spec is explicit that `tasks/result` on a non-terminal task
    /// "**MUST** block the response until the task reaches a terminal status."
    ///
    /// This blocks on the store alone, so nothing reads the transport while it
    /// waits - a task that asks for input during it can never be answered. It
    /// is used only where the transport cannot be pumped; see
    /// [`try_result`](Self::try_result).
    pub fn await_result(&self, task_id: &str) -> Result<TaskOutcome> {
        let mut tasks = self.lock()?;
        Self::sweep(&mut tasks, SystemTime::now());

        loop {
            let record = tasks.get(task_id).ok_or_else(|| not_found(task_id))?;
            if record.task.status.is_terminal() {
                return record.outcome.clone().ok_or_else(|| {
                    McpError::Internal(format!("task {task_id} is terminal but has no result"))
                });
            }

            tasks = self
                .changed
                .wait(tasks)
                .map_err(|_| McpError::Internal("Task store lock poisoned".into()))?;
        }
    }
}

/// Unknown or expired task: the spec's `-32602` with an informative message.
///
/// A purged expired task is deliberately indistinguishable from one that never
/// existed - "It is compliant behavior for a receiver to return an error
/// stating the task cannot be found if it has purged an expired task."
fn not_found(task_id: &str) -> McpError {
    McpError::InvalidParams(format!(
        "Failed to retrieve task: Task not found: {task_id}"
    ))
}

fn status_name(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Working => "working",
        TaskStatus::InputRequired => "input_required",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
    }
}

/// Build the `_meta` object associating a message with a task.
pub fn related_task_meta(task_id: &str) -> crate::types::Meta {
    let mut meta = crate::types::Meta::new();
    meta.insert(
        RELATED_TASK.to_string(),
        serde_json::json!({ "taskId": task_id }),
    );
    meta
}

//
// Task identifiers
//

/// Generate a task ID with 128 bits of entropy.
///
/// Task IDs are the only thing protecting a task's results when the transport
/// carries no authorization context - which is every stdio server. The spec is
/// blunt about it: receivers that cannot bind tasks to an auth context
/// "**MUST** generate cryptographically secure task IDs with enough entropy to
/// prevent guessing."
///
/// Entropy comes from the platform CSPRNG via `getrandom`, which is the same
/// source `rand` uses: `getrandom(2)` on Linux, `arc4random_buf` on the BSDs
/// and macOS, `ProcessPrng` on Windows. That makes every target equally strong,
/// rather than unix being solid and everything else best-effort.
fn new_task_id() -> String {
    let bytes = random_bytes();
    let mut id = String::with_capacity(32);
    for byte in bytes {
        id.push_str(&format!("{:02x}", byte));
    }
    id
}

/// 16 bytes from the OS.
///
/// `getrandom` fails only when the platform has no usable entropy source at
/// all, which for a running process is close to unheard of. Falling back beats
/// panicking in a tool call, and beats handing out a predictable id: the
/// fallback is weaker but not trivially guessable.
fn random_bytes() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    match getrandom::fill(&mut bytes) {
        Ok(()) => bytes,
        Err(e) => {
            eprintln!("sml_mcps: OS entropy unavailable ({e}); task ids are degraded");
            fallback_random_bytes()
        }
    }
}

/// Entropy from `RandomState`, which the standard library seeds from the
/// platform CSPRNG, mixed with the clock and a counter so that repeated calls
/// on one thread do not become predictable from a single observation.
fn fallback_random_bytes() -> [u8; 16] {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};
    use std::sync::atomic::AtomicU64;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut bytes = [0u8; 16];
    for (index, chunk) in bytes.chunks_mut(8).enumerate() {
        let mut hasher = RandomState::new().build_hasher();
        COUNTER.fetch_add(1, Ordering::Relaxed).hash(&mut hasher);
        index.hash(&mut hasher);
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .hash(&mut hasher);
        chunk.copy_from_slice(&hasher.finish().to_le_bytes());
    }
    bytes
}

//
// Timestamps
//

/// Format a `SystemTime` as an ISO 8601 / RFC 3339 UTC timestamp.
///
/// The spec requires `createdAt` and `lastUpdatedAt` on every task response.
/// Written out rather than pulling in a date library for two fields.
pub fn iso8601(time: SystemTime) -> String {
    let total = time
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let days = total.div_euclid(86_400);
    let seconds_of_day = total.rem_euclid(86_400);

    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year,
        month,
        day,
        seconds_of_day / 3600,
        (seconds_of_day % 3600) / 60,
        seconds_of_day % 60,
    )
}

/// Days since the Unix epoch to a civil (year, month, day).
///
/// Howard Hinnant's `civil_from_days`, which is exact for the whole proleptic
/// Gregorian range and needs no lookup tables.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]

    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn store() -> TaskStore {
        TaskStore::new(TaskConfig::default())
    }

    //
    // Status machine
    //

    #[test]
    fn terminal_statuses_are_the_three_the_spec_names() {
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Cancelled.is_terminal());
        assert!(!TaskStatus::Working.is_terminal());
        assert!(!TaskStatus::InputRequired.is_terminal());
    }

    #[test]
    fn transitions_follow_the_state_diagram() {
        use TaskStatus::*;

        for next in [InputRequired, Completed, Failed, Cancelled] {
            assert!(Working.can_transition_to(next), "working -> {next:?}");
        }
        assert!(!Working.can_transition_to(Working));

        for next in [Working, Completed, Failed, Cancelled] {
            assert!(InputRequired.can_transition_to(next), "input -> {next:?}");
        }

        // "Tasks with a completed, failed, or cancelled status are in a
        // terminal state and MUST NOT transition to any other status."
        for terminal in [Completed, Failed, Cancelled] {
            for next in [Working, InputRequired, Completed, Failed, Cancelled] {
                assert!(
                    !terminal.can_transition_to(next),
                    "{terminal:?} -> {next:?}"
                );
            }
        }
    }

    #[test]
    fn status_wire_names_are_snake_case() {
        assert_eq!(
            serde_json::to_value(TaskStatus::InputRequired).unwrap(),
            serde_json::json!("input_required")
        );
        assert_eq!(
            serde_json::to_value(TaskStatus::Working).unwrap(),
            serde_json::json!("working")
        );
    }

    //
    // Creation
    //

    #[test]
    fn create_starts_in_working_with_required_fields() {
        // "Tasks MUST begin in the working status when created."
        let store = store();
        let (task, cancelled) = store.create(None).unwrap();

        assert_eq!(task.status, TaskStatus::Working);
        assert!(!cancelled.load(Ordering::SeqCst));
        // createdAt / lastUpdatedAt are MUSTs on all task responses.
        assert!(task.created_at.ends_with('Z'));
        assert_eq!(task.created_at, task.last_updated_at);
        assert_eq!(task.ttl, Some(300_000));
        assert_eq!(task.poll_interval, Some(1_000));
    }

    #[test]
    fn create_is_immediately_visible_to_get() {
        // "The task must be durably created before sending the response."
        let store = store();
        let (task, _) = store.create(None).unwrap();
        assert_eq!(
            store.get(&task.task_id).unwrap().status,
            TaskStatus::Working
        );
    }

    #[test]
    fn requested_ttl_is_honored_and_clamped() {
        let store = TaskStore::new(TaskConfig {
            max_ttl_ms: 60_000,
            ..Default::default()
        });

        assert_eq!(store.create(Some(30_000)).unwrap().0.ttl, Some(30_000));
        // Receivers MAY override the requested ttl; clamping degrades rather
        // than rejecting.
        assert_eq!(store.create(Some(999_999_999)).unwrap().0.ttl, Some(60_000));
    }

    #[test]
    fn concurrency_is_bounded() {
        let store = TaskStore::new(TaskConfig {
            max_concurrent: 2,
            ..Default::default()
        });

        let (first, _) = store.create(None).unwrap();
        store.create(None).unwrap();
        assert!(store.create(None).is_err());

        // Finishing one frees a slot.
        store
            .finish(
                &first.task_id,
                TaskStatus::Completed,
                TaskOutcome::Value(serde_json::json!({})),
            )
            .unwrap();
        assert!(store.create(None).is_ok());
    }

    #[test]
    fn task_ids_are_unique_and_high_entropy() {
        let ids: HashSet<String> = (0..1000).map(|_| new_task_id()).collect();
        assert_eq!(ids.len(), 1000, "task ids must not repeat");

        for id in ids.iter().take(10) {
            assert_eq!(id.len(), 32, "128 bits as hex");
            assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn task_ids_are_not_sequential() {
        // A guessable id is the whole attack surface for an unauthenticated
        // transport, so consecutive ids must not share a prefix.
        let a = new_task_id();
        let b = new_task_id();
        let shared = a.chars().zip(b.chars()).take_while(|(x, y)| x == y).count();
        assert!(shared < 8, "ids {a} and {b} share {shared} leading chars");
    }

    #[test]
    fn fallback_entropy_is_also_unique() {
        let ids: HashSet<[u8; 16]> = (0..1000).map(|_| fallback_random_bytes()).collect();
        assert_eq!(ids.len(), 1000);
    }

    #[test]
    fn os_entropy_is_available_on_this_platform() {
        // The fallback exists for a platform with no entropy source; every
        // platform this crate is built for has one, and this fails loudly if a
        // new target does not.
        let mut bytes = [0u8; 16];
        assert!(
            getrandom::fill(&mut bytes).is_ok(),
            "getrandom must work on a supported target"
        );
    }

    #[test]
    fn every_bit_of_a_task_id_varies() {
        // A source stuck at a constant - or one only stirring the low bytes -
        // still produces unique-looking ids, so uniqueness alone proves very
        // little. Over enough samples each of the 128 bits must take both
        // values; the chance of a healthy source failing this is 2^-255.
        let mut ones = [0u32; 128];
        const SAMPLES: u32 = 256;

        for _ in 0..SAMPLES {
            let bytes = random_bytes();
            for (index, slot) in ones.iter_mut().enumerate() {
                if bytes[index / 8] & (1 << (index % 8)) != 0 {
                    *slot += 1;
                }
            }
        }

        for (index, count) in ones.iter().enumerate() {
            assert!(*count > 0, "bit {index} was never set");
            assert!(*count < SAMPLES, "bit {index} was always set");
        }
    }

    #[test]
    fn task_ids_do_not_repeat_across_threads() {
        // Ids are minted from whatever thread starts a task, so the generator
        // must not depend on per-thread state to stay unique.
        let mut handles = Vec::new();
        for _ in 0..8 {
            handles.push(std::thread::spawn(|| {
                (0..250).map(|_| new_task_id()).collect::<Vec<_>>()
            }));
        }

        let ids: HashSet<String> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        assert_eq!(ids.len(), 8 * 250);
    }

    //
    // Lifecycle
    //

    #[test]
    fn finish_records_the_outcome() {
        let store = store();
        let (task, _) = store.create(None).unwrap();

        store
            .finish(
                &task.task_id,
                TaskStatus::Completed,
                TaskOutcome::Value(serde_json::json!({ "done": true })),
            )
            .unwrap();

        assert_eq!(
            store.get(&task.task_id).unwrap().status,
            TaskStatus::Completed
        );
        let TaskOutcome::Value(value) = store.await_result(&task.task_id).unwrap() else {
            panic!("expected a value");
        };
        assert_eq!(value["done"], true);
    }

    #[test]
    fn finish_records_error_outcomes_and_a_status_message() {
        let store = store();
        let (task, _) = store.create(None).unwrap();

        store
            .finish(
                &task.task_id,
                TaskStatus::Failed,
                TaskOutcome::Error(JsonRpcError::internal_error("API rate limit exceeded")),
            )
            .unwrap();

        let fetched = store.get(&task.task_id).unwrap();
        assert_eq!(fetched.status, TaskStatus::Failed);
        // "The tasks/get response SHOULD include a statusMessage field with
        // diagnostic information about the failure."
        assert!(
            fetched
                .status_message
                .unwrap()
                .contains("API rate limit exceeded")
        );
    }

    #[test]
    fn finish_cannot_move_a_terminal_task() {
        let store = store();
        let (task, _) = store.create(None).unwrap();

        store.cancel(&task.task_id).unwrap();
        // "Once a task is cancelled, it MUST remain in cancelled status even
        // if execution continues to completion or fails."
        store
            .finish(
                &task.task_id,
                TaskStatus::Completed,
                TaskOutcome::Value(serde_json::json!({})),
            )
            .unwrap();

        assert_eq!(
            store.get(&task.task_id).unwrap().status,
            TaskStatus::Cancelled
        );
    }

    #[test]
    fn finish_on_a_swept_task_is_not_an_error() {
        // The worker may outlive its task's TTL; that must not panic or fail.
        let store = store();
        assert!(
            store
                .finish(
                    "gone",
                    TaskStatus::Completed,
                    TaskOutcome::Value(serde_json::json!({}))
                )
                .is_ok()
        );
    }

    #[test]
    fn last_updated_at_is_always_present() {
        let store = store();
        let (task, _) = store.create(None).unwrap();
        store.set_status_message(&task.task_id, "halfway").unwrap();

        let fetched = store.get(&task.task_id).unwrap();
        assert!(fetched.last_updated_at.ends_with('Z'));
        assert_eq!(fetched.status_message.as_deref(), Some("halfway"));
        // A status message alone must not move the status.
        assert_eq!(fetched.status, TaskStatus::Working);
    }

    //
    // Cancellation
    //

    #[test]
    fn cancel_moves_to_cancelled_and_signals_the_worker() {
        let store = store();
        let (task, cancelled) = store.create(None).unwrap();

        let result = store.cancel(&task.task_id).unwrap();
        // "receivers ... MUST transition the task to cancelled status before
        // sending the response."
        assert_eq!(result.status, TaskStatus::Cancelled);
        assert!(cancelled.load(Ordering::SeqCst));
    }

    #[test]
    fn cancelling_a_terminal_task_is_invalid_params() {
        let store = store();
        let (task, _) = store.create(None).unwrap();
        store
            .finish(
                &task.task_id,
                TaskStatus::Completed,
                TaskOutcome::Value(serde_json::json!({})),
            )
            .unwrap();

        // "Receivers MUST reject cancellation requests for tasks already in a
        // terminal status with error code -32602."
        let err = store.cancel(&task.task_id).unwrap_err();
        assert_eq!(err.to_jsonrpc_error().code, -32602);
        assert!(err.to_string().contains("terminal"), "{err}");
    }

    #[test]
    fn cancelling_an_unknown_task_is_invalid_params() {
        let err = store().cancel("nope").unwrap_err();
        assert_eq!(err.to_jsonrpc_error().code, -32602);
    }

    #[test]
    fn a_cancelled_task_yields_its_error_from_result() {
        let store = store();
        let (task, _) = store.create(None).unwrap();
        store.cancel(&task.task_id).unwrap();

        let TaskOutcome::Error(error) = store.await_result(&task.task_id).unwrap() else {
            panic!("expected an error outcome");
        };
        assert!(error.message.contains("cancelled"));
    }

    //
    // Lookup failures
    //

    #[test]
    fn unknown_tasks_are_invalid_params_everywhere() {
        let store = store();
        for err in [
            store.get("nope").unwrap_err(),
            store.await_result("nope").unwrap_err(),
            store.cancel("nope").unwrap_err(),
        ] {
            assert_eq!(err.to_jsonrpc_error().code, -32602);
            assert!(err.to_string().contains("not found"), "{err}");
        }
    }

    //
    // TTL expiry
    //

    #[test]
    fn expired_tasks_are_swept() {
        let store = TaskStore::new(TaskConfig {
            default_ttl_ms: 0, // expires immediately
            ..Default::default()
        });
        let (task, _) = store.create(None).unwrap();

        // A zero TTL means already elapsed, so the next operation purges it.
        let err = store.get(&task.task_id).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn a_live_ttl_keeps_the_task() {
        let store = store();
        let (task, _) = store.create(None).unwrap();
        assert!(store.get(&task.task_id).is_ok());
        assert_eq!(store.list().unwrap().len(), 1);
    }

    #[test]
    fn expiry_frees_concurrency_slots() {
        let store = TaskStore::new(TaskConfig {
            default_ttl_ms: 0,
            max_concurrent: 1,
            ..Default::default()
        });
        store.create(None).unwrap();
        // The first task expired, so a slot is available again.
        assert!(store.create(None).is_ok());
    }

    //
    // Listing
    //

    #[test]
    fn list_returns_every_live_task_in_stable_order() {
        let store = store();
        let ids: Vec<String> = (0..3)
            .map(|_| store.create(None).unwrap().0.task_id)
            .collect();

        let listed = store.list().unwrap();
        assert_eq!(listed.len(), 3);
        for id in &ids {
            assert!(listed.iter().any(|t| &t.task_id == id));
        }

        // Order must be deterministic so paginated pages stay coherent.
        assert_eq!(
            store
                .list()
                .unwrap()
                .iter()
                .map(|t| &t.task_id)
                .collect::<Vec<_>>(),
            listed.iter().map(|t| &t.task_id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn running_counts_only_non_terminal_tasks() {
        let store = store();
        let (a, _) = store.create(None).unwrap();
        store.create(None).unwrap();
        assert_eq!(store.running().unwrap(), 2);

        store
            .finish(
                &a.task_id,
                TaskStatus::Completed,
                TaskOutcome::Value(serde_json::json!({})),
            )
            .unwrap();
        assert_eq!(store.running().unwrap(), 1);
    }

    //
    // Blocking result retrieval
    //

    #[test]
    fn await_result_returns_immediately_when_already_terminal() {
        let store = store();
        let (task, _) = store.create(None).unwrap();
        store
            .finish(
                &task.task_id,
                TaskStatus::Completed,
                TaskOutcome::Value(serde_json::json!({ "n": 1 })),
            )
            .unwrap();

        let TaskOutcome::Value(value) = store.await_result(&task.task_id).unwrap() else {
            panic!("expected a value");
        };
        assert_eq!(value["n"], 1);
    }

    #[test]
    fn await_result_blocks_until_the_task_finishes() {
        // The MUST that makes tasks/result useful: it waits.
        let store = Arc::new(store());
        let (task, _) = store.create(None).unwrap();

        let finisher = {
            let store = store.clone();
            let task_id = task.task_id.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(100));
                store
                    .finish(
                        &task_id,
                        TaskStatus::Completed,
                        TaskOutcome::Value(serde_json::json!({ "late": true })),
                    )
                    .unwrap();
            })
        };

        let started = SystemTime::now();
        let outcome = store.await_result(&task.task_id).unwrap();
        let waited = started.elapsed().unwrap();

        let TaskOutcome::Value(value) = outcome else {
            panic!("expected a value");
        };
        assert_eq!(value["late"], true);
        assert!(waited >= Duration::from_millis(90), "returned too early");

        finisher.join().unwrap();
    }

    #[test]
    fn await_result_wakes_on_cancellation() {
        let store = Arc::new(store());
        let (task, _) = store.create(None).unwrap();

        let canceller = {
            let store = store.clone();
            let task_id = task.task_id.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                store.cancel(&task_id).unwrap();
            })
        };

        let outcome = store.await_result(&task.task_id).unwrap();
        assert!(matches!(outcome, TaskOutcome::Error(_)));
        canceller.join().unwrap();
    }

    //
    // Metadata helpers
    //

    #[test]
    fn related_task_meta_uses_the_reserved_key() {
        let meta = related_task_meta("abc123");
        assert_eq!(meta[RELATED_TASK]["taskId"], "abc123");
        assert_eq!(RELATED_TASK, "io.modelcontextprotocol/related-task");
    }

    #[test]
    fn create_task_result_serializes_to_the_spec_shape() {
        let store = store();
        let (task, _) = store.create(None).unwrap();
        let result = CreateTaskResult { task, meta: None };

        let wire: Value = serde_json::to_value(&result).unwrap();
        assert_eq!(wire["task"]["status"], "working");
        assert!(wire["task"]["taskId"].is_string());
        assert!(wire["task"]["createdAt"].is_string());
        assert!(wire["task"]["lastUpdatedAt"].is_string());
        assert_eq!(wire["task"]["ttl"], 300_000);
        assert_eq!(wire["task"]["pollInterval"], 1_000);
    }

    #[test]
    fn task_params_parse_from_a_request() {
        let params: TaskParams =
            serde_json::from_value(serde_json::json!({ "ttl": 60000 })).unwrap();
        assert_eq!(params.ttl, Some(60_000));

        let empty: TaskParams = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(empty.ttl, None);
    }

    //
    // Timestamps
    //

    #[test]
    fn iso8601_formats_the_epoch() {
        assert_eq!(iso8601(UNIX_EPOCH), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn iso8601_formats_known_instants() {
        let cases = [
            (1_000_000_000u64, "2001-09-09T01:46:40Z"),
            (1_700_000_000, "2023-11-14T22:13:20Z"),
            (1_764_028_800, "2025-11-25T00:00:00Z"),
            // Leap day, to exercise the civil-date math.
            (1_709_164_800, "2024-02-29T00:00:00Z"),
            // Century non-leap-year boundary.
            (951_782_400, "2000-02-29T00:00:00Z"),
        ];
        for (secs, expected) in cases {
            assert_eq!(
                iso8601(UNIX_EPOCH + Duration::from_secs(secs)),
                expected,
                "for {secs}"
            );
        }
    }

    #[test]
    fn iso8601_output_is_sortable_and_well_formed() {
        let earlier = iso8601(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        let later = iso8601(UNIX_EPOCH + Duration::from_secs(1_800_000_000));
        assert!(earlier < later, "timestamps must sort lexicographically");
        assert_eq!(earlier.len(), 20);
    }

    #[test]
    fn civil_from_days_handles_month_and_year_ends() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(365), (1971, 1, 1));
        // 1972 was a leap year.
        assert_eq!(civil_from_days(365 + 365 + 59), (1972, 2, 29));
    }
}
