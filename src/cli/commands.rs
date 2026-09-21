//! The two subcommands the crate implements outright: `install` and
//! `uninstall`.
//!
//! Both are a loop over a list of clients plus a report, and both are written to
//! be run more than once - after a rebuild, after moving the binary, or just to
//! see what the machine looks like today.
//!
//! Doing the work and saying what happened are kept apart. Every function here
//! takes the clients it should act on and returns the text it would print, so a
//! report is asserted against a temporary directory rather than against whoever
//! is running the suite.

use super::install::{InstallStatus, McpClient, ServerEntry};
use std::path::Path;

/// What a command produced: what to print, and how it went.
pub(super) struct Report {
    /// Everything the command has to say, printed as-is.
    pub text: String,

    /// How it went, which is also what the process exits with.
    pub outcome: Outcome,
}

/// How a command went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outcome {
    /// Did what was asked. A client that was already configured counts.
    Done,

    /// Ran, and something went wrong on the way - a config that could not be
    /// written, a machine with nowhere to look.
    Failed,

    /// The command line itself was wrong, and nothing was attempted. Kept apart
    /// from [`Failed`](Self::Failed) so a mistyped `--client cursur` exits the
    /// same way a mistyped `--clients` does.
    Usage,
}

impl Report {
    /// A report of one message and nothing else.
    pub(super) fn of(outcome: Outcome, text: impl Into<String>) -> Self {
        let mut text: String = text.into();
        if !text.ends_with('\n') {
            text.push('\n');
        }

        Self { text, outcome }
    }
}

//
// Selecting clients
//

/// Keep only the clients `selectors` name, or all of them if it names none.
///
/// A selector matching nothing is an error rather than an empty run: `install
/// --client cursur` that quietly configured nothing would look exactly like a
/// success. See [`selector_error`] for the one case that is not an error.
pub(super) fn select(
    clients: Vec<Box<dyn McpClient>>,
    selectors: &[String],
) -> Result<Vec<Box<dyn McpClient>>, String> {
    if selectors.is_empty() {
        return Ok(clients);
    }

    if let Some(message) = selector_error(&clients, selectors) {
        return Err(message);
    }

    Ok(clients
        .into_iter()
        .filter(|client| {
            selectors
                .iter()
                .any(|selector| client.matches_selector(selector))
        })
        .collect())
}

/// What is wrong with `selectors`, before anything has been attempted.
///
/// The check [`select`] makes, hoisted so it can be made early: `install` runs
/// an author's hook before it writes anything, and a command line that is about
/// to be refused must not first send that hook off to prompt for an API key.
///
/// An empty `clients` is not a selector problem - it is a machine with no home
/// directory, which [`install`] and [`uninstall`] each say in their own words
/// before they get this far.
pub(super) fn selector_error(
    clients: &[Box<dyn McpClient>],
    selectors: &[String],
) -> Option<String> {
    if clients.is_empty() {
        return None;
    }

    selectors
        .iter()
        .find(|selector| {
            !clients
                .iter()
                .any(|client| client.matches_selector(selector))
        })
        .map(|selector| {
            format!(
                "unknown client `{selector}`. Known clients: {}",
                known(clients)
            )
        })
}

/// The names `--client` accepts, for the message that says one was wrong.
fn known(clients: &[Box<dyn McpClient>]) -> String {
    clients
        .iter()
        .map(|client| client.slug())
        .collect::<Vec<_>>()
        .join(", ")
}

//
// install
//

/// Register `entry` with every client in `clients` that is on this machine.
pub(super) fn install(
    entry: &ServerEntry,
    clients: Vec<Box<dyn McpClient>>,
    selectors: &[String],
) -> Report {
    let mut out = String::new();
    out.push_str(&format!(
        "Installing {} as `{}`.\n\n",
        entry.command(),
        entry.name
    ));

    if clients.is_empty() {
        out.push_str("MCP clients: could not look for them - no home directory.\n");
        out.push_str(&fallback(entry));
        return Report {
            text: out,
            outcome: Outcome::Failed,
        };
    }

    let clients = match select(clients, selectors) {
        Ok(clients) => clients,
        Err(message) => return Report::of(Outcome::Usage, message),
    };

    out.push_str("MCP clients:\n");
    let mut detected = 0;
    let mut changed = 0;
    let mut failed = false;

    for client in &clients {
        let status = client.check_existing(entry);
        if status == InstallStatus::ClientNotFound {
            out.push_str(&format!("  {}: not installed\n", client.name()));
            continue;
        }
        detected += 1;

        if !status.wants_install() {
            out.push_str(&format!(
                "  {}: up to date ({})\n",
                client.name(),
                client.config_path().display()
            ));
            continue;
        }

        match client.install(entry) {
            Ok(()) => {
                out.push_str(&changed_line(client.name(), &status, &client.config_path()));
                changed += 1;
            }
            Err(error) => {
                out.push_str(&format!(
                    "  {}: could not be configured - {error}\n",
                    client.name()
                ));
                failed = true;
            }
        }
    }

    out.push('\n');
    if detected == 0 {
        // A targeted install that found nothing is a different sentence from a
        // machine with no MCP clients on it at all.
        out.push_str(if selectors.is_empty() {
            "No supported MCP clients detected.\n"
        } else {
            "None of the named MCP clients are installed.\n"
        });
        out.push_str(&fallback(entry));
    } else if changed > 0 {
        out.push_str(&format!(
            "Detected {detected} client(s), changed {changed}.\n"
        ));
        out.push_str("Restart your MCP client to pick up the change.\n");
    } else if failed {
        // Nothing was written, but "all up to date" would be a claim about
        // clients this run never managed to look at properly.
        out.push_str(&format!("Detected {detected} client(s), changed 0.\n"));
    } else {
        out.push_str(&format!("Detected {detected} client(s), all up to date.\n"));
    }

    Report {
        text: out,
        outcome: if failed {
            Outcome::Failed
        } else {
            Outcome::Done
        },
    }
}

/// One client's line, once its entry has been written.
///
/// The three ways an install can change a config are worth telling apart, and
/// `status` is the only thing that still knows which one happened: the config
/// has already been overwritten by the time this is called. A client that had
/// no entry was *configured*; one that had a current entry with something stale
/// in it was *updated*; one that named another binary was updated too, and
/// which binary it used to name is the part worth seeing.
fn changed_line(name: &str, status: &InstallStatus, config_path: &Path) -> String {
    let path = config_path.display();

    match status {
        InstallStatus::NeedsUpdate { current_command } => {
            format!("  {name}: updated — was pointing at {current_command} ({path})\n")
        }
        InstallStatus::NeedsRefresh => format!("  {name}: updated ({path})\n"),
        _ => format!("  {name}: configured ({path})\n"),
    }
}

/// What to print when nothing could be configured automatically.
fn fallback(entry: &ServerEntry) -> String {
    format!(
        "\nAdd this to your client's MCP configuration by hand:\n\n{}\n",
        entry.snippet()
    )
}

//
// uninstall
//

/// Remove `entry` from every client in `clients` that has it.
pub(super) fn uninstall(
    entry: &ServerEntry,
    clients: Vec<Box<dyn McpClient>>,
    selectors: &[String],
) -> Report {
    if clients.is_empty() {
        return Report::of(
            Outcome::Failed,
            "MCP clients: could not look for them - no home directory.",
        );
    }

    let clients = match select(clients, selectors) {
        Ok(clients) => clients,
        Err(message) => return Report::of(Outcome::Usage, message),
    };

    let mut out = format!("Removing `{}` from MCP clients:\n", entry.name);
    let mut removed = 0;
    let mut failed = false;

    for client in &clients {
        let status = client.check_existing(entry);
        if status == InstallStatus::ClientNotFound {
            continue;
        }

        // Asked even where the status says there is nothing to remove, because
        // `NotInstalled` is also the answer for a config that could not be
        // parsed - and a file we cannot read may well still name this server.
        // `uninstall` re-reads it and reports the parse failure the way
        // `install` does, rather than this command claiming it looked and found
        // nothing.
        let removal = client.uninstall(entry);

        match (removal, status.has_entry()) {
            (Ok(()), true) => {
                out.push_str(&format!(
                    "  {}: removed ({})\n",
                    client.name(),
                    client.config_path().display()
                ));
                removed += 1;
            }
            // A client that had no entry says nothing, the same as before: a
            // list of every client that was already clean is noise.
            (Ok(()), false) => {}
            (Err(error), _) => {
                out.push_str(&format!(
                    "  {}: could not be changed - {error}\n",
                    client.name()
                ));
                failed = true;
            }
        }
    }

    out.push('\n');
    if removed == 0 && !failed {
        out.push_str(&format!(
            "Nothing to remove - no client had an entry for `{}`.\n",
            entry.name
        ));
    } else {
        out.push_str(&format!("Removed from {removed} client(s).\n"));
    }

    Report {
        text: out,
        outcome: if failed {
            Outcome::Failed
        } else {
            Outcome::Done
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::install::{InstallError, JsonClient, clients_under};
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// The entry the tests here install.
    fn entry() -> ServerEntry {
        ServerEntry::new("my-mcp", &["serve"])
    }

    /// Every client, rooted in `dir`, made to look installed.
    ///
    /// Config directories for all of them, plus `~/.claude/` - Claude Code is
    /// detected by that marker rather than by its config file's parent, which
    /// is the home directory itself.
    fn detected_clients(dir: &TempDir) -> Vec<Box<dyn McpClient>> {
        std::fs::create_dir_all(dir.path().join(".claude")).unwrap();

        let clients = clients_under(dir.path());
        for client in &clients {
            std::fs::create_dir_all(client.config_path().parent().unwrap()).unwrap();
        }
        clients
    }

    /// A client that fails whatever it is asked to do.
    struct BrokenClient;

    impl McpClient for BrokenClient {
        fn name(&self) -> &str {
            "Broken"
        }
        fn config_path(&self) -> PathBuf {
            PathBuf::from("/dev/null/config.json")
        }
        fn detect(&self) -> bool {
            true
        }
        fn check_existing(&self, _entry: &ServerEntry) -> InstallStatus {
            InstallStatus::NotInstalled
        }
        fn install(&self, _entry: &ServerEntry) -> Result<(), InstallError> {
            Err(InstallError::Parse("/dev/null/config.json: no".to_string()))
        }
        fn uninstall(&self, _entry: &ServerEntry) -> Result<(), InstallError> {
            Err(InstallError::Parse("/dev/null/config.json: no".to_string()))
        }
    }

    /// A client that always has an entry to remove, and always fails to.
    struct BrokenRemoval;

    impl McpClient for BrokenRemoval {
        fn name(&self) -> &str {
            "Broken"
        }
        fn config_path(&self) -> PathBuf {
            PathBuf::from("/dev/null/config.json")
        }
        fn detect(&self) -> bool {
            true
        }
        fn check_existing(&self, _entry: &ServerEntry) -> InstallStatus {
            InstallStatus::Installed
        }
        fn install(&self, _entry: &ServerEntry) -> Result<(), InstallError> {
            Ok(())
        }
        fn uninstall(&self, _entry: &ServerEntry) -> Result<(), InstallError> {
            Err(InstallError::Parse("/dev/null/config.json: no".to_string()))
        }
    }

    //
    // Selecting
    //

    #[test]
    fn no_selector_keeps_every_client() {
        let clients = clients_under("/home/test");

        assert_eq!(select(clients, &[]).unwrap().len(), 6);
    }

    #[test]
    fn a_selector_keeps_only_what_it_names() {
        let clients = clients_under("/home/test");

        let kept = select(clients, &["claude-code".to_string()]).unwrap();

        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].name(), "Claude Code");
    }

    #[test]
    fn selectors_can_be_repeated() {
        let clients = clients_under("/home/test");

        let kept = select(clients, &["cursor".to_string(), "codex".to_string()]).unwrap();

        let names: Vec<&str> = kept.iter().map(|client| client.name()).collect();
        assert_eq!(names, ["Cursor", "Codex"]);
    }

    #[test]
    fn a_selector_is_forgiving_about_spelling() {
        for spelling in ["vscode", "vs-code", "VS Code"] {
            let kept = select(clients_under("/home/test"), &[spelling.to_string()]).unwrap();

            assert_eq!(kept.len(), 1, "{spelling}");
            assert_eq!(kept[0].name(), "VS Code");
        }
    }

    #[test]
    fn a_selector_naming_nothing_is_an_error_that_lists_the_alternatives() {
        let Err(error) = select(clients_under("/home/test"), &["cursur".to_string()]) else {
            panic!("a typo must not be a silent no-op");
        };

        assert!(error.contains("cursur"), "{error}");
        assert!(error.contains("claude-desktop"), "{error}");
        assert!(error.contains("vs-code"), "{error}");
    }

    #[test]
    fn a_bad_selector_is_visible_before_anything_is_attempted() {
        let clients = clients_under("/home/test");

        let error = selector_error(&clients, &["cursur".to_string()]).unwrap();

        assert!(error.contains("cursur"), "{error}");
        assert!(error.contains("cursor"), "{error}");
        assert!(selector_error(&clients, &["cursor".to_string()]).is_none());
        assert!(selector_error(&clients, &[]).is_none());
    }

    #[test]
    fn a_selector_against_no_clients_at_all_is_not_a_selector_problem() {
        // A machine with no home directory has no client list to be wrong
        // about, and both commands have their own words for that. Answering
        // "unknown client `cursor`" here would lose the pasteable snippet.
        assert!(selector_error(&[], &["cursor".to_string()]).is_none());
    }

    #[test]
    fn one_bad_selector_among_good_ones_is_still_an_error() {
        let Err(error) = select(
            clients_under("/home/test"),
            &["cursor".to_string(), "nope".to_string()],
        ) else {
            panic!("a typo alongside a real client is still a typo");
        };

        assert!(error.contains("nope"), "{error}");
    }

    //
    // install
    //

    #[test]
    fn install_configures_every_detected_client() {
        let dir = TempDir::new().unwrap();

        let report = install(&entry(), detected_clients(&dir), &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Detected 6 client(s), changed 6."));
        for client in clients_under(dir.path()) {
            assert_eq!(
                client.check_existing(&entry()),
                InstallStatus::Installed,
                "{}",
                client.name()
            );
        }
    }

    #[test]
    fn install_writes_a_remote_entry_to_every_client_and_uninstall_removes_it() {
        // Every registry format, through the command: the flag lands in each
        // config's `args`, each client reports it installed, and the plain
        // entry - no address - is enough to take it out again.
        let dir = TempDir::new().unwrap();
        let remote = entry().with_connect("jetson-memory:7211");

        let report = install(&remote, detected_clients(&dir), &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Detected 6 client(s), changed 6."));
        for client in clients_under(dir.path()) {
            assert_eq!(
                client.check_existing(&remote),
                InstallStatus::Installed,
                "{}",
                client.name()
            );
            let text = std::fs::read_to_string(client.config_path()).unwrap();
            assert!(
                text.contains("--connect") && text.contains("jetson-memory:7211"),
                "{}: {text}",
                client.name()
            );
        }

        let report = uninstall(&entry(), detected_clients(&dir), &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Removed from 6 client(s)."));
        for client in clients_under(dir.path()) {
            assert_eq!(
                client.check_existing(&remote),
                InstallStatus::NotInstalled,
                "{}",
                client.name()
            );
            let text = std::fs::read_to_string(client.config_path()).unwrap();
            assert!(
                !text.contains("jetson-memory:7211"),
                "{}: {text}",
                client.name()
            );
        }
    }

    #[test]
    fn install_over_a_local_entry_with_an_address_updates_every_client() {
        // A server already installed locally, then pointed at another
        // machine: `install --connect` is an update, not a no-op.
        let dir = TempDir::new().unwrap();
        install(&entry(), detected_clients(&dir), &[]);

        let report = install(
            &entry().with_connect("jetson-memory:7211"),
            detected_clients(&dir),
            &[],
        );

        assert_eq!(report.outcome, Outcome::Done);
        assert!(
            report.text.contains("Detected 6 client(s), changed 6."),
            "{}",
            report.text
        );
        assert!(report.text.contains(": updated ("), "{}", report.text);
    }

    #[test]
    fn install_names_the_binary_and_the_entry() {
        let dir = TempDir::new().unwrap();

        let report = install(&entry(), detected_clients(&dir), &[]);

        assert!(report.text.contains(&entry().command()), "{}", report.text);
        assert!(report.text.contains("as `my-mcp`"), "{}", report.text);
    }

    #[test]
    fn install_says_where_each_config_went() {
        let dir = TempDir::new().unwrap();

        let report = install(&entry(), detected_clients(&dir), &[]);

        for client in clients_under(dir.path()) {
            let line = format!(
                "  {}: configured ({})",
                client.name(),
                client.config_path().display()
            );
            assert!(report.text.contains(&line), "{}\n---\n{line}", report.text);
        }
    }

    #[test]
    fn install_honours_a_client_selector() {
        let dir = TempDir::new().unwrap();

        let report = install(&entry(), detected_clients(&dir), &["cursor".to_string()]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Detected 1 client(s), changed 1."));
        for client in clients_under(dir.path()) {
            let expected = if client.name() == "Cursor" {
                InstallStatus::Installed
            } else {
                InstallStatus::NotInstalled
            };
            assert_eq!(
                client.check_existing(&entry()),
                expected,
                "{}",
                client.name()
            );
        }
    }

    #[test]
    fn install_reports_an_unknown_selector_and_writes_nothing() {
        let dir = TempDir::new().unwrap();

        let report = install(&entry(), detected_clients(&dir), &["nope".to_string()]);

        assert_eq!(report.outcome, Outcome::Usage);
        assert!(report.text.contains("unknown client `nope`"));
        for client in clients_under(dir.path()) {
            assert!(!client.config_path().exists(), "{}", client.name());
        }
    }

    #[test]
    fn a_report_is_always_a_whole_line() {
        // It is printed with `print!`, so a message with no newline runs into
        // the shell prompt.
        let dir = TempDir::new().unwrap();

        for report in [
            install(&entry(), detected_clients(&dir), &["nope".to_string()]),
            install(&entry(), detected_clients(&dir), &[]),
            install(&entry(), vec![], &[]),
            uninstall(&entry(), detected_clients(&dir), &["nope".to_string()]),
            uninstall(&entry(), detected_clients(&dir), &[]),
            uninstall(&entry(), vec![], &[]),
        ] {
            assert!(report.text.ends_with('\n'), "{:?}", report.text);
        }
    }

    #[test]
    fn a_targeted_install_that_finds_nothing_says_which_kind_of_nothing() {
        let dir = TempDir::new().unwrap();
        let absent = clients_under(dir.path().join("no-such-home"));

        let targeted = install(&entry(), absent, &["cursor".to_string()]);

        assert_eq!(targeted.outcome, Outcome::Done);
        assert!(
            targeted.text.contains("None of the named MCP clients"),
            "{}",
            targeted.text
        );
        assert!(
            !targeted.text.contains("No supported MCP clients detected"),
            "asking for one client and being told there are none reads as a bug"
        );
    }

    #[test]
    fn install_is_idempotent() {
        let dir = TempDir::new().unwrap();
        install(&entry(), detected_clients(&dir), &[]);

        let report = install(&entry(), detected_clients(&dir), &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(
            report
                .text
                .contains("Detected 6 client(s), all up to date."),
            "{}",
            report.text
        );
        assert!(report.text.contains("up to date ("), "{}", report.text);
        assert!(
            !report.text.contains("Restart your MCP client"),
            "nothing changed, so there is nothing to restart for"
        );
    }

    #[test]
    fn install_says_up_to_date_for_every_client_it_checked() {
        // The whole complaint this answers: a run that reports nothing but a
        // count of zero gives no sign it looked at anything.
        let dir = TempDir::new().unwrap();
        install(&entry(), detected_clients(&dir), &[]);

        let report = install(&entry(), detected_clients(&dir), &[]);

        for client in clients_under(dir.path()) {
            let line = format!(
                "  {}: up to date ({})",
                client.name(),
                client.config_path().display()
            );
            assert!(report.text.contains(&line), "{}\n---\n{line}", report.text);
        }
    }

    #[test]
    fn install_says_what_a_stale_entry_was_pointing_at() {
        let dir = TempDir::new().unwrap();
        let client = JsonClient::new("Test", dir.path().join("config.json"), "mcpServers");
        std::fs::write(
            client.config_path(),
            serde_json::json!({"mcpServers": {"my-mcp": {"command": "/old/build"}}}).to_string(),
        )
        .unwrap();
        let path = client.config_path();

        let report = install(&entry(), vec![Box::new(client)], &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(
            report.text.contains(&format!(
                "  Test: updated — was pointing at /old/build ({})",
                path.display()
            )),
            "{}",
            report.text
        );
        assert!(report.text.contains("Detected 1 client(s), changed 1."));
    }

    #[test]
    fn install_says_it_updated_a_client_whose_binary_did_not_move() {
        // The entry names this binary already, so there is nothing to say about
        // where it went - but the pinned environment is not what this version
        // of the server asks for, and the config really is being rewritten.
        let dir = TempDir::new().unwrap();
        let client = JsonClient::new("Test", dir.path().join("config.json"), "mcpServers");
        client.install(&entry()).unwrap();
        let path = client.config_path();
        let wanted = entry().with_env("MY_MCP_HOME", "/var/lib/my-mcp");

        let report = install(&wanted, vec![Box::new(client)], &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(
            report
                .text
                .contains(&format!("  Test: updated ({})", path.display())),
            "{}",
            report.text
        );
        assert!(
            !report.text.contains("pointing at"),
            "the binary did not move, so nothing was pointing anywhere else: {}",
            report.text
        );
        assert!(report.text.contains("Detected 1 client(s), changed 1."));
        assert!(report.text.contains("Restart your MCP client"));
    }

    #[test]
    fn a_stale_approval_mode_is_a_client_the_install_command_writes() {
        // End to end over the whole fix: turning auto-approval on for a server
        // that is already installed used to report "already configured" and
        // write nothing at all.
        let dir = TempDir::new().unwrap();
        let asked = entry().with_auto_approve();
        install(&entry(), detected_clients(&dir), &["codex".to_string()]);

        let report = install(&asked, detected_clients(&dir), &["codex".to_string()]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Codex: updated ("), "{}", report.text);
        assert!(report.text.contains("Detected 1 client(s), changed 1."));
        let written = std::fs::read_to_string(dir.path().join(".codex/config.toml")).unwrap();
        assert!(
            written.contains("default_tools_approval_mode = \"auto\""),
            "{written}"
        );
    }

    #[test]
    fn every_kind_of_change_says_which_one_it_was() {
        // One run with all three: a client with no entry, one whose entry is
        // stale in place, and one whose entry names another binary.
        let dir = TempDir::new().unwrap();
        let fresh = JsonClient::new("Fresh", dir.path().join("fresh.json"), "mcpServers");
        let stale = JsonClient::new("Stale", dir.path().join("stale.json"), "mcpServers");
        let moved = JsonClient::new("Moved", dir.path().join("moved.json"), "mcpServers");
        stale.install(&entry()).unwrap();
        std::fs::write(
            moved.config_path(),
            serde_json::json!({"mcpServers": {"my-mcp": {"command": "/old/build"}}}).to_string(),
        )
        .unwrap();

        let report = install(
            &entry().with_env("A", "1"),
            vec![Box::new(fresh), Box::new(stale), Box::new(moved)],
            &[],
        );

        assert!(
            report.text.contains("  Fresh: configured ("),
            "{}",
            report.text
        );
        assert!(
            report.text.contains("  Stale: updated ("),
            "{}",
            report.text
        );
        assert!(
            report
                .text
                .contains("  Moved: updated — was pointing at /old/build ("),
            "{}",
            report.text
        );
        assert!(report.text.contains("Detected 3 client(s), changed 3."));
    }

    #[test]
    fn install_skips_clients_that_are_not_on_this_machine() {
        let dir = TempDir::new().unwrap();
        // Nothing is pre-created, so nothing is detected.
        let clients = clients_under(dir.path().join("no-such-home"));

        let report = install(&entry(), clients, &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Claude Desktop: not installed"));
        assert!(report.text.contains("No supported MCP clients detected."));
    }

    #[test]
    fn install_prints_the_snippet_when_it_configured_nothing() {
        let dir = TempDir::new().unwrap();
        let clients = clients_under(dir.path().join("no-such-home"));

        let report = install(&entry(), clients, &[]);

        assert!(report.text.contains("by hand"));
        assert!(report.text.contains(&entry().snippet()));
    }

    #[test]
    fn install_with_no_home_directory_prints_the_snippet_and_fails() {
        let report = install(&entry(), vec![], &[]);

        assert_eq!(report.outcome, Outcome::Failed);
        assert!(report.text.contains("no home directory"));
        assert!(report.text.contains(&entry().snippet()));
    }

    #[test]
    fn install_reports_a_client_it_could_not_write() {
        let report = install(&entry(), vec![Box::new(BrokenClient)], &[]);

        assert_eq!(
            report.outcome,
            Outcome::Failed,
            "a failed write must not report success"
        );
        assert!(report.text.contains("Broken: could not be configured"));
        assert!(
            report.text.contains("Detected 1 client(s), changed 0."),
            "{}",
            report.text
        );
        assert!(
            !report.text.contains("all up to date"),
            "a client that could not be written is not one that is up to date: {}",
            report.text
        );
    }

    #[test]
    fn one_broken_client_does_not_stop_the_others() {
        let dir = TempDir::new().unwrap();
        let good = JsonClient::new("Good", dir.path().join("config.json"), "mcpServers");

        let report = install(&entry(), vec![Box::new(BrokenClient), Box::new(good)], &[]);

        assert_eq!(report.outcome, Outcome::Failed);
        assert!(report.text.contains("Good: configured"));
        assert!(dir.path().join("config.json").exists());
    }

    //
    // uninstall
    //

    #[test]
    fn uninstall_removes_the_entry_from_every_client_that_has_one() {
        let dir = TempDir::new().unwrap();
        install(&entry(), detected_clients(&dir), &[]);

        let report = uninstall(&entry(), detected_clients(&dir), &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Removed from 6 client(s)."));
        for client in clients_under(dir.path()) {
            assert_eq!(
                client.check_existing(&entry()),
                InstallStatus::NotInstalled,
                "{}",
                client.name()
            );
        }
    }

    #[test]
    fn uninstall_says_where_each_entry_was() {
        let dir = TempDir::new().unwrap();
        install(&entry(), detected_clients(&dir), &[]);

        let report = uninstall(&entry(), detected_clients(&dir), &[]);

        for client in clients_under(dir.path()) {
            let line = format!(
                "  {}: removed ({})",
                client.name(),
                client.config_path().display()
            );
            assert!(report.text.contains(&line), "{}\n---\n{line}", report.text);
        }
    }

    #[test]
    fn uninstall_removes_an_entry_that_only_needed_refreshing() {
        // An entry that names this binary and is stale in some other way is
        // still ours. `has_entry` is what decides whether `uninstall` looks at
        // a client at all, so a status it did not account for is an entry left
        // behind by a command that reported success.
        let dir = TempDir::new().unwrap();
        install(&entry(), detected_clients(&dir), &[]);
        let wanted = entry().with_env("MY_MCP_HOME", "/var/lib/my-mcp");

        let report = uninstall(&wanted, detected_clients(&dir), &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(
            report.text.contains("Removed from 6 client(s)."),
            "{}",
            report.text
        );
        for client in clients_under(dir.path()) {
            assert_eq!(
                client.check_existing(&wanted),
                InstallStatus::NotInstalled,
                "{}",
                client.name()
            );
        }
    }

    #[test]
    fn uninstall_from_nothing_is_a_success_that_says_so() {
        let dir = TempDir::new().unwrap();

        let report = uninstall(&entry(), detected_clients(&dir), &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Nothing to remove"));
    }

    #[test]
    fn uninstall_is_idempotent() {
        let dir = TempDir::new().unwrap();
        install(&entry(), detected_clients(&dir), &[]);
        uninstall(&entry(), detected_clients(&dir), &[]);

        let report = uninstall(&entry(), detected_clients(&dir), &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Nothing to remove"));
    }

    #[test]
    fn uninstall_honours_a_client_selector() {
        let dir = TempDir::new().unwrap();
        install(&entry(), detected_clients(&dir), &[]);

        let report = uninstall(&entry(), detected_clients(&dir), &["codex".to_string()]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Removed from 1 client(s)."));
        for client in clients_under(dir.path()) {
            let expected = if client.name() == "Codex" {
                InstallStatus::NotInstalled
            } else {
                InstallStatus::Installed
            };
            assert_eq!(
                client.check_existing(&entry()),
                expected,
                "{}",
                client.name()
            );
        }
    }

    #[test]
    fn uninstall_reports_an_unknown_selector_and_removes_nothing() {
        let dir = TempDir::new().unwrap();
        install(&entry(), detected_clients(&dir), &[]);

        let report = uninstall(&entry(), detected_clients(&dir), &["nope".to_string()]);

        assert_eq!(report.outcome, Outcome::Usage);
        assert!(report.text.contains("unknown client `nope`"));
        for client in clients_under(dir.path()) {
            assert_eq!(
                client.check_existing(&entry()),
                InstallStatus::Installed,
                "{}",
                client.name()
            );
        }
    }

    #[test]
    fn uninstall_removes_a_stale_entry_too() {
        let dir = TempDir::new().unwrap();
        let client = JsonClient::new("Test", dir.path().join("config.json"), "mcpServers");
        std::fs::write(
            client.config_path(),
            serde_json::json!({"mcpServers": {"my-mcp": {"command": "/old/build"}}}).to_string(),
        )
        .unwrap();

        let report = uninstall(&entry(), vec![Box::new(client)], &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Removed from 1 client(s)."));
    }

    #[test]
    fn uninstall_leaves_another_server_alone() {
        let dir = TempDir::new().unwrap();
        let other = ServerEntry::new("other-mcp", &["serve"]);
        install(&entry(), detected_clients(&dir), &[]);
        install(&other, detected_clients(&dir), &[]);

        uninstall(&entry(), detected_clients(&dir), &[]);

        for client in clients_under(dir.path()) {
            assert_eq!(
                client.check_existing(&other),
                InstallStatus::Installed,
                "{}",
                client.name()
            );
        }
    }

    #[test]
    fn uninstall_with_no_home_directory_fails() {
        let report = uninstall(&entry(), vec![], &[]);

        assert_eq!(report.outcome, Outcome::Failed);
        assert!(report.text.contains("no home directory"));
    }

    #[test]
    fn uninstall_reports_a_client_it_could_not_change() {
        let report = uninstall(&entry(), vec![Box::new(BrokenRemoval)], &[]);

        assert_eq!(report.outcome, Outcome::Failed);
        assert!(report.text.contains("Broken: could not be changed"));
        assert!(
            !report.text.contains("Nothing to remove"),
            "a failure is not nothing to do"
        );
    }

    #[test]
    fn uninstall_reports_a_config_it_could_not_read() {
        // A config that will not parse is the one thing `check_existing` cannot
        // see past: it answers `NotInstalled`, which used to make this command
        // skip the client and report there was nothing to remove - about a file
        // that may well still name this server.
        let dir = TempDir::new().unwrap();
        install(&entry(), detected_clients(&dir), &[]);
        let cursor = clients_under(dir.path())
            .into_iter()
            .find(|client| client.name() == "Cursor")
            .expect("Cursor must be a known client");
        std::fs::write(cursor.config_path(), "{ not json").unwrap();

        let report = uninstall(&entry(), detected_clients(&dir), &[]);

        assert_eq!(report.outcome, Outcome::Failed);
        assert!(
            report.text.contains("Cursor: could not be changed"),
            "{}",
            report.text
        );
        assert!(
            report.text.contains("left untouched"),
            "and says the file was not rewritten: {}",
            report.text
        );
        assert_eq!(
            std::fs::read_to_string(cursor.config_path()).unwrap(),
            "{ not json",
            "a config we cannot parse is one we have no business rewriting"
        );
    }

    #[test]
    fn uninstall_reports_a_client_whose_status_hid_the_failure() {
        // `BrokenClient` is that shape exactly: it reports `NotInstalled` and
        // then fails the removal, which is what an unreadable config does.
        let report = uninstall(&entry(), vec![Box::new(BrokenClient)], &[]);

        assert_eq!(report.outcome, Outcome::Failed);
        assert!(report.text.contains("Broken: could not be changed"));
        assert!(
            !report.text.contains("Nothing to remove"),
            "a client that could not be read is not a client with nothing in it"
        );
    }

    #[test]
    fn uninstall_still_says_nothing_about_a_client_that_was_already_clean() {
        // Every detected client is asked now, so the quiet has to come from the
        // status rather than from never having called `uninstall`.
        let dir = TempDir::new().unwrap();
        let clients = detected_clients(&dir);

        let report = uninstall(&entry(), clients, &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Nothing to remove"));
        assert!(!report.text.contains("removed ("), "{}", report.text);
        for client in clients_under(dir.path()) {
            assert!(
                !client.config_path().exists(),
                "{} must not have a config invented for it",
                client.name()
            );
        }
    }

    #[test]
    fn uninstall_does_not_ask_a_client_that_is_not_installed() {
        // Nothing is on this machine, so there is nothing to read and nothing
        // to report - the loop stops at the status.
        let dir = TempDir::new().unwrap();

        let report = uninstall(&entry(), clients_under(dir.path()), &[]);

        assert_eq!(report.outcome, Outcome::Done);
        assert!(report.text.contains("Nothing to remove"));
    }
}
