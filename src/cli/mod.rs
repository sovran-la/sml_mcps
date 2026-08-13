//! Built-in subcommands for a binary that serves MCP.
//!
//! Every MCP server repeats the same command line: `install`, `uninstall`,
//! `serve`, `health`. Only the last two vary by server - the first two are a
//! loop over [`all_clients`] and an edit to each one's config file, which is the
//! same mechanical work everywhere. This module is that work, plus enough of a
//! dispatcher to reach it.
//!
//! A server with setup of its own - a data directory to create, a config file to
//! write, an API key to ask for - hangs it off [`on_install`](Cli::on_install)
//! and [`on_uninstall`](Cli::on_uninstall), which run either side of the config
//! edits.
//!
//! ```no_run
//! use sml_mcps::cli::{Cli, ServerEntry, SubCommand};
//!
//! fn main() -> std::process::ExitCode {
//!     Cli::new(ServerEntry::new("my-mcp", &["serve"]))
//!         .description("An MCP server for doing cool stuff")
//!         .on_install(|_args| {
//!             // whatever this server needs on disk before a client points
//!             // at it; an error here stops the install
//!             std::fs::create_dir_all("/tmp/my-mcp")?;
//!             Ok(())
//!         })
//!         .on_uninstall(|_args| {
//!             // and what to do about it once the entries are gone
//!             std::fs::remove_dir_all("/tmp/my-mcp")?;
//!             Ok(())
//!         })
//!         .on_serve(|_args| {
//!             // build the server, start the transport
//!             Ok(())
//!         })
//!         .on_health(|_args| {
//!             println!("ok");
//!             Ok(())
//!         })
//!         .command(
//!             SubCommand::new("doctor", "Run diagnostic checks")
//!                 .flag("--verbose", "Show detailed output for each check"),
//!             |args| {
//!                 println!("checking… {args:?}");
//!                 Ok(())
//!             },
//!         )
//!         .run()
//! }
//! ```
//!
//! # The registry is usable on its own
//!
//! [`Cli`] is a convenience, not a gatekeeper. A server that owns its own
//! command line - clap, hand-rolled, anything - can call the same functions
//! directly; see [`install`] for the shape of that.
//!
//! # What this is not
//!
//! An argument parser. Subcommand names are matched, `--help` is answered, and
//! `install`'s own `--client` is read. Everything after a custom command's name
//! is handed to its closure as `&[String]` untouched: the flags declared on a
//! [`SubCommand`] exist to be *printed*, and nothing validates that the ones
//! that arrived are among them.

pub mod install;

mod commands;

use commands::Outcome;

pub use install::{
    CodexClient, InstallError, InstallStatus, JsonClient, McpClient, ServerEntry, all_clients,
    backup_path, clients_under, home_dir, read_config_text, write_atomically,
};

use std::fmt;
use std::process::ExitCode;

/// What a subcommand's closure returns.
///
/// Boxed rather than this crate's [`McpError`](crate::McpError): a `doctor`
/// command's failures are its own, and a harness that forced them through one
/// error type would only be making authors write conversions.
pub type CommandResult = Result<(), Box<dyn std::error::Error>>;

/// One subcommand's closure.
///
/// `FnOnce` because exactly one subcommand runs per process, which lets `serve`
/// move a built [`Server`](crate::Server) into itself rather than borrowing one.
type Handler = Box<dyn FnOnce(&[String]) -> CommandResult>;

/// Exit status for a command line that was not understood, as distinct from a
/// command that ran and failed.
const USAGE_EXIT: u8 = 2;

//
// Subcommands
//

/// One subcommand, and the help it prints.
///
/// Display only. Declaring a flag here puts a line in `<command> --help`; it
/// does not parse anything, and a closure still sees every argument it was
/// given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubCommand {
    /// The word that selects this command, e.g. `doctor`.
    pub name: &'static str,

    /// One line, printed beside the name in the command list.
    pub description: &'static str,

    /// `(flag, description)`, in the order they should print.
    flags: Vec<(&'static str, &'static str)>,
}

impl SubCommand {
    /// A subcommand called `name`, described by `description`.
    pub fn new(name: &'static str, description: &'static str) -> Self {
        Self {
            name,
            description,
            flags: Vec::new(),
        }
    }

    /// Add a flag to this command's help.
    ///
    /// `flag` is printed verbatim, so it is the place to say what a flag takes:
    /// `.flag("--client <name>", "…")`.
    pub fn flag(mut self, flag: &'static str, description: &'static str) -> Self {
        self.flags.push((flag, description));
        self
    }

    /// The flags declared on this command, in the order they were added.
    pub fn flags(&self) -> &[(&'static str, &'static str)] {
        &self.flags
    }

    /// This command's `--help` screen.
    fn help(&self, program: &str) -> String {
        let mut out = format!("{}\n\n", self.description);

        if self.flags.is_empty() {
            out.push_str(&format!("USAGE: {program} {}\n", self.name));
            return out;
        }

        out.push_str(&format!(
            "USAGE: {program} {} [OPTIONS]\n\nOPTIONS:\n",
            self.name
        ));
        let width = column_width(self.flags.iter().map(|(flag, _)| flag.len()));
        for (flag, description) in &self.flags {
            out.push_str(&format!("    {flag:width$}{description}\n"));
        }

        out
    }
}

/// The built-in commands, as the dispatcher refers to them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Builtin {
    Install,
    Uninstall,
    Serve,
    Health,
}

impl Builtin {
    /// This command's name and help.
    fn subcommand(self) -> SubCommand {
        match self {
            Self::Install => {
                SubCommand::new("install", "Install this server into MCP client configs").flag(
                    "--client <name>",
                    "Only configure the named client (repeatable)",
                )
            }
            Self::Uninstall => {
                SubCommand::new("uninstall", "Remove this server from MCP client configs").flag(
                    "--client <name>",
                    "Only remove from the named client (repeatable)",
                )
            }
            Self::Serve => SubCommand::new("serve", "Start the MCP server"),
            Self::Health => SubCommand::new("health", "Run health checks"),
        }
    }
}

//
// The builder
//

/// A command line for an MCP server.
///
/// See the [module documentation](self) for the whole shape. Every method but
/// [`run`](Self::run) takes and returns `self`, so a `Cli` is written as one
/// expression.
pub struct Cli {
    /// What gets written into client configs.
    entry: ServerEntry,

    /// One line about the server, for the top of `--help`.
    description: Option<&'static str>,

    /// What to call this program in help, when the command line does not say.
    bin_name: Option<String>,

    /// The author's `serve`, if there is one.
    serve: Option<Handler>,

    /// The author's `health`, if there is one.
    health: Option<Handler>,

    /// The author's own install work, run before any client config is written.
    ///
    /// Named for when it runs rather than for the method that sets it, because
    /// when it runs is the whole of what it is.
    before_install: Option<Handler>,

    /// The author's own cleanup, run after every client config has been edited.
    after_uninstall: Option<Handler>,

    /// Commands the author added, in the order they were added.
    custom: Vec<(SubCommand, Handler)>,
}

impl fmt::Debug for Cli {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cli")
            .field("entry", &self.entry)
            .field("description", &self.description)
            .field("bin_name", &self.bin_name)
            .field(
                "commands",
                &self
                    .command_list()
                    .iter()
                    .map(|command| command.name)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Cli {
    /// A command line for the server `entry` describes.
    ///
    /// `install` and `uninstall` work immediately. `serve` and `health` appear
    /// only once [`on_serve`](Self::on_serve) and [`on_health`](Self::on_health)
    /// have been given something to call - a command that would do nothing is
    /// not one worth advertising.
    pub fn new(entry: ServerEntry) -> Self {
        Self {
            entry,
            description: None,
            bin_name: None,
            serve: None,
            health: None,
            before_install: None,
            after_uninstall: None,
            custom: Vec::new(),
        }
    }

    /// One line about this server, printed at the top of `--help`.
    pub fn description(mut self, description: &'static str) -> Self {
        self.description = Some(description);
        self
    }

    /// What to call this program in help output.
    ///
    /// Rarely needed: the name the user typed is used, falling back to the file
    /// name of the running binary and then to the entry name.
    pub fn bin_name(mut self, name: impl Into<String>) -> Self {
        self.bin_name = Some(name.into());
        self
    }

    /// What `serve` should do.
    ///
    /// Called with everything after `serve` on the command line. Whether this
    /// starts a [`StdioTransport`](crate::StdioTransport), calls
    /// [`Server::serve_daemon`](crate::Server::serve_daemon), or listens on
    /// HTTP is entirely the author's business.
    pub fn on_serve<F>(mut self, handler: F) -> Self
    where
        F: FnOnce(&[String]) -> CommandResult + 'static,
    {
        self.serve = Some(Box::new(handler));
        self
    }

    /// What `health` should do.
    pub fn on_health<F>(mut self, handler: F) -> Self
    where
        F: FnOnce(&[String]) -> CommandResult + 'static,
    {
        self.health = Some(Box::new(handler));
        self
    }

    /// What `install` should do before it writes any client config.
    ///
    /// The client entries are only half of an install. The other half is
    /// whatever this server needs on disk to be worth pointing at - a data
    /// directory, a config file, an API key asked for at a prompt - and it
    /// belongs here, where it happens before any client is told the server
    /// exists.
    ///
    /// An `Err` stops the install where it stands: the error is printed, no
    /// config is touched, and the process exits non-zero. A command line that
    /// was refused outright - an unknown flag, an unknown `--client` - never
    /// reaches this at all, so a typo cannot leave half an install behind.
    ///
    /// ```
    /// # use sml_mcps::cli::{Cli, ServerEntry};
    /// let cli = Cli::new(ServerEntry::new("my-mcp", &["serve"])).on_install(|args| {
    ///     println!("setting up, for `install {}`", args.join(" "));
    ///     Ok(())
    /// });
    /// ```
    pub fn on_install<F>(mut self, handler: F) -> Self
    where
        F: FnOnce(&[String]) -> CommandResult + 'static,
    {
        self.before_install = Some(Box::new(handler));
        self
    }

    /// What `uninstall` should do once the client configs have been edited.
    ///
    /// The other end of [`on_install`](Self::on_install): what that created,
    /// this removes. It runs after the entries are gone rather than before,
    /// because the two half-states are not equally bad - data left behind by a
    /// server no client can start is litter, and a server still being started
    /// after its data has been deleted is a crash.
    ///
    /// An `Err` is printed and fails the process, but the removals stand. There
    /// is no putting them back, and a report that implied otherwise would be
    /// worse than one that says only what went wrong.
    pub fn on_uninstall<F>(mut self, handler: F) -> Self
    where
        F: FnOnce(&[String]) -> CommandResult + 'static,
    {
        self.after_uninstall = Some(Box::new(handler));
        self
    }

    /// Add a subcommand of this server's own.
    ///
    /// `handler` is called with everything after the command's name, unparsed.
    /// A command whose name is one of the built-ins replaces it, which is the
    /// escape hatch for a server that wants its own `install`.
    pub fn command<F>(mut self, command: SubCommand, handler: F) -> Self
    where
        F: FnOnce(&[String]) -> CommandResult + 'static,
    {
        self.custom.push((command, Box::new(handler)));
        self
    }

    //
    // Running
    //

    /// Dispatch on this process's command line.
    ///
    /// ```no_run
    /// # use sml_mcps::cli::{Cli, ServerEntry};
    /// fn main() -> std::process::ExitCode {
    ///     Cli::new(ServerEntry::new("my-mcp", &["serve"]))
    ///         .on_serve(|_| Ok(()))
    ///         .run()
    /// }
    /// ```
    pub fn run(self) -> ExitCode {
        self.run_from(std::env::args())
    }

    /// [`run`](Self::run) with the command line supplied rather than read from
    /// the process.
    ///
    /// `argv` is process-global and tests run threaded inside one process, so
    /// taking it as an argument is the only way the dispatch itself is testable
    /// - which goes for a server's tests as much as this crate's.
    ///
    /// The first element is the program name and is not treated as a command.
    pub fn run_from<I, S>(mut self, argv: I) -> ExitCode
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let argv: Vec<String> = argv.into_iter().map(Into::into).collect();
        self = self.named_by(&argv);

        match self.plan(argv.get(1..).unwrap_or_default()) {
            Plan::Help => {
                print!("{}", self.help());
                ExitCode::SUCCESS
            }
            Plan::CommandHelp(name) => match self.command_help(&name) {
                Some(help) => {
                    print!("{help}");
                    ExitCode::SUCCESS
                }
                // Unreachable: a plan only names a command that exists.
                None => self.unknown(&name),
            },
            Plan::Builtin(Builtin::Install, args) => self.install(&args),
            Plan::Builtin(Builtin::Uninstall, args) => self.uninstall(&args),
            Plan::Builtin(Builtin::Serve, args) => match self.serve.take() {
                Some(handler) => finish(handler(&args)),
                None => self.unknown("serve"),
            },
            Plan::Builtin(Builtin::Health, args) => match self.health.take() {
                Some(handler) => finish(handler(&args)),
                None => self.unknown("health"),
            },
            Plan::Custom(index, args) => {
                let (_, handler) = self.custom.swap_remove(index);
                finish(handler(&args))
            }
            Plan::Unknown(name) => self.unknown(&name),
        }
    }

    /// Take this program's name from `argv`, unless one was set by hand.
    ///
    /// The name the user typed is the name their shell knows, which is the one
    /// that belongs in `USAGE` - a binary installed as `mcp-thing` should not
    /// call itself something else in its own help.
    fn named_by(mut self, argv: &[String]) -> Self {
        if self.bin_name.is_none() {
            self.bin_name = argv.first().and_then(|path| file_stem(path));
        }

        self
    }

    /// Run the built-in `install`, and the author's own work before it.
    fn install(&mut self, args: &[String]) -> ExitCode {
        let selectors = match client_selectors(args) {
            Ok(selectors) => selectors,
            Err(message) => return self.usage_error(&message, "install"),
        };

        let clients = all_clients();
        // Every way this command line can be refused is settled before the
        // hook runs. An `install --client cursur` that had already prompted
        // for an API key before it said "unknown client" would have done half
        // a job and reported none of it.
        if let Some(message) = commands::selector_error(&clients, &selectors) {
            return self.report(commands::Report::of(Outcome::Usage, message));
        }

        if let Some(failed) = hook_failure(self.before_install.take(), args) {
            return failed;
        }

        self.report(commands::install(&self.entry, clients, &selectors))
    }

    /// Run the built-in `uninstall`, and the author's own cleanup after it.
    fn uninstall(&mut self, args: &[String]) -> ExitCode {
        let selectors = match client_selectors(args) {
            Ok(selectors) => selectors,
            Err(message) => return self.usage_error(&message, "uninstall"),
        };

        let clients = all_clients();
        if let Some(message) = commands::selector_error(&clients, &selectors) {
            return self.report(commands::Report::of(Outcome::Usage, message));
        }

        let removals = self.report(commands::uninstall(&self.entry, clients, &selectors));

        // Afterwards, and unconditionally: the entries are gone whatever the
        // removals reported, so whatever they pointed at is now cleanup either
        // way. A hook that fails fails the process without undoing anything -
        // there is nothing to undo it with.
        hook_failure(self.after_uninstall.take(), args).unwrap_or(removals)
    }

    /// Print what a built-in command had to say, and exit accordingly.
    ///
    /// A command line that was wrong goes to stderr, because nothing was
    /// attempted and there is no report to pipe anywhere. Everything else is
    /// this command's output.
    fn report(&self, report: commands::Report) -> ExitCode {
        match report.outcome {
            Outcome::Done => {
                print!("{}", report.text);
                ExitCode::SUCCESS
            }
            Outcome::Failed => {
                print!("{}", report.text);
                ExitCode::FAILURE
            }
            Outcome::Usage => {
                eprint!("error: {}", report.text);
                ExitCode::from(USAGE_EXIT)
            }
        }
    }

    /// Complain about a command that does not exist, and show the ones that do.
    fn unknown(&self, name: &str) -> ExitCode {
        eprintln!("error: unknown command `{name}`");
        eprintln!();
        eprint!("{}", self.help());

        ExitCode::from(USAGE_EXIT)
    }

    /// Complain about an argument a built-in command did not understand.
    fn usage_error(&self, message: &str, command: &str) -> ExitCode {
        eprintln!("error: {message}");
        if let Some(help) = self.command_help(command) {
            eprintln!();
            eprint!("{help}");
        }

        ExitCode::from(USAGE_EXIT)
    }

    //
    // Help
    //

    /// The top-level `--help` screen.
    pub fn help(&self) -> String {
        let program = self.program();

        let mut out = match self.description {
            Some(description) => format!("{program} — {description}\n\n"),
            None => format!("{program}\n\n"),
        };
        out.push_str(&format!("USAGE: {program} <COMMAND>\n\nCOMMANDS:\n"));

        let commands = self.command_list();
        let width = column_width(commands.iter().map(|command| command.name.len()));
        for command in &commands {
            let name = command.name;
            out.push_str(&format!("    {name:width$}{}\n", command.description));
        }

        out
    }

    /// One command's `--help` screen, if there is such a command.
    pub fn command_help(&self, name: &str) -> Option<String> {
        self.command_list()
            .iter()
            .find(|command| command.name == name)
            .map(|command| command.help(&self.program()))
    }

    /// Every command this CLI answers to, in the order help lists them.
    ///
    /// Built-ins first, then the author's, with any built-in whose name an
    /// author took dropped in favour of theirs.
    pub fn command_list(&self) -> Vec<SubCommand> {
        let mut builtins = vec![Builtin::Install, Builtin::Uninstall];
        if self.serve.is_some() {
            builtins.push(Builtin::Serve);
        }
        if self.health.is_some() {
            builtins.push(Builtin::Health);
        }

        builtins
            .into_iter()
            .map(Builtin::subcommand)
            .filter(|builtin| {
                !self
                    .custom
                    .iter()
                    .any(|(command, _)| command.name == builtin.name)
            })
            .chain(self.custom.iter().map(|(command, _)| command.clone()))
            .collect()
    }

    /// What to call this program in help.
    fn program(&self) -> String {
        self.bin_name
            .clone()
            .or_else(|| {
                std::env::current_exe()
                    .ok()
                    .and_then(|path| file_stem(&path.display().to_string()))
            })
            .unwrap_or_else(|| self.entry.name.to_string())
    }

    //
    // Dispatch
    //

    /// What a command line asks for.
    fn plan(&self, args: &[String]) -> Plan {
        let Some(first) = args.first() else {
            // A server whose entry starts it with no arguments at all is
            // launched bare by its clients, so bare has to mean `serve`.
            return if self.entry.args.is_empty() && self.serve.is_some() {
                Plan::Builtin(Builtin::Serve, Vec::new())
            } else {
                Plan::Help
            };
        };
        let rest = args[1..].to_vec();

        if is_help_flag(first) || first == "help" {
            return match rest.first() {
                Some(name) if self.find(name).is_some() => Plan::CommandHelp(name.clone()),
                _ => Plan::Help,
            };
        }

        let Some(target) = self.find(first) else {
            return Plan::Unknown(first.clone());
        };

        // `install --help` is a request for help, not an install with a strange
        // flag - so no closure ever has to answer for `--help` itself.
        if rest.iter().any(|argument| is_help_flag(argument)) {
            return Plan::CommandHelp(first.clone());
        }

        match target {
            Target::Builtin(builtin) => Plan::Builtin(builtin, rest),
            Target::Custom(index) => Plan::Custom(index, rest),
        }
    }

    /// What, if anything, `name` selects. The author's commands win.
    fn find(&self, name: &str) -> Option<Target> {
        if let Some(index) = self
            .custom
            .iter()
            .position(|(command, _)| command.name == name)
        {
            return Some(Target::Custom(index));
        }

        match name {
            "install" => Some(Target::Builtin(Builtin::Install)),
            "uninstall" => Some(Target::Builtin(Builtin::Uninstall)),
            "serve" if self.serve.is_some() => Some(Target::Builtin(Builtin::Serve)),
            "health" if self.health.is_some() => Some(Target::Builtin(Builtin::Health)),
            _ => None,
        }
    }
}

/// What a command name selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// One of this crate's commands.
    Builtin(Builtin),

    /// The author's command at this index of `custom`.
    Custom(usize),
}

/// What a command line asks for, worked out before anything is run.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Plan {
    /// Print the command list.
    Help,

    /// Print one command's help.
    CommandHelp(String),

    /// Run one of this crate's commands.
    Builtin(Builtin, Vec<String>),

    /// Run the author's command at this index, with these arguments.
    Custom(usize, Vec<String>),

    /// There is no such command.
    Unknown(String),
}

//
// Odds and ends
//

/// Run `hook`, if there is one, and say what to exit with if it failed.
///
/// `None` covers both "there was nothing to run" and "it worked", because the
/// two callers want different things from a success: `install` carries on into
/// the config writes, and `uninstall` keeps the exit status its removals
/// already earned.
fn hook_failure(hook: Option<Handler>, args: &[String]) -> Option<ExitCode> {
    let hook = hook?;

    match hook(args) {
        Ok(()) => None,
        Err(error) => {
            eprintln!("error: {error}");
            Some(ExitCode::FAILURE)
        }
    }
}

/// Turn a handler's result into an exit status, saying what went wrong first.
fn finish(result: CommandResult) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Whether `argument` is a request for help.
fn is_help_flag(argument: &str) -> bool {
    argument == "--help" || argument == "-h"
}

/// The `--client` values in `args`, or what was wrong with them.
///
/// The one place the harness reads a flag, because `--client` is the harness's
/// own. Anything else is a typo worth reporting: an `install --clients cursor`
/// that quietly configured everything is the sort of thing nobody notices.
fn client_selectors(args: &[String]) -> Result<Vec<String>, String> {
    let mut selectors = Vec::new();
    let mut remaining = args.iter();

    while let Some(argument) = remaining.next() {
        let value = if argument == "--client" {
            remaining
                .next()
                .ok_or_else(|| "`--client` needs a client name".to_string())?
                .clone()
        } else if let Some(value) = argument.strip_prefix("--client=") {
            value.to_string()
        } else {
            return Err(format!("unknown argument `{argument}`"));
        };

        if value.is_empty() {
            return Err("`--client` needs a client name".to_string());
        }
        selectors.push(value);
    }

    Ok(selectors)
}

/// How wide the left column of a help listing has to be.
fn column_width(lengths: impl Iterator<Item = usize>) -> usize {
    lengths.max().unwrap_or(0) + 4
}

/// The file name of `path`, without any extension.
///
/// A program invoked as `/usr/local/bin/my-mcp` calls itself `my-mcp` in its own
/// help, which is what the person reading it typed. A path with no file name in
/// it at all - `""`, `/`, `..` - has no stem, and the caller falls back.
fn file_stem(path: &str) -> Option<String> {
    std::path::Path::new(path)
        .file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A CLI for a server that does nothing, named so help is predictable.
    fn cli() -> Cli {
        Cli::new(ServerEntry::new("my-mcp", &["serve"])).bin_name("my-mcp")
    }

    /// A CLI for a server no machine has ever heard of.
    ///
    /// `uninstall` writes only to a config that has an entry to remove, and no
    /// config anywhere has one under this name. That makes the built-in safe
    /// to run for real here - against whoever's home directory the suite is
    /// running in - which is the only way to see what happens *after* it.
    fn stranger() -> Cli {
        Cli::new(ServerEntry::new(
            "sml-mcps-cli-tests-no-such-server",
            &["serve"],
        ))
        .bin_name("my-mcp")
    }

    /// Somewhere for a handler to record that it ran, and with what.
    type Log = Arc<Mutex<Vec<Vec<String>>>>;

    /// A fresh log.
    fn log() -> Log {
        Arc::new(Mutex::new(Vec::new()))
    }

    /// A handler that records its arguments and succeeds.
    fn recorder(log: &Log) -> impl FnOnce(&[String]) -> CommandResult + use<> {
        let log = Arc::clone(log);
        move |args| {
            log.lock().unwrap().push(args.to_vec());
            Ok(())
        }
    }

    /// A handler that records its arguments and fails.
    ///
    /// What an `on_install` test wants: it proves the hook was reached, and
    /// then stops the run before the built-in opens a single config file - so
    /// the dispatch is testable against whoever's home directory is running
    /// the suite without writing to it.
    fn refuser(log: &Log) -> impl FnOnce(&[String]) -> CommandResult + use<> {
        let log = Arc::clone(log);
        move |args| {
            log.lock().unwrap().push(args.to_vec());
            Err("setup broke".into())
        }
    }

    /// What a log saw, if anything.
    fn seen(log: &Log) -> Vec<Vec<String>> {
        log.lock().unwrap().clone()
    }

    /// Run a CLI over a command line, program name included.
    fn run(cli: Cli, argv: &[&str]) -> ExitCode {
        cli.run_from(argv.iter().map(|argument| argument.to_string()))
    }

    /// A command line that worked.
    fn success() -> ExitCode {
        ExitCode::SUCCESS
    }

    /// A command that ran and failed.
    fn failure() -> ExitCode {
        ExitCode::FAILURE
    }

    /// A command line that was not understood.
    fn usage() -> ExitCode {
        ExitCode::from(USAGE_EXIT)
    }

    //
    // SubCommand
    //

    #[test]
    fn a_subcommand_carries_its_own_help() {
        let command = SubCommand::new("doctor", "Run diagnostic checks")
            .flag("--verbose", "Show detailed output for each check")
            .flag("--fix", "Attempt to auto-fix problems found");

        assert_eq!(command.name, "doctor");
        assert_eq!(command.description, "Run diagnostic checks");
        assert_eq!(
            command.flags(),
            [
                ("--verbose", "Show detailed output for each check"),
                ("--fix", "Attempt to auto-fix problems found")
            ]
        );
    }

    #[test]
    fn a_subcommands_help_is_the_shape_the_plan_specifies() {
        let help = SubCommand::new("doctor", "Run diagnostic checks")
            .flag("--verbose", "Show detailed output for each check")
            .flag("--fix", "Attempt to auto-fix problems found")
            .help("my-mcp");

        assert_eq!(
            help,
            "Run diagnostic checks\n\
             \n\
             USAGE: my-mcp doctor [OPTIONS]\n\
             \n\
             OPTIONS:\n    \
             --verbose    Show detailed output for each check\n    \
             --fix        Attempt to auto-fix problems found\n"
        );
    }

    #[test]
    fn a_subcommand_with_no_flags_prints_no_options_section() {
        let help = SubCommand::new("serve", "Start the MCP server").help("my-mcp");

        assert_eq!(help, "Start the MCP server\n\nUSAGE: my-mcp serve\n");
    }

    //
    // Top-level help
    //

    #[test]
    fn help_is_the_shape_the_plan_specifies() {
        let help = cli()
            .description("An MCP server for doing cool stuff")
            .on_serve(|_| Ok(()))
            .on_health(|_| Ok(()))
            .command(SubCommand::new("doctor", "Run diagnostic checks"), |_| {
                Ok(())
            })
            .help();

        assert_eq!(
            help,
            "my-mcp — An MCP server for doing cool stuff\n\
             \n\
             USAGE: my-mcp <COMMAND>\n\
             \n\
             COMMANDS:\n    \
             install      Install this server into MCP client configs\n    \
             uninstall    Remove this server from MCP client configs\n    \
             serve        Start the MCP server\n    \
             health       Run health checks\n    \
             doctor       Run diagnostic checks\n"
        );
    }

    #[test]
    fn help_without_a_description_still_names_the_program() {
        let help = cli().help();

        assert!(help.starts_with("my-mcp\n\n"), "{help}");
        assert!(help.contains("USAGE: my-mcp <COMMAND>"), "{help}");
    }

    #[test]
    fn help_lists_install_and_uninstall_with_no_handlers_at_all() {
        // They are this crate's to run; nothing has to be provided for them.
        let names = command_names(&cli());

        assert_eq!(names, ["install", "uninstall"]);
    }

    #[test]
    fn a_command_with_no_handler_is_not_advertised() {
        let names = command_names(&cli().on_health(|_| Ok(())));

        assert_eq!(
            names,
            ["install", "uninstall", "health"],
            "serve has nothing to call, so it must not be offered"
        );
    }

    #[test]
    fn custom_commands_come_last_in_the_order_they_were_added() {
        let names = command_names(
            &cli()
                .command(SubCommand::new("doctor", ""), |_| Ok(()))
                .command(SubCommand::new("audit", ""), |_| Ok(())),
        );

        assert_eq!(names, ["install", "uninstall", "doctor", "audit"]);
    }

    #[test]
    fn a_custom_command_replaces_the_built_in_it_names() {
        let cli = cli().command(SubCommand::new("install", "Install it my way"), |_| Ok(()));

        let names = command_names(&cli);
        assert_eq!(names, ["uninstall", "install"]);
        assert!(cli.help().contains("Install it my way"));
        assert!(
            !cli.help()
                .contains("Install this server into MCP client configs"),
            "the built-in must not be listed twice"
        );
    }

    #[test]
    fn the_command_column_is_as_wide_as_the_longest_name() {
        let help = cli()
            .command(SubCommand::new("a-very-long-command", "Yes"), |_| Ok(()))
            .help();

        assert!(
            help.contains("    install                Install"),
            "{help}"
        );
        assert!(help.contains("    a-very-long-command    Yes"), "{help}");
    }

    #[test]
    fn the_program_name_falls_back_to_the_running_binary() {
        // With nothing set by hand and no command line to read, `current_exe`
        // decides - the entry name is the last resort, not the second one. In
        // a test the running binary is the test binary.
        let running = file_stem(&std::env::current_exe().unwrap().display().to_string()).unwrap();
        let cli = Cli::new(ServerEntry::new("not-the-binary-name", &[]));

        assert_eq!(cli.program(), running);
        assert_ne!(cli.program(), "not-the-binary-name");
    }

    #[test]
    fn the_program_name_comes_from_the_command_line() {
        // A binary installed as `mcp-thing` should not call itself something
        // else in its own help.
        let cli = Cli::new(ServerEntry::new("my-mcp", &["serve"]))
            .named_by(&["/usr/local/bin/mcp-thing".to_string()]);

        assert_eq!(cli.program(), "mcp-thing");
        assert!(cli.help().contains("USAGE: mcp-thing <COMMAND>"), "{cli:?}");
        assert!(
            cli.command_help("install")
                .unwrap()
                .contains("USAGE: mcp-thing install"),
            "a command's own help names it too"
        );
    }

    #[test]
    fn a_name_set_by_hand_beats_the_command_line() {
        let cli = cli().named_by(&["/usr/local/bin/mcp-thing".to_string()]);

        assert_eq!(cli.program(), "my-mcp");
    }

    #[test]
    fn a_command_line_with_no_usable_program_name_falls_back() {
        // `argv[0]` is whatever the caller was invoked as, and it can be
        // missing or have no file name in it at all.
        for argv in [vec![], vec![String::new()], vec!["/".to_string()]] {
            let program = Cli::new(ServerEntry::new("my-mcp", &[]))
                .named_by(&argv)
                .program();

            assert!(!program.is_empty(), "{argv:?}");
        }
    }

    //
    // Per-command help
    //

    #[test]
    fn a_built_in_command_has_help_of_its_own() {
        let help = cli().command_help("install").unwrap();

        assert!(help.starts_with("Install this server into MCP client configs"));
        assert!(help.contains("USAGE: my-mcp install [OPTIONS]"), "{help}");
        assert!(help.contains("--client <name>"), "{help}");
    }

    #[test]
    fn a_custom_command_has_help_of_its_own() {
        let cli = cli().command(
            SubCommand::new("doctor", "Run diagnostic checks").flag("--fix", "Fix what is broken"),
            |_| Ok(()),
        );

        let help = cli.command_help("doctor").unwrap();

        assert!(help.contains("USAGE: my-mcp doctor [OPTIONS]"), "{help}");
        assert!(help.contains("--fix    Fix what is broken"), "{help}");
    }

    #[test]
    fn a_command_that_does_not_exist_has_no_help() {
        assert!(cli().command_help("doctor").is_none());
        assert!(cli().command_help("serve").is_none());
    }

    //
    // Dispatch
    //

    #[test]
    fn a_command_name_reaches_its_own_handler() {
        let doctor = log();
        let audit = log();
        let cli = cli()
            .command(SubCommand::new("doctor", ""), recorder(&doctor))
            .command(SubCommand::new("audit", ""), recorder(&audit));

        assert_eq!(run(cli, &["my-mcp", "audit"]), success());
        assert!(seen(&doctor).is_empty());
        assert_eq!(seen(&audit), [Vec::<String>::new()]);
    }

    #[test]
    fn a_handler_is_given_everything_after_its_name() {
        let log = log();
        let cli = cli().command(SubCommand::new("doctor", ""), recorder(&log));

        run(cli, &["my-mcp", "doctor", "--fix", "-v", "extra"]);

        assert_eq!(seen(&log), [["--fix", "-v", "extra"]]);
    }

    #[test]
    fn serve_reaches_the_serve_handler() {
        let log = log();

        assert_eq!(
            run(cli().on_serve(recorder(&log)), &["my-mcp", "serve"]),
            success()
        );
        assert_eq!(seen(&log).len(), 1);
    }

    #[test]
    fn health_reaches_the_health_handler() {
        let log = log();

        assert_eq!(
            run(
                cli().on_health(recorder(&log)),
                &["my-mcp", "health", "--deep"]
            ),
            success()
        );
        assert_eq!(seen(&log), [["--deep"]]);
    }

    #[test]
    fn serve_and_health_are_different_handlers() {
        let serve = log();
        let health = log();
        let cli = cli()
            .on_serve(recorder(&serve))
            .on_health(recorder(&health));

        run(cli, &["my-mcp", "health"]);

        assert!(seen(&serve).is_empty());
        assert_eq!(seen(&health).len(), 1);
    }

    #[test]
    fn the_last_handler_given_for_a_command_is_the_one_that_runs() {
        let first = log();
        let second = log();
        let cli = cli().on_serve(recorder(&first)).on_serve(recorder(&second));

        run(cli, &["my-mcp", "serve"]);

        assert!(seen(&first).is_empty());
        assert_eq!(seen(&second).len(), 1);
    }

    #[test]
    fn a_custom_command_overriding_a_built_in_is_the_one_that_runs() {
        let log = log();
        let cli = cli().command(SubCommand::new("install", "Mine"), recorder(&log));

        assert_eq!(run(cli, &["my-mcp", "install", "--anything"]), success());
        assert_eq!(seen(&log), [["--anything"]]);
    }

    #[test]
    fn an_unknown_command_is_a_usage_error() {
        assert_eq!(run(cli(), &["my-mcp", "frobnicate"]), usage());
    }

    #[test]
    fn a_command_with_no_handler_is_unknown() {
        // `serve` is only a command once there is something to call.
        assert_eq!(run(cli(), &["my-mcp", "serve"]), usage());
        assert_eq!(run(cli(), &["my-mcp", "health"]), usage());
    }

    #[test]
    fn help_for_a_command_with_no_handler_lists_the_ones_that_exist() {
        // `serve` is not a command here, so `help serve` is just `help` - not
        // an error about a command the user was never offered.
        for command in ["serve", "health"] {
            assert_eq!(
                cli().plan(&["help".to_string(), command.to_string()]),
                Plan::Help,
                "{command}"
            );
            assert_eq!(run(cli(), &["my-mcp", "help", command]), success());
        }
    }

    #[test]
    fn a_handler_that_fails_fails_the_process() {
        let cli = cli().command(SubCommand::new("doctor", ""), |_| {
            Err("something broke".into())
        });

        assert_eq!(run(cli, &["my-mcp", "doctor"]), failure());
    }

    #[test]
    fn a_serve_handler_can_move_what_it_needs_into_itself() {
        // The reason handlers are FnOnce: a built server is moved, not
        // borrowed.
        let owned = String::from("a server, notionally");
        let cli = cli().on_serve(move |_| {
            drop(owned);
            Ok(())
        });

        assert_eq!(run(cli, &["my-mcp", "serve"]), success());
    }

    //
    // Help dispatch
    //

    #[test]
    fn no_arguments_prints_help() {
        assert_eq!(cli().plan(&[]), Plan::Help);
    }

    #[test]
    fn a_bare_launch_serves_when_the_entry_takes_no_arguments() {
        // A client configured with `args: []` launches the binary with nothing
        // on the command line and expects a server, not a help screen.
        let cli = Cli::new(ServerEntry::new("my-mcp", &[])).on_serve(|_| Ok(()));

        assert_eq!(cli.plan(&[]), Plan::Builtin(Builtin::Serve, Vec::new()));
    }

    #[test]
    fn a_bare_launch_prints_help_when_the_entry_names_a_command() {
        let cli = cli().on_serve(|_| Ok(()));

        assert_eq!(cli.plan(&[]), Plan::Help);
    }

    #[test]
    fn a_bare_launch_prints_help_when_there_is_nothing_to_serve_with() {
        let cli = Cli::new(ServerEntry::new("my-mcp", &[]));

        assert_eq!(cli.plan(&[]), Plan::Help);
    }

    #[test]
    fn the_help_flags_print_help() {
        for flag in ["--help", "-h", "help"] {
            assert_eq!(cli().plan(&[flag.to_string()]), Plan::Help, "{flag}");
            assert_eq!(run(cli(), &["my-mcp", flag]), success(), "{flag}");
        }
    }

    #[test]
    fn help_followed_by_a_command_prints_that_commands_help() {
        assert_eq!(
            cli().plan(&["help".to_string(), "install".to_string()]),
            Plan::CommandHelp("install".to_string())
        );
    }

    #[test]
    fn help_followed_by_a_non_command_prints_the_command_list() {
        assert_eq!(
            cli().plan(&["--help".to_string(), "frobnicate".to_string()]),
            Plan::Help
        );
    }

    #[test]
    fn a_command_asked_for_help_prints_help_instead_of_running() {
        let log = log();
        let cli = cli().command(SubCommand::new("doctor", "Check"), recorder(&log));

        assert_eq!(run(cli, &["my-mcp", "doctor", "--help"]), success());
        assert!(
            seen(&log).is_empty(),
            "no closure should have to answer for --help itself"
        );
    }

    #[test]
    fn a_help_flag_anywhere_in_a_commands_arguments_prints_help() {
        let cli = cli().command(SubCommand::new("doctor", "Check"), |_| Ok(()));

        assert_eq!(
            cli.plan(&["doctor".to_string(), "--fix".to_string(), "-h".to_string()]),
            Plan::CommandHelp("doctor".to_string())
        );
    }

    #[test]
    fn help_for_an_unknown_command_is_still_a_usage_error() {
        assert_eq!(run(cli(), &["my-mcp", "frobnicate", "--help"]), usage());
    }

    //
    // Built-in dispatch
    //

    #[test]
    fn install_and_uninstall_reach_this_crates_commands() {
        assert_eq!(
            cli().plan(&["install".to_string()]),
            Plan::Builtin(Builtin::Install, Vec::new())
        );
        assert_eq!(
            cli().plan(&[
                "uninstall".to_string(),
                "--client".to_string(),
                "codex".to_string()
            ]),
            Plan::Builtin(
                Builtin::Uninstall,
                vec!["--client".to_string(), "codex".to_string()]
            )
        );
    }

    #[test]
    fn install_with_an_unknown_client_touches_nothing_and_fails() {
        // The one built-in that is safe to run end to end here: the selector
        // matches nothing, so no config file is ever opened. A mistyped client
        // name exits the same way a mistyped flag does.
        assert_eq!(
            run(cli(), &["my-mcp", "install", "--client", "not-a-client"]),
            usage()
        );
        assert_eq!(
            run(cli(), &["my-mcp", "uninstall", "--client", "not-a-client"]),
            usage()
        );
    }

    #[test]
    fn install_with_an_unknown_flag_is_a_usage_error() {
        assert_eq!(
            run(cli(), &["my-mcp", "install", "--clients", "cursor"]),
            usage()
        );
        assert_eq!(run(cli(), &["my-mcp", "uninstall", "--force"]), usage());
    }

    #[test]
    fn install_with_a_client_flag_and_no_client_is_a_usage_error() {
        assert_eq!(run(cli(), &["my-mcp", "install", "--client"]), usage());
    }

    //
    // install and uninstall hooks
    //

    #[test]
    fn an_install_hook_that_fails_fails_the_install() {
        // And gets no further: this is the run that must not reach a config
        // file, which is also what makes it safe to run here.
        let log = log();

        assert_eq!(
            run(cli().on_install(refuser(&log)), &["my-mcp", "install"]),
            failure()
        );
        assert_eq!(seen(&log).len(), 1, "the hook must have been reached");
    }

    #[test]
    fn an_install_hook_is_given_everything_after_the_command() {
        // A hook that cannot see `--client` cannot do anything different for a
        // targeted install.
        let log = log();
        let cli = cli().on_install(refuser(&log));

        run(cli, &["my-mcp", "install", "--client", "cursor"]);

        assert_eq!(seen(&log), [["--client", "cursor"]]);
    }

    #[test]
    fn a_command_line_that_is_refused_never_reaches_the_install_hook() {
        // Nothing was attempted, so nothing may have been set up: an install
        // that prompted for an API key and then said `unknown client` would
        // have done half a job and reported none of it.
        for args in [
            vec!["my-mcp", "install", "--clients", "cursor"],
            vec!["my-mcp", "install", "--client"],
            vec!["my-mcp", "install", "--client", "not-a-client"],
        ] {
            let log = log();

            assert_eq!(
                run(cli().on_install(refuser(&log)), &args),
                usage(),
                "{args:?}"
            );
            assert!(seen(&log).is_empty(), "{args:?}");
        }
    }

    #[test]
    fn a_command_line_that_is_refused_never_reaches_the_uninstall_hook() {
        let log = log();
        let cli = cli().on_uninstall(recorder(&log));

        assert_eq!(
            run(cli, &["my-mcp", "uninstall", "--client", "not-a-client"]),
            usage()
        );
        assert!(
            seen(&log).is_empty(),
            "nothing was removed, so there is nothing to clean up after"
        );
    }

    #[test]
    fn an_uninstall_hook_runs_when_the_removals_are_done() {
        let log = log();

        assert_eq!(
            run(
                stranger().on_uninstall(recorder(&log)),
                &["my-mcp", "uninstall"]
            ),
            success()
        );
        assert_eq!(seen(&log), [Vec::<String>::new()]);
    }

    #[test]
    fn an_uninstall_hook_that_fails_fails_the_process() {
        // The removals stood - there is no putting them back - but a failure
        // that exited zero would be a lie.
        let cli = stranger().on_uninstall(|_| Err("cleanup broke".into()));

        assert_eq!(run(cli, &["my-mcp", "uninstall"]), failure());
    }

    #[test]
    fn install_and_uninstall_without_hooks_are_what_they_always_were() {
        assert_eq!(run(stranger(), &["my-mcp", "uninstall"]), success());
        assert_eq!(
            run(cli(), &["my-mcp", "install", "--client", "not-a-client"]),
            usage()
        );
    }

    //
    // --client parsing
    //

    #[test]
    fn no_client_flag_selects_nothing_in_particular() {
        assert_eq!(client_selectors(&[]).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn a_client_flag_takes_the_next_argument() {
        let args = ["--client".to_string(), "cursor".to_string()];

        assert_eq!(client_selectors(&args).unwrap(), ["cursor"]);
    }

    #[test]
    fn a_client_flag_also_takes_an_equals_sign() {
        let args = ["--client=cursor".to_string()];

        assert_eq!(client_selectors(&args).unwrap(), ["cursor"]);
    }

    #[test]
    fn a_client_flag_can_be_repeated() {
        let args = [
            "--client".to_string(),
            "cursor".to_string(),
            "--client=codex".to_string(),
        ];

        assert_eq!(client_selectors(&args).unwrap(), ["cursor", "codex"]);
    }

    #[test]
    fn a_client_flag_with_nothing_after_it_is_an_error() {
        assert!(client_selectors(&["--client".to_string()]).is_err());
        assert!(client_selectors(&["--client=".to_string()]).is_err());
    }

    #[test]
    fn an_argument_that_is_not_a_client_flag_is_an_error() {
        let error = client_selectors(&["--force".to_string()]).unwrap_err();

        assert!(error.contains("--force"), "{error}");
    }

    //
    // Odds and ends
    //

    #[test]
    fn a_column_is_four_wider_than_its_longest_entry() {
        assert_eq!(column_width([7, 9, 5].into_iter()), 13);
        assert_eq!(column_width(std::iter::empty()), 4);
    }

    #[test]
    fn a_program_name_is_the_file_name_without_its_extension() {
        assert_eq!(file_stem("/usr/local/bin/my-mcp").unwrap(), "my-mcp");
        assert_eq!(file_stem("my-mcp").unwrap(), "my-mcp");
        assert_eq!(file_stem("./target/debug/my-mcp.exe").unwrap(), "my-mcp");
        assert_eq!(file_stem(""), None);
    }

    #[test]
    fn a_cli_says_what_it_answers_to() {
        let debug = format!("{:?}", cli().on_serve(|_| Ok(())));

        assert!(debug.contains("my-mcp"), "{debug}");
        assert!(debug.contains("serve"), "{debug}");
    }

    /// The names of every command a CLI lists, in order.
    fn command_names(cli: &Cli) -> Vec<&'static str> {
        cli.command_list()
            .iter()
            .map(|command| command.name)
            .collect()
    }
}
