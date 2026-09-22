//! Authenticated connector dispatch must not fall through to raw TCP.
use super::*;
use std::{cell::Cell, rc::Rc};

#[test]
fn connector_failure_is_final_for_serve_and_health() {
    for command in ["serve", "health"] {
        let calls = Rc::new(Cell::new(0));
        let captured = calls.clone();
        let status = Cli::new(ServerEntry::new("test", &["serve"]))
            .on_serve(|_| panic!("remote serve reached local handler"))
            .on_health(|_| panic!("remote health reached local handler"))
            .remote_connector(move |address| {
                assert_eq!(address, "127.0.0.1:1");
                captured.set(captured.get() + 1);
                Err(crate::McpError::Internal("certificate rejected".into()))
            })
            .run_from(["test", command, "--connect", "127.0.0.1:1"]);
        assert_eq!(status, ExitCode::FAILURE);
        assert_eq!(calls.get(), 1);
    }
}

#[test]
fn malformed_and_conflicting_remote_arguments_do_not_dial() {
    for args in [
        vec!["test", "serve", "--connect"],
        vec!["test", "serve", "--connect", "bad"],
        vec!["test", "serve", "--connect", "a:1", "--connect", "b:2"],
        vec!["test", "serve", "--connect", "a:1", "--foreground"],
        vec!["test", "serve", "--connect", "a:1", "--daemon"],
        vec!["test", "serve", "--connect", "a:1", "--socket=/tmp/no"],
    ] {
        let status = Cli::new(ServerEntry::new("test", &["serve"]))
            .on_serve(|_| panic!("invalid remote arguments reached local handler"))
            .remote_connector(|_| panic!("invalid arguments dialed"))
            .run_from(args);
        assert_eq!(status, ExitCode::from(USAGE_EXIT));
    }
}

#[test]
fn configured_connector_does_not_change_local_dispatch() {
    for command in ["serve", "health"] {
        let status = Cli::new(ServerEntry::new("test", &["serve"]))
            .on_serve(|_| Ok(()))
            .on_health(|_| Ok(()))
            .remote_connector(|_| panic!("local command dialed remote"))
            .run_from(["test", command]);
        assert_eq!(status, ExitCode::SUCCESS);
    }
}

#[test]
fn remote_health_rejects_transport_without_deadlines() {
    let status = Cli::new(ServerEntry::new("test", &["serve"]))
        .on_health(|_| panic!("remote probe reached local handler"))
        .remote_connector(|_| {
            Ok(Box::new(crate::StreamTransport::new(std::io::Cursor::new(
                Vec::new(),
            ))))
        })
        .run_from(["test", "health", "--connect", "a:1"]);
    assert_eq!(status, ExitCode::FAILURE);
}
