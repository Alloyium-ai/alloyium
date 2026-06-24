#![forbid(unsafe_code)]
//! `a2a-shim` binary entrypoint (thin). All modules live in the library (see `lib.rs`):
//! a thin per-agent MCP-over-UDS relay to the FROZEN §A-v3 wire contract.
use std::env;
use std::process::ExitCode;

use a2a_shim::config::Config;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();

    // §A.7 precondition probe: `a2a-shim --ping [sock]` (sock defaults to A2A_CORE_SOCK).
    if let Some(sock_arg) = probe_sock_arg(&args) {
        let sock = match sock_arg {
            Some(sock) => sock,
            None => match Config::from_env() {
                Ok(cfg) => cfg.core_sock,
                Err(err) => {
                    eprintln!("{err}");
                    return ExitCode::FAILURE;
                }
            },
        };
        return match a2a_shim::transport::ping(&sock).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("{err}");
                ExitCode::FAILURE
            }
        };
    }

    let cfg = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::FAILURE;
        }
    };

    match a2a_shim::app::run(cfg).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn probe_sock_arg(args: &[String]) -> Option<Option<String>> {
    let flag_index = args
        .iter()
        .position(|arg| arg == "--ping" || arg == "--health")?;
    let sock = args
        .iter()
        .skip(flag_index + 1)
        .find(|arg| !arg.starts_with("--"))
        .cloned();
    Some(sock)
}
