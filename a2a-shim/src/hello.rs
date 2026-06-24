//! Section A.3 pre-steady-state handshake.

use tokio::net::UnixStream;
use tokio::time::timeout;

use crate::ctrl::Ctrl;
use crate::framing::{read_frame, write_frame, Frame, FrameType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ok {
    pub session: String,
    pub epoch: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum HelloError {
    #[error("handshake timed out")]
    Timeout,

    #[error("handshake rejected: {0}")]
    Rejected(String),

    #[error("handshake I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("handshake protocol error: {0}")]
    Proto(String),
}

pub async fn run_handshake(
    stream: &mut UnixStream,
    cfg: &crate::config::Config,
) -> Result<Ok, HelloError> {
    let hello = Ctrl::Hello {
        v: 1,
        agent_id: cfg.agent_id.clone(),
        host: hostname(),
        pid: std::process::id(),
        subs_key: cfg.subs_key.clone(),
        tool_only: cfg.tool_only,
        caps: vec![String::from("delivered")],
    };
    let hello_payload = hello.to_json().map_err(proto_error)?;
    write_frame(stream, FrameType::Ctrl, &hello_payload).await?;

    let exchange = async {
        let challenge = ctrl_from_frame(read_frame(stream).await?)?;
        let nonce = match challenge {
            Ctrl::Challenge { nonce } => nonce,
            Ctrl::Err { code } => return Err(HelloError::Rejected(code)),
            other => return Err(unexpected_ctrl(&other)),
        };

        let sig = crate::signer::sign_pop(&nonce).map_err(proto_error)?;
        let auth = Ctrl::Auth {
            alg: String::from("ed25519"),
            sig,
        };
        let auth_payload = auth.to_json().map_err(proto_error)?;
        write_frame(stream, FrameType::Ctrl, &auth_payload).await?;

        let ok = ctrl_from_frame(read_frame(stream).await?)?;
        match ok {
            Ctrl::Ok { session, epoch } => std::result::Result::Ok(Ok { session, epoch }),
            Ctrl::Err { code } => Err(HelloError::Rejected(code)),
            other => Err(unexpected_ctrl(&other)),
        }
    };

    match timeout(cfg.hello_timeout(), exchange).await {
        std::result::Result::Ok(result) => result,
        Err(_) => Err(HelloError::Timeout),
    }
}

fn ctrl_from_frame(frame: Frame) -> Result<Ctrl, HelloError> {
    if frame.frame_type != FrameType::Ctrl {
        return Err(HelloError::Proto(String::from("expected ctrl frame")));
    }

    Ctrl::from_json(&frame.payload).map_err(proto_error)
}

fn hostname() -> String {
    match std::env::var("HOSTNAME") {
        Ok(host) if !host.is_empty() => host,
        _ => String::from("unknown"),
    }
}

fn proto_error(error: impl std::fmt::Display) -> HelloError {
    HelloError::Proto(error.to_string())
}

fn unexpected_ctrl(ctrl: &Ctrl) -> HelloError {
    HelloError::Proto(format!("unexpected ctrl: {:?}", ctrl))
}
