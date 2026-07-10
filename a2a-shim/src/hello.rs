//! Section A.3 pre-steady-state handshake.

use tokio::net::UnixStream;
use tokio::time::timeout;

use crate::ctrl::{Ctrl, CtrlApp, CtrlAppImage, CtrlProto, CtrlProtoRange};
use crate::framing::{read_frame, write_frame, Frame, FrameType};

const DEFAULT_A2A_PROTOCOL_VERSION: &str = "1.0.11";
const DEFAULT_A2A_APP_NAME: &str = "alloyium";
const DEFAULT_A2A_APP_VERSION: &str = "0.1.0";

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
        deployment_id: cfg.deployment_id.clone(),
        bus_id: cfg.bus_id.clone(),
        host_id: cfg.host_id.clone(),
        tool_only: cfg.tool_only,
        inbox_db_path: cfg.inbox_db_path.clone().filter(|_| cfg.tool_only),
        caps: vec![String::from("delivered")],
        proto: Some(CtrlProto {
            protocol_version: protocol_version(),
            a2a: CtrlProtoRange { min: 1, max: 1 },
            features: vec![
                String::from("shim.delivered.v1"),
                String::from("shim.tool-inbox-db.v1"),
                String::from("a2a.app.version.v1"),
                String::from("mcp.a2a.tools.v1"),
                String::from("mcp.taskboard.read.v1"),
                String::from("mcp.taskboard.lifecycle.v1"),
                String::from("mcp.taskboard.planning.v1"),
            ],
            app: Some(app_metadata()),
        }),
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

fn protocol_version() -> String {
    std::env::var("A2A_PROTOCOL_VERSION")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| String::from(DEFAULT_A2A_PROTOCOL_VERSION))
}

fn env_string(names: &[&str], max: usize) -> Option<String> {
    for name in names {
        if let Ok(value) = std::env::var(name) {
            let trimmed = value.trim();
            if !trimmed.is_empty() && trimmed.len() <= max {
                return Some(String::from(trimmed));
            }
        }
    }
    None
}

fn app_metadata() -> CtrlApp {
    let image = {
        let image = CtrlAppImage {
            name: env_string(
                &["A2A_IMAGE_NAME", "A2A_CONTAINER_IMAGE", "IMAGE_NAME"],
                256,
            ),
            tag: env_string(&["A2A_IMAGE_TAG", "CC_IMAGE_TAG", "IMAGE_TAG"], 128),
            digest: env_string(&["A2A_IMAGE_DIGEST", "IMAGE_DIGEST"], 256),
            id: env_string(&["A2A_IMAGE_ID", "IMAGE_ID"], 256),
        };
        if image.name.is_some()
            || image.tag.is_some()
            || image.digest.is_some()
            || image.id.is_some()
        {
            Some(image)
        } else {
            None
        }
    };
    CtrlApp {
        name: env_string(&["A2A_APP_NAME", "npm_package_name"], 64)
            .unwrap_or_else(|| String::from(DEFAULT_A2A_APP_NAME)),
        version: env_string(&["A2A_APP_VERSION", "npm_package_version"], 128)
            .unwrap_or_else(|| String::from(DEFAULT_A2A_APP_VERSION)),
        revision: env_string(
            &[
                "A2A_APP_REVISION",
                "A2A_GIT_SHA",
                "SOURCE_REVISION",
                "GIT_SHA",
            ],
            128,
        ),
        build_id: env_string(&["A2A_BUILD_ID", "BUILD_ID"], 128),
        image,
    }
}

fn proto_error(error: impl std::fmt::Display) -> HelloError {
    HelloError::Proto(error.to_string())
}

fn unexpected_ctrl(ctrl: &Ctrl) -> HelloError {
    HelloError::Proto(format!("unexpected ctrl: {:?}", ctrl))
}
