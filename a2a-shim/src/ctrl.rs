//! CTRL lane wire messages.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtrlProtoRange {
    pub min: u32,
    pub max: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtrlAppImage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtrlApp {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<CtrlAppImage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtrlProto {
    pub protocol_version: String,
    pub a2a: CtrlProtoRange,
    pub features: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app: Option<CtrlApp>,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t")]
pub enum Ctrl {
    #[serde(rename = "hello")]
    Hello {
        v: u32,
        #[serde(rename = "agentId")]
        agent_id: String,
        host: String,
        pid: u32,
        #[serde(rename = "subsKey")]
        subs_key: String,
        #[serde(
            rename = "deploymentId",
            default,
            skip_serializing_if = "Option::is_none"
        )]
        deployment_id: Option<String>,
        #[serde(rename = "busId", default, skip_serializing_if = "Option::is_none")]
        bus_id: Option<String>,
        #[serde(rename = "hostId", default, skip_serializing_if = "Option::is_none")]
        host_id: Option<String>,
        #[serde(rename = "toolOnly", default, skip_serializing_if = "is_false")]
        tool_only: bool,
        #[serde(
            rename = "inboxDbPath",
            default,
            skip_serializing_if = "Option::is_none"
        )]
        inbox_db_path: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        caps: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        proto: Option<CtrlProto>,
    },

    #[serde(rename = "challenge")]
    Challenge { nonce: String },

    #[serde(rename = "auth")]
    Auth { alg: String, sig: String },

    #[serde(rename = "ok")]
    Ok { session: String, epoch: u32 },

    #[serde(rename = "err")]
    Err { code: String },

    #[serde(rename = "sign")]
    Sign {
        #[serde(rename = "reqId")]
        req_id: u32,
        canon: String,
    },

    #[serde(rename = "sig")]
    Sig {
        #[serde(rename = "reqId")]
        req_id: u32,
        sig: String,
    },

    #[serde(rename = "ping")]
    Ping { ts: Option<i64> },

    #[serde(rename = "pong")]
    Pong { ts: Option<i64> },

    #[serde(rename = "delivered")]
    Delivered {
        #[serde(rename = "notifId")]
        notif_id: String,
        epoch: u32,
        status: String,
    },
}

fn is_false(v: &bool) -> bool {
    !*v
}

impl Ctrl {
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn from_json(b: &[u8]) -> Result<Ctrl, serde_json::Error> {
        serde_json::from_slice(b)
    }
}
