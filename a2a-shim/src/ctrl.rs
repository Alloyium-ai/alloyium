//! CTRL lane wire messages.

use serde::{Deserialize, Serialize};

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
        #[serde(rename = "toolOnly", default, skip_serializing_if = "is_false")]
        tool_only: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        caps: Vec<String>,
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
