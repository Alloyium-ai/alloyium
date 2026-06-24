#![forbid(unsafe_code)]

use std::{
    env, fs,
    path::PathBuf,
    process,
    sync::{Mutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use a2a_shim::{
    app::run_with_io,
    config::Config,
    ctrl::Ctrl,
    framing::{read_frame, write_frame, Frame, FrameType},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde_json::{json, Value};
use tokio::{
    io::{duplex, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream},
    net::{UnixListener, UnixStream},
    task::JoinHandle,
    time::{sleep, timeout},
};

const IO_CAP: usize = 256 * 1024;
const WAIT: Duration = Duration::from_secs(2);
// Reconnect budget. The fast-heartbeat scenarios deliberately induce a ~2s
// hung-core detection (ping 1000ms × MISSED_PONG_LIMIT 2), so the accept/
// reconnect wait must sit comfortably above that under full-suite CPU load
// (~4× margin) — a 2s accept deadline was a photo-finish with the 2s detection.
const RECONNECT_WAIT: Duration = Duration::from_secs(8);
const TEST_SEED: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

struct Harness {
    listener: UnixListener,
    claude_stdin: DuplexStream,
    claude_stdout: DuplexStream,
    shim_task: JoinHandle<Result<(), String>>,
}

impl Harness {
    async fn start(name: &str) -> Self {
        Self::start_with_heartbeat(name, false).await
    }

    async fn start_fast_heartbeat(name: &str) -> Self {
        Self::start_with_heartbeat(name, true).await
    }

    async fn start_with_heartbeat(name: &str, fast_heartbeat: bool) -> Self {
        Self::start_with_options(name, fast_heartbeat, IO_CAP).await
    }

    async fn start_with_stdout_cap(name: &str, stdout_cap: usize) -> Self {
        Self::start_with_options(name, false, stdout_cap).await
    }

    async fn start_with_options(name: &str, fast_heartbeat: bool, stdout_cap: usize) -> Self {
        let dir = unique_dir(name);
        fs::create_dir_all(&dir).unwrap();

        let seed_path = dir.join("agent.seed");
        fs::write(&seed_path, TEST_SEED).unwrap();

        let sock_path = dir.join("core.sock");
        let sock = sock_path.to_string_lossy().to_string();

        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        env::set_var("A2A_AGENT_ID", "scenario-agent");
        env::set_var("A2A_SIGNING_KEY", seed_path.to_string_lossy().to_string());
        env::set_var("A2A_SIG_ALG", "ed25519");
        env::set_var("A2A_CORE_SOCK", &sock);
        env::set_var("SUBS_KEY", "scenario-subs");
        env::set_var("A2A_REQUEST_TIMEOUT_MS", "250");
        env::set_var("A2A_CORE_REQUEST_TIMEOUT_MS", "250");
        env::set_var("A2A_MCP_REQUEST_TIMEOUT_MS", "250");
        env::set_var("A2A_PUB_TIMEOUT_MS", "250");
        env::set_var("A2A_HELLO_TIMEOUT_MS", "250");
        env::set_var("A2A_CONNECT_TIMEOUT_MS", "150");
        env::set_var("A2A_RECONNECT_MIN_MS", "20");
        env::set_var("A2A_RECONNECT_MAX_MS", "80");

        // scenario_B1 drives the hung-core→reconnect path via missed pongs.
        // 1000ms (→ ~2s liveness with MISSED_PONG_LIMIT=2) detects the hung core
        // well within RECONNECT_WAIT (5s) while leaving wide margin over the
        // sequential mock's fixed ~250ms claude-side assert gaps, so the healthy
        // reconnected core is never spuriously closed. Steady-state default
        // stays 10000ms (effectively off for the other scenarios).
        let heartbeat_ms = if fast_heartbeat { "1000" } else { "10000" };
        env::set_var("A2A_PING_INTERVAL_MS", heartbeat_ms);
        env::set_var("A2A_PING_MS", heartbeat_ms);
        env::set_var("A2A_HEARTBEAT_MS", heartbeat_ms);
        env::set_var("A2A_PONG_TIMEOUT_MS", heartbeat_ms);
        env::set_var("A2A_PING_TIMEOUT_MS", heartbeat_ms);

        let cfg = Config::from_env().unwrap();
        drop(_guard);

        let listener = UnixListener::bind(&sock_path).unwrap();

        let (claude_stdin, shim_stdin) = duplex(IO_CAP);
        let (shim_stdout, claude_stdout) = duplex(stdout_cap);

        let sock_for_task = sock.clone();
        let shim_task = tokio::spawn(async move {
            run_with_io(cfg, shim_stdin, shim_stdout, &sock_for_task)
                .await
                .map_err(|err| err.to_string())
        });

        Self {
            listener,
            claude_stdin,
            claude_stdout,
            shim_task,
        }
    }

    async fn stop(self) {
        self.shim_task.abort();
        let _ = self.shim_task.await;
    }
}

#[tokio::test]
async fn scenario_6_sign_during_tools_call_no_deadlock() {
    let mut h = Harness::start("scenario-6").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x11).await;

    let call = json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {
            "name": "a2a_send",
            "arguments": { "body": "hello" }
        }
    });
    write_lsp_json(&mut h.claude_stdin, &call).await;

    let frame = read_mcp_method(&mut core, "tools/call").await;
    assert_eq!(frame_json(&frame)["id"], json!(7));

    write_ctrl(
        &mut core,
        Ctrl::Sign {
            req_id: 42,
            canon: "test-canon".to_string(),
        },
    )
    .await;

    let sig_json = read_ctrl_t(&mut core, "sig").await;
    assert_eq!(
        sig_json
            .get("reqId")
            .or_else(|| sig_json.get("req_id"))
            .and_then(Value::as_u64),
        Some(42)
    );

    let sig = sig_json
        .get("sig")
        .and_then(Value::as_str)
        .expect("sig field");
    verify_signature(b"test-canon", sig);

    let result = json!({
        "jsonrpc": "2.0",
        "id": 7,
        "result": {
            "content": [{ "type": "text", "text": "done" }]
        }
    });
    write_mcp_json(&mut core, &result).await;

    let stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(stdout["id"], json!(7));
    assert_eq!(stdout["result"]["content"][0]["text"], json!("done"));

    h.stop().await;
}

#[tokio::test]
async fn scenario_7_reconnect_no_duplicate_initialize() {
    let mut h = Harness::start("scenario-7").await;
    let (mut core1, _) = accept_handshake(&h.listener, 1, 0x21).await;

    let init = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": { "protocolVersion": "2025-06-18" }
    });
    write_lsp_json(&mut h.claude_stdin, &init).await;

    let init_frame = read_mcp_method(&mut core1, "initialize").await;
    assert_eq!(frame_json(&init_frame)["id"], json!(1));

    write_mcp_json(
        &mut core1,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "serverInfo": { "name": "mock-core", "version": "1" }
            }
        }),
    )
    .await;

    let first = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(first["id"], json!(1));
    assert_eq!(first["result"]["serverInfo"]["name"], json!("mock-core"));

    drop(core1);

    let (mut core2, _) = accept_handshake(&h.listener, 2, 0x22).await;
    let reinit_frame = read_mcp_method(&mut core2, "initialize").await;
    assert_eq!(frame_json(&reinit_frame)["id"], json!(1));

    write_mcp_json(
        &mut core2,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "serverInfo": { "name": "mock-core-second", "version": "2" }
            }
        }),
    )
    .await;

    assert_no_lsp(&mut h.claude_stdout, Duration::from_millis(400)).await;

    h.stop().await;
}

#[tokio::test]
async fn scenario_codex_json_line_stdio_initialize() {
    let mut h = Harness::start("scenario-codex-json-line").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x27).await;

    let init = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": { "protocolVersion": "2025-06-18" }
    });
    write_json_line(&mut h.claude_stdin, &init).await;

    let init_frame = read_mcp_method(&mut core, "initialize").await;
    assert_eq!(frame_json(&init_frame)["id"], json!(1));

    write_mcp_json(
        &mut core,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "serverInfo": { "name": "mock-core", "version": "1" }
            }
        }),
    )
    .await;

    let stdout = read_json_line(&mut h.claude_stdout).await;
    assert_eq!(stdout["id"], json!(1));
    assert_eq!(stdout["result"]["serverInfo"]["name"], json!("mock-core"));

    h.stop().await;
}

#[tokio::test]
async fn scenario_10_notification_is_buffered_until_claude_initialized() {
    let mut h = Harness::start("scenario-10").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x31).await;

    let early = json!({
        "jsonrpc": "2.0",
        "method": "notifications/claude/channel",
        "params": {
            "channel": "test",
            "body": "buffered before init"
        }
    });
    write_mcp_json(&mut core, &early).await;
    assert_no_lsp(&mut h.claude_stdout, Duration::from_millis(250)).await;

    complete_initialize(&mut h, &mut core, 1).await;

    let flushed = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(flushed["method"], json!("notifications/claude/channel"));
    assert_eq!(flushed["params"]["body"], json!("buffered before init"));

    let late = json!({
        "jsonrpc": "2.0",
        "method": "notifications/claude/channel",
        "params": {
            "channel": "test",
            "body": "after init"
        }
    });
    write_mcp_json(&mut core, &late).await;

    let stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(stdout["method"], json!("notifications/claude/channel"));
    assert_eq!(stdout["params"]["body"], json!("after init"));

    h.stop().await;
}

#[tokio::test]
async fn scenario_10b_direct_notification_waits_for_stdout_before_delivered() {
    let mut h = Harness::start("scenario-10b").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x33).await;

    let early = json!({
        "jsonrpc": "2.0",
        "method": "notifications/claude/channel",
        "params": {
            "body": "durable buffered",
            "meta": { "kind": "direct", "id": "n-buffer", "notifId": "n-buffer" }
        }
    });
    write_mcp_json(&mut core, &early).await;

    assert_no_frame(&mut core, Duration::from_millis(250)).await;
    assert_no_lsp(&mut h.claude_stdout, Duration::from_millis(250)).await;

    complete_initialize(&mut h, &mut core, 1).await;

    let flushed = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(flushed["params"]["body"], json!("durable buffered"));

    assert_no_frame(&mut core, Duration::from_millis(250)).await;

    write_mcp_json(&mut core, &early).await;

    let delivered = read_ctrl_t(&mut core, "delivered").await;
    assert_eq!(delivered["notifId"], json!("n-buffer"));
    assert_eq!(delivered["epoch"], json!(1));
    assert_eq!(delivered["status"], json!("ok"));
    assert_no_lsp(&mut h.claude_stdout, Duration::from_millis(250)).await;

    h.stop().await;
}

#[tokio::test]
async fn scenario_10c_duplicate_notif_id_sends_delivered_without_double_write() {
    let mut h = Harness::start("scenario-10c").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x34).await;
    complete_initialize(&mut h, &mut core, 1).await;

    let notif = json!({
        "jsonrpc": "2.0",
        "method": "notifications/claude/channel",
        "params": {
            "body": "only once",
            "meta": { "kind": "direct", "id": "n-dup", "notifId": "n-dup" }
        }
    });

    write_mcp_json(&mut core, &notif).await;
    let first = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(first["params"]["body"], json!("only once"));
    assert_eq!(read_ctrl_t(&mut core, "delivered").await["notifId"], json!("n-dup"));

    write_mcp_json(&mut core, &notif).await;
    assert_eq!(read_ctrl_t(&mut core, "delivered").await["notifId"], json!("n-dup"));
    assert_no_lsp(&mut h.claude_stdout, Duration::from_millis(250)).await;

    h.stop().await;
}

#[tokio::test]
async fn scenario_10d_topic_notif_id_does_not_poison_direct_dedupe() {
    let mut h = Harness::start("scenario-10d").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x35).await;
    complete_initialize(&mut h, &mut core, 1).await;

    let topic = json!({
        "jsonrpc": "2.0",
        "method": "notifications/claude/channel",
        "params": {
            "body": "topic poison attempt",
            "meta": { "kind": "topic", "id": "topic-1", "notifId": "same-id" }
        }
    });
    write_mcp_json(&mut core, &topic).await;

    let topic_stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(topic_stdout["params"]["body"], json!("topic poison attempt"));
    assert_no_frame(&mut core, Duration::from_millis(250)).await;

    let direct = json!({
        "jsonrpc": "2.0",
        "method": "notifications/claude/channel",
        "params": {
            "body": "direct must survive",
            "meta": { "kind": "direct", "id": "same-id", "notifId": "same-id" }
        }
    });
    write_mcp_json(&mut core, &direct).await;

    let direct_stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(direct_stdout["params"]["body"], json!("direct must survive"));
    assert_eq!(read_ctrl_t(&mut core, "delivered").await["notifId"], json!("same-id"));

    h.stop().await;
}

#[tokio::test]
async fn scenario_inbound_notifications_backpressure_no_loss() {
    const N: usize = 400;
    let mut h = Harness::start_with_stdout_cap("scenario-inbound-backpressure", 512).await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x32).await;
    complete_initialize(&mut h, &mut core, 1).await;

    let sender = tokio::spawn(async move {
        for seq in 0..N {
            let notif = json!({
                "jsonrpc": "2.0",
                "method": "notifications/claude/channel",
                "params": {
                    "seq": seq,
                    "body": format!("evt-{seq}")
                }
            });
            let payload = serde_json::to_vec(&notif).unwrap();
            timeout(
                Duration::from_secs(10),
                write_frame(&mut core, FrameType::Mcp, &payload),
            )
            .await
            .expect("timed out writing inbound notification")
            .expect("failed writing inbound notification");
        }
        core
    });

    // Let the tiny Claude stdout pipe fill first. The shim must backpressure the
    // UDS reader instead of dropping already-accepted notifications.
    sleep(Duration::from_millis(150)).await;

    let mut received = 0_usize;
    timeout(Duration::from_secs(15), async {
        while received < N {
            let value = read_lsp_json(&mut h.claude_stdout).await;
            if value["method"] != json!("notifications/claude/channel") {
                continue;
            }
            assert_eq!(
                value["params"]["seq"],
                json!(received),
                "notifications must remain ordered"
            );
            received += 1;
        }
    })
    .await
    .expect("timed out draining notifications from shim stdout");

    let core = sender.await.expect("sender task panicked");
    assert_eq!(
        received, N,
        "every accepted inbound notification must reach Claude"
    );

    drop(core);
    h.stop().await;
}

#[tokio::test]
async fn scenario_11_reconnect_rehello_same_agent_and_valid_pop() {
    let h = Harness::start("scenario-11").await;
    let (core1, first) = accept_handshake(&h.listener, 1, 0x41).await;
    drop(core1);

    let (core2, second) = accept_handshake(&h.listener, 2, 0x42).await;
    drop(core2);

    assert_eq!(first.agent_id, "scenario-agent");
    assert_eq!(second.agent_id, "scenario-agent");

    h.stop().await;
}

#[tokio::test]
async fn scenario_b1_hung_core_reconnect_no_hang_no_leak() {
    let mut h = Harness::start_fast_heartbeat("scenario-b1").await;
    let (mut core1, _) = accept_handshake(&h.listener, 1, 0x51).await;

    complete_initialize(&mut h, &mut core1, 1).await;

    let (mut core2, _) = timeout(RECONNECT_WAIT, accept_handshake(&h.listener, 2, 0x52))
        .await
        .expect("shim did not reconnect after missed pongs");

    let reinit_frame = read_mcp_method(&mut core2, "initialize").await;
    let reinit_id = frame_json(&reinit_frame)["id"].clone();
    write_mcp_json(
        &mut core2,
        &json!({
            "jsonrpc": "2.0",
            "id": reinit_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "serverInfo": { "name": "mock-core-recovered", "version": "2" }
            }
        }),
    )
    .await;

    // §A.6: the shim replays notifications/initialized to the new core after the
    // re-init result — consume it before the next core read.
    read_mcp_method(&mut core2, "notifications/initialized").await;

    assert_no_lsp(&mut h.claude_stdout, Duration::from_millis(250)).await;

    let list = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    });
    write_lsp_json(&mut h.claude_stdin, &list).await;

    let list_frame = read_mcp_method(&mut core2, "tools/list").await;
    assert_eq!(frame_json(&list_frame)["id"], json!(2));

    write_mcp_json(
        &mut core2,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": { "tools": [] }
        }),
    )
    .await;

    let stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(stdout["id"], json!(2));
    assert_eq!(stdout["result"]["tools"], json!([]));

    drop(core1);
    h.stop().await;
}

#[tokio::test]
async fn scenario_b2_timeout_then_late_reply_dropped() {
    let mut h = Harness::start("scenario-b2").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x61).await;

    complete_initialize(&mut h, &mut core, 1).await;

    let call = json!({
        "jsonrpc": "2.0",
        "id": 70,
        "method": "tools/call",
        "params": {
            "name": "a2a_send",
            "arguments": { "body": "expire this request" }
        }
    });
    write_lsp_json(&mut h.claude_stdin, &call).await;

    let call_frame = read_mcp_method(&mut core, "tools/call").await;
    assert_eq!(frame_json(&call_frame)["id"], json!(70));

    let stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_core_timeout_error(&stdout, 70);

    write_mcp_json(
        &mut core,
        &json!({
            "jsonrpc": "2.0",
            "id": 70,
            "result": {
                "content": [{ "type": "text", "text": "late real reply" }]
            }
        }),
    )
    .await;

    assert_no_lsp(&mut h.claude_stdout, Duration::from_millis(400)).await;

    h.stop().await;
}

// --- Fold-4 catching tests (the coverage holes that hid the re-gate findings) ---

#[tokio::test]
async fn scenario_large_mcp_request_is_one_contiguous_frame() {
    // §A-v3 (A): a >256KiB (MCP_WRITE_CHUNK) MCP request must reach the core as
    // ONE contiguous frame — NOT split into multiple standalone MCP frames (a
    // contiguous-length receiver would decode the chunks as partial-JSON).
    let mut h = Harness::start("scenario-large-mcp").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x31).await;
    complete_initialize(&mut h, &mut core, 1).await;

    let blob = "x".repeat(300 * 1024);
    let req = json!({
        "jsonrpc": "2.0",
        "id": 42,
        "method": "tools/call",
        "params": { "name": "big", "arguments": { "blob": blob } }
    });
    let expected = serde_json::to_vec(&req).unwrap();
    assert!(
        expected.len() > 256 * 1024,
        "test body must exceed one write chunk"
    );

    // The body exceeds the duplex capacity, so drive claude->shim from a task
    // while we read the core side (avoids a sequential write/read deadlock).
    let Harness {
        claude_stdin,
        claude_stdout: _claude_stdout,
        shim_task,
        listener: _listener,
    } = h;
    let writer = tokio::spawn(async move {
        let mut claude_stdin = claude_stdin;
        write_lsp_json(&mut claude_stdin, &req).await;
        claude_stdin
    });

    let frame = timeout(Duration::from_secs(5), read_frame(&mut core))
        .await
        .expect("timed out reading large MCP frame")
        .expect("read_frame error on large MCP");
    assert_eq!(frame.frame_type, FrameType::Mcp);
    assert_eq!(
        frame.payload, expected,
        "a >256KiB MCP request must arrive as ONE contiguous frame"
    );

    let _ = writer.await;
    shim_task.abort();
    let _ = shim_task.await;
}

#[tokio::test]
async fn scenario_init_no_second_response_on_unanswered_reinit() {
    // §A.6: claude's initialize gets EXACTLY ONE final response. After a success,
    // a reconnect whose replayed initialize is never answered must NOT emit a
    // second response (a -32001) for the same id.
    let mut h = Harness::start_fast_heartbeat("scenario-init-dup").await;
    let (mut core1, _) = accept_handshake(&h.listener, 1, 0x71).await;
    complete_initialize(&mut h, &mut core1, 1).await; // claude gets the ONE success

    // core1 hangs → reconnect; accept core2 but never answer the replayed init.
    let (mut core2, _) = timeout(RECONNECT_WAIT, accept_handshake(&h.listener, 2, 0x72))
        .await
        .expect("shim did not reconnect after missed pongs");
    let reinit = read_mcp_method(&mut core2, "initialize").await;
    assert_eq!(frame_json(&reinit)["id"], json!(1));

    // The replayed init is not re-tracked once answered → it cannot time out into
    // a second claude response. Wait past the (250ms) request budget to prove it.
    assert_no_lsp(&mut h.claude_stdout, Duration::from_millis(600)).await;

    drop(core1);
    h.stop().await;
}

#[tokio::test]
async fn scenario_cross_epoch_late_reply_dropped() {
    // A reply for a request tracked under a PRE-reconnect epoch must be dropped
    // (tombstone matched across epochs), not forwarded as a second response.
    let mut h = Harness::start_fast_heartbeat("scenario-cross-epoch").await;
    let (mut core1, _) = accept_handshake(&h.listener, 1, 0x81).await;
    complete_initialize(&mut h, &mut core1, 1).await;

    let call = json!({
        "jsonrpc": "2.0",
        "id": 55,
        "method": "tools/call",
        "params": { "name": "slow", "arguments": {} }
    });
    write_lsp_json(&mut h.claude_stdin, &call).await;
    let call_frame = read_mcp_method(&mut core1, "tools/call").await;
    assert_eq!(frame_json(&call_frame)["id"], json!(55));

    // core1 hangs → reconnect to epoch 2. id=55 is pre-epoch: failed (one
    // -32001 to claude) and tombstoned under epoch 1.
    let (mut core2, _) = timeout(RECONNECT_WAIT, accept_handshake(&h.listener, 2, 0x82))
        .await
        .expect("shim did not reconnect after missed pongs");
    let reinit = read_mcp_method(&mut core2, "initialize").await;
    assert_eq!(frame_json(&reinit)["id"], json!(1));
    let err = read_lsp_json(&mut h.claude_stdout).await;
    assert_core_timeout_error(&err, 55);

    // A late reply for id=55 arrives on epoch 2 → must be dropped via the
    // cross-epoch tombstone match (not forwarded as a 2nd response).
    write_mcp_json(
        &mut core2,
        &json!({ "jsonrpc": "2.0", "id": 55, "result": { "late": true } }),
    )
    .await;
    assert_no_lsp(&mut h.claude_stdout, Duration::from_millis(400)).await;

    drop(core1);
    h.stop().await;
}

#[tokio::test]
async fn scenario_reconnect_replays_initialized_for_b4_flush() {
    // §A.6 (amended @6ae189c): after the re-init RESULT on reconnect, the shim
    // MUST replay claude's `notifications/initialized` to the new core — the
    // acceptor's B4 gate buffers OUTBOUND notifications until the per-connection
    // session sees `initialized`, so without this the agent goes deaf to inbound
    // bus/a2a events after any UDS reconnect.
    let mut h = Harness::start_fast_heartbeat("scenario-reinit-initialized").await;
    let (mut core1, _) = accept_handshake(&h.listener, 1, 0x91).await;
    complete_initialize(&mut h, &mut core1, 1).await; // shim caches claude's initialized

    // core1 hangs → reconnect to core2; answer the replayed initialize.
    let (mut core2, _) = timeout(RECONNECT_WAIT, accept_handshake(&h.listener, 2, 0x92))
        .await
        .expect("shim did not reconnect after missed pongs");
    let reinit = read_mcp_method(&mut core2, "initialize").await;
    let reinit_id = frame_json(&reinit)["id"].clone();
    write_mcp_json(
        &mut core2,
        &json!({
            "jsonrpc": "2.0",
            "id": reinit_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "serverInfo": { "name": "mock-core-2", "version": "2" }
            }
        }),
    )
    .await;

    // After the re-init RESULT, the new core MUST receive notifications/initialized
    // (without the fix this read times out → the agent would be deaf post-reconnect).
    let notif2 = read_mcp_method(&mut core2, "notifications/initialized").await;
    assert_eq!(
        frame_json(&notif2)["method"],
        json!("notifications/initialized")
    );

    drop(core1);
    h.stop().await;
}

struct HelloSeen {
    agent_id: String,
}

async fn accept_handshake(
    listener: &UnixListener,
    epoch: u32,
    nonce_byte: u8,
) -> (UnixStream, HelloSeen) {
    let (mut stream, _) = timeout(RECONNECT_WAIT, listener.accept())
        .await
        .expect("timed out accepting core connection")
        .unwrap();

    let hello_frame = read_frame_timeout(&mut stream).await;
    let hello_json = assert_ctrl_json(&hello_frame);
    assert_eq!(hello_json.get("t").and_then(Value::as_str), Some("hello"));
    assert_eq!(hello_json.get("v").and_then(Value::as_u64), Some(1));
    assert!(hello_json
        .get("caps")
        .and_then(Value::as_array)
        .map(|caps| caps.iter().any(|cap| cap.as_str() == Some("delivered")))
        .unwrap_or(false));
    let agent_id = json_str(&hello_json, "agentId", "agent_id").to_string();

    let nonce = [nonce_byte; 32];
    write_ctrl(
        &mut stream,
        Ctrl::Challenge {
            nonce: STANDARD.encode(nonce),
        },
    )
    .await;

    let auth_frame = read_frame_timeout(&mut stream).await;
    let auth_json = assert_ctrl_json(&auth_frame);
    assert_eq!(auth_json.get("t").and_then(Value::as_str), Some("auth"));
    assert_eq!(
        auth_json.get("alg").and_then(Value::as_str),
        Some("ed25519")
    );

    let sig = auth_json
        .get("sig")
        .and_then(Value::as_str)
        .expect("auth sig");
    verify_signature(&nonce, sig);

    write_ctrl(
        &mut stream,
        Ctrl::Ok {
            session: format!("session-{epoch}"),
            epoch,
        },
    )
    .await;

    (stream, HelloSeen { agent_id })
}

async fn complete_initialize(h: &mut Harness, core: &mut UnixStream, id: i64) {
    let init = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": { "protocolVersion": "2025-06-18" }
    });
    write_lsp_json(&mut h.claude_stdin, &init).await;

    let init_frame = read_mcp_method(core, "initialize").await;
    assert_eq!(frame_json(&init_frame), init);

    write_mcp_json(
        core,
        &json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": { "experimental": { "claude/channel": true } },
                "serverInfo": { "name": "mock-core", "version": "1" }
            }
        }),
    )
    .await;

    let stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(stdout["id"], json!(id));
    assert_eq!(stdout["result"]["serverInfo"]["name"], json!("mock-core"));

    let initialized = json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    });
    write_lsp_json(&mut h.claude_stdin, &initialized).await;

    let initialized_frame = read_mcp_method(core, "notifications/initialized").await;
    assert_eq!(frame_json(&initialized_frame), initialized);
}

async fn read_frame_timeout(stream: &mut UnixStream) -> Frame {
    timeout(WAIT, read_frame(stream))
        .await
        .expect("timed out reading frame")
        .unwrap()
}

async fn assert_no_frame(stream: &mut UnixStream, wait: Duration) {
    match timeout(wait, read_frame(stream)).await {
        Ok(Ok(frame)) => panic!("unexpected frame: {}", frame_json(&frame)),
        Ok(Err(err)) => panic!("unexpected frame read error: {err}"),
        Err(_) => {}
    }
}

async fn read_mcp_method(stream: &mut UnixStream, method: &str) -> Frame {
    loop {
        let frame = read_frame_timeout(stream).await;
        match frame.frame_type {
            FrameType::Mcp => {
                let value = frame_json(&frame);
                assert_eq!(value["method"], json!(method));
                return frame;
            }
            FrameType::Ctrl => {
                let value = frame_json(&frame);
                if value.get("t").and_then(Value::as_str) == Some("ping") {
                    write_ctrl_json(stream, &json!({ "t": "pong" })).await;
                    continue;
                }
                panic!("expected MCP method {method}, got CTRL {value}");
            }
        }
    }
}

async fn read_ctrl_t(stream: &mut UnixStream, t: &str) -> Value {
    loop {
        let frame = read_frame_timeout(stream).await;
        match frame.frame_type {
            FrameType::Ctrl => {
                let value = frame_json(&frame);
                if value.get("t").and_then(Value::as_str) == Some("ping") {
                    write_ctrl_json(stream, &json!({ "t": "pong" })).await;
                    continue;
                }
                assert_eq!(value.get("t").and_then(Value::as_str), Some(t));
                return value;
            }
            FrameType::Mcp => panic!("expected CTRL {t}, got MCP {}", frame_json(&frame)),
        }
    }
}

async fn write_ctrl(stream: &mut UnixStream, ctrl: Ctrl) {
    let payload = ctrl.to_json().unwrap();
    timeout(WAIT, write_frame(stream, FrameType::Ctrl, &payload))
        .await
        .expect("timed out writing ctrl")
        .unwrap();
}

async fn write_ctrl_json(stream: &mut UnixStream, value: &Value) {
    let payload = serde_json::to_vec(value).unwrap();
    timeout(WAIT, write_frame(stream, FrameType::Ctrl, &payload))
        .await
        .expect("timed out writing raw ctrl")
        .unwrap();
}

async fn write_mcp_json(stream: &mut UnixStream, value: &Value) {
    let payload = serde_json::to_vec(value).unwrap();
    timeout(WAIT, write_frame(stream, FrameType::Mcp, &payload))
        .await
        .expect("timed out writing mcp")
        .unwrap();
}

fn assert_ctrl_json(frame: &Frame) -> Value {
    match frame.frame_type {
        FrameType::Ctrl => {}
        FrameType::Mcp => panic!("expected ctrl frame"),
    }

    frame_json(frame)
}

fn frame_json(frame: &Frame) -> Value {
    serde_json::from_slice(&frame.payload).unwrap()
}

async fn write_lsp_json<W>(writer: &mut W, value: &Value)
where
    W: AsyncWrite + Unpin,
{
    let body = serde_json::to_vec(value).unwrap();
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    writer.write_all(header.as_bytes()).await.unwrap();
    writer.write_all(&body).await.unwrap();
    writer.flush().await.unwrap();
}

async fn write_json_line<W>(writer: &mut W, value: &Value)
where
    W: AsyncWrite + Unpin,
{
    let body = serde_json::to_vec(value).unwrap();
    writer.write_all(&body).await.unwrap();
    writer.write_all(b"\n").await.unwrap();
    writer.flush().await.unwrap();
}

async fn read_lsp_json<R>(reader: &mut R) -> Value
where
    R: AsyncRead + Unpin,
{
    let body = timeout(WAIT, read_lsp_body(reader))
        .await
        .expect("timed out reading lsp body")
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn read_json_line<R>(reader: &mut R) -> Value
where
    R: AsyncRead + Unpin,
{
    let mut body = Vec::new();
    let mut byte = [0_u8; 1];
    timeout(WAIT, async {
        loop {
            reader.read_exact(&mut byte).await?;
            if byte[0] == b'\n' {
                return Ok::<(), std::io::Error>(());
            }
            body.push(byte[0]);
        }
    })
    .await
    .expect("timed out reading json-line body")
    .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn assert_no_lsp<R>(reader: &mut R, wait: Duration)
where
    R: AsyncRead + Unpin,
{
    match timeout(wait, read_lsp_body(reader)).await {
        Ok(Ok(body)) => panic!("unexpected lsp body: {}", String::from_utf8_lossy(&body)),
        Ok(Err(err)) => panic!("unexpected lsp read error: {err}"),
        Err(_) => {}
    }
}

async fn read_lsp_body<R>(reader: &mut R) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut header = Vec::new();
    let mut byte = [0_u8; 1];

    loop {
        reader.read_exact(&mut byte).await?;
        header.push(byte[0]);
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }

    let header = String::from_utf8(header)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    let len = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "missing content-length")
        })?;

    let mut body = vec![0_u8; len];
    reader.read_exact(&mut body).await?;
    Ok(body)
}

fn assert_core_timeout_error(value: &Value, id: i64) {
    assert_eq!(value["jsonrpc"], json!("2.0"));
    assert_eq!(value["id"], json!(id));
    assert_eq!(value["error"]["code"], json!(-32001));

    let message = value["error"]["message"].as_str().unwrap_or("");
    let data_code = value["error"]["data"]["code"].as_str().unwrap_or("");
    assert!(
        message.contains("core_timeout") || data_code == "core_timeout",
        "timeout error must carry core_timeout, got {value}"
    );
}

fn verify_signature(message: &[u8], sig_b64: &str) {
    let signing_key = SigningKey::from_bytes(&TEST_SEED);
    let verifying_key: VerifyingKey = signing_key.verifying_key();

    let sig_bytes = STANDARD.decode(sig_b64).unwrap();
    let sig_array: [u8; 64] = sig_bytes.try_into().unwrap();
    let sig = Signature::from_bytes(&sig_array);

    verifying_key.verify(message, &sig).unwrap();
}

fn json_str<'a>(value: &'a Value, camel: &str, snake: &str) -> &'a str {
    value
        .get(camel)
        .or_else(|| value.get(snake))
        .and_then(Value::as_str)
        .unwrap()
}

fn unique_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    env::temp_dir().join(format!("a2a-shim-{name}-{}-{nanos}", process::id()))
}

#[test]
fn test_seed_sanity_verifies_direct_signature() {
    let signing_key = SigningKey::from_bytes(&TEST_SEED);
    let verifying_key = signing_key.verifying_key();
    let sig = signing_key.sign(b"sanity");
    verifying_key.verify(b"sanity", &sig).unwrap();
}
