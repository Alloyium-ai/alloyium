#![forbid(unsafe_code)]

use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
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
    time::timeout,
};

const IO_CAP: usize = 256 * 1024;
const WAIT: Duration = Duration::from_secs(2);
const SHORT_WAIT: Duration = Duration::from_millis(120);
const TEST_SEED: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[tokio::test]
async fn framing_round_trip_partial_and_oversize() {
    let vectors = load_json_fixture("tests/shim-conformance/framing-vectors.json");
    assert!(
        vectors.get("frame_layout").is_some(),
        "framing vectors must describe frame_layout"
    );
    assert!(
        vectors.get("error_codes").is_some(),
        "framing vectors must describe error_codes"
    );

    let cases = vectors["cases"]
        .as_array()
        .expect("framing vectors cases[]");
    assert!(
        !cases.is_empty(),
        "framing vectors cases[] must not be empty"
    );

    let mut seen = BTreeSet::new();
    for case in cases {
        run_frame_vector_case(case, &mut seen).await;
    }

    for required in [
        "valid-ctrl",
        "valid-mcp",
        "two-frames-one-chunk",
        "partial-complete",
        "oversize-error",
        "zero-error",
        "unknown-type-error",
    ] {
        assert!(
            seen.contains(required),
            "framing-vectors.json did not exercise {required}"
        );
    }
}

#[tokio::test]
async fn hello_happy_path_and_each_error_code() {
    let h = Harness::start("conf-hello-happy").await;
    let (core, seen) = accept_handshake(&h.listener, 1, 0x11).await;
    drop(core);
    assert_eq!(seen.agent_id, "conformance-agent");
    h.stop().await;

    for code in [
        "dup_agent",
        "unknown_agent",
        "bad_sig",
        "bad_nonce",
        "bad_alg",
        "unauthorized",
        "internal",
    ] {
        let h = Harness::start(&format!("conf-hello-err-{code}")).await;
        let (mut core, _) = accept_hello_and_auth(&h.listener, 0x12).await;
        write_ctrl_json(
            &mut core,
            &json!({
                "t": "err",
                "code": code,
                "msg": format!("forced {code}")
            }),
        )
        .await;

        let done = h.wait_done(WAIT).await;
        assert!(done.is_err(), "hello err code {code} must fail closed");
    }

    let h = Harness::start("conf-hello-timeout").await;
    let (mut stream, _) = timeout(WAIT, h.listener.accept())
        .await
        .expect("timed out accepting hello-timeout connection")
        .unwrap();
    let hello = read_frame_timeout(&mut stream).await;
    assert_hello_frame(&hello);

    match timeout(WAIT, read_frame(&mut stream)).await {
        Ok(Ok(frame)) => panic!(
            "hello-timeout produced unexpected frame: {:?}",
            frame.payload
        ),
        Ok(Err(_)) => {}
        Err(_) => panic!("shim did not close or retry after hello timeout"),
    }

    h.stop().await;
}

#[tokio::test]
async fn pop_sign_known_seed_nonce_fixed_signature() {
    let h = Harness::start("conf-pop").await;
    let (mut core, auth_sig) = accept_hello_and_auth(&h.listener, 0x11).await;

    assert_eq!(
        auth_sig,
        "PzYoQB9rowQzPjFQH09PPffhWwK/93icBm3rR5lDbO1EHTbNXu2HUpid9xngQ6ShnCSGzjoKi5ICocm05rYuDQ=="
    );

    let nonce = [0x11_u8; 32];
    verify_signature(&nonce, &auth_sig);

    let nonce_b64 = STANDARD.encode(nonce);
    let signing_key = SigningKey::from_bytes(&TEST_SEED);
    let wrong_sig = STANDARD.encode(signing_key.sign(nonce_b64.as_bytes()).to_bytes());
    assert_ne!(
        auth_sig, wrong_sig,
        "PoP must sign decoded nonce bytes, not the base64 text"
    );

    write_ctrl(
        &mut core,
        Ctrl::Ok {
            session: "session-pop".to_string(),
            epoch: 1,
        },
    )
    .await;
    h.stop().await;
}

#[tokio::test]
async fn canonical_byte_vectors_match_typescript_and_rust() {
    let h = Harness::start("conf-canon").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x21).await;

    let cases = [
        (
            1_u64,
            "test-canon",
            "lHWJfwkVAi8sEgVE9WTW+FHOlKpVxHjW1gRLLawqqF19H+7DLIvtzmYd4SAw1l1n2QbSqZLrKw5sDIG6rqsdBQ==",
        ),
        (
            2,
            "edge:\nquote=\" slash=\\ unicode=\u{2603}",
            "xji5jdsaB7x6X8dv0MSoj+uECRDdpjTgE2rBKlO6gIB+ip6ZsajyybB2C5B9UCtwNcilebcFLU1KubUOovr3Bw==",
        ),
        (
            3,
            "{\"z\":0,\"a\":[true,null,\"x\"]}",
            "O2qvGahhdlfhP/luSqCFIqZ0wi9RVKnpYVsi/nFvR90+TUozdxxsV3GhkVwghRADEJpQPitMGnHVQ2hF3AWxBA==",
        ),
    ];

    for (req_id, canon, expected_sig) in cases {
        write_ctrl(
            &mut core,
            Ctrl::Sign {
                req_id: req_id as u32,
                canon: canon.to_string(),
            },
        )
        .await;
        let sig_json = read_ctrl_t(&mut core, "sig").await;
        assert_eq!(
            sig_json
                .get("reqId")
                .or_else(|| sig_json.get("req_id"))
                .and_then(Value::as_u64),
            Some(req_id as u64)
        );
        let sig = sig_json["sig"].as_str().expect("sig field");
        assert_eq!(sig, expected_sig);
        verify_signature(canon.as_bytes(), sig);
    }

    h.stop().await;
}

#[tokio::test]
async fn mcp_relay_parity_initialize_tools_and_notifications() {
    let mut h = Harness::start("conf-mcp").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x31).await;

    complete_initialize(&mut h, &mut core, 1).await;

    let tools_list = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    });
    write_lsp_json(&mut h.claude_stdin, &tools_list).await;
    let list_frame = read_mcp_method(&mut core, "tools/list").await;
    assert_eq!(frame_json(&list_frame)["id"], json!(2));

    write_mcp_json(
        &mut core,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": {
                "tools": [
                    {
                        "name": "a2a_send",
                        "description": "send",
                        "inputSchema": { "type": "object" }
                    }
                ]
            }
        }),
    )
    .await;

    let list_stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(list_stdout["id"], json!(2));
    assert_eq!(list_stdout["result"]["tools"][0]["name"], json!("a2a_send"));

    let call = json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "a2a_send",
            "arguments": { "body": "hello" }
        }
    });
    write_lsp_json(&mut h.claude_stdin, &call).await;
    let call_frame = read_mcp_method(&mut core, "tools/call").await;
    assert_eq!(frame_json(&call_frame), call);

    write_mcp_json(
        &mut core,
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "result": {
                "content": [{ "type": "text", "text": "ok" }]
            }
        }),
    )
    .await;

    let call_stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(call_stdout["id"], json!(3));
    assert_eq!(call_stdout["result"]["content"][0]["text"], json!("ok"));

    let notification = json!({
        "jsonrpc": "2.0",
        "method": "notifications/claude/channel",
        "params": { "channel": "conf", "body": "from core" }
    });
    write_mcp_json(&mut core, &notification).await;

    let notif_stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(notif_stdout, notification);

    h.stop().await;
}

#[tokio::test]
async fn request_timeout_reconnect_epoch_and_dropped_late_reply() {
    let mut h = Harness::start("conf-timeout-reconnect").await;
    let (mut core1, _) = accept_handshake(&h.listener, 1, 0x41).await;
    complete_initialize(&mut h, &mut core1, 1).await;

    let call = json!({
        "jsonrpc": "2.0",
        "id": 88,
        "method": "tools/call",
        "params": {
            "name": "a2a_send",
            "arguments": { "body": "timeout me" }
        }
    });
    write_lsp_json(&mut h.claude_stdin, &call).await;
    let call_frame = read_mcp_method(&mut core1, "tools/call").await;
    assert_eq!(frame_json(&call_frame)["id"], json!(88));

    let timeout_stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_core_timeout_error(&timeout_stdout, 88);

    write_mcp_json(
        &mut core1,
        &json!({
            "jsonrpc": "2.0",
            "id": 88,
            "result": {
                "content": [{ "type": "text", "text": "late" }]
            }
        }),
    )
    .await;
    assert_no_lsp(&mut h.claude_stdout, Duration::from_millis(400)).await;

    drop(core1);

    let (mut core2, _) = accept_handshake(&h.listener, 2, 0x42).await;
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
                "serverInfo": { "name": "mock-core-epoch-2", "version": "2" }
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
        "id": 89,
        "method": "tools/list",
        "params": {}
    });
    write_lsp_json(&mut h.claude_stdin, &list).await;
    let list_frame = read_mcp_method(&mut core2, "tools/list").await;
    assert_eq!(frame_json(&list_frame)["id"], json!(89));

    write_mcp_json(
        &mut core2,
        &json!({
            "jsonrpc": "2.0",
            "id": 89,
            "result": { "tools": [] }
        }),
    )
    .await;

    let list_stdout = read_lsp_json(&mut h.claude_stdout).await;
    assert_eq!(list_stdout["id"], json!(89));
    assert_eq!(list_stdout["result"]["tools"], json!([]));

    h.stop().await;
}

#[tokio::test]
async fn shim_signs_round_trip() {
    let h = Harness::start("conf-shim-signs").await;
    let (mut core, _) = accept_handshake(&h.listener, 1, 0x51).await;

    write_ctrl(
        &mut core,
        Ctrl::Sign {
            req_id: 777,
            canon: "round-trip-canon".to_string(),
        },
    )
    .await;

    let sig_json = read_ctrl_t(&mut core, "sig").await;
    assert_eq!(
        sig_json
            .get("reqId")
            .or_else(|| sig_json.get("req_id"))
            .and_then(Value::as_u64),
        Some(777)
    );

    let sig = sig_json["sig"].as_str().expect("sig field");
    verify_signature(b"round-trip-canon", sig);

    h.stop().await;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WantFrameType {
    Ctrl,
    Mcp,
}

impl WantFrameType {
    fn to_frame_type(self) -> FrameType {
        match self {
            Self::Ctrl => FrameType::Ctrl,
            Self::Mcp => FrameType::Mcp,
        }
    }
}

#[derive(Clone, Debug)]
struct ExpectedFrame {
    frame_type: WantFrameType,
    payload: Vec<u8>,
}

async fn run_frame_vector_case(case: &Value, seen: &mut BTreeSet<&'static str>) {
    let name = case_name(case);
    let lower = name.to_ascii_lowercase();
    let should_err = expects_error(case);
    let chunks = case_chunks(case);

    if should_err {
        if lower.contains("oversize") {
            seen.insert("oversize-error");
        }
        if lower.contains("zero") || lower.contains("len<1") || lower.contains("len_lt_1") {
            seen.insert("zero-error");
        }
        if lower.contains("unknown") {
            seen.insert("unknown-type-error");
        }

        if let Some(chunks) = chunks {
            run_chunk_frame_case(&name, &chunks, &[], true).await;
        } else {
            let wire = case_bytes(case).unwrap_or_else(|| panic!("{name}: missing wire bytes"));
            assert_read_frame_error(&name, &wire).await;
        }
        return;
    }

    let expected = expected_frames(case);
    assert!(
        !expected.is_empty(),
        "{name}: valid case must declare expected frame(s)"
    );

    if lower.contains("partial") {
        seen.insert("partial-complete");
    }
    if lower.contains("two")
        || lower.contains("two-frames")
        || lower.contains("one-chunk")
        || expected.len() > 1
    {
        seen.insert("two-frames-one-chunk");
    }
    for frame in &expected {
        match frame.frame_type {
            WantFrameType::Ctrl => {
                seen.insert("valid-ctrl");
            }
            WantFrameType::Mcp => {
                seen.insert("valid-mcp");
            }
        }
    }

    if let Some(chunks) = chunks {
        run_chunk_frame_case(&name, &chunks, &expected, false).await;
    } else {
        let wire = case_bytes(case).unwrap_or_else(|| panic!("{name}: missing wire bytes"));
        let written = write_expected_frames_to_vec(&expected, wire.len()).await;
        assert_eq!(
            written, wire,
            "{name}: write_frame output must be byte-exact"
        );

        let read = read_expected_count_from_bytes(&name, &wire, expected.len()).await;
        assert_eq!(read.len(), expected.len());
        for (idx, (actual, want)) in read.iter().zip(expected.iter()).enumerate() {
            assert_frame_matches(&format!("{name}[{idx}]"), actual, want);
        }
    }
}

async fn run_chunk_frame_case(
    name: &str,
    chunks: &[Vec<u8>],
    expected: &[ExpectedFrame],
    should_err: bool,
) {
    assert!(!chunks.is_empty(), "{name}: chunks[] must not be empty");

    let total: usize = chunks.iter().map(Vec::len).sum();
    let (mut reader, mut writer) = duplex(total.saturating_add(1024).max(1024));
    let mut read_task = tokio::spawn(async move { read_frame(&mut reader).await });

    for (idx, chunk) in chunks.iter().enumerate() {
        writer.write_all(chunk).await.unwrap();
        writer.flush().await.unwrap();

        if idx + 1 < chunks.len() {
            assert!(
                timeout(SHORT_WAIT, &mut read_task).await.is_err(),
                "{name}: read_frame completed before partial frame was complete"
            );
        }
    }
    drop(writer);

    let joined = timeout(WAIT, read_task)
        .await
        .unwrap_or_else(|_| panic!("{name}: timed out waiting for chunked read"))
        .unwrap();

    if should_err {
        assert!(joined.is_err(), "{name}: expected read_frame error");
        return;
    }

    let frame = joined.unwrap();
    assert_eq!(
        expected.len(),
        1,
        "{name}: chunked partial vector must expect exactly one frame"
    );
    assert_frame_matches(name, &frame, &expected[0]);
}

async fn assert_read_frame_error(name: &str, wire: &[u8]) {
    let (mut reader, mut writer) = duplex(wire.len().saturating_add(1024).max(1024));
    writer.write_all(wire).await.unwrap();
    writer.flush().await.unwrap();
    drop(writer);

    let result = timeout(WAIT, read_frame(&mut reader))
        .await
        .unwrap_or_else(|_| panic!("{name}: timed out waiting for read_frame error"));
    assert!(
        result.is_err(),
        "{name}: expected read_frame to reject invalid frame"
    );
}

async fn read_expected_count_from_bytes(name: &str, wire: &[u8], count: usize) -> Vec<Frame> {
    let (mut reader, mut writer) = duplex(wire.len().saturating_add(1024).max(1024));
    writer.write_all(wire).await.unwrap();
    writer.flush().await.unwrap();
    drop(writer);

    let mut out = Vec::with_capacity(count);
    for idx in 0..count {
        let frame = timeout(WAIT, read_frame(&mut reader))
            .await
            .unwrap_or_else(|_| panic!("{name}: timed out reading frame {idx}"))
            .unwrap();
        out.push(frame);
    }
    out
}

async fn write_expected_frames_to_vec(frames: &[ExpectedFrame], expected_len: usize) -> Vec<u8> {
    let (mut reader, mut writer) = duplex(expected_len.saturating_add(1024).max(1024));

    for frame in frames {
        write_frame(
            &mut writer,
            frame.frame_type.to_frame_type(),
            &frame.payload,
        )
        .await
        .unwrap();
    }
    drop(writer);

    let mut out = Vec::new();
    timeout(WAIT, reader.read_to_end(&mut out))
        .await
        .expect("timed out collecting write_frame bytes")
        .unwrap();
    out
}

fn assert_frame_matches(name: &str, actual: &Frame, expected: &ExpectedFrame) {
    match (actual.frame_type, expected.frame_type) {
        (FrameType::Ctrl, WantFrameType::Ctrl) | (FrameType::Mcp, WantFrameType::Mcp) => {}
        (FrameType::Ctrl, WantFrameType::Mcp) => panic!("{name}: expected MCP frame, got CTRL"),
        (FrameType::Mcp, WantFrameType::Ctrl) => panic!("{name}: expected CTRL frame, got MCP"),
    }

    assert_eq!(
        actual.payload.as_slice(),
        expected.payload.as_slice(),
        "{name}: payload bytes differ"
    );
}

fn expected_frames(case: &Value) -> Vec<ExpectedFrame> {
    if let Some(frames) =
        value_at(case, &["frames", "expected_frames", "want_frames"]).and_then(Value::as_array)
    {
        return frames.iter().map(parse_expected_frame).collect();
    }

    // The framing-vectors.json fixture nests the expected frames under
    // `expect.frames` (single-push cases).
    if let Some(frames) = value_at(case, &["expect", "expected", "want"])
        .and_then(|v| v.get("frames"))
        .and_then(Value::as_array)
    {
        return frames.iter().map(parse_expected_frame).collect();
    }

    // Chunked cases describe per-chunk outcomes in `expect_per_chunk[]`; the
    // completed frame(s) live in the final step that carries a `frames` array.
    if let Some(steps) =
        value_at(case, &["expect_per_chunk", "expectPerChunk"]).and_then(Value::as_array)
    {
        if let Some(frames) = steps
            .iter()
            .rev()
            .find_map(|step| step.get("frames").and_then(Value::as_array))
        {
            return frames.iter().map(parse_expected_frame).collect();
        }
    }

    if let Some(frame) = value_at(case, &["expected", "want"]).filter(|v| v.is_object()) {
        return vec![parse_expected_frame(frame)];
    }

    vec![parse_expected_frame(case)]
}

fn parse_expected_frame(value: &Value) -> ExpectedFrame {
    let frame_type = value_at(value, &["frame_type", "frameType", "type", "t"])
        .map(frame_type_from_value)
        .unwrap_or_else(|| panic!("frame vector case missing frame type: {value}"));

    let payload = payload_from_case(value)
        .unwrap_or_else(|| panic!("frame vector case missing payload: {value}"));
    ExpectedFrame {
        frame_type,
        payload,
    }
}

fn frame_type_from_value(value: &Value) -> WantFrameType {
    if let Some(text) = value.as_str() {
        return match text.to_ascii_lowercase().as_str() {
            "ctrl" | "control" => WantFrameType::Ctrl,
            "mcp" | "jsonrpc" => WantFrameType::Mcp,
            other => panic!("unknown expected frame type {other}"),
        };
    }

    match value.as_i64() {
        // Wire type byte (§A.2): 0x01 = MCP, 0x02 = CTRL.
        Some(1) => WantFrameType::Mcp,
        Some(2) => WantFrameType::Ctrl,
        other => panic!("unknown numeric expected frame type {other:?}"),
    }
}

fn payload_from_case(value: &Value) -> Option<Vec<u8>> {
    if let Some(v) = value_at(value, &["payload_hex", "payloadHex"]) {
        return Some(parse_hex(v.as_str().expect("payload_hex string")));
    }
    if let Some(v) = value_at(
        value,
        &[
            "payload_b64",
            "payloadB64",
            "payload_base64",
            "payloadBase64",
        ],
    ) {
        return Some(
            STANDARD
                .decode(v.as_str().expect("payload_b64 string"))
                .unwrap(),
        );
    }
    if let Some(v) = value_at(
        value,
        &["payload_json", "payloadJson", "json_payload", "jsonPayload"],
    ) {
        return Some(serde_json::to_vec(v).unwrap());
    }
    if let Some(v) = value_at(value, &["payload", "body"]) {
        let encoding = value_at(value, &["payload_encoding", "payloadEncoding", "encoding"])
            .and_then(Value::as_str);
        return Some(bytes_from_payload_value(v, encoding));
    }
    if let Some(v) = value.get("json") {
        return Some(serde_json::to_vec(v).unwrap());
    }
    None
}

fn bytes_from_payload_value(value: &Value, encoding: Option<&str>) -> Vec<u8> {
    match value {
        Value::String(text) => match encoding.map(str::to_ascii_lowercase).as_deref() {
            Some("hex") => parse_hex(text),
            Some("base64") | Some("b64") => STANDARD.decode(text).unwrap(),
            _ => text.as_bytes().to_vec(),
        },
        Value::Array(items) => items
            .iter()
            .map(|v| v.as_u64().expect("byte array item") as u8)
            .collect(),
        _ => serde_json::to_vec(value).unwrap(),
    }
}

fn case_bytes(case: &Value) -> Option<Vec<u8>> {
    if let Some(v) = value_at(case, &["input_hex", "inputHex"]) {
        return Some(parse_hex(v.as_str().expect("input_hex string")));
    }

    for key in ["bytes", "wire", "input", "encoded", "raw"] {
        if let Some(value) = case.get(key) {
            return Some(bytes_from_wire_value(value));
        }
    }

    case.get("frame")
        .filter(|v| !v.is_object())
        .map(bytes_from_wire_value)
}

fn case_chunks(case: &Value) -> Option<Vec<Vec<u8>>> {
    if let Some(chunks) = value_at(case, &["chunks_hex", "chunksHex"]).and_then(Value::as_array) {
        return Some(
            chunks
                .iter()
                .map(|chunk| parse_hex(chunk.as_str().expect("chunks_hex entry string")))
                .collect(),
        );
    }

    let chunks = value_at(case, &["chunks", "parts", "fragments"])?.as_array()?;
    Some(
        chunks
            .iter()
            .map(|chunk| {
                if chunk.is_object() {
                    case_bytes(chunk)
                        .unwrap_or_else(|| panic!("chunk object missing bytes: {chunk}"))
                } else {
                    bytes_from_wire_value(chunk)
                }
            })
            .collect(),
    )
}

fn bytes_from_wire_value(value: &Value) -> Vec<u8> {
    match value {
        Value::String(text) => decode_wire_string(text),
        Value::Array(items) => items
            .iter()
            .map(|v| v.as_u64().expect("byte array item") as u8)
            .collect(),
        Value::Object(_) => case_bytes(value).expect("wire object missing bytes"),
        _ => panic!("unsupported wire bytes value: {value}"),
    }
}

fn decode_wire_string(text: &str) -> Vec<u8> {
    let compact: String = text
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace() && *ch != '_')
        .collect();
    let hex = compact.strip_prefix("0x").unwrap_or(&compact);

    if hex.len() % 2 == 0 && hex.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        parse_hex(hex)
    } else {
        STANDARD
            .decode(text)
            .unwrap_or_else(|_| text.as_bytes().to_vec())
    }
}

fn parse_hex(text: &str) -> Vec<u8> {
    let compact: String = text
        .trim_start_matches("0x")
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace() && *ch != '_')
        .collect();

    assert_eq!(
        compact.len() % 2,
        0,
        "hex string must have even length: {text}"
    );
    compact
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let hi = hex_value(pair[0]);
            let lo = hex_value(pair[1]);
            (hi << 4) | lo
        })
        .collect()
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex byte {}", byte as char),
    }
}

fn expects_error(case: &Value) -> bool {
    let name = case_name(case).to_ascii_lowercase();
    name.contains("oversize")
        || name.contains("unknown")
        || name.contains("len<1")
        || name.contains("len_lt_1")
        || name.contains("zero")
        || value_at(
            case,
            &[
                "error",
                "err",
                "want_error",
                "wantError",
                "expected_error",
                "expectedError",
            ],
        )
        .is_some_and(|v| !v.is_null() && v != &json!(false))
}

fn case_name(case: &Value) -> String {
    value_at(case, &["name", "case", "id"])
        .and_then(Value::as_str)
        .unwrap_or("<unnamed>")
        .to_string()
}

fn value_at<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| value.get(*key))
}

struct Harness {
    listener: UnixListener,
    claude_stdin: DuplexStream,
    claude_stdout: DuplexStream,
    shim_task: JoinHandle<Result<(), String>>,
}

impl Harness {
    async fn start(name: &str) -> Self {
        let dir = unique_dir(name);
        fs::create_dir_all(&dir).unwrap();

        let seed_path = dir.join("agent.seed");
        fs::write(&seed_path, TEST_SEED).unwrap();

        let sock_path = dir.join("core.sock");
        let sock = sock_path.to_string_lossy().to_string();

        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        env::set_var("A2A_AGENT_ID", "conformance-agent");
        env::set_var("A2A_SIGNING_KEY", seed_path.to_string_lossy().to_string());
        env::set_var("A2A_SIG_ALG", "ed25519");
        env::set_var("A2A_CORE_SOCK", &sock);
        env::set_var("SUBS_KEY", "conformance-subs");
        env::set_var("A2A_REQUEST_TIMEOUT_MS", "250");
        env::set_var("A2A_CORE_REQUEST_TIMEOUT_MS", "250");
        env::set_var("A2A_MCP_REQUEST_TIMEOUT_MS", "250");
        env::set_var("A2A_PUB_TIMEOUT_MS", "250");
        env::set_var("A2A_HELLO_TIMEOUT_MS", "250");
        env::set_var("A2A_CONNECT_TIMEOUT_MS", "150");
        env::set_var("A2A_RECONNECT_MIN_MS", "20");
        env::set_var("A2A_RECONNECT_MAX_MS", "80");
        env::set_var("A2A_PING_INTERVAL_MS", "10000");
        env::set_var("A2A_PONG_TIMEOUT_MS", "10000");
        let cfg = Config::from_env().unwrap();
        drop(_guard);

        let listener = UnixListener::bind(&sock_path).unwrap();

        let (claude_stdin, shim_stdin) = duplex(IO_CAP);
        let (shim_stdout, claude_stdout) = duplex(IO_CAP);

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

    async fn wait_done(self, wait: Duration) -> Result<(), String> {
        let Self { mut shim_task, .. } = self;
        match timeout(wait, &mut shim_task).await {
            Ok(joined) => joined.map_err(|err| err.to_string())?,
            Err(_) => Err("timed out waiting for shim task to finish".to_string()),
        }
    }
}

struct HelloSeen {
    agent_id: String,
}

async fn accept_handshake(
    listener: &UnixListener,
    epoch: u32,
    nonce_byte: u8,
) -> (UnixStream, HelloSeen) {
    let (mut stream, _auth_sig) = accept_hello_and_auth(listener, nonce_byte).await;
    write_ctrl(
        &mut stream,
        Ctrl::Ok {
            session: format!("session-{epoch}"),
            epoch,
        },
    )
    .await;
    let agent_id = "conformance-agent".to_string();
    (stream, HelloSeen { agent_id })
}

async fn accept_hello_and_auth(listener: &UnixListener, nonce_byte: u8) -> (UnixStream, String) {
    let (mut stream, _) = timeout(WAIT, listener.accept())
        .await
        .expect("timed out accepting core connection")
        .unwrap();

    let hello_frame = read_frame_timeout(&mut stream).await;
    assert_hello_frame(&hello_frame);

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

    let sig = auth_json["sig"].as_str().expect("auth sig").to_string();
    verify_signature(&nonce, &sig);

    (stream, sig)
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

fn assert_hello_frame(frame: &Frame) {
    match frame.frame_type {
        FrameType::Ctrl => {}
        FrameType::Mcp => panic!("expected hello CTRL frame"),
    }

    let hello = frame_json(frame);
    assert_eq!(hello.get("t").and_then(Value::as_str), Some("hello"));
    assert_eq!(hello.get("v").and_then(Value::as_u64), Some(1));
    assert_eq!(json_str(&hello, "agentId", "agent_id"), "conformance-agent");
    assert!(hello
        .get("caps")
        .and_then(Value::as_array)
        .map(|caps| caps.iter().any(|cap| cap.as_str() == Some("delivered")))
        .unwrap_or(false));
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

fn load_json_fixture(path: &str) -> Value {
    let text = fs::read_to_string(Path::new(path))
        .unwrap_or_else(|err| panic!("failed to read {path}: {err}"));
    serde_json::from_str(&text).unwrap_or_else(|err| panic!("failed to parse {path}: {err}"))
}

fn unique_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    env::temp_dir().join(format!("a2a-shim-{name}-{}-{nanos}", process::id()))
}
