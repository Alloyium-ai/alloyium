use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    fmt, io,
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::{json, Value};
use tokio::{
    io::{AsyncRead, AsyncWrite, BufReader},
    sync::{
        mpsc::{self, error::TryRecvError},
        Mutex,
    },
    task::JoinSet,
    time,
};

use crate::{
    config::Config,
    ctrl::Ctrl,
    framing::{read_frame, write_frame, write_frame_chunked, FrameType},
    hello::run_handshake,
    mcp_pump::{peek_id_method, read_stdio, write_stdio, StdinGate, StdioMode},
    resilience::{backoff, should_drop_late_reply, timeout_budget, Epoch},
    signer::sign_canon,
    transport::connect,
};

const CTRL_QUEUE_CAP: usize = 128;
const MCP_QUEUE_CAP: usize = 128;
const STDIN_QUEUE_CAP: usize = 128;
const UDS_QUEUE_CAP: usize = 128;
const MISSED_PONG_LIMIT: u32 = 2;
const TIMEOUT_SWEEP_INTERVAL: Duration = Duration::from_millis(100);
const MCP_WRITE_CHUNK: usize = 256 * 1024;
const TOMBSTONE_EPOCH_CAP: usize = 16;
const TOMBSTONE_ID_CAP_PER_EPOCH: usize = 4096;
const INBOUND_NOTIFICATION_BUFFER_CAP: usize = 1024;
const NOTIFICATION_DEDUP_CAP: usize = 4096;

fn shim_debug_enabled() -> bool {
    std::env::var("A2A_SHIM_DEBUG")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn shim_debug(event: &str, detail: impl fmt::Display) {
    if shim_debug_enabled() {
        eprintln!("[a2a-shim] {event} {detail}");
    }
}

enum StdinEvent {
    Body(Vec<u8>, StdioMode),
    Eof,
    Error(String),
}

enum UdsEvent {
    Mcp(Vec<u8>),
    Pong,
}

enum SessionEnd {
    Reconnect,
    Exit,
}

#[derive(Default)]
struct AppState {
    pending: HashMap<String, (Epoch, Instant)>,
    tombstones: Tombstones,
    cached_init: Option<Vec<u8>>,
    init_id: Option<Value>,
    init_key: Option<String>,
    init_result_sent: bool,
    stdio_mode: Option<StdioMode>,
    cached_initialized: Option<Vec<u8>>,
    replay_initialized: bool,
    claude_initialized: bool,
    notification_gate: StdinGate,
    notification_buffer: VecDeque<Vec<u8>>,
    notification_dedup: HashSet<String>,
    notification_dedup_order: VecDeque<String>,
}

#[derive(Default)]
struct Tombstones {
    epochs: HashMap<u32, EpochTombstones>,
    epoch_order: VecDeque<u32>,
}

#[derive(Default)]
struct EpochTombstones {
    ids: HashSet<String>,
    order: VecDeque<String>,
}

impl Tombstones {
    /// A late reply carries only its id, not its origin epoch, so a tombstone
    /// must be matched across ALL retained epoch buckets (the per-epoch keying
    /// is only for bounded GC). Checking a single epoch would miss a tombstone
    /// inserted under a now-superseded epoch.
    ///
    /// CLIENT CONTRACT: tombstones key by request id ALONE (not id+epoch). A
    /// reused id while its tombstone is still retained could drop a legitimate
    /// reply — safe for Claude Code, which issues strictly monotonic JSON-RPC ids
    /// (never reused). Uphold this if another client is ever placed behind the shim.
    fn contains_any(&self, key: &str) -> bool {
        self.epochs.values().any(|t| t.ids.contains(key))
    }

    fn insert(&mut self, epoch: Epoch, key: &str) {
        let epoch_key = epoch.0;
        if !self.epochs.contains_key(&epoch_key) {
            self.epochs.insert(epoch_key, EpochTombstones::default());
            self.epoch_order.push_back(epoch_key);
        }

        while self.epochs.len() > TOMBSTONE_EPOCH_CAP {
            let Some(old_epoch) = self.epoch_order.pop_front() else {
                break;
            };
            self.epochs.remove(&old_epoch);
        }

        let Some(epoch_tombstones) = self.epochs.get_mut(&epoch_key) else {
            return;
        };

        if epoch_tombstones.ids.insert(key.to_string()) {
            epoch_tombstones.order.push_back(key.to_string());
        }

        while epoch_tombstones.ids.len() > TOMBSTONE_ID_CAP_PER_EPOCH {
            let Some(old_key) = epoch_tombstones.order.pop_front() else {
                break;
            };
            epoch_tombstones.ids.remove(&old_key);
        }
    }
}

impl AppState {
    fn note_stdio_mode(&mut self, mode: StdioMode) {
        if self.stdio_mode.is_none() {
            self.stdio_mode = Some(mode);
        }
    }

    fn output_stdio_mode(&self) -> StdioMode {
        self.stdio_mode.unwrap_or(StdioMode::Lsp)
    }

    fn record_notification_dedup(&mut self, notif_id: &str) {
        if self.notification_dedup.insert(notif_id.to_string()) {
            self.notification_dedup_order.push_back(notif_id.to_string());
        }

        while self.notification_dedup_order.len() > NOTIFICATION_DEDUP_CAP {
            let Some(old) = self.notification_dedup_order.pop_front() else {
                break;
            };
            self.notification_dedup.remove(&old);
        }
    }
}

pub async fn run(cfg: Config) -> Result<(), Box<dyn Error>> {
    let sock = cfg.core_sock.clone();
    run_with_io(cfg, tokio::io::stdin(), tokio::io::stdout(), &sock).await
}

pub async fn run_with_io<I, O>(
    cfg: Config,
    stdin: I,
    stdout: O,
    sock: &str,
) -> Result<(), Box<dyn Error>>
where
    I: AsyncRead + Unpin + Send + 'static,
    O: AsyncWrite + Unpin + Send + 'static,
{
    let stdout = Arc::new(Mutex::new(stdout));
    let (stdin_tx, mut stdin_rx) = mpsc::channel(STDIN_QUEUE_CAP);
    let _stdin_task = spawn_stdin_task(stdin, stdin_tx);

    let mut state = AppState::default();
    let mut have_connected = false;
    let mut attempt = 0_u32;

    loop {
        sweep_expired_pending(&stdout, &mut state).await?;

        let connect_attempt = match time::timeout(cfg.connect_timeout(), connect(sock)).await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "core connect timed out",
            )),
        };
        let mut stream = match connect_attempt {
            Ok(stream) => stream,
            Err(err) => {
                if !have_connected {
                    write_first_connect_error_if_possible(
                        &stdout,
                        &mut stdin_rx,
                        &mut state,
                        "core_unavailable",
                        "core unavailable",
                    )
                    .await?;
                    return Err(err.into());
                }

                sleep_with_deadline_sweeps(
                    &stdout,
                    &mut state,
                    backoff(attempt, cfg.reconnect_min(), cfg.reconnect_max()),
                )
                .await?;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        let hello = match run_handshake(&mut stream, &cfg).await {
            Ok(ok) => ok,
            Err(err) => {
                if !have_connected {
                    write_first_connect_error_if_possible(
                        &stdout,
                        &mut stdin_rx,
                        &mut state,
                        "hello_failed",
                        "core hello failed",
                    )
                    .await?;
                    return Err(
                        io::Error::new(io::ErrorKind::PermissionDenied, format!("{err}")).into(),
                    );
                }

                sleep_with_deadline_sweeps(
                    &stdout,
                    &mut state,
                    backoff(attempt, cfg.reconnect_min(), cfg.reconnect_max()),
                )
                .await?;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        let epoch = Epoch(hello.epoch);
        let reconnecting = have_connected;
        have_connected = true;
        attempt = 0;

        match run_session(
            &cfg,
            stream,
            epoch,
            reconnecting,
            &mut stdin_rx,
            stdout.clone(),
            &mut state,
        )
        .await
        {
            Ok(SessionEnd::Reconnect) => {}
            Ok(SessionEnd::Exit) => return Ok(()),
            Err(err) => return Err(err.into()),
        }
    }
}

fn spawn_stdin_task<I>(stdin: I, tx: mpsc::Sender<StdinEvent>) -> tokio::task::JoinHandle<()>
where
    I: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut reader = BufReader::new(stdin);

        loop {
            match read_stdio(&mut reader).await {
                Ok(Some(msg)) => {
                    if tx.send(StdinEvent::Body(msg.body, msg.mode)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => {
                    let _ = tx.send(StdinEvent::Eof).await;
                    break;
                }
                Err(err) => {
                    let _ = tx.send(StdinEvent::Error(err.to_string())).await;
                    break;
                }
            }
        }
    })
}

async fn run_session<O>(
    cfg: &Config,
    stream: tokio::net::UnixStream,
    epoch: Epoch,
    reconnecting: bool,
    stdin_rx: &mut mpsc::Receiver<StdinEvent>,
    stdout: Arc<Mutex<O>>,
    state: &mut AppState,
) -> io::Result<SessionEnd>
where
    O: AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = stream.into_split();
    let (ctrl_tx, ctrl_rx) = mpsc::channel(CTRL_QUEUE_CAP);
    let (mcp_tx, mcp_rx) = mpsc::channel(MCP_QUEUE_CAP);
    let (uds_tx, mut uds_rx) = mpsc::channel(UDS_QUEUE_CAP);

    let mut tasks = JoinSet::new();
    tasks.spawn(writer_loop(writer, ctrl_rx, mcp_rx));
    tasks.spawn(uds_read_loop(reader, ctrl_tx.clone(), uds_tx));

    if reconnecting {
        for (key, request_epoch) in take_pre_epoch(&mut state.pending, epoch) {
            // initialize is replayed on reconnect (re-tracked below), not
            // abandoned — never fail claude's initialize from the epoch
            // transition, or claude could get an error for a request a later
            // core still answers (§A.6 single init response).
            if state.init_key.as_deref() == Some(key.as_str()) {
                continue;
            }
            state.tombstones.insert(request_epoch, &key);
            write_error_for_key(state, &stdout, &key, "core_timeout", "core timeout").await?;
        }
    }

    if let Some(init) = state.cached_init.clone() {
        // Once claude's initialize has its one response, the replay only
        // re-establishes the new core's session — do NOT re-track it, or a
        // re-init the new core never answers would emit a SECOND claude
        // response (timeout) for an id that already succeeded.
        if should_track_replayed_init(state.init_result_sent) {
            track_body_if_request(
                &mut state.pending,
                epoch,
                &init,
                cfg.request_timeout(),
                cfg.mcp_request_timeout(),
            );
        }
        if mcp_tx.try_send(init).is_err() {
            return end_session(&mut tasks, SessionEnd::Reconnect).await;
        }
    }

    let mut ping_tick = time::interval(cfg.ping_interval());
    ping_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    ping_tick.tick().await;

    let mut timeout_tick = time::interval(TIMEOUT_SWEEP_INTERVAL);
    timeout_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let mut missed_pongs = 0_u32;

    loop {
        tokio::select! {
            stdin_event = stdin_rx.recv() => {
                match stdin_event {
                    Some(StdinEvent::Body(body, mode)) => {
                        state.note_stdio_mode(mode);
                        let (id, method) = peek_id_method(&body);
                        let saw_initialized = method.as_deref() == Some("notifications/initialized");
                        shim_debug(
                            "stdin_body",
                            format_args!(
                                "mode={mode:?} id={} method={} saw_initialized={}",
                                id.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
                                method.as_deref().unwrap_or("-"),
                                saw_initialized,
                            ),
                        );

                        remember_initialize(
                            &mut state.cached_init,
                            &mut state.init_id,
                            &mut state.init_key,
                            &body,
                        );
                        track_body_if_request(
                            &mut state.pending,
                            epoch,
                            &body,
                            cfg.request_timeout(),
                            cfg.mcp_request_timeout(),
                        );

                        if saw_initialized {
                            state.claude_initialized = true;
                            // §A.6: cache claude's `notifications/initialized` so it can
                            // be replayed to the new core after the re-init result on
                            // reconnect — the acceptor's B4 buffers OUTBOUND notifications
                            // until its per-connection session sees `initialized`.
                            state.cached_initialized = Some(body.clone());
                        }

                        if mcp_tx.try_send(body).is_err() {
                            return end_session(&mut tasks, SessionEnd::Reconnect).await;
                        }

                        if saw_initialized {
                            if let Err(err) = maybe_open_notification_gate(&stdout, state).await {
                                abort_session(&mut tasks).await;
                                return Err(err);
                            }
                        }
                    }
                    Some(StdinEvent::Eof) | None => {
                        return end_session(&mut tasks, SessionEnd::Exit).await;
                    }
                    Some(StdinEvent::Error(err)) => {
                        abort_session(&mut tasks).await;
                        return Err(io::Error::new(io::ErrorKind::InvalidData, err));
                    }
                }
            }
            uds_event = uds_rx.recv() => {
                match uds_event {
                    Some(UdsEvent::Mcp(payload)) => {
                        if let Err(err) = handle_mcp_from_core(&stdout, &ctrl_tx, state, epoch, payload).await {
                            abort_session(&mut tasks).await;
                            return Err(err);
                        }
                        // §A.6: after the re-init RESULT on reconnect, replay claude's
                        // cached `notifications/initialized` to the new core so its B4
                        // outbound-notification buffer flushes (else inbound bus/a2a
                        // events are withheld forever post-reconnect).
                        if state.replay_initialized {
                            state.replay_initialized = false;
                            if let Some(initialized) = state.cached_initialized.clone() {
                                if mcp_tx.try_send(initialized).is_err() {
                                    return end_session(&mut tasks, SessionEnd::Reconnect).await;
                                }
                            }
                        }
                    }
                    Some(UdsEvent::Pong) => missed_pongs = 0,
                    None => {
                        return end_session(&mut tasks, SessionEnd::Reconnect).await;
                    }
                }
            }
            _ = ping_tick.tick() => {
                missed_pongs = missed_pongs.saturating_add(1);
                if missed_pongs >= MISSED_PONG_LIMIT {
                    return end_session(&mut tasks, SessionEnd::Reconnect).await;
                }

                if send_ctrl(&ctrl_tx, Ctrl::Ping { ts: None }).await.is_err() {
                    return end_session(&mut tasks, SessionEnd::Reconnect).await;
                }
            }
            _ = timeout_tick.tick() => {
                if let Err(err) = sweep_expired_pending(&stdout, state).await {
                    abort_session(&mut tasks).await;
                    return Err(err);
                }
            }
            task_done = tasks.join_next() => {
                let _ = task_done;
                drain_queued_uds_notifications(&mut uds_rx, &stdout, state).await?;
                return end_session(&mut tasks, SessionEnd::Reconnect).await;
            }
        }
    }
}

/// Abort + join all session tasks. Shared by the graceful end_session paths and
/// the error exits so every run_session return cleans up consistently (a session
/// error otherwise relies on JoinSet's Drop — no leak, but inconsistent).
async fn abort_session(tasks: &mut JoinSet<io::Result<()>>) {
    tasks.abort_all();
    while let Some(_result) = tasks.join_next().await {}
}

async fn end_session(
    tasks: &mut JoinSet<io::Result<()>>,
    end: SessionEnd,
) -> io::Result<SessionEnd> {
    abort_session(tasks).await;
    Ok(end)
}

async fn writer_loop<W>(
    mut writer: W,
    mut ctrl_rx: mpsc::Receiver<Vec<u8>>,
    mut mcp_rx: mpsc::Receiver<Vec<u8>>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut ctrl_open = true;
    let mut mcp_open = true;

    loop {
        if ctrl_open {
            drain_ctrl_lane(&mut writer, &mut ctrl_rx, &mut ctrl_open).await?;
        }

        if !ctrl_open && !mcp_open {
            return Ok(());
        }

        tokio::select! {
            biased;

            ctrl = ctrl_rx.recv(), if ctrl_open => {
                match ctrl {
                    Some(payload) => write_frame(&mut writer, FrameType::Ctrl, &payload).await?,
                    None => ctrl_open = false,
                }
            }
            mcp = mcp_rx.recv(), if mcp_open => {
                match mcp {
                    Some(payload) => {
                        write_mcp_chunked(&mut writer, &payload).await?;
                    }
                    None => mcp_open = false,
                }
            }
        }
    }
}

async fn write_mcp_chunked<W>(writer: &mut W, payload: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    // §A-v3 (A): one MCP message = ONE contiguous frame (≤16MiB). CTRL priority
    // is handled by writer_loop (drain at top of each iteration + biased
    // select); CTRL that arrives mid-frame waits for the next iteration (bounded
    // one-flush). The body is written in ≤256KiB socket-writes purely for
    // cooperative yielding — NOT split into multiple frames (which would desync
    // the receiver's contiguous-length read).
    write_frame_chunked(writer, FrameType::Mcp, payload, MCP_WRITE_CHUNK).await
}

async fn drain_ctrl_lane<W>(
    writer: &mut W,
    ctrl_rx: &mut mpsc::Receiver<Vec<u8>>,
    ctrl_open: &mut bool,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    loop {
        match ctrl_rx.try_recv() {
            Ok(payload) => write_frame(writer, FrameType::Ctrl, &payload).await?,
            Err(TryRecvError::Empty) => return Ok(()),
            Err(TryRecvError::Disconnected) => {
                *ctrl_open = false;
                return Ok(());
            }
        }
    }
}

async fn uds_read_loop<R>(
    mut reader: R,
    ctrl_tx: mpsc::Sender<Vec<u8>>,
    event_tx: mpsc::Sender<UdsEvent>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    loop {
        let frame = read_frame(&mut reader).await?;

        match frame.frame_type {
            FrameType::Mcp => {
                event_tx
                    .send(UdsEvent::Mcp(frame.payload))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "uds event queue closed")
                    })?;
            }
            FrameType::Ctrl => {
                let ctrl = Ctrl::from_json(&frame.payload)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

                match ctrl {
                    Ctrl::Sign { req_id, canon } => {
                        let sig =
                            sign_canon(&canon).map_err(|err| io::Error::other(err.to_string()))?;
                        send_ctrl(&ctrl_tx, Ctrl::Sig { req_id, sig }).await?;
                    }
                    Ctrl::Ping { ts } => {
                        send_ctrl(&ctrl_tx, Ctrl::Pong { ts }).await?;
                    }
                    Ctrl::Pong { .. } => {
                        event_tx.send(UdsEvent::Pong).await.map_err(|_| {
                            io::Error::new(io::ErrorKind::BrokenPipe, "uds event queue closed")
                        })?;
                    }
                    Ctrl::Err { .. } => {}
                    _ => {}
                }
            }
        }
    }
}

async fn drain_queued_uds_notifications<O>(
    uds_rx: &mut mpsc::Receiver<UdsEvent>,
    stdout: &Arc<Mutex<O>>,
    state: &mut AppState,
) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    loop {
        match uds_rx.try_recv() {
            Ok(UdsEvent::Mcp(payload)) => {
                let (_id, method) = peek_id_method(&payload);
                if method
                    .as_deref()
                    .map(|m| m.starts_with("notifications/"))
                    .unwrap_or(false)
                {
                    let meta = notification_meta(&payload);
                    if let Some(notif_id) = meta.direct_notif_id() {
                        if state.notification_dedup.contains(notif_id) {
                            shim_debug("notif_duplicate_drain_suppressed", format_args!("notif_id={notif_id}"));
                            continue;
                        }
                    }
                    let written = if state.notification_gate.is_open() {
                        write_stdout(stdout, state.output_stdio_mode(), &payload).await?;
                        true
                    } else {
                        buffer_inbound_notification(state, payload)?;
                        false
                    };
                    if written {
                        if let Some(notif_id) = meta.direct_notif_id() {
                            state.record_notification_dedup(notif_id);
                        }
                    }
                } else {
                    shim_debug(
                        "drop_queued_uds_mcp",
                        "non-notification frame during reconnect",
                    );
                }
            }
            Ok(UdsEvent::Pong) => {}
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => return Ok(()),
        }
    }
}

async fn send_ctrl(tx: &mpsc::Sender<Vec<u8>>, ctrl: Ctrl) -> io::Result<()> {
    let payload = ctrl
        .to_json()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    // §A.2b: bounded queues fail closed on overflow rather than awaiting.
    tx.try_send(payload)
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ctrl writer closed or full"))
}

fn remember_initialize(
    cached_init: &mut Option<Vec<u8>>,
    init_id: &mut Option<Value>,
    init_key: &mut Option<String>,
    body: &[u8],
) {
    let (id, method) = peek_id_method(body);

    if method.as_deref() != Some("initialize") {
        return;
    }

    if cached_init.is_none() {
        *cached_init = Some(body.to_vec());
    }

    if init_id.is_none() {
        *init_id = id.clone();
        *init_key = id.as_ref().map(id_key);
    }
}

/// On reconnect the cached `initialize` is replayed to re-establish the core
/// session, but it is re-tracked as a timeout-eligible pending request ONLY while
/// claude's initialize is still unanswered. Once answered, a re-init the new core
/// never answers must not emit a second claude response (§A.6 single init
/// response). One of three independent guards (this + take_pre_epoch skip +
/// write_error_respecting_init); unit-tested in isolation so a single-guard
/// regression is caught even though the scenario test is masked by the others.
fn should_track_replayed_init(init_result_sent: bool) -> bool {
    !init_result_sent
}

fn track_body_if_request(
    pending: &mut HashMap<String, (Epoch, Instant)>,
    epoch: Epoch,
    body: &[u8],
    normal: Duration,
    long: Duration,
) {
    let (id, method) = peek_id_method(body);
    let Some(id) = id else {
        return;
    };

    let method = method.unwrap_or_default();
    let deadline = Instant::now() + timeout_budget(&method, normal, long);
    pending.insert(id_key(&id), (epoch, deadline));
}

async fn handle_mcp_from_core<O>(
    stdout: &Arc<Mutex<O>>,
    ctrl_tx: &mpsc::Sender<Vec<u8>>,
    state: &mut AppState,
    current_epoch: Epoch,
    payload: Vec<u8>,
) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    let (id, method) = peek_id_method(&payload);

    if let Some(id) = id {
        let key = id_key(&id);
        // Init-key reply handled FIRST — BEFORE the tombstone check. The init id
        // is tombstoned after its one result (below), so a re-init result on
        // reconnect would otherwise be dropped by `contains_any` and never reach
        // the replay trigger. The first (success) result has init_result_sent ==
        // false and falls through to be forwarded; any later re-init result is
        // dropped (the dup response is suppressed per §A.6) but still flags the
        // §A.6 replay of `notifications/initialized` so the new core's B4 outbound
        // buffer flushes (else the agent goes deaf to inbound post-reconnect).
        if state.init_key.as_deref() == Some(key.as_str()) && state.init_result_sent {
            state.replay_initialized = true;
            return Ok(());
        }

        if state.tombstones.contains_any(&key) {
            return Ok(());
        }

        let reply_epoch = state
            .pending
            .remove(&key)
            .map(|(epoch, _)| epoch)
            .unwrap_or(current_epoch);

        if should_drop_late_reply(current_epoch, reply_epoch) {
            state.tombstones.insert(reply_epoch, &key);
            return Ok(());
        }

        let is_init_reply = state.init_key.as_ref() == Some(&key);
        if is_init_reply {
            state.init_result_sent = true;
        }

        state.tombstones.insert(reply_epoch, &key);
        write_stdout(stdout, state.output_stdio_mode(), &payload).await?;

        if is_init_reply {
            maybe_open_notification_gate(stdout, state).await?;
        }

        return Ok(());
    }

    if method
        .as_deref()
        .map(|m| m.starts_with("notifications/"))
        .unwrap_or(false)
    {
        shim_debug(
            "core_notification",
            format_args!(
                "method={} gate_open={} init_result_sent={} claude_initialized={} buffered={}",
                method.as_deref().unwrap_or(""),
                state.notification_gate.is_open(),
                state.init_result_sent,
                state.claude_initialized,
                state.notification_buffer.len(),
            ),
        );
        accept_inbound_notification(stdout, ctrl_tx, state, current_epoch, payload).await?;
    }

    Ok(())
}

async fn accept_inbound_notification<O>(
    stdout: &Arc<Mutex<O>>,
    ctrl_tx: &mpsc::Sender<Vec<u8>>,
    state: &mut AppState,
    current_epoch: Epoch,
    payload: Vec<u8>,
) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    let meta = notification_meta(&payload);

    if let Some(notif_id) = meta.direct_notif_id() {
        if state.notification_dedup.contains(notif_id) {
            shim_debug("notif_duplicate_delivered", format_args!("notif_id={notif_id}"));
            send_delivered(ctrl_tx, current_epoch, notif_id).await?;
            return Ok(());
        }
    } else if meta.direct {
        shim_debug("notif_missing_notif_id", "direct notification missing notifId");
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "direct notification missing notifId",
        ));
    }

    if let Some(notif_id) = meta.direct_notif_id() {
        if !state.notification_gate.is_open() {
            buffer_inbound_notification(state, payload)?;
            return Ok(());
        }

        write_stdout(stdout, state.output_stdio_mode(), &payload).await?;
        state.record_notification_dedup(notif_id);
        shim_debug("notif_delivered_sent", format_args!("notif_id={notif_id}"));
        send_delivered(ctrl_tx, current_epoch, notif_id).await?;
        return Ok(());
    }

    if state.notification_gate.is_open() {
        write_stdout(stdout, state.output_stdio_mode(), &payload).await?;
    } else {
        buffer_inbound_notification(state, payload)?;
    }

    Ok(())
}

fn buffer_inbound_notification(state: &mut AppState, payload: Vec<u8>) -> io::Result<()> {
    if state.notification_buffer.len() >= INBOUND_NOTIFICATION_BUFFER_CAP {
        shim_debug("notif_buffer_full_no_delivered", "inbound notification buffer full");
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "inbound notification buffer exceeds limit",
        ));
    }

    state.notification_buffer.push_back(payload);
    Ok(())
}

async fn maybe_open_notification_gate<O>(
    stdout: &Arc<Mutex<O>>,
    state: &mut AppState,
) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    if state.notification_gate.is_open() || !state.init_result_sent {
        shim_debug(
            "notification_gate_wait",
            format_args!(
                "gate_open={} init_result_sent={} claude_initialized={} buffered={}",
                state.notification_gate.is_open(),
                state.init_result_sent,
                state.claude_initialized,
                state.notification_buffer.len(),
            ),
        );
        return Ok(());
    }

    state.notification_gate.open();
    shim_debug(
        "notification_gate_open",
        format_args!("flushing={}", state.notification_buffer.len()),
    );

    flush_notification_buffer(stdout, state).await
}

async fn flush_notification_buffer<O>(
    stdout: &Arc<Mutex<O>>,
    state: &mut AppState,
) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    while let Some(payload) = state.notification_buffer.front().cloned() {
        write_stdout(stdout, state.output_stdio_mode(), &payload).await?;
        let meta = notification_meta(&payload);
        if let Some(notif_id) = meta.direct_notif_id() {
            state.record_notification_dedup(notif_id);
        }
        state.notification_buffer.pop_front();
    }

    Ok(())
}

struct NotificationMeta {
    notif_id: Option<String>,
    direct: bool,
}

impl NotificationMeta {
    fn direct_notif_id(&self) -> Option<&str> {
        if self.direct {
            self.notif_id.as_deref()
        } else {
            None
        }
    }
}

fn notification_meta(payload: &[u8]) -> NotificationMeta {
    let Ok(v) = serde_json::from_slice::<Value>(payload) else {
        return NotificationMeta {
            notif_id: None,
            direct: false,
        };
    };
    let meta = v
        .get("params")
        .and_then(|p| p.get("meta"))
        .and_then(|m| m.as_object());
    let direct = meta
        .and_then(|m| m.get("kind"))
        .and_then(|v| v.as_str())
        == Some("direct");
    let notif_id = if direct {
        meta
            .and_then(|m| m.get("notifId"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty() && s.len() <= 128)
            .map(ToOwned::to_owned)
    } else {
        None
    };

    NotificationMeta { notif_id, direct }
}

async fn send_delivered(
    tx: &mpsc::Sender<Vec<u8>>,
    epoch: Epoch,
    notif_id: &str,
) -> io::Result<()> {
    send_ctrl(
        tx,
        Ctrl::Delivered {
            notif_id: notif_id.to_string(),
            epoch: epoch.0,
            status: "ok".to_string(),
        },
    )
    .await
}

async fn sweep_expired_pending<O>(stdout: &Arc<Mutex<O>>, state: &mut AppState) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    for (key, request_epoch) in take_expired(&mut state.pending) {
        state.tombstones.insert(request_epoch, &key);
        write_error_respecting_init(state, stdout, &key, "core_timeout", "core timeout").await?;
    }

    Ok(())
}

async fn sleep_with_deadline_sweeps<O>(
    stdout: &Arc<Mutex<O>>,
    state: &mut AppState,
    duration: Duration,
) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    if duration.is_zero() {
        sweep_expired_pending(stdout, state).await?;
        return Ok(());
    }

    let sleep = time::sleep(duration);
    tokio::pin!(sleep);

    let mut timeout_tick = time::interval(TIMEOUT_SWEEP_INTERVAL);
    timeout_tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = &mut sleep => {
                sweep_expired_pending(stdout, state).await?;
                return Ok(());
            }
            _ = timeout_tick.tick() => {
                sweep_expired_pending(stdout, state).await?;
            }
        }
    }
}

fn take_expired(pending: &mut HashMap<String, (Epoch, Instant)>) -> Vec<(String, Epoch)> {
    let now = Instant::now();
    let mut expired = Vec::new();

    pending.retain(|key, (epoch, deadline)| {
        if *deadline <= now {
            expired.push((key.clone(), *epoch));
            false
        } else {
            true
        }
    });

    expired
}

fn take_pre_epoch(
    pending: &mut HashMap<String, (Epoch, Instant)>,
    new_epoch: Epoch,
) -> Vec<(String, Epoch)> {
    let mut failed = Vec::new();

    pending.retain(|key, (epoch, _)| {
        if should_drop_late_reply(new_epoch, *epoch) {
            failed.push((key.clone(), *epoch));
            false
        } else {
            true
        }
    });

    failed
}

async fn write_first_connect_error_if_possible<O>(
    stdout: &Arc<Mutex<O>>,
    stdin_rx: &mut mpsc::Receiver<StdinEvent>,
    state: &mut AppState,
    code: &str,
    message: &str,
) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    if state.cached_init.is_none() {
        if let Ok(Some(StdinEvent::Body(body, mode))) =
            time::timeout(Duration::from_millis(250), stdin_rx.recv()).await
        {
            state.note_stdio_mode(mode);
            remember_initialize(
                &mut state.cached_init,
                &mut state.init_id,
                &mut state.init_key,
                &body,
            );
        }
    }

    if state.cached_init.is_some() {
        let body = jsonrpc_error(state.init_id.clone(), code, message)?;
        write_stdout(stdout, state.output_stdio_mode(), &body).await?;
    }

    Ok(())
}

/// Like `write_error_for_key` but enforces §A.6's single-response invariant for
/// claude's `initialize`: if the init already received its one response,
/// suppress; otherwise this error IS that response (a later success/error for
/// the same init id is then dropped).
async fn write_error_respecting_init<O>(
    state: &mut AppState,
    stdout: &Arc<Mutex<O>>,
    key: &str,
    code: &str,
    message: &str,
) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    if state.init_key.as_deref() == Some(key) {
        if state.init_result_sent {
            return Ok(());
        }
        state.init_result_sent = true;
    }
    write_error_for_key(state, stdout, key, code, message).await
}

async fn write_error_for_key<O>(
    state: &AppState,
    stdout: &Arc<Mutex<O>>,
    key: &str,
    code: &str,
    message: &str,
) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    let body = jsonrpc_error(Some(id_from_key(key)), code, message)?;
    write_stdout(stdout, state.output_stdio_mode(), &body).await
}

fn jsonrpc_error(id: Option<Value>, code: &str, message: &str) -> io::Result<Vec<u8>> {
    serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "error": {
            "code": -32001,
            "message": message,
            "data": { "code": code }
        }
    }))
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

async fn write_stdout<O>(stdout: &Arc<Mutex<O>>, mode: StdioMode, body: &[u8]) -> io::Result<()>
where
    O: AsyncWrite + Unpin,
{
    let (id, method) = peek_id_method(body);
    shim_debug(
        "stdout_write",
        format_args!(
            "mode={mode:?} id={} method={}",
            id.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
            method.unwrap_or_else(|| "-".to_string()),
        ),
    );
    let mut stdout = stdout.lock().await;
    write_stdio(&mut *stdout, body, mode).await
}

fn id_key(id: &Value) -> String {
    match serde_json::to_string(id) {
        Ok(s) => s,
        Err(_) => id.to_string(),
    }
}

fn id_from_key(key: &str) -> Value {
    match serde_json::from_str(key) {
        Ok(value) => value,
        Err(_) => Value::String(key.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::should_track_replayed_init;

    #[test]
    fn replayed_init_tracked_only_while_unanswered() {
        // The cached `initialize` is replayed to a new core on every reconnect,
        // but re-tracked as a timeout-eligible request ONLY before claude's
        // initialize has been answered — otherwise a reconnect whose re-init the
        // new core never answers would emit a SECOND claude response (§A.6 single
        // init response). This guard is one of three independent defenses (with
        // the take_pre_epoch init-skip and write_error_respecting_init); pinned
        // here in isolation so a single-guard regression is caught even though the
        // scenario test is masked by the other two.
        assert!(should_track_replayed_init(false), "track while unanswered");
        assert!(
            !should_track_replayed_init(true),
            "do NOT track once answered"
        );
    }
}
