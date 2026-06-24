export const FRAME_LENGTH_CAP = 16_777_216;
export const FRAME_HEADER_BYTES = 4;
export const FRAME_TYPE_BYTES = 1;
export const FRAME_TYPE_MCP = 0x01;
export const FRAME_TYPE_CTRL = 0x02;
export const FRAME_WRITER_MAX_CHUNK = 256 * 1024;

export type FrameType = typeof FRAME_TYPE_MCP | typeof FRAME_TYPE_CTRL;

export interface DecodedFrame {
  readonly type: FrameType;
  readonly payload: Uint8Array;
}

export type FrameProtocolErrorCode =
  | "FRAME_LENGTH_TOO_LARGE"
  | "FRAME_LENGTH_TOO_SMALL"
  | "FRAME_UNKNOWN_TYPE";

interface FrameProtocolErrorDetails {
  readonly frameLength?: number;
  readonly typeByte?: number;
}

export class FrameProtocolError extends Error {
  readonly code: FrameProtocolErrorCode;
  readonly frameLength: number | undefined;
  readonly typeByte: number | undefined;

  constructor(code: FrameProtocolErrorCode, details: FrameProtocolErrorDetails = {}) {
    super(frameProtocolErrorMessage(code, details));
    this.name = "FrameProtocolError";
    this.code = code;
    this.frameLength = details.frameLength;
    this.typeByte = details.typeByte;
  }
}

function frameProtocolErrorMessage(
  code: FrameProtocolErrorCode,
  details: FrameProtocolErrorDetails,
): string {
  switch (code) {
    case "FRAME_LENGTH_TOO_LARGE":
      return `frame length ${details.frameLength ?? "?"} exceeds cap ${FRAME_LENGTH_CAP}`;
    case "FRAME_LENGTH_TOO_SMALL":
      return `frame length ${details.frameLength ?? "?"} is smaller than 1`;
    case "FRAME_UNKNOWN_TYPE":
      return `unknown frame type ${details.typeByte ?? "?"}`;
  }
}

function readU32Le(bytes: Uint8Array, offset: number): number {
  return new DataView(bytes.buffer, bytes.byteOffset + offset, 4).getUint32(0, true);
}

function writeU32Le(bytes: Uint8Array, offset: number, value: number): void {
  new DataView(bytes.buffer, bytes.byteOffset + offset, 4).setUint32(0, value, true);
}

function assertFrameLength(length: number): void {
  if (length < FRAME_TYPE_BYTES) {
    throw new FrameProtocolError("FRAME_LENGTH_TOO_SMALL", { frameLength: length });
  }

  if (length > FRAME_LENGTH_CAP) {
    throw new FrameProtocolError("FRAME_LENGTH_TOO_LARGE", { frameLength: length });
  }
}

function toFrameType(typeByte: number): FrameType {
  if (typeByte === FRAME_TYPE_MCP || typeByte === FRAME_TYPE_CTRL) {
    return typeByte;
  }

  throw new FrameProtocolError("FRAME_UNKNOWN_TYPE", { typeByte });
}

export function encodeFrame(type: FrameType, payload: Uint8Array): Uint8Array {
  const frameType = toFrameType(type);
  const length = FRAME_TYPE_BYTES + payload.byteLength;
  assertFrameLength(length);

  const out = new Uint8Array(FRAME_HEADER_BYTES + length);
  writeU32Le(out, 0, length);
  out[FRAME_HEADER_BYTES] = frameType;
  out.set(payload, FRAME_HEADER_BYTES + FRAME_TYPE_BYTES);
  return out;
}

export class FrameDecoder {
  // Grow-and-compact buffer (P2 fold): `buf` is a capacity buffer; valid bytes are buf[0..len).
  // Appending into spare capacity (doubling growth) + compacting the remainder to the front is
  // amortized O(n), vs the old re-copy-the-whole-buffer-every-chunk which was O(n^2) for a large
  // frame delivered in many small chunks.
  private buf = new Uint8Array(0);
  private len = 0;

  push(chunk: Uint8Array): DecodedFrame[] {
    if (chunk.byteLength > 0) {
      if (this.len + chunk.byteLength > this.buf.byteLength) {
        const cap = Math.max(this.buf.byteLength * 2, this.len + chunk.byteLength, 64);
        const grown = new Uint8Array(cap);
        grown.set(this.buf.subarray(0, this.len), 0);
        this.buf = grown;
      }
      this.buf.set(chunk, this.len);
      this.len += chunk.byteLength;
    }

    const frames: DecodedFrame[] = [];
    let offset = 0;

    while (this.len - offset >= FRAME_HEADER_BYTES) {
      const available = this.len - offset;
      const frameLength = readU32Le(this.buf, offset);
      assertFrameLength(frameLength);

      if (available >= FRAME_HEADER_BYTES + FRAME_TYPE_BYTES) {
        toFrameType(this.buf[offset + FRAME_HEADER_BYTES]);
      }

      const wireLength = FRAME_HEADER_BYTES + frameLength;
      if (available < wireLength) {
        break;
      }

      const type = toFrameType(this.buf[offset + FRAME_HEADER_BYTES]);
      const payloadStart = offset + FRAME_HEADER_BYTES + FRAME_TYPE_BYTES;
      const payloadEnd = offset + wireLength;
      frames.push({
        type,
        payload: this.buf.slice(payloadStart, payloadEnd),
      });

      offset += wireLength;
    }

    if (offset > 0) {
      this.buf.copyWithin(0, offset, this.len);
      this.len -= offset;
    }

    return frames;
  }

  finish(): DecodedFrame[] {
    this.len = 0;
    return [];
  }

  get bufferedBytes(): number {
    return this.len;
  }
}

export class PendingTimeoutError<K = unknown> extends Error {
  readonly code = "PENDING_TIMEOUT";
  readonly key: K;
  readonly timeoutMs: number;

  constructor(key: K, timeoutMs: number) {
    super(`pending entry timed out after ${timeoutMs}ms`);
    this.name = "PendingTimeoutError";
    this.key = key;
    this.timeoutMs = timeoutMs;
  }
}

export class PendingDuplicateError<K = unknown> extends Error {
  readonly code = "PENDING_DUPLICATE";
  readonly key: K;

  constructor(key: K) {
    super("pending entry already exists");
    this.name = "PendingDuplicateError";
    this.key = key;
  }
}

interface PendingEntry<V> {
  readonly resolve: (value: V) => void;
  readonly reject: (reason: unknown) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

export class PendingMap<K, V> {
  private readonly entries = new Map<K, PendingEntry<V>>();

  get size(): number {
    return this.entries.size;
  }

  register(key: K, timeoutMs: number): Promise<V> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new RangeError("timeoutMs must be a finite non-negative number");
    }

    if (this.entries.has(key)) {
      throw new PendingDuplicateError(key);
    }

    return new Promise<V>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (!this.entries.delete(key)) {
          return;
        }

        reject(new PendingTimeoutError(key, timeoutMs));
      }, timeoutMs);

      this.entries.set(key, { resolve, reject, timer });
    });
  }

  resolve(key: K, value: V): boolean {
    const entry = this.entries.get(key);
    if (entry === undefined) {
      return false;
    }

    this.entries.delete(key);
    clearTimeout(entry.timer);
    entry.resolve(value);
    return true;
  }

  reject(key: K, error: unknown): boolean {
    const entry = this.entries.get(key);
    if (entry === undefined) {
      return false;
    }

    this.entries.delete(key);
    clearTimeout(entry.timer);
    entry.reject(error);
    return true;
  }

  rejectAll(error: unknown): void {
    const entries = Array.from(this.entries.values());
    this.entries.clear();

    for (const entry of entries) {
      clearTimeout(entry.timer);
      entry.reject(error);
    }
  }

  get size(): number {
    return this.entries.size;
  }
}

export type JsonRpcId = string | number;
export type CtrlRequestId = number;

export class ConnectionPending<McpReply, CtrlReply> {
  readonly mcpPending = new PendingMap<JsonRpcId, McpReply>();
  readonly ctrlPending = new PendingMap<CtrlRequestId, CtrlReply>();
}

export type FrameHandler = (
  payload: Uint8Array,
  frame: DecodedFrame,
) => void | Promise<void>;

export interface DispatcherOptions {
  readonly decoder?: FrameDecoder;
  readonly onMcp: FrameHandler;
  readonly onCtrl: FrameHandler;
  readonly onError?: (error: unknown) => void;
  readonly onHandlerError?: (error: unknown, frame: DecodedFrame) => void;
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

export class Dispatcher {
  private readonly decoder: FrameDecoder;
  private running = false;
  private stopped = false;

  constructor(
    private readonly source: AsyncIterable<Uint8Array>,
    private readonly options: DispatcherOptions,
  ) {
    this.decoder = options.decoder ?? new FrameDecoder();
  }

  async start(): Promise<void> {
    if (this.running) {
      throw new Error("dispatcher is already running");
    }

    this.running = true;
    this.stopped = false;

    try {
      for await (const chunk of this.source) {
        if (this.stopped) {
          break;
        }

        for (const frame of this.decoder.push(chunk)) {
          this.dispatch(frame);
        }
      }
    } catch (error) {
      try {
        this.options.onError?.(error);
      } catch {
        // Keep the original protocol/read error as the caller-visible failure.
      }

      throw error;
    } finally {
      this.decoder.finish();
      this.running = false;
    }
  }

  stop(): void {
    this.stopped = true;
    this.decoder.finish();
  }

  private dispatch(frame: DecodedFrame): void {
    const handler = frame.type === FRAME_TYPE_MCP ? this.options.onMcp : this.options.onCtrl;
    const dispatchedFrame: DecodedFrame = {
      type: frame.type,
      payload: frame.payload,
    };

    queueMicrotask(() => {
      try {
        const result = handler(dispatchedFrame.payload, dispatchedFrame);
        if (isPromiseLike(result)) {
          void Promise.resolve(result).catch((error: unknown) => {
            this.options.onHandlerError?.(error, dispatchedFrame);
          });
        }
      } catch (error) {
        this.options.onHandlerError?.(error, dispatchedFrame);
      }
    });
  }
}

export interface FrameWriterOptions {
  readonly highWater: number;
  readonly chunk?: number;
  readonly maxBytes?: number;
  readonly onOverflow: () => void;
}

type FrameWriterLane = "ctrl" | "mcp";

interface FrameWriterItem {
  readonly lane: FrameWriterLane;
  readonly bytes: Uint8Array;
  offset: number;
}

interface IdleWaiter {
  readonly resolve: () => void;
  readonly reject: (error: unknown) => void;
}

export class FrameWriter {
  private readonly highWater: number;
  private readonly chunkSize: number;
  private readonly ctrlQueue: FrameWriterItem[] = [];
  private readonly mcpQueue: FrameWriterItem[] = [];
  private readonly idleWaiters: IdleWaiter[] = [];

  private currentMcp: FrameWriterItem | undefined;
  private currentCtrl: FrameWriterItem | undefined;
  private pendingItems = 0;
  private queuedBytes = 0;
  private pumping = false;
  private paused = false;
  private failed = false;
  private failure: unknown;
  private readonly maxBytes: number | undefined;

  constructor(
    private readonly write: (bytes: Uint8Array) => number | void,
    opts: FrameWriterOptions,
  ) {
    if (!Number.isInteger(opts.highWater) || opts.highWater < 1) {
      throw new RangeError("highWater must be a positive integer");
    }

    const requestedChunk = opts.chunk ?? FRAME_WRITER_MAX_CHUNK;
    if (!Number.isInteger(requestedChunk) || requestedChunk < 1) {
      throw new RangeError("chunk must be a positive integer");
    }

    this.highWater = opts.highWater;
    this.chunkSize = Math.min(requestedChunk, FRAME_WRITER_MAX_CHUNK);
    this.maxBytes = opts.maxBytes;
    this.onOverflow = opts.onOverflow;
  }

  private readonly onOverflow: () => void;

  enqueueCtrl(payload: Uint8Array): void {
    this.enqueue("ctrl", FRAME_TYPE_CTRL, payload);
  }

  enqueueMcp(payload: Uint8Array): void {
    this.enqueue("mcp", FRAME_TYPE_MCP, payload);
  }

  drain(): Promise<void> {
    if (this.failed) {
      return Promise.reject(this.failure);
    }

    if (!this.pumping && !this.paused && this.pendingItems === 0) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve, reject) => {
      this.idleWaiters.push({ resolve, reject });
    });
  }

  private enqueue(lane: FrameWriterLane, type: FrameType, payload: Uint8Array): void {
    if (this.failed) {
      throw this.failure;
    }

    const bytes = encodeFrame(type, payload);
    if (
      this.pendingItems >= this.highWater ||
      (this.maxBytes !== undefined && this.queuedBytes + bytes.byteLength > this.maxBytes)
    ) {
      this.onOverflow();
      this.fail(new Error("frame writer overflow"));
      return;
    }

    const item: FrameWriterItem = { lane, bytes, offset: 0 };

    if (lane === "ctrl") {
      this.ctrlQueue.push(item);
    } else {
      this.mcpQueue.push(item);
    }

    this.pendingItems += 1;
    this.queuedBytes += bytes.byteLength;
    this.schedule();
  }

  private schedule(): void {
    if (this.pumping || this.failed || this.paused) {
      return;
    }

    this.pumping = true;
    this.pumpLoop();
  }

  /** Resume pumping after a short write paused us (called by the socket's drain handler). */
  resume(): void {
    if (this.paused && !this.failed) {
      this.paused = false;
      this.schedule();
    }
  }

  /**
   * Fail the writer, REJECTING all pending drain() awaits — call on connection teardown so an
   * in-flight makeUdsInject/transport.send drain (e.g. paused mid-flush at teardown) settles
   * promptly (rejects -> the inbox naks -> JetStream redelivers) instead of leaking forever.
   */
  close(err: unknown = new Error("frame writer closed")): void {
    if (!this.failed) this.fail(err);
  }

  private pumpLoop(): void {
    try {
      while (true) {
        // A frame's bytes (header+payload) MUST be written CONTIGUOUSLY: the length-prefixed
        // receiver reads exactly `len` bytes for a frame, so NO other frame (CTRL or MCP) may be
        // interleaved mid-frame or the receiver desyncs/corrupts. Continue any IN-PROGRESS frame
        // first; switch lanes (CTRL has priority over not-yet-started MCP) ONLY at a frame boundary.
        // (CTRL is thus delayed by at most one in-flight MCP frame's flush — bounded + fast on UDS.)
        let item: FrameWriterItem | undefined;
        let isCtrl: boolean;
        if (this.currentCtrl !== undefined) {
          item = this.currentCtrl;
          isCtrl = true;
        } else if (this.currentMcp !== undefined) {
          item = this.currentMcp;
          isCtrl = false;
        } else {
          this.currentCtrl = this.ctrlQueue.shift();
          if (this.currentCtrl !== undefined) {
            item = this.currentCtrl;
            isCtrl = true;
          } else {
            this.currentMcp = this.mcpQueue.shift();
            item = this.currentMcp;
            isCtrl = false;
          }
        }

        if (item === undefined) break;

        const end = Math.min(item.bytes.byteLength, item.offset + this.chunkSize);
        const unit = item.bytes.subarray(item.offset, end);
        const r = this.write(unit);
        const n = typeof r === "number" ? r : unit.byteLength; // void => fully accepted (in-memory fake)

        if (!Number.isFinite(n) || n < 0 || n > unit.byteLength) {
          throw new Error("write() returned invalid byte count: " + String(r));
        }

        item.offset += n;
        this.queuedBytes -= n;

        if (n < unit.byteLength) {
          // SHORT WRITE — OS send buffer is full; pause and wait for the socket drain handler
          // to call resume(). Do NOT advance past the accepted bytes (no silent byte-drop).
          this.paused = true;
          this.pumping = false;
          return;
        }

        if (item.offset >= item.bytes.byteLength) {
          if (isCtrl) this.currentCtrl = undefined;
          else this.currentMcp = undefined;
          this.pendingItems -= 1;
        }
      }

      this.pumping = false;
      this.resolveIdle();
    } catch (error) {
      this.fail(error);
    }
  }

  private fail(error: unknown): void {
    this.failed = true;
    this.failure = error;
    this.pumping = false;
    this.paused = false;
    this.ctrlQueue.length = 0;
    this.mcpQueue.length = 0;
    this.currentMcp = undefined;
    this.currentCtrl = undefined;
    this.pendingItems = 0;
    this.queuedBytes = 0;

    const waiters = this.idleWaiters.splice(0);
    for (const waiter of waiters) {
      waiter.reject(error);
    }
  }

  private resolveIdle(): void {
    if (this.pendingItems !== 0) {
      return;
    }

    const waiters = this.idleWaiters.splice(0);
    for (const waiter of waiters) {
      waiter.resolve();
    }
  }
}
