import { describe, expect, test } from "bun:test";
import {
  FRAME_LENGTH_CAP,
  FrameDecoder,
  FrameProtocolError,
  FrameWriter,
  PendingMap,
  PendingTimeoutError,
  encodeFrame,
  type DecodedFrame,
} from "../uds_frame";

function concatBytes(...parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;

  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }

  return out;
}

function readU32Le(bytes: Uint8Array): number {
  return new DataView(bytes.buffer, bytes.byteOffset, 4).getUint32(0, true);
}

function header(length: number, type?: number): Uint8Array {
  const out = new Uint8Array(type === undefined ? 4 : 5);
  new DataView(out.buffer).setUint32(0, length, true);
  if (type !== undefined) {
    out[4] = type;
  }

  return out;
}

function expectFrameError(fn: () => unknown, code: FrameProtocolError["code"]): void {
  let error: unknown;

  try {
    fn();
  } catch (caught) {
    error = caught;
  }

  expect(error).toBeInstanceOf(FrameProtocolError);
  expect((error as FrameProtocolError).code).toBe(code);
}

function filledBytes(length: number, seed = 0): Uint8Array {
  const out = new Uint8Array(length);
  for (let index = 0; index < out.byteLength; index += 1) {
    out[index] = (index + seed) & 0xff;
  }

  return out;
}

describe("frame codec", () => {
  test("encodes and decodes both frame types, including empty payloads", () => {
    const mcpPayload = new Uint8Array([10, 11, 12]);
    const ctrlPayload = new Uint8Array();

    const mcpWire = encodeFrame(1, mcpPayload);
    const ctrlWire = encodeFrame(2, ctrlPayload);

    expect(readU32Le(mcpWire)).toBe(4);
    expect(readU32Le(ctrlWire)).toBe(1);
    expect(ctrlWire.byteLength).toBe(5);

    const decoder = new FrameDecoder();
    const frames = decoder.push(concatBytes(mcpWire, ctrlWire));

    expect(frames).toHaveLength(2);
    expect(frames[0]!.type).toBe(1);
    expect(Array.from(frames[0]!.payload)).toEqual([10, 11, 12]);
    expect(frames[1]!.type).toBe(2);
    expect(Array.from(frames[1]!.payload)).toEqual([]);
  });

  test("reassembles a frame split across multiple chunks", () => {
    const payload = new Uint8Array([1, 2, 3, 4, 5]);
    const wire = encodeFrame(1, payload);
    const decoder = new FrameDecoder();

    expect(decoder.push(wire.slice(0, 2))).toHaveLength(0);
    expect(decoder.push(wire.slice(2, 5))).toHaveLength(0);

    const frames = decoder.push(wire.slice(5));
    expect(frames).toHaveLength(1);
    expect(frames[0]!.type).toBe(1);
    expect(Array.from(frames[0]!.payload)).toEqual([1, 2, 3, 4, 5]);
  });

  test("discards a trailing partial frame on finish", () => {
    const decoder = new FrameDecoder();
    const partial = encodeFrame(1, new Uint8Array([1, 2, 3, 4])).slice(0, 7);

    expect(decoder.push(partial)).toHaveLength(0);
    expect(decoder.finish()).toHaveLength(0);

    const frames = decoder.push(encodeFrame(2, new Uint8Array()));
    expect(frames).toHaveLength(1);
    expect(frames[0]!.type).toBe(2);
    expect(frames[0]!.payload.byteLength).toBe(0);
  });

  test("rejects length greater than cap", () => {
    const decoder = new FrameDecoder();

    expectFrameError(
      () => decoder.push(header(FRAME_LENGTH_CAP + 1)),
      "FRAME_LENGTH_TOO_LARGE",
    );
  });

  test("rejects length smaller than one", () => {
    const decoder = new FrameDecoder();

    expectFrameError(() => decoder.push(header(0)), "FRAME_LENGTH_TOO_SMALL");
  });

  test("rejects unknown frame type", () => {
    const decoder = new FrameDecoder();

    expectFrameError(() => decoder.push(header(1, 0xff)), "FRAME_UNKNOWN_TYPE");
  });
});

describe("PendingMap", () => {
  test("register resolves by key", async () => {
    const pending = new PendingMap<string, string>();
    const promise = pending.register("rpc-1", 1_000);

    expect(pending.resolve("rpc-1", "ok")).toBe(true);
    await expect(promise).resolves.toBe("ok");
    expect(pending.resolve("rpc-1", "again")).toBe(false);
    expect(pending.size).toBe(0);
  });

  test("register rejects on timeout", async () => {
    const pending = new PendingMap<number, string>();
    const promise = pending.register(7, 5);
    let error: unknown;

    try {
      await promise;
    } catch (caught) {
      error = caught;
    }

    expect(error).toBeInstanceOf(PendingTimeoutError);
    expect((error as PendingTimeoutError<number>).code).toBe("PENDING_TIMEOUT");
    expect((error as PendingTimeoutError<number>).key).toBe(7);
    expect(pending.size).toBe(0);
  });
});

describe("FrameWriter", () => {
  test("a CTRL enqueued mid-MCP is written only AFTER the MCP frame completes (contiguous frames)", async () => {
    // WIRE-CORRECTNESS (gate-fold, dev-pm receiver-side catch): a frame's bytes must be CONTIGUOUS
    // on the wire — the length-prefixed receiver reads `len` bytes for a frame, so a CTRL injected
    // mid-MCP-frame would corrupt the read. CTRL has priority over NOT-YET-STARTED MCP, but is
    // delayed past an IN-FLIGHT MCP frame to the next boundary.
    const largePayload = filledBytes(40);
    const ctrlPayload = new Uint8Array([99, 100]);
    const writes: Uint8Array[] = [];

    let writer!: FrameWriter;
    let queuedCtrl = false;

    writer = new FrameWriter(
      (bytes) => {
        writes.push(bytes.slice());
        if (!queuedCtrl) {
          queuedCtrl = true;
          writer.enqueueCtrl(ctrlPayload); // enqueue a CTRL WHILE the MCP frame is mid-write
        }
      },
      { highWater: 8, chunk: 12, onOverflow: () => { throw new Error("unexpected overflow"); } },
    );

    writer.enqueueMcp(largePayload);
    await writer.drain();

    // The MCP frame took >1 chunk (proves CTRL was enqueued mid-frame), yet decoding the reassembled
    // wire must yield exactly TWO frames in order: the MCP frame in FULL, then the CTRL frame.
    expect(writes.length).toBeGreaterThan(2);
    const decoder = new FrameDecoder();
    const decoded: DecodedFrame[] = [];
    for (const w of writes) decoded.push(...decoder.push(w));
    expect(decoder.bufferedBytes).toBe(0); // no partial/garbled frame left over
    expect(decoded.length).toBe(2);
    expect(decoded[0]!.type).toBe(1); // MCP first, contiguous
    expect(Array.from(decoded[0]!.payload)).toEqual(Array.from(largePayload));
    expect(decoded[1]!.type).toBe(2); // CTRL only after the MCP frame completed
    expect(Array.from(decoded[1]!.payload)).toEqual(Array.from(ctrlPayload));
  });

  test("calls onOverflow and fails closed when high-water mark is exceeded", async () => {
    let overflowCount = 0;
    // A write that accepts 0 bytes is a short write: the pump pauses and the first item stays
    // queued, so a second enqueue trips the high-water mark. Overflow is now fail-CLOSED
    // (onOverflow + fail) so a dropped frame can never falsely resolve drain() / ack a durable msg.
    const writer = new FrameWriter(() => 0, {
      highWater: 1,
      onOverflow: () => {
        overflowCount += 1;
      },
    });

    writer.enqueueMcp(new Uint8Array([1])); // pendingItems=1, pump pauses (wrote 0)
    writer.enqueueCtrl(new Uint8Array([2])); // pendingItems(1) >= highWater(1) -> overflow

    expect(overflowCount).toBe(1);
    await expect(writer.drain()).rejects.toThrow("frame writer overflow");
    expect(() => writer.enqueueMcp(new Uint8Array([3]))).toThrow("frame writer overflow");
  });
});

describe("FrameWriter backpressure", () => {
  function concatBackpressureChunks(parts: readonly Uint8Array[]): Uint8Array {
    const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    for (const part of parts) {
      out.set(part, offset);
      offset += part.byteLength;
    }
    return out;
  }

  function expectBackpressureBytes(actual: Uint8Array, expected: Uint8Array): void {
    expect(Array.from(actual)).toEqual(Array.from(expected));
  }

  async function encodedByVoidWriter(lane: "ctrl" | "mcp", payload: Uint8Array): Promise<Uint8Array> {
    const chunks: Uint8Array[] = [];
    const writer = new FrameWriter(
      (bytes) => {
        chunks.push(bytes.slice());
      },
      { highWater: 1024, onOverflow: () => {} },
    );

    if (lane === "ctrl") writer.enqueueCtrl(payload);
    else writer.enqueueMcp(payload);

    await writer.drain();
    return concatBackpressureChunks(chunks);
  }

  test("short write pauses, resume continues, drain resolves only after full write", async () => {
    let accept = 10;
    const written: number[] = [];
    const chunks: Uint8Array[] = [];
    const payload = new Uint8Array(50).fill(0x5a);

    const writer = new FrameWriter(
      (bytes) => {
        const n = Math.min(bytes.byteLength, accept);
        written.push(n);
        chunks.push(bytes.slice(0, n));
        return n;
      },
      { highWater: 1024, maxBytes: 64 * 1024 * 1024, onOverflow: () => {} },
    );

    writer.enqueueMcp(payload);

    let drained = false;
    const drainPromise = writer.drain().then(() => {
      drained = true;
    });

    await Promise.resolve();
    expect(drained).toBe(false);

    accept = Infinity;
    writer.resume();
    await drainPromise;

    const expected = await encodedByVoidWriter("mcp", payload);
    const actual = concatBackpressureChunks(chunks);

    expect(written.reduce((sum, n) => sum + n, 0)).toBe(expected.byteLength);
    expectBackpressureBytes(actual, expected);
  });

  test("an in-flight MCP frame completes contiguously before a CTRL queued mid-pause", async () => {
    let accept = 10;
    const chunks: Uint8Array[] = [];
    const mcpPayload = new Uint8Array(50).fill(0x6d);
    const ctrlPayload = new Uint8Array([0x63, 0x74, 0x72, 0x6c]);

    const writer = new FrameWriter(
      (bytes) => {
        const n = Math.min(bytes.byteLength, accept);
        chunks.push(bytes.slice(0, n));
        return n;
      },
      { highWater: 1024, maxBytes: 64 * 1024 * 1024, onOverflow: () => {} },
    );

    writer.enqueueMcp(mcpPayload); // short write (accept=10) pauses mid-MCP-frame
    writer.enqueueCtrl(ctrlPayload); // CTRL queued WHILE the MCP frame is paused mid-write

    accept = Infinity;
    const drainPromise = writer.drain();
    writer.resume();
    await drainPromise;

    // Contiguity (gate-fold): the MCP frame resumes to COMPLETION first, THEN the CTRL — the CTRL
    // is NEVER interleaved into the paused MCP frame's bytes (length-prefixed receiver reads `len`).
    const mcpFrame = await encodedByVoidWriter("mcp", mcpPayload);
    const ctrlFrame = await encodedByVoidWriter("ctrl", ctrlPayload);
    expectBackpressureBytes(concatBackpressureChunks(chunks), concatBackpressureChunks([mcpFrame, ctrlFrame]));

    // Reassembled wire decodes to [MCP, CTRL] in order, nothing garbled.
    const decoder = new FrameDecoder();
    const decoded: DecodedFrame[] = [];
    for (const c of chunks) decoded.push(...decoder.push(c));
    expect(decoder.bufferedBytes).toBe(0);
    expect(decoded.map((f) => f.type)).toEqual([1, 2]);
    expect(Array.from(decoded[0]!.payload)).toEqual(Array.from(mcpPayload));
    expect(Array.from(decoded[1]!.payload)).toEqual(Array.from(ctrlPayload));
  });

  test("maxBytes overflow fails closed", async () => {
    let overflowed = false;
    const writer = new FrameWriter(
      () => {},
      {
        highWater: 1024,
        maxBytes: 100,
        onOverflow: () => {
          overflowed = true;
        },
      },
    );

    writer.enqueueMcp(new Uint8Array(256));

    expect(overflowed).toBe(true);
    await expect(writer.drain()).rejects.toThrow("frame writer overflow");
    expect(() => writer.enqueueCtrl(new Uint8Array([1]))).toThrow("frame writer overflow");
  });

  test("void write callback still treats bytes as fully accepted", async () => {
    const writer = new FrameWriter(
      () => {},
      { highWater: 1024, onOverflow: () => {} },
    );

    writer.enqueueMcp(new Uint8Array(16));
    await writer.drain();
  });
});
