import { describe, expect, test } from "bun:test";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";
import { PendingMap, type CtrlRequestId, type FrameWriter } from "../uds_frame.ts";
import {
  UdsServerTransport,
  feedCtrlDelivered,
  feedCtrlSig,
  makeUdsInject,
  makeUdsSign,
  type DeliveryReply,
  type SignReply,
} from "../uds_transport.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

type CapturedFrame = { type: "mcp" | "ctrl"; payload: Uint8Array };

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, resolve, reject };
}

class FakeWriter {
  readonly frames: CapturedFrame[] = [];
  drainCalls = 0;

  private drainDeferred = deferred<void>();

  enqueueCtrl(payload: Uint8Array): void {
    this.frames.push({ type: "ctrl", payload: new Uint8Array(payload) });
  }

  enqueueMcp(payload: Uint8Array): void {
    this.frames.push({ type: "mcp", payload: new Uint8Array(payload) });
  }

  drain(): Promise<void> {
    this.drainCalls++;
    return this.drainDeferred.promise;
  }

  resolveDrain(): void {
    this.drainDeferred.resolve();
  }

  asFrameWriter(): FrameWriter {
    return this as unknown as FrameWriter;
  }
}

async function tick(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

function jsonPayload(value: unknown): Uint8Array {
  return enc.encode(JSON.stringify(value));
}

function decodeJson<T = any>(payload: Uint8Array): T {
  return JSON.parse(dec.decode(payload)) as T;
}

function ctrlAt(writer: FakeWriter, index: number): any {
  expect(writer.frames[index]?.type).toBe("ctrl");
  return decodeJson(writer.frames[index].payload);
}

describe("makeUdsInject", () => {
  test("enqueues one MCP frame and resolves only after drain", async () => {
    const writer = new FakeWriter();
    const inject = makeUdsInject(writer.asFrameWriter());
    const notif = { jsonrpc: "2.0", method: "notifications/message", params: { body: "hi" } };

    let resolved = false;
    const promise = inject(notif).then(() => {
      resolved = true;
    });

    await tick();

    expect(resolved).toBe(false);
    expect(writer.drainCalls).toBe(1);
    expect(writer.frames).toHaveLength(1);
    expect(writer.frames[0].type).toBe("mcp");
    expect(decodeJson(writer.frames[0].payload)).toEqual(notif);

    writer.resolveDrain();
    await promise;

    expect(resolved).toBe(true);
  });

  test("direct inbox notification resolves only after matching delivered ctrl", async () => {
    const writer = new FakeWriter();
    const pending = new PendingMap<string, DeliveryReply>();
    const inject = makeUdsInject(writer.asFrameWriter(), {
      pending,
      epoch: 7,
      timeoutMs: 1000,
      maxInflight: 4,
    });
    const notif = {
      jsonrpc: "2.0",
      method: "notifications/claude/channel",
      params: { content: "hi", meta: { kind: "direct", id: "n1", notifId: "n1" } },
    };

    let resolved = false;
    const promise = inject(notif).then(() => {
      resolved = true;
    });

    await tick();
    writer.resolveDrain();
    await tick();

    expect(resolved).toBe(false);
    expect(pending.size).toBe(1);

    feedCtrlDelivered(jsonPayload({ t: "delivered", notifId: "n1", epoch: 7, status: "ok" }), pending, 7);
    await promise;

    expect(resolved).toBe(true);
    expect(pending.size).toBe(0);
  });

  test("topic notification keeps flush-only behavior, even with sender-supplied notifId", async () => {
    const writer = new FakeWriter();
    const pending = new PendingMap<string, DeliveryReply>();
    const inject = makeUdsInject(writer.asFrameWriter(), {
      pending,
      epoch: 7,
      timeoutMs: 1000,
      maxInflight: 4,
    });

    const promise = inject({
      jsonrpc: "2.0",
      method: "notifications/claude/channel",
      params: { content: "hi", meta: { kind: "topic", id: "t1", notifId: "n1" } },
    });

    await tick();
    writer.resolveDrain();
    await promise;

    expect(pending.size).toBe(0);
  });
});

describe("UdsServerTransport", () => {
  test("send enqueues one MCP frame and resolves only after drain", async () => {
    const writer = new FakeWriter();
    const transport = new UdsServerTransport(writer.asFrameWriter());
    const message = { jsonrpc: "2.0", id: 1, method: "tools/list" } as const;

    let resolved = false;
    const promise = transport.send(message).then(() => {
      resolved = true;
    });

    await tick();

    expect(resolved).toBe(false);
    expect(writer.drainCalls).toBe(1);
    expect(writer.frames).toHaveLength(1);
    expect(writer.frames[0].type).toBe("mcp");
    expect(decodeJson(writer.frames[0].payload)).toEqual(message);

    writer.resolveDrain();
    await promise;

    expect(resolved).toBe(true);
  });

  test("feedMcp parses valid payloads and calls onmessage once", () => {
    const writer = new FakeWriter();
    const transport = new UdsServerTransport(writer.asFrameWriter());
    const seen: JSONRPCMessage[] = [];
    const message = { jsonrpc: "2.0", id: 2, result: { ok: true } } as const;

    transport.onmessage = (msg) => {
      seen.push(msg);
    };

    transport.feedMcp(jsonPayload(message));

    expect(seen).toEqual([message]);
  });

  test("feedMcp reports malformed JSON without throwing or calling onmessage", () => {
    const writer = new FakeWriter();
    const transport = new UdsServerTransport(writer.asFrameWriter());
    const seen: JSONRPCMessage[] = [];
    const errors: Error[] = [];

    transport.onmessage = (msg) => {
      seen.push(msg);
    };
    transport.onerror = (error) => {
      errors.push(error);
    };

    expect(() => transport.feedMcp(enc.encode("{not json"))).not.toThrow();

    expect(seen).toHaveLength(0);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(Error);
  });

  test("close is idempotent and calls onclose once", async () => {
    const writer = new FakeWriter();
    const transport = new UdsServerTransport(writer.asFrameWriter());
    let closeCount = 0;

    transport.onclose = () => {
      closeCount++;
    };

    await transport.close();
    await transport.close();

    expect(closeCount).toBe(1);
  });
});

describe("makeUdsSign and feedCtrlSig", () => {
  test("round-trips one sign request through a sig reply", async () => {
    const writer = new FakeWriter();
    const pending = new PendingMap<CtrlRequestId, SignReply>();
    const sign = makeUdsSign({ writer: writer.asFrameWriter(), pending, timeoutMs: 1000 });

    const promise = sign("abc");

    expect(writer.frames).toHaveLength(1);
    expect(ctrlAt(writer, 0)).toEqual({ t: "sign", reqId: 1, canon: "abc" });

    feedCtrlSig(jsonPayload({ t: "sig", reqId: 1, sig: "AAAA" }), pending);

    await expect(promise).resolves.toBe("AAAA");
  });

  test("allows two concurrent signs to resolve independently", async () => {
    const writer = new FakeWriter();
    const pending = new PendingMap<CtrlRequestId, SignReply>();
    const sign = makeUdsSign({ writer: writer.asFrameWriter(), pending, timeoutMs: 1000 });

    const first = sign("one");
    const second = sign("two");

    expect(ctrlAt(writer, 0)).toEqual({ t: "sign", reqId: 1, canon: "one" });
    expect(ctrlAt(writer, 1)).toEqual({ t: "sign", reqId: 2, canon: "two" });

    feedCtrlSig(jsonPayload({ t: "sig", reqId: 2, sig: "BBBB" }), pending);
    feedCtrlSig(jsonPayload({ t: "sig", reqId: 1, sig: "AAAA" }), pending);

    await expect(first).resolves.toBe("AAAA");
    await expect(second).resolves.toBe("BBBB");
  });

  test("rejects when the shim returns sig-err", async () => {
    const writer = new FakeWriter();
    const pending = new PendingMap<CtrlRequestId, SignReply>();
    const sign = makeUdsSign({ writer: writer.asFrameWriter(), pending, timeoutMs: 1000 });

    const promise = sign("bad");

    feedCtrlSig(jsonPayload({ t: "sig-err", reqId: 1, err: "denied" }), pending);

    await expect(promise).rejects.toThrow("shim_sign_error: denied");
  });

  test("rejects when no reply arrives before timeout", async () => {
    const writer = new FakeWriter();
    const pending = new PendingMap<CtrlRequestId, SignReply>();
    const sign = makeUdsSign({ writer: writer.asFrameWriter(), pending, timeoutMs: 1 });

    await expect(sign("no-reply")).rejects.toThrow();
  });

  test("ignores unknown and malformed CTRL without resolving pending entries", async () => {
    const pending = new PendingMap<CtrlRequestId, SignReply>();
    const errors: unknown[] = [];
    const promise = pending.register(7, 1000);

    let settled = false;
    promise.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );

    expect(() => feedCtrlSig(jsonPayload({ t: "other", reqId: 7, sig: "NO" }), pending, (e) => errors.push(e))).not.toThrow();
    expect(() => feedCtrlSig(enc.encode("{not json"), pending, (e) => errors.push(e))).not.toThrow();

    await tick();

    expect(settled).toBe(false);
    expect(errors).toHaveLength(2);

    feedCtrlSig(jsonPayload({ t: "sig", reqId: 7, sig: "YES" }), pending);

    await expect(promise).resolves.toEqual({ ok: true, sig: "YES" });
  });
});

describe("feedCtrlDelivered", () => {
  test("rejects wrong epoch and unknown notifId without resolving pending", async () => {
    const pending = new PendingMap<string, DeliveryReply>();
    const errors: unknown[] = [];
    const promise = pending.register("n1", 1000);

    let settled = false;
    promise.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );

    expect(feedCtrlDelivered(jsonPayload({ t: "delivered", notifId: "n1", epoch: 8, status: "ok" }), pending, 7, (e) => errors.push(e))).toBe(true);
    expect(feedCtrlDelivered(jsonPayload({ t: "delivered", notifId: "missing", epoch: 7, status: "ok" }), pending, 7, (e) => errors.push(e))).toBe(true);

    await tick();

    expect(settled).toBe(false);
    expect(errors).toHaveLength(2);

    feedCtrlDelivered(jsonPayload({ t: "delivered", notifId: "n1", epoch: 7, status: "ok" }), pending, 7);
    await expect(promise).resolves.toEqual({ ok: true, epoch: 7 });
  });
});
