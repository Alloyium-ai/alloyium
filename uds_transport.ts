import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";
import type { CtrlRequestId, FrameWriter, PendingMap } from "./uds_frame.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function reportError(onError: ((error: unknown) => void) | undefined, error: unknown): void {
  if (!onError) return;

  try {
    onError(error);
  } catch {
    // Error callbacks must not let frame dispatch escape.
  }
}

export class UdsServerTransport implements Transport {
  onmessage?: (message: JSONRPCMessage, extra?: any) => void;
  onclose?: () => void;
  onerror?: (error: Error) => void;

  private closed = false;

  constructor(private readonly writer: FrameWriter) {}

  async start(): Promise<void> {
    return;
  }

  async send(message: JSONRPCMessage, options?: any): Promise<void> {
    void options;
    this.writer.enqueueMcp(enc.encode(JSON.stringify(message)));
    await this.writer.drain();
  }

  async close(): Promise<void> {
    if (this.closed) return;

    this.closed = true;
    this.onclose?.();
  }

  feedMcp(payload: Uint8Array): void {
    let message: JSONRPCMessage;

    try {
      message = JSON.parse(dec.decode(payload)) as JSONRPCMessage;
    } catch (error) {
      reportError(this.onerror, asError(error));
      return;
    }

    this.onmessage?.(message);
  }
}

export type DeliveryReply = { ok: true; epoch: number };

export function makeUdsInject(
  writer: FrameWriter,
  delivery?: {
    pending: PendingMap<string, DeliveryReply>;
    epoch: number;
    timeoutMs: number;
    maxInflight: number;
    onEvent?: (event: string, fields?: Record<string, unknown>) => void;
  },
): (notif: unknown) => Promise<void> {
  // ph2 compatibility: without delivery options this resolves after the frame flushes
  // to the OS socket buffer. ph3 direct-inbox callers pass delivery options and wait
  // for a shim/session CTRL {t:"delivered"} for the notification id.
  return async (notif: unknown): Promise<void> => {
    const notifId = directNotifId(notif);
    const waitForDelivery = !!delivery && notifId !== undefined;
    let delivered: Promise<DeliveryReply> | undefined;

    if (waitForDelivery) {
      if (delivery!.pending.size >= delivery!.maxInflight) {
        delivery!.onEvent?.("a2a_delivery_inflight_full", { notifId, epoch: delivery!.epoch, max: delivery!.maxInflight });
        throw new Error("delivery inflight full");
      }
      delivered = delivery!.pending.register(notifId, delivery!.timeoutMs);
      delivery!.onEvent?.("a2a_delivery_pending_registered", { notifId, epoch: delivery!.epoch });
    }

    try {
      writer.enqueueMcp(enc.encode(JSON.stringify(notif)));
      await writer.drain();
      if (!delivered) return;
      await delivered;
      delivery!.onEvent?.("a2a_delivery_ack_received", { notifId, epoch: delivery!.epoch });
    } catch (err) {
      if (waitForDelivery) {
        delivery!.pending.reject(notifId!, err);
        void delivered?.catch(() => {});
      }
      if (err instanceof Error && err.name === "PendingTimeoutError") {
        delivery!.onEvent?.("a2a_delivery_ack_timeout", { notifId, epoch: delivery!.epoch });
      }
      throw err;
    }
  };
}

function directNotifId(notif: unknown): string | undefined {
  if (!notif || typeof notif !== "object") return undefined;
  const params = (notif as Record<string, unknown>).params;
  if (!params || typeof params !== "object") return undefined;
  const meta = (params as Record<string, unknown>).meta;
  if (!meta || typeof meta !== "object") return undefined;
  const r = meta as Record<string, unknown>;
  const id = typeof r.notifId === "string" ? r.notifId : r.id;
  return r.kind === "direct" && typeof id === "string" && id.length > 0 ? id : undefined;
}

export type SignReply = { ok: true; sig: string } | { ok: false; err: string };

export function makeUdsSign(opts: {
  writer: FrameWriter;
  pending: PendingMap<CtrlRequestId, SignReply>;
  timeoutMs: number;
}): (canon: string) => Promise<string> {
  let nextReqId = 1;

  return async (canon: string): Promise<string> => {
    const id = nextReqId++ as CtrlRequestId;
    const reply = opts.pending.register(id, opts.timeoutMs);

    try {
      opts.writer.enqueueCtrl(enc.encode(JSON.stringify({ t: "sign", reqId: id, canon })));
    } catch (err) {
      // enqueue failed (e.g. the writer overflowed/failed) — resolve the just-registered pending so
      // it does not dangle until timeoutMs; the await below then throws shim_sign_error.
      opts.pending.resolve(id, { ok: false, err: "sign_enqueue_failed: " + (err instanceof Error ? err.message : String(err)) });
    }

    const result = await reply;
    if (result.ok) return result.sig;

    throw new Error("shim_sign_error: " + result.err);
  };
}

export function feedCtrlSig(
  payload: Uint8Array,
  pending: PendingMap<CtrlRequestId, SignReply>,
  onError?: (e: unknown) => void,
): void {
  let message: unknown;

  try {
    message = JSON.parse(dec.decode(payload));
  } catch (error) {
    reportError(onError, error);
    return;
  }

  if (!message || typeof message !== "object") {
    reportError(onError, new Error("malformed sign ctrl"));
    return;
  }

  const ctrl = message as Record<string, unknown>;

  if (ctrl.t === "sig" && typeof ctrl.reqId === "number" && typeof ctrl.sig === "string") {
    // resolve() is false for an unknown/late reqId (no live pending) — surface it as protocol noise.
    if (!pending.resolve(ctrl.reqId as CtrlRequestId, { ok: true, sig: ctrl.sig })) {
      reportError(onError, new Error("sig for unknown/late reqId " + ctrl.reqId));
    }
    return;
  }

  if (ctrl.t === "sig-err" && typeof ctrl.reqId === "number") {
    if (!pending.resolve(ctrl.reqId as CtrlRequestId, { ok: false, err: String(ctrl.err ?? "sig-err") })) {
      reportError(onError, new Error("sig-err for unknown/late reqId " + ctrl.reqId));
    }
    return;
  }

  reportError(onError, new Error("unknown or malformed sign ctrl"));
}

export function feedCtrlDelivered(
  payload: Uint8Array,
  pending: PendingMap<string, DeliveryReply>,
  epoch: number,
  onError?: (e: unknown) => void,
): boolean {
  let message: unknown;

  try {
    message = JSON.parse(dec.decode(payload));
  } catch (error) {
    reportError(onError, error);
    return false;
  }

  if (!message || typeof message !== "object") {
    reportError(onError, new Error("malformed delivered ctrl"));
    return false;
  }

  const ctrl = message as Record<string, unknown>;
  if (ctrl.t !== "delivered") return false;

  if (
    typeof ctrl.notifId !== "string" ||
    ctrl.notifId.length === 0 ||
    ctrl.notifId.length > 128 ||
    ctrl.epoch !== epoch ||
    ctrl.status !== "ok"
  ) {
    reportError(onError, new Error("malformed delivered ctrl"));
    return true;
  }

  if (!pending.resolve(ctrl.notifId, { ok: true, epoch })) {
    reportError(onError, new Error("delivered for unknown/late notifId " + ctrl.notifId));
  }
  return true;
}
