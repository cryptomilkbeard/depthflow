import WebSocket from "ws";
import { LevelTracker } from "./tracker";
import { Side } from "./types";

type BookMap = Map<number, number>;

interface BookState {
  bids: BookMap;
  asks: BookMap;
  tracker: LevelTracker;
}

interface MexcOrderbookOptions {
  symbols: string[];
  depth: number;
  wsUrl?: string;
  onReady?: () => void;
  onUpdate?: (symbol: string, bids: Array<[number, number]>, asks: Array<[number, number]>, tracker: LevelTracker) => void;
}

const DEFAULT_WS_URL = "wss://contract.mexc.com/edge";
const SUPPORTED_DEPTHS = [5, 10, 20];

function toNumber(raw: string | number): number {
  return Number(raw);
}

function sortLevels(map: BookMap, side: Side, depth: number): Array<[number, number]> {
  const entries = Array.from(map.entries());
  entries.sort((a, b) => (side === "bids" ? b[0] - a[0] : a[0] - b[0]));
  return entries.slice(0, depth);
}

function normalizeLevel(raw: unknown): [number, number] | null {
  if (Array.isArray(raw) && raw.length >= 2) {
    const price = toNumber(raw[0] as string | number);
    const size = toNumber(raw[1] as string | number);
    if (Number.isFinite(price) && Number.isFinite(size)) return [price, size];
  }
  if (raw && typeof raw === "object") {
    const record = raw as Record<string, string | number>;
    const price = toNumber(record.p ?? record.price ?? record[0] ?? 0);
    const size = toNumber(record.q ?? record.qty ?? record.size ?? record[1] ?? 0);
    if (Number.isFinite(price) && Number.isFinite(size)) return [price, size];
  }
  return null;
}

function normalizeSide(raw: unknown): Array<[number, number]> {
  if (!Array.isArray(raw)) return [];
  const result: Array<[number, number]> = [];
  for (const item of raw) {
    const level = normalizeLevel(item);
    if (level) result.push(level);
  }
  return result;
}

function extractSymbol(payload: Record<string, unknown>): string | null {
  const direct = payload.s ?? payload.symbol ?? payload.S;
  if (typeof direct === "string" && direct) return direct.toUpperCase();
  const channel = payload.c ?? payload.channel ?? payload.stream ?? "";
  if (typeof channel === "string" && channel.includes("@")) {
    const parts = channel.split("@");
    for (let i = parts.length - 1; i >= 0; i -= 1) {
      const part = parts[i];
      if (!part) continue;
      if (/^\d+$/.test(part)) continue;
      const lower = part.toLowerCase();
      if (["spot", "public.depth.v3.api", "public.depth", "depth", "v3", "api"].includes(lower)) {
        continue;
      }
      if (/[a-z]/i.test(part)) return part.toUpperCase();
    }
  }
  return null;
}

export class MexcOrderbook {
  private ws: WebSocket | null = null;
  private options: MexcOrderbookOptions;
  private state = new Map<string, BookState>();
  private depth: number;
  private symbolMap = new Map<string, string>();
  private seenSymbols = new Set<string>();

  constructor(options: MexcOrderbookOptions) {
    this.options = options;
    this.depth = SUPPORTED_DEPTHS.includes(options.depth)
      ? options.depth
      : SUPPORTED_DEPTHS[SUPPORTED_DEPTHS.length - 1];
    for (const symbol of options.symbols) {
      this.state.set(symbol, { bids: new Map(), asks: new Map(), tracker: new LevelTracker() });
      this.symbolMap.set(this.toContractSymbol(symbol), symbol);
    }
  }

  connect(): void {
    const wsUrl = this.options.wsUrl ?? DEFAULT_WS_URL;
    this.ws = new WebSocket(wsUrl);

    this.ws.on("open", () => {
      if (this.depth !== this.options.depth) {
        console.warn(
          `MEXC depth ${this.options.depth} unsupported; using ${this.depth} instead.`
        );
      }
      for (const symbol of this.options.symbols) {
        const contractSymbol = this.toContractSymbol(symbol);
        this.ws?.send(
          JSON.stringify({
            method: "sub.depth.full",
            param: { symbol: contractSymbol, limit: this.depth },
          })
        );
      }
      this.options.onReady?.();
    });

    this.ws.on("message", (data) => {
      const payload = JSON.parse(data.toString()) as Record<string, unknown>;
      if (payload?.code && payload.code !== 0) {
        console.warn("MEXC WS error payload:", payload);
      }
      if (payload?.msg && String(payload.msg).toLowerCase().includes("error")) {
        console.warn("MEXC WS message error:", payload);
      }
      if (payload?.method === "ping") {
        this.ws?.send(JSON.stringify({ method: "pong" }));
        return;
      }
      if (payload?.ping) {
        this.ws?.send(JSON.stringify({ pong: payload.ping }));
        return;
      }
      const dataNode = (payload.data as Record<string, unknown> | undefined) ?? payload;
      if (!dataNode) return;
      const rawSymbol = extractSymbol(payload) ?? extractSymbol(dataNode);
      if (!rawSymbol) return;
      const mappedSymbol = this.symbolMap.get(rawSymbol);
      if (!mappedSymbol) return;
      const state = this.state.get(mappedSymbol);
      if (!state) return;
      const bids = normalizeSide(dataNode.bids ?? dataNode.b);
      const asks = normalizeSide(dataNode.asks ?? dataNode.a);
      if (bids.length === 0 && asks.length === 0) return;
      if (!this.seenSymbols.has(mappedSymbol)) {
        console.log(`MEXC first book update: ${mappedSymbol} (${rawSymbol})`);
        this.seenSymbols.add(mappedSymbol);
      }
      this.applySnapshot(state, "bids", bids);
      this.applySnapshot(state, "asks", asks);

      const sortedBids = sortLevels(state.bids, "bids", this.options.depth);
      const sortedAsks = sortLevels(state.asks, "asks", this.options.depth);
      this.options.onUpdate?.(mappedSymbol, sortedBids, sortedAsks, state.tracker);
    });

    this.ws.on("error", (err) => {
      console.error("MEXC WS error:", err);
    });

    this.ws.on("close", () => {
      setTimeout(() => this.connect(), 2000);
    });
  }

  private applySnapshot(state: BookState, side: Side, snapshot: Array<[number, number]>): void {
    const prevMap = side === "bids" ? state.bids : state.asks;
    const nextMap: BookMap = new Map();
    for (const [priceRaw, sizeRaw] of snapshot) {
      const price = toNumber(priceRaw);
      const size = toNumber(sizeRaw);
      if (!Number.isFinite(price) || !Number.isFinite(size) || size <= 0) continue;
      const prevSize = prevMap.has(price) ? prevMap.get(price)! : null;
      nextMap.set(price, size);
      state.tracker.recordUpdate(side, price, size, prevSize);
    }
    for (const [price, prevSize] of prevMap.entries()) {
      if (!nextMap.has(price)) {
        state.tracker.recordUpdate(side, price, 0, prevSize);
      }
    }
    if (side === "bids") {
      state.bids = nextMap;
    } else {
      state.asks = nextMap;
    }
  }

  private toContractSymbol(symbol: string): string {
    const cleaned = symbol.replace(/[^a-zA-Z0-9]/g, "").toUpperCase();
    if (cleaned.endsWith("USDT")) {
      return `${cleaned.slice(0, -4)}_USDT`;
    }
    if (cleaned.endsWith("USDC")) {
      return `${cleaned.slice(0, -4)}_USDC`;
    }
    if (cleaned.endsWith("USD")) {
      return `${cleaned.slice(0, -3)}_USD`;
    }
    return cleaned;
  }
}
