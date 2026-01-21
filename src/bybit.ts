import WebSocket from "ws";
import { LevelTracker } from "./tracker";
import { Side } from "./types";

type BookMap = Map<number, number>;

interface BookState {
  bids: BookMap;
  asks: BookMap;
  tracker: LevelTracker;
}

interface BybitOrderbookOptions {
  symbols: string[];
  depth: number;
  wsUrl?: string;
  onReady?: () => void;
  onUpdate?: (symbol: string, bids: Array<[number, number]>, asks: Array<[number, number]>, tracker: LevelTracker) => void;
}

function toNumber(raw: string): number {
  return Number(raw);
}

function sortLevels(map: BookMap, side: Side, depth: number): Array<[number, number]> {
  const entries = Array.from(map.entries());
  entries.sort((a, b) => (side === "bids" ? b[0] - a[0] : a[0] - b[0]));
  return entries.slice(0, depth);
}

export class BybitOrderbook {
  private ws: WebSocket | null = null;
  private options: BybitOrderbookOptions;
  private state = new Map<string, BookState>();

  constructor(options: BybitOrderbookOptions) {
    this.options = options;
    for (const symbol of options.symbols) {
      this.state.set(symbol, { bids: new Map(), asks: new Map(), tracker: new LevelTracker() });
    }
  }

  connect(): void {
    const wsUrl = this.options.wsUrl ?? "wss://stream.bybit.com/v5/public/spot";
    this.ws = new WebSocket(wsUrl);

    this.ws.on("open", () => {
      const topics = this.options.symbols.map((symbol) => `orderbook.${this.options.depth}.${symbol}`);
      this.ws?.send(JSON.stringify({ op: "subscribe", args: topics }));
      this.options.onReady?.();
    });

    this.ws.on("message", (data) => {
      const payload = JSON.parse(data.toString());
      if (!payload || !payload.topic || !payload.data) return;
      if (!String(payload.topic).startsWith("orderbook.")) return;

      const symbol = payload.data.s as string;
      const state = this.state.get(symbol);
      if (!state) return;

      const bids = payload.data.b ?? [];
      const asks = payload.data.a ?? [];
      this.applyUpdates(state, "bids", bids);
      this.applyUpdates(state, "asks", asks);

      const sortedBids = sortLevels(state.bids, "bids", this.options.depth);
      const sortedAsks = sortLevels(state.asks, "asks", this.options.depth);

      this.options.onUpdate?.(symbol, sortedBids, sortedAsks, state.tracker);
    });

    this.ws.on("error", (err) => {
      console.error("Bybit WS error:", err);
    });

    this.ws.on("close", () => {
      setTimeout(() => this.connect(), 2000);
    });
  }

  private applyUpdates(state: BookState, side: Side, updates: Array<[string, string]>): void {
    const map = side === "bids" ? state.bids : state.asks;
    for (const [priceRaw, sizeRaw] of updates) {
      const price = toNumber(priceRaw);
      const size = toNumber(sizeRaw);
      const prevSize = map.has(price) ? map.get(price)! : null;
      if (size === 0) {
        map.delete(price);
      } else {
        map.set(price, size);
      }
      state.tracker.recordUpdate(side, price, size, prevSize);
    }
  }
}
