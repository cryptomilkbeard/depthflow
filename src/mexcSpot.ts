import { LevelTracker } from "./tracker";
import { Side } from "./types";

type BookMap = Map<number, number>;

interface BookState {
  bids: BookMap;
  asks: BookMap;
  tracker: LevelTracker;
}

interface MexcSpotOrderbookOptions {
  symbols: string[];
  depth: number;
  pollMs?: number;
  onReady?: () => void;
  onUpdate?: (symbol: string, bids: Array<[number, number]>, asks: Array<[number, number]>, tracker: LevelTracker) => void;
}

function toNumber(raw: string | number): number {
  return Number(raw);
}

function sortLevels(map: BookMap, side: Side, depth: number): Array<[number, number]> {
  const entries = Array.from(map.entries());
  entries.sort((a, b) => (side === "bids" ? b[0] - a[0] : a[0] - b[0]));
  return entries.slice(0, depth);
}

export class MexcSpotOrderbook {
  private options: MexcSpotOrderbookOptions;
  private state = new Map<string, BookState>();
  private timer: NodeJS.Timeout | null = null;
  private ready = false;

  constructor(options: MexcSpotOrderbookOptions) {
    this.options = options;
    for (const symbol of options.symbols) {
      this.state.set(symbol, { bids: new Map(), asks: new Map(), tracker: new LevelTracker() });
    }
  }

  connect(): void {
    if (this.timer) return;
    const interval = Math.max(1000, this.options.pollMs ?? 2000);
    this.poll();
    this.timer = setInterval(() => this.poll(), interval);
  }

  private async poll(): Promise<void> {
    await Promise.all(
      this.options.symbols.map(async (symbol) => {
        try {
          const response = await fetch(
            `https://api.mexc.com/api/v3/depth?symbol=${encodeURIComponent(symbol)}&limit=${this.options.depth}`
          );
          if (!response.ok) return;
          const payload = (await response.json()) as { bids?: Array<[string, string]>; asks?: Array<[string, string]> };
          const bidsRaw = payload.bids ?? [];
          const asksRaw = payload.asks ?? [];
          const state = this.state.get(symbol);
          if (!state) return;
          this.applySnapshot(state, "bids", bidsRaw);
          this.applySnapshot(state, "asks", asksRaw);
          const sortedBids = sortLevels(state.bids, "bids", this.options.depth);
          const sortedAsks = sortLevels(state.asks, "asks", this.options.depth);
          this.options.onUpdate?.(symbol, sortedBids, sortedAsks, state.tracker);
          if (!this.ready) {
            this.ready = true;
            this.options.onReady?.();
          }
        } catch {
          return;
        }
      })
    );
  }

  private applySnapshot(state: BookState, side: Side, snapshot: Array<[string, string]>): void {
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
}
