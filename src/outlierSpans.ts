import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

export interface OutlierSpanRecord {
  startTs: number;
  endTs: number;
  durationMs: number;
  symbol: string;
  market: string;
  exchange: string;
  side: string;
  price: number;
  maxZ: number;
  avgZ: number;
  count: number;
  startSize: number;
  endSize: number;
  filledPct: number;
  startBps: number;
  endBps: number;
  startBook: string;
  endBook: string;
  startBestBid: number;
  startBestAsk: number;
  endBestBid: number;
  endBestAsk: number;
  startSpreadBps: number;
  endSpreadBps: number;
  startImbalance: number;
  endImbalance: number;
  startBidDepth: number;
  startAskDepth: number;
  endBidDepth: number;
  endAskDepth: number;
  startMicroprice: number;
  endMicroprice: number;
  startLevelRank: number;
  endLevelRank: number;
  startVol1m: number;
  endVol1m: number;
  startVol5m: number;
  endVol5m: number;
  sizeDelta: number;
  sizeDeltaPct: number;
  tradeBuyQty: number;
  tradeSellQty: number;
  tradeCount: number;
}

interface ActiveSpan {
  startTs: number;
  lastTs: number;
  sumZ: number;
  maxZ: number;
  count: number;
  symbol: string;
  market: string;
  exchange: string;
  side: string;
  price: number;
  startSize: number;
  lastSize: number;
  startBps: number;
  lastBps: number;
  startBook: string;
  lastBook: string;
  startBestBid: number;
  startBestAsk: number;
  lastBestBid: number;
  lastBestAsk: number;
  startSpreadBps: number;
  lastSpreadBps: number;
  startImbalance: number;
  lastImbalance: number;
  startBidDepth: number;
  startAskDepth: number;
  lastBidDepth: number;
  lastAskDepth: number;
  startMicroprice: number;
  lastMicroprice: number;
  startLevelRank: number;
  lastLevelRank: number;
  startVol1m: number;
  lastVol1m: number;
  startVol5m: number;
  lastVol5m: number;
  tradeBuyQty: number;
  tradeSellQty: number;
  tradeCount: number;
}

export class OutlierSpanStore {
  private data: OutlierSpanRecord[] = [];
  private retentionMs: number;
  private db: Database.Database;
  private insertStmt: Database.Statement;

  constructor(dataDir: string, retentionMs: number) {
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }
    const dbPath = path.join(dataDir, "metrics.sqlite");
    this.retentionMs = retentionMs;
    this.db = new Database(dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db
      .prepare(
        `
        CREATE TABLE IF NOT EXISTS outlier_spans (
          start_ts INTEGER NOT NULL,
          end_ts INTEGER NOT NULL,
          duration_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          market TEXT NOT NULL,
          exchange TEXT NOT NULL,
          side TEXT NOT NULL,
          price REAL NOT NULL,
          max_z REAL NOT NULL,
          avg_z REAL NOT NULL,
          count INTEGER NOT NULL,
          start_size REAL DEFAULT 0,
          end_size REAL DEFAULT 0,
          filled_pct REAL DEFAULT 0,
          start_bps REAL DEFAULT 0,
          end_bps REAL DEFAULT 0,
          start_book TEXT DEFAULT '',
          end_book TEXT DEFAULT '',
          start_best_bid REAL DEFAULT 0,
          start_best_ask REAL DEFAULT 0,
          end_best_bid REAL DEFAULT 0,
          end_best_ask REAL DEFAULT 0,
          start_spread_bps REAL DEFAULT 0,
          end_spread_bps REAL DEFAULT 0,
          start_imbalance REAL DEFAULT 0,
          end_imbalance REAL DEFAULT 0,
          start_bid_depth REAL DEFAULT 0,
          start_ask_depth REAL DEFAULT 0,
          end_bid_depth REAL DEFAULT 0,
          end_ask_depth REAL DEFAULT 0,
          start_microprice REAL DEFAULT 0,
          end_microprice REAL DEFAULT 0,
          start_level_rank INTEGER DEFAULT 0,
          end_level_rank INTEGER DEFAULT 0,
          start_vol_1m REAL DEFAULT 0,
          end_vol_1m REAL DEFAULT 0,
          start_vol_5m REAL DEFAULT 0,
          end_vol_5m REAL DEFAULT 0,
          size_delta REAL DEFAULT 0,
          size_delta_pct REAL DEFAULT 0,
          trade_buy_qty REAL DEFAULT 0,
          trade_sell_qty REAL DEFAULT 0,
          trade_count INTEGER DEFAULT 0
        )
      `
      )
      .run();
    this.ensureColumns();
    this.db.prepare("CREATE INDEX IF NOT EXISTS idx_outlier_spans_end_ts ON outlier_spans(end_ts)").run();
    this.db
      .prepare("CREATE INDEX IF NOT EXISTS idx_outlier_spans_symbol_end_ts ON outlier_spans(symbol, end_ts)")
      .run();
    this.db
      .prepare(
        "CREATE INDEX IF NOT EXISTS idx_outlier_spans_symbol_market_exchange_end_ts ON outlier_spans(symbol, market, exchange, end_ts)"
      )
      .run();
    this.insertStmt = this.db.prepare(
      `
      INSERT INTO outlier_spans (
        start_ts,
        end_ts,
        duration_ms,
        symbol,
        market,
        exchange,
        side,
        price,
        max_z,
        avg_z,
        count,
        start_size,
        end_size,
        filled_pct,
        start_bps,
        end_bps,
        start_book,
        end_book,
        start_best_bid,
        start_best_ask,
        end_best_bid,
        end_best_ask,
        start_spread_bps,
        end_spread_bps,
        start_imbalance,
        end_imbalance,
        start_bid_depth,
        start_ask_depth,
        end_bid_depth,
        end_ask_depth,
        start_microprice,
        end_microprice,
        start_level_rank,
        end_level_rank,
        start_vol_1m,
        end_vol_1m,
        start_vol_5m,
        end_vol_5m,
        size_delta,
        size_delta_pct,
        trade_buy_qty,
        trade_sell_qty,
        trade_count
      )
      VALUES (
        @startTs,
        @endTs,
        @durationMs,
        @symbol,
        @market,
        @exchange,
        @side,
        @price,
        @maxZ,
        @avgZ,
        @count,
        @startSize,
        @endSize,
        @filledPct,
        @startBps,
        @endBps,
        @startBook,
        @endBook,
        @startBestBid,
        @startBestAsk,
        @endBestBid,
        @endBestAsk,
        @startSpreadBps,
        @endSpreadBps,
        @startImbalance,
        @endImbalance,
        @startBidDepth,
        @startAskDepth,
        @endBidDepth,
        @endAskDepth,
        @startMicroprice,
        @endMicroprice,
        @startLevelRank,
        @endLevelRank,
        @startVol1m,
        @endVol1m,
        @startVol5m,
        @endVol5m,
        @sizeDelta,
        @sizeDeltaPct,
        @tradeBuyQty,
        @tradeSellQty,
        @tradeCount
      )
      `
    );
    this.loadExisting();
  }

  append(record: OutlierSpanRecord): void {
    this.data.push(record);
    this.insertStmt.run(record);
    this.prune();
  }

  getHistory(limit: number, symbol?: string, market?: string, exchange?: string): OutlierSpanRecord[] {
    this.prune();
    let filtered = this.data;
    if (symbol) {
      filtered = filtered.filter((item) => item.symbol === symbol);
    }
    if (market) {
      filtered = filtered.filter((item) => item.market === market);
    }
    if (exchange) {
      const target = exchange.toLowerCase();
      filtered = filtered.filter((item) => item.exchange.toLowerCase() === target);
    }
    return filtered.slice(Math.max(0, filtered.length - limit));
  }

  getAll(): OutlierSpanRecord[] {
    const rows = this.db
      .prepare(
        `
        SELECT start_ts as startTs, end_ts as endTs, duration_ms as durationMs,
               symbol, market, exchange, side, price, max_z as maxZ, avg_z as avgZ, count,
               COALESCE(start_size, 0) as startSize,
               COALESCE(end_size, 0) as endSize,
               COALESCE(filled_pct, 0) as filledPct,
               COALESCE(start_bps, 0) as startBps,
               COALESCE(end_bps, 0) as endBps,
               COALESCE(start_book, '') as startBook,
               COALESCE(end_book, '') as endBook,
               COALESCE(start_best_bid, 0) as startBestBid,
               COALESCE(start_best_ask, 0) as startBestAsk,
               COALESCE(end_best_bid, 0) as endBestBid,
               COALESCE(end_best_ask, 0) as endBestAsk,
               COALESCE(start_spread_bps, 0) as startSpreadBps,
               COALESCE(end_spread_bps, 0) as endSpreadBps,
               COALESCE(start_imbalance, 0) as startImbalance,
               COALESCE(end_imbalance, 0) as endImbalance,
               COALESCE(start_bid_depth, 0) as startBidDepth,
               COALESCE(start_ask_depth, 0) as startAskDepth,
               COALESCE(end_bid_depth, 0) as endBidDepth,
               COALESCE(end_ask_depth, 0) as endAskDepth,
               COALESCE(start_microprice, 0) as startMicroprice,
               COALESCE(end_microprice, 0) as endMicroprice,
               COALESCE(start_level_rank, 0) as startLevelRank,
               COALESCE(end_level_rank, 0) as endLevelRank,
               COALESCE(start_vol_1m, 0) as startVol1m,
               COALESCE(end_vol_1m, 0) as endVol1m,
               COALESCE(start_vol_5m, 0) as startVol5m,
               COALESCE(end_vol_5m, 0) as endVol5m,
               COALESCE(size_delta, 0) as sizeDelta,
               COALESCE(size_delta_pct, 0) as sizeDeltaPct,
               COALESCE(trade_buy_qty, 0) as tradeBuyQty,
               COALESCE(trade_sell_qty, 0) as tradeSellQty,
               COALESCE(trade_count, 0) as tradeCount
        FROM outlier_spans
        ORDER BY end_ts ASC
        `
      )
      .all() as OutlierSpanRecord[];
    return rows;
  }

  private prune(): void {
    const cutoff = Date.now() - this.retentionMs;
    while (this.data.length > 0 && this.data[0].endTs < cutoff) {
      this.data.shift();
    }
    this.db.prepare("DELETE FROM outlier_spans WHERE end_ts < ?").run(cutoff);
  }

  private loadExisting(): void {
    const cutoff = Date.now() - this.retentionMs;
    const rows = this.db
      .prepare(
        `
        SELECT start_ts as startTs, end_ts as endTs, duration_ms as durationMs,
               symbol, market, exchange, side, price, max_z as maxZ, avg_z as avgZ, count,
               COALESCE(start_size, 0) as startSize,
               COALESCE(end_size, 0) as endSize,
               COALESCE(filled_pct, 0) as filledPct,
               COALESCE(start_bps, 0) as startBps,
               COALESCE(end_bps, 0) as endBps,
               COALESCE(start_book, '') as startBook,
               COALESCE(end_book, '') as endBook,
               COALESCE(start_best_bid, 0) as startBestBid,
               COALESCE(start_best_ask, 0) as startBestAsk,
               COALESCE(end_best_bid, 0) as endBestBid,
               COALESCE(end_best_ask, 0) as endBestAsk,
               COALESCE(start_spread_bps, 0) as startSpreadBps,
               COALESCE(end_spread_bps, 0) as endSpreadBps,
               COALESCE(start_imbalance, 0) as startImbalance,
               COALESCE(end_imbalance, 0) as endImbalance,
               COALESCE(start_bid_depth, 0) as startBidDepth,
               COALESCE(start_ask_depth, 0) as startAskDepth,
               COALESCE(end_bid_depth, 0) as endBidDepth,
               COALESCE(end_ask_depth, 0) as endAskDepth,
               COALESCE(start_microprice, 0) as startMicroprice,
               COALESCE(end_microprice, 0) as endMicroprice,
               COALESCE(start_level_rank, 0) as startLevelRank,
               COALESCE(end_level_rank, 0) as endLevelRank,
               COALESCE(start_vol_1m, 0) as startVol1m,
               COALESCE(end_vol_1m, 0) as endVol1m,
               COALESCE(start_vol_5m, 0) as startVol5m,
               COALESCE(end_vol_5m, 0) as endVol5m,
               COALESCE(size_delta, 0) as sizeDelta,
               COALESCE(size_delta_pct, 0) as sizeDeltaPct,
               COALESCE(trade_buy_qty, 0) as tradeBuyQty,
               COALESCE(trade_sell_qty, 0) as tradeSellQty,
               COALESCE(trade_count, 0) as tradeCount
        FROM outlier_spans
        WHERE end_ts >= ?
        ORDER BY end_ts ASC
        `
      )
      .all(cutoff) as OutlierSpanRecord[];
    this.data = rows;
  }

  private ensureColumns(): void {
    const existing = this.db
      .prepare("PRAGMA table_info(outlier_spans)")
      .all() as Array<{ name: string }>;
    const columns = new Set(existing.map((row) => row.name));
    const addColumn = (name: string, type: string, defaultValue: string): void => {
      if (columns.has(name)) return;
      this.db.prepare(`ALTER TABLE outlier_spans ADD COLUMN ${name} ${type} DEFAULT ${defaultValue}`).run();
    };
    addColumn("start_size", "REAL", "0");
    addColumn("end_size", "REAL", "0");
    addColumn("filled_pct", "REAL", "0");
    addColumn("start_bps", "REAL", "0");
    addColumn("end_bps", "REAL", "0");
    addColumn("start_book", "TEXT", "''");
    addColumn("end_book", "TEXT", "''");
    addColumn("start_best_bid", "REAL", "0");
    addColumn("start_best_ask", "REAL", "0");
    addColumn("end_best_bid", "REAL", "0");
    addColumn("end_best_ask", "REAL", "0");
    addColumn("start_spread_bps", "REAL", "0");
    addColumn("end_spread_bps", "REAL", "0");
    addColumn("start_imbalance", "REAL", "0");
    addColumn("end_imbalance", "REAL", "0");
    addColumn("start_bid_depth", "REAL", "0");
    addColumn("start_ask_depth", "REAL", "0");
    addColumn("end_bid_depth", "REAL", "0");
    addColumn("end_ask_depth", "REAL", "0");
    addColumn("start_microprice", "REAL", "0");
    addColumn("end_microprice", "REAL", "0");
    addColumn("start_level_rank", "INTEGER", "0");
    addColumn("end_level_rank", "INTEGER", "0");
    addColumn("start_vol_1m", "REAL", "0");
    addColumn("end_vol_1m", "REAL", "0");
    addColumn("start_vol_5m", "REAL", "0");
    addColumn("end_vol_5m", "REAL", "0");
    addColumn("size_delta", "REAL", "0");
    addColumn("size_delta_pct", "REAL", "0");
    addColumn("trade_buy_qty", "REAL", "0");
    addColumn("trade_sell_qty", "REAL", "0");
    addColumn("trade_count", "INTEGER", "0");
  }
}

export class OutlierSpanTracker {
  private active = new Map<string, ActiveSpan>();
  private store: OutlierSpanStore;

  constructor(store: OutlierSpanStore) {
    this.store = store;
  }

  recordTrade(input: {
    ts: number;
    symbol: string;
    market: string;
    exchange: string;
    price: number;
    qty: number;
    side: "Buy" | "Sell";
  }): void {
    const targetExchange = input.exchange.toLowerCase();
    const targetSymbol = input.symbol;
    for (const span of this.active.values()) {
      if (span.exchange.toLowerCase() !== targetExchange) continue;
      if (span.symbol !== targetSymbol) continue;
      if (span.market !== input.market) continue;
      const mid = (span.price + input.price) / 2;
      if (mid <= 0) continue;
      const bps = (Math.abs(span.price - input.price) / mid) * 10000;
      if (bps > 5) continue;
      if (input.side === "Buy") {
        span.tradeBuyQty += input.qty;
      } else {
        span.tradeSellQty += input.qty;
      }
      span.tradeCount += 1;
    }
  }

  update(records: Array<{
    ts: number;
    symbol: string;
    market: string;
    exchange: string;
    side: string;
    price: number;
    zScore: number;
    size: number;
    mid: number;
    bookSnapshot: string;
    bestBid: number;
    bestAsk: number;
    spreadBps: number;
    imbalance: number;
    bidDepth: number;
    askDepth: number;
    microprice: number;
    levelRank: number;
    vol1m: number;
    vol5m: number;
  }>): void {
    const seen = new Set<string>();
    for (const record of records) {
      const key = this.key(record);
      seen.add(key);
      const existing = this.active.get(key);
      if (existing) {
        existing.lastTs = record.ts;
        existing.sumZ += record.zScore;
        existing.count += 1;
        existing.lastSize = record.size;
        existing.lastBps = record.mid > 0 ? (Math.abs(record.price - record.mid) / record.mid) * 10000 : 0;
        existing.lastBook = record.bookSnapshot;
        existing.lastBestBid = record.bestBid;
        existing.lastBestAsk = record.bestAsk;
        existing.lastSpreadBps = record.spreadBps;
        existing.lastImbalance = record.imbalance;
        existing.lastBidDepth = record.bidDepth;
        existing.lastAskDepth = record.askDepth;
        existing.lastMicroprice = record.microprice;
        existing.lastLevelRank = record.levelRank;
        existing.lastVol1m = record.vol1m;
        existing.lastVol5m = record.vol5m;
        if (record.zScore > existing.maxZ) {
          existing.maxZ = record.zScore;
        }
      } else {
        const startBps = record.mid > 0 ? (Math.abs(record.price - record.mid) / record.mid) * 10000 : 0;
        this.active.set(key, {
          startTs: record.ts,
          lastTs: record.ts,
          sumZ: record.zScore,
          maxZ: record.zScore,
          count: 1,
          symbol: record.symbol,
          market: record.market,
          exchange: record.exchange,
          side: record.side,
          price: record.price,
          startSize: record.size,
          lastSize: record.size,
          startBps,
          lastBps: startBps,
          startBook: record.bookSnapshot,
          lastBook: record.bookSnapshot,
          startBestBid: record.bestBid,
          startBestAsk: record.bestAsk,
          lastBestBid: record.bestBid,
          lastBestAsk: record.bestAsk,
          startSpreadBps: record.spreadBps,
          lastSpreadBps: record.spreadBps,
          startImbalance: record.imbalance,
          lastImbalance: record.imbalance,
          startBidDepth: record.bidDepth,
          startAskDepth: record.askDepth,
          lastBidDepth: record.bidDepth,
          lastAskDepth: record.askDepth,
          startMicroprice: record.microprice,
          lastMicroprice: record.microprice,
          startLevelRank: record.levelRank,
          lastLevelRank: record.levelRank,
          startVol1m: record.vol1m,
          lastVol1m: record.vol1m,
          startVol5m: record.vol5m,
          lastVol5m: record.vol5m,
          tradeBuyQty: 0,
          tradeSellQty: 0,
          tradeCount: 0,
        });
      }
    }

    for (const [key, span] of this.active.entries()) {
      if (seen.has(key)) continue;
      this.active.delete(key);
      const durationMs = Math.max(0, span.lastTs - span.startTs);
      const filledPct = span.startSize > 0 ? Math.min(1, Math.max(0, (span.startSize - span.lastSize) / span.startSize)) : 0;
      const sizeDelta = span.lastSize - span.startSize;
      const sizeDeltaPct = span.startSize > 0 ? sizeDelta / span.startSize : 0;
      this.store.append({
        startTs: span.startTs,
        endTs: span.lastTs,
        durationMs,
        symbol: span.symbol,
        market: span.market,
        exchange: span.exchange,
        side: span.side,
        price: span.price,
        maxZ: span.maxZ,
        avgZ: span.sumZ / Math.max(1, span.count),
        count: span.count,
        startSize: span.startSize,
        endSize: span.lastSize,
        filledPct,
        startBps: span.startBps,
        endBps: span.lastBps,
        startBook: span.startBook,
        endBook: span.lastBook,
        startBestBid: span.startBestBid,
        startBestAsk: span.startBestAsk,
        endBestBid: span.lastBestBid,
        endBestAsk: span.lastBestAsk,
        startSpreadBps: span.startSpreadBps,
        endSpreadBps: span.lastSpreadBps,
        startImbalance: span.startImbalance,
        endImbalance: span.lastImbalance,
        startBidDepth: span.startBidDepth,
        startAskDepth: span.startAskDepth,
        endBidDepth: span.lastBidDepth,
        endAskDepth: span.lastAskDepth,
        startMicroprice: span.startMicroprice,
        endMicroprice: span.lastMicroprice,
        startLevelRank: span.startLevelRank,
        endLevelRank: span.lastLevelRank,
        startVol1m: span.startVol1m,
        endVol1m: span.lastVol1m,
        startVol5m: span.startVol5m,
        endVol5m: span.lastVol5m,
        sizeDelta,
        sizeDeltaPct,
        tradeBuyQty: span.tradeBuyQty,
        tradeSellQty: span.tradeSellQty,
        tradeCount: span.tradeCount,
      });
    }
  }

  getActive(): OutlierSpanRecord[] {
    const now = Date.now();
    return Array.from(this.active.values()).map((span) => ({
      startTs: span.startTs,
      endTs: now,
      durationMs: Math.max(0, now - span.startTs),
      symbol: span.symbol,
      market: span.market,
      exchange: span.exchange,
      side: span.side,
      price: span.price,
      maxZ: span.maxZ,
      avgZ: span.sumZ / Math.max(1, span.count),
      count: span.count,
      startSize: span.startSize,
      endSize: span.lastSize,
      filledPct:
        span.startSize > 0
          ? Math.min(1, Math.max(0, (span.startSize - span.lastSize) / span.startSize))
          : 0,
      startBps: span.startBps,
      endBps: span.lastBps,
      startBook: span.startBook,
      endBook: span.lastBook,
      startBestBid: span.startBestBid,
      startBestAsk: span.startBestAsk,
      endBestBid: span.lastBestBid,
      endBestAsk: span.lastBestAsk,
      startSpreadBps: span.startSpreadBps,
      endSpreadBps: span.lastSpreadBps,
      startImbalance: span.startImbalance,
      endImbalance: span.lastImbalance,
      startBidDepth: span.startBidDepth,
      startAskDepth: span.startAskDepth,
      endBidDepth: span.lastBidDepth,
      endAskDepth: span.lastAskDepth,
      startMicroprice: span.startMicroprice,
      endMicroprice: span.lastMicroprice,
      startLevelRank: span.startLevelRank,
      endLevelRank: span.lastLevelRank,
      startVol1m: span.startVol1m,
      endVol1m: span.lastVol1m,
      startVol5m: span.startVol5m,
      endVol5m: span.lastVol5m,
      sizeDelta: span.lastSize - span.startSize,
      sizeDeltaPct:
        span.startSize > 0 ? (span.lastSize - span.startSize) / span.startSize : 0,
      tradeBuyQty: span.tradeBuyQty,
      tradeSellQty: span.tradeSellQty,
      tradeCount: span.tradeCount,
    }));
  }

  private key(record: { symbol: string; market: string; exchange: string; side: string; price: number }): string {
    return `${record.symbol}:${record.market}:${record.exchange}:${record.side}:${record.price}`;
  }
}
