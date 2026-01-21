import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

export interface OutlierRecord {
  ts: number;
  symbol: string;
  market: string;
  exchange: string;
  side: string;
  price: number;
  size: number;
  zScore: number;
  bpsFromMid: number;
}

export class OutlierStore {
  private data: OutlierRecord[] = [];
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
        CREATE TABLE IF NOT EXISTS outliers (
          ts INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          market TEXT NOT NULL,
          exchange TEXT NOT NULL,
          side TEXT NOT NULL,
          price REAL NOT NULL,
          size REAL NOT NULL,
          z_score REAL NOT NULL,
          bps_from_mid REAL NOT NULL
        )
      `
      )
      .run();
    this.db.prepare("CREATE INDEX IF NOT EXISTS idx_outliers_ts ON outliers(ts)").run();
    this.db
      .prepare("CREATE INDEX IF NOT EXISTS idx_outliers_symbol_ts ON outliers(symbol, ts)")
      .run();
    this.db
      .prepare(
        "CREATE INDEX IF NOT EXISTS idx_outliers_symbol_market_exchange_ts ON outliers(symbol, market, exchange, ts)"
      )
      .run();
    this.insertStmt = this.db.prepare(
      `
      INSERT INTO outliers (ts, symbol, market, exchange, side, price, size, z_score, bps_from_mid)
      VALUES (@ts, @symbol, @market, @exchange, @side, @price, @size, @zScore, @bpsFromMid)
      `
    );
    this.loadExisting();
  }

  appendAll(records: OutlierRecord[]): void {
    if (records.length === 0) return;
    for (const record of records) {
      this.data.push(record);
      this.insertStmt.run(record);
    }
    this.prune();
  }

  getHistory(limit: number, symbol?: string, market?: string, exchange?: string): OutlierRecord[] {
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

  private prune(): void {
    const cutoff = Date.now() - this.retentionMs;
    while (this.data.length > 0 && this.data[0].ts < cutoff) {
      this.data.shift();
    }
    this.db.prepare("DELETE FROM outliers WHERE ts < ?").run(cutoff);
  }

  private loadExisting(): void {
    const cutoff = Date.now() - this.retentionMs;
    const rows = this.db
      .prepare(
        `
        SELECT ts, symbol, market, exchange, side, price, size,
               z_score as zScore, bps_from_mid as bpsFromMid
        FROM outliers
        WHERE ts >= ?
        ORDER BY ts ASC
        `
      )
      .all(cutoff) as OutlierRecord[];
    this.data = rows;
  }
}
