import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

export interface OiFundingPoint {
  ts: number;
  symbol: string;
  exchange: string;
  openInterest: number;
  fundingRate: number;
  nextFundingTime?: number;
}

export class OiFundingStore {
  private data: OiFundingPoint[] = [];
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
        CREATE TABLE IF NOT EXISTS oi_funding (
          ts INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          exchange TEXT NOT NULL DEFAULT 'bybit',
          open_interest REAL,
          funding_rate REAL,
          next_funding_time INTEGER
        )
      `
      )
      .run();
    this.ensureExchangeColumn();
    this.db.prepare("CREATE INDEX IF NOT EXISTS idx_oi_ts ON oi_funding(ts)").run();
    this.db
      .prepare("CREATE INDEX IF NOT EXISTS idx_oi_symbol_ts ON oi_funding(symbol, ts)")
      .run();
    this.db
      .prepare("CREATE INDEX IF NOT EXISTS idx_oi_symbol_exchange_ts ON oi_funding(symbol, exchange, ts)")
      .run();
    this.insertStmt = this.db.prepare(
      "INSERT INTO oi_funding (ts, symbol, exchange, open_interest, funding_rate, next_funding_time) VALUES (@ts, @symbol, @exchange, @openInterest, @fundingRate, @nextFundingTime)"
    );
    this.loadExisting();
  }

  append(point: OiFundingPoint): void {
    this.data.push(point);
    this.prune();
    this.insertStmt.run({
      ...point,
      exchange: point.exchange ?? "bybit",
      nextFundingTime: point.nextFundingTime ?? null,
    });
  }

  getHistory(limit: number, symbol?: string, exchange?: string): OiFundingPoint[] {
    this.prune();
    let filtered = this.data;
    if (symbol) {
      filtered = filtered.filter((item) => item.symbol === symbol);
    }
    if (exchange) {
      filtered = filtered.filter((item) => item.exchange === exchange);
    }
    return filtered.slice(Math.max(0, filtered.length - limit));
  }

  private prune(): void {
    const cutoff = Date.now() - this.retentionMs;
    while (this.data.length > 0 && this.data[0].ts < cutoff) {
      this.data.shift();
    }
    this.db.prepare("DELETE FROM oi_funding WHERE ts < ?").run(cutoff);
  }

  private loadExisting(): void {
    const cutoff = Date.now() - this.retentionMs;
    const rows = this.db
      .prepare(
        "SELECT ts, symbol, exchange, open_interest as openInterest, funding_rate as fundingRate, next_funding_time as nextFundingTime FROM oi_funding WHERE ts >= ? ORDER BY ts ASC"
      )
      .all(cutoff) as OiFundingPoint[];
    this.data = rows;
  }

  private ensureExchangeColumn(): void {
    const columns = this.db.prepare("PRAGMA table_info(oi_funding)").all() as Array<{ name: string }>;
    const hasExchange = columns.some((column) => column.name === "exchange");
    if (!hasExchange) {
      this.db.prepare("ALTER TABLE oi_funding ADD COLUMN exchange TEXT NOT NULL DEFAULT 'bybit'").run();
      this.db.prepare("UPDATE oi_funding SET exchange = 'bybit' WHERE exchange IS NULL").run();
    }
  }
}
