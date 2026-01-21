import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

export interface LiquidationEvent {
  ts: number;
  symbol: string;
  side: string;
  price: number;
  size: number;
  notional: number;
}

export class LiquidationStore {
  private data: LiquidationEvent[] = [];
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
        CREATE TABLE IF NOT EXISTS liquidations (
          ts INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          price REAL NOT NULL,
          size REAL NOT NULL,
          notional REAL NOT NULL
        )
      `
      )
      .run();
    this.db
      .prepare("CREATE INDEX IF NOT EXISTS idx_liquidations_ts ON liquidations(ts)")
      .run();
    this.db
      .prepare("CREATE INDEX IF NOT EXISTS idx_liquidations_symbol_ts ON liquidations(symbol, ts)")
      .run();
    this.insertStmt = this.db.prepare(
      "INSERT INTO liquidations (ts, symbol, side, price, size, notional) VALUES (@ts, @symbol, @side, @price, @size, @notional)"
    );
    this.loadExisting();
  }

  append(event: LiquidationEvent): void {
    this.data.push(event);
    this.prune();
    this.insertStmt.run(event);
  }

  getHistory(limit: number, symbol?: string): LiquidationEvent[] {
    this.prune();
    const filtered = symbol ? this.data.filter((item) => item.symbol === symbol) : this.data;
    return filtered.slice(Math.max(0, filtered.length - limit));
  }

  private prune(): void {
    const cutoff = Date.now() - this.retentionMs;
    while (this.data.length > 0 && this.data[0].ts < cutoff) {
      this.data.shift();
    }
    this.db.prepare("DELETE FROM liquidations WHERE ts < ?").run(cutoff);
  }

  private loadExisting(): void {
    const cutoff = Date.now() - this.retentionMs;
    const rows = this.db
      .prepare(
        "SELECT ts, symbol, side, price, size, notional FROM liquidations WHERE ts >= ? ORDER BY ts ASC"
      )
      .all(cutoff) as LiquidationEvent[];
    this.data = rows;
  }
}
