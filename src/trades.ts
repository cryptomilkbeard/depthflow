import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

export interface TradeRecord {
  ts: number;
  symbol: string;
  exchange: string;
  side: string;
  price: number;
  qty: number;
}

export class TradeStore {
  private data: TradeRecord[] = [];
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
        CREATE TABLE IF NOT EXISTS trades (
          ts INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          exchange TEXT NOT NULL,
          side TEXT NOT NULL,
          price REAL NOT NULL,
          qty REAL NOT NULL
        )
      `
      )
      .run();
    this.db.prepare("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts)").run();
    this.db
      .prepare("CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON trades(symbol, ts)")
      .run();
    this.insertStmt = this.db.prepare(
      `
      INSERT INTO trades (ts, symbol, exchange, side, price, qty)
      VALUES (@ts, @symbol, @exchange, @side, @price, @qty)
      `
    );
    this.loadExisting();
  }

  append(record: TradeRecord): void {
    this.data.push(record);
    this.insertStmt.run(record);
    this.prune();
  }

  getHistory(limit: number, symbol?: string, exchange?: string): TradeRecord[] {
    this.prune();
    let filtered = this.data;
    if (symbol) {
      filtered = filtered.filter((item) => item.symbol === symbol);
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
    this.db.prepare("DELETE FROM trades WHERE ts < ?").run(cutoff);
  }

  private loadExisting(): void {
    const cutoff = Date.now() - this.retentionMs;
    const rows = this.db
      .prepare(
        `
        SELECT ts, symbol, exchange, side, price, qty
        FROM trades
        WHERE ts >= ?
        ORDER BY ts ASC
        `
      )
      .all(cutoff) as TradeRecord[];
    this.data = rows;
  }
}
