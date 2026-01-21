import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

export interface LargeMoveEvent {
  ts: number;
  symbol: string;
  side: string;
  price: number;
  deltaSize: number;
  notionalDelta: number;
  bpsFromMid: number;
}

export class LargeMoveStore {
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
        CREATE TABLE IF NOT EXISTS large_moves (
          ts INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          price REAL NOT NULL,
          delta_size REAL NOT NULL,
          notional_delta REAL NOT NULL,
          bps_from_mid REAL NOT NULL
        )
      `
      )
      .run();
    this.db.prepare("CREATE INDEX IF NOT EXISTS idx_large_moves_ts ON large_moves(ts)").run();
    this.db
      .prepare("CREATE INDEX IF NOT EXISTS idx_large_moves_symbol_ts ON large_moves(symbol, ts)")
      .run();
    this.insertStmt = this.db.prepare(
      "INSERT INTO large_moves (ts, symbol, side, price, delta_size, notional_delta, bps_from_mid) VALUES (@ts, @symbol, @side, @price, @deltaSize, @notionalDelta, @bpsFromMid)"
    );
  }

  appendAll(events: LargeMoveEvent[]): void {
    if (events.length === 0) return;
    const cutoff = Date.now() - this.retentionMs;
    const insert = this.insertStmt;
    const txn = this.db.transaction((rows: LargeMoveEvent[]) => {
      for (const row of rows) insert.run(row);
    });
    txn(events);
    this.db.prepare("DELETE FROM large_moves WHERE ts < ?").run(cutoff);
  }
}
