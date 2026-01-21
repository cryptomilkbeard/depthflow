import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { MetricsPoint } from "./types";

export class MetricsStore {
  private data: MetricsPoint[] = [];
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
        CREATE TABLE IF NOT EXISTS metrics (
          ts INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          payload TEXT NOT NULL
        )
      `
      )
      .run();
    this.db
      .prepare("CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics(ts)")
      .run();
    this.db
      .prepare("CREATE INDEX IF NOT EXISTS idx_metrics_symbol_ts ON metrics(symbol, ts)")
      .run();
    this.insertStmt = this.db.prepare(
      "INSERT INTO metrics (ts, symbol, payload) VALUES (@ts, @symbol, @payload)"
    );
    this.loadExisting();
  }

  append(point: MetricsPoint): void {
    this.data.push(point);
    this.prune();
    this.insertStmt.run({ ts: point.ts, symbol: point.symbol, payload: JSON.stringify(point) });
  }

  getHistory(): MetricsPoint[] {
    this.prune();
    return this.data;
  }

  private prune(): void {
    const cutoff = Date.now() - this.retentionMs;
    while (this.data.length > 0 && this.data[0].ts < cutoff) {
      this.data.shift();
    }
    this.db.prepare("DELETE FROM metrics WHERE ts < ?").run(cutoff);
  }

  private loadExisting(): void {
    const cutoff = Date.now() - this.retentionMs;
    const rows = this.db
      .prepare("SELECT payload FROM metrics WHERE ts >= ? ORDER BY ts ASC")
      .all(cutoff) as Array<{ payload: string }>;
    for (const row of rows) {
      try {
        const point = JSON.parse(row.payload) as MetricsPoint;
        this.data.push(point);
      } catch {
        continue;
      }
    }
  }
}
