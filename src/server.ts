import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import express from "express";
import { WebSocketServer } from "ws";
import { loadConfig } from "./config";
import { BybitOrderbook } from "./bybit";
import { MexcOrderbook } from "./mexc";
import { MexcSpotOrderbook } from "./mexcSpot";
import { computeMetrics } from "./metrics";
import { MetricsStore } from "./storage";
import { LevelTracker } from "./tracker";
import { ExchangeMetrics, LevelMove, MoveStats } from "./types";
import { LiquidationStore, LiquidationEvent } from "./liquidations";
import WebSocket from "ws";
import { OiFundingStore, OiFundingPoint } from "./oiFunding";
import { LargeMoveStore } from "./largeMoves";
import { OutlierStore } from "./outliers";
import { OutlierSpanStore, OutlierSpanTracker } from "./outlierSpans";
import { TradeStore } from "./trades";
import PDFDocument from "pdfkit";
import type PDFKit from "pdfkit";

interface LatestState {
  bids: Array<[number, number]>;
  asks: Array<[number, number]>;
  tracker: LevelTracker;
}

function loadEnvFile(): void {
  const envPath = path.join(process.cwd(), ".env");
  if (!fs.existsSync(envPath)) return;
  const contents = fs.readFileSync(envPath, "utf8");
  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/);
    if (!match) continue;
    const [, key, valueRaw] = match;
    if (process.env[key] !== undefined) continue;
    const value = valueRaw.replace(/^"(.*)"$/, "$1").replace(/^'(.*)'$/, "$1");
    process.env[key] = value;
  }
}

function parseEnvFlag(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined) return fallback;
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

loadEnvFile();

const config = loadConfig();
const liveMonitoring = parseEnvFlag(process.env.LIVE_MONITORING, true);
const retentionMs = 24 * 60 * 60 * 1000;
const store = new MetricsStore(config.dataDir, retentionMs);
const liquidationStore = new LiquidationStore(config.dataDir, retentionMs);
const oiFundingStore = new OiFundingStore(config.dataDir, retentionMs);
const largeMoveStore = new LargeMoveStore(config.dataDir, retentionMs);
const outlierStore = new OutlierStore(config.dataDir, 90 * 24 * 60 * 60 * 1000);
const outlierSpanStore = new OutlierSpanStore(config.dataDir, 90 * 24 * 60 * 60 * 1000);
const outlierSpanTracker = new OutlierSpanTracker(outlierSpanStore);
const tradeStore = new TradeStore(config.dataDir, 90 * 24 * 60 * 60 * 1000);

const rawBasePath = process.env.BASE_PATH ?? "";
const trimmedBasePath = rawBasePath.trim().replace(/\/+$/, "");
const basePath =
  trimmedBasePath.length > 0
    ? trimmedBasePath.startsWith("/")
      ? trimmedBasePath
      : `/${trimmedBasePath}`
    : "";

const app = express();
const router = express.Router();
router.use(express.static(path.join(__dirname, "..", "public")));
router.get("/api/config", (req, res) => {
  res.json({
    symbols: config.symbols,
    depth: config.depth,
    baseMmNotional: config.baseMmNotional,
    largeMoveNotional: config.largeMoveNotional,
    sizeBins: config.sizeBins,
  });
});
router.get("/api/status", (req, res) => {
  res.json({ liveMonitoring });
});
router.get("/api/history", (req, res) => {
  const limit = Number(req.query.limit ?? 600);
  const history = store.getHistory();
  const slice = history.slice(Math.max(0, history.length - limit));
  res.json(slice);
});
router.get("/api/liquidations", (req, res) => {
  const limit = Number(req.query.limit ?? 200);
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  res.json(liquidationStore.getHistory(limit, symbol));
});
router.get("/api/oi-funding", (req, res) => {
  const limit = Number(req.query.limit ?? 600);
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  const exchange = typeof req.query.exchange === "string" ? req.query.exchange : undefined;
  res.json(oiFundingStore.getHistory(limit, symbol, exchange));
});
router.get("/api/trades", (req, res) => {
  const limit = Number(req.query.limit ?? 200);
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  const exchange = typeof req.query.exchange === "string" ? req.query.exchange : undefined;
  res.json(tradeStore.getHistory(limit, symbol, exchange));
});
router.get("/api/outliers", (req, res) => {
  const limit = Number(req.query.limit ?? 200);
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  const market = typeof req.query.market === "string" ? req.query.market : undefined;
  const exchange = typeof req.query.exchange === "string" ? req.query.exchange : undefined;
  res.json(outlierStore.getHistory(limit, symbol, market, exchange));
});
router.get("/api/outliers/spans", (req, res) => {
  const limit = Number(req.query.limit ?? 200);
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  const market = typeof req.query.market === "string" ? req.query.market : undefined;
  const exchange = typeof req.query.exchange === "string" ? req.query.exchange : undefined;
  res.json({
    closed: outlierSpanStore.getHistory(limit, symbol, market, exchange),
    active: outlierSpanTracker.getActive().filter((item) => {
      if (symbol && item.symbol !== symbol) return false;
      if (market && item.market !== market) return false;
      if (exchange && item.exchange.toLowerCase() !== exchange.toLowerCase()) return false;
      return true;
    }),
  });
});
type OutlierRow = ReturnType<OutlierStore["getHistory"]>[number];
type OutlierSpanRow = ReturnType<OutlierSpanStore["getHistory"]>[number];
type ActiveSpanRow = ReturnType<OutlierSpanTracker["getActive"]>[number];

const escapeHtml = (value: string): string =>
  value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const formatDuration = (ms: number): string => {
  if (!Number.isFinite(ms) || ms <= 0) return "0s";
  const totalSeconds = Math.round(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts = [];
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0 || hours > 0) parts.push(`${minutes}m`);
  parts.push(`${seconds}s`);
  return parts.join(" ");
};

const formatNumber = (value: number, decimals = 2): string =>
  Number.isFinite(value) ? value.toFixed(decimals) : "0.00";

const renderTable = (rows: string, colSpan: number): string =>
  rows.length > 0 ? rows : `<tr><td colspan="${colSpan}" class="empty">No data</td></tr>`;

const renderBars = (data: Array<{ label: string; value: number }>, color: string): string => {
  if (data.length === 0) {
    return `<div class="empty">No data</div>`;
  }
  const max = Math.max(1, ...data.map((item) => item.value));
  return data
    .map((item) => {
      const height = Math.round((item.value / max) * 100);
      return `
          <div class="bar">
            <div class="bar-fill" style="height:${height}%;background:${color}"></div>
            <div class="bar-label">${escapeHtml(item.label)}</div>
            <div class="bar-value">${item.value}</div>
          </div>
        `;
    })
    .join("");
};

const buildOutlierReportHtml = ({
  title,
  history,
  spans,
  activeSpans,
  symbol,
  market,
  exchange,
  metaLines,
  windowLabel,
  exportLinks,
}: {
  title: string;
  history: OutlierRow[];
  spans: OutlierSpanRow[];
  activeSpans: ActiveSpanRow[];
  symbol?: string;
  market?: string;
  exchange?: string;
  metaLines?: string[];
  windowLabel?: string;
  exportLinks?: { csv: string; pdf: string; csvAll?: string };
}): string => {
  const summary = (() => {
    if (history.length === 0) {
      return {
        window: "n/a",
        avgZ: "0.00",
        maxZ: "0.00",
        avgBps: "0.00",
      };
    }
    const firstTs = history[0].ts;
    const lastTs = history[history.length - 1].ts;
    const avgZ =
      history.reduce((sum, row) => sum + row.zScore, 0) / Math.max(1, history.length);
    const avgBps =
      history.reduce((sum, row) => sum + row.bpsFromMid, 0) / Math.max(1, history.length);
    const maxZ = history.reduce((max, row) => (row.zScore > max ? row.zScore : max), 0);
    return {
      window: `${new Date(firstTs).toISOString()} → ${new Date(lastTs).toISOString()}`,
      avgZ: formatNumber(avgZ, 2),
      maxZ: formatNumber(maxZ, 2),
      avgBps: formatNumber(avgBps, 2),
    };
  })();

  const exchanges = new Map<string, number>();
  const sides = new Map<string, number>();
  for (const row of history) {
    exchanges.set(row.exchange, (exchanges.get(row.exchange) ?? 0) + 1);
    sides.set(row.side, (sides.get(row.side) ?? 0) + 1);
  }
  const exchangeSeries = Array.from(exchanges.entries()).map(([label, value]) => ({
    label,
    value,
  }));
  const sideSeries = Array.from(sides.entries()).map(([label, value]) => ({
    label,
    value,
  }));

  const byHour = new Map<string, number>();
  for (const row of history) {
    const hour = new Date(row.ts).toISOString().slice(0, 13);
    byHour.set(hour, (byHour.get(hour) ?? 0) + 1);
  }
  const timeSeries = Array.from(byHour.entries())
    .sort((a, b) => (a[0] < b[0] ? -1 : 1))
    .slice(-24)
    .map(([label, value]) => ({ label: label.slice(11), value }));

  const combinedSpans = [
    ...spans.map((row) => ({ ...row, status: "closed", endLabel: new Date(row.endTs).toISOString() })),
    ...activeSpans.map((row) => ({ ...row, status: "active", endLabel: "Now" })),
  ];
  const topOutliers = [...combinedSpans].sort((a, b) => b.maxZ - a.maxZ).slice(0, 10);
  const longestSpans = [...combinedSpans]
    .sort((a, b) => b.durationMs - a.durationMs)
    .slice(0, 10);
  const activeTop = [...activeSpans].sort((a, b) => b.durationMs - a.durationMs).slice(0, 10);
  const historyWindows = (() => {
    const map = new Map<string, { minTs: number; maxTs: number }>();
    for (const row of history) {
      const key = `${row.symbol}:${row.market}:${row.exchange}:${row.side}:${row.price.toFixed(8)}`;
      const existing = map.get(key);
      if (!existing) {
        map.set(key, { minTs: row.ts, maxTs: row.ts });
      } else {
        existing.minTs = Math.min(existing.minTs, row.ts);
        existing.maxTs = Math.max(existing.maxTs, row.ts);
      }
    }
    return map;
  })();

  const summaryByExchange = (() => {
    const exchangeSet = new Set<string>();
    history.forEach((row) => exchangeSet.add(row.exchange));
    combinedSpans.forEach((row) => exchangeSet.add(row.exchange));
    return Array.from(exchangeSet.values())
      .sort((a, b) => a.localeCompare(b))
      .map((ex) => {
        const exchangeEvents = history.filter((row) => row.exchange === ex);
        const exchangeSpans = combinedSpans.filter((row) => row.exchange === ex);
        const historyDurations = Array.from(historyWindows.entries())
          .filter(([key]) => key.split(":")[2] === ex)
          .map(([, window]) => Math.max(0, window.maxTs - window.minTs));
        const avgZ =
          exchangeEvents.reduce((sum, row) => sum + row.zScore, 0) /
          Math.max(1, exchangeEvents.length);
        const maxZ = exchangeEvents.reduce((max, row) => (row.zScore > max ? row.zScore : max), 0);
        const avgBps =
          exchangeEvents.reduce((sum, row) => sum + row.bpsFromMid, 0) /
          Math.max(1, exchangeEvents.length);
        const avgDurationMs =
          exchangeSpans.length > 0
            ? exchangeSpans.reduce((sum, row) => sum + row.durationMs, 0) /
              Math.max(1, exchangeSpans.length)
            : historyDurations.reduce((sum, value) => sum + value, 0) /
              Math.max(1, historyDurations.length);
        return {
          exchange: ex,
          events: exchangeEvents.length,
          spans: exchangeSpans.length,
          avgZ,
          maxZ,
          avgBps,
          avgDurationMs,
        };
      });
  })();

  const metaExtra = (metaLines ?? [])
    .filter((line) => line.trim().length > 0)
    .map((line) => `<br />${escapeHtml(line)}`)
    .join("");
  const exportHtml = exportLinks
    ? `<div class="exports">
        <a class="export-btn" href="${escapeHtml(exportLinks.csv)}">Export CSV</a>
        <a class="export-btn export-btn--pdf" href="${escapeHtml(exportLinks.pdf)}">Export PDF</a>
        ${
          exportLinks.csvAll
            ? `<a class="export-btn export-btn--all" href="${escapeHtml(
                exportLinks.csvAll
              )}">Export All CSV</a>`
            : ""
        }
      </div>
      <div class="export-window" data-csv-base="${escapeHtml(exportLinks.csv)}">
        <label>
          Start
          <input id="exportStart" type="datetime-local" />
        </label>
        <label>
          End
          <input id="exportEnd" type="datetime-local" />
        </label>
        <a id="exportWindowCsv" class="export-btn export-btn--window" href="${escapeHtml(
          exportLinks.csv
        )}">Export Window CSV</a>
      </div>`
    : "";

  return `<!doctype html>
  <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>${escapeHtml(title)}</title>
      <style>
        :root {
          color-scheme: light;
        }
        body {
          margin: 0;
          padding: 24px;
          font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
          background: #0f131a;
          color: #e7edf5;
        }
        h1 {
          font-size: 20px;
          margin: 0 0 6px;
        }
        .exports {
          display: flex;
          gap: 8px;
          margin: 12px 0 18px;
        }
        .export-btn {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 6px 12px;
          border-radius: 8px;
          font-size: 12px;
          text-decoration: none;
          color: #0f131a;
          background: #8bd3ff;
          font-weight: 600;
        }
        .export-btn--all {
          background: #f08fff;
        }
        .export-btn--pdf {
          background: #f4c542;
        }
        .export-btn--window {
          background: #65d6ad;
        }
        .export-window {
          display: flex;
          flex-wrap: wrap;
          align-items: center;
          gap: 8px;
          margin-bottom: 18px;
          font-size: 12px;
        }
        .export-window label {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          color: #9fb2c8;
        }
        .export-window input {
          background: #10161f;
          border: 1px solid #2a3545;
          border-radius: 6px;
          padding: 4px 6px;
          color: #e7edf5;
          font-size: 12px;
        }
        h2 {
          font-size: 14px;
          margin: 22px 0 10px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          color: #9fb2c8;
        }
        .meta {
          font-size: 12px;
          color: #b4c1d1;
          margin-bottom: 14px;
        }
        .grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          gap: 12px;
        }
        .card {
          background: #161c26;
          border: 1px solid #2a3545;
          border-radius: 10px;
          padding: 12px;
        }
        .card .label {
          font-size: 11px;
          text-transform: uppercase;
          letter-spacing: 0.06em;
          color: #8ea2b9;
        }
        .card .value {
          margin-top: 6px;
          font-size: 15px;
          font-weight: 600;
        }
        .charts {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
          gap: 16px;
        }
        .chart {
          background: #161c26;
          border: 1px solid #2a3545;
          border-radius: 10px;
          padding: 12px;
          min-height: 160px;
        }
        .chart-title {
          font-size: 12px;
          color: #9fb2c8;
          margin-bottom: 8px;
        }
        .bar-grid {
          display: flex;
          gap: 8px;
          align-items: flex-end;
          height: 110px;
        }
        .bar {
          flex: 1;
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: flex-end;
          height: 100%;
        }
        .bar-fill {
          width: 100%;
          border-radius: 6px 6px 0 0;
          min-height: 4px;
        }
        .bar-label {
          margin-top: 6px;
          font-size: 10px;
          color: #93a5b9;
          text-align: center;
        }
        .bar-value {
          font-size: 10px;
          color: #d6e0ec;
        }
        table {
          width: 100%;
          border-collapse: collapse;
          table-layout: fixed;
          font-size: 11px;
          background: #161c26;
          border-radius: 10px;
          overflow: hidden;
        }
        thead {
          background: #1c2430;
        }
        th, td {
          padding: 8px 6px;
          border-bottom: 1px solid #2a3545;
          text-align: left;
          word-break: break-word;
        }
        th {
          color: #9fb2c8;
          font-weight: 600;
        }
        tbody tr:nth-child(even) {
          background: #141a24;
        }
        .empty {
          text-align: center;
          color: #7f91a6;
          padding: 18px;
        }
      </style>
    </head>
    <body>
      <h1>${escapeHtml(title)}</h1>
      <div class="meta">
        Generated ${escapeHtml(new Date().toISOString())}<br />
        Symbol: ${escapeHtml(symbol ?? "All")} &middot; Market: ${escapeHtml(
    market ?? "All"
  )} &middot; Exchange: ${escapeHtml(exchange ?? "All")}${metaExtra}
      </div>
      ${exportHtml}
      <div class="grid">
        <div class="card"><div class="label">Records</div><div class="value">${history.length}</div></div>
        <div class="card"><div class="label">Closed Spans</div><div class="value">${spans.length}</div></div>
        <div class="card"><div class="label">Active Spans</div><div class="value">${activeSpans.length}</div></div>
        <div class="card"><div class="label">Window</div><div class="value">${escapeHtml(
          windowLabel ?? summary.window
        )}</div></div>
        <div class="card"><div class="label">Avg Z</div><div class="value">${summary.avgZ}</div></div>
        <div class="card"><div class="label">Max Z</div><div class="value">${summary.maxZ}</div></div>
        <div class="card"><div class="label">Avg BPS</div><div class="value">${summary.avgBps}</div></div>
      </div>

      <h2>Summary by Exchange</h2>
      <table>
        <thead>
          <tr>
            <th>Exchange</th>
            <th>Events</th>
            <th>Spans</th>
            <th>Avg Z</th>
            <th>Max Z</th>
            <th>Avg BPS</th>
            <th>Avg Duration</th>
          </tr>
        </thead>
        <tbody>
          ${renderTable(
            summaryByExchange
              .map(
                (row) => `<tr>
                  <td>${escapeHtml(row.exchange)}</td>
                  <td>${row.events}</td>
                  <td>${row.spans}</td>
                  <td>${formatNumber(row.avgZ, 2)}</td>
                  <td>${formatNumber(row.maxZ, 2)}</td>
                  <td>${formatNumber(row.avgBps, 2)}</td>
                  <td>${escapeHtml(formatDuration(row.avgDurationMs))}</td>
                </tr>`
              )
              .join(""),
            7
          )}
        </tbody>
      </table>

      <h2>Charts</h2>
      <div class="charts">
        <div class="chart">
          <div class="chart-title">Outliers per Hour (last 24)</div>
          <div class="bar-grid">${renderBars(timeSeries, "#f4c542")}</div>
        </div>
        <div class="chart">
          <div class="chart-title">Outliers by Exchange</div>
          <div class="bar-grid">${renderBars(exchangeSeries, "#8bd3ff")}</div>
        </div>
        <div class="chart">
          <div class="chart-title">Outliers by Side</div>
          <div class="bar-grid">${renderBars(sideSeries, "#ff8c7a")}</div>
        </div>
      </div>

      <h2>Top Outliers (by Z)</h2>
      <table>
        <thead>
          <tr>
            <th>Start</th>
            <th>End</th>
            <th>Duration</th>
            <th>Symbol</th>
            <th>Market</th>
            <th>Exchange</th>
            <th>Side</th>
            <th>Price</th>
            <th>Max Z</th>
            <th>Avg Z</th>
            <th>Count</th>
            <th>Start BPS</th>
            <th>End BPS</th>
            <th>Filled %</th>
            <th>Start Size</th>
            <th>End Size</th>
            <th>Start Best Bid</th>
            <th>Start Best Ask</th>
            <th>End Best Bid</th>
            <th>End Best Ask</th>
            <th>Start Spread BPS</th>
            <th>End Spread BPS</th>
            <th>Start Imb</th>
            <th>End Imb</th>
            <th>Start Bid Depth</th>
            <th>Start Ask Depth</th>
            <th>End Bid Depth</th>
            <th>End Ask Depth</th>
            <th>Start Micro</th>
            <th>End Micro</th>
            <th>Start Rank</th>
            <th>End Rank</th>
            <th>Start Vol 1m</th>
            <th>End Vol 1m</th>
            <th>Start Vol 5m</th>
            <th>End Vol 5m</th>
            <th>Size Delta</th>
            <th>Size Delta %</th>
            <th>Trade Buy</th>
            <th>Trade Sell</th>
            <th>Trade Count</th>
          </tr>
        </thead>
        <tbody>
          ${renderTable(
            topOutliers
              .map(
                (row) => `<tr>
                  <td>${escapeHtml(new Date(row.startTs).toISOString())}</td>
                  <td>${escapeHtml(row.endLabel)}</td>
                  <td>${escapeHtml(formatDuration(row.durationMs))}</td>
                  <td>${escapeHtml(row.symbol)}</td>
                  <td>${escapeHtml(row.market)}</td>
                  <td>${escapeHtml(row.exchange)}</td>
                  <td>${escapeHtml(row.side)}</td>
                  <td>${formatNumber(row.price, 5)}</td>
                  <td>${formatNumber(row.maxZ, 2)}</td>
                  <td>${formatNumber(row.avgZ, 2)}</td>
                  <td>${row.count}</td>
                  <td>${formatNumber(row.startBps ?? 0, 2)}</td>
                  <td>${formatNumber(row.endBps ?? 0, 2)}</td>
                  <td>${formatNumber((row.filledPct ?? 0) * 100, 2)}</td>
                  <td>${formatNumber(row.startSize ?? 0, 2)}</td>
                  <td>${formatNumber(row.endSize ?? 0, 2)}</td>
                  <td>${formatNumber(row.startBestBid ?? 0, 5)}</td>
                  <td>${formatNumber(row.startBestAsk ?? 0, 5)}</td>
                  <td>${formatNumber(row.endBestBid ?? 0, 5)}</td>
                  <td>${formatNumber(row.endBestAsk ?? 0, 5)}</td>
                  <td>${formatNumber(row.startSpreadBps ?? 0, 2)}</td>
                  <td>${formatNumber(row.endSpreadBps ?? 0, 2)}</td>
                  <td>${formatNumber(row.startImbalance ?? 0, 4)}</td>
                  <td>${formatNumber(row.endImbalance ?? 0, 4)}</td>
                  <td>${formatNumber(row.startBidDepth ?? 0, 2)}</td>
                  <td>${formatNumber(row.startAskDepth ?? 0, 2)}</td>
                  <td>${formatNumber(row.endBidDepth ?? 0, 2)}</td>
                  <td>${formatNumber(row.endAskDepth ?? 0, 2)}</td>
                  <td>${formatNumber(row.startMicroprice ?? 0, 5)}</td>
                  <td>${formatNumber(row.endMicroprice ?? 0, 5)}</td>
                  <td>${row.startLevelRank ?? 0}</td>
                  <td>${row.endLevelRank ?? 0}</td>
                  <td>${formatNumber(row.startVol1m ?? 0, 6)}</td>
                  <td>${formatNumber(row.endVol1m ?? 0, 6)}</td>
                  <td>${formatNumber(row.startVol5m ?? 0, 6)}</td>
                  <td>${formatNumber(row.endVol5m ?? 0, 6)}</td>
                  <td>${formatNumber(row.sizeDelta ?? 0, 2)}</td>
                  <td>${formatNumber((row.sizeDeltaPct ?? 0) * 100, 2)}</td>
                  <td>${formatNumber(row.tradeBuyQty ?? 0, 2)}</td>
                  <td>${formatNumber(row.tradeSellQty ?? 0, 2)}</td>
                  <td>${row.tradeCount ?? 0}</td>
                </tr>`
              )
              .join(""),
            42
          )}
        </tbody>
      </table>

      <h2>Longest Outlier Spans</h2>
      <table>
        <thead>
          <tr>
            <th>Start</th>
            <th>End</th>
            <th>Duration</th>
            <th>Symbol</th>
            <th>Market</th>
            <th>Exchange</th>
            <th>Side</th>
            <th>Price</th>
            <th>Max Z</th>
          </tr>
        </thead>
        <tbody>
          ${renderTable(
            longestSpans
              .map(
                (row) => `<tr>
                  <td>${escapeHtml(new Date(row.startTs).toISOString())}</td>
                  <td>${escapeHtml(new Date(row.endTs).toISOString())}</td>
                  <td>${escapeHtml(formatDuration(row.durationMs))}</td>
                  <td>${escapeHtml(row.symbol)}</td>
                  <td>${escapeHtml(row.market)}</td>
                  <td>${escapeHtml(row.exchange)}</td>
                  <td>${escapeHtml(row.side)}</td>
                  <td>${formatNumber(row.price, 5)}</td>
                  <td>${formatNumber(row.maxZ, 2)}</td>
                </tr>`
              )
              .join(""),
            9
          )}
        </tbody>
      </table>

      <h2>Active Outlier Spans</h2>
      <table>
        <thead>
          <tr>
            <th>Start</th>
            <th>Duration</th>
            <th>Symbol</th>
            <th>Market</th>
            <th>Exchange</th>
            <th>Side</th>
            <th>Price</th>
            <th>Max Z</th>
          </tr>
        </thead>
        <tbody>
          ${renderTable(
            activeTop
              .map(
                (row) => `<tr>
                  <td>${escapeHtml(new Date(row.startTs).toISOString())}</td>
                  <td>${escapeHtml(formatDuration(row.durationMs))}</td>
                  <td>${escapeHtml(row.symbol)}</td>
                  <td>${escapeHtml(row.market)}</td>
                  <td>${escapeHtml(row.exchange)}</td>
                  <td>${escapeHtml(row.side)}</td>
                  <td>${formatNumber(row.price, 5)}</td>
                  <td>${formatNumber(row.maxZ, 2)}</td>
                </tr>`
              )
              .join(""),
            8
          )}
        </tbody>
      </table>

      <h2>Outlier Spans (All)</h2>
      <table>
        <thead>
          <tr>
            <th>Start</th>
            <th>End</th>
            <th>Duration</th>
            <th>Symbol</th>
            <th>Market</th>
            <th>Exchange</th>
            <th>Side</th>
            <th>Price</th>
            <th>Max Z</th>
            <th>Avg Z</th>
            <th>Count</th>
            <th>Status</th>
            <th>Start BPS</th>
            <th>End BPS</th>
            <th>Filled %</th>
            <th>Start Size</th>
            <th>End Size</th>
            <th>Start Best Bid</th>
            <th>Start Best Ask</th>
            <th>End Best Bid</th>
            <th>End Best Ask</th>
            <th>Start Spread BPS</th>
            <th>End Spread BPS</th>
            <th>Start Imb</th>
            <th>End Imb</th>
            <th>Start Bid Depth</th>
            <th>Start Ask Depth</th>
            <th>End Bid Depth</th>
            <th>End Ask Depth</th>
            <th>Start Micro</th>
            <th>End Micro</th>
            <th>Start Rank</th>
            <th>End Rank</th>
            <th>Start Vol 1m</th>
            <th>End Vol 1m</th>
            <th>Start Vol 5m</th>
            <th>End Vol 5m</th>
            <th>Size Delta</th>
            <th>Size Delta %</th>
            <th>Trade Buy</th>
            <th>Trade Sell</th>
            <th>Trade Count</th>
          </tr>
        </thead>
        <tbody>
          ${renderTable(
            combinedSpans
              .sort((a, b) => b.startTs - a.startTs)
              .map(
                (row) => `<tr>
                  <td>${escapeHtml(new Date(row.startTs).toISOString())}</td>
                  <td>${escapeHtml(row.endLabel)}</td>
                  <td>${escapeHtml(formatDuration(row.durationMs))}</td>
                  <td>${escapeHtml(row.symbol)}</td>
                  <td>${escapeHtml(row.market)}</td>
                  <td>${escapeHtml(row.exchange)}</td>
                  <td>${escapeHtml(row.side)}</td>
                  <td>${formatNumber(row.price, 5)}</td>
                  <td>${formatNumber(row.maxZ, 2)}</td>
                  <td>${formatNumber(row.avgZ, 2)}</td>
                  <td>${row.count}</td>
                  <td>${escapeHtml(row.status)}</td>
                  <td>${formatNumber(row.startBps ?? 0, 2)}</td>
                  <td>${formatNumber(row.endBps ?? 0, 2)}</td>
                  <td>${formatNumber((row.filledPct ?? 0) * 100, 2)}</td>
                  <td>${formatNumber(row.startSize ?? 0, 2)}</td>
                  <td>${formatNumber(row.endSize ?? 0, 2)}</td>
                  <td>${formatNumber(row.startBestBid ?? 0, 5)}</td>
                  <td>${formatNumber(row.startBestAsk ?? 0, 5)}</td>
                  <td>${formatNumber(row.endBestBid ?? 0, 5)}</td>
                  <td>${formatNumber(row.endBestAsk ?? 0, 5)}</td>
                  <td>${formatNumber(row.startSpreadBps ?? 0, 2)}</td>
                  <td>${formatNumber(row.endSpreadBps ?? 0, 2)}</td>
                  <td>${formatNumber(row.startImbalance ?? 0, 4)}</td>
                  <td>${formatNumber(row.endImbalance ?? 0, 4)}</td>
                  <td>${formatNumber(row.startBidDepth ?? 0, 2)}</td>
                  <td>${formatNumber(row.startAskDepth ?? 0, 2)}</td>
                  <td>${formatNumber(row.endBidDepth ?? 0, 2)}</td>
                  <td>${formatNumber(row.endAskDepth ?? 0, 2)}</td>
                  <td>${formatNumber(row.startMicroprice ?? 0, 5)}</td>
                  <td>${formatNumber(row.endMicroprice ?? 0, 5)}</td>
                  <td>${row.startLevelRank ?? 0}</td>
                  <td>${row.endLevelRank ?? 0}</td>
                  <td>${formatNumber(row.startVol1m ?? 0, 6)}</td>
                  <td>${formatNumber(row.endVol1m ?? 0, 6)}</td>
                  <td>${formatNumber(row.startVol5m ?? 0, 6)}</td>
                  <td>${formatNumber(row.endVol5m ?? 0, 6)}</td>
                  <td>${formatNumber(row.sizeDelta ?? 0, 2)}</td>
                  <td>${formatNumber((row.sizeDeltaPct ?? 0) * 100, 2)}</td>
                  <td>${formatNumber(row.tradeBuyQty ?? 0, 2)}</td>
                  <td>${formatNumber(row.tradeSellQty ?? 0, 2)}</td>
                  <td>${row.tradeCount ?? 0}</td>
                </tr>`
              )
              .join(""),
            43
          )}
        </tbody>
      </table>
    </body>
    <script>
      (function () {
        const container = document.querySelector('.export-window');
        if (!container) return;
        const startInput = document.getElementById('exportStart');
        const endInput = document.getElementById('exportEnd');
        const link = document.getElementById('exportWindowCsv');
        const base = container.getAttribute('data-csv-base');
        const updateLink = () => {
          const params = new URLSearchParams();
          if (startInput && startInput.value) {
            params.set('start', new Date(startInput.value).toISOString());
          }
          if (endInput && endInput.value) {
            params.set('end', new Date(endInput.value).toISOString());
          }
          const query = params.toString();
          link.href = query ? base + (base.includes('?') ? '&' : '?') + query : base;
        };
        startInput?.addEventListener('change', updateLink);
        endInput?.addEventListener('change', updateLink);
      })();
    </script>
  </html>`;
};

const parseWindowMs = (value: string | undefined, fallbackMs: number): number => {
  if (!value) return fallbackMs;
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) {
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : fallbackMs;
  }
  const match = trimmed.match(/^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/);
  if (!match) return fallbackMs;
  const amount = Number(match[1]);
  const unit = match[2];
  if (!Number.isFinite(amount)) return fallbackMs;
  switch (unit) {
    case "ms":
      return amount;
    case "s":
      return amount * 1000;
    case "m":
      return amount * 60 * 1000;
    case "h":
      return amount * 60 * 60 * 1000;
    case "d":
      return amount * 24 * 60 * 60 * 1000;
    default:
      return fallbackMs;
  }
};

const buildQueryString = (params: Record<string, string | number | undefined>): string => {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== "") {
      search.set(key, String(value));
    }
  });
  const query = search.toString();
  return query.length > 0 ? `?${query}` : "";
};

const parseTimeParam = (value: string | undefined): number | null => {
  if (!value) return null;
  if (/^\d+$/.test(value)) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const findBusiestWindow = (
  rows: OutlierRow[],
  windowMs: number
): { startTs: number; endTs: number; count: number } | null => {
  if (rows.length === 0) return null;
  const sorted = [...rows].sort((a, b) => a.ts - b.ts);
  let bestStart = sorted[0].ts;
  let bestEnd = bestStart + windowMs;
  let bestCount = 0;
  let j = 0;
  for (let i = 0; i < sorted.length; i += 1) {
    const startTs = sorted[i].ts;
    const endTs = startTs + windowMs;
    while (j < sorted.length && sorted[j].ts <= endTs) {
      j += 1;
    }
    const count = j - i;
    if (count > bestCount) {
      bestCount = count;
      bestStart = startTs;
      bestEnd = endTs;
    }
  }
  return { startTs: bestStart, endTs: bestEnd, count: bestCount };
};

function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function variance(values: number[]): number {
  if (values.length < 2) return 0;
  const m = mean(values);
  return values.reduce((sum, value) => sum + Math.pow(value - m, 2), 0) / (values.length - 1);
}

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.round((sorted.length - 1) * p)));
  return sorted[idx];
}

function pearsonCorr(a: number[], b: number[]): number {
  if (a.length < 2 || b.length < 2 || a.length !== b.length) return 0;
  const ma = mean(a);
  const mb = mean(b);
  const da = Math.sqrt(a.reduce((sum, v) => sum + Math.pow(v - ma, 2), 0));
  const db = Math.sqrt(b.reduce((sum, v) => sum + Math.pow(v - mb, 2), 0));
  if (da === 0 || db === 0) return 0;
  let num = 0;
  for (let i = 0; i < a.length; i += 1) {
    num += (a[i] - ma) * (b[i] - mb);
  }
  return num / (da * db);
}

function rank(values: number[]): number[] {
  const indices = values.map((_, idx) => idx).sort((a, b) => values[a] - values[b]);
  const ranks = new Array(values.length).fill(0);
  indices.forEach((idx, rankValue) => {
    ranks[idx] = rankValue + 1;
  });
  return ranks;
}

function spearmanCorr(x: number[], y: number[]): number {
  if (x.length < 2 || y.length < 2 || x.length !== y.length) return 0;
  return pearsonCorr(rank(x), rank(y));
}

function buildAnalysisSummary(): {
  changePoint: string;
  leadLag: string;
  burstiness: string;
  survival: string;
  odds: string;
  tradeCorr: string;
  volCorr: string;
} {
  const windowMs = 24 * 60 * 60 * 1000;
  const cutoff = Date.now() - windowMs;
  const spans = outlierSpanStore.getAll().filter((row) => row.endTs >= cutoff);

  const byExchangeMinute = new Map<string, Map<number, number>>();
  for (const row of spans) {
    const bucket = Math.floor(row.endTs / 60000);
    const ex = row.exchange;
    if (!byExchangeMinute.has(ex)) byExchangeMinute.set(ex, new Map());
    const map = byExchangeMinute.get(ex)!;
    map.set(bucket, (map.get(bucket) ?? 0) + 1);
  }

  let changePoint = "n/a";
  const changeParts = [];
  for (const [ex, map] of byExchangeMinute.entries()) {
    const buckets = Array.from(map.keys()).sort((a, b) => a - b);
    const series = buckets.map((b) => map.get(b) ?? 0);
    if (series.length < 20) continue;
    let bestIdx = -1;
    let bestDiff = 0;
    let bestPre = 0;
    let bestPost = 0;
    for (let i = 10; i < series.length - 10; i += 1) {
      const pre = series.slice(0, i);
      const post = series.slice(i);
      const diff = Math.abs(mean(pre) - mean(post));
      if (diff > bestDiff) {
        bestDiff = diff;
        bestIdx = i;
        bestPre = mean(pre);
        bestPost = mean(post);
      }
    }
    if (bestIdx >= 0) {
      changeParts.push(`${ex}: ${bestPre.toFixed(1)}->${bestPost.toFixed(1)}`);
    }
  }
  if (changeParts.length > 0) changePoint = changeParts.join(" | ");

  let leadLag = "n/a";
  if (byExchangeMinute.has("Bybit") && byExchangeMinute.has("MEXC")) {
    const byMap = byExchangeMinute.get("Bybit")!;
    const mxMap = byExchangeMinute.get("MEXC")!;
    const common = Array.from(byMap.keys()).filter((b) => mxMap.has(b)).sort((a, b) => a - b);
    const bySeries = common.map((b) => byMap.get(b) ?? 0);
    const mxSeries = common.map((b) => mxMap.get(b) ?? 0);
    let bestCorr = -1;
    let bestLag = 0;
    for (let lag = -30; lag <= 30; lag += 1) {
      let a: number[] = [];
      let b: number[] = [];
      if (lag < 0) {
        a = bySeries.slice(-lag);
        b = mxSeries.slice(0, a.length);
      } else if (lag > 0) {
        a = bySeries.slice(0, bySeries.length - lag);
        b = mxSeries.slice(lag);
      } else {
        a = bySeries;
        b = mxSeries;
      }
      const corr = pearsonCorr(a, b);
      if (corr > bestCorr) {
        bestCorr = corr;
        bestLag = lag;
      }
    }
    leadLag = `lag=${bestLag}m corr=${bestCorr.toFixed(2)}`;
  }

  const burstinessParts = [];
  for (const [ex, map] of byExchangeMinute.entries()) {
    const series = Array.from(map.values());
    const m = mean(series);
    const fano = m > 0 ? variance(series) / m : 0;
    burstinessParts.push(`${ex}: ${fano.toFixed(2)}`);
  }
  const burstiness = burstinessParts.length > 0 ? burstinessParts.join(" | ") : "n/a";

  const survivalBuckets = new Map<string, number[]>();
  for (const row of spans) {
    const key = `${row.exchange}:${row.side}`;
    if (!survivalBuckets.has(key)) survivalBuckets.set(key, []);
    survivalBuckets.get(key)!.push(row.durationMs);
  }
  const survivalParts = [];
  for (const [key, arr] of survivalBuckets.entries()) {
    survivalParts.push(`${key} med=${median(arr).toFixed(0)}ms`);
  }
  const survival = survivalParts.length > 0 ? survivalParts.join(" | ") : "n/a";

  const durations = spans.map((row) => row.durationMs);
  const flags = durations.map((d) => (d > 10_000 ? 1 : 0));
  const oddsParts = [];
  const features = {
    maxZ: spans.map((row) => row.maxZ),
    spread: spans.map((row) => row.startSpreadBps),
    vol1m: spans.map((row) => row.startVol1m),
  };
  for (const [name, arr] of Object.entries(features)) {
    if (arr.length < 4) continue;
    const q1 = percentile(arr, 0.25);
    const q3 = percentile(arr, 0.75);
    const top = flags.filter((_, idx) => arr[idx] >= q3);
    const bot = flags.filter((_, idx) => arr[idx] <= q1);
    const pTop = mean(top);
    const pBot = mean(bot);
    const oTop = pTop > 0 && pTop < 1 ? pTop / (1 - pTop) : 0;
    const oBot = pBot > 0 && pBot < 1 ? pBot / (1 - pBot) : 0;
    const oratio = oTop > 0 && oBot > 0 ? oTop / oBot : 0;
    oddsParts.push(`${name}=${oratio.toFixed(2)}`);
  }
  const odds = oddsParts.length > 0 ? oddsParts.join(" | ") : "n/a";

  const tradePairs = spans
    .map((row) => {
      const total = row.tradeBuyQty + row.tradeSellQty;
      if (total <= 0) return null;
      const tradeImbalance = (row.tradeBuyQty - row.tradeSellQty) / total;
      return { maxZ: row.maxZ, tradeImbalance };
    })
    .filter(Boolean) as Array<{ maxZ: number; tradeImbalance: number }>;
  let tradeCorr = "n/a";
  if (tradePairs.length > 1) {
    tradeCorr = pearsonCorr(
      tradePairs.map((item) => item.maxZ),
      tradePairs.map((item) => item.tradeImbalance)
    ).toFixed(2);
  }

  let volCorr = "n/a";
  if (byExchangeMinute.has("Bybit")) {
    const minuteRows = new Map<number, { count: number; volSum: number; n: number }>();
    for (const row of spans.filter((row) => row.exchange === "Bybit" && row.startVol1m > 0)) {
      const bucket = Math.floor(row.endTs / 60000);
      const entry = minuteRows.get(bucket) ?? { count: 0, volSum: 0, n: 0 };
      entry.count += 1;
      entry.volSum += row.startVol1m;
      entry.n += 1;
      minuteRows.set(bucket, entry);
    }
    const counts = [];
    const vols = [];
    for (const entry of minuteRows.values()) {
      counts.push(entry.count);
      vols.push(entry.volSum / Math.max(1, entry.n));
    }
    volCorr = pearsonCorr(counts, vols).toFixed(2);
  }

  return {
    changePoint,
    leadLag,
    burstiness,
    survival,
    odds,
    tradeCorr,
    volCorr,
  };
}

function erfc(x: number): number {
  // Abramowitz and Stegun 7.1.26 approximation.
  const z = Math.abs(x);
  const t = 1 / (1 + 0.5 * z);
  const tau =
    t *
    Math.exp(
      -z * z -
        1.26551223 +
        t *
          (1.00002368 +
            t *
              (0.37409196 +
                t *
                  (0.09678418 +
                    t *
                      (-0.18628806 +
                        t * (0.27886807 + t * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.17087277))))))))
    );
  return x >= 0 ? tau : 2 - tau;
}

const buildOutlierSpanCsv = (spans: OutlierSpanRow[], activeSpans: ActiveSpanRow[]): string => {
  const header =
    "start_ts,end_ts,duration_ms,status,symbol,market,exchange,side,price,max_z,avg_z,count,start_size,end_size,filled_pct,start_bps,end_bps,start_book,end_book,start_best_bid,start_best_ask,end_best_bid,end_best_ask,start_spread_bps,end_spread_bps,start_imbalance,end_imbalance,start_bid_depth,start_ask_depth,end_bid_depth,end_ask_depth,start_microprice,end_microprice,start_level_rank,end_level_rank,start_vol_1m,end_vol_1m,start_vol_5m,end_vol_5m,size_delta,size_delta_pct,trade_buy_qty,trade_sell_qty,trade_count";
  const now = Date.now();
  const rows = [
    ...spans.map((row) => ({ ...row, status: "closed", endTs: row.endTs, durationMs: row.durationMs })),
    ...activeSpans.map((row) => ({
      ...row,
      status: "active",
      endTs: now,
      durationMs: row.durationMs,
    })),
  ];
  const lines = rows.map((row) =>
    [
      new Date(row.startTs).toISOString(),
      new Date(row.endTs).toISOString(),
      Math.round(row.durationMs),
      row.status,
      row.symbol,
      row.market,
      row.exchange,
      row.side,
      formatNumber(row.price, 8),
      formatNumber(row.maxZ, 6),
      formatNumber(row.avgZ, 6),
      row.count,
      formatNumber(row.startSize, 8),
      formatNumber(row.endSize, 8),
      formatNumber(row.filledPct, 6),
      formatNumber(row.startBps, 4),
      formatNumber(row.endBps, 4),
      row.startBook || "",
      row.endBook || "",
      formatNumber(row.startBestBid, 8),
      formatNumber(row.startBestAsk, 8),
      formatNumber(row.endBestBid, 8),
      formatNumber(row.endBestAsk, 8),
      formatNumber(row.startSpreadBps, 4),
      formatNumber(row.endSpreadBps, 4),
      formatNumber(row.startImbalance, 6),
      formatNumber(row.endImbalance, 6),
      formatNumber(row.startBidDepth, 6),
      formatNumber(row.startAskDepth, 6),
      formatNumber(row.endBidDepth, 6),
      formatNumber(row.endAskDepth, 6),
      formatNumber(row.startMicroprice, 8),
      formatNumber(row.endMicroprice, 8),
      String(row.startLevelRank ?? 0),
      String(row.endLevelRank ?? 0),
      formatNumber(row.startVol1m, 8),
      formatNumber(row.endVol1m, 8),
      formatNumber(row.startVol5m, 8),
      formatNumber(row.endVol5m, 8),
      formatNumber(row.sizeDelta, 8),
      formatNumber(row.sizeDeltaPct, 6),
      formatNumber(row.tradeBuyQty, 8),
      formatNumber(row.tradeSellQty, 8),
      String(row.tradeCount ?? 0),
    ]
      .map((value) => `"${String(value).replace(/"/g, '""')}"`)
      .join(",")
  );
  return [header, ...lines].join("\n");
};

const writeOutlierPdf = (
  res: express.Response,
  {
    title,
    spans,
    activeSpans,
    symbol,
    market,
    exchange,
    windowLabel,
    metaLines,
  }: {
    title: string;
    spans: OutlierSpanRow[];
    activeSpans: ActiveSpanRow[];
    symbol?: string;
    market?: string;
    exchange?: string;
    windowLabel?: string;
    metaLines?: string[];
  }
): void => {
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", "inline; filename=\"outlier-report.pdf\"");
  const doc = new PDFDocument({ margin: 36, size: "LETTER", layout: "landscape" });
  doc.pipe(res);

  doc.fontSize(16).text(title);
  doc.moveDown(0.4);
  doc.fontSize(9).text(`Generated: ${new Date().toISOString()}`);
  doc.text(`Symbol: ${symbol ?? "All"}  Market: ${market ?? "All"}  Exchange: ${exchange ?? "All"}`);
  if (windowLabel) {
    doc.text(`Window: ${windowLabel}`);
  }
  (metaLines ?? []).forEach((line) => {
    if (line.trim().length > 0) doc.text(line);
  });
  doc.moveDown(0.6);

  const headers = [
    "Start",
    "End",
    "Dur",
    "Market",
    "Exchange",
    "Side",
    "Price",
    "Max Z",
    "Avg Z",
    "Count",
  ];
  const tableWidth = doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const colWidths = [
    tableWidth * 0.15,
    tableWidth * 0.15,
    tableWidth * 0.06,
    tableWidth * 0.08,
    tableWidth * 0.1,
    tableWidth * 0.06,
    tableWidth * 0.12,
    tableWidth * 0.08,
    tableWidth * 0.08,
    tableWidth * 0.12,
  ];
  let y = doc.y;
  let x = doc.page.margins.left;
  doc.fontSize(8).font("Helvetica-Bold");
  headers.forEach((header, i) => {
    doc.text(header, x, y, { width: colWidths[i], lineBreak: false });
    x += colWidths[i];
  });
  doc.moveDown(0.4);
  doc.font("Helvetica").fontSize(8);
  y = doc.y;

  const now = Date.now();
  const rows = [
    ...spans.map((row) => ({ ...row, endTs: row.endTs, durationMs: row.durationMs })),
    ...activeSpans.map((row) => ({ ...row, endTs: now, durationMs: row.durationMs })),
  ];
  for (const row of rows) {
    if (y > doc.page.height - 50) {
      doc.addPage();
      y = doc.y;
    }
    x = doc.page.margins.left;
    const line = [
      new Date(row.startTs).toISOString().replace("T", " ").slice(0, 19),
      new Date(row.endTs).toISOString().replace("T", " ").slice(0, 19),
      formatDuration(row.durationMs),
      row.market,
      row.exchange,
      row.side,
      formatNumber(row.price, 5),
      formatNumber(row.maxZ, 2),
      formatNumber(row.avgZ, 2),
      String(row.count),
    ];
    line.forEach((value, i) => {
      doc.text(value, x, y, { width: colWidths[i], lineBreak: false, ellipsis: true });
      x += colWidths[i];
    });
    y += 12;
  }

  doc.end();
};

router.get("/api/outliers/report", (req, res) => {
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  const market = typeof req.query.market === "string" ? req.query.market : undefined;
  const exchange = typeof req.query.exchange === "string" ? req.query.exchange : undefined;
  const limit = Number(req.query.limit ?? 1000);
  const history = outlierStore.getHistory(limit, symbol, market, exchange);
  const spans = outlierSpanStore.getHistory(200, symbol, market, exchange);
  const activeSpans = outlierSpanTracker.getActive().filter((item) => {
    if (symbol && item.symbol !== symbol) return false;
    if (market && item.market !== market) return false;
    if (exchange && item.exchange.toLowerCase() !== exchange.toLowerCase()) return false;
    return true;
  });
  const query = buildQueryString({ symbol, market, exchange, limit });

  const reportHtml = buildOutlierReportHtml({
    title: "Order Book Outlier Report",
    history,
    spans,
    activeSpans,
    symbol,
    market,
    exchange,
    exportLinks: {
      csv: `${basePath}/api/outliers/report/csv${query}`,
      pdf: `${basePath}/api/outliers/report/pdf${query}`,
      csvAll: `${basePath}/api/outliers/report/csv/all`,
    },
  });

  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(reportHtml);
});

router.get("/api/outliers/report/csv", (req, res) => {
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  const market = typeof req.query.market === "string" ? req.query.market : undefined;
  const exchange = typeof req.query.exchange === "string" ? req.query.exchange : undefined;
  const limit = Number(req.query.limit ?? 1000);
  const startTs = parseTimeParam(typeof req.query.start === "string" ? req.query.start : undefined);
  const endTs = parseTimeParam(typeof req.query.end === "string" ? req.query.end : undefined);
  const spans = outlierSpanStore.getHistory(200, symbol, market, exchange).filter((row) => {
    if (startTs !== null && row.endTs < startTs) return false;
    if (endTs !== null && row.startTs > endTs) return false;
    return true;
  });
  const activeSpans = outlierSpanTracker.getActive().filter((item) => {
    if (symbol && item.symbol !== symbol) return false;
    if (market && item.market !== market) return false;
    if (exchange && item.exchange.toLowerCase() !== exchange.toLowerCase()) return false;
    if (startTs !== null && item.endTs < startTs) return false;
    if (endTs !== null && item.startTs > endTs) return false;
    return true;
  });
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", "attachment; filename=\"outlier-report.csv\"");
  res.send(buildOutlierSpanCsv(spans, activeSpans));
});

router.get("/api/outliers/report/csv/all", (req, res) => {
  const spans = outlierSpanStore.getAll();
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", "attachment; filename=\"outlier-report-all.csv\"");
  res.send(buildOutlierSpanCsv(spans, []));
});

router.get("/api/outliers/report/pdf", (req, res) => {
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  const market = typeof req.query.market === "string" ? req.query.market : undefined;
  const exchange = typeof req.query.exchange === "string" ? req.query.exchange : undefined;
  const spans = outlierSpanStore.getHistory(200, symbol, market, exchange);
  const activeSpans = outlierSpanTracker.getActive().filter((item) => {
    if (symbol && item.symbol !== symbol) return false;
    if (market && item.market !== market) return false;
    if (exchange && item.exchange.toLowerCase() !== exchange.toLowerCase()) return false;
    return true;
  });
  writeOutlierPdf(res, {
    title: "Order Book Outlier Report",
    spans,
    activeSpans,
    symbol,
    market,
    exchange,
  });
});

router.get("/api/outliers/report/busiest", (req, res) => {
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  const market = typeof req.query.market === "string" ? req.query.market : undefined;
  const exchange =
    typeof req.query.exchange === "string" ? req.query.exchange : "bybit";
  const limit = Number(req.query.limit ?? 20000);
  const zThreshold = Number(req.query.z ?? 5);
  const windowMs = parseWindowMs(
    typeof req.query.window === "string" ? req.query.window : "1h",
    60 * 60 * 1000
  );

  const rawHistory = outlierStore.getHistory(limit, symbol, market, exchange);
  const highZHistory = rawHistory.filter((row) => row.zScore >= zThreshold);
  const selectionHistory = highZHistory.length > 0 ? highZHistory : rawHistory;
  const busiest = findBusiestWindow(selectionHistory, windowMs);
  const windowStart = busiest?.startTs ?? 0;
  const windowEnd = busiest?.endTs ?? 0;

  const windowHistory = highZHistory.filter(
    (row) => row.ts >= windowStart && row.ts <= windowEnd
  );
  const highZWindowCount = highZHistory.filter(
    (row) => row.ts >= windowStart && row.ts <= windowEnd
  ).length;
  const spans = outlierSpanStore
    .getHistory(limit, symbol, market, exchange)
    .filter((row) => row.endTs >= windowStart && row.startTs <= windowEnd);
  const highZSpans = spans.filter((row) => row.maxZ >= zThreshold);
  const activeSpans = outlierSpanTracker
    .getActive()
    .filter((row) => row.exchange.toLowerCase() === exchange.toLowerCase())
    .filter((row) => (symbol ? row.symbol === symbol : true))
    .filter((row) => (market ? row.market === market : true))
    .filter((row) => row.startTs <= windowEnd);
  const activeHighZSpans = activeSpans.filter((row) => row.maxZ >= zThreshold);

  const windowLabel =
    busiest && rawHistory.length > 0
      ? `${new Date(windowStart).toISOString()} → ${new Date(windowEnd).toISOString()}`
      : "n/a";

  const reportHtml = buildOutlierReportHtml({
    title: "High Z Outlier Window Report",
    history: windowHistory,
    spans: highZSpans,
    activeSpans: activeHighZSpans,
    symbol,
    market,
    exchange,
    windowLabel,
    metaLines: [
      `Window length: ${formatDuration(windowMs)}`,
      `Z threshold: ${formatNumber(zThreshold, 2)}+`,
      `High-Z outliers in busiest window: ${highZWindowCount}`,
      highZHistory.length === 0 ? "No high-Z outliers found; window picked from all outliers." : "",
    ],
    exportLinks: {
      csv: `${basePath}/api/outliers/report/busiest/csv${buildQueryString({
        symbol,
        market,
        exchange,
        window: typeof req.query.window === "string" ? req.query.window : "1h",
        z: zThreshold,
        limit,
      })}`,
      pdf: `${basePath}/api/outliers/report/busiest/pdf${buildQueryString({
        symbol,
        market,
        exchange,
        window: typeof req.query.window === "string" ? req.query.window : "1h",
        z: zThreshold,
        limit,
      })}`,
    },
  });

  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(reportHtml);
});

router.get("/api/outliers/report/busiest/csv", (req, res) => {
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  const market = typeof req.query.market === "string" ? req.query.market : undefined;
  const exchange =
    typeof req.query.exchange === "string" ? req.query.exchange : "bybit";
  const limit = Number(req.query.limit ?? 20000);
  const zThreshold = Number(req.query.z ?? 5);
  const windowMs = parseWindowMs(
    typeof req.query.window === "string" ? req.query.window : "1h",
    60 * 60 * 1000
  );

  const rawHistory = outlierStore.getHistory(limit, symbol, market, exchange);
  const highZHistory = rawHistory.filter((row) => row.zScore >= zThreshold);
  const selectionHistory = highZHistory.length > 0 ? highZHistory : rawHistory;
  const busiest = findBusiestWindow(selectionHistory, windowMs);
  const windowStart = busiest?.startTs ?? 0;
  const windowEnd = busiest?.endTs ?? 0;
  const windowHistory = highZHistory.filter(
    (row) => row.ts >= windowStart && row.ts <= windowEnd
  );

  const spans = outlierSpanStore
    .getHistory(limit, symbol, market, exchange)
    .filter((row) => row.maxZ >= zThreshold)
    .filter((row) => row.endTs >= windowStart && row.startTs <= windowEnd);
  const activeSpans = outlierSpanTracker
    .getActive()
    .filter((row) => row.exchange.toLowerCase() === exchange.toLowerCase())
    .filter((row) => (symbol ? row.symbol === symbol : true))
    .filter((row) => (market ? row.market === market : true))
    .filter((row) => row.maxZ >= zThreshold)
    .filter((row) => row.startTs <= windowEnd);

  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", "attachment; filename=\"outlier-busiest.csv\"");
  res.send(buildOutlierSpanCsv(spans, activeSpans));
});

router.get("/api/outliers/report/busiest/pdf", (req, res) => {
  const symbol = typeof req.query.symbol === "string" ? req.query.symbol : undefined;
  const market = typeof req.query.market === "string" ? req.query.market : undefined;
  const exchange =
    typeof req.query.exchange === "string" ? req.query.exchange : "bybit";
  const limit = Number(req.query.limit ?? 20000);
  const zThreshold = Number(req.query.z ?? 5);
  const windowMs = parseWindowMs(
    typeof req.query.window === "string" ? req.query.window : "1h",
    60 * 60 * 1000
  );

  const rawHistory = outlierStore.getHistory(limit, symbol, market, exchange);
  const highZHistory = rawHistory.filter((row) => row.zScore >= zThreshold);
  const selectionHistory = highZHistory.length > 0 ? highZHistory : rawHistory;
  const busiest = findBusiestWindow(selectionHistory, windowMs);
  const windowStart = busiest?.startTs ?? 0;
  const windowEnd = busiest?.endTs ?? 0;
  const windowHistory = highZHistory.filter(
    (row) => row.ts >= windowStart && row.ts <= windowEnd
  );
  const spans = outlierSpanStore
    .getHistory(limit, symbol, market, exchange)
    .filter((row) => row.maxZ >= zThreshold)
    .filter((row) => row.endTs >= windowStart && row.startTs <= windowEnd);
  const activeSpans = outlierSpanTracker
    .getActive()
    .filter((row) => row.exchange.toLowerCase() === exchange.toLowerCase())
    .filter((row) => (symbol ? row.symbol === symbol : true))
    .filter((row) => (market ? row.market === market : true))
    .filter((row) => row.maxZ >= zThreshold)
    .filter((row) => row.startTs <= windowEnd);
  const windowLabel =
    busiest && windowHistory.length > 0
      ? `${new Date(windowStart).toISOString()} → ${new Date(windowEnd).toISOString()}`
      : "n/a";

  writeOutlierPdf(res, {
    title: "High Z Outlier Window Report",
    spans,
    activeSpans,
    symbol,
    market,
    exchange,
    windowLabel,
    metaLines: [
      `Window length: ${formatDuration(windowMs)}`,
      `Z threshold: ${formatNumber(zThreshold, 2)}+`,
      `High-Z outliers in busiest window: ${windowHistory.length}`,
      highZHistory.length === 0 ? "No high-Z outliers found; window picked from all outliers." : "",
    ],
  });
});

router.get("/api/analysis/report/pdf", (req, res) => {
  const windowMs = parseWindowMs(
    typeof req.query.window === "string" ? req.query.window : "24h",
    24 * 60 * 60 * 1000
  );
  const cutoff = Date.now() - windowMs;
  const spans = outlierSpanStore.getAll().filter((row) => row.endTs >= cutoff);

  const durations = spans.map((row) => row.durationMs);
  const byExchangeMinute = new Map<string, Map<number, number>>();
  for (const row of spans) {
    const bucket = Math.floor(row.endTs / 60000);
    const ex = row.exchange;
    if (!byExchangeMinute.has(ex)) byExchangeMinute.set(ex, new Map());
    const map = byExchangeMinute.get(ex)!;
    map.set(bucket, (map.get(bucket) ?? 0) + 1);
  }

  const changePoints: Array<{ exchange: string; bucket: number; meanPre: number; meanPost: number }> = [];
  for (const [ex, map] of byExchangeMinute.entries()) {
    const buckets = Array.from(map.keys()).sort((a, b) => a - b);
    const series = buckets.map((b) => map.get(b) ?? 0);
    if (series.length < 20) continue;
    let bestIdx = -1;
    let bestDiff = 0;
    let bestPre = 0;
    let bestPost = 0;
    for (let i = 10; i < series.length - 10; i += 1) {
      const pre = series.slice(0, i);
      const post = series.slice(i);
      const diff = Math.abs(mean(pre) - mean(post));
      if (diff > bestDiff) {
        bestDiff = diff;
        bestIdx = i;
        bestPre = mean(pre);
        bestPost = mean(post);
      }
    }
    if (bestIdx >= 0) {
      changePoints.push({ exchange: ex, bucket: buckets[bestIdx], meanPre: bestPre, meanPost: bestPost });
    }
  }

  let leadLag = { lag: 0, corr: 0 };
  if (byExchangeMinute.has("Bybit") && byExchangeMinute.has("MEXC")) {
    const byMap = byExchangeMinute.get("Bybit")!;
    const mxMap = byExchangeMinute.get("MEXC")!;
    const common = Array.from(byMap.keys()).filter((b) => mxMap.has(b)).sort((a, b) => a - b);
    const bySeries = common.map((b) => byMap.get(b) ?? 0);
    const mxSeries = common.map((b) => mxMap.get(b) ?? 0);
    let bestCorr = -1;
    let bestLag = 0;
    for (let lag = -30; lag <= 30; lag += 1) {
      let a: number[] = [];
      let b: number[] = [];
      if (lag < 0) {
        a = bySeries.slice(-lag);
        b = mxSeries.slice(0, a.length);
      } else if (lag > 0) {
        a = bySeries.slice(0, bySeries.length - lag);
        b = mxSeries.slice(lag);
      } else {
        a = bySeries;
        b = mxSeries;
      }
      const corr = pearsonCorr(a, b);
      if (corr > bestCorr) {
        bestCorr = corr;
        bestLag = lag;
      }
    }
    leadLag = { lag: bestLag, corr: bestCorr };
  }

  const spearman = {
    maxZ: spearmanCorr(durations, spans.map((row) => row.maxZ)),
    imbalance: spearmanCorr(durations, spans.map((row) => row.startImbalance)),
    spreadBps: spearmanCorr(durations, spans.map((row) => row.startSpreadBps)),
    bidDepth: spearmanCorr(durations, spans.map((row) => row.startBidDepth)),
    askDepth: spearmanCorr(durations, spans.map((row) => row.startAskDepth)),
    vol1m: spearmanCorr(durations, spans.map((row) => row.startVol1m)),
    vol5m: spearmanCorr(durations, spans.map((row) => row.startVol5m)),
  };

  const survival = new Map<string, { n: number; medianMs: number; p75: number; p90: number }>();
  const buckets = new Map<string, number[]>();
  for (const row of spans) {
    const key = `${row.exchange}:${row.side}`;
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key)!.push(row.durationMs);
  }
  for (const [key, arr] of buckets.entries()) {
    survival.set(key, {
      n: arr.length,
      medianMs: median(arr),
      p75: percentile(arr, 0.75),
      p90: percentile(arr, 0.9),
    });
  }

  const longThreshold = 10_000;
  const flags = durations.map((d) => (d > longThreshold ? 1 : 0));
  const features = {
    maxZ: spans.map((row) => row.maxZ),
    imbalance: spans.map((row) => row.startImbalance),
    spreadBps: spans.map((row) => row.startSpreadBps),
    bidDepth: spans.map((row) => row.startBidDepth),
    askDepth: spans.map((row) => row.startAskDepth),
    vol1m: spans.map((row) => row.startVol1m),
    vol5m: spans.map((row) => row.startVol5m),
  };
  const oddsRatio: Record<string, number> = {};
  for (const [name, arr] of Object.entries(features)) {
    if (arr.length < 4) {
      oddsRatio[name] = 0;
      continue;
    }
    const q1 = percentile(arr, 0.25);
    const q3 = percentile(arr, 0.75);
    const top = flags.filter((_, idx) => arr[idx] >= q3);
    const bot = flags.filter((_, idx) => arr[idx] <= q1);
    const pTop = mean(top);
    const pBot = mean(bot);
    const oTop = pTop > 0 && pTop < 1 ? pTop / (1 - pTop) : 0;
    const oBot = pBot > 0 && pBot < 1 ? pBot / (1 - pBot) : 0;
    oddsRatio[name] = oTop > 0 && oBot > 0 ? oTop / oBot : 0;
  }

  const burstiness: Record<string, number> = {};
  for (const [ex, map] of byExchangeMinute.entries()) {
    const series = Array.from(map.values());
    const m = mean(series);
    burstiness[ex] = m > 0 ? variance(series) / m : 0;
  }

  const tradePairs = spans
    .map((row) => {
      const total = row.tradeBuyQty + row.tradeSellQty;
      if (total <= 0) return null;
      const tradeImbalance = (row.tradeBuyQty - row.tradeSellQty) / total;
      return { maxZ: row.maxZ, tradeImbalance };
    })
    .filter(Boolean) as Array<{ maxZ: number; tradeImbalance: number }>;
  const tradeCorr = tradePairs.length > 1
    ? pearsonCorr(
        tradePairs.map((item) => item.maxZ),
        tradePairs.map((item) => item.tradeImbalance)
      )
    : 0;

  let volCorr = 0;
  if (byExchangeMinute.has("Bybit")) {
    const minuteRows = new Map<number, { count: number; volSum: number; n: number }>();
    for (const row of spans.filter((row) => row.exchange === "Bybit" && row.startVol1m > 0)) {
      const bucket = Math.floor(row.endTs / 60000);
      const entry = minuteRows.get(bucket) ?? { count: 0, volSum: 0, n: 0 };
      entry.count += 1;
      entry.volSum += row.startVol1m;
      entry.n += 1;
      minuteRows.set(bucket, entry);
    }
    const counts = [];
    const vols = [];
    for (const entry of minuteRows.values()) {
      counts.push(entry.count);
      vols.push(entry.volSum / Math.max(1, entry.n));
    }
    volCorr = pearsonCorr(counts, vols);
  }

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", "inline; filename=\"span-analysis.pdf\"");
  const doc = new PDFDocument({ margin: 36, size: "LETTER" });
  doc.pipe(res);

  doc.fontSize(16).text("Span Statistical Analysis");
  doc.moveDown(0.4);
  doc.fontSize(9).text(`Generated: ${new Date().toISOString()}`);
  doc.text(`Window: ${formatDuration(windowMs)} (${new Date(cutoff).toISOString()} → ${new Date().toISOString()})`);
  doc.text(`Total spans: ${spans.length}`);
  doc.moveDown(0.8);

  doc.font("Helvetica-Bold").text("Change Point (Minute Counts)");
  doc.font("Helvetica");
  if (changePoints.length === 0) {
    doc.text("No change points detected.");
  } else {
    changePoints.forEach((item) => {
      doc.text(
        `${item.exchange}: bucket ${item.bucket} mean_pre=${item.meanPre.toFixed(2)} mean_post=${item.meanPost.toFixed(2)}`
      );
    });
  }
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Lead/Lag (Bybit vs MEXC)");
  doc.font("Helvetica");
  doc.text(`Best lag: ${leadLag.lag} min, correlation: ${leadLag.corr.toFixed(3)}`);
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Duration vs Features (Spearman)");
  doc.font("Helvetica");
  Object.entries(spearman).forEach(([key, value]) => {
    doc.text(`${key}: ${value.toFixed(3)}`);
  });
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Survival Stats (Median / P75 / P90 ms)");
  doc.font("Helvetica");
  for (const [key, stats] of survival.entries()) {
    doc.text(`${key}: n=${stats.n} median=${stats.medianMs.toFixed(0)} p75=${stats.p75.toFixed(0)} p90=${stats.p90.toFixed(0)}`);
  }
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Long Span Odds Ratio (>10s)");
  doc.font("Helvetica");
  Object.entries(oddsRatio).forEach(([key, value]) => {
    doc.text(`${key}: ${value.toFixed(2)}`);
  });
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Burstiness (Fano Factor)");
  doc.font("Helvetica");
  Object.entries(burstiness).forEach(([key, value]) => {
    doc.text(`${key}: ${value.toFixed(2)}`);
  });
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Trade Imbalance vs Max Z");
  doc.font("Helvetica");
  doc.text(`Correlation: ${tradeCorr.toFixed(3)} (n=${tradePairs.length})`);
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Volatility vs Count (Bybit)");
  doc.font("Helvetica");
  doc.text(`Correlation: ${volCorr.toFixed(3)}`);

  doc.end();
});

router.get("/api/analysis/report/compare/pdf", (req, res) => {
  const startA = parseTimeParam(typeof req.query.startA === "string" ? req.query.startA : undefined);
  const endA = parseTimeParam(typeof req.query.endA === "string" ? req.query.endA : undefined);
  const startB = parseTimeParam(typeof req.query.startB === "string" ? req.query.startB : undefined);
  const endB = parseTimeParam(typeof req.query.endB === "string" ? req.query.endB : undefined);
  const exchange = typeof req.query.exchange === "string" ? req.query.exchange : "Bybit";
  if (startA === null || endA === null || startB === null || endB === null) {
    res.status(400).json({ error: "startA,endA,startB,endB are required (ms or ISO)." });
    return;
  }

  const spans = outlierSpanStore.getAll().filter((row) => row.exchange === exchange);
  const windowA = spans.filter((row) => row.endTs >= startA && row.endTs < endA);
  const windowB = spans.filter((row) => row.endTs >= startB && row.endTs < endB);

  const durationA = windowA.map((row) => row.durationMs);
  const durationB = windowB.map((row) => row.durationMs);
  const maxZA = windowA.map((row) => row.maxZ);
  const maxZB = windowB.map((row) => row.maxZ);
  const spreadA = windowA.map((row) => row.startSpreadBps);
  const spreadB = windowB.map((row) => row.startSpreadBps);
  const imbalanceA = windowA.map((row) => row.startImbalance);
  const imbalanceB = windowB.map((row) => row.startImbalance);
  const vol1mA = windowA.map((row) => row.startVol1m);
  const vol1mB = windowB.map((row) => row.startVol1m);
  const tradeCountA = windowA.reduce((sum, row) => sum + (row.tradeCount ?? 0), 0);
  const tradeCountB = windowB.reduce((sum, row) => sum + (row.tradeCount ?? 0), 0);

  const countA = windowA.length;
  const countB = windowB.length;
  const secondsA = Math.max(1, (endA - startA) / 1000);
  const secondsB = Math.max(1, (endB - startB) / 1000);
  const rateA = countA / secondsA;
  const rateB = countB / secondsB;
  const zCount = countA + countB > 0 ? (countB - countA) / Math.sqrt(countA + countB) : 0;
  const pCount = erfc(Math.abs(zCount) / Math.sqrt(2));

  let tDuration = 0;
  let pDuration = 1;
  let dfDuration = 0;
  let dDuration = 0;
  if (durationA.length > 1 && durationB.length > 1) {
    const meanA = mean(durationA);
    const meanB = mean(durationB);
    const varA = variance(durationA);
    const varB = variance(durationB);
    const nA = durationA.length;
    const nB = durationB.length;
    tDuration = (meanA - meanB) / Math.sqrt(varA / nA + varB / nB);
    dfDuration =
      Math.pow(varA / nA + varB / nB, 2) /
      (Math.pow(varA / nA, 2) / (nA - 1) + Math.pow(varB / nB, 2) / (nB - 1));
    pDuration = erfc(Math.abs(tDuration) / Math.sqrt(2));
    const pooled = Math.sqrt(((nA - 1) * varA + (nB - 1) * varB) / (nA + nB - 2));
    dDuration = pooled > 0 ? (meanA - meanB) / pooled : 0;
  }

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", "inline; filename=\"span-compare.pdf\"");
  const doc = new PDFDocument({ margin: 36, size: "LETTER" });
  doc.pipe(res);

  doc.fontSize(16).text("Span Window Comparison");
  doc.moveDown(0.4);
  doc.fontSize(9).text(`Generated: ${new Date().toISOString()}`);
  doc.text(`Exchange: ${exchange}`);
  doc.text(`Window A: ${new Date(startA).toISOString()} -> ${new Date(endA).toISOString()}`);
  doc.text(`Window B: ${new Date(startB).toISOString()} -> ${new Date(endB).toISOString()}`);
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Counts / Rates");
  doc.font("Helvetica");
  doc.text(`A: ${countA} spans (${(rateA * 60).toFixed(2)}/min)`);
  doc.text(`B: ${countB} spans (${(rateB * 60).toFixed(2)}/min)`);
  doc.text(`Poisson z: ${zCount.toFixed(2)}  p~${pCount.toFixed(4)}`);
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Duration Summary (ms)");
  doc.font("Helvetica");
  doc.text(`A: mean=${mean(durationA).toFixed(0)} median=${median(durationA).toFixed(0)} p90=${percentile(durationA, 0.9).toFixed(0)} n=${durationA.length}`);
  doc.text(`B: mean=${mean(durationB).toFixed(0)} median=${median(durationB).toFixed(0)} p90=${percentile(durationB, 0.9).toFixed(0)} n=${durationB.length}`);
  doc.text(`Welch t: ${tDuration.toFixed(2)}  df~${dfDuration.toFixed(1)}  p~${pDuration.toFixed(4)}  d=${dDuration.toFixed(2)}`);
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Context Features (A vs B)");
  doc.font("Helvetica");
  doc.text(`Max Z mean: ${mean(maxZA).toFixed(2)} vs ${mean(maxZB).toFixed(2)}`);
  doc.text(`Start spread bps mean: ${mean(spreadA).toFixed(2)} vs ${mean(spreadB).toFixed(2)}`);
  doc.text(`Start imbalance mean: ${mean(imbalanceA).toFixed(3)} vs ${mean(imbalanceB).toFixed(3)}`);
  doc.text(`Start vol1m mean: ${mean(vol1mA).toFixed(6)} vs ${mean(vol1mB).toFixed(6)}`);
  doc.text(`Trades matched to spans: ${tradeCountA} vs ${tradeCountB}`);
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Additional Analyses (Last 24h, Global)");
  doc.font("Helvetica");
  doc.text("These metrics summarize broader regime behavior and are not window-specific.");
  const summary = buildAnalysisSummary();
  doc.text(`Change point (minute counts): ${summary.changePoint}`);
  doc.text(`Lead/lag (Bybit vs MEXC): ${summary.leadLag}`);
  doc.text(`Burstiness (Fano): ${summary.burstiness}`);
  doc.text(`Survival medians: ${summary.survival}`);
  doc.text(`Long-span odds (>10s): ${summary.odds}`);
  doc.text(`Trade imbalance vs Max Z: ${summary.tradeCorr}`);
  doc.text(`Volatility vs count (Bybit): ${summary.volCorr}`);
  doc.moveDown(0.6);

  doc.font("Helvetica-Bold").text("Conclusion");
  doc.font("Helvetica");
  const countTrend = countB === countA ? "flat" : countB > countA ? "higher" : "lower";
  const durationTrend = mean(durationB) === mean(durationA)
    ? "unchanged"
    : mean(durationB) > mean(durationA)
      ? "longer"
      : "shorter";
  doc.text(
    `Counts are ${countTrend} in window B, with p~${pCount.toFixed(4)} for the count difference. ` +
      `Span durations are ${durationTrend} in window B (Welch p~${pDuration.toFixed(4)}). ` +
      `Context feature averages (max Z, spread, imbalance, vol) above provide additional signal for regime differences.`
  );

  doc.end();
});
app.use(basePath || "/", router);

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

function broadcast(payload: unknown): void {
  const message = JSON.stringify(payload);
  for (const client of wss.clients) {
    if (client.readyState === client.OPEN) {
      client.send(message);
    }
  }
}

const spotLatest: Record<string, Map<string, LatestState>> = {
  bybit: new Map(),
  mexc: new Map(),
};
const perpLatest: Record<string, Map<string, LatestState>> = {
  bybit: new Map(),
  mexc: new Map(),
};
const spotMidHistory: Record<string, Map<string, Array<{ ts: number; mid: number }>>> = {
  bybit: new Map(),
  mexc: new Map(),
};
const lastSnapshot = new Map<string, { bids: Array<[number, number]>; asks: Array<[number, number]> }>();
const lastMetrics = new Map<string, ReturnType<typeof computeMetrics>>();

const OUTLIER_Z = 4;

function mergeLevels(
  bybit: Array<[number, number]> | undefined,
  mexc: Array<[number, number]> | undefined,
  side: "bids" | "asks",
  depth: number
): Array<[number, number]> {
  const combined = new Map<number, number>();
  for (const [price, size] of bybit ?? []) {
    combined.set(price, (combined.get(price) ?? 0) + size);
  }
  for (const [price, size] of mexc ?? []) {
    combined.set(price, (combined.get(price) ?? 0) + size);
  }
  const entries = Array.from(combined.entries());
  entries.sort((a, b) => (side === "bids" ? b[0] - a[0] : a[0] - b[0]));
  return entries.slice(0, depth);
}

function buildBookSnapshot(state: LatestState | undefined, depth: number): string {
  if (!state) return "";
  const bids = state.bids.slice(0, depth);
  const asks = state.asks.slice(0, depth);
  return JSON.stringify({ bids, asks });
}

function recordMid(exchange: "bybit" | "mexc", symbol: string, mid: number): void {
  if (mid <= 0) return;
  const list = spotMidHistory[exchange].get(symbol) ?? [];
  list.push({ ts: Date.now(), mid });
  const cutoff = Date.now() - 5 * 60 * 1000;
  while (list.length > 0 && list[0].ts < cutoff) {
    list.shift();
  }
  spotMidHistory[exchange].set(symbol, list);
}

function computeVol(points: Array<{ ts: number; mid: number }>, windowMs: number): number {
  if (points.length < 2) return 0;
  const cutoff = Date.now() - windowMs;
  const window = points.filter((p) => p.ts >= cutoff);
  if (window.length < 2) return 0;
  let sumSq = 0;
  for (let i = 1; i < window.length; i += 1) {
    const prev = window[i - 1].mid;
    const curr = window[i].mid;
    if (prev <= 0 || curr <= 0) continue;
    const ret = Math.log(curr / prev);
    sumSq += ret * ret;
  }
  const n = Math.max(1, window.length - 1);
  return Math.sqrt(sumSq / n);
}

function computeZScoreOutliers(
  symbol: string,
  market: string,
  exchange: string,
  side: "Bid" | "Ask",
  levels: Array<[number, number]>,
  context: {
    mid: number;
    bookSnapshot: string;
    bestBid: number;
    bestAsk: number;
    spreadBps: number;
    imbalance: number;
    bidDepth: number;
    askDepth: number;
    microprice: number;
    levelRanks: Map<number, number>;
    vol1m: number;
    vol5m: number;
  }
): Array<{
  ts: number;
  symbol: string;
  market: string;
  exchange: string;
  side: string;
  price: number;
  size: number;
  zScore: number;
  bpsFromMid: number;
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
}> {
  if (levels.length === 0 || context.mid <= 0) return [];
  const sizes = levels.map((level) => level[1]);
  const mean = sizes.reduce((sum, size) => sum + size, 0) / sizes.length;
  const variance =
    sizes.reduce((sum, size) => sum + Math.pow(size - mean, 2), 0) / sizes.length;
  const stdDev = Math.sqrt(variance);
  if (stdDev === 0) return [];
  const ts = Date.now();
  return levels
    .map(([price, size]) => {
      const zScore = (size - mean) / stdDev;
      return {
        ts,
        symbol,
        market,
        exchange,
        side,
        price,
        size,
        zScore,
        bpsFromMid: (Math.abs(price - context.mid) / context.mid) * 10000,
        mid: context.mid,
        bookSnapshot: context.bookSnapshot,
        bestBid: context.bestBid,
        bestAsk: context.bestAsk,
        spreadBps: context.spreadBps,
        imbalance: context.imbalance,
        bidDepth: context.bidDepth,
        askDepth: context.askDepth,
        microprice: context.microprice,
        levelRank: context.levelRanks.get(price) ?? 0,
        vol1m: context.vol1m,
        vol5m: context.vol5m,
      };
    })
    .filter((row) => row.zScore >= OUTLIER_Z);
}

function mergeMoveStats(...stats: Array<MoveStats | null | undefined>): MoveStats {
  const merged = {
    bids: { adds: 0, changes: 0, removals: 0, sizeDelta: 0 },
    asks: { adds: 0, changes: 0, removals: 0, sizeDelta: 0 },
  };
  for (const stat of stats) {
    if (!stat) continue;
    merged.bids.adds += stat.bids.adds;
    merged.bids.changes += stat.bids.changes;
    merged.bids.removals += stat.bids.removals;
    merged.bids.sizeDelta += stat.bids.sizeDelta;
    merged.asks.adds += stat.asks.adds;
    merged.asks.changes += stat.asks.changes;
    merged.asks.removals += stat.asks.removals;
    merged.asks.sizeDelta += stat.asks.sizeDelta;
  }
  return merged;
}

function toExchangeMetrics(point: NonNullable<ReturnType<typeof computeMetrics>>): ExchangeMetrics {
  return {
    mid: point.mid,
    bestBid: point.bestBid,
    bestAsk: point.bestAsk,
    depth: point.depth,
    baseMmNotional: point.baseMmNotional,
    totalNotionalBid: point.totalNotionalBid,
    totalNotionalAsk: point.totalNotionalAsk,
    distanceBinsBps: point.distanceBinsBps,
    distanceBinCountsBid: point.distanceBinCountsBid,
    distanceBinCountsAsk: point.distanceBinCountsAsk,
    maxDistanceBpsBid: point.maxDistanceBpsBid,
    maxDistanceBpsAsk: point.maxDistanceBpsAsk,
    avgDistanceBpsBid: point.avgDistanceBpsBid,
    avgDistanceBpsAsk: point.avgDistanceBpsAsk,
    outlierCountBid: point.outlierCountBid,
    outlierCountAsk: point.outlierCountAsk,
  };
}

const orderbook = new BybitOrderbook({
  symbols: config.symbols,
  depth: config.depth,
  onReady: () => {
    console.log(`Connected to Bybit spot orderbook: ${config.symbols.join(", ")}`);
  },
  onUpdate: (symbol, bids, asks, tracker) => {
    spotLatest.bybit.set(symbol, { bids, asks, tracker });
  },
});

if (liveMonitoring) {
  orderbook.connect();
}
const mexcSpotOrderbook = new MexcSpotOrderbook({
  symbols: config.symbols,
  depth: config.depth,
  onReady: () => {
    console.log(`Connected to MEXC spot orderbook: ${config.symbols.join(", ")}`);
  },
  onUpdate: (symbol, bids, asks, tracker) => {
    spotLatest.mexc.set(symbol, { bids, asks, tracker });
  },
});
const mexcOrderbook = new MexcOrderbook({
  symbols: config.symbols,
  depth: config.depth,
  onReady: () => {
    console.log(`Connected to MEXC perp orderbook: ${config.symbols.join(", ")}`);
  },
  onUpdate: (symbol, bids, asks, tracker) => {
    perpLatest.mexc.set(symbol, { bids, asks, tracker });
  },
});
if (liveMonitoring) {
  mexcSpotOrderbook.connect();
  mexcOrderbook.connect();
  connectLiquidations(config.symbols);
  connectOiFunding(config.symbols);
  connectMexcOiFunding(config.symbols);
  connectBybitSpotTrades(config.symbols);
  startMexcSpotTradePolling(config.symbols);
  fetchOiFundingSnapshot(config.symbols);
  setInterval(() => {
    fetchOiFundingSnapshot(config.symbols);
  }, 30_000);
}
const perpOrderbook = new BybitOrderbook({
  symbols: config.symbols,
  depth: config.depth,
  wsUrl: "wss://stream.bybit.com/v5/public/linear",
  onReady: () => {
    console.log(`Connected to Bybit linear orderbook: ${config.symbols.join(", ")}`);
  },
  onUpdate: (symbol, bids, asks, tracker) => {
    perpLatest.bybit.set(symbol, { bids, asks, tracker });
  },
});
if (liveMonitoring) {
  perpOrderbook.connect();
}

if (liveMonitoring) {
  setInterval(() => {
    for (const symbol of config.symbols) {
    const spotBybitState = spotLatest.bybit.get(symbol);
    const spotMexcState = spotLatest.mexc.get(symbol);
    if (spotBybitState || spotMexcState) {
      const mergedBids = mergeLevels(spotBybitState?.bids, spotMexcState?.bids, "bids", config.depth);
      const mergedAsks = mergeLevels(spotBybitState?.asks, spotMexcState?.asks, "asks", config.depth);
      const spotBestBid = mergedBids[0]?.[0] ?? 0;
      const spotBestAsk = mergedAsks[0]?.[0] ?? 0;
      const spotMid = spotBestBid && spotBestAsk ? (spotBestBid + spotBestAsk) / 2 : 0;
      const outliers: Array<{
        ts: number;
        symbol: string;
        market: string;
        exchange: string;
        side: string;
        price: number;
        size: number;
        zScore: number;
        bpsFromMid: number;
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
      }> = [];
      if (spotBybitState) {
        const bestBid = spotBybitState.bids[0]?.[0] ?? 0;
        const bestAsk = spotBybitState.asks[0]?.[0] ?? 0;
        const mid = bestBid && bestAsk ? (bestBid + bestAsk) / 2 : 0;
        if (mid > 0) {
          const bookSnapshot = buildBookSnapshot(spotBybitState, 20);
          const bidDepth = spotBybitState.bids.slice(0, 20).reduce((sum, level) => sum + level[1], 0);
          const askDepth = spotBybitState.asks.slice(0, 20).reduce((sum, level) => sum + level[1], 0);
          const spreadBps = (bestAsk - bestBid) / mid * 10000;
          const imbalance =
            bidDepth + askDepth > 0 ? (bidDepth - askDepth) / (bidDepth + askDepth) : 0;
          const bestBidSize = spotBybitState.bids[0]?.[1] ?? 0;
          const bestAskSize = spotBybitState.asks[0]?.[1] ?? 0;
          const microprice =
            bestBidSize + bestAskSize > 0
              ? (bestAsk * bestBidSize + bestBid * bestAskSize) / (bestBidSize + bestAskSize)
              : mid;
          const levelRanks = new Map<number, number>();
          spotBybitState.bids.slice(0, 20).forEach(([price], idx) => levelRanks.set(price, idx + 1));
          spotBybitState.asks.slice(0, 20).forEach(([price], idx) => levelRanks.set(price, idx + 1));
          const history = spotMidHistory.bybit.get(symbol) ?? [];
          const vol1m = computeVol(history, 60_000);
          const vol5m = computeVol(history, 5 * 60_000);
          recordMid("bybit", symbol, mid);
          outliers.push(
            ...computeZScoreOutliers(
              symbol,
              "Spot",
              "Bybit",
              "Bid",
              spotBybitState.bids,
              {
                mid,
                bookSnapshot,
                bestBid,
                bestAsk,
                spreadBps,
                imbalance,
                bidDepth,
                askDepth,
                microprice,
                levelRanks,
                vol1m,
                vol5m,
              }
            ),
            ...computeZScoreOutliers(
              symbol,
              "Spot",
              "Bybit",
              "Ask",
              spotBybitState.asks,
              {
                mid,
                bookSnapshot,
                bestBid,
                bestAsk,
                spreadBps,
                imbalance,
                bidDepth,
                askDepth,
                microprice,
                levelRanks,
                vol1m,
                vol5m,
              }
            )
          );
        }
      }
      if (spotMexcState) {
        const bestBid = spotMexcState.bids[0]?.[0] ?? 0;
        const bestAsk = spotMexcState.asks[0]?.[0] ?? 0;
        const mid = bestBid && bestAsk ? (bestBid + bestAsk) / 2 : 0;
        if (mid > 0) {
          const bookSnapshot = buildBookSnapshot(spotMexcState, 20);
          const bidDepth = spotMexcState.bids.slice(0, 20).reduce((sum, level) => sum + level[1], 0);
          const askDepth = spotMexcState.asks.slice(0, 20).reduce((sum, level) => sum + level[1], 0);
          const spreadBps = (bestAsk - bestBid) / mid * 10000;
          const imbalance =
            bidDepth + askDepth > 0 ? (bidDepth - askDepth) / (bidDepth + askDepth) : 0;
          const bestBidSize = spotMexcState.bids[0]?.[1] ?? 0;
          const bestAskSize = spotMexcState.asks[0]?.[1] ?? 0;
          const microprice =
            bestBidSize + bestAskSize > 0
              ? (bestAsk * bestBidSize + bestBid * bestAskSize) / (bestBidSize + bestAskSize)
              : mid;
          const levelRanks = new Map<number, number>();
          spotMexcState.bids.slice(0, 20).forEach(([price], idx) => levelRanks.set(price, idx + 1));
          spotMexcState.asks.slice(0, 20).forEach(([price], idx) => levelRanks.set(price, idx + 1));
          const history = spotMidHistory.mexc.get(symbol) ?? [];
          const vol1m = computeVol(history, 60_000);
          const vol5m = computeVol(history, 5 * 60_000);
          recordMid("mexc", symbol, mid);
          outliers.push(
            ...computeZScoreOutliers(
              symbol,
              "Spot",
              "MEXC",
              "Bid",
              spotMexcState.bids,
              {
                mid,
                bookSnapshot,
                bestBid,
                bestAsk,
                spreadBps,
                imbalance,
                bidDepth,
                askDepth,
                microprice,
                levelRanks,
                vol1m,
                vol5m,
              }
            ),
            ...computeZScoreOutliers(
              symbol,
              "Spot",
              "MEXC",
              "Ask",
              spotMexcState.asks,
              {
                mid,
                bookSnapshot,
                bestBid,
                bestAsk,
                spreadBps,
                imbalance,
                bidDepth,
                askDepth,
                microprice,
                levelRanks,
                vol1m,
                vol5m,
              }
            )
          );
        }
      }
      outlierStore.appendAll(
        outliers.map(
          ({ mid, bookSnapshot, bestBid, bestAsk, spreadBps, imbalance, bidDepth, askDepth, microprice, levelRank, vol1m, vol5m, ...record }) =>
            record
        )
      );
      outlierSpanTracker.update(outliers);
      const sources: Record<string, { bids: Array<[number, number]>; asks: Array<[number, number]> }> = {};
      if (spotBybitState) {
        sources.bybit = { bids: spotBybitState.bids, asks: spotBybitState.asks };
      }
      if (spotMexcState) {
        sources.mexc = { bids: spotMexcState.bids, asks: spotMexcState.asks };
      }
      broadcast({
        type: "book",
        data: {
          symbol,
          mid: spotMid,
          bids: mergedBids,
          asks: mergedAsks,
          depth: mergedBids.length,
          sources,
        },
      });
    }

    const bybitPerpState = perpLatest.bybit.get(symbol);
    const mexcPerpState = perpLatest.mexc.get(symbol);
    if (!bybitPerpState && !mexcPerpState) continue;

    const bybitMoves = bybitPerpState?.tracker.snapshotAndReset();
    const mexcMoves = mexcPerpState?.tracker.snapshotAndReset();

    const exchangeMetrics: Record<string, ExchangeMetrics> = {};
    if (bybitPerpState) {
      const bybitMetrics = computeMetrics(
        symbol,
        bybitPerpState.bids,
        bybitPerpState.asks,
        config.baseMmNotional,
        config.distanceBinsBps,
        bybitMoves ?? mergeMoveStats()
      );
      if (bybitMetrics) {
        exchangeMetrics.bybit = toExchangeMetrics(bybitMetrics);
      }
    }
    if (mexcPerpState) {
      const mexcMetrics = computeMetrics(
        symbol,
        mexcPerpState.bids,
        mexcPerpState.asks,
        config.baseMmNotional,
        config.distanceBinsBps,
        mexcMoves ?? mergeMoveStats()
      );
      if (mexcMetrics) {
        exchangeMetrics.mexc = toExchangeMetrics(mexcMetrics);
      }
    }

    const mergedBids = mergeLevels(bybitPerpState?.bids, mexcPerpState?.bids, "bids", config.depth);
    const mergedAsks = mergeLevels(bybitPerpState?.asks, mexcPerpState?.asks, "asks", config.depth);
    const moveStats = mergeMoveStats(bybitMoves, mexcMoves);
    const metrics = computeMetrics(
      symbol,
      mergedBids,
      mergedAsks,
      config.baseMmNotional,
      config.distanceBinsBps,
      moveStats
    );
    if (!metrics) continue;
    if (Object.keys(exchangeMetrics).length > 0) {
      metrics.exchanges = exchangeMetrics;
    }
    const combinedState = { bids: mergedBids, asks: mergedAsks };
    const prev = lastSnapshot.get(symbol);
    const allLargeMoves = computeLargeMoves(
      prev,
      combinedState,
      metrics.mid,
      config.baseMmNotional,
      config.largeMoveWindowBps,
      config.largeMoveNotionalFloor
    );
    metrics.largeMovesBid = allLargeMoves.bids.slice(0, 8);
    metrics.largeMovesAsk = allLargeMoves.asks.slice(0, 8);
    largeMoveStore.appendAll(
      [
        ...allLargeMoves.bids.map((move) => ({ ...move, side: "Bid" })),
        ...allLargeMoves.asks.map((move) => ({ ...move, side: "Ask" })),
      ].map((move) => ({
        ts: metrics.ts,
        symbol,
        side: move.side,
        price: move.price,
        deltaSize: move.deltaSize,
        notionalDelta: move.notionalDelta,
        bpsFromMid: move.bpsFromMid,
      }))
    );
    store.append(metrics);
    lastMetrics.set(symbol, metrics);
    lastSnapshot.set(symbol, { bids: mergedBids, asks: mergedAsks });
    broadcast({ type: "metrics", data: metrics });
    const sources: Record<string, { bids: Array<[number, number]>; asks: Array<[number, number]> }> = {};
    if (bybitPerpState) {
      sources.bybit = { bids: bybitPerpState.bids, asks: bybitPerpState.asks };
    }
    if (mexcPerpState) {
      sources.mexc = { bids: mexcPerpState.bids, asks: mexcPerpState.asks };
    }
    broadcast({
      type: "perpBook",
      data: {
        symbol,
        mid: metrics.mid,
        bids: mergedBids,
        asks: mergedAsks,
        depth: mergedBids.length,
        sources,
      },
    });
    }
  }, config.metricsIntervalMs);

  setInterval(() => {
    for (const [symbol, metrics] of lastMetrics.entries()) {
      if (!metrics) continue;
      console.log(
        `[${symbol}] mid=${metrics.mid.toFixed(5)} ` +
          `outliers(bid/ask)=${metrics.outlierCountBid}/${metrics.outlierCountAsk} ` +
          `maxBps(bid/ask)=${metrics.maxDistanceBpsBid.toFixed(1)}/${metrics.maxDistanceBpsAsk.toFixed(1)} ` +
          `moves(bid/ask)=${metrics.moveStats.bids.changes}/${metrics.moveStats.asks.changes}`
      );
    }
  }, config.logIntervalMs);
} else {
  console.log("Live monitoring disabled (LIVE_MONITORING=false).");
}

const host = process.env.HOST ?? "127.0.0.1";
const port = Number(process.env.PORT ?? 3000);
const displayPath = basePath ? `${basePath}/` : "/";
server.listen(port, host, () => {
  console.log(`Dashboard listening on http://${host}:${port}${displayPath}`);
});

function computeLargeMoves(
  prev: { bids: Array<[number, number]>; asks: Array<[number, number]> } | undefined,
  next: { bids: Array<[number, number]>; asks: Array<[number, number]> },
  mid: number,
  baseMmNotional: number,
  windowBps: number,
  notionalFloor: number
): { bids: LevelMove[]; asks: LevelMove[] } {
  const bids = compareSide(prev?.bids ?? [], next.bids, mid, baseMmNotional, windowBps, notionalFloor);
  const asks = compareSide(prev?.asks ?? [], next.asks, mid, baseMmNotional, windowBps, notionalFloor);
  return { bids, asks };
}

function compareSide(
  prev: Array<[number, number]>,
  next: Array<[number, number]>,
  mid: number,
  baseMmNotional: number,
  windowBps: number,
  notionalFloor: number
): LevelMove[] {
  const prevMap = new Map(prev.map(([price, size]) => [price, size]));
  const moves: LevelMove[] = [];
  const windowLevels = next.filter(
    ([price]) => (Math.abs(price - mid) / mid) * 10000 <= windowBps
  ).length;
  const scaledNotional = baseMmNotional / Math.max(1, windowLevels);
  const minNotional = Math.max(scaledNotional, notionalFloor);
  for (const [price, nextSize] of next) {
    const prevSize = prevMap.get(price) ?? 0;
    const deltaSize = nextSize - prevSize;
    if (deltaSize === 0) continue;
    const notionalDelta = Math.abs(deltaSize) * price;
    if (notionalDelta < minNotional) continue;
    const bpsFromMid = Math.abs(price - mid) / mid * 10000;
    moves.push({ price, prevSize, nextSize, deltaSize, notionalDelta, bpsFromMid });
  }
  moves.sort((a, b) => b.notionalDelta - a.notionalDelta);
  return moves;
}

function connectLiquidations(symbols: string[]): void {
  const ws = new WebSocket("wss://stream.bybit.com/v5/public/linear");
  const logUntil = Date.now() + 60_000;
  let lastLogTs = 0;
  ws.on("open", () => {
    const topics = symbols.map((symbol) => `liquidation.${symbol}`);
    ws.send(JSON.stringify({ op: "subscribe", args: topics }));
    console.log(`Connected to Bybit linear liquidations: ${symbols.join(", ")}`);
  });

  ws.on("message", (data) => {
    const payload = JSON.parse(data.toString());
    if (payload?.op === "subscribe") {
      console.log("Liquidation subscribe:", payload);
      if (payload.success === false && String(payload.ret_msg ?? "").includes("handler not found")) {
        console.warn("Liquidation WS not supported; falling back to REST polling.");
        ws.close();
        startLiquidationPolling(symbols);
        return;
      }
    }
    if (!payload || !payload.topic || !payload.data) return;
    if (!String(payload.topic).startsWith("liquidation.")) return;
    const now = Date.now();
    if (now < logUntil && now - lastLogTs > 1000) {
      console.log("Liquidation raw:", payload.data);
      lastLogTs = now;
    }
    const events = normalizeLiquidations(payload.data, payload.ts);
    for (const event of events) {
      liquidationStore.append(event);
      broadcast({ type: "liquidation", data: event });
    }
  });

  ws.on("error", (err) => {
    console.error("Bybit liquidation WS error:", err);
  });

  ws.on("close", () => {
    setTimeout(() => connectLiquidations(symbols), 2000);
  });
}
const lastLiquidationTs = new Map<string, number>();

function startLiquidationPolling(symbols: string[]): void {
  setInterval(() => {
    fetchLiquidationSnapshot(symbols);
  }, 30_000);
  fetchLiquidationSnapshot(symbols);
}

async function fetchLiquidationSnapshot(symbols: string[]): Promise<void> {
  const baseUrl = "https://api.bybit.com/v5/market/liquidation?category=linear&symbol=";
  await Promise.all(
    symbols.map(async (symbol) => {
      try {
        const response = await fetch(`${baseUrl}${encodeURIComponent(symbol)}`);
        if (!response.ok) return;
        const payload = await response.json();
        const list = payload?.result?.list ?? payload?.result?.data ?? payload?.result ?? [];
        if (!Array.isArray(list)) return;
        const events = normalizeLiquidationRest(list, symbol);
        const lastTs = lastLiquidationTs.get(symbol) ?? 0;
        const fresh = events.filter((event) => event.ts > lastTs);
        if (fresh.length > 0) {
          lastLiquidationTs.set(symbol, fresh[fresh.length - 1].ts);
        }
        for (const event of fresh) {
          liquidationStore.append(event);
          broadcast({ type: "liquidation", data: event });
        }
      } catch {
        return;
      }
    })
  );
}

function normalizeLiquidationRest(items: unknown[], symbol: string): LiquidationEvent[] {
  const events: LiquidationEvent[] = [];
  for (const item of items) {
    if (!item || typeof item !== "object") continue;
    const record = item as Record<string, string>;
    const price = Number(record.price ?? record.p ?? 0);
    const size = Number(record.qty ?? record.q ?? record.size ?? 0);
    const side = record.side ?? record.S ?? "Unknown";
    let ts = Number(record.time ?? record.ts ?? record.T ?? Date.now());
    if (ts > 0 && ts < 1_000_000_000_000) ts *= 1000;
    if (!Number.isFinite(price) || !Number.isFinite(size)) continue;
    events.push({
      ts,
      symbol,
      side,
      price,
      size,
      notional: price * size,
    });
  }
  events.sort((a, b) => a.ts - b.ts);
  return events;
}

function normalizeLiquidations(raw: unknown, fallbackTs?: number): LiquidationEvent[] {
  const items = Array.isArray(raw) ? raw : [raw];
  const events: LiquidationEvent[] = [];
  for (const item of items) {
    if (!item || typeof item !== "object") continue;
    const record = item as Record<string, string>;
    const symbol = record.s ?? record.symbol ?? "";
    if (!symbol) continue;
    const price = Number(record.price ?? record.p ?? 0);
    const size = Number(record.qty ?? record.q ?? record.size ?? 0);
    if (!Number.isFinite(price) || !Number.isFinite(size)) continue;
    const ts = Number(record.ts ?? record.T ?? fallbackTs ?? Date.now());
    events.push({
      ts,
      symbol,
      side: record.side ?? record.S ?? "Unknown",
      price,
      size,
      notional: price * size,
    });
  }
  return events;
}

function connectOiFunding(symbols: string[]): void {
  const ws = new WebSocket("wss://stream.bybit.com/v5/public/linear");
  const logUntil = Date.now() + 60_000;
  let lastLogTs = 0;
  ws.on("open", () => {
    const topics = symbols.map((symbol) => `tickers.${symbol}`);
    ws.send(JSON.stringify({ op: "subscribe", args: topics }));
    console.log(`Connected to Bybit linear tickers: ${symbols.join(", ")}`);
  });

  ws.on("message", (data) => {
    const payload = JSON.parse(data.toString());
    if (!payload || !payload.topic || !payload.data) return;
    if (!String(payload.topic).startsWith("tickers.")) return;
    const now = Date.now();
    if (now < logUntil && now - lastLogTs > 1000) {
      console.log("Ticker raw:", payload.data);
      lastLogTs = now;
    }
    const points = normalizeOiFunding(payload.data, Date.now());
    for (const point of points) {
      oiFundingStore.append(point);
      broadcast({ type: "oiFunding", data: point });
    }
  });

  ws.on("error", (err) => {
    console.error("Bybit ticker WS error:", err);
  });

  ws.on("close", () => {
    setTimeout(() => connectOiFunding(symbols), 2000);
  });
}

function normalizeOiFunding(raw: unknown, fallbackTs?: number): OiFundingPoint[] {
  const items = Array.isArray(raw) ? raw : [raw];
  const points: OiFundingPoint[] = [];
  for (const item of items) {
    if (!item || typeof item !== "object") continue;
    const record = item as Record<string, string>;
    const symbol = record.symbol ?? record.s ?? "";
    if (!symbol) continue;
    const openInterestRaw = record.openInterest ?? record.oi ?? record.open_interest;
    const openInterestValueRaw = record.openInterestValue ?? record.open_interest_value;
    const priceRaw = record.markPrice ?? record.indexPrice ?? record.lastPrice;
    const fundingRateRaw = record.fundingRate ?? record.fr ?? record.funding_rate;
    let openInterest =
      openInterestRaw === undefined || openInterestRaw === "" ? Number.NaN : Number(openInterestRaw);
    if (!Number.isFinite(openInterest) && openInterestValueRaw !== undefined && openInterestValueRaw !== "") {
      const value = Number(openInterestValueRaw);
      const price = Number(priceRaw);
      if (Number.isFinite(value) && value > 0 && Number.isFinite(price) && price > 0) {
        openInterest = value / price;
      }
    }
    const fundingRate =
      fundingRateRaw === undefined || fundingRateRaw === "" ? Number.NaN : Number(fundingRateRaw);
    let nextFundingTime = Number(record.nextFundingTime ?? record.nft ?? 0);
    if (!Number.isFinite(openInterest) && !Number.isFinite(fundingRate)) continue;
    let ts = Number(record.ts ?? record.T ?? fallbackTs ?? Date.now());
    if (ts > 0 && ts < 1_000_000_000_000) {
      ts *= 1000;
    }
    if (nextFundingTime > 0 && nextFundingTime < 1_000_000_000_000) {
      nextFundingTime *= 1000;
    }
    points.push({
      ts,
      symbol,
      exchange: "bybit",
      openInterest,
      fundingRate,
      nextFundingTime: Number.isFinite(nextFundingTime) ? nextFundingTime : undefined,
    });
  }
  return points;
}

async function fetchOiFundingSnapshot(symbols: string[]): Promise<void> {
  const baseUrl = "https://api.bybit.com/v5/market/tickers?category=linear&symbol=";
  await Promise.all(
    symbols.map(async (symbol) => {
      try {
        const response = await fetch(`${baseUrl}${encodeURIComponent(symbol)}`);
        if (!response.ok) return;
        const payload = await response.json();
        const list = payload?.result?.list ?? [];
        if (!Array.isArray(list) || list.length === 0) return;
        const point = normalizeOiFunding(list[0], Date.now())[0];
        if (!point) return;
        oiFundingStore.append(point);
        broadcast({ type: "oiFunding", data: point });
      } catch {
        return;
      }
    })
  );
}

function connectMexcOiFunding(symbols: string[]): void {
  const ws = new WebSocket("wss://contract.mexc.com/edge");
  ws.on("open", () => {
    for (const symbol of symbols) {
      ws.send(
        JSON.stringify({
          method: "sub.ticker",
          param: { symbol: toMexcContractSymbol(symbol) },
        })
      );
      ws.send(
        JSON.stringify({
          method: "sub.funding.rate",
          param: { symbol: toMexcContractSymbol(symbol) },
        })
      );
    }
    console.log(`Connected to MEXC perp tickers: ${symbols.join(", ")}`);
  });

  ws.on("message", (data) => {
    const payload = JSON.parse(data.toString()) as Record<string, unknown>;
    const channel = payload?.channel ?? "";
    if (typeof channel !== "string") return;
    if (channel === "push.ticker") {
      const point = normalizeMexcTicker(payload);
      if (!point) return;
      oiFundingStore.append(point);
      broadcast({ type: "oiFunding", data: point });
    }
    if (channel === "push.funding.rate") {
      const point = normalizeMexcFunding(payload);
      if (!point) return;
      oiFundingStore.append(point);
      broadcast({ type: "oiFunding", data: point });
    }
  });

  ws.on("error", (err) => {
    console.error("MEXC ticker WS error:", err);
  });

  ws.on("close", () => {
    setTimeout(() => connectMexcOiFunding(symbols), 2000);
  });
}

function connectBybitSpotTrades(symbols: string[]): void {
  const ws = new WebSocket("wss://stream.bybit.com/v5/public/spot");
  ws.on("open", () => {
    const topics = symbols.map((symbol) => `publicTrade.${symbol}`);
    ws.send(JSON.stringify({ op: "subscribe", args: topics }));
    console.log(`Connected to Bybit spot trades: ${symbols.join(", ")}`);
  });

  ws.on("message", (data) => {
    const payload = JSON.parse(data.toString());
    if (!payload || !payload.topic || !payload.data) return;
    if (!String(payload.topic).startsWith("publicTrade.")) return;
    const trades = Array.isArray(payload.data) ? payload.data : [payload.data];
    for (const trade of trades) {
      const symbol = trade.s;
      const price = Number(trade.p);
      const qty = Number(trade.v);
      const side = trade.S ?? trade.side;
      const ts = Number(trade.T ?? trade.t ?? Date.now());
      if (!symbol || !Number.isFinite(price) || !Number.isFinite(qty) || !side) continue;
      outlierSpanTracker.recordTrade({
        ts,
        symbol,
        market: "Spot",
        exchange: "Bybit",
        price,
        qty,
        side: side === "Buy" ? "Buy" : "Sell",
      });
      tradeStore.append({
        ts,
        symbol,
        exchange: "Bybit",
        side: side === "Buy" ? "Buy" : "Sell",
        price,
        qty,
      });
      broadcast({
        type: "trade",
        data: { ts, symbol, exchange: "Bybit", side: side === "Buy" ? "Buy" : "Sell", price, qty },
      });
    }
  });

  ws.on("error", (err) => {
    console.error("Bybit spot trade WS error:", err);
  });

  ws.on("close", () => {
    setTimeout(() => connectBybitSpotTrades(symbols), 2000);
  });
}

const lastMexcTradeId = new Map<string, number>();

function startMexcSpotTradePolling(symbols: string[]): void {
  setInterval(() => {
    fetchMexcSpotTrades(symbols);
  }, 2000);
  fetchMexcSpotTrades(symbols);
}

async function fetchMexcSpotTrades(symbols: string[]): Promise<void> {
  await Promise.all(
    symbols.map(async (symbol) => {
      try {
        const response = await fetch(
          `https://api.mexc.com/api/v3/trades?symbol=${encodeURIComponent(symbol)}&limit=100`
        );
        if (!response.ok) return;
        const payload = (await response.json()) as Array<{
          id?: number | string;
          tradeId?: number | string;
          price: string;
          qty: string;
          time: number;
          isBuyerMaker?: boolean;
        }>;
        if (!Array.isArray(payload)) return;
        const lastId = lastMexcTradeId.get(symbol) ?? 0;
        let maxId = lastId;
        const fresh = payload
          .map((trade) => ({
            id: Number(trade.id ?? trade.tradeId ?? 0),
            price: Number(trade.price),
            qty: Number(trade.qty),
            ts: Number(trade.time ?? Date.now()),
            side: trade.isBuyerMaker ? "Sell" : "Buy",
          }))
          .filter((trade) => Number.isFinite(trade.price) && Number.isFinite(trade.qty))
          .filter((trade) => trade.id === 0 || trade.id > lastId)
          .sort((a, b) => a.ts - b.ts);
        for (const trade of fresh) {
          if (trade.id > maxId) maxId = trade.id;
          outlierSpanTracker.recordTrade({
            ts: trade.ts,
            symbol,
            market: "Spot",
            exchange: "MEXC",
            price: trade.price,
            qty: trade.qty,
            side: trade.side === "Buy" ? "Buy" : "Sell",
          });
          tradeStore.append({
            ts: trade.ts,
            symbol,
            exchange: "MEXC",
            side: trade.side === "Buy" ? "Buy" : "Sell",
            price: trade.price,
            qty: trade.qty,
          });
          broadcast({
            type: "trade",
            data: {
              ts: trade.ts,
              symbol,
              exchange: "MEXC",
              side: trade.side === "Buy" ? "Buy" : "Sell",
              price: trade.price,
              qty: trade.qty,
            },
          });
        }
        if (maxId > lastId) {
          lastMexcTradeId.set(symbol, maxId);
        }
      } catch {
        return;
      }
    })
  );
}

function normalizeMexcTicker(payload: Record<string, unknown>): OiFundingPoint | null {
  const data = payload.data as Record<string, number | string> | undefined;
  if (!data) return null;
  const symbolRaw = (payload.symbol ?? data.symbol ?? "") as string;
  if (!symbolRaw) return null;
  const symbol = fromMexcContractSymbol(symbolRaw);
  const tsRaw = (payload.ts ?? payload.timestamp ?? data.timestamp ?? Date.now()) as number;
  const ts = Number(tsRaw);
  const openInterest = Number(data.holdVol ?? data.holdvol ?? data.hold_volume ?? data.holdVolume);
  const fundingRate = Number(data.fundingRate ?? data.funding_rate ?? data.rate);
  const point: OiFundingPoint = {
    ts: Number.isFinite(ts) ? ts : Date.now(),
    symbol,
    exchange: "mexc",
    openInterest: Number.isFinite(openInterest) ? openInterest : Number.NaN,
    fundingRate: Number.isFinite(fundingRate) ? fundingRate : Number.NaN,
  };
  return point;
}

function normalizeMexcFunding(payload: Record<string, unknown>): OiFundingPoint | null {
  const data = payload.data as Record<string, number | string> | undefined;
  if (!data) return null;
  const symbolRaw = (payload.symbol ?? data.symbol ?? "") as string;
  if (!symbolRaw) return null;
  const symbol = fromMexcContractSymbol(symbolRaw);
  const tsRaw = (payload.ts ?? Date.now()) as number;
  const ts = Number(tsRaw);
  const fundingRate = Number(data.rate ?? data.fundingRate ?? data.funding_rate);
  const nextFundingTimeRaw = Number(data.nextSettleTime ?? data.next_settle_time ?? 0);
  const nextFundingTime =
    nextFundingTimeRaw > 0 && nextFundingTimeRaw < 1_000_000_000_000
      ? nextFundingTimeRaw * 1000
      : nextFundingTimeRaw || undefined;
  const point: OiFundingPoint = {
    ts: Number.isFinite(ts) ? ts : Date.now(),
    symbol,
    exchange: "mexc",
    openInterest: Number.NaN,
    fundingRate: Number.isFinite(fundingRate) ? fundingRate : Number.NaN,
    nextFundingTime,
  };
  return point;
}

function toMexcContractSymbol(symbol: string): string {
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

function fromMexcContractSymbol(symbol: string): string {
  return symbol.replace(/[^a-zA-Z0-9]/g, "").toUpperCase();
}
