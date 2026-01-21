# Market Microstructure Monitor

Real-time orderbook monitoring and analytics for crypto markets. The service collects live data, stores recent history in SQLite, and serves a web dashboard plus JSON APIs.

## Features
- Live orderbook tracking with depth metrics
- Liquidation, trade, and open-interest/funding feeds
- Outlier detection and reporting (CSV/PDF)
- Web dashboard served from the same Node.js process

## Requirements
- Node.js 18+ (TypeScript build targets Node 14+, but runtime should be modern)

## Quick start
```bash
npm install
npm run build
npm start
```

For development:
```bash
npm run dev
```

## Configuration
Configuration is done via environment variables (optionally placed in `.env`). Defaults are applied when variables are absent.

Common settings:
- `SYMBOLS` (default `WHITEWHALEUSDT`): Comma-separated list of symbols to track.
- `DEPTH` (default `50`): Orderbook depth to request and compute metrics for.
- `BASE_MM_NOTIONAL` (default `30000`): Baseline notional used for market-making depth buckets.
- `LARGE_MOVE_NOTIONAL` (default `30000`): Notional threshold for large move detection.
- `LARGE_MOVE_WINDOW_BPS` (default `200`): Price window, in basis points, for large move scans.
- `LARGE_MOVE_NOTIONAL_FLOOR` (default `2000`): Minimum notional to qualify large moves.
- `SIZE_BINS` (default `500,1000,2500,5000,10000,25000,50000`): Size histogram bins for dashboard charts.
- `DISTANCE_BINS_BPS` (default `5,10,25,50,100,200`): Price distance buckets (bps) used in depth metrics.
- `LOG_INTERVAL_MS` (default `5000`): Console logging interval.
- `METRICS_INTERVAL_MS` (default `1000`): Metrics sampling interval.
- `DATA_DIR` (default `data`): Directory for SQLite files.
- `BASE_PATH` (default empty): Prefix for hosting behind a reverse proxy (e.g. `/dashboard`).
- `LIVE_MONITORING` (default `true`): Toggle live websocket feeds and streaming updates.

## Data storage
SQLite files are written under `DATA_DIR` (default `data/`) and are intentionally ignored by git.

## Project layout
- `src/` TypeScript server and data collectors
- `public/` Dashboard static assets
- `data/` Local SQLite storage (ignored)

## Scripts
- `npm run build` Compile TypeScript to `dist/`
- `npm start` Run the compiled server
- `npm run dev` Run the server with `ts-node`
