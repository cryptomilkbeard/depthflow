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
- `SYMBOLS` (default `WHITEWHALEUSDT`): Comma-separated list of symbols.
- `DEPTH` (default `50`)
- `BASE_MM_NOTIONAL` (default `30000`)
- `LARGE_MOVE_NOTIONAL` (default `30000`)
- `LARGE_MOVE_WINDOW_BPS` (default `200`)
- `LARGE_MOVE_NOTIONAL_FLOOR` (default `2000`)
- `SIZE_BINS` (default `500,1000,2500,5000,10000,25000,50000`)
- `DISTANCE_BINS_BPS` (default `5,10,25,50,100,200`)
- `LOG_INTERVAL_MS` (default `5000`)
- `METRICS_INTERVAL_MS` (default `1000`)
- `DATA_DIR` (default `data`)
- `BASE_PATH` (default empty)
- `LIVE_MONITORING` (default `true`)

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
