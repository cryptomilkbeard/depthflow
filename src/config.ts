export interface Config {
  symbols: string[];
  depth: number;
  baseMmNotional: number;
  largeMoveNotional: number;
  largeMoveWindowBps: number;
  largeMoveNotionalFloor: number;
  sizeBins: number[];
  distanceBinsBps: number[];
  logIntervalMs: number;
  metricsIntervalMs: number;
  dataDir: string;
}

function parseNumber(value: string | undefined, fallback: number): number {
  const parsed = value ? Number(value) : Number.NaN;
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseNumberList(value: string | undefined, fallback: number[]): number[] {
  if (!value) return fallback;
  const parsed = value
    .split(",")
    .map((item) => Number(item.trim()))
    .filter((item) => Number.isFinite(item));
  return parsed.length > 0 ? parsed : fallback;
}

export function loadConfig(): Config {
  const symbols = (process.env.SYMBOLS ?? "WHITEWHALEUSDT")
    .split(",")
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);

  return {
    symbols,
    depth: Math.max(1, parseNumber(process.env.DEPTH, 50)),
    baseMmNotional: parseNumber(process.env.BASE_MM_NOTIONAL, 30000),
    largeMoveNotional: parseNumber(process.env.LARGE_MOVE_NOTIONAL, 30000),
    largeMoveWindowBps: parseNumber(process.env.LARGE_MOVE_WINDOW_BPS, 200),
    largeMoveNotionalFloor: parseNumber(process.env.LARGE_MOVE_NOTIONAL_FLOOR, 2000),
    sizeBins: parseNumberList(process.env.SIZE_BINS, [500, 1000, 2500, 5000, 10000, 25000, 50000]),
    distanceBinsBps: parseNumberList(process.env.DISTANCE_BINS_BPS, [5, 10, 25, 50, 100, 200]),
    logIntervalMs: parseNumber(process.env.LOG_INTERVAL_MS, 5000),
    metricsIntervalMs: parseNumber(process.env.METRICS_INTERVAL_MS, 1000),
    dataDir: process.env.DATA_DIR ?? "data",
  };
}
