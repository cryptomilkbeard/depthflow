export type Side = "bids" | "asks";

export interface Level {
  price: number;
  size: number;
}

export interface SideMoveStats {
  adds: number;
  changes: number;
  removals: number;
  sizeDelta: number;
}

export interface MoveStats {
  bids: SideMoveStats;
  asks: SideMoveStats;
}

export interface LevelMetrics extends Level {
  notional: number;
  bpsFromMid: number;
}

export interface LevelMove {
  price: number;
  prevSize: number;
  nextSize: number;
  deltaSize: number;
  notionalDelta: number;
  bpsFromMid: number;
}

export interface MetricsPoint {
  ts: number;
  symbol: string;
  mid: number;
  bestBid: number;
  bestAsk: number;
  depth: number;
  baseMmNotional: number;
  totalNotionalBid: number;
  totalNotionalAsk: number;
  distanceBinsBps: number[];
  distanceBinCountsBid: number[];
  distanceBinCountsAsk: number[];
  maxDistanceBpsBid: number;
  maxDistanceBpsAsk: number;
  avgDistanceBpsBid: number;
  avgDistanceBpsAsk: number;
  outlierCountBid: number;
  outlierCountAsk: number;
  largeLevelsBid: LevelMetrics[];
  largeLevelsAsk: LevelMetrics[];
  moveStats: MoveStats;
  largeMovesBid?: LevelMove[];
  largeMovesAsk?: LevelMove[];
  exchanges?: Record<string, ExchangeMetrics>;
}

export interface ExchangeMetrics {
  mid: number;
  bestBid: number;
  bestAsk: number;
  depth: number;
  baseMmNotional: number;
  totalNotionalBid: number;
  totalNotionalAsk: number;
  distanceBinsBps: number[];
  distanceBinCountsBid: number[];
  distanceBinCountsAsk: number[];
  maxDistanceBpsBid: number;
  maxDistanceBpsAsk: number;
  avgDistanceBpsBid: number;
  avgDistanceBpsAsk: number;
  outlierCountBid: number;
  outlierCountAsk: number;
}
