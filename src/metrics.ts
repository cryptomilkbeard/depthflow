import { LevelMetrics, MetricsPoint, Side, MoveStats } from "./types";

const Z_SCORE_THRESHOLD = 4;

interface OrderbookSide {
  levels: Array<[number, number]>;
  totalNotional: number;
  distances: number[];
  maxDistance: number;
  avgDistance: number;
  largeLevels: LevelMetrics[];
}

function computeSideMetrics(
  side: Side,
  levels: Array<[number, number]>,
  mid: number,
  baseMmNotional: number,
  bins: number[],
  maxLargeLevels: number
): OrderbookSide {
  let totalNotional = 0;
  let maxDistance = 0;
  let distanceSum = 0;
  const distances: number[] = [];
  const largeLevels: LevelMetrics[] = [];

  for (const [price, size] of levels) {
    const notional = price * size;
    totalNotional += notional;
    const bpsFromMid = Math.abs(price - mid) / mid * 10000;
    distances.push(bpsFromMid);
    distanceSum += bpsFromMid;
    if (bpsFromMid > maxDistance) maxDistance = bpsFromMid;

    if (notional >= baseMmNotional) {
      largeLevels.push({ price, size, notional, bpsFromMid });
    }
  }

  largeLevels.sort((a, b) => b.notional - a.notional);
  if (largeLevels.length > maxLargeLevels) {
    largeLevels.length = maxLargeLevels;
  }

  return {
    levels,
    totalNotional,
    distances,
    maxDistance,
    avgDistance: distances.length > 0 ? distanceSum / distances.length : 0,
    largeLevels,
  };
}

function countBins(distances: number[], bins: number[]): number[] {
  const counts = new Array(bins.length + 1).fill(0);
  for (const distance of distances) {
    let placed = false;
    for (let i = 0; i < bins.length; i += 1) {
      if (distance <= bins[i]) {
        counts[i] += 1;
        placed = true;
        break;
      }
    }
    if (!placed) counts[bins.length] += 1;
  }
  return counts;
}

function countOutliers(levels: Array<[number, number]>): number {
  if (levels.length === 0) return 0;
  const sizes = levels.map((level) => level[1]);
  const mean = sizes.reduce((sum, size) => sum + size, 0) / sizes.length;
  const variance =
    sizes.reduce((sum, size) => sum + Math.pow(size - mean, 2), 0) / sizes.length;
  const stdDev = Math.sqrt(variance);
  if (stdDev === 0) return 0;
  let count = 0;
  for (const size of sizes) {
    const zScore = (size - mean) / stdDev;
    if (zScore >= Z_SCORE_THRESHOLD) count += 1;
  }
  return count;
}

export function computeMetrics(
  symbol: string,
  bids: Array<[number, number]>,
  asks: Array<[number, number]>,
  baseMmNotional: number,
  bins: number[],
  moveStats: MoveStats
): MetricsPoint | null {
  if (bids.length === 0 || asks.length === 0) return null;

  const bestBid = bids[0][0];
  const bestAsk = asks[0][0];
  const mid = (bestBid + bestAsk) / 2;

  const bidMetrics = computeSideMetrics("bids", bids, mid, baseMmNotional, bins, 5);
  const askMetrics = computeSideMetrics("asks", asks, mid, baseMmNotional, bins, 5);
  const outlierCountBid = countOutliers(bids);
  const outlierCountAsk = countOutliers(asks);

  return {
    ts: Date.now(),
    symbol,
    mid,
    bestBid,
    bestAsk,
    depth: bids.length,
    baseMmNotional,
    totalNotionalBid: bidMetrics.totalNotional,
    totalNotionalAsk: askMetrics.totalNotional,
    distanceBinsBps: bins,
    distanceBinCountsBid: countBins(bidMetrics.distances, bins),
    distanceBinCountsAsk: countBins(askMetrics.distances, bins),
    maxDistanceBpsBid: bidMetrics.maxDistance,
    maxDistanceBpsAsk: askMetrics.maxDistance,
    avgDistanceBpsBid: bidMetrics.avgDistance,
    avgDistanceBpsAsk: askMetrics.avgDistance,
    outlierCountBid,
    outlierCountAsk,
    largeLevelsBid: bidMetrics.largeLevels,
    largeLevelsAsk: askMetrics.largeLevels,
    moveStats,
  };
}
