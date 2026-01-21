import { Side, SideMoveStats, MoveStats } from "./types";

interface LevelState {
  size: number;
}

function emptySideStats(): SideMoveStats {
  return { adds: 0, changes: 0, removals: 0, sizeDelta: 0 };
}

export class LevelTracker {
  private levels = new Map<string, LevelState>();
  private stats: MoveStats = { bids: emptySideStats(), asks: emptySideStats() };

  recordUpdate(side: Side, price: number, nextSize: number, prevSize: number | null): void {
    const key = `${side}:${price}`;
    const sideStats = this.stats[side];

    if (prevSize === null && nextSize > 0) {
      this.levels.set(key, { size: nextSize });
      sideStats.adds += 1;
      sideStats.sizeDelta += nextSize;
      return;
    }

    if (prevSize !== null && nextSize === 0) {
      this.levels.delete(key);
      sideStats.removals += 1;
      sideStats.sizeDelta += prevSize;
      return;
    }

    if (prevSize !== null && nextSize !== prevSize) {
      this.levels.set(key, { size: nextSize });
      sideStats.changes += 1;
      sideStats.sizeDelta += Math.abs(nextSize - prevSize);
    }
  }

  snapshotAndReset(): MoveStats {
    const snapshot: MoveStats = {
      bids: { ...this.stats.bids },
      asks: { ...this.stats.asks },
    };
    this.stats = { bids: emptySideStats(), asks: emptySideStats() };
    return snapshot;
  }
}
