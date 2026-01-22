const state = {
  data: new Map(),
  selectedSymbol: null,
  charts: {},
  sizeBins: [],
  largeMovesHistory: new Map(),
  lastMid: new Map(),
  oiFunding: new Map(),
  trades: [],
  recentTrades: new Map(),
  outlierLevelMap: new Map(),
};

const basePath = (() => {
  const pathname = location.pathname.replace(/\/+$/, "");
  if (!pathname || pathname === "/") return "";
  if (pathname.endsWith("/index.html")) {
    return pathname.slice(0, -"/index.html".length);
  }
  return pathname;
})();
const apiBase = `${basePath}/api`;

const symbolSelect = document.getElementById("symbolSelect");
const depthValue = document.getElementById("depthValue");
const baseNotional = document.getElementById("baseNotional");
const largeMovesBids = document.getElementById("largeMovesBids");
const largeMovesAsks = document.getElementById("largeMovesAsks");
const orderbookAsks = document.getElementById("orderbookAsks");
const orderbookBids = document.getElementById("orderbookBids");
const outlierLevels = document.getElementById("outlierLevels");
const tradeStream = document.getElementById("tradeStream");
const reportCsvBtn = document.getElementById("reportCsv");
const reportPdfBtn = document.getElementById("reportPdf");
const liveStatus = document.getElementById("liveStatus");
const perpOrderbookAsks = document.getElementById("perpOrderbookAsks");
const perpOrderbookBids = document.getElementById("perpOrderbookBids");
const orderbookState = new Map();
const perpOrderbookState = new Map();
const Z_THRESHOLD = 5;
const BPS_WINDOW = 250;
const outlierStore = new Map();
const oiValue = document.getElementById("oiValue");
const fundingValue = document.getElementById("fundingValue");
const fundingCountdown = document.getElementById("fundingCountdown");
const SYMLOG_C = 1e-6;
const SMA_WINDOW = 5;
const EXCHANGES = ["bybit", "mexc"];
const ORDERBOOK_COLORS = {
  bybit: { bid: "rgba(31, 166, 106, 0.65)", ask: "rgba(231, 76, 60, 0.65)" },
  mexc: { bid: "rgba(41, 128, 185, 0.65)", ask: "rgba(243, 156, 18, 0.65)" },
};

function formatNumber(value, decimals = 2) {
  return Number(value).toFixed(decimals);
}

function formatTime(ts) {
  const date = new Date(ts);
  return date.toLocaleTimeString([], { hour12: false });
}

function formatCountdown(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return "--";
  const totalSeconds = Math.floor(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(
    seconds
  ).padStart(2, "0")}`;
}

function updateLegendValues(chart, formatValue) {
  if (!chart) return;
  for (const dataset of chart.data.datasets) {
    if (!dataset.baseLabel) {
      dataset.baseLabel = dataset.label;
    }
    const data = dataset.data ?? [];
    let lastValue = null;
    if (data.length > 0 && typeof data[0] === "object") {
      for (let i = data.length - 1; i >= 0; i -= 1) {
        const entry = data[i];
        if (entry && Number.isFinite(entry.y)) {
          lastValue = entry.y;
          break;
        }
      }
    } else {
      for (let i = data.length - 1; i >= 0; i -= 1) {
        const entry = data[i];
        if (Number.isFinite(entry)) {
          lastValue = entry;
          break;
        }
      }
    }
    const formatted = lastValue === null ? "--" : formatValue(lastValue, dataset);
    dataset.label = `${dataset.baseLabel} (${formatted})`;
  }
}

function smoothArray(values, window) {
  const out = new Array(values.length).fill(null);
  let sum = 0;
  let count = 0;
  const queue = [];
  for (let i = 0; i < values.length; i += 1) {
    const value = values[i];
    queue.push(value);
    if (Number.isFinite(value)) {
      sum += value;
      count += 1;
    }
    if (queue.length > window) {
      const removed = queue.shift();
      if (Number.isFinite(removed)) {
        sum -= removed;
        count -= 1;
      }
    }
    out[i] = count > 0 ? sum / count : null;
  }
  return out;
}

function smoothXY(points, window) {
  const out = [];
  let sum = 0;
  let count = 0;
  const queue = [];
  for (const point of points) {
    const value = point.y;
    queue.push(value);
    if (Number.isFinite(value)) {
      sum += value;
      count += 1;
    }
    if (queue.length > window) {
      const removed = queue.shift();
      if (Number.isFinite(removed)) {
        sum -= removed;
        count -= 1;
      }
    }
    out.push({ x: point.x, y: count > 0 ? sum / count : null });
  }
  return out;
}

function symlog(value, constant = SYMLOG_C) {
  if (!Number.isFinite(value)) return 0;
  const sign = Math.sign(value);
  return sign * Math.log10(1 + Math.abs(value) / constant);
}

function invSymlog(value, constant = SYMLOG_C) {
  if (!Number.isFinite(value)) return 0;
  const sign = Math.sign(value);
  return sign * constant * (Math.pow(10, Math.abs(value)) - 1);
}

function ensureSymbol(symbol) {
  if (!state.data.has(symbol)) {
    state.data.set(symbol, []);
  }
}

function pushPoint(point) {
  ensureSymbol(point.symbol);
  const series = state.data.get(point.symbol);
  series.push(point);
  if (series.length > 600) series.shift();
  updateLargeMovesHistory(point);
}

function buildCharts() {
  const midCtx = document.getElementById("midChart");
  const distanceCtx = document.getElementById("distanceChart");
  const histCtx = document.getElementById("sizeHistChart");
  const oiFundingCtx = document.getElementById("oiFundingChart");

  state.charts.mid = new Chart(midCtx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Bybit Mid",
          baseLabel: "Bybit Mid",
          data: [],
          borderColor: "#9bdcff",
          borderWidth: 1,
          pointRadius: 0,
        },
        {
          label: "MEXC Mid",
          baseLabel: "MEXC Mid",
          data: [],
          borderColor: "#ffd166",
          borderWidth: 1,
          pointRadius: 0,
          borderDash: [6, 4],
        },
      ],
    },
    options: chartOptions(),
  });

  state.charts.distance = new Chart(distanceCtx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Bybit Max Bid BPS",
          baseLabel: "Bybit Max Bid BPS",
          data: [],
          borderColor: "#5aa2ff",
          borderWidth: 1,
          pointRadius: 0,
        },
        {
          label: "Bybit Max Ask BPS",
          baseLabel: "Bybit Max Ask BPS",
          data: [],
          borderColor: "#ff7b5a",
          borderWidth: 1,
          pointRadius: 0,
        },
        {
          label: "MEXC Max Bid BPS",
          baseLabel: "MEXC Max Bid BPS",
          data: [],
          borderColor: "#63d4a5",
          borderWidth: 1,
          pointRadius: 0,
          borderDash: [6, 4],
        },
        {
          label: "MEXC Max Ask BPS",
          baseLabel: "MEXC Max Ask BPS",
          data: [],
          borderColor: "#ffb36b",
          borderWidth: 1,
          pointRadius: 0,
          borderDash: [6, 4],
        },
      ],
    },
    options: chartOptions(),
  });

  state.charts.sizeHist = new Chart(histCtx, {
    type: "bar",
    data: {
      labels: [],
      datasets: [
        {
          label: "Bybit Bid Sizes",
          data: [],
          backgroundColor: "rgba(31, 166, 106, 0.75)",
          stack: "bid",
        },
        {
          label: "Bybit Ask Sizes",
          data: [],
          backgroundColor: "rgba(231, 76, 60, 0.75)",
          stack: "ask",
        },
        {
          label: "MEXC Bid Sizes",
          data: [],
          backgroundColor: "rgba(41, 128, 185, 0.75)",
          stack: "bid",
        },
        {
          label: "MEXC Ask Sizes",
          data: [],
          backgroundColor: "rgba(243, 156, 18, 0.75)",
          stack: "ask",
        },
      ],
    },
    options: {
      ...chartOptions(),
      scales: {
        x: { stacked: true, ticks: { color: "#a7b6c9" }, grid: { display: false } },
        y: { stacked: true, ticks: { color: "#a7b6c9" }, grid: { color: "rgba(230, 236, 243, 0.12)" } },
      },
    },
  });

  state.charts.oiFunding = new Chart(oiFundingCtx, {
    type: "line",
    data: {
      datasets: [
        {
          label: "Bybit Open Interest",
          baseLabel: "Bybit Open Interest",
          data: [],
          borderColor: "#8bd3ff",
          borderWidth: 1,
          pointRadius: 0,
          yAxisID: "y",
        },
        {
          label: "Bybit Funding Rate",
          baseLabel: "Bybit Funding Rate",
          data: [],
          borderColor: "#f4c542",
          borderWidth: 1,
          pointRadius: 0,
          yAxisID: "y1",
        },
        {
          label: "MEXC Open Interest",
          baseLabel: "MEXC Open Interest",
          data: [],
          borderColor: "#a1f0ff",
          borderWidth: 1,
          pointRadius: 0,
          borderDash: [6, 4],
          yAxisID: "y",
        },
        {
          label: "MEXC Funding Rate",
          baseLabel: "MEXC Funding Rate",
          data: [],
          borderColor: "#ffd166",
          borderWidth: 1,
          pointRadius: 0,
          borderDash: [6, 4],
          yAxisID: "y1",
        },
      ],
    },
    options: {
      ...chartOptions(),
      scales: {
        x: {
          type: "linear",
          bounds: "data",
          offset: false,
          ticks: {
            color: "#a7b6c9",
            callback: (value) => formatTime(value),
            autoSkip: true,
            maxTicksLimit: 6,
            maxRotation: 0,
            minRotation: 0,
            padding: 6,
          },
          grid: { display: false },
        },
        y: {
          ticks: { color: "#a7b6c9", autoSkip: true, maxTicksLimit: 6, padding: 6 },
          grid: { color: "rgba(230, 236, 243, 0.12)" },
        },
        y1: {
          position: "right",
          min: symlog(-0.025),
          max: symlog(0.025),
          ticks: {
            color: "#a7b6c9",
            callback: (value) => invSymlog(value).toFixed(6),
            autoSkip: true,
            maxTicksLimit: 6,
            padding: 6,
          },
          grid: { drawOnChartArea: false },
        },
      },
    },
  });
}

function chartOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    font: {
      size: 10,
    },
    layout: {
      padding: { top: 8, right: 10, bottom: 8, left: 6 },
    },
    scales: {
      x: {
        ticks: {
          color: "#a7b6c9",
          autoSkip: true,
          maxTicksLimit: 6,
          maxRotation: 0,
          minRotation: 0,
          padding: 6,
        },
        grid: { display: false },
      },
      y: {
        ticks: {
          color: "#a7b6c9",
          autoSkip: true,
          maxTicksLimit: 6,
          padding: 6,
        },
        grid: { color: "rgba(230, 236, 243, 0.12)" },
      },
    },
    plugins: {
      legend: {
        labels: {
          color: "#e6ecf3",
          boxWidth: 16,
          boxHeight: 8,
          padding: 12,
          font: { size: 12, lineHeight: 1.2 },
        },
      },
    },
  };
}

function updateCharts(symbol) {
  const series = state.data.get(symbol) ?? [];
  const labels = series.map((point) => formatTime(point.ts));

  state.charts.mid.data.labels = labels;
  const bybitMid = series.map(
    (point) => point.exchanges?.bybit?.mid ?? point.mid ?? null
  );
  const mexcMid = series.map(
    (point) => point.exchanges?.mexc?.mid ?? null
  );
  state.charts.mid.data.datasets[0].data = smoothArray(bybitMid, SMA_WINDOW);
  state.charts.mid.data.datasets[1].data = smoothArray(mexcMid, SMA_WINDOW);
  updateLegendValues(state.charts.mid, (value) => formatNumber(value, 5));

  state.charts.distance.data.labels = labels;
  const bybitBid = series.map(
    (point) => point.exchanges?.bybit?.maxDistanceBpsBid ?? point.maxDistanceBpsBid ?? null
  );
  const bybitAsk = series.map(
    (point) => point.exchanges?.bybit?.maxDistanceBpsAsk ?? point.maxDistanceBpsAsk ?? null
  );
  const mexcBid = series.map(
    (point) => point.exchanges?.mexc?.maxDistanceBpsBid ?? null
  );
  const mexcAsk = series.map(
    (point) => point.exchanges?.mexc?.maxDistanceBpsAsk ?? null
  );
  state.charts.distance.data.datasets[0].data = smoothArray(bybitBid, SMA_WINDOW);
  state.charts.distance.data.datasets[1].data = smoothArray(bybitAsk, SMA_WINDOW);
  state.charts.distance.data.datasets[2].data = smoothArray(mexcBid, SMA_WINDOW);
  state.charts.distance.data.datasets[3].data = smoothArray(mexcAsk, SMA_WINDOW);
  updateLegendValues(state.charts.distance, (value) => formatNumber(value, 2));

  const latest = updateOiFundingChart(symbol);
  updateOiFundingLegend(latest);
  updateLegendValues(state.charts.oiFunding, (value, dataset) => {
    if (String(dataset.baseLabel).includes("Open Interest")) {
      return formatNumber(value, 0);
    }
    return formatNumber(invSymlog(value), 6);
  });

  state.charts.mid.update("none");
  state.charts.distance.update("none");
  state.charts.oiFunding.update("none");

  const lastPoint = series[series.length - 1];
  if (lastPoint) {
    depthValue.textContent = lastPoint.depth;
    baseNotional.textContent = formatNumber(lastPoint.baseMmNotional, 0);
    const history = state.largeMovesHistory.get(symbol);
    renderMoveTable(largeMovesBids, history?.bids ?? []);
    renderMoveTable(largeMovesAsks, history?.asks ?? []);
    state.lastMid.set(symbol, lastPoint.mid);
  }
}

function updateOiFundingChart(symbol) {
  const byExchange = state.oiFunding.get(symbol) ?? { bybit: [], mexc: [] };
  const bybitSeries = byExchange.bybit ?? [];
  const mexcSeries = byExchange.mexc ?? [];

  const bybitOi = bybitSeries
    .filter((point) => Number.isFinite(point.openInterest) && point.openInterest > 0)
    .sort((a, b) => a.ts - b.ts);
  const bybitFunding = bybitSeries
    .filter((point) => Number.isFinite(point.fundingRate))
    .sort((a, b) => a.ts - b.ts);
  const mexcOi = mexcSeries
    .filter((point) => Number.isFinite(point.openInterest) && point.openInterest > 0)
    .sort((a, b) => a.ts - b.ts);
  const mexcFunding = mexcSeries
    .filter((point) => Number.isFinite(point.fundingRate))
    .sort((a, b) => a.ts - b.ts);

  state.charts.oiFunding.data.datasets[0].data = smoothXY(
    bybitOi.map((point) => ({ x: point.ts, y: point.openInterest })),
    SMA_WINDOW
  );
  state.charts.oiFunding.data.datasets[1].data = smoothXY(
    bybitFunding.map((point) => ({ x: point.ts, y: symlog(point.fundingRate) })),
    SMA_WINDOW
  );
  state.charts.oiFunding.data.datasets[2].data = smoothXY(
    mexcOi.map((point) => ({ x: point.ts, y: point.openInterest })),
    SMA_WINDOW
  );
  state.charts.oiFunding.data.datasets[3].data = smoothXY(
    mexcFunding.map((point) => ({ x: point.ts, y: symlog(point.fundingRate) })),
    SMA_WINDOW
  );

  const lastBybit = bybitSeries[bybitSeries.length - 1];
  const lastMexc = mexcSeries[mexcSeries.length - 1];
  return { bybit: lastBybit, mexc: lastMexc };
}

function updateOiFundingLegend(point) {
  if (!point) return;
  const preferred = point.bybit ?? point.mexc ?? point;
  if (oiValue && Number.isFinite(preferred.openInterest)) {
    oiValue.textContent = formatNumber(preferred.openInterest, 0);
  }
  if (fundingValue) {
    const rate = preferred.fundingRate;
    fundingValue.textContent = Number.isFinite(rate) ? formatNumber(rate, 5) : "--";
  }
  if (fundingCountdown) {
    const nextTs = preferred.nextFundingTime ?? 0;
    fundingCountdown.textContent = nextTs ? formatCountdown(nextTs - Date.now()) : "--";
  }
}

function renderMoveTable(root, rows) {
  root.innerHTML = "";
  for (const row of rows) {
    const tr = document.createElement("tr");
    const deltaClass = row.deltaSize >= 0 ? "delta-up" : "delta-down";
    tr.innerHTML = `
      <td>${row.ts ?? ""}</td>
      <td>${row.market ?? "--"}</td>
      <td>${row.exchange ?? "--"}</td>
      <td>${formatNumber(row.price, 5)}</td>
      <td class="${deltaClass}">${formatNumber(row.deltaSize)}</td>
      <td>${formatNumber(row.notionalDelta)}</td>
      <td>${formatNumber(row.bpsFromMid, 2)}</td>
    `;
    root.appendChild(tr);
  }
}

function renderOrderbook(symbol) {
  const book = orderbookState.get(symbol);
  if (!book) return;
  const sources =
    book.sources ??
    {
      bybit: { bids: book.bids ?? [], asks: book.asks ?? [] },
    };
  const depth = book.depth ?? book.bids?.length ?? 0;
  const asksAsc = buildStackedLevels(sources, "ask", book.mid, depth);
  const bidsDesc = buildStackedLevels(sources, "bid", book.mid, depth);
  const outliers = computeOutliers(
    asksAsc.map((level) => [level.price, level.total]),
    bidsDesc.map((level) => [level.price, level.total]),
    book.mid,
    "Spot",
    sources.mexc ? "Aggregated" : "Bybit"
  );
  const outlierKeys = new Set(outliers.map((row) => outlierKey(row.side, row.price)));
  state.outlierLevelMap = buildOutlierLevelMap(asksAsc, bidsDesc, outlierKeys);
  renderSide(orderbookAsks, asksAsc, "ask", outlierKeys);
  renderSide(orderbookBids, bidsDesc, "bid", outlierKeys);
  updateOutliers("Spot", outliers);
}

function renderPerpOrderbook(symbol) {
  const book = perpOrderbookState.get(symbol);
  if (!book) return;
  const sources =
    book.sources ??
    {
      bybit: { bids: book.bids ?? [], asks: book.asks ?? [] },
    };
  const depth = book.depth ?? book.bids?.length ?? 0;
  const asksAsc = buildStackedLevels(sources, "ask", book.mid, depth);
  const bidsDesc = buildStackedLevels(sources, "bid", book.mid, depth);
  renderSide(perpOrderbookAsks, asksAsc, "ask");
  renderSide(perpOrderbookBids, bidsDesc, "bid");
  updateHistogram(sources);
  const outliers = computeOutliers(
    asksAsc.map((level) => [level.price, level.total]),
    bidsDesc.map((level) => [level.price, level.total]),
    book.mid,
    "Perp",
    "Aggregated"
  );
  updateOutliers("Perp", outliers);
}

function renderSide(root, levels, side, outlierKeys) {
  root.innerHTML = "";
  const totals = [];
  let cumulativeTotal = 0;
  let cumulativeBybit = 0;
  let cumulativeMexc = 0;
  for (const level of levels) {
    cumulativeTotal += level.total;
    cumulativeBybit += level.bybit;
    cumulativeMexc += level.mexc;
    totals.push({
      total: cumulativeTotal,
      bybit: cumulativeBybit,
      mexc: cumulativeMexc,
    });
  }
  const maxTotal = totals[totals.length - 1]?.total ?? 1;

  levels.forEach((level, index) => {
    const cumulative = totals[index];
    const tr = document.createElement("tr");
    const key = outlierKey(side, level.price);
    const isOutlier = outlierKeys ? outlierKeys.has(key) : false;
    if (isOutlier) {
      tr.classList.add("book-outlier");
    }
    const totalPct = (cumulative.total / maxTotal) * 100;
    const bybitPct = (cumulative.bybit / maxTotal) * 100;
    const mexcPct = (cumulative.mexc / maxTotal) * 100;
    const bybitStop = Math.min(bybitPct, totalPct);
    const mexcStop = Math.min(bybitPct + mexcPct, totalPct);
    const colors =
      side === "bid"
        ? { bybit: ORDERBOOK_COLORS.bybit.bid, mexc: ORDERBOOK_COLORS.mexc.bid }
        : { bybit: ORDERBOOK_COLORS.bybit.ask, mexc: ORDERBOOK_COLORS.mexc.ask };
    const direction = side === "bid" ? "to left" : "to right";
    tr.style.backgroundImage = `linear-gradient(${direction}, ${colors.bybit} 0% ${bybitStop}%, ${colors.mexc} ${bybitStop}% ${mexcStop}%, transparent ${mexcStop}% 100%)`;
    tr.innerHTML = `
      <td>${isOutlier ? `<span class="level-pill">${index + 1}</span>` : ""}</td>
      <td>${formatNumber(level.price, 5)}</td>
      <td>${formatNumber(level.total)}</td>
      <td>${formatNumber(cumulative.total)}</td>
    `;
    root.appendChild(tr);
  });
}

function computeOutliers(asksDesc, bidsDesc, mid, market, exchange) {
  const askOutliers = collectOutliers(asksDesc, "Ask", mid, market, exchange);
  const bidOutliers = collectOutliers(bidsDesc, "Bid", mid, market, exchange);
  return [...askOutliers, ...bidOutliers];
}

function updateOutliers(market, outliers) {
  outlierStore.set(market, outliers);
  renderOutlierTable();
}

function buildStackedLevels(sources, side, mid, depth) {
  const sideKey = side === "bid" ? "bids" : "asks";
  const merged = new Map();
  for (const exchange of EXCHANGES) {
    const levels = sources?.[exchange]?.[sideKey] ?? [];
    for (const [price, size] of levels) {
      if (!Number.isFinite(price) || !Number.isFinite(size) || size <= 0) continue;
      const entry = merged.get(price) ?? { price, total: 0, bybit: 0, mexc: 0 };
      entry[exchange] += size;
      entry.total += size;
      merged.set(price, entry);
    }
  }
  let levels = Array.from(merged.values());
  levels = filterByBps(levels, mid);
  levels.sort((a, b) => (side === "bid" ? b.price - a.price : a.price - b.price));
  if (depth) {
    levels = levels.slice(0, depth);
  }
  return levels;
}

function collectOutliers(levels, side, mid, market, exchange) {
  const stats = calcStats(levels);
  if (stats.stdDev === 0) return [];
  return levels
    .map((level) => {
      const zScore = (level[1] - stats.mean) / stats.stdDev;
      return {
        market,
        exchange,
        side,
        price: level[0],
        size: level[1],
        zScore,
        bps: (Math.abs(level[0] - mid) / mid) * 10000,
      };
    })
    .filter((row) => row.zScore >= Z_THRESHOLD);
}

function renderOutlierTable() {
  outlierLevels.innerHTML = "";
  const combined = [...(outlierStore.get("Spot") ?? [])]
    .sort((a, b) => b.zScore - a.zScore)
    .slice(0, 5);
  for (const [index, row] of combined.entries()) {
    const tr = document.createElement("tr");
    tr.classList.add("outlier-row");
    if (index < 2) {
      tr.classList.add("outlier-pulse");
    }
    const levelIndex = state.outlierLevelMap.get(outlierKey(row.side, row.price));
    tr.innerHTML = `
      <td>${levelIndex ? `<span class="level-pill">${levelIndex}</span>` : ""}</td>
      <td>${row.exchange ?? "--"}</td>
      <td>${row.side}</td>
      <td>${formatNumber(row.price, 5)}</td>
      <td>${formatNumber(row.size)}</td>
      <td>${formatNumber(row.zScore, 2)}</td>
      <td>${formatNumber(row.bps, 2)}</td>
    `;
    outlierLevels.appendChild(tr);
  }
}

function buildOutlierSubrow(row, exchange, size) {
  const tr = document.createElement("tr");
  tr.classList.add("outlier-subrow");
  tr.innerHTML = `
    <td></td>
    <td>${exchange}</td>
    <td>${row.side}</td>
    <td>${formatNumber(row.price, 5)}</td>
    <td>${Number.isFinite(size) ? formatNumber(size) : "--"}</td>
    <td></td>
    <td></td>
  `;
  return tr;
}

function outlierKey(side, price) {
  const normalizedSide = String(side).toLowerCase();
  return `${normalizedSide}:${Number(price).toFixed(8)}`;
}

function buildOutlierLevelMap(asksAsc, bidsDesc, outlierKeys) {
  const map = new Map();
  for (const [index, level] of asksAsc.entries()) {
    const key = outlierKey("ask", level.price);
    if (outlierKeys.has(key)) {
      map.set(key, index + 1);
    }
  }
  for (const [index, level] of bidsDesc.entries()) {
    const key = outlierKey("bid", level.price);
    if (outlierKeys.has(key)) {
      map.set(key, index + 1);
    }
  }
  return map;
}

function getPerpBreakdown(side, price) {
  const book = perpOrderbookState.get(state.selectedSymbol);
  if (!book?.sources) return null;
  const sideKey = side.toLowerCase() === "ask" ? "asks" : "bids";
  const bybit = findSizeAtPrice(book.sources.bybit?.[sideKey] ?? [], price);
  const mexc = findSizeAtPrice(book.sources.mexc?.[sideKey] ?? [], price);
  if (!Number.isFinite(bybit) && !Number.isFinite(mexc)) return null;
  return { bybit, mexc };
}

function findSizeAtPrice(levels, price) {
  for (const [levelPrice, size] of levels) {
    if (levelPrice === price) return size;
  }
  return null;
}

function calcStats(levels) {
  const sizes = levels.map((level) => level[1]);
  const mean = sizes.length ? sizes.reduce((a, b) => a + b, 0) / sizes.length : 0;
  const variance = sizes.length
    ? sizes.reduce((sum, size) => sum + Math.pow(size - mean, 2), 0) / sizes.length
    : 0;
  const stdDev = Math.sqrt(variance);
  return { mean, stdDev };
}

function filterByBps(levels, mid) {
  if (!mid || mid <= 0) return levels;
  return levels.filter((level) => {
    const price = Array.isArray(level) ? level[0] : level.price;
    return (Math.abs(price - mid) / mid) * 10000 <= BPS_WINDOW;
  });
}

function updateLargeMovesHistory(point) {
  const symbol = point.symbol;
  if (!state.largeMovesHistory.has(symbol)) {
    state.largeMovesHistory.set(symbol, { bids: [], asks: [] });
  }
  const history = state.largeMovesHistory.get(symbol);
  const stamp = new Date(point.ts).toLocaleTimeString([], { hour12: false });
  const bids = (point.largeMovesBid ?? []).map((move) => ({
    ...move,
    ts: stamp,
    market: "Perp",
    exchange: "Aggregated",
    side: "Bid",
  }));
  const asks = (point.largeMovesAsk ?? []).map((move) => ({
    ...move,
    ts: stamp,
    market: "Perp",
    exchange: "Aggregated",
    side: "Ask",
  }));
  history.bids.unshift(...bids);
  history.asks.unshift(...asks);
  if (history.bids.length > 10) history.bids.length = 10;
  if (history.asks.length > 10) history.asks.length = 10;
}

function updateOiFunding(point) {
  const symbol = point.symbol;
  const exchange = point.exchange ?? "bybit";
  if (!state.oiFunding.has(symbol)) {
    state.oiFunding.set(symbol, { bybit: [], mexc: [] });
  }
  const bucket = state.oiFunding.get(symbol);
  if (!bucket[exchange]) {
    bucket[exchange] = [];
  }
  const list = bucket[exchange];
  const entry = { ...point, exchange, ts: Number(point.ts) };
  if (!Number.isFinite(entry.ts)) {
    entry.ts = Date.now();
  }
  if (!Number.isFinite(entry.openInterest) || entry.openInterest <= 0) {
    entry.openInterest = undefined;
  }
  if (!Number.isFinite(entry.fundingRate)) {
    entry.fundingRate = undefined;
  }
  const last = list[list.length - 1];
  if (last && last.ts === entry.ts) {
    const merged = { ...last, ...entry };
    if (!Number.isFinite(entry.fundingRate) || entry.fundingRate === 0) {
      merged.fundingRate = last.fundingRate;
    }
    if (!Number.isFinite(entry.openInterest) || entry.openInterest === 0) {
      merged.openInterest = last.openInterest;
    }
    if (!Number.isFinite(entry.nextFundingTime) || entry.nextFundingTime === 0) {
      merged.nextFundingTime = last.nextFundingTime;
    }
    list[list.length - 1] = merged;
    if (list.length > 600) list.shift();
    return merged;
  } else {
    if (!Number.isFinite(entry.fundingRate) || entry.fundingRate === 0) {
      entry.fundingRate = last?.fundingRate;
    }
    if (!Number.isFinite(entry.openInterest) || entry.openInterest === 0) {
      entry.openInterest = last?.openInterest;
    }
    if (!Number.isFinite(entry.nextFundingTime) || entry.nextFundingTime === 0) {
      entry.nextFundingTime = last?.nextFundingTime;
    }
    list.push(entry);
    if (list.length > 600) list.shift();
    return entry;
  }
}

function updateHistogram(sources) {
  if (!state.charts.sizeHist) return;
  const bins = state.sizeBins;
  if (!bins || bins.length === 0) return;
  const bybitBids = sources?.bybit?.bids ?? [];
  const bybitAsks = sources?.bybit?.asks ?? [];
  const mexcBids = sources?.mexc?.bids ?? [];
  const mexcAsks = sources?.mexc?.asks ?? [];
  const bidCounts = countBins(bybitBids, bins);
  const askCounts = countBins(bybitAsks, bins);
  const mexcBidCounts = countBins(mexcBids, bins);
  const mexcAskCounts = countBins(mexcAsks, bins);
  state.charts.sizeHist.data.labels = binLabels(bins);
  state.charts.sizeHist.data.datasets[0].data = bidCounts;
  state.charts.sizeHist.data.datasets[1].data = askCounts;
  state.charts.sizeHist.data.datasets[2].data = mexcBidCounts;
  state.charts.sizeHist.data.datasets[3].data = mexcAskCounts;
  state.charts.sizeHist.update("none");
}

function countBins(levels, bins) {
  const counts = new Array(bins.length + 1).fill(0);
  for (const level of levels) {
    const size = level[1];
    let placed = false;
    for (let i = 0; i < bins.length; i += 1) {
      if (size <= bins[i]) {
        counts[i] += 1;
        placed = true;
        break;
      }
    }
    if (!placed) counts[bins.length] += 1;
  }
  return counts;
}

function binLabels(bins) {
  const labels = bins.map((bin) => `<=${bin}`);
  labels.push(`>${bins[bins.length - 1]}`);
  return labels;
}

function refreshSymbols() {
  const symbols = Array.from(state.data.keys());
  symbolSelect.innerHTML = "";
  for (const symbol of symbols) {
    const option = document.createElement("option");
    option.value = symbol;
    option.textContent = symbol;
    symbolSelect.appendChild(option);
  }
  if (!state.selectedSymbol && symbols.length > 0) {
    state.selectedSymbol = symbols[0];
    symbolSelect.value = state.selectedSymbol;
  }
  if (state.selectedSymbol) {
    updateCharts(state.selectedSymbol);
    renderOrderbook(state.selectedSymbol);
    renderPerpOrderbook(state.selectedSymbol);
    renderTrades();
    updateReportLinks();
  }
}

symbolSelect.addEventListener("change", (event) => {
  state.selectedSymbol = event.target.value;
  updateCharts(state.selectedSymbol);
  renderTrades();
  updateReportLinks();
});

function renderTrades() {
  if (!tradeStream || !state.selectedSymbol) return;
  tradeStream.innerHTML = "";
  const now = Date.now();
  for (const [key, ts] of state.recentTrades.entries()) {
    if (now - ts > 1800) {
      state.recentTrades.delete(key);
    }
  }
  const rows = state.trades
    .filter((trade) => trade.symbol === state.selectedSymbol)
    .slice(0, 10);
  for (const trade of rows) {
    const tr = document.createElement("tr");
    const sideClass = trade.side === "Buy" ? "delta-up" : "delta-down";
    const key = tradeKey(trade);
    const flash = state.recentTrades.has(key);
    if (flash) {
      tr.classList.add("trade-flash", trade.side === "Buy" ? "trade-buy" : "trade-sell");
    }
    tr.innerHTML = `
      <td>${formatTime(trade.ts)}</td>
      <td>${trade.exchange}</td>
      <td class="${sideClass}">${trade.side}</td>
      <td>${formatNumber(trade.price, 6)}</td>
      <td>${formatNumber(trade.qty, 4)}</td>
    `;
    tradeStream.appendChild(tr);
  }
}

function tradeKey(trade) {
  return [trade.ts, trade.exchange, trade.side, trade.price, trade.qty, trade.symbol].join("|");
}

function updateReportLinks() {
  if (!state.selectedSymbol) return;
  const params = new URLSearchParams({ symbol: state.selectedSymbol });
  const query = params.toString();
  if (reportCsvBtn) {
    reportCsvBtn.href = `${apiBase}/outliers/report/csv?${query}`;
  }
  if (reportPdfBtn) {
    reportPdfBtn.href = `${apiBase}/outliers/report/pdf?${query}`;
  }
}

async function bootstrap() {
  const config = await fetch(`${apiBase}/config`).then((res) => res.json());
  state.sizeBins = config.sizeBins ?? [];
  buildCharts();
  if (liveStatus) {
    const status = await fetch(`${apiBase}/status`).then((res) => res.json());
    const isLive = Boolean(status?.liveMonitoring);
    liveStatus.textContent = isLive ? "On" : "Off";
    liveStatus.classList.toggle("status-on", isLive);
    liveStatus.classList.toggle("status-off", !isLive);
  }
  const response = await fetch(`${apiBase}/history?limit=600`);
  const history = await response.json();
  for (const point of history) {
    pushPoint(point);
  }
  refreshSymbols();
  let ws = null;
  let reconnectTimer = null;

  const wsProtocol = location.protocol === "https:" ? "wss" : "ws";
  const wsPath = basePath ? `${basePath}/` : "/";
  const wsUrl = `${wsProtocol}://${location.host}${wsPath}`;

  const connectWs = () => {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
      return;
    }
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    ws = new WebSocket(wsUrl);
    ws.addEventListener("message", (event) => {
      const payload = JSON.parse(event.data);
      if (payload.type === "metrics") {
        pushPoint(payload.data);
        refreshSymbols();
      }
      if (payload.type === "book") {
        orderbookState.set(payload.data.symbol, payload.data);
        if (payload.data.symbol === state.selectedSymbol) {
          renderOrderbook(state.selectedSymbol);
        }
      }
      if (payload.type === "perpBook") {
        const mid = state.lastMid.get(payload.data.symbol);
        const data = { ...payload.data, mid: mid ?? payload.data.bids[0]?.[0] ?? 0 };
        perpOrderbookState.set(payload.data.symbol, data);
        if (payload.data.symbol === state.selectedSymbol) {
          renderPerpOrderbook(state.selectedSymbol);
        }
      }
      if (payload.type === "oiFunding") {
        const raw = payload.data;
        updateOiFunding({ ...raw, ts: Number(raw.ts) });
        if (raw.symbol === state.selectedSymbol) {
          const latest = updateOiFundingChart(raw.symbol);
          updateOiFundingLegend(latest);
          state.charts.oiFunding.update("none");
        }
      }
      if (payload.type === "trade") {
        const trade = payload.data;
        if (!trade || !trade.symbol) return;
        state.trades.unshift(trade);
        state.recentTrades.set(tradeKey(trade), Date.now());
        if (state.trades.length > 500) {
          state.trades.length = 500;
        }
        if (trade.symbol === state.selectedSymbol) {
          renderTrades();
        }
      }
    });
    ws.addEventListener("close", () => {
      if (document.visibilityState !== "visible") return;
      reconnectTimer = setTimeout(connectWs, 1500);
    });
    ws.addEventListener("error", () => {
      if (ws) ws.close();
    });
  };

  connectWs();

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      connectWs();
    }
  });
  window.addEventListener("focus", connectWs);
  window.addEventListener("online", connectWs);

  const [bybitOiResponse, mexcOiResponse] = await Promise.all([
    fetch(`${apiBase}/oi-funding?limit=600&exchange=bybit`),
    fetch(`${apiBase}/oi-funding?limit=600&exchange=mexc`),
  ]);
  const bybitHistory = await bybitOiResponse.json();
  const mexcHistory = await mexcOiResponse.json();
  for (const raw of bybitHistory) {
    updateOiFunding({ ...raw, exchange: "bybit", ts: Number(raw.ts) });
  }
  for (const raw of mexcHistory) {
    updateOiFunding({ ...raw, exchange: "mexc", ts: Number(raw.ts) });
  }
}

bootstrap();
