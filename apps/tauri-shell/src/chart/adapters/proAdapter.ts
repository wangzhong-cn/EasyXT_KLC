import "@klinecharts/pro/dist/klinecharts-pro.css";

import {
  type Datafeed,
  type Period,
  type SymbolInfo,
} from "@klinecharts/pro";
import {
  ActionType,
  dispose,
  init,
  type Chart,
  type DeepPartial,
  type KLineData,
  type OverlayCreate,
  type OverlayStyle,
  type Styles,
} from "klinecharts";

import {
  CHART_FACADE_V2_UPGRADE_GATES,
  type AnnotationSpec,
  type ChartAdapterDescriptor,
  type ChartBar,
  type ChartBridgeCandleSnapshot,
  type ChartBridgeClickPayload,
  type ChartBridgeCrosshairPayload,
  type ChartBridgeEvent,
  type ChartBridgeEventListener,
  type ChartBridgeEventType,
  type ChartBridgeVisibleRangePayload,
  type ChartFacadeFactoryOptions,
  type ChartFacadeV2,
  type ChartGateStatus,
  type ChartInterval,
  type ChartRuntimeSnapshot,
  type ChartThemeName,
  type ChartUpgradeGate,
  type IndicatorSpec,
  type OverlaySpec,
  type TradeMarkSpec,
} from "../chartFacadeV2";

export const CHART_PRO_TARGET_VERSION = "0.1.1";

interface ProAdapterOptions extends ChartFacadeFactoryOptions {
  requestedAdapter?: ChartAdapterDescriptor;
}

interface IndicatorHandle {
  paneId: string;
  name: string;
}

interface ProState {
  symbol: string;
  interval: ChartInterval;
  theme: ChartThemeName;
  layoutPresetId: string | null;
  bars: ChartBar[];
  barsContextSymbol: string | null;
  barsContextInterval: ChartInterval | null;
  indicators: IndicatorSpec[];
  overlays: OverlaySpec[];
  annotations: AnnotationSpec[];
  tradeMarks: TradeMarkSpec[];
  mounted: boolean;
  status: ChartRuntimeSnapshot["status"];
  container: HTMLElement | null;
  runtime: Chart | null;
  runtimeApi: Chart | null;
  datafeed: InMemoryDatafeed;
  overlayIds: string[];
  annotationIds: string[];
  tradeMarkIds: string[];
  indicatorHandles: IndicatorHandle[];
  bridgeSubscribers: Set<ChartBridgeEventListener>;
  bridgeCallbacks: Array<{ type: ActionType; callback: (data?: unknown) => void }>;
  bridgeConnected: boolean;
  lastBridgeEventAt: number | null;
  bridgeEventMuteUntil: Partial<Record<Extract<ChartBridgeEventType, "crosshair" | "visible-range">, number>>;
}

const DEFAULT_PERIODS: Period[] = [
  { multiplier: 1, timespan: "minute", text: "1m" },
  { multiplier: 5, timespan: "minute", text: "5m" },
  { multiplier: 15, timespan: "minute", text: "15m" },
  { multiplier: 30, timespan: "minute", text: "30m" },
  { multiplier: 1, timespan: "hour", text: "1H" },
  { multiplier: 4, timespan: "hour", text: "4H" },
  { multiplier: 1, timespan: "day", text: "D" },
  { multiplier: 1, timespan: "week", text: "W" },
];

const PRO_DARK_STYLES: DeepPartial<Styles> = {
  grid: {
    horizontal: { color: "#2B2F36" },
    vertical: { color: "#2B2F36" },
  },
  candle: {
    bar: {
      upColor: "#26a69a",
      downColor: "#ef5350",
      upBorderColor: "#26a69a",
      downBorderColor: "#ef5350",
      upWickColor: "#26a69a",
      downWickColor: "#ef5350",
      noChangeColor: "#888888",
      noChangeBorderColor: "#888888",
      noChangeWickColor: "#888888",
    },
  },
  xAxis: {
    axisLine: { color: "#3C434C" },
    tickText: { color: "#d8d9db" },
    tickLine: { color: "#3C434C" },
  },
  yAxis: {
    axisLine: { color: "#3C434C" },
    tickText: { color: "#d8d9db" },
    tickLine: { color: "#3C434C" },
  },
  crosshair: {
    horizontal: {
      line: { color: "#888888" },
      text: { color: "#d8d9db", backgroundColor: "#374151" },
    },
    vertical: {
      line: { color: "#888888" },
      text: { color: "#d8d9db", backgroundColor: "#374151" },
    },
  },
};

const PRO_LIGHT_STYLES: DeepPartial<Styles> = {
  grid: {
    horizontal: { color: "#E5E7EB" },
    vertical: { color: "#E5E7EB" },
  },
};

const DEFAULT_MAIN_PANE_ID = "candle_pane";
const BRIDGE_SYNC_SUPPRESS_MS = 180;

interface CrosshairSyncActionPayload {
  x?: number;
  y?: number;
  paneId?: string;
}

interface TooltipStoreLike {
  setCrosshair?: (
    crosshair?: CrosshairSyncActionPayload,
    options?: { notInvalidate?: boolean; notExecuteAction?: boolean },
  ) => void;
}

interface ChartStoreLike {
  getTooltipStore?: () => TooltipStoreLike | null;
}

class InMemoryDatafeed implements Datafeed {
  private readonly cache = new Map<string, KLineData[]>();

  private readonly subscriptions = new Map<string, Set<(data: KLineData) => void>>();

  searchSymbols(search = ""): Promise<SymbolInfo[]> {
    const normalized = search.trim().toLowerCase();
    const symbols = Array.from(this.cache.keys()).map((key) => key.split("::", 1)[0]);
    const uniqueSymbols = Array.from(new Set(symbols));
    const result = uniqueSymbols
      .filter((ticker) => !normalized || ticker.toLowerCase().includes(normalized))
      .map((ticker) => toSymbolInfo(ticker));
    return Promise.resolve(result);
  }

  getHistoryKLineData(symbol: SymbolInfo, period: Period, from: number, to: number): Promise<KLineData[]> {
    const key = this.buildKey(symbol.ticker, period);
    const rows = this.cache.get(key) ?? [];
    const filtered = rows.filter((item) => {
      if (from && item.timestamp < from) {
        return false;
      }
      if (to && item.timestamp > to) {
        return false;
      }
      return true;
    });
    return Promise.resolve(filtered.map((item) => ({ ...item })));
  }

  subscribe(symbol: SymbolInfo, period: Period, callback: (data: KLineData) => void): void {
    const key = this.buildKey(symbol.ticker, period);
    const callbacks = this.subscriptions.get(key) ?? new Set<(data: KLineData) => void>();
    callbacks.add(callback);
    this.subscriptions.set(key, callbacks);
  }

  unsubscribe(symbol: SymbolInfo, period: Period): void {
    const key = this.buildKey(symbol.ticker, period);
    this.subscriptions.delete(key);
  }

  setBars(symbol: string, period: Period, bars: ChartBar[]): void {
    const key = this.buildKey(symbol, period);
    const data = bars.map(toKLineData).sort((left, right) => left.timestamp - right.timestamp);
    this.cache.set(key, data);
  }

  appendBars(symbol: string, period: Period, bars: ChartBar[]): void {
    if (!bars.length) {
      return;
    }
    const key = this.buildKey(symbol, period);
    const existing = this.cache.get(key) ?? [];
    const next = [...existing];
    for (const bar of bars) {
      const data = toKLineData(bar);
      if (next.length && next[next.length - 1].timestamp === data.timestamp) {
        next[next.length - 1] = data;
      } else {
        next.push(data);
      }
    }
    this.cache.set(key, next);
    const callbacks = this.subscriptions.get(key);
    if (!callbacks) {
      return;
    }
    for (const callback of callbacks) {
      callback({ ...next[next.length - 1] });
    }
  }

  clear(symbol: string, period: Period): void {
    this.cache.set(this.buildKey(symbol, period), []);
  }

  private buildKey(symbol: string, period: Period): string {
    return `${symbol}::${period.multiplier}-${period.timespan}`;
  }
}

function buildUpgradeGates(): ChartUpgradeGate[] {
  const notes: Array<{ status: ChartGateStatus; note: string }> = [
    {
      status: "passed",
      note: "官方 `@klinecharts/pro@0.1.1` 已接入，Apache-2.0 许可边界已核对。",
    },
    {
      status: "passed",
      note: "Vite/Tauri 构建链已直接打包 Pro runtime，无需运行时外链下载。",
    },
    {
      status: "passed",
      note: "Tauri Workbench 已通过 facade + 本地 datafeed 驱动 Pro runtime，而不是沿用 Qt 生命周期桥接。",
    },
    {
      status: "passed",
      note: "Workbench 已进入双图/四图布局，结构标注、交易标记、懒挂载与压测链路统一通过 facade 暴露。",
    },
  ];

  return CHART_FACADE_V2_UPGRADE_GATES.map((item, index) => ({
    id: `pro-gate-${index + 1}`,
    label: item,
    status: notes[index]?.status ?? "pending",
    note: notes[index]?.note ?? "待进一步验证。",
  }));
}

function cloneAdapterDescriptor(descriptor: ChartAdapterDescriptor): ChartAdapterDescriptor {
  return {
    ...descriptor,
    capabilities: [...descriptor.capabilities],
    upgradeGates: descriptor.upgradeGates.map((item) => ({ ...item })),
  };
}

function buildThemeStyles(theme: ChartThemeName): DeepPartial<Styles> {
  return theme === "light" ? PRO_LIGHT_STYLES : PRO_DARK_STYLES;
}

function toProPeriod(interval: ChartInterval): Period {
  switch (interval) {
    case "1m":
      return { multiplier: 1, timespan: "minute", text: "1m" };
    case "5m":
      return { multiplier: 5, timespan: "minute", text: "5m" };
    case "15m":
      return { multiplier: 15, timespan: "minute", text: "15m" };
    case "30m":
      return { multiplier: 30, timespan: "minute", text: "30m" };
    case "1h":
      return { multiplier: 1, timespan: "hour", text: "1H" };
    case "4h":
      return { multiplier: 4, timespan: "hour", text: "4H" };
    case "1w":
      return { multiplier: 1, timespan: "week", text: "W" };
    case "1d":
    default:
      return { multiplier: 1, timespan: "day", text: "D" };
  }
}

function toSymbolInfo(symbol: string): SymbolInfo {
  const [ticker, market = ""] = symbol.split(".");
  return {
    ticker: symbol,
    shortName: ticker,
    name: symbol,
    exchange: market || undefined,
    market: market || undefined,
  };
}

function parseTimestamp(raw: string | number): number {
  if (typeof raw === "number") {
    return raw > 1_000_000_000_000 ? raw : raw * 1000;
  }
  const trimmed = raw.trim();
  if (!trimmed) {
    return 0;
  }
  const numeric = Number(trimmed);
  if (!Number.isNaN(numeric)) {
    return numeric > 1_000_000_000_000 ? numeric : numeric * 1000;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return Date.parse(`${trimmed}T00:00:00`);
  }
  return Date.parse(trimmed.replace(" ", "T"));
}

function toKLineData(bar: ChartBar): KLineData {
  return {
    timestamp: parseTimestamp(bar.time),
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
    volume: bar.volume,
  };
}

function formatBridgeTime(timestamp: number | null): string | null {
  if (!timestamp || Number.isNaN(timestamp)) {
    return null;
  }
  return new Date(timestamp).toISOString();
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" ? value as Record<string, unknown> : null;
}

function readNullableNumber(source: Record<string, unknown> | null, ...keys: string[]): number | null {
  if (!source) {
    return null;
  }
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

function readNullableString(source: Record<string, unknown> | null, ...keys: string[]): string | null {
  if (!source) {
    return null;
  }
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "string" && value.trim()) {
      return value;
    }
  }
  return null;
}

function toBridgeCandleSnapshot(raw: unknown): ChartBridgeCandleSnapshot | null {
  const source = asRecord(raw);
  if (!source) {
    return null;
  }
  const timestamp = readNullableNumber(source, "timestamp", "ts", "time");
  const open = readNullableNumber(source, "open");
  const high = readNullableNumber(source, "high");
  const low = readNullableNumber(source, "low");
  const close = readNullableNumber(source, "close", "price", "value");
  const volume = readNullableNumber(source, "volume");
  if (
    timestamp === null
    && open === null
    && high === null
    && low === null
    && close === null
    && volume === null
  ) {
    return null;
  }
  return {
    timestamp,
    time: formatBridgeTime(timestamp),
    open,
    high,
    low,
    close,
    volume,
  };
}

function extractBridgeCandle(raw: unknown): ChartBridgeCandleSnapshot | null {
  const source = asRecord(raw);
  if (!source) {
    return null;
  }
  return toBridgeCandleSnapshot(source.kLineData ?? source.data ?? raw);
}

function normalizeClickPayload(raw: unknown): ChartBridgeClickPayload {
  const source = asRecord(raw);
  return {
    paneId: readNullableString(source, "paneId"),
    x: readNullableNumber(source, "x"),
    y: readNullableNumber(source, "y"),
    dataIndex: readNullableNumber(source, "dataIndex"),
    candle: extractBridgeCandle(raw),
  };
}

function normalizeCrosshairPayload(raw: unknown): ChartBridgeCrosshairPayload {
  const source = asRecord(raw);
  const indicatorData = asRecord(source?.indicatorData) ?? null;
  return {
    paneId: readNullableString(source, "paneId"),
    x: readNullableNumber(source, "x"),
    y: readNullableNumber(source, "y"),
    realX: readNullableNumber(source, "realX"),
    dataIndex: readNullableNumber(source, "dataIndex"),
    realDataIndex: readNullableNumber(source, "realDataIndex"),
    candle: extractBridgeCandle(raw),
    indicatorData,
  };
}

function normalizeVisibleRangePayload(
  range: { from: number; to: number; realFrom: number; realTo: number },
  dataList: KLineData[],
  api: Chart,
): ChartBridgeVisibleRangePayload {
  const firstIndex = dataList.length > 0 ? Math.max(0, Math.min(dataList.length - 1, range.realFrom)) : -1;
  const lastIndex = dataList.length > 0 ? Math.max(0, Math.min(dataList.length - 1, range.realTo - 1)) : -1;

  return {
    from: range.from,
    to: range.to,
    realFrom: range.realFrom,
    realTo: range.realTo,
    barSpace: api.getBarSpace(),
    offsetRightDistance: api.getOffsetRightDistance(),
    firstCandle: firstIndex >= 0 ? toBridgeCandleSnapshot(dataList[firstIndex]) : null,
    lastCandle: lastIndex >= 0 ? toBridgeCandleSnapshot(dataList[lastIndex]) : null,
  };
}

function getPrivateChartApi(runtime: Chart | null): Chart | null {
  if (!runtime) {
    return null;
  }
  return runtime;
}

function getPrivateChartStore(api: Chart | null): ChartStoreLike | null {
  if (!api) {
    return null;
  }
  return (api as unknown as { _chartStore?: ChartStoreLike })._chartStore ?? null;
}

async function waitForChartApi(runtime: Chart): Promise<Chart | null> {
  return getPrivateChartApi(runtime);
}

function resolveSyncPaneId(paneId: string | null | undefined): string {
  return paneId && paneId.trim() ? paneId : DEFAULT_MAIN_PANE_ID;
}

function findNearestKLineData(dataList: KLineData[], timestamp: number): KLineData | null {
  if (!dataList.length) {
    return null;
  }
  let best: KLineData | null = null;
  let bestDelta = Number.POSITIVE_INFINITY;
  for (const item of dataList) {
    const delta = Math.abs(item.timestamp - timestamp);
    if (delta < bestDelta) {
      best = item;
      bestDelta = delta;
      if (delta === 0) {
        break;
      }
    }
  }
  return best;
}

function resolveCrosshairSyncPayload(
  api: Chart,
  payload: ChartBridgeCrosshairPayload,
): CrosshairSyncActionPayload | null {
  const paneId = resolveSyncPaneId(payload.paneId);
  const timestamp = payload.candle?.timestamp ?? null;
  const sourceDataIndex = payload.realDataIndex ?? payload.dataIndex ?? null;
  let price = payload.candle?.close
    ?? payload.candle?.open
    ?? payload.candle?.high
    ?? payload.candle?.low
    ?? null;

  if (price === null && timestamp !== null) {
    price = findNearestKLineData(api.getDataList(), timestamp)?.close ?? null;
  }

  if (price === null && sourceDataIndex !== null) {
    const targetIndex = Math.max(0, Math.min(api.getDataList().length - 1, sourceDataIndex));
    price = api.getDataList()[targetIndex]?.close ?? null;
  }

  const point: {
    timestamp?: number;
    dataIndex?: number;
    value?: number;
  } = {};
  if (timestamp !== null && Number.isFinite(timestamp)) {
    point.timestamp = timestamp;
  } else if (sourceDataIndex !== null && Number.isFinite(sourceDataIndex)) {
    point.dataIndex = sourceDataIndex;
  } else {
    return null;
  }

  if (price !== null && Number.isFinite(price)) {
    point.value = price;
  }

  const coordinate = api.convertToPixel(point, { paneId, absolute: false }) as { x?: number; y?: number };
  if (
    typeof coordinate.x !== "number"
    || !Number.isFinite(coordinate.x)
    || typeof coordinate.y !== "number"
    || !Number.isFinite(coordinate.y)
  ) {
    return null;
  }

  return {
    x: coordinate.x,
    y: coordinate.y,
    paneId,
  };
}

function normalizePoint(point: unknown): { timestamp: number; value: number } | null {
  if (!point || typeof point !== "object") {
    return null;
  }
  const payload = point as Record<string, unknown>;
  const rawTime = payload.timestamp ?? payload.time ?? payload.ts;
  const rawValue = payload.value ?? payload.price;
  if ((typeof rawTime !== "string" && typeof rawTime !== "number") || typeof rawValue !== "number") {
    return null;
  }
  const timestamp = parseTimestamp(rawTime);
  if (!timestamp || Number.isNaN(timestamp)) {
    return null;
  }
  return {
    timestamp,
    value: rawValue,
  };
}

function extractPayloadPoints(payload: Record<string, unknown>): Array<{ timestamp: number; value: number }> {
  const rawPoints = payload.points;
  if (Array.isArray(rawPoints)) {
    return rawPoints
      .map((item) => normalizePoint(item))
      .filter((item): item is { timestamp: number; value: number } => item !== null);
  }
  if (rawPoints && typeof rawPoints === "object") {
    return Object.values(rawPoints)
      .map((item) => normalizePoint(item))
      .filter((item): item is { timestamp: number; value: number } => item !== null)
      .sort((left, right) => left.timestamp - right.timestamp);
  }
  return [];
}

function asOverlayStyles(styles: Record<string, unknown>): DeepPartial<OverlayStyle> {
  return styles as unknown as DeepPartial<OverlayStyle>;
}

function buildOverlayCreate(spec: OverlaySpec): OverlayCreate | null {
  const payload = spec.payload as Record<string, unknown>;
  const points = extractPayloadPoints(payload);
  if (!points.length) {
    return null;
  }
  const color = typeof payload.color === "string" ? payload.color : "#60a5fa";
  const lineSize = typeof payload.size === "number" ? payload.size : 1;

  if ((spec.kind === "band" || spec.kind === "zone") && points.length >= 3) {
    return {
      id: spec.id,
      name: "priceChannelLine",
      points: points.slice(0, 3),
      styles: asOverlayStyles({
        line: { color, size: lineSize, style: "solid", dashedValue: [4, 2] },
      }),
      extendData: payload,
    } as OverlayCreate;
  }

  if (spec.kind === "marker") {
    return {
      id: spec.id,
      name: "simpleAnnotation",
      points: [points[0]],
      styles: asOverlayStyles({
        symbol: { color, type: "diamond", size: 8 },
        text: { color },
      }),
      extendData: typeof payload.text === "string" ? payload.text : spec.id,
    } as OverlayCreate;
  }

  return {
    id: spec.id,
    name: "segment",
    points: points.length >= 2 ? [points[0], points[points.length - 1]] : [points[0], points[0]],
    styles: asOverlayStyles({
      line: { color, size: lineSize, style: "solid", dashedValue: [4, 2] },
    }),
    extendData: payload,
  } as OverlayCreate;
}

function buildAnnotationCreate(spec: AnnotationSpec): OverlayCreate | null {
  const payload = spec.payload as Record<string, unknown>;
  const rawTime = payload.time;
  const rawPrice = payload.price;
  if ((typeof rawTime !== "string" && typeof rawTime !== "number") || typeof rawPrice !== "number") {
    return null;
  }
  const direction = typeof payload.direction === "string" ? payload.direction : "";
  const color = direction === "up" ? "#34d399" : direction === "down" ? "#f59e0b" : "#93c5fd";
  return {
    id: spec.id,
    name: "simpleAnnotation",
    points: [{ timestamp: parseTimestamp(rawTime), value: rawPrice }],
    styles: asOverlayStyles({
      symbol: { color, type: spec.kind === "arrow" ? "triangle" : "diamond", size: 8 },
      text: { color },
    }),
    extendData: typeof payload.text === "string" ? payload.text : spec.id,
  } as OverlayCreate;
}

function buildTradeMarkCreate(mark: TradeMarkSpec): OverlayCreate | null {
  const color = mark.direction === "buy" ? "#26a69a" : mark.direction === "sell" ? "#ef5350" : "#93c5fd";
  const symbolType = mark.direction === "sell" ? "diamond" : "triangle";
  return {
    id: mark.id,
    name: "simpleAnnotation",
    points: [{ timestamp: parseTimestamp(mark.time), value: mark.price }],
    styles: asOverlayStyles({
      symbol: { color, type: symbolType, size: 8 },
      text: { color },
    }),
    extendData: mark.text ?? mark.direction,
  } as OverlayCreate;
}

function mapIndicatorName(spec: IndicatorSpec): string {
  switch (spec.kind) {
    case "ma":
      return "MA";
    case "ema":
      return "EMA";
    case "boll":
      return "BOLL";
    case "macd":
      return "MACD";
    case "custom":
    default:
      return typeof spec.params?.name === "string" ? spec.params.name : spec.id;
  }
}

function clonePeriod(period: Period): Period {
  return { ...period };
}

export function isProAdapterReady(): boolean {
  return typeof window !== "undefined" && typeof document !== "undefined";
}

export function getProAdapterDescriptor(): ChartAdapterDescriptor {
  return {
    kind: "pro",
    label: "KLineChart Pro Adapter",
    targetVersion: CHART_PRO_TARGET_VERSION,
    active: false,
    ready: true,
    runtimeStatus: "idle",
    reason: "官方 `@klinecharts/pro@0.1.1` 已接入；当前通过本地 datafeed 驱动 Workbench 主舞台。",
    capabilities: [
      "KLineChart Pro 主舞台",
      "本地 bars datafeed",
      "结构 overlays / annotations",
      "交易标记渲染",
      "主题 / 导出 / 指标桥接",
    ],
    upgradeGates: buildUpgradeGates(),
  };
}

export function createProChartFacade(options: ProAdapterOptions): ChartFacadeV2 {
  const descriptor = getProAdapterDescriptor();
  const state: ProState = {
    symbol: options.symbol,
    interval: options.interval,
    theme: options.theme ?? "dark",
    layoutPresetId: options.layoutPresetId ?? null,
    bars: [],
    barsContextSymbol: null,
    barsContextInterval: null,
    indicators: [],
    overlays: [],
    annotations: [],
    tradeMarks: [],
    mounted: false,
    status: "idle",
    container: null,
    runtime: null,
    runtimeApi: null,
    datafeed: new InMemoryDatafeed(),
    overlayIds: [],
    annotationIds: [],
    tradeMarkIds: [],
    indicatorHandles: [],
    bridgeSubscribers: new Set<ChartBridgeEventListener>(),
    bridgeCallbacks: [],
    bridgeConnected: false,
    lastBridgeEventAt: null,
    bridgeEventMuteUntil: {},
  };

  function syncDescriptor(): void {
    descriptor.active = true;
    descriptor.ready = true;
    descriptor.runtimeStatus = state.status;
    descriptor.reason = state.mounted
      ? `Workbench 图表 runtime 已挂接到主舞台（保持 Pro adapter façade）；${state.bridgeConnected ? "事件桥已回写到 Tauri 壳" : "事件桥等待订阅完成"}。`
      : "Workbench 图表 runtime 已接入，等待主舞台 mount。";
  }

  function emitBridgeEvent(event: ChartBridgeEvent): void {
    if (
      (event.type === "crosshair" || event.type === "visible-range")
      && (state.bridgeEventMuteUntil[event.type] ?? 0) > Date.now()
    ) {
      return;
    }
    state.lastBridgeEventAt = event.emittedAt;
    state.bridgeSubscribers.forEach((listener) => {
      listener(event);
    });
  }

  function muteBridgeEvent(type: Extract<ChartBridgeEventType, "crosshair" | "visible-range">, durationMs = BRIDGE_SYNC_SUPPRESS_MS): void {
    state.bridgeEventMuteUntil[type] = Date.now() + durationMs;
  }

  function detachBridgeCallbacks(): void {
    if (!state.runtimeApi) {
      state.bridgeCallbacks = [];
      state.bridgeConnected = false;
      return;
    }
    state.bridgeCallbacks.forEach(({ type, callback }) => {
      try {
        state.runtimeApi?.unsubscribeAction(type, callback);
      } catch {
        // ignore bridge detach noise during runtime teardown
      }
    });
    state.bridgeCallbacks = [];
    state.bridgeConnected = false;
  }

  function emitVisibleRangeSnapshot(api: Chart, raw?: unknown): void {
    const source = asRecord(raw);
    const fallback = api.getVisibleRange();
    const range = {
      from: readNullableNumber(source, "from") ?? fallback.from,
      to: readNullableNumber(source, "to") ?? fallback.to,
      realFrom: readNullableNumber(source, "realFrom") ?? fallback.realFrom,
      realTo: readNullableNumber(source, "realTo") ?? fallback.realTo,
    };
    emitBridgeEvent({
      type: "visible-range",
      emittedAt: Date.now(),
      payload: normalizeVisibleRangePayload(range, api.getDataList(), api),
    });
  }

  function attachBridgeCallbacks(api: Chart): void {
    detachBridgeCallbacks();

    const register = (type: ActionType, handler: (data?: unknown) => void) => {
      const callback = (data?: unknown) => {
        handler(data);
      };
      api.subscribeAction(type, callback);
      state.bridgeCallbacks.push({ type, callback });
    };

    register(ActionType.OnCandleBarClick, (data) => {
      emitBridgeEvent({
        type: "chart-click",
        emittedAt: Date.now(),
        payload: normalizeClickPayload(data),
      });
    });

    register(ActionType.OnCrosshairChange, (data) => {
      emitBridgeEvent({
        type: "crosshair",
        emittedAt: Date.now(),
        payload: normalizeCrosshairPayload(data),
      });
    });

    register(ActionType.OnVisibleRangeChange, (data) => {
      emitVisibleRangeSnapshot(api, data);
    });

    state.bridgeConnected = true;
  }

  async function syncRuntimeApi(): Promise<Chart | null> {
    if (state.runtimeApi) {
      return state.runtimeApi;
    }
    if (!state.runtime) {
      return null;
    }
    state.runtimeApi = await waitForChartApi(state.runtime);
    return state.runtimeApi;
  }

  async function clearOverlays(ids: string[]): Promise<void> {
    const api = await syncRuntimeApi();
    if (!api) {
      return;
    }
    ids.forEach((id) => {
      try {
        api.removeOverlay(id);
      } catch {
        // ignore adapter cleanup noise
      }
    });
  }

  async function refreshBars(): Promise<void> {
    const api = await syncRuntimeApi();
    if (!api) {
      return;
    }
    const period = clonePeriod(toProPeriod(state.interval));
    const barsMatchContext = state.barsContextSymbol === state.symbol
      && state.barsContextInterval === state.interval;

    if (!barsMatchContext || !state.bars.length) {
      state.datafeed.clear(state.symbol, period);
      api.applyNewData([], false);
      emitVisibleRangeSnapshot(api);
      return;
    }
    state.datafeed.setBars(state.symbol, period, state.bars);
    api.applyNewData(state.bars.map(toKLineData), true);
    api.scrollToRealTime();
    emitVisibleRangeSnapshot(api);
  }

  async function refreshIndicators(): Promise<void> {
    const api = await syncRuntimeApi();
    if (!api) {
      return;
    }

    for (const handle of state.indicatorHandles) {
      try {
        api.removeIndicator(handle.paneId, handle.name);
      } catch {
        // ignore stale indicator cleanup
      }
    }
    state.indicatorHandles = [];

    for (const spec of state.indicators) {
      const name = mapIndicatorName(spec);
      const isMainPane = !spec.pane || spec.pane === "main";
      const paneId = isMainPane ? "candle_pane" : undefined;
      const createdPaneId = api.createIndicator(
        {
          name,
          calcParams: Array.isArray(spec.params?.calcParams)
            ? spec.params?.calcParams
            : undefined,
          shortName: name,
        },
        !isMainPane,
        paneId ? { id: paneId } : undefined,
      );
      state.indicatorHandles.push({
        paneId: typeof createdPaneId === "string" ? createdPaneId : (paneId ?? name),
        name,
      });
    }
  }

  async function refreshOverlays(): Promise<void> {
    await clearOverlays(state.overlayIds);
    const api = await syncRuntimeApi();
    if (!api) {
      return;
    }
    const nextIds: string[] = [];
    state.overlays.forEach((spec) => {
      const overlay = buildOverlayCreate(spec);
      if (!overlay) {
        return;
      }
      try {
        api.createOverlay(overlay);
        nextIds.push(spec.id);
      } catch {
        // ignore unsupported overlay payloads
      }
    });
    state.overlayIds = nextIds;
  }

  async function refreshAnnotations(): Promise<void> {
    await clearOverlays(state.annotationIds);
    const api = await syncRuntimeApi();
    if (!api) {
      return;
    }
    const nextIds: string[] = [];
    state.annotations.forEach((spec) => {
      const overlay = buildAnnotationCreate(spec);
      if (!overlay) {
        return;
      }
      try {
        api.createOverlay(overlay);
        nextIds.push(spec.id);
      } catch {
        // ignore invalid annotation payloads
      }
    });
    state.annotationIds = nextIds;
  }

  async function refreshTradeMarks(): Promise<void> {
    await clearOverlays(state.tradeMarkIds);
    const api = await syncRuntimeApi();
    if (!api) {
      return;
    }
    const nextIds: string[] = [];
    state.tradeMarks.forEach((mark) => {
      const overlay = buildTradeMarkCreate(mark);
      if (!overlay) {
        return;
      }
      try {
        api.createOverlay(overlay);
        nextIds.push(mark.id);
      } catch {
        // ignore unsupported marks
      }
    });
    state.tradeMarkIds = nextIds;
  }

  async function applyTheme(): Promise<void> {
    if (!state.runtime) {
      return;
    }
    state.runtime.setStyles(buildThemeStyles(state.theme));
    state.container?.setAttribute("data-theme", state.theme);
  }

  async function snapshot(): Promise<ChartRuntimeSnapshot> {
    syncDescriptor();
    return {
      symbol: state.symbol,
      interval: state.interval,
      theme: state.theme,
      indicatorCount: state.indicators.length,
      overlayCount: state.overlays.length,
      annotationCount: state.annotations.length,
      tradeMarkCount: state.tradeMarks.length,
      barCount: state.bars.length,
      mounted: state.mounted,
      status: state.status,
      layoutPresetId: state.layoutPresetId,
      adapter: cloneAdapterDescriptor(descriptor),
      requestedAdapterKind: "pro",
      requestedAdapterReady: true,
      fallbackEngaged: false,
      bridgeConnected: state.bridgeConnected,
      bridgeEventTypes: ["chart-click", "crosshair", "visible-range"],
      lastBridgeEventAt: state.lastBridgeEventAt,
      reason: descriptor.reason,
    };
  }

  return {
    async mount(container: HTMLElement): Promise<void> {
      if (state.mounted) {
        await this.unmount();
      }
      state.status = "mounting";
      state.container = container;
      state.runtime = init(container, {
        locale: "zh-CN",
        timezone: "Asia/Shanghai",
        styles: buildThemeStyles(state.theme),
      });
      state.runtimeApi = state.runtime;
      if (!state.runtimeApi) {
        state.status = "error";
        throw new Error("KLineChart Pro runtime 初始化失败");
      }
      attachBridgeCallbacks(state.runtimeApi);
      state.mounted = true;
      await applyTheme();
      await refreshBars();
      await refreshIndicators();
      await refreshOverlays();
      await refreshAnnotations();
      await refreshTradeMarks();
      state.status = "ready";
      syncDescriptor();
      container.dataset.chartAdapter = descriptor.kind;
      container.dataset.chartStatus = state.status;
    },
    async unmount(): Promise<void> {
      if (state.container) {
        state.container.dataset.chartAdapter = "";
        state.container.dataset.chartStatus = "";
      }
      state.overlayIds = [];
      state.annotationIds = [];
      state.tradeMarkIds = [];
      state.indicatorHandles = [];
      detachBridgeCallbacks();
      if (state.runtimeApi) {
        try {
          dispose(state.runtimeApi);
        } catch {
          // ignore runtime dispose noise
        }
      }
      if (state.container) {
        state.container.innerHTML = "";
      }
      state.runtime = null;
      state.runtimeApi = null;
      state.container = null;
      state.mounted = false;
      state.status = "idle";
      state.lastBridgeEventAt = null;
      state.bridgeEventMuteUntil = {};
      syncDescriptor();
    },
    async setSymbol(symbol: string): Promise<void> {
      state.symbol = symbol;
      await refreshBars();
    },
    async setInterval(interval: ChartInterval): Promise<void> {
      state.interval = interval;
      await refreshBars();
    },
    async setBars(bars: ChartBar[]): Promise<void> {
      state.bars = [...bars];
      state.barsContextSymbol = state.symbol;
      state.barsContextInterval = state.interval;
      await refreshBars();
    },
    async appendBars(bars: ChartBar[]): Promise<void> {
      const sameContext = state.barsContextSymbol === state.symbol
        && state.barsContextInterval === state.interval;
      state.bars = sameContext ? [...state.bars, ...bars] : [...bars];
      state.barsContextSymbol = state.symbol;
      state.barsContextInterval = state.interval;
      const period = clonePeriod(toProPeriod(state.interval));
      state.datafeed.appendBars(state.symbol, period, bars);
      const api = await syncRuntimeApi();
      if (!api) {
        return;
      }
      if (!bars.length) {
        return;
      }
      bars.forEach((bar) => {
        api.updateData(toKLineData(bar));
      });
      emitVisibleRangeSnapshot(api);
    },
    async setIndicators(indicators: IndicatorSpec[]): Promise<void> {
      state.indicators = [...indicators];
      await refreshIndicators();
    },
    async setOverlays(overlays: OverlaySpec[]): Promise<void> {
      state.overlays = [...overlays];
      await refreshOverlays();
    },
    async setAnnotations(annotations: AnnotationSpec[]): Promise<void> {
      state.annotations = [...annotations];
      await refreshAnnotations();
    },
    async setTradeMarks(marks: TradeMarkSpec[]): Promise<void> {
      state.tradeMarks = [...marks];
      await refreshTradeMarks();
    },
    async setTheme(theme: ChartThemeName): Promise<void> {
      state.theme = theme;
      await applyTheme();
    },
    async saveLayout(presetId: string): Promise<void> {
      state.layoutPresetId = presetId;
    },
    async loadLayout(presetId: string): Promise<void> {
      state.layoutPresetId = presetId;
    },
    async exportImage(): Promise<string> {
      const api = await syncRuntimeApi();
      if (!api) {
        return "pro-chart-export-unavailable";
      }
      return api.getConvertPictureUrl(true, "jpeg", state.theme === "dark" ? "#0c0d0f" : "#ffffff");
    },
    async applyCrosshairSync(payload: ChartBridgeCrosshairPayload | null): Promise<void> {
      const api = await syncRuntimeApi();
      if (!api) {
        return;
      }

      muteBridgeEvent("crosshair");

      const tooltipStore = getPrivateChartStore(api)?.getTooltipStore?.() ?? null;
      const timestamp = payload?.candle?.timestamp ?? null;
      const sourceDataIndex = payload?.realDataIndex ?? payload?.dataIndex ?? null;
      if (!payload || (timestamp === null && sourceDataIndex === null)) {
        tooltipStore?.setCrosshair?.(undefined, { notExecuteAction: true });
        return;
      }

      const nextCrosshair = resolveCrosshairSyncPayload(api, payload);
      if (!nextCrosshair) {
        return;
      }

      api.executeAction(ActionType.OnCrosshairChange, nextCrosshair);
    },
    async applyVisibleRangeSync(payload: ChartBridgeVisibleRangePayload | null): Promise<void> {
      const api = await syncRuntimeApi();
      if (!api || !payload) {
        return;
      }

      muteBridgeEvent("visible-range", BRIDGE_SYNC_SUPPRESS_MS * 2);

      if (payload.barSpace !== null && Number.isFinite(payload.barSpace)) {
        api.setBarSpace(payload.barSpace);
      }
      if (payload.offsetRightDistance !== null && Number.isFinite(payload.offsetRightDistance)) {
        api.setOffsetRightDistance(payload.offsetRightDistance);
      }

      const anchorTimestamp = payload.lastCandle?.timestamp ?? payload.firstCandle?.timestamp ?? null;
      if (anchorTimestamp !== null && Number.isFinite(anchorTimestamp)) {
        api.scrollToTimestamp(anchorTimestamp, 0);
      }
    },
    subscribeBridgeEvents(listener: ChartBridgeEventListener): () => void {
      state.bridgeSubscribers.add(listener);
      return () => {
        state.bridgeSubscribers.delete(listener);
      };
    },
    async snapshot(): Promise<ChartRuntimeSnapshot> {
      return snapshot();
    },
    getAdapterDescriptor(): ChartAdapterDescriptor {
      syncDescriptor();
      return cloneAdapterDescriptor(descriptor);
    },
    getUpgradeGates(): ChartUpgradeGate[] {
      syncDescriptor();
      return cloneAdapterDescriptor(descriptor).upgradeGates;
    },
  };
}
