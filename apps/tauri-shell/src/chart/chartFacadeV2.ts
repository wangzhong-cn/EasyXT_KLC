export const CHART_FACADE_V2_DRAFT_VERSION = "0.3.0-stage-runtime";
export const CHART_FACADE_V2_TARGET_RUNTIME = "KLineChart Pro 0.1.1";

export type ChartThemeName = "dark" | "light";
export type ChartInterval =
  | "1m"
  | "5m"
  | "15m"
  | "30m"
  | "1h"
  | "4h"
  | "1d"
  | "1w";

export type ChartAdapterKind = "pro" | "fallback";
export type ChartRuntimeStatus = "idle" | "mounting" | "ready" | "degraded" | "error";
export type ChartGateStatus = "pending" | "passed" | "blocked";

export interface ChartBar {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface IndicatorSpec {
  id: string;
  kind: "ma" | "ema" | "boll" | "macd" | "custom";
  pane?: "main" | "bottom" | string;
  params?: Record<string, number | string | boolean>;
}

export interface OverlaySpec {
  id: string;
  kind: "line" | "band" | "marker" | "trade" | "zone";
  payload: Record<string, unknown>;
}

export interface AnnotationSpec {
  id: string;
  kind: "label" | "arrow" | "note" | "structure";
  payload: Record<string, unknown>;
}

export interface TradeMarkSpec {
  id: string;
  direction: "buy" | "sell" | "flat";
  time: string;
  price: number;
  text?: string;
}

export type ChartBridgeEventType = "chart-click" | "crosshair" | "visible-range";

export interface ChartBridgeCandleSnapshot {
  timestamp: number | null;
  time: string | null;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume: number | null;
}

export interface ChartBridgeClickPayload {
  paneId: string | null;
  x: number | null;
  y: number | null;
  dataIndex: number | null;
  candle: ChartBridgeCandleSnapshot | null;
}

export interface ChartBridgeCrosshairPayload {
  paneId: string | null;
  x: number | null;
  y: number | null;
  realX: number | null;
  dataIndex: number | null;
  realDataIndex: number | null;
  candle: ChartBridgeCandleSnapshot | null;
  indicatorData: Record<string, unknown> | null;
}

export interface ChartBridgeVisibleRangePayload {
  from: number;
  to: number;
  realFrom: number;
  realTo: number;
  barSpace: number | null;
  offsetRightDistance: number | null;
  firstCandle: ChartBridgeCandleSnapshot | null;
  lastCandle: ChartBridgeCandleSnapshot | null;
}

export type ChartBridgeEvent =
  | {
    type: "chart-click";
    emittedAt: number;
    payload: ChartBridgeClickPayload;
  }
  | {
    type: "crosshair";
    emittedAt: number;
    payload: ChartBridgeCrosshairPayload;
  }
  | {
    type: "visible-range";
    emittedAt: number;
    payload: ChartBridgeVisibleRangePayload;
  };

export type ChartBridgeEventListener = (event: ChartBridgeEvent) => void;

export interface LayoutPreset {
  id: string;
  title: string;
  description: string;
  slotCount: 1 | 2 | 4;
}

export interface ChartUpgradeGate {
  id: string;
  label: string;
  status: ChartGateStatus;
  note: string;
}

export interface ChartAdapterDescriptor {
  kind: ChartAdapterKind;
  label: string;
  targetVersion: string | null;
  active: boolean;
  ready: boolean;
  runtimeStatus: ChartRuntimeStatus;
  reason: string | null;
  capabilities: string[];
  upgradeGates: ChartUpgradeGate[];
}

export interface ChartRuntimeSnapshot {
  symbol: string;
  interval: ChartInterval;
  theme: ChartThemeName;
  indicatorCount: number;
  overlayCount: number;
  annotationCount: number;
  tradeMarkCount: number;
  barCount: number;
  mounted: boolean;
  status: ChartRuntimeStatus;
  layoutPresetId: string | null;
  adapter: ChartAdapterDescriptor;
  requestedAdapterKind: ChartAdapterKind;
  requestedAdapterReady: boolean;
  fallbackEngaged: boolean;
  bridgeConnected: boolean;
  bridgeEventTypes: ChartBridgeEventType[];
  lastBridgeEventAt: number | null;
  reason: string | null;
}

export interface ChartFacadeV2 {
  mount(container: HTMLElement): Promise<void>;
  unmount(): Promise<void>;
  setSymbol(symbol: string): Promise<void>;
  setInterval(interval: ChartInterval): Promise<void>;
  setBars(bars: ChartBar[]): Promise<void>;
  appendBars(bars: ChartBar[]): Promise<void>;
  setIndicators(indicators: IndicatorSpec[]): Promise<void>;
  setOverlays(overlays: OverlaySpec[]): Promise<void>;
  setAnnotations(annotations: AnnotationSpec[]): Promise<void>;
  setTradeMarks(marks: TradeMarkSpec[]): Promise<void>;
  setTheme(theme: ChartThemeName): Promise<void>;
  saveLayout(presetId: string): Promise<void>;
  loadLayout(presetId: string): Promise<void>;
  exportImage(): Promise<string>;
  applyCrosshairSync(payload: ChartBridgeCrosshairPayload | null): Promise<void>;
  applyVisibleRangeSync(payload: ChartBridgeVisibleRangePayload | null): Promise<void>;
  subscribeBridgeEvents(listener: ChartBridgeEventListener): () => void;
  snapshot(): Promise<ChartRuntimeSnapshot>;
  getAdapterDescriptor(): ChartAdapterDescriptor;
  getUpgradeGates(): ChartUpgradeGate[];
}

export interface ChartFacadeFactoryOptions {
  preferredAdapter?: ChartAdapterKind;
  symbol: string;
  interval: ChartInterval;
  theme?: ChartThemeName;
  layoutPresetId?: string | null;
}

export interface ChartFacadeBundle {
  facade: ChartFacadeV2;
  activeAdapter: ChartAdapterDescriptor;
  requestedAdapter: ChartAdapterDescriptor;
  fallbackEngaged: boolean;
}

export const CHART_FACADE_V2_UPGRADE_GATES = [
  "KLineChart v9.8.12 与 Pro 0.1.1 的 API 差异必须通过 adapter 吸收",
  "离线打包能力必须可验证，不能依赖运行时下载图表扩展",
  "图表事件桥必须服务于 Tauri 壳，不再直接服务 Qt 生命周期",
  "多图布局、交易标注、结构注解必须通过统一 facade 暴露",
];
