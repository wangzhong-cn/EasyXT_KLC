import {
  type AnnotationSpec,
  type ChartAdapterDescriptor,
  type ChartBar,
  type ChartFacadeFactoryOptions,
  type ChartFacadeV2,
  type ChartRuntimeSnapshot,
  type IndicatorSpec,
  type OverlaySpec,
  type TradeMarkSpec,
} from "../chartFacadeV2";

interface FallbackAdapterOptions extends ChartFacadeFactoryOptions {
  requestedAdapter?: ChartAdapterDescriptor;
}

interface FallbackState {
  symbol: string;
  interval: ChartFacadeFactoryOptions["interval"];
  theme: NonNullable<ChartFacadeFactoryOptions["theme"]>;
  layoutPresetId: string | null;
  bars: ChartBar[];
  indicators: IndicatorSpec[];
  overlays: OverlaySpec[];
  annotations: AnnotationSpec[];
  tradeMarks: TradeMarkSpec[];
  mounted: boolean;
  status: ChartRuntimeSnapshot["status"];
  container: HTMLElement | null;
}

function cloneAdapterDescriptor(descriptor: ChartAdapterDescriptor): ChartAdapterDescriptor {
  return {
    ...descriptor,
    capabilities: [...descriptor.capabilities],
    upgradeGates: descriptor.upgradeGates.map((item) => ({ ...item })),
  };
}

export function createFallbackChartFacade(options: FallbackAdapterOptions): ChartFacadeV2 {
  const requestedAdapter = options.requestedAdapter ?? null;
  const fallbackEngaged = Boolean(requestedAdapter && !requestedAdapter.ready);

  const state: FallbackState = {
    symbol: options.symbol,
    interval: options.interval,
    theme: options.theme ?? "dark",
    layoutPresetId: options.layoutPresetId ?? null,
    bars: [],
    indicators: [],
    overlays: [],
    annotations: [],
    tradeMarks: [],
    mounted: false,
    status: fallbackEngaged ? "degraded" : "idle",
    container: null,
  };

  const descriptor: ChartAdapterDescriptor = {
    kind: "fallback",
    label: "Fallback Shell Adapter",
    targetVersion: null,
    active: true,
    ready: true,
    runtimeStatus: state.status,
    reason: fallbackEngaged
      ? `未检测到 ${requestedAdapter?.label}，当前先使用 fallback shell adapter 保持工作台主舞台可运行。`
      : "使用 fallback shell adapter 承接 facade 主舞台。",
    capabilities: [
      "主舞台容器挂载",
      "symbol / interval / theme 上下文同步",
      "布局预设占位",
        "事件桥占位接口",
      "运行态快照回传",
      "为 Pro adapter 保留切换位",
    ],
    upgradeGates: [
      {
        id: "fallback-gate-runtime",
        label: "Workbench 主舞台已挂接 facade 边界",
        status: "passed",
        note: "当前工作台已经通过 facade / adapter 进行装配，而不是直接绑定图表实现。",
      },
      {
        id: "fallback-gate-pro",
        label: "真实 Pro SDK 接入待完成",
        status: fallbackEngaged ? "pending" : "passed",
        note: fallbackEngaged
          ? "等待 Pro adapter 从 skeleton 进入真实 SDK 接入阶段。"
          : "当前未请求 Pro adapter。",
      },
    ],
  };

  function syncDescriptor(): void {
    descriptor.runtimeStatus = state.status;
    descriptor.reason = fallbackEngaged
      ? `未检测到 ${requestedAdapter?.label}，当前先使用 fallback shell adapter 保持工作台主舞台可运行。`
      : "使用 fallback shell adapter 承接 facade 主舞台。";
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
      requestedAdapterKind: requestedAdapter?.kind ?? "fallback",
      requestedAdapterReady: requestedAdapter?.ready ?? true,
      fallbackEngaged,
      bridgeConnected: false,
      bridgeEventTypes: [],
      lastBridgeEventAt: null,
      reason: descriptor.reason,
    };
  }

  return {
    async mount(container: HTMLElement): Promise<void> {
      state.container = container;
      state.mounted = true;
      state.status = fallbackEngaged ? "degraded" : "ready";
      syncDescriptor();
      container.dataset.chartAdapter = descriptor.kind;
      container.dataset.chartStatus = state.status;
    },
    async unmount(): Promise<void> {
      if (state.container) {
        state.container.dataset.chartAdapter = "";
        state.container.dataset.chartStatus = "";
      }
      state.container = null;
      state.mounted = false;
      state.status = fallbackEngaged ? "degraded" : "idle";
      syncDescriptor();
    },
    async setSymbol(symbol: string): Promise<void> {
      state.symbol = symbol;
    },
    async setInterval(interval: ChartFacadeFactoryOptions["interval"]): Promise<void> {
      state.interval = interval;
    },
    async setBars(bars: ChartBar[]): Promise<void> {
      state.bars = [...bars];
    },
    async appendBars(bars: ChartBar[]): Promise<void> {
      state.bars = [...state.bars, ...bars];
    },
    async setIndicators(indicators: IndicatorSpec[]): Promise<void> {
      state.indicators = [...indicators];
    },
    async setOverlays(overlays: OverlaySpec[]): Promise<void> {
      state.overlays = [...overlays];
    },
    async setAnnotations(annotations: AnnotationSpec[]): Promise<void> {
      state.annotations = [...annotations];
    },
    async setTradeMarks(marks: TradeMarkSpec[]): Promise<void> {
      state.tradeMarks = [...marks];
    },
    async setTheme(theme: NonNullable<ChartFacadeFactoryOptions["theme"]>): Promise<void> {
      state.theme = theme;
    },
    async saveLayout(presetId: string): Promise<void> {
      state.layoutPresetId = presetId;
    },
    async loadLayout(presetId: string): Promise<void> {
      state.layoutPresetId = presetId;
    },
    async exportImage(): Promise<string> {
      return "fallback-chart-export-unavailable";
    },
    async applyCrosshairSync(): Promise<void> {
      // fallback adapter does not own a runtime bridge
    },
    async applyVisibleRangeSync(): Promise<void> {
      // fallback adapter does not own a runtime bridge
    },
    subscribeBridgeEvents() {
      return () => {
        // fallback adapter does not emit runtime bridge events
      };
    },
    async snapshot(): Promise<ChartRuntimeSnapshot> {
      return snapshot();
    },
    getAdapterDescriptor(): ChartAdapterDescriptor {
      syncDescriptor();
      return cloneAdapterDescriptor(descriptor);
    },
    getUpgradeGates() {
      return cloneAdapterDescriptor(descriptor).upgradeGates;
    },
  };
}
