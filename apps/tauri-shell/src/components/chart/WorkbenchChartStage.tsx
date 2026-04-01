import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { createChartFacadeV2 } from "../../chart/createChartFacadeV2";
import {
  CHART_FACADE_V2_DRAFT_VERSION,
  CHART_FACADE_V2_TARGET_RUNTIME,
  type AnnotationSpec,
  type ChartAdapterDescriptor,
  type ChartBar,
  type ChartBridgeClickPayload,
  type ChartBridgeCrosshairPayload,
  type ChartBridgeEvent,
  type ChartBridgeEventType,
  type ChartBridgeVisibleRangePayload,
  type ChartFacadeBundle,
  type ChartInterval,
  type ChartRuntimeSnapshot,
  type ChartRuntimeStatus,
  type ChartThemeName,
  type OverlaySpec,
  type TradeMarkSpec,
} from "../../chart/chartFacadeV2";
import {
  fetchChartBars,
  fetchStructuredSignals,
  fetchStructures,
  type ChartBarsPayload,
  type ChartBarsQualityPayload,
  type StructureItemPayload,
  type StructuredSignalItemPayload,
  type StructureQueryParams,
} from "../../lib/api";
import { getToneClassName, getToneMarker, type UiTone } from "../../lib/uiTone";
import {
  resolveWorkbenchChartLayout,
  type WorkbenchChartLayoutConfig,
  type WorkbenchChartPanelConfig,
} from "./workbenchChartLayout";

interface WorkbenchChartStageProps {
  symbol: string;
  interval: ChartInterval;
  layoutPresetId: string;
  theme?: ChartThemeName;
  reloadToken?: number;
  onSnapshotChange?: (snapshot: ChartRuntimeSnapshot | null) => void;
  onQualityChange?: (quality: ChartBarsQualityPayload | null) => void;
  onBridgeStateChange?: (snapshot: WorkbenchChartBridgeSnapshot | null) => void;
  onPerformanceChange?: (snapshot: WorkbenchChartPerformanceSnapshot | null) => void;
}

export type WorkbenchChartQualitySnapshot = ChartBarsQualityPayload;

type WorkbenchChartSyncEventType = "crosshair" | "visible-range";
export type WorkbenchChartSyncMode = "off" | "crosshair" | "visible-range" | "full";

export interface WorkbenchChartCrosshairAlignmentItem {
  paneId: string;
  paneTitle: string;
  interval: ChartInterval;
  nearestBarTime: string | null;
  deltaMs: number | null;
}

export interface WorkbenchChartRangeAlignmentItem {
  paneId: string;
  paneTitle: string;
  interval: ChartInterval;
  fromBarTime: string | null;
  fromDeltaMs: number | null;
  toBarTime: string | null;
  toDeltaMs: number | null;
}

export interface WorkbenchChartCrosshairAlignmentSnapshot {
  sourcePaneTitle: string | null;
  sourceInterval: ChartInterval | null;
  sourceTime: string | null;
  targets: WorkbenchChartCrosshairAlignmentItem[];
}

export interface WorkbenchChartRangeAlignmentSnapshot {
  sourcePaneTitle: string | null;
  sourceInterval: ChartInterval | null;
  sourceFromTime: string | null;
  sourceToTime: string | null;
  targets: WorkbenchChartRangeAlignmentItem[];
}

export interface WorkbenchChartBridgeSnapshot {
  paneCount: number;
  mountedPaneCount: number;
  connectedPaneCount: number;
  focusedPaneId: string | null;
  focusedPaneTitle: string | null;
  mainPaneId: string | null;
  mainPaneTitle: string | null;
  mainPaneInterval: ChartInterval | null;
  lastEventType: ChartBridgeEventType | null;
  lastEventAt: number | null;
  eventCounts: Record<ChartBridgeEventType, number>;
  lastClick: ScopedBridgePayload<ChartBridgeClickPayload> | null;
  lastCrosshair: ScopedBridgePayload<ChartBridgeCrosshairPayload> | null;
  lastVisibleRange: ScopedBridgePayload<ChartBridgeVisibleRangePayload> | null;
  syncMode: WorkbenchChartSyncMode;
  followSourceEnabled: boolean;
  crosshairSyncEnabled: boolean;
  visibleRangeSyncEnabled: boolean;
  lockedPaneIds: string[];
  lockedPaneTitles: string[];
  syncDispatchCounts: Record<WorkbenchChartSyncEventType, number>;
  lastSyncType: WorkbenchChartSyncEventType | null;
  lastSyncAt: number | null;
  lastSyncPaneTitle: string | null;
  barsReadyPaneCount: number;
  markerReadyPaneCount: number;
  structuredPaneCount: number;
  crosshairAlignment: WorkbenchChartCrosshairAlignmentSnapshot | null;
  rangeAlignment: WorkbenchChartRangeAlignmentSnapshot | null;
}

export interface WorkbenchChartPerformanceSnapshot {
  paneCount: number;
  mountedPaneCount: number;
  lazyPendingCount: number;
  stressTargetCount: number;
  lastUpdatedAt: number | null;
  lastPaneId: string | null;
  lastMarkerApplyMs: number | null;
  lastStressDurationMs: number | null;
  maxMarkerApplyMs: number | null;
}

interface ScopedBridgePayload<TPayload> {
  paneId: string;
  paneTitle: string;
  emittedAt: number;
  payload: TPayload;
}

interface PanePerformanceMetrics {
  panelId: string;
  overlayApplyMs: number | null;
  annotationApplyMs: number | null;
  tradeMarkApplyMs: number | null;
  markerApplyMs: number | null;
  stressAnnotationCount: number;
  stressTradeMarkCount: number;
  stressDurationMs: number | null;
  updatedAt: number;
}

interface PaneRuntimeCardState {
  snapshot: ChartRuntimeSnapshot | null;
  requestedAdapter: ChartAdapterDescriptor | null;
  barsPayload: ChartBarsPayload | null;
  barsLoading: boolean;
  barsError: string | null;
  markerLoading: boolean;
  markerError: string | null;
  stageError: string | null;
  structureCount: number;
  signalCount: number;
  shouldMount: boolean;
}

interface PaneScopedBridgeEvent {
  panelId: string;
  panelTitle: string;
  event: ChartBridgeEvent;
}

interface PaneSyncController {
  applyCrosshairSync(payload: ChartBridgeCrosshairPayload | null): Promise<void>;
  applyVisibleRangeSync(payload: ChartBridgeVisibleRangePayload | null): Promise<void>;
}

interface LastSyncRecord {
  type: WorkbenchChartSyncEventType;
  at: number;
  paneId: string;
  paneTitle: string;
}

interface WorkbenchChartPaneProps {
  panel: WorkbenchChartPanelConfig;
  panelIndex: number;
  symbol: string;
  layoutPresetId: string;
  theme: ChartThemeName;
  reloadToken: number;
  shouldMount: boolean;
  stressTargetCount: number;
  focused: boolean;
  isMainPane: boolean;
  locked: boolean;
  onRequestMount: (panelId: string) => void;
  onFocusPane: (panelId: string) => void;
  onPromoteMainPane: (panelId: string) => void;
  onTogglePaneLock: (panelId: string) => void;
  onPaneStateChange: (panelId: string, state: PaneRuntimeCardState) => void;
  onPaneSnapshotChange: (panelId: string, snapshot: ChartRuntimeSnapshot | null) => void;
  onPaneQualityChange: (panelId: string, quality: ChartBarsQualityPayload | null) => void;
  onPaneBridgeEvent: (event: PaneScopedBridgeEvent) => void;
  onPanePerformanceChange: (panelId: string, metrics: PanePerformanceMetrics | null) => void;
  onPaneControllerChange: (panelId: string, controller: PaneSyncController | null) => void;
}

const STRUCTURE_QUERY_DEFAULTS: Pick<StructureQueryParams, "status" | "signalType" | "groupStrategy" | "minObservations" | "limit" | "offset"> = {
  status: "",
  signalType: "",
  groupStrategy: "adaptive",
  minObservations: 3,
  limit: 12,
  offset: 0,
};

const STRESS_OPTIONS = [0, 250, 1000, 2500] as const;
const SYNC_MODE_OPTIONS: Array<{ value: WorkbenchChartSyncMode; label: string }> = [
  { value: "full", label: "全同步" },
  { value: "crosshair", label: "仅准星" },
  { value: "visible-range", label: "仅视区" },
  { value: "off", label: "关闭同步" },
];

function displayValue(value: string | number | boolean | null | undefined): string {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  return String(value);
}

function formatRuntimeTime(value: number | null | undefined): string {
  if (!value || Number.isNaN(value)) {
    return "—";
  }
  return new Date(value).toLocaleTimeString("zh-CN", { hour12: false });
}

function formatDuration(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return `${value.toFixed(1)} ms`;
}

function formatAlignmentDelta(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  const absolute = Math.abs(value);
  if (absolute < 1000) {
    return `${absolute.toFixed(0)} ms`;
  }
  if (absolute < 60_000) {
    return `${(absolute / 1000).toFixed(1)} s`;
  }
  return `${(absolute / 60_000).toFixed(1)} min`;
}

function getSyncModeLabel(mode: WorkbenchChartSyncMode): string {
  switch (mode) {
    case "crosshair":
      return "仅准星";
    case "visible-range":
      return "仅视区";
    case "off":
      return "关闭同步";
    case "full":
    default:
      return "全同步";
  }
}

function isCrosshairSyncMode(mode: WorkbenchChartSyncMode): boolean {
  return mode === "full" || mode === "crosshair";
}

function isVisibleRangeSyncMode(mode: WorkbenchChartSyncMode): boolean {
  return mode === "full" || mode === "visible-range";
}

function getRuntimeTone(status: ChartRuntimeStatus | undefined): UiTone {
  switch (status) {
    case "ready":
      return "ok";
    case "degraded":
    case "mounting":
      return "warning";
    case "error":
      return "danger";
    case "idle":
    default:
      return "neutral";
  }
}

function getQualityTone(status: ChartBarsQualityPayload["golden_status"] | undefined): UiTone {
  switch (status) {
    case "golden":
      return "ok";
    case "partial_trust":
      return "warning";
    case "degraded":
      return "danger";
    case "unknown":
    default:
      return "neutral";
  }
}

function getRepairTone(status: string | undefined): UiTone {
  switch (status) {
    case "complete":
    case "queued":
      return "ok";
    case "pending":
    case "in_progress":
      return "warning";
    case "manual_review":
    case "blocked":
    case "failed":
      return "danger";
    case "noop":
    case "unknown":
    default:
      return "neutral";
  }
}

function resolveStructureInterval(interval: ChartInterval): string | null {
  switch (interval) {
    case "1m":
    case "5m":
    case "15m":
    case "30m":
    case "1d":
      return interval;
    case "1h":
      return "60m";
    case "1w":
    case "4h":
    default:
      return null;
  }
}

function formatMarkerTime(rawTs: number | null, interval: ChartInterval): string | null {
  if (rawTs === null || rawTs === undefined || Number.isNaN(rawTs)) {
    return null;
  }
  const normalized = rawTs > 1_000_000_000_000 ? rawTs : rawTs * 1000;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  if (interval === "1d" || interval === "1w") {
    return date.toISOString().slice(0, 10);
  }
  return date.toISOString();
}

function getStructureColor(direction: string | null | undefined, status: string | null | undefined): string {
  if (status === "reversed") {
    return "#ff6b6b";
  }
  if (direction === "up") {
    return "#34d399";
  }
  if (direction === "down") {
    return "#f59e0b";
  }
  return "#93c5fd";
}

function buildStructureOverlays(structures: StructureItemPayload[]): OverlaySpec[] {
  return structures.map((item) => ({
    id: `structure-overlay-${item.structure_id}`,
    kind: "line",
    payload: {
      structureId: item.structure_id,
      code: item.code,
      interval: item.interval,
      direction: item.direction,
      status: item.status,
      color: getStructureColor(item.direction, item.status),
      retraceRatio: item.retrace_ratio,
      points: item.points,
    },
  }));
}

function buildStructureAnnotations(
  structures: StructureItemPayload[],
  interval: ChartInterval,
): AnnotationSpec[] {
  return structures.flatMap((item) => {
    const candidates = [item.points.p3, item.points.p2, item.points.p1, item.points.p0];
    const anchor = candidates.find((point) => point.ts !== null && point.price !== null);
    const time = formatMarkerTime(anchor?.ts ?? null, interval);
    if (!anchor || !time || anchor.price === null) {
      return [];
    }
    return [{
      id: `structure-annotation-${item.structure_id}`,
      kind: "structure",
      payload: {
        structureId: item.structure_id,
        time,
        price: anchor.price,
        direction: item.direction,
        status: item.status,
        text: `${item.direction ?? "—"} / ${item.status ?? "—"}`,
        bayesGroup: item.layer4.bayes_group_level,
      },
    }];
  });
}

function buildTradeMarks(
  signals: StructuredSignalItemPayload[],
  interval: ChartInterval,
): TradeMarkSpec[] {
  return signals.flatMap((item) => {
    const time = formatMarkerTime(item.signal_ts, interval);
    const price = item.trigger_price ?? item.risk.stop_loss_price;
    if (!time || price === null || price === undefined || Number.isNaN(price)) {
      return [];
    }
    let direction: TradeMarkSpec["direction"] = "flat";
    if (item.signal_type === "LONG") {
      direction = "buy";
    } else if (item.signal_type === "SHORT") {
      direction = "sell";
    }
    return [{
      id: `trade-mark-${item.signal_id}`,
      direction,
      time,
      price,
      text: item.signal_type ?? item.remarks ?? undefined,
    }];
  });
}

function buildSyntheticAnnotations(
  bars: ChartBar[],
  interval: ChartInterval,
  panelId: string,
  targetCount: number,
): AnnotationSpec[] {
  if (!targetCount || !bars.length) {
    return [];
  }
  const annotations: AnnotationSpec[] = [];
  const step = Math.max(1, Math.floor(bars.length / Math.min(targetCount, bars.length)));
  for (let index = 0; index < targetCount; index += 1) {
    const bar = bars[(index * step) % bars.length];
    const price = index % 3 === 0 ? bar.high : index % 3 === 1 ? bar.low : bar.close;
    annotations.push({
      id: `stress-annotation-${panelId}-${index}`,
      kind: index % 2 === 0 ? "label" : "note",
      payload: {
        time: formatMarkerTime(typeof bar.time === "string" ? Date.parse(bar.time.replace(" ", "T")) : null, interval) ?? bar.time,
        price,
        text: `stress-${index + 1}`,
        direction: index % 2 === 0 ? "up" : "down",
        status: "stress",
      },
    });
  }
  return annotations;
}

function buildSyntheticTradeMarks(
  bars: ChartBar[],
  panelId: string,
  targetCount: number,
): TradeMarkSpec[] {
  if (!targetCount || !bars.length) {
    return [];
  }
  const marks: TradeMarkSpec[] = [];
  const markCount = Math.max(12, Math.floor(targetCount / 4));
  const step = Math.max(1, Math.floor(bars.length / Math.min(markCount, bars.length)));
  for (let index = 0; index < markCount; index += 1) {
    const bar = bars[(index * step) % bars.length];
    marks.push({
      id: `stress-trademark-${panelId}-${index}`,
      direction: index % 2 === 0 ? "buy" : "sell",
      time: bar.time,
      price: index % 2 === 0 ? bar.low : bar.high,
      text: `stress-mark-${index + 1}`,
    });
  }
  return marks;
}

function parseChartBarTimestamp(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(trimmed) ? `${trimmed}T00:00:00` : trimmed.replace(" ", "T");
  const parsed = Date.parse(normalized);
  return Number.isNaN(parsed) ? null : parsed;
}

function findNearestBarMatch(
  bars: ChartBar[],
  targetTimestamp: number | null | undefined,
): { time: string; timestamp: number; deltaMs: number } | null {
  if (!bars.length || targetTimestamp === null || targetTimestamp === undefined || Number.isNaN(targetTimestamp)) {
    return null;
  }

  let best: { time: string; timestamp: number; deltaMs: number } | null = null;
  for (const bar of bars) {
    const timestamp = parseChartBarTimestamp(bar.time);
    if (timestamp === null) {
      continue;
    }
    const deltaMs = Math.abs(timestamp - targetTimestamp);
    if (!best || deltaMs < best.deltaMs) {
      best = {
        time: bar.time,
        timestamp,
        deltaMs,
      };
      if (deltaMs === 0) {
        break;
      }
    }
  }
  return best;
}

function pickPrimarySnapshot(
  layout: WorkbenchChartLayoutConfig,
  paneSnapshots: Record<string, ChartRuntimeSnapshot | null>,
  primaryPaneId: string | null,
): ChartRuntimeSnapshot | null {
  const primaryPanel = layout.panels.find((panel) => panel.id === primaryPaneId) ?? layout.panels[0];
  if (!primaryPanel) {
    return null;
  }
  return paneSnapshots[primaryPanel.id] ?? null;
}

function pickPrimaryQuality(
  layout: WorkbenchChartLayoutConfig,
  paneQualities: Record<string, ChartBarsQualityPayload | null>,
  primaryPaneId: string | null,
): ChartBarsQualityPayload | null {
  const primaryPanel = layout.panels.find((panel) => panel.id === primaryPaneId) ?? layout.panels[0];
  if (!primaryPanel) {
    return null;
  }
  return paneQualities[primaryPanel.id] ?? null;
}

function WorkbenchChartPane(props: WorkbenchChartPaneProps) {
  const shellRef = useRef<HTMLDivElement | null>(null);
  const hostRef = useRef<HTMLDivElement | null>(null);
  const bundleRef = useRef<ChartFacadeBundle | null>(null);
  const [snapshot, setSnapshot] = useState<ChartRuntimeSnapshot | null>(null);
  const [requestedAdapter, setRequestedAdapter] = useState<ChartAdapterDescriptor | null>(null);
  const [barsPayload, setBarsPayload] = useState<ChartBarsPayload | null>(null);
  const [barsLoading, setBarsLoading] = useState(false);
  const [barsError, setBarsError] = useState<string | null>(null);
  const [markerLoading, setMarkerLoading] = useState(false);
  const [markerError, setMarkerError] = useState<string | null>(null);
  const [stageError, setStageError] = useState<string | null>(null);
  const [structureCount, setStructureCount] = useState(0);
  const [signalCount, setSignalCount] = useState(0);

  useEffect(() => {
    props.onPaneStateChange(props.panel.id, {
      snapshot,
      requestedAdapter,
      barsPayload,
      barsLoading,
      barsError,
      markerLoading,
      markerError,
      stageError,
      structureCount,
      signalCount,
      shouldMount: props.shouldMount,
    });
  }, [barsError, barsLoading, barsPayload, markerError, markerLoading, props.onPaneStateChange, props.panel.id, props.shouldMount, requestedAdapter, signalCount, snapshot, stageError, structureCount]);

  const syncSnapshot = useCallback(async () => {
    const bundle = bundleRef.current;
    if (!bundle) {
      props.onPaneSnapshotChange(props.panel.id, null);
      setSnapshot(null);
      return null;
    }
    const nextSnapshot = await bundle.facade.snapshot();
    setSnapshot(nextSnapshot);
    props.onPaneSnapshotChange(props.panel.id, nextSnapshot);
    return nextSnapshot;
  }, [props.onPaneSnapshotChange, props.panel.id]);

  const applyCrosshairSync = useCallback(async (payload: ChartBridgeCrosshairPayload | null) => {
    const bundle = bundleRef.current;
    if (!bundle || !props.shouldMount) {
      return;
    }
    await bundle.facade.applyCrosshairSync(payload);
  }, [props.shouldMount]);

  const applyVisibleRangeSync = useCallback(async (payload: ChartBridgeVisibleRangePayload | null) => {
    const bundle = bundleRef.current;
    if (!bundle || !props.shouldMount) {
      return;
    }
    await bundle.facade.applyVisibleRangeSync(payload);
  }, [props.shouldMount]);

  useEffect(() => {
    if (!props.shouldMount || !snapshot?.mounted) {
      props.onPaneControllerChange(props.panel.id, null);
      return undefined;
    }

    props.onPaneControllerChange(props.panel.id, {
      applyCrosshairSync,
      applyVisibleRangeSync,
    });

    return () => {
      props.onPaneControllerChange(props.panel.id, null);
    };
  }, [applyCrosshairSync, applyVisibleRangeSync, props.onPaneControllerChange, props.panel.id, props.shouldMount, snapshot?.mounted]);

  useEffect(() => {
    if (props.shouldMount) {
      return undefined;
    }
    const node = shellRef.current;
    if (!node) {
      return undefined;
    }
    let cancelled = false;
    let timeoutId: number | null = null;
    const activate = () => {
      if (cancelled) {
        return;
      }
      timeoutId = window.setTimeout(() => {
        if (!cancelled) {
          props.onRequestMount(props.panel.id);
        }
      }, props.panelIndex * 160);
    };

    if ("IntersectionObserver" in window) {
      const observer = new IntersectionObserver((entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          observer.disconnect();
          activate();
        }
      }, { rootMargin: "200px" });
      observer.observe(node);
      return () => {
        cancelled = true;
        observer.disconnect();
        if (timeoutId !== null) {
          window.clearTimeout(timeoutId);
        }
      };
    }

    activate();
    return () => {
      cancelled = true;
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [props.onRequestMount, props.panel.id, props.panelIndex, props.shouldMount]);

  useEffect(() => {
    if (!props.shouldMount) {
      return undefined;
    }
    const host = hostRef.current;
    if (!host) {
      return undefined;
    }
    const mountHost = host;

    let active = true;
    const bundle = createChartFacadeV2({
      preferredAdapter: "pro",
      symbol: props.symbol,
      interval: props.panel.interval,
      theme: props.theme,
      layoutPresetId: props.layoutPresetId,
    });
    bundleRef.current = bundle;
    setRequestedAdapter(bundle.requestedAdapter);

    const unsubscribe = bundle.facade.subscribeBridgeEvents((event) => {
      props.onPaneBridgeEvent({
        panelId: props.panel.id,
        panelTitle: props.panel.title,
        event,
      });
    });

    async function boot() {
      try {
        setStageError(null);
        await bundle.facade.mount(mountHost);
        await bundle.facade.setTheme(props.theme);
        await bundle.facade.setSymbol(props.symbol);
        await bundle.facade.setInterval(props.panel.interval);
        await bundle.facade.loadLayout(props.layoutPresetId);
        if (!active) {
          return;
        }
        await syncSnapshot();
      } catch (reason) {
        if (!active) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "图表主舞台启动失败";
        setStageError(message);
      }
    }

    void boot();

    return () => {
      active = false;
      unsubscribe();
      const currentBundle = bundleRef.current;
      bundleRef.current = null;
      props.onPaneSnapshotChange(props.panel.id, null);
      props.onPaneQualityChange(props.panel.id, null);
      props.onPanePerformanceChange(props.panel.id, null);
      if (currentBundle) {
        void currentBundle.facade.unmount();
      }
    };
  }, [props.layoutPresetId, props.onPanePerformanceChange, props.onPaneQualityChange, props.onPaneSnapshotChange, props.onPaneBridgeEvent, props.panel.id, props.panel.interval, props.panel.title, props.shouldMount, props.symbol, props.theme, syncSnapshot]);

  useEffect(() => {
    if (!props.shouldMount || !bundleRef.current) {
      return undefined;
    }
    let active = true;
    async function applyContext() {
      const bundle = bundleRef.current;
      if (!bundle) {
        return;
      }
      try {
        await bundle.facade.setSymbol(props.symbol);
        await bundle.facade.setInterval(props.panel.interval);
        await bundle.facade.loadLayout(props.layoutPresetId);
        await bundle.facade.setTheme(props.theme);
        if (!active) {
          return;
        }
        await syncSnapshot();
      } catch (reason) {
        if (!active) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "图表上下文刷新失败";
        setStageError(message);
      }
    }
    void applyContext();
    return () => {
      active = false;
    };
  }, [props.layoutPresetId, props.panel.interval, props.shouldMount, props.symbol, props.theme, syncSnapshot]);

  useEffect(() => {
    if (!props.shouldMount || !bundleRef.current) {
      return undefined;
    }

    let active = true;
    const controller = new AbortController();

    async function loadBars() {
      const bundle = bundleRef.current;
      if (!bundle) {
        return;
      }
      setBarsLoading(true);
      setBarsError(null);
      setBarsPayload(null);
      props.onPaneQualityChange(props.panel.id, null);

      try {
        await bundle.facade.setBars([]);
        await syncSnapshot();
        const payload = await fetchChartBars({
          symbol: props.symbol,
          interval: props.panel.interval,
          limit: props.panelIndex === 0 ? 800 : 500,
        }, controller.signal);
        if (!active) {
          return;
        }
        await bundle.facade.setBars(payload.bars);
        await syncSnapshot();
        setBarsPayload(payload);
        setBarsLoading(false);
        props.onPaneQualityChange(props.panel.id, payload.quality);
      } catch (reason) {
        if (!active || controller.signal.aborted) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "bars 数据加载失败";
        setBarsError(message);
        setBarsLoading(false);
        props.onPaneQualityChange(props.panel.id, null);
        await bundle.facade.setBars([]);
        await syncSnapshot();
      }
    }

    void loadBars();
    return () => {
      active = false;
      controller.abort();
    };
  }, [props.panel.id, props.panel.interval, props.panelIndex, props.reloadToken, props.shouldMount, props.symbol, props.onPaneQualityChange, syncSnapshot]);

  useEffect(() => {
    if (!props.shouldMount || !bundleRef.current) {
      return undefined;
    }

    let active = true;
    const controller = new AbortController();

    async function loadMarkers() {
      const bundle = bundleRef.current;
      if (!bundle) {
        return;
      }

      setMarkerLoading(true);
      setMarkerError(null);
      const structureInterval = resolveStructureInterval(props.panel.interval);

      if (!structureInterval) {
        await bundle.facade.setOverlays([]);
        await bundle.facade.setAnnotations([]);
        await bundle.facade.setTradeMarks([]);
        await syncSnapshot();
        setStructureCount(0);
        setSignalCount(0);
        setMarkerLoading(false);
        props.onPanePerformanceChange(props.panel.id, {
          panelId: props.panel.id,
          overlayApplyMs: 0,
          annotationApplyMs: 0,
          tradeMarkApplyMs: 0,
          markerApplyMs: 0,
          stressAnnotationCount: 0,
          stressTradeMarkCount: 0,
          stressDurationMs: null,
          updatedAt: Date.now(),
        });
        return;
      }

      const query: StructureQueryParams = {
        code: props.symbol,
        interval: structureInterval,
        ...STRUCTURE_QUERY_DEFAULTS,
      };

      try {
        const [structuresResult, signalsResult] = await Promise.allSettled([
          fetchStructures(query, controller.signal),
          fetchStructuredSignals(query, controller.signal),
        ]);
        if (!active) {
          return;
        }

        const structures = structuresResult.status === "fulfilled" ? structuresResult.value.items : [];
        const signals = signalsResult.status === "fulfilled" ? signalsResult.value.items : [];
        const baseOverlays = buildStructureOverlays(structures);
        const baseAnnotations = buildStructureAnnotations(structures, props.panel.interval);
        const baseTradeMarks = buildTradeMarks(signals, props.panel.interval);
        const syntheticAnnotations = buildSyntheticAnnotations(
          barsPayload?.bars ?? [],
          props.panel.interval,
          props.panel.id,
          props.stressTargetCount,
        );
        const syntheticTradeMarks = buildSyntheticTradeMarks(
          barsPayload?.bars ?? [],
          props.panel.id,
          props.stressTargetCount,
        );
        const annotations = [...baseAnnotations, ...syntheticAnnotations];
        const tradeMarks = [...baseTradeMarks, ...syntheticTradeMarks];

        const overlayStartedAt = performance.now();
        await bundle.facade.setOverlays(baseOverlays);
        const overlayApplyMs = performance.now() - overlayStartedAt;

        const annotationStartedAt = performance.now();
        await bundle.facade.setAnnotations(annotations);
        const annotationApplyMs = performance.now() - annotationStartedAt;

        const markStartedAt = performance.now();
        await bundle.facade.setTradeMarks(tradeMarks);
        const tradeMarkApplyMs = performance.now() - markStartedAt;

        await syncSnapshot();

        const markerApplyMs = overlayApplyMs + annotationApplyMs + tradeMarkApplyMs;
        const stressDurationMs = syntheticAnnotations.length + syntheticTradeMarks.length > 0
          ? annotationApplyMs + tradeMarkApplyMs
          : null;

        props.onPanePerformanceChange(props.panel.id, {
          panelId: props.panel.id,
          overlayApplyMs,
          annotationApplyMs,
          tradeMarkApplyMs,
          markerApplyMs,
          stressAnnotationCount: syntheticAnnotations.length,
          stressTradeMarkCount: syntheticTradeMarks.length,
          stressDurationMs,
          updatedAt: Date.now(),
        });

        setStructureCount(structures.length);
        setSignalCount(signals.length);
        setMarkerLoading(false);

        if (structuresResult.status === "rejected" && signalsResult.status === "rejected") {
          const message = structuresResult.reason instanceof Error
            ? structuresResult.reason.message
            : "结构/信号读模型加载失败";
          setMarkerError(message);
        } else if (structuresResult.status === "rejected") {
          const message = structuresResult.reason instanceof Error
            ? structuresResult.reason.message
            : "结构读模型加载失败";
          setMarkerError(message);
        } else if (signalsResult.status === "rejected") {
          const message = signalsResult.reason instanceof Error
            ? signalsResult.reason.message
            : "信号读模型加载失败";
          setMarkerError(message);
        }
      } catch (reason) {
        if (!active || controller.signal.aborted) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "结构标注/交易标记加载失败";
        setMarkerError(message);
        setMarkerLoading(false);
        setStructureCount(0);
        setSignalCount(0);
        await bundle.facade.setOverlays([]);
        await bundle.facade.setAnnotations([]);
        await bundle.facade.setTradeMarks([]);
        await syncSnapshot();
      }
    }

    void loadMarkers();
    return () => {
      active = false;
      controller.abort();
    };
  }, [barsPayload?.bars, props.onPanePerformanceChange, props.panel.id, props.panel.interval, props.shouldMount, props.stressTargetCount, props.symbol, syncSnapshot]);

  const qualityTone = getQualityTone(barsPayload?.quality.golden_status);
  const repairTone = getRepairTone(barsPayload?.quality.repair.plan_status);
  const runtimeTone = getRuntimeTone(snapshot?.status);
  const mountTone: UiTone = props.shouldMount ? (snapshot?.mounted ? "ok" : barsLoading ? "warning" : "neutral") : "warning";

  return (
    <article
      ref={shellRef}
      className={[
        "workbench-chart-pane-card",
        props.focused ? "focused" : "",
        props.isMainPane ? "main-pane" : "",
        props.locked ? "locked" : "",
      ].filter(Boolean).join(" ")}
      onClick={() => props.onFocusPane(props.panel.id)}
    >
      <header className="workbench-chart-pane-header">
        <div>
          <strong>{props.panel.title}</strong>
          <span>{props.panel.subtitle}</span>
        </div>
        <div className="workbench-chart-pane-header-actions">
          <div className="workbench-chart-pane-chip-row">
            <span className="status-chip">{props.panel.interval}</span>
            <span className={`status-chip ${mountTone === "neutral" ? "" : mountTone}`}>
              {props.shouldMount ? "已挂载" : "懒挂载待激活"}
            </span>
            <span className={`status-chip ${runtimeTone === "neutral" ? "" : runtimeTone}`}>
              {getToneMarker(runtimeTone)} {displayValue(snapshot?.status ?? (props.shouldMount ? "mounting" : "idle"))}
            </span>
            {props.isMainPane ? <span className="status-chip ok">主 pane</span> : null}
            {props.locked ? <span className="status-chip warning">已锁定</span> : null}
          </div>
          <div className="workbench-chart-pane-action-row">
            <button
              type="button"
              className={`chip-button ${props.isMainPane ? "active" : ""}`}
              onClick={(event) => {
                event.stopPropagation();
                props.onPromoteMainPane(props.panel.id);
              }}
            >
              {props.isMainPane ? "主 pane" : "设为主 pane"}
            </button>
            <button
              type="button"
              className={`chip-button ${props.locked ? "active" : ""}`}
              onClick={(event) => {
                event.stopPropagation();
                props.onTogglePaneLock(props.panel.id);
              }}
            >
              {props.locked ? "解锁 pane" : "锁定 pane"}
            </button>
          </div>
        </div>
      </header>

      <div className="workbench-chart-pane-meta">
        <span>bars {displayValue(barsPayload?.bar_count ?? snapshot?.barCount)}</span>
        <span>structures {displayValue(structureCount)}</span>
        <span>signals {displayValue(signalCount)}</span>
        <span className={getToneClassName(qualityTone)}>
          {getToneMarker(qualityTone)} Golden {displayValue(barsPayload?.quality.golden_status)}
        </span>
        <span className={getToneClassName(repairTone)}>
          {getToneMarker(repairTone)} Repair {displayValue(barsPayload?.quality.repair.plan_status)}
        </span>
      </div>

      {stageError ? <div className="status-chip danger">runtime: {stageError}</div> : null}
      {barsError ? <div className="status-chip danger">bars: {barsError}</div> : null}
      {markerError ? <div className="status-chip danger">markers: {markerError}</div> : null}

      {!props.shouldMount ? (
        <div className="workbench-chart-pane-placeholder">
          <strong>懒挂载就绪</strong>
          <p>该 pane 会在进入视口或点击激活后启动 Pro runtime，用来把首屏压力留给主 pane。</p>
          <button
            type="button"
            className="chip-button active"
            onClick={(event) => {
              event.stopPropagation();
              props.onRequestMount(props.panel.id);
            }}
          >
            立即挂载 {props.panel.title}
          </button>
        </div>
      ) : (
        <div className="workbench-chart-pane-body">
          <div ref={hostRef} className="workbench-chart-pane-host" />
          <div className="workbench-chart-pane-overlay">
            <div className="workbench-chart-pane-overlay-top">
              <span>{requestedAdapter?.label ?? "—"}</span>
              <span>{barsLoading || markerLoading ? "同步中…" : props.locked ? "本 pane 已隔离" : "已联动"}</span>
            </div>
            <div className="workbench-chart-pane-overlay-bottom">
              <span>{displayValue(snapshot?.annotationCount)} annotations / {displayValue(snapshot?.tradeMarkCount)} marks</span>
              <span>
                {props.isMainPane ? "main" : props.locked ? "locked" : "follow"} · {displayValue(snapshot?.bridgeConnected ? "bridge on" : "bridge off")}
              </span>
            </div>
          </div>
        </div>
      )}
    </article>
  );
}

export function WorkbenchChartStage(props: WorkbenchChartStageProps) {
  const layout = useMemo(
    () => resolveWorkbenchChartLayout(props.layoutPresetId, props.interval),
    [props.interval, props.layoutPresetId],
  );
  const [activatedPaneIds, setActivatedPaneIds] = useState<string[]>(() => layout.panels.filter((panel) => panel.eager).map((panel) => panel.id));
  const [focusedPaneId, setFocusedPaneId] = useState<string | null>(layout.panels[0]?.id ?? null);
  const [mainPaneId, setMainPaneId] = useState<string | null>(layout.panels[0]?.id ?? null);
  const [lockedPaneIds, setLockedPaneIds] = useState<string[]>([]);
  const [paneSnapshots, setPaneSnapshots] = useState<Record<string, ChartRuntimeSnapshot | null>>({});
  const [paneQualities, setPaneQualities] = useState<Record<string, ChartBarsQualityPayload | null>>({});
  const [paneStateMap, setPaneStateMap] = useState<Record<string, PaneRuntimeCardState>>({});
  const [panePerformance, setPanePerformance] = useState<Record<string, PanePerformanceMetrics | null>>({});
  const [stressTargetCount, setStressTargetCount] = useState<number>(0);
  const [bridgeEventCounts, setBridgeEventCounts] = useState<Record<ChartBridgeEventType, number>>({
    "chart-click": 0,
    crosshair: 0,
    "visible-range": 0,
  });
  const [lastBridgeEvent, setLastBridgeEvent] = useState<PaneScopedBridgeEvent | null>(null);
  const [lastClick, setLastClick] = useState<ScopedBridgePayload<ChartBridgeClickPayload> | null>(null);
  const [lastCrosshair, setLastCrosshair] = useState<ScopedBridgePayload<ChartBridgeCrosshairPayload> | null>(null);
  const [lastVisibleRange, setLastVisibleRange] = useState<ScopedBridgePayload<ChartBridgeVisibleRangePayload> | null>(null);
  const [syncMode, setSyncMode] = useState<WorkbenchChartSyncMode>("full");
  const [followSourceEnabled, setFollowSourceEnabled] = useState(true);
  const [syncDispatchCounts, setSyncDispatchCounts] = useState<Record<WorkbenchChartSyncEventType, number>>({
    crosshair: 0,
    "visible-range": 0,
  });
  const [lastSync, setLastSync] = useState<LastSyncRecord | null>(null);
  const paneControllersRef = useRef<Record<string, PaneSyncController>>({});
  const crosshairSyncFrameRef = useRef<number | null>(null);
  const visibleRangeSyncFrameRef = useRef<number | null>(null);
  const lastCrosshairRef = useRef<ScopedBridgePayload<ChartBridgeCrosshairPayload> | null>(null);
  const lastVisibleRangeRef = useRef<ScopedBridgePayload<ChartBridgeVisibleRangePayload> | null>(null);
  const crosshairSyncEnabled = isCrosshairSyncMode(syncMode);
  const visibleRangeSyncEnabled = isVisibleRangeSyncMode(syncMode);
  const lockedPaneIdSet = useMemo(() => new Set(lockedPaneIds), [lockedPaneIds]);
  const crosshairSyncEnabledRef = useRef(crosshairSyncEnabled);
  const visibleRangeSyncEnabledRef = useRef(visibleRangeSyncEnabled);
  const lockedPaneIdsRef = useRef(new Set<string>());
  const followSourceEnabledRef = useRef(followSourceEnabled);

  useEffect(() => {
    lastCrosshairRef.current = lastCrosshair;
  }, [lastCrosshair]);

  useEffect(() => {
    lastVisibleRangeRef.current = lastVisibleRange;
  }, [lastVisibleRange]);

  useEffect(() => {
    crosshairSyncEnabledRef.current = crosshairSyncEnabled;
  }, [crosshairSyncEnabled]);

  useEffect(() => {
    visibleRangeSyncEnabledRef.current = visibleRangeSyncEnabled;
  }, [visibleRangeSyncEnabled]);

  useEffect(() => {
    lockedPaneIdsRef.current = new Set(lockedPaneIds);
  }, [lockedPaneIds]);

  useEffect(() => {
    followSourceEnabledRef.current = followSourceEnabled;
  }, [followSourceEnabled]);

  useEffect(() => {
    const validIds = new Set(layout.panels.map((panel) => panel.id));
    setActivatedPaneIds((current) => {
      const eagerIds = layout.panels.filter((panel) => panel.eager).map((panel) => panel.id);
      return Array.from(new Set([...current.filter((id) => validIds.has(id)), ...eagerIds]));
    });
    setFocusedPaneId((current) => (current && validIds.has(current) ? current : layout.panels[0]?.id ?? null));
    setMainPaneId((current) => (current && validIds.has(current) ? current : layout.panels[0]?.id ?? null));
    setLockedPaneIds((current) => current.filter((id) => validIds.has(id)));
    setPaneSnapshots((current) => Object.fromEntries(Object.entries(current).filter(([key]) => validIds.has(key))));
    setPaneQualities((current) => Object.fromEntries(Object.entries(current).filter(([key]) => validIds.has(key))));
    setPaneStateMap((current) => Object.fromEntries(Object.entries(current).filter(([key]) => validIds.has(key))));
    setPanePerformance((current) => Object.fromEntries(Object.entries(current).filter(([key]) => validIds.has(key))));
    paneControllersRef.current = Object.fromEntries(
      Object.entries(paneControllersRef.current).filter(([key]) => validIds.has(key)),
    ) as Record<string, PaneSyncController>;
  }, [layout]);

  useEffect(() => {
    setBridgeEventCounts({
      "chart-click": 0,
      crosshair: 0,
      "visible-range": 0,
    });
    setLastBridgeEvent(null);
    setLastClick(null);
    setLastCrosshair(null);
    setLastVisibleRange(null);
    setStressTargetCount(0);
    setSyncDispatchCounts({
      crosshair: 0,
      "visible-range": 0,
    });
    setLastSync(null);
  }, [layout.presetId, props.interval, props.symbol]);

  useEffect(() => () => {
    if (crosshairSyncFrameRef.current !== null) {
      window.cancelAnimationFrame(crosshairSyncFrameRef.current);
    }
    if (visibleRangeSyncFrameRef.current !== null) {
      window.cancelAnimationFrame(visibleRangeSyncFrameRef.current);
    }
  }, []);

  const requestMountPane = useCallback((panelId: string) => {
    setActivatedPaneIds((current) => (current.includes(panelId) ? current : [...current, panelId]));
  }, []);

  const promoteMainPane = useCallback((panelId: string) => {
    requestMountPane(panelId);
    setFocusedPaneId(panelId);
    setMainPaneId(panelId);
  }, [requestMountPane]);

  const togglePaneLock = useCallback((panelId: string) => {
    setLockedPaneIds((current) => (
      current.includes(panelId)
        ? current.filter((id) => id !== panelId)
        : [...current, panelId]
    ));
  }, []);

  const handlePaneControllerChange = useCallback((panelId: string, controller: PaneSyncController | null) => {
    if (!controller) {
      delete paneControllersRef.current[panelId];
      return;
    }

    paneControllersRef.current[panelId] = controller;

    if (lockedPaneIdsRef.current.has(panelId)) {
      return;
    }

    const latestVisibleRange = lastVisibleRangeRef.current;
    if (
      visibleRangeSyncEnabledRef.current
      && latestVisibleRange
      && latestVisibleRange.paneId !== panelId
      && !lockedPaneIdsRef.current.has(latestVisibleRange.paneId)
    ) {
      void controller.applyVisibleRangeSync(latestVisibleRange.payload);
    }

    const latestCrosshair = lastCrosshairRef.current;
    if (
      crosshairSyncEnabledRef.current
      && latestCrosshair
      && latestCrosshair.paneId !== panelId
      && !lockedPaneIdsRef.current.has(latestCrosshair.paneId)
    ) {
      void controller.applyCrosshairSync(latestCrosshair.payload);
    }
  }, []);

  const handlePaneStateChange = useCallback((panelId: string, state: PaneRuntimeCardState) => {
    setPaneStateMap((current) => ({
      ...current,
      [panelId]: state,
    }));
  }, []);

  const handlePaneSnapshotChange = useCallback((panelId: string, snapshot: ChartRuntimeSnapshot | null) => {
    setPaneSnapshots((current) => ({
      ...current,
      [panelId]: snapshot,
    }));
  }, []);

  const handlePaneQualityChange = useCallback((panelId: string, quality: ChartBarsQualityPayload | null) => {
    setPaneQualities((current) => ({
      ...current,
      [panelId]: quality,
    }));
  }, []);

  const handlePanePerformanceChange = useCallback((panelId: string, metrics: PanePerformanceMetrics | null) => {
    setPanePerformance((current) => ({
      ...current,
      [panelId]: metrics,
    }));
  }, []);

  const handlePaneBridgeEvent = useCallback((record: PaneScopedBridgeEvent) => {
    setFocusedPaneId(record.panelId);
    if (followSourceEnabledRef.current && !lockedPaneIdsRef.current.has(record.panelId)) {
      setMainPaneId(record.panelId);
    }
    setLastBridgeEvent(record);
    setBridgeEventCounts((current) => ({
      ...current,
      [record.event.type]: current[record.event.type] + 1,
    }));
    if (record.event.type === "chart-click") {
      setLastClick({
        paneId: record.panelId,
        paneTitle: record.panelTitle,
        emittedAt: record.event.emittedAt,
        payload: record.event.payload,
      });
      return;
    }
    if (record.event.type === "crosshair") {
      setLastCrosshair({
        paneId: record.panelId,
        paneTitle: record.panelTitle,
        emittedAt: record.event.emittedAt,
        payload: record.event.payload,
      });
      return;
    }
    setLastVisibleRange({
      paneId: record.panelId,
      paneTitle: record.panelTitle,
      emittedAt: record.event.emittedAt,
      payload: record.event.payload,
    });
  }, []);

  useEffect(() => {
    if (!crosshairSyncEnabled || !lastCrosshair || lockedPaneIdSet.has(lastCrosshair.paneId)) {
      return undefined;
    }

    if (crosshairSyncFrameRef.current !== null) {
      window.cancelAnimationFrame(crosshairSyncFrameRef.current);
    }

    crosshairSyncFrameRef.current = window.requestAnimationFrame(() => {
      crosshairSyncFrameRef.current = null;
      const targets = layout.panels
        .filter((panel) => panel.id !== lastCrosshair.paneId && !lockedPaneIdSet.has(panel.id))
        .map((panel) => paneControllersRef.current[panel.id])
        .filter((controller): controller is PaneSyncController => Boolean(controller));
      if (!targets.length) {
        return;
      }
      const syncAt = Date.now();
      void Promise.allSettled(targets.map((controller) => controller.applyCrosshairSync(lastCrosshair.payload))).finally(() => {
        setSyncDispatchCounts((current) => ({
          ...current,
          crosshair: current.crosshair + 1,
        }));
        setLastSync({
          type: "crosshair",
          at: syncAt,
          paneId: lastCrosshair.paneId,
          paneTitle: lastCrosshair.paneTitle,
        });
      });
    });

    return () => {
      if (crosshairSyncFrameRef.current !== null) {
        window.cancelAnimationFrame(crosshairSyncFrameRef.current);
        crosshairSyncFrameRef.current = null;
      }
    };
  }, [crosshairSyncEnabled, lastCrosshair, layout.panels, lockedPaneIdSet]);

  useEffect(() => {
    if (!visibleRangeSyncEnabled || !lastVisibleRange || lockedPaneIdSet.has(lastVisibleRange.paneId)) {
      return undefined;
    }

    if (visibleRangeSyncFrameRef.current !== null) {
      window.cancelAnimationFrame(visibleRangeSyncFrameRef.current);
    }

    visibleRangeSyncFrameRef.current = window.requestAnimationFrame(() => {
      visibleRangeSyncFrameRef.current = null;
      const targets = layout.panels
        .filter((panel) => panel.id !== lastVisibleRange.paneId && !lockedPaneIdSet.has(panel.id))
        .map((panel) => paneControllersRef.current[panel.id])
        .filter((controller): controller is PaneSyncController => Boolean(controller));
      if (!targets.length) {
        return;
      }
      const syncAt = Date.now();
      void Promise.allSettled(targets.map((controller) => controller.applyVisibleRangeSync(lastVisibleRange.payload))).finally(() => {
        setSyncDispatchCounts((current) => ({
          ...current,
          "visible-range": current["visible-range"] + 1,
        }));
        setLastSync({
          type: "visible-range",
          at: syncAt,
          paneId: lastVisibleRange.paneId,
          paneTitle: lastVisibleRange.paneTitle,
        });
      });
    });

    return () => {
      if (visibleRangeSyncFrameRef.current !== null) {
        window.cancelAnimationFrame(visibleRangeSyncFrameRef.current);
        visibleRangeSyncFrameRef.current = null;
      }
    };
  }, [lastVisibleRange, layout.panels, lockedPaneIdSet, visibleRangeSyncEnabled]);

  const mountedPaneCount = useMemo(
    () => layout.panels.filter((panel) => paneSnapshots[panel.id]?.mounted).length,
    [layout.panels, paneSnapshots],
  );

  const connectedPaneCount = useMemo(
    () => layout.panels.filter((panel) => paneSnapshots[panel.id]?.bridgeConnected).length,
    [layout.panels, paneSnapshots],
  );
  const multiPaneSyncAvailable = layout.slotCount > 1;

  const mainPanel = useMemo(
    () => layout.panels.find((panel) => panel.id === mainPaneId) ?? layout.panels[0] ?? null,
    [layout.panels, mainPaneId],
  );

  const primarySnapshot = useMemo(
    () => pickPrimarySnapshot(layout, paneSnapshots, mainPanel?.id ?? null),
    [layout, mainPanel?.id, paneSnapshots],
  );
  const primaryQuality = useMemo(
    () => pickPrimaryQuality(layout, paneQualities, mainPanel?.id ?? null),
    [layout, mainPanel?.id, paneQualities],
  );

  const lockedPaneTitles = useMemo(
    () => layout.panels.filter((panel) => lockedPaneIdSet.has(panel.id)).map((panel) => panel.title),
    [layout.panels, lockedPaneIdSet],
  );

  const barsReadyPaneCount = useMemo(
    () => layout.panels.filter((panel) => {
      const state = paneStateMap[panel.id];
      return !state?.barsError && ((state?.barsPayload?.bar_count ?? 0) > 0);
    }).length,
    [layout.panels, paneStateMap],
  );

  const markerReadyPaneCount = useMemo(
    () => layout.panels.filter((panel) => {
      const state = paneStateMap[panel.id];
      const markerCount = (state?.snapshot?.annotationCount ?? 0) + (state?.snapshot?.tradeMarkCount ?? 0);
      return !state?.markerError && !state?.markerLoading && markerCount > 0;
    }).length,
    [layout.panels, paneStateMap],
  );

  const structuredPaneCount = useMemo(
    () => layout.panels.filter((panel) => {
      const state = paneStateMap[panel.id];
      return (state?.structureCount ?? 0) > 0 || (state?.signalCount ?? 0) > 0;
    }).length,
    [layout.panels, paneStateMap],
  );

  const crosshairAlignment = useMemo<WorkbenchChartCrosshairAlignmentSnapshot | null>(() => {
    if (!lastCrosshair) {
      return null;
    }
    const sourcePanel = layout.panels.find((panel) => panel.id === lastCrosshair.paneId);
    const sourceTimestamp = lastCrosshair.payload.candle?.timestamp ?? null;
    return {
      sourcePaneTitle: lastCrosshair.paneTitle,
      sourceInterval: sourcePanel?.interval ?? null,
      sourceTime: lastCrosshair.payload.candle?.time ?? null,
      targets: layout.panels
        .filter((panel) => panel.id !== lastCrosshair.paneId && !lockedPaneIdSet.has(panel.id))
        .map((panel) => {
          const bars = paneStateMap[panel.id]?.barsPayload?.bars ?? [];
          const nearestBar = findNearestBarMatch(bars, sourceTimestamp);
          return {
            paneId: panel.id,
            paneTitle: panel.title,
            interval: panel.interval,
            nearestBarTime: nearestBar?.time ?? null,
            deltaMs: nearestBar?.deltaMs ?? null,
          };
        }),
    };
  }, [lastCrosshair, layout.panels, lockedPaneIdSet, paneStateMap]);

  const rangeAlignment = useMemo<WorkbenchChartRangeAlignmentSnapshot | null>(() => {
    if (!lastVisibleRange) {
      return null;
    }
    const sourcePanel = layout.panels.find((panel) => panel.id === lastVisibleRange.paneId);
    const sourceFromTimestamp = lastVisibleRange.payload.firstCandle?.timestamp ?? null;
    const sourceToTimestamp = lastVisibleRange.payload.lastCandle?.timestamp ?? null;
    return {
      sourcePaneTitle: lastVisibleRange.paneTitle,
      sourceInterval: sourcePanel?.interval ?? null,
      sourceFromTime: lastVisibleRange.payload.firstCandle?.time ?? null,
      sourceToTime: lastVisibleRange.payload.lastCandle?.time ?? null,
      targets: layout.panels
        .filter((panel) => panel.id !== lastVisibleRange.paneId && !lockedPaneIdSet.has(panel.id))
        .map((panel) => {
          const bars = paneStateMap[panel.id]?.barsPayload?.bars ?? [];
          const nearestFromBar = findNearestBarMatch(bars, sourceFromTimestamp);
          const nearestToBar = findNearestBarMatch(bars, sourceToTimestamp);
          return {
            paneId: panel.id,
            paneTitle: panel.title,
            interval: panel.interval,
            fromBarTime: nearestFromBar?.time ?? null,
            fromDeltaMs: nearestFromBar?.deltaMs ?? null,
            toBarTime: nearestToBar?.time ?? null,
            toDeltaMs: nearestToBar?.deltaMs ?? null,
          };
        }),
    };
  }, [lastVisibleRange, layout.panels, lockedPaneIdSet, paneStateMap]);

  const bridgeSnapshot = useMemo<WorkbenchChartBridgeSnapshot | null>(() => ({
    paneCount: layout.slotCount,
    mountedPaneCount,
    connectedPaneCount,
    focusedPaneId,
    focusedPaneTitle: layout.panels.find((panel) => panel.id === focusedPaneId)?.title ?? null,
    mainPaneId: mainPanel?.id ?? null,
    mainPaneTitle: mainPanel?.title ?? null,
    mainPaneInterval: mainPanel?.interval ?? null,
    lastEventType: lastBridgeEvent?.event.type ?? null,
    lastEventAt: lastBridgeEvent?.event.emittedAt ?? null,
    eventCounts: bridgeEventCounts,
    lastClick,
    lastCrosshair,
    lastVisibleRange,
    syncMode,
    followSourceEnabled,
    crosshairSyncEnabled,
    visibleRangeSyncEnabled,
    lockedPaneIds: [...lockedPaneIds],
    lockedPaneTitles,
    syncDispatchCounts,
    lastSyncType: lastSync?.type ?? null,
    lastSyncAt: lastSync?.at ?? null,
    lastSyncPaneTitle: lastSync?.paneTitle ?? null,
    barsReadyPaneCount,
    markerReadyPaneCount,
    structuredPaneCount,
    crosshairAlignment,
    rangeAlignment,
  }), [barsReadyPaneCount, bridgeEventCounts, connectedPaneCount, crosshairAlignment, crosshairSyncEnabled, focusedPaneId, followSourceEnabled, lastBridgeEvent, lastClick, lastCrosshair, lastSync, lastVisibleRange, layout, lockedPaneIds, lockedPaneTitles, mainPanel, markerReadyPaneCount, mountedPaneCount, rangeAlignment, structuredPaneCount, syncDispatchCounts, syncMode, visibleRangeSyncEnabled]);

  const performanceSnapshot = useMemo<WorkbenchChartPerformanceSnapshot | null>(() => {
    const metrics = Object.values(panePerformance).filter((item): item is PanePerformanceMetrics => item !== null);
    const lastMetric = metrics.reduce<PanePerformanceMetrics | null>((best, item) => {
      if (!best || item.updatedAt > best.updatedAt) {
        return item;
      }
      return best;
    }, null);
    const maxMarkerApplyMs = metrics.reduce<number | null>((best, item) => {
      if (item.markerApplyMs === null) {
        return best;
      }
      if (best === null || item.markerApplyMs > best) {
        return item.markerApplyMs;
      }
      return best;
    }, null);

    return {
      paneCount: layout.slotCount,
      mountedPaneCount,
      lazyPendingCount: Math.max(0, layout.slotCount - activatedPaneIds.length),
      stressTargetCount,
      lastUpdatedAt: lastMetric?.updatedAt ?? null,
      lastPaneId: lastMetric?.panelId ?? null,
      lastMarkerApplyMs: lastMetric?.markerApplyMs ?? null,
      lastStressDurationMs: lastMetric?.stressDurationMs ?? null,
      maxMarkerApplyMs,
    };
  }, [activatedPaneIds.length, layout.slotCount, mountedPaneCount, panePerformance, stressTargetCount]);

  useEffect(() => {
    props.onSnapshotChange?.(primarySnapshot);
  }, [primarySnapshot, props.onSnapshotChange]);

  useEffect(() => {
    props.onQualityChange?.(primaryQuality);
  }, [primaryQuality, props.onQualityChange]);

  useEffect(() => {
    props.onBridgeStateChange?.(bridgeSnapshot);
  }, [bridgeSnapshot, props.onBridgeStateChange]);

  useEffect(() => {
    props.onPerformanceChange?.(performanceSnapshot);
  }, [performanceSnapshot, props.onPerformanceChange]);

  const focusedPanelState = focusedPaneId ? paneStateMap[focusedPaneId] : null;
  const primaryRuntimeTone = getRuntimeTone(primarySnapshot?.status);
  const primaryQualityTone = getQualityTone(primaryQuality?.golden_status);
  const primaryRepairTone = getRepairTone(primaryQuality?.repair.plan_status);

  const summaryItems = [
    ["布局", layout.presetId, "neutral"],
    ["主 pane", displayValue(mainPanel?.title), mainPanel ? "ok" : "neutral"],
    ["主周期", displayValue(mainPanel?.interval ?? props.interval), primaryRuntimeTone],
    ["pane 数", String(layout.slotCount), "neutral"],
    ["已挂载", String(mountedPaneCount), mountedPaneCount === layout.slotCount ? "ok" : "warning"],
    ["bridge 已连", String(connectedPaneCount), connectedPaneCount > 0 ? "ok" : "warning"],
    ["sync 模式", multiPaneSyncAvailable ? getSyncModeLabel(syncMode) : "单图", multiPaneSyncAvailable && syncMode !== "off" ? "ok" : "neutral"],
    ["跟随源切换", multiPaneSyncAvailable ? (followSourceEnabled ? "开" : "关") : "单图", multiPaneSyncAvailable && followSourceEnabled ? "ok" : "neutral"],
    ["锁定 pane", String(lockedPaneIds.length), lockedPaneIds.length ? "warning" : "neutral"],
    ["Golden 1D", displayValue(primaryQuality?.golden_status), primaryQualityTone],
    ["Repair", displayValue(primaryQuality?.repair.plan_status), primaryRepairTone],
    ["压测量", stressTargetCount ? String(stressTargetCount) : "关闭", stressTargetCount > 0 ? "warning" : "neutral"],
  ] as Array<[string, string, UiTone]>;

  return (
    <div className="workbench-chart-stage full-height">
      <div className="summary-card-grid chart-stage-summary-grid">
        {summaryItems.map(([label, value, tone]) => (
          <div key={label} className={`status-chip ${tone === "neutral" ? "" : tone}`}>
            {label}: <strong>{value}</strong>
          </div>
        ))}
      </div>

      <div className="workbench-chart-stage-toolbar">
        <div className="chart-runtime-pill-row">
          {SYNC_MODE_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              className={`chip-button ${syncMode === option.value ? "active" : ""}`}
              disabled={!multiPaneSyncAvailable}
              onClick={() => setSyncMode(option.value)}
            >
              {option.label}
            </button>
          ))}
          <button
            type="button"
            className={`chip-button ${followSourceEnabled ? "active" : ""}`}
            disabled={!multiPaneSyncAvailable}
            onClick={() => setFollowSourceEnabled((current) => !current)}
          >
            主 pane 跟随源 {followSourceEnabled ? "开" : "关"}
          </button>
          {STRESS_OPTIONS.map((option) => (
            <button
              key={option}
              type="button"
              className={`chip-button ${stressTargetCount === option ? "active" : ""}`}
              onClick={() => setStressTargetCount(option)}
            >
              {option === 0 ? "关闭压测" : `压测 ${option}`}
            </button>
          ))}
        </div>
        <div className="inline-note">
          事件桥现在已经升级为交易台控制层：支持 pane 锁定/解锁、主 pane 跟随源切换，以及仅准星 / 仅视区 / 全同步三种核心联动模式。
        </div>
      </div>

      <div className={`workbench-chart-pane-grid ${layout.layoutClassName}`}>
        {layout.panels.map((panel, index) => (
          <WorkbenchChartPane
            key={`${panel.id}-${panel.interval}-${layout.presetId}`}
            panel={panel}
            panelIndex={index}
            symbol={props.symbol}
            layoutPresetId={layout.presetId}
            theme={props.theme ?? "dark"}
            reloadToken={props.reloadToken ?? 0}
            shouldMount={activatedPaneIds.includes(panel.id)}
            stressTargetCount={stressTargetCount}
            focused={focusedPaneId === panel.id}
            isMainPane={mainPanel?.id === panel.id}
            locked={lockedPaneIdSet.has(panel.id)}
            onRequestMount={requestMountPane}
            onFocusPane={setFocusedPaneId}
            onPromoteMainPane={promoteMainPane}
            onTogglePaneLock={togglePaneLock}
            onPaneStateChange={handlePaneStateChange}
            onPaneSnapshotChange={handlePaneSnapshotChange}
            onPaneQualityChange={handlePaneQualityChange}
            onPaneBridgeEvent={handlePaneBridgeEvent}
            onPanePerformanceChange={handlePanePerformanceChange}
            onPaneControllerChange={handlePaneControllerChange}
          />
        ))}
      </div>

      <div className="chart-stage-detail-grid">
        <section className="chart-stage-card">
          <h4>事件桥回写</h4>
          <ul className="metric-list large">
            <li><span>focused pane</span><strong>{displayValue(bridgeSnapshot?.focusedPaneTitle)}</strong></li>
            <li><span>main pane</span><strong>{displayValue(bridgeSnapshot?.mainPaneTitle)}</strong></li>
            <li><span>last event</span><strong>{displayValue(bridgeSnapshot?.lastEventType)}</strong></li>
            <li><span>event at</span><strong>{formatRuntimeTime(bridgeSnapshot?.lastEventAt)}</strong></li>
            <li><span>click count</span><strong>{displayValue(bridgeSnapshot?.eventCounts["chart-click"])}</strong></li>
            <li><span>crosshair count</span><strong>{displayValue(bridgeSnapshot?.eventCounts.crosshair)}</strong></li>
            <li><span>range count</span><strong>{displayValue(bridgeSnapshot?.eventCounts["visible-range"])}</strong></li>
            <li><span>sync mode</span><strong>{displayValue(bridgeSnapshot ? getSyncModeLabel(bridgeSnapshot.syncMode) : null)}</strong></li>
            <li><span>follow source</span><strong>{bridgeSnapshot?.followSourceEnabled ? "enabled" : multiPaneSyncAvailable ? "disabled" : "single-pane"}</strong></li>
            <li><span>crosshair sync</span><strong>{bridgeSnapshot?.crosshairSyncEnabled ? "enabled" : multiPaneSyncAvailable ? "disabled" : "single-pane"}</strong></li>
            <li><span>range sync</span><strong>{bridgeSnapshot?.visibleRangeSyncEnabled ? "enabled" : multiPaneSyncAvailable ? "disabled" : "single-pane"}</strong></li>
            <li><span>last sync</span><strong>{displayValue(bridgeSnapshot?.lastSyncType)}</strong></li>
            <li><span>sync at</span><strong>{formatRuntimeTime(bridgeSnapshot?.lastSyncAt)}</strong></li>
          </ul>
          <div className="chart-capability-grid">
            <span className={`status-chip ${bridgeSnapshot?.crosshairSyncEnabled ? "ok" : "warning"}`}>
              准星联动 · {bridgeSnapshot?.crosshairSyncEnabled ? "on" : multiPaneSyncAvailable ? "off" : "single"} · {displayValue(bridgeSnapshot?.syncDispatchCounts.crosshair)} 次
            </span>
            <span className={`status-chip ${bridgeSnapshot?.visibleRangeSyncEnabled ? "ok" : "warning"}`}>
              视区联动 · {bridgeSnapshot?.visibleRangeSyncEnabled ? "on" : multiPaneSyncAvailable ? "off" : "single"} · {displayValue(bridgeSnapshot?.syncDispatchCounts["visible-range"])} 次
            </span>
            {bridgeSnapshot?.lastSyncType ? (
              <span className="status-chip ok">
                sync · {displayValue(bridgeSnapshot.lastSyncPaneTitle)} · {bridgeSnapshot.lastSyncType}
              </span>
            ) : null}
            {bridgeSnapshot?.lastClick ? (
              <span className="status-chip ok">
                click · {bridgeSnapshot.lastClick.paneTitle} · idx {displayValue(bridgeSnapshot.lastClick.payload.dataIndex)} · {displayValue(bridgeSnapshot.lastClick.payload.candle?.close)}
              </span>
            ) : null}
            {bridgeSnapshot?.lastCrosshair ? (
              <span className="status-chip warning">
                crosshair · {bridgeSnapshot.lastCrosshair.paneTitle} · idx {displayValue(bridgeSnapshot.lastCrosshair.payload.dataIndex)} · {displayValue(bridgeSnapshot.lastCrosshair.payload.candle?.close)}
              </span>
            ) : null}
            {bridgeSnapshot?.lastVisibleRange ? (
              <span className="status-chip">
                range · {bridgeSnapshot.lastVisibleRange.paneTitle} · {bridgeSnapshot.lastVisibleRange.payload.realFrom} → {bridgeSnapshot.lastVisibleRange.payload.realTo}
              </span>
            ) : null}
            {bridgeSnapshot?.lastClick || bridgeSnapshot?.lastCrosshair || bridgeSnapshot?.lastVisibleRange ? null : (
              <span className="status-chip">等待用户交互触发事件桥</span>
            )}
          </div>
        </section>

        <section className="chart-stage-card">
          <h4>多图 / Lazy Mount</h4>
          <ul className="metric-list large">
            <li><span>layout preset</span><strong>{layout.presetId}</strong></li>
            <li><span>slot count</span><strong>{layout.slotCount}</strong></li>
            <li><span>activated panes</span><strong>{activatedPaneIds.length}</strong></li>
            <li><span>mounted panes</span><strong>{mountedPaneCount}</strong></li>
            <li><span>pending lazy panes</span><strong>{displayValue(performanceSnapshot?.lazyPendingCount)}</strong></li>
            <li><span>focused pane</span><strong>{displayValue(bridgeSnapshot?.focusedPaneTitle)}</strong></li>
            <li><span>main pane</span><strong>{displayValue(bridgeSnapshot?.mainPaneTitle)}</strong></li>
            <li><span>locked panes</span><strong>{displayValue(bridgeSnapshot?.lockedPaneTitles.length)}</strong></li>
            <li><span>last sync pane</span><strong>{displayValue(bridgeSnapshot?.lastSyncPaneTitle)}</strong></li>
          </ul>
          <div className="chart-capability-grid">
            {layout.panels.map((panel) => (
              <span
                key={panel.id}
                className={`status-chip ${mainPanel?.id === panel.id ? "ok" : lockedPaneIdSet.has(panel.id) ? "warning" : activatedPaneIds.includes(panel.id) ? "ok" : "warning"}`}
              >
                {panel.title} · {panel.interval} · {mainPanel?.id === panel.id ? "main" : lockedPaneIdSet.has(panel.id) ? "locked" : activatedPaneIds.includes(panel.id) ? "mounted" : "lazy"}
              </span>
            ))}
            {(bridgeSnapshot?.lockedPaneTitles ?? []).map((title) => (
              <span key={`locked-${title}`} className="status-chip warning">
                lock · {title}
              </span>
            ))}
          </div>
        </section>

        <section className="chart-stage-card">
          <h4>真数据对齐 / 标注验证</h4>
          <ul className="metric-list large">
            <li><span>bars ready panes</span><strong>{displayValue(bridgeSnapshot?.barsReadyPaneCount)}</strong></li>
            <li><span>marker ready panes</span><strong>{displayValue(bridgeSnapshot?.markerReadyPaneCount)}</strong></li>
            <li><span>structured panes</span><strong>{displayValue(bridgeSnapshot?.structuredPaneCount)}</strong></li>
            <li><span>crosshair source</span><strong>{displayValue(bridgeSnapshot?.crosshairAlignment?.sourcePaneTitle)}</strong></li>
            <li><span>crosshair at</span><strong>{displayValue(bridgeSnapshot?.crosshairAlignment?.sourceTime)}</strong></li>
            <li><span>range source</span><strong>{displayValue(bridgeSnapshot?.rangeAlignment?.sourcePaneTitle)}</strong></li>
            <li><span>range from</span><strong>{displayValue(bridgeSnapshot?.rangeAlignment?.sourceFromTime)}</strong></li>
            <li><span>range to</span><strong>{displayValue(bridgeSnapshot?.rangeAlignment?.sourceToTime)}</strong></li>
          </ul>
          <div className="chart-capability-grid">
            {(bridgeSnapshot?.crosshairAlignment?.targets ?? []).map((item) => (
              <span key={`crosshair-align-${item.paneId}`} className={`status-chip ${item.deltaMs !== null && item.deltaMs <= 60_000 ? "ok" : "warning"}`}>
                准星 · {item.paneTitle} · {item.interval} · {displayValue(item.nearestBarTime)} · Δ {formatAlignmentDelta(item.deltaMs)}
              </span>
            ))}
            {(bridgeSnapshot?.rangeAlignment?.targets ?? []).map((item) => (
              <span key={`range-align-${item.paneId}`} className={`status-chip ${item.toDeltaMs !== null && item.toDeltaMs <= 86_400_000 ? "ok" : "warning"}`}>
                视区 · {item.paneTitle} · {item.interval} · ΔF {formatAlignmentDelta(item.fromDeltaMs)} · ΔT {formatAlignmentDelta(item.toDeltaMs)}
              </span>
            ))}
            {bridgeSnapshot?.crosshairAlignment?.targets.length || bridgeSnapshot?.rangeAlignment?.targets.length ? null : (
              <span className="status-chip">等待真实 bars 交互后生成对齐探针</span>
            )}
          </div>
        </section>

        <section className="chart-stage-card">
          <h4>标注量压测</h4>
          <ul className="metric-list large">
            <li><span>stress target</span><strong>{displayValue(performanceSnapshot?.stressTargetCount)}</strong></li>
            <li><span>last marker apply</span><strong>{formatDuration(performanceSnapshot?.lastMarkerApplyMs)}</strong></li>
            <li><span>last stress run</span><strong>{formatDuration(performanceSnapshot?.lastStressDurationMs)}</strong></li>
            <li><span>max marker apply</span><strong>{formatDuration(performanceSnapshot?.maxMarkerApplyMs)}</strong></li>
            <li><span>updated at</span><strong>{formatRuntimeTime(performanceSnapshot?.lastUpdatedAt)}</strong></li>
          </ul>
          <div className="inline-note chart-stage-note">
            压测通过真实 `setAnnotations / setTradeMarks` 落到 Pro runtime；不是空跑计时器，而是实际把标注批量打进图表。
          </div>
        </section>

        <section className="chart-stage-card">
          <h4>当前焦点 pane</h4>
          <ul className="metric-list large">
            <li><span>runtime</span><strong>{displayValue(focusedPanelState?.snapshot?.status)}</strong></li>
            <li><span>is main</span><strong>{focusedPaneId && mainPanel?.id === focusedPaneId ? "yes" : "no"}</strong></li>
            <li><span>is locked</span><strong>{focusedPaneId && lockedPaneIdSet.has(focusedPaneId) ? "yes" : "no"}</strong></li>
            <li><span>bars</span><strong>{displayValue(focusedPanelState?.barsPayload?.bar_count ?? focusedPanelState?.snapshot?.barCount)}</strong></li>
            <li><span>structures</span><strong>{displayValue(focusedPanelState?.structureCount)}</strong></li>
            <li><span>signals</span><strong>{displayValue(focusedPanelState?.signalCount)}</strong></li>
            <li><span>bridge</span><strong>{displayValue(focusedPanelState?.snapshot?.bridgeConnected ? "connected" : "waiting")}</strong></li>
            <li><span>adapter</span><strong>{displayValue(focusedPanelState?.snapshot?.adapter.label)}</strong></li>
          </ul>
          <div className="chart-capability-grid">
            {focusedPanelState?.barsError ? <span className="status-chip danger">bars · {focusedPanelState.barsError}</span> : null}
            {focusedPanelState?.markerError ? <span className="status-chip danger">markers · {focusedPanelState.markerError}</span> : null}
            {focusedPanelState?.stageError ? <span className="status-chip danger">runtime · {focusedPanelState.stageError}</span> : null}
            {focusedPanelState?.barsLoading ? <span className="status-chip warning">bars loading</span> : null}
            {focusedPanelState?.markerLoading ? <span className="status-chip warning">markers loading</span> : null}
            {focusedPanelState?.barsError || focusedPanelState?.markerError || focusedPanelState?.stageError || focusedPanelState?.barsLoading || focusedPanelState?.markerLoading ? null : (
              <span className="status-chip ok">当前 pane 运行平稳</span>
            )}
          </div>
        </section>
      </div>

      <div className="chart-runtime-stage workbench-chart-runtime-summary">
        <div className="chart-runtime-overlay">
          <div className="chart-runtime-topline">
            <span className={getToneClassName(primaryRuntimeTone)}>
              {getToneMarker(primaryRuntimeTone)} {displayValue(primarySnapshot?.status ?? "idle")}
            </span>
            <span className="mono-text">Facade {CHART_FACADE_V2_DRAFT_VERSION}</span>
          </div>
          <div className="chart-runtime-empty">
            <strong>专业图表 Workbench 已进入多 pane 运行态</strong>
            <p>
              当前主 runtime 目标为 {CHART_FACADE_V2_TARGET_RUNTIME}；click / crosshair / visible range
              已回写到 Tauri 壳，同时支持主 pane 跟随源切换、pane 锁定/解锁、真实 bars 对齐探针与多图懒挂载。
            </p>
            <div className="chart-runtime-pill-row">
              <span className="status-chip">symbol: {props.symbol}</span>
              <span className="status-chip">base interval: {props.interval}</span>
              <span className="status-chip">main pane: {displayValue(mainPanel?.title)}</span>
              <span className="status-chip">sync: {getSyncModeLabel(syncMode)}</span>
              <span className={`status-chip ${primaryQualityTone === "neutral" ? "" : primaryQualityTone}`}>
                Golden: {displayValue(primaryQuality?.golden_status)}
              </span>
              <span className={`status-chip ${primaryRepairTone === "neutral" ? "" : primaryRepairTone}`}>
                Repair: {displayValue(primaryQuality?.repair.plan_status)}
              </span>
              <span className={`status-chip ${connectedPaneCount > 0 ? "ok" : "warning"}`}>
                bridge panes: {connectedPaneCount}/{layout.slotCount}
              </span>
            </div>
          </div>
          <div className="chart-runtime-bottomline">
            <span>焦点：{displayValue(bridgeSnapshot?.focusedPaneTitle)} · 主 pane：{displayValue(bridgeSnapshot?.mainPaneTitle)}</span>
            <span>{lastBridgeEvent?.panelTitle ? `${lastBridgeEvent.panelTitle} · ${lastBridgeEvent.event.type}` : "等待交互事件"}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
