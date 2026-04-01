import { Suspense, lazy, useCallback, useEffect, useMemo, useState } from "react";

import {
  CHART_FACADE_V2_TARGET_RUNTIME,
  type ChartInterval,
  type ChartRuntimeSnapshot,
  type ChartRuntimeStatus,
} from "../chart/chartFacadeV2";
import type {
  WorkbenchChartBridgeSnapshot,
  WorkbenchChartPerformanceSnapshot,
  WorkbenchChartQualitySnapshot,
} from "../components/chart/WorkbenchChartStage";
import {
  fetchIngestionGateStatus,
  triggerGolden1dRepair,
  type Golden1dRepairTriggerPayload,
  type IngestionGateStatusPayload,
} from "../lib/api";
import { WORKSPACE_PRESETS } from "../lib/navigation";
import { getWorkbenchLinkEventName, readWorkbenchLinkPayload } from "../lib/routeBridge";
import { getToneClassName, getToneMarker, type UiTone } from "../lib/uiTone";
import { WorkbenchShell } from "../components/layout/WorkbenchShell";

const WorkbenchChartStage = lazy(async () => import("../components/chart/WorkbenchChartStage").then((module) => ({
  default: module.WorkbenchChartStage,
})));

interface WatchlistItem {
  symbol: string;
  market: string;
  note: string;
}

const WATCHLIST_ITEMS: WatchlistItem[] = [
  { symbol: "000001.SZ", market: "CN Equity", note: "A 股日线 / Golden 1D / 结构标注" },
  { symbol: "159981.SZ", market: "ETF", note: "宽基 / 行业联动" },
  { symbol: "BTC/USDT", market: "Crypto", note: "主舞台验证标的" },
  { symbol: "XAUUSD", market: "Macro", note: "贵金属图表入口" },
];

const INTERVAL_OPTIONS: ChartInterval[] = ["1m", "15m", "1h", "1d", "1w"];
const TOOLBAR_GROUPS = ["指标", "叠加", "结构", "风控"] as const;

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

function formatRuntimeDuration(value: number | null | undefined): string {
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

function getSyncModeLabel(mode: WorkbenchChartBridgeSnapshot["syncMode"] | null | undefined): string {
  switch (mode) {
    case "crosshair":
      return "仅准星";
    case "visible-range":
      return "仅视区";
    case "off":
      return "关闭同步";
    case "full":
      return "全同步";
    default:
      return "—";
  }
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

function getQualityTone(status: WorkbenchChartQualitySnapshot["golden_status"] | undefined): UiTone {
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

function getGateTone(status: IngestionGateStatusPayload["quality_grade"] | undefined | null): UiTone {
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

export function WorkbenchRoute() {
  const initialWorkbenchLink = readWorkbenchLinkPayload();
  const [selectedSymbol, setSelectedSymbol] = useState<string>(initialWorkbenchLink?.symbol ?? WATCHLIST_ITEMS[0].symbol);
  const [selectedInterval, setSelectedInterval] = useState<ChartInterval>(initialWorkbenchLink?.interval ?? "1d");
  const [selectedLayoutPreset, setSelectedLayoutPreset] = useState<string>(WORKSPACE_PRESETS[0]);
  const [activeToolGroup, setActiveToolGroup] = useState<(typeof TOOLBAR_GROUPS)[number]>("结构");
  const [stageSnapshot, setStageSnapshot] = useState<ChartRuntimeSnapshot | null>(null);
  const [chartQuality, setChartQuality] = useState<WorkbenchChartQualitySnapshot | null>(null);
  const [bridgeState, setBridgeState] = useState<WorkbenchChartBridgeSnapshot | null>(null);
  const [performanceState, setPerformanceState] = useState<WorkbenchChartPerformanceSnapshot | null>(null);
  const [repairTriggerState, setRepairTriggerState] = useState<Golden1dRepairTriggerPayload | null>(null);
  const [repairActionError, setRepairActionError] = useState<string | null>(null);
  const [repairBusy, setRepairBusy] = useState<"standard" | "force" | null>(null);
  const [repairReloadToken, setRepairReloadToken] = useState(0);
  const [ingestionGateStatus, setIngestionGateStatus] = useState<IngestionGateStatusPayload | null>(null);
  const [ingestionGateError, setIngestionGateError] = useState<string | null>(null);

  const selectedWatchItem = useMemo(
    () => WATCHLIST_ITEMS.find((item) => item.symbol === selectedSymbol) ?? {
      symbol: selectedSymbol,
      market: "Linked",
      note: "治理联动标的",
    },
    [selectedSymbol],
  );

  const runtimeTone = getRuntimeTone(stageSnapshot?.status);
  const qualityTone = getQualityTone(chartQuality?.golden_status);
  const gateTone = getGateTone(ingestionGateStatus?.quality_grade);
  const repairSnapshot = chartQuality?.repair ?? null;
  const repairTone = getRepairTone(repairSnapshot?.plan_status);
  const visibleRepairResult = repairTriggerState?.symbol === selectedSymbol ? repairTriggerState : null;

  const handleTriggerRepair = useCallback(async (forceFull: boolean) => {
    setRepairActionError(null);
    setRepairBusy(forceFull ? "force" : "standard");
    try {
      const payload = await triggerGolden1dRepair(selectedSymbol, forceFull);
      setRepairTriggerState(payload);
      setRepairReloadToken((current) => current + 1);
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : "Golden 1D repair 触发失败";
      setRepairActionError(message);
    } finally {
      setRepairBusy(null);
    }
  }, [selectedSymbol]);

  useEffect(() => {
    function handleWorkbenchLink(event: Event) {
      const detail = (event as CustomEvent<{ symbol?: string; interval?: ChartInterval }>).detail;
      if (!detail?.symbol) {
        return;
      }
      setSelectedSymbol(String(detail.symbol));
      if (detail.interval) {
        setSelectedInterval(detail.interval);
      }
    }
    window.addEventListener(getWorkbenchLinkEventName(), handleWorkbenchLink as EventListener);
    return () => {
      window.removeEventListener(getWorkbenchLinkEventName(), handleWorkbenchLink as EventListener);
    };
  }, []);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    async function loadGateStatus() {
      setIngestionGateError(null);
      try {
        const payload = await fetchIngestionGateStatus(selectedSymbol, selectedInterval, controller.signal);
        if (!active) {
          return;
        }
        setIngestionGateStatus(payload);
      } catch (reason) {
        if (!active || controller.signal.aborted) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "数据状态条加载失败";
        setIngestionGateError(message);
        setIngestionGateStatus(null);
      }
    }

    void loadGateStatus();
    return () => {
      active = false;
      controller.abort();
    };
  }, [selectedInterval, selectedSymbol]);

  return (
    <WorkbenchShell
      title="交易工作台"
      subtitle="以专业图表为中心舞台：Workbench 图表 adapter 已接入主容器，当前聚焦多图布局、事件桥与交易控件联动。"
      leftRail={
        <div className="stack-gap">
          <section>
            <h3>自选 / 搜索</h3>
            <div className="inline-note">
              真实 watchlist / 搜索读模型尚未接线；当前先用主舞台验证标的驱动图表容器与 adapter 运行态。
            </div>
            <div className="route-list">
              {WATCHLIST_ITEMS.map((item) => (
                <button
                  key={item.symbol}
                  type="button"
                  className={`route-button ${selectedSymbol === item.symbol ? "active" : ""}`}
                  onClick={() => setSelectedSymbol(item.symbol)}
                >
                  <strong>{item.symbol}</strong>
                  <span>{item.market}</span>
                  <span>{item.note}</span>
                </button>
              ))}
            </div>
          </section>
          <section>
            <h3>工作区预设</h3>
            <div className="chip-grid">
              {WORKSPACE_PRESETS.map((item) => (
                <button
                  key={item}
                  className={`chip-button ${selectedLayoutPreset === item ? "active" : ""}`}
                  type="button"
                  onClick={() => setSelectedLayoutPreset(item)}
                >
                  {item}
                </button>
              ))}
            </div>
            <div className="inline-note">
              当前主舞台已经能感知 layout preset，上层布局持久化将在图表工作区存储完成后接入。
            </div>
          </section>
        </div>
      }
      mainPane={
        <div className="stack-gap full-height">
          <div className="chart-toolbar">
            <span className="symbol-pill">{selectedSymbol}</span>
            <div className="toolbar-segment">
              {INTERVAL_OPTIONS.map((item) => (
                <button
                  key={item}
                  type="button"
                  className={selectedInterval === item ? "active" : ""}
                  onClick={() => setSelectedInterval(item)}
                >
                  {item}
                </button>
              ))}
            </div>
            <div className="toolbar-segment">
              {TOOLBAR_GROUPS.map((item) => (
                <button
                  key={item}
                  type="button"
                  className={activeToolGroup === item ? "active" : ""}
                  onClick={() => setActiveToolGroup(item)}
                >
                  {item}
                </button>
              ))}
            </div>
          </div>
          <Suspense fallback={<div className="status-chip warning">图表工作区组件分块加载中…</div>}>
            <WorkbenchChartStage
              symbol={selectedSymbol}
              interval={selectedInterval}
              layoutPresetId={selectedLayoutPreset}
              reloadToken={repairReloadToken}
              onSnapshotChange={setStageSnapshot}
              onQualityChange={setChartQuality}
              onBridgeStateChange={setBridgeState}
              onPerformanceChange={setPerformanceState}
            />
          </Suspense>
        </div>
      }
      rightRail={
        <div className="stack-gap">
          <section>
            <h3>交易面板</h3>
            <div className="form-grid">
              <label>
                <span>账户</span>
                <input value="待接入真实账户读模型" readOnly />
              </label>
              <label>
                <span>标的</span>
                <input value={selectedWatchItem.symbol} readOnly />
              </label>
              <label>
                <span>主 pane</span>
                <input value={bridgeState?.mainPaneTitle ?? "等待主舞台回写"} readOnly />
              </label>
              <label>
                <span>交易周期</span>
                <input value={bridgeState?.mainPaneInterval ?? selectedInterval} readOnly />
              </label>
            </div>
            <div className="action-row">
              <button type="button" className="primary">买入</button>
              <button type="button" className="danger">卖出</button>
            </div>
            <div className="inline-note">交易控件现在跟随主 pane 视角；后续再把账户 / 实时报价 / 风控真链路补到这一层。</div>
          </section>
          <section>
            <h3>图表运行态</h3>
            <ul className="metric-list">
              <li>
                <span>runtime status</span>
                <strong className={getToneClassName(runtimeTone)}>
                  {getToneMarker(runtimeTone)} {displayValue(stageSnapshot?.status)}
                </strong>
              </li>
              <li><span>active adapter</span><strong>{displayValue(stageSnapshot?.adapter.label)}</strong></li>
              <li><span>target runtime</span><strong>{CHART_FACADE_V2_TARGET_RUNTIME}</strong></li>
              <li><span>bars</span><strong>{displayValue(stageSnapshot?.barCount)}</strong></li>
              <li><span>annotations</span><strong>{displayValue(stageSnapshot?.annotationCount)}</strong></li>
              <li><span>trade marks</span><strong>{displayValue(stageSnapshot?.tradeMarkCount)}</strong></li>
              <li><span>layout preset</span><strong>{displayValue(stageSnapshot?.layoutPresetId ?? selectedLayoutPreset)}</strong></li>
              <li><span>toolbar focus</span><strong>{activeToolGroup}</strong></li>
              <li><span>main pane</span><strong>{displayValue(bridgeState?.mainPaneTitle)}</strong></li>
              <li><span>sync mode</span><strong>{getSyncModeLabel(bridgeState?.syncMode)}</strong></li>
              <li><span>bridge panes</span><strong>{displayValue(bridgeState?.connectedPaneCount)}</strong></li>
              <li><span>last bridge event</span><strong>{displayValue(bridgeState?.lastEventType)}</strong></li>
            </ul>
          </section>
          <section>
            <h3>事件桥状态</h3>
            <ul className="metric-list">
              <li><span>focused pane</span><strong>{displayValue(bridgeState?.focusedPaneTitle)}</strong></li>
              <li><span>main pane</span><strong>{displayValue(bridgeState?.mainPaneTitle)}</strong></li>
              <li><span>main interval</span><strong>{displayValue(bridgeState?.mainPaneInterval)}</strong></li>
              <li><span>event at</span><strong>{formatRuntimeTime(bridgeState?.lastEventAt)}</strong></li>
              <li><span>click count</span><strong>{displayValue(bridgeState?.eventCounts["chart-click"])}</strong></li>
              <li><span>crosshair count</span><strong>{displayValue(bridgeState?.eventCounts.crosshair)}</strong></li>
              <li><span>range count</span><strong>{displayValue(bridgeState?.eventCounts["visible-range"])}</strong></li>
              <li><span>sync mode</span><strong>{getSyncModeLabel(bridgeState?.syncMode)}</strong></li>
              <li><span>follow source</span><strong>{bridgeState?.followSourceEnabled ? "enabled" : bridgeState?.paneCount && bridgeState.paneCount > 1 ? "disabled" : "single-pane"}</strong></li>
              <li><span>locked panes</span><strong>{displayValue(bridgeState?.lockedPaneTitles.length)}</strong></li>
              <li><span>crosshair sync</span><strong>{bridgeState?.crosshairSyncEnabled ? "enabled" : bridgeState?.paneCount && bridgeState.paneCount > 1 ? "disabled" : "single-pane"}</strong></li>
              <li><span>range sync</span><strong>{bridgeState?.visibleRangeSyncEnabled ? "enabled" : bridgeState?.paneCount && bridgeState.paneCount > 1 ? "disabled" : "single-pane"}</strong></li>
              <li><span>last sync</span><strong>{displayValue(bridgeState?.lastSyncType)}</strong></li>
              <li><span>sync at</span><strong>{formatRuntimeTime(bridgeState?.lastSyncAt)}</strong></li>
            </ul>
            <div className="chip-grid">
              <span className={`status-chip ${bridgeState?.crosshairSyncEnabled ? "ok" : "warning"}`}>
                准星联动 · {bridgeState?.crosshairSyncEnabled ? "on" : bridgeState?.paneCount && bridgeState.paneCount > 1 ? "off" : "single"} · {displayValue(bridgeState?.syncDispatchCounts.crosshair)} 次
              </span>
              <span className={`status-chip ${bridgeState?.visibleRangeSyncEnabled ? "ok" : "warning"}`}>
                视区联动 · {bridgeState?.visibleRangeSyncEnabled ? "on" : bridgeState?.paneCount && bridgeState.paneCount > 1 ? "off" : "single"} · {displayValue(bridgeState?.syncDispatchCounts["visible-range"])} 次
              </span>
              {bridgeState?.lastSyncType ? (
                <span className="status-chip ok">
                  sync · {displayValue(bridgeState.lastSyncPaneTitle)} · {bridgeState.lastSyncType}
                </span>
              ) : null}
              {bridgeState?.lastClick ? (
                <span className="status-chip ok">
                  click · {bridgeState.lastClick.paneTitle} · idx {displayValue(bridgeState.lastClick.payload.dataIndex)} · close {displayValue(bridgeState.lastClick.payload.candle?.close)}
                </span>
              ) : null}
              {bridgeState?.lastCrosshair ? (
                <span className="status-chip warning">
                  crosshair · {bridgeState.lastCrosshair.paneTitle} · idx {displayValue(bridgeState.lastCrosshair.payload.dataIndex)} · close {displayValue(bridgeState.lastCrosshair.payload.candle?.close)}
                </span>
              ) : null}
              {bridgeState?.lastVisibleRange ? (
                <span className="status-chip">
                  range · {bridgeState.lastVisibleRange.paneTitle} · {bridgeState.lastVisibleRange.payload.realFrom} → {bridgeState.lastVisibleRange.payload.realTo}
                </span>
              ) : null}
              {(bridgeState?.lockedPaneTitles ?? []).map((title) => (
                <span key={`locked-${title}`} className="status-chip warning">
                  locked · {title}
                </span>
              ))}
              {bridgeState?.lastClick || bridgeState?.lastCrosshair || bridgeState?.lastVisibleRange ? null : (
                <span className="status-chip">等待 click / crosshair / visible range 事件回写</span>
              )}
            </div>
          </section>
          <section>
            <h3>真数据联调</h3>
            <ul className="metric-list">
              <li><span>bars ready panes</span><strong>{displayValue(bridgeState?.barsReadyPaneCount)}</strong></li>
              <li><span>marker ready panes</span><strong>{displayValue(bridgeState?.markerReadyPaneCount)}</strong></li>
              <li><span>structured panes</span><strong>{displayValue(bridgeState?.structuredPaneCount)}</strong></li>
              <li><span>crosshair source</span><strong>{displayValue(bridgeState?.crosshairAlignment?.sourcePaneTitle)}</strong></li>
              <li><span>crosshair at</span><strong>{displayValue(bridgeState?.crosshairAlignment?.sourceTime)}</strong></li>
              <li><span>range source</span><strong>{displayValue(bridgeState?.rangeAlignment?.sourcePaneTitle)}</strong></li>
              <li><span>range from</span><strong>{displayValue(bridgeState?.rangeAlignment?.sourceFromTime)}</strong></li>
              <li><span>range to</span><strong>{displayValue(bridgeState?.rangeAlignment?.sourceToTime)}</strong></li>
            </ul>
            <div className="chip-grid">
              {(bridgeState?.crosshairAlignment?.targets ?? []).map((item) => (
                <span key={`right-crosshair-${item.paneId}`} className={`status-chip ${item.deltaMs !== null && item.deltaMs <= 60_000 ? "ok" : "warning"}`}>
                  准星 · {item.paneTitle} · {item.interval} · Δ {formatAlignmentDelta(item.deltaMs)}
                </span>
              ))}
              {(bridgeState?.rangeAlignment?.targets ?? []).map((item) => (
                <span key={`right-range-${item.paneId}`} className={`status-chip ${item.toDeltaMs !== null && item.toDeltaMs <= 86_400_000 ? "ok" : "warning"}`}>
                  视区 · {item.paneTitle} · {item.interval} · ΔF {formatAlignmentDelta(item.fromDeltaMs)} · ΔT {formatAlignmentDelta(item.toDeltaMs)}
                </span>
              ))}
              {bridgeState?.crosshairAlignment?.targets.length || bridgeState?.rangeAlignment?.targets.length ? null : (
                <span className="status-chip">等待真实 bars 与交互事件生成对齐结果</span>
              )}
            </div>
          </section>
          <section>
            <h3>数据质量</h3>
            <div className="chip-grid">
              <span className={`status-chip ${gateTone === "neutral" ? "" : gateTone}`}>
                {getToneMarker(gateTone)} Gate {displayValue(ingestionGateStatus?.quality_grade)}
              </span>
              <span className={`status-chip ${ingestionGateStatus?.replayable ? "ok" : "warning"}`}>
                Replay {displayValue(ingestionGateStatus?.replayable)}
              </span>
              <span className={`status-chip ${ingestionGateStatus?.lineage_complete ? "ok" : "warning"}`}>
                Lineage {displayValue(ingestionGateStatus?.lineage_complete)}
              </span>
              <span className={`status-chip ${ingestionGateStatus?.tick_verified ? "ok" : "warning"}`}>
                Tick {displayValue(ingestionGateStatus?.tick_verified)}
              </span>
              <span className="status-chip">
                Source {displayValue(ingestionGateStatus?.source_grade)}
              </span>
            </div>
            <ul className="metric-list">
              <li>
                <span>golden status</span>
                <strong className={getToneClassName(qualityTone)}>
                  {getToneMarker(qualityTone)} {displayValue(chartQuality?.golden_status)}
                </strong>
              </li>
              <li>
                <span>gate quality</span>
                <strong className={getToneClassName(gateTone)}>
                  {getToneMarker(gateTone)} {displayValue(ingestionGateStatus?.quality_grade)}
                </strong>
              </li>
              <li><span>tick verified</span><strong>{displayValue(ingestionGateStatus?.tick_verified)}</strong></li>
              <li><span>lineage complete</span><strong>{displayValue(ingestionGateStatus?.lineage_complete)}</strong></li>
              <li><span>replayable</span><strong>{displayValue(ingestionGateStatus?.replayable)}</strong></li>
              <li><span>source grade</span><strong>{displayValue(ingestionGateStatus?.source_grade)}</strong></li>
              <li><span>missing days</span><strong>{displayValue(chartQuality?.missing_days)}</strong></li>
              <li><span>cross source</span><strong>{displayValue(chartQuality?.cross_source_status)}</strong></li>
              <li><span>backfill</span><strong>{displayValue(chartQuality?.backfill_status)}</strong></li>
              <li>
                <span>repair plan</span>
                <strong className={getToneClassName(repairTone)}>
                  {getToneMarker(repairTone)} {displayValue(repairSnapshot?.plan_status)}
                </strong>
              </li>
              <li><span>audit anchor</span><strong>{displayValue(chartQuality?.audit_anchor_date)}</strong></li>
              <li><span>gate updated</span><strong>{displayValue(ingestionGateStatus?.last_updated)}</strong></li>
            </ul>
            {ingestionGateError ? <div className="status-chip warning">gate 状态: {ingestionGateError}</div> : null}
            <div className="inline-note">
              `degraded` 不是隐藏态，而是工作台的一等状态：主舞台继续展示 bars，同时把缺失与污染暴露给操作者。
            </div>
          </section>
          <section>
            <h3>性能专项</h3>
            <ul className="metric-list">
              <li><span>pane count</span><strong>{displayValue(performanceState?.paneCount)}</strong></li>
              <li><span>mounted panes</span><strong>{displayValue(performanceState?.mountedPaneCount)}</strong></li>
              <li><span>lazy pending</span><strong>{displayValue(performanceState?.lazyPendingCount)}</strong></li>
              <li><span>stress target</span><strong>{displayValue(performanceState?.stressTargetCount)}</strong></li>
              <li><span>last marker apply</span><strong>{formatRuntimeDuration(performanceState?.lastMarkerApplyMs)}</strong></li>
              <li><span>last stress run</span><strong>{formatRuntimeDuration(performanceState?.lastStressDurationMs)}</strong></li>
              <li><span>max marker apply</span><strong>{formatRuntimeDuration(performanceState?.maxMarkerApplyMs)}</strong></li>
              <li><span>updated at</span><strong>{formatRuntimeTime(performanceState?.lastUpdatedAt)}</strong></li>
            </ul>
            <div className="inline-note">
              主舞台组件已改为动态分块加载；图表运行时再配合双图/四图懒挂载，把首屏压力优先收敛到主 pane。
            </div>
          </section>
          <section>
            <h3>后台修复</h3>
            <ul className="metric-list">
              <li>
                <span>plan status</span>
                <strong className={getToneClassName(repairTone)}>
                  {getToneMarker(repairTone)} {displayValue(repairSnapshot?.plan_status)}
                </strong>
              </li>
              <li><span>queued tasks</span><strong>{displayValue(repairSnapshot?.queued_tasks)}</strong></li>
              <li><span>failed tasks</span><strong>{displayValue(repairSnapshot?.failed_tasks)}</strong></li>
              <li><span>task count</span><strong>{displayValue(repairSnapshot?.task_count)}</strong></li>
              <li><span>generated at</span><strong>{displayValue(repairSnapshot?.generated_at)}</strong></li>
            </ul>
            <div className="action-row">
              <button
                type="button"
                className="primary"
                disabled={repairBusy !== null}
                onClick={() => {
                  void handleTriggerRepair(false);
                }}
              >
                {repairBusy === "standard" ? "编排中…" : "立即编排"}
              </button>
              <button
                type="button"
                className="danger"
                disabled={repairBusy !== null}
                onClick={() => {
                  void handleTriggerRepair(true);
                }}
              >
                {repairBusy === "force" ? "复审中…" : "强制复审+编排"}
              </button>
            </div>
            {repairActionError ? <div className="status-chip danger">repair 错误：{repairActionError}</div> : null}
            {visibleRepairResult ? (
              <div className={`status-chip ${getRepairTone(visibleRepairResult.status)}`}>
                最新操作：{visibleRepairResult.status} · queued {visibleRepairResult.queued_tasks} · failed {visibleRepairResult.failed_tasks}
              </div>
            ) : null}
            <div className="chip-grid">
              {(repairSnapshot?.blocker_issues ?? []).map((issue) => (
                <span key={`blocker-${issue}`} className={`status-chip ${repairTone === "neutral" ? "warning" : repairTone}`}>
                  blocker · {issue}
                </span>
              ))}
              {(repairSnapshot?.notes ?? []).map((note) => (
                <span key={`note-${note}`} className={`status-chip ${repairTone === "neutral" ? "warning" : repairTone}`}>
                  {note}
                </span>
              ))}
              {(repairSnapshot?.tasks ?? []).map((task) => (
                <span
                  key={`repair-task-${task.period}-${task.start_date}-${task.end_date}-${task.reason}`}
                  className={`status-chip ${repairTone === "neutral" ? "warning" : repairTone}`}
                >
                  task · {task.period} · {task.reason}
                </span>
              ))}
              {(
                (repairSnapshot?.blocker_issues.length ?? 0)
                + (repairSnapshot?.notes.length ?? 0)
                + (repairSnapshot?.tasks.length ?? 0)
              ) > 0 ? null : <span className="status-chip">暂无 repair plan 细节</span>}
            </div>
          </section>
        </div>
      }
      bottomPane={
        <div className="stack-gap">
          <div className="bottom-tabs">
            <button type="button" className="active">持仓</button>
            <button type="button">委托</button>
            <button type="button">成交</button>
            <button type="button">历史</button>
            <button type="button">账户</button>
          </div>
          <div className="status-chip warning">
            ◐ 底栏当前只承接容器布局，不展示伪造账户/订单数据；等真实持仓、委托、成交读模型接线后再填充。
          </div>
          <div className="table-placeholder">
            <div className="table-header">
              <span>主标的</span>
              <span>图表状态</span>
              <span>订单链路</span>
              <span>工作区</span>
              <span>adapter</span>
            </div>
            <div className="table-row">
              <span>{selectedSymbol}</span>
              <span>{displayValue(stageSnapshot?.status)}</span>
              <span>{displayValue(bridgeState?.lastEventType ?? "等待事件桥")}</span>
              <span>{selectedLayoutPreset}</span>
              <span className={getToneClassName(runtimeTone)}>{getToneMarker(runtimeTone)} {displayValue(stageSnapshot?.adapter.kind)}</span>
            </div>
            <div className="table-row">
              <span>{selectedInterval}</span>
              <span className={getToneClassName(qualityTone)}>{getToneMarker(qualityTone)} {displayValue(chartQuality?.golden_status)}</span>
              <span>{displayValue(stageSnapshot?.annotationCount)} annotations / {displayValue(stageSnapshot?.tradeMarkCount)} marks / {formatRuntimeDuration(performanceState?.lastMarkerApplyMs)}</span>
              <span>{displayValue(chartQuality?.audit_anchor_date)}</span>
              <span>{displayValue(chartQuality?.cross_source_status)}</span>
            </div>
            <div className="table-row">
              <span>数据状态条</span>
              <span className={getToneClassName(gateTone)}>{getToneMarker(gateTone)} {displayValue(ingestionGateStatus?.quality_grade)}</span>
              <span>tick={displayValue(ingestionGateStatus?.tick_verified)} / replay={displayValue(ingestionGateStatus?.replayable)}</span>
              <span>lineage={displayValue(ingestionGateStatus?.lineage_complete)}</span>
              <span>{displayValue(ingestionGateStatus?.source_grade)}</span>
            </div>
          </div>
        </div>
      }
    />
  );
}
