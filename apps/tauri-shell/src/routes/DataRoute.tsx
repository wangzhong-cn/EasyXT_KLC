import { useEffect, useMemo, useState } from "react";

import {
  createGovernanceActionAudit,
  fetchDataGovernanceOverview,
  fetchGovernanceActionAudit,
  fetchGovernanceSlaThresholds,
  exportGovernanceSnapshot,
  fetchDataGovernanceTraceability,
  fetchDataGovernanceTradingCalendar,
  fetchLineageAnchorDetail,
  fetchGolden1dRepairPlans,
  fetchReceiptHistory,
  fetchReceiptTimeline,
  triggerGolden1dRepair,
  triggerLateEventReplay,
  updateGovernanceSlaThresholds,
  type DataEnvironmentConfigItemPayload,
  type DataGovernanceOverviewPayload,
  type GateDimensionTrendPointPayload,
  type Golden1dRepairPlanListPayload,
  type LineageAnchorDetailPayload,
  type ReceiptHistoryPayload,
  type ReceiptTimelinePayload,
  type DataGovernanceTraceabilityPayload,
  type DataGovernanceTradingCalendarPayload,
} from "../lib/api";
import { openWorkbenchLink } from "../lib/routeBridge";
import { getBooleanTone, getToneClassName, getToneMarker, type UiTone } from "../lib/uiTone";

type DataTabKey = "overview" | "calendar" | "traceability" | "repair";

const TRACEABILITY_PERIODS = ["", "1d", "1m", "5m", "tick"];
const TIMELINE_RECEIPT_TYPES = ["", "publish_gate", "repair", "replay"] as const;
const TIMELINE_SEVERITIES = ["", "ok", "warning", "critical", "unknown"] as const;

function displayValue(value: string | number | boolean | null | undefined): string {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  return String(value);
}

function formatPercent(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return `${(value * 100).toFixed(digits)}%`;
}

function formatJson(value: unknown): string {
  if (value === null || value === undefined) {
    return "—";
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function flattenEnvIssues(itemsByGroup: Record<string, DataEnvironmentConfigItemPayload[]>): DataEnvironmentConfigItemPayload[] {
  return Object.values(itemsByGroup)
    .flat()
    .filter((item) => item.required || item.status !== "ok")
    .slice(0, 10);
}

function getPipelineItemTone(healthy: boolean | undefined): UiTone {
  if (healthy === true) {
    return "ok";
  }
  if (healthy === false) {
    return "warning";
  }
  return "neutral";
}

function getTraceabilityStatusTone(status: string | undefined): UiTone {
  switch (status) {
    case "success":
      return "ok";
    case "error":
      return "danger";
    default:
      return "neutral";
  }
}

function getAvailabilityTone(value: unknown): UiTone {
  if (value === true || value === "true" || value === 1) {
    return "ok";
  }
  if (value === false || value === "false" || value === 0) {
    return "warning";
  }
  return "neutral";
}

function getRepairPlanTone(status: string | undefined): UiTone {
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

function getGateRejectTone(reason: string | undefined): UiTone {
  switch (reason) {
    case "passed":
      return "ok";
    case "manual_review":
    case "tick_mismatch":
    case "lineage_incomplete":
      return "warning";
    case "contract_failed":
    case "cross_source_conflict":
      return "danger";
    default:
      return "neutral";
  }
}

function getSlaImpactTone(impact: string | undefined): UiTone {
  switch (impact) {
    case "within_sla":
      return "ok";
    case "monitor":
      return "warning";
    case "gate_block":
      return "danger";
    default:
      return "neutral";
  }
}

function renderDimensionTrendTable(title: string, rows: GateDimensionTrendPointPayload[]) {
  return (
    <article className="panel card">
      <h3>{title}</h3>
      <div className="table-shell compact-shell">
        <table className="data-table compact-table">
          <thead>
            <tr>
              <th>日期</th>
              <th>维度</th>
              <th>总量</th>
              <th>rejected</th>
              <th>critical</th>
              <th>warning</th>
            </tr>
          </thead>
          <tbody>
            {rows.length
              ? rows.map((item) => (
                <tr key={`${title}-${item.trade_day}-${item.dimension_value}`}>
                  <td>{displayValue(item.trade_day)}</td>
                  <td>{displayValue(item.dimension_value)}</td>
                  <td>{displayValue(item.total)}</td>
                  <td>{displayValue(item.rejected)}</td>
                  <td>{displayValue(item.critical)}</td>
                  <td>{displayValue(item.warning)}</td>
                </tr>
              ))
              : (
                <tr>
                  <td colSpan={6} className="empty-cell">暂无维度趋势</td>
                </tr>
              )}
          </tbody>
        </table>
      </div>
    </article>
  );
}

export function DataRoute() {
  const [activeTab, setActiveTab] = useState<DataTabKey>("overview");
  const [overview, setOverview] = useState<DataGovernanceOverviewPayload | null>(null);
  const [overviewLoading, setOverviewLoading] = useState(true);
  const [overviewError, setOverviewError] = useState<string | null>(null);
  const [overviewTrendDays, setOverviewTrendDays] = useState("7");
  const [governanceOperator, setGovernanceOperator] = useState("tauri-user");
  const [governanceNote, setGovernanceNote] = useState("");
  const [slaThresholdOverrides, setSlaThresholdOverrides] = useState<Record<string, number>>({});
  const [slaThresholdSaving, setSlaThresholdSaving] = useState(false);
  const [snapshotExporting, setSnapshotExporting] = useState(false);
  const [snapshotExportFormat, setSnapshotExportFormat] = useState<"json" | "jsonl" | "csv">("json");
  const [governanceActionLoading, setGovernanceActionLoading] = useState<string | null>(null);
  const [governanceActionFeedback, setGovernanceActionFeedback] = useState<string | null>(null);
  const [actionAuditForm, setActionAuditForm] = useState({
    actionType: "",
    source: "",
    stockCode: "",
    period: "",
    lineageAnchor: "",
  });
  const [actionAuditQuery, setActionAuditQuery] = useState({
    actionType: "",
    source: "",
    stockCode: "",
    period: "",
    lineageAnchor: "",
  });
  const [actionAuditData, setActionAuditData] = useState(overview?.receipts.action_audit_recent ?? []);
  const [actionAuditLoading, setActionAuditLoading] = useState(false);
  const [actionAuditError, setActionAuditError] = useState<string | null>(null);
  const [repairPlans, setRepairPlans] = useState<Golden1dRepairPlanListPayload | null>(null);
  const [repairReceipts, setRepairReceipts] = useState<ReceiptHistoryPayload | null>(null);
  const [replayReceipts, setReplayReceipts] = useState<ReceiptHistoryPayload | null>(null);
  const [timelineForm, setTimelineForm] = useState({
    symbol: "",
    period: "",
    lineageAnchor: "",
    receiptType: "",
    gateRejectReason: "",
    severity: "",
    lookbackDays: "7",
  });
  const [timelineQuery, setTimelineQuery] = useState({
    symbol: "",
    period: "",
    lineageAnchor: "",
    receiptType: "",
    gateRejectReason: "",
    severity: "",
    lookbackDays: "7",
  });
  const [timelineData, setTimelineData] = useState<ReceiptTimelinePayload | null>(null);
  const [timelineLoading, setTimelineLoading] = useState(false);
  const [timelineError, setTimelineError] = useState<string | null>(null);
  const [selectedLineageAnchor, setSelectedLineageAnchor] = useState("");
  const [lineageDetail, setLineageDetail] = useState<LineageAnchorDetailPayload | null>(null);
  const [lineageLoading, setLineageLoading] = useState(false);
  const [lineageError, setLineageError] = useState<string | null>(null);
  const [repairLoading, setRepairLoading] = useState(false);
  const [repairError, setRepairError] = useState<string | null>(null);

  const [calendarForm, setCalendarForm] = useState({ startDate: "2026-01-01", endDate: "2026-12-31" });
  const [calendarQuery, setCalendarQuery] = useState(calendarForm);
  const [calendar, setCalendar] = useState<DataGovernanceTradingCalendarPayload | null>(null);
  const [calendarLoading, setCalendarLoading] = useState(false);
  const [calendarError, setCalendarError] = useState<string | null>(null);

  const [traceabilityForm, setTraceabilityForm] = useState({ stockCode: "", period: "" });
  const [traceabilityQuery, setTraceabilityQuery] = useState(traceabilityForm);
  const [traceability, setTraceability] = useState<DataGovernanceTraceabilityPayload | null>(null);
  const [traceabilityLoading, setTraceabilityLoading] = useState(false);
  const [traceabilityError, setTraceabilityError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    async function loadOverview() {
      setOverviewLoading(true);
      setOverviewError(null);
      setRepairLoading(true);
      try {
        const [overviewPayload, repairPayload, repairReceiptPayload, replayReceiptPayload] = await Promise.all([
          fetchDataGovernanceOverview(Number(overviewTrendDays) || 7, controller.signal),
          fetchGolden1dRepairPlans(12, controller.signal),
          fetchReceiptHistory("repair", 8, controller.signal),
          fetchReceiptHistory("replay", 8, controller.signal),
        ]);
        if (!active) {
          return;
        }
        setOverview(overviewPayload);
        setSlaThresholdOverrides(overviewPayload.receipts.sla_threshold_overrides ?? {});
        setRepairPlans(repairPayload);
        setRepairReceipts(repairReceiptPayload);
        setReplayReceipts(replayReceiptPayload);
        setTimelineData({
          items: overviewPayload.receipts.timeline,
          returned: overviewPayload.receipts.timeline.length,
          filters: { symbol: "", period: "", lineage_anchor: "", lookback_days: Number(overviewTrendDays) || 7, limit: 12 },
          server_time: overviewPayload.server_time,
        });
        setRepairError(null);
      } catch (reason) {
        if (!active) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "数据治理概览读取失败";
        setOverviewError(message);
        setOverview(null);
        setSlaThresholdOverrides({});
        setRepairPlans(null);
        setRepairReceipts(null);
        setReplayReceipts(null);
        setTimelineData(null);
        setRepairError(message);
      } finally {
        if (active) {
          setOverviewLoading(false);
          setRepairLoading(false);
        }
      }
    }

    void loadOverview();
    const intervalId = window.setInterval(() => {
      void loadOverview();
    }, 20000);

    return () => {
      active = false;
      controller.abort();
      window.clearInterval(intervalId);
    };
  }, [overviewTrendDays]);

  useEffect(() => {
    if (activeTab !== "calendar") {
      return;
    }
    let active = true;
    const controller = new AbortController();

    async function loadCalendar() {
      setCalendarLoading(true);
      setCalendarError(null);
      try {
        const payload = await fetchDataGovernanceTradingCalendar(
          calendarQuery.startDate,
          calendarQuery.endDate,
          controller.signal,
        );
        if (!active) {
          return;
        }
        setCalendar(payload);
      } catch (reason) {
        if (!active) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "交易日历读取失败";
        setCalendarError(message);
        setCalendar(null);
      } finally {
        if (active) {
          setCalendarLoading(false);
        }
      }
    }

    void loadCalendar();
    return () => {
      active = false;
      controller.abort();
    };
  }, [activeTab, calendarQuery]);

  useEffect(() => {
    if (activeTab !== "traceability") {
      return;
    }
    let active = true;
    const controller = new AbortController();

    async function loadTraceability() {
      setTraceabilityLoading(true);
      setTraceabilityError(null);
      try {
        const payload = await fetchDataGovernanceTraceability(
          traceabilityQuery.stockCode,
          traceabilityQuery.period,
          controller.signal,
        );
        if (!active) {
          return;
        }
        setTraceability(payload);
      } catch (reason) {
        if (!active) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "数据溯源读取失败";
        setTraceabilityError(message);
        setTraceability(null);
      } finally {
        if (active) {
          setTraceabilityLoading(false);
        }
      }
    }

    void loadTraceability();
    return () => {
      active = false;
      controller.abort();
    };
  }, [activeTab, traceabilityQuery]);

  const envIssues = useMemo(
    () => flattenEnvIssues(overview?.environment.groups ?? {}),
    [overview],
  );

  const traceabilitySummary = useMemo(() => {
    const records = traceability?.records ?? [];
    const sourceCounts = records.reduce<Record<string, number>>((acc, item) => {
      const key = item.source || "unknown";
      acc[key] = (acc[key] ?? 0) + 1;
      return acc;
    }, {});
    const errorCount = records.filter((item) => item.status === "error").length;
    return {
      sourceCount: Object.keys(sourceCounts).length,
      errorCount,
      sourceCounts,
    };
  }, [traceability]);

  const repairSummary = useMemo(() => {
    const items = repairPlans?.items ?? [];
    const counts = items.reduce<Record<string, number>>((acc, item) => {
      const key = item.plan_status || "unknown";
      acc[key] = (acc[key] ?? 0) + 1;
      return acc;
    }, {});
    return {
      total: items.length,
      queued: counts.queued ?? 0,
      blocked: counts.blocked ?? 0,
      manualReview: counts.manual_review ?? 0,
      failed: counts.failed ?? 0,
    };
  }, [repairPlans]);

  const rejectSeveritySummary = useMemo(
    () => overview?.receipts.gate_reject_severity ?? {},
    [overview],
  );

  const symbolTrendSummary = useMemo(
    () => overview?.receipts.trend_by_symbol_7d ?? [],
    [overview],
  );

  const periodTrendSummary = useMemo(
    () => overview?.receipts.trend_by_period_7d ?? [],
    [overview],
  );

  const effectiveSlaThresholdPanel = useMemo(() => {
    const panel = overview?.receipts.sla_threshold_panel;
    if (!panel) {
      return null;
    }
    const thresholds = {
      ...panel.thresholds,
      ...slaThresholdOverrides,
    };
    const current = panel.current ?? {};
    const breaches = Object.fromEntries(
      Object.keys(thresholds).map((key) => [key, Number(current[key] ?? 0) > Number(thresholds[key] ?? 0)]),
    );
    let status = "ok";
    if (Object.values(breaches).some(Boolean)) {
      status = "warning";
    }
    if (breaches.gate_block || breaches.degraded) {
      status = "critical";
    }
    return {
      status,
      thresholds,
      current,
      breaches,
    };
  }, [overview, slaThresholdOverrides]);

  const governanceActions = useMemo(
    () => overview?.receipts.action_recommendations ?? [],
    [overview],
  );

  async function handleGovernanceRecommendation(actionType: string, payload: Record<string, unknown>): Promise<void> {
    const stockCode = String(payload.stock_code ?? "");
    const period = String(payload.period ?? "");
    setGovernanceActionLoading(actionType);
    setGovernanceActionFeedback(null);
    try {
      if (actionType === "open_traceability") {
        await createGovernanceActionAudit({
          actionId: "open_traceability_from_recommendation",
          actionType,
          tone: "warning",
          title: "治理建议跳转溯源",
          detail: `${stockCode} / ${period}`,
          payload,
        });
        setTraceabilityForm({ stockCode, period });
        setTraceabilityQuery({ stockCode, period });
        setActiveTab("traceability");
      } else if (actionType === "trigger_repair") {
        const response = await triggerGolden1dRepair(stockCode, false);
        setGovernanceActionFeedback(`repair 已触发: ${displayValue(response.status)}`);
        setActiveTab("repair");
      } else if (actionType === "trigger_replay") {
        const response = await triggerLateEventReplay(stockCode, period);
        setGovernanceActionFeedback(`replay 已触发: succeeded=${displayValue(response.result.succeeded)}`);
      } else if (actionType === "open_workbench") {
        await createGovernanceActionAudit({
          actionId: "open_workbench_from_recommendation",
          actionType,
          tone: "ok",
          title: "治理建议打开图表",
          detail: `${stockCode} / ${period}`,
          payload,
        });
        openWorkbenchLink(stockCode, period || "1d", "governance-action");
      } else {
        setActiveTab("overview");
      }
      handleRefreshOverview();
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : "治理动作执行失败";
      setGovernanceActionFeedback(message);
    } finally {
      setGovernanceActionLoading(null);
    }
  }

  async function handleSaveSlaThresholds(): Promise<void> {
    setSlaThresholdSaving(true);
    setGovernanceActionFeedback(null);
    try {
      const payload = await updateGovernanceSlaThresholds(
        slaThresholdOverrides,
        governanceOperator,
        governanceNote,
      );
      setSlaThresholdOverrides(payload.overrides);
      setGovernanceActionFeedback(
        `SLA 阈值已保存，状态=${displayValue(payload.panel.status)} / version=${displayValue(payload.config_version)} / operator=${displayValue(payload.updated_by)}`,
      );
      handleRefreshOverview();
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : "SLA 阈值保存失败";
      setGovernanceActionFeedback(message);
    } finally {
      setSlaThresholdSaving(false);
    }
  }

  async function handleExportGovernanceSnapshot(): Promise<void> {
    setSnapshotExporting(true);
    setGovernanceActionFeedback(null);
    try {
      const payload = await exportGovernanceSnapshot(Number(overviewTrendDays) || 7, 100, snapshotExportFormat);
      const blob = "blob" in payload
        ? payload.blob
        : new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
      const filename = "filename" in payload ? payload.filename : `${payload.snapshot_name}.json`;
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      anchor.click();
      window.URL.revokeObjectURL(url);
      setGovernanceActionFeedback(`治理快照已导出: ${filename}`);
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : "治理快照导出失败";
      setGovernanceActionFeedback(message);
    } finally {
      setSnapshotExporting(false);
    }
  }

  useEffect(() => {
    if (activeTab !== "overview") {
      return;
    }
    let active = true;
    const controller = new AbortController();

    async function loadTimeline() {
      setTimelineLoading(true);
      setTimelineError(null);
      try {
        const payload = await fetchReceiptTimeline(
          {
            symbol: timelineQuery.symbol || undefined,
            period: timelineQuery.period || undefined,
            lineageAnchor: timelineQuery.lineageAnchor || undefined,
            receiptType: timelineQuery.receiptType ? timelineQuery.receiptType as "publish_gate" | "repair" | "replay" : undefined,
            gateRejectReason: timelineQuery.gateRejectReason || undefined,
            severity: timelineQuery.severity ? timelineQuery.severity as "ok" | "warning" | "critical" | "unknown" : undefined,
            lookbackDays: Number(timelineQuery.lookbackDays) || undefined,
            limit: 20,
          },
          controller.signal,
        );
        if (!active) {
          return;
        }
        setTimelineData(payload);
      } catch (reason) {
        if (!active) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "receipt timeline 读取失败";
        setTimelineError(message);
        setTimelineData(null);
      } finally {
        if (active) {
          setTimelineLoading(false);
        }
      }
    }

    const hasExplicitFilter = Object.values(timelineQuery).some((value) => value !== "");
    if (hasExplicitFilter) {
      void loadTimeline();
    } else {
      setTimelineData(
        overview
          ? {
            items: overview.receipts.timeline,
            returned: overview.receipts.timeline.length,
            filters: { symbol: "", period: "", lineage_anchor: "", lookback_days: Number(overviewTrendDays) || 7, limit: 12 },
            server_time: overview.server_time,
          }
          : null,
      );
    }

    return () => {
      active = false;
      controller.abort();
    };
  }, [activeTab, overview, timelineQuery, overviewTrendDays]);

  useEffect(() => {
    if (activeTab !== "overview") {
      return;
    }
    let active = true;
    const controller = new AbortController();

    async function loadActionAudit() {
      setActionAuditLoading(true);
      setActionAuditError(null);
      try {
        const payload = await fetchGovernanceActionAudit(
          {
            limit: 20,
            actionType: actionAuditQuery.actionType || undefined,
            source: actionAuditQuery.source || undefined,
            stockCode: actionAuditQuery.stockCode || undefined,
            period: actionAuditQuery.period || undefined,
            lineageAnchor: actionAuditQuery.lineageAnchor || undefined,
          },
          controller.signal,
        );
        if (!active) {
          return;
        }
        setActionAuditData(payload.records ?? []);
      } catch (reason) {
        if (!active) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "治理动作审计读取失败";
        setActionAuditError(message);
        setActionAuditData([]);
      } finally {
        if (active) {
          setActionAuditLoading(false);
        }
      }
    }

    const hasExplicitFilter = Object.values(actionAuditQuery).some((value) => value !== "");
    if (hasExplicitFilter) {
      void loadActionAudit();
    } else {
      setActionAuditData(overview?.receipts.action_audit_recent ?? []);
      setActionAuditError(null);
      setActionAuditLoading(false);
    }
    return () => {
      active = false;
      controller.abort();
    };
  }, [activeTab, overview, actionAuditQuery]);

  useEffect(() => {
    if (activeTab !== "overview" || !selectedLineageAnchor) {
      setLineageDetail(null);
      setLineageError(null);
      setLineageLoading(false);
      return;
    }
    let active = true;
    const controller = new AbortController();

    async function loadLineageDetail() {
      setLineageLoading(true);
      setLineageError(null);
      try {
        const payload = await fetchLineageAnchorDetail(selectedLineageAnchor, controller.signal);
        if (!active) {
          return;
        }
        setLineageDetail(payload);
      } catch (reason) {
        if (!active) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "lineage 明细读取失败";
        setLineageError(message);
        setLineageDetail(null);
      } finally {
        if (active) {
          setLineageLoading(false);
        }
      }
    }

    void loadLineageDetail();
    return () => {
      active = false;
      controller.abort();
    };
  }, [activeTab, selectedLineageAnchor]);

  function handleRefreshOverview(): void {
    setOverviewLoading(true);
    setOverviewError(null);
    setRepairLoading(true);
    void Promise.all([
      fetchDataGovernanceOverview(Number(overviewTrendDays) || 7),
      fetchGolden1dRepairPlans(12),
      fetchReceiptHistory("repair", 8),
      fetchReceiptHistory("replay", 8),
    ])
      .then(([overviewPayload, repairPayload, repairReceiptPayload, replayReceiptPayload]) => {
        setOverview(overviewPayload);
        setSlaThresholdOverrides(overviewPayload.receipts.sla_threshold_overrides ?? {});
        setRepairPlans(repairPayload);
        setRepairReceipts(repairReceiptPayload);
        setReplayReceipts(replayReceiptPayload);
        setTimelineData({
          items: overviewPayload.receipts.timeline,
          returned: overviewPayload.receipts.timeline.length,
          filters: { symbol: "", period: "", lineage_anchor: "", lookback_days: Number(overviewTrendDays) || 7, limit: 12 },
          server_time: overviewPayload.server_time,
        });
        setRepairError(null);
        setTimelineError(null);
      })
      .catch((reason) => {
        const message = reason instanceof Error ? reason.message : "数据治理概览读取失败";
        setOverviewError(message);
        setRepairError(message);
        setOverview(null);
        setSlaThresholdOverrides({});
        setRepairPlans(null);
        setRepairReceipts(null);
        setReplayReceipts(null);
        setTimelineData(null);
        setTimelineError(message);
      })
      .finally(() => {
        setOverviewLoading(false);
        setRepairLoading(false);
      });
  }

  return (
    <section className="route-stack">
      <header className="panel card route-header">
        <h2>数据治理 / 路由 / 日历 / 溯源</h2>
        <p>
          这是从 `DataGovernancePanel` 迁出的第一版 DataRoute：优先承接系统当前状态、数据路由健康、
          交易日历与入库溯源，把“数据治理必须打开 Qt 才能看见”的旧路径切掉。
        </p>
      </header>

      <nav className="bottom-tabs route-tab-nav data-tab-switcher" aria-label="数据治理页签切换">
        <button type="button" className={activeTab === "overview" ? "active" : ""} onClick={() => setActiveTab("overview")}>治理概览</button>
        <button type="button" className={activeTab === "calendar" ? "active" : ""} onClick={() => setActiveTab("calendar")}>交易日历</button>
        <button type="button" className={activeTab === "traceability" ? "active" : ""} onClick={() => setActiveTab("traceability")}>数据溯源</button>
        <button type="button" className={activeTab === "repair" ? "active" : ""} onClick={() => setActiveTab("repair")}>Repair 队列</button>
      </nav>

      {activeTab === "overview" ? (
        <div className="route-stack">
          <article className="panel card">
            <div className="action-strip">
              <label className="field-block">
                <span>趋势窗口(天)</span>
                <input
                  className="control-input"
                  value={overviewTrendDays}
                  onChange={(event) => setOverviewTrendDays(event.target.value)}
                />
              </label>
              <button
                type="button"
                className="ghost-button"
                onClick={handleRefreshOverview}
              >
                刷新概览
              </button>
              <div className="inline-note">DataRoute 当前优先承接只读治理快照；高危写操作暂不迁入新壳。</div>
            </div>
            {overviewError ? <div className="status-chip danger">读取失败：{overviewError}</div> : null}
            {repairError ? <div className="status-chip danger">repair 计划读取失败：{repairError}</div> : null}
            <div className="summary-card-grid data-summary-grid">
              <div className={`status-chip ${overview?.summary.datasource_status === "ok" ? "ok" : "warning"}`}>
                数据源健康: {displayValue(overview?.summary.datasource_status)}
              </div>
              <div className={`status-chip ${overview?.summary.sla_status === "ok" ? "ok" : "warning"}`}>
                SLA 门禁: {displayValue(overview?.summary.sla_status)}
              </div>
              <div className={`status-chip ${overview?.summary.pipeline_healthy ? "ok" : "warning"}`}>
                管道状态: {overview?.summary.pipeline_healthy ? "稳定" : "需要关注"}
              </div>
              <div className="status-chip">
                在线数据源: {displayValue(overview?.summary.healthy_sources)} / {displayValue(overview?.summary.total_sources)}
              </div>
              <div className={`status-chip ${overview?.summary.duckdb_healthy ? "ok" : "warning"}`}>
                DuckDB: {overview?.summary.duckdb_healthy ? "健康" : "降级"}
              </div>
              <div className={`status-chip ${overview?.summary.env_valid ? "ok" : "warning"}`}>
                环境配置: {overview?.summary.env_valid ? "完整" : "缺少必填项"}
              </div>
              <div className={`status-chip ${repairSummary.queued > 0 ? "ok" : ""}`}>
                Repair queued: {displayValue(repairSummary.queued)}
              </div>
              <div className={`status-chip ${repairSummary.blocked + repairSummary.manualReview + repairSummary.failed > 0 ? "danger" : ""}`}>
                Repair blockers: {displayValue(repairSummary.blocked + repairSummary.manualReview + repairSummary.failed)}
              </div>
              <div className={`status-chip ${(overview?.summary.gate_degraded ?? 0) > 0 ? "danger" : "ok"}`}>
                Gate degraded: {displayValue(overview?.summary.gate_degraded)}
              </div>
              <div className={`status-chip ${(overview?.summary.gate_reject_total ?? 0) > 0 ? "warning" : "ok"}`}>
                Gate rejects: {displayValue(overview?.summary.gate_reject_total)}
              </div>
              <div className={`status-chip ${(overview?.summary.gate_critical ?? 0) > 0 ? "danger" : "ok"}`}>
                Gate critical: {displayValue(overview?.summary.gate_critical)}
              </div>
              <div className={`status-chip ${(overview?.summary.gate_warning ?? 0) > 0 ? "warning" : "ok"}`}>
                Gate warning: {displayValue(overview?.summary.gate_warning)}
              </div>
              <div className={`status-chip ${(overview?.summary.sla_gate_block ?? 0) > 0 ? "danger" : "ok"}`}>
                SLA gate block: {displayValue(overview?.summary.sla_gate_block)}
              </div>
              <div className={`status-chip ${(overview?.summary.sla_monitor ?? 0) > 0 ? "warning" : "ok"}`}>
                SLA monitor: {displayValue(overview?.summary.sla_monitor)}
              </div>
              <div className="status-chip">
                Repair / Replay Receipts: {displayValue(overview?.summary.repair_receipts)} / {displayValue(overview?.summary.replay_receipts)}
              </div>
              <div className={`status-chip ${getBooleanTone(overview?.realtime.connected)}`}>
                实时链路: {displayValue(overview?.realtime.connected)} / 降级={displayValue(overview?.realtime.degraded)}
              </div>
              <div className={`status-chip ${overviewLoading ? "warning" : "ok"}`}>
                {overviewLoading ? `${getToneMarker(getBooleanTone(false))} 概览刷新中` : `${getToneMarker(getBooleanTone(true))} 概览已刷新`}
              </div>
            </div>
          </article>

          <div className="data-route-grid">
            <article className="panel card">
              <h3>数据路由指标</h3>
              <div className="table-shell compact-shell">
                <table className="data-table compact-table">
                  <thead>
                    <tr>
                      <th>数据源</th>
                      <th>命中</th>
                      <th>漏命中</th>
                      <th>错误</th>
                      <th>质量拒绝</th>
                      <th>延迟</th>
                      <th>健康</th>
                    </tr>
                  </thead>
                  <tbody>
                    {overview?.routing.sources && Object.entries(overview.routing.sources).length
                      ? Object.entries(overview.routing.sources).map(([name, item]) => (
                        <tr key={name}>
                          <td>{name}</td>
                          <td>{displayValue(item.hits)}</td>
                          <td>{displayValue(item.misses)}</td>
                          <td>{displayValue(item.errors)}</td>
                          <td>{displayValue(item.quality_rejects)}</td>
                          <td>{displayValue(item.last_latency_ms)}</td>
                          <td>{displayValue(item.health?.available as string | boolean | number | null | undefined)}</td>
                        </tr>
                      ))
                      : (
                        <tr>
                          <td colSpan={7} className="empty-cell">暂无路由指标</td>
                        </tr>
                      )}
                  </tbody>
                </table>
              </div>
            </article>

            <article className="panel card">
              <h3>管道状态</h3>
              <ul className="metric-list large">
                {overview?.pipeline.checks && Object.entries(overview.pipeline.checks).length
                  ? Object.entries(overview.pipeline.checks).map(([name, item]) => (
                    <li key={name}>
                      <span>{name}</span>
                      <strong className={getToneClassName(getPipelineItemTone(item.healthy))}>
                        {getToneMarker(getPipelineItemTone(item.healthy))} {formatJson(item)}
                      </strong>
                    </li>
                  ))
                  : <li><span>pipeline</span><strong>{overview?.pipeline.error ?? "暂无数据"}</strong></li>}
              </ul>
            </article>

            <article className="panel card">
              <h3>DuckDB / 实时链路</h3>
              <ul className="metric-list large">
                <li><span>数据库路径</span><strong className="mono-text">{displayValue(overview?.duckdb.path)}</strong></li>
                <li><span>表数量</span><strong>{displayValue(overview?.duckdb.table_count)}</strong></li>
                <li><span>stock_daily 行数</span><strong>{displayValue(overview?.duckdb.stock_daily_rows)}</strong></li>
                <li><span>最新日期</span><strong>{displayValue(overview?.duckdb.latest_date)}</strong></li>
                <li><span>实时标的</span><strong>{displayValue(overview?.realtime.symbol)}</strong></li>
                <li><span>丢帧率</span><strong>{formatPercent(overview?.realtime.drop_rate)}</strong></li>
                <li><span>队列深度</span><strong>{displayValue(overview?.realtime.queue_depth)}</strong></li>
              </ul>
            </article>

            <article className="panel card">
              <h3>环境与质量提醒</h3>
              <div className="stack-gap">
                <div className="status-chip">
                  已配置项: {displayValue(overview?.environment.summary.configured)} / {displayValue(overview?.environment.summary.total)}
                </div>
                <div className={`status-chip ${overview?.environment.overall_valid ? "ok" : "warning"}`}>
                  缺失必填项: {displayValue(overview?.environment.summary.missing_required)}
                </div>
                <div className="status-chip">
                  死信总数: {displayValue(overview?.datasource_health.checks.quarantine?.dead_letter as string | number | boolean | null | undefined)}
                </div>
                <div className="status-chip">
                  Step6 采样率: {displayValue(overview?.datasource_health.checks.thresholds?.step6_validate_sample_rate as string | number | boolean | null | undefined)}
                </div>
              </div>
              <div className="data-issue-list">
                {envIssues.length
                  ? envIssues.map((item) => (
                    <div key={item.key} className={`status-chip ${item.status === "ok" ? "ok" : item.status === "invalid" ? "danger" : "warning"}`}>
                      {item.key}: {item.status}
                    </div>
                  ))
                  : <div className="status-chip ok">必填配置当前无异常</div>}
              </div>
            </article>

            <article className="panel card">
              <h3>Golden 1D Repair</h3>
              <div className="stack-gap">
                <div className="status-chip">最近计划数: {displayValue(repairSummary.total)}</div>
                <div className={`status-chip ${repairSummary.queued > 0 ? "ok" : ""}`}>queued: {displayValue(repairSummary.queued)}</div>
                <div className={`status-chip ${repairSummary.blocked > 0 ? "danger" : ""}`}>blocked: {displayValue(repairSummary.blocked)}</div>
                <div className={`status-chip ${repairSummary.manualReview > 0 ? "danger" : ""}`}>manual_review: {displayValue(repairSummary.manualReview)}</div>
                <div className={`status-chip ${repairSummary.failed > 0 ? "danger" : ""}`}>failed: {displayValue(repairSummary.failed)}</div>
                <div className={`status-chip ${repairLoading ? "warning" : "ok"}`}>{repairLoading ? `${getToneMarker(getBooleanTone(false))} repair 刷新中` : `${getToneMarker(getBooleanTone(true))} repair 已刷新`}</div>
              </div>
              <div className="data-issue-list">
                {repairPlans?.items?.length
                  ? repairPlans.items.slice(0, 6).map((item) => {
                    const tone = getRepairPlanTone(item.plan_status);
                    return (
                      <div key={`${item.symbol}-${item.generated_at ?? "na"}`} className={`status-chip ${tone === "neutral" ? "" : tone}`}>
                        {item.symbol}: {item.plan_status} / q={item.queued_tasks} / f={item.failed_tasks}
                      </div>
                    );
                  })
                  : <div className="status-chip">暂无 Golden 1D repair plan</div>}
              </div>
            </article>
            <article className="panel card">
              <h3>Gate 拒绝原因</h3>
              <div className="summary-card-grid data-summary-grid">
                {Object.entries(rejectSeveritySummary).length
                  ? Object.entries(rejectSeveritySummary).map(([severity, count]) => (
                    <div key={severity} className={`status-chip ${severity === "critical" ? "danger" : severity === "warning" ? "warning" : severity === "ok" ? "ok" : ""}`}>
                      severity {severity}: {displayValue(count)}
                    </div>
                  ))
                  : <div className="status-chip">暂无 severity 聚合</div>}
              </div>
              <div className="summary-card-grid data-summary-grid">
                {overview?.receipts.gate_sla_impact && Object.entries(overview.receipts.gate_sla_impact).length
                  ? Object.entries(overview.receipts.gate_sla_impact).map(([impact, count]) => {
                    const tone = getSlaImpactTone(impact);
                    return (
                      <div key={impact} className={`status-chip ${tone === "neutral" ? "" : tone}`}>
                        sla {impact}: {displayValue(count)}
                      </div>
                    );
                  })
                  : <div className="status-chip">暂无 SLA impact 聚合</div>}
              </div>
              <div className="data-issue-list">
                {overview?.receipts.gate_reject_reasons && Object.entries(overview.receipts.gate_reject_reasons).length
                  ? Object.entries(overview.receipts.gate_reject_reasons).map(([reason, count]) => {
                    const tone = getGateRejectTone(reason);
                    return (
                      <div key={reason} className={`status-chip ${tone === "neutral" ? "" : tone}`}>
                        {getToneMarker(tone)} {reason}: {displayValue(count)}
                      </div>
                    );
                  })
                  : <div className="status-chip">暂无 gate reject reason</div>}
              </div>
            </article>
            <article className="panel card">
              <h3>SLA 告警阈值面板</h3>
              {governanceActionFeedback ? <div className="status-chip warning">{governanceActionFeedback}</div> : null}
              <div className="status-chip">
                threshold config: {displayValue(overview?.receipts.sla_threshold_config_meta?.updated_at)} / {displayValue(overview?.receipts.sla_threshold_config_meta?.path)}
              </div>
              <div className="status-chip">
                threshold version: {displayValue(overview?.receipts.sla_threshold_version)} / operator={displayValue(overview?.receipts.sla_threshold_updated_by)} / note={displayValue(overview?.receipts.sla_threshold_note)}
              </div>
              <div className="summary-card-grid data-summary-grid">
                <div className={`status-chip ${effectiveSlaThresholdPanel?.status === "critical" ? "danger" : effectiveSlaThresholdPanel?.status === "warning" ? "warning" : "ok"}`}>
                  panel status: {displayValue(effectiveSlaThresholdPanel?.status)}
                </div>
                {effectiveSlaThresholdPanel?.thresholds
                  ? Object.entries(effectiveSlaThresholdPanel.thresholds).map(([key, threshold]) => {
                    const current = effectiveSlaThresholdPanel.current?.[key];
                    const breached = effectiveSlaThresholdPanel.breaches?.[key];
                    return (
                      <div key={key} className={`status-chip ${breached ? "danger" : "ok"}`}>
                        {key}: {displayValue(current)} / threshold={displayValue(threshold)}
                      </div>
                    );
                  })
                  : null}
              </div>
              <div className="structure-filter-grid">
                {effectiveSlaThresholdPanel?.thresholds
                  ? Object.entries(effectiveSlaThresholdPanel.thresholds).map(([key, threshold]) => (
                    <label key={`threshold-${key}`} className="field-block">
                      <span>{key}</span>
                      <input
                        className="control-input"
                        value={String(slaThresholdOverrides[key] ?? threshold)}
                        onChange={(event) => {
                          const nextValue = Number(event.target.value);
                          setSlaThresholdOverrides((current) => ({
                            ...current,
                            [key]: Number.isFinite(nextValue) ? nextValue : 0,
                          }));
                        }}
                      />
                    </label>
                  ))
                  : null}
                <label className="field-block">
                  <span>operator</span>
                  <input
                    className="control-input"
                    value={governanceOperator}
                    onChange={(event) => setGovernanceOperator(event.target.value)}
                  />
                </label>
                <label className="field-block">
                  <span>note</span>
                  <input
                    className="control-input"
                    value={governanceNote}
                    onChange={(event) => setGovernanceNote(event.target.value)}
                  />
                </label>
                <div className="field-block">
                  <span>&nbsp;</span>
                  <div className="action-strip">
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={() => void handleSaveSlaThresholds()}
                    >
                      {slaThresholdSaving ? "保存中..." : "保存到服务端"}
                    </button>
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={() => {
                        void fetchGovernanceSlaThresholds().then((payload) => {
                          setSlaThresholdOverrides(payload.overrides);
                          setGovernanceOperator(payload.updated_by ?? "tauri-user");
                          setGovernanceNote(payload.note ?? "");
                          setGovernanceActionFeedback("已恢复服务端阈值");
                        }).catch((reason) => {
                          const message = reason instanceof Error ? reason.message : "阈值读取失败";
                          setGovernanceActionFeedback(message);
                        });
                      }}
                    >
                      恢复服务端阈值
                    </button>
                  </div>
                </div>
              </div>
            </article>
            <article className="panel card">
              <h3>治理建议动作</h3>
              <div className="status-chip">
                rulebook: {displayValue(overview?.receipts.action_rulebook_meta?.updated_at)} / {displayValue(overview?.receipts.action_rulebook_meta?.path)}
              </div>
              <div className="status-chip">
                rulebook version: {displayValue(overview?.receipts.action_rulebook_meta?.version ?? overview?.receipts.action_rulebook_meta?.config_version)} / maintainer={displayValue(overview?.receipts.action_rulebook_meta?.maintainer ?? overview?.receipts.action_rulebook_meta?.updated_by)}
              </div>
              <div className="status-chip">
                audit log: {displayValue(overview?.receipts.action_audit_meta?.updated_at)} / {displayValue(overview?.receipts.action_audit_meta?.path)}
              </div>
              <div className="action-strip">
                <label className="field-block">
                  <span>导出格式</span>
                  <select
                    className="control-input"
                    value={snapshotExportFormat}
                    onChange={(event) => setSnapshotExportFormat(event.target.value as "json" | "jsonl" | "csv")}
                  >
                    <option value="json">json</option>
                    <option value="jsonl">jsonl</option>
                    <option value="csv">csv</option>
                  </select>
                </label>
                <button
                  type="button"
                  className="ghost-button"
                  onClick={() => void handleExportGovernanceSnapshot()}
                >
                  {snapshotExporting ? "导出中..." : "导出治理快照"}
                </button>
              </div>
              <div className={`status-chip ${overview?.receipts.action_rulebook_validation?.valid ? "ok" : "danger"}`}>
                rulebook validation: {overview?.receipts.action_rulebook_validation?.valid ? "valid" : "invalid"} / rules={displayValue(overview?.receipts.action_rulebook_validation?.rule_count)}
              </div>
              {overview?.receipts.action_rulebook_validation?.errors?.length
                ? (
                  <div className="data-issue-list">
                    {overview.receipts.action_rulebook_validation.errors.map((item) => (
                      <div key={item} className="status-chip danger">{item}</div>
                    ))}
                  </div>
                )
                : null}
              <div className="stack-gap">
                {governanceActions.map((item) => (
                  <div key={item.action_id} className={`status-chip ${item.tone === "neutral" ? "" : item.tone}`}>
                    {getToneMarker(item.tone as UiTone)} {item.title}: {item.detail}
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={() => void handleGovernanceRecommendation(item.action_type, item.payload)}
                    >
                      {governanceActionLoading === item.action_type ? "执行中..." : item.action_type}
                    </button>
                  </div>
                ))}
              </div>
              <div className="table-shell compact-shell">
                <div className="structure-filter-grid">
                  <label className="field-block">
                    <span>动作类型</span>
                    <input
                      className="control-input"
                      value={actionAuditForm.actionType}
                      onChange={(event) => setActionAuditForm((current) => ({ ...current, actionType: event.target.value }))}
                    />
                  </label>
                  <label className="field-block">
                    <span>来源</span>
                    <input
                      className="control-input"
                      value={actionAuditForm.source}
                      onChange={(event) => setActionAuditForm((current) => ({ ...current, source: event.target.value }))}
                    />
                  </label>
                  <label className="field-block">
                    <span>标的</span>
                    <input
                      className="control-input"
                      value={actionAuditForm.stockCode}
                      onChange={(event) => setActionAuditForm((current) => ({ ...current, stockCode: event.target.value }))}
                    />
                  </label>
                  <label className="field-block">
                    <span>周期</span>
                    <input
                      className="control-input"
                      value={actionAuditForm.period}
                      onChange={(event) => setActionAuditForm((current) => ({ ...current, period: event.target.value }))}
                    />
                  </label>
                  <label className="field-block">
                    <span>lineage</span>
                    <input
                      className="control-input"
                      value={actionAuditForm.lineageAnchor}
                      onChange={(event) => setActionAuditForm((current) => ({ ...current, lineageAnchor: event.target.value }))}
                    />
                  </label>
                  <div className="field-block">
                    <span>&nbsp;</span>
                    <div className="action-strip">
                      <button type="button" className="ghost-button" onClick={() => setActionAuditQuery(actionAuditForm)}>
                        筛选审计
                      </button>
                      <button
                        type="button"
                        className="ghost-button"
                        onClick={() => {
                          const reset = { actionType: "", source: "", stockCode: "", period: "", lineageAnchor: "" };
                          setActionAuditForm(reset);
                          setActionAuditQuery(reset);
                        }}
                      >
                        重置
                      </button>
                    </div>
                  </div>
                </div>
                {actionAuditError ? <div className="status-chip danger">审计读取失败：{actionAuditError}</div> : null}
                <table className="data-table compact-table">
                  <thead>
                    <tr>
                      <th>规则</th>
                      <th>原因</th>
                      <th>严重度</th>
                      <th>SLA</th>
                      <th>建议动作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {overview?.receipts.action_rulebook?.length
                      ? overview.receipts.action_rulebook.map((item) => (
                        <tr key={item.rule_id}>
                          <td>{displayValue(item.rule_id)}</td>
                          <td>{displayValue(item.match_reason)}</td>
                          <td>{displayValue(item.severity)}</td>
                          <td>{displayValue(item.sla_impact)}</td>
                          <td>{displayValue(item.recommended_action)}</td>
                        </tr>
                      ))
                      : (
                        <tr>
                          <td colSpan={5} className="empty-cell">暂无治理规则字典</td>
                        </tr>
                      )}
                  </tbody>
                </table>
              </div>
              <div className="table-shell compact-shell">
                <table className="data-table compact-table">
                  <thead>
                    <tr>
                      <th>时间</th>
                      <th>动作</th>
                      <th>来源</th>
                      <th>标的</th>
                      <th>周期</th>
                      <th>lineage</th>
                      <th>operator</th>
                      <th>说明</th>
                    </tr>
                  </thead>
                  <tbody>
                    {actionAuditData?.length
                      ? actionAuditData.map((item) => (
                        <tr key={item.event_id}>
                          <td>{displayValue(item.event_time)}</td>
                          <td>{displayValue(item.action_type)}</td>
                          <td>{displayValue(item.source)}</td>
                          <td>{displayValue(item.stock_code)}</td>
                          <td>{displayValue(item.period)}</td>
                          <td className="mono-text">{displayValue(item.lineage_anchor ? String(item.lineage_anchor).slice(0, 12) : null)}</td>
                          <td>{displayValue(item.operator)}</td>
                          <td>{displayValue(item.detail)}</td>
                        </tr>
                      ))
                      : (
                        <tr>
                          <td colSpan={8} className="empty-cell">{actionAuditLoading ? "审计日志加载中" : "暂无治理动作审计日志"}</td>
                        </tr>
                      )}
                  </tbody>
                </table>
              </div>
            </article>
            <article className="panel card structure-span-full">
              <h3>Gate 7日趋势</h3>
              <div className="table-shell">
                <table className="data-table compact-table">
                  <thead>
                    <tr>
                      <th>日期</th>
                      <th>总量</th>
                      <th>golden</th>
                      <th>degraded</th>
                      <th>rejected</th>
                      <th>tick mismatch</th>
                      <th>cross source</th>
                    </tr>
                  </thead>
                  <tbody>
                    {overview?.receipts.trend_7d?.length
                      ? overview.receipts.trend_7d.map((item) => (
                        <tr key={item.trade_day}>
                          <td>{displayValue(item.trade_day)}</td>
                          <td>{displayValue(item.total)}</td>
                          <td>{displayValue(item.golden)}</td>
                          <td>{displayValue(item.degraded)}</td>
                          <td>{displayValue(item.rejected)}</td>
                          <td>{displayValue(item.tick_mismatch)}</td>
                          <td>{displayValue(item.cross_source_conflict)}</td>
                        </tr>
                      ))
                      : (
                        <tr>
                          <td colSpan={7} className="empty-cell">暂无 7 日 gate 趋势</td>
                        </tr>
                      )}
                  </tbody>
                </table>
              </div>
            </article>
            {renderDimensionTrendTable("按 Symbol 的 7日趋势", symbolTrendSummary)}
            {renderDimensionTrendTable("按 Period 的 7日趋势", periodTrendSummary)}
            <article className="panel card structure-span-full">
              <h3>Receipt Timeline</h3>
              <div className="structure-filter-grid">
                <label className="field-block">
                  <span>标的</span>
                  <input
                    className="control-input"
                    value={timelineForm.symbol}
                    onChange={(event) => setTimelineForm((current) => ({ ...current, symbol: event.target.value }))}
                    placeholder="000001.SZ"
                  />
                </label>
                <label className="field-block">
                  <span>周期</span>
                  <input
                    className="control-input"
                    value={timelineForm.period}
                    onChange={(event) => setTimelineForm((current) => ({ ...current, period: event.target.value }))}
                    placeholder="1m"
                  />
                </label>
                <label className="field-block">
                  <span>lineage anchor</span>
                  <input
                    className="control-input"
                    value={timelineForm.lineageAnchor}
                    onChange={(event) => setTimelineForm((current) => ({ ...current, lineageAnchor: event.target.value }))}
                    placeholder="anchor..."
                  />
                </label>
                <label className="field-block">
                  <span>回执类型</span>
                  <select
                    className="control-input"
                    value={timelineForm.receiptType}
                    onChange={(event) => setTimelineForm((current) => ({ ...current, receiptType: event.target.value }))}
                  >
                    {TIMELINE_RECEIPT_TYPES.map((value) => (
                      <option key={value || "all"} value={value}>
                        {value || "全部"}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="field-block">
                  <span>拒绝原因</span>
                  <input
                    className="control-input"
                    value={timelineForm.gateRejectReason}
                    onChange={(event) => setTimelineForm((current) => ({ ...current, gateRejectReason: event.target.value }))}
                    placeholder="tick_mismatch"
                  />
                </label>
                <label className="field-block">
                  <span>严重度</span>
                  <select
                    className="control-input"
                    value={timelineForm.severity}
                    onChange={(event) => setTimelineForm((current) => ({ ...current, severity: event.target.value }))}
                  >
                    {TIMELINE_SEVERITIES.map((value) => (
                      <option key={value || "all"} value={value}>
                        {value || "全部"}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="field-block">
                  <span>时间窗口(天)</span>
                  <input
                    className="control-input"
                    value={timelineForm.lookbackDays}
                    onChange={(event) => setTimelineForm((current) => ({ ...current, lookbackDays: event.target.value }))}
                    placeholder="7"
                  />
                </label>
                <div className="field-block">
                  <span>&nbsp;</span>
                  <div className="action-strip">
                    <button type="button" className="ghost-button" onClick={() => setTimelineQuery(timelineForm)}>
                      筛选 Timeline
                    </button>
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={() => {
                        const reset = {
                          symbol: "",
                          period: "",
                          lineageAnchor: "",
                          receiptType: "",
                          gateRejectReason: "",
                          severity: "",
                          lookbackDays: overviewTrendDays,
                        };
                        setTimelineForm(reset);
                        setTimelineQuery(reset);
                        setSelectedLineageAnchor("");
                      }}
                    >
                      重置
                    </button>
                  </div>
                </div>
              </div>
              {timelineError ? <div className="status-chip danger">timeline 读取失败：{timelineError}</div> : null}
              <div className="status-chip">
                timeline returned: {displayValue(timelineData?.returned)} / severity={displayValue(timelineQuery.severity || "all")} / days={displayValue(timelineQuery.lookbackDays)}
              </div>
              <div className="table-shell">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>类型</th>
                      <th>标的</th>
                      <th>周期</th>
                      <th>状态</th>
                      <th>结果</th>
                      <th>严重度</th>
                      <th>SLA 影响</th>
                      <th>锚点</th>
                      <th>联动</th>
                      <th>时间</th>
                    </tr>
                  </thead>
                  <tbody>
                    {timelineData?.items?.length
                      ? timelineData.items.map((item) => {
                        const tone = getGateRejectTone(item.gate_reject_reason ?? item.status ?? undefined);
                        return (
                          <tr key={`${item.receipt_type}-${item.receipt_id}`}>
                            <td>{displayValue(item.receipt_type)}</td>
                            <td>{displayValue(item.stock_code)}</td>
                            <td>{displayValue(item.period)}</td>
                            <td>
                              <span className={getToneClassName(tone)}>
                                {getToneMarker(tone)} {displayValue(item.status)}
                              </span>
                            </td>
                            <td>{displayValue(item.gate_reject_reason ?? item.result_status)}</td>
                            <td>{displayValue(item.severity)}</td>
                            <td>{displayValue(item.sla_impact)}</td>
                            <td className="mono-text">
                              {item.lineage_anchor ? (
                                <button
                                  type="button"
                                  className="ghost-button mono-text"
                                  onClick={() => {
                                    const anchor = String(item.lineage_anchor);
                                    setSelectedLineageAnchor(anchor);
                                    setTimelineForm((current) => ({ ...current, lineageAnchor: anchor }));
                                    setTimelineQuery((current) => ({ ...current, lineageAnchor: anchor }));
                                  }}
                                >
                                  {String(item.lineage_anchor).slice(0, 12)}
                                </button>
                              ) : displayValue(null)}
                            </td>
                            <td>
                              <div className="action-strip">
                                <button
                                  type="button"
                                  className="ghost-button"
                                  onClick={() => {
                                    setTraceabilityForm({
                                      stockCode: String(item.stock_code ?? ""),
                                      period: String(item.period ?? ""),
                                    });
                                    setTraceabilityQuery({
                                      stockCode: String(item.stock_code ?? ""),
                                      period: String(item.period ?? ""),
                                    });
                                    setActiveTab("traceability");
                                  }}
                                >
                                  查溯源
                                </button>
                                <button
                                  type="button"
                                  className="ghost-button"
                                  onClick={() => openWorkbenchLink(String(item.stock_code ?? ""), String(item.period ?? "1d"), "timeline")}
                                >
                                  打开图表
                                </button>
                              </div>
                            </td>
                            <td>{displayValue(item.created_at)}</td>
                          </tr>
                        );
                      })
                      : (
                        <tr>
                          <td colSpan={10} className="empty-cell">{timelineLoading ? "timeline 加载中" : "暂无 receipt timeline"}</td>
                        </tr>
                      )}
                  </tbody>
                </table>
              </div>
            </article>
            <article className="panel card structure-span-full">
              <h3>Lineage Anchor Drilldown</h3>
              {lineageError ? <div className="status-chip danger">lineage 读取失败：{lineageError}</div> : null}
              <div className="summary-card-grid data-summary-grid">
                <div className="status-chip">selected anchor: {displayValue(selectedLineageAnchor || "—")}</div>
                <div className={`status-chip ${lineageLoading ? "warning" : "ok"}`}>
                  {lineageLoading ? "lineage 加载中" : "lineage 就绪"}
                </div>
                <div className="status-chip">latest receipt: {displayValue(lineageDetail?.latest_receipt_id)}</div>
                <div className="status-chip">latest status: {displayValue(lineageDetail?.latest_status)}</div>
              </div>
              <div className="data-issue-list">
                {lineageDetail?.receipt_counts
                  ? Object.entries(lineageDetail.receipt_counts).map(([receiptType, count]) => (
                    <div key={receiptType} className="status-chip">
                      {receiptType}: {displayValue(count)}
                    </div>
                  ))
                  : <div className="status-chip">点击上方锚点查看完整 receipt 链</div>}
              </div>
              <div className="data-issue-list">
                {lineageDetail?.symbols?.length
                  ? lineageDetail.symbols.map((symbol) => (
                    <button
                      key={symbol}
                      type="button"
                      className="ghost-button"
                      onClick={() => {
                        const period = lineageDetail.periods?.[0] ? String(lineageDetail.periods[0]) : "";
                        setTraceabilityForm({ stockCode: symbol, period });
                        setTraceabilityQuery({ stockCode: symbol, period });
                        setActiveTab("traceability");
                      }}
                    >
                      溯源联动 {symbol}
                    </button>
                  ))
                  : null}
              </div>
              <div className="table-shell">
                <table className="data-table compact-table">
                  <thead>
                    <tr>
                      <th>类型</th>
                      <th>标的</th>
                      <th>周期</th>
                      <th>状态</th>
                      <th>结果</th>
                      <th>严重度</th>
                      <th>SLA</th>
                    </tr>
                  </thead>
                  <tbody>
                    {lineageDetail?.timeline?.length
                      ? lineageDetail.timeline.map((item) => (
                        <tr key={`drill-${item.receipt_type}-${item.receipt_id}`}>
                          <td>{displayValue(item.receipt_type)}</td>
                          <td>{displayValue(item.stock_code)}</td>
                          <td>{displayValue(item.period)}</td>
                          <td>{displayValue(item.status)}</td>
                          <td>{displayValue(item.gate_reject_reason ?? item.result_status)}</td>
                          <td>{displayValue(item.severity)}</td>
                          <td>{displayValue(item.sla_impact)}</td>
                        </tr>
                      ))
                      : (
                        <tr>
                          <td colSpan={7} className="empty-cell">暂无 lineage 明细</td>
                        </tr>
                      )}
                  </tbody>
                </table>
              </div>
              <div className="table-shell">
                <table className="data-table compact-table">
                  <thead>
                    <tr>
                      <th>溯源标的</th>
                      <th>周期</th>
                      <th>来源</th>
                      <th>状态</th>
                      <th>质量</th>
                      <th>拒绝原因</th>
                    </tr>
                  </thead>
                  <tbody>
                    {lineageDetail?.traceability_records?.length
                      ? lineageDetail.traceability_records.map((item, index) => (
                        <tr key={`trace-${index}-${item.stock_code}-${item.period}`}>
                          <td>{displayValue(item.stock_code)}</td>
                          <td>{displayValue(item.period)}</td>
                          <td>{displayValue(item.source)}</td>
                          <td>{displayValue(item.status)}</td>
                          <td>{displayValue(item.quality_grade)}</td>
                          <td>{displayValue(item.gate_reject_reason)}</td>
                        </tr>
                      ))
                      : (
                        <tr>
                          <td colSpan={6} className="empty-cell">暂无联动 traceability 记录</td>
                        </tr>
                      )}
                  </tbody>
                </table>
              </div>
            </article>
          </div>
        </div>
      ) : null}

      {activeTab === "calendar" ? (
        <div className="route-stack">
          <article className="panel card">
            <h3>交易日历查询</h3>
            <div className="structure-filter-grid">
              <label className="field-block">
                <span>起始日期</span>
                <input
                  className="control-input"
                  value={calendarForm.startDate}
                  onChange={(event) => setCalendarForm((current) => ({ ...current, startDate: event.target.value }))}
                />
              </label>
              <label className="field-block">
                <span>截止日期</span>
                <input
                  className="control-input"
                  value={calendarForm.endDate}
                  onChange={(event) => setCalendarForm((current) => ({ ...current, endDate: event.target.value }))}
                />
              </label>
              <div className="field-block">
                <span>&nbsp;</span>
                <button type="button" className="ghost-button" onClick={() => setCalendarQuery(calendarForm)}>
                  查询日历
                </button>
              </div>
            </div>
            {calendarError ? <div className="status-chip danger">读取失败：{calendarError}</div> : null}
            <div className="summary-card-grid data-summary-grid">
              <div className="status-chip">日历总天数: {displayValue(calendar?.total_days)}</div>
              <div className="status-chip ok">交易日: {displayValue(calendar?.trading_days)}</div>
              <div className="status-chip warning">非交易日: {displayValue(calendar?.non_trading_days)}</div>
              <div className="status-chip">周末: {displayValue(calendar?.weekend_days)}</div>
              <div className="status-chip">节假日: {displayValue(calendar?.holiday_days)}</div>
              <div className={`status-chip ${calendarLoading ? "warning" : "ok"}`}>{calendarLoading ? `${getToneMarker(getBooleanTone(false))} 交易日历刷新中` : `${getToneMarker(getBooleanTone(true))} 交易日历已刷新`}</div>
            </div>
          </article>

          <div className="data-route-grid">
            <article className="panel card">
              <h3>交易日列表</h3>
              <ul className="event-feed data-list-feed">
                {calendar?.trading_days_list?.length
                  ? calendar.trading_days_list.slice(0, 80).map((item) => (
                    <li key={item} className="event-row"><div className="event-meta"><strong>{item}</strong></div></li>
                  ))
                  : <li className="event-row">暂无交易日列表</li>}
              </ul>
            </article>
            <article className="panel card">
              <h3>非交易日列表</h3>
              <ul className="event-feed data-list-feed">
                {calendar?.non_trading_list?.length
                  ? calendar.non_trading_list.slice(0, 80).map((item) => (
                    <li key={item} className="event-row"><div className="event-meta"><strong>{item}</strong></div></li>
                  ))
                  : <li className="event-row">暂无非交易日列表</li>}
              </ul>
            </article>
          </div>
        </div>
      ) : null}

      {activeTab === "traceability" ? (
        <div className="route-stack">
          <article className="panel card">
            <h3>数据来源溯源</h3>
            <div className="structure-filter-grid">
              <label className="field-block">
                <span>标的代码</span>
                <input
                  className="control-input"
                  placeholder="如 000001.SZ，留空查全部"
                  value={traceabilityForm.stockCode}
                  onChange={(event) => setTraceabilityForm((current) => ({ ...current, stockCode: event.target.value }))}
                />
              </label>
              <label className="field-block">
                <span>周期</span>
                <select
                  className="control-input"
                  value={traceabilityForm.period}
                  onChange={(event) => setTraceabilityForm((current) => ({ ...current, period: event.target.value }))}
                >
                  {TRACEABILITY_PERIODS.map((item) => <option key={item || "all"} value={item}>{item || "全部"}</option>)}
                </select>
              </label>
              <div className="field-block">
                <span>&nbsp;</span>
                <button type="button" className="ghost-button" onClick={() => setTraceabilityQuery(traceabilityForm)}>
                  查询溯源
                </button>
              </div>
            </div>
            {traceabilityError ? <div className="status-chip danger">读取失败：{traceabilityError}</div> : null}
            <div className="summary-card-grid data-summary-grid">
              <div className="status-chip">记录数: {displayValue(traceability?.total)}</div>
              <div className="status-chip">来源种类: {displayValue(traceabilitySummary.sourceCount)}</div>
              <div className={`status-chip ${traceabilitySummary.errorCount > 0 ? "danger" : "ok"}`}>异常状态行: {displayValue(traceabilitySummary.errorCount)}</div>
              <div className={`status-chip ${traceabilityLoading ? "warning" : "ok"}`}>{traceabilityLoading ? `${getToneMarker(getBooleanTone(false))} 溯源刷新中` : `${getToneMarker(getBooleanTone(true))} 溯源已刷新`}</div>
            </div>
          </article>

          <article className="panel card structure-span-full">
            <h3>溯源表</h3>
            <div className="table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>标的</th>
                    <th>周期</th>
                    <th>来源</th>
                    <th>状态</th>
                    <th>行数</th>
                    <th>起始</th>
                    <th>截止</th>
                    <th>更新时间</th>
                    <th>批次</th>
                  </tr>
                </thead>
                <tbody>
                  {traceability?.records?.length
                    ? traceability.records.map((item) => (
                      <tr key={`${item.stock_code}-${item.period}-${item.last_updated}`.replace("undefined", "na")}>
                        <td>{displayValue(item.stock_code)}</td>
                        <td>{displayValue(item.period)}</td>
                        <td>
                          <span className={getToneClassName("neutral")}>
                            {getToneMarker("neutral")} {displayValue(item.source)}
                          </span>
                        </td>
                        <td>
                          <span className={getToneClassName(getTraceabilityStatusTone(item.status))}>
                            {getToneMarker(getTraceabilityStatusTone(item.status))} {displayValue(item.status)}
                          </span>
                        </td>
                        <td>{displayValue(item.record_count)}</td>
                        <td>{displayValue(item.start_date)}</td>
                        <td>{displayValue(item.end_date)}</td>
                        <td>{displayValue(item.last_updated)}</td>
                          <td className="mono-text">
                            <div className="action-strip">
                              <span>{displayValue(item.ingest_run_id)}</span>
                              <button
                                type="button"
                                className="ghost-button"
                                onClick={() => {
                                  setTimelineForm({
                                    symbol: String(item.stock_code ?? ""),
                                    period: String(item.period ?? ""),
                                    lineageAnchor: "",
                                    receiptType: "",
                                    gateRejectReason: String(item.gate_reject_reason ?? ""),
                                    severity: "",
                                    lookbackDays: overviewTrendDays,
                                  });
                                  setTimelineQuery({
                                    symbol: String(item.stock_code ?? ""),
                                    period: String(item.period ?? ""),
                                    lineageAnchor: "",
                                    receiptType: "",
                                    gateRejectReason: String(item.gate_reject_reason ?? ""),
                                    severity: "",
                                    lookbackDays: overviewTrendDays,
                                  });
                                  setActiveTab("overview");
                                }}
                              >
                                看 Timeline
                              </button>
                              <button
                                type="button"
                                className="ghost-button"
                                onClick={() => openWorkbenchLink(String(item.stock_code ?? ""), String(item.period ?? "1d"), "traceability")}
                              >
                                打开图表
                              </button>
                            </div>
                          </td>
                      </tr>
                    ))
                    : (
                      <tr>
                        <td colSpan={9} className="empty-cell">当前筛选条件下没有溯源记录</td>
                      </tr>
                    )}
                </tbody>
              </table>
            </div>
          </article>
        </div>
      ) : null}

      {activeTab === "repair" ? (
        <div className="route-stack">
          <article className="panel card">
            <h3>Golden 1D Repair 计划</h3>
            {repairError ? <div className="status-chip danger">读取失败：{repairError}</div> : null}
            <div className="summary-card-grid data-summary-grid">
              <div className="status-chip">计划数: {displayValue(repairSummary.total)}</div>
              <div className={`status-chip ${repairSummary.queued > 0 ? "ok" : ""}`}>queued: {displayValue(repairSummary.queued)}</div>
              <div className={`status-chip ${repairSummary.blocked > 0 ? "danger" : ""}`}>blocked: {displayValue(repairSummary.blocked)}</div>
              <div className={`status-chip ${repairSummary.manualReview > 0 ? "danger" : ""}`}>manual_review: {displayValue(repairSummary.manualReview)}</div>
              <div className={`status-chip ${repairSummary.failed > 0 ? "danger" : ""}`}>failed: {displayValue(repairSummary.failed)}</div>
              <div className={`status-chip ${(repairReceipts?.returned ?? 0) > 0 ? "ok" : ""}`}>repair receipts: {displayValue(repairReceipts?.returned)}</div>
              <div className={`status-chip ${(replayReceipts?.returned ?? 0) > 0 ? "ok" : ""}`}>replay receipts: {displayValue(replayReceipts?.returned)}</div>
              <div className={`status-chip ${repairLoading ? "warning" : "ok"}`}>{repairLoading ? `${getToneMarker(getBooleanTone(false))} 队列刷新中` : `${getToneMarker(getBooleanTone(true))} 队列已刷新`}</div>
            </div>
          </article>

          <article className="panel card structure-span-full">
            <h3>最近 Repair Plans</h3>
            <div className="table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>标的</th>
                    <th>计划状态</th>
                    <th>queued</th>
                    <th>failed</th>
                    <th>任务数</th>
                    <th>生成时间</th>
                    <th>notes / blockers</th>
                  </tr>
                </thead>
                <tbody>
                  {repairPlans?.items?.length
                    ? repairPlans.items.map((item) => {
                      const tone = getRepairPlanTone(item.plan_status);
                      return (
                        <tr key={`${item.symbol}-${item.generated_at ?? "na"}`}>
                          <td>{displayValue(item.symbol)}</td>
                          <td>
                            <span className={getToneClassName(tone)}>
                              {getToneMarker(tone)} {displayValue(item.plan_status)}
                            </span>
                          </td>
                          <td>{displayValue(item.queued_tasks)}</td>
                          <td>{displayValue(item.failed_tasks)}</td>
                          <td>{displayValue(item.task_count)}</td>
                          <td>{displayValue(item.generated_at)}</td>
                          <td>
                            <div className="chart-capability-grid">
                              {(item.blocker_issues ?? []).map((issue) => (
                                <span key={`${item.symbol}-block-${issue}`} className={`status-chip ${tone === "neutral" ? "warning" : tone}`}>blocker · {issue}</span>
                              ))}
                              {(item.notes ?? []).map((note) => (
                                <span key={`${item.symbol}-note-${note}`} className={`status-chip ${tone === "neutral" ? "warning" : tone}`}>{note}</span>
                              ))}
                              {(item.tasks ?? []).map((task) => (
                                <span key={`${item.symbol}-${task.period}-${task.reason}-${task.start_date}`} className={`status-chip ${tone === "neutral" ? "warning" : tone}`}>{task.period} · {task.reason}</span>
                              ))}
                              {(
                                (item.blocker_issues?.length ?? 0)
                                + (item.notes?.length ?? 0)
                                + (item.tasks?.length ?? 0)
                              ) > 0 ? null : <span className="status-chip">暂无细节</span>}
                            </div>
                          </td>
                        </tr>
                      );
                    })
                    : (
                      <tr>
                        <td colSpan={7} className="empty-cell">暂无 Golden 1D repair plan</td>
                      </tr>
                    )}
                </tbody>
              </table>
            </div>
          </article>
          <article className="panel card structure-span-full">
            <h3>最新 Repair / Replay Receipts</h3>
            <div className="table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>类型</th>
                    <th>标的</th>
                    <th>周期</th>
                    <th>状态</th>
                    <th>区间</th>
                    <th>附加信息</th>
                    <th>动作</th>
                  </tr>
                </thead>
                <tbody>
                  {[...(repairReceipts?.items ?? []).map((item) => ({ ...item, receiptType: "repair" })), ...(replayReceipts?.items ?? []).map((item) => ({ ...item, receiptType: "replay" }))]
                    .slice(0, 12)
                    .map((item) => {
                      const tone = getRepairPlanTone(item.status ?? undefined);
                      return (
                        <tr key={`${item.receiptType}-${item.receipt_id}`}>
                          <td>{displayValue(item.receiptType)}</td>
                          <td>{displayValue(item.stock_code)}</td>
                          <td>{displayValue(item.period)}</td>
                          <td>
                            <span className={getToneClassName(tone)}>
                              {getToneMarker(tone)} {displayValue(item.status)}
                            </span>
                          </td>
                          <td>{displayValue(item.range_start)} → {displayValue(item.range_end)}</td>
                          <td>
                            <div className="chart-capability-grid">
                              {item.reason ? <span className="status-chip">{item.reason}</span> : null}
                              {item.replay_kind ? <span className="status-chip">{item.replay_kind}</span> : null}
                              {item.related_gate_receipt_id ? <span className="status-chip">gate linked</span> : null}
                              {item.related_repair_receipt_id ? <span className="status-chip">repair linked</span> : null}
                              {item.related_replay_receipt_id ? <span className="status-chip">replay linked</span> : null}
                              {item.lineage_anchor ? <span className="status-chip">anchor={String(item.lineage_anchor).slice(0, 8)}</span> : null}
                              {item.tick_verified !== undefined && item.tick_verified !== null ? <span className={`status-chip ${item.tick_verified ? "ok" : "warning"}`}>tick={displayValue(item.tick_verified)}</span> : null}
                              {item.replayable !== undefined && item.replayable !== null ? <span className={`status-chip ${item.replayable ? "ok" : "warning"}`}>replay={displayValue(item.replayable)}</span> : null}
                              {item.last_error ? <span className="status-chip danger">{item.last_error}</span> : null}
                              {!item.reason && !item.replay_kind && !item.related_gate_receipt_id && !item.related_repair_receipt_id && !item.related_replay_receipt_id && !item.lineage_anchor && item.tick_verified === undefined && item.replayable === undefined && !item.last_error ? <span className="status-chip">暂无细节</span> : null}
                            </div>
                          </td>
                          <td>
                            <div className="action-strip">
                              {item.receiptType === "repair" ? (
                                <button
                                  type="button"
                                  className="ghost-button"
                                  onClick={() => {
                                    void triggerGolden1dRepair(String(item.stock_code ?? ""), false)
                                      .then((payload) => setGovernanceActionFeedback(`repair 已触发: ${displayValue(payload.status)}`))
                                      .catch((reason) => setGovernanceActionFeedback(reason instanceof Error ? reason.message : "repair 触发失败"));
                                  }}
                                >
                                  触发 Repair
                                </button>
                              ) : null}
                              {item.receiptType === "replay" ? (
                                <button
                                  type="button"
                                  className="ghost-button"
                                  onClick={() => {
                                    void triggerLateEventReplay(String(item.stock_code ?? ""), String(item.period ?? ""), 3, 20)
                                      .then((payload) => setGovernanceActionFeedback(`replay 已触发: succeeded=${displayValue(payload.result.succeeded)}`))
                                      .catch((reason) => setGovernanceActionFeedback(reason instanceof Error ? reason.message : "replay 触发失败"));
                                  }}
                                >
                                  触发 Replay
                                </button>
                              ) : null}
                              <button
                                type="button"
                                className="ghost-button"
                                onClick={() => openWorkbenchLink(String(item.stock_code ?? ""), String(item.period ?? "1d"), "receipt-action")}
                              >
                                打开图表
                              </button>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  {((repairReceipts?.items?.length ?? 0) + (replayReceipts?.items?.length ?? 0)) === 0 ? (
                    <tr>
                      <td colSpan={7} className="empty-cell">暂无 repair / replay receipt</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </article>
        </div>
      ) : null}
    </section>
  );
}
