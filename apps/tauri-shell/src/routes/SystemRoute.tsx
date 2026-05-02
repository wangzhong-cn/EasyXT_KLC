import { useCallback, useEffect, useMemo, useState } from "react";

import {
  fetchGolden1dRepairPlans,
  fetchSystemFrontendEvents,
  fetchSystemStateStatus,
  type Golden1dRepairPlanListPayload,
  type SystemFrontendEventsPayload,
  type SystemStatePayload,
} from "../lib/api";
import { getBooleanTone, getToneClassName, getToneMarker, type UiTone } from "../lib/uiTone";

function displayValue(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  return String(value);
}

function formatPayload(value: unknown): string {
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

function getSyncTone(status: string | null | undefined): UiTone {
  switch (status) {
    case "ready":
    case "ok":
      return "ok";
    case "failed":
    case "error":
      return "danger";
    case "syncing":
    case "degraded":
      return "warning";
    default:
      return "neutral";
  }
}

function getEventTone(eventType: string | null | undefined): UiTone {
  const normalized = (eventType ?? "").toLowerCase();
  if (!normalized) {
    return "neutral";
  }
  if (normalized.includes("error") || normalized.includes("fail")) {
    return "danger";
  }
  if (normalized.includes("warn") || normalized.includes("lag") || normalized.includes("degraded")) {
    return "warning";
  }
  if (normalized.includes("ready") || normalized.includes("ok") || normalized.includes("success")) {
    return "ok";
  }
  return "neutral";
}

function getRepairPlanTone(status: string | null | undefined): UiTone {
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

export function SystemRoute() {
  const [activeTab, setActiveTab] = useState<"overview" | "events" | "repair">("overview");
  const [payload, setPayload] = useState<SystemStatePayload | null>(null);
  const [eventsPayload, setEventsPayload] = useState<SystemFrontendEventsPayload | null>(null);
  const [repairPayload, setRepairPayload] = useState<Golden1dRepairPlanListPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [eventsError, setEventsError] = useState<string | null>(null);
  const [repairError, setRepairError] = useState<string | null>(null);

  const loadSnapshot = useCallback(async (signal?: AbortSignal) => {
    setRefreshing(true);
    try {
      const [statusResult, eventsResult, repairResult] = await Promise.allSettled([
        fetchSystemStateStatus(signal),
        fetchSystemFrontendEvents(signal),
        fetchGolden1dRepairPlans(10, signal),
      ]);

      if (statusResult.status === "fulfilled") {
        setPayload(statusResult.value);
        setError(null);
      } else {
        const message = statusResult.reason instanceof Error
          ? statusResult.reason.message
          : "系统状态读取失败";
        setError(message);
      }

      if (eventsResult.status === "fulfilled") {
        setEventsPayload(eventsResult.value);
        setEventsError(null);
      } else {
        const message = eventsResult.reason instanceof Error
          ? eventsResult.reason.message
          : "系统事件读取失败";
        setEventsError(message);
      }

      if (repairResult.status === "fulfilled") {
        setRepairPayload(repairResult.value);
        setRepairError(null);
      } else {
        const message = repairResult.reason instanceof Error
          ? repairResult.reason.message
          : "Golden 1D repair 计划读取失败";
        setRepairError(message);
        setRepairPayload(null);
      }
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    async function refresh() {
      if (!active) {
        return;
      }
      await loadSnapshot(controller.signal);
    }

    void refresh();
    const intervalId = window.setInterval(() => {
      void refresh();
    }, 15000);

    return () => {
      active = false;
      controller.abort();
      window.clearInterval(intervalId);
    };
  }, [loadSnapshot]);

  const repairSummary = useMemo(() => {
    const items = repairPayload?.items ?? [];
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
  }, [repairPayload]);

  const summaryCards = useMemo(
    () => [
      ["SQLite 逻辑序号", displayValue(payload?.sqlite_logical_seq), "neutral"],
      ["活动分片", displayValue(payload?.active_shard_id), "neutral"],
      ["影子版本", displayValue(payload?.duckdb_shadow_version), getSyncTone(payload?.sync_status)],
      ["同步状态", displayValue(payload?.sync_status), getSyncTone(payload?.sync_status)],
      ["最后良好版本", displayValue(payload?.last_good_version), "neutral"],
      ["最近备份", displayValue(payload?.backup_last_success_at), "neutral"],
      ["repair queued", displayValue(repairSummary.queued), repairSummary.queued > 0 ? "ok" : "neutral"],
      ["repair blocked", displayValue(repairSummary.blocked + repairSummary.manualReview + repairSummary.failed), repairSummary.blocked + repairSummary.manualReview + repairSummary.failed > 0 ? "danger" : "neutral"],
      ["联邦执行器", payload?.federation_executor_ready ? "ready" : "unavailable", getBooleanTone(payload?.federation_executor_ready)],
      ["刷新状态", refreshing ? "刷新中" : "已同步", refreshing ? "warning" : "ok"],
    ],
    [payload, refreshing, repairSummary],
  );

  const stateMetrics = useMemo(
    () => [
      ["state_root", displayValue(payload?.state_root)],
      ["catalog_path", displayValue(payload?.catalog_path)],
      ["active_shard_count", displayValue(payload?.active_shard_count)],
      ["federation_attach_budget", displayValue(payload?.federation_attach_budget)],
      ["build_version", displayValue(payload?.build_version)],
      ["commit_sha", displayValue(payload?.commit_sha)],
    ],
    [payload],
  );

  const eventSummary = useMemo(
    () => [
      ["事件来源", displayValue(eventsPayload?.source)],
      ["attached_shards", displayValue(eventsPayload?.attached_shards)],
      ["latest_logical_seq", displayValue(eventsPayload?.latest_logical_seq)],
      ["returned", displayValue(eventsPayload?.returned)],
    ],
    [eventsPayload],
  );

  const repairHeaderSummary = useMemo(
    () => [
      ["计划数", displayValue(repairSummary.total)],
      ["queued", displayValue(repairSummary.queued)],
      ["blocked", displayValue(repairSummary.blocked)],
      ["manual_review", displayValue(repairSummary.manualReview)],
      ["failed", displayValue(repairSummary.failed)],
    ],
    [repairSummary],
  );

  return (
    <section className="route-stack">
      <header className="panel card route-header">
        <h2>同步状态 / 影子版本</h2>
        <p>
          对照 `09_frontend_reference_atlas.md` 的 Route D，这一页优先回答三个问题：系统现在是否健康、
          最近失败在哪里、前端到底读到了哪一版状态快照。
        </p>
      </header>

      <article className="panel card">
        <div className="action-strip">
          <button type="button" className="ghost-button" onClick={() => void loadSnapshot()}>
            刷新系统快照
          </button>
          <div className="inline-note">SystemRoute 现在遵循 Route D：先展示系统当下状态，再展示最近事件与失败阶段。</div>
        </div>
        {error ? <div className="status-chip danger">系统状态读取失败：{error}</div> : null}
        {eventsError ? <div className="status-chip danger">事件读模型失败：{eventsError}</div> : null}
        {repairError ? <div className="status-chip danger">repair 读模型失败：{repairError}</div> : null}
        <div className="summary-card-grid">
          {summaryCards.map(([label, value, tone]) => (
            <div key={String(label)} className={`status-chip ${tone === "neutral" ? "" : String(tone)}`}>
              {label}: <strong>{value}</strong>
            </div>
          ))}
        </div>
      </article>

      <nav className="bottom-tabs route-tab-nav" aria-label="系统页签切换">
        <button type="button" className={activeTab === "overview" ? "active" : ""} onClick={() => setActiveTab("overview")}>
          系统概览
        </button>
        <button type="button" className={activeTab === "events" ? "active" : ""} onClick={() => setActiveTab("events")}>
          最近事件
        </button>
        <button type="button" className={activeTab === "repair" ? "active" : ""} onClick={() => setActiveTab("repair")}>
          Repair 计划
        </button>
      </nav>

      {activeTab === "overview" ? (
        <div className="system-grid">
          <article className="panel card">
            <h3>主状态快照</h3>
            <div className="system-meta">
              <span>state_root: <code className="mono-text">{displayValue(payload?.state_root)}</code></span>
              <span>catalog_path: <code className="mono-text">{displayValue(payload?.catalog_path)}</code></span>
              <span>active_shard_count: <strong>{displayValue(payload?.active_shard_count)}</strong></span>
            </div>
            <ul className="metric-list large">
              {stateMetrics.map(([label, value]) => (
                <li key={String(label)}><span>{label}</span><strong>{value}</strong></li>
              ))}
            </ul>
          </article>

          <article className="panel card">
            <h3>告警与恢复</h3>
            <div className="stack-gap">
              <div className={`status-chip ${getSyncTone(payload?.sync_status)}`}>
                同步状态: {getToneMarker(getSyncTone(payload?.sync_status))} {displayValue(payload?.sync_status)}
              </div>
              <div className={`status-chip ${payload?.shadow_failed_stage ? "danger" : "warning"}`}>
                失败阶段: {displayValue(payload?.shadow_failed_stage)}
              </div>
              <div className={`status-chip ${payload?.shadow_error ? "danger" : "warning"}`}>
                影子错误: <span className="mono-text">{displayValue(payload?.shadow_error)}</span>
              </div>
              <div className={`status-chip ${getBooleanTone(payload?.federation_executor_ready)}`}>
                联邦执行器: {getToneMarker(getBooleanTone(payload?.federation_executor_ready))} {payload?.federation_executor_ready ? "ready" : "unavailable"}
              </div>
              <div className="status-chip">
                附着预算: {displayValue(payload?.federation_attach_budget)}
              </div>
              <div className="status-chip">
                manifest: <span className="mono-text">{displayValue(payload?.shadow_manifest_path)}</span>
              </div>
              {loading ? <div className="status-chip warning">{getToneMarker("warning")} 系统状态首次加载中</div> : null}
            </div>
          </article>
        </div>
      ) : null}

      {activeTab === "events" ? (
        <article className="panel card system-span-full">
          <h3>最近状态事件</h3>
          <div className="summary-card-grid">
            {eventSummary.map(([label, value]) => (
              <div key={String(label)} className="status-chip">
                {label}: <strong>{value}</strong>
              </div>
            ))}
          </div>
          <ul className="event-feed">
            {eventsPayload?.items?.length
              ? eventsPayload.items.map((item) => {
                const tone = getEventTone(item.event_type);
                return (
                  <li key={`${item.event_id ?? item.event_ts ?? "event"}`} className="event-row">
                    <div className="event-meta">
                      <strong className={getToneClassName(tone)}>
                        {getToneMarker(tone)} {displayValue(item.event_type)}
                      </strong>
                      <span>{displayValue(item.event_ts)}</span>
                    </div>
                    <div className="event-meta mono-text">event_id: {displayValue(item.event_id)}</div>
                    <div className="event-payload mono-text">{formatPayload(item.payload ?? item.raw_payload_json)}</div>
                  </li>
                );
              })
              : <li className="event-row">暂无 frontend_events 读模型数据</li>}
          </ul>
        </article>
      ) : null}

      {activeTab === "repair" ? (
        <article className="panel card system-span-full">
          <h3>Golden 1D Repair 计划</h3>
          <div className="summary-card-grid">
            {repairHeaderSummary.map(([label, value]) => (
              <div key={String(label)} className="status-chip">
                {label}: <strong>{value}</strong>
              </div>
            ))}
          </div>
          <ul className="event-feed">
            {repairPayload?.items?.length
              ? repairPayload.items.map((item) => {
                const tone = getRepairPlanTone(item.plan_status);
                return (
                  <li key={`${item.symbol}-${item.generated_at ?? "unknown"}`} className="event-row">
                    <div className="event-meta">
                      <strong className={getToneClassName(tone)}>
                        {getToneMarker(tone)} {item.symbol} · {displayValue(item.plan_status)}
                      </strong>
                      <span>{displayValue(item.generated_at)}</span>
                    </div>
                    <div className="event-meta">
                      <span>queued {displayValue(item.queued_tasks)}</span>
                      <span>failed {displayValue(item.failed_tasks)}</span>
                      <span>tasks {displayValue(item.task_count)}</span>
                    </div>
                    <div className="chart-capability-grid">
                      {(item.blocker_issues ?? []).map((issue) => (
                        <span key={`${item.symbol}-blocker-${issue}`} className={`status-chip ${tone === "neutral" ? "warning" : tone}`}>
                          blocker · {issue}
                        </span>
                      ))}
                      {(item.notes ?? []).map((note) => (
                        <span key={`${item.symbol}-note-${note}`} className={`status-chip ${tone === "neutral" ? "warning" : tone}`}>
                          {note}
                        </span>
                      ))}
                      {(item.tasks ?? []).map((task) => (
                        <span key={`${item.symbol}-${task.period}-${task.start_date}-${task.reason}`} className={`status-chip ${tone === "neutral" ? "warning" : tone}`}>
                          {task.period} · {task.reason}
                        </span>
                      ))}
                    </div>
                  </li>
                );
              })
              : <li className="event-row">暂无 Golden 1D repair plan</li>}
          </ul>
        </article>
      ) : null}
    </section>
  );
}