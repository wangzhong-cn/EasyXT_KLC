import { useEffect, useMemo, useState } from "react";

import {
  applyStructureBayesianBaseline,
  fetchStructureBayesianSummary,
  fetchStructureDetail,
  fetchStructuredSignals,
  fetchStructures,
  type BayesianSummaryItemPayload,
  type StructureDetailPayload,
  type StructureItemPayload,
  type StructuredSignalItemPayload,
  type StructureQueryParams,
} from "../lib/api";
import { getBooleanTone, getToneClassName, getToneMarker, type UiTone } from "../lib/uiTone";

const INTERVAL_OPTIONS = ["1d", "5m", "1m", "15m", "30m", "60m"];
const STATUS_OPTIONS = [
  { value: "", label: "全部状态" },
  { value: "active", label: "active" },
  { value: "closed", label: "closed" },
  { value: "reversed", label: "reversed" },
];
const SIGNAL_OPTIONS = [
  { value: "", label: "全部信号" },
  { value: "LONG", label: "LONG" },
  { value: "SHORT", label: "SHORT" },
  { value: "EXIT", label: "EXIT" },
  { value: "HOLD", label: "HOLD" },
];
const GROUP_STRATEGY_OPTIONS = [
  { value: "adaptive", label: "自适应 Bayes" },
  { value: "fixed", label: "固定 Bayes" },
];

type QueryFormState = StructureQueryParams;

function displayValue(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  return String(value);
}

function formatNumber(value: number | null | undefined, digits = 3): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return value.toFixed(digits);
}

function formatPercent(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return `${value.toFixed(digits)}%`;
}

function formatShortId(value: string | null | undefined): string {
  if (!value) {
    return "—";
  }
  return value.length > 10 ? `${value.slice(0, 8)}…` : value;
}

function getStructureStatusTone(status: string | null | undefined): UiTone {
  switch (status) {
    case "active":
      return "ok";
    case "closed":
      return "warning";
    case "reversed":
      return "danger";
    default:
      return "neutral";
  }
}

function getDirectionTone(direction: string | null | undefined): UiTone {
  switch (direction) {
    case "up":
      return "ok";
    case "down":
      return "warning";
    default:
      return "neutral";
  }
}

function getSignalTone(signalType: string | null | undefined): UiTone {
  switch (signalType) {
    case "LONG":
      return "ok";
    case "SHORT":
      return "warning";
    case "EXIT":
      return "danger";
    case "HOLD":
    default:
      return "neutral";
  }
}

export function StructureRoute() {
  const [form, setForm] = useState<QueryFormState>({
    code: "000001.SZ",
    interval: "1d",
    status: "",
    signalType: "",
    groupStrategy: "adaptive",
    minObservations: 3,
    limit: 60,
    offset: 0,
  });
  const [query, setQuery] = useState<QueryFormState>(form);
  const [structures, setStructures] = useState<StructureItemPayload[]>([]);
  const [signals, setSignals] = useState<StructuredSignalItemPayload[]>([]);
  const [bayesSummary, setBayesSummary] = useState<BayesianSummaryItemPayload[]>([]);
  const [selectedStructureId, setSelectedStructureId] = useState<string | null>(null);
  const [detail, setDetail] = useState<StructureDetailPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [applying, setApplying] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [actionTone, setActionTone] = useState<UiTone>("ok");

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    async function loadStructureSnapshot() {
      setLoading(true);
      setError(null);
      try {
        const [structuresResult, signalsResult, summaryResult] = await Promise.allSettled([
          fetchStructures(query, controller.signal),
          fetchStructuredSignals(query, controller.signal),
          fetchStructureBayesianSummary(query, controller.signal),
        ]);
        if (!active) {
          return;
        }

        if (structuresResult.status === "fulfilled") {
          setStructures(structuresResult.value.items);
        } else {
          throw structuresResult.reason;
        }

        if (signalsResult.status === "fulfilled") {
          setSignals(signalsResult.value.items);
        } else {
          setSignals([]);
        }

        if (summaryResult.status === "fulfilled") {
          setBayesSummary(summaryResult.value.items);
        } else {
          setBayesSummary([]);
        }
      } catch (reason) {
        if (!active) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "结构分析读取失败";
        setError(message);
        setStructures([]);
        setSignals([]);
        setBayesSummary([]);
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }

    void loadStructureSnapshot();
    return () => {
      active = false;
      controller.abort();
    };
  }, [query]);

  useEffect(() => {
    if (!structures.length) {
      setSelectedStructureId(null);
      setDetail(null);
      return;
    }
    const stillExists = structures.some((item) => item.structure_id === selectedStructureId);
    if (!stillExists) {
      setSelectedStructureId(structures[0].structure_id);
    }
  }, [structures, selectedStructureId]);

  useEffect(() => {
    if (!selectedStructureId) {
      setDetail(null);
      setDetailError(null);
      return;
    }
    const structureId = selectedStructureId;
    let active = true;
    const controller = new AbortController();
    setDetailError(null);

    async function loadDetail() {
      try {
        const payload = await fetchStructureDetail(
          structureId,
          query.groupStrategy,
          query.minObservations,
          controller.signal,
        );
        if (!active) {
          return;
        }
        setDetail(payload);
      } catch (reason) {
        if (!active) {
          return;
        }
        const message = reason instanceof Error ? reason.message : "结构详情读取失败";
        setDetailError(message);
        setDetail(null);
      }
    }

    void loadDetail();
    return () => {
      active = false;
      controller.abort();
    };
  }, [selectedStructureId, query.groupStrategy, query.minObservations]);

  const drawdownSummary = useMemo(() => {
    const latestSignal = signals[0] ?? null;
    const drawdowns = signals
      .map((item) => item.risk.drawdown_pct)
      .filter((value): value is number => typeof value === "number");
    const latestStructure = structures[0] ?? null;
    return {
      latestDrawdown: latestSignal?.risk.drawdown_pct ?? null,
      maxDrawdown: drawdowns.length ? Math.max(...drawdowns) : null,
      calmar: latestSignal?.risk.calmar_snapshot ?? null,
      latestDirection: latestStructure?.direction ?? null,
      latestStatus: latestStructure?.status ?? null,
      latestStopLoss: latestSignal?.risk.stop_loss_price ?? latestStructure?.points.p2.price ?? null,
    };
  }, [signals, structures]);

  const topSummary = bayesSummary[0] ?? null;
  const selectedStructure = structures.find((item) => item.structure_id === selectedStructureId) ?? null;

  async function handleApplyBayes() {
    setApplying(true);
    setActionMessage(null);
    try {
      const payload = await applyStructureBayesianBaseline(query);
      setActionTone("ok");
      setActionMessage(`Bayesian 写回完成：updated=${payload.updated}`);
      setQuery({ ...query });
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : "Bayesian 写回失败";
      setActionTone("danger");
      setActionMessage(message);
    } finally {
      setApplying(false);
    }
  }

  return (
    <section className="route-stack">
      <header className="panel card route-header">
        <h2>结构分析 / 信号 / 回撤</h2>
        <p>
          这是从 `StructureMonitorPanel` 迁出的第一版 Tauri 骨架：优先承接结构监控、信号表、
          回撤摘要与 Bayesian 摘要，减少主验证路径对 Qt 壳的依赖。
        </p>
      </header>

      <div className="structure-grid">
        <article className="panel card">
          <h3>查询条件</h3>
          <div className="structure-filter-grid">
            <label className="field-block">
              <span>股票代码</span>
              <input
                className="control-input"
                value={form.code}
                onChange={(event) => setForm((current) => ({ ...current, code: event.target.value }))}
              />
            </label>
            <label className="field-block">
              <span>周期</span>
              <select
                className="control-input"
                value={form.interval}
                onChange={(event) => setForm((current) => ({ ...current, interval: event.target.value }))}
              >
                {INTERVAL_OPTIONS.map((item) => <option key={item} value={item}>{item}</option>)}
              </select>
            </label>
            <label className="field-block">
              <span>结构状态</span>
              <select
                className="control-input"
                value={form.status}
                onChange={(event) => setForm((current) => ({ ...current, status: event.target.value }))}
              >
                {STATUS_OPTIONS.map((item) => <option key={item.value || "all"} value={item.value}>{item.label}</option>)}
              </select>
            </label>
            <label className="field-block">
              <span>信号过滤</span>
              <select
                className="control-input"
                value={form.signalType}
                onChange={(event) => setForm((current) => ({ ...current, signalType: event.target.value }))}
              >
                {SIGNAL_OPTIONS.map((item) => <option key={item.value || "all"} value={item.value}>{item.label}</option>)}
              </select>
            </label>
            <label className="field-block">
              <span>Bayes 模式</span>
              <select
                className="control-input"
                value={form.groupStrategy}
                onChange={(event) => setForm((current) => ({ ...current, groupStrategy: event.target.value }))}
              >
                {GROUP_STRATEGY_OPTIONS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
              </select>
            </label>
            <label className="field-block">
              <span>最小样本数</span>
              <input
                className="control-input"
                type="number"
                min={1}
                value={form.minObservations}
                onChange={(event) => setForm((current) => ({
                  ...current,
                  minObservations: Math.max(1, Number(event.target.value || 1)),
                }))}
              />
            </label>
          </div>
          <div className="action-strip">
            <button type="button" className="ghost-button" onClick={() => setQuery({ ...form, offset: 0 })}>
              刷新只读
            </button>
            <button type="button" className="ghost-button" onClick={() => setForm(query)}>
              回滚到当前查询
            </button>
            <button type="button" className="ghost-button" onClick={() => void handleApplyBayes()} disabled={applying}>
              {applying ? "正在写回…" : "写回 Bayesian 区间"}
            </button>
          </div>
          <div className="inline-note">
            当前主路径：API only / Qt freeze / `.venv` + headless 验证优先。
          </div>
          {actionMessage ? <div className={`status-chip ${actionTone}`}>{actionMessage}</div> : null}
          {error ? <div className="status-chip danger">读取失败：{error}</div> : null}
        </article>

        <article className="panel card">
          <h3>回撤摘要</h3>
          <ul className="metric-list large">
            <li><span>结构数量</span><strong>{displayValue(structures.length)}</strong></li>
            <li><span>信号数量</span><strong>{displayValue(signals.length)}</strong></li>
            <li><span>最新回撤</span><strong>{formatPercent(drawdownSummary.latestDrawdown)}</strong></li>
            <li><span>最大回撤</span><strong>{formatPercent(drawdownSummary.maxDrawdown)}</strong></li>
            <li><span>Calmar 快照</span><strong>{formatNumber(drawdownSummary.calmar, 2)}</strong></li>
            <li>
              <span>最新方向 / 状态</span>
              <strong>
                <span className={getToneClassName(getDirectionTone(drawdownSummary.latestDirection))}>
                  {getToneMarker(getDirectionTone(drawdownSummary.latestDirection))} {displayValue(drawdownSummary.latestDirection)}
                </span>
                {" / "}
                <span className={getToneClassName(getStructureStatusTone(drawdownSummary.latestStatus))}>
                  {getToneMarker(getStructureStatusTone(drawdownSummary.latestStatus))} {displayValue(drawdownSummary.latestStatus)}
                </span>
              </strong>
            </li>
            <li><span>最新止损位（P2）</span><strong>{formatNumber(drawdownSummary.latestStopLoss, 4)}</strong></li>
          </ul>
        </article>

        <article className="panel card structure-span-full">
          <h3>Bayesian 摘要（预览即读取）</h3>
          <div className="summary-card-grid structure-summary-grid">
            <div className="status-chip">
              分组层级: <strong>{displayValue(topSummary?.bayes_group_level)}</strong>
            </div>
            <div className="status-chip">
              桶数量: <strong>{displayValue(topSummary?.bucket_count)}</strong>
            </div>
            <div className="status-chip">
              结构数: <strong>{displayValue(topSummary?.structure_count)}</strong>
            </div>
            <div className="status-chip">
              后验均值: <strong>{formatNumber(topSummary?.mean_posterior_mean)}</strong>
            </div>
            <div className="status-chip">
              平均样本数: <strong>{formatNumber(topSummary?.mean_observation_count, 2)}</strong>
            </div>
            <div className="status-chip">
              反转比率: <strong>{formatNumber(topSummary?.reversed_ratio)}</strong>
            </div>
            <div className="status-chip">
              平均审计事件: <strong>{formatNumber(topSummary?.mean_audit_event_count, 2)}</strong>
            </div>
            <div className={`status-chip ${loading ? "warning" : "ok"}`}>
              {loading ? `${getToneMarker(getBooleanTone(false))} 结构与摘要刷新中` : `${getToneMarker(getBooleanTone(true))} 结构快照已刷新`}
            </div>
          </div>
        </article>

        <article className="panel card structure-span-full">
          <h3>结构表</h3>
          <div className="table-shell">
            <table className="data-table">
              <thead>
                <tr>
                  <th>结构</th>
                  <th>方向</th>
                  <th>状态</th>
                  <th>P0</th>
                  <th>P1</th>
                  <th>P2</th>
                  <th>P3</th>
                  <th>折返深度</th>
                  <th>Bayes 区间</th>
                </tr>
              </thead>
              <tbody>
                {structures.length
                  ? structures.map((item) => {
                    const selected = item.structure_id === selectedStructureId;
                    return (
                      <tr
                        key={item.structure_id}
                        className={selected ? "selected-row" : "clickable-row"}
                        onClick={() => setSelectedStructureId(item.structure_id)}
                      >
                        <td className="mono-text">{formatShortId(item.structure_id)}</td>
                        <td>
                          <span className={getToneClassName(getDirectionTone(item.direction))}>
                            {getToneMarker(getDirectionTone(item.direction))} {displayValue(item.direction)}
                          </span>
                        </td>
                        <td>
                          <span className={getToneClassName(getStructureStatusTone(item.status))}>
                            {getToneMarker(getStructureStatusTone(item.status))} {displayValue(item.status)}
                          </span>
                        </td>
                        <td>{formatNumber(item.points.p0.price, 4)}</td>
                        <td>{formatNumber(item.points.p1.price, 4)}</td>
                        <td>{formatNumber(item.points.p2.price, 4)}</td>
                        <td>{formatNumber(item.points.p3.price, 4)}</td>
                        <td>{formatNumber(item.retrace_ratio)}</td>
                        <td>
                          {formatNumber(item.layer4.bayes_lower)} ~ {formatNumber(item.layer4.bayes_upper)}
                        </td>
                      </tr>
                    );
                  })
                  : (
                    <tr>
                      <td colSpan={9} className="empty-cell">当前筛选条件下没有结构数据</td>
                    </tr>
                  )}
              </tbody>
            </table>
          </div>
        </article>

        <article className="panel card">
          <h3>信号表</h3>
          <div className="table-shell compact-shell">
            <table className="data-table compact-table">
              <thead>
                <tr>
                  <th>信号</th>
                  <th>结构</th>
                  <th>触发价</th>
                  <th>止损价</th>
                  <th>回撤</th>
                  <th>Calmar</th>
                </tr>
              </thead>
              <tbody>
                {signals.length
                  ? signals.slice(0, 10).map((item) => (
                    <tr key={item.signal_id}>
                      <td>
                        <span className={getToneClassName(getSignalTone(item.signal_type))}>
                          {getToneMarker(getSignalTone(item.signal_type))} {displayValue(item.signal_type)}
                        </span>
                      </td>
                      <td className="mono-text">{formatShortId(item.structure_id)}</td>
                      <td>{formatNumber(item.trigger_price, 4)}</td>
                      <td>{formatNumber(item.risk.stop_loss_price, 4)}</td>
                      <td>{formatPercent(item.risk.drawdown_pct)}</td>
                      <td>{formatNumber(item.risk.calmar_snapshot, 2)}</td>
                    </tr>
                  ))
                  : (
                    <tr>
                      <td colSpan={6} className="empty-cell">当前筛选条件下没有信号数据</td>
                    </tr>
                  )}
              </tbody>
            </table>
          </div>
        </article>

        <article className="panel card">
          <h3>结构详情 / 审计</h3>
          {selectedStructure
            ? (
              <div className="stack-gap">
                <div className="system-meta">
                  <span>结构ID: <code className="mono-text">{selectedStructure.structure_id}</code></span>
                  <span>
                    方向 / 状态: <strong>
                      <span className={getToneClassName(getDirectionTone(selectedStructure.direction))}>
                        {getToneMarker(getDirectionTone(selectedStructure.direction))} {displayValue(selectedStructure.direction)}
                      </span>
                      {" / "}
                      <span className={getToneClassName(getStructureStatusTone(selectedStructure.status))}>
                        {getToneMarker(getStructureStatusTone(selectedStructure.status))} {displayValue(selectedStructure.status)}
                      </span>
                    </strong>
                  </span>
                  <span>后验均值: <strong>{formatNumber(selectedStructure.layer4.posterior_mean)}</strong></span>
                </div>
                <div className="stack-gap">
                  <div className="status-chip">
                    Bayes 分组: <strong>{displayValue(detail?.structure.layer4.bayes_group_level ?? selectedStructure.layer4.bayes_group_level)}</strong>
                  </div>
                  <div className="status-chip">
                    样本数: <strong>{displayValue(detail?.structure.layer4.observation_count ?? selectedStructure.layer4.observation_count)}</strong>
                  </div>
                  <div className="status-chip">
                    最新信号: <strong className={getToneClassName(getSignalTone(detail?.latest_signal?.signal_type))}>{getToneMarker(getSignalTone(detail?.latest_signal?.signal_type))} {displayValue(detail?.latest_signal?.signal_type)}</strong>
                  </div>
                  <div className="status-chip">
                    审计事件数: <strong>{displayValue(detail?.audit_summary.audit_event_count)}</strong>
                  </div>
                  <div className="status-chip">
                    最新事件: <strong>{displayValue(detail?.audit_summary.last_event_type)}</strong>
                  </div>
                </div>
                <ul className="event-feed structure-audit-feed">
                  {detail?.audit_items?.length
                    ? detail.audit_items.map((item) => (
                      <li key={item.audit_id} className="event-row">
                        <div className="event-meta">
                          <strong className={getToneClassName(getStructureStatusTone(item.snapshot?.status))}>{getToneMarker(getStructureStatusTone(item.snapshot?.status))} {displayValue(item.event_type)}</strong>
                          <span>{displayValue(item.event_ts)}</span>
                        </div>
                        <div className="event-meta mono-text">审计ID: {formatShortId(item.audit_id)}</div>
                        <div className="event-payload mono-text">
                          快照: status={displayValue(item.snapshot?.status)} direction={displayValue(item.snapshot?.direction)} retrace={displayValue(item.snapshot?.retrace_ratio)}
                        </div>
                      </li>
                    ))
                    : <li className="event-row">{detailError ? `详情读取失败：${detailError}` : "选中结构后显示最新审计事件"}</li>}
                </ul>
              </div>
            )
            : <div className="status-chip warning">{getToneMarker(getBooleanTone(false))} 请先从左侧结构表选择一条结构。</div>}
        </article>
      </div>
    </section>
  );
}