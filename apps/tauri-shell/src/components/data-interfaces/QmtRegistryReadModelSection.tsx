import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  fetchQmtAssets,
  fetchQmtConflicts,
  fetchQmtLayouts,
  fetchQmtProbes,
  fetchQmtRouteDecisions,
  fetchQmtSessions,
  type QmtAssetsPayload,
  type QmtConflictsPayload,
  type QmtLayoutsPayload,
  type QmtProbesPayload,
  type QmtRouteDecisionsPayload,
  type QmtSessionsPayload,
} from "../../lib/api";
import {
  BINDING_PROBE_ACTION_LABEL,
  BINDING_PROBE_ACTION_LOADING_LABEL,
  getBindingProbeEmptyStateNote,
} from "../../lib/qmtBindingDrafts";
import { getQmtConflictTone, getQmtSessionTone } from "../../lib/qmtRuntimeUiUtils";

export function QmtRegistryReadModelSection({
  probeRefreshNonce,
  onProbeRequested,
}: {
  probeRefreshNonce: number;
  onProbeRequested: () => void;
}) {
  const [layouts, setLayouts] = useState<QmtLayoutsPayload | null>(null);
  const [assets, setAssets] = useState<QmtAssetsPayload | null>(null);
  const [sessions, setSessions] = useState<QmtSessionsPayload | null>(null);
  const [conflicts, setConflicts] = useState<QmtConflictsPayload | null>(null);
  const [routes, setRoutes] = useState<QmtRouteDecisionsPayload | null>(null);
  const [probes, setProbes] = useState<QmtProbesPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [probeLoading, setProbeLoading] = useState(false);
  const [probeError, setProbeError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const load = useCallback(() => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setLoading(true);
    setError(null);
    Promise.allSettled([
      fetchQmtLayouts({ signal: ac.signal }),
      fetchQmtAssets({ signal: ac.signal }),
      fetchQmtSessions(ac.signal),
      fetchQmtConflicts({ signal: ac.signal }),
      fetchQmtRouteDecisions({ signal: ac.signal }),
    ])
      .then((results) => {
        if (ac.signal.aborted) {
          return;
        }
        const [layoutsResult, assetsResult, sessionsResult, conflictsResult, routesResult] = results;
        if (layoutsResult.status === "fulfilled") setLayouts(layoutsResult.value);
        if (assetsResult.status === "fulfilled") setAssets(assetsResult.value);
        if (sessionsResult.status === "fulfilled") setSessions(sessionsResult.value);
        if (conflictsResult.status === "fulfilled") setConflicts(conflictsResult.value);
        if (routesResult.status === "fulfilled") setRoutes(routesResult.value);

        const firstRejected = results.find((item) => item.status === "rejected");
        if (firstRejected?.status === "rejected") {
          setError(firstRejected.reason instanceof Error ? firstRejected.reason.message : "QMT 只读视图加载失败");
        }
      })
      .finally(() => {
        if (!ac.signal.aborted) {
          setLoading(false);
        }
      });
  }, []);

  useEffect(() => {
    load();
    return () => abortRef.current?.abort();
  }, [load]);

  const handleProbe = useCallback(() => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setProbeLoading(true);
    setProbeError(null);
    fetchQmtProbes({ signal: ac.signal })
      .then((payload) => {
        if (!ac.signal.aborted) {
          setProbes(payload);
        }
      })
      .catch((reason: unknown) => {
        if (!ac.signal.aborted) {
          setProbeError(reason instanceof Error ? reason.message : "QMT probe 加载失败");
        }
      })
      .finally(() => {
        if (!ac.signal.aborted) {
          setProbeLoading(false);
        }
      });
  }, []);

  useEffect(() => {
    if (probeRefreshNonce <= 0) {
      return;
    }
    handleProbe();
  }, [handleProbe, probeRefreshNonce]);

  const sessionSummary = useMemo(() => {
    const items = sessions?.items ?? [];
    return {
      total: items.length,
      healthy: items.filter((item) => item.status === "healthy").length,
      active: items.filter((item) => item.connected || item.authenticated).length,
      degraded: items.filter((item) => item.status === "degraded" || item.status === "quarantined").length,
    };
  }, [sessions]);

  const topConflicts = (conflicts?.items ?? []).slice(0, 4);
  const topRoutes = (routes?.items ?? []).slice(0, 4);

  return (
    <section className="ifr-interface-section">
      <div className="ifr-section-header">
        <div>
          <div className="wbd-data-section-title">QMT 编排观测</div>
          <div className="ifr-section-note">{`直接读取 qmt_registry v0.1 的 layouts / assets / sessions / conflicts / route-decisions；点击任一"${BINDING_PROBE_ACTION_LABEL}"都会同时刷新原始 Probe 结果与账户绑定 explain。`}</div>
        </div>
        <div className="ifr-toolbar-actions">
          <button type="button" className="ifr-action-btn" disabled={loading} onClick={load}>
            {loading ? "刷新中…" : "刷新观测"}
          </button>
          <button type="button" className="ifr-action-btn" disabled={probeLoading} onClick={onProbeRequested}>
            {probeLoading ? BINDING_PROBE_ACTION_LOADING_LABEL : BINDING_PROBE_ACTION_LABEL}
          </button>
        </div>
      </div>

      {error ? <div className="status-chip danger">{error}</div> : null}
      {probeError ? <div className="status-chip warning">{probeError}</div> : null}

      <div className="ifr-summary-grid">
        <span className={`status-chip ${loading ? "warning" : (layouts?.total ?? 0) > 0 ? "ok" : "warning"}`}>Layouts {layouts?.total ?? "—"}</span>
        <span className={`status-chip ${loading ? "warning" : (assets?.total ?? 0) > 0 ? "ok" : "warning"}`}>Assets {assets?.total ?? "—"}</span>
        <span className={`status-chip ${sessionSummary.healthy > 0 ? "ok" : sessionSummary.total > 0 ? "warning" : ""}`}>Healthy Sessions {sessionSummary.healthy}</span>
        <span className={`status-chip ${sessionSummary.degraded > 0 ? "danger" : sessionSummary.active > 0 ? "ok" : "warning"}`}>活动会话 {sessionSummary.active}</span>
        <span className={`status-chip ${(conflicts?.total ?? 0) > 0 ? "danger" : "ok"}`}>Conflicts {conflicts?.total ?? 0}</span>
        <span className={`status-chip ${(routes?.total ?? 0) > 0 ? "ok" : "warning"}`}>Route Snapshots {routes?.total ?? 0}</span>
        <span className={`status-chip ${probes ? ((probes.total ?? 0) > 0 ? "ok" : "warning") : ""}`}>Probes {probes?.total ?? "未触发"}</span>
      </div>

      <div className="ifr-qmt-grid">
        <article className="ifr-qmt-card">
          <div className="ifr-qmt-card-title">会话快照</div>
          {sessions?.items?.length ? (
            <div className="ifr-qmt-chip-list">
              {sessions.items.slice(0, 6).map((item) => (
                <div key={item.session_id} className="ifr-qmt-chip-row">
                  <span className={`status-chip ${getQmtSessionTone(item.status)}`}>{item.status}</span>
                  <span className="mono-text ifr-qmt-chip-text">{item.userdata_path || item.session_anchor_key}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="wbd-data-card-empty">当前还没有 session 只读快照。</div>
          )}
        </article>

        <article className="ifr-qmt-card">
          <div className="ifr-qmt-card-title">冲突摘要</div>
          {topConflicts.length ? (
            <div className="ifr-qmt-chip-list">
              {topConflicts.map((item) => (
                <div key={`${item.code}-${item.target_id}`} className="ifr-qmt-chip-row">
                  <span className={`status-chip ${getQmtConflictTone(item.severity)}`}>{item.code}</span>
                  <span className="ifr-qmt-chip-text">{item.message}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="wbd-data-card-empty">当前未发现静态冲突。</div>
          )}
        </article>

        <article className="ifr-qmt-card">
          <div className="ifr-qmt-card-title">默认路由</div>
          {topRoutes.length ? (
            <div className="ifr-qmt-chip-list">
              {topRoutes.map((item) => (
                <div key={item.snapshot_id} className="ifr-qmt-chip-row">
                  <span className="status-chip ok">{item.purpose}</span>
                  <span className="ifr-qmt-chip-text">winner: <span className="mono-text">{item.winner || "—"}</span></span>
                </div>
              ))}
            </div>
          ) : (
            <div className="wbd-data-card-empty">当前还没有 route decision 快照。</div>
          )}
        </article>

        <article className="ifr-qmt-card">
          <div className="ifr-qmt-card-title">Probe 结果</div>
          {probes?.items?.length ? (
            <div className="ifr-qmt-chip-list">
              {probes.items.slice(0, 6).map((item) => (
                <div key={item.probe_id} className="ifr-qmt-chip-row">
                  <span className={`status-chip ${item.probe_success ? "ok" : "warning"}`}>{item.account_id || item.account_type || "probe"}</span>
                  <span className="ifr-qmt-chip-text">{item.userdata_path}</span>
                </div>
              ))}
              {probes.errors?.length ? probes.errors.slice(0, 3).map((message, index) => (
                <div key={`probe-error-${index}`} className="status-chip warning">{message}</div>
              )) : null}
            </div>
          ) : (
            <div className="wbd-data-card-empty">{getBindingProbeEmptyStateNote("qmt_registry")}</div>
          )}
        </article>
      </div>
    </section>
  );
}
