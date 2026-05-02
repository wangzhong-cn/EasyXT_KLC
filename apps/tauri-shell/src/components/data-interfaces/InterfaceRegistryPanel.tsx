import { useCallback, useEffect, useRef, useState } from "react";
import {
  fetchDataSourceConfig,
  fetchBrokerAccounts,
  fetchInterfaceRegistry,
  patchSourcePriority,
  testDataSource,
  triggerDatasourceRescan,
  type BrokerAccountPayload,
  type DataSourceConfigPayload,
  type DataSourceKey,
  type DataSourceTestPayload,
  type InterfaceRegistryPayload,
} from "../../lib/api";
import { FrontendEntryControlPanel } from "../../modules/frontend-entry/FrontendEntryControlPanel";
import { AccountBindingsSection } from "./AccountBindingsSection";
import {
  type DiscoverySnapshot,
  type DiscoveryRescanSummary,
  summarizeDiscoverySnapshot,
  DiscoveryRescanSummaryCard,
  SummaryBar,
  DiscoveryOverview,
  InterfaceOverview,
  InterfaceOverviewFallback,
} from "./InterfaceCatalog";
import { QmtRegistryReadModelSection } from "./QmtRegistryReadModelSection";
import { QmtSetupWizard } from "./QmtSetupWizard";


export function InterfaceRegistryPanel() {
  const [registry, setRegistry] = useState<InterfaceRegistryPayload | null>(null);
  const [dsConfig, setDsConfig] = useState<DataSourceConfigPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);
  const [brokerAccounts, setBrokerAccounts] = useState<BrokerAccountPayload[]>([]);
  const [accountsError, setAccountsError] = useState<string | null>(null);
  const refreshAccounts = useCallback(async () => {
    setAccountsError(null);
    try {
      const payload = await fetchBrokerAccounts();
      setBrokerAccounts(payload);
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : "券商账户读取失败";
      setAccountsError(message);
      throw new Error(message);
    }
  }, []);
  const [testResults, setTestResults] = useState<Record<string, DataSourceTestPayload>>({});
  const [testLoading, setTestLoading] = useState<Record<string, boolean>>({});
  const [priorityBusy, setPriorityBusy] = useState(false);
  const [rescanSummary, setRescanSummary] = useState<DiscoveryRescanSummary | null>(null);
  const [probeRefreshNonce, setProbeRefreshNonce] = useState(0);
  const [showWizard, setShowWizard] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const discoverySnapshotRef = useRef<DiscoverySnapshot | null>(null);

  const requestUnifiedProbeRefresh = useCallback(() => {
    setProbeRefreshNonce((value) => value + 1);
  }, []);

  const load = () => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setLoading(true);
    setError(null);
    setConfigError(null);
    setAccountsError(null);
    Promise.allSettled([
      fetchInterfaceRegistry(ac.signal),
      fetchDataSourceConfig(ac.signal),
      fetchBrokerAccounts(ac.signal),
    ])
      .then((results) => {
        if (ac.signal.aborted) {
          return;
        }
        const [registryResult, configResult, accountsResult] = results;
        if (registryResult.status === "fulfilled") {
          setRegistry(registryResult.value);
        } else {
          setError(registryResult.reason instanceof Error ? registryResult.reason.message : "接口注册表加载失败");
        }
        if (configResult.status === "fulfilled") {
          setDsConfig(configResult.value);
          const nextSnapshot = summarizeDiscoverySnapshot(configResult.value);
          const previousSnapshot = discoverySnapshotRef.current;
          if (previousSnapshot == null) {
            setRescanSummary({
              kind: "baseline",
              capturedAt: nextSnapshot.capturedAt,
              qmtCandidateDelta: nextSnapshot.qmtCandidateCount,
              tdxCandidateDelta: nextSnapshot.tdxCandidateCount,
              qmtErrorRootDelta: nextSnapshot.qmtErrorRoots,
              tdxErrorRootDelta: nextSnapshot.tdxErrorRoots,
              qmtDurationMs: nextSnapshot.qmtDurationMs,
              tdxDurationMs: nextSnapshot.tdxDurationMs,
            });
          } else {
            setRescanSummary({
              kind: "delta",
              capturedAt: nextSnapshot.capturedAt,
              qmtCandidateDelta: nextSnapshot.qmtCandidateCount - previousSnapshot.qmtCandidateCount,
              tdxCandidateDelta: nextSnapshot.tdxCandidateCount - previousSnapshot.tdxCandidateCount,
              qmtErrorRootDelta: nextSnapshot.qmtErrorRoots - previousSnapshot.qmtErrorRoots,
              tdxErrorRootDelta: nextSnapshot.tdxErrorRoots - previousSnapshot.tdxErrorRoots,
              qmtDurationMs: nextSnapshot.qmtDurationMs,
              tdxDurationMs: nextSnapshot.tdxDurationMs,
            });
          }
          discoverySnapshotRef.current = nextSnapshot;
        } else {
          setConfigError(configResult.reason instanceof Error ? configResult.reason.message : "本地发现读取失败");
        }
        if (accountsResult.status === "fulfilled") {
          setBrokerAccounts(accountsResult.value);
        } else {
          setAccountsError(accountsResult.reason instanceof Error ? accountsResult.reason.message : "账户绑定读取失败");
        }
      })
      .finally(() => {
        if (!ac.signal.aborted) {
          setLoading(false);
        }
      });
  };

  // rescan: POST /datasource/rescan (force) → update dsConfig → refresh registry from fresh cache
  const rescan = () => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setLoading(true);
    setError(null);
    setConfigError(null);
    setAccountsError(null);
    triggerDatasourceRescan(ac.signal)
      .then((fresh) => {
        if (ac.signal.aborted) return Promise.resolve();
        setDsConfig(fresh);
        const nextSnapshot = summarizeDiscoverySnapshot(fresh);
        const prev = discoverySnapshotRef.current;
        setRescanSummary(
          prev == null
            ? {
                kind: "baseline",
                capturedAt: nextSnapshot.capturedAt,
                qmtCandidateDelta: nextSnapshot.qmtCandidateCount,
                tdxCandidateDelta: nextSnapshot.tdxCandidateCount,
                qmtErrorRootDelta: nextSnapshot.qmtErrorRoots,
                tdxErrorRootDelta: nextSnapshot.tdxErrorRoots,
                qmtDurationMs: nextSnapshot.qmtDurationMs,
                tdxDurationMs: nextSnapshot.tdxDurationMs,
              }
            : {
                kind: "delta",
                capturedAt: nextSnapshot.capturedAt,
                qmtCandidateDelta: nextSnapshot.qmtCandidateCount - prev.qmtCandidateCount,
                tdxCandidateDelta: nextSnapshot.tdxCandidateCount - prev.tdxCandidateCount,
                qmtErrorRootDelta: nextSnapshot.qmtErrorRoots - prev.qmtErrorRoots,
                tdxErrorRootDelta: nextSnapshot.tdxErrorRoots - prev.tdxErrorRoots,
                qmtDurationMs: nextSnapshot.qmtDurationMs,
                tdxDurationMs: nextSnapshot.tdxDurationMs,
              },
        );
        discoverySnapshotRef.current = nextSnapshot;
        // Registry and accounts can now be fetched in parallel; registry hits the fresh server cache.
        return Promise.allSettled([
          fetchInterfaceRegistry(ac.signal),
          fetchBrokerAccounts(ac.signal),
        ]).then(([regResult, acctResult]) => {
          if (ac.signal.aborted) return;
          if (regResult.status === "fulfilled") {
            setRegistry(regResult.value);
          } else {
            setError(regResult.reason instanceof Error ? regResult.reason.message : "接口注册表加载失败");
          }
          if (acctResult.status === "fulfilled") {
            setBrokerAccounts(acctResult.value);
          } else {
            setAccountsError(acctResult.reason instanceof Error ? acctResult.reason.message : "账户绑定读取失败");
          }
        });
      })
      .catch((reason: unknown) => {
        if (!ac.signal.aborted) {
          setConfigError(reason instanceof Error ? reason.message : "重扫失败");
        }
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false);
      });
  };

  const handleReorder = (sourceId: string, dir: "up" | "down") => {
    if (priorityBusy || !dsConfig?.sources) return;
    const sorted = [...dsConfig.sources].sort(
      (a, b) => (a.priority ?? 999) - (b.priority ?? 999),
    );
    const idx = sorted.findIndex((s) => s.source === sourceId);
    if (idx < 0) return;
    const neighborIdx = dir === "up" ? idx - 1 : idx + 1;
    if (neighborIdx < 0 || neighborIdx >= sorted.length) return;
    const curr = sorted[idx];
    const neighbor = sorted[neighborIdx];
    const currPri = curr.priority ?? 999;
    const neighborPri = neighbor.priority ?? 999;
    if (currPri === neighborPri) return;
    setPriorityBusy(true);
    patchSourcePriority({ [sourceId]: neighborPri, [neighbor.source]: currPri })
      .then((resp) => {
        setDsConfig((prev) => prev ? { ...prev, sources: resp.sources } : prev);
      })
      .catch(() => {/* silent — user can rescan to recover */})
      .finally(() => setPriorityBusy(false));
  };

  const handleTestSource = (source: string) => {
    setTestLoading((prev) => ({ ...prev, [source]: true }));
    void testDataSource(source as DataSourceKey)
      .then((payload) => {
        setTestResults((prev) => ({ ...prev, [source]: payload }));
      })
      .catch((reason: unknown) => {
        const message = reason instanceof Error ? reason.message : "请求失败";
        setTestResults((prev) => ({
          ...prev,
          [source]: {
            source,
            status: "error",
            message,
            latency_ms: null,
            server_time: Date.now(),
          },
        }));
      })
      .finally(() => {
        setTestLoading((prev) => ({ ...prev, [source]: false }));
      });
  };

  useEffect(() => {
    load();
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  const summary = registry?.summary ?? {
    total: 0,
    configured: 0,
    unconfigured: 0,
    planned: 0,
  };

  const qmtCandidateCount = dsConfig?.discovery?.qmt_local_dat?.candidate_count ?? 0;
  const tdxCandidateCount = dsConfig?.discovery?.pytdx_local_tdx?.candidate_count ?? 0;

  return (
    <div className="ifr-root">
      <div className="ifr-toolbar">
        <div>
          <div className="wbd-data-section-title">接口管理</div>
          <div className="ifr-section-note">工作台侧主入口：统一查看接口注册表、本地发现结果与数据源连通状态。</div>
        </div>
        <div className="ifr-toolbar-actions">
          <button
            type="button"
            className="ifr-action-btn"
            disabled={loading}
            onClick={rescan}
          >
            {loading ? "重扫中…" : "重新扫描"}
          </button>
        </div>
      </div>

      {error ? <div className="status-chip danger">注册表读取失败：{error}</div> : null}

      <SummaryBar summary={summary} />
      <QmtRegistryReadModelSection
        probeRefreshNonce={probeRefreshNonce}
        onProbeRequested={requestUnifiedProbeRefresh}
      />
      <div className="ifr-summary-grid">
        <span className={`status-chip ${qmtCandidateCount > 0 ? "ok" : "warning"}`}>
          QMT 本地候选 {qmtCandidateCount}
        </span>
        {qmtCandidateCount === 0 && !showWizard && (
          <button
            type="button"
            onClick={() => setShowWizard(true)}
            style={{
              background: "#1e3a5f",
              border: "1px solid #2563eb",
              borderRadius: 4,
              color: "#93c5fd",
              cursor: "pointer",
              fontSize: 11,
              padding: "2px 10px",
            }}
          >
            快速配置 QMT
          </button>
        )}
        <span className={`status-chip ${tdxCandidateCount > 0 ? "ok" : "warning"}`}>
          TDX 本地候选 {tdxCandidateCount}
        </span>
        <span className={`status-chip ${registry ? "ok" : loading ? "warning" : "danger"}`}>
          接口注册表 {registry ? "已加载" : loading ? "加载中" : "不可达"}
        </span>
        <span className="status-chip">本地发现 {configError ? "降级" : "已接线"}</span>
        <span className="status-chip">连通测试 {Object.keys(testResults).length}</span>
        {dsConfig?.last_scan_at ? (
          <span className={`status-chip ${dsConfig.cache_status === "fresh" ? "ok" : dsConfig.cache_status === "stale" ? "warning" : ""}`}>
            {dsConfig.cache_status === "fresh"
              ? `缓存 ${Math.round((dsConfig.cache_age_ms ?? 0) / 1000)}s`
              : dsConfig.cache_status === "stale"
                ? `陈旧缓存 ${Math.round((dsConfig.cache_age_ms ?? 0) / 1000)}s`
                : `扫描完成 ${new Date(dsConfig.last_scan_at).toLocaleTimeString("zh-CN", { hour12: false })}`}
          </span>
        ) : null}
      </div>

      {rescanSummary ? <DiscoveryRescanSummaryCard summary={rescanSummary} /> : null}

      {showWizard && (
        <div style={{ margin: "12px 0" }}>
          <QmtSetupWizard
            accounts={brokerAccounts}
            onDone={() => { setShowWizard(false); load(); }}
            onCancel={() => setShowWizard(false)}
          />
        </div>
      )}

      <FrontendEntryControlPanel variant="compact" />

      <DiscoveryOverview dsConfig={dsConfig} configError={configError} />
      <AccountBindingsSection
        accounts={brokerAccounts}
        accountsError={accountsError}
        dsConfig={dsConfig}
        onApplied={refreshAccounts}
        onConfigureRequested={() => setShowWizard(true)}
        probeRefreshNonce={probeRefreshNonce}
        onProbeRequested={requestUnifiedProbeRefresh}
      />
      {registry ? (
        <InterfaceOverview
          registry={registry}
          dsConfig={dsConfig}
          testLoading={testLoading}
          testResults={testResults}
          onTest={handleTestSource}
          onReorder={handleReorder}
          reorderBusy={priorityBusy}
          onCredentialSaved={load}
        />
      ) : (
        <InterfaceOverviewFallback loading={loading} error={error} />
      )}

      <div className="ifr-footer">
        <button
          type="button"
          className="ifr-action-btn"
          disabled={loading}
          onClick={rescan}
        >
          {loading ? "重扫中…" : "重新扫描"}
        </button>
      </div>
    </div>
  );
}
