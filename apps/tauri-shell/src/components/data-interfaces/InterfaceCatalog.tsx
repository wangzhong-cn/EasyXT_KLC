import { useMemo, useState } from "react";
import {
  patchDatasourceCredentials,
  deleteDatasourceCredentials,
  type DataSourceConfigPayload,
  type DataSourceDiscoveryCandidatePayload,
  type DataSourceDiscoveryPayload,
  type DataSourceDiscoveryRootResultPayload,
  type DataSourceDiscoveryScanMetricsPayload,
  type DataSourceDiscoveryScanPolicyPayload,
  type DataSourceTestPayload,
  type DatasourceCredentialsMasked,
  type InterfaceClass,
  type InterfaceProfilePayload,
  type InterfaceRegistryPayload,
  type InterfaceStatusIndicator,
} from "../../lib/api";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CLASS_LABELS: Record<InterfaceClass | string, string> = {
  market_data: "市场数据",
  trade_gateway: "交易网关",
  account_broker: "账户通道",
  storage_backend: "存储后端",
  control_ops: "控制与运维",
};

const ANCHOR_KIND_LABELS: Record<string, string> = {
  fact_anchor: "事实锚",
  realtime_anchor: "实时锚",
  storage: "存储",
  independent_evidence: "证据锚",
};

const PROTOCOL_LABELS: Record<string, string> = {
  local_file: "本地文件",
  local_sdk: "本地 SDK",
  rest_api: "REST API",
  ctp: "CTP 标准接口",
  xtp: "XTP 极速接口",
  mmap_shm: "共享内存 mmap",
  sql_db: "SQL 数据库",
  sql_tsdb: "时序 SQL 库",
  websocket: "WebSocket",
};

const STATUS_META: Record<
  InterfaceStatusIndicator,
  { label: string; tone: "ok" | "neutral" | "planned" }
> = {
  configured: { label: "已配置", tone: "ok" },
  unconfigured: { label: "未配置", tone: "neutral" },
  planned: { label: "规划中", tone: "planned" },
};

const TEST_STATUS_META: Record<
  DataSourceTestPayload["status"],
  { label: string; tone: "ok" | "warning" | "danger" }
> = {
  ok: { label: "可连通", tone: "ok" },
  unconfigured: { label: "未配置", tone: "warning" },
  error: { label: "异常", tone: "danger" },
};

const DISCOVERY_META: Record<
  string,
  { title: string; subtitle: string; emptyLabel: string }
> = {
  qmt_local_dat: {
    title: "QMT 本地 DAT",
    subtitle: "安装目录 / userdata_mini / datadir / 历史覆盖",
    emptyLabel: "当前未发现带历史数据的 QMT 本地候选目录",
  },
  pytdx_local_tdx: {
    title: "本地 TDX / pytdx",
    subtitle: "本地缓存目录 / 市场目录 / 最近更新时间",
    emptyLabel: "当前未发现本地 TDX 缓存候选目录",
  },
};

// Class display order
const CLASS_ORDER: (InterfaceClass | string)[] = [
  "market_data",
  "trade_gateway",
  "account_broker",
  "storage_backend",
  "control_ops",
];

const SCAN_ERROR_KIND_LABELS: Record<string, string> = {
  permission_denied: "权限受限",
  not_found: "路径缺失",
  resource_busy: "资源占用",
  path_too_long: "路径过长",
  invalid_path: "路径非法",
  os_error: "系统错误",
};

const SCAN_POLICY_SOURCE_LABELS: Record<string, string> = {
  env_override: "EASYXT_SOURCE_DISCOVERY_ROOTS 环境覆盖",
  all_mounted_drives: "自动枚举已挂载逻辑盘",
  fallback_roots: "回退默认根目录",
};

export interface DiscoverySnapshot {
  capturedAt: number;
  qmtCandidateCount: number;
  tdxCandidateCount: number;
  qmtErrorRoots: number;
  tdxErrorRoots: number;
  qmtDurationMs: number | null;
  tdxDurationMs: number | null;
}
export interface DiscoveryRescanSummary {
  kind: "baseline" | "delta";
  capturedAt: number;
  qmtCandidateDelta: number;
  tdxCandidateDelta: number;
  qmtErrorRootDelta: number;
  tdxErrorRootDelta: number;
  qmtDurationMs: number | null;
  tdxDurationMs: number | null;
}
function getSourceCategoryLabel(category: string | undefined): string {
  switch (category) {
    case "local_anchor":
      return "本地锚点";
    case "broker_runtime":
      return "券商在线";
    case "local_store":
      return "本地存储";
    case "third_party":
      return "第三方证据";
    default:
      return "其他";
  }
}
function formatShortList(values: string[] | undefined, max = 6): string {
  if (!values || values.length === 0) {
    return "—";
  }
  const head = values.slice(0, max);
  return values.length > max ? `${head.join(", ")} +${values.length - max}` : head.join(", ");
}
function getDiscoverySummaryTone(candidateCount: number | undefined): "ok" | "warning" {
  return (candidateCount ?? 0) > 0 ? "ok" : "warning";
}
function getScanPolicyLabel(policy: DataSourceDiscoveryScanPolicyPayload | null | undefined): string {
  switch (policy?.mode) {
    case "all_mounted_drives":
      return "全盘逻辑盘扫描";
    case "env_override":
      return "环境变量覆盖";
    case "fallback_roots":
      return "回退根目录";
    default:
      return "默认扫描策略";
  }
}

function getScanRootTone(result: DataSourceDiscoveryRootResultPayload): "ok" | "warning" | "danger" {
  if ((result.error_count ?? 0) > 0) {
    return (result.match_events ?? 0) > 0 ? "warning" : "danger";
  }
  return (result.match_events ?? 0) > 0 ? "ok" : "warning";
}

function formatScanDuration(metrics: DataSourceDiscoveryScanMetricsPayload | null | undefined): string {
  if (metrics?.duration_ms == null) {
    return "—";
  }
  if (metrics.duration_ms >= 1000) {
    return `${(metrics.duration_ms / 1000).toFixed(2)}s`;
  }
  return `${metrics.duration_ms} ms`;
}

function formatDurationMs(durationMs: number | null | undefined): string {
  if (durationMs == null) {
    return "—";
  }
  if (durationMs >= 1000) {
    return `${(durationMs / 1000).toFixed(2)}s`;
  }
  return `${durationMs} ms`;
}

function formatDelta(value: number): string {
  if (value > 0) {
    return `+${value}`;
  }
  return `${value}`;
}

function getCandidateDeltaTone(value: number): "ok" | "warning" {
  return value >= 0 ? "ok" : "warning";
}

function getErrorDeltaTone(value: number): "ok" | "warning" | "danger" {
  if (value < 0) {
    return "ok";
  }
  if (value > 0) {
    return "danger";
  }
  return "warning";
}

function formatCapturedAt(ts: number | null | undefined): string {
  if (!ts) {
    return "—";
  }
  return new Date(ts).toLocaleString("zh-CN", {
    hour12: false,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function getScanPolicySourceLabel(policy: DataSourceDiscoveryScanPolicyPayload | null | undefined): string {
  return SCAN_POLICY_SOURCE_LABELS[policy?.mode ?? ""] ?? "默认扫描来源";
}

function getScanPolicySourceNote(policy: DataSourceDiscoveryScanPolicyPayload | null | undefined): string {
  if (policy?.env_override) {
    return "当前扫描根由 EASYXT_SOURCE_DISCOVERY_ROOTS 显式覆盖，优先使用配置根目录。";
  }
  if (policy?.mode === "all_mounted_drives") {
    return "当前会自动枚举所有已挂载逻辑盘，并按最大深度截断扫描范围。";
  }
  return "当前使用回退根目录策略，请检查环境变量与本机盘符可见性。";
}

function summarizeRootErrorKinds(
  rootResults: DataSourceDiscoveryRootResultPayload[],
): Array<[string, number]> {
  const totals = new Map<string, number>();
  for (const result of rootResults) {
    for (const [kind, count] of Object.entries(result.error_summary ?? {})) {
      totals.set(kind, (totals.get(kind) ?? 0) + count);
    }
  }
  return Array.from(totals.entries()).sort((left, right) => right[1] - left[1]);
}

export function summarizeDiscoverySnapshot(config: DataSourceConfigPayload): DiscoverySnapshot {
  const qmtDiscovery = config.discovery?.qmt_local_dat;
  const tdxDiscovery = config.discovery?.pytdx_local_tdx;
  return {
    capturedAt: Date.now(),
    qmtCandidateCount: qmtDiscovery?.candidate_count ?? 0,
    tdxCandidateCount: tdxDiscovery?.candidate_count ?? 0,
    qmtErrorRoots: qmtDiscovery?.scan_metrics?.roots_with_errors ?? 0,
    tdxErrorRoots: tdxDiscovery?.scan_metrics?.roots_with_errors ?? 0,
    qmtDurationMs: qmtDiscovery?.scan_metrics?.duration_ms ?? null,
    tdxDurationMs: tdxDiscovery?.scan_metrics?.duration_ms ?? null,
  };
}

export function DiscoveryRescanSummaryCard({ summary }: { summary: DiscoveryRescanSummary }) {
  const qmtCandidateLabel = summary.kind === "baseline"
    ? `QMT 当前候选 ${summary.qmtCandidateDelta}`
    : `QMT 候选变化 ${formatDelta(summary.qmtCandidateDelta)}`;
  const tdxCandidateLabel = summary.kind === "baseline"
    ? `TDX 当前候选 ${summary.tdxCandidateDelta}`
    : `TDX 候选变化 ${formatDelta(summary.tdxCandidateDelta)}`;
  const qmtErrorLabel = summary.kind === "baseline"
    ? `QMT 错误根 ${summary.qmtErrorRootDelta}`
    : `QMT 错误根变化 ${formatDelta(summary.qmtErrorRootDelta)}`;
  const tdxErrorLabel = summary.kind === "baseline"
    ? `TDX 错误根 ${summary.tdxErrorRootDelta}`
    : `TDX 错误根变化 ${formatDelta(summary.tdxErrorRootDelta)}`;

  return (
    <section className="ifr-rescan-summary">
      <div className="ifr-section-header">
        <div>
          <div className="wbd-data-section-title">最近重扫</div>
          <div className="ifr-section-note">
            {summary.kind === "baseline" ? "已建立当前扫描基线，后续重扫会显示差异。" : "对比上一次扫描结果的候选变化与错误根变化。"}
          </div>
        </div>
        <span className="status-chip">{formatCapturedAt(summary.capturedAt)}</span>
      </div>
      <div className="ifr-summary-grid">
        <span className={`status-chip ${summary.kind === "baseline" ? "ok" : getCandidateDeltaTone(summary.qmtCandidateDelta)}`}>
          {qmtCandidateLabel}
        </span>
        <span className={`status-chip ${summary.kind === "baseline" ? "ok" : getCandidateDeltaTone(summary.tdxCandidateDelta)}`}>
          {tdxCandidateLabel}
        </span>
        <span className={`status-chip ${summary.kind === "baseline" ? "warning" : getErrorDeltaTone(summary.qmtErrorRootDelta)}`}>
          {qmtErrorLabel}
        </span>
        <span className={`status-chip ${summary.kind === "baseline" ? "warning" : getErrorDeltaTone(summary.tdxErrorRootDelta)}`}>
          {tdxErrorLabel}
        </span>
        <span className="status-chip">QMT 耗时 {formatDurationMs(summary.qmtDurationMs)}</span>
        <span className="status-chip">TDX 耗时 {formatDurationMs(summary.tdxDurationMs)}</span>
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function StatusChip({ status }: { status: InterfaceStatusIndicator }) {
  const meta = STATUS_META[status] ?? { label: status, tone: "neutral" };
  return (
    <span
      className={`status-chip ifr-status-chip${
        meta.tone === "planned" ? " ifr-status-chip--planned" : meta.tone === "ok" ? " ok" : ""
      }`}
    >
      {meta.label}
    </span>
  );
}

function TestChip({ result }: { result: DataSourceTestPayload }) {
  const meta = TEST_STATUS_META[result.status] ?? { label: result.status, tone: "warning" };
  const latencyLabel = result.latency_ms != null ? `${result.latency_ms.toFixed(0)} ms` : meta.label;
  return (
    <span className={`status-chip ${meta.tone === "warning" ? "warning" : meta.tone}`} title={result.message}>
      {meta.tone === "ok" ? "✓" : meta.tone === "danger" ? "✗" : "…"} {latencyLabel}
    </span>
  );
}

function CapabilityBadges({ caps }: { caps: string[] }) {
  return (
    <div className="ifr-cap-grid">
      {caps.map((c) => (
        <span key={c} className="ifr-cap-badge">
          {c}
        </span>
      ))}
    </div>
  );
}

function DiscoveryCandidateCard({
  candidate,
}: {
  candidate: DataSourceDiscoveryCandidatePayload;
}) {
  return (
    <div className="ifr-candidate-card">
      <div className="ifr-candidate-header">
        <span className="mono-text">{candidate.install_path ?? candidate.cache_root ?? candidate.userdata_path ?? "—"}</span>
        <span className="status-chip">score {candidate.score ?? "—"}</span>
      </div>
      <div className="ifr-meta-list">
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">用户目录</span>
          <span className="mono-text">{candidate.userdata_path ?? candidate.cache_root ?? "—"}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">datadir / cache</span>
          <span className="mono-text">{candidate.datadir_path ?? candidate.cache_root ?? "—"}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">市场</span>
          <span>{formatShortList(candidate.market_dirs, 4)}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">周期</span>
          <span>{formatShortList(candidate.period_dirs, 4)}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">样本</span>
          <span>
            {candidate.has_downloaded_history
              ? `DAT ${candidate.dat_file_count_hint ?? 0} 个`
              : candidate.has_local_cache
                ? "本地缓存存在"
                : "未见样本"}
          </span>
        </div>
        {candidate.history_sample_files?.length ? (
          <div className="ifr-meta-row">
            <span className="ifr-meta-label">样本文件</span>
            <span className="mono-text">{formatShortList(candidate.history_sample_files, 3)}</span>
          </div>
        ) : null}
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">最近更新</span>
          <span>{candidate.latest_modified_at ?? "—"}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">来源</span>
          <span>{candidate.discovered_by ?? "—"}</span>
        </div>
      </div>
    </div>
  );
}

function DiscoveryCard({
  discoveryKey,
  payload,
}: {
  discoveryKey: string;
  payload: DataSourceDiscoveryPayload | null | undefined;
}) {
  const meta = DISCOVERY_META[discoveryKey] ?? {
    title: discoveryKey,
    subtitle: "本地发现结果",
    emptyLabel: "当前未发现候选路径",
  };
  const candidateCount = payload?.candidate_count ?? 0;
  const bestCandidate = payload?.candidates?.[0] ?? null;
  const scanPolicy = payload?.scan_policy;
  const scanMetrics = payload?.scan_metrics;
  const rootResults = payload?.root_results ?? [];
  const aggregatedErrorKinds = summarizeRootErrorKinds(rootResults);
  return (
    <article className="wbd-data-card ifr-discovery-card">
      <div className="ifr-card-header">
        <div className="ifr-card-title-block">
          <strong>{meta.title}</strong>
          <span className="ifr-card-subtitle">{meta.subtitle}</span>
        </div>
        <span className={`status-chip ${getDiscoverySummaryTone(candidateCount)}`}>
          候选 {candidateCount}
        </span>
      </div>
      {scanPolicy ? (
        <div className="ifr-policy-banner">
          <span className={`status-chip ${scanPolicy.env_override ? "warning" : "ok"}`}>
            {getScanPolicySourceLabel(scanPolicy)}
          </span>
          <span>{getScanPolicySourceNote(scanPolicy)}</span>
        </div>
      ) : null}
      <div className="ifr-meta-list">
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">策略</span>
          <span>{getScanPolicyLabel(scanPolicy)}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">来源</span>
          <span>{getScanPolicySourceLabel(scanPolicy)}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">深度</span>
          <span>{scanPolicy?.max_depth ?? "—"} 层</span>
        </div>
        {scanPolicy?.configured_roots?.length ? (
          <div className="ifr-meta-row">
            <span className="ifr-meta-label">配置根</span>
            <span className="mono-text">{formatShortList(scanPolicy.configured_roots, 4)}</span>
          </div>
        ) : null}
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">扫描根</span>
          <span className="mono-text">{formatShortList(payload?.search_roots)}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">覆盖根数</span>
          <span>{scanPolicy?.effective_roots_count ?? payload?.search_roots.length ?? 0}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">耗时</span>
          <span>{formatScanDuration(scanMetrics)}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">错误根</span>
          <span>{scanMetrics?.roots_with_errors ?? 0}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">最近完成</span>
          <span>{scanMetrics?.finished_at ?? scanMetrics?.started_at ?? "—"}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">最佳候选</span>
          <span>{bestCandidate ? (bestCandidate.install_path ?? bestCandidate.cache_root ?? bestCandidate.userdata_path ?? "—") : meta.emptyLabel}</span>
        </div>
      </div>
      {aggregatedErrorKinds.length > 0 ? (
        <div className="ifr-root-scan-kinds">
          {aggregatedErrorKinds.map(([kind, count]) => (
            <span key={`${discoveryKey}-agg-${kind}`} className="status-chip warning">
              {SCAN_ERROR_KIND_LABELS[kind] ?? kind} × {count}
            </span>
          ))}
        </div>
      ) : null}
      {bestCandidate ? <DiscoveryCandidateCard candidate={bestCandidate} /> : <div className="wbd-data-card-empty">{meta.emptyLabel}</div>}
      {payload?.candidates != null && payload.candidates.length > 1 ? (
        <details className="ifr-candidate-details">
          <summary>查看更多候选 ({payload.candidates.length - 1})</summary>
          <div className="ifr-candidate-list">
            {payload.candidates.slice(1).map((candidate, index) => (
              <DiscoveryCandidateCard
                key={`${discoveryKey}-${index}-${candidate.install_path ?? candidate.cache_root ?? candidate.userdata_path ?? index}`}
                candidate={candidate}
              />
            ))}
          </div>
        </details>
      ) : null}
      {rootResults.length > 0 ? (
        <details className="ifr-candidate-details">
          <summary>查看扫描根结果 ({rootResults.length})</summary>
          <div className="ifr-root-scan-list">
            {rootResults.map((result, index) => {
              const tone = getScanRootTone(result);
              return (
                <div key={`${result.root ?? "root"}-${index}`} className="ifr-root-scan-item">
                  <div className="ifr-root-scan-head">
                    <span className="mono-text">{result.root ?? "—"}</span>
                    <span className={`status-chip ${tone}`}>{result.status}</span>
                  </div>
                  <div className="ifr-root-scan-meta">
                    <span>命中 {result.match_events ?? 0}</span>
                    <span>错误 {result.error_count ?? 0}</span>
                  </div>
                  {result.error_summary && Object.keys(result.error_summary).length > 0 ? (
                    <div className="ifr-root-scan-kinds">
                      {Object.entries(result.error_summary).map(([kind, count]) => (
                        <span key={`${result.root ?? "root"}-kind-${kind}`} className="status-chip warning">
                          {SCAN_ERROR_KIND_LABELS[kind] ?? kind} × {count}
                        </span>
                      ))}
                    </div>
                  ) : null}
                  {result.errors?.length ? (
                    <div className="ifr-root-scan-errors">
                      {result.errors.map((message, errorIndex) => (
                        <div key={`${result.root ?? "root"}-err-${errorIndex}`} className="status-chip warning">
                          {message}
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </details>
      ) : null}
    </article>
  );
}

// ---------------------------------------------------------------------------
// 凭证编辑内嵌表单（tushare / tqsdk）
// ---------------------------------------------------------------------------

function CredentialEditSection({
  source,
  currentMasked,
  onSaved,
}: {
  source: string;
  currentMasked: string | null;
  onSaved: () => void;
}) {
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [token, setToken] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      const payload: { token?: string; username?: string; password?: string } = {};
      if (source === "tushare") {
        payload.token = token;
      } else if (source === "tqsdk") {
        if (username) payload.username = username;
        if (password) payload.password = password;
      }
      await patchDatasourceCredentials(source, payload);
      setEditing(false);
      setToken("");
      setUsername("");
      setPassword("");
      onSaved();
    } catch (err) {
      setError(err instanceof Error ? err.message : "保存失败");
    } finally {
      setSaving(false);
    }
  };

  const handleClear = async () => {
    setSaving(true);
    setError(null);
    try {
      await deleteDatasourceCredentials(source);
      setEditing(false);
      onSaved();
    } catch (err) {
      setError(err instanceof Error ? err.message : "清除失败");
    } finally {
      setSaving(false);
    }
  };

  if (!editing) {
    return (
      <div className="ifr-cred-row">
        <span className="ifr-meta-label">凭证</span>
        <span className="mono-text ifr-cred-masked">{currentMasked ?? "未配置"}</span>
        <button
          type="button"
          className="ifr-action-btn ifr-cred-edit-btn"
          onClick={() => setEditing(true)}
        >
          {currentMasked ? "修改" : "配置"}
        </button>
      </div>
    );
  }

  return (
    <div className="ifr-cred-form">
      {source === "tushare" && (
        <input
          className="ifr-cred-input"
          type="password"
          placeholder="Tushare Token"
          value={token}
          onChange={(e) => setToken(e.target.value)}
          autoComplete="new-password"
        />
      )}
      {source === "tqsdk" && (
        <>
          <input
            className="ifr-cred-input"
            type="text"
            placeholder="天勤 TQSdk 用户名"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="username"
          />
          <input
            className="ifr-cred-input"
            type="password"
            placeholder="天勤 TQSdk 密码"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="new-password"
          />
        </>
      )}
      <div className="ifr-cred-actions">
        <button type="button" className="ifr-action-btn" disabled={saving} onClick={handleSave}>
          {saving ? "保存中…" : "保存"}
        </button>
        {currentMasked ? (
          <button
            type="button"
            className="ifr-action-btn ifr-action-btn--danger"
            disabled={saving}
            onClick={handleClear}
          >
            清除
          </button>
        ) : null}
        <button
          type="button"
          className="ifr-action-btn"
          disabled={saving}
          onClick={() => {
            setEditing(false);
            setError(null);
          }}
        >
          取消
        </button>
      </div>
      {error ? <div className="ifr-cred-error">{error}</div> : null}
    </div>
  );
}

function InterfaceCard({
  iface,
  testLoading,
  testResult,
  onTest,
  onReorder,
  reorderBusy,
  isFirst,
  isLast,
  onCredentialSaved,
}: {
  iface: InterfaceProfilePayload;
  testLoading: boolean;
  testResult?: DataSourceTestPayload;
  onTest: (source: string) => void;
  onReorder?: (source: string, dir: "up" | "down") => void;
  reorderBusy?: boolean;
  isFirst?: boolean;
  isLast?: boolean;
  onCredentialSaved?: () => void;
}) {
  const si = (iface.status_indicator ?? "unconfigured") as InterfaceStatusIndicator;
  const isPlanned = si === "planned";
  const canTest = iface.testable !== false && !isPlanned;
  const hasCredentialEdit = iface.source === "tushare" || iface.source === "tqsdk";
  const credentialMasked = iface.credential_masked ?? iface.token_masked ?? null;
  return (
    <div className={`ifr-card${isPlanned ? " ifr-card--planned" : ""}`}>
      <div className="ifr-card-header">
        <div className="ifr-card-title-block">
          <strong>{iface.label}</strong>
          <span className="ifr-source-id">{iface.source}</span>
        </div>
        <StatusChip status={si} />
        <span className="ifr-protocol-label">
          {PROTOCOL_LABELS[iface.protocol] ?? iface.protocol}
        </span>
      </div>
      <div className="ifr-card-note">{iface.note}</div>
      <div className="ifr-meta-list">
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">分类</span>
          <span>{getSourceCategoryLabel(iface.category)}</span>
        </div>
        <div className="ifr-meta-row">
          <span className="ifr-meta-label">优先级</span>
          <span>
            #{iface.priority ?? "—"}
            {" "}
            <span className="ifr-anchor-kind">{ANCHOR_KIND_LABELS[iface.anchor_kind ?? ""] ?? iface.anchor_kind ?? "—"}</span>
          </span>
          {onReorder ? (
            <span className="ifr-reorder-btns">
              <button
                type="button"
                className="ifr-reorder-btn"
                disabled={reorderBusy || isFirst}
                title="上移（提高优先级）"
                onClick={() => onReorder(iface.source, "up")}
              >▲</button>
              <button
                type="button"
                className="ifr-reorder-btn"
                disabled={reorderBusy || isLast}
                title="下移（降低优先级）"
                onClick={() => onReorder(iface.source, "down")}
              >▼</button>
            </span>
          ) : null}
        </div>
        {iface.path != null ? (
          <div className="ifr-meta-row">
            <span className="ifr-meta-label">路径</span>
            <span className="mono-text">
              {iface.path_exists ? "✓ " : "✗ "}{iface.path}
            </span>
          </div>
        ) : null}
        {!hasCredentialEdit && iface.token_masked ? (
          <div className="ifr-meta-row">
            <span className="ifr-meta-label">凭证</span>
            <span className="mono-text">{iface.token_masked}</span>
          </div>
        ) : null}
      </div>
      {iface.capabilities && iface.capabilities.length > 0 && (
        <CapabilityBadges caps={iface.capabilities} />
      )}
      {hasCredentialEdit && onCredentialSaved ? (
        <CredentialEditSection
          source={iface.source}
          currentMasked={credentialMasked}
          onSaved={onCredentialSaved}
        />
      ) : null}
      <div className="ifr-test-row">
        <button
          type="button"
          className="ifr-action-btn"
          disabled={!canTest || testLoading}
          onClick={() => onTest(iface.source)}
        >
          {!canTest ? "无需测试" : testLoading ? "测试中…" : "连通测试"}
        </button>
        {testResult ? <TestChip result={testResult} /> : null}
      </div>
    </div>
  );
}

function InterfaceGroup({
  cls,
  items,
  testLoading,
  testResults,
  onTest,
  onReorder,
  reorderBusy,
  onCredentialSaved,
}: {
  cls: string;
  items: InterfaceProfilePayload[];
  testLoading: Record<string, boolean>;
  testResults: Record<string, DataSourceTestPayload>;
  onTest: (source: string) => void;
  onReorder?: (source: string, dir: "up" | "down") => void;
  reorderBusy?: boolean;
  onCredentialSaved?: () => void;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const configuredCount = items.filter((i) => i.status_indicator === "configured").length;
  const label = CLASS_LABELS[cls] ?? cls;
  return (
    <div className="ifr-group">
      <button
        type="button"
        className="ifr-group-toggle"
        onClick={() => setCollapsed((v) => !v)}
      >
        <span className={`ifr-group-caret${collapsed ? " collapsed" : ""}`}>
          ▾
        </span>
        <span className="ifr-group-title">{label}</span>
        <span className="ifr-group-count">
          {configuredCount}/{items.length}
        </span>
      </button>
      {!collapsed && (
        <div className="ifr-group-grid">
          {items.map((iface, idx) => (
            <InterfaceCard
              key={iface.source}
              iface={iface}
              testLoading={!!testLoading[iface.source]}
              testResult={testResults[iface.source]}
              onTest={onTest}
              onReorder={onReorder}
              reorderBusy={reorderBusy}
              isFirst={idx === 0}
              isLast={idx === items.length - 1}
              onCredentialSaved={onCredentialSaved}
            />
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Summary bar
// ---------------------------------------------------------------------------

export function SummaryBar({ summary }: { summary: InterfaceRegistryPayload["summary"] }) {
  return (
    <div className="ifr-summary-grid">
      <span className="status-chip ok">已配置 {summary.configured}</span>
      <span className="status-chip warning">未配置 {summary.unconfigured}</span>
      <span className="status-chip ifr-status-chip--planned">规划中 {summary.planned}</span>
      <span className="status-chip">合计 {summary.total}</span>
    </div>
  );
}

export function InterfaceOverviewFallback({
  loading,
  error,
}: {
  loading: boolean;
  error: string | null;
}) {
  return (
    <section className="ifr-interface-section">
      <div className="ifr-section-header">
        <div>
          <div className="wbd-data-section-title">接口注册表</div>
          <div className="ifr-section-note">后端不可达时，接口管理页仍保留固定入口、重扫摘要、本地发现和账户闭环建议。</div>
        </div>
      </div>
      <div className="ifr-offline-card">
        <span className={`status-chip ${loading ? "warning" : error ? "danger" : "warning"}`}>
          {loading ? "接口注册表加载中" : "接口注册表暂不可用"}
        </span>
        <div className="ifr-card-note">
          {loading ? "正在尝试读取接口注册表；其余前端模块将保持可见。" : error ?? "当前没有可用的接口注册表响应。"}
        </div>
        <div className="ifr-offline-tip-list">
          <div className="status-chip">固定入口与后端状态：仍可查看</div>
          <div className="status-chip">本地发现：可显示扫描失败与重扫摘要</div>
          <div className="status-chip">账户绑定：可继续显示候选建议</div>
        </div>
      </div>
    </section>
  );
}

export function DiscoveryOverview({ dsConfig, configError }: { dsConfig: DataSourceConfigPayload | null; configError: string | null }) {
  const discoveryEntries = Object.entries(dsConfig?.discovery ?? {});
  if (!dsConfig && !configError) {
    return null;
  }
  return (
    <section className="ifr-discovery-section">
      <div className="ifr-section-header">
        <div>
          <div className="wbd-data-section-title">本地发现</div>
          <div className="ifr-section-note">优先显示 QMT 本地事实锚点和本地 TDX / pytdx 候选目录。</div>
        </div>
      </div>
      {configError ? <div className="status-chip danger">{configError}</div> : null}
      {discoveryEntries.length > 0 ? (
        <div className="ifr-discovery-grid">
          {discoveryEntries.map(([discoveryKey, payload]) => (
            <DiscoveryCard key={discoveryKey} discoveryKey={discoveryKey} payload={payload} />
          ))}
        </div>
      ) : !configError ? (
        <div className="wbd-data-card-empty">当前未返回本地发现结果。</div>
      ) : null}
    </section>
  );
}

export function InterfaceOverview({
  registry,
  dsConfig,
  testLoading,
  testResults,
  onTest,
  onReorder,
  reorderBusy,
  onCredentialSaved,
}: {
  registry: InterfaceRegistryPayload;
  dsConfig: DataSourceConfigPayload | null;
  testLoading: Record<string, boolean>;
  testResults: Record<string, DataSourceTestPayload>;
  onTest: (source: string) => void;
  onReorder?: (source: string, dir: "up" | "down") => void;
  reorderBusy?: boolean;
  onCredentialSaved?: () => void;
}) {
  const configSourceMap = useMemo(
    () => new Map((dsConfig?.sources ?? []).map((item) => [item.source, item])),
    [dsConfig],
  );

  const groups = useMemo(() => {
    const mergedInterfaces = registry.interfaces
      .map((iface) => ({
        ...iface,
        ...(configSourceMap.get(iface.source) ?? {}),
      }))
      .sort((left, right) => (left.priority ?? 999) - (right.priority ?? 999));
    const nextGroups: Record<string, InterfaceProfilePayload[]> = {};
    for (const iface of mergedInterfaces) {
      const cls = iface.interface_class ?? "market_data";
      nextGroups[cls] = [...(nextGroups[cls] ?? []), iface];
    }
    return nextGroups;
  }, [configSourceMap, registry.interfaces]);

  const orderedClasses = [
    ...CLASS_ORDER.filter((cls) => groups[cls]),
    ...Object.keys(groups).filter((cls) => !CLASS_ORDER.includes(cls)),
  ];

  return (
    <section className="ifr-interface-section">
      <div className="ifr-section-header">
        <div>
          <div className="wbd-data-section-title">接口注册表</div>
          <div className="ifr-section-note">统一展示市场数据 / 交易网关 / 存储端点，并支持现有数据源连通测试。</div>
        </div>
      </div>
      {orderedClasses.map((cls) => (
        <InterfaceGroup
          key={cls}
          cls={cls}
          items={groups[cls] ?? []}
          testLoading={testLoading}
          testResults={testResults}
          onTest={onTest}
          onReorder={onReorder}
          reorderBusy={reorderBusy}
          onCredentialSaved={onCredentialSaved}
        />
      ))}
    </section>
  );
}
// End of extracted interface catalog components.
