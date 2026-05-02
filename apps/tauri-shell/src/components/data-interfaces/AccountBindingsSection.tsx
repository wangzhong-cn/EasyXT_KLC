import { useCallback, useEffect, useMemo, useState } from "react";
import {
  applyAccountBinding,
  applyQmtRuntimeConfigFromAccount,
  discoverAccountBindings,
  fetchAccountBindings,
  fetchQmtRuntimeConfig,
  type AccountBindingsPayload,
  type BrokerAccountPayload,
  type DataSourceConfigPayload,
  type DataSourceDiscoveryCandidatePayload,
  type QmtRuntimeConfigPayload,
} from "../../lib/api";
import {
  BINDING_PROBE_ACTION_LABEL,
  BINDING_PROBE_ACTION_LOADING_LABEL,
  getBindingApplySuccessNote,
  getBindingDraftLabel,
  getBindingDraftTone,
  getBindingFormalSourceNote,
  mapBindingItemToDraft,
  type AccountBindingDraftViewModel,
  type AccountBindingObservationViewModel,
} from "../../lib/qmtBindingDrafts";
import {
  getBrokerAccountDisplayLabel,
  normalizeComparablePath,
  runtimeConfigMatchesAccount,
} from "../../lib/qmtPathUtils";
import { getQmtConflictTone, getQmtSessionTone } from "../../lib/qmtRuntimeUiUtils";
import { QmtRuntimeConfigCard } from "./QmtRuntimeConfigCard";

interface AccountBindingObservation {
  accountId: string;
  signal: "path_aligned" | "candidate_available" | "candidate_missing";
  note: string;
  candidatePath: string | null;
  pathAligned: boolean;
}

interface AccountBindingObservationCardViewModel {
  accountId: string;
  reasons: string[];
  observation: AccountBindingObservationViewModel;
}

interface AccountBindingsSectionProps {
  accounts: BrokerAccountPayload[];
  accountsError: string | null;
  dsConfig: DataSourceConfigPayload | null;
  onApplied: () => Promise<void> | void;
  onConfigureRequested?: () => void;
  probeRefreshNonce: number;
  onProbeRequested: () => void;
}

function buildObservationCardModel(
  observation: AccountBindingObservation,
): AccountBindingObservationCardViewModel {
  const reasons: string[] = [];

  if (observation.pathAligned) {
    reasons.push("本地路径观测已与候选对齐，但仍需等待 formal review 返回正式 binding 项。");
  } else if (observation.candidatePath) {
    reasons.push("本地 discovery 提供了候选路径线索，仅用于人工核对，不参与写回裁决。");
  } else {
    reasons.push("当前没有可参考的本地候选路径，需要等待 formal review 或人工补全。 ");
  }

  return {
    accountId: observation.accountId,
    reasons,
    observation: {
      accountId: observation.accountId,
      status: observation.pathAligned ? "matched" : observation.candidatePath ? "suggested" : "missing",
      message: observation.note,
      candidatePath: observation.candidatePath,
    },
  };
}

function mapObservationTone(
  signal: AccountBindingObservation["signal"],
): "ok" | "warning" | "danger" {
  switch (signal) {
    case "path_aligned":
      return "ok";
    case "candidate_available":
      return "warning";
    case "candidate_missing":
    default:
      return "danger";
  }
}

function buildLocalBindingObservation(
  account: BrokerAccountPayload,
  qmtCandidates: DataSourceDiscoveryCandidatePayload[],
): AccountBindingObservation {
  const normalizedUserdata = normalizeComparablePath(account.qmt_userdata_path);
  const normalizedExe = normalizeComparablePath(account.qmt_exe_path);
  const matchedCandidate = qmtCandidates.find((candidate) => {
    const candidateUserdata = normalizeComparablePath(candidate.userdata_path);
    const candidateInstall = normalizeComparablePath(candidate.install_path);
    return (normalizedUserdata && candidateUserdata && normalizedUserdata === candidateUserdata)
      || (normalizedExe && candidateInstall && normalizedExe.startsWith(candidateInstall));
  }) ?? null;

  if (matchedCandidate) {
    return {
      accountId: account.id,
      signal: "path_aligned",
      note: "本地路径观测已对齐候选",
      candidatePath: matchedCandidate.userdata_path ?? matchedCandidate.install_path ?? null,
      pathAligned: true,
    };
  }

  const bestCandidate = qmtCandidates[0] ?? null;
  if (!bestCandidate) {
    return {
      accountId: account.id,
      signal: "candidate_missing",
      note: "当前缺少可参考的本地候选",
      candidatePath: null,
      pathAligned: false,
    };
  }

  return {
    accountId: account.id,
    signal: "candidate_available",
    note: normalizedUserdata ? "当前路径未命中本地候选，建议人工核对" : "发现可核对的本地候选路径",
    candidatePath: bestCandidate.userdata_path ?? bestCandidate.install_path ?? null,
    pathAligned: false,
  };
}

function AccountBindingClosureCard({
  accounts,
  observations,
  qmtCandidateCount,
}: {
  accounts: BrokerAccountPayload[];
  observations: AccountBindingObservation[];
  qmtCandidateCount: number;
}) {
  const accountMap = useMemo(() => new Map(accounts.map((account) => [account.id, account])), [accounts]);
  const alignedItems = observations.filter((item) => item.signal === "path_aligned");
  const reviewItems = observations.filter((item) => item.signal === "candidate_available");
  const missingCandidateItems = observations.filter((item) => item.signal === "candidate_missing");
  const suggestionGroups = useMemo(() => {
    const nextGroups = new Map<string, Array<{ accountId: string; label: string }>>();
    for (const item of reviewItems) {
      if (!item.candidatePath) continue;
      const account = accountMap.get(item.accountId);
      const label = account?.label || account?.trade_account || item.accountId;
      const existing = nextGroups.get(item.candidatePath) ?? [];
      nextGroups.set(item.candidatePath, [...existing, { accountId: item.accountId, label }]);
    }
    return Array.from(nextGroups.entries());
  }, [accountMap, reviewItems]);

  return (
    <section className="ifr-binding-closure">
      <div className="ifr-section-header">
        <div>
          <div className="wbd-data-section-title">本地候选观测</div>
          <div className="ifr-section-note">这里仅展示本地 discovery 观察到的候选路径，不参与正式 binding 写回；可写回资格以 `account-bindings` 正式审阅结果为准。</div>
        </div>
      </div>
      <div className="ifr-summary-grid">
        <span className="status-chip ok">路径已对齐 {alignedItems.length}</span>
        <span className={`status-chip ${reviewItems.length > 0 ? "warning" : "ok"}`}>待人工核对 {reviewItems.length}</span>
        <span className={`status-chip ${missingCandidateItems.length > 0 ? "danger" : "ok"}`}>缺本地候选 {missingCandidateItems.length}</span>
        <span className={`status-chip ${qmtCandidateCount > 0 ? "ok" : "warning"}`}>本地候选 {qmtCandidateCount}</span>
      </div>
      {suggestionGroups.length > 0 ? (
        <div className="ifr-binding-suggestion-list">
          {suggestionGroups.map(([candidatePath, items]) => (
            <div key={candidatePath} className="ifr-binding-suggestion-card">
              <div className="ifr-binding-suggestion-head">
                <strong>观测到候选路径</strong>
                <span className="status-chip warning">账户 {items.length}</span>
                <span className="status-chip">只读观测</span>
              </div>
              <div className="mono-text ifr-binding-suggestion-path">{candidatePath}</div>
              <div className="ifr-binding-suggestion-accounts">
                {items.map(({ accountId, label }) => (
                  <span key={`${candidatePath}-${accountId}`} className="status-chip warning">{label}</span>
                ))}
              </div>
              <div className="ifr-binding-draft-note">请回到下方正式审阅卡，基于 `account-bindings` 返回的 draft 执行写回。</div>
            </div>
          ))}
        </div>
      ) : null}
      {missingCandidateItems.length > 0 ? (
        <div className="status-chip danger">
          当前缺少本地候选：{missingCandidateItems.map((item) => accountMap.get(item.accountId)?.label || accountMap.get(item.accountId)?.trade_account || item.accountId).join(" / ")}
        </div>
      ) : null}
      {suggestionGroups.length === 0 && missingCandidateItems.length === 0 ? (
        <div className="status-chip ok">当前账户的本地路径观测已基本对齐，可继续以正式审阅结果推进配置治理。</div>
      ) : null}
    </section>
  );
}

const CONFLICT_DETAIL_PRIORITY = [
  "userdata_paths",
  "broker_ids",
  "current_route_claims",
  "status",
  "install_root",
  "exe_path",
  "userdata_roots",
];

function formatBindingExplainValue(value: unknown): string {
  if (value == null) {
    return "—";
  }
  if (Array.isArray(value)) {
    const normalized = value
      .map((item) => (typeof item === "string" || typeof item === "number" || typeof item === "boolean" ? String(item) : ""))
      .filter(Boolean);
    return normalized.length > 0 ? normalized.join(" / ") : "—";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function getBindingConflictDetailEntries(details: Record<string, unknown>): Array<[string, string]> {
  const entries: Array<[string, string]> = [];
  for (const key of CONFLICT_DETAIL_PRIORITY) {
    if (!(key in details)) {
      continue;
    }
    entries.push([key, formatBindingExplainValue(details[key])]);
  }
  for (const [key, value] of Object.entries(details)) {
    if (CONFLICT_DETAIL_PRIORITY.includes(key)) {
      continue;
    }
    entries.push([key, formatBindingExplainValue(value)]);
  }
  return entries.filter(([, value]) => value && value !== "—");
}

function shouldRenderBindingMonoValue(key: string, value: string): boolean {
  return (
    key.includes("path")
    || key.includes("id")
    || key.includes("account")
    || value.includes("\\")
    || value.includes("/")
  );
}

function getBindingSessionExplainEntries(draft: AccountBindingDraftViewModel): Array<[string, string]> {
  return draft.session ? [
    ["session_health", formatBindingExplainValue(draft.session.session_health)] as [string, string],
    ["status", formatBindingExplainValue(draft.session.status)] as [string, string],
    ["login_status", formatBindingExplainValue(draft.session.login_status)] as [string, string],
    ["connected_accounts", formatBindingExplainValue(draft.session.connected_accounts)] as [string, string],
    ["current_route_claims", formatBindingExplainValue(draft.session.current_route_claims)] as [string, string],
    ["userdata_path", formatBindingExplainValue(draft.session.userdata_path)] as [string, string],
    ["last_error", formatBindingExplainValue(draft.session.last_error)] as [string, string],
    ["freshness_state", formatBindingExplainValue(draft.session.freshness_state)] as [string, string],
  ].filter(([, value]) => value && value !== "—") : [];
}

function getBindingProbeExplainEntries(draft: AccountBindingDraftViewModel): Array<[string, string]> {
  return draft.probe ? [
    ["probe_success", formatBindingExplainValue(draft.probe.probe_success)] as [string, string],
    ["account_id", formatBindingExplainValue(draft.probe.account_id)] as [string, string],
    ["account_type", formatBindingExplainValue(draft.probe.account_type)] as [string, string],
    ["login_status", formatBindingExplainValue(draft.probe.login_status)] as [string, string],
    ["reachable", formatBindingExplainValue(draft.probe.reachable)] as [string, string],
    ["probe_method", formatBindingExplainValue(draft.probe.probe_method)] as [string, string],
    ["probe_error_code", formatBindingExplainValue(draft.probe.probe_error_code)] as [string, string],
    ["probe_error_message", formatBindingExplainValue(draft.probe.probe_error_message)] as [string, string],
    ["freshness_state", formatBindingExplainValue(draft.probe.freshness_state)] as [string, string],
    ["userdata_path", formatBindingExplainValue(draft.probe.userdata_path)] as [string, string],
  ].filter(([, value]) => value && value !== "—") : [];
}

function BindingDraftExplainSections({
  draft,
}: {
  draft: AccountBindingDraftViewModel;
}) {
  const sessionExplainEntries = getBindingSessionExplainEntries(draft);
  const probeExplainEntries = getBindingProbeExplainEntries(draft);
  const routeScores = Object.entries(draft.route?.score_breakdown ?? {}).sort((left, right) => right[1] - left[1]);

  return (
    <>
      {draft.session || draft.probe ? (
        <div className="ifr-binding-draft-explain">
          <div className="ifr-binding-draft-title">Runtime Explain</div>
          <div className="ifr-binding-draft-grid ifr-binding-draft-grid--explain">
            <div className="ifr-binding-draft-block">
              <div className="ifr-binding-draft-title">Session Explain</div>
              {draft.session ? (
                <div className="ifr-meta-list">
                  {sessionExplainEntries.map(([key, value]) => (
                    <div key={`${draft.accountId}-session-${key}`} className="ifr-meta-row">
                      <span className="ifr-meta-label">{key}</span>
                      <span className={shouldRenderBindingMonoValue(key, value) ? "mono-text" : undefined}>{value}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="ifr-binding-draft-note">当前未命中 session 锚点。</div>
              )}
            </div>
            <div className="ifr-binding-draft-block">
              <div className="ifr-binding-draft-title">Probe Explain</div>
              {draft.probe ? (
                <div className="ifr-meta-list">
                  {probeExplainEntries.map(([key, value]) => (
                    <div key={`${draft.accountId}-probe-${key}`} className="ifr-meta-row">
                      <span className="ifr-meta-label">{key}</span>
                      <span className={shouldRenderBindingMonoValue(key, value) ? "mono-text" : undefined}>{value}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="ifr-binding-draft-note">当前未命中 probe explain。</div>
              )}
            </div>
          </div>
        </div>
      ) : null}
      {draft.route ? (
        <div className="ifr-binding-draft-explain">
          <div className="ifr-binding-draft-title">Route Explain</div>
          <div className="ifr-binding-draft-grid ifr-binding-draft-grid--explain">
            <div className="ifr-binding-draft-block">
              <div className="ifr-binding-draft-title">决策结果</div>
              <div className="ifr-binding-draft-reason-list">
                <span className="status-chip ok">winner {draft.route.winner || "—"}</span>
                <span className={`status-chip ${draft.route.runner_up ? "warning" : ""}`}>runner-up {draft.route.runner_up || "—"}</span>
                <span className="status-chip">purpose {draft.route.purpose || "—"}</span>
              </div>
            </div>
            <div className="ifr-binding-draft-block">
              <div className="ifr-binding-draft-title">候选集</div>
              <div className="ifr-binding-draft-reason-list">
                {draft.route.candidate_ids.length > 0 ? draft.route.candidate_ids.map((candidateId) => (
                  <span key={`${draft.accountId}-route-candidate-${candidateId}`} className={`status-chip ${candidateId === draft.route?.winner ? "ok" : candidateId === draft.route?.runner_up ? "warning" : ""}`}>
                    {candidateId}
                  </span>
                )) : <span className="status-chip">—</span>}
              </div>
            </div>
            <div className="ifr-binding-draft-block">
              <div className="ifr-binding-draft-title">判定理由</div>
              <div className="ifr-binding-draft-note">
                {draft.route.decision_reason || "当前未提供额外 decision_reason。"}
              </div>
              {draft.route.rejection_reasons.length > 0 ? (
                <div className="ifr-binding-draft-reason-list">
                  {draft.route.rejection_reasons.map((reason, index) => (
                    <span key={`${draft.accountId}-route-reject-${index}`} className="status-chip warning">
                      {reason}
                    </span>
                  ))}
                </div>
              ) : null}
            </div>
          </div>
          <div className="ifr-binding-draft-score-list">
            {routeScores.length > 0 ? routeScores.map(([key, score]) => (
              <div key={`${draft.accountId}-route-score-${key}`} className="ifr-binding-draft-score-row">
                <span className="mono-text">{key}</span>
                <span className={`status-chip ${key === draft.route?.winner ? "ok" : key === draft.route?.runner_up ? "warning" : ""}`}>
                  {score.toFixed(3)}
                </span>
              </div>
            )) : (
              <div className="ifr-binding-draft-note">当前未提供 score_breakdown。</div>
            )}
          </div>
        </div>
      ) : null}
      {draft.conflicts.length > 0 ? (
        <div className="ifr-binding-draft-explain">
          <div className="ifr-binding-draft-title">Conflict Explain</div>
          <div className="ifr-binding-draft-conflict-stack">
            {draft.conflicts.map((item) => {
              const detailEntries = getBindingConflictDetailEntries(item.details ?? {});
              return (
                <div key={`${draft.accountId}-${item.code}-${item.target_id}`} className="ifr-binding-draft-conflict-card">
                  <div className="ifr-binding-draft-conflicts">
                    <div className={`status-chip ${getQmtConflictTone(item.severity)}`}>
                      {item.code}: {item.message}
                    </div>
                    <span className="status-chip">target {item.target_kind}:{item.target_id}</span>
                  </div>
                  {detailEntries.length > 0 ? (
                    <div className="ifr-meta-list">
                      {detailEntries.map(([key, value]) => (
                        <div key={`${draft.accountId}-${item.code}-${key}`} className="ifr-meta-row">
                          <span className="ifr-meta-label">{key}</span>
                          <span className={value.includes("/") || value.includes("\\") ? "mono-text" : undefined}>{value}</span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="ifr-binding-draft-note">当前未提供结构化 conflict details。</div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      ) : null}
    </>
  );
}

function BindingDraftReviewCard({
  account,
  draft,
  onApplied,
}: {
  account: BrokerAccountPayload;
  draft: AccountBindingDraftViewModel;
  onApplied: () => Promise<void> | void;
}) {
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);
  const canApply = draft.canWriteback && Boolean(draft.applyPath) && draft.applyPath !== account.qmt_userdata_path;

  async function handleApply() {
    if (!draft.bindingId || !draft.applyPath) {
      return;
    }
    setSaving(true);
    setSaveError(null);
    try {
      await applyAccountBinding(draft.bindingId, { includeProbes: true, force: true });
      await onApplied();
      setSaved(true);
    } catch (err) {
      setSaveError(err instanceof Error ? err.message : "写回绑定或刷新审阅结果失败");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="ifr-binding-draft-card">
      <div className="ifr-binding-draft-head">
        <div>
          <strong>{account.label || account.trade_account}</strong>
          <div className="ifr-section-note">资金账号 {account.trade_account || "—"} / 券商 {account.broker || "—"}</div>
        </div>
        <div className="ifr-binding-draft-head-actions">
          <span className={`status-chip ${getBindingDraftTone(draft.state)}`}>{getBindingDraftLabel(draft.state)}</span>
          <span className="status-chip">置信度 {draft.confidence}</span>
          {saved ? <span className="status-chip ok">配置已写回</span> : null}
        </div>
      </div>
      <div className="ifr-binding-draft-grid">
        <div className="ifr-binding-draft-block">
          <div className="ifr-binding-draft-title">候选路径</div>
          <div className="mono-text">{draft.applyPath || "—"}</div>
          {canApply ? (
            <button type="button" className="action-btn" disabled={saving} onClick={() => void handleApply()}>
              {saving ? "写回中…" : "确认写回"}
            </button>
          ) : (
            <span className="status-chip">{draft.applyPath ? "当前已对齐" : "暂无可写回路径"}</span>
          )}
        </div>
        <div className="ifr-binding-draft-block">
          <div className="ifr-binding-draft-title">匹配理由</div>
          <div className="ifr-binding-draft-reason-list">
            {draft.reasons.map((reason, index) => (
              <span key={`${draft.accountId}-reason-${index}`} className="status-chip">
                {reason}
              </span>
            ))}
          </div>
        </div>
        <div className="ifr-binding-draft-block">
          <div className="ifr-binding-draft-title">运行态锚点</div>
          <div className="ifr-binding-draft-reason-list">
            <span className={`status-chip ${draft.session ? getQmtSessionTone(draft.session.status) : "warning"}`}>
              session {draft.session?.status || "未命中"}
            </span>
            <span className={`status-chip ${draft.probe?.probe_success ? "ok" : "warning"}`}>
              probe {draft.probe?.account_id || draft.probe?.status || "未触发"}
            </span>
            <span className={`status-chip ${draft.route ? "ok" : "warning"}`}>
              route {draft.route?.purpose || "未命中"}
            </span>
          </div>
        </div>
      </div>
      {saved ? (
        <div className="ifr-binding-draft-note">
          {getBindingApplySuccessNote("bindings")}
        </div>
      ) : null}
      <BindingDraftExplainSections draft={draft} />
      {saveError ? <div className="status-chip danger">{saveError}</div> : null}
    </div>
  );
}

function BindingObservationCard({
  account,
  observationCard,
}: {
  account: BrokerAccountPayload;
  observationCard: AccountBindingObservationCardViewModel;
}) {
  const displayedPath = observationCard.observation.candidatePath
    ?? account.qmt_userdata_path
    ?? account.qmt_exe_path
    ?? null;

  return (
    <div className="ifr-binding-draft-card">
      <div className="ifr-binding-draft-head">
        <div>
          <strong>{account.label || account.trade_account}</strong>
          <div className="ifr-section-note">资金账号 {account.trade_account || "—"} / 券商 {account.broker || "—"}</div>
        </div>
        <div className="ifr-binding-draft-head-actions">
          <span className="status-chip warning">本地观测</span>
          <span className="status-chip">不参与写回</span>
        </div>
      </div>
      <div className="ifr-binding-draft-grid">
        <div className="ifr-binding-draft-block">
          <div className="ifr-binding-draft-title">观测路径线索</div>
          <div className="mono-text">{displayedPath || "—"}</div>
          <span className="status-chip warning">仅本地观测，等待正式审阅</span>
        </div>
        <div className="ifr-binding-draft-block">
          <div className="ifr-binding-draft-title">观测说明</div>
          <div className="ifr-binding-draft-reason-list">
            <span className="status-chip">{observationCard.observation.message || "当前未提供额外 observation 说明。"}</span>
            {observationCard.reasons.map((reason, index) => (
              <span key={`${observationCard.accountId}-observation-reason-${index}`} className="status-chip">
                {reason}
              </span>
            ))}
          </div>
        </div>
        <div className="ifr-binding-draft-block">
          <div className="ifr-binding-draft-title">当前账户配置</div>
          <div className="ifr-meta-list">
            <div className="ifr-meta-row">
              <span className="ifr-meta-label">userdata</span>
              <span className="mono-text">{account.qmt_userdata_path || "—"}</span>
            </div>
            <div className="ifr-meta-row">
              <span className="ifr-meta-label">exe</span>
              <span className="mono-text">{account.qmt_exe_path || "—"}</span>
            </div>
          </div>
        </div>
      </div>
      <div className="ifr-binding-draft-note">当前卡片只展示本地 discovery 观察到的路径线索，不定义正式 binding 状态、置信度或写回目标；请等待 `account-bindings` 提供正式审阅项。</div>
    </div>
  );
}

export function AccountBindingsSection({
  accounts,
  accountsError,
  dsConfig,
  onApplied,
  onConfigureRequested,
  probeRefreshNonce,
  onProbeRequested,
}: AccountBindingsSectionProps) {
  const activeCount = accounts.filter((item) => item.is_active).length;
  const defaultCount = accounts.filter((item) => item.is_default).length;
  const localPathBoundCount = accounts.filter((item) => Boolean(item.qmt_userdata_path || item.qmt_exe_path)).length;
  const qmtCandidates = dsConfig?.discovery?.qmt_local_dat?.candidates ?? [];
  const [accountBindings, setAccountBindings] = useState<AccountBindingsPayload | null>(null);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [runtimeConfig, setRuntimeConfig] = useState<QmtRuntimeConfigPayload | null>(null);
  const [runtimeConfigError, setRuntimeConfigError] = useState<string | null>(null);
  const [runtimeSyncAccountId, setRuntimeSyncAccountId] = useState<string | null>(null);
  const [probeBusy, setProbeBusy] = useState(false);

  const loadAccountBindings = useCallback(async (includeProbes = false) => {
    const payload = await fetchAccountBindings({ includeProbes });
    setAccountBindings(payload);
    setRuntimeError(null);
  }, []);

  const loadRuntimeConfig = useCallback(async () => {
    const payload = await fetchQmtRuntimeConfig();
    setRuntimeConfig(payload);
    setRuntimeConfigError(null);
  }, []);

  useEffect(() => {
    let active = true;
    loadAccountBindings(false).catch((reason: unknown) => {
      if (!active) {
        return;
      }
      setRuntimeError(reason instanceof Error ? reason.message : "account-bindings 读取失败");
    });
    return () => {
      active = false;
    };
  }, [loadAccountBindings, accounts]);

  useEffect(() => {
    let active = true;
    loadRuntimeConfig().catch((reason: unknown) => {
      if (!active) {
        return;
      }
      setRuntimeConfigError(reason instanceof Error ? reason.message : "运行时 QMT 主配置读取失败");
    });
    return () => {
      active = false;
    };
  }, [loadRuntimeConfig]);

  const candidateUserdataSet = useMemo(
    () => new Set(
      qmtCandidates
        .map((item) => normalizeComparablePath(item.userdata_path ?? item.datadir_path ?? item.install_path ?? null))
        .filter(Boolean),
    ),
    [qmtCandidates],
  );
  const accountObservationMap = useMemo(() => {
    const next = new Map<string, AccountBindingObservation>();
    for (const item of accountBindings?.items ?? []) {
      next.set(item.broker_account_id, {
        accountId: item.broker_account_id,
        signal: item.recommendation_status === "matched"
          ? "path_aligned"
          : item.candidate_path
            ? "candidate_available"
            : "candidate_missing",
        note: item.recommendation_message,
        candidatePath: item.candidate_path,
        pathAligned: item.recommendation_status === "matched",
      });
    }
    return next;
  }, [accountBindings?.items]);
  const accountObservations = useMemo(
    () => accounts.map((account) => accountObservationMap.get(account.id) ?? buildLocalBindingObservation(account, qmtCandidates)),
    [accountObservationMap, accounts, qmtCandidates],
  );
  const formalBindingDrafts = useMemo(
    () => (accountBindings?.items ?? []).map((item) => mapBindingItemToDraft(item)),
    [accountBindings?.items],
  );
  const observationCards = useMemo(
    () => {
      const formalAccountIds = new Set(formalBindingDrafts.map((item) => item.accountId));
      return accounts
        .filter((account) => !formalAccountIds.has(account.id))
        .map((account) => {
          const observation = accountObservationMap.get(account.id) ?? buildLocalBindingObservation(account, qmtCandidates);
          return buildObservationCardModel(observation);
        });
    },
    [accountObservationMap, accounts, formalBindingDrafts, qmtCandidates],
  );
  const confirmedDraftCount = formalBindingDrafts.filter((item) => item.state === "confirmed").length;
  const reviewDraftCount = formalBindingDrafts.filter((item) => item.state === "review_required").length;
  const proposedDraftCount = formalBindingDrafts.filter((item) => item.state === "proposed").length;

  const handleProbeRefresh = useCallback(async () => {
    setProbeBusy(true);
    try {
      const payload = await discoverAccountBindings({ includeProbes: true, force: true });
      setAccountBindings(payload);
      setRuntimeError(null);
    } catch (err) {
      setRuntimeError(err instanceof Error ? err.message : "account-bindings probe explain 读取失败");
    } finally {
      setProbeBusy(false);
    }
  }, []);

  useEffect(() => {
    if (probeRefreshNonce <= 0) {
      return;
    }
    void handleProbeRefresh();
  }, [handleProbeRefresh, probeRefreshNonce]);

  const runtimeDefaultAccounts = useMemo(
    () => accounts.filter((account) => runtimeConfigMatchesAccount(runtimeConfig, account)),
    [accounts, runtimeConfig],
  );
  const runtimeConfigReady = Boolean(
    runtimeConfig?.exists && (runtimeConfig.qmt_path || runtimeConfig.qmt_userdata_path),
  );

  const handleSetRuntimeDefault = useCallback(async (accountId: string) => {
    setRuntimeSyncAccountId(accountId);
    setRuntimeConfigError(null);
    try {
      const payload = await applyQmtRuntimeConfigFromAccount(accountId);
      setRuntimeConfig(payload.runtime_config);
    } catch (err) {
      setRuntimeConfigError(err instanceof Error ? err.message : "运行时 QMT 主配置写回失败");
    } finally {
      setRuntimeSyncAccountId(null);
    }
  }, []);

  const handleApplied = useCallback(async () => {
    await Promise.resolve(onApplied());
    try {
      await loadAccountBindings(false);
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : "account-bindings 刷新失败";
      setRuntimeError(`账户配置已写回，但正式审阅结果刷新失败：${message}`);
      throw new Error(`账户配置已写回，但正式审阅结果刷新失败：${message}`);
    }
    void loadRuntimeConfig().catch((reason: unknown) => {
      setRuntimeConfigError(reason instanceof Error ? reason.message : "运行时 QMT 主配置读取失败");
    });
  }, [loadAccountBindings, loadRuntimeConfig, onApplied]);

  const accountDiagnostics = useMemo(() => {
    const missingPaths = accounts.filter((item) => !item.qmt_userdata_path && !item.qmt_exe_path);
    const unmatchedUserdata = accounts.filter((item) => {
      const normalized = normalizeComparablePath(item.qmt_userdata_path);
      return normalized && !candidateUserdataSet.has(normalized);
    });

    const pathMap = new Map<string, BrokerAccountPayload[]>();
    for (const account of accounts) {
      const normalized = normalizeComparablePath(account.qmt_userdata_path);
      if (!normalized) continue;
      pathMap.set(normalized, [...(pathMap.get(normalized) ?? []), account]);
    }
    const sharedUserdataGroups = Array.from(pathMap.values()).filter((group) => group.length > 1);
    const matchedAccountCount = accounts.filter((item) => {
      const normalized = normalizeComparablePath(item.qmt_userdata_path);
      return normalized && candidateUserdataSet.has(normalized);
    }).length;
    const candidateReviewNeededCount = accountObservations.filter((item) => item.signal === "candidate_available").length;

    return {
      missingPaths,
      unmatchedUserdata,
      sharedUserdataGroups,
      matchedAccountCount,
      candidateReviewNeededCount,
      candidateCount: candidateUserdataSet.size,
    };
  }, [accountObservations, accounts, candidateUserdataSet]);

  return (
    <section className="ifr-account-section">
      <div className="ifr-section-header">
        <div>
          <div className="wbd-data-section-title">账户绑定</div>
          <div className="ifr-section-note">{getBindingFormalSourceNote("bindings")}</div>
        </div>
        <div className="ifr-inline-actions">
          <button type="button" className="ghost-button" onClick={onProbeRequested} disabled={probeBusy}>
            {probeBusy ? BINDING_PROBE_ACTION_LOADING_LABEL : BINDING_PROBE_ACTION_LABEL}
          </button>
        </div>
      </div>
      {accountsError ? <div className="status-chip danger">{accountsError}</div> : null}
      {runtimeError ? <div className="status-chip warning">{runtimeError}</div> : null}
      {runtimeConfigError ? <div className="status-chip warning">{runtimeConfigError}</div> : null}
      <div className="ifr-summary-grid">
        <span className="status-chip ok">账户 {accounts.length}</span>
        <span className="status-chip">启用 {activeCount}</span>
        <span className={`status-chip ${defaultCount > 0 ? "ok" : "warning"}`}>默认账户 {defaultCount}</span>
        <span className={`status-chip ${localPathBoundCount > 0 ? "ok" : "warning"}`}>本地路径绑定 {localPathBoundCount}</span>
        <span className={`status-chip ${runtimeConfigReady ? "ok" : "warning"}`}>运行主配置 {runtimeConfigReady ? "已加载" : "未配置"}</span>
        <span className={`status-chip ${runtimeDefaultAccounts.length > 0 ? "ok" : "warning"}`}>运行默认命中 {runtimeDefaultAccounts.length}</span>
        <span className={`status-chip ${confirmedDraftCount > 0 ? "ok" : "warning"}`}>正式已确认 {confirmedDraftCount}</span>
        <span className={`status-chip ${proposedDraftCount > 0 ? "warning" : "ok"}`}>正式待确认 {proposedDraftCount}</span>
        <span className={`status-chip ${reviewDraftCount > 0 ? "danger" : "ok"}`}>正式需复核 {reviewDraftCount}</span>
        <span className={`status-chip ${accountDiagnostics.matchedAccountCount > 0 ? "ok" : "warning"}`}>
          命中本地候选 {accountDiagnostics.matchedAccountCount}
        </span>
        <span className={`status-chip ${accountDiagnostics.missingPaths.length === 0 ? "ok" : "warning"}`}>
          缺失路径 {accountDiagnostics.missingPaths.length}
        </span>
        <span className={`status-chip ${accountDiagnostics.unmatchedUserdata.length === 0 ? "ok" : "warning"}`}>
          未命中候选 {accountDiagnostics.unmatchedUserdata.length}
        </span>
        <span className={`status-chip ${accountDiagnostics.sharedUserdataGroups.length === 0 ? "ok" : "warning"}`}>
          路径冲突组 {accountDiagnostics.sharedUserdataGroups.length}
        </span>
        <span className={`status-chip ${accountDiagnostics.candidateReviewNeededCount > 0 ? "warning" : "ok"}`}>
          待人工核对 {accountDiagnostics.candidateReviewNeededCount}
        </span>
      </div>
      <QmtRuntimeConfigCard
        runtimeConfig={runtimeConfig}
        runtimeConfigReady={runtimeConfigReady}
        runtimeDefaultAccounts={runtimeDefaultAccounts}
        onConfigureRequested={onConfigureRequested}
      />
      <AccountBindingClosureCard
        accounts={accounts}
        observations={accountObservations}
        qmtCandidateCount={qmtCandidates.length}
      />
      {formalBindingDrafts.length > 0 ? (
        <div className="ifr-binding-draft-section">
          <div className="ifr-section-header">
            <div>
              <div className="wbd-data-section-title">正式审阅结果</div>
              <div className="ifr-section-note">以下卡片来自 `account-bindings` 正式审阅项，负责定义状态、置信度与写回资格。</div>
            </div>
          </div>
          <div className="ifr-binding-draft-list">
            {formalBindingDrafts.map((draft) => {
              const account = accounts.find((item) => item.id === draft.accountId);
              return account ? (
                <BindingDraftReviewCard
                  key={`formal-draft-${account.id}`}
                  account={account}
                  draft={draft}
                  onApplied={handleApplied}
                />
              ) : null;
            })}
          </div>
        </div>
      ) : null}
      {observationCards.length > 0 ? (
        <div className="ifr-binding-draft-section">
          <div className="ifr-section-header">
            <div>
              <div className="wbd-data-section-title">本地观测卡片</div>
              <div className="ifr-section-note">以下卡片只展示本地 discovery 看到的路径线索与当前账户配置，不再把 observation 包装成 fallback draft。</div>
            </div>
          </div>
          <div className="ifr-binding-draft-list">
            {observationCards.map((observationCard) => {
              const account = accounts.find((item) => item.id === observationCard.accountId);
              return account ? (
                <BindingObservationCard
                  key={`observation-card-${account.id}`}
                  account={account}
                  observationCard={observationCard}
                />
              ) : null;
            })}
          </div>
        </div>
      ) : null}
      {formalBindingDrafts.length === 0 && observationCards.length === 0 ? (
        <div className="ifr-binding-draft-list">
          <div className="wbd-data-card-empty">当前没有可展示的 binding 审阅结果或本地观测占位。</div>
        </div>
      ) : null}
      <div className="ifr-diagnostic-list">
        {accountDiagnostics.missingPaths.map((account) => (
          <div key={`missing-${account.id}`} className="status-chip warning">
            缺失本地路径：{account.label || account.trade_account}
          </div>
        ))}
        {accountDiagnostics.unmatchedUserdata.map((account) => (
          <div key={`unmatched-${account.id}`} className="status-chip warning">
            未命中 QMT 候选：{account.label || account.trade_account} → {account.qmt_userdata_path}
          </div>
        ))}
        {accountDiagnostics.sharedUserdataGroups.map((group, index) => (
          <div key={`shared-${index}`} className="status-chip danger">
            共享 userdata：{group.map((item) => item.label || item.trade_account).join(" / ")}
          </div>
        ))}
        {accounts.length > 0
          && accountDiagnostics.missingPaths.length === 0
          && accountDiagnostics.unmatchedUserdata.length === 0
          && accountDiagnostics.sharedUserdataGroups.length === 0 ? (
            <div className="status-chip ok">当前账户路径绑定未发现明显冲突。</div>
          ) : null}
      </div>
      {accounts.length > 0 ? (
        <div className="wbd-account-list ifr-account-list">
          {accounts.map((account) => {
            const observation = accountObservations.find((item) => item.accountId === account.id);
            const isRuntimeDefault = runtimeConfigMatchesAccount(runtimeConfig, account);
            const canSetRuntimeDefault = Boolean(account.qmt_exe_path || account.qmt_userdata_path);

            return (
              <div key={account.id} className="wbd-account-card">
                {observation ? (
                  <div className="ifr-account-recommendation">
                    <span className={`status-chip ${mapObservationTone(observation.signal)}`}>
                      {observation.note}
                    </span>
                    {observation.candidatePath ? (
                      <span className="mono-text ifr-account-recommendation-path">{observation.candidatePath}</span>
                    ) : null}
                  </div>
                ) : null}
                <div className="ifr-inline-actions">
                  {isRuntimeDefault ? (
                    <span className="status-chip ok">运行默认</span>
                  ) : canSetRuntimeDefault ? (
                    <button
                      type="button"
                      className="ghost-button"
                      disabled={Boolean(runtimeSyncAccountId)}
                      onClick={() => void handleSetRuntimeDefault(account.id)}
                    >
                      {runtimeSyncAccountId === account.id ? "设为默认中…" : "设为运行默认"}
                    </button>
                  ) : (
                    <span className="status-chip warning">缺少 QMT 路径</span>
                  )}
                </div>
                <div className="wbd-account-header">
                  <span className="wbd-account-id">{getBrokerAccountDisplayLabel(account)}</span>
                  <span className={`wbd-account-badge wbd-account-badge--${account.is_active ? "ok" : "neutral"}`}>
                    {account.is_active ? "启用" : "禁用"}
                  </span>
                  {account.is_default ? (
                    <span className="wbd-account-badge wbd-account-badge--ok">默认</span>
                  ) : null}
                </div>
                <div className="wbd-account-meta">
                  <span className="wbd-account-tag">broker: {account.broker}</span>
                  <span className="wbd-account-tag">资金账号: {account.trade_account}</span>
                  <span className="wbd-account-tag">类型: {account.account_types.join(", ") || "—"}</span>
                </div>
                <div className="ifr-meta-list">
                  <div className="ifr-meta-row">
                    <span className="ifr-meta-label">userdata</span>
                    <span className="mono-text">{account.qmt_userdata_path || "—"}</span>
                  </div>
                  <div className="ifr-meta-row">
                    <span className="ifr-meta-label">exe</span>
                    <span className="mono-text">{account.qmt_exe_path || "—"}</span>
                  </div>
                  {account.notes ? (
                    <div className="ifr-meta-row">
                      <span className="ifr-meta-label">备注</span>
                      <span>{account.notes}</span>
                    </div>
                  ) : null}
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <div className="wbd-data-card-empty">当前没有已绑定的券商账户，本地路径与资金账号还未形成闭环。</div>
      )}
    </section>
  );
}