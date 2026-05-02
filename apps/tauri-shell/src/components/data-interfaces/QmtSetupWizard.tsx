import { useCallback, useState } from "react";
import {
  applyAccountBinding,
  applyQmtRuntimeConfigFromAccount,
  discoverAccountBindings,
  fetchQmtLocalScan,
  fetchQmtProbes,
  fetchQmtRouteDecisions,
  type AccountBindingItemPayload,
  type BrokerAccountPayload,
  type QmtLocalCandidate,
  type QmtProbeItemPayload,
  type QmtRouteDecisionItemPayload,
} from "../../lib/api";
import { getBrokerAccountDisplayLabel, pathsMostlyMatch } from "../../lib/qmtPathUtils";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface WizardCandidate {
  candidate: QmtLocalCandidate;
  matchedAccount: BrokerAccountPayload | null;
  matchReason: string;
  bindingDraft?: AccountBindingItemPayload | null;
}

type WizardStep = "scan" | "match" | "probe" | "apply";

interface QmtSetupWizardProps {
  accounts: BrokerAccountPayload[];
  onDone: () => void;
  onCancel: () => void;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function candidateSummaryLabel(c: QmtLocalCandidate): string {
  const base = c.install_path ?? c.userdata_path;
  const parts: string[] = [];
  if (c.has_downloaded_history) parts.push("有历史行情");
  if (c.dat_file_count_hint > 0) parts.push(`${c.dat_file_count_hint} 个数据文件`);
  return `${base}${parts.length ? "（" + parts.join("、") + "）" : ""}`;
}

function matchCandidateToAccount(
  candidate: QmtLocalCandidate,
  accounts: BrokerAccountPayload[],
): { account: BrokerAccountPayload | null; reason: string } {
  for (const acc of accounts) {
    if (acc.qmt_userdata_path && pathsMostlyMatch(candidate.userdata_path, acc.qmt_userdata_path)) {
      return { account: acc, reason: "userdata 路径匹配" };
    }
    if (acc.qmt_exe_path && candidate.install_path && pathsMostlyMatch(candidate.install_path, acc.qmt_exe_path)) {
      return { account: acc, reason: "安装路径匹配" };
    }
  }
  return { account: null, reason: "无路径匹配" };
}

function bindingMatchesCandidate(
  binding: AccountBindingItemPayload,
  selected: WizardCandidate,
): boolean {
  const candidate = selected.candidate;
  return pathsMostlyMatch(binding.apply_path, candidate.userdata_path)
    || pathsMostlyMatch(binding.candidate_path, candidate.userdata_path)
    || pathsMostlyMatch(binding.configured_userdata_path, candidate.userdata_path)
    || pathsMostlyMatch(binding.configured_exe_path, candidate.install_path);
}

function findBindingDraftForSelection(
  selected: WizardCandidate,
  items: AccountBindingItemPayload[],
): AccountBindingItemPayload | null {
  const accountId = selected.matchedAccount?.id;
  if (!accountId) {
    return null;
  }
  const sameAccount = items.filter((item) => item.broker_account_id === accountId);
  return sameAccount.find((item) => bindingMatchesCandidate(item, selected)) ?? null;
}

// ---------------------------------------------------------------------------
// Step components
// ---------------------------------------------------------------------------

interface StepScanProps {
  onNext: (candidates: WizardCandidate[]) => void;
  accounts: BrokerAccountPayload[];
}

function StepScan({ onNext, accounts }: StepScanProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchRoots, setSearchRoots] = useState("");

  const handleScan = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await fetchQmtLocalScan({
        searchRoots: searchRoots.trim() || undefined,
        force: true,
      });
      const matched: WizardCandidate[] = result.candidates.map((c) => {
        const { account, reason } = matchCandidateToAccount(c, accounts);
        return { candidate: c, matchedAccount: account, matchReason: reason };
      });
      onNext(matched);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [searchRoots, accounts, onNext]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <p style={{ color: "#94a3b8", margin: 0, fontSize: 13 }}>
        扫描本机已安装的 QMT（miniQMT / XtQuant）路径，自动发现可用的数据目录。
      </p>
      <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        <span style={{ color: "#94a3b8", fontSize: 12 }}>自定义搜索根目录（可选，多个路径用分号分隔）</span>
        <input
          type="text"
          value={searchRoots}
          onChange={(e) => setSearchRoots(e.target.value)}
          placeholder="例：C:\\Program Files\\国金证券QMT交易端;D:\\qmt"
          style={{
            background: "#1e293b",
            border: "1px solid #334155",
            borderRadius: 4,
            color: "#e2e8f0",
            fontSize: 12,
            padding: "6px 10px",
          }}
        />
      </label>
      {error && (
        <div style={{ color: "#f87171", fontSize: 12, background: "#1e293b", padding: "6px 10px", borderRadius: 4 }}>
          {error}
        </div>
      )}
      <button
        onClick={handleScan}
        disabled={loading}
        style={{
          alignSelf: "flex-start",
          background: loading ? "#1e293b" : "#2563eb",
          border: "none",
          borderRadius: 4,
          color: "#e2e8f0",
          cursor: loading ? "not-allowed" : "pointer",
          fontSize: 13,
          padding: "6px 16px",
        }}
      >
        {loading ? "扫描中…" : "开始扫描"}
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------

interface StepMatchProps {
  candidates: WizardCandidate[];
  accounts: BrokerAccountPayload[];
  onNext: (selected: WizardCandidate) => void;
  onBack: () => void;
}

function StepMatch({ candidates, accounts, onNext, onBack }: StepMatchProps) {
  const [selected, setSelected] = useState<number | null>(() => {
    const idx = candidates.findIndex((c) => c.matchedAccount !== null);
    return idx >= 0 ? idx : candidates.length > 0 ? 0 : null;
  });

  const [overrideAccountId, setOverrideAccountId] = useState<string>("");

  const handleNext = useCallback(() => {
    if (selected === null) return;
    const base = candidates[selected];
    const override = accounts.find((a) => a.id === overrideAccountId);
    onNext(override ? { ...base, matchedAccount: override, matchReason: "手动选择" } : base);
  }, [selected, overrideAccountId, candidates, accounts, onNext]);

  if (candidates.length === 0) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <div style={{ color: "#f87171", fontSize: 13 }}>
          未找到 QMT 安装路径。请确认 QMT 已安装，或在上一步输入自定义搜索目录。
        </div>
        <button onClick={onBack} style={{ alignSelf: "flex-start", background: "#1e293b", border: "1px solid #334155", borderRadius: 4, color: "#94a3b8", cursor: "pointer", fontSize: 13, padding: "6px 16px" }}>
          返回
        </button>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <p style={{ color: "#94a3b8", margin: 0, fontSize: 13 }}>
        发现 {candidates.length} 个候选安装。选择要绑定的 QMT 路径，并确认关联的券商账号。
      </p>
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {candidates.map((c, i) => (
          <label
            key={i}
            style={{
              display: "flex",
              gap: 10,
              alignItems: "flex-start",
              background: selected === i ? "#1e3a5f" : "#1e293b",
              border: `1px solid ${selected === i ? "#2563eb" : "#334155"}`,
              borderRadius: 6,
              cursor: "pointer",
              padding: "10px 12px",
            }}
          >
            <input
              type="radio"
              name="candidate"
              checked={selected === i}
              onChange={() => setSelected(i)}
              style={{ marginTop: 2, flexShrink: 0 }}
            />
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ color: "#e2e8f0", fontSize: 13, wordBreak: "break-all" }}>
                {candidateSummaryLabel(c.candidate)}
              </div>
              <div style={{ color: "#64748b", fontSize: 11, marginTop: 2 }}>
                {c.matchedAccount
                  ? `已匹配账号：${getBrokerAccountDisplayLabel(c.matchedAccount)}（${c.matchReason}）`
                  : `未自动匹配账号（${c.matchReason}）`}
              </div>
            </div>
          </label>
        ))}
      </div>

      {selected !== null && (
        <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <span style={{ color: "#94a3b8", fontSize: 12 }}>
            关联账号（留空则使用自动匹配结果）
          </span>
          <select
            value={overrideAccountId}
            onChange={(e) => setOverrideAccountId(e.target.value)}
            style={{
              background: "#1e293b",
              border: "1px solid #334155",
              borderRadius: 4,
              color: "#e2e8f0",
              fontSize: 12,
              padding: "5px 8px",
            }}
          >
            <option value="">— 使用自动匹配 —</option>
            {accounts.map((a) => (
              <option key={a.id} value={a.id}>
                {getBrokerAccountDisplayLabel(a)} ({a.id})
              </option>
            ))}
          </select>
        </label>
      )}

      <div style={{ display: "flex", gap: 8 }}>
        <button onClick={onBack} style={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 4, color: "#94a3b8", cursor: "pointer", fontSize: 13, padding: "6px 16px" }}>
          返回
        </button>
        <button
          onClick={handleNext}
          disabled={selected === null}
          style={{
            background: selected === null ? "#1e293b" : "#2563eb",
            border: "none",
            borderRadius: 4,
            color: "#e2e8f0",
            cursor: selected === null ? "not-allowed" : "pointer",
            fontSize: 13,
            padding: "6px 16px",
          }}
        >
          下一步：Probe 验证
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------

interface StepProbeProps {
  selected: WizardCandidate;
  onNext: (selected: WizardCandidate) => void;
  onBack: () => void;
}

function StepProbe({ selected, onNext, onBack }: StepProbeProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [probes, setProbes] = useState<QmtProbeItemPayload[] | null>(null);
  const [routes, setRoutes] = useState<QmtRouteDecisionItemPayload[] | null>(null);
  const [bindingDraft, setBindingDraft] = useState<AccountBindingItemPayload | null>(selected.bindingDraft ?? null);

  const handleProbe = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const userdataPath = selected.candidate.userdata_path ?? undefined;
      const [probeResult, routeResult, bindingResult] = await Promise.all([
        fetchQmtProbes({ userdataPath }),
        fetchQmtRouteDecisions({ force: true }),
        discoverAccountBindings({ includeProbes: true, force: true }),
      ]);
      setProbes(probeResult.items);
      setRoutes(routeResult.items);
      setBindingDraft(findBindingDraftForSelection(selected, bindingResult.items));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [selected]);

  const accountLabel = selected.matchedAccount
    ? getBrokerAccountDisplayLabel(selected.matchedAccount)
    : "（无关联账号）";

  const successCount = probes?.filter((p) => p.probe_success).length ?? 0;
  const totalCount = probes?.length ?? 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <p style={{ color: "#94a3b8", margin: 0, fontSize: 13 }}>
        对选定账号 <strong style={{ color: "#e2e8f0" }}>{accountLabel}</strong> 执行 Probe 连通性验证，并查看路由决策。
        此步骤可选，跳过也可直接应用配置。
      </p>

      {selected.matchedAccount === null && (
        <div style={{ color: "#facc15", fontSize: 12, background: "#1e293b", padding: "6px 10px", borderRadius: 4 }}>
          未关联券商账号，无法执行账号级 Probe。建议返回上一步选择账号后再验证。
        </div>
      )}

      {error && (
        <div style={{ color: "#f87171", fontSize: 12, background: "#1e293b", padding: "6px 10px", borderRadius: 4 }}>
          {error}
        </div>
      )}

      {probes !== null && (
        <div style={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 6, padding: "10px 12px" }}>
          <div style={{ color: "#94a3b8", fontSize: 12, marginBottom: 6 }}>
            Probe 结果：{successCount} / {totalCount} 成功
            {routes !== null && `，路由决策 ${routes.length} 条`}
          </div>
          {probes.slice(0, 5).map((p, i) => (
            <div key={i} style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 12, padding: "3px 0" }}>
              <span style={{ ...{ display: "inline-block", width: 6, height: 6, borderRadius: "50%", background: p.probe_success ? "#4ade80" : "#f87171" } as React.CSSProperties }} />
              <span style={{ color: "#e2e8f0", flex: 1, minWidth: 0, wordBreak: "break-all" }}>{p.userdata_path}</span>
              <span style={{ color: p.probe_success ? "#4ade80" : "#f87171", flexShrink: 0 }}>
                {p.probe_success ? "可达" : "不可达"}
              </span>
            </div>
          ))}
          {probes.length > 5 && (
            <div style={{ color: "#64748b", fontSize: 11, marginTop: 4 }}>… 共 {probes.length} 条</div>
          )}
        </div>
      )}

      {bindingDraft ? (
        <div style={{ color: "#94a3b8", fontSize: 12, background: "#1e293b", padding: "6px 10px", borderRadius: 4 }}>
          已命中正式绑定审阅：{bindingDraft.recommendation_status} / {bindingDraft.recommendation_message}
        </div>
      ) : null}

      <div style={{ display: "flex", gap: 8 }}>
        <button onClick={onBack} style={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 4, color: "#94a3b8", cursor: "pointer", fontSize: 13, padding: "6px 16px" }}>
          返回
        </button>
        {selected.matchedAccount !== null && (
          <button
            onClick={handleProbe}
            disabled={loading}
            style={{
              background: loading ? "#1e293b" : "#0f172a",
              border: "1px solid #334155",
              borderRadius: 4,
              color: loading ? "#64748b" : "#94a3b8",
              cursor: loading ? "not-allowed" : "pointer",
              fontSize: 13,
              padding: "6px 16px",
            }}
          >
            {loading ? "Probe 中…" : "执行 Probe"}
          </button>
        )}
        <button
          onClick={() => onNext({ ...selected, bindingDraft })}
          style={{
            background: "#2563eb",
            border: "none",
            borderRadius: 4,
            color: "#e2e8f0",
            cursor: "pointer",
            fontSize: 13,
            padding: "6px 16px",
          }}
        >
          下一步：应用配置
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------

interface StepApplyProps {
  selected: WizardCandidate;
  onDone: () => void;
  onBack: () => void;
}

function StepApply({ selected, onDone, onBack }: StepApplyProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [applied, setApplied] = useState(false);
  const [appliedFields, setAppliedFields] = useState<string[]>([]);
  const [bindingUpdated, setBindingUpdated] = useState<boolean | null>(null);

  const accountLabel = selected.matchedAccount
    ? getBrokerAccountDisplayLabel(selected.matchedAccount)
    : null;

  const handleApply = useCallback(async () => {
    if (!selected.matchedAccount) return;
    setLoading(true);
    setError(null);
    try {
      let draft = selected.bindingDraft ?? null;
      if (!draft) {
        const bindingResult = await discoverAccountBindings({ includeProbes: true, force: true });
        draft = findBindingDraftForSelection(selected, bindingResult.items);
      }
      if (draft?.binding_id && draft.apply_path) {
        const bindingResult = await applyAccountBinding(draft.binding_id, { includeProbes: true, force: true });
        setBindingUpdated(bindingResult.updated);
      } else {
        setBindingUpdated(false);
      }
      const result = await applyQmtRuntimeConfigFromAccount(selected.matchedAccount.id);
      setApplied(true);
      setAppliedFields(result.synced_fields);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [selected]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <p style={{ color: "#94a3b8", margin: 0, fontSize: 13 }}>
        将根据选定账号同步写入运行时 QMT 配置（qmt_path、qmt_userdata_path 等字段）。
      </p>

      <div style={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 6, padding: "10px 12px", display: "flex", flexDirection: "column", gap: 6 }}>
        <div style={{ color: "#94a3b8", fontSize: 12 }}>确认信息</div>
        <div style={{ fontSize: 12 }}>
          <span style={{ color: "#64748b" }}>候选路径：</span>
          <span style={{ color: "#e2e8f0", wordBreak: "break-all" }}>
            {selected.candidate.install_path ?? selected.candidate.userdata_path}
          </span>
        </div>
        <div style={{ fontSize: 12 }}>
          <span style={{ color: "#64748b" }}>关联账号：</span>
          <span style={{ color: accountLabel ? "#e2e8f0" : "#f87171" }}>
            {accountLabel ?? "未选择（无法应用）"}
          </span>
        </div>
      </div>

      {!selected.matchedAccount && (
        <div style={{ color: "#f87171", fontSize: 12, background: "#1e293b", padding: "6px 10px", borderRadius: 4 }}>
          未关联券商账号，无法应用配置。请返回上一步选择账号。
        </div>
      )}

      {error && (
        <div style={{ color: "#f87171", fontSize: 12, background: "#1e293b", padding: "6px 10px", borderRadius: 4 }}>
          {error}
        </div>
      )}

      {applied && (
        <div style={{ color: "#4ade80", fontSize: 12, background: "#0f2a1a", border: "1px solid #166534", padding: "8px 12px", borderRadius: 4 }}>
          ✓ 配置已应用。账户绑定：{bindingUpdated ? "已写回" : "无需写回或未命中正式审阅"}；同步字段：{appliedFields.length > 0 ? appliedFields.join("、") : "（无变更）"}
        </div>
      )}

      <div style={{ display: "flex", gap: 8 }}>
        {!applied && (
          <button onClick={onBack} style={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 4, color: "#94a3b8", cursor: "pointer", fontSize: 13, padding: "6px 16px" }}>
            返回
          </button>
        )}
        {!applied && selected.matchedAccount && (
          <button
            onClick={handleApply}
            disabled={loading}
            style={{
              background: loading ? "#1e293b" : "#16a34a",
              border: "none",
              borderRadius: 4,
              color: "#e2e8f0",
              cursor: loading ? "not-allowed" : "pointer",
              fontSize: 13,
              padding: "6px 16px",
            }}
          >
            {loading ? "应用中…" : "应用配置"}
          </button>
        )}
        <button
          onClick={onDone}
          style={{
            background: applied ? "#2563eb" : "#1e293b",
            border: applied ? "none" : "1px solid #334155",
            borderRadius: 4,
            color: "#e2e8f0",
            cursor: "pointer",
            fontSize: 13,
            padding: "6px 16px",
          }}
        >
          {applied ? "完成" : "跳过"}
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Step indicator
// ---------------------------------------------------------------------------

const STEPS: { key: WizardStep; label: string }[] = [
  { key: "scan", label: "扫描候选" },
  { key: "match", label: "匹配账号" },
  { key: "probe", label: "Probe 验证" },
  { key: "apply", label: "应用配置" },
];

function StepIndicator({ current }: { current: WizardStep }) {
  const currentIdx = STEPS.findIndex((s) => s.key === current);
  return (
    <div style={{ display: "flex", gap: 0, marginBottom: 20 }}>
      {STEPS.map((s, i) => {
        const done = i < currentIdx;
        const active = i === currentIdx;
        return (
          <div key={s.key} style={{ display: "flex", alignItems: "center", flex: i < STEPS.length - 1 ? 1 : undefined }}>
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
              <div
                style={{
                  width: 24,
                  height: 24,
                  borderRadius: "50%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontSize: 11,
                  fontWeight: 600,
                  background: done ? "#166534" : active ? "#2563eb" : "#1e293b",
                  border: `1px solid ${done ? "#16a34a" : active ? "#3b82f6" : "#334155"}`,
                  color: done ? "#4ade80" : active ? "#e2e8f0" : "#64748b",
                }}
              >
                {done ? "✓" : i + 1}
              </div>
              <span style={{ fontSize: 11, color: active ? "#e2e8f0" : "#64748b", whiteSpace: "nowrap" }}>
                {s.label}
              </span>
            </div>
            {i < STEPS.length - 1 && (
              <div style={{ flex: 1, height: 1, background: done ? "#16a34a" : "#334155", margin: "0 6px", marginBottom: 16 }} />
            )}
          </div>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main wizard
// ---------------------------------------------------------------------------

export function QmtSetupWizard({ accounts, onDone, onCancel }: QmtSetupWizardProps) {
  const [step, setStep] = useState<WizardStep>("scan");
  const [candidates, setCandidates] = useState<WizardCandidate[]>([]);
  const [selected, setSelected] = useState<WizardCandidate | null>(null);

  const handleScanDone = useCallback((result: WizardCandidate[]) => {
    setCandidates(result);
    setStep("match");
  }, []);

  const handleMatchDone = useCallback((sel: WizardCandidate) => {
    setSelected(sel);
    setStep("probe");
  }, []);

  return (
    <div
      style={{
        background: "#0f172a",
        border: "1px solid #1e3a5f",
        borderRadius: 8,
        padding: "20px 24px",
        maxWidth: 640,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <h3 style={{ color: "#e2e8f0", fontSize: 15, fontWeight: 600, margin: 0 }}>QMT 快速配置向导</h3>
        <button
          onClick={onCancel}
          style={{ background: "none", border: "none", color: "#64748b", cursor: "pointer", fontSize: 16, padding: 0, lineHeight: 1 }}
          title="关闭"
        >
          ✕
        </button>
      </div>

      <StepIndicator current={step} />

      {step === "scan" && <StepScan accounts={accounts} onNext={handleScanDone} />}
      {step === "match" && (
        <StepMatch
          candidates={candidates}
          accounts={accounts}
          onNext={handleMatchDone}
          onBack={() => setStep("scan")}
        />
      )}
      {step === "probe" && selected && (
        <StepProbe
          selected={selected}
          onNext={(nextSelected) => {
            setSelected(nextSelected);
            setStep("apply");
          }}
          onBack={() => setStep("match")}
        />
      )}
      {step === "apply" && selected && (
        <StepApply selected={selected} onDone={onDone} onBack={() => setStep("probe")} />
      )}
    </div>
  );
}
