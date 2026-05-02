import type {
  AccountBindingItemPayload,
  QmtConflictItemPayload,
  QmtProbeItemPayload,
  QmtRouteDecisionItemPayload,
  QmtSessionItemPayload,
} from "./api";

export interface AccountBindingObservationViewModel {
  accountId: string;
  status: "matched" | "suggested" | "missing";
  message: string;
  candidatePath: string | null;
}

export interface AccountBindingDraftViewModel {
  bindingId: string | null;
  accountId: string;
  source: "formal" | "placeholder";
  canWriteback: boolean;
  state: "confirmed" | "proposed" | "review_required" | "missing";
  confidence: number;
  applyPath: string | null;
  reasons: string[];
  observation: AccountBindingObservationViewModel;
  session: QmtSessionItemPayload | null;
  probe: QmtProbeItemPayload | null;
  route: QmtRouteDecisionItemPayload | null;
  conflicts: QmtConflictItemPayload[];
}

export const BINDING_PROBE_ACTION_LABEL = "联动 Probe";
export const BINDING_PROBE_ACTION_LOADING_LABEL = "联动 Probe 中…";

export function getBindingFormalSourceNote(target: "overview" | "bindings"): string {
  if (target === "bindings") {
    return "把 trade account、本地路径、session、probe 与冲突摘要收束成可审阅的 binding draft；点击任一“联动 Probe”都会同步刷新本区 explain。";
  }
  return "正式审阅结果来自 `account-bindings`；点击“联动 Probe”会同时刷新正式 explain 与下方原始 Probe 观测。";
}

export function getBindingProbeEmptyStateNote(target: "overview" | "qmt_registry"): string {
  if (target === "qmt_registry") {
    return "Probe 默认不自动执行；需要时点击“联动 Probe”同步原始观测与 binding explain。";
  }
  return "Probe 默认不自动执行；需要时点击“联动 Probe”同步正式 explain 与原始 Probe 观测。";
}

export function getBindingApplySuccessNote(target: "overview" | "bindings"): string {
  if (target === "bindings") {
    return "配置已写回，正式审阅结果已刷新；运行态 `session / probe / route` 仍可能是旧快照，需手动刷新 explain。";
  }
  return "配置已写回，正式审阅结果已刷新；系统页下方 Probe/冲突卡片仍可能是旧运行态快照，可再触发“联动 Probe”同步。";
}

export function mapBindingItemToDraft(item: AccountBindingItemPayload): AccountBindingDraftViewModel {
  let state: AccountBindingDraftViewModel["state"] = "missing";
  switch (item.status) {
    case "confirmed":
      state = "confirmed";
      break;
    case "proposed":
      state = "proposed";
      break;
    case "conflicted":
      state = "review_required";
      break;
    default:
      state = item.apply_path ? "proposed" : "missing";
      break;
  }

  return {
    bindingId: item.binding_id,
    accountId: item.broker_account_id,
    source: "formal",
    canWriteback: item.approval_state !== "review_required" && Boolean(item.apply_path),
    state,
    confidence: Math.round((item.confidence_score ?? 0) * 100),
    applyPath: item.apply_path,
    reasons: item.reasons ?? [],
    observation: {
      accountId: item.broker_account_id,
      status: item.recommendation_status,
      message: item.recommendation_message,
      candidatePath: item.candidate_path,
    },
    session: item.session,
    probe: item.probe,
    route: item.route,
    conflicts: item.conflicts ?? [],
  };
}

export function getBindingDraftTone(
  state: AccountBindingDraftViewModel["state"],
): "ok" | "warning" | "danger" {
  switch (state) {
    case "confirmed":
      return "ok";
    case "review_required":
      return "danger";
    case "proposed":
    case "missing":
    default:
      return "warning";
  }
}

export function getBindingDraftLabel(state: AccountBindingDraftViewModel["state"]): string {
  switch (state) {
    case "confirmed":
      return "已确认";
    case "proposed":
      return "待确认";
    case "review_required":
      return "需复核";
    case "missing":
    default:
      return "缺绑定";
  }
}
