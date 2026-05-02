"""
QMT 本地编排内核的合同对象。

本模块先冻结 v0.1 的读模型、状态枚举和审计快照结构，
供 P0/P0.5 逐步接入 discovery/config/api/frontend 主链使用。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


SCHEMA_VERSION = 1
API_CONTRACT_VERSION = 1
ROUTE_POLICY_VERSION = 1
EVENT_CONTRACT_VERSION = 1
UI_CONTRACT_VERSION = 1


def utc_now_iso() -> str:
    """返回 UTC ISO8601 时间戳。"""
    return datetime.now(timezone.utc).isoformat()


class StrEnum(str, Enum):
    """兼容较老 Python 版本的字符串枚举。"""


class LayoutStatus(StrEnum):
    DISCOVERED = "discovered"
    NORMALIZED = "normalized"
    VALIDATED = "validated"
    REJECTED = "rejected"
    STALE = "stale"


class AssetStatus(StrEnum):
    DETECTED = "detected"
    PROFILED = "profiled"
    SCORED = "scored"
    APPROVED = "approved"
    DEGRADED = "degraded"
    QUARANTINED = "quarantined"
    STALE = "stale"


class ProbeStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    STALE = "stale"


class BindingStatus(StrEnum):
    DRAFT = "draft"
    PROPOSED = "proposed"
    CONFIRMED = "confirmed"
    CONFLICTED = "conflicted"
    DISABLED = "disabled"
    REJECTED = "rejected"


class SessionStatus(StrEnum):
    LAUNCH_READY = "launch_ready"
    LAUNCHING = "launching"
    PROCESS_ALIVE = "process_alive"
    LOGIN_PENDING = "login_pending"
    CONNECTED = "connected"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DISCONNECTED = "disconnected"
    RETRYING = "retrying"
    QUARANTINED = "quarantined"
    STALE = "stale"


class PolicyStatus(StrEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    STICKY = "sticky"
    DISABLED = "disabled"
    SUPERSEDED = "superseded"


class FreshnessState(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"


class PolicyKind(StrEnum):
    HISTORY = "history"
    TRADE = "trade"


class RoutePurpose(StrEnum):
    CHART = "chart"
    HISTORY = "history"
    BACKFILL = "backfill"
    BACKTEST = "backtest"
    TRADE = "trade"
    AUDIT = "audit"
    REPLAY = "replay"


class IntentSource(StrEnum):
    MANUAL = "manual"
    STICKY_POLICY = "sticky_policy"
    AUTO_POLICY = "auto_policy"
    HEURISTIC_SUGGESTION = "heuristic_suggestion"


class BindingScope(StrEnum):
    TRADE_DEFAULT = "trade_default"
    HISTORY_DEFAULT = "history_default"
    BACKUP = "backup"
    MANUAL_ONLY = "manual_only"


class ConflictSeverity(StrEnum):
    WARNING = "warning"
    BLOCKING = "blocking"
    MANUAL_REVIEW_REQUIRED = "manual_review_required"
    ALLOW_WITH_AUDIT = "allow_with_audit"


class ConflictCode(StrEnum):
    ACCOUNT_MULTI_USERDATA = "ACCOUNT_MULTI_USERDATA"
    USERDATA_MULTI_BROKER = "USERDATA_MULTI_BROKER"
    ASSET_MULTI_PRIMARY_HISTORY = "ASSET_MULTI_PRIMARY_HISTORY"
    CHANNEL_MULTI_PRIMARY_TRADE = "CHANNEL_MULTI_PRIMARY_TRADE"
    LAYOUT_PATH_INCONSISTENT = "LAYOUT_PATH_INCONSISTENT"
    PROBE_MULTI_BINDING = "PROBE_MULTI_BINDING"
    SESSION_STALE_BUT_POLICY_ACTIVE = "SESSION_STALE_BUT_POLICY_ACTIVE"
    PRIMARY_ROUTE_DEGRADED = "PRIMARY_ROUTE_DEGRADED"
    AUTO_BIND_LOW_CONFIDENCE = "AUTO_BIND_LOW_CONFIDENCE"
    ASSET_SCORE_UNTRUSTED = "ASSET_SCORE_UNTRUSTED"
    MANUAL_OVERRIDE_CONFLICT = "MANUAL_OVERRIDE_CONFLICT"
    ROUTE_SWITCH_THROTTLED = "ROUTE_SWITCH_THROTTLED"


class EventRecordMode(StrEnum):
    RECORD_ONLY = "record_only"
    PROJECT = "project"
    GUARDED_SIDE_EFFECT = "guarded_side_effect"


@dataclass(slots=True)
class ContractEnvelope:
    """所有读模型共享的包头字段。"""

    schema_version: int = SCHEMA_VERSION
    entity_version: int = 1
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    source_refs: list[str] = field(default_factory=list)
    evidence_ts: str = field(default_factory=utc_now_iso)
    collector_version: str = "v0.1"
    confidence_score: float = 0.0
    status: str = ""
    notes: str = ""


@dataclass(slots=True)
class RuntimeEnvelope(ContractEnvelope):
    """运行态对象额外要求 observed/expires/stale 字段。"""

    observed_at: str = field(default_factory=utc_now_iso)
    expires_at: str = ""
    stale_after_seconds: int = 0
    freshness_state: FreshnessState = FreshnessState.UNKNOWN


@dataclass(slots=True)
class QmtLayoutRecord(ContractEnvelope):
    layout_id: str = ""
    fingerprint: str = ""
    install_root: str = ""
    exe_path: str = ""
    bin_path: str = ""
    python_roots: list[str] = field(default_factory=list)
    xtquant_roots: list[str] = field(default_factory=list)
    userdata_roots: list[str] = field(default_factory=list)
    datadir_paths: list[str] = field(default_factory=list)
    data_paths: list[str] = field(default_factory=list)
    datas_paths: list[str] = field(default_factory=list)
    cfg_paths: list[str] = field(default_factory=list)
    log_paths: list[str] = field(default_factory=list)
    broker_guess: str = ""
    channel_hints: list[str] = field(default_factory=list)
    version_guess: str = ""
    launchable: bool = False
    running_processes: list[str] = field(default_factory=list)
    discovered_from: list[str] = field(default_factory=list)
    scan_root: str = ""
    raw_hints: dict[str, Any] = field(default_factory=dict)
    source_process: str = ""
    observed_at: str = field(default_factory=utc_now_iso)
    expires_at: str = ""
    rejected_reason: str = ""
    status: str = LayoutStatus.DISCOVERED.value


@dataclass(slots=True)
class QmtLocalAssetRecord(RuntimeEnvelope):
    asset_id: str = ""
    layout_id: str = ""
    userdata_path: str = ""
    datadir_path: str = ""
    data_path: str = ""
    datas_path: str = ""
    cfg_path: str = ""
    market_coverage: list[str] = field(default_factory=list)
    period_coverage: list[str] = field(default_factory=list)
    instrument_family_coverage: list[str] = field(default_factory=list)
    latest_market_day: str = ""
    latest_modified_at: str = ""
    readable_sample_rate: float = 0.0
    continuity_gap_rate: float = 0.0
    parse_failure_rate: float = 0.0
    integrity_flags: list[str] = field(default_factory=list)
    tick_score: float = 0.0
    m1_score: float = 0.0
    m5_score: float = 0.0
    d1_score: float = 0.0
    stability_score: float = 0.0
    read_speed_ms_p50: float = 0.0
    read_speed_ms_p95: float = 0.0
    discovered_from: list[str] = field(default_factory=list)
    scan_root: str = ""
    sample_files: list[str] = field(default_factory=list)
    profile_method: str = ""
    quarantined_reason: str = ""
    status: str = AssetStatus.DETECTED.value


@dataclass(slots=True)
class QmtAccountProbeRecord(RuntimeEnvelope):
    probe_id: str = ""
    layout_id: str = ""
    channel_id: str = ""
    userdata_path: str = ""
    broker_id: str = ""
    account_id: str = ""
    account_type: str = ""
    login_status: str = ""
    reachable: bool = False
    probe_method: str = ""
    probe_latency_ms: int = 0
    raw_account_infos: dict[str, Any] = field(default_factory=dict)
    probe_success: bool = False
    probe_error_code: str = ""
    probe_error_message: str = ""
    discovered_from: list[str] = field(default_factory=list)
    source_process: str = ""
    status: str = ProbeStatus.PENDING.value


@dataclass(slots=True)
class AccountBindingRecord(ContractEnvelope):
    binding_id: str = ""
    broker_account_id: str = ""
    probe_id: str = ""
    channel_id: str = ""
    asset_id: str = ""
    session_anchor_key: str = ""
    binding_scope: BindingScope = BindingScope.MANUAL_ONLY
    priority: int = 0
    manual_override: bool = False
    sticky_until: str = ""
    intent_source: IntentSource = IntentSource.HEURISTIC_SUGGESTION
    change_reason: str = ""
    updated_by: str = ""
    disabled_reason: str = ""
    conflict_flags: list[str] = field(default_factory=list)
    approval_required: bool = False
    approval_state: str = ""
    status: str = BindingStatus.DRAFT.value


@dataclass(slots=True)
class ChannelProfile:
    channel_id: str = ""
    broker_id: str = ""
    broker_guess: str = ""
    channel_kind: str = ""
    cfg_fingerprint: str = ""
    supported_account_types: list[str] = field(default_factory=list)
    server_endpoint_hint: str = ""
    login_entry_hint: str = ""


@dataclass(slots=True)
class GatewaySessionRecord(RuntimeEnvelope):
    session_id: str = ""
    session_anchor_key: str = ""
    layout_id: str = ""
    userdata_path: str = ""
    connected_accounts: list[str] = field(default_factory=list)
    current_route_claims: list[str] = field(default_factory=list)
    channel_profile: ChannelProfile = field(default_factory=ChannelProfile)
    process_status: str = ""
    login_status: str = ""
    session_health: str = ""
    connected: bool = False
    authenticated: bool = False
    latency_ms_p50: float = 0.0
    latency_ms_p95: float = 0.0
    last_heartbeat_at: str = ""
    last_success_at: str = ""
    last_error: str = ""
    retry_count: int = 0
    quarantine_state: str = ""
    quarantine_reason: str = ""
    quarantine_entered_at: str = ""
    quarantine_release_policy: str = ""
    status: str = SessionStatus.STALE.value


@dataclass(slots=True)
class RoutePolicyRecord(ContractEnvelope):
    policy_id: str = ""
    policy_kind: PolicyKind = PolicyKind.HISTORY
    purpose: RoutePurpose = RoutePurpose.HISTORY
    market: str = ""
    period: str = ""
    instrument_family: str = ""
    account_id: str = ""
    preferred_candidates: list[str] = field(default_factory=list)
    fallback_candidates: list[str] = field(default_factory=list)
    score_formula_version: str = ""
    quality_thresholds: dict[str, Any] = field(default_factory=dict)
    switch_policy: dict[str, Any] = field(default_factory=dict)
    min_hold_seconds: int = 0
    switch_cooldown_seconds: int = 0
    max_switch_per_day: int = 0
    route_freeze_until: str = ""
    manual_override: bool = False
    sticky_until: str = ""
    intent_priority: int = 0
    updated_by: str = ""
    change_reason: str = ""
    effective_from: str = ""
    effective_to: str = ""
    status: str = PolicyStatus.DRAFT.value


@dataclass(slots=True)
class RouteDecisionSnapshot:
    snapshot_id: str = ""
    policy_id: str = ""
    policy_version: int = 1
    route_policy_version: int = ROUTE_POLICY_VERSION
    algorithm_version: str = "v0.1"
    evaluated_at: str = field(default_factory=utc_now_iso)
    triggered_by_event: str = ""
    purpose: str = ""
    market: str = ""
    period: str = ""
    instrument_family: str = ""
    account_id: str = ""
    candidate_ids: list[str] = field(default_factory=list)
    runtime_snapshot_refs: list[str] = field(default_factory=list)
    asset_snapshot_refs: list[str] = field(default_factory=list)
    binding_snapshot_refs: list[str] = field(default_factory=list)
    manual_override_active: bool = False
    sticky_until: str = ""
    freeze_window: dict[str, Any] = field(default_factory=dict)
    winner: str = ""
    runner_up: str = ""
    score_breakdown: dict[str, Any] = field(default_factory=dict)
    rejection_reasons: list[str] = field(default_factory=list)
    decision_reason: str = ""
    min_hold_seconds: int = 0
    switch_cooldown_seconds: int = 0
    max_switch_per_day: int = 0
    route_freeze_until: str = ""
    effective_from: str = ""
    effective_to: str = ""
    stale_at: str = ""


@dataclass(slots=True)
class ConflictRecord:
    code: ConflictCode
    severity: ConflictSeverity
    message: str
    target_kind: str
    target_id: str
    observed_at: str = field(default_factory=utc_now_iso)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AuditEvent:
    event_id: str = ""
    event_type: str = ""
    event_contract_version: int = EVENT_CONTRACT_VERSION
    occurred_at: str = field(default_factory=utc_now_iso)
    actor_type: str = ""
    actor_id: str = ""
    operation_id: str = ""
    correlation_id: str = ""
    target_kind: str = ""
    target_id: str = ""
    record_mode: EventRecordMode = EventRecordMode.RECORD_ONLY
    payload: dict[str, Any] = field(default_factory=dict)
    snapshot_refs: list[str] = field(default_factory=list)
