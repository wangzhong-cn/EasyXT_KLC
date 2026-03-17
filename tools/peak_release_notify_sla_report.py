from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG_JSONL = PROJECT_ROOT / "logs" / "peak_release_notify_delivery.jsonl"
DEFAULT_OUT_MD = PROJECT_ROOT / "artifacts" / "peak_release_notify_sla_latest.md"
DEFAULT_OUT_JSON = PROJECT_ROOT / "artifacts" / "peak_release_notify_sla_latest.json"
DEFAULT_OUT_ONCALL_JSON = PROJECT_ROOT / "artifacts" / "peak_release_notify_oncall_latest.json"
DEFAULT_ACK_BATCH_REPORT_JSON = PROJECT_ROOT / "artifacts" / "peak_release_notify_ack_batch_report.json"
DEFAULT_POLICY_JSON = PROJECT_ROOT / "configs" / "peak_release_notify_oncall_policy.json"
SCHEMA_VERSION = "v1"
DEFAULT_POLICY: dict[str, Any] = {
    "checkpoint_minutes": {"high": 15, "medium": 30, "low": 60, "default": 120},
    "response_sla_minutes": {"high": 15, "medium": 30, "low": 60, "default": 0},
    "escalation_wait_minutes": {"high": 30, "medium": 60, "low": 120, "default": 0},
    "breach_level_score": {"none": 0, "medium": 50, "high": 75, "critical": 100},
    "alert_level_bonus": {"info": 0, "low": 5, "medium": 10, "high": 15},
    "dispatch_priority": {"critical": "urgent", "high": "high", "medium": "normal", "none": "low", "default": "normal"},
    "risk_policy": {
        "version": "v1",
        "weights": {
            "failure": 40,
            "skipped": 20,
            "conflict": 25,
            "validation": 15,
            "aborted": 15,
        },
        "level_thresholds": {
            "low_max_exclusive": 25,
            "medium_max_exclusive": 50,
            "high_max_exclusive": 80,
            "max_score": 100,
        },
        "alert_level_map": {"critical": "p1", "high": "p1", "medium": "p2", "low": "p3", "info": "p4"},
        "recommended_action_map": {
            "healthy": "monitor",
            "partial": "retry_failed_requests",
            "aborted": "rollback_and_manual_review",
            "failed": "manual_intervention_required",
        },
    },
    "governance_policy": {
        "version": "v1",
        "single_point_threshold": 0.8,
        "concentration_thresholds": {"very_high_min": 0.8, "high_min": 0.5, "medium_min": 0.3},
        "urgency_thresholds": {"p1_min": 0.8, "p2_min": 0.6, "p3_min": 0.3},
        "risk_level_weight": {"critical": 1.0, "high": 0.8, "medium": 0.6, "low": 0.3, "info": 0.1},
        "priority_to_eta_minutes": {"p1": 15, "p2": 60, "p3": 240, "p4": 1440, "none": 0},
        "priority_to_route": {
            "p1": "immediate_page",
            "p2": "expedite_queue",
            "p3": "planned_queue",
            "p4": "backlog_review",
            "none": "observe",
        },
        "owner_map": {
            "default": "platform_oncall",
            "idle": "none",
            "single_point": "qa_oncall",
            "release": "release_oncall",
        },
        "playbook_map": {
            "idle": "PB-IDLE-000",
            "stabilize_single_point": "PB-SP-001",
            "reduce_long_tail": "PB-LT-001",
            "balance_monitoring": "PB-BM-001",
        },
        "trend_window": 10,
        "priority_order": ["none", "p4", "p3", "p2", "p1"],
        "merge_key_fields": ["error_signature", "error_structure_tag", "error_governance_owner"],
    },
}


def _parse_ts(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _severity_from_alert(alert_level: str) -> str:
    if alert_level == "high":
        return "P1"
    if alert_level == "medium":
        return "P2"
    if alert_level == "low":
        return "P3"
    return "P4"


def _response_deadline_utc(generated_at: str, response_sla_minutes: int) -> str:
    if response_sla_minutes <= 0:
        return ""
    ts = _parse_ts(generated_at)
    if ts is None:
        return ""
    return (ts + timedelta(minutes=response_sla_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _plus_minutes_utc(ts_text: str, minutes: int) -> str:
    if not ts_text or minutes <= 0:
        return ""
    ts = _parse_ts(ts_text)
    if ts is None:
        return ""
    return (ts + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _next_checkpoint_utc(alert_level: str, generated_at: str) -> str:
    cadence = _policy_rule("checkpoint_minutes")
    return _plus_minutes_utc(generated_at, int(cadence.get(alert_level, cadence.get("default", 120))))


def _to_epoch(ts_text: str) -> float | None:
    ts = _parse_ts(ts_text)
    if ts is None:
        return None
    return ts.timestamp()


def _overdue_flags(*, evaluation_utc: str, response_deadline_utc: str, escalation_deadline_utc: str) -> dict[str, Any]:
    now_ts = _to_epoch(evaluation_utc)
    resp_ts = _to_epoch(response_deadline_utc)
    esc_ts = _to_epoch(escalation_deadline_utc)
    response_overdue = bool(now_ts is not None and resp_ts is not None and now_ts > resp_ts)
    escalation_overdue = bool(now_ts is not None and esc_ts is not None and now_ts > esc_ts)
    if escalation_overdue:
        overdue_stage = "escalation"
    elif response_overdue:
        overdue_stage = "response"
    else:
        overdue_stage = "none"
    return {
        "response_overdue": response_overdue,
        "escalation_overdue": escalation_overdue,
        "overdue_stage": overdue_stage,
    }


def _breach_profile(*, overdue_stage: str, alert_level: str) -> dict[str, str]:
    if overdue_stage == "escalation":
        if alert_level in ("high", "medium"):
            return {
                "sla_breach_level": "critical",
                "immediate_action": "立即升级到主管与责任团队负责人，并执行人工接管。",
            }
        return {"sla_breach_level": "high", "immediate_action": "立即执行升级路径并在30分钟内回报处置进展。"}
    if overdue_stage == "response":
        return {"sla_breach_level": "medium", "immediate_action": "立即完成首轮缓解并同步值班频道更新进展。"}
    return {"sla_breach_level": "none", "immediate_action": "按既定SLA节奏持续跟进。"}


def _breach_score(sla_breach_level: str, alert_level: str) -> int:
    level_score = _policy_rule("breach_level_score")
    alert_bonus = _policy_rule("alert_level_bonus")
    return int(level_score.get(sla_breach_level, 0) + alert_bonus.get(alert_level, 0))


def _escalation_required(sla_breach_level: str) -> bool:
    return sla_breach_level in ("high", "critical")


def _execution_command_template(*, incident_key: str, owner: str, sla_breach_level: str) -> str:
    return f"oncall-escalate --incident {incident_key} --owner \"{owner}\" --level {sla_breach_level}"


def _page_required(sla_breach_level: str, escalation_required: bool) -> bool:
    return bool(escalation_required or sla_breach_level == "critical")


def _dispatch_priority(sla_breach_level: str) -> str:
    mapping = _policy_rule("dispatch_priority")
    return str(mapping.get(sla_breach_level, mapping.get("default", "normal")))


def _breach_summary(*, sla_breach_level: str, overdue_stage: str, immediate_action: str) -> str:
    return f"breach={sla_breach_level}; stage={overdue_stage}; action={immediate_action}"


def _ack_state_template(generated_at: str) -> dict[str, Any]:
    return {
        "ack_status": "pending",
        "ack_revision": 0,
        "ack_at_utc": "",
        "mitigated_at_utc": "",
        "escalated_at_utc": "",
        "closed_at_utc": "",
        "owner_note": "",
        "history": [],
        "last_updated_utc": generated_at,
    }


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _update_ack_state(
    event: dict[str, Any],
    *,
    ack_status: str,
    owner_note: str,
    event_time_utc: str,
) -> dict[str, Any]:
    out = copy.deepcopy(event)
    ack = out.get("ack_state", {})
    if not isinstance(ack, dict):
        ack = _ack_state_template(event_time_utc)
    prev_status = str(ack.get("ack_status", "pending") or "pending")
    prev_revision = int(ack.get("ack_revision", 0) or 0)
    ack["ack_status"] = ack_status
    ack["ack_revision"] = prev_revision + 1
    if owner_note:
        ack["owner_note"] = owner_note
    ts = event_time_utc or _utc_now()
    if ack_status == "acked":
        ack["ack_at_utc"] = ts
        out["event_status"] = "open"
    elif ack_status == "mitigated":
        ack["mitigated_at_utc"] = ts
        out["event_status"] = "monitoring"
    elif ack_status == "escalated":
        ack["escalated_at_utc"] = ts
        out["event_status"] = "breached"
    elif ack_status == "closed":
        ack["closed_at_utc"] = ts
        out["event_status"] = "closed"
    ack["last_updated_utc"] = ts
    history = ack.get("history", [])
    if not isinstance(history, list):
        history = []
    history.append(
        {
            "from_status": prev_status,
            "to_status": ack_status,
            "at_utc": ts,
            "owner_note": str(owner_note or ""),
        }
    )
    if len(history) > 50:
        history = history[-50:]
    ack["history"] = history
    out["ack_state"] = ack
    return out


def _ack_transition_allowed(current_status: str, next_status: str) -> bool:
    allowed = {
        "pending": {"pending", "acked"},
        "acked": {"acked", "mitigated", "escalated", "closed"},
        "mitigated": {"mitigated", "escalated", "closed"},
        "escalated": {"escalated", "mitigated", "closed"},
        "closed": {"closed"},
    }
    return next_status in allowed.get(current_status, {current_status})


def _validate_ack_update(*, ack_state: dict[str, Any], next_status: str, event_time_utc: str, owner_note: str) -> str:
    ts = _parse_ts(event_time_utc)
    if ts is None:
        return "invalid --event-time-utc format, expected ISO-8601 UTC like 2026-03-15T00:05:00Z"
    last_ts = _parse_ts(str(ack_state.get("last_updated_utc", "") or ""))
    if last_ts is not None and ts < last_ts:
        return "event_time_utc is older than last_updated_utc"
    if next_status == "closed" and not str(owner_note or "").strip() and not str(ack_state.get("owner_note", "")).strip():
        return "closing event requires --owner-note or existing owner_note"
    return ""


def _apply_ack_update_request(
    oncall: dict[str, Any],
    *,
    ack_status: str,
    owner_note: str,
    event_time_utc: str,
    expected_last_updated_utc: str,
    expected_ack_revision: int,
    expected_incident_key: str,
) -> tuple[dict[str, Any] | None, str]:
    if expected_incident_key and str(oncall.get("incident_key", "")) != expected_incident_key:
        return None, "incident_key mismatch"
    status = str(ack_status or "").strip().lower()
    if status not in {"pending", "acked", "mitigated", "escalated", "closed"}:
        return None, "--ack-status must be one of: pending|acked|mitigated|escalated|closed"
    ack_state = oncall.get("ack_state", {})
    if not isinstance(ack_state, dict):
        ack_state = _ack_state_template(_utc_now())
    current_status = str(ack_state.get("ack_status", "pending") or "pending").lower()
    if expected_last_updated_utc:
        actual_last = str(ack_state.get("last_updated_utc", "") or "")
        if actual_last != expected_last_updated_utc:
            return None, f"ack state conflict: expected last_updated_utc={expected_last_updated_utc}, actual={actual_last}"
    if expected_ack_revision >= 0:
        actual_revision = int(ack_state.get("ack_revision", 0) or 0)
        if actual_revision != int(expected_ack_revision):
            return None, f"ack state conflict: expected ack_revision={int(expected_ack_revision)}, actual={actual_revision}"
    if not _ack_transition_allowed(current_status, status):
        return None, f"invalid ack status transition: {current_status} -> {status}"
    validation_error = _validate_ack_update(
        ack_state=ack_state,
        next_status=status,
        event_time_utc=event_time_utc,
        owner_note=owner_note,
    )
    if validation_error:
        return None, validation_error
    updated = _update_ack_state(
        oncall,
        ack_status=status,
        owner_note=owner_note,
        event_time_utc=event_time_utc,
    )
    return updated, ""


def _parse_expected_ack_revision(value: Any) -> tuple[int, str]:
    if value in (None, "", -1):
        return -1, ""
    try:
        return int(value), ""
    except Exception:
        return -1, f"invalid expected_ack_revision: {value}"


ACTIVE_POLICY: dict[str, Any] = dict(DEFAULT_POLICY)


def _policy_rule(name: str) -> dict[str, Any]:
    value = ACTIVE_POLICY.get(name, {})
    return value if isinstance(value, dict) else {}


def _risk_policy_config() -> dict[str, Any]:
    policy = _policy_rule("risk_policy")
    if not isinstance(policy, dict):
        return {}
    return policy


def _governance_policy_config() -> dict[str, Any]:
    policy = _policy_rule("governance_policy")
    if not isinstance(policy, dict):
        return {}
    return policy


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        return dict(DEFAULT_POLICY)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(DEFAULT_POLICY)
    if not isinstance(payload, dict):
        return dict(DEFAULT_POLICY)
    merged: dict[str, Any] = dict(DEFAULT_POLICY)
    for k, v in payload.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            base = dict(merged.get(k, {}))
            base.update(v)
            merged[k] = base
        else:
            merged[k] = v
    return merged


def _build_incident_key(alert_level: str, primary_failure_class: str, generated_at: str) -> str:
    day = generated_at[:10] if len(generated_at) >= 10 else generated_at
    raw = f"{day}|{alert_level}|{primary_failure_class or 'normal'}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"peak-notify-{digest}"


def _load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _window_rows(rows: list[dict[str, Any]], window_days: int) -> list[dict[str, Any]]:
    if window_days <= 0:
        return rows
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=window_days)
    out: list[dict[str, Any]] = []
    for r in rows:
        ts = _parse_ts(str(r.get("ts", "") or ""))
        if ts is None:
            continue
        if ts >= cutoff:
            out.append(r)
    return out


def _build_runbook_actions(failure_class_breakdown: dict[str, int], alert_level: str) -> list[str]:
    actions: list[str] = []
    if alert_level == "high":
        actions.append("立即检查Webhook配置与权限，修复配置错误后重新触发通知任务。")
    elif alert_level == "medium":
        actions.append("检查通知服务可用性与网络连通性，必要时提升重试次数并观察恢复。")
    elif alert_level == "low":
        actions.append("持续观察异常分布，若连续出现失败请升级为人工排查。")
    else:
        actions.append("通知链路稳定，保持当前配置并持续监控。")

    if failure_class_breakdown.get("client_error", 0) > 0:
        actions.append("出现 client_error：核验Webhook地址、签名密钥、消息体格式。")
    if failure_class_breakdown.get("config_missing", 0) > 0:
        actions.append("出现 config_missing：在生产环境补齐Webhook配置并开启严格投递。")
    if failure_class_breakdown.get("payload_invalid", 0) > 0:
        actions.append("出现 payload_invalid：检查通知模板字段完整性与数据生成流程。")
    if failure_class_breakdown.get("server_error", 0) > 0:
        actions.append("出现 server_error：关注第三方服务状态页，必要时执行重试或延后重放。")
    if failure_class_breakdown.get("network_timeout", 0) > 0 or failure_class_breakdown.get("network_error", 0) > 0:
        actions.append("出现 network_*：检查出口网络与DNS，临时提高timeout与退避参数。")
    if failure_class_breakdown.get("throttle_timeout", 0) > 0:
        actions.append("出现 throttle_timeout：降低发送频率并扩大指数退避。")
    if failure_class_breakdown.get("deduped", 0) > 0:
        actions.append("去重命中正常：核验run_id与template_key策略，确认无误杀。")
    return actions


def _failure_owner_and_escalation(failure_class: str) -> tuple[str, str]:
    mapping = {
        "client_error": ("配置治理", "值班T+0修复Webhook地址/鉴权并立即重放，持续失败升级到平台主管"),
        "config_missing": ("配置治理", "值班T+0补齐密钥与Webhook配置，30分钟未恢复升级到平台主管"),
        "payload_invalid": ("应用研发", "值班通知应用研发修复模板/字段，修复后执行回归并重放"),
        "server_error": ("三方服务", "值班关注三方状态页并降速重试，连续30分钟失败升级到供应商支持"),
        "network_timeout": ("网络平台", "值班检查出口网络与DNS，必要时切换链路，未恢复升级到网络负责人"),
        "network_error": ("网络平台", "值班排查网络连通性并校验TLS/代理，持续失败升级到网络负责人"),
        "throttle_timeout": ("流控治理", "值班降低发送频率并扩大退避，超过阈值升级到平台容量负责人"),
        "unknown_failure": ("平台值班", "值班拉取完整日志定位根因，1小时内无法定位升级到平台主管"),
        "not_configured": ("配置治理", "非生产可观察；生产若出现需补齐配置并转严格模式"),
        "deduped": ("发布治理", "确认去重策略生效并抽检无误杀，异常时回滚到日志兜底模式"),
        "success": ("平台值班", "保持监控，无需升级"),
        "skipped": ("平台值班", "检查是否符合预期跳过策略"),
    }
    return mapping.get(failure_class, ("平台值班", "按未知异常路径处理并升级平台主管"))


def _build_owner_breakdown(failure_class_breakdown: dict[str, int]) -> dict[str, int]:
    owner_breakdown: dict[str, int] = {}
    for fc, cnt in failure_class_breakdown.items():
        owner, _ = _failure_owner_and_escalation(fc)
        owner_breakdown[owner] = owner_breakdown.get(owner, 0) + int(cnt or 0)
    return owner_breakdown


def _build_escalation_paths(failure_class_breakdown: dict[str, int]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for fc, cnt in sorted(failure_class_breakdown.items()):
        if int(cnt or 0) <= 0:
            continue
        owner, path = _failure_owner_and_escalation(fc)
        items.append({"failure_class": fc, "owner": owner, "escalation_path": path})
    return items


def _response_sla_minutes(alert_level: str) -> int:
    rules = _policy_rule("response_sla_minutes")
    return int(rules.get(alert_level, rules.get("default", 0)))


def _escalation_wait_minutes(alert_level: str) -> int:
    rules = _policy_rule("escalation_wait_minutes")
    return int(rules.get(alert_level, rules.get("default", 0)))


def _build_oncall_handoff(failure_class_breakdown: dict[str, int], alert_level: str) -> dict[str, Any]:
    ignored = {"success", "deduped", "not_configured", "skipped"}
    candidates = [
        (k, int(v or 0))
        for k, v in failure_class_breakdown.items()
        if int(v or 0) > 0 and str(k) not in ignored
    ]
    if not candidates:
        candidates = [(k, int(v or 0)) for k, v in failure_class_breakdown.items() if int(v or 0) > 0]
    if not candidates:
        return {
            "primary_failure_class": "",
            "primary_owner": "平台值班",
            "escalation_path": "无异常，持续监控。",
            "response_sla_minutes": _response_sla_minutes(alert_level),
        }
    candidates.sort(key=lambda x: x[1], reverse=True)
    primary_fc = str(candidates[0][0])
    owner, path = _failure_owner_and_escalation(primary_fc)
    return {
        "primary_failure_class": primary_fc,
        "primary_owner": owner,
        "escalation_path": path,
        "response_sla_minutes": _response_sla_minutes(alert_level),
    }


def _build_execution_checklist(
    *,
    owner: str,
    generated_at: str,
    response_deadline_utc: str,
    escalation_deadline_utc: str,
) -> list[dict[str, str]]:
    return [
        {"id": "ack", "owner": owner, "due_utc": generated_at, "title": "确认告警并建立值班事件", "status": "pending"},
        {"id": "mitigate", "owner": owner, "due_utc": response_deadline_utc, "title": "执行首轮缓解与验证", "status": "pending"},
        {"id": "escalate", "owner": owner, "due_utc": escalation_deadline_utc, "title": "未恢复则按升级路径升级", "status": "pending"},
        {"id": "closeout", "owner": owner, "due_utc": "", "title": "恢复后完成复盘并关闭事件", "status": "pending"},
    ]


def build_json(
    rows: list[dict[str, Any]], window_days: int, generated_at: str, reference_utc: str | None = None
) -> dict[str, Any]:
    total = len(rows)
    attempted = [r for r in rows if str(r.get("status", "")) in ("sent", "failed")]
    sent_ok = [r for r in rows if str(r.get("status", "")) == "sent" and bool(r.get("ok", False))]
    dedupe_skipped = [r for r in rows if str(r.get("reason", "")) == "duplicate_delivery_deduped"]
    webhook_skipped = [r for r in rows if str(r.get("reason", "")) == "webhook_not_configured"]
    failed = [r for r in rows if str(r.get("status", "")) == "failed"]
    retry_count = sum(1 for r in attempted if bool(r.get("retried", False)))
    avg_attempt_count = (
        round(sum(int(r.get("attempt_count", 1) or 1) for r in attempted) / len(attempted), 3) if attempted else 0.0
    )
    success_rate = round((len(sent_ok) / len(attempted) * 100.0), 2) if attempted else 0.0
    dedupe_hit_rate = round((len(dedupe_skipped) / total * 100.0), 2) if total else 0.0
    reason_breakdown: dict[str, int] = {}
    failure_class_breakdown: dict[str, int] = {}
    for r in rows:
        reason = str(r.get("reason", "") or "")
        if reason:
            reason_breakdown[reason] = reason_breakdown.get(reason, 0) + 1
        fc = str(r.get("failure_class", "") or "")
        if fc:
            failure_class_breakdown[fc] = failure_class_breakdown.get(fc, 0) + 1
    if failed:
        if any(failure_class_breakdown.get(k, 0) > 0 for k in ("config_missing", "payload_invalid", "client_error")):
            alert_level = "high"
        elif any(failure_class_breakdown.get(k, 0) > 0 for k in ("server_error", "network_error", "network_timeout", "throttle_timeout")):
            alert_level = "medium"
        else:
            alert_level = "low"
    else:
        alert_level = "info"
    runbook_actions = _build_runbook_actions(failure_class_breakdown, alert_level)
    owner_breakdown = _build_owner_breakdown(failure_class_breakdown)
    escalation_paths = _build_escalation_paths(failure_class_breakdown)
    oncall_handoff = _build_oncall_handoff(failure_class_breakdown, alert_level)
    response_sla_minutes = int(oncall_handoff.get("response_sla_minutes", 0) or 0)
    primary_failure_class = str(oncall_handoff.get("primary_failure_class", "") or "")
    incident_key = _build_incident_key(alert_level, primary_failure_class, generated_at)
    severity = _severity_from_alert(alert_level)
    response_deadline_utc = _response_deadline_utc(generated_at, response_sla_minutes)
    escalation_wait_minutes = _escalation_wait_minutes(alert_level)
    escalation_deadline_utc = _plus_minutes_utc(response_deadline_utc, escalation_wait_minutes)
    evaluation_utc = str(reference_utc or generated_at)
    overdue_flags = _overdue_flags(
        evaluation_utc=evaluation_utc,
        response_deadline_utc=response_deadline_utc,
        escalation_deadline_utc=escalation_deadline_utc,
    )
    breach_profile = _breach_profile(
        overdue_stage=str(overdue_flags.get("overdue_stage", "none")),
        alert_level=alert_level,
    )
    sla_breach_level = str(breach_profile.get("sla_breach_level", "none"))
    breach_score = _breach_score(sla_breach_level, alert_level)
    escalation_required = _escalation_required(sla_breach_level)
    page_required = _page_required(sla_breach_level, escalation_required)
    dispatch_priority = _dispatch_priority(sla_breach_level)
    immediate_action = str(breach_profile.get("immediate_action", ""))
    overdue_stage = str(overdue_flags.get("overdue_stage", "none"))
    handoff_summary = (
        f"owner={oncall_handoff.get('primary_owner', '平台值班')} | "
        f"class={primary_failure_class or 'normal'} | "
        f"deadline={response_deadline_utc or 'N/A'}"
    )
    primary_owner = str(oncall_handoff.get("primary_owner", "平台值班"))
    execution_checklist = _build_execution_checklist(
        owner=primary_owner,
        generated_at=generated_at,
        response_deadline_utc=response_deadline_utc,
        escalation_deadline_utc=escalation_deadline_utc,
    )
    oncall_event = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "incident_key": incident_key,
        "severity": severity,
        "alert_level": alert_level,
        "title": f"峰值通知异常处置 {primary_failure_class or 'normal'}",
        "primary_owner": primary_owner,
        "primary_failure_class": primary_failure_class,
        "escalation_policy_version": "v1",
        "response_sla_minutes": response_sla_minutes,
        "response_deadline_utc": response_deadline_utc,
        "escalation_wait_minutes": escalation_wait_minutes,
        "escalation_deadline_utc": escalation_deadline_utc,
        "evaluation_utc": evaluation_utc,
        "response_overdue": bool(overdue_flags.get("response_overdue", False)),
        "escalation_overdue": bool(overdue_flags.get("escalation_overdue", False)),
        "overdue_stage": overdue_stage,
        "sla_breach_level": sla_breach_level,
        "breach_score": breach_score,
        "escalation_required": escalation_required,
        "page_required": page_required,
        "dispatch_priority": dispatch_priority,
        "immediate_action": immediate_action,
        "breach_summary": _breach_summary(
            sla_breach_level=sla_breach_level,
            overdue_stage=overdue_stage,
            immediate_action=immediate_action,
        ),
        "next_checkpoint_utc": _next_checkpoint_utc(alert_level, generated_at),
        "escalation_path": oncall_handoff.get("escalation_path", ""),
        "event_status": (
            "breached" if str(overdue_flags.get("overdue_stage", "none")) != "none" else "open"
        )
        if alert_level in ("high", "medium", "low")
        else "monitoring",
        "owner_queue": str(oncall_handoff.get("primary_owner", "平台值班")),
        "notify_channel": "oncall-im",
        "handoff_summary": handoff_summary,
        "ack_state": _ack_state_template(generated_at),
        "timeline": [
            {"stage": "detected", "at_utc": generated_at},
            {"stage": "response_due", "at_utc": response_deadline_utc},
            {"stage": "escalation_due", "at_utc": escalation_deadline_utc},
        ],
        "closure_criteria": [
            "近3次通知发送成功率恢复到100%",
            "failure_class不再出现高/中风险类型",
            "值班确认工单完成并记录复盘结论",
        ],
        "execution_checklist": execution_checklist,
        "execution_command_template": _execution_command_template(
            incident_key=incident_key, owner=primary_owner, sla_breach_level=sla_breach_level
        ),
        "ticket_payload": {
            "summary": f"[{severity}] 峰值通知异常 {primary_failure_class or 'normal'}",
            "description": str(oncall_handoff.get("escalation_path", "")),
            "labels": [
                "peak_release_notify",
                f"alert:{alert_level}",
                f"class:{primary_failure_class or 'normal'}",
            ],
            "priority": dispatch_priority,
        },
        "runbook_actions": runbook_actions,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "window_days": window_days,
        "record_count": total,
        "attempted_count": len(attempted),
        "sent_ok_count": len(sent_ok),
        "failed_count": len(failed),
        "dedupe_skipped_count": len(dedupe_skipped),
        "webhook_not_configured_count": len(webhook_skipped),
        "retry_count": retry_count,
        "avg_attempt_count": avg_attempt_count,
        "success_rate_pct": success_rate,
        "dedupe_hit_rate_pct": dedupe_hit_rate,
        "reason_breakdown": reason_breakdown,
        "failure_class_breakdown": failure_class_breakdown,
        "failure_owner_breakdown": owner_breakdown,
        "escalation_paths": escalation_paths,
        "oncall_handoff": oncall_handoff,
        "oncall_event": oncall_event,
        "alert_level": alert_level,
        "runbook_actions": runbook_actions,
        "meets_sla_99": success_rate >= 99.0 if attempted else True,
    }


def render_md(payload: dict[str, Any]) -> str:
    reason = payload.get("reason_breakdown", {})
    if not isinstance(reason, dict):
        reason = {}
    reason_lines = [f"- {k}: {v}" for k, v in sorted(reason.items())] or ["- N/A"]
    failure_class = payload.get("failure_class_breakdown", {})
    if not isinstance(failure_class, dict):
        failure_class = {}
    failure_class_lines = [f"- {k}: {v}" for k, v in sorted(failure_class.items())] or ["- N/A"]
    owner_breakdown = payload.get("failure_owner_breakdown", {})
    if not isinstance(owner_breakdown, dict):
        owner_breakdown = {}
    owner_lines = [f"- {k}: {v}" for k, v in sorted(owner_breakdown.items())] or ["- N/A"]
    escalation_paths = payload.get("escalation_paths", [])
    if not isinstance(escalation_paths, list):
        escalation_paths = []
    escalation_lines = [
        f"- {str(x.get('failure_class', 'unknown'))} → {str(x.get('owner', '平台值班'))}: {str(x.get('escalation_path', ''))}"
        for x in escalation_paths
        if isinstance(x, dict)
    ] or ["- N/A"]
    handoff = payload.get("oncall_handoff", {})
    if not isinstance(handoff, dict):
        handoff = {}
    oncall_event = payload.get("oncall_event", {})
    if not isinstance(oncall_event, dict):
        oncall_event = {}
    timeline = oncall_event.get("timeline", [])
    if not isinstance(timeline, list):
        timeline = []
    timeline_lines = [
        f"- {str(x.get('stage', 'unknown'))}: {str(x.get('at_utc', ''))}"
        for x in timeline
        if isinstance(x, dict)
    ] or ["- N/A"]
    closure_criteria = oncall_event.get("closure_criteria", [])
    if not isinstance(closure_criteria, list):
        closure_criteria = []
    closure_lines = [f"- {str(x)}" for x in closure_criteria if str(x).strip()] or ["- N/A"]
    checklist = oncall_event.get("execution_checklist", [])
    if not isinstance(checklist, list):
        checklist = []
    checklist_lines = [
        f"- {str(x.get('id', 'task'))} | owner={str(x.get('owner', ''))} | due={str(x.get('due_utc', ''))} | {str(x.get('title', ''))}"
        for x in checklist
        if isinstance(x, dict)
    ] or ["- N/A"]
    ack_state = oncall_event.get("ack_state", {})
    if not isinstance(ack_state, dict):
        ack_state = {}
    ack_history = ack_state.get("history", [])
    if not isinstance(ack_history, list):
        ack_history = []
    latest_transition = ack_history[-1] if ack_history and isinstance(ack_history[-1], dict) else {}
    runbook_actions = payload.get("runbook_actions", [])
    if not isinstance(runbook_actions, list):
        runbook_actions = []
    runbook_lines = [f"- {str(x)}" for x in runbook_actions if str(x).strip()] or ["- N/A"]
    return "\n".join(
        [
            "# 峰值通知投递SLA报告",
            "",
            f"> 生成时间: {payload.get('generated_at', '?')}",
            f"> 统计窗口: 最近 {payload.get('window_days', 0)} 天",
            "",
            "| 指标 | 值 |",
            "|---|---|",
            f"| record_count | {payload.get('record_count', 0)} |",
            f"| attempted_count | {payload.get('attempted_count', 0)} |",
            f"| sent_ok_count | {payload.get('sent_ok_count', 0)} |",
            f"| failed_count | {payload.get('failed_count', 0)} |",
            f"| dedupe_skipped_count | {payload.get('dedupe_skipped_count', 0)} |",
            f"| webhook_not_configured_count | {payload.get('webhook_not_configured_count', 0)} |",
            f"| retry_count | {payload.get('retry_count', 0)} |",
            f"| avg_attempt_count | {payload.get('avg_attempt_count', 0.0)} |",
            f"| success_rate_pct | {payload.get('success_rate_pct', 0.0)} |",
            f"| dedupe_hit_rate_pct | {payload.get('dedupe_hit_rate_pct', 0.0)} |",
            f"| alert_level | {payload.get('alert_level', 'info')} |",
            f"| meets_sla_99 | {'true' if bool(payload.get('meets_sla_99', False)) else 'false'} |",
            "",
            "## 原因分布",
            "",
            *reason_lines,
            "",
            "## 失败分类分布",
            "",
            *failure_class_lines,
            "",
            "## 责任归属分布",
            "",
            *owner_lines,
            "",
            "## 升级路径",
            "",
            *escalation_lines,
            "",
            "## 值班闭环",
            "",
            f"- incident_key: {oncall_event.get('incident_key', '')}",
            f"- escalation_policy_version: {oncall_event.get('escalation_policy_version', '')}",
            f"- primary_failure_class: {handoff.get('primary_failure_class', '')}",
            f"- primary_owner: {handoff.get('primary_owner', '平台值班')}",
            f"- response_sla_minutes: {handoff.get('response_sla_minutes', 0)}",
            f"- response_deadline_utc: {oncall_event.get('response_deadline_utc', '')}",
            f"- escalation_wait_minutes: {oncall_event.get('escalation_wait_minutes', 0)}",
            f"- escalation_deadline_utc: {oncall_event.get('escalation_deadline_utc', '')}",
            f"- evaluation_utc: {oncall_event.get('evaluation_utc', '')}",
            f"- response_overdue: {oncall_event.get('response_overdue', False)}",
            f"- escalation_overdue: {oncall_event.get('escalation_overdue', False)}",
            f"- overdue_stage: {oncall_event.get('overdue_stage', 'none')}",
            f"- sla_breach_level: {oncall_event.get('sla_breach_level', 'none')}",
            f"- breach_score: {oncall_event.get('breach_score', 0)}",
            f"- escalation_required: {oncall_event.get('escalation_required', False)}",
            f"- page_required: {oncall_event.get('page_required', False)}",
            f"- dispatch_priority: {oncall_event.get('dispatch_priority', 'normal')}",
            f"- immediate_action: {oncall_event.get('immediate_action', '')}",
            f"- breach_summary: {oncall_event.get('breach_summary', '')}",
            f"- execution_command_template: {oncall_event.get('execution_command_template', '')}",
            f"- next_checkpoint_utc: {oncall_event.get('next_checkpoint_utc', '')}",
            f"- event_status: {oncall_event.get('event_status', '')}",
            f"- ack_status: {ack_state.get('ack_status', 'pending')}",
            f"- ack_revision: {ack_state.get('ack_revision', 0)}",
            f"- ack_at_utc: {ack_state.get('ack_at_utc', '')}",
            f"- mitigated_at_utc: {ack_state.get('mitigated_at_utc', '')}",
            f"- escalated_at_utc: {ack_state.get('escalated_at_utc', '')}",
            f"- closed_at_utc: {ack_state.get('closed_at_utc', '')}",
            f"- ack_history_count: {len(ack_history)}",
            f"- latest_transition: {str(latest_transition.get('from_status', ''))}->{str(latest_transition.get('to_status', ''))}",
            f"- handoff_summary: {oncall_event.get('handoff_summary', '')}",
            f"- escalation_path: {handoff.get('escalation_path', '')}",
            "",
            "## 升级时间线",
            "",
            *timeline_lines,
            "",
            "## 关闭判据",
            "",
            *closure_lines,
            "",
            "## 执行清单",
            "",
            *checklist_lines,
            "",
            "## 处置建议",
            "",
            *runbook_lines,
            "",
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="统计峰值通知投递SLA")
    parser.add_argument("--log-jsonl", type=Path, default=DEFAULT_LOG_JSONL)
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--out-md", type=Path, default=DEFAULT_OUT_MD)
    parser.add_argument("--out-json", type=Path, default=DEFAULT_OUT_JSON)
    parser.add_argument("--out-oncall-json", type=Path, default=DEFAULT_OUT_ONCALL_JSON)
    parser.add_argument("--policy-json", type=Path, default=DEFAULT_POLICY_JSON)
    parser.add_argument("--reference-utc", default="")
    parser.add_argument("--update-ack-state", action="store_true")
    parser.add_argument("--batch-update-ack-file", type=Path, default=None)
    parser.add_argument("--batch-atomic", action="store_true")
    parser.add_argument("--batch-report-json", type=Path, default=DEFAULT_ACK_BATCH_REPORT_JSON)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ack-status", default="")
    parser.add_argument("--owner-note", default="")
    parser.add_argument("--event-time-utc", default="")
    parser.add_argument("--expected-last-updated-utc", default="")
    parser.add_argument("--expected-ack-revision", type=int, default=-1)
    args = parser.parse_args(argv)
    global ACTIVE_POLICY
    ACTIVE_POLICY = _load_policy(args.policy_json)
    ack_mode_enabled = bool(args.update_ack_state) or args.batch_update_ack_file is not None
    if bool(args.update_ack_state) and args.batch_update_ack_file is not None:
        print("[ERR] --update-ack-state and --batch-update-ack-file are mutually exclusive")
        return 1
    if bool(args.batch_atomic) and args.batch_update_ack_file is None:
        print("[ERR] --batch-atomic requires --batch-update-ack-file")
        return 1
    if not ack_mode_enabled and (
        bool(args.dry_run)
        or bool(args.batch_atomic)
        or bool(str(args.ack_status or "").strip())
        or bool(str(args.owner_note or "").strip())
        or bool(str(args.event_time_utc or "").strip())
        or bool(str(args.expected_last_updated_utc or "").strip())
        or int(args.expected_ack_revision) >= 0
    ):
        print("[ERR] ack update flags require --update-ack-state or --batch-update-ack-file")
        return 1

    if ack_mode_enabled:
        if not args.out_oncall_json.exists():
            print(f"[ERR] oncall json not found: {args.out_oncall_json}")
            return 1
        try:
            oncall = json.loads(args.out_oncall_json.read_text(encoding="utf-8"))
        except Exception:
            print(f"[ERR] invalid oncall json: {args.out_oncall_json}")
            return 1
        if not isinstance(oncall, dict):
            print(f"[ERR] invalid oncall payload type: {args.out_oncall_json}")
            return 1
        if bool(args.update_ack_state):
            updated_oncall, error = _apply_ack_update_request(
                oncall,
                ack_status=str(args.ack_status or ""),
                owner_note=str(args.owner_note or ""),
                event_time_utc=str(args.event_time_utc or _utc_now()),
                expected_last_updated_utc=str(args.expected_last_updated_utc or "").strip(),
                expected_ack_revision=int(args.expected_ack_revision),
                expected_incident_key="",
            )
            if updated_oncall is None:
                print(f"[ERR] {error}")
                return 1
            batch_report = None
            batch_exit_code = 0
        else:
            if not args.batch_update_ack_file or not args.batch_update_ack_file.exists():
                print(f"[ERR] batch update file not found: {args.batch_update_ack_file}")
                return 1
            try:
                batch_payload = json.loads(args.batch_update_ack_file.read_text(encoding="utf-8"))
            except Exception:
                print(f"[ERR] invalid batch update json: {args.batch_update_ack_file}")
                return 1
            if not isinstance(batch_payload, list):
                print("[ERR] batch update json must be an array")
                return 1
            working = copy.deepcopy(oncall)
            origin_oncall = copy.deepcopy(oncall)
            results: list[dict[str, Any]] = []
            success_count = 0
            failure_count = 0
            aborted = False
            error_breakdown: dict[str, int] = {}
            first_error_index = -1
            first_error_reason = ""
            first_error_request_id = ""
            seen_request_ids: set[str] = set()
            success_request_ids: list[str] = []
            failed_request_ids: list[str] = []
            skipped_request_ids: list[str] = []
            for idx, item in enumerate(batch_payload):
                request_id = f"idx-{idx}"
                if not isinstance(item, dict):
                    results.append({"index": idx, "request_id": request_id, "ok": False, "error": "item must be object"})
                    failure_count += 1
                    failed_request_ids.append(request_id)
                    if first_error_index < 0:
                        first_error_index = idx
                        first_error_reason = "item must be object"
                        first_error_request_id = request_id
                    if bool(args.batch_atomic):
                        aborted = True
                        break
                    continue
                request_id = str(item.get("request_id", "") or request_id)
                if request_id in seen_request_ids:
                    updated = None
                    error = f"duplicate request_id: {request_id}"
                else:
                    seen_request_ids.add(request_id)
                    parsed_revision, revision_error = _parse_expected_ack_revision(item.get("expected_ack_revision", -1))
                    if revision_error:
                        updated = None
                        error = revision_error
                    else:
                        updated, error = _apply_ack_update_request(
                            working,
                            ack_status=str(item.get("ack_status", "") or ""),
                            owner_note=str(item.get("owner_note", "") or ""),
                            event_time_utc=str(item.get("event_time_utc", "") or _utc_now()),
                            expected_last_updated_utc=str(item.get("expected_last_updated_utc", "") or "").strip(),
                            expected_ack_revision=parsed_revision,
                            expected_incident_key=str(item.get("expected_incident_key", "") or "").strip(),
                        )
                if updated is None:
                    results.append({"index": idx, "request_id": request_id, "ok": False, "error": error})
                    failure_count += 1
                    failed_request_ids.append(request_id)
                    if first_error_index < 0:
                        first_error_index = idx
                        first_error_reason = str(error or "")
                        first_error_request_id = request_id
                    key = "other"
                    e = str(error or "")
                    if "conflict" in e:
                        key = "conflict"
                    elif "transition" in e:
                        key = "transition"
                    elif "event_time_utc" in e:
                        key = "time"
                    elif "owner-note" in e:
                        key = "owner_note"
                    elif "incident_key mismatch" in e:
                        key = "incident_key"
                    elif "duplicate request_id" in e:
                        key = "duplicate_request_id"
                    elif "invalid expected_ack_revision" in e:
                        key = "invalid_request"
                    error_breakdown[key] = error_breakdown.get(key, 0) + 1
                    if bool(args.batch_atomic):
                        aborted = True
                        break
                    continue
                working = updated
                success_count += 1
                success_request_ids.append(request_id)
                ack_state_now = working.get("ack_state", {})
                ack_revision_now = int(ack_state_now.get("ack_revision", 0) or 0) if isinstance(ack_state_now, dict) else 0
                results.append({"index": idx, "request_id": request_id, "ok": True, "ack_revision": ack_revision_now})
            skipped_count = 0
            if aborted and len(results) < len(batch_payload):
                for rest_idx in range(len(results), len(batch_payload)):
                    results.append(
                        {
                            "index": rest_idx,
                            "request_id": f"idx-{rest_idx}",
                            "ok": False,
                            "skipped": True,
                            "error": "aborted_due_to_previous_error",
                        }
                    )
                    skipped_request_ids.append(f"idx-{rest_idx}")
                    skipped_count += 1
                error_breakdown["aborted"] = error_breakdown.get("aborted", 0) + skipped_count
            if aborted:
                updated_oncall = origin_oncall
                batch_exit_code = 3
            else:
                updated_oncall = working
                batch_exit_code = 0 if failure_count == 0 else 2
            request_status_by_id: dict[str, list[dict[str, Any]]] = {}
            for x in results:
                rid = str(x.get("request_id", ""))
                if not rid:
                    continue
                idx_raw = x.get("index", -1)
                try:
                    idx_value = int(idx_raw)
                except Exception:
                    idx_value = -1
                request_status_by_id.setdefault(rid, []).append(
                    {
                        "index": idx_value,
                        "ok": bool(x.get("ok", False)),
                        "skipped": bool(x.get("skipped", False)),
                        "error": str(x.get("error", "")),
                    }
                )
            request_final_outcome_by_id: dict[str, str] = {}
            request_final_detail_by_id: dict[str, dict[str, Any]] = {}
            request_outcome_counts = {"success": 0, "failed": 0, "skipped": 0, "unknown": 0}
            request_multi_event_ids: list[str] = []
            request_failed_with_error_ids: list[str] = []
            for rid, statuses in request_status_by_id.items():
                if len(statuses) > 1:
                    request_multi_event_ids.append(rid)
                final = statuses[-1] if statuses else {}
                if bool(final.get("skipped", False)):
                    outcome = "skipped"
                elif bool(final.get("ok", False)):
                    outcome = "success"
                elif "error" in final:
                    outcome = "failed"
                else:
                    outcome = "unknown"
                request_final_outcome_by_id[rid] = outcome
                final_index_raw = final.get("index", -1)
                try:
                    final_index_value = int(final_index_raw)
                except Exception:
                    final_index_value = -1
                request_final_detail_by_id[rid] = {
                    "outcome": outcome,
                    "final_index": final_index_value,
                    "final_ok": bool(final.get("ok", False)),
                    "final_skipped": bool(final.get("skipped", False)),
                    "final_error": str(final.get("error", "")),
                }
                if outcome == "failed" and str(final.get("error", "")):
                    request_failed_with_error_ids.append(rid)
                request_outcome_counts[outcome] = int(request_outcome_counts.get(outcome, 0)) + 1
            has_failures = failure_count > 0
            has_skipped = skipped_count > 0
            has_conflicts = int(error_breakdown.get("conflict", 0) or 0) > 0
            has_validation_errors = (
                int(error_breakdown.get("invalid_request", 0) or 0) > 0
                or int(error_breakdown.get("duplicate_request_id", 0) or 0) > 0
            )
            risk_policy = _risk_policy_config()
            risk_version = str(risk_policy.get("version", "v1") or "v1")
            risk_weights = risk_policy.get("weights", {}) if isinstance(risk_policy.get("weights", {}), dict) else {}
            failure_weight = max(0, _safe_int(risk_weights.get("failure", 40), 40))
            skipped_weight = max(0, _safe_int(risk_weights.get("skipped", 20), 20))
            conflict_weight = max(0, _safe_int(risk_weights.get("conflict", 25), 25))
            validation_weight = max(0, _safe_int(risk_weights.get("validation", 15), 15))
            aborted_weight = max(0, _safe_int(risk_weights.get("aborted", 15), 15))
            risk_components = {
                "failure": int(failure_count) * failure_weight,
                "skipped": int(skipped_count) * skipped_weight,
                "conflict": int(error_breakdown.get("conflict", 0) or 0) * conflict_weight,
                "validation": (
                    int(error_breakdown.get("invalid_request", 0) or 0)
                    + int(error_breakdown.get("duplicate_request_id", 0) or 0)
                )
                * validation_weight,
                "aborted": aborted_weight if aborted else 0,
            }
            risk_thresholds = (
                risk_policy.get("level_thresholds", {})
                if isinstance(risk_policy.get("level_thresholds", {}), dict)
                else {}
            )
            risk_low = _safe_int(risk_thresholds.get("low_max_exclusive", 25), 25)
            risk_medium = _safe_int(risk_thresholds.get("medium_max_exclusive", 50), 50)
            risk_high = _safe_int(risk_thresholds.get("high_max_exclusive", 80), 80)
            risk_max = _safe_int(risk_thresholds.get("max_score", 100), 100)
            if risk_low <= 0:
                risk_low = 25
            if risk_medium <= risk_low:
                risk_medium = max(50, risk_low + 1)
            if risk_high <= risk_medium:
                risk_high = max(80, risk_medium + 1)
            if risk_max <= 0:
                risk_max = 100
            if risk_max <= risk_high:
                risk_max = risk_high + 1
            risk_score = int(sum(risk_components.values()))
            if risk_score > risk_max:
                risk_score = risk_max
            if risk_score <= 0:
                risk_level = "info"
            elif risk_score < risk_low:
                risk_level = "low"
            elif risk_score < risk_medium:
                risk_level = "medium"
            elif risk_score < risk_high:
                risk_level = "high"
            else:
                risk_level = "critical"
            if not has_failures and not has_skipped:
                request_health = "healthy"
            elif aborted:
                request_health = "aborted"
            elif success_count > 0:
                request_health = "partial"
            else:
                request_health = "failed"
            alert_map = risk_policy.get("alert_level_map", {}) if isinstance(risk_policy.get("alert_level_map", {}), dict) else {}
            sla_alert_level = str(alert_map.get(risk_level, "p4"))
            action_map = (
                risk_policy.get("recommended_action_map", {})
                if isinstance(risk_policy.get("recommended_action_map", {}), dict)
                else {}
            )
            recommended_action = str(action_map.get(request_health, "manual_intervention_required"))
            dominant_error_type = ""
            dominant_error_count = 0
            error_total_count = 0
            dominant_error_ratio = 0.0
            error_type_count = 0
            error_signature = ""
            error_signature_basis = ""
            error_concentration_hhi = 0.0
            error_diversity_index = 0.0
            error_entropy = 0.0
            error_entropy_normalized = 0.0
            error_effective_type_count = 0.0
            error_top2_ratio = 0.0
            error_long_tail_ratio = 0.0
            dominant_to_second_ratio = 0.0
            error_tail_type_count = 0
            error_tail_avg_ratio_per_type = 0.0
            dominant_gap_to_second_ratio = 0.0
            error_pareto_80_type_count = 0
            error_pareto_80_ratio_covered = 0.0
            error_pareto_90_type_count = 0
            error_pareto_90_ratio_covered = 0.0
            error_pareto_95_type_count = 0
            error_pareto_95_ratio_covered = 0.0
            error_single_point_failure = False
            error_long_tail_present = False
            error_structure_tag = "none"
            error_concentration_tag = "none"
            error_focus_index = 0.0
            error_tail_pressure = 0.0
            error_governance_mode = "idle"
            error_governance_urgency_score = 0.0
            error_governance_priority = "none"
            error_governance_reason_tags: list[str] = []
            error_governance_eta_minutes = 0
            error_governance_owner = "none"
            error_governance_route = "observe"
            error_governance_playbook_id = "PB-IDLE-000"
            error_governance_policy_version = "v1"
            error_governance_rule_hits: list[str] = []
            error_alert_merge_key = ""
            error_alert_merge_key_basis = ""
            error_types_sorted: list[dict[str, Any]] = []
            if error_breakdown:
                governance_policy = _governance_policy_config()
                error_governance_policy_version = str(governance_policy.get("version", "v1") or "v1")
                concentration_cfg = (
                    governance_policy.get("concentration_thresholds", {})
                    if isinstance(governance_policy.get("concentration_thresholds", {}), dict)
                    else {}
                )
                urgency_cfg = (
                    governance_policy.get("urgency_thresholds", {})
                    if isinstance(governance_policy.get("urgency_thresholds", {}), dict)
                    else {}
                )
                risk_weight_cfg = (
                    governance_policy.get("risk_level_weight", {})
                    if isinstance(governance_policy.get("risk_level_weight", {}), dict)
                    else {}
                )
                eta_cfg = (
                    governance_policy.get("priority_to_eta_minutes", {})
                    if isinstance(governance_policy.get("priority_to_eta_minutes", {}), dict)
                    else {}
                )
                route_cfg = (
                    governance_policy.get("priority_to_route", {})
                    if isinstance(governance_policy.get("priority_to_route", {}), dict)
                    else {}
                )
                owner_cfg = (
                    governance_policy.get("owner_map", {})
                    if isinstance(governance_policy.get("owner_map", {}), dict)
                    else {}
                )
                playbook_cfg = (
                    governance_policy.get("playbook_map", {})
                    if isinstance(governance_policy.get("playbook_map", {}), dict)
                    else {}
                )
                merge_key_fields = governance_policy.get("merge_key_fields", [])
                if not isinstance(merge_key_fields, list):
                    merge_key_fields = []
                single_point_threshold = _safe_float(governance_policy.get("single_point_threshold", 0.8), 0.8)
                conc_very_high = _safe_float(concentration_cfg.get("very_high_min", 0.8), 0.8)
                conc_high = _safe_float(concentration_cfg.get("high_min", 0.5), 0.5)
                conc_medium = _safe_float(concentration_cfg.get("medium_min", 0.3), 0.3)
                p1_min = _safe_float(urgency_cfg.get("p1_min", 0.8), 0.8)
                p2_min = _safe_float(urgency_cfg.get("p2_min", 0.6), 0.6)
                p3_min = _safe_float(urgency_cfg.get("p3_min", 0.3), 0.3)
                ordered_errors = sorted(error_breakdown.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
                dominant_error_type = str(ordered_errors[0][0])
                dominant_error_count = int(ordered_errors[0][1])
                second_error_count = int(ordered_errors[1][1]) if len(ordered_errors) > 1 else 0
                error_total_count = int(sum(int(v) for v in error_breakdown.values()))
                error_type_count = int(len(ordered_errors))
                if error_total_count > 0:
                    probs = [float(int(v) / error_total_count) for v in error_breakdown.values()]
                    dominant_error_ratio = round(float(dominant_error_count / error_total_count), 6)
                    hhi_raw = float(sum(p * p for p in probs))
                    entropy_raw = float(-sum(p * math.log2(p) for p in probs if p > 0.0))
                    error_concentration_hhi = round(hhi_raw, 6)
                    error_diversity_index = round(float(1.0 - hhi_raw), 6)
                    error_entropy = round(entropy_raw, 6)
                    if error_type_count > 1:
                        error_entropy_normalized = round(float(entropy_raw / math.log2(error_type_count)), 6)
                    if hhi_raw > 0.0:
                        error_effective_type_count = round(float(1.0 / hhi_raw), 6)
                    top2_count = int(sum(int(v) for _, v in ordered_errors[:2]))
                    error_top2_ratio = round(float(top2_count / error_total_count), 6)
                    error_long_tail_ratio = round(float(1.0 - error_top2_ratio), 6)
                    second_error_ratio = round(float(second_error_count / error_total_count), 6) if second_error_count > 0 else 0.0
                    dominant_gap_to_second_ratio = round(float(dominant_error_ratio - second_error_ratio), 6)
                    error_tail_type_count = max(0, error_type_count - 2)
                    if error_tail_type_count > 0:
                        error_tail_avg_ratio_per_type = round(float(error_long_tail_ratio / error_tail_type_count), 6)
                    if second_error_count > 0:
                        dominant_to_second_ratio = round(float(dominant_error_count / second_error_count), 6)
                    covered = 0
                    covered_types = 0
                    for _, c in ordered_errors:
                        covered += int(c)
                        covered_types += 1
                        if covered >= int(math.ceil(error_total_count * 0.8)):
                            break
                    error_pareto_80_type_count = int(covered_types)
                    error_pareto_80_ratio_covered = round(float(covered / error_total_count), 6)
                    covered_90 = 0
                    covered_types_90 = 0
                    for _, c in ordered_errors:
                        covered_90 += int(c)
                        covered_types_90 += 1
                        if covered_90 >= int(math.ceil(error_total_count * 0.9)):
                            break
                    error_pareto_90_type_count = int(covered_types_90)
                    error_pareto_90_ratio_covered = round(float(covered_90 / error_total_count), 6)
                    covered_95 = 0
                    covered_types_95 = 0
                    for _, c in ordered_errors:
                        covered_95 += int(c)
                        covered_types_95 += 1
                        if covered_95 >= int(math.ceil(error_total_count * 0.95)):
                            break
                    error_pareto_95_type_count = int(covered_types_95)
                    error_pareto_95_ratio_covered = round(float(covered_95 / error_total_count), 6)
                    error_single_point_failure = bool(dominant_error_ratio >= single_point_threshold)
                    error_long_tail_present = bool(error_tail_type_count > 0 and error_long_tail_ratio > 0.0)
                    if error_single_point_failure:
                        error_structure_tag = "single_point"
                    elif error_long_tail_present:
                        error_structure_tag = "long_tail"
                    else:
                        error_structure_tag = "balanced"
                    if error_concentration_hhi >= conc_very_high:
                        error_concentration_tag = "very_high"
                    elif error_concentration_hhi >= conc_high:
                        error_concentration_tag = "high"
                    elif error_concentration_hhi >= conc_medium:
                        error_concentration_tag = "medium"
                    else:
                        error_concentration_tag = "low"
                    error_focus_index = round(float(dominant_error_ratio * error_concentration_hhi), 6)
                    error_tail_pressure = round(float(error_long_tail_ratio * error_type_count), 6)
                    if error_structure_tag == "single_point":
                        error_governance_mode = "stabilize_single_point"
                    elif error_structure_tag == "long_tail":
                        error_governance_mode = "reduce_long_tail"
                    else:
                        error_governance_mode = "balance_monitoring"
                    risk_level_weight = _safe_float(risk_weight_cfg.get(risk_level, 0.1), 0.1)
                    error_governance_urgency_score = round(
                        float(
                            max(
                                error_focus_index,
                                min(1.0, error_tail_pressure),
                                float(risk_level_weight),
                            )
                        ),
                        6,
                    )
                    if error_governance_urgency_score >= p1_min:
                        error_governance_priority = "p1"
                    elif error_governance_urgency_score >= p2_min:
                        error_governance_priority = "p2"
                    elif error_governance_urgency_score >= p3_min:
                        error_governance_priority = "p3"
                    else:
                        error_governance_priority = "p4"
                    if error_single_point_failure:
                        error_governance_reason_tags.append("single_point_failure")
                    if error_long_tail_present:
                        error_governance_reason_tags.append("long_tail_present")
                    if has_conflicts:
                        error_governance_reason_tags.append("has_conflicts")
                    if has_validation_errors:
                        error_governance_reason_tags.append("has_validation_errors")
                    if error_concentration_tag in {"very_high", "high"}:
                        error_governance_reason_tags.append("high_concentration")
                    error_governance_eta_minutes = _safe_int(eta_cfg.get(error_governance_priority, 0), 0)
                    error_governance_route = str(route_cfg.get(error_governance_priority, "observe") or "observe")
                    if has_validation_errors or has_conflicts:
                        error_governance_owner = str(owner_cfg.get("release", "release_oncall") or "release_oncall")
                    elif error_governance_mode == "stabilize_single_point":
                        error_governance_owner = str(owner_cfg.get("single_point", "qa_oncall") or "qa_oncall")
                    else:
                        error_governance_owner = str(owner_cfg.get("default", "platform_oncall") or "platform_oncall")
                    error_governance_playbook_id = str(playbook_cfg.get(error_governance_mode, "PB-BM-001") or "PB-BM-001")
                    error_governance_rule_hits = [
                        f"structure={error_structure_tag}",
                        f"concentration={error_concentration_tag}",
                        f"priority={error_governance_priority}",
                        f"owner={error_governance_owner}",
                        f"route={error_governance_route}",
                        f"playbook={error_governance_playbook_id}",
                    ]
                    merge_values: list[str] = []
                    merge_source = {
                        "error_signature": error_signature,
                        "error_structure_tag": error_structure_tag,
                        "error_governance_owner": error_governance_owner,
                    }
                    for f in merge_key_fields:
                        merge_values.append(str(merge_source.get(str(f), "")))
                    error_alert_merge_key_basis = "|".join(merge_values)
                    if error_alert_merge_key_basis:
                        error_alert_merge_key = hashlib.sha1(error_alert_merge_key_basis.encode("utf-8")).hexdigest()[:16]
                error_types_sorted = [{"type": str(k), "count": int(v)} for k, v in ordered_errors]
                error_signature_basis = "|".join(
                    f"{str(k)}:{int(v)}" for k, v in sorted(error_breakdown.items(), key=lambda kv: str(kv[0]))
                )
                error_signature = hashlib.sha1(error_signature_basis.encode("utf-8")).hexdigest()[:12]
            effective_risk_policy = {
                "version": risk_version,
                "weights": {
                    "failure": failure_weight,
                    "skipped": skipped_weight,
                    "conflict": conflict_weight,
                    "validation": validation_weight,
                    "aborted": aborted_weight,
                },
                "level_thresholds": {
                    "low_max_exclusive": risk_low,
                    "medium_max_exclusive": risk_medium,
                    "high_max_exclusive": risk_high,
                    "max_score": risk_max,
                },
            }
            batch_report = {
                "total": len(batch_payload),
                "success_count": success_count,
                "failed_count": failure_count,
                "skipped_count": skipped_count,
                "aborted": aborted,
                "atomic": bool(args.batch_atomic),
                "applied_count": 0 if aborted else success_count,
                "error_breakdown": error_breakdown,
                "dominant_error_type": dominant_error_type,
                "dominant_error_count": dominant_error_count,
                "error_total_count": error_total_count,
                "dominant_error_ratio": dominant_error_ratio,
                "error_type_count": error_type_count,
                "error_signature": error_signature,
                "error_signature_basis": error_signature_basis,
                "error_concentration_hhi": error_concentration_hhi,
                "error_diversity_index": error_diversity_index,
                "error_entropy": error_entropy,
                "error_entropy_normalized": error_entropy_normalized,
                "error_effective_type_count": error_effective_type_count,
                "error_top2_ratio": error_top2_ratio,
                "error_long_tail_ratio": error_long_tail_ratio,
                "dominant_to_second_ratio": dominant_to_second_ratio,
                "error_tail_type_count": error_tail_type_count,
                "error_tail_avg_ratio_per_type": error_tail_avg_ratio_per_type,
                "dominant_gap_to_second_ratio": dominant_gap_to_second_ratio,
                "error_pareto_80_type_count": error_pareto_80_type_count,
                "error_pareto_80_ratio_covered": error_pareto_80_ratio_covered,
                "error_pareto_90_type_count": error_pareto_90_type_count,
                "error_pareto_90_ratio_covered": error_pareto_90_ratio_covered,
                "error_pareto_95_type_count": error_pareto_95_type_count,
                "error_pareto_95_ratio_covered": error_pareto_95_ratio_covered,
                "error_single_point_failure": error_single_point_failure,
                "error_long_tail_present": error_long_tail_present,
                "error_structure_tag": error_structure_tag,
                "error_concentration_tag": error_concentration_tag,
                "error_focus_index": error_focus_index,
                "error_tail_pressure": error_tail_pressure,
                "error_governance_mode": error_governance_mode,
                "error_governance_urgency_score": error_governance_urgency_score,
                "error_governance_priority": error_governance_priority,
                "error_governance_reason_tags": error_governance_reason_tags,
                "error_governance_eta_minutes": error_governance_eta_minutes,
                "error_governance_owner": error_governance_owner,
                "error_governance_route": error_governance_route,
                "error_governance_playbook_id": error_governance_playbook_id,
                "error_governance_policy_version": error_governance_policy_version,
                "error_governance_rule_hits": error_governance_rule_hits,
                "error_alert_merge_key": error_alert_merge_key,
                "error_alert_merge_key_basis": error_alert_merge_key_basis,
                "error_types_sorted": error_types_sorted,
                "first_error_index": first_error_index,
                "first_error_reason": first_error_reason,
                "first_error_request_id": first_error_request_id,
                "success_request_ids": success_request_ids,
                "failed_request_ids": failed_request_ids,
                "skipped_request_ids": skipped_request_ids,
                "request_status_by_id": request_status_by_id,
                "request_final_outcome_by_id": request_final_outcome_by_id,
                "request_final_detail_by_id": request_final_detail_by_id,
                "request_outcome_counts": request_outcome_counts,
                "request_unique_count": len(request_status_by_id),
                "request_multi_event_ids": request_multi_event_ids,
                "request_failed_with_error_ids": request_failed_with_error_ids,
                "has_failures": has_failures,
                "has_skipped": has_skipped,
                "has_conflicts": has_conflicts,
                "has_validation_errors": has_validation_errors,
                "request_health": request_health,
                "risk_score": risk_score,
                "risk_level": risk_level,
                "risk_components": risk_components,
                "risk_policy_version": risk_version,
                "effective_risk_policy": effective_risk_policy,
                "sla_alert_level": sla_alert_level,
                "recommended_action": recommended_action,
                "final_ack_status": str(updated_oncall.get("ack_state", {}).get("ack_status", "pending"))
                if isinstance(updated_oncall.get("ack_state", {}), dict)
                else "pending",
                "final_ack_revision": int(updated_oncall.get("ack_state", {}).get("ack_revision", 0) or 0)
                if isinstance(updated_oncall.get("ack_state", {}), dict)
                else 0,
                "results": results,
            }
            governance_policy = _governance_policy_config()
            trend_window = max(1, _safe_int(governance_policy.get("trend_window", 10), 10))
            previous_history: list[dict[str, Any]] = []
            if args.batch_report_json.exists():
                try:
                    old_report = json.loads(args.batch_report_json.read_text(encoding="utf-8"))
                except Exception:
                    old_report = {}
                if isinstance(old_report, dict) and isinstance(old_report.get("trend_recent_reports"), list):
                    previous_history = [x for x in old_report.get("trend_recent_reports", []) if isinstance(x, dict)]
            current_snapshot = {
                "priority": batch_report.get("error_governance_priority", "none"),
                "structure": batch_report.get("error_structure_tag", "none"),
                "concentration": batch_report.get("error_concentration_tag", "none"),
                "urgency_score": batch_report.get("error_governance_urgency_score", 0.0),
                "merge_key": batch_report.get("error_alert_merge_key", ""),
                "report_generated_at_utc": _utc_now(),
            }
            trend_recent_reports = (previous_history + [current_snapshot])[-trend_window:]
            priority_dist: dict[str, int] = {}
            structure_dist: dict[str, int] = {}
            for r in trend_recent_reports:
                p = str(r.get("priority", "none") or "none")
                s = str(r.get("structure", "none") or "none")
                priority_dist[p] = int(priority_dist.get(p, 0)) + 1
                structure_dist[s] = int(structure_dist.get(s, 0)) + 1
            structure_shift = "none"
            priority_shift = "none"
            priority_order_cfg = governance_policy.get("priority_order", ["none", "p4", "p3", "p2", "p1"])
            if not isinstance(priority_order_cfg, list):
                priority_order_cfg = ["none", "p4", "p3", "p2", "p1"]
            if len(trend_recent_reports) >= 2:
                prev = trend_recent_reports[-2]
                curr = trend_recent_reports[-1]
                prev_s = str(prev.get("structure", "none") or "none")
                curr_s = str(curr.get("structure", "none") or "none")
                structure_shift = "changed" if prev_s != curr_s else "stable"
                prev_p = str(prev.get("priority", "none") or "none")
                curr_p = str(curr.get("priority", "none") or "none")
                try:
                    prev_idx = priority_order_cfg.index(prev_p)
                except Exception:
                    prev_idx = -1
                try:
                    curr_idx = priority_order_cfg.index(curr_p)
                except Exception:
                    curr_idx = -1
                if curr_idx > prev_idx:
                    priority_shift = "up"
                elif curr_idx < prev_idx:
                    priority_shift = "down"
                else:
                    priority_shift = "flat"
            batch_report["trend_window"] = trend_window
            batch_report["trend_recent_reports"] = trend_recent_reports
            batch_report["trend_recent_count"] = len(trend_recent_reports)
            batch_report["trend_priority_distribution"] = priority_dist
            batch_report["trend_structure_distribution"] = structure_dist
            batch_report["trend_structure_shift"] = structure_shift
            batch_report["trend_priority_shift"] = priority_shift
        if bool(args.dry_run):
            if batch_report is not None:
                print("[DRY-RUN] batch ack update evaluated, no file writes")
                print(json.dumps(batch_report, ensure_ascii=False, indent=2))
                return batch_exit_code
            ack_state_preview = updated_oncall.get("ack_state", {})
            if not isinstance(ack_state_preview, dict):
                ack_state_preview = {}
            print("[DRY-RUN] single ack update evaluated, no file writes")
            print(
                json.dumps(
                    {
                        "ack_status": ack_state_preview.get("ack_status", ""),
                        "ack_revision": ack_state_preview.get("ack_revision", 0),
                        "last_updated_utc": ack_state_preview.get("last_updated_utc", ""),
                        "event_status": updated_oncall.get("event_status", ""),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
        args.out_oncall_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_oncall_json.write_text(json.dumps(updated_oncall, ensure_ascii=False, indent=2), encoding="utf-8")
        if args.out_json.exists():
            try:
                report = json.loads(args.out_json.read_text(encoding="utf-8"))
            except Exception:
                report = {}
            if isinstance(report, dict):
                report["oncall_event"] = updated_oncall
                args.out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
                if args.out_md:
                    args.out_md.parent.mkdir(parents=True, exist_ok=True)
                    args.out_md.write_text(render_md(report), encoding="utf-8")
        if batch_report is not None:
            args.batch_report_json.parent.mkdir(parents=True, exist_ok=True)
            args.batch_report_json.write_text(json.dumps(batch_report, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[OK] oncall ack batch updated: {args.out_oncall_json}")
            print(f"[OK] ack batch report: {args.batch_report_json}")
            return batch_exit_code
        print(f"[OK] oncall ack state updated: {args.out_oncall_json}")
        return 0

    rows = _load_rows(args.log_jsonl)
    in_window = _window_rows(rows, max(1, int(args.window_days)))
    generated_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = build_json(
        in_window,
        max(1, int(args.window_days)),
        generated_at,
        reference_utc=str(args.reference_utc or generated_at),
    )
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_oncall_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text(render_md(payload), encoding="utf-8")
    args.out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    args.out_oncall_json.write_text(
        json.dumps(payload.get("oncall_event", {}), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[OK] peak release notify sla markdown: {args.out_md}")
    print(f"[OK] peak release notify sla json: {args.out_json}")
    print(f"[OK] peak release notify oncall json: {args.out_oncall_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
