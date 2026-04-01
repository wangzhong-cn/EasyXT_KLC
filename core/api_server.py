"""
EasyXT 轻量化中台服务（Phase 3）

提供统一的 HTTP + WebSocket 接口，解耦 QMT 行情/交易与前端/外部策略之间的直连依赖。

架构：
  - FastAPI 主应用
  - /health                          — 健康检查
  - /api/v1/strategies/              — 策略注册表 REST（list/get/patch status）
  - /api/v1/accounts/                — 账户注册表 REST（list/post/get/delete）
  - /api/v1/market/snapshot/{symbol} — 最新行情快照（HTTP）
  - /ws/market/{symbol}              — 实时行情推送（WebSocket，支持多客户端）

部署入口：  python -m core.api_server          （开发热重载）
           uvicorn core.api_server:app         （生产）

配置项（环境变量或 config/server_config.json）：
  EASYXT_API_HOST  默认 0.0.0.0
  EASYXT_API_PORT  默认 8765
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import secrets
import sys
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 配置（环境变量驱动）
# ---------------------------------------------------------------------------

_API_TOKEN: str = os.environ.get("EASYXT_API_TOKEN", "")  # 空 = 生产环境拒绝启动
_DEV_MODE: bool = os.environ.get("EASYXT_DEV_MODE", "").lower() in (
    "1",
    "true",
    "yes",
)  # 本地开发跳过鉴权
_TEST_MODE: bool = ("PYTEST_CURRENT_TEST" in os.environ) or any(
    "pytest" in x.lower() for x in sys.argv
)
_RATE_LIMIT: int = int(os.environ.get("EASYXT_RATE_LIMIT", "60"))  # 每分钟每IP上限
_WS_SEND_TIMEOUT: float = float(os.environ.get("EASYXT_WS_TIMEOUT", "0.1"))  # 慢消费者超时(秒)
_WS_MAX_QUEUE_SIZE: int = int(
    os.environ.get("EASYXT_WS_QUEUE_SIZE", "64")
)  # 每连接队列上限（满则丢帧）

# 丢帧率告警阈值（可通过环境变量覆盖）
_DROP_RATE_WARN: float = float(os.environ.get("EASYXT_DROP_RATE_WARN", "0.01"))  # 1%  → warning
_DROP_RATE_CRIT: float = float(os.environ.get("EASYXT_DROP_RATE_CRIT", "0.05"))  # 5%  → critical
_DROP_RATE_MIN_SAMPLES: int = int(
    os.environ.get("EASYXT_DROP_RATE_MIN_SAMPLES", "20")
)  # 1m 窗口最小样本量（不足时不判定告警）

# 构建版本信息（CI 注入，本地开发时为 "dev"）
_BUILD_VERSION: str = os.environ.get("EASYXT_BUILD_VERSION", "dev")
_COMMIT_SHA: str = os.environ.get("EASYXT_COMMIT_SHA", "unknown")
_ROOT_DIR: Path = Path(__file__).resolve().parents[1]
_GOVERNANCE_THRESHOLD_CONFIG_PATH: Path = Path(
    os.environ.get(
        "EASYXT_GOVERNANCE_THRESHOLD_CONFIG",
        str(_ROOT_DIR / "config" / "data_governance_thresholds.json"),
    )
)
_GOVERNANCE_ACTION_RULEBOOK_PATH: Path = Path(
    os.environ.get(
        "EASYXT_GOVERNANCE_ACTION_RULEBOOK",
        str(_ROOT_DIR / "config" / "governance_action_rulebook.json"),
    )
)
_GOVERNANCE_ACTION_AUDIT_PATH: Path = Path(
    os.environ.get(
        "EASYXT_GOVERNANCE_ACTION_AUDIT_LOG",
        str(_ROOT_DIR / "artifacts" / "governance_action_audit.jsonl"),
    )
)


class GovernanceSlaThresholdUpdateBody(BaseModel):
    overrides: dict[str, int]
    operator: str = "unknown"
    note: str = ""


class GovernanceActionAuditBody(BaseModel):
    action_id: str
    action_type: str
    tone: str = "neutral"
    title: str = ""
    detail: str = ""
    source: str = "tauri-data-route"
    payload: dict[str, Any] = {}

# ---------------------------------------------------------------------------
# Prometheus 指标定义（prometheus_client 可选；不可用时 /metrics 降级为 JSON）
# ---------------------------------------------------------------------------


def _init_prometheus() -> tuple[bool, Any, Any, Any, Any, Any, Any, Any]:
    """初始化 Prometheus 指标对象。返回 (enabled, registry, counter_rl, g_drop, g_drop1m, g_strat, g_queue, g_uptime)。"""
    try:
        from prometheus_client import CollectorRegistry, Counter, Gauge  # noqa: PLC0415

        reg = CollectorRegistry(auto_describe=False)
        c_rl = Counter("easyxt_rate_limit_hits_total", "累计限流命中次数", registry=reg)
        g_drop = Gauge("easyxt_ws_drop_rate", "WebSocket 全生命周期丢帧率", registry=reg)
        g_drop1m = Gauge(
            "easyxt_ws_drop_rate_1m", "WebSocket 最近60s丢帧率（-1=样本不足）", registry=reg
        )
        g_strat = Gauge("easyxt_strategies_running", "当前运行中策略数", registry=reg)
        g_queue = Gauge("easyxt_ws_queue_total_len", "所有WS连接队列积压帧总数", registry=reg)
        g_up = Gauge("easyxt_uptime_seconds", "服务运行时长（秒）", registry=reg)
        return True, reg, c_rl, g_drop, g_drop1m, g_strat, g_queue, g_up
    except Exception:  # pragma: no cover
        return False, None, None, None, None, None, None, None


(
    _prom_enabled,
    _prom_registry,
    _prom_rate_limit_hits,
    _prom_ws_drop_rate,
    _prom_ws_drop_rate_1m,
    _prom_strategies_running,
    _prom_ws_queue_len,
    _prom_uptime,
) = _init_prometheus()

# ---------------------------------------------------------------------------
# 限流：滑动窗口（每 IP 每 60 秒最多 _RATE_LIMIT 次）
# ---------------------------------------------------------------------------

_rate_buckets: dict[str, deque] = {}
_rate_limit_lock = threading.Lock()  # 保护 _rate_buckets 和 _rate_limit_hits 的并发访问
_rate_limit_hits: int = 0  # 限流命中累计计数（仅增不减，供监控采集）
_cleanup_stats: dict[str, Any] = {
    "last_run_epoch": None,  # 最近一次清理任务运行的 epoch(s)，None 表示尚未运行
    "last_removed_count": 0,  # 最近一次清理删除的 IP 桶数量
    "error_count": 0,  # 清理任务累计异常次数（任务活着但反复报错时可见）
}
_datasource_health_lock = threading.Lock()
_datasource_health_interface: Any = None
_data_governance_controller_lock = threading.Lock()
_data_governance_controller: Any = None


def _check_rate_limit(client_ip: str) -> bool:
    """返回 True 表示放行，False 表示已超限（同时递增命中计数）。线程安全。"""
    global _rate_limit_hits
    if _RATE_LIMIT <= 0:
        return True
    now = time.monotonic()
    with _rate_limit_lock:
        bucket = _rate_buckets.setdefault(client_ip, deque())
        while bucket and now - bucket[0] > 60.0:
            bucket.popleft()
        if len(bucket) >= _RATE_LIMIT:
            _rate_limit_hits += 1
            return False
        bucket.append(now)
    return True


def _get_datasource_health_interface() -> Any:
    global _datasource_health_interface
    if _datasource_health_interface is not None:
        return _datasource_health_interface
    with _datasource_health_lock:
        if _datasource_health_interface is None:
            from data_manager.unified_data_interface import UnifiedDataInterface

            duckdb_path = os.environ.get("EASYXT_DUCKDB_PATH", "") or None
            _datasource_health_interface = UnifiedDataInterface(
                duckdb_path=duckdb_path,
                eager_init=False,
                silent_init=True,
            )
    return _datasource_health_interface


def _get_data_governance_controller() -> Any:
    global _data_governance_controller
    if _data_governance_controller is not None:
        return _data_governance_controller
    with _data_governance_controller_lock:
        if _data_governance_controller is None:
            from gui_app.data_manager_controller import DataManagerController

            _data_governance_controller = DataManagerController()
    return _data_governance_controller


def _load_governance_threshold_overrides() -> dict[str, int]:
    return _load_governance_threshold_bundle()["overrides"]


def _load_governance_threshold_bundle() -> dict[str, Any]:
    try:
        if not _GOVERNANCE_THRESHOLD_CONFIG_PATH.exists():
            return {"overrides": {}, "config_version": 0, "updated_by": "unknown", "note": ""}
        payload = json.loads(_GOVERNANCE_THRESHOLD_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"overrides": {}, "config_version": 0, "updated_by": "unknown", "note": ""}
    if not isinstance(payload, dict):
        return {"overrides": {}, "config_version": 0, "updated_by": "unknown", "note": ""}
    overrides = payload.get("overrides", payload)
    if not isinstance(overrides, dict):
        return {"overrides": {}, "config_version": 0, "updated_by": "unknown", "note": ""}
    normalized: dict[str, int] = {}
    for key, value in overrides.items():
        try:
            normalized[str(key)] = int(value)
        except Exception:
            continue
    return {
        "overrides": normalized,
        "config_version": int(payload.get("config_version", 0) or 0),
        "updated_by": str(payload.get("updated_by", "unknown")),
        "note": str(payload.get("note", "")),
    }


def _save_governance_threshold_overrides(overrides: dict[str, int]) -> dict[str, int]:
    bundle = _save_governance_threshold_bundle(overrides=overrides, operator="unknown", note="")
    return bundle["overrides"]


def _save_governance_threshold_bundle(
    *,
    overrides: dict[str, int],
    operator: str,
    note: str,
) -> dict[str, Any]:
    normalized = {str(key): int(value) for key, value in overrides.items()}
    current = _load_governance_threshold_bundle()
    next_version = int(current.get("config_version", 0) or 0) + 1
    _GOVERNANCE_THRESHOLD_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _GOVERNANCE_THRESHOLD_CONFIG_PATH.write_text(
        json.dumps(
            {
                "config_version": next_version,
                "updated_by": operator or "unknown",
                "note": note,
                "updated_at": datetime.utcnow().isoformat() + "Z",
                "overrides": normalized,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "overrides": normalized,
        "config_version": next_version,
        "updated_by": operator or "unknown",
        "note": note,
    }


def _describe_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "updated_at": None,
        }
    stat = path.stat()
    payload_meta: dict[str, Any] = {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            if "config_version" in payload:
                payload_meta["config_version"] = int(payload.get("config_version", 0) or 0)
            if "updated_by" in payload:
                payload_meta["updated_by"] = str(payload.get("updated_by", "unknown"))
            if "note" in payload:
                payload_meta["note"] = str(payload.get("note", ""))
            if "version" in payload:
                payload_meta["version"] = str(payload.get("version", ""))
            if "maintainer" in payload:
                payload_meta["maintainer"] = str(payload.get("maintainer", ""))
    except Exception:
        payload_meta = {}
    return {
        "path": str(path),
        "exists": True,
        "updated_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "size_bytes": int(stat.st_size),
        **payload_meta,
    }


def _get_default_governance_action_rulebook() -> list[dict[str, Any]]:
    return [
        {
            "rule_id": "tick_mismatch_repair",
            "match_reason": "tick_mismatch",
            "severity": "warning",
            "sla_impact": "monitor",
            "recommended_action": "trigger_repair_then_open_workbench",
            "business_meaning": "tick 聚合无法解释分钟 bar，优先修复并回看主图确认。",
        },
        {
            "rule_id": "cross_source_conflict_traceability",
            "match_reason": "cross_source_conflict",
            "severity": "critical",
            "sla_impact": "gate_block",
            "recommended_action": "open_traceability_and_hold_publish",
            "business_meaning": "跨源冲突会直接降低可用性，应先排除数据源或对账口径问题。",
        },
        {
            "rule_id": "lineage_incomplete_replay",
            "match_reason": "lineage_incomplete",
            "severity": "warning",
            "sla_impact": "monitor",
            "recommended_action": "trigger_replay_and_review_lineage",
            "business_meaning": "回执链不完整会削弱审计闭环，应补 replay/repair 链路。",
        },
        {
            "rule_id": "contract_failed_traceability",
            "match_reason": "contract_failed",
            "severity": "critical",
            "sla_impact": "gate_block",
            "recommended_action": "open_traceability_and_stop_publish",
            "business_meaning": "时间戳/周期契约失败代表数据结构异常，应暂停放行。",
        },
    ]


def _get_governance_action_rulebook() -> list[dict[str, Any]]:
    default_rulebook = _get_default_governance_action_rulebook()
    try:
        if not _GOVERNANCE_ACTION_RULEBOOK_PATH.exists():
            return default_rulebook
        payload = json.loads(_GOVERNANCE_ACTION_RULEBOOK_PATH.read_text(encoding="utf-8"))
    except Exception:
        return default_rulebook
    if isinstance(payload, dict):
        rules = payload.get("rules", [])
    else:
        rules = payload
    if not isinstance(rules, list):
        return default_rulebook
    normalized: list[dict[str, Any]] = []
    for item in rules:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "rule_id": str(item.get("rule_id", "")),
                "match_reason": str(item.get("match_reason", "")),
                "severity": str(item.get("severity", "")),
                "sla_impact": str(item.get("sla_impact", "")),
                "recommended_action": str(item.get("recommended_action", "")),
                "business_meaning": str(item.get("business_meaning", "")),
            }
        )
    return normalized or default_rulebook


def _get_governance_action_rulebook_bundle() -> dict[str, Any]:
    rules = _get_governance_action_rulebook()
    return {
        "rules": rules,
        "meta": _describe_config_file(_GOVERNANCE_ACTION_RULEBOOK_PATH),
        "validation": _validate_governance_action_rulebook(rules),
    }


def _validate_governance_action_rulebook(rules: list[dict[str, Any]]) -> dict[str, Any]:
    required_fields = [
        "rule_id",
        "match_reason",
        "severity",
        "sla_impact",
        "recommended_action",
        "business_meaning",
    ]
    errors: list[str] = []
    allowed_severity = {"ok", "warning", "critical", "unknown"}
    for index, rule in enumerate(rules):
        for field in required_fields:
            if not str(rule.get(field, "")).strip():
                errors.append(f"rule[{index}].{field} 不能为空")
        severity = str(rule.get("severity", "")).strip().lower()
        if severity and severity not in allowed_severity:
            errors.append(f"rule[{index}].severity 非法: {severity}")
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "rule_count": len(rules),
        "required_fields": required_fields,
    }


def _append_governance_action_audit(
    *,
    action_id: str,
    action_type: str,
    tone: str,
    title: str,
    detail: str,
    source: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    stock_code = str(payload.get("stock_code") or payload.get("symbol") or "")
    period = str(payload.get("period") or "")
    lineage_anchor = str(payload.get("lineage_anchor") or "")
    operator = str(payload.get("operator") or "")
    config_version = payload.get("config_version")
    record = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.utcnow().isoformat() + "Z",
        "action_id": action_id,
        "action_type": action_type,
        "tone": tone,
        "title": title,
        "detail": detail,
        "source": source,
        "stock_code": stock_code,
        "period": period,
        "lineage_anchor": lineage_anchor,
        "operator": operator,
        "config_version": int(config_version or 0) if str(config_version or "").strip() else None,
        "payload": payload,
    }
    _GOVERNANCE_ACTION_AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _GOVERNANCE_ACTION_AUDIT_PATH.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


def _read_governance_action_audit(
    *,
    limit: int = 20,
    action_type: str = "",
    source: str = "",
    stock_code: str = "",
    period: str = "",
    lineage_anchor: str = "",
) -> list[dict[str, Any]]:
    if not _GOVERNANCE_ACTION_AUDIT_PATH.exists():
        return []
    records: list[dict[str, Any]] = []
    try:
        lines = _GOVERNANCE_ACTION_AUDIT_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        if action_type and str(item.get("action_type", "")) != action_type:
            continue
        if source and str(item.get("source", "")) != source:
            continue
        if stock_code and str(item.get("stock_code", "")) != stock_code:
            continue
        if period and str(item.get("period", "")) != period:
            continue
        if lineage_anchor and str(item.get("lineage_anchor", "")) != lineage_anchor:
            continue
        records.append(item)
        if len(records) >= max(int(limit), 1):
            break
    return records


def _build_governance_action_recommendations(
    receipt_timeline: list[dict[str, Any]],
    threshold_panel: dict[str, Any],
) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []
    first_item = receipt_timeline[0] if receipt_timeline else {}
    tick_item = next(
        (item for item in receipt_timeline if str(item.get("gate_reject_reason") or "") == "tick_mismatch"),
        None,
    )
    conflict_item = next(
        (item for item in receipt_timeline if str(item.get("gate_reject_reason") or "") == "cross_source_conflict"),
        None,
    )
    if threshold_panel.get("breaches", {}).get("gate_block"):
        recommendations.append(
            {
                "action_id": "sla_gate_block",
                "tone": "danger",
                "title": "SLA gate_block 超阈值",
                "detail": f"gate_block={threshold_panel.get('current', {}).get('gate_block', 0)}，建议先做溯源核查。",
                "action_type": "open_traceability",
                "payload": {
                    "stock_code": first_item.get("stock_code", ""),
                    "period": first_item.get("period", ""),
                },
            }
        )
    if tick_item:
        recommendations.append(
            {
                "action_id": "tick_mismatch",
                "tone": "warning",
                "title": "发现 tick_mismatch",
                "detail": "建议先触发 repair，再联动到图表复核分钟聚合。",
                "action_type": "trigger_repair",
                "payload": {
                    "stock_code": tick_item.get("stock_code", ""),
                    "period": tick_item.get("period", ""),
                    "lineage_anchor": tick_item.get("lineage_anchor", ""),
                },
            }
        )
    if conflict_item:
        recommendations.append(
            {
                "action_id": "cross_source_conflict",
                "tone": "danger",
                "title": "发现 cross_source_conflict",
                "detail": "建议转到 traceability 追源，不建议直接 replay。",
                "action_type": "open_traceability",
                "payload": {
                    "stock_code": conflict_item.get("stock_code", ""),
                    "period": conflict_item.get("period", ""),
                },
            }
        )
    if first_item and not recommendations:
        recommendations.append(
            {
                "action_id": "healthy_scan",
                "tone": "ok",
                "title": "当前未发现高优先级阻断",
                "detail": "建议继续做样本巡检并保留最新 receipt timeline 快照。",
                "action_type": "open_timeline",
                "payload": {
                    "stock_code": first_item.get("stock_code", ""),
                    "period": first_item.get("period", ""),
                },
            }
        )
    return recommendations[:4]


def _build_governance_snapshot_payload(trend_days: int, audit_limit: int) -> dict[str, Any]:
    overview = get_data_governance_overview(trend_days=trend_days)
    audit_records = _read_governance_action_audit(limit=audit_limit)
    return {
        "snapshot_name": f"data_governance_snapshot_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "overview": overview,
        "action_audit": audit_records,
        "config_sources": {
            "sla_thresholds": _describe_config_file(_GOVERNANCE_THRESHOLD_CONFIG_PATH),
            "action_rulebook": _describe_config_file(_GOVERNANCE_ACTION_RULEBOOK_PATH),
            "action_audit": _describe_config_file(_GOVERNANCE_ACTION_AUDIT_PATH),
        },
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


def _get_structure_query_db_manager() -> Any:
    """获取七层结构查询所需的 DuckDB 管理器，并确保结构表存在。"""
    from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path
    from data_manager.structure_schema import ensure_structure_tables

    db_mgr = get_db_manager(resolve_duckdb_path())
    ensure_structure_tables(db_mgr)
    return db_mgr


def _df_to_records(df: Any) -> list[dict[str, Any]]:
    """将 DataFrame 安全转为 JSON 友好的 records。"""
    if df is None or (hasattr(df, "empty") and df.empty):
        return []
    try:
        sanitized = df.where(df.notna(), other=None)
        return json.loads(sanitized.to_json(orient="records"))
    except Exception:
        return []


_CHART_INTERVAL_TO_BACKEND_PERIOD: dict[str, str] = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "60m",
    "4h": "240m",
    "1d": "1d",
    "1w": "1w",
}

_CHART_DEFAULT_RANGE: dict[str, timedelta] = {
    "1m": timedelta(days=5),
    "5m": timedelta(days=5),
    "15m": timedelta(days=15),
    "30m": timedelta(days=15),
    "1h": timedelta(days=15),
    "4h": timedelta(days=90),
    "1d": timedelta(days=365),
    "1w": timedelta(days=365 * 2),
}

_CHART_ADJUST_OPTIONS = {"none", "front", "back", "geometric_front", "geometric_back"}
_CHART_DATE_ONLY_PERIODS = {"1d", "1w"}


def _resolve_chart_backend_period(interval: str) -> str:
    normalized = str(interval or "").strip().lower()
    backend_period = _CHART_INTERVAL_TO_BACKEND_PERIOD.get(normalized)
    if backend_period is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "interval 参数非法，可选值: "
                f"{sorted(_CHART_INTERVAL_TO_BACKEND_PERIOD.keys())}"
            ),
        )
    return backend_period


def _resolve_chart_request_window(
    interval: str, start_date: str, end_date: str
) -> tuple[str, str]:
    end_dt = datetime.now()
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="end_date 必须为 YYYY-MM-DD 格式",
            ) from exc

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="start_date 必须为 YYYY-MM-DD 格式",
            ) from exc
    else:
        start_dt = end_dt - _CHART_DEFAULT_RANGE.get(interval, timedelta(days=30))

    if start_dt > end_dt:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="start_date 不能晚于 end_date",
        )

    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")


def _resolve_chart_available_window(
    interval: str, available_start: str, available_end: str
) -> tuple[str, str]:
    try:
        start_dt = datetime.strptime(available_start, "%Y-%m-%d")
        end_dt = datetime.strptime(available_end, "%Y-%m-%d")
    except ValueError:
        return available_start, available_end

    fallback_start = max(start_dt, end_dt - _CHART_DEFAULT_RANGE.get(interval, timedelta(days=30)))
    if fallback_start > end_dt:
        fallback_start = start_dt
    return fallback_start.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")


def _format_chart_bar_time(value: Any, requested_interval: str) -> str:
    import pandas as pd

    ts = pd.Timestamp(value)
    if requested_interval in _CHART_DATE_ONLY_PERIODS:
        return ts.strftime("%Y-%m-%d")
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _serialize_chart_bars(df: Any, requested_interval: str, limit: int) -> list[dict[str, Any]]:
    import pandas as pd

    if df is None or (hasattr(df, "empty") and df.empty):
        return []

    data = df.copy()
    if isinstance(getattr(data, "index", None), pd.DatetimeIndex):
        index_name = data.index.name or "index"
        if "datetime" not in data.columns and "time" not in data.columns:
            data = data.reset_index().rename(columns={index_name: "time"})
    data.columns = [str(col).lower() for col in data.columns]

    if "time" not in data.columns:
        if "datetime" in data.columns:
            data["time"] = data["datetime"]
        elif "date" in data.columns:
            data["time"] = data["date"]
        elif "index" in data.columns:
            data["time"] = data["index"]
        else:
            return []

    data["time"] = pd.to_datetime(data["time"], errors="coerce")
    data = data[data["time"].notna()].sort_values("time")

    required_cols = ["open", "high", "low", "close"]
    for col in required_cols:
        if col not in data.columns:
            return []

    if limit > 0:
        data = data.tail(limit)

    bars: list[dict[str, Any]] = []
    for row in data.itertuples(index=False):
        item = {
            "time": _format_chart_bar_time(getattr(row, "time"), requested_interval),
            "open": float(getattr(row, "open")),
            "high": float(getattr(row, "high")),
            "low": float(getattr(row, "low")),
            "close": float(getattr(row, "close")),
        }
        volume = getattr(row, "volume", None)
        if volume is not None:
            item["volume"] = float(volume)
        bars.append(item)
    return bars


def _build_chart_quality_payload(symbol: str) -> dict[str, Any]:
    from data_manager.golden_1d_audit import Golden1dAuditor

    def _serialize_repair_task(task: Any) -> dict[str, Any]:
        return {
            "stock_code": getattr(task, "stock_code", ""),
            "period": getattr(task, "period", "1d"),
            "start_date": getattr(task, "start_date", ""),
            "end_date": getattr(task, "end_date", ""),
            "reason": getattr(task, "reason", ""),
            "priority_hint": getattr(task, "priority_hint", None),
            "current_symbol": getattr(task, "current_symbol", ""),
            "gap_length": getattr(task, "gap_length", None),
        }

    def _default_repair_payload() -> dict[str, Any]:
        return {
            "plan_status": "unknown",
            "generated_at": None,
            "queued_tasks": 0,
            "failed_tasks": 0,
            "task_count": 0,
            "blocker_issues": [],
            "notes": [],
            "tasks": [],
        }

    def _build_golden_repair_payload(target_symbol: str) -> dict[str, Any]:
        try:
            from data_manager.golden_1d_repair_orchestrator import Golden1DRepairOrchestrator

            snapshot = Golden1DRepairOrchestrator().get_latest_plan(target_symbol)
            if snapshot is None:
                return _default_repair_payload()
            return {
                "plan_status": snapshot.plan_status,
                "generated_at": snapshot.generated_at,
                "queued_tasks": snapshot.queued_tasks,
                "failed_tasks": snapshot.failed_tasks,
                "task_count": snapshot.task_count,
                "blocker_issues": snapshot.blocker_issues[:5],
                "notes": snapshot.notes[:5],
                "tasks": [_serialize_repair_task(task) for task in snapshot.tasks[:5]],
            }
        except Exception:
            return _default_repair_payload()

    summary = Golden1dAuditor().get_audit_status(symbol)
    if summary is None:
        return {
            "golden_status": "unknown",
            "is_golden_1d_ready": False,
            "missing_days": None,
            "cross_source_status": "unknown",
            "backfill_status": "pending",
            "last_audited_at": None,
            "audit_anchor_date": None,
            "listing_date": None,
            "listing_date_confidence": "unknown",
            "issues": [],
            "repair": _build_golden_repair_payload(symbol),
        }

    listing_confidence = (
        "verified" if summary.listing_date and str(summary.listing_date) > "1990-01-01" else "fallback"
    )
    audit_anchor = (
        summary.listing_date if listing_confidence == "verified" else summary.local_first_date
    )
    return {
        "golden_status": summary.golden_status,
        "is_golden_1d_ready": summary.is_golden_1d_ready,
        "missing_days": summary.missing_days,
        "cross_source_status": summary.cross_source_status,
        "backfill_status": summary.backfill_status,
        "last_audited_at": summary.last_audited_at,
        "audit_anchor_date": audit_anchor,
        "listing_date": summary.listing_date,
        "listing_date_confidence": listing_confidence,
        "issues": summary.issues[:5],
        "repair": _build_golden_repair_payload(summary.symbol),
    }


def _serialize_structure_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "structure_id": row.get("structure_id"),
        "code": row.get("code"),
        "interval": row.get("interval"),
        "created_at": row.get("created_at"),
        "direction": row.get("direction"),
        "status": row.get("status"),
        "closed_at": row.get("closed_at"),
        "retrace_ratio": row.get("retrace_ratio"),
        "layer4": {
            "attractor_mean": row.get("attractor_mean"),
            "attractor_std": row.get("attractor_std"),
            "bayes_lower": row.get("bayes_lower"),
            "bayes_upper": row.get("bayes_upper"),
            "posterior_mean": row.get("posterior_mean"),
            "observation_count": row.get("observation_count"),
            "continuation_count": row.get("continuation_count"),
            "reversal_count": row.get("reversal_count"),
            "bayes_group_level": row.get("bayes_group_level"),
            "bayes_group_key": row.get("bayes_group_key"),
        },
        "points": {
            "p0": {"ts": row.get("p0_ts"), "price": row.get("p0_price")},
            "p1": {"ts": row.get("p1_ts"), "price": row.get("p1_price")},
            "p2": {"ts": row.get("p2_ts"), "price": row.get("p2_price")},
            "p3": {"ts": row.get("p3_ts"), "price": row.get("p3_price")},
        },
    }


def _serialize_audit_row(row: dict[str, Any]) -> dict[str, Any]:
    snapshot = None
    raw = row.get("snapshot_json")
    if isinstance(raw, str) and raw:
        try:
            snapshot = json.loads(raw)
        except Exception:
            snapshot = None
    return {
        "audit_id": row.get("audit_id"),
        "structure_id": row.get("structure_id"),
        "code": row.get("code"),
        "interval": row.get("interval"),
        "event_type": row.get("event_type"),
        "event_ts": row.get("event_ts"),
        "snapshot": snapshot,
    }


def _serialize_signal_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal_id": row.get("signal_id"),
        "structure_id": row.get("structure_id"),
        "code": row.get("code"),
        "interval": row.get("interval"),
        "signal_ts": row.get("signal_ts"),
        "signal_type": row.get("signal_type"),
        "trigger_price": row.get("trigger_price"),
        "risk": {
            "stop_loss_price": row.get("stop_loss_price"),
            "stop_loss_distance": row.get("stop_loss_distance"),
            "drawdown_pct": row.get("drawdown_pct"),
            "calmar_snapshot": row.get("calmar_snapshot"),
        },
        "remarks": row.get("remarks"),
    }


async def _cleanup_rate_buckets() -> None:
    """后台定期清理长时间无活动的 IP 限流桶，防止服务长期运行后内存无限增长。"""
    while True:
        await asyncio.sleep(300)  # 每 5 分钟扫描一次
        now = time.monotonic()
        try:
            with _rate_limit_lock:
                stale = [
                    ip
                    for ip, bucket in _rate_buckets.items()
                    if not bucket or now - bucket[-1] > 300.0
                ]
                for ip in stale:
                    del _rate_buckets[ip]
            _cleanup_stats["last_run_epoch"] = int(time.time())
            _cleanup_stats["last_removed_count"] = len(stale)
            if stale:
                log.debug("限流桶清理: 移除 %d 个过期 IP 桶", len(stale))
        except Exception:  # pragma: no cover
            _cleanup_stats["error_count"] = _cleanup_stats.get("error_count", 0) + 1
            log.exception("限流桶清理任务异常")


# ---------------------------------------------------------------------------
# 鉴权 + 限流组合依赖（/health 端点不使用）
# ---------------------------------------------------------------------------


async def _verify_auth_and_rate(
    request: Request,
    x_api_token: str = Header(default=""),
) -> None:
    """
    FastAPI 依赖：限流 + Token 鉴权。

    - 限流：每 IP 每分钟最多 _RATE_LIMIT 次（EASYXT_RATE_LIMIT env）
    - 鉴权：比对 EASYXT_API_TOKEN env；为空时跳过（开发模式）
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="请求过于频繁，请稍后再试",
        )
    if _API_TOKEN and (not x_api_token or not secrets.compare_digest(x_api_token, _API_TOKEN)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效或缺失的 X-API-Token",
        )


# ---------------------------------------------------------------------------
# 统一错误响应格式
# ---------------------------------------------------------------------------

_HTTP_MESSAGES: dict[int, str] = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    409: "Conflict",
    422: "Unprocessable Entity",
    429: "Too Many Requests",
    500: "Internal Server Error",
}

# ---------------------------------------------------------------------------
# WebSocket 广播器
# ---------------------------------------------------------------------------


class _MarketBroadcaster:
    """
    管理 WebSocket 订阅的行情广播器（per-connection 队列模型）。

    协议约定（客户端去重键：symbol + seq）：
      {"symbol": "...", "price": ..., "event_ts_ms": <ms>, "seq": <int>, "source": "..."}

    稳定性保证：
      - 每个连接独立 asyncio.Queue（上限 _WS_MAX_QUEUE_SIZE），队列满即丢帧并计数
      - broadcast 仅做 put_nowait（纯内存操作），不阻塞生产者协程
      - 每个 WS 连接有独立 drain 协程负责实际发送，发送失败后自动清理
      - seq 单调递增，客户端可检测丢帧

    可观测指标（通过 /health 暴露）：
      - drop_counts()          — 各标的累计丢帧数（队列满时丢弃）
      - drop_rate              — 全生命周期丢帧率（总丢帧 / 总尝试）
      - drop_rate_1m           — 最近 60 s 窗口丢帧率（可感知瞬时抖动）
      - drop_alert_level       — ok / warning / critical（基于 drop_rate_1m）
      - queue_depths()         — 各连接当前队列水位（可检测慢消费者积压）
      - avg_publish_latency_ms — broadcast 循环内 put_nowait 平均耗时（微秒级，用于基线监控）
    """

    # 延迟滑动窗口：取最近 N 次 broadcast 的耗时均值
    _LATENCY_WINDOW = 100
    # 时间窗口丢帧率：统计最近 _WINDOW_SECS 秒内的事件
    _WINDOW_SECS: int = 60
    _EVENT_WINDOW_MAX: int = 10_000  # 最多保留条目数（100 次/秒 × ~100 s）

    def __init__(self) -> None:
        self._channels: dict[str, set[WebSocket]] = {}
        self._seq: dict[str, int] = {}  # per-symbol 单调递增序号
        self._queues: dict[WebSocket, asyncio.Queue] = {}
        self._drain_tasks: dict[WebSocket, asyncio.Task] = {}
        self._drop_counts: dict[str, int] = {}  # per-symbol 丢帧累计
        self._total_attempted: int = 0  # 全生命周期 put_nowait 调用总次数（含成功与丢帧）
        # publish_latency 滑动窗口（单位 ms，仅统计有订阅者时的 broadcast 耗时）
        self._latency_window: deque = deque(maxlen=self._LATENCY_WINDOW)
        # 时间窗口事件：每次有效 broadcast 追加 (monotonic_ts, attempted, dropped)
        self._event_window: deque = deque(maxlen=self._EVENT_WINDOW_MAX)

    async def _drain(self, ws: WebSocket, symbol: str) -> None:
        """每个 WS 连接的独立消耗协程：从队列取帧 → send_text。

        注意：使用 asyncio.wait({task}, timeout) 而非 asyncio.wait_for(coro, timeout)
        避免 Python 3.11 + pytest-asyncio 1.3.0 中 asyncio.wait_for 内部 call_later
        回调在事件循环关闭阶段导致的挂起问题。
        """
        queue = self._queues.get(ws)
        if queue is None:
            return
        try:
            while True:
                msg = await queue.get()
                if msg is None:  # sentinel：正常关闭
                    break
                try:
                    send_task = asyncio.ensure_future(ws.send_text(msg))
                    done, pending = await asyncio.wait({send_task}, timeout=_WS_SEND_TIMEOUT)
                    if pending:
                        send_task.cancel()
                        await asyncio.gather(send_task, return_exceptions=True)
                        break  # 慢消费者：超时后退出 drain
                    else:
                        send_task.result()  # 传播发送异常
                except Exception as exc:
                    log.debug("WS 发送失败 symbol=%s error=%s", symbol, exc)
                    break  # 连接已死，退出 drain
        except asyncio.CancelledError:
            pass
        finally:
            # 幂等清理（可能已由 unsubscribe 先执行）
            self._queues.pop(ws, None)
            self._drain_tasks.pop(ws, None)
            self._channels.get(symbol, set()).discard(ws)

    async def asubscribe(self, symbol: str, ws: WebSocket) -> None:
        """订阅：创建专属队列并启动 drain 协程（需在 event loop 中调用）。"""
        self._channels.setdefault(symbol, set()).add(ws)
        queue: asyncio.Queue = asyncio.Queue(maxsize=_WS_MAX_QUEUE_SIZE)
        self._queues[ws] = queue
        self._drain_tasks[ws] = asyncio.create_task(self._drain(ws, symbol))

    def unsubscribe(self, symbol: str, ws: WebSocket) -> None:
        """退出订阅（同步）：从频道移除并取消 drain 任务。"""
        self._channels.get(symbol, set()).discard(ws)
        self._queues.pop(ws, None)
        task = self._drain_tasks.pop(ws, None)
        if task and not task.done():
            task.cancel()

    def subscriber_count(self, symbol: str) -> int:
        return len(self._channels.get(symbol, set()))

    def all_symbols(self) -> list[str]:
        return [s for s, ch in self._channels.items() if ch]

    def drop_counts(self) -> dict[str, int]:
        """返回各标的累计丢帧数（队列满时丢弃）。"""
        return dict(self._drop_counts)

    def queue_depths(self) -> dict[str, int]:
        """返回每个活跃 WS 连接的当前队列水位（key 为连接对象 id 的字符串）。"""
        return {str(id(ws)): q.qsize() for ws, q in self._queues.items()}

    @property
    def avg_publish_latency_ms(self) -> float | None:
        """最近 _LATENCY_WINDOW 次 broadcast 的平均耗时（ms），无数据时返回 None。"""
        if not self._latency_window:
            return None
        return round(sum(self._latency_window) / len(self._latency_window), 3)

    @property
    def max_publish_latency_ms(self) -> float | None:
        """最近 _LATENCY_WINDOW 次 broadcast 的最大耗时（ms），无数据时返回 None。

        用于灰度阶段感知尾延迟：单次异常帧（如 GC 停顿、事件循环阻塞）
        在均值中被稀释，但会在 max 上显现，适合告警触发基准。
        """
        if not self._latency_window:
            return None
        return round(max(self._latency_window), 3)

    @property
    def drop_rate(self) -> float:
        """
        全生命周期丢帧率 = total_drops / total_attempted。

        语义：每 100 次帧投递尝试中有多少帧被丢弃（慢消费者）。
        0.0 表示无丢帧；> 0.01（1%）建议触发告警。
        """
        total_drops = sum(self._drop_counts.values())
        if self._total_attempted == 0:
            return 0.0
        return round(total_drops / self._total_attempted, 4)

    @property
    def drop_rate_1m(self) -> float:
        """
        最近 60 s 窗口丢帧率 = drops_1m / attempted_1m。0.0 表示无数据或无丢帧。

        用途：相比全生命周期 drop_rate，1m 窗口对瞬时抖动更敏感，适合告警触发。
        样本量不足 _DROP_RATE_MIN_SAMPLES 时返回 -1.0（表示低样本状态）。
        """
        cutoff = time.monotonic() - self._WINDOW_SECS
        attempted_w = sum(a for ts, a, _ in self._event_window if ts >= cutoff)
        dropped_w = sum(d for ts, _, d in self._event_window if ts >= cutoff)
        if attempted_w == 0:
            return 0.0
        if attempted_w < _DROP_RATE_MIN_SAMPLES:
            return -1.0  # 哨兵值：表示样本量不足，不应计入告警判断
        return round(dropped_w / attempted_w, 4)

    @property
    def drop_alert_level(self) -> str:
        """
        基于近 1 分钟丢帧率的告警级别，优先感知瞬时抖动。

        级别：
          ok            — drop_rate_1m < _DROP_RATE_WARN（默认 1%）
          ok_low_sample — 1m 内样本量 < _DROP_RATE_MIN_SAMPLES，不判定告警（默认 20）
          warning       — drop_rate_1m in [1%, 5%)
          critical      — drop_rate_1m ≥ _DROP_RATE_CRIT（默认 5%）

        阈值可通过 EASYXT_DROP_RATE_WARN / EASYXT_DROP_RATE_CRIT 环境变量覆盖。
        """
        dr1m = self.drop_rate_1m
        if dr1m < 0:  # 哨兵值：样本量不足
            return "ok_low_sample"
        if dr1m >= _DROP_RATE_CRIT:
            return "critical"
        if dr1m >= _DROP_RATE_WARN:
            return "warning"
        return "ok"

    def _next_seq(self, symbol: str) -> int:
        self._seq[symbol] = self._seq.get(symbol, 0) + 1
        return self._seq[symbol]

    async def broadcast(self, symbol: str, payload: dict) -> None:
        """
        广播行情：put_nowait 到各订阅队列，队列满则丢帧并计数。

        本方法不做任何网络 I/O，广播延迟由各连接的 drain 协程承担。
        publish_latency_ms 统计本方法从入口到全部 put_nowait 完成的耗时。
        """
        t0 = time.monotonic()
        seq = self._next_seq(symbol)
        now_ms = int(time.time() * 1000)
        out_payload = dict(payload)
        if out_payload.get("source_event_ts_ms") in (None, ""):
            src_ts = out_payload.get("event_ts_ms")
            if src_ts not in (None, ""):
                out_payload["source_event_ts_ms"] = src_ts
        if out_payload.get("event_ts_ms") in (None, ""):
            out_payload["event_ts_ms"] = now_ms
        out_payload["gateway_event_ts_ms"] = now_ms
        msg = json.dumps(
            {**out_payload, "seq": seq},
            ensure_ascii=False,
        )
        attempts = 0
        dropped = 0
        for ws in list(self._channels.get(symbol, set())):
            queue = self._queues.get(ws)
            if queue is None:
                continue
            attempts += 1
            self._total_attempted += 1  # 全生命周期计数
            try:
                queue.put_nowait(msg)
            except asyncio.QueueFull:
                dropped += 1
        if dropped:
            self._drop_counts[symbol] = self._drop_counts.get(symbol, 0) + dropped
            log.warning("广播丢帧 symbol=%s dropped=%d（队列满，慢消费者）", symbol, dropped)
        # 有效广播（至少一个订阅者）时记录延迟和时间窗口事件
        if attempts > 0:
            elapsed_ms = (time.monotonic() - t0) * 1000
            self._latency_window.append(elapsed_ms)
            self._event_window.append((t0, attempts, dropped))


broadcaster = _MarketBroadcaster()

# ---------------------------------------------------------------------------
# 线程→事件循环桥接（QMT 回调注入实时行情）
# ---------------------------------------------------------------------------

_server_loop: asyncio.AbstractEventLoop | None = None
_server_start_time: float | None = None  # monotonic 启动时刻，用于计算 uptime_s


def _diag_logging_enabled() -> bool:
    return str(os.environ.get("EASYXT_QMT_DIAG", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def ingest_tick_from_thread(symbol: str, tick_data: dict) -> None:
    """
    从非异步线程（如 QMT xtdata 回调）注入实时行情，线程安全。

    使用 run_coroutine_threadsafe 将广播协程提交到服务事件循环，
    不阻塞回调线程。若服务未启动则静默丢弃。

    接入 QMT 示例::

        from core.api_server import ingest_tick_from_thread

        def on_tick(data):
            for symbol, tick in data.items():
                ingest_tick_from_thread(symbol, {
                    "price": tick["lastPrice"],
                    "volume": tick["volume"],
                    "source": "qmt_live",
                })

        from xtquant import xtdata
        xtdata.subscribe_quote("000001.SZ", period="tick", callback=on_tick)
    """
    if _diag_logging_enabled():
        log.warning(
            "[DIAG] ingest_tick_from_thread symbol=%s price=%s source=%s",
            symbol,
            tick_data.get("price"),
            tick_data.get("source", "unknown"),
        )

    if _server_loop is None or _server_loop.is_closed():
        return
    asyncio.run_coroutine_threadsafe(broadcaster.broadcast(symbol, tick_data), _server_loop)


# ---------------------------------------------------------------------------
# Pydantic 模型
# ---------------------------------------------------------------------------


class StrategyStatusPatch(BaseModel):
    status: str  # "running" | "paused" | "stopped" | "error"


class SubscribeRequest(BaseModel):
    symbol: str
    period: str = "tick"  # "tick" | "1m" | "5m" | "1d"


class AccountRegisterBody(BaseModel):
    account_id: str
    broker: str = ""
    enabled: bool = True


# ---------------------------------------------------------------------------
# App 生命周期
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _server_loop, _server_start_time
    _server_loop = asyncio.get_event_loop()
    _server_start_time = time.monotonic()
    _cleanup_task = asyncio.create_task(_cleanup_rate_buckets())
    if not _API_TOKEN:
        if _DEV_MODE or _TEST_MODE:
            log.warning(
                "⚠️  [DEV_MODE] EASYXT_API_TOKEN 未设置，鉴权已跳过（仅限本地开发）。"
                " 生产部署必须设置 EASYXT_API_TOKEN 并移除 EASYXT_DEV_MODE=1。"
            )
        else:
            raise RuntimeError(
                "EASYXT_API_TOKEN 未设置，服务拒绝启动。\n"
                "  生产环境：设置 EASYXT_API_TOKEN=<secret>\n"
                "  本地开发：设置 EASYXT_DEV_MODE=1（不得用于生产）"
            )
    log.info(
        "EasyXT 中台服务启动 (auth=%s, dev_mode=%s, rate_limit=%d req/min, ws_timeout=%.2fs)",
        "enabled" if _API_TOKEN else "disabled(DEV)",
        _DEV_MODE,
        _RATE_LIMIT,
        _WS_SEND_TIMEOUT,
    )
    yield
    _cleanup_task.cancel()
    _server_loop = None
    log.info("EasyXT 中台服务关闭")


app = FastAPI(
    title="EasyXT 中台 API",
    version="1.0.0",
    description="统一行情、交易与策略管理接口层",
    lifespan=_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PATCH"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """
    统一 HTTP 错误响应格式：
      {"code": <int>, "message": <str>, "detail": <str>, "trace_id": <uuid>}

    trace_id 用于日志追踪，每次请求唯一。
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "code": exc.status_code,
            "message": _HTTP_MESSAGES.get(exc.status_code, "Error"),
            "detail": exc.detail,
            "trace_id": str(uuid.uuid4()),
        },
    )


# ---------------------------------------------------------------------------
# 健康检查
# ---------------------------------------------------------------------------


@app.get("/health", tags=["运维"])
def health_check() -> dict:
    """服务健康检查（无需鉴权，适用于负载均衡探针）。"""
    uptime = (
        round(time.monotonic() - _server_start_time, 1) if _server_start_time is not None else None
    )

    # --- registry 子检查 ---
    try:
        from strategies.registry import strategy_registry

        running_count = len(strategy_registry.list_running())
        registry_status = "ok"
    except Exception:
        running_count = -1
        registry_status = "error"

    # --- ws 子检查（内存结构，始终可用） ---
    ws_symbols = broadcaster.all_symbols()
    ws_cleanup = {
        "last_run_epoch": _cleanup_stats["last_run_epoch"],
        "last_removed_count": _cleanup_stats["last_removed_count"],
        "error_count": _cleanup_stats.get("error_count", 0),
    }
    total_queue_len = sum(broadcaster.queue_depths().values())

    # --- db 子检查（轻量探针；失败仅标记 unavailable，不影响整体状态） ---
    try:
        from data_manager.duckdb_connection_pool import get_db_manager

        get_db_manager()
        db_status = "ok"
    except Exception:
        db_status = "unavailable"

    # 聚合：注册中心异常才降级，DB 离线属软故障
    agg_status = "ok" if registry_status == "ok" else "degraded"

    return {
        "status": agg_status,
        "checks": {
            "registry": {"status": registry_status, "strategies_running": running_count},
            "ws": {
                "status": "ok",
                "symbols": ws_symbols,
                "cleanup": ws_cleanup,
                "drop_counts": broadcaster.drop_counts(),
                "drop_rate": broadcaster.drop_rate,
                "drop_rate_1m": broadcaster.drop_rate_1m,
                "drop_alert": broadcaster.drop_alert_level,
                "drop_alert_thresholds": {
                    "warn": _DROP_RATE_WARN,
                    "crit": _DROP_RATE_CRIT,
                    "min_samples": _DROP_RATE_MIN_SAMPLES,
                },
                "queue_len": total_queue_len,
                "publish_latency_ms": broadcaster.avg_publish_latency_ms,
                "publish_latency_max_ms": broadcaster.max_publish_latency_ms,
            },
            "db": {"status": db_status},
        },
        # 以下平铺字段保持向后兼容（与旧版调用方/探针保持契约）
        "server_time": int(time.time() * 1000),
        "strategies_running": running_count,
        "ws_symbols": ws_symbols,
        "auth_enabled": bool(_API_TOKEN),
        "rate_limit_hits": _rate_limit_hits,
        "uptime_s": uptime,
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


@app.get("/health/datasource", tags=["运维"])
def datasource_health_check() -> dict[str, Any]:
    payload: dict[str, Any] = {"status": "ok", "checks": {}}
    try:
        iface = _get_datasource_health_interface()
        summary = iface.data_registry.get_health_summary()
        payload["checks"]["sources"] = summary
        payload["checks"]["circuit_breaker"] = dict(getattr(iface, "_cb_state", {}) or {})
        q_counts = iface.get_quarantine_status_counts()
        payload["checks"]["quarantine"] = q_counts
        total = int(q_counts.get("total", 0) or 0)
        dead = int(q_counts.get("dead_letter", 0) or 0)
        dead_ratio = (dead / total) if total > 0 else 0.0
        payload["checks"]["quarantine"]["dead_letter_ratio"] = dead_ratio
        payload["checks"]["data_quality_incident"] = iface.get_data_quality_incident_counts()
        payload["checks"]["step6_validation"] = iface.get_step6_validation_metrics()
        payload["checks"]["publish_gate"] = iface.get_publish_gate_summary()
        dl_abs_warn = int(os.environ.get("EASYXT_QUARANTINE_DEADLETTER_WARN", "100") or 100)
        dl_ratio_warn = float(
            os.environ.get("EASYXT_QUARANTINE_DEADLETTER_RATIO_WARN", "0.01") or 0.01
        )
        step6_sample_rate = float(os.environ.get("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "1.0") or 1.0)
        canary_shadow_write = str(os.environ.get("EASYXT_CANARY_SHADOW_WRITE", "0")).lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        canary_shadow_only = str(os.environ.get("EASYXT_CANARY_SHADOW_ONLY", "1")).lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        payload["checks"]["thresholds"] = {
            "dead_letter_abs_warn": dl_abs_warn,
            "dead_letter_ratio_warn": dl_ratio_warn,
            "step6_validate_sample_rate": step6_sample_rate,
            "canary_shadow_write_enabled": canary_shadow_write,
            "canary_shadow_only": canary_shadow_only,
        }
        if dead >= dl_abs_warn or dead_ratio >= dl_ratio_warn:
            payload["status"] = "degraded"
        if int(payload["checks"]["publish_gate"].get("degraded", 0) or 0) > 0:
            payload["status"] = "degraded"
    except Exception as e:
        payload["status"] = "degraded"
        payload["checks"]["error"] = str(e)
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.get(
    "/api/v1/data-quality/ingestion-status",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_ingestion_gate_status(
    symbol: str = Query(..., description="标的代码，例如 000001.SZ"),
    period: str = Query("1d", description="周期代码"),
) -> dict[str, Any]:
    try:
        iface = _get_datasource_health_interface()
        payload = iface.get_latest_gate_status(symbol, period)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"门禁状态查询失败: {exc}",
        ) from exc
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未找到 {symbol} / {period} 的门禁状态",
        )
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.get(
    "/api/v1/data-quality/receipts",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_receipt_history(
    receipt_type: str = Query(..., pattern="^(publish_gate|repair|replay)$", description="回执类型"),
    symbol: str = Query("", description="标的代码，可选"),
    period: str = Query("", description="周期代码，可选"),
    limit: int = Query(default=20, ge=1, le=100, description="返回条数"),
) -> dict[str, Any]:
    try:
        iface = _get_datasource_health_interface()
        items = iface.get_receipt_history(
            receipt_type,
            symbol=symbol,
            period=period,
            limit=limit,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"回执历史查询失败: {exc}",
        ) from exc
    return {
        "receipt_type": receipt_type,
        "items": items,
        "returned": len(items),
        "limit": limit,
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


@app.get(
    "/api/v1/data-quality/receipt-timeline",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_receipt_timeline(
    symbol: str = Query("", description="标的代码，可选"),
    period: str = Query("", description="周期代码，可选"),
    lineage_anchor: str = Query("", description="lineage 锚点，可选"),
    receipt_type: str = Query("", pattern="^(|publish_gate|repair|replay)$", description="回执类型过滤"),
    gate_reject_reason: str = Query("", description="gate 拒绝原因过滤"),
    severity: str = Query("", pattern="^(|ok|warning|critical|unknown)$", description="严重度过滤"),
    lookback_days: int = Query(default=0, ge=0, le=365, description="时间窗口天数，0表示不限"),
    limit: int = Query(default=50, ge=1, le=200, description="返回条数"),
) -> dict[str, Any]:
    try:
        iface = _get_datasource_health_interface()
        items = iface.get_receipt_timeline(
            symbol=symbol,
            period=period,
            lineage_anchor=lineage_anchor,
            receipt_type=receipt_type,
            gate_reject_reason=gate_reject_reason,
            severity=severity,
            lookback_days=lookback_days,
            limit=limit,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"回执时间线查询失败: {exc}",
        ) from exc
    return {
        "items": items,
        "returned": len(items),
        "filters": {
            "symbol": symbol,
            "period": period,
            "lineage_anchor": lineage_anchor,
            "receipt_type": receipt_type,
            "gate_reject_reason": gate_reject_reason,
            "severity": severity,
            "lookback_days": lookback_days,
            "limit": limit,
        },
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


@app.get(
    "/api/v1/data-quality/lineage-anchor-detail",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_lineage_anchor_detail(
    lineage_anchor: str = Query(..., description="lineage 锚点"),
) -> dict[str, Any]:
    try:
        iface = _get_datasource_health_interface()
        payload = iface.get_lineage_anchor_detail(lineage_anchor)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"lineage 锚点详情查询失败: {exc}",
        ) from exc
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未找到 lineage_anchor={lineage_anchor} 对应的回执链",
        )
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.get(
    "/api/v1/data-governance/sla-thresholds",
    tags=["数据治理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_data_governance_sla_thresholds() -> dict[str, Any]:
    iface = _get_datasource_health_interface()
    threshold_bundle = _load_governance_threshold_bundle()
    overrides = threshold_bundle["overrides"]
    panel = iface.get_sla_alert_threshold_panel_with_overrides(overrides)
    return {
        "overrides": overrides,
        "panel": panel,
        "config_meta": _describe_config_file(_GOVERNANCE_THRESHOLD_CONFIG_PATH),
        "config_version": int(threshold_bundle.get("config_version", 0) or 0),
        "updated_by": str(threshold_bundle.get("updated_by", "unknown")),
        "note": str(threshold_bundle.get("note", "")),
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


@app.patch(
    "/api/v1/data-governance/sla-thresholds",
    tags=["数据治理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def patch_data_governance_sla_thresholds(body: GovernanceSlaThresholdUpdateBody) -> dict[str, Any]:
    iface = _get_datasource_health_interface()
    threshold_bundle = _save_governance_threshold_bundle(
        overrides=body.overrides,
        operator=body.operator,
        note=body.note,
    )
    overrides = threshold_bundle["overrides"]
    panel = iface.get_sla_alert_threshold_panel_with_overrides(overrides)
    audit_record = _append_governance_action_audit(
        action_id="sla_threshold_update",
        action_type="update_sla_thresholds",
        tone="warning" if panel.get("status") != "ok" else "ok",
        title="更新 SLA 阈值",
        detail=f"已写入 {len(overrides)} 个阈值覆盖项",
        source="api_server",
        payload={
            "overrides": overrides,
            "panel_status": panel.get("status"),
            "operator": threshold_bundle["updated_by"],
            "config_version": threshold_bundle["config_version"],
        },
    )
    return {
        "overrides": overrides,
        "panel": panel,
        "config_meta": _describe_config_file(_GOVERNANCE_THRESHOLD_CONFIG_PATH),
        "config_version": int(threshold_bundle.get("config_version", 0) or 0),
        "updated_by": str(threshold_bundle.get("updated_by", "unknown")),
        "note": str(threshold_bundle.get("note", "")),
        "audit_record": audit_record,
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


@app.get(
    "/api/v1/data-governance/action-audit",
    tags=["数据治理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_governance_action_audit(
    limit: int = Query(default=20, ge=1, le=200, description="返回条数"),
    action_type: str = Query("", description="动作类型过滤"),
    source: str = Query("", description="来源过滤"),
    stock_code: str = Query("", description="标的过滤"),
    period: str = Query("", description="周期过滤"),
    lineage_anchor: str = Query("", description="lineage 锚点过滤"),
) -> dict[str, Any]:
    records = _read_governance_action_audit(
        limit=limit,
        action_type=action_type,
        source=source,
        stock_code=stock_code,
        period=period,
        lineage_anchor=lineage_anchor,
    )
    return {
        "records": records,
        "returned": len(records),
        "filters": {
            "limit": limit,
            "action_type": action_type,
            "source": source,
            "stock_code": stock_code,
            "period": period,
            "lineage_anchor": lineage_anchor,
        },
        "config_meta": _describe_config_file(_GOVERNANCE_ACTION_AUDIT_PATH),
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


@app.post(
    "/api/v1/data-governance/action-audit",
    tags=["数据治理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def create_governance_action_audit(body: GovernanceActionAuditBody) -> dict[str, Any]:
    record = _append_governance_action_audit(
        action_id=body.action_id,
        action_type=body.action_type,
        tone=body.tone,
        title=body.title,
        detail=body.detail,
        source=body.source,
        payload=body.payload,
    )
    return {
        "record": record,
        "config_meta": _describe_config_file(_GOVERNANCE_ACTION_AUDIT_PATH),
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


@app.get(
    "/api/v1/data-governance/export-snapshot",
    tags=["数据治理"],
    response_model=None,
    dependencies=[Depends(_verify_auth_and_rate)],
)
def export_data_governance_snapshot(
    trend_days: int = Query(default=7, ge=1, le=365, description="趋势窗口天数"),
    audit_limit: int = Query(default=50, ge=1, le=500, description="附带审计日志条数"),
    export_format: str = Query(default="json", pattern="^(json|jsonl|csv)$", description="导出格式"),
) -> Any:
    payload = _build_governance_snapshot_payload(trend_days=trend_days, audit_limit=audit_limit)
    snapshot_name = str(payload["snapshot_name"])
    if export_format == "json":
        return payload
    if export_format == "jsonl":
        lines = [
            json.dumps({"record_type": "snapshot_meta", "snapshot_name": payload["snapshot_name"], "generated_at": payload["generated_at"]}, ensure_ascii=False),
            json.dumps({"record_type": "summary", "summary": payload["overview"].get("summary", {})}, ensure_ascii=False),
        ]
        for item in payload["action_audit"]:
            lines.append(json.dumps({"record_type": "action_audit", **item}, ensure_ascii=False))
        return Response(
            content="\n".join(lines) + "\n",
            media_type="application/x-ndjson",
            headers={"Content-Disposition": f'attachment; filename="{snapshot_name}.jsonl"'},
        )
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["section", "key", "value"])
    for key, value in payload["overview"].get("summary", {}).items():
        writer.writerow(["summary", key, value])
    for item in payload["action_audit"]:
        writer.writerow(
            [
                "action_audit",
                item.get("event_id", ""),
                json.dumps(
                    {
                        "event_time": item.get("event_time"),
                        "action_type": item.get("action_type"),
                        "stock_code": item.get("stock_code"),
                        "period": item.get("period"),
                        "detail": item.get("detail"),
                    },
                    ensure_ascii=False,
                ),
            ]
        )
    return Response(
        content=csv_buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{snapshot_name}.csv"'},
    )


@app.get("/health/sla", tags=["运维"])
def sla_health_check(report_date: str = "") -> dict[str, Any]:
    """
    数据质量 SLA 报告（当日或指定日期）。

    - `report_date`: 可选，格式 YYYY-MM-DD，默认为今天。
    - `gate_pass=false` 时 status 返回 "degraded"。
    """
    payload: dict[str, Any] = {"status": "ok"}
    try:
        iface = _get_datasource_health_interface()
        payload["sla"] = iface.generate_daily_sla_report(report_date or None)
        if not payload["sla"].get("gate_pass", True):
            payload["status"] = "degraded"
    except Exception as e:
        payload["status"] = "degraded"
        payload["error"] = str(e)
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.get(
    "/api/v1/system/state-status",
    tags=["系统状态"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_system_state_status() -> dict[str, Any]:
    """返回状态主线与影子同步的真实快照，供 Tauri SystemRoute 直接消费。"""
    try:
        from core.state_store.system_status import get_system_state_snapshot

        snapshot = get_system_state_snapshot().to_dict()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"系统状态查询失败: {exc}",
        ) from exc

    snapshot["server_time"] = int(time.time() * 1000)
    snapshot["build_version"] = _BUILD_VERSION
    snapshot["commit_sha"] = _COMMIT_SHA
    return snapshot


@app.get(
    "/api/v1/data-quality/golden-1d-status",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_golden_1d_status(symbol: str = Query("", description="标的代码，留空返回汇总")) -> dict[str, Any]:
    """查询黄金标准 1D 数据质量状态。

    - `symbol`: 标的代码（如 000001.SZ），留空时返回全量汇总
    - 返回 golden/partial_trust/degraded/unknown 状态
    - 供 Qt/Tauri 图表左上角质量叠层消费
    """
    try:
        from data_manager.golden_1d_audit import Golden1dAuditor

        auditor = Golden1dAuditor()

        def _serialize_repair_snapshot(target_symbol: str) -> dict[str, Any]:
            from data_manager.golden_1d_repair_orchestrator import Golden1DRepairOrchestrator

            snapshot = Golden1DRepairOrchestrator(auditor=auditor).get_latest_plan(target_symbol)
            if snapshot is None:
                return {
                    "plan_status": "unknown",
                    "generated_at": None,
                    "queued_tasks": 0,
                    "failed_tasks": 0,
                    "task_count": 0,
                    "blocker_issues": [],
                    "notes": [],
                    "tasks": [],
                }
            return {
                "plan_status": snapshot.plan_status,
                "generated_at": snapshot.generated_at,
                "queued_tasks": snapshot.queued_tasks,
                "failed_tasks": snapshot.failed_tasks,
                "task_count": snapshot.task_count,
                "blocker_issues": snapshot.blocker_issues[:5],
                "notes": snapshot.notes[:5],
                "tasks": [
                    {
                        "stock_code": task.stock_code,
                        "period": task.period,
                        "start_date": task.start_date,
                        "end_date": task.end_date,
                        "reason": task.reason,
                        "priority_hint": task.priority_hint,
                        "current_symbol": task.current_symbol,
                        "gap_length": task.gap_length,
                    }
                    for task in snapshot.tasks[:5]
                ],
            }

        if symbol:
            summary = auditor.get_audit_status(symbol)
            if summary is None:
                return {
                    "symbol": symbol,
                    "status": "unknown",
                    "message": "该标的尚未执行审计",
                    "repair": _serialize_repair_snapshot(symbol),
                    "server_time": int(time.time() * 1000),
                }
            return {
                "symbol": summary.symbol,
                "golden_status": summary.golden_status,
                "is_golden_1d_ready": summary.is_golden_1d_ready,
                "listing_date": summary.listing_date,
                "local_first_date": summary.local_first_date,
                "local_last_date": summary.local_last_date,
                "expected_trading_days": summary.expected_trading_days,
                "actual_trading_days": summary.actual_trading_days,
                "missing_days": summary.missing_days,
                "has_listing_gap": summary.has_listing_gap,
                "cross_source_status": summary.cross_source_status,
                "cross_source_fields_passed": f"{summary.cross_source_fields_passed}/{summary.cross_source_fields_total}",
                "backfill_status": summary.backfill_status,
                "last_audited_at": summary.last_audited_at,
                "issues": summary.issues[:5],
                "repair": _serialize_repair_snapshot(summary.symbol),
                "server_time": int(time.time() * 1000),
            }
        else:
            import sqlite3

            conn = sqlite3.connect(auditor.audit_db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT golden_status, COUNT(*) as cnt FROM golden_1d_audit GROUP BY golden_status"
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) as cnt FROM golden_1d_audit").fetchone()["cnt"]
            conn.close()

            summary = {"golden": 0, "partial_trust": 0, "degraded": 0, "unknown": 0}
            for row in rows:
                summary[row["golden_status"]] = row["cnt"]

            return {
                "total_audited": total,
                "golden_count": summary["golden"],
                "partial_trust_count": summary["partial_trust"],
                "degraded_count": summary["degraded"],
                "unknown_count": summary["unknown"],
                "golden_ratio": summary["golden"] / total if total > 0 else 0.0,
                "server_time": int(time.time() * 1000),
            }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"黄金标准 1D 状态查询失败: {exc}",
        ) from exc


@app.get(
    "/api/v1/data-quality/golden-1d-repair-plan",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_golden_1d_repair_plan(
    symbol: str = Query("", description="标的代码，留空返回最近 repair plans"),
    limit: int = Query(default=20, ge=1, le=100, description="批量查询时返回最近 plan 条数"),
) -> dict[str, Any]:
    """查询 Golden 1D 后台修复编排状态。"""
    try:
        from data_manager.golden_1d_repair_orchestrator import Golden1DRepairOrchestrator

        orchestrator = Golden1DRepairOrchestrator()

        def _serialize_snapshot(snapshot: Any) -> dict[str, Any]:
            summary_snapshot = (
                snapshot.summary_snapshot if isinstance(getattr(snapshot, "summary_snapshot", None), dict) else {}
            )
            return {
                "symbol": snapshot.symbol,
                "plan_status": snapshot.plan_status,
                "generated_at": snapshot.generated_at,
                "queued_tasks": snapshot.queued_tasks,
                "failed_tasks": snapshot.failed_tasks,
                "task_count": snapshot.task_count,
                "blocker_issues": snapshot.blocker_issues[:5],
                "notes": snapshot.notes[:5],
                "governance": summary_snapshot.get("governance", {}),
                "tasks": [
                    {
                        "stock_code": task.stock_code,
                        "period": task.period,
                        "start_date": task.start_date,
                        "end_date": task.end_date,
                        "reason": task.reason,
                        "priority_hint": task.priority_hint,
                        "current_symbol": task.current_symbol,
                        "gap_length": task.gap_length,
                    }
                    for task in snapshot.tasks[:5]
                ],
            }

        if symbol:
            snapshot = orchestrator.get_latest_plan(symbol)
            if snapshot is None:
                return {
                    "symbol": symbol,
                    "plan_status": "unknown",
                    "generated_at": None,
                    "queued_tasks": 0,
                    "failed_tasks": 0,
                    "task_count": 0,
                    "blocker_issues": [],
                    "notes": [],
                    "tasks": [],
                    "server_time": int(time.time() * 1000),
                }
            payload = _serialize_snapshot(snapshot)
            payload["server_time"] = int(time.time() * 1000)
            return payload

        snapshots = orchestrator.list_recent_plans(limit=limit)
        return {
            "items": [_serialize_snapshot(item) for item in snapshots],
            "returned": len(snapshots),
            "limit": limit,
            "server_time": int(time.time() * 1000),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Golden 1D repair plan 查询失败: {exc}",
        ) from exc


@app.post(
    "/api/v1/data-quality/golden-1d-repair",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def trigger_golden_1d_repair(
    symbol: str = Query("", description="标的代码，留空执行限量批量 repair orchestration"),
    force_full: bool = Query(default=False, description="是否先强制全量复审再执行 repair orchestration"),
    limit: int = Query(default=25, ge=1, le=200, description="批量 repair 时最多处理的标的数"),
) -> dict[str, Any]:
    """手动触发 Golden 1D repair orchestration。"""
    try:
        from data_manager.golden_1d_audit import Golden1dAuditor
        from data_manager.golden_1d_repair_orchestrator import Golden1DRepairOrchestrator

        auditor = Golden1dAuditor()
        orchestrator = Golden1DRepairOrchestrator(auditor=auditor)

        if symbol:
            result = orchestrator.audit_and_schedule(symbol, force_full=force_full, current_symbol=symbol)
            snapshot = orchestrator.get_latest_plan(symbol)
            audit_record = _append_governance_action_audit(
                action_id="trigger_golden_1d_repair",
                action_type="trigger_repair",
                tone="warning" if result.status != "complete" else "ok",
                title="触发 Golden 1D Repair",
                detail=f"{symbol} -> {result.status}",
                source="api_server",
                payload={"symbol": symbol, "force_full": force_full, "status": result.status},
            )
            return {
                "symbol": symbol,
                "status": result.status,
                "queued_tasks": result.queued_tasks,
                "failed_tasks": result.failed_tasks,
                "blocker_issues": result.blocker_issues[:5],
                "notes": result.notes[:5],
                "force_full": force_full,
                "repair": {
                    "plan_status": snapshot.plan_status if snapshot else "unknown",
                    "generated_at": snapshot.generated_at if snapshot else None,
                    "queued_tasks": snapshot.queued_tasks if snapshot else 0,
                    "failed_tasks": snapshot.failed_tasks if snapshot else 0,
                    "task_count": snapshot.task_count if snapshot else 0,
                    "blocker_issues": snapshot.blocker_issues[:5] if snapshot else [],
                    "notes": snapshot.notes[:5] if snapshot else [],
                    "tasks": [
                        {
                            "stock_code": task.stock_code,
                            "period": task.period,
                            "start_date": task.start_date,
                            "end_date": task.end_date,
                            "reason": task.reason,
                            "priority_hint": task.priority_hint,
                            "current_symbol": task.current_symbol,
                            "gap_length": task.gap_length,
                        }
                        for task in (snapshot.tasks[:5] if snapshot else [])
                    ],
                },
                "audit_record": audit_record,
                "server_time": int(time.time() * 1000),
            }

        symbols = auditor.list_stored_symbols(limit=limit)
        items: list[dict[str, Any]] = []
        status_counts: dict[str, int] = {}
        for item_symbol in symbols[:limit]:
            result = orchestrator.audit_and_schedule(item_symbol, force_full=force_full)
            status_counts[result.status] = int(status_counts.get(result.status, 0)) + 1
            items.append(
                {
                    "symbol": item_symbol,
                    "status": result.status,
                    "queued_tasks": result.queued_tasks,
                    "failed_tasks": result.failed_tasks,
                }
            )
        audit_record = _append_governance_action_audit(
            action_id="trigger_golden_1d_repair_batch",
            action_type="trigger_repair_batch",
            tone="warning" if status_counts.get("blocked", 0) or status_counts.get("failed", 0) else "ok",
            title="批量触发 Golden 1D Repair",
            detail=f"processed={len(items)}",
            source="api_server",
            payload={"force_full": force_full, "limit": limit, "status_counts": status_counts},
        )
        return {
            "processed": len(items),
            "status_counts": status_counts,
            "force_full": force_full,
            "limit": limit,
            "items": items,
            "audit_record": audit_record,
            "server_time": int(time.time() * 1000),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Golden 1D repair 触发失败: {exc}",
        ) from exc


@app.post(
    "/api/v1/data-quality/late-event-replay",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def trigger_late_event_replay(
    symbol: str = Query("", description="标的代码，可选"),
    period: str = Query("", description="周期代码，可选"),
    limit: int = Query(default=20, ge=1, le=200, description="最大处理条数"),
    max_retries: int = Query(default=3, ge=1, le=10, description="最大重试次数"),
    reason_regex: str = Query(
        default=r"(late|out_of_order|watermark|stale|reorder)",
        description="reason 正则过滤",
    ),
) -> dict[str, Any]:
    try:
        iface = _get_datasource_health_interface()
        result = iface.run_late_event_replay(
            limit=limit,
            max_retries=max_retries,
            reason_regex=reason_regex,
            stock_code=symbol,
            period=period,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"late event replay 触发失败: {exc}",
        ) from exc
    audit_record = _append_governance_action_audit(
        action_id="trigger_late_event_replay",
        action_type="trigger_replay",
        tone="warning" if int(result.get("failed", 0) or 0) > 0 else "ok",
        title="触发 Late Event Replay",
        detail=f"{symbol or 'ALL'} / {period or 'ALL'} -> succeeded={result.get('succeeded', 0)}",
        source="api_server",
        payload={
            "symbol": symbol,
            "period": period,
            "limit": limit,
            "max_retries": max_retries,
            "reason_regex": reason_regex,
            "result": result,
        },
    )
    return {
        "symbol": symbol,
        "period": period,
        "result": result,
        "limit": limit,
        "max_retries": max_retries,
        "reason_regex": reason_regex,
        "audit_record": audit_record,
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


@app.post(
    "/api/v1/data-quality/golden-1d-audit",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def trigger_golden_1d_audit(
    symbol: str = Query("", description="标的代码，留空审计全部"),
    force_full: bool = Query(default=False, description="是否忽略分区 hash 缓存并执行全量重验"),
    limit: int = Query(default=50, ge=1, le=5000, description="批量审计时最多处理的标的数"),
) -> dict[str, Any]:
    """触发黄金标准 1D 数据质量审计。

    - `symbol`: 标的代码，留空时执行全量审计
    - 执行 DAT 直读 + 全历史逐日穷举 + 1m→1d 不变量验证
    """
    try:
        from data_manager.golden_1d_audit import Golden1dAuditor

        auditor = Golden1dAuditor()

        if symbol:
            summary = auditor.audit_symbol(symbol, force_full=force_full)
            return {
                "symbol": summary.symbol,
                "golden_status": summary.golden_status,
                "is_golden_1d_ready": summary.is_golden_1d_ready,
                "missing_days": summary.missing_days,
                "force_full": force_full,
                "issues": summary.issues[:5],
                "server_time": int(time.time() * 1000),
            }
        else:
            symbols = auditor.list_stored_symbols(limit=limit)
            if not symbols:
                symbols = ["000001.SZ", "000002.SZ", "600000.SH"]

            report = auditor.audit_batch(symbols[:limit], max_workers=4, force_full=force_full)
            return {
                "total_audited": report.total_symbols,
                "golden_count": report.golden_count,
                "partial_trust_count": report.partial_trust_count,
                "degraded_count": report.degraded_count,
                "unknown_count": report.unknown_count,
                "force_full": force_full,
                "limit": limit,
                "audited_at": report.audited_at,
                "server_time": int(time.time() * 1000),
            }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"黄金标准 1D 审计触发失败: {exc}",
        ) from exc


@app.get(
    "/api/v1/system/frontend-events",
    tags=["系统状态"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_system_frontend_events(
    event_type: str = "",
    start_time: str = "",
    end_time: str = "",
    limit: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    """通过 federation executor 读取状态主线中的 frontend_events 读模型。"""
    try:
        from core.state_store.system_read_models import read_frontend_events_read_model

        payload = read_frontend_events_read_model(
            limit=limit,
            event_type=event_type or None,
            start_time=start_time or None,
            end_time=end_time or None,
        ).to_dict()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"系统事件查询失败: {exc}",
        ) from exc

    payload["filters"] = {
        "event_type": event_type,
        "start_time": start_time,
        "end_time": end_time,
        "limit": limit,
    }
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.get(
    "/api/v1/data-governance/overview",
    tags=["数据治理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_data_governance_overview(
    sla_report_date: str = "",
    trend_days: int = Query(default=7, ge=1, le=365, description="趋势与时间线窗口天数"),
) -> dict[str, Any]:
    """聚合数据治理 Route 所需的只读快照。"""
    try:
        controller = _get_data_governance_controller()
        iface = _get_datasource_health_interface()
        datasource_health = datasource_health_check()
        sla_health = sla_health_check(sla_report_date)
        pipeline = controller.get_pipeline_status()
        routing = controller.get_routing_metrics()
        duckdb = controller.get_duckdb_summary()
        environment = controller.get_all_env_config()
        realtime = controller.get_realtime_pipeline_info()
        receipt_store = iface.get_receipt_store_summary()
        publish_gate = iface.get_publish_gate_summary()
        reject_reasons = iface.get_gate_reject_reason_summary()
        reject_severity = iface.get_gate_reject_severity_summary()
        gate_sla_impact = iface.get_gate_sla_impact_summary()
        threshold_bundle = _load_governance_threshold_bundle()
        threshold_overrides = threshold_bundle["overrides"]
        receipt_timeline = iface.get_receipt_timeline(limit=12, lookback_days=trend_days)
        gate_trend = iface.get_gate_trend_summary(days=trend_days)
        gate_trend_by_symbol = iface.get_gate_dimension_trend_summary(days=trend_days, dimension="symbol", limit=5)
        gate_trend_by_period = iface.get_gate_dimension_trend_summary(days=trend_days, dimension="period", limit=5)
        sla_threshold_panel = iface.get_sla_alert_threshold_panel_with_overrides(threshold_overrides)
        rulebook_bundle = _get_governance_action_rulebook_bundle()
        governance_action_rulebook = rulebook_bundle["rules"]
        governance_action_recommendations = _build_governance_action_recommendations(
            receipt_timeline=receipt_timeline,
            threshold_panel=sla_threshold_panel,
        )
        recent_action_audit = _read_governance_action_audit(limit=12)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"数据治理概览查询失败: {exc}",
        ) from exc

    return {
        "datasource_health": datasource_health,
        "sla_health": sla_health,
        "pipeline": pipeline,
        "routing": routing,
        "duckdb": duckdb,
        "environment": environment,
        "realtime": realtime,
        "receipts": {
            "store": receipt_store,
            "publish_gate": publish_gate,
            "gate_reject_reasons": reject_reasons,
            "gate_reject_severity": reject_severity,
            "gate_sla_impact": gate_sla_impact,
            "sla_threshold_panel": sla_threshold_panel,
            "sla_threshold_overrides": threshold_overrides,
            "sla_threshold_config_meta": _describe_config_file(_GOVERNANCE_THRESHOLD_CONFIG_PATH),
            "sla_threshold_version": int(threshold_bundle.get("config_version", 0) or 0),
            "sla_threshold_updated_by": str(threshold_bundle.get("updated_by", "unknown")),
            "sla_threshold_note": str(threshold_bundle.get("note", "")),
            "action_rulebook": governance_action_rulebook,
            "action_rulebook_meta": rulebook_bundle["meta"],
            "action_rulebook_validation": rulebook_bundle["validation"],
            "action_recommendations": governance_action_recommendations,
            "action_audit_recent": recent_action_audit,
            "action_audit_meta": _describe_config_file(_GOVERNANCE_ACTION_AUDIT_PATH),
            "timeline": receipt_timeline,
            "trend_7d": gate_trend,
            "trend_by_symbol_7d": gate_trend_by_symbol,
            "trend_by_period_7d": gate_trend_by_period,
        },
        "summary": {
            "datasource_status": datasource_health.get("status", "unknown"),
            "sla_status": sla_health.get("status", "unknown"),
            "pipeline_healthy": bool(pipeline.get("overall_healthy", False)),
            "healthy_sources": int(routing.get("healthy_sources", 0) or 0),
            "total_sources": int(routing.get("total_sources", 0) or 0),
            "duckdb_healthy": bool(duckdb.get("healthy", False)),
            "env_valid": bool(environment.get("overall_valid", False)),
            "realtime_connected": realtime.get("connected"),
            "gate_degraded": int(publish_gate.get("degraded", 0) or 0),
            "gate_reject_total": sum(int(v or 0) for k, v in reject_reasons.items() if k != "passed"),
            "gate_critical": int(reject_severity.get("critical", 0) or 0),
            "gate_warning": int(reject_severity.get("warning", 0) or 0),
            "sla_gate_block": int(gate_sla_impact.get("gate_block", 0) or 0),
            "sla_monitor": int(gate_sla_impact.get("monitor", 0) or 0),
            "repair_receipts": int(receipt_store.get("repair", 0) or 0),
            "replay_receipts": int(receipt_store.get("replay", 0) or 0),
        },
        "filters": {"sla_report_date": sla_report_date, "trend_days": trend_days},
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


@app.get(
    "/api/v1/data-governance/trading-calendar",
    tags=["数据治理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_data_governance_trading_calendar(start_date: str, end_date: str) -> dict[str, Any]:
    """返回 DataRoute 使用的交易日历摘要与列表。"""
    try:
        payload = _get_data_governance_controller().get_trading_calendar_info(
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"交易日历查询失败: {exc}",
        ) from exc

    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.get(
    "/api/v1/data-governance/traceability",
    tags=["数据治理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_data_governance_traceability(
    stock_code: str = "",
    period: str = "",
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    """返回逐标的数据来源溯源记录。"""
    try:
        payload = _get_data_governance_controller().get_ingestion_traceability(
            stock_code=stock_code or None,
            period=period or None,
            limit=limit,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"数据溯源查询失败: {exc}",
        ) from exc

    payload["filters"] = {
        "stock_code": stock_code,
        "period": period,
        "limit": limit,
    }
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


# ---------------------------------------------------------------------------
# 策略注册表 REST API
# ---------------------------------------------------------------------------


@app.get("/api/v1/strategies/", tags=["策略管理"], dependencies=[Depends(_verify_auth_and_rate)])
def list_strategies(status_filter: str = "") -> list[dict]:
    """
    枚举所有已注册策略。

    - `status_filter` 可选过滤：running / stopped / error（空则返回全部）
    """
    from strategies.registry import strategy_registry

    items = strategy_registry.list_all()
    if status_filter:
        items = [i for i in items if i["status"] == status_filter]
    return items


@app.get(
    "/api/v1/strategies/{strategy_id}",
    tags=["策略管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_strategy(strategy_id: str) -> dict:
    """获取单个策略详情。"""
    from strategies.registry import strategy_registry

    info = strategy_registry.get(strategy_id)
    if info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"策略 {strategy_id!r} 未找到",
        )
    return {
        "strategy_id": info.strategy_id,
        "account_id": info.account_id,
        "status": info.status,
        "tags": info.tags,
        "params": info.params,
        "registered_at": info.registered_at,
        "has_instance": info.strategy_obj is not None,
    }


@app.patch(
    "/api/v1/strategies/{strategy_id}/status",
    tags=["策略管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def patch_strategy_status(strategy_id: str, body: StrategyStatusPatch) -> dict:
    """
    更新策略状态（状态机约束，非法转换返回 409）。

    允许值：running / paused / stopped / error
    转换规则：
      created → running | stopped
      running → paused | stopped | error
      paused  → running | stopped
      error   → running | stopped
      stopped → （终态，拒绝一切转换）
    """
    allowed = {"running", "paused", "stopped", "error"}
    if body.status not in allowed:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"非法状态值 {body.status!r}，可选：{sorted(allowed)}",
        )

    from strategies.registry import strategy_registry

    result = strategy_registry.update_status(strategy_id, body.status)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"策略 {strategy_id!r} 未找到",
        )
    ok, reason = result
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"非法状态转换: {reason}",
        )
    return {"strategy_id": strategy_id, "status": body.status, "updated": True}


@app.post(
    "/api/v1/strategies/snapshot", tags=["策略管理"], dependencies=[Depends(_verify_auth_and_rate)]
)
def snapshot_all_strategies() -> dict:
    """触发全量策略参数快照写入 DuckDB（每次追加新记录）。"""
    from strategies.registry import strategy_registry

    written = strategy_registry.snapshot_to_db()
    return {"snapshot_written": written}


# ---------------------------------------------------------------------------
# 行情快照（HTTP）
# ---------------------------------------------------------------------------


@app.get(
    "/api/v1/market/snapshot/{symbol}", tags=["行情"], dependencies=[Depends(_verify_auth_and_rate)]
)
def get_market_snapshot(symbol: str) -> dict:
    """
    获取标的最新行情快照。

    优先从 DuckDB 缓存读取，不可用时返回占位响应。
    """
    try:
        from data_manager import unified_data_interface

        get_latest_tick = getattr(unified_data_interface, "get_latest_tick", None)
        tick = get_latest_tick(symbol) if callable(get_latest_tick) else None
        if tick is not None:
            return {"symbol": symbol, "data": tick, "source": "duckdb"}
    except Exception:
        pass

    return {
        "symbol": symbol,
        "data": None,
        "source": "unavailable",
        "message": "行情数据暂不可用，请启动 QMT 或等待数据同步",
    }


@app.get(
    "/api/v1/chart/bars",
    tags=["图表"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_chart_bars(
    symbol: str = Query(..., description="标的代码，如 000001.SZ"),
    interval: str = Query(default="1d", description="图表周期，支持 1m/5m/15m/30m/1h/4h/1d/1w"),
    start_date: str = Query(default="", description="开始日期，YYYY-MM-DD；留空按周期默认窗口"),
    end_date: str = Query(default="", description="结束日期，YYYY-MM-DD；留空默认今天"),
    adjust: str = Query(default="none", description="复权类型"),
    limit: int = Query(default=800, ge=1, le=5000, description="最多返回 bars 数量"),
) -> dict[str, Any]:
    """返回 Workbench 图表主舞台使用的 K 线 bars 与 Golden 1D 质量元数据。"""
    if adjust not in _CHART_ADJUST_OPTIONS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"adjust 参数非法，可选值: {sorted(_CHART_ADJUST_OPTIONS)}",
        )

    requested_interval = str(interval or "1d").strip().lower()
    backend_period = _resolve_chart_backend_period(requested_interval)
    requested_start_supplied = bool(start_date)
    requested_end_supplied = bool(end_date)
    start_at, end_at = _resolve_chart_request_window(requested_interval, start_date, end_date)

    try:
        iface = _get_datasource_health_interface()
        if getattr(iface, "con", None) is None:
            try:
                iface.connect(read_only=False)
            except Exception:
                pass

        listing_date = getattr(iface, "get_listing_date", lambda _symbol: None)(symbol)

        def _load_frame(window_start: str, window_end: str):
            local_reader = getattr(iface, "_read_from_duckdb", None)
            if callable(local_reader):
                return local_reader(
                    symbol,
                    window_start,
                    window_end,
                    backend_period,
                    adjust,
                    listing_date=listing_date,
                )
            return iface.get_stock_data(
                stock_code=symbol,
                start_date=window_start,
                end_date=window_end,
                period=backend_period,
                adjust=adjust,
                auto_save=False,
            )

        df = _load_frame(start_at, end_at)

        if df is None or (hasattr(df, "empty") and df.empty):
            date_range_getter = getattr(iface, "get_stock_date_range", None)
            if (
                callable(date_range_getter)
                and not requested_start_supplied
                and not requested_end_supplied
            ):
                available_window = date_range_getter(symbol, backend_period)
                if available_window:
                    fallback_start, fallback_end = _resolve_chart_available_window(
                        requested_interval,
                        available_window[0],
                        available_window[1],
                    )
                    if (fallback_start, fallback_end) != (start_at, end_at):
                        start_at, end_at = fallback_start, fallback_end
                        df = _load_frame(start_at, end_at)

        bars = _serialize_chart_bars(df, requested_interval, limit)
        quality = _build_chart_quality_payload(symbol)
        return {
            "symbol": symbol,
            "interval": requested_interval,
            "resolved_period": backend_period,
            "adjust": adjust,
            "start_date": start_at,
            "end_date": end_at,
            "bar_count": len(bars),
            "bars": bars,
            "quality": quality,
            "server_time": int(time.time() * 1000),
            "build_version": _BUILD_VERSION,
            "commit_sha": _COMMIT_SHA,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"图表 bars 查询失败: {exc}",
        ) from exc


# ---------------------------------------------------------------------------

@app.get(
    "/api/v1/accounts/{account_id}",
    tags=["账户管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_account_api(account_id: str) -> dict:
    """获取单个账户详情。"""
    from core.account_registry import account_registry

    data = account_registry.get_account(account_id)
    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"账户 {account_id!r} 未找到",
        )
    return data


@app.delete(
    "/api/v1/accounts/{account_id}",
    tags=["账户管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def delete_account_api(account_id: str) -> dict:
    """注销账户（幂等：不存在时返回 404）。"""
    from core.account_registry import account_registry

    deleted = account_registry.delete_account(account_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"账户 {account_id!r} 未找到",
        )
    return {"account_id": account_id, "deleted": True}


# ---------------------------------------------------------------------------
# 行情订阅管理（QMT xtdata）
# ---------------------------------------------------------------------------


@app.post("/api/v1/market/subscribe", tags=["行情"], dependencies=[Depends(_verify_auth_and_rate)])
def subscribe_symbol(req: SubscribeRequest) -> dict:
    """
    订阅标的实时行情（通过 QMT xtdata）。

    QMT 不可用时返回 source=error（禁止 mock 降级）。
    重复订阅同一标的安全幂等。
    """
    try:
        from core.qmt_feed import qmt_feed

        result = qmt_feed.subscribe(req.symbol, req.period)
    except Exception as exc:
        result = {"subscribed": False, "source": "error", "message": str(exc)}
    return {"symbol": req.symbol, "period": req.period, **result}


@app.delete(
    "/api/v1/market/subscribe/{symbol}",
    tags=["行情"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def unsubscribe_symbol(symbol: str) -> dict:
    """取消订阅指定标的实时行情。"""
    try:
        from core.qmt_feed import qmt_feed

        result = qmt_feed.unsubscribe(symbol)
    except Exception as exc:
        result = {"unsubscribed": False, "message": str(exc)}
    return {"symbol": symbol, **result}


@app.get(
    "/api/v1/market/subscriptions", tags=["行情"], dependencies=[Depends(_verify_auth_and_rate)]
)
def list_subscriptions() -> dict:
    """列出当前所有 QMT 实时行情订阅及统计信息。"""
    try:
        from core.qmt_feed import qmt_feed

        subs = qmt_feed.all_subscriptions()
        stats = qmt_feed.stats()
    except Exception:
        subs = []
        stats = {}
    return {"subscriptions": subs, "stats": stats}


# ---------------------------------------------------------------------------
# 行情 WebSocket
# ---------------------------------------------------------------------------


@app.websocket("/ws/market/{symbol}")
async def ws_market(
    websocket: WebSocket,
    symbol: str,
    token: str = Query(default=""),
) -> None:
    """
    实时行情推送（WebSocket）。

    鉴权：通过 ?token=<api_token> 查询参数（EASYXT_API_TOKEN 为空时不校验）。
    数据格式：{"symbol": ..., "price": ..., "event_ts_ms": <ms>, "seq": <int>, "source": ...}
    客户端去重键：symbol + seq
    数据通过 ingest_tick_from_thread() 从 QMT 实时推送（无 mock）。
    """
    if _API_TOKEN and (not token or not secrets.compare_digest(token, _API_TOKEN)):
        await websocket.close(code=4001, reason="Unauthorized")
        return

    await websocket.accept()
    await broadcaster.asubscribe(symbol, websocket)
    log.info("WS 订阅 symbol=%s 当前订阅数=%d", symbol, broadcaster.subscriber_count(symbol))

    # 自动通过 QMT 订阅该标的实时行情（禁止 mock 降级）
    try:
        from core.qmt_feed import qmt_feed as _qf

        if not _qf.is_subscribed(symbol):
            _qf.subscribe(symbol, period="tick")
            log.info("WS 触发自动订阅 symbol=%s via qmt_feed", symbol)
    except Exception as exc:
        log.warning("WS 自动订阅失败 symbol=%s: %s", symbol, exc)

    try:
        while True:
            data = await websocket.receive_text()
            if data.strip().lower() in ("ping", '{"type":"ping"}'):
                await websocket.send_text('{"type":"pong"}')
    except WebSocketDisconnect:
        pass
    finally:
        broadcaster.unsubscribe(symbol, websocket)
        log.info("WS 断开 symbol=%s 剩余订阅数=%d", symbol, broadcaster.subscriber_count(symbol))


# ---------------------------------------------------------------------------
# 财务数据 REST API
# ---------------------------------------------------------------------------


@app.get(
    "/api/v1/data/financial/{stock_code}",
    tags=["数据查询"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_financial_data(
    stock_code: str,
    start_date: str = "",
    end_date: str = "",
    table: str = "",
) -> dict[str, Any]:
    """
    查询股票财务数据（利润表 / 资产负债表 / 现金流量表）。

    - `stock_code`: EasyXT 格式，如 ``000001.SZ``
    - `start_date` / `end_date`: 可选，格式 ``YYYY-MM-DD``，筛选报告期范围
    - `table`: 可选过滤，``income`` / ``balance`` / ``cashflow``，空=返回三表
    """
    try:
        from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path
        from data_manager.financial_data_saver import FinancialDataSaver

        db_mgr = get_db_manager(resolve_duckdb_path())
        saver = FinancialDataSaver(db_mgr)
        raw = saver.load_financial_data(
            stock_code=stock_code,
            start_date=start_date or None,
            end_date=end_date or None,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"财务数据查询失败: {exc}",
        ) from exc

    def _df_to_records(df: Any) -> list[dict]:
        if df is None or (hasattr(df, "empty") and df.empty):
            return []
        try:
            return df.where(df.notna(), other=None).to_dict(orient="records")
        except Exception:
            return []

    allowed_tables = {"income", "balance", "cashflow"}
    if table and table not in allowed_tables:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"table 参数非法，可选值: {sorted(allowed_tables)}",
        )

    payload: dict[str, Any] = {
        "stock_code": stock_code,
        "start_date": start_date,
        "end_date": end_date,
        "server_time": int(time.time() * 1000),
    }
    if not table or table == "income":
        payload["income"] = _df_to_records(raw.get("income"))
    if not table or table == "balance":
        payload["balance"] = _df_to_records(raw.get("balance"))
    if not table or table == "cashflow":
        payload["cashflow"] = _df_to_records(raw.get("cashflow"))
    return payload


@app.post(
    "/api/v1/data/financial/{stock_code}/refresh",
    tags=["数据查询"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def refresh_financial_data(
    stock_code: str,
    start_date: str = "",
    end_date: str = "",
) -> dict[str, Any]:
    """
    触发单只股票财务数据刷新（优先 QMT，降级 Tushare）。

    - `stock_code`: EasyXT 格式，如 ``000001.SZ``
    - `start_date` / `end_date`: 可选报告期范围，格式 ``YYYY-MM-DD``
    """
    try:
        from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path
        from data_manager.financial_data_saver import FinancialDataSaver

        db_mgr = get_db_manager(resolve_duckdb_path())
        saver = FinancialDataSaver(db_mgr)

        # 尝试 QMT 路径
        qmt_result: dict[str, Any] = {"success": False, "skip_reason": "not_attempted"}
        try:
            iface = _get_datasource_health_interface()
            if getattr(iface, "qmt_available", False):
                import pandas as pd
                from xtquant import xtdata  # type: ignore[import]

                raw = xtdata.get_financial_data(
                    stock_list=[stock_code],
                    table_list=["Income", "Balance", "CashFlow"],
                    start_time="",
                    end_time="",
                )
                stock_raw = (raw or {}).get(stock_code, {})
                qmt_result = saver.save_from_qmt(
                    stock_code,
                    stock_raw.get("Income", pd.DataFrame()),
                    stock_raw.get("Balance", pd.DataFrame()),
                    stock_raw.get("CashFlow", pd.DataFrame()),
                )
            else:
                qmt_result["skip_reason"] = "qmt_unavailable"
        except Exception as exc:
            qmt_result["skip_reason"] = str(exc)

        # 若 QMT 未写入任何数据，降级到 Tushare
        ts_result: dict[str, Any] = {"success": False, "skip_reason": "not_attempted"}
        qmt_wrote = (
            qmt_result.get("success")
            and (
                int(qmt_result.get("income_count", 0))
                + int(qmt_result.get("balance_count", 0))
                + int(qmt_result.get("cashflow_count", 0))
            )
            > 0
        )
        if not qmt_wrote:
            ts_result = saver.save_from_tushare(
                stock_code, start_date=start_date, end_date=end_date
            )

        overall_ok = qmt_wrote or ts_result.get("success", False)
        return {
            "stock_code": stock_code,
            "success": overall_ok,
            "qmt": qmt_result,
            "tushare": ts_result,
            "server_time": int(time.time() * 1000),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"财务数据刷新失败: {exc}",
        ) from exc


# ---------------------------------------------------------------------------
# 七层结构 / 审计 / 信号查询 API
# ---------------------------------------------------------------------------


@app.get(
    "/api/v1/structures/",
    tags=["七层架构"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_structures(
    code: str = "",
    interval: str = "",
    direction: str = "",
    status_filter: str = Query(default="", alias="status"),
    include_bayes_meta: bool = Query(default=False),
    group_strategy: str = Query(default="fixed"),
    min_observations: int = Query(default=3, ge=1),
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """查询 structure_analyze 主表，供前端结构面板和离线实验底座消费。"""
    allowed_direction = {"up", "down"}
    allowed_status = {"active", "closed", "reversed"}
    if direction and direction not in allowed_direction:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"direction 参数非法，可选值: {sorted(allowed_direction)}",
        )
    if status_filter and status_filter not in allowed_status:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"status 参数非法，可选值: {sorted(allowed_status)}",
        )
    if group_strategy not in {"fixed", "adaptive"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="group_strategy 参数非法，可选值: ['adaptive', 'fixed']",
        )

    sql = """
        SELECT
            id AS structure_id,
            code,
            interval,
            created_at,
            direction,
            p0_ts,
            p0_price,
            p1_ts,
            p1_price,
            p2_ts,
            p2_price,
            p3_ts,
            p3_price,
            attractor_mean,
            attractor_std,
            bayes_lower,
            bayes_upper,
            retrace_ratio,
            status,
            closed_at
        FROM structure_analyze
    """
    clauses: list[str] = []
    params: list[Any] = []
    if code:
        clauses.append("code = ?")
        params.append(code)
    if interval:
        clauses.append("interval = ?")
        params.append(interval)
    if direction:
        clauses.append("direction = ?")
        params.append(direction)
    if status_filter:
        clauses.append("status = ?")
        params.append(status_filter)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY created_at DESC, structure_id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    try:
        db_mgr = _get_structure_query_db_manager()
        rows = _df_to_records(
            db_mgr.execute_read_query(sql, tuple(params))
        )
        if include_bayes_meta and rows:
            from data_manager.structure_bayesian_baseline import StructureBayesianBaseline
            from data_manager.structure_dataset_builder import StructureDatasetBuilder

            builder = StructureDatasetBuilder(db_manager=db_mgr)
            baseline = StructureBayesianBaseline(dataset_builder=builder)
            dataset = builder.build_dataset(
                code=code,
                interval=interval,
                direction=direction,
                statuses=[status_filter] if status_filter else None,
                limit=limit,
                offset=offset,
                order_desc=True,
            )
            annotated = baseline.annotate_dataset(
                dataset,
                group_by=("code", "interval", "direction"),
                group_strategy=group_strategy,
                min_observations=min_observations,
            )
            meta_by_id = {
                row["structure_id"]: row for row in _df_to_records(annotated)
            }
            for row in rows:
                meta = meta_by_id.get(row.get("structure_id"))
                if not meta:
                    continue
                for key in (
                    "posterior_mean",
                    "observation_count",
                    "continuation_count",
                    "reversal_count",
                    "bayes_group_level",
                    "bayes_group_key",
                ):
                    row[key] = meta.get(key)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"结构查询失败: {exc}",
        ) from exc

    items = [_serialize_structure_row(row) for row in rows]
    return {
        "items": items,
        "returned": len(items),
        "limit": limit,
        "offset": offset,
        "filters": {
            "code": code,
            "interval": interval,
            "direction": direction,
            "status": status_filter,
            "include_bayes_meta": include_bayes_meta,
            "group_strategy": group_strategy,
            "min_observations": min_observations,
        },
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/structures/bayesian-baseline",
    tags=["七层架构"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def preview_structure_bayesian_baseline(
    code: str = "",
    interval: str = "",
    direction: str = "",
    statuses: list[str] | None = Query(default=None, alias="status"),
    signal_types: list[str] | None = Query(default=None, alias="signal_type"),
    group_by: list[str] | None = Query(default=None),
    group_strategy: str = Query(default="fixed"),
    min_observations: int = Query(default=3, ge=1),
    alpha_prior: float = Query(default=1.0, gt=0.0),
    beta_prior: float = Query(default=1.0, gt=0.0),
    credible_level: float = Query(default=0.95, gt=0.0, lt=1.0),
) -> dict[str, Any]:
    """预览结构 Bayesian baseline 分桶 posterior，不写回数据库。"""
    allowed_group_by = {"code", "interval", "direction", "status", "latest_signal_type"}
    effective_group_by = group_by or ["interval", "direction"]
    invalid = sorted(set(effective_group_by) - allowed_group_by)
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"group_by 参数非法，可选值: {sorted(allowed_group_by)}",
        )
    if group_strategy not in {"fixed", "adaptive"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="group_strategy 参数非法，可选值: ['adaptive', 'fixed']",
        )

    try:
        from data_manager.structure_bayesian_baseline import StructureBayesianBaseline
        from data_manager.structure_dataset_builder import StructureDatasetBuilder

        db_mgr = _get_structure_query_db_manager()
        builder = StructureDatasetBuilder(db_manager=db_mgr)
        baseline = StructureBayesianBaseline(dataset_builder=builder)
        dataset = builder.build_dataset(
            code=code,
            interval=interval,
            direction=direction,
            statuses=statuses,
            signal_types=signal_types,
        )
        posterior = baseline.fit(
            dataset,
            group_by=tuple(effective_group_by),
            group_strategy=group_strategy,
            min_observations=min_observations,
            alpha_prior=alpha_prior,
            beta_prior=beta_prior,
            credible_level=credible_level,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Bayesian baseline 预览失败: {exc}",
        ) from exc

    return {
        "items": _df_to_records(posterior),
        "returned": len(posterior),
        "dataset_rows": len(dataset),
        "group_by": effective_group_by,
        "group_strategy": group_strategy,
        "min_observations": min_observations,
        "writeback": False,
        "filters": {
            "code": code,
            "interval": interval,
            "direction": direction,
            "status": statuses or [],
            "signal_type": signal_types or [],
        },
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/structures/bayesian-baseline/summary",
    tags=["七层架构"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def summarize_structure_bayesian_baseline(
    code: str = "",
    interval: str = "",
    direction: str = "",
    statuses: list[str] | None = Query(default=None, alias="status"),
    signal_types: list[str] | None = Query(default=None, alias="signal_type"),
    group_by: list[str] | None = Query(default=None),
    group_strategy: str = Query(default="fixed"),
    min_observations: int = Query(default=3, ge=1),
    alpha_prior: float = Query(default=1.0, gt=0.0),
    beta_prior: float = Query(default=1.0, gt=0.0),
    credible_level: float = Query(default=0.95, gt=0.0, lt=1.0),
) -> dict[str, Any]:
    """返回结构 Bayesian 注解后的 Layer 4 摘要（含审计事件均值）。"""
    allowed_group_by = {"code", "interval", "direction", "status", "latest_signal_type"}
    effective_group_by = group_by or ["interval", "direction"]
    invalid = sorted(set(effective_group_by) - allowed_group_by)
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"group_by 参数非法，可选值: {sorted(allowed_group_by)}",
        )
    if group_strategy not in {"fixed", "adaptive"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="group_strategy 参数非法，可选值: ['adaptive', 'fixed']",
        )

    try:
        from data_manager.structure_bayesian_baseline import StructureBayesianBaseline
        from data_manager.structure_dataset_builder import StructureDatasetBuilder

        db_mgr = _get_structure_query_db_manager()
        builder = StructureDatasetBuilder(db_manager=db_mgr)
        baseline = StructureBayesianBaseline(dataset_builder=builder)
        dataset = builder.build_dataset(
            code=code,
            interval=interval,
            direction=direction,
            statuses=statuses,
            signal_types=signal_types,
        )
        annotated = baseline.annotate_dataset(
            dataset,
            group_by=tuple(effective_group_by),
            group_strategy=group_strategy,
            min_observations=min_observations,
            alpha_prior=alpha_prior,
            beta_prior=beta_prior,
            credible_level=credible_level,
        )
        summary = baseline.summarize_annotated_dataset(annotated)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Bayesian baseline 摘要失败: {exc}",
        ) from exc

    return {
        "items": _df_to_records(summary),
        "returned": len(summary),
        "dataset_rows": len(dataset),
        "group_by": effective_group_by,
        "group_strategy": group_strategy,
        "min_observations": min_observations,
        "filters": {
            "code": code,
            "interval": interval,
            "direction": direction,
            "status": statuses or [],
            "signal_type": signal_types or [],
        },
        "server_time": int(time.time() * 1000),
    }


@app.post(
    "/api/v1/structures/bayesian-baseline/apply",
    tags=["七层架构"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def apply_structure_bayesian_baseline(
    code: str = "",
    interval: str = "",
    direction: str = "",
    statuses: list[str] | None = Query(default=None, alias="status"),
    signal_types: list[str] | None = Query(default=None, alias="signal_type"),
    group_by: list[str] | None = Query(default=None),
    group_strategy: str = Query(default="fixed"),
    min_observations: int = Query(default=3, ge=1),
    alpha_prior: float = Query(default=1.0, gt=0.0),
    beta_prior: float = Query(default=1.0, gt=0.0),
    credible_level: float = Query(default=0.95, gt=0.0, lt=1.0),
) -> dict[str, Any]:
    """计算并将 Bayesian baseline 区间写回 structure_analyze。"""
    allowed_group_by = {"code", "interval", "direction", "status", "latest_signal_type"}
    effective_group_by = group_by or ["interval", "direction"]
    invalid = sorted(set(effective_group_by) - allowed_group_by)
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"group_by 参数非法，可选值: {sorted(allowed_group_by)}",
        )
    if group_strategy not in {"fixed", "adaptive"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="group_strategy 参数非法，可选值: ['adaptive', 'fixed']",
        )

    try:
        from data_manager.structure_bayesian_baseline import StructureBayesianBaseline
        from data_manager.structure_dataset_builder import StructureDatasetBuilder

        db_mgr = _get_structure_query_db_manager()
        builder = StructureDatasetBuilder(db_manager=db_mgr)
        baseline = StructureBayesianBaseline(dataset_builder=builder)
        dataset = builder.build_dataset(
            code=code,
            interval=interval,
            direction=direction,
            statuses=statuses,
            signal_types=signal_types,
        )
        posterior = baseline.fit(
            dataset,
            group_by=tuple(effective_group_by),
            group_strategy=group_strategy,
            min_observations=min_observations,
            alpha_prior=alpha_prior,
            beta_prior=beta_prior,
            credible_level=credible_level,
        )
        updated = baseline.writeback_structure_bounds(
            dataset,
            posterior=posterior,
            group_by=tuple(effective_group_by),
            group_strategy=group_strategy,
            min_observations=min_observations,
            alpha_prior=alpha_prior,
            beta_prior=beta_prior,
            credible_level=credible_level,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Bayesian baseline 写回失败: {exc}",
        ) from exc

    return {
        "items": _df_to_records(posterior),
        "returned": len(posterior),
        "dataset_rows": len(dataset),
        "updated": updated,
        "group_by": effective_group_by,
        "group_strategy": group_strategy,
        "min_observations": min_observations,
        "writeback": True,
        "filters": {
            "code": code,
            "interval": interval,
            "direction": direction,
            "status": statuses or [],
            "signal_type": signal_types or [],
        },
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/structures/{structure_id}/detail",
    tags=["七层架构"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_structure_detail(
    structure_id: str,
    audit_limit: int = Query(default=20, ge=1, le=200),
    include_bayes_meta: bool = Query(default=True),
    group_strategy: str = Query(default="adaptive"),
    min_observations: int = Query(default=3, ge=1),
    alpha_prior: float = Query(default=1.0, gt=0.0),
    beta_prior: float = Query(default=1.0, gt=0.0),
    credible_level: float = Query(default=0.95, gt=0.0, lt=1.0),
) -> dict[str, Any]:
    """查询单个结构详情，返回结构主记录、最新信号、审计明细与审计摘要。"""
    if group_strategy not in {"fixed", "adaptive"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="group_strategy 参数非法，可选值: ['adaptive', 'fixed']",
        )

    structure_sql = """
        SELECT
            id AS structure_id,
            code,
            interval,
            created_at,
            direction,
            p0_ts,
            p0_price,
            p1_ts,
            p1_price,
            p2_ts,
            p2_price,
            p3_ts,
            p3_price,
            attractor_mean,
            attractor_std,
            bayes_lower,
            bayes_upper,
            retrace_ratio,
            status,
            closed_at
        FROM structure_analyze
        WHERE id = ?
        LIMIT 1
    """
    audit_sql = """
        SELECT
            id AS audit_id,
            structure_id,
            code,
            interval,
            event_type,
            event_ts,
            snapshot_json
        FROM structure_audit
        WHERE structure_id = ?
        ORDER BY event_ts DESC, audit_id DESC
        LIMIT ?
    """
    audit_summary_sql = """
        SELECT
            COUNT(*) AS audit_event_count,
            SUM(CASE WHEN event_type = 'create' THEN 1 ELSE 0 END) AS create_event_count,
            SUM(CASE WHEN event_type = 'extend' THEN 1 ELSE 0 END) AS extend_event_count,
            SUM(CASE WHEN event_type = 'reverse' THEN 1 ELSE 0 END) AS reverse_event_count,
            MAX(event_ts) AS last_event_ts,
            arg_max(event_type, event_ts) AS last_event_type
        FROM structure_audit
        WHERE structure_id = ?
    """
    latest_signal_sql = """
        SELECT
            id AS signal_id,
            structure_id,
            code,
            interval,
            signal_ts,
            signal_type,
            trigger_price,
            stop_loss_price,
            stop_loss_distance,
            drawdown_pct,
            calmar_snapshot,
            remarks
        FROM signal_structured
        WHERE structure_id = ?
        ORDER BY signal_ts DESC,
                 CASE WHEN signal_type = 'EXIT' THEN 1 ELSE 0 END DESC,
                 signal_id DESC
        LIMIT 1
    """

    try:
        db_mgr = _get_structure_query_db_manager()
        row_records = _df_to_records(db_mgr.execute_read_query(structure_sql, (structure_id,)))
        if not row_records:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到 structure_id={structure_id} 对应的结构",
            )
        structure_row = row_records[0]
        audit_rows = _df_to_records(db_mgr.execute_read_query(audit_sql, (structure_id, audit_limit)))
        audit_summary_rows = _df_to_records(db_mgr.execute_read_query(audit_summary_sql, (structure_id,)))
        signal_rows = _df_to_records(db_mgr.execute_read_query(latest_signal_sql, (structure_id,)))

        if include_bayes_meta:
            from data_manager.structure_bayesian_baseline import StructureBayesianBaseline
            from data_manager.structure_dataset_builder import StructureDatasetBuilder

            builder = StructureDatasetBuilder(db_manager=db_mgr)
            dataset = builder.build_dataset(
                code=str(structure_row.get("code") or ""),
                interval=str(structure_row.get("interval") or ""),
                direction=str(structure_row.get("direction") or ""),
            )
            annotated = StructureBayesianBaseline(dataset_builder=builder).annotate_dataset(
                dataset,
                group_by=("code", "interval", "direction"),
                group_strategy=group_strategy,
                min_observations=min_observations,
                alpha_prior=alpha_prior,
                beta_prior=beta_prior,
                credible_level=credible_level,
            )
            meta = next(
                (
                    item
                    for item in _df_to_records(annotated)
                    if str(item.get("structure_id")) == str(structure_id)
                ),
                None,
            )
            if meta:
                for key in (
                    "posterior_mean",
                    "observation_count",
                    "continuation_count",
                    "reversal_count",
                    "bayes_group_level",
                    "bayes_group_key",
                ):
                    structure_row[key] = meta.get(key)
        structure = _serialize_structure_row(structure_row)
        audit_items = [_serialize_audit_row(row) for row in audit_rows]
        latest_signal = _serialize_signal_row(signal_rows[0]) if signal_rows else None
        audit_summary = audit_summary_rows[0] if audit_summary_rows else {}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"结构详情查询失败: {exc}",
        ) from exc

    return {
        "structure": structure,
        "latest_signal": latest_signal,
        "audit_items": audit_items,
        "audit_summary": {
            "audit_event_count": audit_summary.get("audit_event_count"),
            "create_event_count": audit_summary.get("create_event_count"),
            "extend_event_count": audit_summary.get("extend_event_count"),
            "reverse_event_count": audit_summary.get("reverse_event_count"),
            "last_event_ts": audit_summary.get("last_event_ts"),
            "last_event_type": audit_summary.get("last_event_type"),
        },
        "filters": {
            "audit_limit": audit_limit,
            "include_bayes_meta": include_bayes_meta,
            "group_strategy": group_strategy,
            "min_observations": min_observations,
        },
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/structure-audit/",
    tags=["七层架构"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_structure_audit(
    structure_id: str = "",
    code: str = "",
    interval: str = "",
    event_type: str = "",
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """查询 structure_audit 审计日志，返回已解析的结构快照。"""
    allowed_event_type = {"create", "extend", "reverse", "close"}
    if event_type and event_type not in allowed_event_type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"event_type 参数非法，可选值: {sorted(allowed_event_type)}",
        )

    sql = """
        SELECT
            id AS audit_id,
            structure_id,
            code,
            interval,
            event_type,
            event_ts,
            snapshot_json
        FROM structure_audit
    """
    clauses: list[str] = []
    params: list[Any] = []
    if structure_id:
        clauses.append("structure_id = ?")
        params.append(structure_id)
    if code:
        clauses.append("code = ?")
        params.append(code)
    if interval:
        clauses.append("interval = ?")
        params.append(interval)
    if event_type:
        clauses.append("event_type = ?")
        params.append(event_type)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY event_ts DESC, audit_id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    try:
        rows = _df_to_records(
            _get_structure_query_db_manager().execute_read_query(sql, tuple(params))
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"结构审计查询失败: {exc}",
        ) from exc

    items = [_serialize_audit_row(row) for row in rows]
    return {
        "items": items,
        "returned": len(items),
        "limit": limit,
        "offset": offset,
        "filters": {
            "structure_id": structure_id,
            "code": code,
            "interval": interval,
            "event_type": event_type,
        },
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/signals/",
    tags=["七层架构"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_structured_signals(
    structure_id: str = "",
    code: str = "",
    interval: str = "",
    signal_type: str = "",
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """查询 signal_structured 信号表，供审计面板/结构实验面板消费。"""
    allowed_signal_type = {"LONG", "SHORT", "EXIT", "HOLD"}
    if signal_type and signal_type not in allowed_signal_type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"signal_type 参数非法，可选值: {sorted(allowed_signal_type)}",
        )

    sql = """
        SELECT
            id AS signal_id,
            structure_id,
            code,
            interval,
            signal_ts,
            signal_type,
            trigger_price,
            stop_loss_price,
            stop_loss_distance,
            drawdown_pct,
            calmar_snapshot,
            remarks
        FROM signal_structured
    """
    clauses: list[str] = []
    params: list[Any] = []
    if structure_id:
        clauses.append("structure_id = ?")
        params.append(structure_id)
    if code:
        clauses.append("code = ?")
        params.append(code)
    if interval:
        clauses.append("interval = ?")
        params.append(interval)
    if signal_type:
        clauses.append("signal_type = ?")
        params.append(signal_type)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY signal_ts DESC, signal_id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    try:
        rows = _df_to_records(
            _get_structure_query_db_manager().execute_read_query(sql, tuple(params))
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"结构信号查询失败: {exc}",
        ) from exc

    items = [_serialize_signal_row(row) for row in rows]
    return {
        "items": items,
        "returned": len(items),
        "limit": limit,
        "offset": offset,
        "filters": {
            "structure_id": structure_id,
            "code": code,
            "interval": interval,
            "signal_type": signal_type,
        },
        "server_time": int(time.time() * 1000),
    }


# ---------------------------------------------------------------------------
# Prometheus /metrics 端点
# ---------------------------------------------------------------------------


@app.get("/metrics", tags=["运维"], include_in_schema=False)
def prometheus_metrics() -> Response:
    """
    Prometheus 指标抓取端点（无需鉴权，适用于 Prometheus scraper）。

    当 prometheus_client 已安装时，返回标准 text/plain Prometheus 格式；
    否则降级返回 JSON 格式的关键指标（Content-Type: application/json）。

    主要指标：
      easyxt_rate_limit_hits_total   — 累计限流命中次数
      easyxt_ws_drop_rate            — WS 全生命周期丢帧率
      easyxt_ws_drop_rate_1m         — WS 近 60s 丢帧率
      easyxt_strategies_running      — 当前运行策略数
      easyxt_ws_queue_total_len      — WS 队列积压帧总数
      easyxt_uptime_seconds          — 服务运行时长
    """
    # 采集当前值
    uptime_s = (
        round(time.monotonic() - _server_start_time, 1) if _server_start_time is not None else 0.0
    )
    try:
        from strategies.registry import strategy_registry

        running_count = len(strategy_registry.list_running())
    except Exception:
        running_count = -1

    total_queue_len = sum(broadcaster.queue_depths().values())

    if _prom_enabled:
        # 同步计数器与 gauge（Counter 只增不减，rate_limit_hits 作为 gauge_since_start）
        _prom_ws_drop_rate.set(broadcaster.drop_rate)  # type: ignore[union-attr]
        _prom_ws_drop_rate_1m.set(broadcaster.drop_rate_1m)  # type: ignore[union-attr]
        _prom_strategies_running.set(max(running_count, 0))  # type: ignore[union-attr]
        _prom_ws_queue_len.set(total_queue_len)  # type: ignore[union-attr]
        _prom_uptime.set(uptime_s)  # type: ignore[union-attr]
        # rate_limit_hits 是只增计数器 —— 将全局计数同步到 prometheus Counter
        # （Counter 内部维护自己的值，这里利用 _value 对齐；仅供参考指标）
        try:
            current_prom_val = int(_prom_rate_limit_hits._value.get())  # type: ignore[union-attr]
            diff = max(0, _rate_limit_hits - current_prom_val)
            if diff > 0:
                _prom_rate_limit_hits.inc(diff)  # type: ignore[union-attr]
        except Exception:
            pass
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        return Response(
            content=generate_latest(_prom_registry),
            media_type=CONTENT_TYPE_LATEST,
        )

    # 降级：纯文本 Prometheus 格式（无 prometheus_client）
    lines = [
        "# HELP easyxt_rate_limit_hits_total 累计限流命中次数",
        "# TYPE easyxt_rate_limit_hits_total counter",
        f"easyxt_rate_limit_hits_total {_rate_limit_hits}",
        "# HELP easyxt_ws_drop_rate WebSocket 全生命周期丢帧率",
        "# TYPE easyxt_ws_drop_rate gauge",
        f"easyxt_ws_drop_rate {broadcaster.drop_rate}",
        "# HELP easyxt_ws_drop_rate_1m WebSocket 近 60s 丢帧率",
        "# TYPE easyxt_ws_drop_rate_1m gauge",
        f"easyxt_ws_drop_rate_1m {broadcaster.drop_rate_1m}",
        "# HELP easyxt_strategies_running 当前运行中的策略数量",
        "# TYPE easyxt_strategies_running gauge",
        f"easyxt_strategies_running {max(running_count, 0)}",
        "# HELP easyxt_ws_queue_total_len WS 队列积压帧总数",
        "# TYPE easyxt_ws_queue_total_len gauge",
        f"easyxt_ws_queue_total_len {total_queue_len}",
        "# HELP easyxt_uptime_seconds 服务运行时长",
        "# TYPE easyxt_uptime_seconds gauge",
        f"easyxt_uptime_seconds {uptime_s}",
    ]
    return Response(
        content="\n".join(lines) + "\n",
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


# ---------------------------------------------------------------------------
# 直接运行入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("EASYXT_API_HOST", "127.0.0.1")
    port = int(os.environ.get("EASYXT_API_PORT", "8765"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    log.info("启动 EasyXT 中台服务 %s:%d", host, port)
    uvicorn.run("core.api_server:app", host=host, port=port, reload=False)
