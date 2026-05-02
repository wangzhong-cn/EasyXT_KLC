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
import concurrent.futures
import csv
import importlib
import json
import logging
import os
import queue
import secrets
import site
import sys
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Optional, cast

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

from core.xt_gateway import (
    xt_get_financial_data,
    xt_get_full_tick,
    xt_get_instrument_detail,
    xt_get_stock_list_in_sector,
)
from core.xt_worker_process import (
    XtWorkerProcessManager,
    ensure_xt_worker_process_ready,
    xt_worker_process_policy_snapshot,
)
from core.xt_runtime_policy import xt_runtime_policy_snapshot, xt_side_effects_allowed


def _strip_user_site_packages_from_sys_path() -> None:
    """避免服务进程误用用户目录 site-packages 中的 xtquant 覆盖 conda 环境依赖。"""
    try:
        user_sites = site.getusersitepackages()
    except Exception:
        return

    if isinstance(user_sites, str):
        candidates = [user_sites]
    else:
        candidates = [str(item) for item in list(user_sites or [])]

    normalized = {
        os.path.normcase(os.path.normpath(path))
        for path in candidates
        if str(path or "").strip()
    }
    if not normalized:
        return

    sys.path[:] = [
        path
        for path in sys.path
        if os.path.normcase(os.path.normpath(str(path or ""))) not in normalized
    ]


_strip_user_site_packages_from_sys_path()

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
_TRUTHY_ENV_VALUES: set[str] = {"1", "true", "yes", "on"}
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
_BASIC_ARSENAL_STATUS_TTL_S: float = max(
    5.0,
    float(os.environ.get("EASYXT_BASIC_ARSENAL_STATUS_TTL_S", "60")),
)
_BASIC_ARSENAL_STRUCTURAL_FRESH_RATIO_THRESHOLD: float = min(
    1.0,
    max(
        0.0,
        float(os.environ.get("EASYXT_BASIC_ARSENAL_STRUCTURAL_FRESH_RATIO_THRESHOLD", "0.99")),
    ),
)
_BASIC_ARSENAL_STRUCTURAL_MAX_LAG_DAYS: int = max(
    0,
    int(os.environ.get("EASYXT_BASIC_ARSENAL_STRUCTURAL_MAX_LAG_DAYS", "1")),
)
_basic_arsenal_status_cache_lock = threading.Lock()
_basic_arsenal_status_cache: dict[str, Any] | None = None
_SYMBOL_SEARCH_CACHE_PATH: Path = Path(
    os.environ.get(
        "EASYXT_SYMBOL_SEARCH_CACHE_PATH",
        str(_ROOT_DIR / "artifacts" / "symbol_search_catalog.json"),
    )
)
_SYMBOL_SEARCH_CACHE_TTL_S: float = max(
    60.0,
    float(os.environ.get("EASYXT_SYMBOL_SEARCH_CACHE_TTL_S", "21600")),
)
_SYMBOL_SEARCH_SECTORS: tuple[str, ...] = (
    "沪深A股",
    "BJ",
    "SHF",
    "DCE",
    "CZC",
    "CFFEX",
    "INE",
    "GFEX",
)
_SYMBOL_SEARCH_FUTURE_EXCHANGES: frozenset[str] = frozenset(
    {"SHF", "DCE", "CZC", "CFFEX", "INE", "GFEX"}
)
_SYMBOL_SEARCH_SCOPE_ALIASES: dict[str, frozenset[str]] = {
    "all": frozenset({"stock", "etf", "index", "bond", "commodity", "other"}),
    "stock": frozenset({"stock"}),
    "etf": frozenset({"etf"}),
    "index": frozenset({"index"}),
    "bond": frozenset({"bond"}),
    "commodity": frozenset({"commodity"}),
    "other": frozenset({"other"}),
}
_SYMBOL_SEARCH_SCOPE_LABELS: dict[str, str] = {
    "stock": "股票",
    "etf": "ETF",
    "index": "指数",
    "bond": "债券",
    "commodity": "商品期货",
    "other": "其他",
}
_symbol_search_cache_lock = threading.Lock()
_symbol_search_cache: dict[str, Any] | None = None


def _invalidate_basic_arsenal_status_cache() -> None:
    global _basic_arsenal_status_cache
    with _basic_arsenal_status_cache_lock:
        _basic_arsenal_status_cache = None


def _parse_cors_allow_origins() -> list[str]:
    raw = str(os.environ.get("EASYXT_CORS_ALLOW_ORIGINS", "") or "").strip()
    if raw:
        origins = [item.strip() for item in raw.split(",") if item.strip()]
        if origins:
            return origins
    return [
        "http://127.0.0.1:1420",
        "http://localhost:1420",
        "http://127.0.0.1:1421",
        "http://localhost:1421",
        "http://127.0.0.1:3000",
        "http://localhost:3000",
        "http://testserver",
        "tauri://localhost",
        "https://tauri.localhost",
    ]


_CORS_ALLOW_ORIGINS: list[str] = _parse_cors_allow_origins()


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


class DataIngestionJobCreateBody(BaseModel):
    stock_codes: list[str] | None = None
    use_all_stocks: bool = False  # True → 自动从 QMT 拉取 SH+SZ+BJ 全市场列表
    exchanges: list[str] | None = None  # e.g. ["SH","SZ","BJ","SHF","DCE"] — 按交易所拉取股票/合约列表
    periods: list[str] | None = None
    start_date: str | None = None
    end_date: str | None = None
    precompute_after_download: bool = True
    enable_audit_after_ingest: bool = True  # 每只标的入库后触发 Golden 1D audit + repair 入队
    download_workers: int | None = None
    intraday_lookback_days: int | None = None  # 0 = 真全历史；None = 使用环境默认
    skip_gap_scan: bool | None = None  # None = 使用环境默认 (EASYXT_BULK_SKIP_GAP_SCAN)
    precompute_workers: int | None = None  # None = 使用环境默认 (EASYXT_PRECOMPUTE_WORKERS)
    stage_label: str | None = None


class CoverageMatrixJobCreateBody(BaseModel):
    config_path: str = ""
    output_dir: str = ""
    workers: int | None = None
    resume: bool | None = None


class DbQueryBody(BaseModel):
    sql: str
    limit: int = 200  # 最多 1 000 行；0 = 不附加额外 LIMIT


class DbCsvImportBody(BaseModel):
    table_name: str
    csv_content: str  # 原始 CSV 文本（含表头行）
    mode: str = "append"  # "append" | "replace"


class IntegrityCheckBody(BaseModel):
    stock_code: str
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD
    detailed: bool = True


class ReconciliationBody(BaseModel):
    stock_code: str
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD


class DbMaintenanceBody(BaseModel):
    operation: str  # "checkpoint" | "force_checkpoint" | "analyze"


class DataSourceTestBody(BaseModel):
    source: str  # "qmt_local_dat" | "qmt_xtquant" | "qmt" | "duckdb" | "tushare" | "akshare" | ...


class AccountBindingDiscoverBody(BaseModel):
    include_probes: bool = False
    force: bool = False


class AccountBindingApplyBody(BaseModel):
    include_probes: bool = True
    force: bool = True


_DATA_INGESTION_DEFAULT_PERIODS: tuple[str, ...] = ("1d", "1m", "5m")
_DATA_INGESTION_JOB_STATUSES: frozenset[str] = frozenset(
    {"queued", "running", "cancelling", "cancelled", "completed", "failed"}
)
_data_ingestion_jobs_lock = threading.Lock()
_data_ingestion_jobs: dict[str, dict[str, Any]] = {}
_DATA_INGESTION_SAFE_DEFAULT_DOWNLOAD_WORKERS = 2
_DATA_INGESTION_SAFE_DEFAULT_PRECOMPUTE_WORKERS = 2
_DATA_INGESTION_SMALL_JOB_DOWNLOAD_WORKERS_MAX = 4
_DATA_INGESTION_SMALL_JOB_PRECOMPUTE_WORKERS_MAX = 4
_DATA_INGESTION_HEAVY_JOB_SYMBOL_THRESHOLD = 1000
_DATA_INGESTION_HEAVY_JOB_DOWNLOAD_WORKERS_MAX = 2
_DATA_INGESTION_HEAVY_JOB_PRECOMPUTE_WORKERS_MAX = 2
_DATA_INGESTION_FULL_HISTORY_INTRADAY_DOWNLOAD_WORKERS_MAX = 1
_DATA_INGESTION_FULL_HISTORY_INTRADAY_PRECOMPUTE_WORKERS_MAX = 1
_COVERAGE_MATRIX_DEFAULT_CONFIG_PATH: Path = _ROOT_DIR / "config" / "falsification_matrix.yaml"
_COVERAGE_MATRIX_OUTPUT_ROOT: Path = Path(
    os.environ.get(
        "EASYXT_COVERAGE_MATRIX_OUTPUT_ROOT",
        str(_ROOT_DIR / "artifacts" / "coverage_matrix_runs"),
    )
)
_COVERAGE_MATRIX_JOB_STATUSES: frozenset[str] = frozenset(
    {"queued", "running", "cancelling", "cancelled", "completed", "failed"}
)
_COVERAGE_MATRIX_ARTIFACT_META: dict[str, tuple[str, str]] = {
    "json": ("application/json", ".json"),
    "summary": ("text/markdown; charset=utf-8", ".md"),
    "heatmap_csv": ("text/csv; charset=utf-8", ".csv"),
    "heatmap_png": ("image/png", ".png"),
}
_coverage_matrix_jobs_lock = threading.Lock()
_coverage_matrix_jobs: dict[str, dict[str, Any]] = {}
_DB_TABLE_TIME_COLUMNS: tuple[str, ...] = (
    "date",
    "trade_date",
    "datetime",
    "time",
    "ts",
    "event_time",
    "event_ts",
    "signal_ts",
    "created_at",
    "updated_at",
    "last_updated",
)
_DB_TABLE_SYMBOL_COLUMNS: tuple[str, ...] = ("stock_code", "code", "symbol")


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _normalize_string_list(values: list[str] | None, *, uppercase: bool = False) -> list[str]:
    if not values:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = str(item or "").strip()
        if not text:
            continue
        if uppercase:
            text = text.upper()
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


# 前端/API 传入的交易所代码 → xtdata get_stock_list_in_sector 参数
_EXCHANGE_SECTOR_MAP: dict[str, str] = {
    "SH": "SH", "SSE": "SH",
    "SZ": "SZ", "SZSE": "SZ",
    "BJ": "BJ", "BSE": "BJ",
    "SHF": "SHF", "SHFE": "SHF", "SF": "SHF",
    "DCE": "DCE", "DF": "DCE",
    "CZC": "CZC", "CZCE": "CZC", "ZF": "CZC",
    "CFFEX": "CFFEX", "IF": "CFFEX",
    "INE": "INE",
    "GFEX": "GFEX",
}

_A_SHARE_EXCHANGES: frozenset[str] = frozenset({"SH", "SZ", "BJ"})
_SH_INDEX_PREFIXES: tuple[str, ...] = ("000", "880", "930", "931", "932", "985", "986")
_SH_ETF_PREFIXES: tuple[str, ...] = ("51", "56", "58")
_SZ_INDEX_PREFIXES: tuple[str, ...] = ("399",)
_SZ_ETF_PREFIXES: tuple[str, ...] = ("15", "16", "18")
_BOND_PREFIXES: tuple[str, ...] = ("01", "02", "10", "11", "12", "13", "14", "204", "511", "519")


def _symbol_exchange(symbol: str) -> str:
    if "." not in symbol:
        return ""
    return symbol.rsplit(".", 1)[1].upper().strip()


def _is_a_share_symbol(symbol: str) -> bool:
    text = str(symbol or "").strip().upper()
    if "." not in text:
        return False
    code, exch = text.rsplit(".", 1)
    if exch == "BJ":
        return True
    if exch not in {"SH", "SZ"}:
        return False
    if code.startswith(_BOND_PREFIXES):
        return False
    if exch == "SH" and code.startswith(_SH_ETF_PREFIXES):
        return False
    if exch == "SZ" and code.startswith(_SZ_ETF_PREFIXES):
        return False
    if exch == "SH" and code.startswith(_SH_INDEX_PREFIXES):
        return False
    if exch == "SZ" and code.startswith(_SZ_INDEX_PREFIXES):
        return False
    return True


def _fetch_xt_sector_codes(sector: str) -> list[str]:
    if not xt_side_effects_allowed(test_mode=_TEST_MODE):
        return []
    return xt_get_stock_list_in_sector(sector)


@lru_cache(maxsize=1)
def _get_symbol_search_pinyin_helpers() -> tuple[Callable[[str], str] | None, Callable[[str], str] | None]:
    try:
        from pypinyin import Style, lazy_pinyin  # type: ignore[import-not-found]
    except Exception:
        return None, None

    def _full(text: str) -> str:
        return "".join(lazy_pinyin(text))

    def _abbr(text: str) -> str:
        return "".join(lazy_pinyin(text, style=Style.FIRST_LETTER))

    return _full, _abbr


def _normalize_symbol_search_token(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    return "".join(ch for ch in raw if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")


def _symbol_short_code(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    if "." not in text:
        return text
    return text.rsplit(".", 1)[0]


def _classify_symbol_search_scope(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    exchange = _symbol_exchange(text)
    short_code = _symbol_short_code(text)
    if exchange in _SYMBOL_SEARCH_FUTURE_EXCHANGES:
        return "commodity"
    if exchange == "BJ":
        return "stock"
    if exchange == "SH":
        if short_code.startswith(_BOND_PREFIXES):
            return "bond"
        if short_code.startswith(_SH_ETF_PREFIXES):
            return "etf"
        if short_code.startswith(_SH_INDEX_PREFIXES):
            return "index"
        return "stock"
    if exchange == "SZ":
        if short_code.startswith(_BOND_PREFIXES):
            return "bond"
        if short_code.startswith(_SZ_ETF_PREFIXES):
            return "etf"
        if short_code.startswith(_SZ_INDEX_PREFIXES):
            return "index"
        return "stock"
    return "other"


def _scope_matches(scope: str, symbol_scope: str) -> bool:
    normalized_scope = str(scope or "all").strip().lower() or "all"
    allowed_scopes = _SYMBOL_SEARCH_SCOPE_ALIASES.get(normalized_scope)
    if allowed_scopes is None:
        allowed_scopes = _SYMBOL_SEARCH_SCOPE_ALIASES["all"]
    return symbol_scope in allowed_scopes


def _symbol_search_market_label(symbol: str, scope: str) -> str:
    exchange = _symbol_exchange(symbol)
    scope_label = _SYMBOL_SEARCH_SCOPE_LABELS.get(scope, "其他")
    return f"{scope_label} · {exchange}" if exchange else scope_label


def _serialize_symbol_search_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": str(item.get("symbol", "")),
        "short_code": str(item.get("short_code", "")),
        "name": str(item.get("name", "") or str(item.get("symbol", ""))),
        "display_name": str(item.get("display_name", "") or str(item.get("symbol", ""))),
        "exchange": str(item.get("exchange", "")),
        "scope": str(item.get("scope", "other")),
        "scope_label": str(item.get("scope_label", "其他")),
        "market_label": str(item.get("market_label", "其他")),
        "product_name": str(item.get("product_name", "")),
        "last_date": item.get("last_date"),
    }


def _is_symbol_search_cache_item_valid(item: dict[str, Any]) -> bool:
    required_fields = {
        "symbol",
        "short_code",
        "name",
        "display_name",
        "exchange",
        "scope",
        "scope_label",
        "market_label",
        "product_name",
        "_name_pinyin",
        "_name_abbr",
        "_product_pinyin",
        "_product_abbr",
    }
    return required_fields.issubset(item)


def _load_symbol_search_cache_file() -> dict[str, Any] | None:
    if not _SYMBOL_SEARCH_CACHE_PATH.exists():
        return None
    try:
        payload = json.loads(_SYMBOL_SEARCH_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    items = payload.get("items")
    if not isinstance(items, list):
        return None
    return payload


def _save_symbol_search_cache_file(items: list[dict[str, Any]], generated_at_ms: int) -> None:
    _SYMBOL_SEARCH_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _SYMBOL_SEARCH_CACHE_PATH.write_text(
        json.dumps(
            {
                "generated_at_ms": generated_at_ms,
                "generated_at": datetime.utcfromtimestamp(generated_at_ms / 1000).isoformat() + "Z",
                "items": items,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _load_symbol_last_dates() -> dict[str, str | None]:
    db_mgr, _ = _get_api_duckdb_manager_and_path()
    with db_mgr.get_read_connection() as con:
        rows = con.execute(
            """
            WITH latest_daily AS (
                SELECT stock_code, MAX(CAST(date AS TIMESTAMP)) AS latest_at
                FROM stock_daily
                GROUP BY stock_code
            ),
            latest_1m AS (
                SELECT stock_code, MAX(datetime) AS latest_at
                FROM stock_1m
                GROUP BY stock_code
            ),
            latest_5m AS (
                SELECT stock_code, MAX(datetime) AS latest_at
                FROM stock_5m
                GROUP BY stock_code
            ),
            merged AS (
                SELECT * FROM latest_daily
                UNION ALL
                SELECT * FROM latest_1m
                UNION ALL
                SELECT * FROM latest_5m
            )
            SELECT stock_code, MAX(latest_at) AS latest_at
            FROM merged
            GROUP BY stock_code
            """
        ).fetchall()
    return {
        str(stock_code).strip().upper(): str(latest_at) if latest_at is not None else None
        for stock_code, latest_at in rows
        if str(stock_code).strip()
    }


def _load_symbol_search_universe(last_date_map: dict[str, str | None]) -> list[str]:
    symbols = {str(code).strip().upper() for code in last_date_map if str(code).strip()}
    for sector in _SYMBOL_SEARCH_SECTORS:
        try:
            symbols.update(
                str(code).strip().upper()
                for code in _fetch_xt_sector_codes(sector)
                if str(code).strip()
            )
        except Exception:
            continue
    return sorted(symbols)


def _fetch_symbol_search_detail(symbol: str) -> dict[str, Any]:
    if not xt_side_effects_allowed(test_mode=_TEST_MODE):
        return {}
    try:
        return xt_get_instrument_detail(symbol, include_complete=True)
    except Exception:
        return {}


def _build_symbol_search_catalog_item(
    symbol: str,
    *,
    last_date: str | None,
    detail: dict[str, Any],
) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").strip().upper()
    short_code = _symbol_short_code(normalized_symbol)
    exchange = _symbol_exchange(normalized_symbol)
    name = str(detail.get("InstrumentName") or detail.get("name") or short_code).strip() or short_code
    product_name = str(detail.get("ProductName") or detail.get("product_name") or "").strip()
    scope = _classify_symbol_search_scope(normalized_symbol)
    scope_label = _SYMBOL_SEARCH_SCOPE_LABELS.get(scope, "其他")
    market_label = _symbol_search_market_label(normalized_symbol, scope)
    pinyin_full_fn, pinyin_abbr_fn = _get_symbol_search_pinyin_helpers()
    name_pinyin = _normalize_symbol_search_token(pinyin_full_fn(name) if pinyin_full_fn else "")
    name_abbr = _normalize_symbol_search_token(pinyin_abbr_fn(name) if pinyin_abbr_fn else "")
    product_pinyin = _normalize_symbol_search_token(
        pinyin_full_fn(product_name) if pinyin_full_fn and product_name else ""
    )
    product_abbr = _normalize_symbol_search_token(
        pinyin_abbr_fn(product_name) if pinyin_abbr_fn and product_name else ""
    )
    display_name = f"{name} · {normalized_symbol}" if name != normalized_symbol else normalized_symbol
    return {
        "symbol": normalized_symbol,
        "short_code": short_code,
        "name": name,
        "display_name": display_name,
        "exchange": exchange,
        "scope": scope,
        "scope_label": scope_label,
        "market_label": market_label,
        "product_name": product_name,
        "last_date": last_date,
        "_symbol_norm": _normalize_symbol_search_token(normalized_symbol),
        "_short_code_norm": _normalize_symbol_search_token(short_code),
        "_name_norm": _normalize_symbol_search_token(name),
        "_product_name_norm": _normalize_symbol_search_token(product_name),
        "_name_pinyin": name_pinyin,
        "_name_abbr": name_abbr,
        "_product_pinyin": product_pinyin,
        "_product_abbr": product_abbr,
    }


def _build_symbol_search_catalog(
    *,
    base_items_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    last_date_map = _load_symbol_last_dates()
    universe = _load_symbol_search_universe(last_date_map)
    base_items = base_items_by_symbol or {}
    items: list[dict[str, Any]] = []
    for symbol in universe:
        cached_item = base_items.get(symbol)
        if cached_item is not None and _is_symbol_search_cache_item_valid(cached_item):
            reused_item = dict(cached_item)
            reused_item["last_date"] = last_date_map.get(symbol, reused_item.get("last_date"))
            items.append(reused_item)
            continue
        detail = _fetch_symbol_search_detail(symbol)
        items.append(
            _build_symbol_search_catalog_item(
                symbol,
                last_date=last_date_map.get(symbol),
                detail=detail,
            )
        )
    return items


def _get_symbol_search_catalog(force_refresh: bool = False) -> list[dict[str, Any]]:
    global _symbol_search_cache
    now_ms = int(time.time() * 1000)
    ttl_ms = int(_SYMBOL_SEARCH_CACHE_TTL_S * 1000)
    if not force_refresh:
        with _symbol_search_cache_lock:
            cached = dict(_symbol_search_cache or {}) if _symbol_search_cache else None
        if cached is not None:
            generated_at_ms = int(cached.get("generated_at_ms", 0) or 0)
            if generated_at_ms > 0 and max(now_ms - generated_at_ms, 0) <= ttl_ms:
                cached_items = cached.get("items")
                if isinstance(cached_items, list):
                    return [cast(dict[str, Any], item) for item in cached_items]

    file_payload = _load_symbol_search_cache_file()
    base_items_by_symbol: dict[str, dict[str, Any]] = {}
    if file_payload is not None:
        for raw_item in cast(list[dict[str, Any]], file_payload.get("items") or []):
            symbol = str(raw_item.get("symbol") or "").strip().upper()
            if symbol:
                base_items_by_symbol[symbol] = raw_item
        if not force_refresh:
            generated_at_ms = int(file_payload.get("generated_at_ms", 0) or 0)
            if generated_at_ms > 0 and max(now_ms - generated_at_ms, 0) <= ttl_ms:
                with _symbol_search_cache_lock:
                    _symbol_search_cache = file_payload
                return [cast(dict[str, Any], item) for item in cast(list[dict[str, Any]], file_payload.get("items") or [])]

    catalog = _build_symbol_search_catalog(base_items_by_symbol=base_items_by_symbol)
    payload = {
        "generated_at_ms": now_ms,
        "generated_at": datetime.utcfromtimestamp(now_ms / 1000).isoformat() + "Z",
        "items": catalog,
    }
    try:
        _save_symbol_search_cache_file(catalog, now_ms)
    except Exception:
        log.exception("failed to persist symbol search catalog")
    with _symbol_search_cache_lock:
        _symbol_search_cache = payload
    return catalog


def _rank_symbol_search_item(item: dict[str, Any], query: str) -> int | None:
    if not query:
        return 90
    symbol_norm = str(item.get("_symbol_norm", ""))
    short_code_norm = str(item.get("_short_code_norm", ""))
    name_norm = str(item.get("_name_norm", ""))
    product_norm = str(item.get("_product_name_norm", ""))
    name_pinyin = str(item.get("_name_pinyin", ""))
    name_abbr = str(item.get("_name_abbr", ""))
    product_pinyin = str(item.get("_product_pinyin", ""))
    product_abbr = str(item.get("_product_abbr", ""))

    if query in {symbol_norm, short_code_norm}:
        return 0
    if symbol_norm.startswith(query) or short_code_norm.startswith(query):
        return 1
    if query in {name_norm, product_norm}:
        return 2
    if name_norm.startswith(query) or product_norm.startswith(query):
        return 3
    if query in {name_abbr, name_pinyin, product_abbr, product_pinyin}:
        return 4
    if (
        name_abbr.startswith(query)
        or name_pinyin.startswith(query)
        or product_abbr.startswith(query)
        or product_pinyin.startswith(query)
    ):
        return 5
    if query in symbol_norm or query in short_code_norm:
        return 6
    if query in name_norm or query in product_norm:
        return 7
    if (
        query in name_abbr
        or query in name_pinyin
        or query in product_abbr
        or query in product_pinyin
    ):
        return 8
    return None


def _search_symbol_catalog(
    *,
    query: str,
    limit: int,
    scope: str,
) -> list[dict[str, Any]]:
    query_norm = _normalize_symbol_search_token(query)
    items = _get_symbol_search_catalog(force_refresh=False)
    if query_norm:
        ranked: list[tuple[int, dict[str, Any]]] = []
        for item in items:
            if not _scope_matches(scope, str(item.get("scope", "other"))):
                continue
            rank = _rank_symbol_search_item(item, query_norm)
            if rank is None:
                continue
            ranked.append((rank, item))
        ranked.sort(
            key=lambda pair: (
                pair[0],
                -len(str(pair[1].get("short_code", ""))),
                str(pair[1].get("last_date") or ""),
                str(pair[1].get("symbol", "")),
            ),
            reverse=False,
        )
        return [item for _, item in ranked[:limit]]

    scoped_items = [
        item for item in items if _scope_matches(scope, str(item.get("scope", "other")))
    ]
    scoped_items.sort(
        key=lambda item: (
            str(item.get("last_date") or ""),
            str(item.get("symbol", "")),
        ),
        reverse=True,
    )
    return scoped_items[:limit]


def _load_a_share_stock_codes(selected_exchanges: list[str] | None = None) -> list[str]:
    exchange_set = {
        str(item or "").strip().upper()
        for item in (selected_exchanges or ["SH", "SZ", "BJ"])
        if str(item or "").strip().upper() in _A_SHARE_EXCHANGES
    }
    if not exchange_set:
        return []

    preferred_sectors: list[str] = []
    if "SH" in exchange_set and "SZ" in exchange_set:
        preferred_sectors.append("沪深A股")
    else:
        if "SH" in exchange_set:
            preferred_sectors.append("上证A股")
        if "SZ" in exchange_set:
            preferred_sectors.append("深证A股")
    if "BJ" in exchange_set:
        preferred_sectors.append("BJ")

    collected: list[str] = []
    for sector in preferred_sectors:
        try:
            collected.extend(_fetch_xt_sector_codes(sector))
        except Exception:
            continue
    filtered = sorted(
        {
            code
            for code in collected
            if _is_a_share_symbol(code) and _symbol_exchange(code) in exchange_set
        }
    )
    if filtered:
        return filtered

    raw_codes: list[str] = []
    for sector in sorted(exchange_set):
        try:
            raw_codes.extend(_fetch_xt_sector_codes(sector))
        except Exception:
            continue
    return sorted(
        {
            code
            for code in raw_codes
            if _is_a_share_symbol(code) and _symbol_exchange(code) in exchange_set
        }
    )


def _is_intraday_ingestion_period(period: str) -> bool:
    return str(period or "").strip().lower() in {"1m", "5m", "tick"}


def _apply_data_ingestion_runtime_policy(request_payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(request_payload)
    periods = [
        str(item or "").strip().lower()
        for item in cast(list[Any], payload.get("periods") or [])
        if str(item or "").strip()
    ]
    stock_count = int(payload.get("stock_codes_count", 0) or 0)
    request_mode = str(payload.get("request_mode", "") or "")
    requested_download_workers = cast(int | None, payload.get("download_workers"))
    requested_precompute_workers = cast(int | None, payload.get("precompute_workers"))
    precompute_after_download = bool(payload.get("precompute_after_download", True))
    has_intraday = any(_is_intraday_ingestion_period(period) for period in periods)
    has_tick = "tick" in periods
    intraday_lookback_days = payload.get("intraday_lookback_days")
    intraday_full_history = has_intraday and intraday_lookback_days == 0
    is_large_job = (
        stock_count >= _DATA_INGESTION_HEAVY_JOB_SYMBOL_THRESHOLD
        or request_mode in {"all_stocks", "exchange_selection"}
    )

    download_target = (
        requested_download_workers
        if requested_download_workers is not None
        else _DATA_INGESTION_SAFE_DEFAULT_DOWNLOAD_WORKERS
    )
    precompute_target = (
        requested_precompute_workers
        if requested_precompute_workers is not None
        else _DATA_INGESTION_SAFE_DEFAULT_PRECOMPUTE_WORKERS
    )

    download_cap = _DATA_INGESTION_SMALL_JOB_DOWNLOAD_WORKERS_MAX
    precompute_cap = _DATA_INGESTION_SMALL_JOB_PRECOMPUTE_WORKERS_MAX
    policy_notes: list[str] = []

    if is_large_job:
        download_cap = min(download_cap, _DATA_INGESTION_HEAVY_JOB_DOWNLOAD_WORKERS_MAX)
        precompute_cap = min(precompute_cap, _DATA_INGESTION_HEAVY_JOB_PRECOMPUTE_WORKERS_MAX)
        policy_notes.append("大批量入库任务默认收敛到低并发")

    if has_intraday and is_large_job:
        download_cap = min(download_cap, _DATA_INGESTION_HEAVY_JOB_DOWNLOAD_WORKERS_MAX)
        precompute_cap = min(precompute_cap, _DATA_INGESTION_HEAVY_JOB_PRECOMPUTE_WORKERS_MAX)
        policy_notes.append("分钟线大任务限制为 2 workers")

    if intraday_full_history and is_large_job:
        download_cap = min(download_cap, _DATA_INGESTION_FULL_HISTORY_INTRADAY_DOWNLOAD_WORKERS_MAX)
        precompute_cap = min(precompute_cap, _DATA_INGESTION_FULL_HISTORY_INTRADAY_PRECOMPUTE_WORKERS_MAX)
        policy_notes.append("真全历史分钟线任务强制单路执行")

    if has_tick and stock_count >= _DATA_INGESTION_HEAVY_JOB_SYMBOL_THRESHOLD:
        download_cap = 1
        policy_notes.append("大批量 Tick 任务强制单路执行")

    effective_download_workers = max(1, min(download_target, download_cap))
    if precompute_after_download:
        effective_precompute_workers = max(1, min(precompute_target, precompute_cap))
    else:
        effective_precompute_workers = requested_precompute_workers

    payload["requested_download_workers"] = requested_download_workers
    payload["requested_precompute_workers"] = requested_precompute_workers
    payload["download_workers"] = effective_download_workers
    payload["precompute_workers"] = effective_precompute_workers

    note_parts: list[str] = []
    if effective_download_workers != download_target:
        note_parts.append(f"download_workers {download_target}→{effective_download_workers}")
    if (
        precompute_after_download
        and effective_precompute_workers is not None
        and effective_precompute_workers != precompute_target
    ):
        note_parts.append(f"precompute_workers {precompute_target}→{effective_precompute_workers}")
    for note in policy_notes:
        if note not in note_parts:
            note_parts.append(note)

    if note_parts:
        payload["runtime_policy_note"] = "；".join(note_parts)
        log.warning(
            "data ingestion runtime policy applied: mode=%s stocks=%d periods=%s note=%s",
            request_mode,
            stock_count,
            periods,
            payload["runtime_policy_note"],
        )

    return payload


def _build_data_ingestion_job_request(body: DataIngestionJobCreateBody) -> dict[str, Any]:
    request_mode = "custom_codes"
    requested_exchanges: list[str] = []
    download_workers = body.download_workers
    if download_workers is not None:
        download_workers = int(download_workers)
        if download_workers < 1 or download_workers > 16:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="download_workers 必须在 1 到 16 之间",
            )
    precompute_workers = body.precompute_workers
    if precompute_workers is not None:
        precompute_workers = int(precompute_workers)
        if precompute_workers < 1 or precompute_workers > 16:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="precompute_workers 必须在 1 到 16 之间",
            )

    intraday_lookback_days = body.intraday_lookback_days
    if intraday_lookback_days is not None:
        intraday_lookback_days = int(intraday_lookback_days)
        if intraday_lookback_days < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="intraday_lookback_days 不能小于 0；0 表示真全历史",
            )
    stage_label = str(body.stage_label or "").strip() or None

    # ── stock_codes 优先：显式指定了个股列表时，直接使用，不展开全市场 ──
    explicit_codes = _normalize_string_list(body.stock_codes, uppercase=True)
    if explicit_codes:
        stock_codes: list[str] = explicit_codes
        request_mode = "custom_codes"
        # 如果同时带了 exchanges，只做记录，不展开
        if body.exchanges:
            requested_exchanges = _normalize_string_list(body.exchanges, uppercase=True)
    elif body.exchanges:
        requested_exchanges = _normalize_string_list(body.exchanges, uppercase=True)
        request_mode = "exchange_selection"
        try:
            all_codes: list[str] = []
            a_share_exchanges = [e for e in requested_exchanges if e in _A_SHARE_EXCHANGES]
            other_exchanges = [e for e in requested_exchanges if e not in _A_SHARE_EXCHANGES]
            if a_share_exchanges:
                all_codes.extend(_load_a_share_stock_codes(a_share_exchanges))
            sectors = list(dict.fromkeys(
                _EXCHANGE_SECTOR_MAP.get(e.upper(), e.upper()) for e in other_exchanges
            ))
            for sector in sectors:
                all_codes.extend(_fetch_xt_sector_codes(sector))
            stock_codes: list[str] = sorted(set(all_codes))
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"按交易所获取标的列表失败，请确认 QMT miniQMT 已登录: {exc}",
            ) from exc
        if not stock_codes:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"QMT 返回空列表（exchanges={requested_exchanges}），请确认 miniQMT 已正常登录",
            )
    elif body.use_all_stocks:
        request_mode = "all_stocks"
        requested_exchanges = ["SH", "SZ", "BJ"]
        try:
            stock_codes = _load_a_share_stock_codes(["SH", "SZ", "BJ"])
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"获取全市场股票列表失败，请确认 QMT miniQMT 已登录: {exc}",
            ) from exc
        if not stock_codes:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="QMT 返回空股票列表，请确认 miniQMT 已正常登录",
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="stock_codes 不能为空，或设置 exchanges=[\"SH\",\"SZ\",\"BJ\"] / use_all_stocks=true",
        )
    periods = _normalize_string_list(body.periods or list(_DATA_INGESTION_DEFAULT_PERIODS))
    if not periods:
        periods = list(_DATA_INGESTION_DEFAULT_PERIODS)
    request_payload = {
        "stock_codes": stock_codes,
        "stock_codes_count": len(stock_codes),
        "stock_codes_preview": stock_codes[:10],
        "request_mode": request_mode,
        "requested_exchanges": requested_exchanges,
        "use_all_stocks": body.use_all_stocks,
        "periods": periods,
        "start_date": str(body.start_date or "").strip() or None,
        "end_date": str(body.end_date or "").strip() or None,
        "precompute_after_download": bool(body.precompute_after_download),
        "enable_audit_after_ingest": bool(body.enable_audit_after_ingest),
        "download_workers": download_workers,
        "intraday_lookback_days": intraday_lookback_days,
        "skip_gap_scan": body.skip_gap_scan,
        "precompute_workers": precompute_workers,
        "stage_label": stage_label,
    }
    return _apply_data_ingestion_runtime_policy(request_payload)


def _serialize_data_ingestion_job(record: dict[str, Any]) -> dict[str, Any]:
    progress = {
        key: value
        for key, value in dict(record.get("progress", {})).items()
        if not str(key).startswith("_")
    }
    return {
        "job_id": str(record.get("job_id", "")),
        "status": str(record.get("status", "unknown")),
        "created_at": record.get("created_at"),
        "started_at": record.get("started_at"),
        "finished_at": record.get("finished_at"),
        "request": dict(record.get("request", {})),
        "progress": progress,
        "summary": dict(record.get("summary", {})),
        "error": record.get("error"),
    }


def _finalize_data_ingestion_payload(payload: dict[str, Any]) -> dict[str, Any]:
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


def _calculate_data_ingestion_total_steps(request_payload: dict[str, Any]) -> int:
    periods = cast(list[Any], request_payload.get("periods") or [])
    stock_count = int(request_payload.get("stock_codes_count", 0) or 0)
    period_count = len(periods) or 1
    return max(0, stock_count * period_count)


def _quote_duckdb_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _serialize_duckdb_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            pass
    return str(value)


def _get_api_duckdb_manager_and_path() -> tuple[Any, str]:
    from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

    db_path = resolve_duckdb_path()
    return get_db_manager(db_path), db_path


def _get_duckdb_table_names(con: Any) -> list[str]:
    rows = con.execute("SHOW TABLES").fetchall()
    return sorted(str(row[0]) for row in rows if row and row[0])


def _get_duckdb_table_columns(con: Any, table_name: str) -> list[dict[str, Any]]:
    rows = con.execute(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
        "WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",
        [table_name],
    ).fetchall()
    return [
        {
            "name": str(column_name),
            "type": str(data_type),
            "nullable": str(is_nullable).upper() != "NO",
        }
        for column_name, data_type, is_nullable in rows
    ]


def _resolve_duckdb_table_name(con: Any, table_name: str) -> str:
    normalized = str(table_name or "").strip()
    if not normalized:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="table_name 不能为空",
        )
    table_names = _get_duckdb_table_names(con)
    if normalized not in table_names:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DuckDB 中不存在表 {normalized}",
        )
    return normalized


def _read_duckdb_table_catalog(
    *,
    include_columns: bool = True,
    include_empty: bool = True,
) -> dict[str, Any]:
    db_mgr, db_path = _get_api_duckdb_manager_and_path()
    db_file = Path(db_path)
    database_meta = {
        "path": db_path,
        "exists": db_file.exists(),
        "file_size_bytes": db_file.stat().st_size if db_file.exists() else 0,
        "modified_at": datetime.utcfromtimestamp(db_file.stat().st_mtime).replace(microsecond=0).isoformat() + "Z"
        if db_file.exists()
        else None,
        "table_count": 0,
    }

    with db_mgr.get_read_connection() as con:
        table_names = _get_duckdb_table_names(con)
        database_meta["table_count"] = len(table_names)
        items: list[dict[str, Any]] = []
        for table_name in table_names:
            quoted_table = _quote_duckdb_identifier(table_name)
            columns = _get_duckdb_table_columns(con, table_name)
            row_count = int(con.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0] or 0)  # noqa: S608
            if not include_empty and row_count == 0:
                continue

            column_names = [str(item["name"]) for item in columns]
            time_column = next((name for name in column_names if name in _DB_TABLE_TIME_COLUMNS), None)
            symbol_column = next((name for name in column_names if name in _DB_TABLE_SYMBOL_COLUMNS), None)
            distinct_symbols: int | None = None
            earliest_value: str | int | float | bool | None = None
            latest_value: str | int | float | bool | None = None

            if symbol_column is not None:
                quoted_symbol = _quote_duckdb_identifier(symbol_column)
                distinct_symbols = int(
                    con.execute(f"SELECT COUNT(DISTINCT {quoted_symbol}) FROM {quoted_table}").fetchone()[0] or 0  # noqa: S608
                )
            if time_column is not None and row_count > 0:
                quoted_time = _quote_duckdb_identifier(time_column)
                lower, upper = con.execute(
                    f"SELECT MIN({quoted_time}), MAX({quoted_time}) FROM {quoted_table}"  # noqa: S608
                ).fetchone()
                earliest_value = _serialize_duckdb_scalar(lower)
                latest_value = _serialize_duckdb_scalar(upper)

            items.append(
                {
                    "table_name": table_name,
                    "row_count": row_count,
                    "column_count": len(columns),
                    "columns": columns if include_columns else [],
                    "time_column": time_column,
                    "symbol_column": symbol_column,
                    "distinct_symbols": distinct_symbols,
                    "earliest_value": earliest_value,
                    "latest_value": latest_value,
                }
            )

    items.sort(key=lambda item: (-int(item.get("row_count", 0) or 0), str(item.get("table_name", ""))))
    return {
        "database": database_meta,
        "items": items,
        "returned": len(items),
        "include_columns": include_columns,
        "include_empty": include_empty,
    }


def _read_duckdb_table_rows(
    table_name: str,
    *,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    db_mgr, db_path = _get_api_duckdb_manager_and_path()
    with db_mgr.get_read_connection() as con:
        resolved_name = _resolve_duckdb_table_name(con, table_name)
        quoted_table = _quote_duckdb_identifier(resolved_name)
        columns = _get_duckdb_table_columns(con, resolved_name)
        query = f"SELECT * FROM {quoted_table} LIMIT {int(limit)} OFFSET {int(offset)}"  # noqa: S608
        cursor = con.execute(query)
        header = [str(item[0]) for item in (cursor.description or [])]
        rows = [
            {name: _serialize_duckdb_scalar(value) for name, value in zip(header, row, strict=False)}
            for row in cursor.fetchall()
        ]
        total_rows = int(con.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0] or 0)  # noqa: S608

    return {
        "database_path": db_path,
        "table_name": resolved_name,
        "columns": header,
        "column_details": columns,
        "rows": rows,
        "returned": len(rows),
        "limit": limit,
        "offset": offset,
        "total_rows": total_rows,
    }


def _export_duckdb_table_response(
    table_name: str,
    *,
    export_format: str,
    limit: int,
) -> Response:
    db_mgr, _ = _get_api_duckdb_manager_and_path()
    with db_mgr.get_read_connection() as con:
        resolved_name = _resolve_duckdb_table_name(con, table_name)
        quoted_table = _quote_duckdb_identifier(resolved_name)
        limit_clause = "" if limit == 0 else f" LIMIT {int(limit)}"
        cursor = con.execute(f"SELECT * FROM {quoted_table}{limit_clause}")  # noqa: S608
        header = [str(item[0]) for item in (cursor.description or [])]
        rows = [
            {name: _serialize_duckdb_scalar(value) for name, value in zip(header, row, strict=False)}
            for row in cursor.fetchall()
        ]

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    exported_rows = len(rows)
    if export_format == "jsonl":
        content = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + ("\n" if rows else "")
        filename = f"{resolved_name}_{timestamp}.jsonl"
        media_type = "application/x-ndjson"
    else:
        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
        content = "\ufeff" + buffer.getvalue()
        filename = f"{resolved_name}_{timestamp}.csv"
        media_type = "text/csv; charset=utf-8"

    return Response(
        content=content,
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-EasyXT-Exported-Rows": str(exported_rows),
        },
    )


def _validate_readonly_sql(sql: str) -> str:
    """检查 SQL 只含 SELECT 语句，返回清洗后的文本；不通过则抛 HTTPException。"""
    import re

    # 去掉行注释 (-- ...) 和块注释 (/* ... */)
    cleaned = re.sub(r"--[^\n]*", " ", sql)
    cleaned = re.sub(r"/\*.*?\*/", " ", cleaned, flags=re.DOTALL)
    cleaned = cleaned.strip()
    if not cleaned:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="SQL 不能为空")
    # 只允许 SELECT 作为首个关键字（防止 INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/COPY 等）
    first_token = re.split(r"\s+", cleaned, maxsplit=1)[0].upper()
    if first_token != "SELECT":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"只允许 SELECT 查询，当前首关键字为 '{first_token}'",
        )
    return cleaned


def _execute_readonly_sql_query(body: DbQueryBody) -> dict[str, Any]:
    import re

    cleaned_sql = _validate_readonly_sql(body.sql)
    limit = max(0, min(int(body.limit), 1000))

    # 如果用户没有写 LIMIT 或者写的 LIMIT 比我们的上限大，追加兜底 LIMIT
    has_limit = bool(re.search(r"\bLIMIT\b", cleaned_sql, re.IGNORECASE))
    if limit > 0 and not has_limit:
        bounded_sql = f"{cleaned_sql.rstrip(';')} LIMIT {limit}"
    else:
        bounded_sql = cleaned_sql

    db_mgr, db_path = _get_api_duckdb_manager_and_path()
    with db_mgr.get_read_connection() as con:
        try:
            cursor = con.execute(bounded_sql)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"SQL 执行失败: {exc}",
            ) from exc
        header = [str(item[0]) for item in (cursor.description or [])]
        rows = [
            {name: _serialize_duckdb_scalar(value) for name, value in zip(header, row, strict=False)}
            for row in cursor.fetchall()
        ]

    return {
        "database_path": db_path,
        "sql": body.sql,
        "bounded_sql": bounded_sql,
        "columns": header,
        "rows": rows,
        "returned": len(rows),
        "limit": limit,
        "truncated": limit > 0 and len(rows) >= limit,
    }


def _import_csv_to_duckdb_table(body: DbCsvImportBody) -> dict[str, Any]:
    mode = str(body.mode or "append").strip().lower()
    if mode not in ("append", "replace"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"mode 只能为 'append' 或 'replace'，收到 '{mode}'",
        )
    if not body.table_name or not body.table_name.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="table_name 不能为空")

    # 解析 CSV
    raw_csv = str(body.csv_content or "").strip()
    if not raw_csv:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="csv_content 不能为空")
    reader = csv.DictReader(StringIO(raw_csv))
    uploaded_rows = list(reader)
    fieldnames: list[str] = list(reader.fieldnames or [])
    if not fieldnames:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="CSV 无法解析表头")
    if not uploaded_rows:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="CSV 无数据行")

    db_mgr, db_path = _get_api_duckdb_manager_and_path()
    with db_mgr.get_write_connection() as con:
        # 确认目标表存在
        resolved_name = _resolve_duckdb_table_name(con, body.table_name)
        quoted_table = _quote_duckdb_identifier(resolved_name)

        # 验证上传的列名在目标表中都存在
        existing_columns_info = _get_duckdb_table_columns(con, resolved_name)
        existing_col_names = {item["name"] for item in existing_columns_info}
        unknown_cols = [col for col in fieldnames if col not in existing_col_names]
        if unknown_cols:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"CSV 中包含目标表不存在的列: {unknown_cols}",
            )

        if mode == "replace":
            con.execute(f"DELETE FROM {quoted_table}")  # noqa: S608

        # 批量插入 — 用参数化 INSERT 逐行写入，避免 SQL 注入
        quoted_cols = ", ".join(_quote_duckdb_identifier(col) for col in fieldnames)
        placeholders = ", ".join("?" * len(fieldnames))
        insert_sql = f"INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})"  # noqa: S608
        inserted_count = 0
        for row in uploaded_rows:
            values = [row.get(col) for col in fieldnames]
            con.execute(insert_sql, values)
            inserted_count += 1

    return {
        "database_path": db_path,
        "table_name": resolved_name,
        "mode": mode,
        "inserted_rows": inserted_count,
        "columns_written": fieldnames,
    }


def _execute_data_ingestion_job(
    stock_codes: list[str],
    periods: list[str],
    start_date: str | None,
    end_date: str | None,
    precompute_after_download: bool,
    enable_audit_after_ingest: bool,
    download_workers: int | None,
    intraday_lookback_days: int | None,
    skip_gap_scan: bool | None,
    precompute_workers: int | None,
    stage_label: str | None,
    progress_cb,
    period_start_cb,
    stop_event: threading.Event,
) -> dict[str, Any]:
    from data_manager.auto_data_updater import AutoDataUpdater

    updater = AutoDataUpdater()
    if not precompute_after_download:
        updater._precompute_custom_period_bars = lambda *args, **kwargs: None  # type: ignore[method-assign]
    return updater.bulk_download(
        stock_codes=stock_codes,
        periods=periods,
        start_date=start_date,
        end_date=end_date,
        on_progress=progress_cb,
        on_period_start=period_start_cb,
        stop_event=stop_event,
        enable_audit=enable_audit_after_ingest,
        download_workers=download_workers,
        intraday_max_lookback_days=intraday_lookback_days,
        skip_gap_scan=skip_gap_scan,
        precompute_workers=precompute_workers,
        stage_label=stage_label,
    )


def _run_data_ingestion_job(job_id: str) -> None:
    with _data_ingestion_jobs_lock:
        job = _data_ingestion_jobs.get(job_id)
        if job is None:
            return
        request_payload = dict(job.get("request", {}))
        stop_event = job.get("_stop_event")
        if not isinstance(stop_event, threading.Event):
            return
        job["status"] = "running"
        job["started_at"] = _now_iso()
        _total_steps = _calculate_data_ingestion_total_steps(request_payload)
        job["progress"].update(
            {
                "current": 0,
                "total": _total_steps,
                "processed_stocks": 0,
                "current_stock_code": None,
                "current_period": None,
                "last_status": None,
                "message": "下载任务已启动",
                "updated_at": _now_iso(),
                "_period_counter": 0,
            }
        )

    def _on_progress(current: int, total: int, stock_code: str, period: str, status_text: str, **kwargs: Any) -> None:
        with _data_ingestion_jobs_lock:
            record = _data_ingestion_jobs.get(job_id)
            if record is None:
                return
            if record.get("status") == "queued":
                record["status"] = "running"
            record["progress"].update(
                {
                    "processed_stocks": int(current),
                    "current_stock_code": stock_code,
                    "last_status": status_text,
                    "message": f"股票进度 {current}/{total}: {stock_code} -> {status_text}",
                    "updated_at": _now_iso(),
                    # 拆分字段：stock_progress 独立于 period_progress
                    "stock_progress": {
                        "completed": int(current),
                        "total": int(total),
                        "stock_code": stock_code,
                        "status": status_text,
                    },
                }
            )
            # 实时暴露 source / flow breakdown
            if kwargs.get("source_breakdown"):
                record["progress"]["source_breakdown"] = kwargs["source_breakdown"]
            if kwargs.get("flow_breakdown"):
                record["progress"]["flow_breakdown"] = kwargs["flow_breakdown"]

    def _on_period_start(
        stock_code: str,
        period: str,
        resolved_start: str,
        resolved_end: str,
    ) -> None:
        with _data_ingestion_jobs_lock:
            record = _data_ingestion_jobs.get(job_id)
            if record is None:
                return
            if record.get("status") == "queued":
                record["status"] = "running"
            progress = cast(dict[str, Any], record.get("progress", {}))
            # 每次新周期开始，细粒度计数器 +1
            period_counter = int(progress.get("_period_counter", 0) or 0) + 1
            progress["_period_counter"] = period_counter
            total = int(progress.get("total", 1) or 1)
            progress.update(
                {
                    "current": period_counter,
                    "total": total,
                    "current_stock_code": stock_code,
                    "current_period": period,
                    "last_status": "running",
                    "message": f"[{period_counter}/{total}] {stock_code} {period} 下载中 ({resolved_start} ~ {resolved_end})",
                    "updated_at": _now_iso(),
                    # 拆分字段：period_progress 独立于 stock_progress
                    "period_progress": {
                        "current": period_counter,
                        "total": total,
                        "stock_code": stock_code,
                        "period": period,
                        "resolved_start": resolved_start,
                        "resolved_end": resolved_end,
                    },
                }
            )

    try:
        result = _execute_data_ingestion_job(
            stock_codes=cast(list[str], request_payload["stock_codes"]),
            periods=cast(list[str], request_payload["periods"]),
            start_date=cast(str | None, request_payload.get("start_date")),
            end_date=cast(str | None, request_payload.get("end_date")),
            precompute_after_download=bool(request_payload.get("precompute_after_download", True)),
            enable_audit_after_ingest=bool(request_payload.get("enable_audit_after_ingest", True)),
            download_workers=cast(int | None, request_payload.get("download_workers")),
            intraday_lookback_days=cast(int | None, request_payload.get("intraday_lookback_days")),
            skip_gap_scan=cast(bool | None, request_payload.get("skip_gap_scan")),
            precompute_workers=cast(int | None, request_payload.get("precompute_workers")),
            stage_label=cast(str | None, request_payload.get("stage_label")),
            progress_cb=_on_progress,
            period_start_cb=_on_period_start,
            stop_event=stop_event,
        )
    except Exception as exc:
        with _data_ingestion_jobs_lock:
            record = _data_ingestion_jobs.get(job_id)
            if record is None:
                return
            record["status"] = "failed"
            record["finished_at"] = _now_iso()
            record["error"] = str(exc)
            record["progress"].update(
                {
                    "message": f"任务失败: {exc}",
                    "updated_at": _now_iso(),
                }
            )
        _invalidate_basic_arsenal_status_cache()
        return

    processed = len(cast(list[Any], result.get("results", []) or []))
    stock_total = int(result.get("total_stocks", request_payload.get("stock_codes_count", 0)) or 0)
    with _data_ingestion_jobs_lock:
        record = _data_ingestion_jobs.get(job_id)
        if record is None:
            return
        progress = cast(dict[str, Any], record.get("progress", {}))
        step_total = int(progress.get("total", 0) or 0)
        period_counter = int(progress.get("_period_counter", 0) or 0)
        force_abort_requested = bool(record.get("_force_abort_requested", False))
        cancelled = force_abort_requested or (stop_event.is_set() and processed < stock_total)
        completed_steps = step_total if not cancelled else min(period_counter, step_total)
        record["summary"] = {
            "processed_stocks": processed,
            "success_stocks": int(result.get("success_stocks", 0) or 0),
            "failed_stocks": int(result.get("failed_stocks", 0) or 0),
            "total_records": int(result.get("total_records", 0) or 0),
            "audit_passed_stocks": int(result.get("audit_passed_stocks", 0) or 0),
            "audit_failed_stocks": int(result.get("audit_failed_stocks", 0) or 0),
            "repair_queued_stocks": int(result.get("repair_queued_stocks", 0) or 0),
            "source_breakdown": dict(result.get("source_breakdown") or {}),
            "flow_breakdown": dict(result.get("flow_breakdown") or {}),
            "source_breakdown_by_period": {
                str(period): {
                    str(source_id): int(count or 0)
                    for source_id, count in cast(dict[str, Any], breakdown).items()
                }
                for period, breakdown in cast(
                    dict[str, Any],
                    result.get("source_breakdown_by_period") or {},
                ).items()
            },
        }
        record["finished_at"] = _now_iso()
        record["error"] = "被用户强制终止" if force_abort_requested else None
        record["status"] = "cancelled" if cancelled else "completed"
        record["progress"].update(
            {
                "current": completed_steps,
                "total": step_total,
                "processed_stocks": processed,
                "message": (
                    f"任务已强制终止：已处理 {processed}/{stock_total} 只股票"
                    if force_abort_requested
                    else f"任务已取消：已处理 {processed}/{stock_total} 只股票"
                    if cancelled
                    else f"任务完成：已处理 {processed}/{stock_total} 只股票"
                ),
                "updated_at": _now_iso(),
            }
        )
    _invalidate_basic_arsenal_status_cache()


def _shutdown_data_ingestion_jobs(timeout_s: float = 0.2) -> None:
    with _data_ingestion_jobs_lock:
        threads = []
        for record in _data_ingestion_jobs.values():
            stop_event = record.get("_stop_event")
            if isinstance(stop_event, threading.Event):
                stop_event.set()
            thread = record.get("_thread")
            if isinstance(thread, threading.Thread):
                threads.append(thread)
    deadline = time.monotonic() + timeout_s
    for thread in threads:
        remaining = max(0.0, deadline - time.monotonic())
        if remaining <= 0:
            break
        try:
            thread.join(timeout=remaining)
        except Exception:
            pass


def _resolve_root_relative_path(raw_path: str | Path | None, default_path: Path) -> Path:
    text = str(raw_path or "").strip()
    if not text:
        return default_path
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = (_ROOT_DIR / path).resolve()
    return path


def _load_coverage_matrix_config_summary(config_path: str | Path | None = None) -> dict[str, Any]:
    from tools.run_coverage_matrix import expand_matrix_tasks, load_matrix_config

    resolved_path = _resolve_root_relative_path(config_path, _COVERAGE_MATRIX_DEFAULT_CONFIG_PATH)
    config = load_matrix_config(resolved_path)
    tasks = expand_matrix_tasks(config)
    meta = cast(dict[str, Any], config.get("meta") or {})
    matrix = cast(dict[str, Any], config.get("matrix") or {})
    runtime = cast(dict[str, Any], config.get("runtime") or {})

    groups: list[dict[str, Any]] = []
    symbol_count = 0
    for group in cast(list[dict[str, Any]], matrix.get("groups") or []):
        group_symbols = [
            {
                "symbol": str(item.get("symbol", "")).upper(),
                "name": str(item.get("name", "") or str(item.get("symbol", "")).upper()),
            }
            for item in cast(list[dict[str, Any]], group.get("symbols") or [])
            if str(item.get("symbol", "")).strip()
        ]
        symbol_count += len(group_symbols)
        groups.append(
            {
                "id": str(group.get("id", "")),
                "asset_class": str(group.get("asset_class", group.get("id", "unknown"))),
                "symbol_count": len(group_symbols),
                "symbols": group_symbols,
            }
        )

    windows = [
        {
            "id": str(window.get("id", "")),
            "start": str(window.get("start", "")),
            "end": str(window.get("end", "")),
            "regime": str(window.get("regime", "")),
        }
        for window in cast(list[dict[str, Any]], matrix.get("windows") or [])
    ]

    return {
        "config_path": str(resolved_path),
        "config_name": str(meta.get("name", "unnamed_matrix")),
        "anchor_period": str(matrix.get("anchor_period", "1d")),
        "task_count": len(tasks),
        "group_count": len(groups),
        "symbol_count": symbol_count,
        "periods": [int(value) for value in cast(list[Any], matrix.get("periods") or [])],
        "windows": windows,
        "groups": groups,
        "runtime_defaults": {
            "workers": int(runtime.get("workers", 1) or 1),
            "resume": bool(runtime.get("resume", True)),
            "tolerance_tick": float(runtime.get("tolerance_tick", 0.01) or 0.01),
            "split_ratio": float(runtime.get("split_ratio", 0.8) or 0.8),
            "duckdb_path": runtime.get("duckdb_path"),
        },
    }


def _summarize_coverage_matrix_report(report: dict[str, Any]) -> dict[str, Any]:
    issue_buckets: dict[tuple[str, str], dict[str, Any]] = {}
    status_order = {"error": 0, "fail": 1, "partial": 2, "unknown": 3}

    def _normalize_issue_reason(status: str, reason: Any) -> str:
        text = str(reason or "").strip()
        if not text:
            return "unknown"
        if status == "error" and text.startswith("无法获取 ") and text.endswith(" 的真实日线数据"):
            return "缺少真实日线数据"
        if text == "本地 DuckDB 中缺少可用 1m 数据":
            return "缺少可用 1m 数据"
        return text

    def _extract_issue_entries(result: dict[str, Any]) -> list[tuple[str, str]]:
        result_status = str(result.get("status", "unknown") or "unknown")
        if result_status == "pass":
            return []
        if result_status == "error":
            return [(result_status, _normalize_issue_reason(result_status, result.get("error")))]

        payload = cast(dict[str, Any], result.get("payload") or {})
        experiments = cast(dict[str, Any], payload.get("experiments") or {})
        experiment_labels = {
            "time_consistency": "时间一致性",
            "causal_pollution_guard": "因果防污染",
            "cross_scale_convergence": "跨尺度收敛",
        }
        entries: list[tuple[str, str]] = []
        for experiment_name, experiment_payload in experiments.items():
            experiment = cast(dict[str, Any], experiment_payload or {})
            experiment_status = str(experiment.get("status", "unknown") or "unknown")
            if experiment_status == "pass":
                continue
            label = experiment_labels.get(experiment_name, experiment_name)
            reason = _normalize_issue_reason(result_status, experiment.get("reason") or experiment_status)
            entries.append((result_status, f"{label}: {reason}"))
        if entries:
            return entries
        return [(result_status, _normalize_issue_reason(result_status, result.get("error") or result_status))]

    for item in cast(list[dict[str, Any]], report.get("results") or []):
        symbol = str(item.get("symbol", "") or "").upper()
        window_id = str(item.get("window_id", "") or "")
        intraday_period = item.get("intraday_period")
        for issue_status, issue_reason in _extract_issue_entries(item):
            key = (issue_status, issue_reason)
            bucket = issue_buckets.setdefault(
                key,
                {
                    "status": issue_status,
                    "reason": issue_reason,
                    "count": 0,
                    "symbols": set(),
                    "windows": set(),
                    "periods": set(),
                },
            )
            bucket["count"] = int(bucket["count"]) + 1
            if symbol:
                cast(set[str], bucket["symbols"]).add(symbol)
            if window_id:
                cast(set[str], bucket["windows"]).add(window_id)
            if intraday_period is not None:
                cast(set[int], bucket["periods"]).add(int(intraday_period))

    issue_breakdown = [
        {
            "status": str(bucket["status"]),
            "reason": str(bucket["reason"]),
            "count": int(bucket["count"]),
            "symbols": sorted(cast(set[str], bucket["symbols"])),
            "windows": sorted(cast(set[str], bucket["windows"])),
            "periods": sorted(cast(set[int], bucket["periods"])),
        }
        for bucket in sorted(
            issue_buckets.values(),
            key=lambda item: (
                status_order.get(str(item["status"]), 99),
                -int(item["count"]),
                str(item["reason"]),
            ),
        )
    ]

    return {
        "generated_at": report.get("generated_at"),
        "config_meta": dict(report.get("config_meta") or {}),
        "totals": dict(report.get("totals") or {}),
        "stopped_early": bool(report.get("stopped_early", False)),
        "by_group": list(report.get("by_group") or []),
        "by_symbol_period": list(report.get("by_symbol_period") or []),
        "issue_breakdown": issue_breakdown,
        "artifacts": dict(report.get("artifacts") or {}),
        "output_dir": report.get("output_dir"),
    }


def _load_coverage_matrix_report_summary(report_path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return _summarize_coverage_matrix_report(payload)


def _load_latest_coverage_matrix_report_summary() -> dict[str, Any] | None:
    if not _COVERAGE_MATRIX_OUTPUT_ROOT.exists():
        return None
    candidates: list[tuple[float, Path]] = []
    for report_path in _COVERAGE_MATRIX_OUTPUT_ROOT.glob("*/coverage_matrix_results.json"):
        try:
            candidates.append((report_path.stat().st_mtime, report_path))
        except OSError:
            continue
    if not candidates:
        return None
    _, latest_path = max(candidates, key=lambda item: item[0])
    summary = _load_coverage_matrix_report_summary(latest_path)
    if summary is None:
        return None
    summary["source"] = "disk"
    return summary


def _build_coverage_matrix_job_request(body: CoverageMatrixJobCreateBody) -> dict[str, Any]:
    config_summary = _load_coverage_matrix_config_summary(body.config_path or None)
    config_path = str(config_summary["config_path"])
    workers_default = int(cast(dict[str, Any], config_summary["runtime_defaults"]).get("workers", 1) or 1)
    resume_default = bool(cast(dict[str, Any], config_summary["runtime_defaults"]).get("resume", True))
    workers = int(body.workers if body.workers is not None else workers_default)
    if workers < 1 or workers > 32:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="workers 必须在 1 到 32 之间",
        )
    output_dir_text = str(body.output_dir or "").strip()
    return {
        "config_path": config_path,
        "output_dir": output_dir_text,
        "workers": workers,
        "resume": resume_default if body.resume is None else bool(body.resume),
        "config_name": config_summary["config_name"],
        "task_count": int(config_summary["task_count"]),
        "periods": list(config_summary["periods"]),
        "windows": [str(item.get("id", "")) for item in cast(list[dict[str, Any]], config_summary.get("windows") or [])],
        "groups": [str(item.get("id", "")) for item in cast(list[dict[str, Any]], config_summary.get("groups") or [])],
    }


def _serialize_coverage_matrix_job(record: dict[str, Any]) -> dict[str, Any]:
    latest_report = record.get("latest_report")
    return {
        "job_id": str(record.get("job_id", "")),
        "status": str(record.get("status", "unknown")),
        "created_at": record.get("created_at"),
        "started_at": record.get("started_at"),
        "finished_at": record.get("finished_at"),
        "request": dict(record.get("request", {})),
        "progress": dict(record.get("progress", {})),
        "summary": dict(record.get("summary", {})),
        "artifacts": dict(record.get("artifacts", {})),
        "latest_report": dict(latest_report) if isinstance(latest_report, dict) else None,
        "error": record.get("error"),
    }


def _finalize_coverage_matrix_payload(payload: dict[str, Any]) -> dict[str, Any]:
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


def _execute_coverage_matrix_job(
    *,
    config_path: str,
    output_dir: str,
    workers: int,
    resume: bool,
    progress_cb,
    stop_event: threading.Event,
) -> dict[str, Any]:
    from tools.run_coverage_matrix import load_matrix_config, run_matrix

    config = load_matrix_config(config_path)
    return run_matrix(
        config,
        output_dir=output_dir,
        workers=workers,
        resume=resume,
        progress_callback=progress_cb,
        stop_requested=stop_event.is_set,
    )


def _build_coverage_matrix_job_summary(report_summary: dict[str, Any]) -> dict[str, Any]:
    totals = cast(dict[str, Any], report_summary.get("totals") or {})
    executed_tasks = int(totals.get("total_tasks", 0) or 0)
    configured_tasks = int(totals.get("configured_tasks", executed_tasks) or executed_tasks)
    return {
        "completed_tasks": executed_tasks,
        "configured_tasks": configured_tasks,
        "pass_tasks": int(totals.get("pass_tasks", 0) or 0),
        "partial_tasks": int(totals.get("partial_tasks", 0) or 0),
        "fail_tasks": int(totals.get("fail_tasks", 0) or 0),
        "error_tasks": int(totals.get("error_tasks", 0) or 0),
        "unknown_tasks": int(totals.get("unknown_tasks", 0) or 0),
        "pass_ratio": float(totals.get("pass_ratio", 0.0) or 0.0),
        "remaining_tasks": int(totals.get("remaining_tasks", max(0, configured_tasks - executed_tasks)) or 0),
    }


def _run_coverage_matrix_job(job_id: str) -> None:
    with _coverage_matrix_jobs_lock:
        job = _coverage_matrix_jobs.get(job_id)
        if job is None:
            return
        request_payload = dict(job.get("request", {}))
        stop_event = job.get("_stop_event")
        if not isinstance(stop_event, threading.Event):
            return
        job["status"] = "running"
        job["started_at"] = _now_iso()
        job["progress"].update(
            {
                "completed_tasks": 0,
                "total_tasks": int(request_payload.get("task_count", 0) or 0),
                "current_task_id": None,
                "current_symbol": None,
                "current_period": None,
                "current_window_id": None,
                "last_status": None,
                "message": "覆盖矩阵任务已启动",
                "updated_at": _now_iso(),
            }
        )
        job["summary"] = {
            "completed_tasks": 0,
            "configured_tasks": int(request_payload.get("task_count", 0) or 0),
            "pass_tasks": 0,
            "partial_tasks": 0,
            "fail_tasks": 0,
            "error_tasks": 0,
            "unknown_tasks": 0,
            "pass_ratio": 0.0,
            "remaining_tasks": int(request_payload.get("task_count", 0) or 0),
        }
        job["artifacts"] = {}
        job["latest_report"] = None
        job["_status_counts"] = {"pass": 0, "partial": 0, "fail": 0, "error": 0, "unknown": 0}

    def _on_progress(completed: int, total: int, task: Any, task_status: str) -> None:
        with _coverage_matrix_jobs_lock:
            record = _coverage_matrix_jobs.get(job_id)
            if record is None:
                return
            counts = cast(dict[str, int], record.get("_status_counts") or {})
            normalized_status = task_status if task_status in counts else "unknown"
            counts[normalized_status] = int(counts.get(normalized_status, 0)) + 1
            record["summary"].update(
                {
                    "completed_tasks": int(completed),
                    "configured_tasks": int(total),
                    "pass_tasks": int(counts.get("pass", 0)),
                    "partial_tasks": int(counts.get("partial", 0)),
                    "fail_tasks": int(counts.get("fail", 0)),
                    "error_tasks": int(counts.get("error", 0)),
                    "unknown_tasks": int(counts.get("unknown", 0)),
                    "pass_ratio": round(int(counts.get("pass", 0)) / completed, 6) if completed else 0.0,
                    "remaining_tasks": max(0, int(total) - int(completed)),
                }
            )
            record["progress"].update(
                {
                    "completed_tasks": int(completed),
                    "total_tasks": int(total),
                    "current_task_id": str(getattr(task, "task_id", "")),
                    "current_symbol": str(getattr(task, "symbol", "")),
                    "current_period": f"{int(getattr(task, 'intraday_period', 0))}m" if getattr(task, "intraday_period", None) is not None else None,
                    "current_window_id": str(getattr(task, "window_id", "")),
                    "last_status": normalized_status,
                    "message": f"[{completed}/{total}] {getattr(task, 'symbol', '')} {getattr(task, 'window_id', '')} {getattr(task, 'intraday_period', '')}m -> {normalized_status}",
                    "updated_at": _now_iso(),
                }
            )

    try:
        report = _execute_coverage_matrix_job(
            config_path=str(request_payload.get("config_path", "")),
            output_dir=str(request_payload.get("output_dir", "")),
            workers=int(request_payload.get("workers", 1) or 1),
            resume=bool(request_payload.get("resume", True)),
            progress_cb=_on_progress,
            stop_event=stop_event,
        )
    except Exception as exc:
        with _coverage_matrix_jobs_lock:
            record = _coverage_matrix_jobs.get(job_id)
            if record is None:
                return
            record["status"] = "failed"
            record["finished_at"] = _now_iso()
            record["error"] = str(exc)
            record["progress"].update(
                {
                    "message": f"覆盖矩阵任务失败: {exc}",
                    "updated_at": _now_iso(),
                }
            )
        _append_governance_action_audit(
            action_id="coverage_matrix_job_failed",
            action_type="coverage_matrix_failed",
            tone="danger",
            title="覆盖矩阵任务失败",
            detail=f"job={job_id[:8]} error={exc}",
            source="api_server",
            payload={
                "job_id": job_id,
                "config_path": request_payload.get("config_path", ""),
                "output_dir": request_payload.get("output_dir", ""),
                "error": str(exc),
            },
        )
        return

    report_summary = _summarize_coverage_matrix_report(report)
    summary = _build_coverage_matrix_job_summary(report_summary)
    cancelled = bool(report_summary.get("stopped_early", False)) and stop_event.is_set()
    final_status = "cancelled" if cancelled else "completed"
    with _coverage_matrix_jobs_lock:
        record = _coverage_matrix_jobs.get(job_id)
        if record is None:
            return
        record["status"] = final_status
        record["finished_at"] = _now_iso()
        record["error"] = None
        record["summary"] = summary
        record["artifacts"] = dict(report_summary.get("artifacts") or {})
        record["latest_report"] = report_summary
        record["progress"].update(
            {
                "completed_tasks": int(summary["completed_tasks"]),
                "total_tasks": int(summary["configured_tasks"]),
                "message": (
                    f"覆盖矩阵任务已取消：已完成 {summary['completed_tasks']}/{summary['configured_tasks']}"
                    if cancelled
                    else f"覆盖矩阵任务完成：pass={summary['pass_tasks']} fail={summary['fail_tasks']} error={summary['error_tasks']}"
                ),
                "updated_at": _now_iso(),
            }
        )

    tone = "ok"
    if int(summary["error_tasks"]) > 0 or int(summary["fail_tasks"]) > 0:
        tone = "danger"
    elif int(summary["partial_tasks"]) > 0 or cancelled:
        tone = "warning"
    _append_governance_action_audit(
        action_id="coverage_matrix_job_completed",
        action_type="coverage_matrix_completed",
        tone=tone,
        title="覆盖矩阵任务完成",
        detail=f"job={job_id[:8]} completed={summary['completed_tasks']}/{summary['configured_tasks']}",
        source="api_server",
        payload={
            "job_id": job_id,
            "config_path": request_payload.get("config_path", ""),
            "output_dir": request_payload.get("output_dir", ""),
            "status": final_status,
            "totals": summary,
        },
    )


def _shutdown_coverage_matrix_jobs(timeout_s: float = 0.5) -> None:
    with _coverage_matrix_jobs_lock:
        threads: list[threading.Thread] = []
        for record in _coverage_matrix_jobs.values():
            stop_event = record.get("_stop_event")
            if isinstance(stop_event, threading.Event):
                stop_event.set()
            thread = record.get("_thread")
            if isinstance(thread, threading.Thread):
                threads.append(thread)
    deadline = time.monotonic() + timeout_s
    for thread in threads:
        remaining = max(0.0, deadline - time.monotonic())
        if remaining <= 0:
            break
        try:
            thread.join(timeout=remaining)
        except Exception:
            pass

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
_datasource_health_access_lock = threading.RLock()
_datasource_health_interface: Any = None
_data_governance_controller_lock = threading.Lock()
_data_governance_controller: Any = None
_trade_autorestore_task: asyncio.Task[Any] | None = None


_LOCALHOST_IPS: frozenset[str] = frozenset({"127.0.0.1", "::1", "localhost"})


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in _TRUTHY_ENV_VALUES


def _trade_autorestore_enabled() -> bool:
    """是否在服务启动后后台恢复可连接的交易账户。"""
    return _env_truthy("EASYXT_TRADE_AUTORESTORE", default=(_DEV_MODE and not _TEST_MODE))


def _trade_autorestore_retry_delays() -> tuple[float, ...]:
    """启动恢复的额外重试等待序列（秒），用于覆盖 QMT 冷启动的附着窗口。"""
    raw = str(os.environ.get("EASYXT_TRADE_AUTORESTORE_RETRY_DELAYS", "") or "").strip()
    if not raw:
        return (0.0, 15.0, 45.0)

    delays: list[float] = []
    for part in raw.split(","):
        text = str(part or "").strip()
        if not text:
            continue
        try:
            delays.append(max(0.0, float(text)))
        except ValueError:
            log.warning("忽略非法 EASYXT_TRADE_AUTORESTORE_RETRY_DELAYS 项: %s", text)
    return tuple(delays) if delays else (0.0, 15.0, 45.0)


def _check_rate_limit(client_ip: str) -> bool:
    """返回 True 表示放行，False 表示已超限（同时递增命中计数）。线程安全。"""
    global _rate_limit_hits
    if _RATE_LIMIT <= 0:
        return True
    # 开发模式下本机 IP 不限流（页面刷新会并发发出多个 init 请求，很容易打爆 60/min 限额）
    if _DEV_MODE and client_ip in _LOCALHOST_IPS:
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


def _call_with_datasource_health_interface(callback: Callable[[Any], Any]) -> Any:
    with _datasource_health_access_lock:
        iface = _get_datasource_health_interface()
        return callback(iface)


def _probe_db_health_status(timeout_s: float = 0.25) -> str:
    result: dict[str, str] = {"status": "ok"}

    def _worker() -> None:
        try:
            _call_with_datasource_health_interface(
                lambda iface: iface.data_registry.get_health_summary()
            )
        except Exception:
            result["status"] = "unavailable"

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    thread.join(timeout_s)
    if thread.is_alive():
        return "timeout"
    return result["status"]


def _get_sla_health_payload(report_date: str = "", *, persist: bool = True) -> dict[str, Any]:
    payload: dict[str, Any] = {"status": "ok"}
    try:
        payload["sla"] = _call_with_datasource_health_interface(
            lambda iface: iface.generate_daily_sla_report(report_date or None, persist=persist)
        )
        if not payload["sla"].get("gate_pass", True):
            payload["status"] = "degraded"
    except Exception as e:
        payload["status"] = "degraded"
        payload["error"] = str(e)
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


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


_CHART_ADJUST_OPTIONS = {"none", "front", "back", "geometric_front", "geometric_back"}
_TRADING_MONTH_RUNTIME_TO_DAYS: dict[str, int] = {
    "2M": 42,
    "3M": 63,
    "5M": 105,
}

_NATURAL_MONTH_RUNTIME_TO_MONTHS: dict[str, int] = {
    "1M": 1,
    "1Q": 3,
    "6M": 6,
}


@lru_cache(maxsize=1)
def _get_chart_period_registry():
    from data_manager.period_registry import PeriodRegistry

    file_path = str(
        os.environ.get("EASYXT_PERIOD_REGISTRY_FILE", "config/period_registry.json")
    ).strip()
    return PeriodRegistry(file_path=file_path)


def _chart_interval_sort_key(interval: str) -> tuple[int, int, str]:
    runtime = str(interval or "").strip()
    if runtime == "tick":
        return (0, 1, runtime)
    if runtime.endswith("m") and runtime[:-1].isdigit():
        return (1, int(runtime[:-1]), runtime)
    if runtime.endswith("h") and runtime[:-1].isdigit():
        return (1, int(runtime[:-1]) * 60, runtime)
    if runtime.endswith("d") and runtime[:-1].isdigit():
        return (2, int(runtime[:-1]), runtime)
    if runtime in _TRADING_MONTH_RUNTIME_TO_DAYS:
        return (2, _TRADING_MONTH_RUNTIME_TO_DAYS[runtime], runtime)
    if runtime.endswith("w") and runtime[:-1].isdigit():
        return (3, int(runtime[:-1]), runtime)
    if runtime in _NATURAL_MONTH_RUNTIME_TO_MONTHS:
        return (4, _NATURAL_MONTH_RUNTIME_TO_MONTHS[runtime], runtime)
    if runtime.endswith("Y") and runtime[:-1].isdigit():
        return (5, int(runtime[:-1]), runtime)
    return (9, 0, runtime)


def _list_chart_enabled_intervals() -> list[str]:
    registry = _get_chart_period_registry()
    items: list[str] = []
    for definition in registry.list_definitions(enabled_only=True):
        runtime = str(definition.runtime_code or definition.period_code).strip()
        if runtime and runtime not in items:
            items.append(runtime)
    return sorted(items, key=_chart_interval_sort_key)


def _resolve_chart_period_definition(interval: str):
    requested = str(interval or "").strip() or "1d"
    registry = _get_chart_period_registry()
    try:
        return registry.resolve(requested)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"interval 参数非法: {requested}；当前已启用周期: "
                f"{_list_chart_enabled_intervals()}"
            ),
        ) from exc


def _chart_runtime_code(resolved_period: Any) -> str:
    return str(getattr(resolved_period, "runtime_code", None) or resolved_period.period_code)


def _parse_chart_intraday_minutes(runtime: str) -> int | None:
    text = str(runtime or "").strip()
    if text.endswith("m") and text[:-1].isdigit():
        return int(text[:-1])
    if text.endswith("h") and text[:-1].isdigit():
        return int(text[:-1]) * 60
    return None


def _chart_default_range(resolved_period: Any) -> timedelta:
    runtime = _chart_runtime_code(resolved_period)
    period_family = str(getattr(resolved_period, "period_family", "") or "")

    if runtime == "tick":
        return timedelta(days=2)

    intraday_minutes = _parse_chart_intraday_minutes(runtime)
    if intraday_minutes is not None:
        if intraday_minutes <= 5:
            return timedelta(days=5)
        if intraday_minutes <= 30:
            return timedelta(days=15)
        if intraday_minutes <= 120:
            return timedelta(days=45)
        return timedelta(days=120)

    # 日级以上周期默认走“全历史窗口”。此前 1d 默认只有 365 天，
    # 导致 Gate 覆盖显示为 1990~2026，但图表实际只请求近一年 bars。
    if _is_chart_date_only_period(resolved_period):
        return timedelta(days=365 * 50)
    return timedelta(days=30)


def _is_chart_date_only_period(resolved_period: Any) -> bool:
    runtime = _chart_runtime_code(resolved_period)
    if runtime == "tick":
        return False
    return _parse_chart_intraday_minutes(runtime) is None


def _resolve_chart_request_window(
    resolved_period: Any, start_date: str, end_date: str
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
        start_dt = end_dt - _chart_default_range(resolved_period)

    if start_dt > end_dt:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="start_date 不能晚于 end_date",
        )

    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")


def _resolve_chart_available_window(
    resolved_period: Any, available_start: str, available_end: str
) -> tuple[str, str]:
    try:
        start_dt = datetime.strptime(available_start, "%Y-%m-%d")
        end_dt = datetime.strptime(available_end, "%Y-%m-%d")
    except ValueError:
        return available_start, available_end

    fallback_start = max(start_dt, end_dt - _chart_default_range(resolved_period))
    if fallback_start > end_dt:
        fallback_start = start_dt
    return fallback_start.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")


def _format_chart_bar_time(value: Any, resolved_period: Any) -> str:
    import pandas as pd

    ts = pd.Timestamp(value)
    if _is_chart_date_only_period(resolved_period):
        return ts.strftime("%Y-%m-%d")
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _serialize_chart_bars(
    df: Any,
    resolved_period: Any,
    limit: int,
    start_datetime: "datetime | None" = None,
    end_datetime: "datetime | None" = None,
) -> list[dict[str, Any]]:
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

    # 精确 datetime 游标过滤（分钟/小时级翻页，避免日期降精度导致同日数据重复）
    if start_datetime is not None:
        start_ts = pd.Timestamp(start_datetime)
        data = data[data["time"] > start_ts]
    if end_datetime is not None:
        end_ts = pd.Timestamp(end_datetime)
        data = data[data["time"] < end_ts]

    if limit > 0:
        data = data.tail(limit)

    bars: list[dict[str, Any]] = []
    for row in data.itertuples(index=False):
        item = {
            "time": _format_chart_bar_time(getattr(row, "time"), resolved_period),
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
    FastAPI 依赖：限流 + 统一鉴权。

    鉴权优先级（任一通过即放行）：
      1. Authorization: Bearer <session_token>  — 前端登录会话
      2. X-API-Token header                     — 机器/API client
      3. EASYXT_API_TOKEN 为空时跳过（开发模式）
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="请求过于频繁，请稍后再试",
        )
    # --- Bearer session 优先 ---
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        from core.user_auth import get_user_manager
        token = auth_header[7:]
        user = get_user_manager().validate_session(token)
        if user:
            return  # Bearer 会话有效 → 放行
    # --- 本地开发 / pytest 直接放行 ---
    if _DEV_MODE or _TEST_MODE:
        return
    # --- 退回旧 X-API-Token ---
    if _API_TOKEN and (not x_api_token or not secrets.compare_digest(x_api_token, _API_TOKEN)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效或缺失的认证凭据（Bearer session 或 X-API-Token）",
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
        # 每个标的最近一次广播的完整消息（含 seq），供新订阅者立即重放
        self._last_msg: dict[str, str] = {}

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
        """订阅：创建专属队列并启动 drain 协程（需在 event loop 中调用）。

        若该标的已有缓存的最近一次 tick，立即将其放入新连接的队列（last-value replay），
        避免客户端等待下一条新 tick 才能看到数据。
        """
        self._channels.setdefault(symbol, set()).add(ws)
        queue: asyncio.Queue = asyncio.Queue(maxsize=_WS_MAX_QUEUE_SIZE)
        self._queues[ws] = queue
        # last-value replay：立即让新订阅者收到最近一次 tick
        last = self._last_msg.get(symbol)
        if last is not None:
            try:
                queue.put_nowait(last)
            except asyncio.QueueFull:
                pass  # 队列满则跳过（极罕见：刚创建的空队列）
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

    def has_cached(self, symbol: str) -> bool:
        """是否有该标的的最近一次 tick 缓存（用于判断是否需要主动快照）。"""
        return symbol in self._last_msg

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
        # 缓存最近一次消息，供新订阅者订阅时立即重放（last-value cache）
        self._last_msg[symbol] = msg
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


@dataclass
class _TickAggregatedBarState:
    """以 tick 聚合实时 bar 的运行态快照。"""

    key: str
    symbol: str
    period_type: str
    period_span: int
    period_code: str
    manager: Any
    subscribed_at: float = field(default_factory=time.time)
    ingested_count: int = 0
    error_count: int = 0
    last_tick_ts: float | None = None
    pending_final_bars: deque[dict[str, Any]] = field(default_factory=deque)
    source: str = "tick_aggregate"


_tick_aggregated_bar_states: dict[str, _TickAggregatedBarState] = {}
_tick_aggregated_bar_states_lock = threading.Lock()


def _realtime_bar_period_code(period_type: str, period_span: int) -> str:
    period_type_norm = str(period_type or "").strip().lower()
    span = int(period_span)
    if period_type_norm == "second":
        return "tick" if span == 1 else f"{span}s"
    if period_type_norm == "minute":
        return f"{span}m"
    if period_type_norm == "hour":
        return f"{span}h"
    if period_type_norm == "day":
        trading_day_alias = {
            42: "2M",
            63: "3M",
            105: "5M",
        }.get(span)
        return trading_day_alias or f"{span}d"
    if period_type_norm == "week":
        return f"{span}w"
    if period_type_norm == "month":
        return {
            1: "1M",
            3: "1Q",
            6: "6M",
        }.get(span, f"{span}M")
    if period_type_norm == "year":
        return f"{span}Y"
    return f"{span}{period_type_norm[:1] or '?'}"


def _supports_tick_aggregated_bar(period_type: str, period_span: int) -> bool:
    period_type_norm = str(period_type or "").strip().lower()
    span = int(period_span)
    if period_type_norm == "minute" and span > 0:
        return True
    return (period_type_norm, span) in {
        ("day", 1),
        ("week", 1),
        ("month", 1),
        ("month", 3),
        ("month", 6),
        ("year", 1),
        ("year", 2),
        ("year", 3),
        ("year", 5),
        ("year", 10),
    }


def _normalize_realtime_bar_payload(bar_data: dict[str, Any]) -> dict[str, Any] | None:
    try:
        time_value = bar_data.get("time")
        if time_value in (None, ""):
            return None
        return {
            "time": str(time_value),
            "open": float(bar_data["open"]),
            "high": float(bar_data["high"]),
            "low": float(bar_data["low"]),
            "close": float(bar_data["close"]),
            "volume": float(bar_data.get("volume") or 0.0),
        }
    except (KeyError, TypeError, ValueError):
        return None


def _load_tick_aggregate_seed_frame(symbol: str, period_code: str) -> Any | None:
    try:
        import pandas as pd

        resolved_period = _resolve_chart_period_definition(period_code)
        window = timedelta(days=60 if period_code == "1d" else 5)
        end_dt = datetime.now()
        start_at = (end_dt - window).strftime("%Y-%m-%d")
        end_at = end_dt.strftime("%Y-%m-%d")

        def _load_seed_frame(iface: Any) -> Any:
            listing_date = getattr(iface, "get_listing_date", lambda _symbol: None)(symbol)
            local_reader = getattr(iface, "_read_from_duckdb", None)
            if callable(local_reader):
                return local_reader(
                    symbol,
                    start_at,
                    end_at,
                    period_code,
                    "none",
                    listing_date=listing_date,
                )
            return iface.get_stock_data(
                stock_code=symbol,
                start_date=start_at,
                end_date=end_at,
                period=period_code,
                adjust="none",
                auto_save=False,
            )

        df = _call_with_datasource_health_interface(_load_seed_frame)
        bars = _serialize_chart_bars(df, resolved_period, 5)
        if not bars:
            return None
        return pd.DataFrame(bars)
    except Exception as exc:
        log.debug(
            "tick 聚合 seed 加载失败 symbol=%s period=%s error=%s",
            symbol,
            period_code,
            exc,
        )
        return None


def _create_tick_aggregated_bar_state(
    symbol: str, period_type: str, period_span: int
) -> _TickAggregatedBarState:
    from data_manager.realtime_pipeline_manager import RealtimePipelineManager

    period_code = _realtime_bar_period_code(period_type, period_span)
    manager = RealtimePipelineManager()
    manager.configure(
        symbol=symbol,
        period=period_code,
        last_data=_load_tick_aggregate_seed_frame(symbol, period_code),
    )
    state = _TickAggregatedBarState(
        key=f"{symbol}:{period_type}:{period_span}",
        symbol=symbol,
        period_type=period_type,
        period_span=period_span,
        period_code=period_code,
        manager=manager,
    )
    manager.on_bar_close = lambda bar, current_state=state: current_state.pending_final_bars.append(
        dict(bar)
    )
    return state


def _ensure_tick_aggregated_bar_state(
    symbol: str, period_type: str, period_span: int
) -> _TickAggregatedBarState:
    key = f"{symbol}:{period_type}:{period_span}"
    with _tick_aggregated_bar_states_lock:
        existing = _tick_aggregated_bar_states.get(key)
    if existing is not None:
        return existing

    created = _create_tick_aggregated_bar_state(symbol, period_type, period_span)
    with _tick_aggregated_bar_states_lock:
        existing = _tick_aggregated_bar_states.get(key)
        if existing is not None:
            return existing
        _tick_aggregated_bar_states[key] = created
        return created


def _drop_tick_aggregated_bar_state(key: str) -> None:
    with _tick_aggregated_bar_states_lock:
        _tick_aggregated_bar_states.pop(key, None)


def _list_tick_aggregated_bar_subscriptions() -> list[dict[str, Any]]:
    with _tick_aggregated_bar_states_lock:
        items = list(_tick_aggregated_bar_states.values())
    return [
        {
            "symbol": item.symbol,
            "period": item.period_code,
            "subscribed_at": item.subscribed_at,
            "ingested_count": item.ingested_count,
            "error_count": item.error_count,
            "last_tick_ts": item.last_tick_ts,
            "source": item.source,
        }
        for item in items
    ]


async def _fanout_tick_to_aggregated_bars(symbol: str, tick_data: dict[str, Any]) -> None:
    with _tick_aggregated_bar_states_lock:
        states = [item for item in _tick_aggregated_bar_states.values() if item.symbol == symbol]

    if not states:
        return

    for state in states:
        try:
            quote_payload = dict(tick_data)
            if quote_payload.get("event_ts_ms") in (None, ""):
                tick_ts = quote_payload.get("tick_ts_ms")
                if tick_ts not in (None, ""):
                    quote_payload["event_ts_ms"] = tick_ts

            state.manager.enqueue_quote(quote_payload)
            result = state.manager.flush(force=False)
            finalized_bars: list[dict[str, Any]] = []
            while state.pending_final_bars:
                finalized_bars.append(state.pending_final_bars.popleft())

            bcast = await _get_bar_broadcaster(state.key)
            emitted = 0
            for finalized_bar in finalized_bars:
                normalized_final = _normalize_realtime_bar_payload(finalized_bar)
                if normalized_final is None:
                    continue
                await bcast.broadcast(
                    symbol,
                    {
                        "type": "bar",
                        "symbol": symbol,
                        "period": state.period_code,
                        "bar": normalized_final,
                        "is_final": True,
                        "source": state.source,
                        "event_ts_ms": quote_payload.get("event_ts_ms"),
                    },
                )
                emitted += 1

            normalized_bar = None
            if isinstance(result, dict):
                normalized_bar = _normalize_realtime_bar_payload(
                    cast(dict[str, Any], result.get("bar") or {})
                )
            if normalized_bar is not None:
                await bcast.broadcast(
                    symbol,
                    {
                        "type": "bar",
                        "symbol": symbol,
                        "period": state.period_code,
                        "bar": normalized_bar,
                        "is_final": False,
                        "source": state.source,
                        "event_ts_ms": quote_payload.get("event_ts_ms"),
                    },
                )
                emitted += 1

            with _tick_aggregated_bar_states_lock:
                state.last_tick_ts = time.time()
                state.ingested_count += emitted
        except Exception as exc:
            with _tick_aggregated_bar_states_lock:
                state.error_count += 1
                state.last_tick_ts = time.time()
            log.debug(
                "tick 聚合 bar 推送失败 symbol=%s period=%s error=%s",
                symbol,
                state.period_code,
                exc,
            )

# ---------------------------------------------------------------------------
# 线程→事件循环桥接（QMT 回调注入实时行情）
# ---------------------------------------------------------------------------

_server_loop: asyncio.AbstractEventLoop | None = None
_server_loop_thread_id: int | None = None
_server_start_time: float | None = None  # monotonic 启动时刻，用于计算 uptime_s
_xt_worker_manager: XtWorkerProcessManager | None = None


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

    推荐接入方式：

        1. QMT 实时行情请优先走 ``core.qmt_feed.qmt_feed.subscribe()``
           由 qmt_feed 负责订阅、字段归一化与回调线程安全注入。
        2. 若接入其他自定义行情源，可在其回调线程中直接调用
           ``ingest_tick_from_thread(symbol, tick_data)``。
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

    async def _broadcast_tick() -> None:
        await broadcaster.broadcast(symbol, tick_data)
        await _fanout_tick_to_aggregated_bars(symbol, tick_data)

    asyncio.run_coroutine_threadsafe(_broadcast_tick(), _server_loop)


def ingest_bar_from_thread(
    symbol: str,
    period_type: str,
    period_span: int,
    bar_data: dict,
    is_final: bool = False,
) -> None:
    """
    从非异步线程（如 QMT xtdata bar 回调）注入实时 bar，线程安全。

    使用 run_coroutine_threadsafe 将广播协程提交到服务事件循环，
    不阻塞回调线程。若服务未启动则静默丢弃。

    ``bar_data`` 应符合 /ws/bar 约定的 bar 字段::

        {
            "time": "YYYY-MM-DD HH:MM:SS",
            "open": float, "high": float, "low": float, "close": float,
            "volume": float,
        }

    推荐接入方式：

        1. QMT bar 链路请优先走 ``core.qmt_feed.qmt_feed.subscribe_bar()``
           或 tick -> bar 聚合回退链路。
        2. 若接入其他自定义 bar 源，可在其回调线程中直接调用
           ``ingest_bar_from_thread(...)``。
    """
    if _server_loop is None or _server_loop.is_closed():
        return
    period_key = f"{symbol}:{period_type}:{period_span}"
    period_suffix = {
        "minute": "m",
        "hour": "h",
        "day": "d",
        "week": "w",
        "month": "n",
        "year": "y",
        "second": "s",
    }.get(period_type, period_type[0] if period_type else "?")

    async def _broadcast_bar() -> None:
        bcast = await _get_bar_broadcaster(period_key)
        payload = {
            "type": "bar",
            "symbol": symbol,
            "period": f"{period_span}{period_suffix}",
            "bar": bar_data,
            "is_final": is_final,
        }
        await bcast.broadcast(symbol, payload)

    asyncio.run_coroutine_threadsafe(_broadcast_bar(), _server_loop)


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


class PlaceOrderBody(BaseModel):
    account_id: str
    code: str
    direction: str  # "buy" | "sell"
    volume: int
    price: float = 0
    price_type: str = "market"
    signal_id: str = ""  # "market" | "limit"
    tp_price: float | None = None
    sl_price: float | None = None


class ReplaceOrderBody(BaseModel):
    account_id: str
    price: float
    price_type: str = "limit"
    volume: int | None = None
    signal_id: str = ""


# ---------------------------------------------------------------------------
# App 生命周期
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _server_loop, _server_loop_thread_id, _server_start_time, _xt_worker_manager, _trade_runtime_thread, _trade_autorestore_task
    _server_loop = asyncio.get_event_loop()
    _server_loop_thread_id = threading.get_ident()
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
    # --- 初始化用户认证 + 券商账户管理 ---
    from core.user_auth import get_user_manager
    from core.broker_accounts import get_broker_manager
    user_mgr = get_user_manager()
    user_mgr.ensure_root("admin", "admin")
    broker_mgr = get_broker_manager()
    _seed_default_broker_accounts(user_mgr, broker_mgr)
    # --- 注入第三方数据源已保存凭证（Tushare / TQSdk）---
    from core.datasource_credentials import inject_all_into_env as _inject_ds_creds
    _inject_ds_creds()
    _xt_worker_manager = ensure_xt_worker_process_ready(test_mode=_TEST_MODE)
    xt_worker_policy = xt_worker_process_policy_snapshot(test_mode=_TEST_MODE)
    log.info(
        "EasyXT 中台服务启动 (auth=%s, dev_mode=%s, rate_limit=%d req/min, ws_timeout=%.2fs,"
        " users=%d, broker_accounts=%d, xt_disabled=%s, xt_side_effects=%s, xt_transport=%s,"
        " xt_worker_required=%s, xt_worker_autostart=%s, trade_autorestore=%s)",
        "enabled" if _API_TOKEN else "disabled(DEV)",
        _DEV_MODE,
        _RATE_LIMIT,
        _WS_SEND_TIMEOUT,
        len(user_mgr.list_users()),
        len(broker_mgr.list_accounts()),
        xt_runtime_policy_snapshot(test_mode=_TEST_MODE)["xt_runtime_disabled"],
        "enabled" if xt_side_effects_allowed(test_mode=_TEST_MODE) else "disabled",
        xt_worker_policy["transport"],
        xt_worker_policy["required"],
        xt_worker_policy["autostart"],
        _trade_autorestore_enabled(),
    )
    if _trade_autorestore_enabled():
        _trade_autorestore_task = asyncio.create_task(_auto_restore_trade_accounts_after_startup())
    yield
    _cleanup_task.cancel()
    if _trade_autorestore_task is not None:
        _trade_autorestore_task.cancel()
        _trade_autorestore_task = None
    _shutdown_data_ingestion_jobs(timeout_s=0.5)
    _shutdown_coverage_matrix_jobs(timeout_s=0.5)
    if _xt_worker_manager is not None:
        _xt_worker_manager.stop(timeout=5.0)
        _xt_worker_manager = None
    if _trade_runtime_thread is not None:
        _trade_runtime_thread.stop(timeout=2.0)
        _trade_runtime_thread = None
    _server_loop = None
    _server_loop_thread_id = None
    log.info("EasyXT 中台服务关闭")


def _seed_default_broker_accounts(user_mgr: Any, broker_mgr: Any) -> None:
    """Seed the default QMT accounts for admin if not yet present.

    NOTE: trade_account must be the FUND account number (资金账号) returned
    by xtquant XtAccountInfo.account_id, NOT the login username.
    申万宏源 real fund accounts discovered 2026-04 via query_account_infos:
      STOCK  → 1678070127  (broker_id 8021)
      CREDIT → 3900016908  (broker_id 6003)
      FUTURE → 88001136    (broker_id 66666)
    """
    existing = broker_mgr.list_accounts(owner_user_id="admin")
    existing_labels = {a["label"] for a in existing}

    _SWY_USERDATA = r"D:\申万宏源策略量化交易终端\userdata_mini"
    _SWY_EXE = r"D:\申万宏源策略量化交易终端\bin.x64\XtItClient.exe"

    if "申万宏源-普通股票" not in existing_labels and "申万宏源实盘" not in existing_labels:
        broker_mgr.add_account(
            owner_user_id="admin",
            label="申万宏源-普通股票",
            broker="申万宏源",
            qmt_exe_path=_SWY_EXE,
            qmt_userdata_path=_SWY_USERDATA,
            trade_account="1678070127",
            trade_password=os.environ.get("EASYXT_QMT_PW_1", ""),
            account_types=["STOCK"],
            is_default=True,
            notes="申万宏源普通股票（资金账号 1678070127，登录名 test1101）",
        )
    if "申万宏源-信用账户" not in existing_labels:
        broker_mgr.add_account(
            owner_user_id="admin",
            label="申万宏源-信用账户",
            broker="申万宏源",
            qmt_exe_path=_SWY_EXE,
            qmt_userdata_path=_SWY_USERDATA,
            trade_account="3900016908",
            trade_password=os.environ.get("EASYXT_QMT_PW_1", ""),
            account_types=["CREDIT"],
            is_default=False,
            notes="申万宏源信用（融资融券）账户（资金账号 3900016908，登录名 test1101）",
        )
    if "申万宏源-期货" not in existing_labels:
        broker_mgr.add_account(
            owner_user_id="admin",
            label="申万宏源-期货",
            broker="申万宏源",
            qmt_exe_path=_SWY_EXE,
            qmt_userdata_path=_SWY_USERDATA,
            trade_account="88001136",
            trade_password=os.environ.get("EASYXT_QMT_PW_1", ""),
            account_types=["FUTURE"],
            is_default=False,
            notes="申万宏源期货账户（资金账号 88001136，登录名 test1101）",
        )
    _GJ_USERDATA = r"D:\国金QMT交易端\userdata_mini"
    _GJ_EXE = r"D:\国金QMT交易端\bin.x64\XtMiniQmt.exe"

    if "国金证券QMT" not in existing_labels:
        broker_mgr.add_account(
            owner_user_id="admin",
            label="国金证券QMT",
            broker="国金证券",
            qmt_exe_path=_GJ_EXE,
            qmt_userdata_path=_GJ_USERDATA,
            trade_account="39946611",
            display_account="8884857727",
            trade_password=os.environ.get("EASYXT_QMT_PW_2", ""),
            account_types=["STOCK"],
            is_default=False,
            is_active=True,
            notes="国金证券QMT 股票账户（交易资金账号 39946611，GUI 展示号 8884857727）",
        )
    else:
        # 该账户已确认仍在使用，启动时纠正到当前实际在线的 D: 安装实例并保持启用态。
        acct_objs = broker_mgr.list_account_objects(owner_user_id="admin")
        for _a in acct_objs:
            if _a.label != "国金证券QMT" or str(_a.trade_account or "").strip() != "39946611":
                continue
            update_kwargs: dict[str, Any] = {}
            if not _a.is_active:
                update_kwargs["is_active"] = True
            if str(_a.qmt_userdata_path or "").strip() != _GJ_USERDATA:
                update_kwargs["qmt_userdata_path"] = _GJ_USERDATA
            if str(_a.qmt_exe_path or "").strip() != _GJ_EXE:
                update_kwargs["qmt_exe_path"] = _GJ_EXE
            if str(getattr(_a, "display_account", "") or "").strip() != "8884857727":
                update_kwargs["display_account"] = "8884857727"
            if update_kwargs:
                broker_mgr.update_account(_a.id, **update_kwargs)
                log.info("已校正国金证券QMT账户配置 trade_account=%s updates=%s", _a.trade_account, sorted(update_kwargs))


app = FastAPI(
    title="EasyXT 中台 API",
    version="1.0.0",
    description="统一行情、交易与策略管理接口层",
    lifespan=_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ALLOW_ORIGINS,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


@app.post(
    "/api/v1/data-ingestion/jobs",
    tags=["数据入库"],
    dependencies=[Depends(_verify_auth_and_rate)],
    status_code=status.HTTP_202_ACCEPTED,
)
def create_data_ingestion_job(body: DataIngestionJobCreateBody) -> dict[str, Any]:
    request_payload = _build_data_ingestion_job_request(body)
    total_steps = _calculate_data_ingestion_total_steps(request_payload)
    job_id = uuid.uuid4().hex
    record: dict[str, Any] = {
        "job_id": job_id,
        "status": "queued",
        "created_at": _now_iso(),
        "started_at": None,
        "finished_at": None,
        "request": request_payload,
        "progress": {
            "current": 0,
            "total": total_steps,
            "processed_stocks": 0,
            "current_stock_code": None,
            "current_period": None,
            "last_status": None,
            "message": "任务已创建，等待执行",
            "updated_at": _now_iso(),
        },
        "summary": {
            "processed_stocks": 0,
            "success_stocks": 0,
            "failed_stocks": 0,
            "total_records": 0,
        },
        "error": None,
        "_stop_event": threading.Event(),
        "_thread": None,
    }
    thread = threading.Thread(
        target=_run_data_ingestion_job,
        args=(job_id,),
        daemon=True,
        name=f"data-ingestion-{job_id[:8]}",
    )
    record["_thread"] = thread
    with _data_ingestion_jobs_lock:
        _data_ingestion_jobs[job_id] = record
    thread.start()
    return _finalize_data_ingestion_payload(_serialize_data_ingestion_job(record))


@app.get(
    "/api/v1/data-ingestion/jobs",
    tags=["数据入库"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_data_ingestion_jobs(
    status_filter: str = Query(
        default="",
        pattern="^(|queued|running|cancelling|cancelled|completed|failed)$",
        description="状态过滤",
    ),
    limit: int = Query(default=20, ge=1, le=200, description="返回条数"),
) -> dict[str, Any]:
    with _data_ingestion_jobs_lock:
        items = [_serialize_data_ingestion_job(record) for record in _data_ingestion_jobs.values()]
    if status_filter:
        items = [item for item in items if item.get("status") == status_filter]
    items.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    items = items[:limit]
    return _finalize_data_ingestion_payload(
        {
            "items": items,
            "returned": len(items),
            "filters": {"status_filter": status_filter, "limit": limit},
        }
    )


@app.get(
    "/api/v1/data-ingestion/jobs/{job_id}",
    tags=["数据入库"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_data_ingestion_job(job_id: str) -> dict[str, Any]:
    with _data_ingestion_jobs_lock:
        record = _data_ingestion_jobs.get(job_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到 data ingestion job: {job_id}",
            )
        payload = _serialize_data_ingestion_job(record)
    return _finalize_data_ingestion_payload(payload)


@app.delete(
    "/api/v1/data-ingestion/jobs/{job_id}",
    tags=["数据入库"],
    dependencies=[Depends(_verify_auth_and_rate)],
    status_code=status.HTTP_202_ACCEPTED,
)
def cancel_data_ingestion_job(job_id: str) -> dict[str, Any]:
    with _data_ingestion_jobs_lock:
        record = _data_ingestion_jobs.get(job_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到 data ingestion job: {job_id}",
            )
        current_status = str(record.get("status", "unknown"))
        if current_status not in _DATA_INGESTION_JOB_STATUSES:
            current_status = "unknown"
        if current_status in {"completed", "failed", "cancelled"}:
            message = f"任务已结束，当前状态为 {current_status}"
        else:
            stop_event = record.get("_stop_event")
            if isinstance(stop_event, threading.Event):
                stop_event.set()
            record["status"] = "cancelling"
            record["progress"].update(
                {
                    "message": "已发送取消请求，等待后台线程收敛",
                    "updated_at": _now_iso(),
                }
            )
            message = "取消请求已发送"
        payload = _serialize_data_ingestion_job(record)
    payload["message"] = message
    return _finalize_data_ingestion_payload(payload)


@app.post(
    "/api/v1/data-ingestion/jobs/{job_id}/force-abort",
    tags=["数据入库"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def force_abort_data_ingestion_job(job_id: str) -> dict[str, Any]:
    """强制终止一个卡在 running/cancelling 状态的入库任务。

    语义：发送更强的终止请求并禁止任务被标记为 completed；
    若后台线程仍在阻塞步骤中，状态保持 cancelling，待线程收敛后落为 cancelled。
    """
    with _data_ingestion_jobs_lock:
        record = _data_ingestion_jobs.get(job_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到 data ingestion job: {job_id}",
            )
        current_status = str(record.get("status", "unknown"))
        if current_status in {"completed", "failed", "cancelled"}:
            payload = _serialize_data_ingestion_job(record)
            payload["message"] = f"任务已经结束 ({current_status})，无需强制终止"
            return _finalize_data_ingestion_payload(payload)

        # 确保 stop_event 被设置
        stop_event = record.get("_stop_event")
        if isinstance(stop_event, threading.Event):
            stop_event.set()
        record["_force_abort_requested"] = True
        thread = record.get("_thread")
        thread_alive = isinstance(thread, threading.Thread) and thread.is_alive()

        if thread_alive:
            record["status"] = "cancelling"
            record["progress"].update(
                {
                    "message": "已发送强制终止请求，等待当前步骤结束后收敛",
                    "updated_at": _now_iso(),
                }
            )
            message = "强制终止请求已发送"
        else:
            record["status"] = "cancelled"
            record["finished_at"] = _now_iso()
            record["error"] = "被用户强制终止"
            record["progress"].update(
                {
                    "message": "任务已被强制终止",
                    "updated_at": _now_iso(),
                }
            )
            message = "任务已被强制终止"
        payload = _serialize_data_ingestion_job(record)
    payload["message"] = message
    return _finalize_data_ingestion_payload(payload)


@app.post(
    "/api/v1/data-ingestion/jobs/{job_id}/retry",
    tags=["数据入库"],
    dependencies=[Depends(_verify_auth_and_rate)],
    status_code=status.HTTP_201_CREATED,
)
def retry_data_ingestion_job(job_id: str) -> dict[str, Any]:
    """基于一个已结束/已失败/已取消的任务，克隆其请求参数创建新任务。"""
    with _data_ingestion_jobs_lock:
        record = _data_ingestion_jobs.get(job_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到 data ingestion job: {job_id}",
            )
        current_status = str(record.get("status", "unknown"))
        if current_status in {"running", "queued", "cancelling"}:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"任务仍在运行中 ({current_status})，请先取消或等待完成",
            )
        original_request = dict(record.get("request", {}))

    # 创建新任务，复用原始请求参数
    new_job_id = uuid.uuid4().hex
    stop_event = threading.Event()
    total_steps = _calculate_data_ingestion_total_steps(original_request)
    new_record: dict[str, Any] = {
        "job_id": new_job_id,
        "status": "queued",
        "created_at": _now_iso(),
        "started_at": None,
        "finished_at": None,
        "request": original_request,
        "progress": {
            "current": 0,
            "total": total_steps,
            "processed_stocks": 0,
            "current_stock_code": None,
            "current_period": None,
            "last_status": None,
            "message": f"重试任务已创建（基于 {job_id[:8]}）",
            "updated_at": _now_iso(),
        },
        "summary": {
            "processed_stocks": 0,
            "success_stocks": 0,
            "failed_stocks": 0,
            "total_records": 0,
        },
        "error": None,
        "_stop_event": stop_event,
        "_thread": None,
    }
    with _data_ingestion_jobs_lock:
        _data_ingestion_jobs[new_job_id] = new_record
    t = threading.Thread(
        target=_run_data_ingestion_job,
        args=(new_job_id,),
        daemon=True,
        name=f"data-ingestion-retry-{new_job_id[:8]}",
    )
    with _data_ingestion_jobs_lock:
        new_record["_thread"] = t
    t.start()
    payload = _serialize_data_ingestion_job(new_record)
    payload["message"] = f"重试任务 {new_job_id[:8]} 已创建并启动"
    return _finalize_data_ingestion_payload(payload)


@app.get(
    "/api/v1/data-quality/coverage-matrix",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_coverage_matrix_overview() -> dict[str, Any]:
    config_summary: dict[str, Any] | None = None
    config_error: str | None = None
    try:
        config_summary = _load_coverage_matrix_config_summary()
    except Exception as exc:
        config_error = str(exc)

    latest_report = _load_latest_coverage_matrix_report_summary()
    with _coverage_matrix_jobs_lock:
        recent_jobs = [_serialize_coverage_matrix_job(record) for record in _coverage_matrix_jobs.values()]
    recent_jobs.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return _finalize_coverage_matrix_payload(
        {
            "config": config_summary,
            "config_error": config_error,
            "latest_report": latest_report,
            "recent_jobs": recent_jobs[:8],
            "returned_jobs": min(len(recent_jobs), 8),
        }
    )


@app.post(
    "/api/v1/data-quality/coverage-matrix/jobs",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
    status_code=status.HTTP_202_ACCEPTED,
)
def create_coverage_matrix_job(body: CoverageMatrixJobCreateBody) -> dict[str, Any]:
    request_payload = _build_coverage_matrix_job_request(body)
    job_id = uuid.uuid4().hex
    default_output_dir = _COVERAGE_MATRIX_OUTPUT_ROOT / f"{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{job_id[:8]}"
    output_dir = _resolve_root_relative_path(request_payload.get("output_dir"), default_output_dir)
    request_payload["output_dir"] = str(output_dir)

    record: dict[str, Any] = {
        "job_id": job_id,
        "status": "queued",
        "created_at": _now_iso(),
        "started_at": None,
        "finished_at": None,
        "request": request_payload,
        "progress": {
            "completed_tasks": 0,
            "total_tasks": int(request_payload.get("task_count", 0) or 0),
            "current_task_id": None,
            "current_symbol": None,
            "current_period": None,
            "current_window_id": None,
            "last_status": None,
            "message": "覆盖矩阵任务已创建，等待执行",
            "updated_at": _now_iso(),
        },
        "summary": {
            "completed_tasks": 0,
            "configured_tasks": int(request_payload.get("task_count", 0) or 0),
            "pass_tasks": 0,
            "partial_tasks": 0,
            "fail_tasks": 0,
            "error_tasks": 0,
            "unknown_tasks": 0,
            "pass_ratio": 0.0,
            "remaining_tasks": int(request_payload.get("task_count", 0) or 0),
        },
        "artifacts": {},
        "latest_report": None,
        "error": None,
        "_stop_event": threading.Event(),
        "_thread": None,
    }
    thread = threading.Thread(
        target=_run_coverage_matrix_job,
        args=(job_id,),
        daemon=True,
        name=f"coverage-matrix-{job_id[:8]}",
    )
    record["_thread"] = thread
    with _coverage_matrix_jobs_lock:
        _coverage_matrix_jobs[job_id] = record

    _append_governance_action_audit(
        action_id="coverage_matrix_job_created",
        action_type="trigger_coverage_matrix",
        tone="warning",
        title="触发覆盖矩阵验证",
        detail=f"job={job_id[:8]} config={request_payload.get('config_name', 'unnamed')}",
        source="api_server",
        payload={
            "job_id": job_id,
            "config_path": request_payload.get("config_path", ""),
            "output_dir": request_payload.get("output_dir", ""),
            "workers": request_payload.get("workers", 1),
            "resume": request_payload.get("resume", True),
            "task_count": request_payload.get("task_count", 0),
        },
    )
    thread.start()
    return _finalize_coverage_matrix_payload(_serialize_coverage_matrix_job(record))


@app.get(
    "/api/v1/data-quality/coverage-matrix/jobs",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_coverage_matrix_jobs(
    status_filter: str = Query(
        default="",
        pattern="^(|queued|running|cancelling|cancelled|completed|failed)$",
        description="状态过滤",
    ),
    limit: int = Query(default=20, ge=1, le=200, description="返回条数"),
) -> dict[str, Any]:
    with _coverage_matrix_jobs_lock:
        items = [_serialize_coverage_matrix_job(record) for record in _coverage_matrix_jobs.values()]
    if status_filter:
        items = [item for item in items if item.get("status") == status_filter]
    items.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    items = items[:limit]
    return _finalize_coverage_matrix_payload(
        {
            "items": items,
            "returned": len(items),
            "filters": {"status_filter": status_filter, "limit": limit},
        }
    )


@app.get(
    "/api/v1/data-quality/coverage-matrix/jobs/{job_id}",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_coverage_matrix_job(job_id: str) -> dict[str, Any]:
    with _coverage_matrix_jobs_lock:
        record = _coverage_matrix_jobs.get(job_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到 coverage matrix job: {job_id}",
            )
        payload = _serialize_coverage_matrix_job(record)
    return _finalize_coverage_matrix_payload(payload)


@app.delete(
    "/api/v1/data-quality/coverage-matrix/jobs/{job_id}",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
    status_code=status.HTTP_202_ACCEPTED,
)
def cancel_coverage_matrix_job(job_id: str) -> dict[str, Any]:
    with _coverage_matrix_jobs_lock:
        record = _coverage_matrix_jobs.get(job_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到 coverage matrix job: {job_id}",
            )
        current_status = str(record.get("status", "unknown"))
        if current_status not in _COVERAGE_MATRIX_JOB_STATUSES:
            current_status = "unknown"
        if current_status in {"completed", "failed", "cancelled"}:
            message = f"任务已结束，当前状态为 {current_status}"
        else:
            stop_event = record.get("_stop_event")
            if isinstance(stop_event, threading.Event):
                stop_event.set()
            record["status"] = "cancelling"
            record["progress"].update(
                {
                    "message": "已发送覆盖矩阵取消请求，等待运行中的任务收敛",
                    "updated_at": _now_iso(),
                }
            )
            message = "取消请求已发送"
        payload = _serialize_coverage_matrix_job(record)
    payload["message"] = message
    return _finalize_coverage_matrix_payload(payload)


@app.get(
    "/api/v1/data-quality/coverage-matrix/jobs/{job_id}/artifacts/{artifact_key}",
    tags=["数据质量"],
    response_model=None,
    dependencies=[Depends(_verify_auth_and_rate)],
)
def download_coverage_matrix_artifact(job_id: str, artifact_key: str) -> Response:
    artifact_meta = _COVERAGE_MATRIX_ARTIFACT_META.get(artifact_key)
    if artifact_meta is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未知 artifact: {artifact_key}",
        )
    media_type, suffix = artifact_meta
    with _coverage_matrix_jobs_lock:
        record = _coverage_matrix_jobs.get(job_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到 coverage matrix job: {job_id}",
            )
        artifact_path = str(cast(dict[str, Any], record.get("artifacts") or {}).get(artifact_key, "") or "").strip()
    if not artifact_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"job {job_id} 尚未生成 {artifact_key} artifact",
        )
    path = Path(artifact_path)
    if not path.exists() or not path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"artifact 文件不存在: {artifact_path}",
        )
    return Response(
        content=path.read_bytes(),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{path.name if path.suffix else path.name + suffix}"'},
    )


@app.get(
    "/api/v1/chart/periods",
    tags=["图表"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_chart_periods(
    ui_visible_only: bool = Query(default=False, description="仅返回默认在 UI 可见的周期"),
) -> dict[str, Any]:
    registry = _get_chart_period_registry()
    definitions = registry.list_definitions(enabled_only=True)
    items: list[dict[str, Any]] = []
    for definition in definitions:
        runtime = str(definition.runtime_code or definition.period_code).strip()
        if not runtime:
            continue
        if ui_visible_only and runtime != "tick" and not definition.ui_visible_default:
            continue
        items.append(
            {
                "code": runtime,
                "label": runtime,
                "period_code": definition.period_code,
                "runtime_code": runtime,
                "aliases": list(definition.aliases),
                "layer": definition.layer,
                "period_family": definition.period_family,
                "base_source": definition.base_source,
                "alignment": definition.alignment,
                "anchor": definition.anchor,
                "precompute_default": definition.precompute_default,
                "ui_visible_default": definition.ui_visible_default,
                "supports_partial": definition.supports_partial,
                "tick_verifiable": definition.tick_verifiable,
                "enabled": definition.enabled,
                "description": definition.description,
            }
        )

    items.sort(
        key=lambda item: _chart_interval_sort_key(
            str(item.get("runtime_code") or item.get("code") or "")
        )
    )
    quick_intervals = [
        item["code"]
        for item in items
        if item["code"] == "tick" or bool(item.get("ui_visible_default"))
    ]
    return {
        "registry_version": registry.registry_version,
        "default_interval": "1d",
        "quick_intervals": quick_intervals,
        "items": items,
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


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

    # --- db 子检查（轻量探针；超时不阻塞 /health） ---
    db_status = _probe_db_health_status()

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
        def _collect_datasource_snapshot(iface: Any) -> dict[str, Any]:
            return {
                "summary": iface.data_registry.get_health_summary(),
                "circuit_breaker": dict(getattr(iface, "_cb_state", {}) or {}),
                "quarantine": iface.get_quarantine_status_counts(),
                "data_quality_incident": iface.get_data_quality_incident_counts(),
                "step6_validation": iface.get_step6_validation_metrics(),
                "publish_gate": iface.get_publish_gate_summary(),
            }

        snapshot = _call_with_datasource_health_interface(_collect_datasource_snapshot)
        summary = snapshot["summary"]
        payload["checks"]["sources"] = summary
        payload["checks"]["circuit_breaker"] = snapshot["circuit_breaker"]
        q_counts = snapshot["quarantine"]
        payload["checks"]["quarantine"] = q_counts
        total = int(q_counts.get("total", 0) or 0)
        dead = int(q_counts.get("dead_letter", 0) or 0)
        dead_ratio = (dead / total) if total > 0 else 0.0
        payload["checks"]["quarantine"]["dead_letter_ratio"] = dead_ratio
        payload["checks"]["data_quality_incident"] = snapshot["data_quality_incident"]
        payload["checks"]["step6_validation"] = snapshot["step6_validation"]
        payload["checks"]["publish_gate"] = snapshot["publish_gate"]
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
        payload = _call_with_datasource_health_interface(
            lambda iface: iface.get_latest_gate_status(symbol, period)
        )
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
        items = _call_with_datasource_health_interface(
            lambda iface: iface.get_receipt_history(
                receipt_type,
                symbol=symbol,
                period=period,
                limit=limit,
            )
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
        items = _call_with_datasource_health_interface(
            lambda iface: iface.get_receipt_timeline(
                symbol=symbol,
                period=period,
                lineage_anchor=lineage_anchor,
                receipt_type=receipt_type,
                gate_reject_reason=gate_reject_reason,
                severity=severity,
                lookback_days=lookback_days,
                limit=limit,
            )
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
        payload = _call_with_datasource_health_interface(
            lambda iface: iface.get_lineage_anchor_detail(lineage_anchor)
        )
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
    threshold_bundle = _load_governance_threshold_bundle()
    overrides = threshold_bundle["overrides"]
    panel = _call_with_datasource_health_interface(
        lambda iface: iface.get_sla_alert_threshold_panel_with_overrides(overrides)
    )
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
    threshold_bundle = _save_governance_threshold_bundle(
        overrides=body.overrides,
        operator=body.operator,
        note=body.note,
    )
    overrides = threshold_bundle["overrides"]
    panel = _call_with_datasource_health_interface(
        lambda iface: iface.get_sla_alert_threshold_panel_with_overrides(overrides)
    )
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


@app.get(
    "/api/v1/db/tables",
    tags=["数据库"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_duckdb_tables(
    include_columns: bool = Query(default=True, description="是否返回字段清单"),
    include_empty: bool = Query(default=True, description="是否返回空表"),
) -> dict[str, Any]:
    try:
        payload = _read_duckdb_table_catalog(
            include_columns=include_columns,
            include_empty=include_empty,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"DuckDB 表目录读取失败: {exc}",
        ) from exc
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.get(
    "/api/v1/db/tables/{table_name}/rows",
    tags=["数据库"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def preview_duckdb_table_rows(
    table_name: str,
    limit: int = Query(default=20, ge=1, le=500, description="预览行数"),
    offset: int = Query(default=0, ge=0, le=100_000, description="偏移量"),
) -> dict[str, Any]:
    try:
        payload = _read_duckdb_table_rows(table_name, limit=limit, offset=offset)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"DuckDB 表预览失败: {exc}",
        ) from exc
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.get(
    "/api/v1/db/tables/{table_name}/export",
    tags=["数据库"],
    response_model=None,
    dependencies=[Depends(_verify_auth_and_rate)],
)
def export_duckdb_table(
    table_name: str,
    export_format: str = Query(default="csv", pattern="^(csv|jsonl)$", description="导出格式"),
    limit: int = Query(default=5000, ge=0, le=200_000, description="导出最大行数，0 表示全表"),
) -> Response:
    try:
        return _export_duckdb_table_response(
            table_name,
            export_format=export_format,
            limit=limit,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"DuckDB 表导出失败: {exc}",
        ) from exc


@app.post(
    "/api/v1/db/query",
    tags=["数据库"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def execute_duckdb_query(body: DbQueryBody) -> dict[str, Any]:
    """执行只读 SELECT 查询并返回结果（最多 1 000 行）。"""
    try:
        result = _execute_readonly_sql_query(body)
        result["server_time"] = int(time.time() * 1000)
        result["build_version"] = _BUILD_VERSION
        result["commit_sha"] = _COMMIT_SHA
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"SQL 查询失败: {exc}",
        ) from exc


@app.post(
    "/api/v1/db/import-csv",
    tags=["数据库"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def import_csv_to_duckdb(body: DbCsvImportBody) -> dict[str, Any]:
    """将 CSV 文本追加（或替换）到指定 DuckDB 表。"""
    try:
        result = _import_csv_to_duckdb_table(body)
        result["server_time"] = int(time.time() * 1000)
        result["build_version"] = _BUILD_VERSION
        result["commit_sha"] = _COMMIT_SHA
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"CSV 导入失败: {exc}",
        ) from exc


@app.post(
    "/api/v1/data-quality/integrity-check",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def run_data_quality_integrity_check(body: IntegrityCheckBody) -> dict[str, Any]:
    """对指定标的和日期区间执行数据完整性检查（缺失交易日、价格异常、成交量异常）。"""
    import time as _t

    t0 = _t.perf_counter()
    try:
        from data_manager.data_integrity_checker import DataIntegrityChecker

        checker = DataIntegrityChecker(verbose=False)
        checker.connect()
        result = checker.check_integrity(
            stock_code=body.stock_code,
            start_date=body.start_date,
            end_date=body.end_date,
            detailed=body.detailed,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"完整性检查失败: {exc}",
        ) from exc
    # check_range 是 tuple，转为 list 以便 JSON 序列化
    if isinstance(result.get("check_range"), tuple):
        result["check_range"] = list(result["check_range"])
    result["elapsed_ms"] = round((_t.perf_counter() - t0) * 1000, 1)
    result["server_time"] = int(time.time() * 1000)
    result["build_version"] = _BUILD_VERSION
    result["commit_sha"] = _COMMIT_SHA
    return result


@app.post(
    "/api/v1/data-quality/reconciliation",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def run_data_quality_reconciliation(body: ReconciliationBody) -> dict[str, Any]:
    """跨源数据对账：比对 DuckDB 本地数据与 AKShare 独立外部源收盘价一致性。"""
    ctrl = _get_data_governance_controller()
    try:
        result = ctrl.cross_validate_sources(
            stock_code=body.stock_code,
            start_date=body.start_date,
            end_date=body.end_date,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"数据对账失败: {exc}",
        ) from exc
    result["server_time"] = int(time.time() * 1000)
    result["build_version"] = _BUILD_VERSION
    result["commit_sha"] = _COMMIT_SHA
    return result


@app.post(
    "/api/v1/data-quality/listing-date-consensus",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_listing_date_consensus(symbol: str) -> dict[str, Any]:
    """通过 BaoStock / AKShare / Tushare 等独立第三方源，并行查询指定股票的上市日期，
    返回多源共识结果与置信度评分。"""
    if not symbol:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="symbol 不能为空")
    try:
        from core.xtdata_lock import xtdata_submit
        from data_manager.multi_source_validator import get_validator

        validator = get_validator(xtdata_submit_fn=xtdata_submit)
        consensus = validator.get_listing_date_consensus(symbol)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"上市日期多源查询失败: {exc}",
        ) from exc
    result = consensus.to_dict()
    result["server_time"] = int(time.time() * 1000)
    return result


@app.post(
    "/api/v1/data-quality/cross-validate-ohlcv",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def cross_validate_ohlcv(
    symbol: str,
    start: str,
    end: str,
    sources: str = "baostock,mootdx",
) -> dict[str, Any]:
    """并行从多个独立外部源（BaoStock / MooTDX）拉取 OHLCV，与本地 DuckDB 数据交叉比对，
    返回每日收盘价最大偏差率和整体一致性评分。"""
    if not symbol:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="symbol 不能为空")
    include_sources = [s.strip() for s in sources.split(",") if s.strip()]
    try:
        from core.xtdata_lock import xtdata_submit
        from data_manager.multi_source_validator import get_validator
        from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path
        import pandas as pd

        validator = get_validator(xtdata_submit_fn=xtdata_submit)
        db_path = resolve_duckdb_path()
        duckdb_df: pd.DataFrame | None = None
        try:
            with get_db_manager(db_path).get_read_connection() as conn:
                duckdb_df = conn.execute(
                    "SELECT * FROM stock_daily WHERE stock_code = ? AND date >= ? AND date <= ?",
                    [symbol, start, end],
                ).df()
        except Exception:
            duckdb_df = None

        matrix = validator.cross_validate_ohlcv(
            symbol, start, end,
            include_sources=include_sources,
            duckdb_df=duckdb_df,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"多源OHLCV交叉比对失败: {exc}",
        ) from exc
    result = matrix.to_dict()
    result["server_time"] = int(time.time() * 1000)
    return result


@app.get(
    "/api/v1/datasource/sources",
    tags=["数据源"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_datasource_health() -> dict[str, Any]:
    """探测所有独立数据源（BaoStock / MooTDX / AKShare / Tushare）的可用性，
    返回每个源的健康状态和延迟（毫秒）。"""
    try:
        from core.xtdata_lock import xtdata_submit
        from data_manager.datasource_discovery import build_datasource_inventory
        from data_manager.multi_source_validator import get_validator

        validator = get_validator(xtdata_submit_fn=xtdata_submit)
        probe = validator.probe_sources()
        inventory = build_datasource_inventory()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"数据源探测失败: {exc}",
        ) from exc
    return {
        "sources": probe,
        "inventory": inventory,
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/interfaces",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_interface_registry() -> dict[str, Any]:
    """返回统一接口注册表：当前 10 个数据源（已配置 / 未配置）+ 规划中极速柜台 / 时序存储端点。\n
    每个接口包含 ``interface_class``（市场数据 / 交易网关 / 存储后端）、
    ``protocol``（local_file / local_sdk / rest_api / ctp / xtp / mmap_shm 等）
    和 ``status_indicator``（configured / unconfigured / planned）。
    """
    try:
        from data_manager.datasource_discovery import (
            build_datasource_inventory_cached,
            build_interface_registry,
        )

        inventory = build_datasource_inventory_cached(force=False)
        registry = build_interface_registry(_inventory=inventory)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"接口注册表构建失败: {exc}",
        ) from exc
    registry["server_time"] = int(time.time() * 1000)
    return registry


@app.get(
    "/api/v1/symbols/search",
    tags=["标的搜索"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def search_symbols(
    q: str = Query(
        "",
        description="搜索关键词，支持代码 / 中文名 / 拼音首字母；留空返回最近标的",
    ),
    limit: int = Query(default=30, ge=1, le=200, description="最多返回条数"),
    scope: str = Query(
        default="all",
        description="范围过滤：all / stock / etf / index / bond / commodity",
    ),
) -> dict[str, Any]:
    """查询标的搜索目录。

    搜索语义：
    1. 代码 / 数字前缀（如 ``000001`` / ``IF`` / ``CU2505``）
    2. 中文名（如 ``平安银行`` / ``沪深300`` / ``白银``）
    3. 拼音 / 首字母（如 ``payh`` / ``hs300`` / ``by``）
    4. 留空时返回最近有数据或常用市场中的标的
    """
    try:
        normalized_scope = str(scope or "all").strip().lower() or "all"
        if normalized_scope not in _SYMBOL_SEARCH_SCOPE_ALIASES:
            normalized_scope = "all"
        q_clean = q.strip()
        items = _search_symbol_catalog(
            query=q_clean,
            limit=limit,
            scope=normalized_scope,
        )
        results = [_serialize_symbol_search_item(item) for item in items]
        return {
            "q": q_clean,
            "limit": limit,
            "scope": normalized_scope,
            "items": results,
            "total": len(results),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"标的搜索失败: {exc}",
        ) from exc


@app.post(
    "/api/v1/db/maintenance",
    tags=["数据库运维"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def run_db_maintenance(body: DbMaintenanceBody) -> dict[str, Any]:
    """对 DuckDB 执行维护操作：checkpoint / force_checkpoint / analyze。\n
    - `checkpoint`：非阻塞 WAL 刷新（如数据库繁忙则跳过）
    - `force_checkpoint`：阻塞式强制 WAL 刷新
    - `analyze`：收集统计信息并返回表行数摘要
    """
    import time as _t

    allowed = {"checkpoint", "force_checkpoint", "analyze"}
    if body.operation not in allowed:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"非法 operation: {body.operation!r}，允许值为 {sorted(allowed)}",
        )
    t0 = _t.perf_counter()
    try:
        from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

        db_mgr = get_db_manager(resolve_duckdb_path())
        if body.operation in ("checkpoint", "force_checkpoint"):
            if body.operation == "force_checkpoint":
                # 阻塞式：临时关闭 nonblocking 标志
                orig = getattr(db_mgr, "_checkpoint_nonblocking", True)
                db_mgr._checkpoint_nonblocking = False  # type: ignore[attr-defined]
                try:
                    ok = db_mgr.checkpoint()
                finally:
                    db_mgr._checkpoint_nonblocking = orig  # type: ignore[attr-defined]
            else:
                ok = db_mgr.checkpoint()
            elapsed_ms = round((_t.perf_counter() - t0) * 1000, 1)
            return {
                "operation": body.operation,
                "success": ok,
                "message": "WAL 刷新完成" if ok else "WAL 刷新跳过（数据库繁忙或锁冲突）",
                "elapsed_ms": elapsed_ms,
                "server_time": int(time.time() * 1000),
                "build_version": _BUILD_VERSION,
                "commit_sha": _COMMIT_SHA,
            }
        else:  # analyze
            with db_mgr.get_read_connection() as con:
                tables = con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
                ).fetchall()
                stats = []
                for (tbl,) in tables:
                    try:
                        row_count = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]  # noqa: S608
                        stats.append({"table": tbl, "rows": row_count})
                    except Exception:
                        stats.append({"table": tbl, "rows": None})
            elapsed_ms = round((_t.perf_counter() - t0) * 1000, 1)
            return {
                "operation": "analyze",
                "success": True,
                "message": f"统计 {len(stats)} 张表",
                "table_stats": stats,
                "elapsed_ms": elapsed_ms,
                "server_time": int(time.time() * 1000),
                "build_version": _BUILD_VERSION,
                "commit_sha": _COMMIT_SHA,
            }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"数据库维护操作失败: {exc}",
        ) from exc


@app.get(
    "/api/v1/datasource/config",
    tags=["数据源"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_datasource_config() -> dict[str, Any]:
    """返回当前进程可见的数据源配置摘要，token 值脱敏（仅显示前2后2位）。\n
    不测试连通性——仅汇报哪些凭证已配置、哪些路径存在。
    """
    from data_manager.datasource_discovery import build_datasource_inventory_cached

    payload = build_datasource_inventory_cached(force=False)
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.post(
    "/api/v1/datasource/rescan",
    tags=["数据源"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def rescan_datasource_config() -> dict[str, Any]:
    """强制重新扫描本地数据源，刷新模块级缓存后返回最新清单。\n
    等同于 GET /api/v1/datasource/config，但始终绕过 TTL 缓存重新全量扫描。
    返回结构同 GET /datasource/config，额外包含 cache_status='miss' 与 last_scan_at。
    """
    from data_manager.datasource_discovery import build_datasource_inventory_cached

    payload = build_datasource_inventory_cached(force=True)
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


@app.get(
    "/api/v1/datasource/source-priority",
    tags=["数据源"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_source_priority() -> dict[str, Any]:
    """返回当前用户优先级覆写表（来自 config/datasource_priority.json）。\n
    空 dict 表示全部使用内置默认优先级。
    """
    from data_manager.datasource_discovery import read_datasource_priority

    return {"priorities": read_datasource_priority()}


class SourcePriorityPatchBody(BaseModel):
    updates: dict[str, int]


@app.patch(
    "/api/v1/datasource/source-priority",
    tags=["数据源"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def patch_source_priority(body: SourcePriorityPatchBody) -> dict[str, Any]:
    """更新数据源优先级覆写，持久化后立即使缓存失效并返回最新排序清单。\n
    body: {"updates": {"qmt_local_dat": 1, "akshare": 5, ...}}
    返回: {"priorities": {...}, "sources": [...最新排序后数据源列表...]}
    """
    from data_manager.datasource_discovery import (
        build_datasource_inventory_cached,
        read_datasource_priority,
        write_datasource_priority,
    )

    existing = read_datasource_priority()
    merged = {**existing, **body.updates}
    write_datasource_priority(merged)
    inventory = build_datasource_inventory_cached(force=True)
    return {
        "priorities": read_datasource_priority(),
        "sources": inventory.get("sources", []),
    }


@app.post(
    "/api/v1/datasource/test",
    tags=["数据源"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def test_datasource(body: DataSourceTestBody) -> dict[str, Any]:
    """对指定数据源执行快速连通性测试并返回延迟。\n
    - qmt_local_dat: 本地 QMT DAT 候选扫描
    - qmt_xtquant / qmt: xtquant 路径快速检查
    - tushare / akshare / duckdb: 原有快速探测
    - baostock / mootdx / pytdx_local_tdx / qstock / tqsdk: 依赖与本地路径就绪探测
    """
    import time as _t

    allowed = {
        "qmt_local_dat",
        "qmt_xtquant",
        "qmt",
        "duckdb",
        "tushare",
        "akshare",
        "baostock",
        "mootdx",
        "pytdx_local_tdx",
        "qstock",
        "tqsdk",
    }
    if body.source not in allowed:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"非法 source: {body.source!r}",
        )
    t0 = _t.perf_counter()
    try:
        from data_manager.datasource_discovery import build_datasource_test_result

        result = build_datasource_test_result(body.source)
        result.setdefault("source", body.source)
        result["latency_ms"] = round((_t.perf_counter() - t0) * 1000, 1)
        result["server_time"] = int(time.time() * 1000)
        return result

    except HTTPException:
        raise
    except Exception as exc:
        latency_ms = round((_t.perf_counter() - t0) * 1000, 1)
        return {
            "source": body.source,
            "status": "error",
            "message": str(exc),
            "latency_ms": latency_ms,
            "server_time": int(time.time() * 1000),
        }


@app.get(
    "/api/v1/qmt/local-scan",
    tags=["数据源"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_qmt_local_scan(
    search_roots: str = Query(
        default="",
        description="逗号分隔的自定义搜索根路径；留空时自动枚举所有已挂载逻辑盘",
    ),
    limit: int = Query(default=12, ge=1, le=50, description="最多返回候选路径数"),
    force: bool = Query(default=False, description="跳过模块级结果缓存，强制重新全盘扫描"),
) -> dict[str, Any]:
    """对本机所有已挂载逻辑盘进行 QMT 安装目录全盘扫描。

    - 先通过已知券商路径快速命中，再对每个逻辑盘根做深度优先文件树遍历（默认最大深度 4）。
    - 每个候选返回 install_path / userdata_path / datadir / market_dirs / period_dirs /
      dat_file_count_hint / score 等字段，按 score 降序排列。
    - `search_roots`: 逗号分隔路径，留空时扫描所有已挂载盘（Windows 下使用 GetLogicalDrives()）。
    - `force=true` 会绕过 datasource inventory 模块级缓存，直接重新扫描。
    """
    from data_manager.datasource_discovery import (
        discover_qmt_local_dat_candidates,
        reset_datasource_inventory_cache,
    )

    explicit_roots: list[str] | None = None
    if search_roots.strip():
        explicit_roots = [item.strip() for item in search_roots.split(",") if item.strip()]

    if force:
        reset_datasource_inventory_cache()

    t0 = time.perf_counter()
    result = discover_qmt_local_dat_candidates(explicit_roots, limit=limit)
    result["scan_duration_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    result["server_time"] = int(time.time() * 1000)
    return result


def _load_qmt_registry_projection(force: bool = False) -> dict[str, Any]:
    from data_manager.datasource_discovery import build_datasource_inventory_cached

    inventory = build_datasource_inventory_cached(force=force)
    discovery = cast(dict[str, Any], inventory.get("discovery") or {})
    qmt_local = cast(dict[str, Any], discovery.get("qmt_local_dat") or {})
    registry_projection = cast(dict[str, Any], qmt_local.get("registry_projection") or {})
    return {
        "inventory": inventory,
        "qmt_local": qmt_local,
        "registry_projection": registry_projection,
    }


def _serialize_qmt_registry_items(items: list[Any]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            serialized.append(cast(dict[str, Any], item))
            continue
        if is_dataclass(item) and not isinstance(item, type):
            serialized.append(cast(dict[str, Any], asdict(item)))
            continue
        serialized.append({"value": item})
    return serialized


def _list_owner_qmt_userdatas(owner_user_id: str | None) -> list[str]:
    from core.broker_accounts import get_broker_manager

    userdatas: list[str] = []
    seen: set[str] = set()
    for acct in get_broker_manager().list_account_objects(owner_user_id=owner_user_id):
        userdata = _normalize_userdata_path(getattr(acct, "qmt_userdata_path", "") or "")
        if not userdata or userdata in seen:
            continue
        seen.add(userdata)
        userdatas.append(userdata)
    return userdatas


def _load_qmt_probe_projection(
    userdata_path: str | None = None,
    *,
    owner_user_id: str | None = None,
) -> dict[str, Any]:
    from core.qmt_registry import project_trading_account_probe_payload

    requested_userdata = _normalize_userdata_path(userdata_path or "")
    if owner_user_id is None:
        raw_payload = discover_trading_accounts({"userdata_path": requested_userdata} if requested_userdata else None)
    else:
        allowed_userdatas = _list_owner_qmt_userdatas(owner_user_id)
        if requested_userdata:
            if requested_userdata not in allowed_userdatas:
                raw_payload = {
                    "discovered": [],
                    "errors": [f"当前用户不可访问指定 userdata_path: {requested_userdata}"],
                }
            else:
                raw_payload = discover_trading_accounts({"userdata_path": requested_userdata})
        else:
            discovered: list[dict[str, Any]] = []
            errors: list[str] = []
            for allowed_userdata in allowed_userdatas:
                probe_payload = discover_trading_accounts({"userdata_path": allowed_userdata})
                discovered.extend(cast(list[dict[str, Any]], probe_payload.get("discovered") or []))
                errors.extend(str(item) for item in probe_payload.get("errors") or [])
            raw_payload = {
                "discovered": discovered,
                "errors": errors,
            }
    projection = project_trading_account_probe_payload(raw_payload)
    return {
        "raw": raw_payload,
        "projection": projection,
    }


def _load_qmt_session_projection(owner_user_id: str | None = None) -> dict[str, Any]:
    from core.broker_accounts import get_broker_manager
    from core.qmt_registry import project_gateway_session_payload

    entries_by_userdata: dict[str, dict[str, Any]] = {}
    broker_mgr = get_broker_manager()

    with _trade_api_lock:
        cached_instances = dict(_trade_api_instances)

    for acct in broker_mgr.list_account_objects(owner_user_id=owner_user_id):
        if not getattr(acct, "is_active", False):
            continue
        userdata_path = _normalize_userdata_path(getattr(acct, "qmt_userdata_path", "") or "")
        if not userdata_path:
            continue
        entry = entries_by_userdata.setdefault(
            userdata_path,
            {
                "userdata_path": userdata_path,
                "broker_id": str(getattr(acct, "broker", "") or ""),
                "broker_guess": str(getattr(acct, "broker", "") or ""),
                "supported_account_types": [],
                "connected_accounts": [],
                "current_route_claims": [],
                "connected": False,
                "authenticated": False,
                "process_status": "disconnected",
                "login_status": "disconnected",
                "last_error": "",
            },
        )
        entry["broker_id"] = entry["broker_id"] or str(getattr(acct, "broker", "") or "")
        entry["broker_guess"] = entry["broker_guess"] or str(getattr(acct, "broker", "") or "")
        entry["supported_account_types"] = sorted(
            {
                *cast(list[str], entry.get("supported_account_types") or []),
                *[str(item or "").strip() for item in getattr(acct, "account_types", []) or [] if str(item or "").strip()],
            }
        )
        trade_account = str(getattr(acct, "trade_account", "") or "").strip()
        status_text, hint, _userdata = _get_trade_account_connection_snapshot(trade_account)
        if status_text == "connected":
            entry["connected"] = True
            entry["authenticated"] = True
            entry["process_status"] = "process_alive"
            entry["login_status"] = "connected"
            if trade_account:
                entry["connected_accounts"] = sorted(
                    {
                        *cast(list[str], entry.get("connected_accounts") or []),
                        trade_account,
                    }
                )
            if getattr(acct, "is_default", False) and trade_account:
                entry["current_route_claims"] = sorted(
                    {
                        *cast(list[str], entry.get("current_route_claims") or []),
                        f"trade_default:{trade_account}",
                    }
                )
        elif hint and not entry["last_error"]:
            entry["last_error"] = hint

    allowed_userdatas = set(entries_by_userdata.keys())
    for userdata_path, api in cached_instances.items():
        if owner_user_id is not None and allowed_userdatas and userdata_path not in allowed_userdatas:
            continue
        entry = entries_by_userdata.setdefault(
            userdata_path,
            {
                "userdata_path": userdata_path,
                "broker_id": "",
                "broker_guess": "",
                "supported_account_types": [],
                "connected_accounts": [],
                "current_route_claims": [],
                "connected": False,
                "authenticated": False,
                "process_status": "disconnected",
                "login_status": "disconnected",
                "last_error": "",
            },
        )
        entry["process_status"] = "process_alive"
        raw_accounts = getattr(api, "accounts", {}) or {}
        account_ids = sorted(str(account_id or "").strip() for account_id in raw_accounts.keys() if str(account_id or "").strip())
        if account_ids:
            entry["connected"] = True
            entry["authenticated"] = True
            entry["login_status"] = "connected"
            entry["connected_accounts"] = sorted(
                {
                    *cast(list[str], entry.get("connected_accounts") or []),
                    *account_ids,
                }
            )

    projection = project_gateway_session_payload(list(entries_by_userdata.values()))
    return {
        "projection": projection,
        "cache_size": len(cached_instances),
    }


def _load_qmt_account_binding_projection(
    *,
    owner_user_id: str | None = None,
    include_probes: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    from core.broker_accounts import get_broker_manager
    from core.qmt_registry import (
        build_qmt_account_binding_projection,
        build_qmt_conflict_projection,
        build_qmt_route_decision_projection,
    )

    registry_payload = _load_qmt_registry_projection(force=force)
    session_payload = _load_qmt_session_projection(owner_user_id=owner_user_id)
    probe_items: list[Any] = []
    probe_errors: list[str] = []
    if include_probes:
        probe_payload = _load_qmt_probe_projection(owner_user_id=owner_user_id)
        probe_projection = cast(dict[str, Any], probe_payload.get("projection") or {})
        probe_raw = cast(dict[str, Any], probe_payload.get("raw") or {})
        probe_items = cast(list[Any], probe_projection.get("probes") or [])
        probe_errors = list(probe_raw.get("errors") or [])

    projection = cast(dict[str, Any], registry_payload.get("registry_projection") or {})
    session_projection = cast(dict[str, Any], session_payload.get("projection") or {})
    route_projection = build_qmt_route_decision_projection(
        assets=cast(list[Any], projection.get("assets") or []),
        sessions=cast(list[Any], session_projection.get("sessions") or []),
    )
    conflict_projection = build_qmt_conflict_projection(
        layouts=cast(list[Any], projection.get("layouts") or []),
        assets=cast(list[Any], projection.get("assets") or []),
        probes=probe_items,
        sessions=cast(list[Any], session_projection.get("sessions") or []),
    )
    accounts = get_broker_manager().list_account_objects(owner_user_id=owner_user_id)
    bindings = build_qmt_account_binding_projection(
        accounts=accounts,
        layouts=cast(list[Any], projection.get("layouts") or []),
        assets=cast(list[Any], projection.get("assets") or []),
        probes=probe_items,
        sessions=cast(list[Any], session_projection.get("sessions") or []),
        conflicts=cast(list[Any], conflict_projection.get("items") or []),
        routes=cast(list[Any], route_projection.get("items") or []),
    )
    return {
        "bindings": bindings,
        "probe_errors": probe_errors,
        "include_probes": include_probes,
    }


@app.get(
    "/api/v1/qmt/layouts",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_qmt_layouts(
    force: bool = Query(default=False, description="为 true 时绕过 inventory TTL 缓存重新扫描"),
) -> dict[str, Any]:
    """返回 qmt_registry v0.1 的 layout 只读投影。"""

    try:
        payload = _load_qmt_registry_projection(force=force)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"QMT layout 投影构建失败: {exc}",
        ) from exc

    inventory = cast(dict[str, Any], payload.get("inventory") or {})
    projection = cast(dict[str, Any], payload.get("registry_projection") or {})
    items = _serialize_qmt_registry_items(cast(list[Any], projection.get("layouts") or []))
    return {
        "items": items,
        "total": len(items),
        "cache_status": inventory.get("cache_status"),
        "cache_age_ms": inventory.get("cache_age_ms"),
        "last_scan_at": inventory.get("last_scan_at"),
        "candidate_count": projection.get("candidate_count", len(items)),
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/qmt/assets",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_qmt_assets(
    force: bool = Query(default=False, description="为 true 时绕过 inventory TTL 缓存重新扫描"),
) -> dict[str, Any]:
    """返回 qmt_registry v0.1 的 asset 只读投影。"""

    try:
        payload = _load_qmt_registry_projection(force=force)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"QMT asset 投影构建失败: {exc}",
        ) from exc

    inventory = cast(dict[str, Any], payload.get("inventory") or {})
    projection = cast(dict[str, Any], payload.get("registry_projection") or {})
    items = _serialize_qmt_registry_items(cast(list[Any], projection.get("assets") or []))
    return {
        "items": items,
        "total": len(items),
        "cache_status": inventory.get("cache_status"),
        "cache_age_ms": inventory.get("cache_age_ms"),
        "last_scan_at": inventory.get("last_scan_at"),
        "candidate_count": projection.get("candidate_count", len(items)),
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/qmt/probes",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_qmt_probes(
    request: Request,
    userdata_path: str = Query(default="", description="可选，限制为单个 userdata_path 的轻量 probe"),
) -> dict[str, Any]:
    """返回 qmt_registry v0.1 的 probe 只读投影。"""

    try:
        user = _require_user(request)
        owner_user_id = user.user_id if getattr(user, "role", "") != "admin" else None
        payload = _load_qmt_probe_projection(
            userdata_path.strip() or None,
            owner_user_id=owner_user_id,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"QMT probe 投影构建失败: {exc}",
        ) from exc

    raw_payload = cast(dict[str, Any], payload.get("raw") or {})
    projection = cast(dict[str, Any], payload.get("projection") or {})
    items = _serialize_qmt_registry_items(cast(list[Any], projection.get("probes") or []))
    return {
        "items": items,
        "total": len(items),
        "errors": list(raw_payload.get("errors") or []),
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/qmt/sessions",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_qmt_sessions(request: Request) -> dict[str, Any]:
    """返回 qmt_registry v0.1 的 session 只读快照。"""

    try:
        user = _require_user(request)
        owner_user_id = user.user_id if getattr(user, "role", "") != "admin" else None
        payload = _load_qmt_session_projection(owner_user_id=owner_user_id)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"QMT session 投影构建失败: {exc}",
        ) from exc

    projection = cast(dict[str, Any], payload.get("projection") or {})
    items = _serialize_qmt_registry_items(cast(list[Any], projection.get("sessions") or []))
    return {
        "items": items,
        "total": len(items),
        "cache_size": int(payload.get("cache_size") or 0),
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/qmt/conflicts",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_qmt_conflicts(
    request: Request,
    include_probes: bool = Query(default=False, description="为 true 时执行轻量账户 probe 以增强冲突判定"),
    force: bool = Query(default=False, description="为 true 时绕过 inventory TTL 缓存重新扫描"),
) -> dict[str, Any]:
    """返回 qmt_registry v0.1 的冲突摘要。"""

    try:
        from core.qmt_registry import build_qmt_conflict_projection

        user = _require_user(request)
        owner_user_id = user.user_id if getattr(user, "role", "") != "admin" else None
        registry_payload = _load_qmt_registry_projection(force=force)
        session_payload = _load_qmt_session_projection(owner_user_id=owner_user_id)
        probe_items: list[Any] = []
        probe_errors: list[str] = []
        if include_probes:
            probe_payload = _load_qmt_probe_projection(owner_user_id=owner_user_id)
            probe_projection = cast(dict[str, Any], probe_payload.get("projection") or {})
            probe_raw = cast(dict[str, Any], probe_payload.get("raw") or {})
            probe_items = cast(list[Any], probe_projection.get("probes") or [])
            probe_errors = list(probe_raw.get("errors") or [])
        projection = cast(dict[str, Any], registry_payload.get("registry_projection") or {})
        session_projection = cast(dict[str, Any], session_payload.get("projection") or {})
        conflicts = build_qmt_conflict_projection(
            layouts=cast(list[Any], projection.get("layouts") or []),
            assets=cast(list[Any], projection.get("assets") or []),
            probes=probe_items,
            sessions=cast(list[Any], session_projection.get("sessions") or []),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"QMT conflict 投影构建失败: {exc}",
        ) from exc

    items = _serialize_qmt_registry_items(cast(list[Any], conflicts.get("items") or []))
    return {
        "items": items,
        "total": len(items),
        "include_probes": include_probes,
        "probe_errors": probe_errors,
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/qmt/route-decisions",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_qmt_route_decisions(
    request: Request,
    force: bool = Query(default=False, description="为 true 时绕过 inventory TTL 缓存重新扫描"),
) -> dict[str, Any]:
    """返回 qmt_registry v0.1 的默认路由快照。"""

    try:
        from core.qmt_registry import build_qmt_route_decision_projection

        user = _require_user(request)
        owner_user_id = user.user_id if getattr(user, "role", "") != "admin" else None
        registry_payload = _load_qmt_registry_projection(force=force)
        session_payload = _load_qmt_session_projection(owner_user_id=owner_user_id)
        projection = cast(dict[str, Any], registry_payload.get("registry_projection") or {})
        session_projection = cast(dict[str, Any], session_payload.get("projection") or {})
        route_projection = build_qmt_route_decision_projection(
            assets=cast(list[Any], projection.get("assets") or []),
            sessions=cast(list[Any], session_projection.get("sessions") or []),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"QMT route 决策快照构建失败: {exc}",
        ) from exc

    items = _serialize_qmt_registry_items(cast(list[Any], route_projection.get("items") or []))
    return {
        "items": items,
        "total": len(items),
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/account-bindings",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_account_bindings(
    request: Request,
    include_probes: bool = Query(default=False, description="为 true 时执行轻量账户 probe 以增强 binding explain"),
    force: bool = Query(default=False, description="为 true 时绕过 inventory TTL 缓存重新扫描"),
) -> dict[str, Any]:
    """返回账户绑定读模型与 explain 摘要。"""

    try:
        user = _require_user(request)
        owner_user_id = user.user_id if getattr(user, "role", "") != "admin" else None
        payload = _load_qmt_account_binding_projection(
            owner_user_id=owner_user_id,
            include_probes=include_probes,
            force=force,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"account binding 投影构建失败: {exc}",
        ) from exc

    bindings = cast(dict[str, Any], payload.get("bindings") or {})
    items = cast(list[dict[str, Any]], bindings.get("items") or [])
    return {
        "items": items,
        "total": len(items),
        "include_probes": include_probes,
        "probe_errors": list(payload.get("probe_errors") or []),
        "server_time": int(time.time() * 1000),
    }


@app.post(
    "/api/v1/account-bindings/discover",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def discover_account_bindings(
    request: Request,
    body: AccountBindingDiscoverBody | None = None,
) -> dict[str, Any]:
    """显式触发账户绑定 discover，并返回最新 formal binding explain。"""

    request_body = body or AccountBindingDiscoverBody()
    include_probes = bool(request_body.include_probes)
    force = bool(request_body.force)

    try:
        user = _require_user(request)
        owner_user_id = user.user_id if getattr(user, "role", "") != "admin" else None
        payload = _load_qmt_account_binding_projection(
            owner_user_id=owner_user_id,
            include_probes=include_probes,
            force=force,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"account binding discover 失败: {exc}",
        ) from exc

    bindings = cast(dict[str, Any], payload.get("bindings") or {})
    items = cast(list[dict[str, Any]], bindings.get("items") or [])
    return {
        "items": items,
        "total": len(items),
        "include_probes": include_probes,
        "force": force,
        "operation": "discover",
        "probe_errors": list(payload.get("probe_errors") or []),
        "server_time": int(time.time() * 1000),
    }


@app.post(
    "/api/v1/account-bindings/{binding_id}/apply",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def apply_account_binding(
    binding_id: str,
    request: Request,
    body: AccountBindingApplyBody | None = None,
) -> dict[str, Any]:
    """将 formal binding 的建议路径写回到账户配置。"""

    from core.broker_accounts import get_broker_manager

    request_body = body or AccountBindingApplyBody()
    include_probes = bool(request_body.include_probes)
    force = bool(request_body.force)

    try:
        user = _require_user(request)
        owner_user_id = user.user_id if getattr(user, "role", "") != "admin" else None
        payload = _load_qmt_account_binding_projection(
            owner_user_id=owner_user_id,
            include_probes=include_probes,
            force=force,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"account binding apply 前置审阅失败: {exc}",
        ) from exc

    bindings = cast(dict[str, Any], payload.get("bindings") or {})
    items = cast(list[dict[str, Any]], bindings.get("items") or [])
    binding_item = next(
        (item for item in items if str(item.get("binding_id") or "").strip() == binding_id),
        None,
    )
    if binding_item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="account binding 未找到")

    broker_account_id = str(binding_item.get("broker_account_id") or "").strip()
    if not broker_account_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="account binding 缺少 broker_account_id，无法写回",
        )

    apply_path = str(binding_item.get("apply_path") or "").strip()
    if not apply_path:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="当前 binding 没有可写回的 apply_path",
        )

    approval_state = str(binding_item.get("approval_state") or "").strip()
    binding_status = str(binding_item.get("status") or "").strip()
    if approval_state == "review_required" or binding_status == "conflicted":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="当前 binding 仍处于 review_required/conflicted，不能直接写回",
        )

    mgr = get_broker_manager()
    account = mgr.get_account(broker_account_id)
    if account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="绑定对应的券商账户未找到")
    if getattr(user, "role", "") != "admin" and getattr(account, "owner_user_id", None) != user.user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="无权修改此账户")

    current_path = str(getattr(account, "qmt_userdata_path", "") or "").strip()
    updated = current_path != apply_path
    if updated:
        mgr.update_account(broker_account_id, qmt_userdata_path=apply_path)

    refreshed_account = mgr.get_account(broker_account_id)
    refreshed_payload = _load_qmt_account_binding_projection(
        owner_user_id=owner_user_id,
        include_probes=include_probes,
        force=False,
    )
    refreshed_bindings = cast(dict[str, Any], refreshed_payload.get("bindings") or {})
    refreshed_items = cast(list[dict[str, Any]], refreshed_bindings.get("items") or [])
    refreshed_item = next(
        (item for item in refreshed_items if str(item.get("broker_account_id") or "").strip() == broker_account_id),
        None,
    )

    return {
        "operation": "apply",
        "binding_id": binding_id,
        "broker_account_id": broker_account_id,
        "applied_path": apply_path,
        "updated": updated,
        "include_probes": include_probes,
        "force": force,
        "binding": refreshed_item,
        "account": refreshed_account.to_safe_dict() if refreshed_account is not None else None,
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/qmt/runtime-config",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_qmt_runtime_config(request: Request) -> dict[str, Any]:
    """返回 unified_config.json 中当前生效的运行时 QMT 主配置。"""

    _require_user(request)
    from core.runtime_qmt_config import read_runtime_qmt_config

    payload = read_runtime_qmt_config()
    payload["server_time"] = int(time.time() * 1000)
    return payload


@app.post(
    "/api/v1/qmt/runtime-config/apply-account/{account_id}",
    tags=["接口管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def apply_qmt_runtime_config_from_account(account_id: str, request: Request) -> dict[str, Any]:
    """把指定券商账户的 QMT 路径同步到 unified_config.json。"""

    from core.broker_accounts import get_broker_manager
    from core.qmt_registry.compat import resolve_legacy_qmt_paths
    from core.runtime_qmt_config import write_runtime_qmt_config

    user = _require_user(request)
    mgr = get_broker_manager()
    account = mgr.get_account(account_id)
    if not account:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="券商账户未找到")
    if user.role != "admin" and getattr(account, "owner_user_id", None) != user.user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="无权修改此账户")

    resolved_qmt = resolve_legacy_qmt_paths(
        {
            "qmt_exe_path": getattr(account, "qmt_exe_path", "") or "",
            "qmt_userdata_path": getattr(account, "qmt_userdata_path", "") or "",
        }
    )
    qmt_path = str(resolved_qmt.exe_path or getattr(account, "qmt_exe_path", "") or "").strip()
    qmt_userdata_path = str(
        resolved_qmt.userdata_path or getattr(account, "qmt_userdata_path", "") or ""
    ).strip()
    if not qmt_path and not qmt_userdata_path:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="当前账户缺少可同步的 QMT 路径信息",
        )

    runtime_payload = write_runtime_qmt_config(
        qmt_path=qmt_path or None,
        qmt_userdata_path=qmt_userdata_path or None,
    )
    updated_fields = list(runtime_payload.pop("updated_fields", []) or [])

    return {
        "operation": "sync_from_broker_account",
        "broker_account_id": account_id,
        "updated": bool(updated_fields),
        "synced_fields": updated_fields,
        "runtime_config": runtime_payload,
        "account": account.to_safe_dict(),
        "server_time": int(time.time() * 1000),
    }


@app.get("/health/sla", tags=["运维"])
def sla_health_check(report_date: str = "") -> dict[str, Any]:
    """
    数据质量 SLA 报告（当日或指定日期）。

    - `report_date`: 可选，格式 YYYY-MM-DD，默认为今天。
    - `gate_pass=false` 时 status 返回 "degraded"。
    """
    return _get_sla_health_payload(report_date, persist=True)


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


def _resolve_golden_1d_batch_symbols(
    auditor: Any,
    *,
    limit: int,
    log_context: str,
    enumeration_error_cls: type[Exception],
) -> tuple[list[str] | None, str | None]:
    """统一解析 Golden 1D 批量任务的标的枚举语义。

    返回：
    - (None, "enumeration_failed")：数据库访问失败，无法枚举
    - ([], "no_stored_symbols")：枚举成功，但当前无可处理标的
    - ([...], None)：正常返回标的列表
    """
    try:
        symbols = auditor.list_stored_symbols(limit=limit)
    except enumeration_error_cls as enum_exc:
        log.warning("%s: 标的枚举失败 → %s", log_context, enum_exc)
        return None, "enumeration_failed"
    if not symbols:
        return [], "no_stored_symbols"
    return symbols, None


def _build_golden_1d_batch_short_circuit_response(
    *,
    mode: str,
    coverage_note: str,
    force_full: bool,
    limit: int,
) -> dict[str, Any]:
    """统一构造 Golden 1D 批量接口的短路响应。"""
    payload: dict[str, Any] = {
        "coverage_note": coverage_note,
        "force_full": force_full,
        "limit": limit,
        "server_time": int(time.time() * 1000),
    }
    if mode == "audit":
        payload.update(
            {
                "total_audited": 0,
                "golden_count": 0,
                "partial_trust_count": 0,
                "degraded_count": 0,
                "unknown_count": 0,
            }
        )
    else:
        payload["processed"] = 0
    return payload


def _finalize_golden_1d_response(payload: dict[str, Any]) -> dict[str, Any]:
    """统一补齐 Golden 1D 端点的公共元信息。"""
    payload["server_time"] = int(time.time() * 1000)
    payload["build_version"] = _BUILD_VERSION
    payload["commit_sha"] = _COMMIT_SHA
    return payload


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
                return _finalize_golden_1d_response({
                    "symbol": symbol,
                    "status": "unknown",
                    "message": "该标的尚未执行审计",
                    "repair": _serialize_repair_snapshot(symbol),
                })
            return _finalize_golden_1d_response({
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
            })
        else:
            import sqlite3

            conn = sqlite3.connect(auditor.audit_db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT golden_status, COUNT(*) as cnt FROM golden_1d_audit GROUP BY golden_status"
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) as cnt FROM golden_1d_audit").fetchone()["cnt"]
            last_row = conn.execute(
                "SELECT MAX(last_audited_at) as last_at FROM golden_1d_audit"
            ).fetchone()
            last_batch_audited_at: str | None = last_row["last_at"] if last_row else None
            conn.close()

            summary = {"golden": 0, "partial_trust": 0, "degraded": 0, "unknown": 0}
            for row in rows:
                status_name = str(row["golden_status"] or "unknown").strip() or "unknown"
                if status_name not in summary:
                    status_name = "unknown"
                summary[status_name] += int(row["cnt"] or 0)

            return _finalize_golden_1d_response({
                "total_audited": total,
                "golden_count": summary["golden"],
                "partial_trust_count": summary["partial_trust"],
                "degraded_count": summary["degraded"],
                "unknown_count": summary["unknown"],
                "golden_ratio": summary["golden"] / total if total > 0 else 0.0,
                "last_batch_audited_at": last_batch_audited_at,
            })
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"黄金标准 1D 状态查询失败: {exc}",
        ) from exc


@app.get(
    "/api/v1/data-quality/period-asset-matrix",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_period_asset_matrix() -> dict[str, Any]:
    """查询周期资产矩阵。

    返回每个已定义周期的元信息（来自 period_registry）与 DuckDB
    custom_period_bars 表中的实际落库覆盖统计，供 DataRoute 周期资产 tab 消费。
    """
    registry = _get_chart_period_registry()
    period_meta: dict[str, dict[str, Any]] = {}
    for defn in registry.list_definitions(enabled_only=True):
        runtime = str(defn.runtime_code or defn.period_code).strip()
        if not runtime:
            continue
        period_meta[runtime] = {
            "period": runtime,
            "period_code": defn.period_code,
            "label": defn.description or runtime,
            "period_family": defn.period_family,
            "base_source": defn.base_source,
            "alignment": defn.alignment,
            "anchor": defn.anchor,
            "layer": defn.layer,
            "precompute_default": defn.precompute_default,
            "ui_visible_default": defn.ui_visible_default,
            "tick_verifiable": defn.tick_verifiable,
            "covered_symbols": 0,
            "total_bars": 0,
            "earliest_bar": None,
            "latest_bar": None,
            "last_indexed_at": None,
        }

    duckdb_error: str | None = None
    try:
        from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

        db_mgr = get_db_manager(resolve_duckdb_path())
        df = db_mgr.execute_read_query("""
            SELECT
                period,
                COUNT(DISTINCT stock_code) AS covered_symbols,
                COUNT(*)                   AS total_bars,
                MIN(datetime)              AS earliest_bar,
                MAX(datetime)              AS latest_bar,
                MAX(created_at)            AS last_indexed_at
            FROM custom_period_bars
            GROUP BY period
            ORDER BY period
        """)
        for _, row in df.iterrows():
            p = str(row["period"])
            if p not in period_meta:
                period_meta[p] = {
                    "period": p,
                    "period_code": p,
                    "label": p,
                    "period_family": "unknown",
                    "base_source": "unknown",
                    "alignment": "unknown",
                    "anchor": "unknown",
                    "layer": "unknown",
                    "precompute_default": False,
                    "ui_visible_default": False,
                    "tick_verifiable": False,
                    "covered_symbols": 0,
                    "total_bars": 0,
                    "earliest_bar": None,
                    "latest_bar": None,
                    "last_indexed_at": None,
                }
            period_meta[p]["covered_symbols"] = int(row["covered_symbols"])
            period_meta[p]["total_bars"] = int(row["total_bars"])
            eb = row.get("earliest_bar")
            lb = row.get("latest_bar")
            li = row.get("last_indexed_at")
            period_meta[p]["earliest_bar"] = str(eb) if eb is not None else None
            period_meta[p]["latest_bar"] = str(lb) if lb is not None else None
            period_meta[p]["last_indexed_at"] = str(li) if li is not None else None
    except Exception as exc:
        log.warning("period_asset_matrix DuckDB query failed: %s", exc)
        duckdb_error = str(exc)

    items = sorted(period_meta.values(), key=lambda x: _chart_interval_sort_key(str(x["period"])))
    return {
        "periods": items,
        "total_periods": len(items),
        "duckdb_error": duckdb_error,
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/data-quality/basic-arsenal-status",
    tags=["数据质量"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_basic_arsenal_status(
    force: bool = Query(default=False, description="是否绕过 TTL 缓存强制重算"),
) -> dict[str, Any]:
    """查询基础历史数据弹药库完整性状态（V2）。

    基于 stock_daily / stock_1m / stock_5m 三张基础原始表，结合：
    - 目标宇宙（优先 QMT 全市场 A 股，失败时回退到 data_ingestion_status / 已落库表）
    - 当前应达成的验收交易日（按 A 股交易日历，收盘前默认验收上一交易日）
    - 每周期的覆盖率、缺口样本、滞后样本、最新交易日滞后天数

    返回更接近“完整 + 完备”的基础弹药库验收读模型，供下载工作台消费。
    """
    global _basic_arsenal_status_cache

    now_ms = int(time.time() * 1000)
    ttl_ms = int(_BASIC_ARSENAL_STATUS_TTL_S * 1000)
    if not force:
        with _basic_arsenal_status_cache_lock:
            cached = dict(_basic_arsenal_status_cache or {}) if _basic_arsenal_status_cache else None
        if cached is not None:
            cached_at_ms = int(cached.get("cached_at_ms", 0) or 0)
            age_ms = max(now_ms - cached_at_ms, 0)
            if age_ms <= ttl_ms:
                payload = dict(cached.get("payload") or {})
                payload["server_time"] = now_ms
                payload["cache_status"] = "fresh"
                payload["cache_age_ms"] = age_ms
                return payload

    period_tables = [
        ("1d", "stock_daily", "date"),
        ("1m", "stock_1m", "datetime"),
        ("5m", "stock_5m", "datetime"),
    ]
    results: list[dict[str, Any]] = []
    duckdb_error: str | None = None
    blocking_issues: list[str] = []
    acceptance_reasons: list[str] = []
    target_universe: list[str] = []
    target_universe_source = "unknown"
    target_universe_note: str | None = None
    acceptance_trade_day: date | None = None
    acceptance_trade_day_str: str | None = None
    structural_coverage_threshold = _BASIC_ARSENAL_STRUCTURAL_FRESH_RATIO_THRESHOLD
    structural_max_lag_days = _BASIC_ARSENAL_STRUCTURAL_MAX_LAG_DAYS

    def _coerce_date_str(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text.lower() in {"nan", "nat", "none"}:
            return None
        return text[:10]

    def _coerce_date(value: Any) -> date | None:
        text = _coerce_date_str(value)
        if not text:
            return None
        try:
            return date.fromisoformat(text)
        except ValueError:
            return None

    def _resolve_acceptance_trade_day() -> tuple[date | None, str | None, str | None]:
        try:
            from data_manager.smart_data_detector import get_trading_calendar

            calendar = get_trading_calendar()
            now_local = datetime.now()
            anchor_day = now_local.date()
            if now_local.hour < 15:
                anchor_day -= timedelta(days=1)
            for offset in range(15):
                candidate = anchor_day - timedelta(days=offset)
                if calendar.is_trading_day(candidate):
                    return candidate, candidate.isoformat(), None
            return None, None, "最近 15 天未解析出有效交易日"
        except Exception as exc:
            return None, None, f"交易日历解析失败: {exc}"

    def _trading_day_lag(latest_day: date | None, target_day: date | None) -> int | None:
        if latest_day is None or target_day is None:
            return None
        if latest_day >= target_day:
            return 0
        try:
            from data_manager.smart_data_detector import get_trading_calendar

            calendar = get_trading_calendar()
            trading_days = calendar.get_trading_days(latest_day, target_day)
            return max(len(trading_days) - 1, 0)
        except Exception:
            lag = 0
            current_day = latest_day + timedelta(days=1)
            while current_day <= target_day:
                if current_day.weekday() < 5:
                    lag += 1
                current_day += timedelta(days=1)
            return lag

    def _resolve_target_universe(db_mgr: Any | None) -> tuple[list[str], str, str | None]:
        resolution_notes: list[str] = []
        try:
            normalized = _load_a_share_stock_codes(["SH", "SZ", "BJ"])
            if normalized:
                return normalized, "qmt_a_share", None
            resolution_notes.append("QMT 返回空 A 股宇宙")
        except Exception as exc:
            resolution_notes.append(f"QMT 宇宙解析失败: {exc}")

        if db_mgr is not None:
            try:
                status_df = db_mgr.execute_read_query(
                    """
                    SELECT DISTINCT stock_code
                    FROM data_ingestion_status
                    WHERE record_count > 0
                      AND period IN ('1d', '1m', '5m')
                      AND (
                        stock_code LIKE '%.SH'
                        OR stock_code LIKE '%.SZ'
                        OR stock_code LIKE '%.BJ'
                      )
                    ORDER BY stock_code
                    """
                )
                if not status_df.empty:
                    normalized = sorted({
                        str(code).strip().upper()
                        for code in status_df["stock_code"].tolist()
                        if str(code).strip()
                    })
                    if normalized:
                        note = "；".join(resolution_notes) if resolution_notes else None
                        return normalized, "data_ingestion_status", note
            except Exception as exc:
                resolution_notes.append(f"data_ingestion_status 宇宙回退失败: {exc}")

        if db_mgr is not None:
            try:
                covered_df = db_mgr.execute_read_query(
                    """
                    SELECT DISTINCT stock_code
                    FROM (
                        SELECT stock_code FROM stock_daily WHERE adjust_type = 'none'
                        UNION ALL
                        SELECT stock_code FROM stock_1m WHERE adjust_type = 'none'
                        UNION ALL
                        SELECT stock_code FROM stock_5m WHERE adjust_type = 'none'
                    ) AS covered_universe
                    WHERE (
                        stock_code LIKE '%.SH'
                        OR stock_code LIKE '%.SZ'
                        OR stock_code LIKE '%.BJ'
                    )
                    ORDER BY stock_code
                    """
                )
                if not covered_df.empty:
                    normalized = sorted({
                        str(code).strip().upper()
                        for code in covered_df["stock_code"].tolist()
                        if str(code).strip()
                    })
                    if normalized:
                        note = "；".join(resolution_notes) if resolution_notes else None
                        return normalized, "covered_tables", note
            except Exception as exc:
                resolution_notes.append(f"基础表宇宙回退失败: {exc}")

        return [], "unresolved", "；".join(resolution_notes) if resolution_notes else None

    try:
        from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

        db_mgr = get_db_manager(resolve_duckdb_path())
        target_universe, target_universe_source, target_universe_note = _resolve_target_universe(db_mgr)
        acceptance_trade_day, acceptance_trade_day_str, trade_day_error = _resolve_acceptance_trade_day()
        if trade_day_error:
            blocking_issues.append(trade_day_error)
        if target_universe_note:
            acceptance_reasons.append(target_universe_note)
        if not target_universe:
            blocking_issues.append("目标宇宙为空，当前无法判断基础历史是否完整")

        for period, table, dt_col in period_tables:
            try:
                summary_df = db_mgr.execute_read_query(
                    f"""
                    SELECT
                        COUNT(*)                   AS total_bars,
                        MIN({dt_col})              AS earliest_bar,
                        MAX({dt_col})              AS latest_bar
                    FROM {table}
                    WHERE adjust_type = 'none'
                    """
                )
                symbol_df = db_mgr.execute_read_query(
                    f"""
                    SELECT stock_code, MAX({dt_col}) AS latest_bar
                    FROM {table}
                    WHERE adjust_type = 'none'
                    GROUP BY stock_code
                    ORDER BY stock_code
                    """
                )
                gate_df = db_mgr.execute_read_query(
                    """
                    SELECT
                        COUNT(DISTINCT CASE
                            WHEN record_count > 0 AND COALESCE(gate_reject_reason, '') = 'passed'
                            THEN stock_code ELSE NULL END
                        ) AS gate_pass_symbols,
                        COUNT(DISTINCT CASE
                            WHEN record_count > 0 AND COALESCE(gate_reject_reason, '') NOT IN ('', 'passed')
                            THEN stock_code ELSE NULL END
                        ) AS gate_reject_symbols
                    FROM data_ingestion_status
                    WHERE period = ?
                    """,
                    (period,),
                )
                row = summary_df.iloc[0] if len(summary_df) > 0 else None
                gate_row = gate_df.iloc[0] if len(gate_df) > 0 else None
                covered_latest_map: dict[str, date] = {}
                fresh_symbols_count = 0
                stale_symbols: list[tuple[str, date, int | None]] = []
                for _, symbol_row in symbol_df.iterrows():
                    code = str(symbol_row.get("stock_code") or "").strip().upper()
                    if not code:
                        continue
                    latest_day = _coerce_date(symbol_row.get("latest_bar"))
                    if latest_day is not None:
                        covered_latest_map[code] = latest_day
                        lag = _trading_day_lag(latest_day, acceptance_trade_day)
                        if lag is None or lag <= structural_max_lag_days:
                            fresh_symbols_count += 1
                        if acceptance_trade_day is not None and latest_day < acceptance_trade_day:
                            stale_symbols.append((code, latest_day, lag))
                covered_symbols = len(covered_latest_map)
                expected_symbols = len(target_universe)
                coverage_ratio = (covered_symbols / expected_symbols) if expected_symbols > 0 else None
                fresh_ratio = (fresh_symbols_count / expected_symbols) if expected_symbols > 0 else None
                missing_symbols = sorted(set(target_universe) - set(covered_latest_map)) if target_universe else []
                stale_symbols = sorted(stale_symbols, key=lambda item: (item[1], item[0]))
                latest_trade_day = max(covered_latest_map.values()) if covered_latest_map else _coerce_date(row["latest_bar"] if row is not None else None)
                latest_trade_day_lag = _trading_day_lag(latest_trade_day, acceptance_trade_day)
                period_structurally_ready = bool(
                    expected_symbols > 0
                    and coverage_ratio is not None
                    and fresh_ratio is not None
                    and coverage_ratio >= structural_coverage_threshold
                    and fresh_ratio >= structural_coverage_threshold
                    and covered_symbols > 0
                )
                period_ready = bool(
                    expected_symbols > 0
                    and not missing_symbols
                    and stale_symbols == []
                    and (latest_trade_day_lag in {None, 0})
                    and covered_symbols > 0
                    and acceptance_trade_day is not None
                )
                results.append({
                    "period": period,
                    "table": table,
                    "covered_symbols": covered_symbols,
                    "expected_symbols": expected_symbols,
                    "coverage_ratio": coverage_ratio,
                    "fresh_symbols_count": fresh_symbols_count,
                    "fresh_ratio": fresh_ratio,
                    "total_bars": int(row["total_bars"]) if row is not None else 0,
                    "earliest_bar": _coerce_date_str(row["earliest_bar"] if row is not None else None),
                    "latest_bar": _coerce_date_str(row["latest_bar"] if row is not None else None),
                    "latest_trade_day": latest_trade_day.isoformat() if latest_trade_day is not None else None,
                    "latest_trade_day_lag": latest_trade_day_lag,
                    "missing_symbols_count": len(missing_symbols),
                    "missing_symbols_sample": missing_symbols[:8],
                    "stale_symbols_count": len(stale_symbols),
                    "stale_symbols_sample": [
                        f"{code}@{latest.isoformat()}(lag={lag if lag is not None else '?'})"
                        for code, latest, lag in stale_symbols[:8]
                    ],
                    "gate_pass_symbols": int(gate_row["gate_pass_symbols"]) if gate_row is not None else 0,
                    "gate_reject_symbols": int(gate_row["gate_reject_symbols"]) if gate_row is not None else 0,
                    "structurally_ready": period_structurally_ready,
                    "ready": period_ready,
                    "error": None,
                })
            except Exception as exc:
                log.warning("basic_arsenal_status query failed for %s: %s", table, exc)
                results.append({
                    "period": period,
                    "table": table,
                    "covered_symbols": 0,
                    "expected_symbols": len(target_universe),
                    "coverage_ratio": 0.0 if target_universe else None,
                    "fresh_symbols_count": 0,
                    "fresh_ratio": 0.0 if target_universe else None,
                    "total_bars": 0,
                    "earliest_bar": None,
                    "latest_bar": None,
                    "latest_trade_day": None,
                    "latest_trade_day_lag": None,
                    "missing_symbols_count": len(target_universe),
                    "missing_symbols_sample": target_universe[:8],
                    "stale_symbols_count": 0,
                    "stale_symbols_sample": [],
                    "gate_pass_symbols": 0,
                    "gate_reject_symbols": 0,
                    "structurally_ready": False,
                    "ready": False,
                    "error": str(exc),
                })
                if duckdb_error is None:
                    duckdb_error = str(exc)
    except Exception as exc:
        log.warning("basic_arsenal_status db init failed: %s", exc)
        duckdb_error = str(exc)
        for period, table, _ in period_tables:
            results.append({
                "period": period,
                "table": table,
                "covered_symbols": 0,
                "expected_symbols": 0,
                "coverage_ratio": None,
                "fresh_symbols_count": 0,
                "fresh_ratio": None,
                "total_bars": 0,
                "earliest_bar": None,
                "latest_bar": None,
                "latest_trade_day": None,
                "latest_trade_day_lag": None,
                "missing_symbols_count": 0,
                "missing_symbols_sample": [],
                "stale_symbols_count": 0,
                "stale_symbols_sample": [],
                "gate_pass_symbols": 0,
                "gate_reject_symbols": 0,
                "structurally_ready": False,
                "ready": False,
                "error": str(exc),
            })

    if target_universe_source == "qmt_a_share":
        acceptance_reasons.append(f"目标宇宙来源: QMT 全市场 A 股 ({len(target_universe)} 只)")
    elif target_universe_source != "unknown":
        acceptance_reasons.append(
            f"目标宇宙来源回退为 {target_universe_source} ({len(target_universe)} 只)"
        )
    if acceptance_trade_day_str:
        acceptance_reasons.append(f"验收交易日: {acceptance_trade_day_str}")
    acceptance_reasons.append(
        f"结构达标阈值: 覆盖率≥{structural_coverage_threshold:.0%} 且 freshness≥{structural_coverage_threshold:.0%}"
    )
    acceptance_reasons.append(f"结构达标允许的最新交易日滞后 ≤ {structural_max_lag_days} 个交易日")

    ready_count = sum(1 for r in results if bool(r.get("ready")))
    structurally_ready_count = sum(1 for r in results if bool(r.get("structurally_ready")))
    for item in results:
        period = str(item.get("period") or "?")
        error = str(item.get("error") or "").strip()
        if error:
            blocking_issues.append(f"{period} 查询失败: {error}")
            continue
        if int(item.get("missing_symbols_count") or 0) > 0:
            blocking_issues.append(f"{period} 缺少 {item['missing_symbols_count']} 只目标标的")
        lag = item.get("latest_trade_day_lag")
        if isinstance(lag, int) and lag > 0:
            blocking_issues.append(f"{period} 最新交易日滞后 {lag} 个交易日")
        stale_count = int(item.get("stale_symbols_count") or 0)
        if stale_count > 0:
            blocking_issues.append(f"{period} 有 {stale_count} 只标的停留在旧数据")
        acceptance_reasons.append(
            f"{period} 覆盖 {item['covered_symbols']}/{item['expected_symbols'] or item['covered_symbols']}"
        )
        if item.get("fresh_ratio") is not None:
            acceptance_reasons.append(
                f"{period} freshness {float(item['fresh_ratio']):.1%} / 结构达标={'是' if item.get('structurally_ready') else '否'}"
            )

    payload = {
        "periods": results,
        "ready_count": ready_count,
        "structurally_ready_count": structurally_ready_count,
        "total_periods": len(period_tables),
        "structurally_ready": (
            structurally_ready_count == len(period_tables)
            and duckdb_error is None
            and bool(target_universe)
            and acceptance_trade_day is not None
        ),
        "accepted": (
            ready_count == len(period_tables)
            and duckdb_error is None
            and not blocking_issues
            and bool(target_universe)
            and acceptance_trade_day is not None
        ),
        "duckdb_error": duckdb_error,
        "structural_coverage_threshold": structural_coverage_threshold,
        "structural_max_lag_days": structural_max_lag_days,
        "target_universe_source": target_universe_source,
        "target_universe_size": len(target_universe),
        "target_universe_sample": target_universe[:8],
        "target_universe_note": target_universe_note,
        "as_of_trade_day": acceptance_trade_day_str,
        "acceptance_reasons": acceptance_reasons[:12],
        "blocking_issues": blocking_issues[:12],
        "server_time": now_ms,
        "cache_status": "miss",
        "cache_age_ms": 0,
    }
    with _basic_arsenal_status_cache_lock:
        _basic_arsenal_status_cache = {
            "cached_at_ms": now_ms,
            "payload": dict(payload),
        }
    return payload


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
                return _finalize_golden_1d_response({
                    "symbol": symbol,
                    "plan_status": "unknown",
                    "generated_at": None,
                    "queued_tasks": 0,
                    "failed_tasks": 0,
                    "task_count": 0,
                    "blocker_issues": [],
                    "notes": [],
                    "tasks": [],
                })
            payload = _serialize_snapshot(snapshot)
            return _finalize_golden_1d_response(payload)

        snapshots = orchestrator.list_recent_plans(limit=limit)
        return _finalize_golden_1d_response({
            "items": [_serialize_snapshot(item) for item in snapshots],
            "returned": len(snapshots),
            "limit": limit,
        })
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
        from data_manager.golden_1d_audit import Golden1dAuditor, SymbolEnumerationError
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
            return _finalize_golden_1d_response({
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
            })

        symbols, coverage_note = _resolve_golden_1d_batch_symbols(
            auditor,
            limit=limit,
            log_context="golden-1d-repair batch",
            enumeration_error_cls=SymbolEnumerationError,
        )
        if coverage_note is not None:
            return _finalize_golden_1d_response(
                _build_golden_1d_batch_short_circuit_response(
                    mode="repair",
                    coverage_note=coverage_note,
                    force_full=force_full,
                    limit=limit,
                )
            )
        assert symbols is not None
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
        return _finalize_golden_1d_response({
            "processed": len(items),
            "stored_symbols_count": len(symbols),
            "status_counts": status_counts,
            "force_full": force_full,
            "limit": limit,
            "items": items,
            "audit_record": audit_record,
        })
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
        result = _call_with_datasource_health_interface(
            lambda iface: iface.run_late_event_replay(
                limit=limit,
                max_retries=max_retries,
                reason_regex=reason_regex,
                stock_code=symbol,
                period=period,
            )
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
        from data_manager.golden_1d_audit import Golden1dAuditor, SymbolEnumerationError

        auditor = Golden1dAuditor()

        if symbol:
            summary = auditor.audit_symbol(symbol, force_full=force_full)
            return _finalize_golden_1d_response({
                "symbol": summary.symbol,
                "golden_status": summary.golden_status,
                "is_golden_1d_ready": summary.is_golden_1d_ready,
                "missing_days": summary.missing_days,
                "force_full": force_full,
                "issues": summary.issues[:5],
            })
        else:
            symbols, coverage_note = _resolve_golden_1d_batch_symbols(
                auditor,
                limit=limit,
                log_context="golden-1d-audit batch",
                enumeration_error_cls=SymbolEnumerationError,
            )
            if coverage_note is not None:
                return _finalize_golden_1d_response(
                    _build_golden_1d_batch_short_circuit_response(
                        mode="audit",
                        coverage_note=coverage_note,
                        force_full=force_full,
                        limit=limit,
                    )
                )
            assert symbols is not None

            report = auditor.audit_batch(symbols[:limit], max_workers=4, force_full=force_full)
            return _finalize_golden_1d_response({
                "total_audited": report.total_symbols,
                "golden_count": report.golden_count,
                "partial_trust_count": report.partial_trust_count,
                "degraded_count": report.degraded_count,
                "unknown_count": report.unknown_count,
                "stored_symbols_count": len(symbols),
                "force_full": force_full,
                "limit": limit,
                "audited_at": report.audited_at,
            })
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
        datasource_health = datasource_health_check()
        sla_health = _get_sla_health_payload(sla_report_date, persist=False)
        pipeline = controller.get_pipeline_status()
        routing = controller.get_routing_metrics()
        duckdb = controller.get_duckdb_summary()
        environment = controller.get_all_env_config()
        realtime = controller.get_realtime_pipeline_info()
        threshold_bundle = _load_governance_threshold_bundle()
        threshold_overrides = threshold_bundle["overrides"]
        def _collect_governance_iface_snapshot(iface: Any) -> dict[str, Any]:
            return {
                "receipt_store": iface.get_receipt_store_summary(),
                "publish_gate": iface.get_publish_gate_summary(),
                "reject_reasons": iface.get_gate_reject_reason_summary(),
                "reject_severity": iface.get_gate_reject_severity_summary(),
                "gate_sla_impact": iface.get_gate_sla_impact_summary(),
                "receipt_timeline": iface.get_receipt_timeline(limit=12, lookback_days=trend_days),
                "gate_trend": iface.get_gate_trend_summary(days=trend_days),
                "gate_trend_by_symbol": iface.get_gate_dimension_trend_summary(
                    days=trend_days,
                    dimension="symbol",
                    limit=5,
                ),
                "gate_trend_by_period": iface.get_gate_dimension_trend_summary(
                    days=trend_days,
                    dimension="period",
                    limit=5,
                ),
                "sla_threshold_panel": iface.get_sla_alert_threshold_panel_with_overrides(
                    threshold_overrides
                ),
            }

        governance_iface_snapshot = _call_with_datasource_health_interface(
            _collect_governance_iface_snapshot
        )
        receipt_store = governance_iface_snapshot["receipt_store"]
        publish_gate = governance_iface_snapshot["publish_gate"]
        reject_reasons = governance_iface_snapshot["reject_reasons"]
        reject_severity = governance_iface_snapshot["reject_severity"]
        gate_sla_impact = governance_iface_snapshot["gate_sla_impact"]
        receipt_timeline = governance_iface_snapshot["receipt_timeline"]
        gate_trend = governance_iface_snapshot["gate_trend"]
        gate_trend_by_symbol = governance_iface_snapshot["gate_trend_by_symbol"]
        gate_trend_by_period = governance_iface_snapshot["gate_trend_by_period"]
        sla_threshold_panel = governance_iface_snapshot["sla_threshold_panel"]
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
    interval: str = Query(default="1d", description="图表周期，按 period registry 解析，支持基础/左对齐派生/自然日历周期"),
    start_date: str = Query(default="", description="开始日期，YYYY-MM-DD；留空按周期默认窗口"),
    end_date: str = Query(default="", description="结束日期，YYYY-MM-DD；留空默认今天"),
    start_datetime: str = Query(
        default="",
        description="精确起点，YYYY-MM-DD HH:MM:SS（交易所本地时间）；分钟/小时线翻页时优先于 start_date",
    ),
    end_datetime: str = Query(
        default="",
        description="精确终点，YYYY-MM-DD HH:MM:SS（交易所本地时间）；分钟/小时线翻页时优先于 end_date",
    ),
    adjust: str = Query(default="none", description="复权类型"),
    limit: int = Query(default=800, ge=1, le=10000, description="最多返回 bars 数量"),
) -> dict[str, Any]:
    """返回 Workbench 图表主舞台使用的 K 线 bars 与 Golden 1D 质量元数据。"""
    if adjust not in _CHART_ADJUST_OPTIONS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"adjust 参数非法，可选值: {sorted(_CHART_ADJUST_OPTIONS)}",
        )

    requested_interval = str(interval or "1d").strip() or "1d"
    resolved_period = _resolve_chart_period_definition(requested_interval)
    backend_period = _chart_runtime_code(resolved_period)

    # 解析精确 datetime 游标（分钟/小时级翻页专用）
    # 校验格式，提取日期部分供 _resolve_chart_request_window 使用
    _DT_FMT = "%Y-%m-%d %H:%M:%S"
    start_datetime_parsed: datetime | None = None
    end_datetime_parsed: datetime | None = None
    if start_datetime:
        try:
            start_datetime_parsed = datetime.strptime(start_datetime, _DT_FMT)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="start_datetime 必须为 YYYY-MM-DD HH:MM:SS 格式",
            ) from exc
        # datetime 游标优先；降级为 date-only 供 window resolver 使用
        start_date = start_datetime_parsed.strftime("%Y-%m-%d")
    if end_datetime:
        try:
            end_datetime_parsed = datetime.strptime(end_datetime, _DT_FMT)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="end_datetime 必须为 YYYY-MM-DD HH:MM:SS 格式",
            ) from exc
        end_date = end_datetime_parsed.strftime("%Y-%m-%d")

    requested_start_supplied = bool(start_date)
    requested_end_supplied = bool(end_date)
    try:
        start_at, end_at = _resolve_chart_request_window(resolved_period, start_date, end_date)
    except HTTPException as exc:
        if (
            exc.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            and requested_start_supplied
            and not requested_end_supplied
            and str(exc.detail) == "start_date 不能晚于 end_date"
        ):
            empty_end = datetime.now().strftime("%Y-%m-%d")
            quality = _build_chart_quality_payload(symbol)
            return {
                "symbol": symbol,
                "interval": requested_interval,
                "resolved_period": backend_period,
                "adjust": adjust,
                "start_date": start_date,
                "end_date": empty_end,
                "bar_count": 0,
                "bars": [],
                "quality": quality,
            }
        raise

    try:
        _side_diag: dict[str, Any] = {}

        def _load_chart_frame(iface: Any) -> tuple[Any, str, str]:
            chart_start = start_at
            chart_end = end_at
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
                        _diagnostics=_side_diag,
                    )
                return iface.get_stock_data(
                    stock_code=symbol,
                    start_date=window_start,
                    end_date=window_end,
                    period=backend_period,
                    adjust=adjust,
                    auto_save=False,
                    _diagnostics=_side_diag,
                )

            df_local = _load_frame(chart_start, chart_end)

            if df_local is None or (hasattr(df_local, "empty") and df_local.empty):
                date_range_getter = getattr(iface, "get_stock_date_range", None)
                if (
                    callable(date_range_getter)
                    and not requested_start_supplied
                    and not requested_end_supplied
                ):
                    available_window = date_range_getter(symbol, backend_period)
                    if available_window:
                        fallback_start, fallback_end = _resolve_chart_available_window(
                            resolved_period,
                            available_window[0],
                            available_window[1],
                        )
                        if (fallback_start, fallback_end) != (chart_start, chart_end):
                            chart_start, chart_end = fallback_start, fallback_end
                            df_local = _load_frame(chart_start, chart_end)
            return df_local, chart_start, chart_end

        df, start_at, end_at = _call_with_datasource_health_interface(_load_chart_frame)

        bars = _serialize_chart_bars(
            df,
            resolved_period,
            limit,
            start_datetime_parsed,
            end_datetime_parsed,
        )
        quality = _build_chart_quality_payload(symbol)

        # ── PR-1: 最小 diagnostics（feature flag 控制）──
        # ── PR-3: 深填 side-channel 7 字段 ──
        diagnostics: dict[str, Any] | None = None
        _diag_enabled = os.environ.get(
            "EASYXT_CHART_BARS_DIAGNOSTICS_ENABLED", "1",
        ) in ("1", "true", "True")
        if _diag_enabled and len(bars) == 0:
            _is_derived = backend_period not in (
                "1m", "5m", "15m", "30m", "60m", "1d", "tick",
            )
            # 计算 available_range
            _avail_range: dict[str, str | None] | None = None
            if df is not None and hasattr(df, "index") and not df.empty:
                try:
                    _idx = df.index
                    _avail_range = {
                        "start": str(_idx.min()),
                        "end": str(_idx.max()),
                    }
                except Exception:
                    pass
            diagnostics = {
                "empty_reason": "derived_period_no_data" if _is_derived else "no_data",
                "resolved_period": backend_period,
                "requested_window": {"start": start_at, "end": end_at},
                "source_period_ready": _side_diag.get("source_period_ready"),
                "gate_reason": _side_diag.get("gate_reason"),
                "builder_reason": _side_diag.get("builder_reason"),
                "cache_hit": _side_diag.get("cache_hit"),
                "build_attempted": _side_diag.get("build_attempted"),
                "listing_anchor_confidence": _side_diag.get("listing_anchor_confidence"),
                "available_range": _avail_range,
            }

        result: dict[str, Any] = {
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
        if diagnostics is not None:
            result["diagnostics"] = diagnostics
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"图表 bars 查询失败: {exc}",
        ) from exc


# ---------------------------------------------------------------------------
# 用户认证 (登录/登出/当前用户/用户管理)
# ---------------------------------------------------------------------------


class LoginBody(BaseModel):
    user_id: str
    password: str


class CreateUserBody(BaseModel):
    user_id: str
    password: str
    display_name: str = ""
    role: str = "user"
    permissions: list[str] = []


class ChangePasswordBody(BaseModel):
    old_password: str
    new_password: str


def _get_current_user(request: Request):
    """从请求头 Authorization: Bearer <token> 提取会话用户。"""
    from core.user_auth import get_user_manager
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        user = get_user_manager().validate_session(token)
        if user:
            return user
    # 开发模式下如果没有 Bearer token，回退到 admin
    if _DEV_MODE or _TEST_MODE:
        mgr = get_user_manager()
        admin = mgr.get_user("admin")
        if admin:
            return admin
    return None


def _require_user(request: Request):
    """FastAPI Depends: 要求已登录用户。"""
    user = _get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话已过期")
    return user


def _require_admin(request: Request):
    """FastAPI Depends: 要求 admin 角色。"""
    user = _require_user(request)
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


@app.post("/api/v1/auth/login", tags=["用户认证"])
def auth_login(body: LoginBody):
    """用户登录，返回会话令牌。"""
    from core.user_auth import get_user_manager
    session = get_user_manager().login(body.user_id, body.password)
    if not session:
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    user = get_user_manager().get_user(body.user_id)
    return {
        "token": session.token,
        "user_id": session.user_id,
        "expires_at": session.expires_at,
        "role": user.role if user else "user",
        "display_name": user.display_name if user else body.user_id,
    }


@app.post("/api/v1/auth/logout", tags=["用户认证"])
def auth_logout(request: Request):
    """注销当前会话。"""
    from core.user_auth import get_user_manager
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        get_user_manager().logout(token)
    return {"ok": True}


@app.get("/api/v1/auth/me", tags=["用户认证"])
def auth_me(request: Request):
    """获取当前登录用户信息。"""
    user = _get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话已过期")
    return {
        "user_id": user.user_id,
        "display_name": user.display_name,
        "role": user.role,
        "permissions": user.permissions,
        "last_login": user.last_login,
    }


@app.post("/api/v1/auth/change-password", tags=["用户认证"])
def auth_change_password(body: ChangePasswordBody, user=Depends(_require_user)):
    """修改当前用户密码。"""
    from core.user_auth import get_user_manager
    mgr = get_user_manager()
    # 验证旧密码
    session = mgr.login(user.user_id, body.old_password)
    if not session:
        raise HTTPException(status_code=400, detail="旧密码错误")
    mgr.update_password(user.user_id, body.new_password)
    return {"ok": True, "message": "密码已更新"}


@app.get(
    "/api/v1/users/",
    tags=["用户管理"],
    dependencies=[Depends(_require_admin)],
)
def list_users_api():
    """列出所有用户（仅管理员）。"""
    from core.user_auth import get_user_manager
    return get_user_manager().list_users()


@app.post(
    "/api/v1/users/",
    tags=["用户管理"],
    dependencies=[Depends(_require_admin)],
)
def create_user_api(body: CreateUserBody):
    """创建新用户（仅管理员）。"""
    from core.user_auth import get_user_manager
    try:
        user = get_user_manager().create_user(
            user_id=body.user_id,
            password=body.password,
            display_name=body.display_name,
            role=body.role,
            permissions=body.permissions,
        )
        return {"user_id": user.user_id, "role": user.role, "created": True}
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.delete(
    "/api/v1/users/{user_id}",
    tags=["用户管理"],
    dependencies=[Depends(_require_admin)],
)
def delete_user_api(user_id: str):
    """删除用户（仅管理员）。"""
    from core.user_auth import get_user_manager
    if not get_user_manager().delete_user(user_id):
        raise HTTPException(status_code=404, detail=f"用户 {user_id!r} 未找到")
    return {"user_id": user_id, "deleted": True}


# ---------------------------------------------------------------------------
# 券商多账户管理
# ---------------------------------------------------------------------------


class BrokerAccountBody(BaseModel):
    label: str
    broker: str
    qmt_exe_path: str = ""
    qmt_userdata_path: str = ""
    trade_account: str = ""
    display_account: str = ""
    trade_password: str = ""
    account_types: list[str] = ["STOCK"]
    is_default: bool = False
    notes: str = ""


class BrokerAccountPatchBody(BaseModel):
    label: str | None = None
    broker: str | None = None
    qmt_exe_path: str | None = None
    qmt_userdata_path: str | None = None
    trade_account: str | None = None
    display_account: str | None = None
    trade_password: str | None = None
    account_types: list[str] | None = None
    is_default: bool | None = None
    notes: str | None = None


@app.get("/api/v1/broker-accounts/", tags=["券商账户"])
def list_broker_accounts(request: Request, user=Depends(_require_user)):
    """列出当前用户的所有券商账户。"""
    from core.broker_accounts import get_broker_manager
    uid = user.user_id if user.role != "admin" else None  # admin 看所有
    return get_broker_manager().list_accounts(owner_user_id=uid)


@app.post("/api/v1/broker-accounts/", tags=["券商账户"])
def add_broker_account(body: BrokerAccountBody, user=Depends(_require_user)):
    """为当前用户添加券商账户。"""
    from core.broker_accounts import get_broker_manager
    acct = get_broker_manager().add_account(
        owner_user_id=user.user_id,
        label=body.label,
        broker=body.broker,
        qmt_exe_path=body.qmt_exe_path,
        qmt_userdata_path=body.qmt_userdata_path,
        trade_account=body.trade_account,
        display_account=body.display_account,
        trade_password=body.trade_password,
        account_types=body.account_types,
        is_default=body.is_default,
        notes=body.notes,
    )
    return acct.to_safe_dict()


@app.get("/api/v1/broker-accounts/{account_id}", tags=["券商账户"])
def get_broker_account(account_id: str, user=Depends(_require_user)):
    """获取单个券商账户详情。"""
    from core.broker_accounts import get_broker_manager
    acct = get_broker_manager().get_account(account_id)
    if not acct:
        raise HTTPException(status_code=404, detail="券商账户未找到")
    if user.role != "admin" and acct.owner_user_id != user.user_id:
        raise HTTPException(status_code=403, detail="无权访问此账户")
    return acct.to_safe_dict()


@app.patch("/api/v1/broker-accounts/{account_id}", tags=["券商账户"])
def update_broker_account(account_id: str, body: BrokerAccountPatchBody, user=Depends(_require_user)):
    """更新券商账户配置。"""
    from core.broker_accounts import get_broker_manager
    mgr = get_broker_manager()
    acct = mgr.get_account(account_id)
    if not acct:
        raise HTTPException(status_code=404, detail="券商账户未找到")
    if user.role != "admin" and acct.owner_user_id != user.user_id:
        raise HTTPException(status_code=403, detail="无权修改此账户")
    update_fields = body.model_dump(exclude_unset=True) if hasattr(body, "model_dump") else body.dict(exclude_unset=True)
    if not update_fields:
        raise HTTPException(status_code=400, detail="未提供可更新字段")
    mgr.update_account(account_id, **update_fields)
    return mgr.get_account(account_id).to_safe_dict()  # type: ignore[union-attr]


@app.delete("/api/v1/broker-accounts/{account_id}", tags=["券商账户"])
def delete_broker_account(account_id: str, user=Depends(_require_user)):
    """删除券商账户。"""
    from core.broker_accounts import get_broker_manager
    mgr = get_broker_manager()
    acct = mgr.get_account(account_id)
    if not acct:
        raise HTTPException(status_code=404, detail="券商账户未找到")
    if user.role != "admin" and acct.owner_user_id != user.user_id:
        raise HTTPException(status_code=403, detail="无权删除此账户")
    mgr.delete_account(account_id)
    return {"id": account_id, "deleted": True}


@app.delete("/api/v1/broker-accounts/{account_id}/password", tags=["券商账户"])
def clear_broker_account_password(account_id: str, user=Depends(_require_user)):
    """清除券商账户的已存密码，不删除账户本身。"""
    from core.broker_accounts import get_broker_manager
    mgr = get_broker_manager()
    acct = mgr.get_account(account_id)
    if not acct:
        raise HTTPException(status_code=404, detail="券商账户未找到")
    if user.role != "admin" and acct.owner_user_id != user.user_id:
        raise HTTPException(status_code=403, detail="无权修改此账户")
    mgr.delete_password(account_id)
    return {"id": account_id, "password_cleared": True}
# 数据源凭证管理（Tushare Token / TQSdk 账户密码）
# ---------------------------------------------------------------------------

class DatasourceCredentialsPatchBody(BaseModel):
    """PATCH 数据源凭证请求体。"""
    token: Optional[str] = None          # Tushare Token
    username: Optional[str] = None       # TQSdk 用户名
    password: Optional[str] = None       # TQSdk 密码


_DS_CRED_SUPPORTED = {"tushare", "tqsdk"}


@app.get("/api/v1/datasource/{source}/credentials", tags=["数据源凭证"])
def get_datasource_credentials(source: str, user=Depends(_require_admin)):
    """返回指定数据源的脱敏凭证概要。仅 admin 可访问。"""
    if source not in _DS_CRED_SUPPORTED:
        raise HTTPException(status_code=404, detail=f"不支持的数据源: {source}")
    from core.datasource_credentials import get_credentials_masked
    return {"source": source, **get_credentials_masked(source)}


@app.patch("/api/v1/datasource/{source}/credentials", tags=["数据源凭证"])
def patch_datasource_credentials(
    source: str, body: DatasourceCredentialsPatchBody, user=Depends(_require_admin)
):
    """设置或更新指定数据源的凭证。仅 admin 可操作。

    - tushare: 提供 token
    - tqsdk  : 提供 username 和/或 password
    """
    if source not in _DS_CRED_SUPPORTED:
        raise HTTPException(status_code=404, detail=f"不支持的数据源: {source}")
    from core.datasource_credentials import set_credentials, get_credentials_masked
    fields: dict[str, str] = {}
    if source == "tushare":
        if body.token is not None:
            fields["token"] = body.token
    elif source == "tqsdk":
        if body.username is not None:
            fields["username"] = body.username
        if body.password is not None:
            fields["password"] = body.password
    if not fields:
        raise HTTPException(status_code=422, detail="未提供任何有效凭证字段")
    set_credentials(source, **fields)
    return {"source": source, "updated": True, **get_credentials_masked(source)}


@app.delete("/api/v1/datasource/{source}/credentials", tags=["数据源凭证"])
def delete_datasource_credentials(source: str, user=Depends(_require_admin)):
    """清除指定数据源的所有已保存凭证并从运行时环境变量中移除。仅 admin 可操作。"""
    if source not in _DS_CRED_SUPPORTED:
        raise HTTPException(status_code=404, detail=f"不支持的数据源: {source}")
    from core.datasource_credentials import clear_credentials
    clear_credentials(source)
    return {"source": source, "cleared": True}


# ---------------------------------------------------------------------------

@app.get(
    "/api/v1/accounts/",
    tags=["账户管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_accounts_api() -> list[dict[str, Any]]:
    """列出全部已注册账户。"""
    from core.account_registry import account_registry

    return account_registry.list_accounts()


@app.post(
    "/api/v1/accounts/",
    tags=["账户管理"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def register_account_api(body: AccountRegisterBody) -> dict[str, Any]:
    """注册/更新账户（按 account_id 幂等 upsert）。"""
    from core.account_registry import account_registry

    payload = body.model_dump() if hasattr(body, "model_dump") else body.dict()
    try:
        return account_registry.register_account(payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc


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
# 交易执行（QMT TradeAPI）
# ---------------------------------------------------------------------------

_trade_api_instances: dict[str, Any] = {}
_trade_api_lock = threading.Lock()
_trade_runtime_thread: "_TradeRuntimeThread | None" = None

# 成交台账（启动时初始化，懒加载）
_trade_ledger: Any = None  # TradeLedger instance
_audit_trail_store: Any = None  # AuditTrail instance
_position_tracker_store: Any = None  # PositionTracker instance


class _TradeRuntimeThread:
    """将 xtquant 交易调用固定到单一后台线程，规避线程池/事件循环上下文附着不稳定。"""

    def __init__(self) -> None:
        self._queue: "queue.Queue[tuple[Callable[..., Any] | None, tuple[Any, ...], dict[str, Any], concurrent.futures.Future[Any] | None]]" = queue.Queue()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name="easyxt-trade-runtime", daemon=True)
        self._owner_thread_id: int | None = None

    def start(self) -> None:
        if self._thread.is_alive():
            return
        self._thread.start()
        if not self._ready.wait(timeout=5.0):
            raise RuntimeError("XT 交易专用线程启动超时")

    def stop(self, timeout: float = 2.0) -> None:
        if not self._thread.is_alive():
            return
        self._queue.put((None, (), {}, None))
        self._thread.join(timeout=timeout)

    def call(self, fn: Callable[..., Any], *args: Any, timeout: float = 20.0, **kwargs: Any) -> Any:
        self.start()
        if self._owner_thread_id == threading.get_ident():
            return fn(*args, **kwargs)
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        self._queue.put((fn, args, kwargs, future))
        return future.result(timeout=timeout)

    def _run(self) -> None:
        self._owner_thread_id = threading.get_ident()
        self._ready.set()
        while True:
            fn, args, kwargs, future = self._queue.get()
            if fn is None:
                break
            try:
                result = fn(*args, **kwargs)
            except Exception as exc:
                if future is not None and not future.cancelled():
                    future.set_exception(exc)
            else:
                if future is not None and not future.cancelled():
                    future.set_result(result)


def _get_trade_runtime_thread() -> _TradeRuntimeThread:
    global _trade_runtime_thread
    if _trade_runtime_thread is None:
        _trade_runtime_thread = _TradeRuntimeThread()
    _trade_runtime_thread.start()
    return _trade_runtime_thread


def _run_on_trade_runtime_thread(
    fn: Callable[..., Any],
    *args: Any,
    timeout: float = 20.0,
    **kwargs: Any,
) -> Any:
    try:
        return _get_trade_runtime_thread().call(fn, *args, timeout=timeout, **kwargs)
    except concurrent.futures.TimeoutError:
        fn_name = getattr(fn, "__name__", repr(fn))
        log.warning("XT 交易专用线程调用超时: fn=%s timeout=%.1fs", fn_name, timeout)
        raise


def _call_trade_api(api: Any, method_name: str, *args: Any, timeout: float = 20.0, **kwargs: Any) -> Any:
    def _invoke() -> Any:
        return getattr(api, method_name)(*args, **kwargs)

    return _run_on_trade_runtime_thread(_invoke, timeout=timeout)


def _get_or_init_trade_ledger() -> Any:
    """获取（并在首次调用时初始化）成交台账实例。"""
    global _trade_ledger
    if _trade_ledger is None:
        try:
            from core.trade_ledger import TradeLedger

            _trade_ledger = TradeLedger()
            _trade_ledger.ensure_tables()
            log.info("成交台账（trade_ledger）已初始化")
        except Exception:
            log.exception("成交台账初始化失败，历史持久化不可用")
    return _trade_ledger


def _get_or_init_audit_trail() -> Any:
    """获取（并在首次调用时初始化）审计链实例。"""
    global _audit_trail_store
    if _audit_trail_store is None:
        try:
            from core.audit_trail import AuditTrail

            _audit_trail_store = AuditTrail()
            log.info("审计链（audit_trail）已初始化")
        except Exception:
            log.exception("审计链初始化失败，signal/order/fill 查询不可用")
    return _audit_trail_store


def _get_or_init_position_tracker() -> Any:
    """获取（并在首次调用时初始化）PositionTracker 实例。"""
    global _position_tracker_store
    if _position_tracker_store is None:
        try:
            from core.position_tracker import PositionTracker

            _position_tracker_store = PositionTracker()
            restored = _position_tracker_store.rehydrate_open_positions()
            log.info("PositionTracker 已初始化，rehydrate open=%d", restored)
        except Exception:
            log.exception("PositionTracker 初始化失败，position_lifecycle 读模型不可用")
    return _position_tracker_store


def _normalize_userdata_path(userdata_path: str) -> str:
    return os.path.normcase(os.path.normpath(str(userdata_path or "").strip()))


def _resolve_trade_userdata(account_id: str | None = None) -> tuple[str, str]:
    """按交易账号优先从 broker_accounts 解析 userdata_path，失败时回退旧 config。"""
    broker_account_type = "STOCK"
    if account_id:
        try:
            from core.broker_accounts import get_broker_manager

            broker_account = get_broker_manager().find_account_by_trade_account(account_id)
            if broker_account and broker_account.is_active and broker_account.qmt_userdata_path:
                account_types = [str(t).upper() for t in broker_account.account_types]
                if "STOCK" in account_types:
                    broker_account_type = "STOCK"
                elif "CREDIT" in account_types:
                    broker_account_type = "CREDIT"
                elif account_types:
                    broker_account_type = account_types[0]
                return broker_account.qmt_userdata_path, broker_account_type
        except Exception as exc:
            log.warning("broker_accounts 解析交易账户失败(%s): %s", account_id, exc)

    from easy_xt.config import config

    userdata = str(config.get("trade.userdata_path") or "").strip()
    return userdata, broker_account_type


def _ensure_trade_account(api: Any, account_id: str, account_type: str) -> bool:
    if not account_id:
        return True
    existing_accounts = getattr(api, "accounts", {}) or {}
    if account_id in existing_accounts:
        return True
    try:
        return bool(api.add_account(account_id, account_type=account_type))
    except Exception as exc:
        log.warning("TradeAPI 添加账户失败(%s/%s): %s", account_id, account_type, exc)
        return False


def _get_cached_trade_api(account_id: str | None = None) -> tuple[Any | None, str, str]:
    """只读取缓存中的 TradeAPI 实例，不触发任何自动连接。"""
    userdata, account_type = _resolve_trade_userdata(account_id)
    if not userdata:
        return None, "", account_type

    cache_key = _normalize_userdata_path(userdata)
    with _trade_api_lock:
        api = _trade_api_instances.get(cache_key)
    return api, userdata, account_type


def _get_trade_account_connection_snapshot(account_id: str | None) -> tuple[str, str | None, str]:
    """返回账户连接快照：仅查看缓存状态，不隐式连接 xttrader。"""
    normalized_account_id = str(account_id or "").strip()
    api, userdata, _account_type = _get_cached_trade_api(normalized_account_id)
    if not userdata:
        return "disconnected", "未配置 QMT userdata 路径", ""
    if api is None:
        return "disconnected", "尚未显式连接交易服务", userdata

    existing_accounts = getattr(api, "accounts", {}) or {}
    if not normalized_account_id or normalized_account_id in existing_accounts:
        return "connected", None, userdata
    return "disconnected", "TradeAPI 已连接，但该账户尚未订阅", userdata


def _run_on_server_loop_thread(
    fn: Callable[..., Any],
    *args: Any,
    timeout: float = 20.0,
    **kwargs: Any,
) -> Any:
    """将 XT 交易相关阻塞调用固定派发到 API 主线程，规避 FastAPI 线程池上下文附着失败。"""
    return _run_on_trade_runtime_thread(fn, *args, timeout=timeout, **kwargs)


def _get_trade_api_impl(account_id: str | None = None):
    """按 broker_account/userdata_path 获取 TradeAPI 实例；未连接 QMT 时返回 None。"""
    userdata, account_type = _resolve_trade_userdata(account_id)
    if not userdata:
        log.warning("TradeAPI 未能解析 userdata_path(account_id=%s)", account_id)
        return None

    cache_key = _normalize_userdata_path(userdata)
    with _trade_api_lock:
        api = _trade_api_instances.get(cache_key)
        if api is None:
            try:
                from easy_xt.trade_api import TradeAPI

                api = TradeAPI()
                if not api.connect(userdata):
                    log.warning("TradeAPI 未能自动连接（userdata=%s）", userdata)
                    return None
                _trade_api_instances[cache_key] = api
                log.info("TradeAPI 自动连接成功: %s", userdata)
                # 注入成交台账到回调（实现 on_stock_trade 持久化）
                ledger = _get_or_init_trade_ledger()
                if ledger is not None and api.callback is not None:
                    api.callback._ledger = ledger
                # 注入审计链路（委托提交/成交 → AuditTrail）
                try:
                    audit = _get_or_init_audit_trail()
                    if audit is not None:
                        api.attach_audit_trail(audit)
                        if api.callback is not None:
                            api.callback._audit_trail = audit
                        log.info("AuditTrail 已注入 TradeAPI（userdata=%s）", userdata)
                except Exception:
                    log.exception("AuditTrail 注入失败，审计写入不可用")
                try:
                    tracker = _get_or_init_position_tracker()
                    if tracker is not None and api.callback is not None:
                        api.callback._position_tracker = tracker
                        log.info("PositionTracker 已注入 TradeAPI（userdata=%s）", userdata)
                except Exception:
                    log.exception("PositionTracker 注入失败，仓位生命周期写入不可用")
            except Exception as exc:
                log.warning("TradeAPI 初始化失败(%s): %s", userdata, exc)
                return None

        if account_id and not _ensure_trade_account(api, account_id, account_type):
            return None
        return api


def _get_trade_api(account_id: str | None = None):
    # QMT 首次附着可能较慢（实测可超过 30s），避免在成功前被服务层误判超时。
    return _run_on_trade_runtime_thread(_get_trade_api_impl, account_id, timeout=120.0)


def _load_xt_trader_class() -> type[Any]:
    """动态加载 XtQuantTrader，避免 API 层静态依赖 xtquant。"""
    from core.xtquant_import import import_xttrader_module

    xttrader = import_xttrader_module()
    return cast(type[Any], xttrader.XtQuantTrader)


def _probe_trading_accounts_impl(paths: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """直接探测 QMT 已登录账号，不依赖 TradeAPI 缓存。"""
    import time as _time

    XtQuantTrader = _load_xt_trader_class()
    discovered: list[dict[str, Any]] = []
    errors: list[str] = []
    account_type_map = {
        1: "FUTURE",
        2: "STOCK",
        3: "CREDIT",
        5: "FUTURE_OPTION",
        6: "STOCK_OPTION",
        7: "HUGANGTONG",
        11: "SHENGANGTONG",
    }

    seen_paths: set[str] = set()
    for raw_path in paths:
        ud_path = str(raw_path or "").strip()
        if not ud_path:
            continue
        normalized = _normalize_userdata_path(ud_path)
        if normalized in seen_paths:
            continue
        seen_paths.add(normalized)
        if not os.path.exists(ud_path):
            errors.append(f"{ud_path}: 路径不存在")
            continue
        trader = None
        try:
            sess = int(_time.time() * 1000) % 999000 + 1000
            trader = XtQuantTrader(ud_path, sess)
            trader.start()
            conn_result = trader.connect()
            if conn_result != 0:
                errors.append(f"{ud_path}: connect 返回 {conn_result}（QMT 未启动或当前进程无法附着）")
                continue
            infos = trader.query_account_infos() or []
            for info in infos:
                acct_id = str(getattr(info, "account_id", "") or "")
                acct_type_int = int(getattr(info, "account_type", 0) or 0)
                acct_type_str = account_type_map.get(acct_type_int, str(acct_type_int))
                raw_login_status = getattr(info, "login_status", -1)
                login_status = -1 if raw_login_status is None else int(raw_login_status)
                broker_id = str(getattr(info, "broker_id", "") or "")
                discovered.append(
                    {
                        "userdata_path": ud_path,
                        "account_id": acct_id,
                        "account_type": acct_type_str,
                        "account_type_int": acct_type_int,
                        "broker_id": broker_id,
                        "login_status": login_status,
                    }
                )
        except Exception as exc:
            errors.append(f"{ud_path}: {exc}")
        finally:
            if trader is not None:
                try:
                    trader.stop()
                except Exception:
                    pass
    return discovered, errors


def _probe_trading_accounts(paths: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    return _run_on_trade_runtime_thread(_probe_trading_accounts_impl, paths, timeout=120.0)


def _require_trade_api(account_id: str | None = None, *, allow_autoconnect: bool = True):
    """获取 TradeAPI 或抛 503。默认允许显式业务动作自动连接；只读接口应关闭 autoconnect。"""
    normalized_account_id = str(account_id or "").strip()
    if allow_autoconnect:
        api = _get_trade_api(normalized_account_id or None)
    else:
        api, _userdata, _account_type = _get_cached_trade_api(normalized_account_id or None)
        if api is not None and normalized_account_id:
            existing_accounts = getattr(api, "accounts", {}) or {}
            if normalized_account_id not in existing_accounts:
                api = None
    if api is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "交易服务未连接（尚未显式连接交易服务、QMT 未启动、userdata 路径未配置，"
                "或交易账户未成功订阅）"
            ),
        )
    return api


def _mark_broker_account_connected(account_id: str | None) -> None:
    normalized_account_id = str(account_id or "").strip()
    if not normalized_account_id:
        return
    try:
        from core.broker_accounts import get_broker_manager

        broker_mgr = get_broker_manager()
        acct = broker_mgr.find_account_by_trade_account(normalized_account_id)
        if acct is not None:
            broker_mgr.update_account(acct.id, last_connected=time.time())
    except Exception:
        log.exception("回写 broker 账户连接时间失败 account_id=%s", normalized_account_id)


def _connect_trading_account_internal(account_id: str) -> tuple[dict[str, Any] | None, str | None]:
    """连接并订阅指定交易账户；成功返回 payload，失败返回 detail。"""
    normalized_account_id = str(account_id or "").strip()
    userdata, _account_type = _resolve_trade_userdata(normalized_account_id)
    if not userdata:
        return None, "未配置 QMT userdata 路径，无法建立交易连接"

    api = _get_trade_api(normalized_account_id)
    if api is None:
        discovered_accounts, probe_errors = _probe_trading_accounts([userdata])
        matched = [
            item for item in discovered_accounts
            if str(item.get("account_id") or "").strip() == normalized_account_id
        ]
        if matched:
            broker_id = str(matched[0].get("broker_id") or "").strip()
            detail = (
                f"QMT 已登录资金账号 {normalized_account_id}"
                + (f"（broker_id={broker_id}）" if broker_id else "")
                + "，但中台当前进程尚未成功附着 TradeAPI；"
                  "账户发现正常，请继续排查同进程交易连接链路"
            )
        else:
            detail = "交易服务连接失败（QMT 未启动、账户未登录，或账户订阅失败）"
            if probe_errors:
                detail = f"{detail}；最近探测: {probe_errors[0]}"
        return None, detail

    _mark_broker_account_connected(normalized_account_id)
    return {
        "account_id": normalized_account_id,
        "status": "connected",
        "userdata_path": userdata,
        "message": "TradeAPI 已连接并订阅账户",
    }, None


async def _auto_restore_trade_accounts_after_startup(
    delay_s: float = 2.0,
    retry_delays: tuple[float, ...] | None = None,
) -> None:
    """服务启动后后台恢复可连接的 QMT 账户，不阻塞健康检查。"""
    try:
        if delay_s > 0:
            await asyncio.sleep(delay_s)
        if not _trade_autorestore_enabled():
            return
        from core.broker_accounts import get_broker_manager

        broker_mgr = get_broker_manager()
        candidates = []
        for acct in broker_mgr.list_account_objects(owner_user_id=None):
            trade_account = str(getattr(acct, "trade_account", "") or "").strip()
            userdata_path = _normalize_userdata_path(getattr(acct, "qmt_userdata_path", "") or "")
            if not getattr(acct, "is_active", False):
                continue
            if not trade_account or not userdata_path:
                continue
            status_text, _hint, _userdata = _get_trade_account_connection_snapshot(trade_account)
            if status_text == "connected":
                continue
            candidates.append(trade_account)

        if not candidates:
            log.info("启动期交易账户恢复：无候选账户")
            return

        retry_plan = retry_delays if retry_delays is not None else _trade_autorestore_retry_delays()
        pending_accounts = list(dict.fromkeys(candidates))
        restored = 0
        final_failures: dict[str, str | None] = {}
        rounds_run = 0

        for round_index, wait_s in enumerate(retry_plan, start=1):
            if not pending_accounts:
                break
            if round_index > 1 and wait_s > 0:
                log.info(
                    "启动期交易账户恢复：等待下一轮 round=%d pending=%d wait_s=%.1f",
                    round_index,
                    len(pending_accounts),
                    wait_s,
                )
                await asyncio.sleep(wait_s)

            rounds_run = round_index
            log.info(
                "启动期交易账户恢复：开始 round=%d pending=%d",
                round_index,
                len(pending_accounts),
            )
            next_pending: list[str] = []
            for account_id in pending_accounts:
                payload, detail = await asyncio.to_thread(_connect_trading_account_internal, account_id)
                if payload is not None:
                    restored += 1
                    final_failures.pop(account_id, None)
                    log.info(
                        "启动期交易账户恢复成功 round=%d account_id=%s userdata=%s",
                        round_index,
                        account_id,
                        payload.get("userdata_path"),
                    )
                else:
                    next_pending.append(account_id)
                    final_failures[account_id] = detail
                    log.warning(
                        "启动期交易账户恢复失败 round=%d account_id=%s detail=%s",
                        round_index,
                        account_id,
                        detail,
                    )
            pending_accounts = next_pending

        failed = len(pending_accounts)
        log.info(
            "启动期交易账户恢复完成 restored=%d failed=%d rounds=%d",
            restored,
            failed,
            rounds_run,
        )
        if pending_accounts:
            log.warning("启动期交易账户恢复仍失败 accounts=%s", [
                {
                    "account_id": account_id,
                    "detail": final_failures.get(account_id),
                }
                for account_id in pending_accounts
            ])
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("启动期交易账户恢复任务异常")


def _serialize_trading_audit_chain(signal_id: str, chain: Any, account_id: str | None = None) -> dict[str, Any]:
    """将 AuditChain 转为稳定的 JSON 结构，并按账户做轻量过滤。"""
    requested_account = str(account_id or "").strip()
    raw_signal = getattr(chain, "signal", None)

    signal_payload: dict[str, Any] | None = None
    if raw_signal is not None:
        signal_account = str(getattr(raw_signal, "account_id", "") or "")
        if not requested_account or signal_account in ("", requested_account):
            signal_payload = {
                "signal_id": str(getattr(raw_signal, "signal_id", "") or ""),
                "strategy_id": str(getattr(raw_signal, "strategy_id", "") or ""),
                "code": str(getattr(raw_signal, "code", "") or ""),
                "direction": str(getattr(raw_signal, "direction", "") or ""),
                "price_hint": getattr(raw_signal, "price_hint", None),
                "volume_hint": getattr(raw_signal, "volume_hint", None),
                "created_at": int(getattr(raw_signal, "created_at", 0) or 0),
                "account_id": signal_account,
            }

    orders: list[dict[str, Any]] = []
    allowed_order_ids: set[str] = set()
    for order in getattr(chain, "orders", []) or []:
        order_account = str(getattr(order, "account_id", "") or "")
        if requested_account and order_account not in ("", requested_account):
            continue
        order_id = str(getattr(order, "order_id", "") or "")
        allowed_order_ids.add(order_id)
        orders.append(
            {
                "order_id": order_id,
                "signal_id": str(getattr(order, "signal_id", "") or ""),
                "code": str(getattr(order, "code", "") or ""),
                "direction": str(getattr(order, "direction", "") or ""),
                "volume": getattr(order, "volume", None),
                "price": getattr(order, "price", None),
                "submitted_at": int(getattr(order, "submitted_at", 0) or 0),
                "status": str(getattr(order, "status", "") or ""),
                "account_id": order_account,
            }
        )

    fills: list[dict[str, Any]] = []
    for fill in getattr(chain, "fills", []) or []:
        fill_account = str(getattr(fill, "account_id", "") or "")
        order_id = str(getattr(fill, "order_id", "") or "")
        if requested_account and fill_account not in ("", requested_account):
            continue
        if allowed_order_ids and order_id not in allowed_order_ids:
            continue
        fills.append(
            {
                "fill_id": str(getattr(fill, "fill_id", "") or ""),
                "order_id": order_id,
                "filled_at": int(getattr(fill, "filled_at", 0) or 0),
                "filled_price": getattr(fill, "filled_price", None),
                "filled_volume": getattr(fill, "filled_volume", None),
                "pnl_snapshot": getattr(fill, "pnl_snapshot", None),
                "account_id": fill_account,
            }
        )

    return {
        "signal_id": signal_id,
        "signal": signal_payload,
        "orders": orders,
        "fills": fills,
        "server_time": int(time.time() * 1000),
        "build_version": _BUILD_VERSION,
        "commit_sha": _COMMIT_SHA,
    }


def _serialize_position_lifecycle(lc: Any) -> dict[str, Any]:
    """将 PositionLifecycle 转为前端稳定 JSON。"""
    linked_signal_ids = list(dict.fromkeys([*(lc.entry_signal_ids or []), *(lc.exit_signal_ids or [])]))
    linked_order_ids = list(dict.fromkeys([*(lc.entry_order_ids or []), *(lc.exit_order_ids or [])]))
    return {
        "position_id": lc.position_id,
        "account_id": lc.account_id,
        "code": lc.code,
        "direction": lc.direction,
        "status": lc.status,
        "execution_mode": lc.execution_mode,
        "strategy_id": lc.strategy_id,
        "strategy_run_id": lc.strategy_run_id,
        "matching_policy": lc.matching_policy,
        "entry_avg_price": lc.entry_avg_price,
        "entry_total_qty": lc.entry_total_qty,
        "entry_total_amount": lc.entry_total_amount,
        "entry_commission": lc.entry_commission,
        "first_entry_at": lc.first_entry_at,
        "last_entry_at": lc.last_entry_at,
        "peak_qty": lc.peak_qty,
        "remaining_qty": lc.remaining_qty,
        "exit_avg_price": lc.exit_avg_price,
        "exit_total_qty": lc.exit_total_qty,
        "exit_total_amount": lc.exit_total_amount,
        "exit_commission": lc.exit_commission,
        "exit_stamp_duty": lc.exit_stamp_duty,
        "first_exit_at": lc.first_exit_at,
        "last_exit_at": lc.last_exit_at,
        "realized_pnl": lc.realized_pnl,
        "net_pnl": lc.net_pnl,
        "max_unrealized_pnl": lc.max_unrealized_pnl,
        "max_drawdown_pnl": lc.max_drawdown_pnl,
        "hold_bars": lc.hold_bars,
        "hold_calendar_days": lc.hold_calendar_days,
        "close_reason": lc.close_reason,
        "entry_signal_ids": lc.entry_signal_ids,
        "exit_signal_ids": lc.exit_signal_ids,
        "entry_fill_ids": lc.entry_fill_ids,
        "exit_fill_ids": lc.exit_fill_ids,
        "entry_order_ids": lc.entry_order_ids,
        "exit_order_ids": lc.exit_order_ids,
        "linked_signal_ids": linked_signal_ids,
        "linked_order_ids": linked_order_ids,
        "created_at": lc.created_at,
        "closed_at": lc.closed_at,
    }


def _serialize_execution_order(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "order_id": str(row.get("order_id") or ""),
        "signal_id": str(row.get("signal_id") or ""),
        "strategy_id": str(row.get("strategy_id") or ""),
        "account_id": str(row.get("account_id") or ""),
        "code": str(row.get("code") or ""),
        "direction": str(row.get("direction") or ""),
        "volume": float(row.get("volume") or 0),
        "price": float(row.get("price") or 0),
        "submitted_at": int(row.get("submitted_at") or 0),
        "status": str(row.get("status") or ""),
        "is_active": bool(row.get("is_active")),
        "is_terminal": bool(row.get("is_terminal")),
    }


def _normalize_trading_order_direction(raw_order_type: Any, fallback: str = "") -> str:
    for candidate in (raw_order_type, fallback):
        text = str(candidate or "").strip().lower()
        if not text:
            continue
        if text in {"buy", "long"} or "买" in text:
            return "buy"
        if text in {"sell", "short"} or "卖" in text:
            return "sell"
    return ""


def _find_cancelable_order_snapshot(api: Any, account_id: str, target_order_id: int) -> dict[str, Any] | None:
    try:
        orders = _call_trade_api(api, "get_orders", account_id, cancelable_only=True, timeout=20.0)
    except Exception:
        log.exception("查询可撤委托失败 account_id=%s order_id=%s", account_id, target_order_id)
        return None
    if orders is None or getattr(orders, "empty", True):
        return None
    for _, row in orders.iterrows():
        try:
            raw_order_id = int(float(row.get("order_id", 0) or 0))
        except Exception:
            continue
        if raw_order_id != int(target_order_id):
            continue
        total_volume = int(float(row.get("volume", 0) or 0))
        traded_volume = int(float(row.get("traded_volume", 0) or 0))
        price = float(row.get("price", 0) or 0)
        status = str(row.get("status") or "")
        direction = _normalize_trading_order_direction(row.get("order_type"))
        return {
            "order_id": raw_order_id,
            "code": str(row.get("code") or "").strip(),
            "direction": direction,
            "volume": total_volume,
            "traded_volume": traded_volume,
            "remaining_volume": max(0, total_volume - traded_volume),
            "price": price,
            "status": status,
            "price_type": "limit" if price > 0 else "market",
        }
    return None


@app.get(
    "/api/v1/trading/status",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_trading_status() -> dict[str, Any]:
    """返回所有已配置券商账户的缓存连接状态（无副作用，不主动连接 QMT）。"""
    from core.broker_accounts import get_broker_manager

    broker_mgr = get_broker_manager()
    accounts = broker_mgr.list_account_objects(owner_user_id=None)
    results: list[dict[str, Any]] = []
    for acct in accounts:
        display_account = str(getattr(acct, "display_account", "") or "")
        if not acct.is_active:
            results.append({
                "id": acct.id,
                "label": acct.label,
                "trade_account": acct.trade_account,
                "display_account": display_account,
                "status": "disabled",
            })
            continue
        status_text, hint, _userdata = _get_trade_account_connection_snapshot(acct.trade_account)
        if status_text != "connected":
            results.append({
                "id": acct.id,
                "label": acct.label,
                "trade_account": acct.trade_account,
                "display_account": display_account,
                "status": "disconnected",
                "hint": hint or "QMT 未启动或账户未登录",
            })
        else:
            results.append({
                "id": acct.id,
                "label": acct.label,
                "trade_account": acct.trade_account,
                "display_account": display_account,
                "status": "connected",
            })
    return {"accounts": results}


@app.post(
    "/api/v1/trading/accounts/{account_id}/connect",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def connect_trading_account(account_id: str) -> dict[str, Any]:
    """显式连接 TradeAPI 并订阅指定账户。"""
    payload, detail = _connect_trading_account_internal(account_id)
    if payload is not None:
        return payload
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=detail or "交易服务连接失败",
    )


@app.post(
    "/api/v1/trading/discover-accounts",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def discover_trading_accounts(body: dict[str, Any] = None) -> dict[str, Any]:
    """连接指定 QMT userdata 路径，返回 QMT 后台已登录的真实资金账号列表。

    body: {"userdata_path": "D:\\broker\\userdata_mini"}
    如不传则遍历所有已配置 broker 账户的 userdata 路径。
    """
    paths: list[str] = []
    if body and body.get("userdata_path"):
        paths = [str(body["userdata_path"])]
    else:
        from core.broker_accounts import get_broker_manager
        seen: set[str] = set()
        for acct in get_broker_manager().list_account_objects():
            ud = _normalize_userdata_path(acct.qmt_userdata_path or "")
            if ud and ud not in seen:
                seen.add(ud)
                paths.append(acct.qmt_userdata_path)
    discovered, errors = _probe_trading_accounts(paths)
    return {"discovered": discovered, "errors": errors}


@app.get(
    "/api/v1/trading/accounts/{account_id}/asset",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_trading_account_asset(account_id: str) -> dict:
    """查询账户资产（可用资金/持仓市值/总资产）。"""
    api = _require_trade_api(account_id, allow_autoconnect=False)
    result = _call_trade_api(api, "get_account_asset", account_id, timeout=20.0)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"无法获取账户 {account_id!r} 资产（账户未添加或查询失败）",
        )
    return result


@app.get(
    "/api/v1/trading/positions",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_trading_positions(
    account_id: str,
    code: str | None = None,
) -> list[dict[str, Any]]:
    """查询持仓列表。"""
    api = _require_trade_api(account_id, allow_autoconnect=False)
    df = _call_trade_api(api, "get_positions", account_id, code=code, timeout=20.0)
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


@app.get(
    "/api/v1/trading/position-lifecycle",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_trading_position_lifecycle(
    account_id: str,
    status_filter: str = Query(default="open", alias="status"),
    code: str | None = None,
    execution_mode: str = Query(default="live"),
    strategy_id: str | None = None,
    limit: int = Query(default=200, ge=1, le=2000),
) -> dict[str, Any]:
    """查询统一仓位生命周期读模型（当前仓位 / 历史仓位）。"""
    if status_filter not in {"open", "closed", "all"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="status 必须为 'open' / 'closed' / 'all'",
        )

    tracker = _get_or_init_position_tracker()
    if tracker is None:
        return {
            "items": [],
            "total": 0,
            "filters": {
                "account_id": account_id,
                "status": status_filter,
                "code": code or "",
                "execution_mode": execution_mode,
                "strategy_id": strategy_id or "",
                "limit": limit,
            },
            "server_time": int(time.time() * 1000),
        }

    items: list[Any] = []
    if status_filter in {"open", "all"}:
        items.extend(
            tracker.get_open(
                account_id=account_id,
                code=code,
                execution_mode=execution_mode,
                strategy_id=strategy_id,
            )
        )
    if status_filter in {"closed", "all"}:
        items.extend(
            tracker.get_closed(
                account_id=account_id,
                code=code,
                execution_mode=execution_mode,
                strategy_id=strategy_id,
                limit=limit,
            )
        )

    if status_filter == "all":
        items = sorted(
            items,
            key=lambda item: max(
                int(getattr(item, "closed_at", 0) or 0),
                int(getattr(item, "first_entry_at", 0) or 0),
            ),
            reverse=True,
        )[:limit]

    payload_items = [_serialize_position_lifecycle(item) for item in items[:limit]]
    return {
        "items": payload_items,
        "total": len(payload_items),
        "filters": {
            "account_id": account_id,
            "status": status_filter,
            "code": code or "",
            "execution_mode": execution_mode,
            "strategy_id": strategy_id or "",
            "limit": limit,
        },
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/trading/execution-orders",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_trading_execution_orders(
    account_id: str,
    status_filter: str = Query(default="active", alias="status"),
    code: str = "",
    limit: int = Query(default=200, ge=1, le=2000),
) -> dict[str, Any]:
    """查询统一委托读模型（当前委托 / 历史委托）。"""
    if status_filter not in {"active", "terminal", "all"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="status 必须为 'active' / 'terminal' / 'all'",
        )

    audit = _get_or_init_audit_trail()
    if audit is None:
        return {
            "items": [],
            "total": 0,
            "filters": {
                "account_id": account_id,
                "status": status_filter,
                "code": code,
                "limit": limit,
            },
            "server_time": int(time.time() * 1000),
        }

    rows = audit.list_execution_orders(
        account_id=account_id,
        status_filter=status_filter,
        code=code,
        limit=limit,
    )
    return {
        "items": [_serialize_execution_order(row) for row in rows],
        "total": len(rows),
        "filters": {
            "account_id": account_id,
            "status": status_filter,
            "code": code,
            "limit": limit,
        },
        "server_time": int(time.time() * 1000),
    }


@app.get(
    "/api/v1/trading/orders",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_trading_orders(
    account_id: str,
    cancelable_only: bool = False,
) -> list[dict[str, Any]]:
    """查询委托列表。"""
    api = _require_trade_api(account_id, allow_autoconnect=False)
    df = _call_trade_api(api, "get_orders", account_id, cancelable_only=cancelable_only, timeout=20.0)
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


@app.get(
    "/api/v1/trading/trades",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_trading_trades(account_id: str) -> list[dict[str, Any]]:
    """查询成交列表。"""
    api = _require_trade_api(account_id, allow_autoconnect=False)
    df = _call_trade_api(api, "get_trades", account_id, timeout=20.0)
    if df is None or df.empty:
        return []
    # 顺便同步今日成交到持久台账
    ledger = _get_or_init_trade_ledger()
    if ledger is not None:
        try:
            ledger.sync_from_df(df, account_id)
        except Exception:
            log.exception("同步今日成交至台账失败 account_id=%s", account_id)
    return df.to_dict(orient="records")


@app.get(
    "/api/v1/trading/ledger",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_trading_ledger(
    account_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    code: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    """查询历史成交台账（跨日持久化数据）。"""
    ledger = _get_or_init_trade_ledger()
    if ledger is None:
        return {"records": [], "total": 0}
    records = ledger.query(account_id, start_date, end_date, code, limit, offset)
    total = ledger.count(account_id, start_date, end_date, code)
    return {"records": records, "total": total}


@app.get(
    "/api/v1/trading/audit-chain/{signal_id}",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def get_trading_audit_chain(signal_id: str, account_id: str | None = None) -> dict[str, Any]:
    """按 signal_id 查询 signal → order → fill 审计链。"""
    audit = _get_or_init_audit_trail()
    if audit is None:
        return _serialize_trading_audit_chain(signal_id, type("EmptyChain", (), {"signal": None, "orders": [], "fills": []})(), account_id)
    chain = audit.get_chain(signal_id)
    return _serialize_trading_audit_chain(signal_id, chain, account_id)


@app.post(
    "/api/v1/trading/orders",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def place_trading_order(body: PlaceOrderBody) -> dict:
    """提交买入/卖出委托。若携带 tp_price/sl_price，成功后自动创建附加条件单。"""
    api = _require_trade_api(body.account_id)
    direction = body.direction.lower()
    if direction not in ("buy", "sell"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="direction 必须为 'buy' 或 'sell'",
        )
    if body.volume <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="volume 必须大于 0",
        )
    if direction == "buy":
        resp = _call_trade_api(
            api,
            "buy",
            account_id=body.account_id,
            code=body.code,
            volume=body.volume,
            price=body.price,
            price_type=body.price_type,
            signal_id=body.signal_id,
            timeout=20.0,
        )
    else:
        resp = _call_trade_api(
            api,
            "sell",
            account_id=body.account_id,
            code=body.code,
            volume=body.volume,
            price=body.price,
            price_type=body.price_type,
            signal_id=body.signal_id,
            timeout=20.0,
        )

    attached_cond_ids: list[str] = []
    if resp.order_id and (body.tp_price is not None or body.sl_price is not None):
        close_dir = "sell" if direction == "buy" else "buy"
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 买入开仓：TP=≥触发卖出，SL=≤触发卖出；卖出开仓：TP=≤触发买入，SL=≥触发买入
        if body.tp_price is not None and body.tp_price > 0:
            tp_cond: dict = {
                "id": str(uuid.uuid4())[:8],
                "account_id": body.account_id,
                "symbol": body.code,
                "direction": close_dir,
                "volume": body.volume,
                "order_price": 0.0,
                "price_type": "market",
                "trigger_price": body.tp_price,
                "trigger_type": "gte" if direction == "buy" else "lte",
                "note": f"attached:tp:{resp.order_id}",
                "status": "pending",
                "triggered_price": None,
                "result_order_id": None,
                "error_msg": None,
                "created_at": now_str,
                "triggered_at": None,
            }
            with _cond_orders_lock:
                _co = _load_cond_orders()
                _co.append(tp_cond)
                _save_cond_orders(_co)
            attached_cond_ids.append(tp_cond["id"])
        if body.sl_price is not None and body.sl_price > 0:
            sl_cond: dict = {
                "id": str(uuid.uuid4())[:8],
                "account_id": body.account_id,
                "symbol": body.code,
                "direction": close_dir,
                "volume": body.volume,
                "order_price": 0.0,
                "price_type": "market",
                "trigger_price": body.sl_price,
                "trigger_type": "lte" if direction == "buy" else "gte",
                "note": f"attached:sl:{resp.order_id}",
                "status": "pending",
                "triggered_price": None,
                "result_order_id": None,
                "error_msg": None,
                "created_at": now_str,
                "triggered_at": None,
            }
            with _cond_orders_lock:
                _co = _load_cond_orders()
                _co.append(sl_cond)
                _save_cond_orders(_co)
            attached_cond_ids.append(sl_cond["id"])

    result: dict = {
        "order_id": resp.order_id,
        "status": resp.status,
        "msg": resp.msg,
    }
    if attached_cond_ids:
        result["attached_cond_ids"] = attached_cond_ids
    return result


@app.post(
    "/api/v1/trading/orders/{order_id}/replace",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def replace_trading_order(order_id: int, body: ReplaceOrderBody) -> dict[str, Any]:
    """撤销当前 live 委托并按新价格重挂，尽量保留 signal/audit 链。"""
    if body.price <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="price 必须大于 0",
        )
    if body.volume is not None and body.volume <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="volume 必须大于 0",
        )
    price_type = str(body.price_type or "limit").strip().lower() or "limit"
    if price_type not in {"limit", "market", "限价", "市价"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="price_type 必须为 market / limit",
        )

    api = _require_trade_api(body.account_id)
    snapshot = _find_cancelable_order_snapshot(api, body.account_id, order_id)
    if snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"找不到可撤委托 {order_id}，请先刷新当前委托列表",
        )

    audit = _get_or_init_audit_trail()
    order_context = audit.get_order_context(str(order_id)) if audit is not None else {}
    direction = _normalize_trading_order_direction(snapshot.get("direction"), str(order_context.get("direction") or ""))
    if direction not in {"buy", "sell"}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"无法解析委托 {order_id} 的方向，拒绝改价",
        )

    remaining_volume = int(snapshot.get("remaining_volume", 0) or 0)
    requested_volume = int(body.volume) if body.volume is not None else remaining_volume
    if requested_volume <= 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"委托 {order_id} 剩余数量为 0，无法改价",
        )
    if remaining_volume > 0 and requested_volume > remaining_volume:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"volume 超过当前剩余数量 {remaining_volume}",
        )

    code = str(snapshot.get("code") or order_context.get("code") or "").strip()
    if not code:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"无法解析委托 {order_id} 的证券代码，拒绝改价",
        )
    signal_id = str(body.signal_id or order_context.get("signal_id") or "").strip()

    cancelled = bool(_call_trade_api(api, "cancel_order", body.account_id, order_id, timeout=20.0))
    if not cancelled:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"撤销原委托 {order_id} 失败，未执行重挂",
        )
    if audit is not None:
        try:
            audit.update_order_status(str(order_id), "cancelled")
        except Exception:
            log.exception("replace_trading_order 更新旧委托状态失败 order_id=%s", order_id)

    if direction == "buy":
        resp = _call_trade_api(
            api,
            "buy",
            account_id=body.account_id,
            code=code,
            volume=requested_volume,
            price=body.price,
            price_type=price_type,
            signal_id=signal_id,
            timeout=20.0,
        )
    else:
        resp = _call_trade_api(
            api,
            "sell",
            account_id=body.account_id,
            code=code,
            volume=requested_volume,
            price=body.price,
            price_type=price_type,
            signal_id=signal_id,
            timeout=20.0,
        )

    replacement_order_id = int(resp.order_id) if resp.order_id else None
    replaced = replacement_order_id is not None and replacement_order_id > 0
    result_status = str(resp.status or "") if replaced else "cancelled_only"
    result_msg = str(resp.msg or "").strip()
    if replaced and not result_msg:
        result_msg = f"旧单 {order_id} 已撤，新单 {replacement_order_id} 已提交"
    if not replaced and not result_msg:
        result_msg = f"旧单 {order_id} 已撤，但新单提交失败"
    return {
        "source_order_id": int(order_id),
        "account_id": body.account_id,
        "code": code,
        "direction": direction,
        "requested_price": float(body.price),
        "requested_volume": requested_volume,
        "remaining_volume_before_replace": remaining_volume,
        "price_type": price_type,
        "signal_id": signal_id,
        "cancelled": True,
        "replacement_order_id": replacement_order_id,
        "replaced": replaced,
        "status": result_status,
        "msg": result_msg,
    }


@app.delete(
    "/api/v1/trading/orders/{order_id}",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def cancel_trading_order(order_id: int, account_id: str) -> dict:
    """撤销委托。"""
    api = _require_trade_api(account_id)
    success = bool(_call_trade_api(api, "cancel_order", account_id, order_id, timeout=20.0))
    if success:
        audit = _get_or_init_audit_trail()
        if audit is not None:
            try:
                audit.update_order_status(str(order_id), "cancelled")
            except Exception:
                log.exception("cancel_trading_order 审计状态更新失败 order_id=%s", order_id)
    return {
        "order_id": order_id,
        "account_id": account_id,
        "cancelled": success,
    }


@app.delete(
    "/api/v1/trading/orders",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def cancel_all_trading_orders(account_id: str) -> dict:
    """撤销账户所有可撤委托（全撤）。"""
    api = _require_trade_api(account_id)
    result = _call_trade_api(api, "cancel_all_orders", account_id, timeout=20.0)
    if result is None:
        result = {"cancelled": 0, "failed": 0, "order_ids": []}
    return result


# ---------------------------------------------------------------------------
# 条件单（止损 / 止盈）管理
# ---------------------------------------------------------------------------

_COND_ORDERS_FILE = Path(__file__).parent.parent / "config" / "cond_orders.json"
_cond_orders_lock = threading.Lock()


def _load_cond_orders() -> list[dict]:
    try:
        if _COND_ORDERS_FILE.exists():
            return json.loads(_COND_ORDERS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


def _save_cond_orders(orders: list[dict]) -> None:
    try:
        _COND_ORDERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _COND_ORDERS_FILE.write_text(
            json.dumps(orders, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as exc:
        log.warning("条件单保存失败: %s", exc)


class CondOrderBody(BaseModel):
    account_id: str
    symbol: str
    direction: str          # "buy" | "sell"
    volume: int
    order_price: float = 0.0    # 0 = 市价
    price_type: str = "market"
    trigger_price: float
    trigger_type: str = "gte"   # "gte"=价格>=触发价(止盈卖/止损买), "lte"=价格<=触发价(止损卖/止盈买)
    note: str = ""


@app.get(
    "/api/v1/trading/cond-orders",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def list_cond_orders(account_id: str | None = None) -> list[dict]:
    """查询条件单列表（止损 / 止盈）。"""
    with _cond_orders_lock:
        orders = _load_cond_orders()
    if account_id:
        orders = [o for o in orders if o.get("account_id") == account_id]
    return orders


@app.post(
    "/api/v1/trading/cond-orders",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def create_cond_order(body: CondOrderBody) -> dict:
    """创建条件单（止损 / 止盈）。"""
    if body.direction not in ("buy", "sell"):
        raise HTTPException(status_code=422, detail="direction 必须为 buy 或 sell")
    if body.trigger_type not in ("gte", "lte"):
        raise HTTPException(status_code=422, detail="trigger_type 必须为 gte 或 lte")
    if body.volume <= 0:
        raise HTTPException(status_code=422, detail="volume 必须大于 0")

    order: dict = {
        "id": str(uuid.uuid4())[:8],
        "account_id": body.account_id,
        "symbol": body.symbol,
        "direction": body.direction,
        "volume": body.volume,
        "order_price": body.order_price,
        "price_type": body.price_type,
        "trigger_price": body.trigger_price,
        "trigger_type": body.trigger_type,
        "note": body.note,
        "status": "pending",         # pending | triggered | cancelled | error
        "triggered_price": None,
        "result_order_id": None,
        "error_msg": None,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "triggered_at": None,
    }
    with _cond_orders_lock:
        orders = _load_cond_orders()
        orders.append(order)
        _save_cond_orders(orders)
    return order


@app.delete(
    "/api/v1/trading/cond-orders/{cond_id}",
    tags=["交易"],
    dependencies=[Depends(_verify_auth_and_rate)],
)
def cancel_cond_order(cond_id: str) -> dict:
    """撤销条件单。"""
    with _cond_orders_lock:
        orders = _load_cond_orders()
        updated = False
        for o in orders:
            if o["id"] == cond_id and o["status"] == "pending":
                o["status"] = "cancelled"
                updated = True
                break
        if updated:
            _save_cond_orders(orders)
    if not updated:
        raise HTTPException(status_code=404, detail=f"条件单 {cond_id} 不存在或已非 pending 状态")
    return {"id": cond_id, "cancelled": True}


def _cond_order_monitor_loop() -> None:
    """后台循环监控条件单，触发时自动下单。"""
    log.info("条件单监控线程启动")
    while True:
        try:
            time.sleep(3)
            with _cond_orders_lock:
                orders = _load_cond_orders()
            pending = [o for o in orders if o["status"] == "pending"]
            if not pending:
                continue

            # 批量取最新价（通过 xtdata get_full_tick）
            symbols = list({o["symbol"] for o in pending})
            prices: dict[str, float] = {}
            try:
                ticks = xt_get_full_tick(symbols)
                for sym, tick in ticks.items():
                    last = tick.get("lastPrice") or tick.get("last_price")
                    if last:
                        prices[sym] = float(last)
            except Exception as exc:
                log.debug("条件单价格查询失败: %s", exc)
                continue

            changed = False
            for o in orders:
                if o["status"] != "pending":
                    continue
                sym = o["symbol"]
                if sym not in prices:
                    continue
                cur_price = prices[sym]
                trigger_price = float(o["trigger_price"])
                hit = (
                    (o["trigger_type"] == "gte" and cur_price >= trigger_price)
                    or (o["trigger_type"] == "lte" and cur_price <= trigger_price)
                )
                if not hit:
                    continue

                # 触发下单
                try:
                    api = _require_trade_api(o["account_id"])
                    direction = o["direction"]
                    vol = int(o["volume"])
                    op = float(o["order_price"])
                    pt = o.get("price_type", "market")
                    if direction == "buy":
                        resp = _call_trade_api(api, "buy", o["account_id"], sym, vol, op, pt, timeout=20.0)
                    else:
                        resp = _call_trade_api(api, "sell", o["account_id"], sym, vol, op, pt, timeout=20.0)
                    o["status"] = "triggered"
                    o["triggered_price"] = cur_price
                    o["triggered_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    o["result_order_id"] = getattr(resp, "order_id", None)
                    log.info(
                        "条件单触发 id=%s sym=%s dir=%s vol=%d price=%.3f cur=%.3f",
                        o["id"], sym, direction, vol, trigger_price, cur_price,
                    )
                except Exception as exc:
                    o["status"] = "error"
                    o["error_msg"] = str(exc)
                    log.warning("条件单下单失败 id=%s: %s", o["id"], exc)
                changed = True

            if changed:
                with _cond_orders_lock:
                    _save_cond_orders(orders)

        except Exception as exc:
            log.warning("条件单监控异常: %s", exc)


def _start_cond_order_monitor_if_enabled() -> threading.Thread | None:
    if not xt_side_effects_allowed(test_mode=_TEST_MODE):
        log.info("条件单监控线程未启动: test mode / EASYXT_DISABLE_XT active")
        return None
    thread = threading.Thread(
        target=_cond_order_monitor_loop, name="cond-order-monitor", daemon=True
    )
    thread.start()
    return thread


# 启动条件单后台监控线程（daemon，进程退出自动结束）
_cond_monitor_thread = _start_cond_order_monitor_if_enabled()


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


@app.get(
    "/api/v1/market/bar-subscriptions", tags=["行情"], dependencies=[Depends(_verify_auth_and_rate)]
)
def list_bar_subscriptions() -> dict:
    """列出当前所有 bar 订阅及统计信息（原生 QMT + tick 聚合回退）。"""
    try:
        from core.qmt_feed import qmt_feed

        native_subs = qmt_feed.all_bar_subscriptions()
        stats = dict(qmt_feed.stats())
    except Exception:
        native_subs = []
        stats = {}
    aggregated_subs = _list_tick_aggregated_bar_subscriptions()
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for item in native_subs:
        merged[(str(item.get("symbol", "")), str(item.get("period", "")))] = dict(item)
    for item in aggregated_subs:
        merged[(str(item.get("symbol", "")), str(item.get("period", "")))] = dict(item)
    subs = list(merged.values())
    stats["total_bar_subscriptions"] = len(subs)
    stats["total_bar_ingested"] = sum(int(item.get("ingested_count", 0) or 0) for item in subs)
    stats["bar_subscription_sources"] = {
        "native": len(native_subs),
        "tick_aggregate": len(aggregated_subs),
    }
    return {"subscriptions": subs, "stats": stats}


# ---------------------------------------------------------------------------
# 行情 WebSocket
# ---------------------------------------------------------------------------


def _resolve_ws_api_token(websocket: WebSocket, query_token: str) -> str:
    header_token = str(websocket.headers.get("x-api-token", "") or "").strip()
    if header_token:
        return header_token
    auth_header = str(websocket.headers.get("authorization", "") or "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return str(query_token or "").strip()


def _is_normal_ws_disconnect_runtime_error(exc: RuntimeError) -> bool:
    message = str(exc or "")
    return "WebSocket is not connected" in message


@app.websocket("/ws/market/{symbol}")
async def ws_market(
    websocket: WebSocket,
    symbol: str,
    token: str = Query(default=""),
) -> None:
    """
    实时行情推送（WebSocket）。

    鉴权：优先使用 `X-API-Token` / `Authorization: Bearer <token>`，
    兼容旧的 `?token=<api_token>` 查询参数（EASYXT_API_TOKEN 为空时不校验）。
    数据格式：{"symbol": ..., "price": ..., "event_ts_ms": <ms>, "seq": <int>, "source": ...}
    客户端去重键：symbol + seq
    数据通过 ingest_tick_from_thread() 从 QMT 实时推送（无 mock）。
    """
    auth_token = _resolve_ws_api_token(websocket, token)
    if not (_DEV_MODE or _TEST_MODE) and _API_TOKEN and (not auth_token or not secrets.compare_digest(auth_token, _API_TOKEN)):
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

    # 主动快照：若 broadcaster 尚无缓存（非交易时段/刚重启），
    # 主动通过 xtdata.get_full_tick 拉一次最新快照注入广播层，
    # 前端可立即看到收盘价等基本信息，而不是永远等到下一条推送。
    if not broadcaster.has_cached(symbol):
        try:
            from core.qmt_feed import _normalize_tick as _nt

            # 必须通过 run_in_executor 在线程中执行阻塞的 xtdata_submit，
            # 避免直接调用阻塞 asyncio event loop
            _loop = asyncio.get_event_loop()
            snap = await _loop.run_in_executor(None, xt_get_full_tick, [symbol])
            _snap = dict(snap.get(symbol) or {})
            if _snap:
                payload = _nt(symbol, _snap)
                await broadcaster.broadcast(symbol, payload)
                log.debug("WS 快照注入 symbol=%s price=%s", symbol, _snap.get("lastPrice"))
        except Exception as exc:
            log.debug("WS 快照拉取失败 symbol=%s: %s", symbol, exc)

    try:
        while True:
            data = await websocket.receive_text()
            if data.strip().lower() in ("ping", '{"type":"ping"}'):
                await websocket.send_text('{"type":"pong"}')
    except WebSocketDisconnect:
        pass
    except RuntimeError as exc:
        if not _is_normal_ws_disconnect_runtime_error(exc):
            raise
    finally:
        broadcaster.unsubscribe(symbol, websocket)
        log.info("WS 断开 symbol=%s 剩余订阅数=%d", symbol, broadcaster.subscriber_count(symbol))


# ---------------------------------------------------------------------------
# WebSocket bar 推送（/ws/bar/{symbol}/{period_type}/{period_span}）
# ---------------------------------------------------------------------------

# 每个 (symbol, period_type, period_span) 组合独立广播器
# 键格式："{symbol}:{period_type}:{period_span}"
_bar_broadcasters: dict[str, "_MarketBroadcaster"] = {}
_bar_broadcasters_lock = asyncio.Lock()


async def _get_bar_broadcaster(key: str) -> "_MarketBroadcaster":
    async with _bar_broadcasters_lock:
        if key not in _bar_broadcasters:
            _bar_broadcasters[key] = _MarketBroadcaster()
        return _bar_broadcasters[key]


@app.websocket("/ws/bar/{symbol}/{period_type}/{period_span}")
async def ws_bar(
    websocket: WebSocket,
    symbol: str,
    period_type: str,
    period_span: int,
    token: str = Query(default=""),
) -> None:
    """
    实时 K 线 bar 推送（WebSocket）。

    消息格式（服务端 → 客户端）：
      {"type":"subscribed"}
      {"type":"bar","symbol":"...","period":"1m",
       "bar":{"time":"YYYY-MM-DD HH:MM:SS","open":...,"high":...,"low":...,"close":...,"volume":...},
       "is_final":false,"seq":<int>}
      {"type":"pong"}

    消息格式（客户端 → 服务端）：
      {"type":"ping"}  — 心跳，服务端回 pong

    鉴权规则同 /ws/market/{symbol}（EASYXT_API_TOKEN 为空时不校验）。

    当前 1m / 5m / 1d 默认优先走 tick → bar 聚合回退链路，
    其余周期仍保持 qmt_feed.subscribe_bar() 原生订阅。
    bar 数据统一通过 ingest_bar_from_thread() / tick 聚合广播层送出。
    """
    auth_token = _resolve_ws_api_token(websocket, token)
    if not (_DEV_MODE or _TEST_MODE) and _API_TOKEN and (not auth_token or not secrets.compare_digest(auth_token, _API_TOKEN)):
        await websocket.close(code=4001, reason="Unauthorized")
        return

    period_key = f"{symbol}:{period_type}:{period_span}"
    bcast = await _get_bar_broadcaster(period_key)

    await websocket.accept()
    await bcast.asubscribe(symbol, websocket)
    log.info("WS bar 订阅 symbol=%s period=%s%s", symbol, period_type, period_span)

    # 自动通过 qmt_feed / tick 聚合回退触发该标的的实时 bar 链路
    try:
        from core.qmt_feed import qmt_feed as _qf

        use_tick_aggregate = _supports_tick_aggregated_bar(period_type, period_span)
        if use_tick_aggregate:
            try:
                _ensure_tick_aggregated_bar_state(symbol, period_type, period_span)
                if not _qf.is_subscribed(symbol):
                    _qf.subscribe(symbol, period="tick")
                log.info(
                    "WS bar 启用 tick 聚合回退 symbol=%s period=%s%s",
                    symbol,
                    period_type,
                    period_span,
                )
            except Exception as exc:
                use_tick_aggregate = False
                log.warning(
                    "WS bar tick 聚合初始化失败，回退 native symbol=%s period=%s%s: %s",
                    symbol,
                    period_type,
                    period_span,
                    exc,
                )

        if not use_tick_aggregate and not _qf.is_bar_subscribed(symbol, period_type, period_span):
            _qf.subscribe_bar(symbol, period_type, period_span)
            log.info("WS bar 触发自动订阅 symbol=%s %s%s via qmt_feed", symbol, period_type, period_span)
    except Exception as exc:
        log.warning("WS bar 自动订阅失败 symbol=%s: %s", symbol, exc)

    # 发送订阅确认
    try:
        await websocket.send_text('{"type":"subscribed"}')
    except Exception:
        pass

    try:
        while True:
            data = await websocket.receive_text()
            if data.strip().lower() in ("ping", '{"type":"ping"}'):
                await websocket.send_text('{"type":"pong"}')
    except WebSocketDisconnect:
        pass
    except RuntimeError as exc:
        if not _is_normal_ws_disconnect_runtime_error(exc):
            raise
    finally:
        bcast.unsubscribe(symbol, websocket)
        if _supports_tick_aggregated_bar(period_type, period_span) and bcast.subscriber_count(symbol) == 0:
            _drop_tick_aggregated_bar_state(period_key)
        log.info("WS bar 断开 symbol=%s period=%s%s", symbol, period_type, period_span)



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
            qmt_available = _call_with_datasource_health_interface(
                lambda iface: bool(getattr(iface, "qmt_available", False))
            )
            if qmt_available:
                raw = xt_get_financial_data(
                    stock_list=[stock_code],
                    table_list=["Income", "Balance", "CashFlow"],
                    start_time="",
                    end_time="",
                )
                stock_raw = (raw or {}).get(stock_code, {})
                qmt_result = saver.save_from_qmt(
                    stock_code,
                    stock_raw.get("Income"),
                    stock_raw.get("Balance"),
                    stock_raw.get("CashFlow"),
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
