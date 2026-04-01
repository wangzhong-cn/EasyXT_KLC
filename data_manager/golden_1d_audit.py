"""黄金标准 1D 数据质量审计系统。

本模块实现"独立事实源 + 全历史穷举 + 不变量验证 + 持久化审计"的黄金标准门禁。

核心原则：
- 抽样是巡检算法，不是门禁算法
- 最可靠的方法不是抽样统计，而是"独立事实源 + 全历史穷举 + 不变量验证"
- 结果持久化到 golden_1d_audit 表，统一供 Qt/Tauri/策略/回测消费

四层验证模型：
- 第 0 层：格式与契约校验（DataContractValidator）
- 第 1 层：本地自洽校验（listing_date → today，完备性门槛）
- 第 2 层：聚合不变量校验（1m→1d 全字段，正确性门槛）
- 第 3 层：独立多源交叉校验（DAT 直读 > QMT API > AKShare/Tushare）
"""

import hashlib
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Optional

import pandas as pd

from data_manager.data_contract_validator import DataContractValidator
from data_manager.dat_binary_reader import DATBinaryReader, read_dat
from data_manager.duckdb_connection_pool import resolve_duckdb_path
from data_manager.smart_data_detector import get_trading_calendar

logger = logging.getLogger(__name__)

_INVARIANT_MODES = {"recent", "changed_partitions", "full"}
_DEFAULT_INVARIANT_MODE = "changed_partitions"
_DEFAULT_RECENT_DAYS = 5

# ─── 状态枚举 ───────────────────────────────────────────────────────────────────

Golden1dStatus = Literal["golden", "partial_trust", "degraded", "unknown"]
CrossSourceStatus = Literal["verified", "degraded", "unavailable", "unknown"]
BackfillStatus = Literal[
    "complete",
    "in_progress",
    "pending",
    "queued",
    "failed",
    "manual_review",
    "blocked",
]

# ─── 数据类 ─────────────────────────────────────────────────────────────────────


@dataclass
class DailyAuditRecord:
    """单标的单日的审计记录。"""

    symbol: str
    trade_date: str  # YYYY-MM-DD
    source_duckdb_close: Optional[float] = None
    source_dat_close: Optional[float] = None
    source_akshare_close: Optional[float] = None
    close_match: Optional[bool] = None
    open_match: Optional[bool] = None
    high_match: Optional[bool] = None
    low_match: Optional[bool] = None
    volume_match: Optional[bool] = None
    aggregated_from_1m: Optional[bool] = None
    listing_aligned: Optional[bool] = None
    row_hash: Optional[str] = None
    issue: Optional[str] = None


@dataclass
class SymbolAuditSummary:
    """单标的的审计摘要（持久化到 golden_1d_audit 表）。"""

    symbol: str
    listing_date: Optional[str] = None
    local_first_date: Optional[str] = None
    local_last_date: Optional[str] = None
    expected_trading_days: int = 0
    actual_trading_days: int = 0
    missing_days: int = 0
    duplicate_days: int = 0
    has_listing_gap: bool = False
    listing_gap_days: int = 0
    cross_source_status: CrossSourceStatus = "unknown"
    cross_source_fields_passed: int = 0
    cross_source_fields_total: int = 5  # OHLCV
    backfill_status: BackfillStatus = "pending"
    is_golden_1d_ready: bool = False
    golden_status: Golden1dStatus = "unknown"
    last_audited_at: Optional[str] = None
    audit_version: str = "v1"
    partition_hashes: dict[str, str] = field(default_factory=dict)
    issues: list[str] = field(default_factory=list)


@dataclass
class Golden1dAuditReport:
    """全量审计报告。"""

    audited_at: str
    total_symbols: int = 0
    golden_count: int = 0
    partial_trust_count: int = 0
    degraded_count: int = 0
    unknown_count: int = 0
    symbol_summaries: list[SymbolAuditSummary] = field(default_factory=list)


# ─── 交易日历工具 ───────────────────────────────────────────────────────────────


def _resolve_trading_days(start: str, end: str) -> tuple[list[str], bool, str | None]:
    """通过仓库统一交易日历能力获取交易日列表。"""
    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end, "%Y-%m-%d").date()
        calendar = get_trading_calendar()
        trading_days = calendar.get_trading_days(start_dt, end_dt)
        return [d.strftime("%Y-%m-%d") for d in trading_days], True, None
    except Exception as exc:
        logger.warning("交易日历计算不可用: %s", exc)
        return [], False, str(exc)


def get_trading_days(start: str, end: str) -> list[str]:
    """获取两个日期之间的所有交易日。"""
    days, _available, _issue = _resolve_trading_days(start, end)
    return days


def _normalize_invariant_mode(raw: str | None) -> str:
    mode = str(raw or _DEFAULT_INVARIANT_MODE).strip().lower()
    return mode if mode in _INVARIANT_MODES else _DEFAULT_INVARIANT_MODE


def _standardize_daily_df(df: pd.DataFrame) -> pd.DataFrame:
    """将任意日线 DataFrame 标准化为包含 datetime/open/high/low/close/volume 的结构。"""
    if df is None or df.empty:
        return pd.DataFrame()

    data = df.copy()
    data.columns = [str(c).lower() for c in data.columns]

    if "datetime" not in data.columns:
        if isinstance(data.index, pd.DatetimeIndex):
            index_name = data.index.name or "index"
            data = data.reset_index().rename(columns={index_name.lower(): "datetime", index_name: "datetime"})
        elif "date" in data.columns:
            data["datetime"] = data["date"]
        elif "time" in data.columns:
            data["datetime"] = data["time"]
        else:
            data["datetime"] = pd.NaT

    data["datetime"] = pd.to_datetime(data["datetime"], errors="coerce")
    data = data[data["datetime"].notna()].reset_index(drop=True)

    for col in ("open", "high", "low", "close", "volume"):
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    return data


# ─── 行级哈希 ───────────────────────────────────────────────────────────────────


def compute_daily_row_hash(
    symbol: str, trade_date: str, open_: float, high: float, low: float, close: float, volume: float
) -> str:
    """计算单条日线记录的规范化哈希。

    使用固定精度序列化，避免浮点精度差异导致哈希不一致。
    """
    raw = f"{symbol}|{trade_date}|{open_:.4f}|{high:.4f}|{low:.4f}|{close:.4f}|{volume:.2f}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def compute_partition_hash(hashes: list[str]) -> str:
    """计算分区（月度）哈希。"""
    combined = "|".join(sorted(hashes))
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()[:16]


# ─── 1m → 1d 聚合不变量验证 ─────────────────────────────────────────────────────


def verify_1m_to_1d_invariants(day_1m: pd.DataFrame, daily_record: dict) -> dict[str, bool]:
    """验证 1 分钟线聚合到日线的一致性。

    不变量：
    - open == first(1m.open)
    - high == max(1m.high)
    - low == min(1m.low)
    - close == last(1m.close)
    - volume == sum(1m.volume)（容差 0.1%）
    """
    results = {}

    if day_1m.empty:
        return {
            "open_match": False,
            "high_match": False,
            "low_match": False,
            "close_match": False,
            "volume_match": False,
        }

    m_open = day_1m["open"].iloc[0]
    m_high = day_1m["high"].max()
    m_low = day_1m["low"].min()
    m_close = day_1m["close"].iloc[-1]
    m_volume = day_1m["volume"].sum()

    d_open = daily_record.get("open", 0)
    d_high = daily_record.get("high", 0)
    d_low = daily_record.get("low", 0)
    d_close = daily_record.get("close", 0)
    d_volume = daily_record.get("volume", 0)

    results["open_match"] = abs(m_open - d_open) / max(abs(d_open), 1e-6) < 1e-4
    results["high_match"] = abs(m_high - d_high) / max(abs(d_high), 1e-6) < 1e-4
    results["low_match"] = abs(m_low - d_low) / max(abs(d_low), 1e-6) < 1e-4
    results["close_match"] = abs(m_close - d_close) / max(abs(d_close), 1e-6) < 1e-4

    # volume 容差稍大，因为可能有舍入
    if d_volume > 0:
        results["volume_match"] = abs(m_volume - d_volume) / d_volume < 1e-3
    else:
        results["volume_match"] = m_volume == 0

    return results


# ─── DAT 直读作为独立事实源 ─────────────────────────────────────────────────────


def read_dat_as_fact_source(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """从 DAT 文件直读日线数据，作为独立事实源。

    完全不依赖 xtquant API 或 DuckDB 已入库结果。
    返回标准化 DataFrame，包含 'datetime' 列（非 index）。
    """
    try:
        df = read_dat(symbol, period="1d", start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            return _standardize_daily_df(df)
    except Exception as e:
        logger.warning("DAT 直读失败（symbol=%s）: %s", symbol, e)
    return pd.DataFrame()


# ─── DuckDB 日线读取 ────────────────────────────────────────────────────────────


def read_duckdb_daily(
    symbol: str, start_date: str, end_date: str, duckdb_path: str = None
) -> pd.DataFrame:
    """从 DuckDB 读取日线数据。

    注意：stock_daily 表的日期列是 'date'，不是 'trade_date'。
    """
    try:
        import duckdb

        if duckdb_path is None:
            duckdb_path = resolve_duckdb_path()

        con = duckdb.connect(duckdb_path, read_only=True)
        query = """
            SELECT
                date as datetime,
                open,
                high,
                low,
                close,
                volume,
                adjust_type,
                created_at,
                updated_at
            FROM stock_daily
            WHERE stock_code = ?
              AND period = '1d'
              AND COALESCE(adjust_type, 'none') = 'none'
              AND date >= ? AND date <= ?
            ORDER BY date, updated_at, created_at
        """
        df = con.execute(query, [symbol, start_date, end_date]).fetchdf()
        con.close()
        return _standardize_daily_df(df)
    except Exception as e:
        logger.warning("DuckDB 日线读取失败（symbol=%s）: %s", symbol, e)
    return pd.DataFrame()


# ─── 核心审计引擎 ───────────────────────────────────────────────────────────────


class Golden1dAuditor:
    """黄金标准 1D 审计引擎。

    对单个标的执行全历史逐日穷举验证：
    1. 从 DAT 直读获取独立事实源
    2. 从 DuckDB 获取已入库数据
    3. 逐日逐字段 OHLCV 比对
    4. 1m→1d 聚合不变量验证
    5. 交易日历完备性检查
    6. 逐日 hash / 分区 hash 增量门禁
    7. 结果持久化到 golden_1d_audit 表
    """

    def __init__(self, db_path: str = None, audit_db_path: str = None):
        self.duckdb_path = resolve_duckdb_path(db_path)

        if audit_db_path is None:
            audit_db_path = (
                os.environ.get("EASYXT_GOLDEN_1D_AUDIT_DB_PATH")
                or str(Path(__file__).resolve().parents[1] / "data" / "golden_1d_audit.db")
            )
        self.audit_db_path = audit_db_path

        self.dat_reader = DATBinaryReader()
        self.contract_validator = DataContractValidator()
        self._invariant_mode = _normalize_invariant_mode(
            os.environ.get("EASYXT_GOLDEN_1D_INVARIANT_MODE")
        )
        self._recent_invariant_days = max(
            1, int(os.environ.get("EASYXT_GOLDEN_1D_RECENT_DAYS", str(_DEFAULT_RECENT_DAYS)))
        )
        self._ensure_audit_table()

    @staticmethod
    def _format_contract_issues(source: str, result: Any) -> list[str]:
        issues: list[str] = []
        for violation in getattr(result, "violations", []) or []:
            issues.append(f"{source}:{violation.check}:{violation.detail}")
        return issues

    @staticmethod
    def _canonicalize_local_daily(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        if df is None or df.empty:
            return pd.DataFrame(), 0

        data = _standardize_daily_df(df)
        data["trade_date"] = data["datetime"].dt.strftime("%Y-%m-%d")
        duplicate_count = max(0, len(data) - data["trade_date"].nunique())
        sort_cols = [col for col in ("trade_date", "updated_at", "created_at") if col in data.columns]
        if sort_cols:
            data = data.sort_values(sort_cols)
        canonical = data.drop_duplicates(subset=["trade_date"], keep="last")
        return canonical.drop(columns=["trade_date"], errors="ignore").reset_index(drop=True), duplicate_count

    def _compute_partition_hashes(self, daily_df: pd.DataFrame, daily_hashes: list[str]) -> dict[str, str]:
        partition_hashes: dict[str, str] = {}
        current_month: str | None = None
        month_hashes: list[str] = []
        for row_hash, row_data in zip(daily_hashes, daily_df.itertuples()):
            month = row_data.datetime.strftime("%Y-%m")
            if month != current_month:
                if current_month and month_hashes:
                    partition_hashes[current_month] = compute_partition_hash(month_hashes)
                current_month = month
                month_hashes = []
            month_hashes.append(row_hash)
        if current_month and month_hashes:
            partition_hashes[current_month] = compute_partition_hash(month_hashes)
        return partition_hashes

    def _select_invariant_dates(
        self,
        daily_df: pd.DataFrame,
        current_hashes: dict[str, str],
        previous_hashes: dict[str, str] | None,
        force_full: bool,
    ) -> list[str]:
        if daily_df is None or daily_df.empty:
            return []

        if force_full or self._invariant_mode == "full":
            selected = daily_df["datetime"].dt.strftime("%Y-%m-%d")
            return list(dict.fromkeys(selected.tolist()))

        if self._invariant_mode == "recent":
            selected = daily_df["datetime"].tail(self._recent_invariant_days).dt.strftime("%Y-%m-%d")
            return list(dict.fromkeys(selected.tolist()))

        if not previous_hashes:
            selected = daily_df["datetime"].dt.strftime("%Y-%m-%d")
            return list(dict.fromkeys(selected.tolist()))

        changed_months = [
            month for month, digest in current_hashes.items()
            if previous_hashes.get(month) != digest
        ]
        if not changed_months:
            return []

        month_mask = daily_df["datetime"].dt.strftime("%Y-%m").isin(changed_months)
        selected = daily_df.loc[month_mask, "datetime"].dt.strftime("%Y-%m-%d")
        return list(dict.fromkeys(selected.tolist()))

    def _ensure_audit_table(self):
        """确保 golden_1d_audit 表存在。"""
        Path(self.audit_db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.audit_db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS golden_1d_audit (
                symbol TEXT PRIMARY KEY,
                listing_date TEXT,
                local_first_date TEXT,
                local_last_date TEXT,
                expected_trading_days INTEGER DEFAULT 0,
                actual_trading_days INTEGER DEFAULT 0,
                missing_days INTEGER DEFAULT 0,
                duplicate_days INTEGER DEFAULT 0,
                has_listing_gap INTEGER DEFAULT 0,
                listing_gap_days INTEGER DEFAULT 0,
                cross_source_status TEXT DEFAULT 'unknown',
                cross_source_fields_passed INTEGER DEFAULT 0,
                cross_source_fields_total INTEGER DEFAULT 5,
                backfill_status TEXT DEFAULT 'pending',
                is_golden_1d_ready INTEGER DEFAULT 0,
                golden_status TEXT DEFAULT 'unknown',
                last_audited_at TEXT,
                audit_version TEXT DEFAULT 'v1',
                partition_hashes TEXT,
                issues TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                source_duckdb_close REAL,
                source_dat_close REAL,
                source_akshare_close REAL,
                close_match INTEGER,
                open_match INTEGER,
                high_match INTEGER,
                low_match INTEGER,
                volume_match INTEGER,
                aggregated_from_1m INTEGER,
                listing_aligned INTEGER,
                row_hash TEXT,
                issue TEXT,
                UNIQUE(symbol, trade_date)
            )
        """)
        conn.commit()
        conn.close()

    def audit_symbol(
        self, symbol: str, listing_date: str = None, force_full: bool = False
    ) -> SymbolAuditSummary:
        """对单个标的执行全量审计。

        Args:
            symbol: 标的代码，如 "000001.SZ"
            listing_date: 上市日期，如 "2000-01-01"。如果不提供则尝试从 UDI 获取。
            force_full: 是否强制全量重验（忽略分区 hash 缓存）

        Returns:
            SymbolAuditSummary: 审计摘要
        """
        logger.info("开始审计标的: %s", symbol)
        previous_summary = None if force_full else self.get_audit_status(symbol)

        # 1. 获取上市日期
        if listing_date is None:
            listing_date = self._get_listing_date(symbol)

        if listing_date is None:
            return SymbolAuditSummary(
                symbol=symbol,
                golden_status="unknown",
                issues=["无法获取上市日期"],
            )

        today = date.today().strftime("%Y-%m-%d")

        # 2. 从独立事实源读取（DAT 直读）
        dat_df = read_dat_as_fact_source(symbol, listing_date, today)
        if dat_df.empty:
            return SymbolAuditSummary(
                symbol=symbol,
                listing_date=listing_date,
                golden_status="degraded",
                issues=["DAT 文件无数据"],
            )

        # 3. 从 DuckDB 读取已入库数据（这是被审计对象）
        duckdb_raw_df = read_duckdb_daily(symbol, listing_date, today, self.duckdb_path)
        duckdb_df, duplicate_count = self._canonicalize_local_daily(duckdb_raw_df)

        dat_contract = self.contract_validator.validate(dat_df, symbol=symbol, source="dat", period="1d")
        duckdb_contract = self.contract_validator.validate(
            duckdb_df, symbol=symbol, source="duckdb", period="1d"
        )
        issues: list[str] = []
        issues.extend(self._format_contract_issues("DAT", dat_contract))
        issues.extend(self._format_contract_issues("DuckDB", duckdb_contract))

        listing_anchor_known = bool(listing_date and str(listing_date) > "1990-01-01")
        if listing_anchor_known:
            effective_start = str(listing_date)
        else:
            candidate_starts: list[str] = []
            if not dat_df.empty:
                candidate_starts.append(dat_df["datetime"].min().strftime("%Y-%m-%d"))
            if not duckdb_df.empty:
                candidate_starts.append(duckdb_df["datetime"].min().strftime("%Y-%m-%d"))
            effective_start = min(candidate_starts) if candidate_starts else str(listing_date or today)
            issues.append(f"上市日期未知或为兜底值，暂以 {effective_start} 作为审计起点")

        # 4. 交易日历完备性检查（围绕 DuckDB 本地可消费数据）
        expected_days, calendar_available, calendar_issue = _resolve_trading_days(effective_start, today)
        if not calendar_available:
            issues.append(f"交易日历不可用: {calendar_issue or 'unknown'}")

        # 以 DuckDB 本地数据为准，不是 DAT
        actual_dates = set()
        if not duckdb_df.empty:
            if "datetime" in duckdb_df.columns:
                actual_dates = set(duckdb_df["datetime"].dt.strftime("%Y-%m-%d").tolist())
            elif "date" in duckdb_df.columns:
                actual_dates = set(duckdb_df["date"].dt.strftime("%Y-%m-%d").tolist())

        missing = [d for d in expected_days if d not in actual_dates] if calendar_available else []
        if duplicate_count > 0:
            issues.append(f"DuckDB: 发现 {duplicate_count} 条同日重复 1d 记录")

        # 5. 逐日逐字段比对 + 哈希计算（以 DAT 为事实源，DuckDB 为被审计对象）
        daily_hashes = []
        field_matches = {"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0}
        total_compared = 0

        conn = sqlite3.connect(self.audit_db_path)

        for _, row in dat_df.iterrows():
            trade_date = row["datetime"].strftime("%Y-%m-%d")

            # 计算行级哈希（基于事实源 DAT）
            row_hash = compute_daily_row_hash(
                symbol,
                trade_date,
                row.get("open", 0),
                row.get("high", 0),
                row.get("low", 0),
                row.get("close", 0),
                row.get("volume", 0),
            )
            daily_hashes.append(row_hash)

            # 与 DuckDB 比对
            duckdb_row = None
            if not duckdb_df.empty:
                if "datetime" in duckdb_df.columns:
                    matches = duckdb_df["datetime"].dt.strftime("%Y-%m-%d") == trade_date
                elif "date" in duckdb_df.columns:
                    matches = duckdb_df["date"].dt.strftime("%Y-%m-%d") == trade_date
                else:
                    matches = pd.Series([False] * len(duckdb_df))
                if matches.any():
                    duckdb_row = duckdb_df[matches].iloc[0]

            if duckdb_row is not None:
                total_compared += 1
                close_match = abs(row.get("close", 0) - duckdb_row.get("close", 0)) < 0.01
                open_match = abs(row.get("open", 0) - duckdb_row.get("open", 0)) < 0.01
                high_match = abs(row.get("high", 0) - duckdb_row.get("high", 0)) < 0.01
                low_match = abs(row.get("low", 0) - duckdb_row.get("low", 0)) < 0.01
                vol_match = abs(row.get("volume", 0) - duckdb_row.get("volume", 0)) < 1

                if close_match:
                    field_matches["close"] += 1
                if open_match:
                    field_matches["open"] += 1
                if high_match:
                    field_matches["high"] += 1
                if low_match:
                    field_matches["low"] += 1
                if vol_match:
                    field_matches["volume"] += 1

                # 记录每日审计日志
                conn.execute(
                    """
                    INSERT OR REPLACE INTO daily_audit_log
                    (symbol, trade_date, source_duckdb_close, source_dat_close,
                     close_match, open_match, high_match, low_match, volume_match, row_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        symbol,
                        trade_date,
                        duckdb_row.get("close"),
                        row.get("close"),
                        int(close_match),
                        int(open_match),
                        int(high_match),
                        int(low_match),
                        int(vol_match),
                        row_hash,
                    ),
                )
            else:
                issues.append(f"{trade_date}: DuckDB 无对应记录")

        conn.commit()
        conn.close()

        # 6. 分区哈希计算（按月）
        partition_hashes = self._compute_partition_hashes(dat_df, daily_hashes)

        # 7. 1m→1d 聚合不变量验证（支持 recent / changed_partitions / full）
        previous_hashes = previous_summary.partition_hashes if previous_summary else None
        invariant_dates = self._select_invariant_dates(
            dat_df,
            current_hashes=partition_hashes,
            previous_hashes=previous_hashes,
            force_full=force_full,
        )
        aggregation_issues = self._verify_aggregation_invariants(symbol, dat_df, invariant_dates)
        issues.extend(aggregation_issues)

        # 8. 判定黄金标准状态
        fields_passed = sum(
            1 for v in field_matches.values() if total_compared > 0 and v / total_compared > 0.99
        )
        local_first_date = min(actual_dates) if actual_dates else None
        local_last_date = max(actual_dates) if actual_dates else None
        has_listing_gap = bool(
            listing_anchor_known and local_first_date and str(listing_date) < local_first_date
        )
        if has_listing_gap and calendar_available and local_first_date:
            listing_gap_days = len(
                [day for day in expected_days if str(listing_date) <= day < local_first_date]
            )
        else:
            listing_gap_days = 0

        has_hard_contract_failure = (not dat_contract.pass_gate) or (not duckdb_contract.pass_gate)
        can_confirm_completeness = calendar_available and listing_anchor_known

        if (
            can_confirm_completeness
            and len(missing) == 0
            and duplicate_count == 0
            and fields_passed == 5
            and not has_listing_gap
            and len(aggregation_issues) == 0
            and not has_hard_contract_failure
        ):
            golden_status = "golden"
            is_golden = True
        elif not can_confirm_completeness and not has_hard_contract_failure and fields_passed >= 3:
            golden_status = "unknown"
            is_golden = False
        elif (
            fields_passed >= 3
            and calendar_available
            and len(missing) <= 5
            and duplicate_count == 0
            and not has_hard_contract_failure
        ):
            golden_status = "partial_trust"
            is_golden = False
        else:
            golden_status = "degraded"
            is_golden = False

        summary = SymbolAuditSummary(
            symbol=symbol,
            listing_date=listing_date,
            local_first_date=local_first_date,
            local_last_date=local_last_date,
            expected_trading_days=len(expected_days) if calendar_available else 0,
            actual_trading_days=len(actual_dates),
            missing_days=len(missing),
            duplicate_days=duplicate_count,
            has_listing_gap=has_listing_gap,
            listing_gap_days=listing_gap_days,
            cross_source_status=(
                "verified" if fields_passed == 5 else ("degraded" if total_compared > 0 else "unavailable")
            ),
            cross_source_fields_passed=fields_passed,
            backfill_status=(
                "complete"
                if calendar_available and len(missing) == 0 and duplicate_count == 0
                else ("failed" if has_hard_contract_failure else "pending")
            ),
            is_golden_1d_ready=is_golden,
            golden_status=golden_status,
            last_audited_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            partition_hashes=partition_hashes,
            issues=issues[:20],  # 最多保留 20 个问题
        )

        # 9. 持久化到 golden_1d_audit 表
        self._save_summary(summary)

        logger.info(
            "审计完成: %s -> %s (missing=%d, fields=%d/5)",
            symbol,
            golden_status,
            len(missing),
            fields_passed,
        )
        return summary

    def _verify_aggregation_invariants(
        self, symbol: str, daily_df: pd.DataFrame, trade_dates: list[str]
    ) -> list[str]:
        """验证 1m→1d 聚合不变量。"""
        issues = []
        if not trade_dates:
            return issues
        try:
            for trade_date in trade_dates:
                start = f"{trade_date} 00:00:00"
                end = f"{trade_date} 23:59:59"
                try:
                    m1_df = self.dat_reader.get_data(symbol, start, end, "1m", "none")
                except Exception:
                    continue

                if m1_df is None or m1_df.empty:
                    continue

                day_record = daily_df[daily_df["datetime"].dt.strftime("%Y-%m-%d") == trade_date]
                if day_record.empty:
                    continue

                daily_rec = day_record.iloc[0].to_dict()
                results = verify_1m_to_1d_invariants(m1_df, daily_rec)

                for field_name, passed in results.items():
                    if not passed:
                        issues.append(f"{trade_date}: 1m→1d {field_name} 不一致")
        except Exception as e:
            logger.debug("聚合不变量验证失败（symbol=%s）: %s", symbol, e)
        return issues

    def _get_listing_date(self, symbol: str) -> Optional[str]:
        """获取标的上市日期。"""
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface

            udi = UnifiedDataInterface(eager_init=False)
            return udi.get_listing_date(symbol)
        except Exception:
            return None

    def list_stored_symbols(self, limit: int | None = None) -> list[str]:
        """列出 DuckDB 中已存储 1d 原始日线的标的。"""
        try:
            import duckdb

            con = duckdb.connect(self.duckdb_path, read_only=True)
            sql = (
                "SELECT DISTINCT stock_code FROM stock_daily "
                "WHERE period = '1d' AND COALESCE(adjust_type, 'none') = 'none' "
                "ORDER BY stock_code"
            )
            params: list[Any] = []
            if limit is not None:
                sql += " LIMIT ?"
                params.append(limit)
            rows = con.execute(sql, params).fetchall()
            con.close()
            return [str(row[0]) for row in rows if row and row[0]]
        except Exception as exc:
            logger.warning("读取已存储标的列表失败: %s", exc)
            return []

    def _save_summary(self, summary: SymbolAuditSummary):
        """保存审计摘要到 SQLite。"""
        conn = sqlite3.connect(self.audit_db_path)
        conn.execute(
            """
            INSERT OR REPLACE INTO golden_1d_audit
            (symbol, listing_date, local_first_date, local_last_date,
             expected_trading_days, actual_trading_days, missing_days, duplicate_days,
             has_listing_gap, listing_gap_days, cross_source_status,
             cross_source_fields_passed, cross_source_fields_total,
             backfill_status, is_golden_1d_ready, golden_status,
             last_audited_at, audit_version, partition_hashes, issues)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                summary.symbol,
                summary.listing_date,
                summary.local_first_date,
                summary.local_last_date,
                summary.expected_trading_days,
                summary.actual_trading_days,
                summary.missing_days,
                summary.duplicate_days,
                int(summary.has_listing_gap),
                summary.listing_gap_days,
                summary.cross_source_status,
                summary.cross_source_fields_passed,
                summary.cross_source_fields_total,
                summary.backfill_status,
                int(summary.is_golden_1d_ready),
                summary.golden_status,
                summary.last_audited_at,
                summary.audit_version,
                json.dumps(summary.partition_hashes, ensure_ascii=False),
                json.dumps(summary.issues, ensure_ascii=False),
            ),
        )
        conn.commit()
        conn.close()

    def update_backfill_status(
        self,
        symbol: str,
        backfill_status: BackfillStatus,
        note: str | None = None,
    ) -> SymbolAuditSummary:
        """更新单标的回填状态并持久化。

        供后台编排层 / 回填执行器写入 `queued` / `in_progress` / `manual_review`
        等运行态，而不需要重新跑全量审计。
        """
        summary = self.get_audit_status(symbol)
        if summary is None:
            summary = SymbolAuditSummary(symbol=symbol)

        summary.backfill_status = backfill_status
        summary.last_audited_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if note:
            issues = list(summary.issues or [])
            if note not in issues:
                issues.insert(0, note)
            summary.issues = issues[:20]

        self._save_summary(summary)
        return summary

    def get_audit_status(self, symbol: str) -> Optional[SymbolAuditSummary]:
        """查询单个标的的审计状态。"""
        conn = sqlite3.connect(self.audit_db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM golden_1d_audit WHERE symbol = ?", (symbol,)).fetchone()
        conn.close()

        if row is None:
            return None

        return SymbolAuditSummary(
            symbol=row["symbol"],
            listing_date=row["listing_date"],
            local_first_date=row["local_first_date"],
            local_last_date=row["local_last_date"],
            expected_trading_days=row["expected_trading_days"],
            actual_trading_days=row["actual_trading_days"],
            missing_days=row["missing_days"],
            duplicate_days=row["duplicate_days"],
            has_listing_gap=bool(row["has_listing_gap"]),
            listing_gap_days=row["listing_gap_days"],
            cross_source_status=row["cross_source_status"],
            cross_source_fields_passed=row["cross_source_fields_passed"],
            cross_source_fields_total=row["cross_source_fields_total"],
            backfill_status=row["backfill_status"],
            is_golden_1d_ready=bool(row["is_golden_1d_ready"]),
            golden_status=row["golden_status"],
            last_audited_at=row["last_audited_at"],
            audit_version=row["audit_version"],
            partition_hashes=json.loads(row["partition_hashes"]) if row["partition_hashes"] else {},
            issues=json.loads(row["issues"]) if row["issues"] else [],
        )

    def audit_batch(
        self, symbols: list[str], max_workers: int = 4, force_full: bool = False
    ) -> Golden1dAuditReport:
        """批量审计多个标的。"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        report = Golden1dAuditReport(
            audited_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            total_symbols=len(symbols),
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.audit_symbol, s, None, force_full): s for s in symbols
            }
            for future in as_completed(futures):
                try:
                    summary = future.result()
                    report.symbol_summaries.append(summary)
                    if summary.golden_status == "golden":
                        report.golden_count += 1
                    elif summary.golden_status == "partial_trust":
                        report.partial_trust_count += 1
                    elif summary.golden_status == "degraded":
                        report.degraded_count += 1
                    else:
                        report.unknown_count += 1
                except Exception as e:
                    logger.error("审计失败 (%s): %s", futures[future], e)
                    report.unknown_count += 1

        return report
