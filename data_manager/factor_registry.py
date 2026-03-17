"""
因子注册与管理中心

提供统一的因子注册、版本管理、计算调度和存储接口，
将 alpha_factors.py 等独立脚本纳入平台主系统。

架构::

    FactorDefinition        — 因子元数据（名称、分类、描述、版本）
    FactorRegistry          — 因子注册表（单例），管理注册/查找/列举
    FactorComputeEngine     — 调用已注册的因子计算函数，返回标准化 DataFrame
    FactorStorage           — DuckDB 存储后端（写入、查询）

快速开始::

    from data_manager.factor_registry import factor_registry, FactorDefinition

    # 1. 定义因子
    @factor_registry.register("momentum_20d", category="momentum", version="1.0")
    def momentum_20d(df: pd.DataFrame) -> pd.Series:
        \"\"\"20 日动量因子 = 当日收盘 / 20 日前收盘 - 1\"\"\"
        return df["close"] / df["close"].shift(20) - 1

    # 2. 计算
    engine = FactorComputeEngine(factor_registry)
    result = engine.compute("momentum_20d", df)

    # 3. 存储到 DuckDB
    storage = FactorStorage(db_manager)
    storage.save(symbol="000001.SZ", factor_name="momentum_20d", series=result)

    # 4. 查询
    df = storage.load(symbol="000001.SZ", factor_name="momentum_20d",
                      start_date="2024-01-01", end_date="2024-12-31")
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Factor Metadata
# ---------------------------------------------------------------------------


@dataclass
class FactorDefinition:
    """因子元数据。"""
    name: str                          # 因子唯一标识（小写下划线，如 "momentum_20d"）
    func: Callable[..., pd.Series]    # 计算函数：接受 DataFrame，返回 Series
    category: str = "alpha"           # 分类：momentum / value / quality / volatility / alpha
    description: str = ""
    version: str = "1.0"
    tags: list[str] = field(default_factory=list)
    registered_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "description": self.description,
            "version": self.version,
            "tags": self.tags,
            "registered_at": self.registered_at,
        }


# ---------------------------------------------------------------------------
# Factor Registry
# ---------------------------------------------------------------------------


class FactorRegistry:
    """
    因子注册中心（进程内单例）。

    线程安全（Python GIL 保护字典读写；批量操作无需额外锁）。
    """

    def __init__(self) -> None:
        self._factors: Dict[str, FactorDefinition] = {}

    # ------------------------------------------------------------------
    # 注册
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        *,
        category: str = "alpha",
        description: str = "",
        version: str = "1.0",
        tags: Optional[List[str]] = None,
    ) -> Callable[[Callable[..., pd.Series]], Callable[..., pd.Series]]:
        """
        装饰器：注册因子计算函数。

        用法::

            @factor_registry.register("rsi_14", category="momentum")
            def rsi_14(df: pd.DataFrame) -> pd.Series:
                ...
        """
        def decorator(func: Callable[..., pd.Series]) -> Callable[..., pd.Series]:
            desc = description or (func.__doc__ or "").strip().split("\n")[0]
            defn = FactorDefinition(
                name=name,
                func=func,
                category=category,
                description=desc,
                version=version,
                tags=list(tags or []),
            )
            if name in self._factors:
                log.warning("因子 '%s' 已存在，将被覆盖（旧版本 %s → 新版本 %s）",
                            name, self._factors[name].version, version)
            self._factors[name] = defn
            log.debug("已注册因子: %s (类别=%s, 版本=%s)", name, category, version)
            return func
        return decorator

    def register_func(
        self,
        name: str,
        func: Callable[..., pd.Series],
        *,
        category: str = "alpha",
        description: str = "",
        version: str = "1.0",
        tags: Optional[List[str]] = None,
    ) -> None:
        """非装饰器方式直接注册（批量导入场景）。"""
        desc = description or (func.__doc__ or "").strip().split("\n")[0]
        self._factors[name] = FactorDefinition(
            name=name,
            func=func,
            category=category,
            description=desc,
            version=version,
            tags=list(tags or []),
        )

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get(self, name: str) -> Optional[FactorDefinition]:
        return self._factors.get(name)

    def list_all(self) -> List[Dict[str, Any]]:
        """返回所有已注册因子的元数据列表（不含计算函数）。"""
        return [f.to_dict() for f in sorted(self._factors.values(), key=lambda x: x.name)]

    def list_by_category(self, category: str) -> List[Dict[str, Any]]:
        return [
            f.to_dict() for f in self._factors.values()
            if f.category == category
        ]

    def list_names(self) -> List[str]:
        return sorted(self._factors.keys())

    def __contains__(self, name: str) -> bool:
        return name in self._factors

    def __len__(self) -> int:
        return len(self._factors)

    def unregister(self, name: str) -> bool:
        """注销因子（返回 True 表示成功删除）。"""
        if name in self._factors:
            del self._factors[name]
            return True
        return False


# ---------------------------------------------------------------------------
# Factor Compute Engine
# ---------------------------------------------------------------------------


class FactorComputeEngine:
    """
    因子计算调度器：根据已注册的计算函数批量计算因子值。
    """

    def __init__(self, registry: FactorRegistry) -> None:
        self._registry = registry

    def compute(
        self,
        factor_name: str,
        df: pd.DataFrame,
        **kwargs: Any,
    ) -> pd.Series:
        """
        计算单个因子。

        Args:
            factor_name: 已注册的因子名称。
            df:          包含 OHLCV 等必要列的 DataFrame（按日期升序排列）。
            **kwargs:    透传给因子计算函数的额外参数。

        Returns:
            pd.Series，index 与 df 相同。

        Raises:
            KeyError:   因子未注册。
            Exception:  计算函数内部异常（不吞异常，保持堆栈可追踪）。
        """
        defn = self._registry.get(factor_name)
        if defn is None:
            raise KeyError(f"因子 '{factor_name}' 未在注册中心中找到")
        try:
            result = defn.func(df, **kwargs)
        except Exception:
            log.exception("因子 '%s' 计算失败", factor_name)
            raise
        if not isinstance(result, pd.Series):
            result = pd.Series(result, index=df.index)
        result.name = factor_name
        return result

    def compute_many(
        self,
        factor_names: List[str],
        df: pd.DataFrame,
        errors: str = "raise",
    ) -> pd.DataFrame:
        """
        批量计算多个因子，返回宽表 DataFrame（各因子一列）。

        Args:
            factor_names: 因子名称列表。
            df:           输入 OHLCV DataFrame。
            errors:       "raise" = 遇到错误立即抛出；"skip" = 跳过并记录 warning。

        Returns:
            pd.DataFrame，columns = factor_names（跳过的列不包含在内）。
        """
        results: Dict[str, pd.Series] = {}
        for name in factor_names:
            try:
                results[name] = self.compute(name, df)
            except Exception as exc:
                if errors == "skip":
                    log.warning("计算因子 '%s' 失败，已跳过：%s", name, exc)
                    continue
                raise
        return pd.DataFrame(results, index=df.index)


# ---------------------------------------------------------------------------
# Factor Storage（DuckDB 后端）
# ---------------------------------------------------------------------------


class FactorStorage:
    """
    因子值存储后端：使用 DuckDB 持久化因子序列。

    表结构::

        CREATE TABLE IF NOT EXISTS factor_values (
            symbol      VARCHAR NOT NULL,
            factor_name VARCHAR NOT NULL,
            date        DATE    NOT NULL,
            value       DOUBLE,
            version     VARCHAR DEFAULT '1.0',
            saved_at    TIMESTAMP DEFAULT now(),
            PRIMARY KEY (symbol, factor_name, date)
        )
    """

    _CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS factor_values (
            symbol      VARCHAR  NOT NULL,
            factor_name VARCHAR  NOT NULL,
            date        DATE     NOT NULL,
            value       DOUBLE,
            version     VARCHAR  DEFAULT '1.0',
            saved_at    TIMESTAMP DEFAULT now(),
            PRIMARY KEY (symbol, factor_name, date)
        )
    """

    def __init__(self, db_manager: Any) -> None:
        """
        Args:
            db_manager: DuckDB 连接管理器（与 duckdb_connection_pool.DuckDBManager 接口兼容）。
                        需提供 execute(sql, params) 和 query(sql, params) 方法。
        """
        self._db = db_manager
        self._ensure_table()

    def _ensure_table(self) -> None:
        try:
            self._db.execute(self._CREATE_TABLE)
        except Exception:
            log.exception("FactorStorage: 创建因子值表失败")

    def save(
        self,
        symbol: str,
        factor_name: str,
        series: pd.Series,
        version: str = "1.0",
        if_exists: str = "replace",
    ) -> int:
        """
        将因子序列写入 DuckDB。

        Args:
            symbol:      股票代码，如 "000001.SZ"。
            factor_name: 因子名称。
            series:      pd.Series，index 为日期（datetime-like 或 date string）。
            version:     因子版本号。
            if_exists:   "replace" = upsert（默认）；"skip" = 已有则跳过。

        Returns:
            写入行数。
        """
        if series is None or series.empty:
            return 0

        rows = []
        for dt, val in series.items():
            if pd.isna(val):
                continue
            try:
                date_str = pd.to_datetime(dt).strftime("%Y-%m-%d")
            except Exception:
                continue
            rows.append((symbol, factor_name, date_str, float(val), version))

        if not rows:
            return 0

        if if_exists == "replace":
            sql = """
                INSERT INTO factor_values (symbol, factor_name, date, value, version)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (symbol, factor_name, date) DO UPDATE SET
                    value    = excluded.value,
                    version  = excluded.version,
                    saved_at = now()
            """
        else:
            sql = """
                INSERT OR IGNORE INTO factor_values (symbol, factor_name, date, value, version)
                VALUES (?, ?, ?, ?, ?)
            """

        written = 0
        for row in rows:
            try:
                self._db.execute(sql, row)
                written += 1
            except Exception:
                log.warning("写入因子值失败 symbol=%s factor=%s date=%s", row[0], row[1], row[2])

        return written

    def load(
        self,
        symbol: str,
        factor_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.Series:
        """
        从 DuckDB 加载因子序列。

        Returns:
            pd.Series（index=date, name=factor_name），如无数据则返回空 Series。
        """
        conditions = ["symbol = ?", "factor_name = ?"]
        params: list[Any] = [symbol, factor_name]
        if start_date:
            conditions.append("date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("date <= ?")
            params.append(end_date)

        sql = (
            "SELECT date, value FROM factor_values WHERE "
            + " AND ".join(conditions)
            + " ORDER BY date"
        )
        try:
            df = self._db.query(sql, params)
        except Exception:
            log.exception("FactorStorage.load 查询失败 symbol=%s factor=%s", symbol, factor_name)
            return pd.Series(name=factor_name)

        if df is None or df.empty:
            return pd.Series(name=factor_name)

        df["date"] = pd.to_datetime(df["date"])
        return df.set_index("date")["value"].rename(factor_name)

    def list_available(
        self,
        symbol: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        列出 DuckDB 中已存储的因子信息。

        Returns:
            DataFrame with columns [symbol, factor_name, date_from, date_to, row_count, version]。
        """
        if symbol:
            sql = """
                SELECT symbol, factor_name, min(date) as date_from, max(date) as date_to,
                       count(*) as row_count, max(version) as version
                FROM factor_values WHERE symbol = ?
                GROUP BY symbol, factor_name ORDER BY factor_name
            """
            params: Any = [symbol]
        else:
            sql = """
                SELECT symbol, factor_name, min(date) as date_from, max(date) as date_to,
                       count(*) as row_count, max(version) as version
                FROM factor_values
                GROUP BY symbol, factor_name ORDER BY symbol, factor_name
            """
            params = []
        try:
            result = self._db.query(sql, params)
            return result if result is not None else pd.DataFrame()
        except Exception:
            log.exception("FactorStorage.list_available 失败")
            return pd.DataFrame()


# ---------------------------------------------------------------------------
# 全局单例
# ---------------------------------------------------------------------------

#: 平台全局因子注册中心（模块级单例，import 后即可使用 @factor_registry.register(...)）
factor_registry = FactorRegistry()

#: 对应的计算引擎（绑定全局注册中心）
factor_compute_engine = FactorComputeEngine(factor_registry)


# ---------------------------------------------------------------------------
# DuckDBConnectionManager 适配器（将连接池接口适配为 FactorStorage 期望的接口）
# ---------------------------------------------------------------------------

class _DuckDBManagerAdapter:
    """
    将 DuckDBConnectionManager（execute_read_query / execute_write_query）
    适配为 FactorStorage 所需的 execute(sql, params) / query(sql, params) 接口。
    """

    def __init__(self, db_manager: Any) -> None:
        self._mgr = db_manager

    def execute(self, sql: str, params: Any = None) -> None:
        if params is not None:
            p = tuple(params) if not isinstance(params, tuple) else params
            self._mgr.execute_write_query(sql, p)
        else:
            self._mgr.execute_write_query(sql)

    def query(self, sql: str, params: Any = None) -> "pd.DataFrame":
        if params is not None:
            p = tuple(params) if not isinstance(params, tuple) else params
            return self._mgr.execute_read_query(sql, p)
        return self._mgr.execute_read_query(sql)


def make_factor_storage(db_manager: Any) -> "FactorStorage":
    """
    工厂函数：根据 db_manager 类型自动选择适配层，返回可用的 FactorStorage。

    兼容两种 db_manager：
    - DuckDBConnectionManager（连接池，生产环境）
    - 直接 duckdb.DuckDBPyConnection（测试场景）
    """
    if hasattr(db_manager, "execute_write_query"):
        # 连接池接口 → 需要适配
        return FactorStorage(_DuckDBManagerAdapter(db_manager))
    # 直接连接接口（有 execute / df 方法）→ 包一层简单适配
    class _DirectConnAdapter:
        def __init__(self, con: Any) -> None:
            self._con = con

        def execute(self, sql: str, params: Any = None) -> None:
            if params is not None:
                self._con.execute(sql, list(params))
            else:
                self._con.execute(sql)

        def query(self, sql: str, params: Any = None) -> "pd.DataFrame":
            if params is not None:
                return self._con.execute(sql, list(params)).df()
            return self._con.execute(sql).df()

    return FactorStorage(_DirectConnAdapter(db_manager))
