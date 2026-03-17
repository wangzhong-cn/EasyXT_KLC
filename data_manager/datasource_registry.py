import os
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

import pandas as pd


class DataSource(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def get_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
    ) -> pd.DataFrame:
        raise NotImplementedError

    def health(self) -> dict[str, Any]:
        return {"name": self.name, "available": True}

    def get_metadata(self) -> dict[str, Any]:
        return {"name": self.name}

    def connect(self) -> None:
        return None

    def close(self) -> None:
        return None


class DuckDBSource(DataSource):
    def __init__(self, interface: Any):
        super().__init__("duckdb")
        self.interface = interface

    def get_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
    ) -> pd.DataFrame:
        try:
            if self.interface.con is None and self.interface.duckdb_available:
                self.interface.connect(read_only=True)
        except Exception:
            pass
        if self.interface.con is None:
            return pd.DataFrame()
        try:
            self.interface._ensure_tables_exist()
        except Exception:
            pass
        data = self.interface._read_from_duckdb(symbol, start_date, end_date, period, adjust)
        if data is None:
            return pd.DataFrame()
        return data

    def health(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "available": self.interface.con is not None,
            "duckdb_path": getattr(self.interface, "duckdb_path", ""),
        }


class ParquetSource(DataSource):
    def __init__(self, root_dir: Optional[str] = None):
        super().__init__("parquet")
        self.root_dir = Path(
            root_dir or os.environ.get("EASYXT_RAW_PARQUET_ROOT", "D:/StockData/raw")
        )

    def get_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
    ) -> pd.DataFrame:
        if period != "1d":
            return pd.DataFrame()
        file_path = self.root_dir / "daily" / f"{symbol}.parquet"
        if not file_path.exists():
            return pd.DataFrame()
        try:
            df = pd.read_parquet(file_path)
        except Exception:
            return pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df[df["date"].notna()]
            if start_date:
                df = df[df["date"] >= pd.to_datetime(start_date, errors="coerce")]
            if end_date:
                df = df[df["date"] <= pd.to_datetime(end_date, errors="coerce")]
            df = df.set_index("date")
        return df

    def health(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "available": self.root_dir.exists(),
            "root_dir": str(self.root_dir),
        }


import logging as _logging

_reg_log = _logging.getLogger(__name__)

# OHLCV 字段别名表（各源命名不一致时统一查找）
_OHLCV_ALIASES: dict[str, tuple[str, ...]] = {
    "open":   ("open", "Open", "open_price"),
    "high":   ("high", "High", "high_price"),
    "low":    ("low",  "Low",  "low_price"),
    "close":  ("close", "Close", "close_price"),
    "volume": ("volume", "Volume", "vol"),
}


class DataSourceRegistry:
    def __init__(
        self,
        *,
        required_fields: Optional[list[str]] = None,
        max_nan_rate: float = 0.05,
        min_close_valid_rate: float = 0.50,
        min_date_coverage: float = 0.80,
    ):
        """
        Args:
            required_fields:      必须存在的列名（从别名组中至少找到一列），默认 OHLCV 五列全需要。
            max_nan_rate:         关键列 NaN 占比上限，超过则拒绝此源（默认 5%）。
            min_close_valid_rate: close > 0 行占比下限，低于则拒绝（默认 50%）。
            min_date_coverage:    实际行数 / 期望行数下限，用于检测数据严重缺失（默认 80%）。
                                  设置为 0.0 可禁用此检查。
        """
        self._sources: dict[str, DataSource] = {}
        self._required_fields: list[str] = required_fields if required_fields is not None else list(_OHLCV_ALIASES.keys())
        self._max_nan_rate = max_nan_rate
        self._min_close_valid_rate = min_close_valid_rate
        self._min_date_coverage = min_date_coverage
        # 轻量指标：{source_name: {hits, misses, errors, last_latency_ms}}
        self._metrics: dict[str, dict[str, Any]] = {}
        self._metrics_lock = threading.Lock()

    def register(self, name: str, source: DataSource) -> None:
        self._sources[name] = source
        with self._metrics_lock:
            if name not in self._metrics:
                self._metrics[name] = {
                    "hits": 0, "misses": 0, "errors": 0,
                    "last_latency_ms": None, "quality_rejects": 0,
                }

    def unregister(self, name: str) -> None:
        if name in self._sources:
            del self._sources[name]

    # ------------------------------------------------------------------
    # 数据质量校验（内部，返回 reject reason 或 None）
    # ------------------------------------------------------------------

    def _quality_check(
        self,
        name: str,
        data: pd.DataFrame,
        start_date: str,
        end_date: str,
        period: str,
    ) -> Optional[str]:
        """对单个源返回的 DataFrame 做多维质量校验。
        返回 None 表示通过，返回字符串描述拒绝原因。"""
        # 0. 完全空的 DataFrame 直接拒绝；有列但无 close 列则视为非 OHLCV 数据，跳过质量检查
        if data.empty and len(data.columns) == 0:
            return "空 DataFrame"
        close_col_present = any(a in data.columns for a in _OHLCV_ALIASES["close"])
        if not close_col_present:
            return None  # 非 OHLCV 数据，直接通过

        # 1. 必需字段校验 —— 每组别名中至少找到一个列
        for field in self._required_fields:
            aliases = _OHLCV_ALIASES.get(field, (field,))
            found = any(a in data.columns for a in aliases)
            if not found:
                return f"缺少必要字段 '{field}'（别名: {aliases}）"

        # 2. close 列有效价格占比
        close_col = next((a for a in _OHLCV_ALIASES["close"] if a in data.columns), None)
        if close_col is not None:
            valid_rate = (data[close_col] > 0).mean()
            if valid_rate < self._min_close_valid_rate:
                return (
                    f"close > 0 占比 {valid_rate:.1%} < 阈值 {self._min_close_valid_rate:.0%}"
                )

        # 3. 关键列 NaN 占比校验
        key_cols = [
            c for field in ("close", "volume")
            for c in (_OHLCV_ALIASES.get(field, ()) or ())
            if c in data.columns
        ][:2]  # 最多检查 close + volume
        for col in key_cols:
            nan_rate = data[col].isna().mean()
            if nan_rate > self._max_nan_rate:
                return f"列 '{col}' NaN 占比 {nan_rate:.1%} 超过阈值 {self._max_nan_rate:.0%}"

        # 4. 日期覆盖率：仅日线 + 提供 start/end 时检查
        if self._min_date_coverage > 0 and period == "1d" and start_date and end_date:
            try:
                s = pd.to_datetime(start_date, errors="coerce")
                e = pd.to_datetime(end_date, errors="coerce")
                if s is not pd.NaT and e is not pd.NaT and s < e:
                    # 粗略估算：用 0.7 × 日历天数 作为交易日期望下界（比精确计算快）
                    calendar_days = (e - s).days
                    expected_min = max(1, int(calendar_days * 0.70 * 0.68))  # ~0.476 × 日历天
                    actual_rows = len(data)
                    if actual_rows < expected_min:
                        coverage = actual_rows / max(expected_min, 1)
                        if coverage < self._min_date_coverage:
                            return (
                                f"日期覆盖不足：实际 {actual_rows} 行，期望 ≥ {expected_min} 行"
                                f"（覆盖率 {coverage:.0%} < {self._min_date_coverage:.0%}）"
                            )
            except Exception:
                pass  # 日期解析失败时跳过覆盖率检查

        return None  # 通过全部检查

    def get_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
        preferred_sources: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        order = preferred_sources or list(self._sources.keys())
        for name in order:
            source = self._sources.get(name)
            if source is None:
                continue
            t0 = time.perf_counter()
            try:
                data = source.get_data(symbol, start_date, end_date, period, adjust)
            except Exception:
                data = pd.DataFrame()
                with self._metrics_lock:
                    m = self._metrics.setdefault(name, {"hits": 0, "misses": 0, "errors": 0, "last_latency_ms": None, "quality_rejects": 0})
                    m["errors"] += 1
                    m["last_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            if data is None or data.empty:
                with self._metrics_lock:
                    m = self._metrics.setdefault(name, {"hits": 0, "misses": 0, "errors": 0, "last_latency_ms": None, "quality_rejects": 0})
                    m["misses"] += 1
                    m["last_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
                continue
            reject_reason = self._quality_check(name, data, start_date, end_date, period)
            if reject_reason is not None:
                _reg_log.warning(
                    "DataSourceRegistry: 源 '%s' 数据质量不合格，跳过。symbol=%s reason=%s",
                    name, symbol, reject_reason,
                )
                with self._metrics_lock:
                    m = self._metrics.setdefault(name, {"hits": 0, "misses": 0, "errors": 0, "last_latency_ms": None, "quality_rejects": 0})
                    m["quality_rejects"] += 1
                    m["last_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
                continue
            with self._metrics_lock:
                m = self._metrics.setdefault(name, {"hits": 0, "misses": 0, "errors": 0, "last_latency_ms": None, "quality_rejects": 0})
                m["hits"] += 1
                m["last_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            return data
        return pd.DataFrame()

    def get_metrics(self) -> dict[str, dict[str, Any]]:
        """返回各数据源的命中/未命中/错误统计（线程安全快照）。"""
        with self._metrics_lock:
            return {k: dict(v) for k, v in self._metrics.items()}

    def get_health_summary(self) -> dict[str, Any]:
        summary: dict[str, Any] = {}
        for name, source in self._sources.items():
            try:
                summary[name] = source.health()
            except Exception:
                summary[name] = {"name": name, "available": False}
        return summary


class DATBinarySource(DataSource):
    """DataSourceRegistry 适配器：将 DATBinaryReader 包装为标准 DataSource。

    优先级应设置在 DuckDB 之后、QMT API 之前：
        DuckDB → DAT直读 → QMT API → AKShare

    适用场景：QMT API 不可用（miniquote 崩溃 / 网络断开）但本地 DAT 文件存在。
    不依赖 xtquant，适用于任何 Python 版本。

    注册示例::

        from data_manager.datasource_registry import DATBinarySource
        from data_manager.dat_binary_reader import DATBinaryReader
        registry.register("dat", DATBinarySource(DATBinaryReader()))
    """

    def __init__(self, reader: Any):
        super().__init__("dat_binary")
        self._reader = reader

    def get_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
    ) -> pd.DataFrame:
        if not self._reader.is_available():
            return pd.DataFrame()
        try:
            return self._reader.get_data(symbol, start_date, end_date, period, adjust)
        except Exception:
            return pd.DataFrame()

    def health(self) -> dict[str, Any]:
        try:
            return self._reader.health()
        except Exception:
            return {"name": self.name, "available": False}


class TushareSource(DataSource):
    def __init__(self, interface: Any):
        super().__init__("tushare")
        self.interface = interface

    def get_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
    ) -> pd.DataFrame:
        try:
            return self.interface._read_from_tushare(symbol, start_date, end_date, period)
        except Exception:
            return pd.DataFrame()

    def health(self) -> dict[str, Any]:
        token = getattr(self.interface, "_tushare_token", "") or ""
        return {
            "name": self.name,
            "available": bool(getattr(self.interface, "tushare_available", False)),
            "token_configured": bool(token),
        }


class AKShareSource(DataSource):
    """DataSourceRegistry 适配器：将 AKShare 包装为标准 DataSource。

    在 Tushare 之后作为最终兜底数据源。仅支持 A 股日线（period="1d"）。
    注册示例::

        from data_manager.datasource_registry import AKShareSource
        registry.register("akshare", AKShareSource(interface))
    """

    def __init__(self, interface: Any):
        super().__init__("akshare")
        self.interface = interface

    def get_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
    ) -> pd.DataFrame:
        if not getattr(self.interface, "akshare_available", False):
            return pd.DataFrame()
        # AKShare 仅稳定支持 A 股日线
        if period not in ("1d",):
            return pd.DataFrame()
        try:
            return self.interface._read_from_akshare(symbol, start_date, end_date, period)
        except Exception:
            return pd.DataFrame()

    def health(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "available": bool(getattr(self.interface, "akshare_available", False)),
        }
