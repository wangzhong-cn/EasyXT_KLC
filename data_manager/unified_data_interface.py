#!/usr/bin/env python3
"""
统一数据接口
实现DuckDB和QMT数据源的统一管理
优先使用DuckDB本地数据，自动回退到QMT在线数据，并自动保存到DuckDB

参考文档：docs/DUCKDB_COMPARISON_ANALYSIS.md
"""

import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
import uuid
import warnings
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from data_manager.dat_binary_reader import DATBinaryReader
    from data_manager.datasource_registry import (
        AKShareSource,
        DataSourceRegistry,
        DATBinarySource,
        DuckDBSource,
        ParquetSource,
        TushareSource,
    )
    from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
    from data_manager.timestamp_utils import qmt_ms_to_beijing  # P0 时间戳契约层
except ModuleNotFoundError:
    _project_root = str(Path(__file__).resolve().parents[1])
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)
    from data_manager.dat_binary_reader import DATBinaryReader
    from data_manager.datasource_registry import (
        AKShareSource,
        DataSourceRegistry,
        DATBinarySource,
        DuckDBSource,
        ParquetSource,
        TushareSource,
    )
    from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
    from data_manager.timestamp_utils import qmt_ms_to_beijing  # P0 时间戳契约层

# 血缘字段版本号 — 升版本策略见 docs/lineage_spec.md §二
# v1.1 (2026-03): 新增财务表 financial_income / financial_balance / financial_cashflow
#                 支持 QMT 主路径 + Tushare 降级路径；auto_data_updater 收盘后 20 分钟调度
# v1.2 (2026-03): 引入 sequence_id + watermark 晚到治理字段与多周期重建审计回执
CURRENT_SCHEMA_VERSION = "1.2"

warnings.filterwarnings("ignore")


class UnifiedDataInterface:
    _table_init_lock = threading.Lock()

    # 跨实例结果缓存——每个 _ChartDataLoadThread 都会新建 UnifiedDataInterface，
    # 类级缓存确保切换股票再切回后无需重新计算自定义周期 K 线。
    _result_cache: dict = {}
    _result_cache_lock: threading.Lock = threading.Lock()
    _RESULT_CACHE_TTL_S: float = 300.0    # 5 分钟 TTL
    _RESULT_CACHE_MAX_ENTRIES: int = 80   # LRU 淘汰上限

    @classmethod
    def _cache_get(cls, key: tuple) -> "pd.DataFrame | None":
        with cls._result_cache_lock:
            entry = cls._result_cache.get(key)
            if entry is None:
                return None
            cached_at, df = entry
            if time.time() - cached_at > cls._RESULT_CACHE_TTL_S:
                del cls._result_cache[key]
                return None
            # 移到末尾（LRU 顺序）
            cls._result_cache[key] = entry
            return df.copy()

    @classmethod
    def _cache_put(cls, key: tuple, df: "pd.DataFrame") -> None:
        with cls._result_cache_lock:
            if key in cls._result_cache:
                del cls._result_cache[key]
            while len(cls._result_cache) >= cls._RESULT_CACHE_MAX_ENTRIES:
                cls._result_cache.pop(next(iter(cls._result_cache)))
            cls._result_cache[key] = (time.time(), df.copy())

    @classmethod
    def _cache_invalidate(cls, duckdb_path: str, stock_code: str, period: str) -> None:
        """按 (duckdb_path, stock_code, period) 使相关缓存失效（写入新数据后调用）。"""
        prefix = (duckdb_path, stock_code, period)
        with cls._result_cache_lock:
            stale = [k for k in cls._result_cache if k[:3] == prefix]
            for k in stale:
                del cls._result_cache[k]

    """
    统一数据接口

    功能：
    1. 优先从DuckDB读取（包含五维复权，速度快）
    2. 如无数据或数据不全，使用QMT在线获取
    3. 获取后自动保存到DuckDB
    4. 智能检测缺失数据
    5. 支持多种复权类型
    """

    def __init__(
        self,
        duckdb_path: str | None = None,
        eager_init: bool = False,
        silent_init: bool = True,
        cb_fail_threshold: int | None = None,
        backoff_base_s: float | None = None,
        backoff_max_s: float | None = None,
    ):
        """
        初始化统一数据接口

        Args:
            duckdb_path: DuckDB数据库路径
        """
        from data_manager.duckdb_connection_pool import resolve_duckdb_path

        self.duckdb_path = resolve_duckdb_path(duckdb_path)
        self.con: Any = None
        self._read_only_connection = False
        self.duckdb_available = False
        self.qmt_available = False
        self.akshare_available = False
        self.tushare_available = False
        self._tables_initialized = False  # 记录表是否已初始化

        self._silent_init = silent_init
        self._duckdb_checked = False
        self._qmt_checked = False
        self._akshare_checked = False
        self._tushare_checked = False
        self.adjustment_manager = None
        self.data_registry = DataSourceRegistry()
        self.data_registry.register("duckdb", DuckDBSource(self))
        self.data_registry.register("dat", DATBinarySource(DATBinaryReader()))
        self.data_registry.register("parquet", ParquetSource())
        self.data_registry.register("tushare", TushareSource(self))
        self.data_registry.register("akshare", AKShareSource(self))
        self._tushare_token = (
            os.environ.get("EASYXT_TUSHARE_TOKEN", "").strip()
            or os.environ.get("TUSHARE_TOKEN", "").strip()
        )
        env_fail_threshold = os.environ.get("EASYXT_REMOTE_CB_THRESHOLD")
        env_base = os.environ.get("EASYXT_REMOTE_BACKOFF_BASE_S")
        env_max = os.environ.get("EASYXT_REMOTE_BACKOFF_MAX_S")
        fail_threshold = cb_fail_threshold
        if fail_threshold is None and env_fail_threshold is not None:
            try:
                fail_threshold = int(env_fail_threshold)
            except Exception:
                fail_threshold = None
        if fail_threshold is None:
            fail_threshold = 5
        base_s = backoff_base_s
        if base_s is None and env_base is not None:
            try:
                base_s = float(env_base)
            except Exception:
                base_s = None
        if base_s is None:
            base_s = 3.0
        max_s = backoff_max_s
        if max_s is None and env_max is not None:
            try:
                max_s = float(env_max)
            except Exception:
                max_s = None
        if max_s is None:
            max_s = 300.0
        self._cb_state = {
            "open": False,
            "fail_count": 0,
            "opened_at": 0.0,
            "cooldown_s": 0.0,
            "base_s": float(base_s),
            "max_s": float(max_s),
            "fail_threshold": int(fail_threshold),
        }
        self._cb_disabled = False  # 批量入库时可临时禁用熔断
        self._skip_third_party_fallback = False  # 批量入库时跳过 Tushare/AKShare 回退
        self._cache_stale_quarantine_enabled = (
            str(os.environ.get("EASYXT_CACHE_STALE_QUARANTINE_ENABLED", "1")).lower()
            in ("1", "true", "yes", "on")
        )
        try:
            _sample_rate = float(os.environ.get("EASYXT_STEP6_VALIDATE_SAMPLE_RATE", "1.0"))
        except Exception:
            _sample_rate = 1.0
        self._step6_validate_sample_rate = max(0.0, min(1.0, _sample_rate))
        self._step6_validation_metrics: dict[str, Any] = {
            "total": 0,
            "sampled": 0,
            "skipped": 0,
            "hard_failed": 0,
            "quarantined": 0,
            "sample_rate": self._step6_validate_sample_rate,
        }
        self._canary_shadow_write_enabled = (
            str(os.environ.get("EASYXT_CANARY_SHADOW_WRITE", "0")).lower()
            in ("1", "true", "yes", "on")
        )
        self._canary_shadow_only = (
            str(os.environ.get("EASYXT_CANARY_SHADOW_ONLY", "1")).lower()
            in ("1", "true", "yes", "on")
        )
        self._backfill_enabled = os.environ.get("EASYXT_BACKFILL_ENABLED", "1") in (
            "1",
            "true",
            "True",
        )
        self._backfill_scheduler = None
        self._backfill_max_queue = int(os.environ.get("EASYXT_BACKFILL_MAX_QUEUE", "512"))
        self._logger = logging.getLogger(__name__)
        # 最近一次 get_stock_data() 的合约验证结论（供调用方读取）
        self._last_contract_validation: object | None = None
        # 因子引擎懒初始化（connect() 后首次调用因子 API 时激活）
        self._factor_storage: Any | None = None
        # 上市首日缓存：{stock_code: 'YYYY-MM-DD'}（多日周期左对齐必需）
        self._listing_date_cache: dict[str, str] = {}
        if eager_init:
            self._ensure_adjustment_manager()
            self._check_duckdb()
            self._check_qmt()
            self._check_akshare()
            self._check_tushare()

    def _ensure_backfill_scheduler(self):
        if not self._backfill_enabled:
            return
        if self._backfill_scheduler is not None:
            return
        try:
            from data_manager.history_backfill_scheduler import HistoryBackfillScheduler

            self._backfill_scheduler = HistoryBackfillScheduler(
                worker=self._run_backfill_task,
                max_queue_size=self._backfill_max_queue,
            )
            self._backfill_scheduler.start()
        except Exception:
            self._backfill_scheduler = None
            self._logger.exception("初始化HistoryBackfillScheduler失败")

    def schedule_backfill(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        period: str = "1d",
        priority: int | None = None,
        reason: str = "manual",
        current_symbol: str = "",
        gap_length: int | None = None,
    ) -> bool:
        if not self._backfill_enabled:
            return False
        self._ensure_backfill_scheduler()
        if self._backfill_scheduler is None:
            return False
        if priority is None:
            priority = self._compute_backfill_priority(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                period=period,
                current_symbol=current_symbol,
                gap_length=gap_length,
            )
        queued = self._backfill_scheduler.schedule(
            stock_code=stock_code,
            start_date=start_date,
            end_date=end_date,
            period=period,
            priority=priority,
            reason=reason,
        )
        if queued:
            try:
                self._record_ingestion_status(
                    stock_code=stock_code,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    source="backfill",
                    status="queued",
                    record_count=0,
                    error_message=None,
                )
            except Exception as _ing_err:
                self._logger.warning("记录ingestion_status(queued)失败: %s", _ing_err)
            try:
                from core.events import Events
                from core.signal_bus import signal_bus

                signal_bus.emit(
                    Events.BACKFILL_TASK_UPDATED,
                    stock_code=stock_code,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    status="queued",
                    record_count=0,
                    error_message=None,
                )
            except Exception:
                pass
        return queued

    def _compute_backfill_priority(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        period: str,
        current_symbol: str = "",
        gap_length: int | None = None,
    ) -> int:
        gap = int(gap_length or 0)
        if gap <= 0:
            try:
                start_ts = pd.to_datetime(start_date)
                end_ts = pd.to_datetime(end_date)
                gap = max((end_ts - start_ts).days, 1)
            except Exception:
                gap = 1
        gap_weight = min(gap / 100.0, 1.0)
        current_weight = 2.0 if current_symbol and stock_code == current_symbol else 1.0
        priority = int(100 - gap_weight * current_weight * 50)
        return max(priority, 0)

    def _run_backfill_task(self, task: dict[str, Any]) -> bool:
        stock_code = str(task.get("stock_code") or "").strip()
        start_date = str(task.get("start_date") or "").strip()
        end_date = str(task.get("end_date") or "").strip()
        period = str(task.get("period") or "1d").strip()
        if not stock_code or not start_date or not end_date:
            return False

        def _emit_backfill_event(status: str, record_count: int = 0, error_message: str | None = None):
            try:
                from core.events import Events
                from core.signal_bus import signal_bus

                signal_bus.emit(
                    Events.BACKFILL_TASK_UPDATED,
                    stock_code=stock_code,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    status=status,
                    record_count=record_count,
                    error_message=error_message,
                )
            except Exception:
                pass

        worker = UnifiedDataInterface(
            duckdb_path=self.duckdb_path,
            eager_init=False,
            silent_init=True,
            cb_fail_threshold=self._cb_state.get("fail_threshold", 5),
            backoff_base_s=self._cb_state.get("base_s", 3.0),
            backoff_max_s=self._cb_state.get("max_s", 300.0),
        )
        try:
            worker.connect(read_only=False)
            worker._ensure_tables_exist()
            worker._record_ingestion_status(
                stock_code=stock_code,
                period=period,
                start_date=start_date,
                end_date=end_date,
                source="backfill",
                status="running",
                record_count=0,
                error_message=None,
            )
            _emit_backfill_event(status="running")
            worker._check_qmt()

            data = None
            if worker.qmt_available:
                try:
                    data = worker._read_from_qmt(stock_code, start_date, end_date, period)
                except Exception:
                    data = None

            if data is None or data.empty:
                try:
                    data = worker._read_from_akshare(stock_code, start_date, end_date, period)
                except Exception:
                    data = None

            if data is None or data.empty:
                worker._record_ingestion_status(
                    stock_code=stock_code,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    source="backfill",
                    status="failed",
                    record_count=0,
                    error_message="empty_result",
                )
                _emit_backfill_event(status="failed", error_message="empty_result")
                return False

            worker._save_to_duckdb(
                data, stock_code, period,
                _ingest_source="backfill",
                _ingest_start=start_date,
                _ingest_end=end_date,
            )  # ingestion_status(success) 已在同一事务内原子写入
            _emit_backfill_event(status="success", record_count=len(data))
            return True
        except Exception:
            self._logger.exception(
                "后台补数失败: %s %s~%s %s", stock_code, start_date, end_date, period
            )
            try:
                worker._record_ingestion_status(
                    stock_code=stock_code,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    source="backfill",
                    status="failed",
                    record_count=0,
                    error_message="exception",
                )
            except Exception as _ing_err:
                self._logger.warning("记录ingestion_status(failed)失败: %s", _ing_err)
            _emit_backfill_event(status="failed", error_message="exception")
            return False
        finally:
            try:
                worker.close()
            except Exception:
                pass

    def _log(self, msg: str):
        if not self._silent_init:
            print(msg)

    def _cb_allow(self) -> bool:
        if self._cb_disabled:
            return True
        if not self._cb_state["open"]:
            return True
        elapsed = time.perf_counter() - self._cb_state["opened_at"]
        if elapsed >= self._cb_state["cooldown_s"]:
            self._cb_state["open"] = False
            return True
        return False

    def _cb_on_success(self):
        self._cb_state["open"] = False
        self._cb_state["fail_count"] = 0
        self._cb_state["cooldown_s"] = 0.0

    def _cb_on_failure(self):
        fc = self._cb_state["fail_count"] + 1
        self._cb_state["fail_count"] = fc
        threshold = self._cb_state.get("fail_threshold", 5)
        if fc < threshold:
            self._log(f"[BACKOFF] 远程数据源失败 {fc} 次，未达短路阈值 {threshold}")
            return
        base = max(self._cb_state.get("base_s", 3.0), 0.5)
        maxv = max(self._cb_state.get("max_s", 300.0), base)
        cooldown = min(base * (2 ** (fc - 1)), maxv)
        self._cb_state["cooldown_s"] = cooldown
        self._cb_state["opened_at"] = time.perf_counter()
        self._cb_state["open"] = True
        self._log(f"[BACKOFF] 远程数据源失败 {fc} 次，短路 {cooldown:.1f}s")

    def _ensure_adjustment_manager(self):
        if self.adjustment_manager is None:
            self.adjustment_manager = FiveFoldAdjustmentManager(self.duckdb_path)
        # Must connect so _db is set; without this get_data_with_adjustment
        # immediately returns pd.DataFrame() and daily data is always re-fetched.
        if self.adjustment_manager._db is None:
            self.adjustment_manager.connect()

    def _check_duckdb(self):
        if self._duckdb_checked:
            return
        self._duckdb_checked = True
        try:
            import duckdb

            _ = duckdb
            self.duckdb_available = True
            self._log("[INFO] DuckDB 可用")
        except ImportError:
            self.duckdb_available = False
            self._log("[WARNING] DuckDB 不可用，将仅使用QMT数据")

    def _check_akshare(self):
        if self._akshare_checked:
            return
        self._akshare_checked = True
        try:
            import akshare as ak
            ver = getattr(ak, "__version__", "unknown")
            self.akshare_available = True
            self._log(f"[INFO] AKShare 可用 (v{ver})")
        except Exception as e:
            self.akshare_available = False
            self._log(f"[WARNING] AKShare 不可用: {e}")

    def _check_tushare(self):
        if self._tushare_checked:
            return
        self._tushare_checked = True
        if not self._tushare_token:
            self.tushare_available = False
            self._log("[INFO] Tushare token 未配置，跳过")
            return
        try:
            import tushare as ts

            _ = ts
            self.tushare_available = True
            self._log("[INFO] Tushare 可用")
        except Exception as e:
            self.tushare_available = False
            self._log(f"[WARNING] Tushare 不可用: {e}")

    def _check_qmt(self):
        if self._qmt_checked:
            return
        self._qmt_checked = True
        # 若 QMT 在线模式被禁用，跳过 xtdata 导入以防止 native 崩溃
        if os.environ.get("EASYXT_ENABLE_QMT_ONLINE", "1") not in ("1", "true", "True"):
            self.qmt_available = False
            return
        project_root = Path(__file__).resolve().parent.parent
        xtquant_path = project_root / "xtquant"
        if xtquant_path.exists() and str(xtquant_path) not in sys.path:
            sys.path.insert(0, str(xtquant_path))
        extra_xtquant_dir = self._ensure_qmt_paths()
        retry_count = max(1, int(os.environ.get("EASYXT_QMT_CHECK_RETRY", "3")))
        retry_sleep = max(0.0, float(os.environ.get("EASYXT_QMT_CHECK_RETRY_SLEEP", "0.6")))
        last_err: Exception | None = None
        for i in range(retry_count):
            try:
                import xtquant as _xtquant_pkg

                if extra_xtquant_dir and extra_xtquant_dir not in _xtquant_pkg.__path__:
                    _xtquant_pkg.__path__.append(extra_xtquant_dir)

                from xtquant import xtdata

                _ = xtdata
                self.qmt_available = True
                self._log("[INFO] QMT xtdata 可用")
                return
            except Exception as e:  # 捕获 ImportError、OSError（pyd加载）和 xtdatacenter rpc_init 抛出的 Exception
                last_err = e
                self.qmt_available = False
                if i < retry_count - 1:
                    time.sleep(retry_sleep)
                    continue
        self._log(f"[WARNING] QMT xtdata 不可用: {last_err}")

    def _refresh_qmt_status(self):
        self._qmt_checked = False
        self._check_qmt()

    def _ensure_qmt_paths(self) -> str | None:
        candidates = []
        env_path = os.environ.get("XTQUANT_PATH") or os.environ.get("QMT_PATH")
        if env_path:
            candidates.append(env_path)
        config_obj: Any | None = None
        try:
            from easy_xt.config import config as config_obj
        except Exception:
            config_obj = None
        if config_obj is not None:
            for key in ("settings.account.qmt_path", "trade.userdata_path", "qmt.detected_path"):
                value = config_obj.get(key)
                if value:
                    base = value
                    if str(value).lower().endswith(".exe"):
                        base = os.path.dirname(os.path.dirname(value))
                    if "userdata" in value.lower():
                        base = os.path.dirname(value)
                    candidates.extend(
                        [
                            base,
                            os.path.dirname(base),
                            os.path.join(base, "bin"),
                            os.path.join(base, "bin.x64"),
                            os.path.join(base, "python"),
                            os.path.join(base, "python", "Lib", "site-packages"),
                            os.path.join(base, "Lib", "site-packages"),
                            os.path.join(base, "lib"),
                            os.path.join(base, "lib", "site-packages"),
                            os.path.join(base, "xtquant"),
                        ]
                    )
        found_root = None
        found_xtquant_dir = None
        for path in candidates:
            found = self._find_qmt_python_root(path)
            if found:
                found_root = found
                candidate_xtquant_dir = os.path.join(found, "xtquant")
                if os.path.isdir(candidate_xtquant_dir):
                    found_xtquant_dir = candidate_xtquant_dir
                break
        if found_root and found_root not in sys.path:
            sys.path.insert(0, found_root)
        return found_xtquant_dir

    def _find_qmt_python_root(self, root: str) -> str | None:
        if not root or not os.path.isdir(root):
            return None
        if os.path.basename(root).lower() == "xtquant":
            if os.path.exists(os.path.join(root, "__init__.py")):
                return os.path.dirname(root)
        root_depth = root.rstrip(os.sep).count(os.sep)
        for dirpath, dirnames, filenames in os.walk(root):
            if "xtpythonclient.pyd" in filenames or "xtpythonclient.dll" in filenames:
                return dirpath
            if "xtquant" in dirnames:
                xtquant_dir = os.path.join(dirpath, "xtquant")
                if os.path.exists(os.path.join(xtquant_dir, "__init__.py")):
                    return dirpath
            if dirpath.count(os.sep) - root_depth >= 6:
                dirnames[:] = []
        return None

    def connect(self, read_only: bool = False):
        """
        连接DuckDB数据库

        Args:
            read_only: 是否只读模式（首次建表需要写权限）

        修复：首次使用时允许写模式以创建表
        """
        self._check_duckdb()
        if not self.duckdb_available:
            return False

        try:
            import duckdb

            from data_manager.duckdb_connection_pool import get_db_manager

            # 确保目录存在
            Path(self.duckdb_path).parent.mkdir(parents=True, exist_ok=True)

            self._db_manager = get_db_manager(self.duckdb_path)
            prefer_rw = os.environ.get("EASYXT_DUCKDB_PREFER_RW", "1") in ("1", "true", "True")
            # :memory: 数据库不支持只读模式，始终使用读写连接
            is_memory_db = (self.duckdb_path == ":memory:")
            effective_read_only = bool(read_only and (not prefer_rw) and (not is_memory_db))
            # 通过连接池创建连接，享受重试与路径归一化
            try:
                self.con = duckdb.connect(self._db_manager.duckdb_path, read_only=effective_read_only)
                self._read_only_connection = bool(effective_read_only)
            except Exception as connect_error:
                msg = str(connect_error).lower()
                if "failure while replaying wal file" in msg:
                    repaired = bool(self._db_manager.repair_wal_if_needed())
                    if repaired:
                        self.con = duckdb.connect(
                            self._db_manager.duckdb_path, read_only=effective_read_only
                        )
                        self._read_only_connection = bool(effective_read_only)
                    else:
                        raise
                elif (
                    (not effective_read_only)
                    and "different configuration than existing connections" in msg
                ):
                    self._logger.warning("检测到连接配置冲突，重试共享读写连接")
                    self.con = duckdb.connect(self._db_manager.duckdb_path, read_only=False)
                    self._read_only_connection = False
                else:
                    raise

            # 配置性能
            self.con.execute("PRAGMA threads=4")
            self.con.execute("PRAGMA memory_limit='4GB'")

            self._logger.debug("DuckDB 连接成功")
            return True
        except Exception as e:
            self._logger.error("DuckDB 连接失败: %s", e)
            self.con = None
            return False

    def _close_duckdb_connection(self):
        """关闭底层 DuckDB 连接，释放文件句柄（内部辅助方法）"""
        if self.con:
            try:
                self.con.close()
            except Exception:
                pass
            self.con = None
            self._read_only_connection = False

    def _ensure_tables_exist(self):
        """确保所有必需的表都存在

        修复：首次使用时自动创建表，避免"Table does not exist"错误
        """
        if not self.con or self._tables_initialized:
            return
        if self._read_only_connection:
            self._tables_initialized = True
            return

        try:
            with self._table_init_lock:
                if not self.con or self._tables_initialized:
                    return
            # 创建 stock_daily 表（日线）
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS stock_daily (
                    stock_code VARCHAR NOT NULL,
                    symbol_type VARCHAR NOT NULL,
                    date DATE NOT NULL,
                    period VARCHAR NOT NULL,
                    open DECIMAL(18, 6),
                    high DECIMAL(18, 6),
                    low DECIMAL(18, 6),
                    close DECIMAL(18, 6),
                    volume BIGINT,
                    amount DECIMAL(24, 6),
                    adjust_type VARCHAR DEFAULT 'none',
                    factor DECIMAL(18, 6) DEFAULT 1.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stock_code, date, period, adjust_type)
                )
                """)

            # 创建 stock_1m 表（1分钟线）
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS stock_1m (
                    stock_code VARCHAR NOT NULL,
                    symbol_type VARCHAR NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    period VARCHAR NOT NULL,
                    open DECIMAL(18, 6),
                    high DECIMAL(18, 6),
                    low DECIMAL(18, 6),
                    close DECIMAL(18, 6),
                    volume BIGINT,
                    amount DECIMAL(24, 6),
                    adjust_type VARCHAR DEFAULT 'none',
                    factor DECIMAL(18, 6) DEFAULT 1.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stock_code, datetime, period, adjust_type)
                )
                """)

            # 创建 stock_5m 表（5分钟线）
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS stock_5m (
                    stock_code VARCHAR NOT NULL,
                    symbol_type VARCHAR NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    period VARCHAR NOT NULL,
                    open DECIMAL(18, 6),
                    high DECIMAL(18, 6),
                    low DECIMAL(18, 6),
                    close DECIMAL(18, 6),
                    volume BIGINT,
                    amount DECIMAL(24, 6),
                    adjust_type VARCHAR DEFAULT 'none',
                    factor DECIMAL(18, 6) DEFAULT 1.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stock_code, datetime, period, adjust_type)
                )
                """)

            # 创建 stock_tick 表（tick数据）
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS stock_tick (
                    stock_code VARCHAR NOT NULL,
                    symbol_type VARCHAR NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    period VARCHAR NOT NULL,
                    open DECIMAL(18, 6),
                    high DECIMAL(18, 6),
                    low DECIMAL(18, 6),
                    close DECIMAL(18, 6),
                    volume BIGINT,
                    amount DECIMAL(24, 6),
                    adjust_type VARCHAR DEFAULT 'none',
                    factor DECIMAL(18, 6) DEFAULT 1.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stock_code, datetime, period, adjust_type)
                )
                """)

                self.con.execute("""
                CREATE TABLE IF NOT EXISTS stock_transaction (
                    stock_code VARCHAR NOT NULL,
                    symbol_type VARCHAR NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    period VARCHAR NOT NULL,
                    price DECIMAL(18, 6),
                    volume BIGINT,
                    amount DECIMAL(24, 6),
                    side VARCHAR,
                    bs_flag VARCHAR,
                    trade_id BIGINT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stock_code, datetime, trade_id, price, volume)
                )
                """)

                self.con.execute("""
                CREATE TABLE IF NOT EXISTS data_ingestion_status (
                    stock_code VARCHAR NOT NULL,
                    period VARCHAR NOT NULL,
                    start_date TIMESTAMP,
                    end_date TIMESTAMP,
                    source VARCHAR,
                    status VARCHAR,
                    record_count INTEGER,
                    error_message VARCHAR,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    schema_version VARCHAR DEFAULT '1.0',
                    ingest_run_id VARCHAR,
                    raw_hash VARCHAR,
                    source_event_time TIMESTAMP,
                    PRIMARY KEY (stock_code, period)
                )
                """)
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS source_conflict_audit (
                    stock_code VARCHAR NOT NULL,
                    period VARCHAR NOT NULL,
                    event_ts TIMESTAMP NOT NULL,
                    source_primary VARCHAR NOT NULL,
                    source_secondary VARCHAR NOT NULL,
                    close_primary DECIMAL(18, 6),
                    close_secondary DECIMAL(18, 6),
                    delta_pct DOUBLE,
                    decision VARCHAR,
                    trace_id VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stock_code, period, event_ts, source_primary, source_secondary)
                )
                """)

                self.con.execute("""
                CREATE TABLE IF NOT EXISTS write_audit_log (
                    audit_id VARCHAR NOT NULL,
                    table_name VARCHAR NOT NULL,
                    stock_code VARCHAR NOT NULL,
                    period VARCHAR NOT NULL,
                    expected_rows INTEGER NOT NULL,
                    actual_rows INTEGER,
                    date_min VARCHAR,
                    date_max VARCHAR,
                    raw_hash VARCHAR,
                    pre_gate_pass BOOLEAN,
                    contract_pass BOOLEAN,
                    post_verify_pass BOOLEAN,
                    error_message VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (audit_id)
                )
                """)
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS data_quarantine_log (
                    quarantine_id VARCHAR NOT NULL,
                    audit_id VARCHAR,
                    table_name VARCHAR NOT NULL,
                    stock_code VARCHAR NOT NULL,
                    period VARCHAR NOT NULL,
                    reason VARCHAR NOT NULL,
                    expected_rows INTEGER,
                    actual_rows INTEGER,
                    date_min VARCHAR,
                    date_max VARCHAR,
                    sample_json VARCHAR,
                    sequence_id VARCHAR,
                    source_event_time TIMESTAMP,
                    ingest_time TIMESTAMP,
                    watermark_ms BIGINT,
                    lateness_ms BIGINT,
                    watermark_late BOOLEAN DEFAULT FALSE,
                    replay_status VARCHAR DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    last_error VARCHAR,
                    replay_at TIMESTAMP,
                    resolved_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (quarantine_id)
                )
                """)
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS multiperiod_rebuild_audit (
                    rebuild_id VARCHAR NOT NULL,
                    stock_code VARCHAR NOT NULL,
                    start_date VARCHAR NOT NULL,
                    end_date VARCHAR NOT NULL,
                    periods_json VARCHAR,
                    persisted_periods_json VARCHAR,
                    row_stats_json VARCHAR,
                    receipt_hash VARCHAR,
                    status VARCHAR NOT NULL,
                    error_message VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (rebuild_id)
                )
                """)
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_sla_daily (
                    report_date DATE NOT NULL,
                    completeness DOUBLE,
                    consistency DOUBLE,
                    lag_p95_ms DOUBLE,
                    trust_score DOUBLE,
                    gate_pass BOOLEAN,
                    write_total_rows BIGINT,
                    write_expected_rows BIGINT,
                    conflict_count BIGINT,
                    step6_total_checks BIGINT,
                    step6_sampled_checks BIGINT,
                    step6_skipped_checks BIGINT,
                    step6_hard_failed_checks BIGINT,
                    step6_hard_fail_rate DOUBLE,
                    step6_sample_rate DOUBLE,
                    canary_shadow_write_enabled BOOLEAN,
                    canary_shadow_only BOOLEAN,
                    reject_count BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (report_date)
                )
                """)
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_incident (
                    incident_id VARCHAR NOT NULL,
                    incident_type VARCHAR NOT NULL,
                    severity VARCHAR NOT NULL,
                    stock_code VARCHAR,
                    period VARCHAR,
                    quarantine_id VARCHAR,
                    payload_json VARCHAR,
                    status VARCHAR DEFAULT 'open',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (incident_id)
                )
                """)

                # ── custom_period_bars：自定义周期 K 线预计算缓存 ──
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS custom_period_bars (
                    stock_code   VARCHAR   NOT NULL,
                    period       VARCHAR   NOT NULL,
                    datetime     TIMESTAMP NOT NULL,
                    open         DECIMAL(18, 6),
                    high         DECIMAL(18, 6),
                    low          DECIMAL(18, 6),
                    close        DECIMAL(18, 6),
                    volume       BIGINT,
                    amount       DECIMAL(18, 6),
                    adjust_type  VARCHAR   DEFAULT 'none',
                    adj_factor_hash VARCHAR DEFAULT '',
                    is_partial   BOOLEAN   DEFAULT FALSE,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stock_code, period, datetime, adjust_type)
                )
                """)

                self._migrate_stock_daily_schema()
                self._migrate_stock_tick_schema()
                self._migrate_ingestion_status_schema()
                self._migrate_quarantine_schema()
                self._migrate_sla_daily_schema()
                self._migrate_custom_period_bars_schema()

                # 因子值存储表
                self.con.execute("""
                CREATE TABLE IF NOT EXISTS factor_values (
                    symbol      VARCHAR  NOT NULL,
                    factor_name VARCHAR  NOT NULL,
                    date        DATE     NOT NULL,
                    value       DOUBLE,
                    version     VARCHAR  DEFAULT '1.0',
                    saved_at    TIMESTAMP DEFAULT now(),
                    PRIMARY KEY (symbol, factor_name, date)
                )
                """)
                try:
                    self.con.execute(
                        "CREATE INDEX IF NOT EXISTS idx_factor_values_lookup "
                        "ON factor_values (symbol, factor_name, date)"
                    )
                except Exception:
                    pass

            # 创建索引
                try:
                    self.con.execute(
                        "CREATE INDEX IF NOT EXISTS idx_stock_code_daily ON stock_daily (stock_code)"
                    )
                    self.con.execute("CREATE INDEX IF NOT EXISTS idx_date_daily ON stock_daily (date)")
                    self.con.execute(
                        "CREATE INDEX IF NOT EXISTS idx_source_conflict_lookup "
                        "ON source_conflict_audit (stock_code, period, event_ts)"
                    )
                    self.con.execute(
                        "CREATE INDEX IF NOT EXISTS idx_write_audit_lookup "
                        "ON write_audit_log (stock_code, table_name, created_at)"
                    )
                    self.con.execute(
                        "CREATE INDEX IF NOT EXISTS idx_quarantine_lookup "
                        "ON data_quarantine_log (stock_code, table_name, created_at)"
                    )
                    self.con.execute(
                        "CREATE INDEX IF NOT EXISTS idx_sla_daily_gate "
                        "ON data_quality_sla_daily (report_date, gate_pass)"
                    )
                    self.con.execute(
                        "CREATE INDEX IF NOT EXISTS idx_incident_lookup "
                        "ON data_quality_incident (incident_type, severity, created_at)"
                    )
                    self.con.execute(
                        "CREATE INDEX IF NOT EXISTS idx_custom_period_bars_lookup "
                        "ON custom_period_bars (stock_code, period, adjust_type, datetime)"
                    )
                except Exception:
                    pass  # 索引可能已存在

                self._tables_initialized = True
                self._logger.debug("数据表检查完成")

        except Exception as e:
            self._logger.warning("创建表失败: %s", e)

    def _get_table_columns(self, table_name: str) -> list[str]:
        if not self._is_safe_identifier(table_name):
            self._logger.warning("_get_table_columns: 非法表名 %r", table_name)
            return []
        try:
            rows = self.con.execute(
                f'SELECT column_name FROM pragma_table_info("{table_name}")'  # noqa: S608
            ).fetchall()
            if rows:
                return [row[0] for row in rows]
        except Exception as _tbl_err:
            self._logger.debug("pragma_table_info(column_name) 失败 %s: %s", table_name, _tbl_err)

        try:
            rows = self.con.execute(
                f'SELECT name FROM pragma_table_info("{table_name}")'  # noqa: S608
            ).fetchall()
            return [row[0] for row in rows]
        except Exception as _tbl_err2:
            self._logger.debug("pragma_table_info(name) 失败 %s: %s", table_name, _tbl_err2)
            return []

    def _migrate_stock_daily_schema(self):
        columns = self._get_table_columns("stock_daily")
        if not columns:
            return
        required = {
            "stock_code",
            "symbol_type",
            "date",
            "period",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "adjust_type",
            "factor",
            "created_at",
            "updated_at",
        }
        if required.issubset(set(columns)):
            return

        columns_set = set(columns)
        columns_to_add = []
        if "symbol_type" not in columns_set:
            columns_to_add.append(("symbol_type", "VARCHAR DEFAULT 'stock'"))
        if "period" not in columns_set:
            columns_to_add.append(("period", "VARCHAR DEFAULT '1d'"))
        if "adjust_type" not in columns_set:
            columns_to_add.append(("adjust_type", "VARCHAR DEFAULT 'none'"))
        if "factor" not in columns_set:
            columns_to_add.append(("factor", "DECIMAL(18, 6) DEFAULT 1.0"))
        if "created_at" not in columns_set:
            columns_to_add.append(("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
        if "updated_at" not in columns_set:
            columns_to_add.append(("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))

        if not columns_to_add:
            return

        for col_name, col_type in columns_to_add:
            try:
                self.con.execute(f"ALTER TABLE stock_daily ADD COLUMN {col_name} {col_type}")
                self._logger.debug("stock_daily 迁移: 添加列 %s", col_name)
            except Exception as _mig_err:
                self._logger.debug("stock_daily 迁移列 %s 跳过: %s", col_name, _mig_err)

    def _migrate_stock_tick_schema(self):
        columns = self._get_table_columns("stock_tick")
        if not columns:
            return
        columns_set = set(columns)
        columns_to_add = []
        if "lastPrice" not in columns_set:
            columns_to_add.append(("lastPrice", "DECIMAL(18, 6)"))
        if "func_type" not in columns_set:
            columns_to_add.append(("func_type", "VARCHAR"))
        if "openInt" not in columns_set:
            columns_to_add.append(("openInt", "BIGINT"))
        if "bidPrice" not in columns_set:
            columns_to_add.append(("bidPrice", "VARCHAR"))
        if "askPrice" not in columns_set:
            columns_to_add.append(("askPrice", "VARCHAR"))
        if "bidVol" not in columns_set:
            columns_to_add.append(("bidVol", "VARCHAR"))
        if "askVol" not in columns_set:
            columns_to_add.append(("askVol", "VARCHAR"))
        for col_name, col_type in columns_to_add:
            try:
                self.con.execute(f"ALTER TABLE stock_tick ADD COLUMN {col_name} {col_type}")
                self._logger.debug("stock_tick 迁移: 添加列 %s", col_name)
            except Exception as _mig_err:
                self._logger.debug("stock_tick 迁移列 %s 跳过: %s", col_name, _mig_err)

    def _migrate_ingestion_status_schema(self) -> None:
        """为 data_ingestion_status 表补充血缘字段（向后兼容迁移）"""
        columns = self._get_table_columns("data_ingestion_status")
        if not columns:
            return
        columns_set = set(columns)
        lineage_columns = [
            ("schema_version", "VARCHAR DEFAULT '1.0'"),
            ("ingest_run_id", "VARCHAR"),
            ("raw_hash", "VARCHAR"),
            ("source_event_time", "TIMESTAMP"),
        ]
        for col_name, col_def in lineage_columns:
            if col_name not in columns_set:
                try:
                    self.con.execute(
                        "ALTER TABLE data_ingestion_status ADD COLUMN "
                        + col_name + " " + col_def
                    )
                    self._logger.debug("ingestion_status 迁移: 添加列 %s", col_name)
                except Exception as _mig_err:
                    self._logger.debug("ingestion_status 迁移列 %s 跳过: %s", col_name, _mig_err)

    def _migrate_quarantine_schema(self) -> None:
        columns = self._get_table_columns("data_quarantine_log")
        if not columns:
            return
        columns_set = set(columns)
        ext_columns = [
            ("replay_status", "VARCHAR DEFAULT 'pending'"),
            ("retry_count", "INTEGER DEFAULT 0"),
            ("last_error", "VARCHAR"),
            ("replay_at", "TIMESTAMP"),
            ("resolved_at", "TIMESTAMP"),
            ("sequence_id", "VARCHAR"),
            ("source_event_time", "TIMESTAMP"),
            ("ingest_time", "TIMESTAMP"),
            ("watermark_ms", "BIGINT"),
            ("lateness_ms", "BIGINT"),
            ("watermark_late", "BOOLEAN DEFAULT FALSE"),
        ]
        for col_name, col_def in ext_columns:
            if col_name not in columns_set:
                try:
                    self.con.execute(
                        "ALTER TABLE data_quarantine_log ADD COLUMN " + col_name + " " + col_def
                    )
                    self._logger.debug("quarantine 迁移: 添加列 %s", col_name)
                except Exception as _mig_err:
                    self._logger.debug("quarantine 迁移列 %s 跳过: %s", col_name, _mig_err)

    def _migrate_sla_daily_schema(self) -> None:
        columns = self._get_table_columns("data_quality_sla_daily")
        if not columns:
            return
        columns_set = set(columns)
        columns_to_add = []
        if "step6_total_checks" not in columns_set:
            columns_to_add.append(("step6_total_checks", "BIGINT DEFAULT 0"))
        if "step6_sampled_checks" not in columns_set:
            columns_to_add.append(("step6_sampled_checks", "BIGINT DEFAULT 0"))
        if "step6_skipped_checks" not in columns_set:
            columns_to_add.append(("step6_skipped_checks", "BIGINT DEFAULT 0"))
        if "step6_hard_failed_checks" not in columns_set:
            columns_to_add.append(("step6_hard_failed_checks", "BIGINT DEFAULT 0"))
        if "step6_hard_fail_rate" not in columns_set:
            columns_to_add.append(("step6_hard_fail_rate", "DOUBLE DEFAULT 0"))
        if "step6_sample_rate" not in columns_set:
            columns_to_add.append(("step6_sample_rate", "DOUBLE DEFAULT 1"))
        if "canary_shadow_write_enabled" not in columns_set:
            columns_to_add.append(("canary_shadow_write_enabled", "BOOLEAN DEFAULT FALSE"))
        if "canary_shadow_only" not in columns_set:
            columns_to_add.append(("canary_shadow_only", "BOOLEAN DEFAULT TRUE"))
        for col_name, col_type in columns_to_add:
            try:
                self.con.execute(f"ALTER TABLE data_quality_sla_daily ADD COLUMN {col_name} {col_type}")
            except Exception as _mig_err:
                self._logger.debug("data_quality_sla_daily 迁移列 %s 跳过: %s", col_name, _mig_err)

    def _migrate_custom_period_bars_schema(self) -> None:
        columns = self._get_table_columns("custom_period_bars")
        if not columns:
            return
        if "adj_factor_hash" in set(columns):
            return
        try:
            self.con.execute("ALTER TABLE custom_period_bars ADD COLUMN adj_factor_hash VARCHAR DEFAULT ''")
            self._logger.debug("custom_period_bars 迁移: 添加列 adj_factor_hash")
        except Exception as _mig_err:
            self._logger.debug("custom_period_bars 迁移列 adj_factor_hash 跳过: %s", _mig_err)

    def get_stock_data(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        period: str = "1d",
        adjust: str = "none",
        auto_save: bool = True,
    ) -> pd.DataFrame:
        """
        获取股票数据（统一入口）

        修复：首次使用时自动创建表

        数据获取策略：
        1. 优先从DuckDB读取（包含五维复权，速度快）
        2. 如DuckDB无数据或数据不全，使用QMT在线获取
        3. 获取后自动保存到DuckDB

        Args:
            stock_code: 股票代码（如 '511380.SH'）
            start_date: 开始日期（'YYYY-MM-DD'）
            end_date: 结束日期（'YYYY-MM-DD'）
            period: 数据周期（'1d'=日线, '1m'=分钟, '5m'=5分钟, 'tick'=tick）
            adjust: 复权类型（'none'=不复权, 'front'=前复权, 'back'=后复权,
                                 'geometric_front'=等比前复权, 'geometric_back'=等比后复权）
            auto_save: 是否自动保存到DuckDB

        Returns:
            DataFrame: 包含 OHLCV 数据
        """
        self._logger.debug("获取数据: %s | %s ~ %s | %s | %s", stock_code, start_date, end_date, period, adjust)

        # ── 结果缓存命中检查 ──────────────────────────────────────────────────────
        # 自定义周期（非基础周期）每次都要从 1m/1d 重新计算，代价极高；通过跨实例
        # 类级 TTL 缓存，切换品种再切回时直接命中，无需重算。
        _cache_key = (self.duckdb_path, stock_code, period, start_date, end_date, adjust)
        _cache_enabled = str(self.duckdb_path) not in (":memory:", "", "None")
        if _cache_enabled:
            _cached = UnifiedDataInterface._cache_get(_cache_key)
            if _cached is not None and not _cached.empty:
                self._logger.debug("结果缓存命中: %s %s %s~%s", stock_code, period, start_date, end_date)
                return _cached
        # ──────────────────────────────────────────────────────────────────────────

        if self.con is None and self.duckdb_available:
            try:
                self.connect(read_only=False)
            except Exception:
                pass

        # 确保表存在（修复首次使用问题）
        self._ensure_tables_exist()
        self._check_qmt()
        self._check_akshare()
        self._check_tushare()

        # Step 1: 尝试从DuckDB读取
        # FiveFoldAdjustmentManager 只适用于日线/周线/月线（含五维复权列）；
        # 分钟线直接走 _read_from_duckdb，避免将 stock_daily 日线数据错误地返回给分钟周期请求。
        data = None
        # Only "1d" uses FiveFoldAdjustmentManager (queries WHERE period='1d').
        # "1w" and "1M" are derived via _PERIOD_AGGREGATION resampling from "1d"
        # through _read_from_duckdb, so they must NOT go through FiveFold.
        _DAILY_PERIODS = {"1d"}
        if self.duckdb_available and self.con:
            if period in _DAILY_PERIODS:
                # 日线/周线/月线：使用 FiveFoldAdjustmentManager（含复权计算）
                if self.adjustment_manager is None:
                    self._ensure_adjustment_manager()
                if self.adjustment_manager is not None:
                    self._logger.debug("使用 FiveFoldAdjustmentManager 查询数据")
                    try:
                        data = self.adjustment_manager.get_data_with_adjustment(
                            stock_code=stock_code,
                            start_date=start_date,
                            end_date=end_date,
                            adjust_type=adjust,
                        )
                        if data is not None and not data.empty:
                            self._logger.debug("从DuckDB获取成功 %d 条记录", len(data))
                        else:
                            data = None
                    except Exception as e:
                        self._logger.warning("FiveFoldAdjustmentManager查询失败: %s", e)
                        self._logger.debug("降级到原有的_read_from_duckdb方法")
                        data = self._read_from_duckdb(stock_code, start_date, end_date, period, adjust)
                else:
                    data = self._read_from_duckdb(stock_code, start_date, end_date, period, adjust)
            else:
                # 非日线周期：直接读对应 DuckDB 表（含派生周期聚合）
                self._logger.debug("直接查询 DuckDB period=%s", period)
                data = self._read_from_duckdb(stock_code, start_date, end_date, period, adjust)
                if data is not None and not data.empty:
                    self._logger.debug("从DuckDB获取成功 %d 条记录 period=%s", len(data), period)
                else:
                    data = None

        # Step 2: 检查数据完整性
        need_download = False
        ingestion_source = "duckdb"

        if data is None or data.empty:
            self._logger.debug("DuckDB 无数据，需要从在线数据源获取")
            need_download = True
            try:
                dat_data = self.data_registry.get_data(
                    symbol=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    period=period,
                    adjust=adjust,
                    preferred_sources=["dat"],
                )
            except Exception:
                dat_data = pd.DataFrame()
            if dat_data is not None and not dat_data.empty:
                data = dat_data
                ingestion_source = "dat"
                missing_days = self._check_missing_trading_days(data, start_date, end_date, period)
                sparse_intraday = self._is_intraday_sparse(data, period)
                if missing_days <= 0:
                    if sparse_intraday:
                        need_download = True
                        self._logger.warning("QMT本地DAT稀疏 period=%s，继续在线补充", period)
                    else:
                        need_download = False
                        self._logger.debug("从QMT本地DAT获取成功 %d 条记录", len(data))
                else:
                    self._logger.warning("QMT本地DAT不完整，缺失 %d 个交易日，继续在线补充", missing_days)
        else:
            # 检查是否有缺失
            missing_days = self._check_missing_trading_days(data, start_date, end_date, period)
            sparse_intraday = self._is_intraday_sparse(data, period)
            if missing_days > 0 or sparse_intraday:
                sparse_text = "，分钟K线稀疏" if sparse_intraday else ""
                self._logger.debug("DuckDB 数据不完整，缺失 %d 个交易日%s，需要补充", missing_days, sparse_text)
                need_download = True

        # Step 3: 如需下载，使用QMT获取
        if need_download:
            # 1w / 1M 属于派生周期，从源周期 (1d) 获取并聚合，
            # 避免直接请求 QMT 周线/月线（QMT 月线不支持、周线复权列问题）。
            if period in self._PERIOD_AGGREGATION:
                src_period, rule = self._PERIOD_AGGREGATION[period]
                if src_period != period:
                    self._logger.debug("%s 为派生周期，先获取 %s 数据再聚合", period, src_period)
                    try:
                        src_data = self.get_stock_data(
                            stock_code, start_date, end_date, src_period, adjust, auto_save=True
                        )
                        if src_data is not None and not src_data.empty:
                            resampled = self._resample_ohlcv(src_data, rule)
                            if resampled is not None and not resampled.empty:
                                self._logger.debug("从 %s 聚合得到 %d 条 %s 数据", src_period, len(resampled), period)
                                return resampled
                    except Exception as _e:
                        self._logger.warning("从 %s 聚合失败: %s，继续尝试直接获取", src_period, _e)

            if not self._cb_allow():
                self._logger.info("远程数据源短路开启，跳过在线请求")
                if data is not None and not data.empty:
                    self._record_ingestion_status(
                        stock_code=stock_code,
                        period=period,
                        start_date=start_date,
                        end_date=end_date,
                        source=ingestion_source,
                        status="success",
                        record_count=len(data),
                        error_message=None,
                    )
                return data if data is not None else pd.DataFrame()
            qmt_data: pd.DataFrame | None = None
            if not self.qmt_available:
                self._refresh_qmt_status()
            if self.qmt_available:
                self._logger.debug("从 QMT 获取在线数据")
                try:
                    qmt_data = self._read_from_qmt(stock_code, start_date, end_date, period)
                except Exception as e:
                    self._logger.error("QMT获取异常: %s", e)
                    qmt_data = None
                if qmt_data is not None and not qmt_data.empty:
                    ingestion_source = "qmt"
            else:
                self._logger.warning("QMT 不可用，尝试第三方数据源兜底")
            if qmt_data is None or qmt_data.empty:
                # QMT 失败：优先尝试本地 DAT 兜底（带时效检查）
                if self._dat_file_is_fresh(stock_code, period):
                    self._logger.debug("QMT 失败，尝试 DAT 本地兜底（在时效内）")
                    try:
                        _dat_fb = self.data_registry.get_data(
                            symbol=stock_code,
                            start_date=start_date,
                            end_date=end_date,
                            period=period,
                            adjust=adjust,
                            preferred_sources=["dat"],
                        )
                    except Exception:
                        _dat_fb = pd.DataFrame()
                    if _dat_fb is not None and not _dat_fb.empty:
                        qmt_data = _dat_fb
                        ingestion_source = "dat"
                        self._logger.debug("DAT 兜底成功 %d 条", len(qmt_data))
                # 期货/港股代码不走 Tushare/AKShare（不支持该资产类别）
                # 批量入库模式下跳过第三方回退（QMT download_history_data 已尝试，无数据即跳过）
                if not self._is_futures_or_hk(stock_code) and not self._skip_third_party_fallback:
                    if qmt_data is None or qmt_data.empty:
                        self._logger.debug("经由 DataSourceRegistry 尝试 Tushare / AKShare 兜底")
                        try:
                            _third_party = self.data_registry.get_data(
                                symbol=stock_code,
                                start_date=start_date,
                                end_date=end_date,
                                period=period,
                                adjust=adjust,
                                preferred_sources=["tushare", "akshare"],
                            )
                        except Exception as _tp_err:
                            self._logger.error("第三方数据源异常: %s", _tp_err)
                            _third_party = pd.DataFrame()
                        if _third_party is not None and not _third_party.empty:
                            qmt_data = _third_party
                            # 记录实际来源（从 registry 健康摘要推断）
                            _ts_ok = bool(getattr(self, "tushare_available", False))
                            ingestion_source = "tushare" if _ts_ok else "akshare"
                            self._logger.debug("%s 兜底成功 %d 条", ingestion_source, len(qmt_data))
                if qmt_data is None or qmt_data.empty:
                    self._logger.error("在线数据获取失败（QMT/Tushare/AKShare均不可用或返回空）")
                    # For periods that need resampling (1M cannot be fetched from QMT directly),
                    # fall back to getting the source period and resampling on-the-fly.
                    if period in self._PERIOD_AGGREGATION:
                        src_period, rule = self._PERIOD_AGGREGATION[period]
                        self._logger.debug("%s 在线获取失败，尝试从 %s 聚合", period, src_period)
                        try:
                            src_data = self.get_stock_data(
                                stock_code, start_date, end_date, src_period, adjust, auto_save=True
                            )
                            if src_data is not None and not src_data.empty:
                                resampled = self._resample_ohlcv(src_data, rule)
                                if resampled is not None and not resampled.empty:
                                    self._logger.debug("从 %s 聚合得到 %d 条 %s 数据", src_period, len(resampled), period)
                                    return resampled
                        except Exception as _e:
                            self._logger.warning("从 %s 聚合失败: %s", src_period, _e)
                    self._logger.error("在线数据获取失败（QMT/Tushare/AKShare均不可用）")
                    self._cb_on_failure()
                    self._record_ingestion_status(
                        stock_code=stock_code,
                        period=period,
                        start_date=start_date,
                        end_date=end_date,
                        source=ingestion_source,
                        status="failed",
                        record_count=0,
                        error_message="online_data_failed_qmt_tushare_akshare",
                    )
                    return data if data is not None else pd.DataFrame()
                else:
                    if ingestion_source not in ("dat", "tushare", "qmt"):
                        ingestion_source = "akshare"
                    self._cb_on_success()
            else:
                self._cb_on_success()

            # 合并数据（DuckDB有的就用，没有的补充）
            if data is not None and not data.empty:
                self._logger.debug("合并 DuckDB 和在线数据")
                merged_data = self._merge_data(data, qmt_data, stock_code, period)
                data = merged_data
            else:
                data = qmt_data

            # Step 3.5: 数据合约验证（在保存之前执行，拦截问题数据入库）
            _contract_pass = True
            if data is not None and not data.empty:
                try:
                    from data_manager.data_contract_validator import DataContractValidator
                    _cv_result = DataContractValidator().validate(
                        data, stock_code, ingestion_source, period=period
                    )
                    self._last_contract_validation = _cv_result
                    if not _cv_result.pass_gate:
                        _contract_pass = False
                        _violations = "; ".join(v.detail for v in _cv_result.violations[:3])
                        self._logger.error("数据合约硬门禁未通过，拒绝入库: %s", _violations)
                except Exception as _cv_err:
                    self._logger.warning("DataContractValidator 内部异常（不阻断）: %s", _cv_err)
                    self._last_contract_validation = None

            # Step 4: 保存到DuckDB（仅在合约验证通过时保存）
            if auto_save and self.duckdb_available and self.con and _contract_pass:
                self._logger.debug("保存数据到 DuckDB")
                try:
                    self._save_to_duckdb(
                        data, stock_code, period,
                        _ingest_source=ingestion_source,
                        _ingest_start=start_date,
                        _ingest_end=end_date,
                    )  # ingestion_status(success) 已在同一事务内原子写入
                    self._logger.debug("数据已保存到 DuckDB")
                    # 新数据入库后使相关缓存失效，避免下次读到写入前的旧结果
                    UnifiedDataInterface._cache_invalidate(self.duckdb_path, stock_code, period)
                except Exception as e:
                    self._logger.error("DuckDB写入失败: %s", e)
                    self._record_ingestion_status(
                        stock_code=stock_code,
                        period=period,
                        start_date=start_date,
                        end_date=end_date,
                        source=ingestion_source,
                        status="failed",
                        record_count=len(data),
                        error_message=str(e),
                    )
        else:
            if data is None:
                data = pd.DataFrame()
            # DAT 首次加载时缓存到 DuckDB，下次直接从 DuckDB 读取（避免重复 DAT 查询）
            if ingestion_source == "dat" and auto_save and self.duckdb_available and self.con and not data.empty:
                try:
                    self._save_to_duckdb(data, stock_code, period)
                except Exception:
                    pass
            self._logger.debug("从DuckDB读取成功 %d 条记录", len(data))
            if ingestion_source != "duckdb":
                self._record_ingestion_status(
                    stock_code=stock_code,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    source=ingestion_source,
                    status="success",
                    record_count=len(data),
                    error_message=None,
                )

        # Step 5: 应用复权
        if data is None:
            data = pd.DataFrame()
        if not data.empty and adjust != "none":
            data = self._apply_adjustment(data, adjust)

        # Step 6: 对 DuckDB 直读数据做二次合约验证（可配置采样，下载路径已在 Step 3.5 完成）
        if not data.empty and self._last_contract_validation is None:
            self._step6_validation_metrics["total"] = int(self._step6_validation_metrics.get("total", 0)) + 1
            sample_basis = f"{stock_code}|{period}|{start_date}|{end_date}|{len(data)}"
            sample_hit = self._step6_should_validate(sample_basis)
            if not sample_hit:
                self._step6_validation_metrics["skipped"] = int(self._step6_validation_metrics.get("skipped", 0)) + 1
                self._last_contract_validation = None
                self._last_ingestion_source = ingestion_source
                return data
            self._step6_validation_metrics["sampled"] = int(self._step6_validation_metrics.get("sampled", 0)) + 1
            try:
                from data_manager.data_contract_validator import DataContractValidator
                _cv6 = DataContractValidator().validate(
                    data, stock_code, ingestion_source, period=period
                )
                self._last_contract_validation = _cv6
                if not _cv6.pass_gate:
                    self._step6_validation_metrics["hard_failed"] = int(self._step6_validation_metrics.get("hard_failed", 0)) + 1
                    _hard_viols = [
                        v for v in _cv6.violations if getattr(v, "severity", "") == "hard"
                    ]
                    _viol_summary = "; ".join(v.detail for v in _hard_viols[:3])
                    self._logger.critical(
                        "DataContract CACHE-STALE [%s | %s | period=%s | %d 行]: "
                        "DuckDB 缓存数据存在硬违规，已触发隔离队列 — %s",
                        stock_code, ingestion_source, period, len(data), _viol_summary,
                    )
                    if self._cache_stale_quarantine_enabled:
                        try:
                            _date_col = next(
                                (
                                    c for c in ("time", "date", "trade_date", "datetime")
                                    if c in data.columns
                                ),
                                None,
                            )
                            _dmin = str(data[_date_col].min()) if _date_col else start_date
                            _dmax = str(data[_date_col].max()) if _date_col else end_date
                            self._record_quarantine_log(
                                audit_id=str(uuid.uuid4()),
                                table_name="market_data",
                                stock_code=stock_code,
                                period=period,
                                reason=f"cache-stale-hard-violation: {_viol_summary[:200]}",
                                expected_rows=len(data),
                                actual_rows=len(data),
                                date_min=_dmin,
                                date_max=_dmax,
                                sample_json="{}",
                            )
                            self._emit_data_quality_alert(
                                stock_code=stock_code,
                                period=period,
                                level="critical",
                                reason="cache-stale-hard-violation",
                                details={"violations": [v.detail for v in _hard_viols[:5]]},
                            )
                            self._step6_validation_metrics["quarantined"] = int(
                                self._step6_validation_metrics.get("quarantined", 0)
                            ) + 1
                        except Exception as _q_err:
                            self._logger.warning("Step6 quarantine 写入失败（不阻断）: %s", _q_err)
                    else:
                        self._logger.warning(
                            "EASYXT_CACHE_STALE_QUARANTINE_ENABLED=0，已跳过缓存脏数据隔离: %s %s %s",
                            stock_code,
                            period,
                            _viol_summary,
                        )
            except Exception as _cv_err:
                self._logger.warning("DataContractValidator 内部异常（不阻断）: %s", _cv_err)
                self._last_contract_validation = None
        else:
            self._last_contract_validation = None

        self._last_ingestion_source = ingestion_source
        # ── 写入结果缓存 ──────────────────────────────────────────────────────────
        if _cache_enabled and data is not None and not data.empty:
            UnifiedDataInterface._cache_put(_cache_key, data)
        # ──────────────────────────────────────────────────────────────────────────
        return data

    def get_stock_data_local(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        period: str = "1d",
        adjust: str = "none",
    ) -> pd.DataFrame:
        # ── 日内自定义周期 ──
        if period in self._INTRADAY_CUSTOM_PERIODS:
            period_minutes = self._INTRADAY_CUSTOM_PERIODS[period]
            src_1m = self.get_stock_data_local(stock_code, start_date, end_date, period="1m", adjust=adjust)
            if src_1m is None or src_1m.empty:
                return pd.DataFrame()
            daily_ref = self.get_stock_data_local(stock_code, start_date, end_date, period="1d", adjust=adjust)
            result = self._make_period_bar_builder(stock_code=stock_code).build_intraday_bars(
                data_1m=src_1m, period_minutes=period_minutes, daily_ref=daily_ref
            )
            return result if result is not None and not result.empty else pd.DataFrame()

        # ── 多日自定义周期 ──
        if period in self._MULTIDAY_CUSTOM_PERIODS:
            trading_days = self._MULTIDAY_CUSTOM_PERIODS[period]
            src_1d = self.get_stock_data_local(stock_code, start_date, end_date, period="1d", adjust=adjust)
            if src_1d is None or src_1d.empty:
                return pd.DataFrame()
            result = self._make_period_bar_builder(stock_code=stock_code).build_multiday_bars(
                data_1d=src_1d, trading_days_per_period=trading_days
            )
            return result if result is not None and not result.empty else pd.DataFrame()

        if period in self._PERIOD_AGGREGATION:
            src_period, rule = self._PERIOD_AGGREGATION[period]
            src_df = self.get_stock_data_local(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                period=src_period,
                adjust=adjust,
            )
            if src_df is None or src_df.empty:
                return pd.DataFrame()
            aggregated = self._resample_ohlcv(src_df, rule)
            if aggregated is None or aggregated.empty:
                return pd.DataFrame()
            return aggregated
        if self.con is None and self.duckdb_available:
            try:
                self.connect(read_only=True)
            except Exception:
                pass
        if self.con is not None:
            self._ensure_tables_exist()
        data = self.data_registry.get_data(
            symbol=stock_code,
            start_date=start_date,
            end_date=end_date,
            period=period,
            adjust=adjust,
            preferred_sources=["duckdb", "dat", "parquet"],
        )
        if data is None or data.empty:
            data = pd.DataFrame()
        if not data.empty and adjust != "none":
            data = self._apply_adjustment(data, adjust)
        return data

    # ─────────── 数据库修复工具：清理历史遗留的脏数据 ────────────

    def repair_daily_adjustments(self, stock_codes: list[str] | None = None) -> dict[str, str]:
        """批量修复 stock_daily 中复权列全 NULL 的历史遗留问题。

        扫描 stock_daily 表，找出 open_front 全为 NULL 的股票代码，
        重新调用 calculate_adjustment 计算并回写。

        Args:
            stock_codes: 需要修复的股票代码列表。None 表示自动检测全部。

        Returns:
            {stock_code: "repaired" | "skipped" | "error:..."} 字典
        """
        results: dict[str, str] = {}
        if not self.duckdb_available or not self.con:
            return {"_error": "DuckDB 不可用"}

        try:
            if stock_codes is None:
                # 自动检测：找出 open_front 全 NULL 的 stock_code
                detect_sql = (
                    "SELECT DISTINCT stock_code FROM stock_daily"
                    " WHERE period = '1d'"
                    " AND stock_code NOT IN ("
                    "   SELECT DISTINCT stock_code FROM stock_daily"
                    "   WHERE period = '1d' AND open_front IS NOT NULL"
                    " )"
                )
                try:
                    rows = self.con.execute(detect_sql).fetchall()
                    stock_codes = [r[0] for r in rows]
                except Exception:
                    stock_codes = []

            if not stock_codes:
                self._logger.info("REPAIR: 未检测到需要修复的股票")
                return results

            self._logger.info("REPAIR: 需要修复 %d 只股票的复权列", len(stock_codes))

            if self.adjustment_manager is None:
                self._ensure_adjustment_manager()
            if self.adjustment_manager is None:
                return {"_error": "FiveFoldAdjustmentManager 不可用"}

            for code in stock_codes:
                try:
                    self.adjustment_manager._try_repair_adjustment(
                        code, "1990-01-01", "2099-12-31"
                    )
                    results[code] = "repaired"
                except Exception as e:
                    results[code] = f"error: {e}"

            repaired = sum(1 for v in results.values() if v == "repaired")
            self._logger.info("[REPAIR] 完成: %d/%d 只股票修复成功", repaired, len(stock_codes))

        except Exception as e:
            results["_error"] = str(e)

        return results

    def purge_stale_derived_periods(self) -> int:
        """清理 stock_daily 中 period='1w' 或 period='1M' 的历史遗留脏记录。

        这些记录来自早期直接向 QMT 请求周线/月线后存入的数据，
        由于 index 对齐 bug 导致复权列全 NULL。
        现在 1w/1M 改为从 1d 聚合，这些直存记录已无用。

        Returns:
            删除的记录数
        """
        if not self.duckdb_available or not self.con:
            return 0
        try:
            count_row = self.con.execute(
                "SELECT COUNT(*) FROM stock_daily WHERE period IN ('1w', '1M')"
            ).fetchone()
            count = count_row[0] if count_row else 0
            if count > 0:
                self.con.execute("DELETE FROM stock_daily WHERE period IN ('1w', '1M')")
                self._logger.info("PURGE: 已清理 %d 条 1w/1M 直存记录", count)
            return count
        except Exception as e:
            self._logger.warning("PURGE: 清理失败: %s", e)
            return 0

    def _compute_data_lineage(
        self, data: pd.DataFrame
    ) -> tuple[str, Any | None]:
        """计算数据血缘字段：(raw_hash, source_event_time)

        口径规范见 docs/lineage_spec.md §三：
        - raw_hash: SHA-256(DataFrame.to_csv(index=True, encoding='utf-8'))[:16]
          列序保留 DataFrame 原始顺序，空值表示为空字符串，浮点不截断。
          序列化异常时降级写 'error'。
        - source_event_time: datetime/date 列的 .max()；列缺失时尝试 index.max()；
          仍无法提取则返回 None（可空原因参见 docs/lineage_spec.md §四）。
        """
        try:
            serialized = data.to_csv(index=True).encode("utf-8", errors="replace")
            raw_hash = hashlib.sha256(serialized).hexdigest()[:16]
        except Exception:
            raw_hash = "error"
        source_event_time = None
        for col in ("datetime", "date"):
            if col in data.columns:
                try:
                    source_event_time = pd.to_datetime(data[col]).max()
                    break
                except Exception:
                    pass
        if source_event_time is None and not data.empty:
            try:
                source_event_time = pd.to_datetime(data.index).max()
            except Exception:
                pass
        return raw_hash, source_event_time

    def _record_ingestion_status(
        self,
        stock_code: str,
        period: str,
        start_date: str,
        end_date: str,
        source: str,
        status: str,
        record_count: int,
        error_message: str | None,
        *,
        raw_hash: str | None = None,
        source_event_time: Any | None = None,
    ) -> None:
        if not self.con:
            return
        ingest_run_id = str(uuid.uuid4())
        schema_version = CURRENT_SCHEMA_VERSION

        _ts_start = self._normalize_date_str(start_date)
        _ts_end = self._normalize_date_str(end_date)

        def _write_once():
            self.con.execute(
                """
                INSERT OR REPLACE INTO data_ingestion_status (
                    stock_code,
                    period,
                    start_date,
                    end_date,
                    source,
                    status,
                    record_count,
                    error_message,
                    schema_version,
                    ingest_run_id,
                    raw_hash,
                    source_event_time
                )
                VALUES (
                    ?, ?,
                    CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP),
                    ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                """,
                [
                    stock_code,
                    period,
                    _ts_start,
                    _ts_end,
                    source,
                    status,
                    int(record_count),
                    error_message,
                    schema_version,
                    ingest_run_id,
                    raw_hash,
                    source_event_time,
                ],
            )

        try:
            _write_once()
        except Exception as exc:
            msg = str(exc).lower()
            if ".wal" in msg and ("cannot open file" in msg or "failed to commit" in msg):
                try:
                    try:
                        self._close_duckdb_connection()
                        self.connect(read_only=False)
                    except Exception:
                        pass
                    _write_once()
                    return
                except Exception:
                    pass
            self._logger.warning("写入data_ingestion_status失败: %s", exc)

    def _get_existing_date_bounds(self, stock_code: str, period: str) -> tuple[Any, Any] | None:
        if not self.con:
            return None
        table_map = {"1d": "stock_daily", "1m": "stock_1m", "5m": "stock_5m", "tick": "stock_tick"}
        table_period, stored_period = self._get_storage_target_period(period)
        table_name = table_map.get(table_period, "stock_daily")
        date_col = "date" if table_name == "stock_daily" else "datetime"
        try:
            row = self.con.execute(
                "SELECT MIN(" + date_col + "), MAX(" + date_col + ") FROM " + table_name  # noqa: S608
                + " WHERE stock_code = ? AND period = ?",
                [stock_code, stored_period],
            ).fetchone()
            if not row:
                return None
            if row[0] is None or row[1] is None:
                if period in self._PERIOD_AGGREGATION:
                    src_period, _rule = self._PERIOD_AGGREGATION[period]
                    return self._get_existing_date_bounds(stock_code, src_period)
                return None
            return row[0], row[1]
        except Exception:
            return None

    def build_incremental_plan(
        self, stock_code: str, start_date: str, end_date: str, period: str = "1d"
    ) -> list[dict[str, Any]]:
        plan: list[dict[str, Any]] = []
        start_dt = pd.to_datetime(start_date, errors="coerce")
        end_dt = pd.to_datetime(end_date, errors="coerce")
        if start_dt is pd.NaT or end_dt is pd.NaT:
            return [{"start_date": start_date, "end_date": end_date, "mode": "full"}]
        bounds = self._get_existing_date_bounds(stock_code, period)
        if bounds is None:
            return [{"start_date": start_date, "end_date": end_date, "mode": "full"}]
        min_dt, max_dt = (
            pd.to_datetime(bounds[0], errors="coerce"),
            pd.to_datetime(bounds[1], errors="coerce"),
        )
        if min_dt is pd.NaT or max_dt is pd.NaT:
            return [{"start_date": start_date, "end_date": end_date, "mode": "full"}]
        if start_dt < min_dt:
            plan.append(
                {
                    "start_date": start_dt.strftime("%Y-%m-%d"),  # type: ignore[union-attr]
                    "end_date": min_dt.strftime("%Y-%m-%d"),  # type: ignore[union-attr]
                    "mode": "prepend",
                }
            )
        if end_dt > max_dt:
            plan.append(
                {
                    "start_date": max_dt.strftime("%Y-%m-%d"),  # type: ignore[union-attr]
                    "end_date": end_dt.strftime("%Y-%m-%d"),  # type: ignore[union-attr]
                    "mode": "append",
                }
            )
        if not plan:
            try:
                existing = self._read_from_duckdb(stock_code, start_date, end_date, period, "none")
                if existing is None or existing.empty:
                    return [{"start_date": start_date, "end_date": end_date, "mode": "full"}]
                missing_days = self._check_missing_trading_days(existing, start_date, end_date)
                if missing_days > 0:
                    return [{"start_date": start_date, "end_date": end_date, "mode": "refresh"}]
                return [{"start_date": start_date, "end_date": end_date, "mode": "skip"}]
            except Exception:
                return [{"start_date": start_date, "end_date": end_date, "mode": "full"}]
        return plan

    def get_ingestion_status(
        self, stock_code: str | None = None, period: str | None = None
    ) -> pd.DataFrame:
        if self.con is None:
            if not self.connect(read_only=True):
                return pd.DataFrame()
            self._ensure_tables_exist()
        query = "SELECT * FROM data_ingestion_status"
        params: list[Any] = []
        where = []
        if stock_code:
            where.append("stock_code = ?")
            params.append(stock_code)
        if period:
            where.append("period = ?")
            params.append(period)
        if where:
            query = query + " WHERE " + " AND ".join(where)
        query = query + " ORDER BY last_updated DESC"
        try:
            if params:
                return self.con.execute(query, params).fetchdf()
            return self.con.execute(query).fetchdf()
        except Exception:
            return pd.DataFrame()

    def get_data_coverage(
        self,
        stock_codes: list[str] | None = None,
        periods: list[str] | None = None,
    ) -> pd.DataFrame:
        """查询数据覆盖率矩阵。

        返回一个 DataFrame：
          - index：stock_code
          - columns：每个周期，格式为 "{period}"
          - 值："{start_date}~{end_date}({count}条)" 或 "" (无数据)

        Args:
            stock_codes: 指定股票列表，None 表示自动从数据库获取全部。
            periods: 指定周期列表，默认 ["1d","1m","5m","tick"]。

        Returns:
            pd.DataFrame（pivot 格式，行=标的，列=周期）
        """
        if periods is None:
            periods = ["1d", "1m", "5m", "tick"]

        if self.con is None:
            if not self.connect(read_only=True):
                return pd.DataFrame()
        self._ensure_tables_exist()

        # 周期 → (表名, 日期列)
        _PERIOD_TABLE: dict[str, tuple[str, str]] = {
            "1d": ("stock_daily", "date"),
            "1m": ("stock_1m", "datetime"),
            "5m": ("stock_5m", "datetime"),
            "tick": ("stock_tick", "datetime"),
        }

        rows: list[dict] = []
        for period in periods:
            if period not in _PERIOD_TABLE:
                continue
            table, date_col = _PERIOD_TABLE[period]
            where_codes = ""
            params: list[Any] = [period]
            if stock_codes:
                placeholders = ",".join(["?"] * len(stock_codes))
                where_codes = f" AND stock_code IN ({placeholders})"
                params.extend(stock_codes)
            sql = f"""
                SELECT stock_code,
                       MIN({date_col}) AS min_dt,
                       MAX({date_col}) AS max_dt,
                       COUNT(*) AS cnt
                FROM {table}  /* noqa: S608 -- values from hardcoded _PERIOD_TABLE */
                WHERE period = ?{where_codes}
                GROUP BY stock_code
                ORDER BY stock_code
            """
            try:
                df = self.con.execute(sql, params).fetchdf()
            except Exception as exc:
                self._logger.warning("get_data_coverage %s 查询失败: %s", period, exc)
                continue
            for _, r in df.iterrows():
                code = str(r["stock_code"])
                min_d = str(r["min_dt"])[:10]
                max_d = str(r["max_dt"])[:10]
                cnt = int(r["cnt"])
                rows.append({
                    "stock_code": code,
                    "period": period,
                    "summary": f"{min_d}~{max_d}({cnt}条)",
                    "min_date": min_d,
                    "max_date": max_d,
                    "count": cnt,
                })

        if not rows:
            return pd.DataFrame()

        detail_df = pd.DataFrame(rows)
        pivot = detail_df.pivot_table(
            index="stock_code",
            columns="period",
            values="summary",
            aggfunc="first",
        ).fillna("")
        # 确保列顺序与 periods 一致（只保留查到数据的列）
        ordered_cols = [p for p in periods if p in pivot.columns]
        pivot = pivot[ordered_cols]
        pivot.index.name = "stock_code"
        return pivot

    def _run_quarantine_replay_core(
        self,
        limit: int = 50,
        max_retries: int = 3,
        *,
        reason_regex: str | None = None,
    ) -> dict[str, int]:
        if self.con is None:
            if not self.connect(read_only=False):
                return {"processed": 0, "succeeded": 0, "failed": 0, "dead_letter": 0}
        self._ensure_tables_exist()
        capped_retries = max(int(max_retries), 1)
        fetch_limit = max(int(limit), 1)
        if reason_regex:
            fetch_limit = fetch_limit * 6
        try:
            rows = self.con.execute(
                """
                SELECT quarantine_id, stock_code, period, date_min, date_max, COALESCE(retry_count, 0), COALESCE(reason, '')
                FROM data_quarantine_log
                WHERE replay_status IN ('pending', 'failed')
                  AND COALESCE(retry_count, 0) < ?
                ORDER BY replay_status ASC, created_at ASC
                LIMIT ?
                """,
                [capped_retries, fetch_limit],
            ).fetchall()
        except Exception as e:
            self._logger.warning("读取quarantine待重放队列失败: %s", e)
            return {"processed": 0, "succeeded": 0, "failed": 0, "dead_letter": 0}
        if reason_regex:
            try:
                reason_re = re.compile(reason_regex, re.IGNORECASE)
                rows = [r for r in rows if reason_re.search(str(r[6] or ""))]
            except Exception:
                rows = []
        rows = rows[: max(int(limit), 1)]
        processed = 0
        succeeded = 0
        failed = 0
        dead_letter = 0
        for quarantine_id, stock_code, period, date_min, date_max, retry_count, _reason in rows:
            processed += 1
            target_period = "1m" if str(period) == "tick" else str(period or "1d")
            start_date = str(date_min or "")[:10]
            end_date = str(date_max or "")[:10]
            if not start_date or start_date.lower() == "none":
                start_date = pd.Timestamp.now().normalize().strftime("%Y-%m-%d")
            if not end_date or end_date.lower() == "none":
                end_date = pd.Timestamp.now().normalize().strftime("%Y-%m-%d")
            ok = False
            err_msg = ""
            try:
                replay_df = self.get_stock_data(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    period=target_period,
                    adjust="none",
                    auto_save=True,
                )
                ok = replay_df is not None and not replay_df.empty
            except Exception as e:
                err_msg = str(e)[:500]
                self._logger.warning("quarantine replay失败 %s: %s", quarantine_id, e)
            if not ok and not err_msg:
                err_msg = "replay_returned_empty"
            try:
                if ok:
                    succeeded += 1
                    self.con.execute(
                        """
                        UPDATE data_quarantine_log
                        SET replay_status = 'resolved',
                            retry_count = COALESCE(retry_count, 0),
                            last_error = NULL,
                            replay_at = CURRENT_TIMESTAMP,
                            resolved_at = CURRENT_TIMESTAMP
                        WHERE quarantine_id = ?
                        """,
                        [quarantine_id],
                    )
                else:
                    failed += 1
                    next_retry = int(retry_count) + 1
                    next_status = "failed" if next_retry < capped_retries else "dead_letter"
                    if next_status == "dead_letter":
                        dead_letter += 1
                    self.con.execute(
                        """
                        UPDATE data_quarantine_log
                        SET replay_status = ?,
                            retry_count = ?,
                            last_error = ?,
                            replay_at = CURRENT_TIMESTAMP,
                            resolved_at = CASE WHEN ? = 'dead_letter' THEN CURRENT_TIMESTAMP ELSE resolved_at END
                        WHERE quarantine_id = ?
                        """,
                        [next_status, next_retry, err_msg, next_status, quarantine_id],
                    )
                    if next_status == "dead_letter":
                        incident_payload = {
                            "quarantine_id": quarantine_id,
                            "start_date": start_date,
                            "end_date": end_date,
                            "retry_count": next_retry,
                            "max_retries": capped_retries,
                            "status": next_status,
                            "last_error": err_msg,
                        }
                        self._record_data_quality_incident(
                            incident_type="quarantine_dead_letter",
                            severity="critical",
                            stock_code=stock_code,
                            period=target_period,
                            quarantine_id=quarantine_id,
                            payload=incident_payload,
                        )
                    self._emit_data_quality_alert(
                        stock_code=stock_code,
                        period=target_period,
                        level="error" if next_status == "dead_letter" else "warning",
                        reason="quarantine_dead_letter" if next_status == "dead_letter" else "quarantine_replay_failed",
                        details={
                            "quarantine_id": quarantine_id,
                            "start_date": start_date,
                            "end_date": end_date,
                            "retry_count": next_retry,
                            "max_retries": capped_retries,
                            "status": next_status,
                            "last_error": err_msg,
                        },
                    )
            except Exception as e:
                failed += 1
                self._logger.warning("更新quarantine replay状态失败 %s: %s", quarantine_id, e)
        return {"processed": processed, "succeeded": succeeded, "failed": failed, "dead_letter": dead_letter}

    def run_quarantine_replay(self, limit: int = 50, max_retries: int = 3) -> dict[str, int]:
        return self._run_quarantine_replay_core(limit=limit, max_retries=max_retries)

    def run_late_event_replay(
        self,
        limit: int = 80,
        max_retries: int = 4,
        reason_regex: str = r"(late|out_of_order|watermark|stale|reorder)",
    ) -> dict[str, int]:
        return self._run_quarantine_replay_core(
            limit=limit,
            max_retries=max_retries,
            reason_regex=reason_regex,
        )

    def run_multiperiod_rebuild(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        periods: list[str] | None = None,
    ) -> dict[str, Any]:
        if self.con is None:
            if not self.connect(read_only=False):
                return {
                    "ok": False,
                    "error": "duckdb_connect_failed",
                    "processed": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "details": [],
                }
        self._ensure_tables_exist()
        symbol = str(stock_code or "").strip()
        if not symbol:
            return {"ok": False, "error": "stock_code_empty", "processed": 0, "succeeded": 0, "failed": 0, "details": []}
        target_periods = [str(p).strip() for p in (periods or ["1m", "5m", "15m", "30m", "60m", "1d", "1w", "1M"]) if str(p).strip()]
        try:
            data_1m = self.get_stock_data(symbol, start_date, end_date, "1m", "none", auto_save=True)
        except Exception:
            data_1m = pd.DataFrame()
        try:
            data_1d = self.get_stock_data(symbol, start_date, end_date, "1d", "none", auto_save=True)
        except Exception:
            data_1d = pd.DataFrame()

        def _ensure_time_column(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame()
            out = df.copy()
            if "time" not in out.columns:
                out = out.reset_index()
                if "datetime" in out.columns:
                    out = out.rename(columns={"datetime": "time"})
                elif "date" in out.columns:
                    out = out.rename(columns={"date": "time"})
                elif "index" in out.columns:
                    out = out.rename(columns={"index": "time"})
            if "time" in out.columns:
                out["time"] = pd.to_datetime(out["time"], errors="coerce")
                out = out[out["time"].notna()]
            return out

        data_1m = _ensure_time_column(data_1m)
        data_1d = _ensure_time_column(data_1d)

        builder = self._make_period_bar_builder(stock_code=symbol)
        details: list[dict[str, Any]] = []
        rebuilt_map: dict[str, pd.DataFrame] = {}
        succeeded = 0
        failed = 0
        for p in target_periods:
            try:
                rebuilt = pd.DataFrame()
                if p == "1m":
                    rebuilt = data_1m.copy()
                elif p == "1d":
                    rebuilt = data_1d.copy()
                elif p == "5m":
                    _src = data_1m.copy()
                    if "time" in _src.columns:
                        _src = _src.set_index("time")
                    rebuilt = self._resample_ohlcv(_src, "5min") if _src is not None else pd.DataFrame()
                elif p in self._INTRADAY_CUSTOM_PERIODS:
                    rebuilt = builder.build_intraday_bars(
                        data_1m=data_1m.copy(),
                        period_minutes=int(self._INTRADAY_CUSTOM_PERIODS[p]),
                        daily_ref=data_1d.copy() if data_1d is not None else None,
                    )
                    # 构建完成后执行跨周期校验，写入 period_validation_report.jsonl
                    if rebuilt is not None and not rebuilt.empty:
                        try:
                            builder.cross_validate(p, rebuilt, daily_ref=data_1d.copy() if data_1d is not None else None)
                        except Exception:
                            pass
                elif p in self._MULTIDAY_CUSTOM_PERIODS:
                    rebuilt = builder.build_multiday_bars(
                        data_1d=data_1d.copy(),
                        trading_days_per_period=int(self._MULTIDAY_CUSTOM_PERIODS[p]),
                        listing_date=self.get_listing_date(symbol),
                    )
                    if rebuilt is not None and not rebuilt.empty:
                        try:
                            builder.cross_validate(p, rebuilt, daily_ref=data_1d.copy() if data_1d is not None else None)
                        except Exception:
                            pass
                elif p in self._PERIOD_AGGREGATION:
                    _src, rule = self._PERIOD_AGGREGATION[p]
                    rebuilt = builder.build_natural_calendar_bars(data_1d=data_1d.copy(), freq=rule)
                    if rebuilt is not None and not rebuilt.empty:
                        try:
                            builder.cross_validate(p, rebuilt, daily_ref=data_1d.copy() if data_1d is not None else None)
                        except Exception:
                            pass
                if rebuilt is None or rebuilt.empty:
                    failed += 1
                    details.append({"period": p, "status": "failed", "rows": 0, "reason": "rebuilt_empty"})
                    continue
                rebuilt_map[p] = rebuilt.copy()
                succeeded += 1
                details.append(
                    {
                        "period": p,
                        "status": "ok",
                        "rows": int(len(rebuilt)),
                        "persisted": p in {"1m", "5m", "1d"},
                        "persist_error": "",
                    }
                )
            except Exception as e:
                failed += 1
                details.append({"period": p, "status": "failed", "rows": 0, "reason": str(e)})
        rebuild_id = str(uuid.uuid4())
        persisted_periods = [p for p in ("1m", "5m", "1d") if p in rebuilt_map]
        atomic_ok = failed == 0
        atomic_error = ""
        if atomic_ok:
            atomic_ok, atomic_error = self._atomic_replace_rebuild_periods(
                stock_code=symbol,
                start_date=start_date,
                end_date=end_date,
                rebuilt_map=rebuilt_map,
                persisted_periods=persisted_periods,
            )
            if not atomic_ok:
                failed += 1
        row_stats = {p: int(len(rebuilt_map[p])) for p in rebuilt_map}
        receipt = self._write_rebuild_receipt(
            rebuild_id=rebuild_id,
            stock_code=symbol,
            start_date=start_date,
            end_date=end_date,
            target_periods=target_periods,
            persisted_periods=persisted_periods,
            row_stats=row_stats,
            status="success" if atomic_ok else "failed",
            error_message=atomic_error,
        )
        try:
            self.con.execute(
                """
                INSERT OR REPLACE INTO multiperiod_rebuild_audit (
                    rebuild_id, stock_code, start_date, end_date, periods_json,
                    persisted_periods_json, row_stats_json, receipt_hash, status, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    rebuild_id,
                    symbol,
                    start_date,
                    end_date,
                    json.dumps(target_periods, ensure_ascii=False),
                    json.dumps(persisted_periods, ensure_ascii=False),
                    json.dumps(row_stats, ensure_ascii=False),
                    str(receipt.get("receipt_hash", "")),
                    "success" if atomic_ok else "failed",
                    atomic_error[:500] if atomic_error else None,
                ],
            )
        except Exception:
            pass
        processed = len(target_periods)
        return {
            "ok": failed == 0 and atomic_ok,
            "processed": processed,
            "succeeded": succeeded,
            "failed": failed,
            "stock_code": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "details": details,
            "rebuild_id": rebuild_id,
            "atomic_replace": atomic_ok,
            "atomic_error": atomic_error,
            "audit_receipt": receipt,
        }

    def _atomic_replace_rebuild_periods(
        self,
        *,
        stock_code: str,
        start_date: str,
        end_date: str,
        rebuilt_map: dict[str, pd.DataFrame],
        persisted_periods: list[str],
    ) -> tuple[bool, str]:
        if not persisted_periods:
            return False, "no_persisted_periods"
        if self.con is None:
            return False, "duckdb_connection_missing"
        table_map = {"1m": ("stock_1m", "datetime"), "5m": ("stock_5m", "datetime"), "1d": ("stock_daily", "date")}
        write_lock = getattr(self, "_db_manager", None)
        write_lock = getattr(write_lock, "_write_lock", None) if write_lock else None
        if write_lock is not None:
            write_lock.acquire()
        try:
            self.con.execute("BEGIN")
            for period in persisted_periods:
                if period not in rebuilt_map:
                    continue
                table_name, date_col = table_map[period]
                df = rebuilt_map[period].copy()
                if df.empty:
                    raise ValueError(f"{period}_rebuilt_empty")
                if "time" not in df.columns:
                    if "datetime" in df.columns:
                        df = df.rename(columns={"datetime": "time"})
                    elif "date" in df.columns:
                        df = df.rename(columns={"date": "time"})
                    elif isinstance(df.index, pd.DatetimeIndex):
                        df = df.reset_index().rename(columns={"index": "time"})
                if "time" not in df.columns:
                    raise ValueError(f"{period}_missing_time_column")
                df["stock_code"] = stock_code
                df["period"] = period
                df["symbol_type"] = "stock"
                df["adjust_type"] = "none"
                df["factor"] = 1.0
                now_ts = pd.Timestamp.now()
                if "created_at" not in df.columns:
                    df["created_at"] = now_ts
                if "updated_at" not in df.columns:
                    df["updated_at"] = now_ts
                if date_col == "date":
                    df["date"] = pd.to_datetime(df["time"], errors="coerce").dt.date
                else:
                    df["datetime"] = pd.to_datetime(df["time"], errors="coerce")
                df = df[df[date_col].notna()]
                if df.empty:
                    raise ValueError(f"{period}_coerce_time_empty")
                table_columns = self.con.execute(f"DESCRIBE {table_name}").fetchdf()["column_name"].tolist()
                df_ordered = pd.DataFrame()
                for col in table_columns:
                    if col in df.columns:
                        df_ordered[col] = df[col]
                    else:
                        df_ordered[col] = None
                df_ordered = df_ordered.drop_duplicates(
                    subset=[c for c in [date_col, "stock_code", "period", "adjust_type"] if c in df_ordered.columns],
                    keep="last",
                )
                self.con.execute(
                    "DELETE FROM " + table_name + " WHERE stock_code = ? AND period = ? AND " + date_col + " >= ? AND " + date_col + " <= ?",
                    [stock_code, period, str(df_ordered[date_col].min()), str(df_ordered[date_col].max())],
                )
                temp_name = f"rebuild_temp_{period.replace(' ', '_').replace('/', '_')}"
                self.con.register(temp_name, df_ordered)
                self.con.execute("INSERT OR REPLACE INTO " + table_name + " SELECT * FROM " + temp_name)
                self.con.unregister(temp_name)
                self.con.execute(
                    """
                    INSERT OR REPLACE INTO data_ingestion_status (
                        stock_code, period, start_date, end_date, source, status, record_count, error_message,
                        schema_version, ingest_run_id, raw_hash, source_event_time
                    ) VALUES (
                        ?, ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        stock_code,
                        period,
                        start_date,
                        end_date,
                        "multiperiod_rebuild",
                        "success",
                        int(len(df_ordered)),
                        None,
                        CURRENT_SCHEMA_VERSION,
                        str(uuid.uuid4()),
                        hashlib.sha256(
                            df_ordered.head(200).to_json(orient="records", date_format="iso", force_ascii=False).encode("utf-8")
                        ).hexdigest(),
                        pd.to_datetime(df_ordered[date_col], errors="coerce").max(),
                    ],
                )
            self.con.execute("COMMIT")
            return True, ""
        except Exception as e:
            try:
                self.con.execute("ROLLBACK")
            except Exception:
                pass
            return False, str(e)
        finally:
            if write_lock is not None:
                write_lock.release()

    def _write_rebuild_receipt(
        self,
        *,
        rebuild_id: str,
        stock_code: str,
        start_date: str,
        end_date: str,
        target_periods: list[str],
        persisted_periods: list[str],
        row_stats: dict[str, int],
        status: str,
        error_message: str,
    ) -> dict[str, Any]:
        payload = {
            "rebuild_id": rebuild_id,
            "stock_code": stock_code,
            "start_date": start_date,
            "end_date": end_date,
            "target_periods": target_periods,
            "persisted_periods": persisted_periods,
            "row_stats": row_stats,
            "status": status,
            "error_message": error_message,
            "created_at": pd.Timestamp.now().isoformat(),
        }
        payload_bytes = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        payload["receipt_hash"] = hashlib.sha256(payload_bytes).hexdigest()
        try:
            artifacts_dir = Path(__file__).resolve().parents[1] / "artifacts"
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            latest_path = artifacts_dir / "rebuild_audit_latest.json"
            history_path = artifacts_dir / f"rebuild_audit_{rebuild_id}.json"
            latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            history_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return payload

    def get_quarantine_status_counts(self) -> dict[str, int]:
        if self.con is None:
            if not self.connect(read_only=False):
                return {
                    "pending": 0,
                    "failed": 0,
                    "resolved": 0,
                    "dead_letter": 0,
                    "total": 0,
                }
        self._ensure_tables_exist()
        try:
            rows = self.con.execute(
                """
                SELECT replay_status, COUNT(*) AS c
                FROM data_quarantine_log
                GROUP BY replay_status
                """
            ).fetchall()
        except Exception:
            return {
                "pending": 0,
                "failed": 0,
                "resolved": 0,
                "dead_letter": 0,
                "total": 0,
            }
        out = {"pending": 0, "failed": 0, "resolved": 0, "dead_letter": 0, "total": 0}
        for status, count in rows:
            k = str(status or "").strip()
            if k in out:
                out[k] = int(count or 0)
        out["total"] = int(sum(v for kk, v in out.items() if kk != "total"))
        return out

    def get_data_quality_incident_counts(self) -> dict[str, int]:
        if self.con is None:
            if not self.connect(read_only=False):
                return {"total": 0, "critical": 0, "high": 0, "medium": 0, "low": 0}
        self._ensure_tables_exist()
        try:
            rows = self.con.execute(
                """
                SELECT severity, COUNT(*) AS c
                FROM data_quality_incident
                GROUP BY severity
                """
            ).fetchall()
        except Exception:
            return {"total": 0, "critical": 0, "high": 0, "medium": 0, "low": 0}
        out = {"total": 0, "critical": 0, "high": 0, "medium": 0, "low": 0}
        for sev, count in rows:
            k = str(sev or "").strip().lower()
            if k in out:
                out[k] = int(count or 0)
        out["total"] = int(sum(v for kk, v in out.items() if kk != "total"))
        return out

    def get_step6_validation_metrics(self) -> dict[str, Any]:
        metrics = dict(self._step6_validation_metrics)
        sampled = int(metrics.get("sampled", 0) or 0)
        failed = int(metrics.get("hard_failed", 0) or 0)
        metrics["hard_fail_rate"] = (failed / sampled) if sampled > 0 else 0.0
        metrics["sample_rate"] = float(self._step6_validate_sample_rate)
        return metrics

    def _step6_should_validate(self, sample_basis: str) -> bool:
        r = float(self._step6_validate_sample_rate)
        if r >= 1.0:
            return True
        if r <= 0.0:
            return False
        h = hashlib.sha256(sample_basis.encode("utf-8")).hexdigest()
        bucket = int(h[:8], 16) / float(0xFFFFFFFF)
        return bucket <= r

    def generate_daily_sla_report(self, report_date: str | None = None) -> dict[str, Any]:
        if self.con is None:
            if not self.connect(read_only=False):
                return {}
        self._ensure_tables_exist()
        day = pd.to_datetime(report_date).date() if report_date else pd.Timestamp.now().date()
        day_s = str(day)
        write_stats = (0, 0)
        conflict_count = 0
        reject_count = 0
        lag_p95_ms: float | None = None
        try:
            write_row = self.con.execute(
                """
                SELECT COALESCE(SUM(actual_rows), 0), COALESCE(SUM(expected_rows), 0)
                FROM write_audit_log
                WHERE DATE(created_at) = ?
                """,
                [day_s],
            ).fetchone()
            if write_row:
                write_stats = (int(write_row[0] or 0), int(write_row[1] or 0))
        except Exception as e:
            self._logger.warning("SLA统计write_audit_log失败: %s", e)
        try:
            row = self.con.execute(
                "SELECT COUNT(*) FROM source_conflict_audit WHERE DATE(created_at) = ?",
                [day_s],
            ).fetchone()
            conflict_count = int(row[0] if row else 0)
        except Exception:
            conflict_count = 0
        try:
            row = self.con.execute(
                "SELECT COUNT(*) FROM realtime_reject_log WHERE DATE(created_at) = ?",
                [day_s],
            ).fetchone()
            reject_count = int(row[0] if row else 0)
        except Exception:
            reject_count = 0
        try:
            if self._get_table_columns("stock_raw_quote"):
                row = self.con.execute(
                    """
                    SELECT quantile_cont(
                        CAST(date_diff('millisecond', event_ts, ingest_ts) AS DOUBLE), 0.95
                    )
                    FROM stock_raw_quote
                    WHERE DATE(event_ts) = ?
                    """,
                    [day_s],
                ).fetchone()
                if row and row[0] is not None:
                    lag_p95_ms = float(row[0])
        except Exception as e:
            self._logger.warning("SLA统计lag失败: %s", e)
        actual_rows, expected_rows = write_stats
        completeness = float(actual_rows / expected_rows) if expected_rows > 0 else 1.0
        baseline = max(actual_rows, 1)
        consistency = max(0.0, 1.0 - float(conflict_count / baseline))
        lag_score = 1.0
        if lag_p95_ms is not None:
            lag_score = max(0.0, 1.0 - min(lag_p95_ms / 2000.0, 1.0))
        trust_score = 0.4 * completeness + 0.3 * consistency + 0.3 * lag_score
        step6_metrics = self.get_step6_validation_metrics()
        step6_total = int(step6_metrics.get("total", 0) or 0)
        step6_sampled = int(step6_metrics.get("sampled", 0) or 0)
        step6_skipped = int(step6_metrics.get("skipped", 0) or 0)
        step6_hard_failed = int(step6_metrics.get("hard_failed", 0) or 0)
        step6_hard_fail_rate = float(step6_metrics.get("hard_fail_rate", 0.0) or 0.0)
        step6_sample_rate = float(step6_metrics.get("sample_rate", self._step6_validate_sample_rate) or 0.0)
        canary_shadow_write_enabled = bool(self._canary_shadow_write_enabled)
        canary_shadow_only = bool(self._canary_shadow_only)
        gate_pass = (
            completeness >= 0.995
            and consistency >= 0.998
            and (lag_p95_ms is None or lag_p95_ms < 2000.0)
        )
        try:
            self.con.execute(
                """
                INSERT INTO data_quality_sla_daily (
                    report_date, completeness, consistency, lag_p95_ms, trust_score,
                    gate_pass, write_total_rows, write_expected_rows, conflict_count,
                    step6_total_checks, step6_sampled_checks, step6_skipped_checks,
                    step6_hard_failed_checks, step6_hard_fail_rate, step6_sample_rate,
                    canary_shadow_write_enabled, canary_shadow_only, reject_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(report_date) DO UPDATE SET
                    completeness=excluded.completeness,
                    consistency=excluded.consistency,
                    lag_p95_ms=excluded.lag_p95_ms,
                    trust_score=excluded.trust_score,
                    gate_pass=excluded.gate_pass,
                    write_total_rows=excluded.write_total_rows,
                    write_expected_rows=excluded.write_expected_rows,
                    conflict_count=excluded.conflict_count,
                    step6_total_checks=excluded.step6_total_checks,
                    step6_sampled_checks=excluded.step6_sampled_checks,
                    step6_skipped_checks=excluded.step6_skipped_checks,
                    step6_hard_failed_checks=excluded.step6_hard_failed_checks,
                    step6_hard_fail_rate=excluded.step6_hard_fail_rate,
                    step6_sample_rate=excluded.step6_sample_rate,
                    canary_shadow_write_enabled=excluded.canary_shadow_write_enabled,
                    canary_shadow_only=excluded.canary_shadow_only,
                    reject_count=excluded.reject_count
                """,
                [
                    day_s,
                    completeness,
                    consistency,
                    lag_p95_ms,
                    trust_score,
                    gate_pass,
                    actual_rows,
                    expected_rows,
                    conflict_count,
                    step6_total,
                    step6_sampled,
                    step6_skipped,
                    step6_hard_failed,
                    step6_hard_fail_rate,
                    step6_sample_rate,
                    canary_shadow_write_enabled,
                    canary_shadow_only,
                    reject_count,
                ],
            )
        except Exception as e:
            self._logger.warning("写入data_quality_sla_daily失败: %s", e)
        return {
            "report_date": day_s,
            "completeness": round(completeness, 6),
            "consistency": round(consistency, 6),
            "lag_p95_ms": None if lag_p95_ms is None else round(lag_p95_ms, 2),
            "trust_score": round(trust_score, 6),
            "gate_pass": bool(gate_pass),
            "write_total_rows": actual_rows,
            "write_expected_rows": expected_rows,
            "conflict_count": conflict_count,
            "step6_total_checks": step6_total,
            "step6_sampled_checks": step6_sampled,
            "step6_skipped_checks": step6_skipped,
            "step6_hard_failed_checks": step6_hard_failed,
            "step6_hard_fail_rate": round(step6_hard_fail_rate, 6),
            "step6_sample_rate": round(step6_sample_rate, 6),
            "canary_shadow_write_enabled": canary_shadow_write_enabled,
            "canary_shadow_only": canary_shadow_only,
            "reject_count": reject_count,
        }

    # --- 自然日历派生周期 → 源周期 + pandas resample freq ---
    # 仅保留自然日历周期（右边界对齐），日内/多日自定义周期由 PeriodBarBuilder 处理
    _PERIOD_AGGREGATION: dict[str, tuple[str, str]] = {
        "1w":  ("1d", "W-FRI"),
        "1M":  ("1d", "ME"),
        "1Q":  ("1d", "QE-DEC"),
        "6M":  ("1d", "6ME"),
        "1Y":  ("1d", "YE"),
        "2Y":  ("1d", "2YE"),
        "3Y":  ("1d", "3YE"),
        "5Y":  ("1d", "5YE"),
        "10Y": ("1d", "10YE"),
    }

    #: 日内自定义周期：{period_str: period_minutes}
    #: 从 1m 构建，A 股时段对齐，最后一根 K 线严格收敛于 1D 黄金标准
    #: 15m/30m/60m 由此路由（取代旧的简单 resample）
    _INTRADAY_CUSTOM_PERIODS: dict[str, int] = {
        "2m": 2, "10m": 10, "15m": 15, "20m": 20, "25m": 25,
        "30m": 30, "50m": 50, "60m": 60, "70m": 70, "120m": 120, "125m": 125,
    }

    #: 多日自定义周期：{period_str: trading_days}
    #: 从 1D 构建，上市首日左对齐；5d ≠ 1W（自然周），3M ≠ 1Q（自然季度）
    _MULTIDAY_CUSTOM_PERIODS: dict[str, int] = {
        "2d": 2, "3d": 3, "5d": 5, "10d": 10, "25d": 25, "50d": 50, "75d": 75,
        "2M": 42, "3M": 63, "5M": 105,
    }

    def _resolve_session_profile_for_symbol(self, stock_code: str | None) -> str:
        explicit_profile = str(os.environ.get("EASYXT_SESSION_PROFILE", "CN_A")).strip()
        if explicit_profile and explicit_profile.upper() != "AUTO":
            return explicit_profile
        rules_file = str(os.environ.get("EASYXT_SESSION_PROFILE_RULES_FILE", "config/session_profile_rules.json")).strip()
        path = Path(rules_file)
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        if not path.exists() or not stock_code:
            return "CN_A"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return "CN_A"
        if not isinstance(payload, dict):
            return "CN_A"
        rules = payload.get("rules")
        if not isinstance(rules, list):
            return str(payload.get("default_profile") or "CN_A")
        symbol = str(stock_code).strip().upper()
        for item in rules:
            if not isinstance(item, dict):
                continue
            pattern = str(item.get("pattern") or "").strip().upper()
            profile = str(item.get("profile") or "").strip()
            if not pattern or not profile:
                continue
            if fnmatchcase(symbol, pattern):
                return profile
        return str(payload.get("default_profile") or "CN_A")

    def _make_period_bar_builder(self, stock_code: str | None = None):
        from data_manager.period_bar_builder import PeriodBarBuilder

        return PeriodBarBuilder(
            session_profile=self._resolve_session_profile_for_symbol(stock_code),
            session_profile_file=str(os.environ.get("EASYXT_SESSION_PROFILE_FILE", "config/session_profiles.json")),
            alignment=str(os.environ.get("EASYXT_PERIOD_ALIGNMENT", "left")),
            anchor=str(os.environ.get("EASYXT_PERIOD_ANCHOR", "daily_close")),
            validation_report_file=str(
                os.environ.get("EASYXT_PERIOD_VALIDATION_REPORT_PATH", "artifacts/period_validation_report.jsonl")
            ),
        )

    @staticmethod
    def _parse_qmt_time_series(series: pd.Series) -> pd.Series:
        def _one(v):
            if v is None:
                return pd.NaT
            if isinstance(v, (int, float)) and not pd.isna(v):
                iv = int(v)
                sv = str(abs(iv))
                if len(sv) == 14:
                    return pd.to_datetime(str(iv), format="%Y%m%d%H%M%S", errors="coerce")
                if len(sv) == 8:
                    return pd.to_datetime(str(iv), format="%Y%m%d", errors="coerce")
                if abs(iv) >= 10**12:
                    return pd.to_datetime(iv, unit="ms", utc=True, errors="coerce").tz_convert("Asia/Shanghai").tz_localize(None)
                if abs(iv) >= 10**9:
                    return pd.to_datetime(iv, unit="s", utc=True, errors="coerce").tz_convert("Asia/Shanghai").tz_localize(None)
                return pd.to_datetime(v, errors="coerce")
            s = str(v).strip()
            if not s:
                return pd.NaT
            if s.isdigit():
                if len(s) == 14:
                    return pd.to_datetime(s, format="%Y%m%d%H%M%S", errors="coerce")
                if len(s) == 8:
                    return pd.to_datetime(s, format="%Y%m%d", errors="coerce")
            return pd.to_datetime(s, errors="coerce")

        return series.apply(_one)

    @staticmethod
    def _get_storage_target_period(period: str) -> tuple[str, str]:
        table_period = {"15m": "1m", "30m": "1m", "60m": "1m", "1w": "1d", "1M": "1d"}.get(period, period)
        stored_period = period
        return table_period, stored_period

    # ------------------------------------------------------------------
    # custom_period_bars 缓存读写
    # ------------------------------------------------------------------

    def _compute_adj_factor_hash(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        adjust: str = "none",
        source_period: str = "1d",
    ) -> str:
        source = str(source_period or "1d")
        adj = str(adjust or "none")
        if source != "1d" or adj == "none":
            return f"na:{source}:{adj}"
        if not self.con:
            return ""
        try:
            df = self.con.execute(
                "SELECT date, factor "
                "FROM stock_daily "
                "WHERE stock_code = ? AND period = '1d' "
                "AND date >= ? AND date <= ? "
                "ORDER BY date",
                [stock_code, start_date, end_date],
            ).df()
            if df is None or df.empty:
                return "empty:1d_factor"
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df[df["date"].notna()]
            if df.empty:
                return "empty:1d_factor"
            payload = (
                df["date"].dt.strftime("%Y-%m-%d").astype(str)
                + "|"
                + df["factor"].fillna(1.0).astype(float).round(6).astype(str)
            )
            return hashlib.sha256("\n".join(payload.tolist()).encode("utf-8")).hexdigest()[:24]
        except Exception:
            return ""

    def _read_cached_custom_bars(
        self,
        stock_code: str,
        period: str,
        start_date: str,
        end_date: str,
        adjust: str = "none",
        expected_adj_factor_hash: str = "",
    ) -> pd.DataFrame | None:
        """从 custom_period_bars 表读取预计算缓存，命中返回 DataFrame，未命中返回 None。"""
        try:
            if not self.con:
                return None
            table_exists = (
                self.con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'custom_period_bars'"
                ).fetchone()[0] > 0
            )
            if not table_exists:
                return None
            has_hash_col = "adj_factor_hash" in set(self._get_table_columns("custom_period_bars"))
            hash_expr = "COALESCE(adj_factor_hash, '') AS adj_factor_hash" if has_hash_col else "'' AS adj_factor_hash"
            df = self.con.execute(
                "SELECT stock_code, datetime, open, high, low, close, volume, amount, is_partial, "
                + hash_expr + " "
                "FROM custom_period_bars "
                "WHERE stock_code = ? AND period = ? AND adjust_type = ? "
                "AND datetime >= ? AND datetime <= ? "
                "ORDER BY datetime",
                [stock_code, period, adjust, start_date, end_date],
            ).df()
            if df is None or df.empty:
                return None
            if expected_adj_factor_hash and has_hash_col:
                hashes = {str(v or "") for v in df["adj_factor_hash"].tolist()}
                is_legacy_empty = hashes == {""}
                expected_is_non_adj = str(expected_adj_factor_hash).startswith("na:")
                if hashes != {expected_adj_factor_hash} and not (is_legacy_empty and expected_is_non_adj):
                    self._logger.debug(
                        "custom_period_bars 缓存失效（adj_factor_hash mismatch）: %s %s expected=%s got=%s",
                        stock_code,
                        period,
                        expected_adj_factor_hash,
                        sorted(hashes),
                    )
                    return None
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df = df[df["datetime"].notna()]
            if "adj_factor_hash" in df.columns:
                df = df.drop(columns=["adj_factor_hash"])
            df.set_index("datetime", inplace=True)
            self._logger.debug(
                "custom_period_bars 缓存命中: %s %s %d 行", stock_code, period, len(df)
            )
            return df
        except Exception as exc:
            self._logger.debug("custom_period_bars 缓存读取失败: %s", exc)
            return None

    def _save_custom_period_bars(
        self,
        df: pd.DataFrame,
        stock_code: str,
        period: str,
        adjust: str = "none",
        adj_factor_hash: str = "",
    ) -> None:
        """将构建好的自定义周期 K 线写入 custom_period_bars 缓存表。

        采用 DELETE-INSERT 策略：删除该品种+周期+复权类型的旧数据后批量写入。
        """
        if df is None or df.empty:
            return
        write_lock = getattr(self, "_db_manager", None)
        write_lock = getattr(write_lock, "_write_lock", None) if write_lock else None
        if write_lock is not None:
            write_lock.acquire()
        try:
            if not self.con:
                return
            self._ensure_tables_exist()
            # 准备写入数据：把 DatetimeIndex 转为列
            if isinstance(df.index, pd.DatetimeIndex):
                df_save = df.reset_index()
                # reset_index 后列名可能是原 index.name 或 'index'
                idx_col = df.index.name or "index"
                if idx_col != "datetime" and idx_col in df_save.columns:
                    df_save = df_save.rename(columns={idx_col: "datetime"})
            else:
                df_save = df.copy()
            # 统一列名
            if "time" in df_save.columns and "datetime" not in df_save.columns:
                df_save = df_save.rename(columns={"time": "datetime"})
            if "datetime" not in df_save.columns:
                self._logger.warning("custom_period_bars 写入跳过: 无 datetime 列")
                return
            df_save["stock_code"] = stock_code
            df_save["period"] = period
            df_save["adjust_type"] = adjust
            df_save["adj_factor_hash"] = str(adj_factor_hash or "")
            if "is_partial" not in df_save.columns:
                df_save["is_partial"] = False
            # 只保留需要的列（与 DDL 列顺序一致，不含 created_at）
            target_cols = ["stock_code", "period", "datetime", "open", "high", "low",
                           "close", "volume", "amount", "adjust_type", "adj_factor_hash", "is_partial"]
            # amount 列可能不存在，补 0
            if "amount" not in df_save.columns:
                df_save["amount"] = 0
            df_save = df_save[[c for c in target_cols if c in df_save.columns]]
            # DELETE 旧数据
            self.con.execute(
                "DELETE FROM custom_period_bars "
                "WHERE stock_code = ? AND period = ? AND adjust_type = ?",
                [stock_code, period, adjust],
            )
            # INSERT（显式列名，跳过 created_at DEFAULT）
            df_insert = df_save[target_cols].copy()
            self.con.register("df_custom_period_insert_temp", df_insert)
            self.con.execute(
                "INSERT INTO custom_period_bars "
                "(stock_code, period, datetime, open, high, low, close, volume, amount, adjust_type, adj_factor_hash, is_partial) "
                "SELECT stock_code, period, datetime, open, high, low, close, volume, amount, adjust_type, adj_factor_hash, is_partial "
                "FROM df_custom_period_insert_temp"
            )
            self.con.unregister("df_custom_period_insert_temp")
            self._logger.debug(
                "custom_period_bars 写入: %s %s %d 行", stock_code, period, len(df_save)
            )
        except Exception as exc:
            self._logger.warning("custom_period_bars 写入失败: %s", exc)
        finally:
            if write_lock is not None:
                write_lock.release()

    @staticmethod
    def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame | None:
        """将细粒度 OHLCV DataFrame resample 到更粗的周期"""
        if df is None or df.empty:
            return df
        agg: dict[str, Any] = {}
        for col, fn in [("open", "first"), ("high", "max"), ("low", "min"),
                        ("close", "last"), ("volume", "sum"), ("amount", "sum")]:
            if col in df.columns:
                agg[col] = fn
        if not agg:
            return df
        resampled = df.resample(rule).agg(agg)  # type: ignore[arg-type]
        resampled = resampled.dropna(subset=["open"])
        if "stock_code" in df.columns:
            resampled["stock_code"] = df["stock_code"].iloc[0]
        return resampled

    @staticmethod
    def _normalize_date_str(d: str) -> str:
        """将 'YYYYMMDD' 格式标准化为 'YYYY-MM-DD'（DuckDB DATE/TIMESTAMP 兼容）。"""
        if d and isinstance(d, str):
            stripped = d.replace("-", "")
            if len(stripped) >= 8 and stripped[:8].isdigit() and "-" not in d[:10]:
                return f"{stripped[:4]}-{stripped[4:6]}-{stripped[6:8]}"
        return d

    def _read_from_duckdb(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
        _allow_aggregate: bool = True,
        listing_date: str | None = None,
    ) -> pd.DataFrame | None:
        """从DuckDB读取数据 - 修复版（添加表存在性检查 + 派生周期聚合）"""
        try:
            start_date = self._normalize_date_str(start_date)
            end_date = self._normalize_date_str(end_date)
            # --- 派生周期聚合策略 ---
            # ── 日内自定义周期：A股时段对齐，严格收敛于1D黄金标准 ──
            if _allow_aggregate and period in self._INTRADAY_CUSTOM_PERIODS:
                expected_hash = self._compute_adj_factor_hash(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                    source_period="1m",
                )
                # 优先读缓存
                cached = self._read_cached_custom_bars(
                    stock_code,
                    period,
                    start_date,
                    end_date,
                    adjust,
                    expected_adj_factor_hash=expected_hash,
                )
                if cached is not None and not cached.empty:
                    return cached
                period_minutes = self._INTRADAY_CUSTOM_PERIODS[period]
                src_1m = self._read_from_duckdb(
                    stock_code, start_date, end_date, "1m", adjust, _allow_aggregate=False
                )
                if src_1m is None or src_1m.empty:
                    return src_1m
                try:
                    daily_ref = self._read_from_duckdb(
                        stock_code, start_date, end_date, "1d", adjust, _allow_aggregate=False
                    )
                except Exception:
                    daily_ref = None
                result = self._make_period_bar_builder(stock_code=stock_code).build_intraday_bars(
                    data_1m=src_1m, period_minutes=period_minutes, daily_ref=daily_ref
                )
                if result is not None and not result.empty:
                    self._save_custom_period_bars(
                        result,
                        stock_code,
                        period,
                        adjust,
                        adj_factor_hash=expected_hash,
                    )
                return result if result is not None and not result.empty else None

            # ── 多日自定义周期：上市首日左对齐，N交易日≠自然周/月 ──
            if _allow_aggregate and period in self._MULTIDAY_CUSTOM_PERIODS:
                listing_date = listing_date or self.get_listing_date(stock_code)
                expected_hash = self._compute_adj_factor_hash(
                    stock_code=stock_code,
                    start_date=listing_date,
                    end_date=end_date,
                    adjust=adjust,
                    source_period="1d",
                )
                # 优先读缓存
                cached = self._read_cached_custom_bars(
                    stock_code,
                    period,
                    start_date,
                    end_date,
                    adjust,
                    expected_adj_factor_hash=expected_hash,
                )
                if cached is not None and not cached.empty:
                    return cached
                trading_days = self._MULTIDAY_CUSTOM_PERIODS[period]
                # 刚性约束：必须从上市首日开始拉全历史 1D，才能保证左对齐计数正确
                src_1d = self._read_from_duckdb(
                    stock_code, listing_date, end_date, "1d", adjust, _allow_aggregate=False
                )
                if src_1d is None or src_1d.empty:
                    return src_1d
                result = self._make_period_bar_builder(stock_code=stock_code).build_multiday_bars(
                    data_1d=src_1d,
                    trading_days_per_period=trading_days,
                    listing_date=listing_date,
                )
                if result is None or result.empty:
                    return None
                # 写缓存（全量，裁剪前）
                self._save_custom_period_bars(
                    result,
                    stock_code,
                    period,
                    adjust,
                    adj_factor_hash=expected_hash,
                )
                # 按用户请求的视图范围裁剪输出（不影响从上市首日起的计数对齐）
                if start_date:
                    result = result[result["time"] >= pd.Timestamp(start_date)]
                return result if not result.empty else None

            if _allow_aggregate and period in self._PERIOD_AGGREGATION:
                src_period, rule = self._PERIOD_AGGREGATION[period]
                # 自然日历周期（1w/1M/1Q/...）始终从 1d 聚合，跳过直存读取
                _AGGREGATE_ONLY = set(self._PERIOD_AGGREGATION.keys())
                if period not in _AGGREGATE_ONLY:
                    direct = self._read_from_duckdb(
                        stock_code,
                        start_date,
                        end_date,
                        period,
                        adjust,
                        _allow_aggregate=False,
                    )
                    if direct is not None and not direct.empty:
                        return direct
                # 从源周期聚合
                src_df = self._read_from_duckdb(stock_code, start_date, end_date, src_period, adjust)
                if src_df is None or src_df.empty:
                    return src_df
                return self._resample_ohlcv(src_df, rule)

            # 确定表名
            table_map = {
                "1d": "stock_daily",
                "1m": "stock_1m",
                "5m": "stock_5m",
                "tick": "stock_tick",
            }
            table_period, stored_period = self._get_storage_target_period(period)
            table_name = table_map.get(table_period, "stock_daily")
            date_col = "date" if table_name == "stock_daily" else "datetime"

            # 检查表是否存在（修复首次使用问题）
            table_exists = (
                self.con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                    [table_name],
                ).fetchone()[0]
                > 0
            )

            if not table_exists:
                self._logger.debug("表 %s 不存在，返回空数据", table_name)
                return None

            # 确定列名（根据复权类型）
            # stock_1m / stock_5m / stock_tick 只有原始列，无复权列；仅 stock_daily 有五维复权列
            if table_name != "stock_daily":
                # 分钟线表：只有原始 OHLCV，忽略 adjust 参数
                price_cols = ["open", "high", "low", "close"]
            elif adjust == "none":
                price_cols = ["open", "high", "low", "close"]
            elif adjust == "front":
                price_cols = ["open_front", "high_front", "low_front", "close_front"]
            elif adjust == "back":
                price_cols = ["open_back", "high_back", "low_back", "close_back"]
            elif adjust == "geometric_front":
                price_cols = [
                    "open_geometric_front",
                    "high_geometric_front",
                    "low_geometric_front",
                    "close_geometric_front",
                ]
            elif adjust == "geometric_back":
                price_cols = [
                    "open_geometric_back",
                    "high_geometric_back",
                    "low_geometric_back",
                    "close_geometric_back",
                ]
            else:
                price_cols = ["open", "high", "low", "close"]

            # 构建SQL（列名/表名来自内部允许列表，数据参数化）
            sql = (
                "SELECT stock_code,"
                " " + date_col + " as datetime,"
                " " + price_cols[0] + " as open,"
                " " + price_cols[1] + " as high,"
                " " + price_cols[2] + " as low,"
                " " + price_cols[3] + " as close,"
                " volume, amount"
                " FROM " + table_name +
                " WHERE stock_code = ?"
                " AND period = ?"
                " AND " + date_col + " >= ?"
                " AND " + date_col + " <= ?"
                " ORDER BY " + date_col
            )

            # 执行查询
            df = self.con.execute(sql, [stock_code, stored_period, start_date, end_date]).df()

            if not df.empty:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df = df[df["datetime"].notna()]
                df.set_index("datetime", inplace=True)

                # 删除全为NaN的列（某些复权类型可能不存在）
                df = df.dropna(axis=1, how="all")

            return df

        except Exception:
            # 可能是表不存在或列不存在
            return None

    def get_listing_date(self, stock_code: str) -> str:
        """获取股票/期货上市首个交易日。

        优先级：XTQuant → DuckDB stock_daily 最早记录 → '1990-01-01'。
        结果内存缓存，频繁画面滚动时无开销。
        """
        if not hasattr(self, "_listing_date_cache") or not isinstance(self._listing_date_cache, dict):
            self._listing_date_cache = {}
        cached = self._listing_date_cache.get(stock_code)
        if cached:
            return cached

        # 1. XTQuant 在线（OpenDate = 股票 IPO 日, CreateDate = 期货上市日）
        if os.environ.get("EASYXT_ENABLE_XT_LISTING_DATE", "0") in ("1", "true", "True"):
            try:
                from xtquant import xtdata
                detail = xtdata.get_instrument_detail(stock_code)
                if detail:
                    raw = detail.get("OpenDate") or detail.get("CreateDate")
                    if raw:
                        s = str(int(raw)).strip()
                        if len(s) == 8:
                            dt_str = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
                            self._listing_date_cache[stock_code] = dt_str
                            return dt_str
            except Exception:
                pass

        # 2. DuckDB stock_daily 最早记录
        if bool(getattr(self, "duckdb_available", True)) and self.con is not None:
            try:
                row = self.con.execute(
                    "SELECT MIN(date) AS d FROM stock_daily WHERE stock_code = ? AND period = '1d'",
                    [stock_code],
                ).df()
                if not row.empty and pd.notna(row["d"].iloc[0]):
                    dt_str = pd.to_datetime(row["d"].iloc[0]).strftime("%Y-%m-%d")
                    self._listing_date_cache[stock_code] = dt_str
                    return dt_str
            except Exception:
                pass

        # 3. 兜底：中国证券市场最早开业日
        return "1990-01-01"

    def get_stock_date_range(self, stock_code: str, period: str) -> tuple[str, str] | None:
        if not self.duckdb_available or not self.con:
            return None
        table_map = {
            "1d": ("stock_daily", "date"),
            "1m": ("stock_1m", "datetime"),
            "5m": ("stock_5m", "datetime"),
            "tick": ("stock_tick", "datetime"),
        }
        table_period, stored_period = self._get_storage_target_period(period)
        table_name, date_col = table_map.get(table_period, ("stock_daily", "date"))
        try:
            sql = (
                "SELECT MIN(" + date_col + ") as start_date, MAX(" + date_col + ") as end_date"
                " FROM " + table_name + " WHERE stock_code = ? AND period = ?"
            )
            df = self.con.execute(sql, [stock_code, stored_period]).df()
            if not df.empty:
                start_date = pd.to_datetime(df["start_date"].iloc[0], errors="coerce")
                end_date = pd.to_datetime(df["end_date"].iloc[0], errors="coerce")
                if pd.notna(start_date) and pd.notna(end_date):
                    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            # 对于派生周期，回退查询源周期的日期范围
            _src_period = None
            if period in self._PERIOD_AGGREGATION:
                _src_period = self._PERIOD_AGGREGATION[period][0]
            elif period in self._INTRADAY_CUSTOM_PERIODS:
                _src_period = "1m"
            elif period in self._MULTIDAY_CUSTOM_PERIODS:
                _src_period = "1d"
            if _src_period is not None:
                src_table_period, src_stored_period = self._get_storage_target_period(_src_period)
                src_table_name, src_date_col = table_map.get(src_table_period, ("stock_daily", "date"))
                src_sql = (
                    "SELECT MIN(" + src_date_col + ") as start_date, MAX(" + src_date_col + ") as end_date"
                    " FROM " + src_table_name + " WHERE stock_code = ? AND period = ?"
                )
                src_df = self.con.execute(src_sql, [stock_code, src_stored_period]).df()
                if src_df.empty:
                    return None
                start_date = pd.to_datetime(src_df["start_date"].iloc[0], errors="coerce")
                end_date = pd.to_datetime(src_df["end_date"].iloc[0], errors="coerce")
                if pd.isna(start_date) or pd.isna(end_date):
                    return None
                return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            return None
        except Exception:
            return None

    def _read_from_qmt(
        self, stock_code: str, start_date: str, end_date: str, period: str
    ) -> pd.DataFrame | None:
        if os.environ.get("EASYXT_ENABLE_QMT_ONLINE", "1") not in ("1", "true", "True"):
            return None
        try:
            from xtquant import xtdata

            if period in {"tick", "l2transaction", "transaction"}:
                if period == "tick":
                    return self._read_tick_from_qmt(xtdata, stock_code, start_date, end_date)
                return self._read_transaction_from_qmt(xtdata, stock_code, start_date, end_date)  # type: ignore[return-value]

            # 防止未来日期导致空数据
            try:
                start_ts = pd.to_datetime(start_date)
                end_ts = pd.to_datetime(end_date)
                today = pd.Timestamp.today().normalize()
                if end_ts > today:
                    end_ts = today
                if start_ts > end_ts:
                    start_ts = end_ts - pd.Timedelta(days=365)
                start_date = start_ts.strftime("%Y-%m-%d")
                end_date = end_ts.strftime("%Y-%m-%d")
            except Exception:
                pass

            # 格式化日期字符串
            start_str = start_date.replace("-", "")
            end_str = end_date.replace("-", "")

            # 针对不同周期调整时间格式
            if period in ["1m", "5m", "15m", "30m", "60m"]:
                if len(start_str) == 8:
                    start_str += "000000"
                if len(end_str) == 8:
                    # 结束日期补到当天末尾，确保包含当天全部 K 线数据
                    # 若补 000000（当天0点）则该天数据全部被排除（off-by-one）
                    end_str += "235959"

            qmt_period = period
            self._logger.debug("QMT请求参数: %s %s~%s %s", stock_code, start_str, end_str, qmt_period)

            # 下载数据
            xtdata.download_history_data(
                stock_code, period=qmt_period, start_time=start_str, end_time=end_str
            )

            # 获取数据
            data = xtdata.get_market_data_ex(
                stock_list=[stock_code],
                period=qmt_period,
                start_time=start_str,
                end_time=end_str,
                count=-1
            )

            if data is None:
                self._logger.error("QMT返回None")
                return None

            if isinstance(data, dict):
                if stock_code in data:
                    df = data[stock_code]
                else:
                    self._logger.error("QMT返回字典中未找到 %s", stock_code)
                    return None
            else:
                df = data

            if df is None or df.empty:
                self._logger.warning("QMT返回空DataFrame")
                return None

            # 统一列名
            df = df.reset_index()
            # QMT返回的列名通常是 time, open, high, low, close, volume, amount 等
            # 需要根据实际情况调整

            if "time" in df.columns:
                parsed_time = self._parse_qmt_time_series(df["time"])
                df["time"] = parsed_time.dt.strftime("%Y-%m-%d %H:%M:%S")

                # 如果是日线，只保留日期部分
                if period == "1d":
                    if "date" not in df.columns:
                        df["date"] = df["time"].apply(lambda x: x.split(" ")[0])
                    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
                    df["datetime"] = pd.to_datetime(df["date"], errors="coerce")
                elif period in ("1w", "1M"):
                    df["datetime"] = pd.to_datetime(parsed_time, errors="coerce").dt.normalize()
                else:
                    if "datetime" in df.columns:
                        dt_src = df["datetime"]
                    else:
                        dt_src = df["time"]
                    df["datetime"] = pd.to_datetime(dt_src, errors="coerce")
            elif "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            elif "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
                df["datetime"] = pd.to_datetime(df["date"], errors="coerce")

            if "datetime" not in df.columns:
                return None
            df = df[df["datetime"].notna()]
            for col in ["open", "high", "low", "close"]:
                if col not in df.columns:
                    return None
            if "volume" not in df.columns:
                df["volume"] = 0
            if "amount" not in df.columns:
                df["amount"] = 0
            df = df.set_index("datetime", drop=False).sort_index()

            return df

        except Exception as e:
            self._logger.error("QMT 数据获取失败: %s", e)
            import traceback
            traceback.print_exc()
            return None

    def _read_tick_from_qmt(
        self, xtdata, stock_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        try:
            start_str = pd.to_datetime(start_date).strftime("%Y%m%d")
            end_str = pd.to_datetime(end_date).strftime("%Y%m%d")
            tick_data = xtdata.get_market_data_ex(
                stock_list=[stock_code],
                period="tick",
                start_time=start_str,
                end_time=end_str,
            )
            if isinstance(tick_data, dict) and stock_code in tick_data:
                df = tick_data[stock_code]
            else:
                df = tick_data if isinstance(tick_data, pd.DataFrame) else None
            if df is None or df.empty:
                return None
            if "time" in df.columns:
                ts = df["time"]
                if ts.dtype.kind in {"i", "u", "f"}:
                    # QMT API 返回 UTC epoch ms，必须经过契约层转为北京时间
                    df["datetime"] = qmt_ms_to_beijing(ts)
                else:
                    df["datetime"] = pd.to_datetime(ts, errors="coerce")
            elif "datetime" not in df.columns:
                df["datetime"] = pd.to_datetime(df.index, errors="coerce")
            df = df[df["datetime"].notna()]
            for col in ["lastPrice", "volume", "amount"]:
                if col not in df.columns:
                    df[col] = 0
            df = df[["datetime", "lastPrice", "volume", "amount"]].copy()
            df = df.sort_values("datetime")
            df = df.set_index("datetime")
            df.index.name = None
            return df
        except Exception:
            return None

    def _read_transaction_from_qmt(
        self, xtdata, stock_code: str, start_date: str, end_date: str
    ) -> tuple[pd.DataFrame | None, bool]:
        try:
            start_str = pd.to_datetime(start_date).strftime("%Y%m%d")
            end_str = pd.to_datetime(end_date).strftime("%Y%m%d")
            l2_data = None
            try:
                l2_data = xtdata.get_market_data_ex(
                    stock_list=[stock_code],
                    period="l2transaction",
                    start_time=start_str,
                    end_time=end_str,
                )
            except Exception:
                l2_data = None
            if isinstance(l2_data, dict) and stock_code in l2_data:
                df = l2_data[stock_code]
            else:
                df = l2_data if isinstance(l2_data, pd.DataFrame) else None
            if df is not None and not df.empty:
                if "time" in df.columns:
                    ts = df["time"]
                    if ts.dtype.kind in {"i", "u", "f"}:
                        # QMT API 返回 UTC epoch ms，必须经过契约层转为北京时间
                        df["datetime"] = qmt_ms_to_beijing(ts)
                    else:
                        df["datetime"] = pd.to_datetime(ts, errors="coerce")
                elif "datetime" not in df.columns:
                    df["datetime"] = pd.to_datetime(df.index, errors="coerce")
                df = df[df["datetime"].notna()]
                price_col = "price" if "price" in df.columns else "lastPrice"
                volume_col = "volume" if "volume" in df.columns else None
                amount_col = "amount" if "amount" in df.columns else None
                bs_flag_col = None
                for col in ["bsflag", "bsFlag", "bs_flag", "side"]:
                    if col in df.columns:
                        bs_flag_col = col
                        break
                trade_id_col = None
                for col in ["trade_id", "tradeId", "index", "seq"]:
                    if col in df.columns:
                        trade_id_col = col
                        break
                out = pd.DataFrame(
                    {
                        "datetime": df["datetime"],
                        "price": df[price_col] if price_col in df.columns else 0,
                        "volume": df[volume_col] if volume_col else 0,
                        "amount": df[amount_col] if amount_col else 0,
                        "bs_flag": df[bs_flag_col] if bs_flag_col else None,
                        "trade_id": df[trade_id_col] if trade_id_col else 0,
                    }
                )
                out = out.sort_values("datetime")
                out = out.set_index("datetime")
                return out, False
            df = self._read_tick_from_qmt(xtdata, stock_code, start_date, end_date)
            if df is None or df.empty:
                return None, True
            dt_series = pd.to_datetime(
                df["datetime"] if "datetime" in df.columns else df.index,
                errors="coerce",
            )
            out = pd.DataFrame(
                {
                    "datetime": dt_series.to_numpy(),
                    "price": (df["lastPrice"] if "lastPrice" in df.columns else 0),
                    "volume": (df["volume"] if "volume" in df.columns else 0),
                    "amount": (df["amount"] if "amount" in df.columns else 0),
                }
            )
            out = out[out["datetime"].notna()]
            out = out.sort_values("datetime")
            out = out.set_index("datetime")
            return out, True
        except Exception:
            return None, True

    # 指数路由规则——从外置配置负载，初始化时填入
    _AKSHARE_ROUTING_CFG: "dict[str, Any]" = {}
    _AKSHARE_ROUTING_LOADED: bool = False

    @classmethod
    def _load_akshare_routing(cls) -> None:
        """Lazy加载 config/akshare_routing.json，失败时使用内置default。"""
        if cls._AKSHARE_ROUTING_LOADED:
            return
        cls._AKSHARE_ROUTING_LOADED = True
        cfg_path = Path(__file__).resolve().parent.parent / "config" / "akshare_routing.json"
        try:
            import json as _json
            with open(cfg_path, encoding="utf-8") as f:
                cls._AKSHARE_ROUTING_CFG = _json.load(f)
        except Exception:
            # 内置fallback——不依赖配置文件也能运行
            cls._AKSHARE_ROUTING_CFG = {
                "index_rules": {
                    "suffix_sh_prefixes": ["000", "399", "999", "688"],
                    "suffix_sz_prefixes": ["399"],
                    "explicit_index_codes": []
                },
                "akshare_retry": {"max_retries": 2, "backoff_seconds": 5, "timeout_seconds": 20}
            }

    @classmethod
    def _is_index_code(cls, stock_code: str) -> bool:
        """判断股票代码是否为指数（规则外置于 config/akshare_routing.json）。"""
        cls._load_akshare_routing()
        rules = cls._AKSHARE_ROUTING_CFG.get("index_rules", {})
        explicit = rules.get("explicit_index_codes", [])
        if stock_code in explicit:
            return True
        plain = stock_code.replace(".SZ", "").replace(".SH", "").replace(".CSI", "")
        if stock_code.endswith(".SH"):
            sh_prefixes = tuple(rules.get("suffix_sh_prefixes", ["000", "399", "999", "688"]))
            return plain.startswith(sh_prefixes)
        if stock_code.endswith(".SZ"):
            sz_prefixes = tuple(rules.get("suffix_sz_prefixes", ["399"]))
            return plain.startswith(sz_prefixes)
        return False

    def _read_from_akshare(
        self, stock_code: str, start_date: str, end_date: str, period: str
    ) -> pd.DataFrame | None:
        """AKShare 数据拉取：支持股票/指数接口自动路由 + 零配置重试。"""
        try:
            import akshare as ak
        except Exception as e:
            self._log(f"[ERROR] AKShare 导入失败: {e}")
            return None

        # 从外置配置读取重试参数
        self._load_akshare_routing()
        retry_cfg = self._AKSHARE_ROUTING_CFG.get("akshare_retry", {})
        max_retries: int = int(retry_cfg.get("max_retries", 2))
        backoff_s: float = float(retry_cfg.get("backoff_seconds", 5))

        symbol = stock_code.replace(".SZ", "").replace(".SH", "")
        start_str = start_date.replace("-", "")
        end_str = end_date.replace("-", "")
        is_index = self._is_index_code(stock_code)

        def _fetch_once() -> pd.DataFrame | None:
            """  单次拉取，返回原始 df 或 None。"""
            if period == "1d":
                if is_index:
                    return ak.index_zh_a_hist(
                        symbol=symbol, period="daily", start_date=start_str, end_date=end_str
                    )
                stock_df = ak.stock_zh_a_hist(
                    symbol=symbol, period="daily", start_date=start_str, end_date=end_str, adjust=""
                )
                if stock_df is not None and not stock_df.empty:
                    return stock_df
                if symbol.startswith(("5", "15", "16", "18")):
                    return ak.fund_etf_hist_em(
                        symbol=symbol, period="daily", start_date=start_str, end_date=end_str, adjust=""
                    )
                return stock_df
            if period in {"1m", "5m"}:
                if is_index:
                    self._log(f"[WARNING] AKShare 暂不支持指数分钟线: {stock_code}")
                    return None
                period_map = {"1m": "1", "5m": "5"}
                return ak.stock_zh_a_hist_min_em(
                    symbol=symbol,
                    start_date=start_str,
                    end_date=end_str,
                    period=period_map[period],
                    adjust="",
                )
            return None  # 不支持的周期

        df: pd.DataFrame | None = None
        last_err: Exception | None = None
        t0 = time.perf_counter()
        for attempt in range(max_retries + 1):
            try:
                df = _fetch_once()
                if df is not None and not df.empty:
                    elapsed = time.perf_counter() - t0
                    src_tag = "index" if is_index else "stock"
                    self._log(
                        f"[INFO] AKShare {src_tag} 成功 {stock_code}"
                        f" rows={len(df)} attempt={attempt+1} elapsed={elapsed:.1f}s"
                    )
                    break
                last_err = None
            except Exception as e:
                last_err = e
                elapsed = time.perf_counter() - t0
                if attempt < max_retries:
                    self._log(
                        f"[WARN] AKShare第{attempt+1}次失败 {stock_code}"
                        f" elapsed={elapsed:.1f}s err={e}，{backoff_s}s后重试"
                    )
                    time.sleep(backoff_s)
                else:
                    self._log(
                        f"[ERROR] AKShare全部重试耗尽 {stock_code}"
                        f" {start_date}~{end_date} attempt={attempt+1} elapsed={elapsed:.1f}s: {e}"
                    )
                    return None

        if df is None or df.empty:
            if last_err:
                return None  # 已在循环中处理
            return None

        # 列名归一化
        rename_map = {
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
        }
        df = df.rename(columns=rename_map)
        if "date" not in df.columns:
            self._log(f"[ERROR] AKShare 返回列名异常，缺少 date 列: {list(df.columns)[:6]}")
            return None
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()]
        for col in ["open", "high", "low", "close"]:
            if col not in df.columns:
                self._log(f"[ERROR] AKShare 返回数据缺少必要列: {col}")
                return None
        if "volume" not in df.columns:
            df["volume"] = 0
        if "amount" not in df.columns:
            df["amount"] = 0
        df = df[["date", "open", "high", "low", "close", "volume", "amount"]]
        df = df.dropna(subset=["open", "high", "low", "close"])
        df = df.rename(columns={"date": "datetime"})
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)
        df = df[~df.index.duplicated(keep="first")]
        return df

    def _read_from_tushare(
        self, stock_code: str, start_date: str, end_date: str, period: str
    ) -> pd.DataFrame | None:
        if period != "1d":
            return None
        if not self._tushare_token:
            return None
        try:
            import tushare as ts

            ts.set_token(self._tushare_token)
            pro = ts.pro_api(self._tushare_token)
            start_str = pd.to_datetime(start_date).strftime("%Y%m%d")
            end_str = pd.to_datetime(end_date).strftime("%Y%m%d")
            df = pro.daily(
                ts_code=stock_code,
                start_date=start_str,
                end_date=end_str,
                fields="ts_code,trade_date,open,high,low,close,vol,amount",
            )
            if df is None or df.empty:
                return None
            rename_map = {"trade_date": "datetime", "vol": "volume", "amount": "amount"}
            df = df.rename(columns=rename_map)
            df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d", errors="coerce")
            df = df[df["datetime"].notna()]
            for col in ["open", "high", "low", "close", "volume"]:
                if col not in df.columns:
                    return None
                df[col] = pd.to_numeric(df[col], errors="coerce")
            if "amount" not in df.columns:
                df["amount"] = 0
            df = df.dropna(subset=["open", "high", "low", "close"])
            df = df[["datetime", "open", "high", "low", "close", "volume", "amount"]]
            df = df.sort_values("datetime")
            df.set_index("datetime", inplace=True)
            df = df[~df.index.duplicated(keep="last")]
            return df
        except Exception as e:
            self._log(f"[WARNING] Tushare 拉取失败 {stock_code}: {e}")
            return None

    def _get_dividends_from_qmt(
        self, stock_code: str, start_date, end_date
    ) -> pd.DataFrame | None:
        """
        从QMT获取分红数据，用于计算复权价格

        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame: 分红数据，包含 ex_date, dividend_per_share 等列
        """
        try:
            from xtquant import xtdata

            # 转换日期格式
            start_str = pd.to_datetime(start_date).strftime("%Y%m%d")
            end_str = pd.to_datetime(end_date).strftime("%Y%m%d")

            # 调用QMT接口获取分红数据
            divid_data = xtdata.get_divid_factors(stock_code, start_str, end_str)

            if divid_data is None or divid_data.empty:
                self._logger.debug("无分红数据: %s", stock_code)
                return pd.DataFrame()

            # 转换为标准格式
            # QMT返回的数据可能包含多列，我们需要提取必要的列
            dividends_df = pd.DataFrame()

            # 检查返回的数据结构并提取需要的字段
            if isinstance(divid_data, pd.DataFrame):
                # 尝试映射列名
                col_mapping = {
                    "date": "ex_date",
                    "ex_date": "ex_date",
                    "exDivDate": "ex_date",
                    "bonus_date": "ex_date",
                    "dividend": "dividend_per_share",
                    "dividend_per_share": "dividend_per_share",
                    "cashBonus": "dividend_per_share",
                    "bonus_ratio": "bonus_ratio",
                    "bonusRatio": "bonus_ratio",
                    "rightsissue_ratio": "rights_issue_ratio",
                }

                # 查找实际的列名
                actual_cols = {}
                for qmt_col, std_col in col_mapping.items():
                    if qmt_col in divid_data.columns:
                        actual_cols[std_col] = qmt_col

                # 提取数据
                for std_col, qmt_col in actual_cols.items():
                    dividends_df[std_col] = divid_data[qmt_col]

                # 确保有ex_date列
                if "ex_date" not in dividends_df.columns and len(divid_data.columns) > 0:
                    # 尝试使用第一列作为ex_date
                    dividends_df["ex_date"] = divid_data.iloc[:, 0]

                # 确保有dividend_per_share列
                if "dividend_per_share" not in dividends_df.columns and len(divid_data.columns) > 1:
                    dividends_df["dividend_per_share"] = divid_data.iloc[:, 1]

                if not dividends_df.empty and "ex_date" in dividends_df.columns:
                    # 确保日期格式正确
                    dividends_df["ex_date"] = pd.to_datetime(dividends_df["ex_date"]).dt.date
                    self._logger.debug("获取 %d 条分红记录", len(dividends_df))
                    return dividends_df
                else:
                    self._logger.warning("分红数据格式不符，无法使用")
                    return pd.DataFrame()

            return pd.DataFrame()

        except Exception as e:
            self._logger.warning("获取分红数据失败: %s", e)
            return pd.DataFrame()

    def _check_missing_trading_days(
        self, data: pd.DataFrame, start_date: str, end_date: str, period: str = "1d"
    ) -> int:
        """检查缺失的交易日数量（P1 精确版：基于 TradingCalendar 逐日精确对比）

        升级策略：
          - 使用 TradingCalendar.get_trading_days() 获取精确 A 股交易日集合。
          - 优先调用 chinese_calendar 权威库（requirements.txt 已列入）。
          - 逐日比对 data.index，返回真实缺失天数（漏检率 = 0）。
          - 春节 / 国庆区间不再触发误判，历史节假日零误报。
          - 兜底层保留：TradingCalendar 不可用时退化为 bdate_range × 0.935。
          - 周线/月线使用 Bar 数量粗估，避免与日历比较产生大量误判。
        """
        if data.empty:
            return 9999  # 返回大数，触发从 QMT / AKShare 下载

        # 周线/月线：不与精确日历比较，只做粗略 Bar 数量充足性检查
        if period in ("1w", "1M"):
            try:
                start_ts = pd.to_datetime(start_date)
                end_ts   = pd.to_datetime(end_date)
                delta_days = (end_ts - start_ts).days
                expected = max(1, delta_days // 7) if period == "1w" else max(1, delta_days // 30)
                actual   = len(data)
                # Allow up to 30 % gap before flagging as incomplete
                if actual >= int(expected * 0.7):
                    return 0
                return max(0, expected - actual)
            except Exception:
                return 0

        try:
            start = pd.to_datetime(start_date).date()
            end   = pd.to_datetime(end_date).date()
            if start > end:
                return 0

            # P1：精确交易日集合（chinese_calendar 优先，内置表兜底）
            from data_manager.smart_data_detector import TradingCalendar
            cal = TradingCalendar()
            expected_trading_days = cal.get_trading_days(start, end)
            if not expected_trading_days:
                return 0

            # 将 data.index 归一化为 date 集合
            existing_dates: set = set(
                pd.to_datetime(data.index, errors="coerce")
                  .normalize()
                  .to_series()
                  .dt.date  # type: ignore[union-attr]
                  .dropna()
            )
            missing = [d for d in expected_trading_days if d not in existing_dates]
            return len(missing)

        except Exception:
            # 退化兜底：bdate_range × 0.935，阈值 0.85
            try:
                start_ts = pd.to_datetime(start_date)
                end_ts   = pd.to_datetime(end_date)
                bdays = len(pd.bdate_range(start=start_ts, end=end_ts))
                expected = max(1, int(bdays * 0.935))
                actual = len(data)
                if actual < expected * 0.85:
                    return int(expected - actual)
            except Exception:
                pass
            return 0

    @staticmethod
    def _is_intraday_sparse(data: pd.DataFrame, period: str) -> bool:
        import re as _re
        # A 股全天 240 交易分钟；按周期分钟数推算每日预期 K 线数
        _KNOWN = {"1m": 240, "5m": 48, "15m": 16, "30m": 8, "60m": 4}
        expected = _KNOWN.get(period)
        if expected is None:
            _m = _re.match(r'^(\d+)m$', period)
            if _m:
                mins = int(_m.group(1))
                if mins > 0:
                    expected = max(1, 240 // mins)
        if expected is None:
            return False
        if data is None or data.empty:
            return True
        idx = pd.to_datetime(data.index, errors="coerce")
        idx = idx[~pd.isna(idx)]
        if len(idx) == 0:
            return True
        day_counts = pd.Series(1, index=idx).groupby(idx.date).sum()
        if len(day_counts) <= 1:
            return False
        p50 = float(day_counts.quantile(0.5))
        p80 = float(day_counts.quantile(0.8))
        return p50 < expected * 0.35 or p80 < expected * 0.55

    @staticmethod
    def _is_futures_or_hk(symbol: str) -> bool:
        """期货或港股代码（后缀为 SF/DF/IF/ZF/HK）。"""
        if "." in symbol:
            return symbol.rsplit(".", 1)[1].upper() in {"SF", "DF", "IF", "ZF", "HK"}
        return False

    def _dat_file_is_fresh(self, symbol: str, period: str) -> bool:
        """检查 DAT 文件是否在时效窗口内。

        阈值由环境变量 EASYXT_DAT_STALE_HOURS 控制（默认 24 小时）。
        文件不存在、QMT 路径未配置、或发生异常时均返回 False。
        """
        try:
            from data_manager.dat_binary_reader import (
                _build_dat_path,
                _load_qmt_base_from_config,
            )
            stale_hours = float(os.environ.get("EASYXT_DAT_STALE_HOURS", "24"))
            qmt_base = _load_qmt_base_from_config()
            if qmt_base is None:
                return False
            dat_path = _build_dat_path(qmt_base, symbol, period)
            if dat_path is None:
                return False
            age_hours = (time.time() - dat_path.stat().st_mtime) / 3600.0
            if age_hours > stale_hours:
                self._logger.warning(
                    "DAT 文件超出时效（%.1fh > %.0fh 阈值）: %s", age_hours, stale_hours, dat_path
                )
                return False
            return True
        except Exception as exc:
            self._logger.debug("检查 DAT 时效失败（忽略）: %s", exc)
            return False

    # ─────────────────── 预写入门禁（Pre-Write Gate）────────────────────
    @staticmethod
    def _pre_write_validate(df: pd.DataFrame) -> tuple[bool, str]:
        """保存到 DuckDB 之前的最后防线验证。

        检查项：
        1. DataFrame 不为空
        2. 必须包含 open/high/low/close 列
        3. 至少有一行的 OHLC 全部非 NaN
        4. 非 NaN 价格必须 > 0

        Returns:
            (pass, reason) — pass=False 时 reason 说明拒绝原因。
        """
        if df is None or df.empty:
            return False, "DataFrame 为空"
        required = {"open", "high", "low", "close"}
        missing = required - set(df.columns)
        if missing:
            return False, f"缺少必要列: {missing}"
        ohlc = df[["open", "high", "low", "close"]]
        valid_rows = ohlc.dropna(how="any")
        if valid_rows.empty:
            return False, "所有行的 OHLC 均为 NaN"
        if (valid_rows[["open", "high", "low", "close"]] <= 0).any(axis=None):
            neg_count = int((valid_rows[["open", "high", "low", "close"]] <= 0).any(axis=1).sum())
            total = len(valid_rows)
            if neg_count / total > 0.01:
                return False, f"存在 {neg_count}/{total} 行非正价格"
        return True, ""

    def _record_source_conflicts(self, rows: list[dict[str, Any]]) -> None:
        if not rows or not self.con or self._read_only_connection:
            return
        try:
            df_rows = pd.DataFrame(rows)
            self.con.register("source_conflict_rows", df_rows)
            self.con.execute(
                """
                INSERT INTO source_conflict_audit (
                    stock_code, period, event_ts, source_primary, source_secondary,
                    close_primary, close_secondary, delta_pct, decision, trace_id
                )
                SELECT
                    stock_code, period, event_ts, source_primary, source_secondary,
                    close_primary, close_secondary, delta_pct, decision, trace_id
                FROM source_conflict_rows
                """
            )
            self.con.unregister("source_conflict_rows")
        except Exception as e:
            self._logger.warning("写入source_conflict_audit失败: %s", e)
            try:
                self.con.unregister("source_conflict_rows")
            except Exception:
                pass

    def _merge_data(
        self, duckdb_data: pd.DataFrame, qmt_data: pd.DataFrame, stock_code: str, period: str
    ) -> pd.DataFrame:
        """合并DuckDB和QMT数据"""
        duckdb_data = duckdb_data.copy()
        qmt_data = qmt_data.copy()
        duckdb_data.index = pd.to_datetime(duckdb_data.index, errors="coerce")
        qmt_data.index = pd.to_datetime(qmt_data.index, errors="coerce")
        duckdb_data = duckdb_data[duckdb_data.index.notna()]
        qmt_data = qmt_data[qmt_data.index.notna()]
        duckdb_data = duckdb_data[~duckdb_data.index.duplicated(keep="last")]
        qmt_data = qmt_data[~qmt_data.index.duplicated(keep="last")]
        # 使用QMT数据作为基础
        merged = qmt_data.copy()

        overlap_idx = duckdb_data.index.intersection(qmt_data.index)
        conflict_threshold = float(os.environ.get("EASYXT_SOURCE_CONFLICT_DELTA", "0.02"))
        if len(overlap_idx) > 0 and "close" in duckdb_data.columns and "close" in qmt_data.columns:
            duck_close = pd.to_numeric(duckdb_data.loc[overlap_idx, "close"], errors="coerce")
            qmt_close = pd.to_numeric(qmt_data.loc[overlap_idx, "close"], errors="coerce")
            baseline = duck_close.abs().replace(0, pd.NA)
            delta = ((qmt_close - duck_close).abs() / baseline).dropna()
            conflict_delta = delta[delta > conflict_threshold]
            if not conflict_delta.empty:
                conflict_idx = conflict_delta.index
                shared_cols = [c for c in duckdb_data.columns if c in merged.columns]
                if shared_cols:
                    merged.loc[conflict_idx, shared_cols] = duckdb_data.loc[conflict_idx, shared_cols]
                trace_id = str(uuid.uuid4())
                rows: list[dict[str, Any]] = []
                for ts, d in conflict_delta.items():
                    rows.append(
                        {
                            "stock_code": stock_code,
                            "period": period,
                            "event_ts": pd.to_datetime(ts),
                            "source_primary": "duckdb",
                            "source_secondary": "qmt",
                            "close_primary": float(duck_close.loc[ts]),
                            "close_secondary": float(qmt_close.loc[ts]),
                            "delta_pct": float(d),
                            "decision": "prefer_duckdb_on_conflict",
                            "trace_id": trace_id,
                        }
                    )
                self._record_source_conflicts(rows)
                self._logger.warning(
                    "检测到跨源价格冲突: %s %s count=%s threshold=%.2f%%, 已优先保留DuckDB",
                    stock_code,
                    period,
                    len(rows),
                    conflict_threshold * 100,
                )

        # 找出DuckDB中有但QMT中没有的日期（用DuckDB补充）
        duckdb_dates = set(pd.to_datetime(duckdb_data.index).unique())
        qmt_dates = set(pd.to_datetime(qmt_data.index).unique())

        only_in_duckdb = duckdb_dates - qmt_dates

        if only_in_duckdb:
            additional = duckdb_data.loc[duckdb_data.index.isin(list(only_in_duckdb))]
            merged = pd.concat([merged, additional]).sort_index()

        # 删除重复索引
        merged = merged[~merged.index.duplicated(keep="first")]

        return merged

    def _post_write_verify(
        self,
        table_name: str,
        stock_code: str,
        period: str,
        date_col: str,
        date_min: str,
        date_max: str,
        expected_rows: int,
    ) -> tuple[bool, int]:
        """COMMIT 后回读验证行数是否与预期一致。

        Returns:
            (pass, actual_rows)
        """
        try:
            row = self.con.execute(
                "SELECT COUNT(*) FROM " + table_name
                + " WHERE stock_code = ? AND period = ?"
                " AND " + date_col + " >= ? AND " + date_col + " <= ?",
                [stock_code, period, date_min, date_max],
            ).fetchone()
            actual = row[0] if row else 0
            return actual >= expected_rows, actual
        except Exception as e:
            self._logger.warning("post-write verify 查询失败: %s", e)
            return False, -1

    def _record_write_audit(
        self,
        table_name: str,
        stock_code: str,
        period: str,
        expected_rows: int,
        actual_rows: int,
        date_min: str,
        date_max: str,
        raw_hash: str,
        pre_gate_pass: bool,
        contract_pass: bool,
        post_verify_pass: bool,
        error_message: str | None = None,
    ) -> str:
        """写入 write_audit_log 记录每次写操作的审计信息。"""
        if not self.con or self._read_only_connection:
            return ""
        audit_id = str(uuid.uuid4())
        try:
            self.con.execute(
                """
                INSERT INTO write_audit_log (
                    audit_id, table_name, stock_code, period,
                    expected_rows, actual_rows, date_min, date_max,
                    raw_hash, pre_gate_pass, contract_pass, post_verify_pass,
                    error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    audit_id, table_name, stock_code, period,
                    expected_rows, actual_rows, date_min, date_max,
                    raw_hash, pre_gate_pass, contract_pass, post_verify_pass,
                    error_message,
                ],
            )
            return audit_id
        except Exception as e:
            self._logger.warning("写入write_audit_log失败: %s", e)
            return ""

    def _build_quarantine_sample_json(self, df: pd.DataFrame | None, limit: int = 20) -> str:
        if df is None or df.empty:
            return ""
        try:
            sample = df.head(max(limit, 1)).copy()
            return sample.to_json(orient="records", date_format="iso", force_ascii=False)
        except Exception:
            return ""

    def _record_quarantine_log(
        self,
        audit_id: str,
        table_name: str,
        stock_code: str,
        period: str,
        reason: str,
        expected_rows: int,
        actual_rows: int,
        date_min: str,
        date_max: str,
        sample_json: str,
        *,
        sequence_id: str | None = None,
        source_event_time: Any | None = None,
        ingest_time: Any | None = None,
        watermark_ms: int | None = None,
        lateness_ms: int | None = None,
        watermark_late: bool = False,
    ) -> None:
        if not self.con or self._read_only_connection:
            return
        try:
            self.con.execute(
                """
                INSERT INTO data_quarantine_log (
                    quarantine_id, audit_id, table_name, stock_code, period, reason,
                    expected_rows, actual_rows, date_min, date_max, sample_json,
                    sequence_id, source_event_time, ingest_time, watermark_ms, lateness_ms, watermark_late
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(uuid.uuid4()),
                    audit_id,
                    table_name,
                    stock_code,
                    period,
                    reason,
                    expected_rows,
                    actual_rows,
                    date_min,
                    date_max,
                    sample_json,
                    sequence_id,
                    source_event_time,
                    ingest_time,
                    watermark_ms,
                    lateness_ms,
                    bool(watermark_late),
                ],
            )
        except Exception as e:
            self._logger.warning("写入data_quarantine_log失败: %s", e)

    def _emit_data_quality_alert(
        self,
        stock_code: str,
        period: str,
        level: str,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        if (
            threading.current_thread() is not threading.main_thread()
            and os.environ.get("EASYXT_ALLOW_CROSS_THREAD_UI_ALERT", "0") not in ("1", "true", "True")
        ):
            return
        try:
            from core.events import Events
            from core.signal_bus import signal_bus

            payload = {
                "stock_code": stock_code,
                "period": period,
                "level": level,
                "reason": reason,
                "details": details or {},
            }
            signal_bus.emit(Events.DATA_QUALITY_ALERT, **payload)
        except Exception as e:
            self._logger.warning("发送DATA_QUALITY_ALERT失败: %s", e)

    def _record_data_quality_incident(
        self,
        incident_type: str,
        severity: str,
        stock_code: str,
        period: str,
        quarantine_id: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if not self.con or self._read_only_connection:
            return
        try:
            payload_json = json.dumps(payload or {}, ensure_ascii=False)
            self.con.execute(
                """
                INSERT INTO data_quality_incident (
                    incident_id, incident_type, severity, stock_code, period,
                    quarantine_id, payload_json, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(uuid.uuid4()),
                    incident_type,
                    severity,
                    stock_code,
                    period,
                    quarantine_id,
                    payload_json,
                    "open",
                ],
            )
        except Exception as e:
            self._logger.warning("写入data_quality_incident失败: %s", e)

    def _save_to_duckdb(
        self,
        data: pd.DataFrame,
        stock_code: str,
        period: str,
        _retry_after_reconnect: bool = False,
        *,
        _ingest_source: str | None = None,
        _ingest_start: str | None = None,
        _ingest_end: str | None = None,
    ):
        """保存数据到DuckDB - 修复版（确保表存在）

        P1.4: 通过 _db_manager._write_lock 保证同一进程内单写者串行化。
        P1.5: 当调用方传入 _ingest_source 时，ingestion_status 的 success 写入
              在同一 BEGIN/COMMIT 事务内完成，保证崩溃安全的原子性。
        """
        # P1.4: 获取连接池写锁（跨线程串行化）
        write_lock = getattr(self, "_db_manager", None)
        write_lock = getattr(write_lock, "_write_lock", None) if write_lock else None
        if write_lock is not None:
            write_lock.acquire()
        try:
            # 确保表存在（修复首次使用问题）
            self._ensure_tables_exist()
            table_period, storage_period = self._get_storage_target_period(period)
            # 确定表名
            table_map = {
                "1d": "stock_daily",
                "1m": "stock_1m",
                "5m": "stock_5m",
                "tick": "stock_tick",
            }
            table_name = table_map.get(table_period, "stock_daily")
            # 防御性校验：深度防御，防止未来代码腐化引入非法表名拼接
            if table_name not in self._SAFE_TABLE_NAMES:
                raise ValueError(
                    f"_save_to_duckdb: 非法表名 {table_name!r}，仅允许: {self._SAFE_TABLE_NAMES}"
                )
            date_col = "date" if table_name == "stock_daily" else "datetime"

            # 重置索引，把datetime变成列，并正规化为 date_col
            # 修复: QMT数据 set_index("datetime", drop=False) 导致 index名与列名同名；
            #       reset_index 后再 rename 会产生重复列 → "cannot assemble with duplicate keys"
            idx_name = data.index.name
            if idx_name and idx_name in data.columns:
                # index 与列同名 — 丢弃 index，只保留已存在的列
                df_to_save = data.reset_index(drop=True)
                # 清理 _read_from_qmt 早期 reset_index() 遗留的 "index" 辅助列，
                # 避免后续 rename 将其改名为 date_col 产生重复列名
                if "index" in df_to_save.columns:
                    df_to_save = df_to_save.drop(columns=["index"])
                # 日线表: 若 date_col="date" 但只有 "datetime" 列，做一次转换
                if date_col == "date" and "date" not in df_to_save.columns:
                    if "datetime" in df_to_save.columns:
                        df_to_save = df_to_save.rename(columns={"datetime": "date"})
            else:
                df_to_save = data.reset_index()
                if date_col == "date":
                    # 逐步安全重命名，避免两个源同时映射到 "date" 产生重复
                    if "index" in df_to_save.columns and "date" not in df_to_save.columns:
                        df_to_save = df_to_save.rename(columns={"index": "date"})
                    if "datetime" in df_to_save.columns and "date" not in df_to_save.columns:
                        df_to_save = df_to_save.rename(columns={"datetime": "date"})
                else:
                    if "index" in df_to_save.columns and "datetime" not in df_to_save.columns:
                        df_to_save = df_to_save.rename(columns={"index": "datetime"})
                    if "time" in df_to_save.columns and "datetime" not in df_to_save.columns:
                        df_to_save = df_to_save.rename(columns={"time": "datetime"})
            # 最终保险：若仍有重复列名，保留第一个
            if df_to_save.columns.duplicated().any():
                df_to_save = df_to_save.loc[:, ~df_to_save.columns.duplicated()]

            # 确保有stock_code列（无条件覆盖：_merge_data合并后QMT行可能含NaN stock_code）
            df_to_save["stock_code"] = stock_code

            # 确保有period列
            if "period" not in df_to_save.columns:
                df_to_save["period"] = storage_period
            else:
                df_to_save["period"] = storage_period

            # 确保有symbol_type列（列存在但含NaN时也需重算）
            if "symbol_type" not in df_to_save.columns or df_to_save["symbol_type"].isna().any():
                # 判断是股票、指数还是ETF
                if stock_code.endswith(".SH"):
                    if stock_code.startswith("5") or stock_code.startswith("51"):
                        df_to_save["symbol_type"] = "etf"
                    elif stock_code.startswith("688"):
                        df_to_save["symbol_type"] = "stock"  # 科创板
                    else:
                        df_to_save["symbol_type"] = "stock"
                elif stock_code.endswith(".SZ"):
                    if stock_code.startswith("15") or stock_code.startswith("16"):
                        df_to_save["symbol_type"] = "etf"
                    elif stock_code.startswith("30"):
                        df_to_save["symbol_type"] = "stock"  # 创业板
                    else:
                        df_to_save["symbol_type"] = "stock"
                else:
                    df_to_save["symbol_type"] = "stock"

            if date_col in df_to_save.columns:
                df_to_save[date_col] = pd.to_datetime(df_to_save[date_col], errors="coerce")
                if date_col == "date":
                    df_to_save[date_col] = df_to_save[date_col].dt.date  # type: ignore[union-attr]
                df_to_save = df_to_save[df_to_save[date_col].notna()]
            else:
                # date_col 既不在列中也无法通过重命名获得 — 尝试从 "date"/"datetime"/"time" 兜底
                fallback_cols = (["date", "time"] if date_col == "datetime" else ["datetime", "time"])
                found = False
                for fb in fallback_cols:
                    if fb in df_to_save.columns:
                        df_to_save = df_to_save.rename(columns={fb: date_col})
                        df_to_save[date_col] = pd.to_datetime(df_to_save[date_col], errors="coerce")
                        if date_col == "date":
                            df_to_save[date_col] = df_to_save[date_col].dt.date  # type: ignore[union-attr]
                        df_to_save = df_to_save[df_to_save[date_col].notna()]
                        found = True
                        break
                if not found:
                    self._logger.error(
                        "保存跳过: DataFrame 缺少日期列 %r，stock=%s period=%s columns=%s",
                        date_col, stock_code, period, list(df_to_save.columns),
                    )
                    return
            if df_to_save.empty:
                return

            # ── 预写入门禁：在 DELETE 旧数据之前验证新数据质量 ────────────
            # tick 表结构不同（无 OHLC），跳过 OHLC 验证
            if table_name != "stock_tick":
                gate_ok, gate_reason = self._pre_write_validate(df_to_save)
                if not gate_ok:
                    self._logger.warning("GATE-REJECT: 预写入门禁拒绝: %s（%s %s）", gate_reason, stock_code, period)
                    audit_id = self._record_write_audit(
                        table_name=table_name,
                        stock_code=stock_code,
                        period=storage_period,
                        expected_rows=len(df_to_save),
                        actual_rows=0,
                        date_min="",
                        date_max="",
                        raw_hash="",
                        pre_gate_pass=False,
                        contract_pass=False,
                        post_verify_pass=False,
                        error_message=f"pre_gate_reject: {gate_reason}",
                    )
                    self._record_quarantine_log(
                        audit_id=audit_id,
                        table_name=table_name,
                        stock_code=stock_code,
                        period=storage_period,
                        reason="pre_gate_reject",
                        expected_rows=len(df_to_save),
                        actual_rows=0,
                        date_min="",
                        date_max="",
                        sample_json=self._build_quarantine_sample_json(df_to_save),
                    )
                    self._emit_data_quality_alert(
                        stock_code=stock_code,
                        period=storage_period,
                        level="error",
                        reason="pre_gate_reject",
                        details={"gate_reason": gate_reason, "table_name": table_name},
                    )
                    return

            if self._canary_shadow_write_enabled:
                self._write_shadow_copy(
                    table_name=table_name,
                    date_col=date_col,
                    df_to_save=df_to_save,
                    stock_code=stock_code,
                    storage_period=storage_period,
                )
                if self._canary_shadow_only:
                    self._logger.warning(
                        "CANARY_SHADOW_ONLY 已启用，已写 shadow 表并跳过主表写入: %s %s %s-%s",
                        stock_code,
                        storage_period,
                        str(df_to_save[date_col].min()),
                        str(df_to_save[date_col].max()),
                    )
                    return

            # 删除已存在的重复数据
            date_min = str(df_to_save[date_col].min())
            date_max = str(df_to_save[date_col].max())

            delete_sql = (
                "DELETE FROM " + table_name +
                " WHERE stock_code = ? AND period = ?"
                " AND " + date_col + " >= ? AND " + date_col + " <= ?"
            )
            self.con.execute("BEGIN")
            self.con.execute(delete_sql, [stock_code, storage_period, date_min, date_max])

            # 添加时间戳列（所有表通用）
            current_time = pd.Timestamp.now()
            if "created_at" not in df_to_save.columns:
                df_to_save["created_at"] = current_time
            if "updated_at" not in df_to_save.columns:
                df_to_save["updated_at"] = current_time

            # 日线表：添加复权列并计算
            if table_name == "stock_daily":
                # 添加 adjust_type 和 factor 列
                if "adjust_type" not in df_to_save.columns:
                    df_to_save["adjust_type"] = "none"
                if "factor" not in df_to_save.columns:
                    df_to_save["factor"] = 1.0

                # 添加所有复权列（使用五维复权管理器计算真实复权价格）
                if len(df_to_save) > 0 and "close" in df_to_save.columns:
                    self._logger.debug("计算五维复权数据")

                    # 获取分红数据（用于计算真实复权价格）
                    dividends = self._get_dividends_from_qmt(
                        stock_code, df_to_save["date"].min(), df_to_save["date"].max()
                    )
                    if dividends is None:
                        dividends = pd.DataFrame()

                    try:
                        adjusted_data: dict[str, pd.DataFrame] = {}
                        if self.adjustment_manager is None:
                            self._ensure_adjustment_manager()
                        if self.adjustment_manager is None:
                            self._logger.warning("FiveFoldAdjustmentManager不可用，跳过复权计算")
                        else:
                            adjusted_data = self.adjustment_manager.calculate_adjustment(
                                df_to_save, dividends=dividends
                            )

                        for adj_type, df_adj in adjusted_data.items():
                            if adj_type == "none":
                                continue

                            col_mapping = {
                                "front": ("open_front", "high_front", "low_front", "close_front"),
                                "back": ("open_back", "high_back", "low_back", "close_back"),
                                "geometric_front": (
                                    "open_geometric_front",
                                    "high_geometric_front",
                                    "low_geometric_front",
                                    "close_geometric_front",
                                ),
                                "geometric_back": (
                                    "open_geometric_back",
                                    "high_geometric_back",
                                    "low_geometric_back",
                                    "close_geometric_back",
                                ),
                            }

                            target_cols: list[str] = list(col_mapping.get(adj_type, []))
                            for i, price_col in enumerate(["open", "high", "low", "close"]):
                                if price_col in df_adj.columns and i < len(target_cols):
                                    # .values 绕过 pandas index 对齐：
                                    # df_adj 使用 DatetimeIndex，df_to_save 使用 RangeIndex，
                                    # 直接赋值会因 index 不匹配导致全部 NaN。
                                    df_to_save[target_cols[i]] = df_adj[price_col].values

                        self._logger.debug("Five-fold adjustment 计算完成")
                    except Exception as e:
                        self._logger.warning("Five-fold adjustment 计算失败: %s", e)
                        self._logger.debug("复权列将复制原始价格")

                        price_cols = ["open", "high", "low", "close"]
                        adjustment_types = [
                            "_front",
                            "_back",
                            "_geometric_front",
                            "_geometric_back",
                        ]

                        for price_col in price_cols:
                            if price_col in df_to_save.columns:
                                for adj_type in adjustment_types:
                                    adj_col = price_col + adj_type
                                    df_to_save[adj_col] = df_to_save[price_col]

            # 获取表的列顺序
            table_columns = (
                self.con.execute(f"DESCRIBE {table_name}").fetchdf()["column_name"].tolist()
            )

            # 对于分钟/其他表，如果存在 adjust_type / factor 字段但数据中缺失，则补默认值
            if "adjust_type" in table_columns and "adjust_type" not in df_to_save.columns:
                df_to_save["adjust_type"] = "none"
            if "factor" in table_columns and "factor" not in df_to_save.columns:
                df_to_save["factor"] = 1.0

            key_cols = [c for c in [date_col, "stock_code", "period", "adjust_type"] if c in df_to_save.columns]
            if key_cols:
                df_to_save = df_to_save.drop_duplicates(subset=key_cols, keep="last")

            # 按表的列顺序重新排列DataFrame
            df_ordered = pd.DataFrame()
            for col in table_columns:
                if col in df_to_save.columns:
                    df_ordered[col] = df_to_save[col]
                else:
                    df_ordered[col] = None  # 缺失列填充NULL

            # 注册并插入新数据
            self.con.register("df_to_save_temp", df_ordered)
            self.con.execute("INSERT OR REPLACE INTO " + table_name + " SELECT * FROM df_to_save_temp")
            self.con.unregister("df_to_save_temp")

            # ── P1.5: 原子性写入 ingestion_status（在 COMMIT 之前，同一事务内）──────────
            # 保证进程在 COMMIT 后、status 更新前崩溃时两者始终一致。
            if _ingest_source is not None:
                try:
                    _i_rh, _i_set = self._compute_data_lineage(df_ordered)
                except Exception:
                    _i_rh, _i_set = None, None
                _ts_start = self._normalize_date_str(_ingest_start) if _ingest_start else _ingest_start
                _ts_end = self._normalize_date_str(_ingest_end) if _ingest_end else _ingest_end
                self.con.execute(
                    """
                    INSERT OR REPLACE INTO data_ingestion_status (
                        stock_code, period, start_date, end_date,
                        source, status, record_count, error_message,
                        schema_version, ingest_run_id, raw_hash, source_event_time
                    ) VALUES (
                        ?, ?,
                        CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP),
                        ?, ?, ?, ?,
                        ?, ?, ?, ?
                    )
                    """,
                    [
                        stock_code, storage_period,
                        _ts_start, _ts_end,
                        _ingest_source, "success", len(df_ordered), None,
                        CURRENT_SCHEMA_VERSION, str(uuid.uuid4()), _i_rh, _i_set,
                    ],
                )

            self.con.execute("COMMIT")

            expected_rows = len(df_ordered)
            self._logger.debug("已保存 %d 条记录到 %s", expected_rows, table_name)

            # ── P1.3 post-write verify + audit ──────────────────
            verify_ok, actual_rows = self._post_write_verify(
                table_name, stock_code, storage_period, date_col, date_min, date_max, expected_rows,
            )
            if not verify_ok:
                self._logger.warning(
                    "post-write验证失败: %s %s expected=%s actual=%s",
                    stock_code, storage_period, expected_rows, actual_rows,
                )
            try:
                rh, _ = self._compute_data_lineage(df_ordered)
            except Exception:
                rh = "error"
            audit_id = self._record_write_audit(
                table_name=table_name,
                stock_code=stock_code,
                period=storage_period,
                expected_rows=expected_rows,
                actual_rows=actual_rows,
                date_min=date_min,
                date_max=date_max,
                raw_hash=rh,
                pre_gate_pass=True,
                contract_pass=True,
                post_verify_pass=verify_ok,
            )
            if not verify_ok:
                self._record_quarantine_log(
                    audit_id=audit_id,
                    table_name=table_name,
                    stock_code=stock_code,
                    period=storage_period,
                    reason="post_write_verify_failed",
                    expected_rows=expected_rows,
                    actual_rows=actual_rows,
                    date_min=date_min,
                    date_max=date_max,
                    sample_json=self._build_quarantine_sample_json(df_ordered),
                )
                self._emit_data_quality_alert(
                    stock_code=stock_code,
                    period=storage_period,
                    level="warning",
                    reason="post_write_verify_failed",
                    details={
                        "table_name": table_name,
                        "expected_rows": expected_rows,
                        "actual_rows": actual_rows,
                    },
                )

        except Exception as e:
            try:
                self.con.execute("ROLLBACK")
            except Exception:
                pass
            msg = str(e).lower()
            if (
                (not _retry_after_reconnect)
                and ".wal" in msg
                and ("cannot open file" in msg or "failed to commit" in msg)
            ):
                try:
                    self._close_duckdb_connection()
                    if self.connect(read_only=False):
                        self._save_to_duckdb(
                            data,
                            stock_code,
                            period,
                            _retry_after_reconnect=True,
                            _ingest_source=_ingest_source,
                            _ingest_start=_ingest_start,
                            _ingest_end=_ingest_end,
                        )
                        return
                except Exception:
                    pass
            audit_id = self._record_write_audit(
                table_name=table_name if "table_name" in dir() else "unknown",
                stock_code=stock_code,
                period=period,
                expected_rows=len(data) if data is not None else 0,
                actual_rows=0,
                date_min="",
                date_max="",
                raw_hash="error",
                pre_gate_pass=True,
                contract_pass=True,
                post_verify_pass=False,
                error_message=str(e)[:500],
            )
            self._record_quarantine_log(
                audit_id=audit_id,
                table_name=table_name if "table_name" in dir() else "unknown",
                stock_code=stock_code,
                period=period,
                reason="save_exception",
                expected_rows=len(data) if data is not None else 0,
                actual_rows=0,
                date_min="",
                date_max="",
                sample_json=self._build_quarantine_sample_json(data if isinstance(data, pd.DataFrame) else None),
            )
            self._emit_data_quality_alert(
                stock_code=stock_code,
                period=period,
                level="error",
                reason="save_exception",
                details={"error_message": str(e)[:300], "table_name": table_name if "table_name" in dir() else "unknown"},
            )
            self._logger.error("保存失败: %s", e)
        finally:
            if write_lock is not None:
                write_lock.release()

    # 已知合法表名的硬编码白名单，防止 f-string SQL 拼接被外部输入利用
    _SAFE_TABLE_NAMES: frozenset[str] = frozenset(
        {"stock_daily", "stock_1m", "stock_5m", "stock_tick", "stock_transaction",
         "market_data", "custom_period_bars"}
    )

    _IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    @classmethod
    def _is_safe_identifier(cls, value: str) -> bool:
        return bool(cls._IDENTIFIER_RE.fullmatch(str(value or "")))

    @classmethod
    def _quote_identifier(cls, value: str) -> str:
        if not cls._is_safe_identifier(value):
            raise ValueError(f"非法标识符: {value!r}")
        return f'"{value}"'

    def _write_shadow_copy(
        self,
        table_name: str,
        date_col: str,
        df_to_save: pd.DataFrame,
        stock_code: str,
        storage_period: str,
    ) -> None:
        if table_name not in self._SAFE_TABLE_NAMES:
            raise ValueError(f"不允许的表名: {table_name!r}（仅接受内部已知表名）")
        shadow_table = f"{table_name}_shadow"
        if not self._is_safe_identifier(date_col):
            raise ValueError(f"非法日期列名: {date_col!r}")
        table_q = self._quote_identifier(table_name)
        shadow_q = self._quote_identifier(shadow_table)
        date_col_q = self._quote_identifier(date_col)
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {shadow_q} AS SELECT * FROM {table_q} WHERE 1=0"
        )
        date_min = str(df_to_save[date_col].min())
        date_max = str(df_to_save[date_col].max())
        self.con.execute(
            "DELETE FROM " + shadow_q +
            " WHERE stock_code = ? AND period = ?"
            " AND " + date_col_q + " >= ? AND " + date_col_q + " <= ?",
            [stock_code, storage_period, date_min, date_max],
        )
        shadow_columns = (
            self.con.execute(f"DESCRIBE {shadow_q}").fetchdf()["column_name"].tolist()
        )
        df_shadow = pd.DataFrame()
        for col in shadow_columns:
            df_shadow[col] = df_to_save[col] if col in df_to_save.columns else None
        self.con.register("df_shadow_temp", df_shadow)
        self.con.execute(f"INSERT INTO {shadow_q} SELECT * FROM df_shadow_temp")
        self.con.unregister("df_shadow_temp")

    def _save_ticks_to_duckdb(self, data: pd.DataFrame, stock_code: str) -> None:
        if self.con is None:
            return
        write_lock = getattr(self, "_db_manager", None)
        write_lock = getattr(write_lock, "_write_lock", None) if write_lock else None
        if write_lock is not None:
            write_lock.acquire()
        try:
            self._ensure_tables_exist()
            idx_name = data.index.name
            if idx_name and idx_name in data.columns:
                df_to_save = data.reset_index(drop=True)
            else:
                df_to_save = data.reset_index().rename(columns={"index": "datetime"})
            df_to_save["stock_code"] = stock_code
            df_to_save["period"] = "tick"
            if "symbol_type" not in df_to_save.columns:
                df_to_save["symbol_type"] = "stock"
            if "bs_flag" not in df_to_save.columns:
                df_to_save["bs_flag"] = None
            if "trade_id" not in df_to_save.columns:
                df_to_save["trade_id"] = 0
            if "adjust_type" not in df_to_save.columns:
                df_to_save["adjust_type"] = "none"
            df_to_save["datetime"] = pd.to_datetime(df_to_save["datetime"], errors="coerce")
            df_to_save = df_to_save[df_to_save["datetime"].notna()]
            if df_to_save.empty:
                return
            date_min = str(df_to_save["datetime"].min())
            date_max = str(df_to_save["datetime"].max())
            self.con.execute("BEGIN")
            self.con.execute(
                "DELETE FROM stock_tick WHERE stock_code = ?"
                " AND datetime >= ? AND datetime <= ?",
                [stock_code, date_min, date_max],
            )
            table_columns = self._get_table_columns("stock_tick")
            ordered = pd.DataFrame()
            for col in table_columns:
                ordered[col] = df_to_save[col] if col in df_to_save.columns else None
            key_cols = [c for c in ["datetime", "stock_code", "period"] if c in ordered.columns]
            if key_cols:
                ordered = ordered.drop_duplicates(subset=key_cols, keep="last")
            expected_rows = len(ordered)
            self.con.register("df_tick_temp", ordered)
            self.con.execute("INSERT INTO stock_tick SELECT * FROM df_tick_temp")
            self.con.unregister("df_tick_temp")
            self.con.execute("COMMIT")
            verify_ok, actual_rows = self._post_write_verify(
                "stock_tick", stock_code, "tick", "datetime", date_min, date_max, expected_rows
            )
            self._record_write_audit(
                table_name="stock_tick",
                stock_code=stock_code,
                period="tick",
                expected_rows=expected_rows,
                actual_rows=actual_rows,
                date_min=date_min,
                date_max=date_max,
                raw_hash="tick",
                pre_gate_pass=True,
                contract_pass=True,
                post_verify_pass=verify_ok,
            )
            if not verify_ok:
                self._record_quarantine_log(
                    audit_id="",
                    table_name="stock_tick",
                    stock_code=stock_code,
                    period="tick",
                    reason="post_write_verify_failed",
                    expected_rows=expected_rows,
                    actual_rows=actual_rows,
                    date_min=date_min,
                    date_max=date_max,
                    sample_json=self._build_quarantine_sample_json(ordered),
                )
                self._emit_data_quality_alert(
                    stock_code=stock_code,
                    period="tick",
                    level="warning",
                    reason="tick_post_write_verify_failed",
                    details={"expected_rows": expected_rows, "actual_rows": actual_rows},
                )
        except Exception as e:
            try:
                self.con.execute("ROLLBACK")
            except Exception as rb_err:
                self._logger.warning("tick写入回滚失败: %s", rb_err)
            self._record_write_audit(
                table_name="stock_tick",
                stock_code=stock_code,
                period="tick",
                expected_rows=len(data) if data is not None else 0,
                actual_rows=0,
                date_min="",
                date_max="",
                raw_hash="tick_error",
                pre_gate_pass=True,
                contract_pass=True,
                post_verify_pass=False,
                error_message=str(e)[:500],
            )
            self._record_quarantine_log(
                audit_id="",
                table_name="stock_tick",
                stock_code=stock_code,
                period="tick",
                reason="save_exception",
                expected_rows=len(data) if data is not None else 0,
                actual_rows=0,
                date_min="",
                date_max="",
                sample_json=self._build_quarantine_sample_json(data if isinstance(data, pd.DataFrame) else None),
            )
            self._emit_data_quality_alert(
                stock_code=stock_code,
                period="tick",
                level="error",
                reason="tick_save_exception",
                details={"error_message": str(e)[:300], "table_name": "stock_tick"},
            )
            return
        finally:
            if write_lock is not None:
                write_lock.release()

    def _save_transactions_to_duckdb(self, data: pd.DataFrame, stock_code: str) -> None:
        if self.con is None:
            return
        write_lock = getattr(self, "_db_manager", None)
        write_lock = getattr(write_lock, "_write_lock", None) if write_lock else None
        if write_lock is not None:
            write_lock.acquire()
        try:
            self._ensure_tables_exist()
            idx_name = data.index.name
            if idx_name and idx_name in data.columns:
                df_to_save = data.reset_index(drop=True)
            else:
                df_to_save = data.reset_index().rename(columns={"index": "datetime"})
            df_to_save["stock_code"] = stock_code
            df_to_save["period"] = "tick"
            if "symbol_type" not in df_to_save.columns:
                df_to_save["symbol_type"] = "stock"
            df_to_save["datetime"] = pd.to_datetime(df_to_save["datetime"], errors="coerce")
            df_to_save = df_to_save[df_to_save["datetime"].notna()]
            if df_to_save.empty:
                return
            date_min = str(df_to_save["datetime"].min())
            date_max = str(df_to_save["datetime"].max())
            self.con.execute("BEGIN")
            self.con.execute(
                "DELETE FROM stock_transaction WHERE stock_code = ?"
                " AND datetime >= ? AND datetime <= ?",
                [stock_code, date_min, date_max],
            )
            table_columns = self._get_table_columns("stock_transaction")
            ordered = pd.DataFrame()
            for col in table_columns:
                ordered[col] = df_to_save[col] if col in df_to_save.columns else None
            key_cols = [c for c in ["datetime", "stock_code", "price", "volume"] if c in ordered.columns]
            if key_cols:
                ordered = ordered.drop_duplicates(subset=key_cols, keep="last")
            expected_rows = len(ordered)
            self.con.register("df_tx_temp", ordered)
            self.con.execute("INSERT INTO stock_transaction SELECT * FROM df_tx_temp")
            self.con.unregister("df_tx_temp")
            self.con.execute("COMMIT")
            verify_ok, actual_rows = self._post_write_verify(
                "stock_transaction", stock_code, "tick", "datetime", date_min, date_max, expected_rows
            )
            self._record_write_audit(
                table_name="stock_transaction",
                stock_code=stock_code,
                period="tick",
                expected_rows=expected_rows,
                actual_rows=actual_rows,
                date_min=date_min,
                date_max=date_max,
                raw_hash="tx",
                pre_gate_pass=True,
                contract_pass=True,
                post_verify_pass=verify_ok,
            )
            if not verify_ok:
                self._record_quarantine_log(
                    audit_id="",
                    table_name="stock_transaction",
                    stock_code=stock_code,
                    period="tick",
                    reason="post_write_verify_failed",
                    expected_rows=expected_rows,
                    actual_rows=actual_rows,
                    date_min=date_min,
                    date_max=date_max,
                    sample_json=self._build_quarantine_sample_json(ordered),
                )
                self._emit_data_quality_alert(
                    stock_code=stock_code,
                    period="tick",
                    level="warning",
                    reason="transaction_post_write_verify_failed",
                    details={"expected_rows": expected_rows, "actual_rows": actual_rows},
                )
        except Exception as e:
            try:
                self.con.execute("ROLLBACK")
            except Exception as rb_err:
                self._logger.warning("transaction写入回滚失败: %s", rb_err)
            self._record_write_audit(
                table_name="stock_transaction",
                stock_code=stock_code,
                period="tick",
                expected_rows=len(data) if data is not None else 0,
                actual_rows=0,
                date_min="",
                date_max="",
                raw_hash="tx_error",
                pre_gate_pass=True,
                contract_pass=True,
                post_verify_pass=False,
                error_message=str(e)[:500],
            )
            self._record_quarantine_log(
                audit_id="",
                table_name="stock_transaction",
                stock_code=stock_code,
                period="tick",
                reason="save_exception",
                expected_rows=len(data) if data is not None else 0,
                actual_rows=0,
                date_min="",
                date_max="",
                sample_json=self._build_quarantine_sample_json(data if isinstance(data, pd.DataFrame) else None),
            )
            self._emit_data_quality_alert(
                stock_code=stock_code,
                period="tick",
                level="error",
                reason="transaction_save_exception",
                details={"error_message": str(e)[:300], "table_name": "stock_transaction"},
            )
            return
        finally:
            if write_lock is not None:
                write_lock.release()

    def _apply_adjustment(self, data: pd.DataFrame, adjust: str) -> pd.DataFrame:
        """应用复权（如果数据中有复权列）"""
        # 检查是否有对应的复权列
        if adjust == "front":
            if "close_front" in data.columns:
                # 使用前复权列
                for col in ["open", "high", "low", "close"]:
                    if f"{col}_front" in data.columns:
                        data[col] = data[f"{col}_front"]
        elif adjust == "back":
            if "close_back" in data.columns:
                # 使用后复权列
                for col in ["open", "high", "low", "close"]:
                    if f"{col}_back" in data.columns:
                        data[col] = data[f"{col}_back"]
        elif adjust == "geometric_front":
            if "close_geometric_front" in data.columns:
                # 使用等比前复权列
                for col in ["open", "high", "low", "close"]:
                    if f"_{col}_geometric_front" in data.columns:
                        data[col] = data[f"_{col}_geometric_front"]
        elif adjust == "geometric_back":
            if "close_geometric_back" in data.columns:
                # 使用等比后复权列
                for col in ["open", "high", "low", "close"]:
                    if f"_{col}_geometric_back" in data.columns:
                        data[col] = data[f"_{col}_geometric_back"]

        return data

    def get_multiple_stocks(
        self,
        stock_codes: list[str],
        start_date: str,
        end_date: str,
        period: str = "1d",
        adjust: str = "none",
    ) -> dict[str, pd.DataFrame]:
        """
        批量获取多个股票的数据

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            period: 数据周期
            adjust: 复权类型

        Returns:
            Dict: {stock_code: DataFrame}
        """
        result = {}

        self._logger.info("批量获取: %d 只股票", len(stock_codes))

        for i, code in enumerate(stock_codes, 1):
            self._logger.debug("处理 [%d/%d] %s", i, len(stock_codes), code)
            data = self.get_stock_data(code, start_date, end_date, period, adjust)
            result[code] = data

        return result

    def ingest_tick_data(
        self, stock_code: str, start_date: str, end_date: str, aggregate_1m: bool = True
    ) -> bool:
        try:
            from xtquant import xtdata
        except Exception:
            return False
        df = self._read_tick_from_qmt(xtdata, stock_code, start_date, end_date)
        if df is None or df.empty:
            return False
        self._save_ticks_to_duckdb(df, stock_code)
        if aggregate_1m:
            try:
                series = df["lastPrice"].copy()
                series.index = pd.to_datetime(df.index)
                ohlc = series.resample("1min").ohlc()
                vol = df["volume"].resample("1min").sum()
                amt = df["amount"].resample("1min").sum()
                bars = pd.concat([ohlc, vol.rename("volume"), amt.rename("amount")], axis=1).dropna(
                    how="all"
                )
                self._save_to_duckdb(bars, stock_code, "1m")
            except Exception:
                pass
        return True

    def ingest_transaction_data(
        self, stock_code: str, start_date: str, end_date: str, aggregate_1m: bool = True
    ) -> tuple[bool, bool]:
        try:
            from xtquant import xtdata
        except Exception:
            return False, False
        df, used_fallback = self._read_transaction_from_qmt(xtdata, stock_code, start_date, end_date)
        if df is None or df.empty:
            return False, used_fallback
        self._save_transactions_to_duckdb(df, stock_code)
        if aggregate_1m:
            try:
                series = df["price"].copy()
                series.index = pd.to_datetime(df.index)
                ohlc = series.resample("1min").ohlc()
                vol = df["volume"].resample("1min").sum()
                amt = df["amount"].resample("1min").sum()
                bars = pd.concat([ohlc, vol.rename("volume"), amt.rename("amount")], axis=1).dropna(
                    how="all"
                )
                self._save_to_duckdb(bars, stock_code, "1m")
            except Exception:
                pass
        return True, used_fallback

    # =========================================================================
    # 因子引擎 API
    # =========================================================================

    def _ensure_factor_storage(self) -> Any:
        """懒初始化 FactorStorage，并确保 factor_values 表已建立。"""
        if self._factor_storage is not None:
            return self._factor_storage
        if self.con is None:
            raise RuntimeError("因子存储不可用：请先调用 connect()")
        try:
            from data_manager.factor_registry import FactorStorage

            class _ConAdapter:
                """将裸 duckdb 连接适配为 FactorStorage 的 execute/query 接口。"""
                def __init__(self, con: Any) -> None:
                    self._con = con

                def execute(self, sql: str, params: Any = None) -> None:
                    if params is not None:
                        self._con.execute(sql, list(params))
                    else:
                        self._con.execute(sql)

                def query(self, sql: str, params: Any = None) -> pd.DataFrame:
                    if params is not None:
                        return self._con.execute(sql, list(params)).df()
                    return self._con.execute(sql).df()

            self._factor_storage = FactorStorage(_ConAdapter(self.con))
        except Exception:
            self._logger.exception("FactorStorage 初始化失败")
            raise
        return self._factor_storage

    def list_factors(self) -> list[dict]:
        """
        列出当前注册中心中所有已注册的因子元数据。

        Returns:
            list of dict，每条包含 name/category/description/version/tags 字段。
        """
        try:
            from data_manager.factor_registry import factor_registry
            return factor_registry.list_all()
        except Exception:
            self._logger.exception("list_factors 失败")
            return []

    def compute_factor(
        self,
        factor_name: str,
        stock_code: str,
        start_date: str,
        end_date: str,
        period: str = "1d",
        adjust: str = "none",
        extra_bars: int = 60,
    ) -> pd.Series:
        """
        计算指定因子值。

        从本地 DuckDB / QMT 取 OHLCV 数据后，调用已注册的因子计算函数。

        Args:
            factor_name: 已注册因子名称，如 "momentum_20d"。
            stock_code:  股票代码，如 "000001.SZ"。
            start_date:  数据起始日（因子计算会自动向前多取 extra_bars 根 K 线热身）。
            end_date:    数据截止日。
            period:      K 线周期（"1d" / "1m" / "5m"）。
            adjust:      复权方式（"none" / "front" / "back"）。
            extra_bars:  热身 K 线根数，避免滚动窗口产生 NaN（默认 60）。

        Returns:
            pd.Series，index 为日期，name 为 factor_name；若无数据则返回空 Series。

        Raises:
            KeyError:  factor_name 未注册。
        """
        from data_manager.factor_registry import FactorComputeEngine, factor_registry

        defn = factor_registry.get(factor_name)
        if defn is None:
            raise KeyError(f"因子 '{factor_name}' 未在注册中心中找到，请先注册")

        # 向前多取 extra_bars 根 K 线做热身
        try:
            start_dt = pd.to_datetime(start_date) - pd.tseries.offsets.BDay(extra_bars)
            warm_start = start_dt.strftime("%Y-%m-%d")
        except Exception:
            warm_start = start_date

        df = self.get_stock_data(stock_code, warm_start, end_date, period, adjust)
        if df is None or df.empty:
            self._logger.warning(
                "compute_factor: 获取 %s 数据为空，无法计算因子 %s", stock_code, factor_name
            )
            return pd.Series(name=factor_name)

        # 统一使用 date/datetime 列作为 index
        for col in ("datetime", "date"):
            if col in df.columns:
                df = df.set_index(col)
                break

        engine = FactorComputeEngine(factor_registry)
        series = engine.compute(factor_name, df)

        # 裁剪到请求的时间范围（去掉热身段）
        try:
            series = series.loc[series.index >= pd.to_datetime(start_date)]
        except Exception:
            pass
        return series

    def save_factor(
        self,
        symbol: str,
        factor_name: str,
        series: pd.Series,
        version: str = "1.0",
        if_exists: str = "replace",
    ) -> int:
        """
        将因子序列持久化到 DuckDB 的 factor_values 表。

        Args:
            symbol:      股票代码。
            factor_name: 因子名称。
            series:      pd.Series（index 为日期）。
            version:     因子版本号。
            if_exists:   "replace"（默认，upsert）或 "skip"。

        Returns:
            写入行数。
        """
        storage = self._ensure_factor_storage()
        return storage.save(symbol, factor_name, series, version=version, if_exists=if_exists)

    def load_factor(
        self,
        symbol: str,
        factor_name: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.Series:
        """
        从 DuckDB 加载指定因子序列。

        Args:
            symbol:      股票代码。
            factor_name: 因子名称。
            start_date:  起始日（含），为 None 则从最早记录开始。
            end_date:    截止日（含），为 None 则到最新记录。

        Returns:
            pd.Series（index=date），无数据返回空 Series。
        """
        storage = self._ensure_factor_storage()
        return storage.load(symbol, factor_name, start_date, end_date)

    def compute_and_save_factor(
        self,
        factor_name: str,
        stock_code: str,
        start_date: str,
        end_date: str,
        period: str = "1d",
        adjust: str = "none",
        version: str = "1.0",
    ) -> int:
        """
        一键计算并存储因子值（组合 compute_factor + save_factor）。

        Returns:
            写入 DuckDB 的行数；计算或存储失败时返回 -1。
        """
        try:
            series = self.compute_factor(
                factor_name, stock_code, start_date, end_date, period, adjust
            )
            if series is None or series.empty:
                return 0
            return self.save_factor(stock_code, factor_name, series, version=version)
        except Exception:
            self._logger.exception(
                "compute_and_save_factor 失败 factor=%s symbol=%s", factor_name, stock_code
            )
            return -1

    def list_stored_factors(self, symbol: str | None = None) -> pd.DataFrame:
        """
        查询 DuckDB 中已存储的因子汇总信息。

        Args:
            symbol: 如指定，只返回该股票的因子记录；否则返回全部。

        Returns:
            DataFrame with columns [symbol, factor_name, date_from, date_to, row_count, version]。
        """
        storage = self._ensure_factor_storage()
        return storage.list_available(symbol)

    def close(self):
        """关闭数据库连接（同时清理后台调度器）"""
        if self._backfill_scheduler is not None:
            try:
                self._backfill_scheduler.stop(timeout=1.5)
            except Exception:
                pass
            self._backfill_scheduler = None
        self._close_duckdb_connection()
        if self.con is None:
            self._logger.debug("DuckDB 连接已关闭")


# 便捷函数
def get_stock_data(
    stock_code: str,
    start_date: str,
    end_date: str,
    period: str = "1d",
    adjust: str = "none",
    duckdb_path: str | None = None,
) -> pd.DataFrame:
    """
    便捷函数：获取股票数据（统一入口）

    Args:
        stock_code: 股票代码
        start_date: 开始日期（'YYYY-MM-DD'）
        end_date: 结束日期（'YYYY-MM-DD'）
        period: 数据周期（'1d', '1m', '5m'）
        adjust: 复权类型（'none', 'front', 'back'）
        duckdb_path: DuckDB路径

    Returns:
        DataFrame: OHLCV数据
    """
    interface = UnifiedDataInterface(duckdb_path=duckdb_path)
    interface.connect()

    try:
        data = interface.get_stock_data(stock_code, start_date, end_date, period, adjust)
        return data
    finally:
        interface.close()


# 测试代码
if __name__ == "__main__":
    print("=" * 80)
    print("统一数据接口测试")
    print("=" * 80)

    # 测试1：获取单只股票数据
    print("\n【测试1】获取511380.SH数据")
    interface = UnifiedDataInterface()
    interface.connect()

    data = interface.get_stock_data(
        stock_code="511380.SH",
        start_date="2024-01-01",
        end_date="2024-12-31",
        period="1d",
        adjust="none",
    )

    if not data.empty:
        print("\n[OK] 数据获取成功")
        print(f"  时间范围: {data.index.min()} ~ {data.index.max()}")
        print(f"  总记录数: {len(data)}")
        print(f"  价格范围: {data['close'].min():.2f} ~ {data['close'].max():.2f}")
        print("\n前5条数据:")
        print(data.head())
    else:
        print("\n[ERROR] 数据获取失败")

    interface.close()

    # 测试2：使用便捷函数
    print("\n\n【测试2】使用便捷函数")
    data2 = get_stock_data(
        stock_code="511380.SH",
        start_date="2024-06-01",
        end_date="2024-12-31",
        period="1d",
        adjust="front",
    )

    if not data2.empty:
        print("\n[OK] 便捷函数测试成功")
        print(f"  获取 {len(data2)} 条记录")
