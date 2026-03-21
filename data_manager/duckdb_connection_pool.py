#!/usr/bin/env python3
"""
DuckDB 连接管理器
解决数据库文件锁定问题，允许多个进程同时访问
"""

import atexit
import logging
import os
import shutil
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

import duckdb

log = logging.getLogger(__name__)


def resolve_duckdb_path(duckdb_path: Optional[str] = None) -> str:
    # 显式传入路径优先级最高，允许目标文件尚未创建。
    if duckdb_path:
        return duckdb_path

    candidates: list[str] = []
    env_path = os.environ.get("EASYXT_DUCKDB_PATH")
    if env_path:
        candidates.append(env_path)
    config_obj: Optional[Any] = None
    try:
        from easy_xt.config import config as config_obj
    except Exception:
        config_obj = None
    if config_obj is not None:
        cfg_path = config_obj.get("data.duckdb_path") or config_obj.get("settings.data.duckdb_path")
        if cfg_path:
            candidates.append(cfg_path)
        userdata_path = config_obj.get("trade.userdata_path")
        if userdata_path:
            candidates.append(os.path.join(userdata_path, "datadir", "stock_data.ddb"))
            candidates.append(os.path.join(userdata_path, "datadir", "duckdb", "stock_data.ddb"))
    project_root = Path(__file__).resolve().parents[1]
    candidates.append(str(project_root / "data" / "stock_data.ddb"))
    # 兜底：仍保留常见外部路径便于迁移期兼容
    _legacy_fallback = os.environ.get(
        "EASYXT_DUCKDB_LEGACY_PATH", r"D:/StockData/stock_data.ddb"
    )
    candidates.append(_legacy_fallback)
    existing_candidates: list[str] = []
    for candidate in candidates:
        if not candidate or not os.path.exists(candidate):
            continue
        existing_candidates.append(candidate)
        parent = os.path.dirname(candidate) or "."
        probe = os.path.join(parent, f".easyxt_write_probe_{uuid.uuid4().hex}.tmp")
        try:
            with open(probe, "w", encoding="utf-8") as f:
                f.write("ok")
            try:
                os.remove(probe)
            except OSError:
                pass
            return candidate
        except OSError:
            continue
    if existing_candidates:
        return existing_candidates[0]
    # 没有任何候选路径存在时，回退到项目内路径（首次运行时自动创建）
    return str(project_root / "data" / "stock_data.ddb")


class DuckDBConnectionManager:
    """
    DuckDB 连接管理器

    功能：
    1. 自动使用只读模式（GUI）
    2. 连接池管理
    3. 自动重试机制
    4. 上下文管理器支持
    """

    _instances: dict[str, "DuckDBConnectionManager"] = {}
    _instances_lock = threading.Lock()
    _wal_repair_lock = threading.Lock()
    _instance_key: str

    def __new__(cls, duckdb_path: Optional[str] = None):
        resolved_path = resolve_duckdb_path(duckdb_path)
        key = os.path.normcase(os.path.abspath(resolved_path))
        with cls._instances_lock:
            instance = cls._instances.get(key)
            if instance is None:
                instance = super().__new__(cls)
                instance._initialized = False
                instance._instance_key = key
                cls._instances[key] = instance
        return instance

    def __init__(self, duckdb_path: Optional[str] = None):
        if self._initialized:
            return

        self.duckdb_path = resolve_duckdb_path(duckdb_path)
        self._write_lock = threading.RLock()
        self._write_file_lock_path = f"{self.duckdb_path}.write.lock"
        self._write_lock_timeout_s = max(
            1.0, float(os.environ.get("EASYXT_WRITE_LOCK_TIMEOUT_S", "60"))
        )
        self._write_lock_stale_s = max(
            5.0, float(os.environ.get("EASYXT_WRITE_LOCK_STALE_S", "180"))
        )
        self._connection_count = 0
        self._wal_repaired_once = False
        self._lock_metrics: dict = {"attempts": 0, "failures": 0, "wait_times_ms": []}
        self._checkpoint_thread: Optional[threading.Thread] = None
        self._checkpoint_stop = threading.Event()
        self._checkpoint_interval_s = max(
            30.0, float(os.environ.get("EASYXT_CHECKPOINT_INTERVAL_S", "300"))
        )
        self._checkpoint_enabled = os.environ.get("EASYXT_ENABLE_AUTO_CHECKPOINT", "1") in (
            "1", "true", "True"
        )
        self._checkpoint_nonblocking = os.environ.get("EASYXT_CHECKPOINT_NONBLOCKING", "1") in (
            "1", "true", "True"
        )
        self._checkpoint_skip_when_busy = os.environ.get("EASYXT_CHECKPOINT_SKIP_WHEN_BUSY", "1") in (
            "1", "true", "True"
        )
        self._checkpoint_on_process_exit = os.environ.get("EASYXT_CHECKPOINT_ON_PROCESS_EXIT", "1") in (
            "1", "true", "True"
        )
        if self._checkpoint_enabled:
            self._start_checkpoint_worker()
        atexit.register(self._on_process_exit)
        self._initialized = True

    @staticmethod
    def _is_lock_error(error: Exception) -> bool:
        text = str(error).lower()
        return (
            "lock" in text
            or "already open" in text
            or "另一个程序正在使用" in text
            or "different configuration than existing connections" in text
        )

    @staticmethod
    def _is_wal_replay_error(error: Exception) -> bool:
        text = str(error).lower()
        return "failure while replaying wal file" in text

    def _repair_wal_if_needed(self) -> bool:
        if os.environ.get("EASYXT_ENABLE_WAL_AUTO_REPAIR", "1") not in ("1", "true", "True"):
            return False
        wal_path = f"{self.duckdb_path}.wal"
        if self._wal_repaired_once or not os.path.exists(wal_path) or self._connection_count > 0:
            return False
        with self._wal_repair_lock:
            if self._wal_repaired_once or not os.path.exists(wal_path) or self._connection_count > 0:
                return False
            backup_path = f"{wal_path}.bak.{int(time.time())}"
            try:
                shutil.copy2(wal_path, backup_path)
                os.remove(wal_path)
                self._wal_repaired_once = True
                log.warning("检测到WAL回放异常，已备份并清理: %s", backup_path)
                return True
            except Exception as _wal_err:
                log.warning("WAL修复失败: %s", _wal_err)
                return False

    def repair_wal_if_needed(self) -> bool:
        """公开的 WAL 自愈接口，供外部调用方使用。"""
        return self._repair_wal_if_needed()

    @contextmanager
    def get_read_connection(self):
        """
        获取读连接（用于GUI查询）

        注意: DuckDB 不允许同一数据库文件同时存在 read_only 和 read_write 连接。
        由于 chart/backfill 等模块始终使用 read_write 连接，此处统一使用
        read_only=False 以避免 "different configuration" 冲突。

        使用方式：
            with manager.get_read_connection() as con:
                df = con.execute("SELECT * FROM stock_daily").df()
        """
        con = None
        max_retries = 30
        retry_delay = 0.25  # 短间隔：5s 窗口内可命中 ~20 次而非 5 次

        for attempt in range(max_retries):
            try:
                con = duckdb.connect(self.duckdb_path, read_only=False)
                self._connection_count += 1
                yield con
                break
            except Exception as e:
                if self._is_wal_replay_error(e) and self._repair_wal_if_needed():
                    continue
                if self._is_lock_error(e):
                    self._lock_metrics["attempts"] += 1
                    if attempt < max_retries - 1:
                        sleep_s = retry_delay
                        # 第一次重试用 WARNING，后续降为 DEBUG 避免日志爆炸
                        if attempt == 0:
                            log.warning("[读取] 数据库被占用，开始重试 (最多 %.0fs)...", max_retries * retry_delay)
                        else:
                            log.debug("[读取] 数据库被占用，重试 %d/%d...", attempt + 1, max_retries)
                        t0 = time.monotonic()
                        time.sleep(sleep_s)
                        self._lock_metrics["wait_times_ms"].append((time.monotonic() - t0) * 1000.0)
                        continue
                    log.warning("[读取] 重试 %d 次仍被占用，放弃", max_retries)
                    self._lock_metrics["failures"] += 1
                raise
            finally:
                if con:
                    try:
                        con.close()
                    except Exception:
                        pass
                    finally:
                        self._connection_count -= 1

    @contextmanager
    def get_write_connection(self):
        """
        获取写连接（用于数据更新）

        使用方式：
            with manager.get_write_connection() as con:
                con.execute("UPDATE stock_daily SET ...")
        """
        con = None
        max_retries = 10
        retry_delay = 1.0

        with self._write_lock, self._acquire_cross_process_write_lock():
            for attempt in range(max_retries):
                try:
                    con = duckdb.connect(self.duckdb_path, read_only=False)
                    self._connection_count += 1
                    con.execute("BEGIN TRANSACTION")
                    try:
                        yield con
                        con.execute("COMMIT")
                    except Exception:
                        try:
                            con.execute("ROLLBACK")
                        except Exception:
                            pass
                        raise
                    break
                except Exception as e:
                    if self._is_wal_replay_error(e) and self._repair_wal_if_needed():
                        continue
                    if self._is_lock_error(e):
                        self._lock_metrics["attempts"] += 1
                        if attempt < max_retries - 1:
                            sleep_s = retry_delay * (attempt + 1)
                            log.warning("[写入] 数据库被占用，重试 %d/%d (%.1fs)...", attempt + 1, max_retries, sleep_s)
                            t0 = time.monotonic()
                            time.sleep(sleep_s)
                            self._lock_metrics["wait_times_ms"].append((time.monotonic() - t0) * 1000.0)
                            continue
                        self._lock_metrics["failures"] += 1
                    raise
                finally:
                    if con:
                        try:
                            con.close()
                        except Exception:
                            pass
                        finally:
                            self._connection_count -= 1

    @contextmanager
    def _acquire_cross_process_write_lock(self):
        fd: Optional[int] = None
        start = time.monotonic()
        while True:
            try:
                fd = os.open(self._write_file_lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(fd, f"{os.getpid()}|{int(time.time())}".encode())
                break
            except FileExistsError:
                try:
                    age = time.time() - os.path.getmtime(self._write_file_lock_path)
                except Exception:
                    age = 0.0
                if age > self._write_lock_stale_s:
                    try:
                        os.remove(self._write_file_lock_path)
                        continue
                    except Exception:
                        pass
                if (time.monotonic() - start) >= self._write_lock_timeout_s:
                    self._lock_metrics["failures"] += 1
                    raise TimeoutError(
                        f"跨进程写锁获取超时: {self._write_file_lock_path}"
                    )
                self._lock_metrics["attempts"] += 1
                time.sleep(0.2)
        try:
            yield
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except Exception:
                    pass
            try:
                if os.path.exists(self._write_file_lock_path):
                    os.remove(self._write_file_lock_path)
            except Exception:
                pass

    def execute_read_query(self, query: str, params: Optional[tuple] = None):
        """
        执行只读查询（快捷方法）

        Args:
            query: SQL查询语句
            params: 查询参数（可选）

        Returns:
            DataFrame: 查询结果
        """
        with self.get_read_connection() as con:
            if params:
                df = con.execute(query, params).df()
            else:
                df = con.execute(query).df()
            return df

    def execute_write_query(self, query: str, params: Optional[tuple] = None):
        """
        执行写操作（快捷方法）

        Args:
            query: SQL更新/插入/删除语句
            params: 查询参数（可选）

        Returns:
            执行结果
        """
        with self.get_write_connection() as con:
            if params:
                result = con.execute(query, params)
            else:
                result = con.execute(query)
            return result

    def insert_dataframe(self, table_name: str, df: Any) -> int:
        if df is None or df.empty:
            return 0

        with self.get_write_connection() as con:
            columns = [row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
            df_to_insert = df.copy()

            if columns:
                for col in columns:
                    if col not in df_to_insert.columns:
                        df_to_insert[col] = None
                df_to_insert = df_to_insert[columns]
                columns_sql = ", ".join(columns)
                con.register("temp_insert_df", df_to_insert)
                con.execute(
                    "INSERT INTO " + table_name + " (" + columns_sql + ") "
                    "SELECT " + columns_sql + " FROM temp_insert_df"
                )
            else:
                con.register("temp_insert_df", df_to_insert)
                con.execute("INSERT INTO " + table_name + " SELECT * FROM temp_insert_df")

            con.unregister("temp_insert_df")
            return len(df_to_insert)

    @property
    def connection_count(self):
        """当前连接数"""
        return self._connection_count

    def get_lock_metrics(self) -> dict:
        """返回锁等待指标，用于SLO监控。目标: failure_rate<0.1%, p95_wait_ms<200。"""
        times = self._lock_metrics["wait_times_ms"]
        p95 = sorted(times)[int(len(times) * 0.95)] if times else 0.0
        total = self._lock_metrics["attempts"]
        return {
            "failure_rate": self._lock_metrics["failures"] / total if total > 0 else 0.0,
            "p95_wait_ms": p95,
            "total_attempts": total,
            "failures": self._lock_metrics["failures"],
        }

    def reset_lock_metrics(self) -> None:
        """重置锁等待计数器（建议每日零点调用）。"""
        self._lock_metrics = {"attempts": 0, "failures": 0, "wait_times_ms": []}

    def checkpoint(self) -> bool:
        """
        强制 WAL 检查点，将 WAL 内容刷入主数据库文件。
        建议在大批量写入后或应用退出前调用。
        """
        if self.duckdb_path == ":memory:":
            return True  # 内存数据库无 WAL，无需 checkpoint
        if getattr(self, '_checkpoint_skip_when_busy', False) and getattr(self, '_connection_count', 0) > 0:
            log.debug("DuckDB checkpoint 跳过: active_connections=%s", getattr(self, '_connection_count', 0))
            return False
        locked = False
        try:
            if getattr(self, '_checkpoint_nonblocking', True):
                locked = self._write_lock.acquire(blocking=False)
                if not locked:
                    log.debug("DuckDB checkpoint 跳过: write_lock busy")
                    return False
            with self.get_write_connection() as con:
                con.execute("CHECKPOINT")
            log.debug("DuckDB checkpoint 完成: %s", self.duckdb_path)
            return True
        except Exception as e:
            log.warning("DuckDB checkpoint 失败: %s", e)
            return False
        finally:
            if locked:
                try:
                    self._write_lock.release()
                except Exception:
                    pass

    def _safe_checkpoint(self, trigger: str) -> bool:
        if not threading.main_thread().is_alive():
            return False
        try:
            return self.checkpoint()
        except Exception as e:
            log.warning("DuckDB checkpoint 异常 trigger=%s err=%s", trigger, e)
            return False

    def _checkpoint_loop(self) -> None:
        """后台 WAL 检查点循环，对解释器退出完全安全。"""
        try:
            while True:
                # 主线程已退出（解释器关闭）→ 安全退出，避免访问已卸载的 C 扩展
                if not threading.main_thread().is_alive():
                    return
                # wait() 返回 True 表示 stop 事件触发
                try:
                    if self._checkpoint_stop.wait(self._checkpoint_interval_s):
                        return
                except Exception:
                    return  # 解释器关闭时 Event 可能抛异常
                if not threading.main_thread().is_alive():
                    return
                try:
                    self._safe_checkpoint("auto")
                except Exception as e:
                    log.warning("自动checkpoint失败: %s", e)
        except Exception:
            pass  # 解释器关闭期间的任何异常均静默退出

    def stop_checkpoint_worker(self, timeout: float = 3.0) -> None:
        """停止后台 checkpoint 线程（可供测试 fixture 显式调用）。"""
        self._checkpoint_stop.set()
        t = self._checkpoint_thread
        if t is not None and t.is_alive():
            t.join(timeout=timeout)

    def _start_checkpoint_worker(self) -> None:
        if self._checkpoint_thread is not None and self._checkpoint_thread.is_alive():
            return
        t = threading.Thread(
            target=self._checkpoint_loop,
            name=f"DuckDBCheckpoint-{os.path.basename(self.duckdb_path)}",
            daemon=True,
        )
        self._checkpoint_thread = t
        t.start()

    def _on_process_exit(self) -> None:
        try:
            self._checkpoint_stop.set()
        except Exception:
            pass
        try:
            t = getattr(self, '_checkpoint_thread', None)
            if t is not None and t.is_alive():
                t.join(timeout=2.0)  # 最多等 2 秒，优先等待后台线程自然退出
        except Exception:
            pass
        try:
            if getattr(self, '_checkpoint_on_process_exit', True) and threading.main_thread().is_alive():
                self._safe_checkpoint("process_exit")
        except Exception as e:
            log.warning("进程退出checkpoint失败: %s", e)


# 按数据库路径缓存管理器（路径归一化后作为 key）
_db_managers: dict[str, DuckDBConnectionManager] = {}
_db_managers_lock = threading.Lock()


def get_db_manager(duckdb_path: Optional[str] = None) -> DuckDBConnectionManager:
    """获取数据库管理器（按数据库路径单例）"""
    resolved_path = resolve_duckdb_path(duckdb_path)
    key = os.path.normcase(os.path.abspath(resolved_path))
    with _db_managers_lock:
        manager = _db_managers.get(key)
        if manager is None:
            manager = DuckDBConnectionManager(resolved_path)
            _db_managers[key] = manager
    return manager


# 便捷函数
def query_dataframe(query: str, params: Optional[tuple] = None) -> Any:
    """快捷查询函数（只读）"""
    manager = get_db_manager()
    return manager.execute_read_query(query, params)


def execute_update(query: str, params: Optional[tuple] = None):
    """快捷更新函数（写操作）"""
    manager = get_db_manager()
    return manager.execute_write_query(query, params)


if __name__ == "__main__":
    """测试代码"""
    print("=" * 80)
    print("DuckDB 连接管理器测试")
    print("=" * 80)

    manager = get_db_manager()

    # 测试1：只读查询
    print("\n[测试1] 只读查询...")
    try:
        df = manager.execute_read_query("""
            SELECT
                COUNT(DISTINCT stock_code) as stock_count,
                COUNT(*) as total_records
            FROM stock_daily
        """)
        print(f"[OK] 股票数: {df['stock_count'].iloc[0]:,}, 记录数: {df['total_records'].iloc[0]:,}")
    except Exception as e:
        print(f"[ERROR] {e}")

    # 测试2：上下文管理器
    print("\n[测试2] 上下文管理器...")
    try:
        with manager.get_read_connection() as con:
            df = con.execute("SELECT * FROM stock_daily LIMIT 3").df()
            print(f"[OK] 查询到 {len(df)} 条记录")
    except Exception as e:
        print(f"[ERROR] {e}")

    # 测试3：快捷函数
    print("\n[测试3] 快捷函数...")
    try:
        df = query_dataframe("SELECT * FROM stock_daily WHERE stock_code = '511380.SH' LIMIT 3")
        print(f"[OK] 查询到 {len(df)} 条记录")
    except Exception as e:
        print(f"[ERROR] {e}")

    print("\n" + "=" * 80)
    print("测试完成")
    print("=" * 80)
