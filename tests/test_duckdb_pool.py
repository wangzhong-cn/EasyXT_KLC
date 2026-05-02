"""
Gate 3 – DuckDB 连接池单元测试

覆盖：
  - lock metrics 初始状态
  - lock metrics 累积（模拟重试）
  - get_lock_metrics 结构与 p95 计算
  - reset_lock_metrics 幂等
  - 内存 DuckDB 读写 round-trip
"""

import os
import sys
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from data_manager.duckdb_connection_pool import DuckDBConnectionManager

# ---------------------------------------------------------------------------
# 使用独立的 :memory: 实例，避免冲突单例
# ---------------------------------------------------------------------------

@pytest.fixture
def mem_manager(tmp_path):
    """返回一个指向 tmp 文件的独立管理器，不污染单例缓存。"""
    db_path = str(tmp_path / "test_pool.ddb")
    # 绕过单例：直接构造
    mgr = object.__new__(DuckDBConnectionManager)
    mgr._initialized = False
    mgr._instance_key = db_path
    mgr.__init__(db_path)
    return mgr


# ---------------------------------------------------------------------------
# Lock Metrics 初始状态
# ---------------------------------------------------------------------------

class TestLockMetricsInit:
    def test_initial_metrics(self, mem_manager):
        m = mem_manager.get_lock_metrics()
        assert m["total_attempts"] == 0
        assert m["failures"] == 0
        assert m["failure_rate"] == 0.0
        assert m["p95_wait_ms"] == 0.0

    def test_reset_is_idempotent(self, mem_manager):
        mem_manager.reset_lock_metrics()
        mem_manager.reset_lock_metrics()
        m = mem_manager.get_lock_metrics()
        assert m["total_attempts"] == 0


# ---------------------------------------------------------------------------
# Lock Metrics 累积
# ---------------------------------------------------------------------------

class TestLockMetricsAccumulation:
    def test_failure_rate_calculation(self, mem_manager):
        # 直接写入指标
        mem_manager._lock_metrics["attempts"] = 10
        mem_manager._lock_metrics["failures"] = 1
        mem_manager._lock_metrics["wait_times_ms"] = [50.0] * 10
        m = mem_manager.get_lock_metrics()
        assert abs(m["failure_rate"] - 0.1) < 1e-9
        assert m["total_attempts"] == 10
        assert m["failures"] == 1

    def test_p95_correct(self, mem_manager):
        # 100 个等待时间：1..100 ms，p95 应在第 95 个
        times = list(range(1, 101))
        mem_manager._lock_metrics["wait_times_ms"] = times
        mem_manager._lock_metrics["attempts"] = 100
        m = mem_manager.get_lock_metrics()
        # sorted[int(100*0.95)] = sorted[95] = 96
        assert m["p95_wait_ms"] == 96

    def test_reset_clears_accumulated(self, mem_manager):
        mem_manager._lock_metrics["attempts"] = 99
        mem_manager._lock_metrics["failures"] = 5
        mem_manager.reset_lock_metrics()
        m = mem_manager.get_lock_metrics()
        assert m["total_attempts"] == 0
        assert m["failures"] == 0

    def test_single_zero_failure_rate(self, mem_manager):
        mem_manager._lock_metrics["attempts"] = 0
        mem_manager._lock_metrics["failures"] = 0
        m = mem_manager.get_lock_metrics()
        assert m["failure_rate"] == 0.0


# ---------------------------------------------------------------------------
# Read / Write round-trip（内存数据库）
# ---------------------------------------------------------------------------

class TestReadWriteRoundTrip:
    def test_write_then_read(self, mem_manager):
        with mem_manager.get_write_connection() as con:
            con.execute("CREATE TABLE t (id INTEGER, val VARCHAR)")
            con.execute("INSERT INTO t VALUES (1, 'hello')")

        with mem_manager.get_read_connection() as con:
            rows = con.execute("SELECT * FROM t").fetchall()

        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == "hello"

    def test_execute_write_and_read_query(self, mem_manager):
        mem_manager.execute_write_query("CREATE TABLE nums (n INTEGER)")
        mem_manager.execute_write_query("INSERT INTO nums VALUES (42)")
        df = mem_manager.execute_read_query("SELECT n FROM nums")
        assert df.iloc[0]["n"] == 42

    def test_multiple_writes(self, mem_manager):
        with mem_manager.get_write_connection() as con:
            con.execute("CREATE TABLE items (name VARCHAR)")
        for i in range(5):
            mem_manager.execute_write_query(f"INSERT INTO items VALUES ('item{i}')")
        df = mem_manager.execute_read_query("SELECT COUNT(*) AS cnt FROM items")
        assert df.iloc[0]["cnt"] == 5

    def test_write_transaction_rolls_back_on_exception(self, mem_manager):
        with mem_manager.get_write_connection() as con:
            con.execute("CREATE TABLE tx_test (id INTEGER)")
        with pytest.raises(RuntimeError):
            with mem_manager.get_write_connection() as con:
                con.execute("INSERT INTO tx_test VALUES (1)")
                raise RuntimeError("boom")
        df = mem_manager.execute_read_query("SELECT COUNT(*) AS cnt FROM tx_test")
        assert int(df.iloc[0]["cnt"]) == 0


# ---------------------------------------------------------------------------
# Thread safety（smoke test）
# ---------------------------------------------------------------------------

class TestThreadSafety:
    def test_concurrent_writes_no_exception(self, mem_manager):
        with mem_manager.get_write_connection() as con:
            con.execute("CREATE TABLE ctr (n INTEGER)")
            con.execute("INSERT INTO ctr VALUES (0)")

        errors = []

        def increment():
            try:
                with mem_manager.get_write_connection() as con:
                    con.execute("UPDATE ctr SET n = n + 1")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=increment) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"并发写入报错: {errors}"


class TestCrossProcessWriteLock:
    def test_stale_lock_self_heal(self, mem_manager):
        lock_path = mem_manager._write_file_lock_path
        with open(lock_path, "w", encoding="utf-8") as f:
            f.write("stale")
        stale_age = max(int(mem_manager._write_lock_stale_s + 2), 2)
        old_ts = time.time() - stale_age
        os.utime(lock_path, (old_ts, old_ts))
        with mem_manager._acquire_cross_process_write_lock():
            assert os.path.exists(lock_path)
        assert not os.path.exists(lock_path)


# ===========================================================================
# Additional coverage: uncovered branches
# ===========================================================================


class TestResolveDuckdbPath:
    def test_config_import_error_falls_back(self):
        """Lines 35-36: easy_xt.config import raises → config_obj = None."""
        from data_manager.duckdb_connection_pool import resolve_duckdb_path
        with patch.dict(os.environ, {"EASYXT_DUCKDB_PATH": ""}, clear=False), \
             patch.dict(sys.modules, {"easy_xt": None, "easy_xt.config": None}):
            result = resolve_duckdb_path()
        assert "stock_data.ddb" in result

    def test_no_existing_candidates_returns_project_root(self):
        """Line 72: all candidates nonexistent → return project root path."""
        from data_manager.duckdb_connection_pool import resolve_duckdb_path
        with patch.dict(os.environ, {"EASYXT_DUCKDB_PATH": "", "EASYXT_DUCKDB_LEGACY_PATH": ""}), \
             patch.dict(sys.modules, {"easy_xt": None, "easy_xt.config": None}), \
             patch("os.path.exists", return_value=False):
            result = resolve_duckdb_path()
        assert "stock_data.ddb" in result


class TestWALRepairPaths:
    def test_repair_already_done_returns_false(self, mem_manager):
        """Line 155: _wal_repaired_once = True."""
        mem_manager._wal_repaired_once = True
        assert mem_manager._repair_wal_if_needed() is False

    def test_repair_no_wal_file_returns_false(self, mem_manager):
        """Line 155: WAL file does not exist."""
        mem_manager._wal_repaired_once = False
        mem_manager._connection_count = 0
        wal_path = f"{mem_manager.duckdb_path}.wal"
        if os.path.exists(wal_path):
            os.remove(wal_path)
        assert mem_manager._repair_wal_if_needed() is False

    def test_repair_active_connections_returns_false(self, mem_manager):
        """Line 155: connection_count > 0."""
        wal_path = f"{mem_manager.duckdb_path}.wal"
        with open(wal_path, "w") as f:
            f.write("fake wal")
        mem_manager._wal_repaired_once = False
        mem_manager._connection_count = 1
        try:
            assert mem_manager._repair_wal_if_needed() is False
        finally:
            if os.path.exists(wal_path):
                os.remove(wal_path)



class TestConnectionFinallyExceptions:
    def test_read_connection_close_raises_is_swallowed(self, mem_manager):
        """Lines 213-214: con.close() raises in get_read_connection finally."""
        mock_con = MagicMock()
        mock_con.close.side_effect = RuntimeError("close failed")
        with patch("data_manager.duckdb_connection_pool.duckdb.connect",
                   return_value=mock_con):
            with mem_manager.get_read_connection():
                pass  # Should not raise despite close() failing

    def test_write_connection_rollback_raises_is_swallowed(self, mem_manager):
        """Lines 241-242: ROLLBACK raises in write connection error path."""
        def execute_side_effect(sql, *args, **kwargs):
            if "ROLLBACK" in str(sql).upper():
                raise RuntimeError("rollback failed")
            return MagicMock()

        mock_con = MagicMock()
        mock_con.execute.side_effect = execute_side_effect

        with patch("data_manager.duckdb_connection_pool.duckdb.connect",
                   return_value=mock_con):
            with pytest.raises(RuntimeError, match="boom"):
                with mem_manager.get_write_connection():
                    raise RuntimeError("boom")

    def test_write_connection_close_raises_is_swallowed(self, mem_manager):
        """Lines 264-265: con.close() raises in get_write_connection finally."""
        mock_con = MagicMock()
        mock_con.close.side_effect = RuntimeError("close failed")
        with patch("data_manager.duckdb_connection_pool.duckdb.connect",
                   return_value=mock_con):
            with mem_manager.get_write_connection():
                pass  # Should not raise


class TestCrossProcessLockAdvanced:
    def test_lock_timeout_raises_timeout_error(self, mem_manager):
        """Lines 287-293: lock file held fresh → times out."""
        lock_path = mem_manager._write_file_lock_path
        with open(lock_path, "w") as f:
            f.write(f"99999|{int(time.time())}")
        mem_manager._write_lock_stale_s = float("inf")
        mem_manager._write_lock_timeout_s = 0.05
        try:
            with pytest.raises(TimeoutError):
                with mem_manager._acquire_cross_process_write_lock():
                    pass
        finally:
            if os.path.exists(lock_path):
                os.remove(lock_path)

    def test_mtime_exception_uses_zero_age(self, mem_manager):
        """Lines 279-280: os.path.getmtime raises → age = 0.0."""
        lock_path = mem_manager._write_file_lock_path
        with open(lock_path, "w") as f:
            f.write("stale")
        mem_manager._write_lock_stale_s = float("inf")
        mem_manager._write_lock_timeout_s = 0.05
        try:
            with patch("os.path.getmtime", side_effect=OSError("no mtime")):
                with pytest.raises(TimeoutError):
                    with mem_manager._acquire_cross_process_write_lock():
                        pass
        finally:
            if os.path.exists(lock_path):
                os.remove(lock_path)

    def test_stale_lock_remove_fails_then_times_out(self, mem_manager):
        """Lines 285-286: stale removal raises → falls through to timeout."""
        lock_path = mem_manager._write_file_lock_path
        with open(lock_path, "w") as f:
            f.write("stale")
        stale_age = int(mem_manager._write_lock_stale_s) + 5
        os.utime(lock_path, (time.time() - stale_age,) * 2)
        mem_manager._write_lock_timeout_s = 0.05
        _orig_remove = os.remove

        def mock_remove(p):
            if p == lock_path:
                raise OSError("locked by OS")
            return _orig_remove(p)

        try:
            with patch("data_manager.duckdb_connection_pool.os.remove",
                       side_effect=mock_remove):
                with pytest.raises((TimeoutError, OSError)):
                    with mem_manager._acquire_cross_process_write_lock():
                        pass
        finally:
            try:
                _orig_remove(lock_path)
            except Exception:
                pass


class TestExecuteWithParams:
    def test_execute_write_query_with_params(self, mem_manager):
        """Line 339: params branch in execute_write_query."""
        mem_manager.execute_write_query(
            "CREATE TABLE param_t (id INTEGER, name VARCHAR)")
        mem_manager.execute_write_query(
            "INSERT INTO param_t VALUES (?, ?)", (42, "alice"))
        df = mem_manager.execute_read_query(
            "SELECT name FROM param_t WHERE id = ?", (42,))
        assert df.iloc[0]["name"] == "alice"


class TestInsertDataframe:
    def test_insert_none_returns_zero(self, mem_manager):
        """Lines 345-346: df is None → 0."""
        assert mem_manager.insert_dataframe("any_table", None) == 0

    def test_insert_empty_df_returns_zero(self, mem_manager):
        """Lines 345-346: empty DataFrame → 0."""
        import pandas as pd
        assert mem_manager.insert_dataframe("any_table", pd.DataFrame()) == 0

    def test_insert_with_columns_returns_count(self, mem_manager):
        """Lines 348-363: insert into table with defined schema."""
        import pandas as pd
        with mem_manager.get_write_connection() as con:
            con.execute("CREATE TABLE stocks_ins (code VARCHAR, close DOUBLE)")
        df = pd.DataFrame({"code": ["000001.SZ", "000002.SZ"],
                           "close": [10.5, 11.0]})
        count = mem_manager.insert_dataframe("stocks_ins", df)
        assert count == 2

    def test_insert_df_with_extra_columns(self, mem_manager):
        """Lines 353-355: df column not in table schema → set to None."""
        import pandas as pd
        with mem_manager.get_write_connection() as con:
            con.execute("CREATE TABLE prices_ins (code VARCHAR, open DOUBLE)")
        df = pd.DataFrame({"code": ["600000.SH"], "open": [9.2],
                           "extra": ["ignored"]})
        count = mem_manager.insert_dataframe("prices_ins", df)
        assert count == 1


class TestCheckpointWorker:
    def test_checkpoint_exception_swallowed_in_loop(self, mem_manager):
        """Lines 409-410: checkpoint() raises inside loop → warning, no crash."""
        calls = []

        def mock_wait(timeout=None):
            calls.append(1)
            return len(calls) > 2  # First 2 calls: False (run body); then: True (exit)

        saved_stop = mem_manager._checkpoint_stop
        mem_manager._checkpoint_stop = MagicMock()
        mem_manager._checkpoint_stop.wait = mock_wait
        try:
            with patch.object(mem_manager, "checkpoint",
                              side_effect=RuntimeError("ckpt err")):
                mem_manager._checkpoint_loop()
        finally:
            mem_manager._checkpoint_stop = saved_stop
        assert len(calls) >= 2

    def test_start_checkpoint_worker_idempotent(self, mem_manager):
        """Line 414: calling again while alive → same thread kept."""
        mem_manager._start_checkpoint_worker()
        first_thread = mem_manager._checkpoint_thread
        assert first_thread is not None and first_thread.is_alive()
        mem_manager._start_checkpoint_worker()
        assert mem_manager._checkpoint_thread is first_thread


class TestProcessExit:
    def test_on_process_exit_exception_swallowed(self, mem_manager):
        """Lines 427-428: checkpoint raises in _on_process_exit → swallowed."""
        with patch.object(mem_manager, "checkpoint",
                          side_effect=RuntimeError("boom")):
            mem_manager._on_process_exit()  # Must not propagate


class TestConvenienceFunctions:
    def test_query_dataframe_calls_manager(self, tmp_path):
        """Lines 451-452: query_dataframe() routes to manager."""
        from data_manager.duckdb_connection_pool import query_dataframe
        db_path = str(tmp_path / "cfe.ddb")
        mgr = object.__new__(DuckDBConnectionManager)
        mgr._initialized = False
        mgr._instance_key = db_path
        mgr.__init__(db_path)
        mgr.execute_write_query("CREATE TABLE qd_t (x INTEGER)")
        mgr.execute_write_query("INSERT INTO qd_t VALUES (7)")
        with patch("data_manager.duckdb_connection_pool.get_db_manager",
                   return_value=mgr):
            df = query_dataframe("SELECT x FROM qd_t")
        assert df.iloc[0]["x"] == 7

    def test_execute_update_calls_manager(self, tmp_path):
        """Lines 457-458: execute_update() routes to manager."""
        from data_manager.duckdb_connection_pool import execute_update
        db_path = str(tmp_path / "eu.ddb")
        mgr = object.__new__(DuckDBConnectionManager)
        mgr._initialized = False
        mgr._instance_key = db_path
        mgr.__init__(db_path)
        mgr.execute_write_query("CREATE TABLE eu_t (x INTEGER)")
        with patch("data_manager.duckdb_connection_pool.get_db_manager",
                   return_value=mgr):
            execute_update("INSERT INTO eu_t VALUES (99)")
        df = mgr.execute_read_query("SELECT x FROM eu_t")
        assert df.iloc[0]["x"] == 99
