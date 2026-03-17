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
import threading
import time

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
