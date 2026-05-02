"""
tests/test_cache_and_perf_monitor.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
core/cache_manager.py  &  core/performance_monitor.py 单元测试
(不依赖 Qt，不需要 QApplication)
"""
import time
import pytest
import pandas as pd

from core.cache_manager import CacheLevel, CacheManager, LRUCache, cache_manager
from core.performance_monitor import (
    PerfEvent, PerfTimer, PerformanceMonitor, perf_monitor, log_performance
)


# ===========================================================================
# LRUCache Tests
# ===========================================================================
class TestLRUCache:
    def setup_method(self):
        self.cache = LRUCache(max_size=3)

    def test_get_missing_returns_none(self):
        assert self.cache.get("x") is None

    def test_set_and_get(self):
        self.cache.set("a", 42)
        assert self.cache.get("a") == 42

    def test_len_zero_initially(self):
        assert len(self.cache) == 0

    def test_len_grows(self):
        self.cache.set("a", 1)
        self.cache.set("b", 2)
        assert len(self.cache) == 2

    def test_evicts_oldest_when_full(self):
        self.cache.set("a", 1)
        self.cache.set("b", 2)
        self.cache.set("c", 3)
        self.cache.set("d", 4)  # should evict "a"
        assert self.cache.get("a") is None
        assert self.cache.get("d") == 4

    def test_access_refreshes_position(self):
        self.cache.set("a", 1)
        self.cache.set("b", 2)
        self.cache.set("c", 3)
        _ = self.cache.get("a")  # refresh "a"
        self.cache.set("d", 4)  # should evict "b" (now oldest)
        assert self.cache.get("a") == 1
        assert self.cache.get("b") is None

    def test_update_existing_key(self):
        self.cache.set("a", 1)
        self.cache.set("a", 99)
        assert self.cache.get("a") == 99
        assert len(self.cache) == 1

    def test_delete_removes_key(self):
        self.cache.set("a", 1)
        self.cache.delete("a")
        assert self.cache.get("a") is None
        assert len(self.cache) == 0

    def test_delete_missing_key_no_error(self):
        self.cache.delete("nonexistent")  # should not raise

    def test_clear_empties_cache(self):
        self.cache.set("a", 1)
        self.cache.set("b", 2)
        self.cache.clear()
        assert len(self.cache) == 0

    def test_contains_true(self):
        self.cache.set("a", 1)
        assert "a" in self.cache

    def test_contains_false(self):
        assert "z" not in self.cache

    def test_none_value_stored(self):
        self.cache.set("a", None)
        # None is stored → get returns None, which looks like a miss
        # But the key IS in the cache
        assert "a" in self.cache

    def test_max_size_one(self):
        c = LRUCache(max_size=1)
        c.set("a", 1)
        c.set("b", 2)
        assert c.get("a") is None
        assert c.get("b") == 2


# ===========================================================================
# CacheManager Tests
# ===========================================================================
@pytest.fixture(autouse=True)
def clean_cache():
    """每个测试前后清空 CacheManager 内存缓存"""
    cache_manager._memory_cache.clear()
    cache_manager._cache_metadata.clear()
    yield
    cache_manager._memory_cache.clear()
    cache_manager._cache_metadata.clear()


class TestCacheManagerSingleton:
    def test_singleton_same_instance(self):
        cm1 = CacheManager()
        cm2 = CacheManager()
        assert cm1 is cm2

    def test_global_is_instance(self):
        assert isinstance(cache_manager, CacheManager)


class TestCacheManagerGenerateCacheKey:
    def test_basic_key_generation(self):
        key = cache_manager._generate_cache_key("ns", symbol="A", period="1d")
        assert "ns:" in key
        assert "A" in key or "1d" in key

    def test_deterministic(self):
        k1 = cache_manager._generate_cache_key("ns", a="1", b="2")
        k2 = cache_manager._generate_cache_key("ns", a="1", b="2")
        assert k1 == k2

    def test_different_kwargs_different_keys(self):
        k1 = cache_manager._generate_cache_key("ns", symbol="A")
        k2 = cache_manager._generate_cache_key("ns", symbol="B")
        assert k1 != k2

    def test_different_namespaces_different_keys(self):
        k1 = cache_manager._generate_cache_key("ns1", x="1")
        k2 = cache_manager._generate_cache_key("ns2", x="1")
        assert k1 != k2


class TestCacheManagerSetGet:
    def test_set_and_get_string(self):
        cache_manager.set("data", "key1", "hello", level=CacheLevel.MEMORY)
        result = cache_manager.get("data", "key1", level=CacheLevel.MEMORY)
        assert result == "hello"

    def test_set_and_get_dict(self):
        d = {"a": 1, "b": 2}
        cache_manager.set("data", "key2", d, level=CacheLevel.MEMORY)
        result = cache_manager.get("data", "key2", level=CacheLevel.MEMORY)
        assert result == d

    def test_set_and_get_dataframe(self):
        df = pd.DataFrame({"label": ["A", "B"], "value": [1, 2]})
        cache_manager.set("data", "df_key", df, level=CacheLevel.MEMORY)
        result = cache_manager.get("data", "df_key", level=CacheLevel.MEMORY)
        pd.testing.assert_frame_equal(result, df)

    def test_missing_key_returns_none(self):
        assert cache_manager.get("data", "nonexistent", level=CacheLevel.MEMORY) is None

    def test_ttl_expiry(self):
        cache_manager.set("data", "ttl_key", "value", ttl=1, level=CacheLevel.MEMORY)
        time.sleep(1.1)
        result = cache_manager.get("data", "ttl_key", level=CacheLevel.MEMORY)
        assert result is None

    def test_no_expiry_when_ttl_zero(self):
        cache_manager.set("data", "persist_key", "value", ttl=0, level=CacheLevel.MEMORY)
        result = cache_manager.get("data", "persist_key", level=CacheLevel.MEMORY)
        assert result == "value"


class TestCacheManagerInvalidate:
    def test_invalidate_specific_key(self):
        cache_manager.set("data", "k1", "v1", level=CacheLevel.MEMORY)
        cache_manager.set("data", "k2", "v2", level=CacheLevel.MEMORY)
        cache_manager.invalidate("data", "k1")
        assert cache_manager.get("data", "k1", level=CacheLevel.MEMORY) is None
        assert cache_manager.get("data", "k2", level=CacheLevel.MEMORY) == "v2"

    def test_invalidate_namespace(self):
        cache_manager.set("data", "k1", "v1", level=CacheLevel.MEMORY)
        cache_manager.set("data", "k2", "v2", level=CacheLevel.MEMORY)
        cache_manager.set("other", "k3", "v3", level=CacheLevel.MEMORY)
        cache_manager.invalidate("data")  # no key → whole namespace
        assert cache_manager.get("data", "k1", level=CacheLevel.MEMORY) is None
        assert cache_manager.get("data", "k2", level=CacheLevel.MEMORY) is None
        assert cache_manager.get("other", "k3", level=CacheLevel.MEMORY) == "v3"


class TestCacheManagerIsExpired:
    def test_not_expired(self):
        meta = {"created_at": time.time(), "ttl": 3600}
        assert not cache_manager._is_expired(meta)

    def test_expired(self):
        meta = {"created_at": time.time() - 10, "ttl": 5}
        assert cache_manager._is_expired(meta)

    def test_zero_ttl_never_expires(self):
        meta = {"created_at": time.time() - 99999, "ttl": 0}
        assert not cache_manager._is_expired(meta)

    def test_negative_ttl_never_expires(self):
        meta = {"created_at": time.time() - 99999, "ttl": -1}
        assert not cache_manager._is_expired(meta)


class TestCacheManagerEstimateSize:
    def test_small_object_positive(self):
        size = cache_manager._estimate_size("hello")
        assert size > 0

    def test_larger_object_bigger_size(self):
        small = cache_manager._estimate_size("a")
        large = cache_manager._estimate_size("a" * 1000)
        assert large > small

    def test_dataframe_size_positive(self):
        df = pd.DataFrame({"a": range(100)})
        size = cache_manager._estimate_size(df)
        assert size > 0

    def test_unpicklable_object_returns_zero(self):
        """lambda 无法被 pickle → except Exception → return 0 (lines 230-231)"""
        size = cache_manager._estimate_size(lambda: None)
        assert size == 0


class TestCacheManagerGetStats:
    def test_empty_stats(self):
        stats = cache_manager.get_stats()
        assert stats["memory_items"] == 0
        assert stats["metadata_items"] == 0
        assert stats["total_memory_bytes"] == 0

    def test_stats_after_set(self):
        cache_manager.set("data", "k1", "v1", level=CacheLevel.MEMORY)
        stats = cache_manager.get_stats()
        assert stats["memory_items"] == 1
        assert stats["metadata_items"] == 1
        assert stats["total_memory_bytes"] > 0

    def test_disk_cache_dir_in_stats(self):
        stats = cache_manager.get_stats()
        assert "disk_cache_dir" in stats


class TestCacheManagerGetDiskCachePath:
    def test_returns_path_object(self):
        from pathlib import Path
        path = cache_manager._get_disk_cache_path("testns", "mykey")
        assert isinstance(path, Path)

    def test_path_in_namespace_dir(self):
        path = cache_manager._get_disk_cache_path("testns", "mykey")
        assert "testns" in str(path)


# ===========================================================================
# PerformanceMonitor Tests
# ===========================================================================
@pytest.fixture(autouse=True)
def clean_perf():
    perf_monitor.clear()
    perf_monitor.set_threshold(500)
    yield
    perf_monitor.clear()


class TestPerformanceMonitorSingleton:
    def test_singleton_same_instance(self):
        pm1 = PerformanceMonitor()
        pm2 = PerformanceMonitor()
        assert pm1 is pm2

    def test_global_is_instance(self):
        assert isinstance(perf_monitor, PerformanceMonitor)


class TestPerformanceMonitorRecord:
    def test_record_adds_event(self):
        perf_monitor.record(PerfEvent.TAB_SWITCH, 100.0)
        stats = perf_monitor.get_stats()
        assert stats["total_events"] == 1

    def test_record_multiple(self):
        perf_monitor.record(PerfEvent.DATA_LOAD, 200.0)
        perf_monitor.record(PerfEvent.DATA_LOAD, 300.0)
        stats = perf_monitor.get_stats()
        assert stats["total_events"] == 2

    def test_record_updates_counts(self):
        perf_monitor.record(PerfEvent.TAB_SWITCH, 50.0)
        perf_monitor.record(PerfEvent.TAB_SWITCH, 60.0)
        stats = perf_monitor.get_stats()
        assert stats["event_counts"]["tab_switch"] == 2

    def test_record_with_metadata(self):
        perf_monitor.record(PerfEvent.QUERY_EXECUTE, 400.0, metadata={"query": "test"})
        events = perf_monitor.get_recent_events(1)
        assert events[0]["metadata"]["query"] == "test"

    def test_record_event_keys(self):
        perf_monitor.record(PerfEvent.CHART_RENDER, 100.0)
        events = perf_monitor.get_recent_events(1)
        event = events[0]
        assert "type" in event
        assert "duration_ms" in event
        assert "timestamp" in event

    def test_max_events_limit(self):
        # Override max to small value
        perf_monitor._max_events = 5
        for i in range(10):
            perf_monitor.record(PerfEvent.TAB_SWITCH, float(i))
        assert len(perf_monitor._events) == 5
        perf_monitor._max_events = 1000  # restore


class TestPerformanceMonitorGetStats:
    def test_empty_stats(self):
        stats = perf_monitor.get_stats()
        assert stats["total_events"] == 0
        assert stats["event_counts"] == {}
        assert stats["event_stats"] == {}

    def test_stats_after_records(self):
        perf_monitor.record(PerfEvent.TAB_SWITCH, 100.0)
        perf_monitor.record(PerfEvent.TAB_SWITCH, 200.0)
        stats = perf_monitor.get_stats()
        ts = stats["event_stats"]["tab_switch"]
        assert ts["count"] == 2
        assert ts["avg_ms"] == 150.0
        assert ts["min_ms"] == 100.0
        assert ts["max_ms"] == 200.0

    def test_stats_p50(self):
        for v in [100.0, 200.0, 300.0]:
            perf_monitor.record(PerfEvent.DATA_LOAD, v)
        stats = perf_monitor.get_stats()
        p50 = stats["event_stats"]["data_load"]["p50_ms"]
        assert p50 == 200.0

    def test_stats_p99_lt_100_uses_last(self):
        for v in [float(i) for i in range(10)]:
            perf_monitor.record(PerfEvent.DATA_LOAD, v)
        stats = perf_monitor.get_stats()
        p99 = stats["event_stats"]["data_load"]["p99_ms"]
        assert p99 == 9.0  # last element when n < 100


class TestPerformanceMonitorGetSlowQueries:
    def test_no_slow_queries_initially(self):
        assert perf_monitor.get_slow_queries() == []

    def test_detects_slow_query(self):
        perf_monitor.record(PerfEvent.QUERY_EXECUTE, 1000.0)  # 1 second > 500ms threshold
        slow = perf_monitor.get_slow_queries()
        assert len(slow) == 1
        assert slow[0]["duration_ms"] == 1000.0

    def test_fast_queries_not_in_slow(self):
        perf_monitor.record(PerfEvent.QUERY_EXECUTE, 100.0)  # fast
        perf_monitor.record(PerfEvent.QUERY_EXECUTE, 200.0)  # fast
        slow = perf_monitor.get_slow_queries()
        assert slow == []

    def test_slow_queries_sorted_descending(self):
        perf_monitor.record(PerfEvent.QUERY_EXECUTE, 600.0)
        perf_monitor.record(PerfEvent.QUERY_EXECUTE, 800.0)
        perf_monitor.record(PerfEvent.QUERY_EXECUTE, 700.0)
        slow = perf_monitor.get_slow_queries()
        assert slow[0]["duration_ms"] == 800.0
        assert slow[1]["duration_ms"] == 700.0

    def test_limit_respected(self):
        for _ in range(5):
            perf_monitor.record(PerfEvent.QUERY_EXECUTE, 1000.0)
        slow = perf_monitor.get_slow_queries(limit=2)
        assert len(slow) == 2

    def test_custom_threshold(self):
        perf_monitor.set_threshold(200)
        perf_monitor.record(PerfEvent.TAB_SWITCH, 300.0)
        slow = perf_monitor.get_slow_queries()
        assert len(slow) == 1


class TestPerformanceMonitorGetRecentEvents:
    def test_empty_initially(self):
        assert perf_monitor.get_recent_events() == []

    def test_returns_last_n(self):
        for i in range(10):
            perf_monitor.record(PerfEvent.TAB_SWITCH, float(i))
        recent = perf_monitor.get_recent_events(limit=3)
        assert len(recent) == 3
        assert recent[-1]["duration_ms"] == 9.0

    def test_returns_all_when_fewer_than_limit(self):
        perf_monitor.record(PerfEvent.TAB_SWITCH, 1.0)
        perf_monitor.record(PerfEvent.TAB_SWITCH, 2.0)
        recent = perf_monitor.get_recent_events(limit=20)
        assert len(recent) == 2


class TestPerformanceMonitorClear:
    def test_clear_removes_all_events(self):
        perf_monitor.record(PerfEvent.TAB_SWITCH, 100.0)
        perf_monitor.clear()
        assert perf_monitor.get_stats()["total_events"] == 0

    def test_clear_resets_counts(self):
        perf_monitor.record(PerfEvent.TAB_SWITCH, 100.0)
        perf_monitor.clear()
        assert perf_monitor.get_stats()["event_counts"] == {}


class TestPerformanceMonitorSetThreshold:
    def test_threshold_changed(self):
        perf_monitor.set_threshold(1000)
        assert perf_monitor._slow_query_threshold_ms == 1000
        perf_monitor.set_threshold(500)

    def test_threshold_affects_slow_detection(self):
        perf_monitor.set_threshold(100)
        perf_monitor.record(PerfEvent.TAB_SWITCH, 150.0)
        slow = perf_monitor.get_slow_queries()
        assert len(slow) == 1
        perf_monitor.set_threshold(500)


# ===========================================================================
# CacheManager 磁盘操作测试（覆盖 lines 134-136, 164-169, 197-198,
# 230-231, 241-252, 256-268, 275-277, 291-301）
# ===========================================================================

class TestCacheManagerDiskOperations:
    """使用 tmp_path 临时目录，避免污染生产 ./cache 目录。"""

    @pytest.fixture(autouse=True)
    def use_temp_disk(self, tmp_path):
        """临时替换 _disk_cache_dir，测试结束后还原。"""
        saved = cache_manager._disk_cache_dir
        tmp_disk = tmp_path / "disk_cache"
        tmp_disk.mkdir(parents=True, exist_ok=True)
        cache_manager._disk_cache_dir = tmp_disk
        yield tmp_disk
        cache_manager._disk_cache_dir = saved
        # 清空内存缓存防止污染其他测试
        cache_manager._memory_cache.clear()
        cache_manager._cache_metadata.clear()

    # ── _generate_cache_key with schema（lines 134-136）─────────────────────

    def test_generate_cache_key_with_schema_adds_hash(self):
        key = cache_manager._generate_cache_key("ns", schema={"fields": ["a", "b"]})
        assert "ns:" in key
        # schema hash fragment 应追加在末尾（8字符十六进制）
        assert len(key) > 5

    def test_generate_cache_key_schema_deterministic(self):
        k1 = cache_manager._generate_cache_key("ns", schema={"a": 1})
        k2 = cache_manager._generate_cache_key("ns", schema={"a": 1})
        assert k1 == k2

    # ── _save_to_disk / _load_from_disk（lines 241-268）─────────────────────

    def test_save_and_load_dataframe(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        cache_manager._save_to_disk("testns", "dfkey", df)
        loaded = cache_manager._load_from_disk("testns", "dfkey")
        assert loaded is not None
        pd.testing.assert_frame_equal(loaded.reset_index(drop=True), df)

    def test_save_non_dataframe_uses_pickle(self):
        """非 DataFrame 对象以 pickle 写入 .parquet 文件（lines 249-252）。"""
        obj = {"x": 42, "y": "hello"}
        # 保存时不抛出
        cache_manager._save_to_disk("testns", "objkey", obj)
        path = cache_manager._get_disk_cache_path("testns", "objkey")
        assert path.exists()

    def test_load_from_disk_missing_returns_none(self):
        """不存在的 key → 返回 None（line 260）"""
        result = cache_manager._load_from_disk("testns", "nonexistent_key_xyz")
        assert result is None

    # ── _delete_from_disk（lines 275-277）───────────────────────────────────

    def test_delete_existing_file(self):
        df = pd.DataFrame({"val": [1]})
        cache_manager._save_to_disk("testns", "delkey", df)
        path = cache_manager._get_disk_cache_path("testns", "delkey")
        assert path.exists()
        cache_manager._delete_from_disk("testns", "delkey")
        assert not path.exists()

    def test_delete_nonexistent_file_no_error(self):
        cache_manager._delete_from_disk("testns", "no_such_key")  # must not raise

    # ── get() / set() with DISK level（lines 164-169, 197-198）─────────────

    def test_get_disk_level_hit(self):
        df = pd.DataFrame({"val": [10, 20]})
        cache_manager._save_to_disk("testns", "dkkey", df)
        result = cache_manager.get("testns", "dkkey", level=CacheLevel.DISK)
        assert result is not None
        pd.testing.assert_frame_equal(result.reset_index(drop=True), df)

    def test_get_disk_level_miss(self):
        result = cache_manager.get("testns", "no_such_key_disk", level=CacheLevel.DISK)
        assert result is None

    def test_set_disk_level_writes_to_disk(self):
        df = pd.DataFrame({"val": [7, 8]})
        cache_manager.set("testns", "setdk", df, level=CacheLevel.DISK)
        path = cache_manager._get_disk_cache_path("testns", "setdk")
        assert path.exists()

    def test_get_both_level_loads_to_memory(self):
        df = pd.DataFrame({"val": [1, 2]})
        cache_manager._save_to_disk("testns", "bothkey", df)
        result = cache_manager.get("testns", "bothkey", level=CacheLevel.BOTH)
        assert result is not None
        # 加载后应同步写入内存
        mem_val = cache_manager._memory_cache.get("testns:bothkey")
        assert mem_val is not None

    def test_set_both_level_saves_to_memory_and_disk(self):
        df = pd.DataFrame({"val": [5]})
        cache_manager.set("testns", "setboth", df, level=CacheLevel.BOTH)
        assert cache_manager._memory_cache.get("testns:setboth") is not None
        path = cache_manager._get_disk_cache_path("testns", "setboth")
        assert path.exists()

    # ── invalidate() calls _delete_from_disk（lines 230-231）──────────────

    def test_invalidate_specific_key_deletes_disk_file(self):
        df = pd.DataFrame({"val": [1]})
        cache_manager.set("testns", "inv_key", df, level=CacheLevel.DISK)
        path = cache_manager._get_disk_cache_path("testns", "inv_key")
        assert path.exists()
        cache_manager.invalidate("testns", "inv_key")
        assert not path.exists()

    # ── clear_all()（lines 291-301）─────────────────────────────────────────

    def test_clear_all_empties_memory_and_disk(self):
        cache_manager.set("testns", "k1", "v1", level=CacheLevel.MEMORY)
        df = pd.DataFrame({"val": [1]})
        cache_manager.set("testns", "k2", df, level=CacheLevel.DISK)
        cache_manager.clear_all()
        assert len(cache_manager._memory_cache) == 0
        assert len(cache_manager._cache_metadata) == 0
        assert cache_manager._disk_cache_dir.exists()  # 目录应被重建

    def test_clear_all_dir_recreated(self):
        """clear_all 先 rmtree 再 mkdir — 目录需存在。"""
        cache_manager.clear_all()
        assert cache_manager._disk_cache_dir.exists()

    # ── 异常处理路径（lines 251-252, 264-268, 276-277）──────────────────────

    def test_save_to_disk_exception_swallowed(self):
        """让 mkdir 抛出 → _save_to_disk except 块 → 不应 raise (lines 251-252)"""
        from pathlib import Path
        from unittest.mock import patch
        with patch.object(Path, "mkdir", side_effect=PermissionError("no perms")):
            cache_manager._save_to_disk("testns", "errkey", pd.DataFrame({"a": [1]}))

    def test_load_from_disk_corrupt_file_returns_none(self):
        """写入损坏的 parquet 文件 → pd.read_parquet 失败 → except → return None (lines 266-268)"""
        path = cache_manager._get_disk_cache_path("testns", "badkey")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"this is definitely not a parquet file")
        result = cache_manager._load_from_disk("testns", "badkey")
        assert result is None

    def test_delete_from_disk_exception_swallowed(self):
        """让 path.unlink 抛出 → _delete_from_disk except 块 → 不应 raise (lines 276-277)"""
        from pathlib import Path
        from unittest.mock import patch
        df = pd.DataFrame({"val": [1]})
        cache_manager._save_to_disk("testns", "delexkey", df)
        with patch.object(Path, "unlink", side_effect=PermissionError("no delete")):
            cache_manager._delete_from_disk("testns", "delexkey")


class TestPerformanceMonitorGetSummaryText:
    def test_no_data_returns_empty_placeholder(self):
        text = perf_monitor.get_summary_text()
        assert "暂无数据" in text

    def test_with_data_includes_event(self):
        perf_monitor.record(PerfEvent.TAB_SWITCH, 100.0)
        text = perf_monitor.get_summary_text()
        assert "tab_switch" in text

    def test_returns_string(self):
        assert isinstance(perf_monitor.get_summary_text(), str)


class TestPerformanceMonitorTimers:
    def test_start_stop_returns_duration(self):
        perf_monitor.start_timer("t1")
        time.sleep(0.01)
        dur = perf_monitor.stop_timer("t1")
        assert dur >= 10.0  # at least 10ms

    def test_stop_missing_timer_returns_zero(self):
        assert perf_monitor.stop_timer("missing") == 0

    def test_stop_with_event_type_records(self):
        perf_monitor.start_timer("t2")
        perf_monitor.stop_timer("t2", event_type=PerfEvent.DATA_LOAD)
        stats = perf_monitor.get_stats()
        assert stats["event_counts"].get("data_load", 0) == 1

    def test_timer_removed_after_stop(self):
        perf_monitor.start_timer("t3")
        perf_monitor.stop_timer("t3")
        assert "t3" not in perf_monitor._timers


# ===========================================================================
# PerfTimer Context Manager Tests
# ===========================================================================
class TestPerfTimer:
    def test_context_manager_runs(self):
        perf_monitor.clear()
        with PerfTimer("test_timer", event_type=PerfEvent.TAB_SWITCH):
            time.sleep(0.01)
        stats = perf_monitor.get_stats()
        assert stats["event_counts"].get("tab_switch", 0) == 1

    def test_context_manager_records_duration(self):
        perf_monitor.clear()
        with PerfTimer("test_timer2", event_type=PerfEvent.CHART_RENDER):
            time.sleep(0.02)
        recent = perf_monitor.get_recent_events(1)
        assert recent[0]["duration_ms"] >= 20.0

    def test_no_event_type_no_record(self):
        perf_monitor.clear()
        with PerfTimer("no_event"):
            pass
        stats = perf_monitor.get_stats()
        assert stats["total_events"] == 0


# ===========================================================================
# log_performance Decorator Tests  (lines 214-221)
# ===========================================================================
class TestLogPerformanceDecorator:
    def test_decorated_function_returns_result(self):
        @log_performance
        def add(a, b):
            return a + b

        result = add(3, 4)
        assert result == 7

    def test_decorated_function_records_event(self):
        perf_monitor.clear()

        @log_performance
        def my_func(x):
            return x * 2

        my_func(5)
        stats = perf_monitor.get_stats()
        assert stats["event_counts"].get("query_execute", 0) >= 1

    def test_decorator_metadata_contains_function_name(self):
        perf_monitor.clear()

        @log_performance
        def named_func():
            return None

        named_func()
        events = perf_monitor.get_recent_events(1)
        assert events[0]["metadata"]["function"] == "named_func"
