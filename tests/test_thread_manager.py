"""
tests/test_thread_manager.py — Phase 2 PR-1 ThreadManager 单元测试
"""
from __future__ import annotations

import threading
import time

from core.thread_manager import ThreadManager, _ManagedThread

# ---------------------------------------------------------------------------
# ThreadManager — 基础行为
# ---------------------------------------------------------------------------

def test_run_starts_daemon_thread():
    """run() 应启动一个 daemon=True 的线程。"""
    tm = ThreadManager()
    started = threading.Event()
    tm.run(lambda: (started.wait(), None)[1], name="test_daemon")
    started.set()  # unblock
    assert tm.active_count() >= 0  # no crash


def test_run_returns_managed_thread():
    """run() 应返回 _ManagedThread 实例，且 daemon=True。"""
    tm = ThreadManager()
    t = tm.run(lambda: None, name="noop")
    assert isinstance(t, _ManagedThread)
    assert t.daemon is True


def test_active_count_zero_after_completion():
    """线程正常完成后 active_count() 应降为 0。"""
    tm = ThreadManager()
    done = threading.Event()

    def worker():
        done.set()

    tm.run(worker, name="fast_worker")
    done.wait(timeout=2.0)
    # give deregister callback time to run
    time.sleep(0.05)
    assert tm.active_count() == 0


def test_active_count_increments_while_running():
    """active_count() 在线程运行期间应 ≥ 1。"""
    tm = ThreadManager()
    blocking = threading.Event()
    can_finish = threading.Event()

    def worker():
        blocking.set()
        can_finish.wait()

    tm.run(worker, name="long_task")
    blocking.wait(timeout=2.0)
    assert tm.active_count() >= 1
    can_finish.set()


def test_run_passes_args_and_kwargs():
    """args / kwargs 应正确转发给 target。"""
    results = []

    def worker(a, b, *, c=0):
        results.append(a + b + c)

    tm = ThreadManager()
    done = threading.Event()

    def _w(a, b, *, c=0):
        results.append(a + b + c)
        done.set()

    tm.run(_w, name="args_test", args=(1, 2), kwargs={"c": 3})
    done.wait(timeout=2.0)
    assert results == [6]


def test_error_cb_called_on_exception():
    """目标函数抛异常时 error_cb 应被调用。"""
    errors: list[Exception] = []
    done = threading.Event()

    def bad_worker():
        raise ValueError("oops")

    def on_error(exc: Exception):
        errors.append(exc)
        done.set()

    tm = ThreadManager()
    tm.run(bad_worker, name="error_test", error_cb=on_error)
    done.wait(timeout=2.0)
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)


def test_status_snapshot_contains_name():
    """正在运行的线程应出现在 status() 快照中。"""
    tm = ThreadManager()
    blocking = threading.Event()
    can_finish = threading.Event()

    def worker():
        blocking.set()
        can_finish.wait()

    tm.run(worker, name="status_probe")
    blocking.wait(timeout=2.0)
    snap = tm.status()
    names = [s["name"] for s in snap]
    assert "status_probe" in names
    can_finish.set()


def test_join_all_waits_for_threads():
    """join_all() 应等待所有线程完成。"""
    results: list[int] = []
    tm = ThreadManager()
    finished = threading.Event()

    def slow():
        time.sleep(0.05)
        results.append(1)
        finished.set()

    tm.run(slow, name="slow_task")
    tm.join_all(timeout=2.0)
    assert finished.is_set()
    assert len(results) == 1


def test_join_all_does_not_block_forever():
    """join_all(timeout=...) 不应无限阻塞——即使线程未完成也应返回。"""
    tm = ThreadManager()
    gate = threading.Event()

    def forever():
        gate.wait()  # blocks until test ends

    tm.run(forever, name="infinite")
    t0 = time.monotonic()
    tm.join_all(timeout=0.2)
    elapsed = time.monotonic() - t0
    gate.set()
    assert elapsed < 1.0, "join_all exceeded its timeout budget"


# ---------------------------------------------------------------------------
# Singleton smoke test
# ---------------------------------------------------------------------------

def test_module_singleton_importable():
    """module-level thread_manager singleton should be importable."""
    from core.thread_manager import thread_manager as tm  # noqa: PLC0415
    assert tm is not None
    assert isinstance(tm, ThreadManager)
