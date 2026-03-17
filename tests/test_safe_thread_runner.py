"""
tests/test_safe_thread_runner.py

ThreadLifecycleMixin 行为覆盖测试。
核心原则：不依赖真实 QThread，全部通过 MagicMock 模拟线程对象。
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call
import pytest

from core.safe_thread_runner import ThreadLifecycleMixin


# ─────────────────────────────────────────────────────────────────────────────
# 辅助：最小化 Mixin 宿主类（无 QWidget 依赖）
# ─────────────────────────────────────────────────────────────────────────────

class _FakeWidget(ThreadLifecycleMixin):
    """只继承 Mixin，不继承 QWidget，用于单元测试。"""

    def __init__(self):
        super().__init__()


# ─────────────────────────────────────────────────────────────────────────────
# 注册接口测试
# ─────────────────────────────────────────────────────────────────────────────

class TestRegistration:
    def test_init_empty_lists(self):
        w = _FakeWidget()
        assert w._network_thread_attrs == []
        assert w._regular_thread_attrs == []

    def test_register_network_thread_default_wait(self):
        w = _FakeWidget()
        w._register_network_thread("_conn")
        assert w._network_thread_attrs == [("_conn", 1000)]

    def test_register_network_thread_custom_wait(self):
        w = _FakeWidget()
        w._register_network_thread("_conn", wait_ms=2000)
        assert w._network_thread_attrs == [("_conn", 2000)]

    def test_register_multiple_network_threads(self):
        w = _FakeWidget()
        w._register_network_thread("_a", 500)
        w._register_network_thread("_b", 1500)
        assert len(w._network_thread_attrs) == 2
        assert ("_a", 500) in w._network_thread_attrs
        assert ("_b", 1500) in w._network_thread_attrs

    def test_register_regular_thread(self):
        w = _FakeWidget()
        w._register_thread("_worker")
        assert "_worker" in w._regular_thread_attrs

    def test_register_multiple_regular_threads(self):
        w = _FakeWidget()
        w._register_thread("_t1")
        w._register_thread("_t2")
        assert w._regular_thread_attrs == ["_t1", "_t2"]


# ─────────────────────────────────────────────────────────────────────────────
# _stop_all_threads — 线程不存在时（属性为 None）
# ─────────────────────────────────────────────────────────────────────────────

class TestStopAllThreads:

    def _make_mock_qthread(self, is_running: bool = True, wait_result: bool = True):
        t = MagicMock()
        t.isRunning.return_value = is_running
        t.wait.return_value = wait_result
        return t

    # ── 网络型：属性不存在 / 为 None → 静默跳过 ─────────────────────────────

    def test_network_thread_attr_missing_skips(self):
        """注册的属性在实例上不存在 → getattr 返回 None → 静默跳过。"""
        w = _FakeWidget()
        w._register_network_thread("_missing_thread")
        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock
        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()  # should not raise

    def test_network_thread_none_skips(self):
        """属性为 None → isRunning() 不会被调用，静默跳过。"""
        w = _FakeWidget()
        w._missing = None
        w._register_network_thread("_missing")
        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock
        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()  # None → continue，不抛出

    # ── 网络型：线程未运行 → 跳过 ────────────────────────────────────────────

    def test_network_thread_not_running_skips(self):
        w = _FakeWidget()
        mock_qthread_cls = MagicMock()
        t = self._make_mock_qthread(is_running=False)
        w._conn = t
        w._register_network_thread("_conn", wait_ms=500)

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = mock_qthread_cls

        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()

        t.requestInterruption.assert_not_called()
        t.quit.assert_not_called()

    # ── 网络型：线程在 wait 内正常退出 ────────────────────────────────────────

    def test_network_thread_exits_in_time(self):
        w = _FakeWidget()
        t = self._make_mock_qthread(is_running=True, wait_result=True)
        w._conn = t
        w._register_network_thread("_conn", wait_ms=1000)

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock

        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()

        t.requestInterruption.assert_called_once()
        t.quit.assert_called_once()
        t.wait.assert_called_once_with(1000)
        t.terminate.assert_not_called()

    # ── 网络型：超时 → terminate + wait(500) ─────────────────────────────────

    def test_network_thread_timeout_terminates(self):
        w = _FakeWidget()
        t = self._make_mock_qthread(is_running=True, wait_result=False)
        w._conn = t
        w._register_network_thread("_conn", wait_ms=1000)

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock

        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore,
                                         "core.events": MagicMock(), "core.signal_bus": MagicMock()}):
            w._stop_all_threads()

        t.terminate.assert_called_once()
        t.wait.assert_any_call(500)

    # ── 普通型：属性为 None → 不加入 running 列表 ────────────────────────────

    def test_regular_thread_none_skips(self):
        w = _FakeWidget()
        w._worker = None
        w._register_thread("_worker")

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock

        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()  # should not raise

    # ── 普通型：线程运行中 → 批量 requestInterruption + quit，再 wait(200) ──

    def test_regular_threads_batch_quit_and_wait(self):
        w = _FakeWidget()
        t1 = self._make_mock_qthread(is_running=True, wait_result=True)
        t2 = self._make_mock_qthread(is_running=True, wait_result=True)
        w._t1 = t1
        w._t2 = t2
        w._register_thread("_t1")
        w._register_thread("_t2")

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock

        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()

        t1.requestInterruption.assert_called_once()
        t1.quit.assert_called_once()
        t1.wait.assert_called_once_with(200)
        t2.requestInterruption.assert_called_once()
        t2.quit.assert_called_once()
        t2.wait.assert_called_once_with(200)

    # ── 普通型：未运行 → 不加入 wait 列表 ──────────────────────────────────

    def test_regular_thread_not_running_no_wait(self):
        w = _FakeWidget()
        t = self._make_mock_qthread(is_running=False)
        w._idle = t
        w._register_thread("_idle")

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock

        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()

        t.requestInterruption.assert_not_called()
        t.wait.assert_not_called()

    # ── 混合：网络 + 普通同时存在 ────────────────────────────────────────────

    def test_mixed_network_and_regular_threads(self):
        w = _FakeWidget()
        net = self._make_mock_qthread(is_running=True, wait_result=True)
        reg = self._make_mock_qthread(is_running=True, wait_result=True)
        w._net = net
        w._reg = reg
        w._register_network_thread("_net", wait_ms=800)
        w._register_thread("_reg")

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock

        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()

        net.wait.assert_called_once_with(800)
        reg.wait.assert_called_once_with(200)

    # ── 异常分支（覆盖 lines 116-119, 131-132, 137-138） ─────────────────────

    def test_network_thread_signal_emit_exception_is_swallowed(self):
        """signal_bus.emit() 抛出 → inner except: pass (lines 116-117)"""
        w = _FakeWidget()
        t = self._make_mock_qthread(is_running=True, wait_result=False)
        w._conn = t
        w._register_network_thread("_conn", wait_ms=1000)

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock
        fake_signal_bus_mod = MagicMock()
        fake_signal_bus_mod.signal_bus.emit.side_effect = RuntimeError("bus down")

        with patch.dict("sys.modules", {
            "PyQt5": MagicMock(),
            "PyQt5.QtCore": fake_qtcore,
            "core.events": MagicMock(),
            "core.signal_bus": fake_signal_bus_mod,
        }):
            w._stop_all_threads()  # must not raise

        t.terminate.assert_called_once()

    def test_network_thread_outer_exception_is_logged(self):
        """requestInterruption() 抛出 → outer except: logger.exception (lines 118-119)"""
        w = _FakeWidget()
        t = self._make_mock_qthread(is_running=True)
        t.requestInterruption.side_effect = RuntimeError("interrupt failed")
        w._conn = t
        w._register_network_thread("_conn", wait_ms=1000)

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock
        w._logger = MagicMock()

        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()  # must not raise

        w._logger.exception.assert_called()

    def test_regular_thread_interrupt_exception_is_logged(self):
        """requestInterruption() 在普通线程循环中抛出 → except: logger.exception (lines 131-132)"""
        w = _FakeWidget()
        t = self._make_mock_qthread(is_running=True)
        t.requestInterruption.side_effect = RuntimeError("interrupt failed")
        w._worker = t
        w._register_thread("_worker")

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock
        w._logger = MagicMock()

        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()  # must not raise

        w._logger.exception.assert_called()

    def test_regular_thread_wait_exception_is_swallowed(self):
        """t.wait(200) 抛出 → except: pass (lines 137-138)"""
        w = _FakeWidget()
        t = self._make_mock_qthread(is_running=True)
        t.wait.side_effect = RuntimeError("wait failed")
        w._worker = t
        w._register_thread("_worker")

        fake_qtcore = MagicMock()
        fake_qtcore.QThread = MagicMock

        with patch.dict("sys.modules", {"PyQt5": MagicMock(), "PyQt5.QtCore": fake_qtcore}):
            w._stop_all_threads()  # must not raise
