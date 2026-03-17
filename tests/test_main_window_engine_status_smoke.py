import os
import sys
import gc

import pytest
from PyQt5.QtCore import QThread
from PyQt5.QtTest import QTest

pytestmark = pytest.mark.gui

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from gui_app.main_window import MainWindow


def _drain_qthreads(timeout_ms: int = 1200) -> None:
    current = QThread.currentThread()
    for obj in gc.get_objects():
        if not isinstance(obj, QThread):
            continue
        if obj is current:
            continue
        try:
            if obj.isRunning():
                obj.quit()
                obj.wait(timeout_ms)
                if obj.isRunning():
                    obj.terminate()
                    obj.wait(timeout_ms)
        except Exception:
            continue


def test_main_window_emits_backtest_engine_log_within_timeout(qapp, capsys, monkeypatch):
    monkeypatch.setattr(MainWindow, "init_ui", lambda self: None)
    monkeypatch.setattr(MainWindow, "_schedule_health_recheck", lambda self: None)
    window = MainWindow()
    # 用轮询代替固定等待，最多 6s，系统高负载下仍可通过
    deadline_ms = 6000
    poll_ms = 100
    elapsed = 0
    try:
        while elapsed < deadline_ms:
            QTest.qWait(poll_ms)
            elapsed += poll_ms
            if window._last_backtest_engine_log is not None:
                break
        assert window._last_backtest_engine_log is not None, (
            f"等待 {deadline_ms}ms 后 _last_backtest_engine_log 仍为 None"
        )
        assert "[BACKTEST_ENGINE]" in window._last_backtest_engine_log
        output = capsys.readouterr().out
        assert "[BACKTEST_ENGINE]" in output
    finally:
        _drain_qthreads()
        window.close()
        _drain_qthreads()


@pytest.mark.slow
def test_main_window_emits_backtest_engine_log_integration(qapp, capsys):
    window = MainWindow()
    timeout_ms = 3500
    try:
        window._closing = True
        QTest.qWait(timeout_ms)
        assert window._last_backtest_engine_log is not None
        assert "[BACKTEST_ENGINE]" in window._last_backtest_engine_log
        output = capsys.readouterr().out
        assert "[BACKTEST_ENGINE]" in output
    finally:
        if hasattr(window, "connection_check_timer"):
            window.connection_check_timer.stop()
        _drain_qthreads()
        window.close()
        _drain_qthreads()
