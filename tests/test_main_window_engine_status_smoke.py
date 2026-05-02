import os
import sys
import gc
from unittest.mock import MagicMock

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


def test_close_event_stops_running_child_threads(qapp, monkeypatch):
    monkeypatch.setattr(MainWindow, "init_ui", lambda self: None)
    monkeypatch.setattr(MainWindow, "_schedule_health_recheck", lambda self: None)
    window = MainWindow()
    try:
        check_thread = MagicMock()
        check_thread.isRunning.return_value = True
        check_thread.wait.return_value = False
        window._check_thread = check_thread
        child_running = MagicMock()
        child_running.isRunning.return_value = True
        child_running.wait.return_value = False
        child_stopped = MagicMock()
        child_stopped.isRunning.return_value = False
        monkeypatch.setattr(window, "findChildren", lambda cls: [child_running, child_stopped])
        stop_spy = MagicMock()
        monkeypatch.setattr(window, "stop_all_services", stop_spy)
        evt = MagicMock()
        window.closeEvent(evt)
        assert check_thread.requestInterruption.called
        assert check_thread.quit.called
        assert check_thread.terminate.called
        assert child_running.requestInterruption.called
        assert child_running.quit.called
        assert child_running.terminate.called
        assert stop_spy.called
        assert evt.accept.called
    finally:
        _drain_qthreads()
        window.close()
        _drain_qthreads()


def test_realtime_pipeline_log_is_not_mirrored_to_stdout_by_default(capsys, monkeypatch):
    monkeypatch.setenv("EASYXT_REALTIME_PIPELINE_STDOUT", "0")
    win = MainWindow.__new__(MainWindow)
    win._logger = MagicMock()
    win._realtime_pipeline_status = {
        "connected": False,
        "reason": "no_quote_data",
        "quote_ts": None,
        "symbol": "000001.SZ",
        "degraded": False,
    }
    win._last_realtime_probe_log = None

    win._log_realtime_pipeline_status()

    assert win._last_realtime_probe_log is not None
    assert "[REALTIME_PIPELINE]" in win._last_realtime_probe_log
    assert capsys.readouterr().out == ""


def test_service_output_logs_without_stdout_mirroring_by_default(capsys, monkeypatch):
    monkeypatch.setenv("EASYXT_SERVICE_OUTPUT_STDOUT", "0")
    win = MainWindow.__new__(MainWindow)
    win._logger = MagicMock()
    win._service_log_last_ts = 0.0
    win._service_log_suppressed = 0
    win._service_log_suppressed_total = 0
    win._service_lock_conflict = False
    win._service_external_manager = False
    win._service_circuit_broken = False
    win._service_restart_scheduled = False
    win.service_process = MagicMock()
    win.service_process.readAllStandardOutput.return_value = b"hello from service\n"
    win.update_service_status = MagicMock()

    win.on_service_output()

    assert capsys.readouterr().out == ""
    assert win._logger.debug.called
