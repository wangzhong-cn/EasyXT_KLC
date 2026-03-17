import os
from unittest.mock import MagicMock

from PyQt5.QtCore import QSettings

from gui_app.widgets.realtime_settings_dialog import RealtimeSettingsDialog


def test_get_settings_contains_advanced_fields(qapp):
    dialog = RealtimeSettingsDialog(parent=None, pipeline_manager=MagicMock())
    try:
        settings = dialog.get_settings()
        assert "ws_reconnect_initial" in settings
        assert "ws_reconnect_max" in settings
        assert "ws_reconnect_factor" in settings
        assert "tdx_error_log_cooldown" in settings
    finally:
        dialog.close()


def test_accept_persists_advanced_fields_and_env(qapp):
    qsettings = QSettings("EasyXT", "KLineChartWorkspace")
    qsettings.remove("realtime/ws_reconnect_initial")
    qsettings.remove("realtime/ws_reconnect_max")
    qsettings.remove("realtime/ws_reconnect_factor")
    qsettings.remove("realtime/tdx_error_log_cooldown")
    pipeline = MagicMock()
    dialog = RealtimeSettingsDialog(parent=None, pipeline_manager=pipeline)
    try:
        dialog.ws_reconnect_initial_spin.setValue(2.0)
        dialog.ws_reconnect_max_spin.setValue(18.0)
        dialog.ws_reconnect_factor_spin.setValue(2.2)
        dialog.tdx_error_log_cooldown_spin.setValue(22.0)
        dialog.accept()
        assert float(qsettings.value("realtime/ws_reconnect_initial", type=float)) == 2.0
        assert float(qsettings.value("realtime/ws_reconnect_max", type=float)) == 18.0
        assert float(qsettings.value("realtime/ws_reconnect_factor", type=float)) == 2.2
        assert float(qsettings.value("realtime/tdx_error_log_cooldown", type=float)) == 22.0
        assert float(os.environ["EASYXT_WS_RECONNECT_INITIAL"]) == 2.0
        assert float(os.environ["EASYXT_WS_RECONNECT_MAX"]) == 18.0
        assert float(os.environ["EASYXT_WS_RECONNECT_FACTOR"]) == 2.2
        assert float(os.environ["EASYXT_TDX_ERROR_LOG_COOLDOWN"]) == 22.0
        assert pipeline.update_config.called
    finally:
        dialog.close()


def test_source_meta_mapping():
    gui = RealtimeSettingsDialog._source_meta("gui")
    env = RealtimeSettingsDialog._source_meta("env")
    unknown = RealtimeSettingsDialog._source_meta("x")
    assert "GUI" in gui[0]
    assert "环境变量" in env[0]
    assert "默认值" in unknown[0]


def test_only_non_default_filter_hides_default_rows(qapp):
    dialog = RealtimeSettingsDialog(parent=None, pipeline_manager=MagicMock())
    try:
        dialog.config_sources["drop_threshold"] = "default"
        dialog.config_sources["max_queue"] = "gui"
        dialog.only_non_default_check.setChecked(True)
        row_map = {k: (left, right) for k, left, right in dialog._source_rows}
        drop_left, drop_right = row_map["drop_threshold"]
        queue_left, queue_right = row_map["max_queue"]
        assert drop_left.isHidden()
        assert drop_right.isHidden()
        assert not queue_left.isHidden()
        assert not queue_right.isHidden()
    finally:
        dialog.close()


def test_only_non_default_filter_state_persisted(qapp):
    qsettings = QSettings("EasyXT", "KLineChartWorkspace")
    qsettings.setValue("realtime/ui/only_non_default_source", True)
    dialog = RealtimeSettingsDialog(parent=None, pipeline_manager=MagicMock())
    try:
        assert dialog.only_non_default_check.isChecked()
        dialog.only_non_default_check.setChecked(False)
        dialog.accept()
        assert not bool(qsettings.value("realtime/ui/only_non_default_source", type=bool))
    finally:
        dialog.close()
