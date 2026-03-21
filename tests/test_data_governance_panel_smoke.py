"""
tests/test_data_governance_panel_smoke.py
==========================================

DataGovernancePanel + DataManagerController 无头 smoke 测试。
使用 offscreen Qt 平台（QT_QPA_PLATFORM=offscreen），不需要真实显示器。
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


# ─── DataManagerController（纯 Python，无需 qapp） ────────────────────────


class TestDataManagerControllerSmoke:
    """在没有任何外部服务的情况下，Controller 所有方法都应可调用且不抛出异常。"""

    def setup_method(self):
        os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
        from gui_app.data_manager_controller import DataManagerController
        self.ctrl = DataManagerController(
            pipeline_health=None,
            integrity_checker=None,
            datasource_registry=None,
        )

    def test_import_succeeds(self):
        from gui_app.data_manager_controller import DataManagerController  # noqa
        assert DataManagerController is not None

    def test_get_pipeline_status_no_crash(self):
        with patch.object(self.ctrl, "_get_pipeline_health", return_value=None):
            result = self.ctrl.get_pipeline_status()
        assert isinstance(result, dict)
        assert "overall_healthy" in result

    def test_run_integrity_check_no_crash(self):
        with patch.object(self.ctrl, "_get_integrity_checker", return_value=None):
            result = self.ctrl.run_integrity_check("000001.SZ", "2023-01-01", "2023-12-31")
        assert isinstance(result, dict)
        assert "passed" in result

    def test_run_batch_integrity_check_empty_list(self):
        with patch.object(self.ctrl, "_get_integrity_checker", return_value=None):
            result = self.ctrl.run_batch_integrity_check([], "2023-01-01", "2023-12-31")
        assert isinstance(result, dict)
        assert result["total"] == 0

    def test_get_routing_metrics_no_crash(self):
        with patch.object(self.ctrl, "_get_datasource_registry", return_value=None):
            result = self.ctrl.get_routing_metrics()
        assert isinstance(result, dict)
        assert "sources" in result

    def test_validate_environment_returns_structure(self):
        result = self.ctrl.validate_environment()
        assert "valid" in result
        assert "items" in result
        # All required keys present
        keys = {i["key"] for i in result["items"]}
        assert "EASYXT_DUCKDB_PATH" in keys

    def test_get_duckdb_summary_no_crash_on_missing_pool(self):
        from gui_app.data_manager_controller import _safe_import as orig_safe
        with patch("gui_app.data_manager_controller._safe_import", return_value=None):
            result = self.ctrl.get_duckdb_summary()
        assert isinstance(result, dict)
        assert result["healthy"] is False

    def test_resolve_duckdb_path_static(self):
        from gui_app.data_manager_controller import DataManagerController
        with patch("gui_app.data_manager_controller._safe_import", return_value=None):
            path = DataManagerController._resolve_duckdb_path()
        assert isinstance(path, str)
        assert len(path) > 0


# ─── DataGovernancePanel Qt smoke（需要 qapp fixture） ────────────────────


class TestDataGovernancePanelSmoke:
    """验证面板可在 offscreen 环境正常实例化，5 个 Tab 都存在。"""

    def test_import_succeeds(self):
        from gui_app.widgets.data_governance_panel import DataGovernancePanel  # noqa
        assert DataGovernancePanel is not None

    def test_import_subtabs_classes(self):
        from gui_app.widgets.data_governance_panel import (  # noqa
            _ControllerThread,
            _IntegrityTab,
            _PipelineTab,
            _RoutingTab,
        )

    def test_panel_instantiates(self, qapp):
        """面板可在 offscreen Qt 平台下无错误实例化。"""
        from gui_app.data_manager_controller import DataManagerController
        from gui_app.widgets.data_governance_panel import DataGovernancePanel

        ctrl = DataManagerController(
            pipeline_health=MagicMock(**{"report.return_value": {"overall_healthy": True, "timestamp": "", "checks": {}}}),
            integrity_checker=None,
            datasource_registry=None,
        )
        panel = DataGovernancePanel(controller=ctrl)
        try:
            assert panel._tabs.count() == 12
        finally:
            panel.close()

    def test_tab_labels(self, qapp):
        from gui_app.data_manager_controller import DataManagerController
        from gui_app.widgets.data_governance_panel import DataGovernancePanel

        ctrl = DataManagerController()
        panel = DataGovernancePanel(controller=ctrl)
        try:
            labels = [panel._tabs.tabText(i) for i in range(panel._tabs.count())]
            assert "数据下载" in labels
            assert "数据质检" in labels
            assert "数据路由" in labels
            assert "管道状态" in labels
            assert "数据查询" in labels
            assert "数据对账" in labels
            assert "交易日历" in labels
            assert "数据修复" in labels
            assert "数据库维护" in labels
            assert "环境配置" in labels
        finally:
            panel.close()

    def test_integrity_tab_run_button_exists(self, qapp):
        from gui_app.data_manager_controller import DataManagerController
        from gui_app.widgets.data_governance_panel import DataGovernancePanel

        ctrl = DataManagerController()
        panel = DataGovernancePanel(controller=ctrl)
        try:
            integrity_tab = panel._tabs.widget(1)
            assert hasattr(integrity_tab, "_run_btn")
        finally:
            panel.close()

    def test_pipeline_tab_refresh_btn_exists(self, qapp):
        from gui_app.data_manager_controller import DataManagerController
        from gui_app.widgets.data_governance_panel import DataGovernancePanel

        ctrl = DataManagerController()
        panel = DataGovernancePanel(controller=ctrl)
        try:
            assert hasattr(panel._pipeline_tab, "_refresh_btn")
        finally:
            panel.close()

    def test_routing_tab_refresh_btn_exists(self, qapp):
        from gui_app.data_manager_controller import DataManagerController
        from gui_app.widgets.data_governance_panel import DataGovernancePanel

        ctrl = DataManagerController()
        panel = DataGovernancePanel(controller=ctrl)
        try:
            routing_tab = panel._tabs.widget(2)
            assert hasattr(routing_tab, "_refresh_btn")
        finally:
            panel.close()


# ─── _ControllerThread 单元测试（不需要 qapp） ──────────────────────────-


class TestControllerThread:
    """验证后台线程正确地 emit result_ready / error_occurred。"""

    def test_result_emitted_on_success(self, qapp):
        from PyQt5.QtWidgets import QApplication

        from gui_app.widgets.data_governance_panel import _ControllerThread

        received = []
        thread = _ControllerThread(lambda: {"ok": True})
        thread.result_ready.connect(received.append)
        thread.start()
        thread.wait(3000)
        # Process pending cross-thread signal deliveries
        QApplication.processEvents()
        assert received == [{"ok": True}]

    def test_error_emitted_on_exception(self, qapp):
        from PyQt5.QtWidgets import QApplication

        from gui_app.widgets.data_governance_panel import _ControllerThread

        errors = []

        def boom():
            raise ValueError("boom")

        thread = _ControllerThread(boom)
        thread.error_occurred.connect(errors.append)
        thread.start()
        thread.wait(3000)
        QApplication.processEvents()
        assert errors and "boom" in errors[0]


# ─── Tab 回调方法覆盖测试 ────────────────────────────────────────────────


class TestIntegrityTabCallbacks:
    """直接调用 _IntegrityTab 的 _on_result / _on_error 方法覆盖关键分支。"""

    def _make_tab(self, qapp):
        from gui_app.data_manager_controller import DataManagerController
        from gui_app.widgets.data_governance_panel import _IntegrityTab

        ctrl = DataManagerController()
        return _IntegrityTab(controller=ctrl)

    def test_on_result_all_pass(self, qapp):
        tab = self._make_tab(qapp)
        result = {
            "total": 2, "passed": 2,
            "reports": {
                "000001.SZ": {"has_errors": False, "errors": [], "warnings": [], "elapsed_ms": 12},
                "600000.SH": {"has_errors": False, "errors": [], "warnings": [], "elapsed_ms": 8},
            },
        }
        tab._on_result(result)
        assert tab._table.rowCount() == 2
        assert "2/2" in tab._status.text()

    def test_on_result_with_errors(self, qapp):
        tab = self._make_tab(qapp)
        result = {
            "total": 1, "passed": 0,
            "reports": {
                "000001.SZ": {"has_errors": True, "errors": ["缺失日期"], "warnings": ["数据稀疏"], "elapsed_ms": 20},
            },
        }
        tab._on_result(result)
        assert tab._table.rowCount() == 1
        log_text = tab._log.toPlainText()
        assert "[ERROR]" in log_text
        assert "[WARN]" in log_text

    def test_on_result_sys_error(self, qapp):
        tab = self._make_tab(qapp)
        result = {"total": 0, "passed": 0, "reports": {}, "error": "服务不可用"}
        tab._on_result(result)
        assert "[SYS ERROR]" in tab._log.toPlainText()

    def test_on_error(self, qapp):
        tab = self._make_tab(qapp)
        tab._on_error("连接超时")
        assert "连接超时" in tab._status.text()
        assert "[FATAL]" in tab._log.toPlainText()

    def test_run_check_empty_input(self, qapp):
        """_run_check 在空输入时弹出 QMessageBox.warning 并直接返回。"""
        from unittest.mock import patch as _patch

        from gui_app.data_manager_controller import DataManagerController
        from gui_app.widgets.data_governance_panel import _IntegrityTab

        ctrl = DataManagerController()
        tab = _IntegrityTab(controller=ctrl)
        tab._code_input.setText("")
        with _patch("gui_app.widgets.data_governance_panel.QMessageBox") as m:
            tab._run_check()
        m.warning.assert_called_once()

    def test_run_check_valid_input(self, qapp):
        """_run_check 有序列时应启动 _ControllerThread（按钮禁用）。"""
        from gui_app.data_manager_controller import DataManagerController
        from gui_app.widgets.data_governance_panel import _IntegrityTab

        ctrl = DataManagerController()
        tab = _IntegrityTab(controller=ctrl)
        tab._code_input.setText("000001.SZ,600000.SH")
        tab._start_edit.setText("2023-01-01")
        tab._end_edit.setText("2023-12-31")
        tab._run_check()
        assert not tab._run_btn.isEnabled()
        if tab._thread and tab._thread.isRunning():
            tab._thread.wait(3000)


class TestRoutingTabCallbacks:
    """覆盖 _RoutingTab 的 _refresh / _on_result / _on_error。"""

    def _make_tab(self, qapp):
        from gui_app.data_manager_controller import DataManagerController
        from gui_app.widgets.data_governance_panel import _RoutingTab

        ctrl = DataManagerController()
        return _RoutingTab(controller=ctrl)

    def test_on_result_populates_table(self, qapp):
        tab = self._make_tab(qapp)
        result = {
            "total_sources": 2, "healthy_sources": 1,
            "sources": {
                "xtquant": {"hits": 100, "misses": 5, "errors": 0, "quality_rejects": 1, "last_latency_ms": 42},
                "duckdb":  {"hits": 80,  "misses": 2, "errors": 1, "quality_rejects": 0, "last_latency_ms": None},
            },
        }
        tab._on_result(result)
        assert tab._table.rowCount() == 2
        assert "2" in tab._healthy_label.text()

    def test_on_result_with_error_key(self, qapp):
        tab = self._make_tab(qapp)
        result = {"total_sources": 0, "healthy_sources": 0, "sources": {}, "error": "registry 为空"}
        tab._on_result(result)
        assert "[ERROR]" in tab._detail.toPlainText()

    def test_on_error(self, qapp):
        tab = self._make_tab(qapp)
        tab._on_error("数据源注册表不可用")
        assert "[FATAL]" in tab._detail.toPlainText()

    def test_refresh_disables_button(self, qapp):
        tab = self._make_tab(qapp)
        tab._refresh()
        assert not tab._refresh_btn.isEnabled()
        if tab._thread and tab._thread.isRunning():
            tab._thread.wait(3000)


# ─── 新 Controller 方法 Smoke ──────────────────────────────────────────────


class TestNewControllerMethodsSmoke:
    """新增 8 个 Controller 方法的 Smoke 测试，无外部依赖。"""

    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController()

    def test_cross_validate_sources_returns_dict(self):
        ctrl = self._make_ctrl()
        result = ctrl.cross_validate_sources("000001.SZ", "2024-01-01", "2024-12-31")
        assert isinstance(result, dict)

    def test_get_trading_calendar_info_returns_dict(self):
        ctrl = self._make_ctrl()
        result = ctrl.get_trading_calendar_info("2024-01-01", "2024-01-31")
        assert isinstance(result, dict)

    def test_repair_missing_data_returns_dict(self):
        ctrl = self._make_ctrl()
        result = ctrl.repair_missing_data("000001.SZ", "2024-01-01", "2024-12-31")
        assert isinstance(result, dict)

    def test_get_duckdb_maintenance_info_returns_dict(self):
        ctrl = self._make_ctrl()
        result = ctrl.get_duckdb_maintenance_info()
        assert isinstance(result, dict)

    def test_run_checkpoint_returns_dict(self):
        ctrl = self._make_ctrl()
        result = ctrl.run_checkpoint()
        assert isinstance(result, dict)

    def test_get_all_env_config_returns_dict(self):
        ctrl = self._make_ctrl()
        result = ctrl.get_all_env_config()
        assert isinstance(result, dict)
        assert "groups" in result

    def test_get_all_env_config_has_summary(self):
        ctrl = self._make_ctrl()
        result = ctrl.get_all_env_config()
        assert "summary" in result
        summary = result["summary"]
        assert "total" in summary
        assert "configured" in summary

    def test_test_datasource_connectivity_duckdb(self):
        ctrl = self._make_ctrl()
        result = ctrl.test_datasource_connectivity("duckdb")
        assert isinstance(result, dict)
        assert "source" in result
        assert "reachable" in result

    def test_test_datasource_connectivity_unknown_source(self):
        ctrl = self._make_ctrl()
        result = ctrl.test_datasource_connectivity("nonexistent_source")
        assert isinstance(result, dict)

    def test_save_env_to_dotenv_whitelist_reject(self, tmp_path):
        ctrl = self._make_ctrl()
        result = ctrl.save_env_to_dotenv("INVALID_SECRET_KEY", "badvalue", str(tmp_path / ".env"))
        assert result.get("ok") is False or "error" in result

    def test_save_env_to_dotenv_allowed_key(self, tmp_path):
        ctrl = self._make_ctrl()
        dotenv = str(tmp_path / ".env")
        result = ctrl.save_env_to_dotenv("EASYXT_DUCKDB_PATH", "/tmp/test.db", dotenv)
        assert isinstance(result, dict)

    def test_env_catalog_has_entries(self):
        from gui_app.data_manager_controller import DataManagerController
        assert len(DataManagerController._ENV_CATALOG) >= 10

    def test_env_write_whitelist_is_frozenset(self):
        from gui_app.data_manager_controller import DataManagerController
        wl = DataManagerController._ENV_WRITE_WHITELIST
        assert isinstance(wl, frozenset)
        assert "EASYXT_DUCKDB_PATH" in wl


# ─── 新 Tab 类 Smoke ──────────────────────────────────────────────────────


class TestNewTabsSmokeInstantiate:
    """5 个新 Tab 类在无头 Qt 平台下可以无错实例化。"""

    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController()

    def test_reconciliation_tab_instantiates(self, qapp):
        from gui_app.widgets.data_governance_panel import _ReconciliationTab
        ctrl = self._make_ctrl()
        tab = _ReconciliationTab(controller=ctrl)
        assert hasattr(tab, "_run_btn")
        assert hasattr(tab, "_code_input")
        tab.close()

    def test_trading_calendar_tab_instantiates(self, qapp):
        from gui_app.widgets.data_governance_panel import _TradingCalendarTab
        ctrl = self._make_ctrl()
        tab = _TradingCalendarTab(controller=ctrl)
        assert hasattr(tab, "_query_btn")
        assert hasattr(tab, "_trade_table")
        assert hasattr(tab, "_non_table")
        tab.close()

    def test_repair_tab_instantiates(self, qapp):
        from gui_app.widgets.data_governance_panel import _RepairTab
        ctrl = self._make_ctrl()
        tab = _RepairTab(controller=ctrl)
        assert hasattr(tab, "_repair_btn")
        assert hasattr(tab, "_log")
        tab.close()

    def test_database_maintenance_tab_instantiates(self, qapp):
        from gui_app.widgets.data_governance_panel import _DatabaseMaintenanceTab
        ctrl = self._make_ctrl()
        tab = _DatabaseMaintenanceTab(controller=ctrl)
        assert hasattr(tab, "_refresh_btn")
        assert hasattr(tab, "_checkpoint_btn")
        assert hasattr(tab, "_table")
        tab.close()

    def test_environment_config_tab_instantiates(self, qapp):
        from gui_app.widgets.data_governance_panel import _EnvironmentConfigTab
        ctrl = self._make_ctrl()
        tab = _EnvironmentConfigTab(controller=ctrl)
        assert hasattr(tab, "_refresh_btn")
        assert hasattr(tab, "_edit_key")
        assert hasattr(tab, "_save_btn")
        tab.close()


class TestNewTabCallbacks:
    """新 Tab 的 _on_result / _on_error 回调不应抛出异常。"""

    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController()

    def test_reconciliation_on_result_consistent(self, qapp):
        from gui_app.widgets.data_governance_panel import _ReconciliationTab
        tab = _ReconciliationTab(controller=self._make_ctrl())
        tab._on_result({
            "consistent": True,
            "consistency_rate": 0.998,
            "max_diff_pct": 0.01,
            "duckdb_rows": 250,
            "live_rows": 250,
            "compared_rows": 250,
            "diff_days": [],
            "note": "ok",
        })
        assert "✅" in tab._lbl_consistent.text()

    def test_reconciliation_on_result_inconsistent(self, qapp):
        from gui_app.widgets.data_governance_panel import _ReconciliationTab
        tab = _ReconciliationTab(controller=self._make_ctrl())
        tab._on_result({
            "consistent": False,
            "consistency_rate": 0.85,
            "max_diff_pct": 2.5,
            "duckdb_rows": 200,
            "live_rows": 200,
            "compared_rows": 200,
            "diff_days": ["2024-03-15", "2024-03-16"],
        })
        assert "⚠️" in tab._lbl_consistent.text()
        assert "2024-03-15" in tab._diff_text.toPlainText()

    def test_reconciliation_on_result_with_error(self, qapp):
        from gui_app.widgets.data_governance_panel import _ReconciliationTab
        tab = _ReconciliationTab(controller=self._make_ctrl())
        tab._on_result({"error": "数据源不可达"})
        assert "失败" in tab._status.text()

    def test_reconciliation_on_error(self, qapp):
        from gui_app.widgets.data_governance_panel import _ReconciliationTab
        tab = _ReconciliationTab(controller=self._make_ctrl())
        tab._on_error("连接超时")
        assert "连接超时" in tab._status.text()

    def test_calendar_on_result_populates_tables(self, qapp):
        from gui_app.widgets.data_governance_panel import _TradingCalendarTab
        tab = _TradingCalendarTab(controller=self._make_ctrl())
        tab._on_result({
            "total_days": 31,
            "trading_days": 22,
            "weekend_days": 8,
            "holiday_days": 1,
            "trading_days_list": ["2024-01-02", "2024-01-03"],
            "non_trading_list": ["2024-01-01", "2024-01-06"],
        })
        assert tab._lbl_total.text() == "31"
        assert tab._lbl_trade.text() == "22"
        assert tab._trade_table.rowCount() == 2
        assert tab._non_table.rowCount() == 2

    def test_calendar_on_result_with_error(self, qapp):
        from gui_app.widgets.data_governance_panel import _TradingCalendarTab
        tab = _TradingCalendarTab(controller=self._make_ctrl())
        # Should not raise, shows QMessageBox in real usage which we mock away
        with patch("gui_app.widgets.data_governance_panel.QMessageBox.warning"):
            tab._on_result({"error": "日历服务不可用"})

    def test_maintenance_on_result_populates_table(self, qapp):
        from gui_app.widgets.data_governance_panel import _DatabaseMaintenanceTab
        tab = _DatabaseMaintenanceTab(controller=self._make_ctrl())
        tab._on_maintenance_result({
            "tables": [
                {"name": "kline_daily", "rows": 100000, "columns": 8, "last_date": "2024-12-31"},
                {"name": "tick_data",   "rows": 5000000, "columns": 12, "last_date": "2024-12-30"},
            ],
            "db_size_mb": 256.5,
        })
        assert tab._table.rowCount() == 2
        assert "256.5 MB" in tab._size_label.text()

    def test_maintenance_checkpoint_result_ok(self, qapp):
        from gui_app.widgets.data_governance_panel import _DatabaseMaintenanceTab
        tab = _DatabaseMaintenanceTab(controller=self._make_ctrl())
        tab._on_checkpoint_result({"ok": True, "message": "CHECKPOINT 完成, 耗时 0.12s"})
        assert "[OK]" in tab._log.toPlainText()

    def test_env_config_on_result_populates_table(self, qapp):
        from gui_app.widgets.data_governance_panel import _EnvironmentConfigTab
        tab = _EnvironmentConfigTab(controller=self._make_ctrl())
        tab._on_env_result({
            "overall_valid": True,
            "summary": {"total": 5, "configured": 4, "missing_required": 1},
            "groups": {
                "数据库": [
                    {"key": "EASYXT_DB_PATH", "status": "ok", "value": "/tmp/db", "description": "DuckDB 路径", "sensitive": False, "required": True},
                ],
                "API密钥": [
                    {"key": "EASYXT_TUSHARE_TOKEN", "status": "missing", "value": "", "description": "Tushare Token", "sensitive": True, "required": False},
                ],
            },
        })
        assert tab._table.rowCount() == 2

    def test_env_config_connectivity_result_ok(self, qapp):
        from gui_app.widgets.data_governance_panel import _EnvironmentConfigTab
        tab = _EnvironmentConfigTab(controller=self._make_ctrl())
        tab._on_connectivity_result({
            "source": "duckdb", "reachable": True, "latency_ms": 1.5, "method": "read_query"
        })
        assert "[OK]" in tab._log.toPlainText()

    def test_env_config_connectivity_result_fail(self, qapp):
        from gui_app.widgets.data_governance_panel import _EnvironmentConfigTab
        tab = _EnvironmentConfigTab(controller=self._make_ctrl())
        tab._on_connectivity_result({
            "source": "tushare", "reachable": False, "error": "Token 无效"
        })
        assert "[FAIL]" in tab._log.toPlainText()

    def test_repair_on_result_queued(self, qapp):
        from gui_app.widgets.data_governance_panel import _RepairTab
        tab = _RepairTab(controller=self._make_ctrl())
        tab._on_result({"queued": True, "message": "任务已加入队列"})
        assert "[OK]" in tab._log.toPlainText()

    def test_repair_on_result_fail(self, qapp):
        from gui_app.widgets.data_governance_panel import _RepairTab
        tab = _RepairTab(controller=self._make_ctrl())
        tab._on_result({"queued": False, "error": "调度器不可用"})
        assert "[FAIL]" in tab._log.toPlainText()

    def test_repair_on_error(self, qapp):
        from gui_app.widgets.data_governance_panel import _RepairTab
        tab = _RepairTab(controller=self._make_ctrl())
        tab._on_error("异常: 调度队列已满")
        assert "[FATAL]" in tab._log.toPlainText()
        if tab._thread and tab._thread.isRunning():
            tab._thread.wait(3000)


class TestPipelineTabCallbacks:
    """覆盖 _PipelineTab 的各回调方法。"""

    def _make_tab(self, qapp):
        from gui_app.data_manager_controller import DataManagerController
        from gui_app.widgets.data_governance_panel import _PipelineTab

        ctrl = DataManagerController()
        return _PipelineTab(controller=ctrl)

    def test_on_pipeline_result_healthy(self, qapp):
        tab = self._make_tab(qapp)
        result = {
            "overall_healthy": True,
            "timestamp": "2024-01-01T12:00:00",
            "checks": {
                "duckdb": {"healthy": True, "table_count": 5},
                "xtquant": {"healthy": False, "error": "离线"},
            },
        }
        tab._on_pipeline_result(result)
        assert "健康" in tab._overall_label.text()
        assert tab._table.rowCount() == 2

    def test_on_pipeline_result_with_error_key(self, qapp):
        tab = self._make_tab(qapp)
        result = {"overall_healthy": False, "timestamp": "", "checks": {}, "error": "pipeline 崩溃"}
        tab._on_pipeline_result(result)
        assert "[ERROR]" in tab._json_view.toPlainText()

    def test_on_db_summary_healthy(self, qapp):
        tab = self._make_tab(qapp)
        result = {
            "healthy": True, "table_count": 3,
            "stock_daily_rows": 500000, "latest_date": "2024-01-15",
            "path": "/data/easyxt.duckdb",
        }
        tab._on_db_summary(result)
        label = tab._summary_label.text()
        assert "500" in label  # 500,000 or 500000

    def test_on_db_summary_unhealthy(self, qapp):
        tab = self._make_tab(qapp)
        result = {"healthy": False, "error": "文件不存在"}
        tab._on_db_summary(result)
        assert "异常" in tab._summary_label.text()

    def test_on_env_result(self, qapp):
        tab = self._make_tab(qapp)
        result = {
            "valid": False,
            "items": [
                {"key": "EASYXT_DUCKDB_PATH", "status": "ok", "value": "/data/db", "note": "DuckDB 路径", "required": True},
                {"key": "QMT_DATA_DIR", "status": "missing", "value": "", "note": "QMT 数据目录", "required": False},
            ],
        }
        tab._on_env_result(result)
        assert tab._env_table.rowCount() == 2

    def test_on_error(self, qapp):
        tab = self._make_tab(qapp)
        tab._on_error("管道状态查询超时")
        assert "[FATAL]" in tab._json_view.toPlainText()
        assert tab._refresh_btn.isEnabled()

    def test_toggle_auto_on_off(self, qapp):
        from unittest.mock import patch as _patch

        tab = self._make_tab(qapp)
        # Prevent _refresh() from spawning threads (it would hang without event loop)
        with _patch.object(tab, "_refresh"):
            tab._toggle_auto(True)
            assert tab._auto_timer.isActive()
            tab._toggle_auto(False)
        assert not tab._auto_timer.isActive()


# ─── Once-only tab auto-refresh guard ──────────────────────────────────────


class TestOnceOnlyTabRefresh:
    """确保 _on_tab_changed 每个 Tab 只触发一次自动刷新。"""

    @staticmethod
    def _make_panel(qapp):
        """创建 DataGovernancePanel，mock 掉会崩溃的 DuckDB 查询 Tab。"""
        from unittest.mock import patch
        from PyQt5.QtWidgets import QWidget
        from gui_app.widgets.data_governance_panel import DataGovernancePanel
        with patch.object(DataGovernancePanel, "_make_query_tab", return_value=QWidget()):
            return DataGovernancePanel()

    def test_auto_refreshed_set_initialized(self, qapp):
        panel = self._make_panel(qapp)
        assert hasattr(panel, "_auto_refreshed")
        assert isinstance(panel._auto_refreshed, set)

    def test_routing_tab_is_instance_var(self, qapp):
        panel = self._make_panel(qapp)
        assert hasattr(panel, "_routing_tab")

    def test_tab_changed_only_refreshes_once(self, qapp):
        from unittest.mock import patch as _patch
        panel = self._make_panel(qapp)
        refresh_calls: list[int] = []

        def _fake_refresh() -> None:
            refresh_calls.append(1)

        with _patch.object(panel._pipeline_tab, "_refresh", side_effect=_fake_refresh):
            panel._on_tab_changed(3)  # 第一次切换 Tab 3
            panel._on_tab_changed(3)  # 第二次切换，不应再刷新
            panel._on_tab_changed(3)  # 第三次切换，不应再刷新

        assert len(refresh_calls) == 1, "管道状态 Tab 应该只自动刷新一次"

    def test_tab_changed_routing_tab_refreshes_once(self, qapp):
        from unittest.mock import patch as _patch
        panel = self._make_panel(qapp)
        calls: list[int] = []

        def _fake_refresh() -> None:
            calls.append(1)

        with _patch.object(panel._routing_tab, "_refresh", side_effect=_fake_refresh):
            panel._on_tab_changed(2)
            panel._on_tab_changed(2)

        assert len(calls) == 1, "数据路由 Tab 应该只自动刷新一次"


# ─── Events 常量验证 ────────────────────────────────────────────────────────


class TestNewEvents:
    """验证新增的治理事件常量。"""

    def test_data_repaired_event_exists(self, _unused=None):
        from core.events import Events
        assert hasattr(Events, "DATA_REPAIRED")
        assert Events.DATA_REPAIRED == "data_repaired"

    def test_env_config_saved_event_exists(self, _unused=None):
        from core.events import Events
        assert hasattr(Events, "ENV_CONFIG_SAVED")
        assert Events.ENV_CONFIG_SAVED == "env_config_saved"


# ─── 死信队列控制器方法 ─────────────────────────────────────────────────────


class TestBackfillDeadLetterController:
    """DataManagerController 死信队列查询 / 清空方法测试。"""

    def _make_ctrl(self):
        from gui_app.data_manager_controller import DataManagerController
        return DataManagerController()

    def test_get_dead_letter_no_file(self, tmp_path, monkeypatch):
        import os
        monkeypatch.setenv("EASYXT_DEAD_LETTER_PATH", str(tmp_path / "absent.jsonl"))
        ctrl = self._make_ctrl()
        result = ctrl.get_backfill_dead_letter()
        assert result["total"] == 0
        assert result["entries"] == []

    def test_get_dead_letter_with_entries(self, tmp_path, monkeypatch):
        import json as _json, os
        dl_file = tmp_path / "dead.jsonl"
        record = {
            "key": "000001.SZ|1d|2024-01-01|2024-01-31",
            "payload": {"stock_code": "000001.SZ", "start_date": "2024-01-01",
                        "end_date": "2024-01-31", "period": "1d", "reason": "manual"},
            "retry_count": 5,
            "reason": "max_retries_exhausted",
            "failed_at": "2024-02-01T00:00:00+00:00",
        }
        dl_file.write_text(_json.dumps(record) + "\n", encoding="utf-8")
        monkeypatch.setenv("EASYXT_DEAD_LETTER_PATH", str(dl_file))
        ctrl = self._make_ctrl()
        result = ctrl.get_backfill_dead_letter()
        assert result["total"] == 1
        assert result["entries"][0]["stock_code"] == "000001.SZ"
        assert result["entries"][0]["retry_count"] == 5
        assert result["entries"][0]["reason"] == "max_retries_exhausted"

    def test_clear_dead_letter_removes_file(self, tmp_path, monkeypatch):
        import os
        dl_file = tmp_path / "dead.jsonl"
        dl_file.write_text('{"key":"test"}\n', encoding="utf-8")
        monkeypatch.setenv("EASYXT_DEAD_LETTER_PATH", str(dl_file))
        ctrl = self._make_ctrl()
        result = ctrl.clear_backfill_dead_letter()
        assert result["ok"] is True
        assert not dl_file.exists()

    def test_clear_dead_letter_no_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("EASYXT_DEAD_LETTER_PATH", str(tmp_path / "absent.jsonl"))
        ctrl = self._make_ctrl()
        result = ctrl.clear_backfill_dead_letter()
        assert result["ok"] is True


# ─── _RepairTab 死信队列 UI 存在性 ──────────────────────────────────────────


class TestRepairTabDeadLetterUI:
    """验证 _RepairTab 死信队列 UI 组件已正确初始化。"""

    def test_dead_letter_widgets_present(self, qapp):
        from gui_app.widgets.data_governance_panel import _RepairTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RepairTab(DataManagerController())
        assert hasattr(tab, "_dl_refresh_btn")
        assert hasattr(tab, "_dl_clear_btn")
        assert hasattr(tab, "_dl_table")
        assert hasattr(tab, "_dl_status")

    def test_dead_letter_table_columns(self, qapp):
        from gui_app.widgets.data_governance_panel import _RepairTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RepairTab(DataManagerController())
        assert tab._dl_table.columnCount() == 7

    def test_on_dead_letter_result_populates_table(self, qapp):
        from gui_app.widgets.data_governance_panel import _RepairTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RepairTab(DataManagerController())
        result = {
            "entries": [
                {"stock_code": "600519.SH", "start_date": "2024-01-01",
                 "end_date": "2024-06-30", "period": "1d",
                 "reason": "max_retries_exhausted", "retry_count": 5,
                 "failed_at": "2024-07-01T00:00:00+00:00", "key": "k1"},
            ],
            "total": 1,
            "file_path": "/data/dead.jsonl",
        }
        tab._on_dead_letter_result(result)
        assert tab._dl_table.rowCount() == 1
        assert tab._dl_table.item(0, 0).text() == "600519.SH"
        assert "1 条" in tab._dl_status.text()

    def test_on_dl_error_sets_status(self, qapp):
        from gui_app.widgets.data_governance_panel import _RepairTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RepairTab(DataManagerController())
        tab._on_dl_error("文件读取失败")
        assert "错误" in tab._dl_status.text()


# ─── MainWindow 新事件处理器 ───────────────────────────────────────────────


class TestMainWindowNewEventHandlers:
    """验证 MainWindow._on_data_repaired / _on_env_config_saved 无崩溃。"""

    def test_on_data_repaired_with_stock_code(self, qapp):
        """有 stock_code payload 时状态栏收到消息、不崩溃。"""
        from gui_app.main_window import MainWindow
        from unittest.mock import MagicMock
        win = MainWindow.__new__(MainWindow)
        sb = MagicMock()
        win.status_bar = sb
        win._on_data_repaired(stock_code="000001.SZ", queued=True, source="repair_tab")
        sb.showMessage.assert_called_once()
        args = sb.showMessage.call_args[0]
        assert "000001.SZ" in args[0]

    def test_on_data_repaired_without_stock_code(self, qapp):
        """无 stock_code payload 时不崩溃，使用默认消息。"""
        from gui_app.main_window import MainWindow
        from unittest.mock import MagicMock
        win = MainWindow.__new__(MainWindow)
        sb = MagicMock()
        win.status_bar = sb
        win._on_data_repaired()
        sb.showMessage.assert_called_once()
        assert "修复" in sb.showMessage.call_args[0][0]

    def test_on_env_config_saved_with_key(self, qapp):
        """有 key payload 时状态栏显示键名。"""
        from gui_app.main_window import MainWindow
        from unittest.mock import MagicMock
        win = MainWindow.__new__(MainWindow)
        sb = MagicMock()
        win.status_bar = sb
        win._on_env_config_saved(key="EASYXT_HOST", source="env_config_tab")
        sb.showMessage.assert_called_once()
        assert "EASYXT_HOST" in sb.showMessage.call_args[0][0]

    def test_on_env_config_saved_without_key(self, qapp):
        """无 key payload 时不崩溃，使用默认消息。"""
        from gui_app.main_window import MainWindow
        from unittest.mock import MagicMock
        win = MainWindow.__new__(MainWindow)
        sb = MagicMock()
        win.status_bar = sb
        win._on_env_config_saved()
        sb.showMessage.assert_called_once()
        assert "配置" in sb.showMessage.call_args[0][0]

    def test_handler_survives_missing_status_bar(self, qapp):
        """status_bar 未初始化时调用不崩溃。"""
        from gui_app.main_window import MainWindow
        win = MainWindow.__new__(MainWindow)
        # no status_bar attribute set
        win._on_data_repaired(stock_code="600519.SH")
        win._on_env_config_saved(key="EASYXT_PORT")

    def test_render_release_gate_status_uses_shared_rag_policy(self, qapp):
        from gui_app.main_window import MainWindow

        class _Label:
            def __init__(self):
                self.text_value = ""
                self.style_value = ""
                self.tip_value = ""

            def setText(self, v):
                self.text_value = v

            def setStyleSheet(self, v):
                self.style_value = v

            def setToolTip(self, v):
                self.tip_value = v

        win = MainWindow.__new__(MainWindow)
        win.release_gate_status = _Label()
        win._release_gate_status = {
            "strict_gate_pass": True,
            "P0_open_count": 0,
            "active_critical_high": 0,
            "duckdb_write_probe_detail": {},
            "intraday_bar_semantic_detail": {},
            "governance_nightly_detail": {},
            "period_validation_detail": {"status": "pass", "failed_items": 0},
            "watermark_quality_detail": {"q_score_pass": True, "today_q_score": 0.99, "q_score_mean_7d": 0.98, "q_score_vol_7d": 0.01, "trend": []},
            "watermark_profile_audit_detail": {"recent": []},
            "watermark_profile_approval_detail": {},
        }

        def _artifact(name):
            if name == "stability_evidence_30d.json":
                return {"period_validation": {"failed_rows": 2}}
            if name == "peak_release_gate_latest.json":
                return {"level": "warn", "period_validation_failed_items": 2, "max_period_validation_failed_items": 0}
            return None

        win._load_artifact_json = _artifact
        win._render_release_gate_status()
        assert "PV=PV[🟡 WARN（2>0）]" in win.release_gate_status.text_value
        assert "R=RAG[🟡 YELLOW]" in win.release_gate_status.text_value
        assert "#ef6c00" in win.release_gate_status.style_value
        assert "period_validation_detail_tag=PV_DETAIL[v=1|pv=PV[🟡 WARN（2>0）]|failed=2|max=0|msg=N%2FA|action=N%2FA]" in win.release_gate_status.tip_value
        assert "gate_detail_tag=GATE_DETAIL[v=1|rag=RAG[🟡 YELLOW]|pv_detail=PV_DETAIL[v=1|pv=PV[🟡 WARN（2>0）]|failed=2|max=0|msg=N%2FA|action=N%2FA]]" in win.release_gate_status.tip_value
        assert "gate_detail_parse_ok=True" in win.release_gate_status.tip_value
        assert "gate_detail_parse_error=" in win.release_gate_status.tip_value
        assert "contract_health=HEALTHY" in win.release_gate_status.tip_value
        assert "debug_period_validation_failed_norm=2" in win.release_gate_status.tip_value
        assert "rag_tag=RAG[🟡 YELLOW]" in win.release_gate_status.tip_value

    def test_render_release_gate_status_fallback_period_validation_from_p0_metrics(self, qapp):
        from gui_app.main_window import MainWindow

        class _Label:
            def __init__(self):
                self.text_value = ""
                self.style_value = ""
                self.tip_value = ""

            def setText(self, v):
                self.text_value = v

            def setStyleSheet(self, v):
                self.style_value = v

            def setToolTip(self, v):
                self.tip_value = v

        win = MainWindow.__new__(MainWindow)
        win.release_gate_status = _Label()
        win._release_gate_status = {
            "strict_gate_pass": False,
            "P0_open_count": 1,
            "active_critical_high": 0,
            "duckdb_write_probe_detail": {},
            "intraday_bar_semantic_detail": {"status": "pass", "anomaly_count": 0},
            "governance_nightly_detail": {"status": "pass", "failed_items": 0},
            "period_validation_detail": {"status": "pass", "failed_items": 3},
            "watermark_quality_detail": {"q_score_pass": True, "today_q_score": 0.99, "q_score_mean_7d": 0.98, "q_score_vol_7d": 0.01, "trend": []},
            "watermark_profile_audit_detail": {"recent": []},
            "watermark_profile_approval_detail": {},
        }
        win._load_artifact_json = lambda _name: None
        win._render_release_gate_status()
        assert "period_validation:3/0" in win.release_gate_status.text_value
        assert "PV=PV[❌ FAIL（3>0）]" in win.release_gate_status.text_value
        assert "#d32f2f" in win.release_gate_status.style_value
        assert "period_validation_detail_tag=PV_DETAIL[v=1|pv=PV[❌ FAIL（3>0）]|failed=3|max=0|msg=N%2FA|action=N%2FA]" in win.release_gate_status.tip_value
        assert "gate_detail_tag=GATE_DETAIL[v=1|rag=RAG[🔴 RED]|pv_detail=PV_DETAIL[v=1|pv=PV[❌ FAIL（3>0）]|failed=3|max=0|msg=N%2FA|action=N%2FA]]" in win.release_gate_status.tip_value
        assert "gate_detail_parse_ok=True" in win.release_gate_status.tip_value
        assert "gate_detail_parse_error=" in win.release_gate_status.tip_value
        assert "contract_health=HEALTHY" in win.release_gate_status.tip_value
        assert "debug_period_validation_failed_norm=3" in win.release_gate_status.tip_value
        assert "rag_tag=RAG[🔴 RED]" in win.release_gate_status.tip_value


# ─── DataManagerController Section 15: export_data_snapshot ─────────────


class TestExportDataSnapshot:
    """Section 15: export_data_snapshot 的单元测试。"""

    def test_empty_stock_codes_returns_error(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController()
        result = ctrl.export_data_snapshot([], "2024-01-01", "2024-12-31", "/tmp/out.csv")
        assert result["ok"] is False
        assert "stock_codes" in result["error"]

    def test_invalid_format_returns_error(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController()
        result = ctrl.export_data_snapshot(
            ["000001.SZ"], "2024-01-01", "2024-12-31", "/tmp/out.xlsx", fmt="xlsx"
        )
        assert result["ok"] is False
        assert "xlsx" in result["error"]

    def test_duckdb_unavailable_returns_error(self, tmp_path):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController(duckdb_path=str(tmp_path / "nonexistent.ddb"))
        result = ctrl.export_data_snapshot(
            ["000001.SZ"], "2024-01-01", "2024-12-31",
            str(tmp_path / "out.csv"),
        )
        assert result["ok"] is False
        assert "error" in result

    def test_successful_csv_export(self, tmp_path):
        """DuckDB 可访问时导出 CSV，验证文件落盘且行数一致。"""
        import pandas as pd
        import duckdb
        from gui_app.data_manager_controller import DataManagerController
        # 建一个内存 DuckDB 写到文件
        db_path = str(tmp_path / "test.ddb")
        con = duckdb.connect(db_path)
        con.execute(
            "CREATE TABLE stock_daily AS "
            "SELECT '000001.SZ' AS code, '2024-01-02' AS date, 10.0 AS close "
            "UNION ALL "
            "SELECT '000001.SZ', '2024-01-03', 10.5"
        )
        con.close()
        out_path = str(tmp_path / "snapshot.csv")
        ctrl = DataManagerController(duckdb_path=db_path)
        result = ctrl.export_data_snapshot(
            ["000001.SZ"], "2024-01-01", "2024-12-31", out_path
        )
        assert result["ok"] is True
        assert result["rows"] == 2
        assert result["symbols"] == 1
        df = pd.read_csv(out_path)
        assert len(df) == 2

    def test_successful_json_export(self, tmp_path):
        """fmt='json' 时导出 JSON，验证文件格式正确。"""
        import json
        import duckdb
        from gui_app.data_manager_controller import DataManagerController
        db_path = str(tmp_path / "test.ddb")
        con = duckdb.connect(db_path)
        con.execute(
            "CREATE TABLE stock_daily AS "
            "SELECT '600519.SH' AS code, '2024-03-01' AS date, 1800.0 AS close"
        )
        con.close()
        out_path = str(tmp_path / "snapshot.json")
        ctrl = DataManagerController(duckdb_path=db_path)
        result = ctrl.export_data_snapshot(
            ["600519.SH"], "2024-01-01", "2024-12-31", out_path, fmt="json"
        )
        assert result["ok"] is True
        with open(out_path, encoding="utf-8") as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["code"] == "600519.SH"


# ─── DatabaseMaintenanceTab 导出快照 UI ──────────────────────────────────


class TestDatabaseMaintenanceTabExportUI:
    """Tab8 导出快照 UI 组件存在性 + 回调正确性。"""

    def test_export_ui_components_exist(self, qapp):
        from gui_app.widgets.data_governance_panel import _DatabaseMaintenanceTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _DatabaseMaintenanceTab(DataManagerController())
        assert hasattr(tab, "_export_codes")
        assert hasattr(tab, "_export_start")
        assert hasattr(tab, "_export_end")
        assert hasattr(tab, "_export_btn")
        assert hasattr(tab, "_export_status")

    def test_on_export_result_ok_updates_ui(self, qapp):
        from gui_app.widgets.data_governance_panel import _DatabaseMaintenanceTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _DatabaseMaintenanceTab(DataManagerController())
        tab._on_export_result({
            "ok": True, "rows": 500, "symbols": 2, "output_path": "/tmp/snap.csv", "fmt": "csv"
        })
        assert "500" in tab._export_status.text()
        assert tab._export_btn.isEnabled()

    def test_on_export_result_error_updates_ui(self, qapp):
        from gui_app.widgets.data_governance_panel import _DatabaseMaintenanceTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _DatabaseMaintenanceTab(DataManagerController())
        tab._on_export_result({"ok": False, "error": "DuckDB 连接失败"})
        assert "DuckDB" in tab._export_status.text() or "❌" in tab._export_status.text()
        assert tab._export_btn.isEnabled()


# ═══════════════════════════════════════════════════════════════════════════
# 实时链路（Tab 10）测试
# ═══════════════════════════════════════════════════════════════════════════


class TestRealtimeMonitorTab:
    """Tab 10: _RealtimeMonitorTab UI 组件 + 事件接收行为。"""

    def test_components_exist(self, qapp):
        from gui_app.widgets.data_governance_panel import _RealtimeMonitorTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RealtimeMonitorTab(DataManagerController())
        assert hasattr(tab, "_conn_label")
        assert hasattr(tab, "_lbl_connected")
        assert hasattr(tab, "_lbl_degraded")
        assert hasattr(tab, "_lbl_symbol")
        assert hasattr(tab, "_lbl_quote_ts")
        assert hasattr(tab, "_lbl_reason")
        assert hasattr(tab, "_lbl_drop_rate")
        assert hasattr(tab, "_event_table")
        assert hasattr(tab, "_refresh_btn")
        assert hasattr(tab, "_clear_btn")

    def test_on_pipeline_event_connected(self, qapp):
        from gui_app.widgets.data_governance_panel import _RealtimeMonitorTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RealtimeMonitorTab(DataManagerController())
        tab.on_pipeline_event(connected=True, degraded=False, symbol="000001.SZ",
                              quote_ts="10:30:00", reason="", drop_rate=0.02)
        assert "已连接" in tab._conn_label.text()
        assert tab._lbl_symbol.text() == "000001.SZ"
        assert tab._event_table.rowCount() == 1

    def test_on_pipeline_event_degraded(self, qapp):
        from gui_app.widgets.data_governance_panel import _RealtimeMonitorTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RealtimeMonitorTab(DataManagerController())
        tab.on_pipeline_event(connected=True, degraded=True, symbol="600519.SH",
                              reason="丢帧率超阈值")
        assert "降级" in tab._conn_label.text()
        assert "是" == tab._lbl_degraded.text()

    def test_on_pipeline_event_disconnected(self, qapp):
        from gui_app.widgets.data_governance_panel import _RealtimeMonitorTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RealtimeMonitorTab(DataManagerController())
        tab.on_pipeline_event(connected=False)
        assert "未连接" in tab._conn_label.text()

    def test_append_event_respects_max(self, qapp):
        from gui_app.widgets.data_governance_panel import _RealtimeMonitorTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RealtimeMonitorTab(DataManagerController())
        # 超出 MAX 条时只保留最新 MAX 条
        for i in range(_RealtimeMonitorTab._MAX_EVENTS + 10):
            tab._append_event("12:00:00", "✅", f"sym{i}", "")
        assert len(tab._event_rows) == _RealtimeMonitorTab._MAX_EVENTS
        assert tab._event_table.rowCount() == _RealtimeMonitorTab._MAX_EVENTS

    def test_clear_events_resets_table(self, qapp):
        from gui_app.widgets.data_governance_panel import _RealtimeMonitorTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RealtimeMonitorTab(DataManagerController())
        tab.on_pipeline_event(connected=True)
        assert tab._event_table.rowCount() > 0
        tab._clear_events()
        assert tab._event_table.rowCount() == 0
        assert len(tab._event_rows) == 0

    def test_poll_result_updates_card(self, qapp):
        from gui_app.widgets.data_governance_panel import _RealtimeMonitorTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RealtimeMonitorTab(DataManagerController())
        tab._on_poll_result({"connected": True, "degraded": False, "symbol": "test.SZ"})
        assert "已连接" in tab._conn_label.text()

    def test_poll_result_error_updates_label(self, qapp):
        from gui_app.widgets.data_governance_panel import _RealtimeMonitorTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RealtimeMonitorTab(DataManagerController())
        tab._on_poll_result({"error": "模块不可用"})
        assert "查询失败" in tab._conn_label.text() or "模块" in tab._conn_label.text()


# ═══════════════════════════════════════════════════════════════════════════
# DataGovernancePanel 事件路由测试（11 Tab）
# ═══════════════════════════════════════════════════════════════════════════


class TestDataGovernancePanelEventRouting:
    """验证 DataGovernancePanel 的 signal_bus 事件转发逻辑。"""

    @staticmethod
    def _make_panel(qapp):
        """创建 DataGovernancePanel，mock 掉会崩溃的 DuckDB 查询 Tab。"""
        from unittest.mock import patch
        from PyQt5.QtWidgets import QWidget
        from gui_app.widgets.data_governance_panel import DataGovernancePanel
        with patch.object(DataGovernancePanel, "_make_query_tab", return_value=QWidget()):
            return DataGovernancePanel()

    def test_panel_has_twelve_tabs(self, qapp):
        from gui_app.widgets.data_governance_panel import DataGovernancePanel
        panel = self._make_panel(qapp)
        assert panel._tabs.count() == 12

    def test_panel_stores_integrity_repair_realtime_refs(self, qapp):
        from gui_app.widgets.data_governance_panel import (
            DataGovernancePanel, _IntegrityTab, _RepairTab, _RealtimeMonitorTab
        )
        panel = self._make_panel(qapp)
        assert isinstance(panel._integrity_tab, _IntegrityTab)
        assert isinstance(panel._repair_tab, _RepairTab)
        assert isinstance(panel._realtime_tab, _RealtimeMonitorTab)

    def test_on_rt_pipeline_event_dispatches_to_realtime_tab(self, qapp):
        from gui_app.widgets.data_governance_panel import DataGovernancePanel
        panel = self._make_panel(qapp)
        panel._on_rt_pipeline_event(connected=True, degraded=False, symbol="AAA")
        assert "已连接" in panel._realtime_tab._conn_label.text()

    def test_on_data_quality_alert_dispatches_to_integrity_tab(self, qapp):
        from gui_app.widgets.data_governance_panel import DataGovernancePanel
        panel = self._make_panel(qapp)
        panel._on_data_quality_alert_event(
            stock_code="000001.SZ", message="缺失交易日", severity="error"
        )
        log_text = panel._integrity_tab._log.toPlainText()
        assert "000001.SZ" in log_text or "缺失" in log_text

    def test_on_backfill_updated_dispatches_to_repair_tab(self, qapp):
        from gui_app.widgets.data_governance_panel import DataGovernancePanel
        from unittest.mock import patch
        panel = self._make_panel(qapp)
        with patch.object(panel._repair_tab, "_load_dead_letter") as mock_load:
            panel._on_backfill_task_updated(stock_code="600000.SH", status="failed")
            mock_load.assert_called_once()

    def test_tab10_triggers_refresh_on_first_visit(self, qapp):
        from gui_app.widgets.data_governance_panel import DataGovernancePanel
        from unittest.mock import patch
        panel = self._make_panel(qapp)
        with patch.object(panel._realtime_tab, "_refresh") as mock_refresh:
            panel._on_tab_changed(10)
            mock_refresh.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
# RoutingTab 60s 自动刷新测试
# ═══════════════════════════════════════════════════════════════════════════


class TestRoutingTabAutoRefresh:
    """Tab 2 路由指标：60s 自动刷新切换逻辑。"""

    def test_auto_refresh_button_exists(self, qapp):
        from gui_app.widgets.data_governance_panel import _RoutingTab
        from gui_app.data_manager_controller import DataManagerController
        tab = _RoutingTab(DataManagerController())
        assert hasattr(tab, "_auto_btn")
        assert tab._auto_btn.isCheckable()

    def test_toggle_auto_true_starts_timer(self, qapp):
        from gui_app.widgets.data_governance_panel import _RoutingTab
        from gui_app.data_manager_controller import DataManagerController
        from unittest.mock import patch
        tab = _RoutingTab(DataManagerController())
        with patch.object(tab._auto_timer, "start") as mock_start, \
             patch.object(tab, "_refresh"):
            tab._toggle_auto(True)
            mock_start.assert_called_once_with(60_000)
            assert "停止" in tab._auto_btn.text()

    def test_toggle_auto_false_stops_timer(self, qapp):
        from gui_app.widgets.data_governance_panel import _RoutingTab
        from gui_app.data_manager_controller import DataManagerController
        from unittest.mock import patch
        tab = _RoutingTab(DataManagerController())
        with patch.object(tab._auto_timer, "stop") as mock_stop:
            tab._toggle_auto(False)
            mock_stop.assert_called_once()
            assert "60s" in tab._auto_btn.text()


# ═══════════════════════════════════════════════════════════════════════════
# Controller Section 16 测试
# ═══════════════════════════════════════════════════════════════════════════


class TestGetRealtimePipelineInfo:
    """DataManagerController.get_realtime_pipeline_info() 单元测试。"""

    def test_returns_dict_with_expected_keys(self):
        from gui_app.data_manager_controller import DataManagerController
        ctrl = DataManagerController()
        result = ctrl.get_realtime_pipeline_info()
        assert isinstance(result, dict)
        for key in ("connected", "degraded", "symbol", "drop_rate", "source"):
            assert key in result, f"missing key: {key}"

    def test_no_singleton_returns_connected_false(self):
        from gui_app.data_manager_controller import DataManagerController
        import sys
        # 确保没有 singleton 实例
        mod = sys.modules.get("data_manager.realtime_pipeline_manager")
        if mod is not None:
            orig = getattr(mod, "_singleton_instance", None)
            try:
                if hasattr(mod, "_singleton_instance"):
                    mod._singleton_instance = None
                ctrl = DataManagerController()
                result = ctrl.get_realtime_pipeline_info()
                # 可能 connected=False 或返回 error (模块可能不可用)
                assert result.get("connected") in (False, None)
            finally:
                if orig is not None:
                    mod._singleton_instance = orig
        else:
            ctrl = DataManagerController()
            result = ctrl.get_realtime_pipeline_info()
            assert result.get("connected") in (False, None)

    def test_with_mock_singleton_returns_metrics(self):
        from gui_app.data_manager_controller import DataManagerController
        from unittest.mock import MagicMock
        import sys
        # 创建 mock RPM 实例
        mock_rpm = MagicMock()
        mock_rpm.metrics.return_value = {
            "sustained_alert": False,
            "drop_rate": 0.05,
            "total_quotes": 100,
            "queue_len": 3,
        }
        mock_rpm._symbol = "000001.SZ"
        # 注入到模块 ns
        mod_name = "data_manager.realtime_pipeline_manager"
        mock_mod = MagicMock()
        mock_mod.RealtimePipelineManager = MagicMock
        mock_mod._singleton_instance = mock_rpm
        original = sys.modules.get(mod_name)
        sys.modules[mod_name] = mock_mod
        try:
            ctrl = DataManagerController()
            result = ctrl.get_realtime_pipeline_info()
            assert result.get("connected") is True
            assert result.get("drop_rate") == 0.05
            assert result.get("total_quotes") == 100
        finally:
            if original is None:
                sys.modules.pop(mod_name, None)
            else:
                sys.modules[mod_name] = original
