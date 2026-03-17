import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd

from core.events import Events
from core.signal_bus import signal_bus
from gui_app.trading_interface_simple import TradingInterface

pytestmark = pytest.mark.gui
from gui_app.widgets.grid_trading_widget import GridTradingWidget
from gui_app.widgets.kline_chart_workspace import KLineChartWorkspace
from gui_app.widgets.local_data_manager_widget import LocalDataManagerWidget


def test_chart_to_trading_to_grid_flow(qapp):
    trading = TradingInterface()
    grid = GridTradingWidget()
    try:
        grid.stock_pool_edit.setText("000001.SZ")

        signal_bus.emit(Events.CHART_DATA_LOADED, symbol="000001.SZ", period="1d")
        assert trading.order_panel.stock_combo.currentText() == "000001.SZ"

        trading.is_connected = True
        trading.easyxt = None
        trading.account_info = {
            "available_cash": 100000,
            "total_asset": 100000,
            "today_pnl": 0,
        }
        trading.positions = []
        assert trading.place_order_signal("000001.SZ", "buy", 10.5, 100)

        assert grid.trade_table.rowCount() == 1
        assert "订单提交" in grid.log_text.toPlainText()
    finally:
        signal_bus.unsubscribe(Events.CHART_DATA_LOADED, trading.update_stock_code)
        signal_bus.unsubscribe(Events.ORDER_SUBMITTED, grid.on_order_submitted)
        trading.close()
        grid.close()


def test_trading_interface_update_stock_code(qapp):
    trading = TradingInterface()
    try:
        trading.update_stock_code("000001.SZ")
        assert trading.order_panel.stock_combo.currentText() == "000001.SZ"
        trading.order_panel.stock_combo.setCurrentText("600000.SH")
        trading.update_stock_code("")
        assert trading.order_panel.stock_combo.currentText() == "600000.SH"
    finally:
        trading.close()


def test_trading_interface_submit_unified_order_disconnected(qapp):
    trading = TradingInterface()
    try:
        trading.is_connected = False
        assert not trading.submit_unified_order("000001.SZ", "buy", 10.5, 100, source="signal")
    finally:
        trading.close()


def test_trading_interface_rejection_stats_for_daily_limit(qapp):
    trading = TradingInterface()
    try:
        trading.is_connected = True
        trading.easyxt = None
        trading.account_info = {"available_cash": 100000, "total_asset": 100000, "today_pnl": 0}
        trading.positions = []
        trading.max_daily_orders = 0
        trading.order_validator = trading._build_order_validator()
        assert not trading.submit_unified_order("000001.SZ", "buy", 10.5, 100, source="signal")
        assert trading.rejection_total == 1
        assert "DAILY_ORDER_LIMIT" in trading.rejection_by_reason
    finally:
        trading.close()


def test_trading_interface_batch_orders(qapp):
    trading = TradingInterface()
    try:
        trading.is_connected = True
        trading.easyxt = None
        trading.account_info = {"available_cash": 100000, "total_asset": 100000, "today_pnl": 0}
        trading.positions = []
        result = trading.submit_batch_orders([
            {"symbol": "000001.SZ", "side": "buy", "price": 10.5, "volume": 100},
            {"symbol": "000002.SZ", "side": "buy", "price": 20.5, "volume": 100},
        ])
        assert result["total"] == 2
        assert result["success"] == 2
        assert trading.get_daily_order_count() == 2
    finally:
        trading.close()


def test_trading_interface_simulated_connection(qapp):
    trading = TradingInterface()
    try:
        trading.is_connected = False
        trading.easyxt = None
        trading.connect_to_trading()
        assert trading.is_connected
        assert trading.account_info.get("total_asset") == 100000.0
        assert len(trading.positions) == 2
        assert "模拟" in trading.connection_status_label.text()
    finally:
        trading.close()


def test_trading_interface_disconnect_clears_state(qapp):
    trading = TradingInterface()
    try:
        trading.is_connected = True
        trading.easyxt = None
        trading.rejection_total = 3
        trading.rejection_by_reason = {"RISK": 2}
        trading.rejection_by_symbol = {"000001.SZ": 1}
        trading.disconnect_from_trading()
        assert not trading.is_connected
        assert trading.rejection_total == 0
        assert trading.rejection_by_reason == {}
        assert trading.rejection_by_symbol == {}
    finally:
        trading.close()


def test_trading_interface_daily_order_count_reset(qapp):
    trading = TradingInterface()
    try:
        trading.daily_order_date = trading.daily_order_date.replace(year=trading.daily_order_date.year - 1)
        trading.daily_order_count = 5
        assert trading.get_daily_order_count() == 0
    finally:
        trading.close()


def test_trading_interface_position_helpers(qapp):
    trading = TradingInterface()
    try:
        trading.account_info = {"available_cash": 8000, "total_asset": 20000, "today_pnl": -500}
        trading.positions = [
            {"stock_code": "000001.SZ", "available_volume": 200, "cost_price": 12.5},
        ]
        assert trading.get_available_cash() == 8000.0
        assert trading.get_total_asset() == 20000.0
        assert trading.get_daily_loss() == 500.0
        assert trading.get_position_volume("000001.SZ") == 200
        assert trading.get_position_value("000001.SZ") == 2500.0
    finally:
        trading.close()


def test_trading_interface_update_time(qapp):
    trading = TradingInterface()
    try:
        trading.update_time()
        assert trading.time_label.text()
    finally:
        trading.close()


def test_trading_interface_manual_order_not_connected(qapp):
    trading = TradingInterface()
    try:
        messages = []

        def warning(_, title, message):
            messages.append((title, message))

        from gui_app import trading_interface_simple as module

        old_warning = module.QMessageBox.warning
        module.QMessageBox.warning = warning
        trading.is_connected = False
        assert not trading.submit_unified_order("000001.SZ", "buy", 10.5, 100, source="manual")
        assert messages
    finally:
        from gui_app import trading_interface_simple as module

        module.QMessageBox.warning = old_warning
        trading.close()


def test_trading_interface_validation_failure_manual(qapp):
    trading = TradingInterface()
    try:
        messages = []

        class ValidationStub:
            is_valid = False
            error_message = "invalid"
            error_code = "ERR"

        class ValidatorStub:
            def validate(self, order):
                return ValidationStub()

        def warning(_, title, message):
            messages.append((title, message))

        from gui_app import trading_interface_simple as module

        old_warning = module.QMessageBox.warning
        module.QMessageBox.warning = warning
        trading.is_connected = True
        trading.order_validator = ValidatorStub()
        assert not trading.submit_unified_order("000001.SZ", "buy", 10.5, 100, source="manual")
        assert trading.rejection_total == 1
        assert "ERR" in trading.rejection_by_reason
        assert messages
    finally:
        from gui_app import trading_interface_simple as module

        module.QMessageBox.warning = old_warning
        trading.close()


def test_trading_interface_easyxt_success_and_fail(qapp):
    trading = TradingInterface()
    try:
        messages = []

        class TradeStub:
            def __init__(self):
                self.next_result = True
                self.disconnected = False

            def buy(self, account_id, stock_code, volume, price, price_type):
                return self.next_result

            def sell(self, account_id, stock_code, volume, price, price_type):
                return self.next_result

            def disconnect(self):
                self.disconnected = True

            def get_account_asset(self, account_id):
                return {"total_asset": 100000, "cash": 50000, "market_value": 50000, "today_pnl": 0}

            def get_positions(self, account_id):
                return pd.DataFrame()

        class EasyXtStub:
            def __init__(self):
                self.trade = TradeStub()

        def information(_, title, message):
            messages.append((title, message))

        def warning(_, title, message):
            messages.append((title, message))

        from gui_app import trading_interface_simple as module

        old_available = module.EASYXT_AVAILABLE
        old_info = module.QMessageBox.information
        old_warning = module.QMessageBox.warning
        module.EASYXT_AVAILABLE = True
        module.QMessageBox.information = information
        module.QMessageBox.warning = warning

        trading.easyxt = EasyXtStub()
        trading.is_connected = True
        trading.account_id = "test"
        trading.account_info = {"available_cash": 50000, "total_asset": 100000, "today_pnl": 0}
        trading.positions = []
        trading.easyxt.trade.next_result = True
        assert trading.submit_unified_order("000001.SZ", "buy", 10.5, 100, source="manual")
        trading.easyxt.trade.next_result = False
        assert not trading.submit_unified_order("000001.SZ", "sell", 10.5, 100, source="manual")
        assert messages
    finally:
        from gui_app import trading_interface_simple as module

        module.EASYXT_AVAILABLE = old_available
        module.QMessageBox.information = old_info
        module.QMessageBox.warning = old_warning
        trading.is_connected = False
        trading.close()


def test_trading_interface_handlers(qapp):
    trading = TradingInterface()
    try:
        trading.is_connected = True
        trading.easyxt = None
        trading.account_info = {"available_cash": 50000, "total_asset": 100000, "today_pnl": 0}
        trading.positions = []
        assert trading._handle_order_request("000001.SZ", "buy", 10.5, 100, source="signal")
        result = trading._handle_batch_request(
            [
                {"symbol": "000001.SZ", "side": "buy", "price": 10.5, "volume": 100},
            ],
            source="batch",
        )
        assert result["total"] == 1
    finally:
        trading.close()


def test_trading_interface_connect_easyxt_path(qapp):
    trading = TradingInterface()
    try:
        messages = []

        class TradeStub:
            def __init__(self):
                self.disconnected = False

            def disconnect(self):
                self.disconnected = True

            def get_account_asset(self, account_id):
                return None

            def get_positions(self, account_id):
                return pd.DataFrame()

        class EasyXtStub:
            def __init__(self):
                self.trade = TradeStub()

            def init_trade(self, userdata_path, session_id):
                return True

            def add_account(self, account_id, account_type):
                return True

        def warning(_, title, message):
            messages.append((title, message))

        from gui_app import trading_interface_simple as module

        old_available = module.EASYXT_AVAILABLE
        old_warning = module.QMessageBox.warning
        module.EASYXT_AVAILABLE = True
        module.QMessageBox.warning = warning

        trading.easyxt = EasyXtStub()
        trading.userdata_path = "D:/test/userdata"
        trading.account_id = "123"
        trading.connect_to_trading()
        assert trading.is_connected
        assert "已连接" in trading.connection_status_label.text()
        assert not messages
    finally:
        from gui_app import trading_interface_simple as module

        module.EASYXT_AVAILABLE = old_available
        module.QMessageBox.warning = old_warning
        trading.is_connected = False
        trading.close()


def test_trading_interface_disconnect_easyxt_path(qapp):
    trading = TradingInterface()
    try:
        class TradeStub:
            def __init__(self):
                self.disconnected = False

            def disconnect(self):
                self.disconnected = True

        class EasyXtStub:
            def __init__(self):
                self.trade = TradeStub()

        from gui_app import trading_interface_simple as module

        old_available = module.EASYXT_AVAILABLE
        module.EASYXT_AVAILABLE = True
        trading.easyxt = EasyXtStub()
        trading.is_connected = True
        trading.disconnect_from_trading()
        assert not trading.is_connected
        assert trading.easyxt.trade.disconnected
    finally:
        from gui_app import trading_interface_simple as module

        module.EASYXT_AVAILABLE = old_available
        trading.is_connected = False
        trading.close()


def test_local_data_manager_download_single_stock_requires_code(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        messages = []

        def warning(_, title, message):
            messages.append((title, message))

        old_warning = module.QMessageBox.warning
        module.QMessageBox.warning = warning
        widget.stock_code_input.setText("")
        widget.download_single_stock()
        assert messages
        assert widget.download_thread is None
    finally:
        module.QMessageBox.warning = old_warning
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_local_data_manager_download_single_stock_builds_thread(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        created = {}

        class DummySignal:
            def __init__(self):
                self.handlers = []

            def connect(self, fn):
                self.handlers.append(fn)

        class DummyThread:
            def __init__(self, stock_code, start_date, end_date, period):
                created["stock_code"] = stock_code
                created["start_date"] = start_date
                created["end_date"] = end_date
                created["period"] = period
                self.log_signal = DummySignal()
                self.finished_signal = DummySignal()
                self.error_signal = DummySignal()
                self.started = False

            def start(self):
                self.started = True

        old_thread = module.SingleStockDownloadThread
        module.SingleStockDownloadThread = DummyThread
        widget.stock_code_input.setText("600000")
        widget.manual_data_type_combo.setCurrentText("5分钟数据")
        widget.download_single_stock()
        assert created["stock_code"] == "600000.SH"
        assert created["period"] == "5m"
        assert not widget.manual_download_btn.isEnabled()
        assert widget.download_thread.started
    finally:
        module.SingleStockDownloadThread = old_thread
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_local_data_manager_load_duckdb_statistics_updates_labels(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        class DummyResult:
            def __init__(self, one=None, all_rows=None):
                self._one = one
                self._all = all_rows

            def fetchone(self):
                return self._one

            def fetchall(self):
                return self._all or []

        class DummyCon:
            def execute(self, sql):
                normalized = " ".join(sql.split())
                if "FROM stock_daily" in normalized:
                    if "SUM" in normalized:
                        return DummyResult((5, 3, 2, 1000, "2024-01-05"))
                    return DummyResult((5, 1000, "2024-01-05"))
                if "FROM stock_1m" in normalized or "FROM stock_5m" in normalized:
                    if "SELECT DISTINCT stock_code" in normalized:
                        return DummyResult(all_rows=[("000001.SZ",), ("600000.SH",)])
                    return DummyResult((2, 20))
                if "FROM stock_15m" in normalized or "FROM stock_30m" in normalized or "FROM stock_60m" in normalized:
                    if "SELECT DISTINCT stock_code" in normalized:
                        return DummyResult(all_rows=[("000001.SZ",)])
                    return DummyResult((1, 10))
                return DummyResult((0, 0))

            def close(self):
                return None

        class DummyDuckdb:
            def connect(self, path, read_only=True):
                return DummyCon()

        old_duckdb = sys.modules.get("duckdb")
        sys.modules["duckdb"] = DummyDuckdb()
        old_columns = module._get_table_columns
        old_ensure = module._ensure_duckdb_tables
        module._get_table_columns = lambda con, table: ["stock_code", "symbol_type", "date"]
        module._ensure_duckdb_tables = lambda: True

        widget.load_duckdb_statistics()
        assert "标的总数" in widget.total_symbols_label.text()
        assert "股票数量" in widget.total_stocks_label.text()
        assert "总记录数" in widget.total_records_label.text()
        assert "存储大小" in widget.total_size_label.text()
        assert "最新日期" in widget.latest_date_label.text()
    finally:
        if old_duckdb is None:
            sys.modules.pop("duckdb", None)
        else:
            sys.modules["duckdb"] = old_duckdb
        module._get_table_columns = old_columns
        module._ensure_duckdb_tables = old_ensure
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_local_data_manager_download_financial_data_requires_table(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        messages = []

        def warning(_, title, message):
            messages.append((title, message))

        old_warning = module.QMessageBox.warning
        module.QMessageBox.warning = warning

        widget.financial_balance_check.setChecked(False)
        widget.financial_income_check.setChecked(False)
        widget.financial_cashflow_check.setChecked(False)
        widget.financial_cap_check.setChecked(False)
        widget.download_financial_data()
        assert messages
        assert widget.download_thread is None
    finally:
        module.QMessageBox.warning = old_warning
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_local_data_manager_update_progress_and_state(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        widget._set_download_state(True)
        assert not widget.download_stocks_btn.isEnabled()
        assert not widget.progress_bar.isHidden()
        widget.update_progress(3, 10)
        assert widget.progress_bar.value() == 3
        assert "3/10" in widget.progress_bar.format()
        widget._set_download_state(False)
        assert widget.download_stocks_btn.isEnabled()
        assert widget.progress_bar.isHidden()
    finally:
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_local_data_manager_on_download_finished_and_error(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        messages = []

        def information(_, title, message):
            messages.append((title, message))

        def warning(_, title, message):
            messages.append((title, message))

        def critical(_, title, message):
            messages.append((title, message))

        old_info = module.QMessageBox.information
        old_warning = module.QMessageBox.warning
        old_critical = module.QMessageBox.critical
        module.QMessageBox.information = information
        module.QMessageBox.warning = warning
        module.QMessageBox.critical = critical

        load_calls = []
        old_load = widget.load_duckdb_statistics
        widget.load_duckdb_statistics = lambda: load_calls.append("loaded")

        widget.progress_bar.setVisible(True)
        widget.on_download_finished({"total": 2, "success": 2, "failed": 0})
        assert messages
        assert load_calls

        messages.clear()
        widget.progress_bar.setVisible(True)
        widget.on_download_finished({"total": 2, "success": 1, "failed": 1})
        assert messages

        messages.clear()
        widget.on_download_error("boom")
        assert messages
    finally:
        widget.load_duckdb_statistics = old_load
        module.QMessageBox.information = old_info
        module.QMessageBox.warning = old_warning
        module.QMessageBox.critical = old_critical
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_local_data_manager_single_download_callbacks(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        messages = []

        def information(_, title, message):
            messages.append((title, message))

        def critical(_, title, message):
            messages.append((title, message))

        old_info = module.QMessageBox.information
        old_critical = module.QMessageBox.critical
        module.QMessageBox.information = information
        module.QMessageBox.critical = critical

        widget.manual_download_btn.setEnabled(False)
        widget.on_single_download_finished(
            {"symbol": "600000.SH", "success": True, "record_count": 10, "file_size": 0.5}
        )
        assert widget.manual_download_btn.isEnabled()
        assert messages

        messages.clear()
        widget.manual_download_btn.setEnabled(False)
        widget.on_single_download_finished({"symbol": "600000.SH", "success": False})
        assert widget.manual_download_btn.isEnabled()

        messages.clear()
        widget.manual_download_btn.setEnabled(False)
        widget.on_single_download_error("failed")
        assert widget.manual_download_btn.isEnabled()
        assert messages
    finally:
        module.QMessageBox.information = old_info
        module.QMessageBox.critical = old_critical
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_local_data_manager_financial_callbacks_and_save_flow(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    old_load = widget.load_duckdb_statistics
    try:
        messages = []

        def information(_, title, message):
            messages.append((title, message))

        def warning(_, title, message):
            messages.append((title, message))

        def critical(_, title, message):
            messages.append((title, message))

        old_info = module.QMessageBox.information
        old_warning = module.QMessageBox.warning
        old_critical = module.QMessageBox.critical
        module.QMessageBox.information = information
        module.QMessageBox.warning = warning
        module.QMessageBox.critical = critical

        widget.progress_bar.setVisible(True)
        widget.on_financial_download_finished({"total": 2, "success": 2, "failed": 0, "skipped": 0})
        assert messages

        messages.clear()
        widget.progress_bar.setVisible(True)
        widget.on_financial_download_finished({"total": 2, "success": 1, "failed": 1, "skipped": 0})
        assert messages

        old_available = module.BATCH_SAVE_AVAILABLE
        old_thread = module.BatchFinancialSaveThread
        module.BATCH_SAVE_AVAILABLE = True

        class DummySignal:
            def __init__(self):
                self.handlers = []

            def connect(self, fn):
                self.handlers.append(fn)

        class DummySaveThread:
            def __init__(self, stock_list):
                self.stock_list = stock_list
                self.log_signal = DummySignal()
                self.progress_signal = DummySignal()
                self.finished_signal = DummySignal()
                self.error_signal = DummySignal()
                self.started = False

            def start(self):
                self.started = True

        module.BatchFinancialSaveThread = DummySaveThread
        widget.save_financial_to_duckdb()
        assert widget.save_thread.started
        assert not widget.progress_bar.isHidden()

        load_calls = []
        widget.load_duckdb_statistics = lambda: load_calls.append("loaded")
        widget.on_financial_save_finished({"total": 1, "success": 1, "failed": 0})
        assert load_calls

        messages.clear()
        widget.on_financial_save_error("save failed")
        assert messages
    finally:
        module.BatchFinancialSaveThread = old_thread
        module.BATCH_SAVE_AVAILABLE = old_available
        module.QMessageBox.information = old_info
        module.QMessageBox.warning = old_warning
        module.QMessageBox.critical = old_critical
        module.QTimer.singleShot = old_single_shot
        widget.load_duckdb_statistics = old_load
        widget.close()


def test_local_data_manager_verify_data_integrity_starts_thread(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        class DummyDialog:
            TextInput = object()

            def __init__(self, parent=None):
                self._value = "600000"
                self._return_value = "600000"

            def setWindowTitle(self, title):
                self.title = title

            def setLabelText(self, text):
                self.label = text

            def setTextValue(self, value):
                self._value = value

            def setInputMode(self, mode):
                self.mode = mode

            def exec_(self):
                return True

            def textValue(self):
                return self._return_value

        created = {}

        class DummySignal:
            def __init__(self):
                self.handlers = []

            def connect(self, fn):
                self.handlers.append(fn)

        class DummyThread:
            def __init__(self, stock_code):
                created["stock_code"] = stock_code
                self.log_signal = DummySignal()
                self.finished_signal = DummySignal()
                self.started = False

            def start(self):
                self.started = True

        old_dialog = module.QInputDialog
        old_thread = module.VerifyDataThread
        module.QInputDialog = DummyDialog
        module.VerifyDataThread = DummyThread
        widget.verify_data_integrity()
        assert created["stock_code"] == "600000.SH"
        assert widget.verify_thread.started
    finally:
        module.VerifyDataThread = old_thread
        module.QInputDialog = old_dialog
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_local_data_manager_verify_data_integrity_cancel(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        class DummyDialog:
            TextInput = object()

            def __init__(self, parent=None):
                self._value = ""

            def setWindowTitle(self, title):
                self.title = title

            def setLabelText(self, text):
                self.label = text

            def setTextValue(self, value):
                self._value = value

            def setInputMode(self, mode):
                self.mode = mode

            def exec_(self):
                return False

            def textValue(self):
                return self._value

        old_dialog = module.QInputDialog
        module.QInputDialog = DummyDialog
        widget.verify_data_integrity()
        assert not hasattr(widget, "verify_thread")
    finally:
        module.QInputDialog = old_dialog
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_local_data_manager_on_verify_finished_messages(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        messages = []

        def information(_, title, message):
            messages.append(("info", title, message))

        def warning(_, title, message):
            messages.append(("warn", title, message))

        old_info = module.QMessageBox.information
        old_warning = module.QMessageBox.warning
        module.QMessageBox.information = information
        module.QMessageBox.warning = warning

        widget.on_verify_finished(
            {
                "stock": "600000.SH",
                "has_1min": True,
                "records_1min": 100,
                "start_1min": "2024-01-01",
                "end_1min": "2024-01-02",
                "has_daily": False,
                "has_tick": False,
            }
        )
        assert messages

        messages.clear()
        widget.on_verify_finished(
            {"stock": "600000.SH", "has_1min": False, "has_daily": False, "has_tick": False}
        )
        assert messages
    finally:
        module.QMessageBox.information = old_info
        module.QMessageBox.warning = old_warning
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_local_data_manager_verify_data_integrity_logs_to_ui(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    old_single_shot = module.QTimer.singleShot
    module.QTimer.singleShot = lambda *args, **kwargs: None
    widget = LocalDataManagerWidget()
    try:
        class DummyDialog:
            TextInput = object()

            def __init__(self, parent=None):
                self._value = "600000"
                self._return_value = "600000"

            def setWindowTitle(self, title):
                self.title = title

            def setLabelText(self, text):
                self.label = text

            def setTextValue(self, value):
                self._value = value

            def setInputMode(self, mode):
                self.mode = mode

            def exec_(self):
                return True

            def textValue(self):
                return self._return_value

        class DummySignal:
            def __init__(self):
                self.handlers = []

            def connect(self, fn):
                self.handlers.append(fn)

            def emit(self, *args, **kwargs):
                for fn in self.handlers:
                    fn(*args, **kwargs)

        class DummyThread:
            def __init__(self, stock_code):
                self.stock_code = stock_code
                self.log_signal = DummySignal()
                self.finished_signal = DummySignal()
                self.started = False

            def start(self):
                self.started = True

        old_dialog = module.QInputDialog
        old_thread = module.VerifyDataThread
        module.QInputDialog = DummyDialog
        module.VerifyDataThread = DummyThread
        widget.verify_data_integrity()
        widget.verify_thread.log_signal.emit("校验日志输出")
        assert "校验日志输出" in widget.log_text.toPlainText()
    finally:
        module.VerifyDataThread = old_thread
        module.QInputDialog = old_dialog
        module.QTimer.singleShot = old_single_shot
        widget.close()


def test_verify_data_thread_error_emits_finished_result(qapp):
    from gui_app.widgets import local_data_manager_widget as module

    thread = module.VerifyDataThread("600000.SH")
    try:
        logs = []
        results = []

        class DummySignal:
            def __init__(self):
                self.handlers = []

            def connect(self, fn):
                self.handlers.append(fn)

            def emit(self, *args, **kwargs):
                for fn in self.handlers:
                    fn(*args, **kwargs)

        thread.log_signal = DummySignal()
        thread.finished_signal = DummySignal()
        thread.log_signal.connect(lambda message: logs.append(message))
        thread.finished_signal.connect(lambda result: results.append(result))

        old_ensure = module._ensure_duckdb_tables
        module._ensure_duckdb_tables = lambda: (_ for _ in ()).throw(Exception("boom"))
        thread.run()
        assert logs
        assert results
        result = results[0]
        assert result["stock"] == "600000.SH"
        assert result["has_1min"] is False
        assert result["has_daily"] is False
        assert result["records_1min"] == 0
        assert result["records_daily"] == 0
    finally:
        module._ensure_duckdb_tables = old_ensure


def test_verify_data_thread_normal_path_emits_logs_and_result(qapp):
    import sys
    import types

    from gui_app.widgets import local_data_manager_widget as module

    thread = module.VerifyDataThread("600000.SH")
    logs = []
    results = []

    class DummySignal:
        def __init__(self):
            self.handlers = []

        def connect(self, fn):
            self.handlers.append(fn)

        def emit(self, *args, **kwargs):
            for fn in self.handlers:
                fn(*args, **kwargs)

    class DummyCursor:
        def __init__(self, result):
            self.result = result

        def fetchone(self):
            return self.result

    class DummyCon:
        def __init__(self):
            self.closed = False

        def execute(self, sql):
            if "FROM stock_1m" in sql:
                return DummyCursor((10000, "2024-01-01", "2024-12-31"))
            if "FROM stock_daily" in sql:
                return DummyCursor((2500, "2020-01-01", "2024-12-31"))
            if "FROM stock_tick" in sql:
                return DummyCursor((50000, "2024-12-01 09:30:00", "2024-12-31 15:00:00"))
            return DummyCursor((0, None, None))

        def close(self):
            self.closed = True

    dummy_duckdb = types.SimpleNamespace(connect=lambda *args, **kwargs: DummyCon())

    old_ensure = module._ensure_duckdb_tables
    old_duckdb = sys.modules.get("duckdb")
    module._ensure_duckdb_tables = lambda: None
    sys.modules["duckdb"] = dummy_duckdb

    try:
        thread.log_signal = DummySignal()
        thread.finished_signal = DummySignal()
        thread.log_signal.connect(lambda message: logs.append(message))
        thread.finished_signal.connect(lambda result: results.append(result))
        thread.run()
        assert any("1分钟数据" in message for message in logs)
        assert any("日线数据" in message for message in logs)
        assert any("Tick数据" in message for message in logs)
        assert results
        result = results[0]
        assert result["stock"] == "600000.SH"
        assert result["has_1min"] is True
        assert result["has_daily"] is True
        assert result["has_tick"] is True
        assert result["records_1min"] == 10000
        assert result["records_daily"] == 2500
        assert result["records_tick"] == 50000
        assert result["start_1min"] == "2024-01-01"
        assert result["end_1min"] == "2024-12-31"
        assert result["start_daily"] == "2020-01-01"
        assert result["end_daily"] == "2024-12-31"
        assert result["start_tick"] == "2024-12-01 09:30:00"
        assert result["end_tick"] == "2024-12-31 15:00:00"
    finally:
        module._ensure_duckdb_tables = old_ensure
        if old_duckdb is None:
            sys.modules.pop("duckdb", None)
        else:
            sys.modules["duckdb"] = old_duckdb


def test_kline_chart_prepare_data_and_signal(qapp):
    workspace = KLineChartWorkspace()
    try:
        data = {
            "date": pd.date_range("2024-01-01", periods=25, freq="D"),
            "open": [10.0] * 25,
            "high": [20.0] * 24 + [30.0],
            "low": [9.0] * 25,
            "close": [10.0] * 24 + [30.0],
        }
        df = pd.DataFrame(data)
        prepared = workspace._prepare_chart_data(df, "1d")
        assert list(prepared.columns) == ["time", "open", "high", "low", "close", "volume"]
        signal = workspace._compute_signal(prepared)
        assert signal["name"] in {"ma_cross_up", "breakout_up"}
    finally:
        workspace.close()


def test_kline_chart_mark_order(qapp):
    workspace = KLineChartWorkspace()
    try:
        markers = []

        class ChartStub:
            def marker(self, text):
                markers.append(text)

        workspace.chart = ChartStub()
        workspace.symbol_input.setText("000001.SZ")
        workspace.mark_order("buy", "000001.SZ", 10.5, 100)
        workspace.mark_order("sell", "000002.SZ", 10.5, 100)
        assert markers == ["📈 BUY"]
    finally:
        workspace.close()


def test_kline_chart_refresh_latest_bar_marks_up(qapp):
    """_apply_latest_bar_from_bg 是 refresh_latest_bar 异步流水线的同步出口，
    直接测试它以验证：新 close 更高时 chart.update 被调用且 marker 写入正确标签。"""
    workspace = KLineChartWorkspace()
    try:
        updates = []
        markers = []

        class ChartStub:
            def update(self, row):
                updates.append(float(row["close"]))

            def marker(self, text=""):
                markers.append(text)

        workspace.chart = ChartStub()
        workspace.chart_adapter = None
        workspace.last_data = pd.DataFrame({
            "time": ["2024-01-01"],
            "open": [10.0],
            "high": [12.0],
            "low": [9.0],
            "close": [10.0],
            "volume": [100],
        })
        workspace.last_close = 10.0
        workspace.last_signal_key = ""

        last_row_dict = {
            "time": "2024-01-02", "open": 10.0, "high": 12.0,
            "low": 9.0, "close": 12.0, "volume": 120,
        }
        dummy_signal = {"time": "2024-01-02", "name": "up", "label": "Up"}
        workspace._apply_latest_bar_from_bg(last_row_dict, dummy_signal, "000001.SZ", "1d")

        assert updates == [12.0], f"chart.update 未被调用，updates={updates}"
        assert markers == ["Up"], f"chart.marker 标签错误，markers={markers}"
    finally:
        workspace.close()


def test_kline_chart_test_mode_no_realtime_connect_thread(qapp, monkeypatch):
    """回归测试：test_mode=True 时 _ensure_realtime_api 必须短路，
    _RealtimeConnectThread 不得启动，防止未来改动引入回归。"""
    from PyQt5.QtCore import QThread
    from gui_app.widgets.kline_chart_workspace import KLineChartWorkspace

    started_threads = []
    original_start = QThread.start

    def spy_start(self, priority=QThread.InheritPriority):
        started_threads.append(type(self).__name__)
        return original_start(self, priority)

    monkeypatch.setattr(QThread, "start", spy_start)
    workspace = KLineChartWorkspace()
    try:
        assert workspace.test_mode, "测试环境下 test_mode 应为 True"
        workspace._ensure_realtime_api()
        realtime_threads = [n for n in started_threads if "Realtime" in n or "Connect" in n]
        assert realtime_threads == [], (
            f"test_mode 下不应启动实盘连接线程，实际启动了：{realtime_threads}"
        )
    finally:
        workspace.close()


# ======================================================================
# 退出稳定性专项：反复创建/销毁 KLineChartWorkspace，验证无 QThread 泄漏
# ======================================================================
class TestKLineWorkspaceExitStability:
    """退出稳定性专项 — 对应工程规范"线程退出安全"。
    每个用例在构造后立即 close()，断言：
      1. close() 正常返回，不抛异常
      2. 无 _RealtimeConnectThread 被启动（test_mode 守卫有效）
      3. close() 后所有已知 QThread 属性 isRunning() == False
      4. 强杀场景下 THREAD_FORCED_TERMINATE 事件被正确上报
    """

    def test_single_create_close_no_exception(self, qapp):
        """最基本：构造+关闭不抛异常。"""
        ws = KLineChartWorkspace()
        ws.close()

    def test_repeated_create_close_five_times(self, qapp):
        """连续创建销毁 5 次，每次均正常退出。"""
        for _ in range(5):
            ws = KLineChartWorkspace()
            ws.close()

    def test_no_realtime_thread_started_on_create(self, qapp, monkeypatch):
        """构造期间不应启动任何 _RealtimeConnectThread。"""
        from PyQt5.QtCore import QThread

        started = []
        orig = QThread.start

        def spy(self, priority=QThread.InheritPriority):
            started.append(type(self).__name__)
            return orig(self, priority)

        monkeypatch.setattr(QThread, "start", spy)
        ws = KLineChartWorkspace()
        ws.close()
        rt = [n for n in started if "Realtime" in n or "Connect" in n]
        assert rt == [], f"构造期间不应启动实盘连接线程，实际：{rt}"

    def test_all_qthreads_stopped_after_close(self, qapp):
        """close() 之后，workspace 上所有已知 QThread 属性均不再 isRunning()。"""
        ws = KLineChartWorkspace()
        ws.close()
        thread_attrs = [
            "_chart_load_thread",
            "_latest_bar_thread",
            "_realtime_connect_thread",
            "_quote_worker",
            "_data_process_thread",
        ]
        for attr in thread_attrs:
            t = getattr(ws, attr, None)
            if t is not None:
                assert not t.isRunning(), f"{attr} 关闭后仍在运行"

    def test_thread_forced_terminate_event_on_mock_stuck_thread(self, qapp, monkeypatch):
        """当 _realtime_connect_thread.wait(1000) 超时时，
        closeEvent 应发出 THREAD_FORCED_TERMINATE 事件。"""
        from PyQt5.QtCore import QThread

        events_received = []

        def _handler(**kwargs):
            events_received.append(kwargs)

        signal_bus.subscribe(Events.THREAD_FORCED_TERMINATE, _handler)
        ws = KLineChartWorkspace()
        try:
            class _StuckThread(QThread):
                def run(self):
                    import time as _t
                    _t.sleep(60)  # 不会真跑满，terminate() 会打断

            stuck = _StuckThread()
            stuck.start()
            # 覆盖 wait 使其立即返回 False（模拟超时），terminate 可正常调用
            monkeypatch.setattr(stuck, "wait", lambda ms: False)
            ws._realtime_connect_thread = stuck
            ws.close()
        finally:
            signal_bus.unsubscribe(Events.THREAD_FORCED_TERMINATE, _handler)

        assert events_received, "强杀后应发出 THREAD_FORCED_TERMINATE 事件"
        assert events_received[0]["thread_name"] == "_RealtimeConnectThread"
