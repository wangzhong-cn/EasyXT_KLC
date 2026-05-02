"""Tests for auto_data_updater module – pure logic paths."""
import sys
import pytest
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Module-level pure functions
# ---------------------------------------------------------------------------

class TestShiftTime:
    """Tests for the _shift_time helper function."""

    def setup_method(self):
        from data_manager.auto_data_updater import _shift_time
        self.fn = _shift_time

    def test_basic_forward_shift(self):
        assert self.fn('15:30', 5) == '15:35'

    def test_zero_shift(self):
        assert self.fn('09:00', 0) == '09:00'

    def test_shift_crosses_hour_boundary(self):
        assert self.fn('14:55', 10) == '15:05'

    def test_shift_at_midnight_rollover(self):
        # 23:50 + 15 = 00:05 next day (rolls over)
        result = self.fn('23:50', 15)
        assert result == '00:05'

    def test_large_shift(self):
        # 00:00 + 90 = 01:30
        assert self.fn('00:00', 90) == '01:30'

    def test_negative_shift(self):
        # -5 from 15:30 = 15:25
        assert self.fn('15:30', -5) == '15:25'

    def test_result_format_two_digits(self):
        result = self.fn('09:05', 0)
        assert len(result) == 5
        assert result[2] == ':'


# ---------------------------------------------------------------------------
# AutoDataUpdater – construction and is_trading_day
# ---------------------------------------------------------------------------

class TestAutoDataUpdaterConstruction:
    """Basic construction and is_trading_day."""

    def _make_updater(self, update_time='15:30'):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            updater = AutoDataUpdater(duckdb_path=':memory:', update_time=update_time)
            return updater

    def test_update_time_stored(self):
        updater = self._make_updater('16:00')
        assert updater.update_time == '16:00'

    def test_running_starts_false(self):
        updater = self._make_updater()
        assert updater.running is False

    def test_thread_starts_none(self):
        updater = self._make_updater()
        assert updater.thread is None

    def test_total_updates_starts_zero(self):
        updater = self._make_updater()
        assert updater.total_updates == 0

    def test_last_update_time_starts_none(self):
        updater = self._make_updater()
        assert updater.last_update_time is None

    def test_has_calendar(self):
        updater = self._make_updater()
        assert updater.calendar is not None

    def test_is_trading_day_weekend(self):
        updater = self._make_updater()
        sat = date(2023, 6, 3)
        assert not updater.is_trading_day(sat)

    def test_is_trading_day_sunday(self):
        updater = self._make_updater()
        sun = date(2023, 6, 4)
        assert not updater.is_trading_day(sun)

    def test_is_trading_day_weekday_returns_bool(self):
        updater = self._make_updater()
        mon = date(2023, 6, 5)
        result = updater.is_trading_day(mon)
        assert isinstance(result, bool)

    def test_is_trading_day_none_uses_today(self):
        updater = self._make_updater()
        # Calling with None should not raise
        result = updater.is_trading_day(None)
        assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# AutoDataUpdater.should_update_today
# ---------------------------------------------------------------------------

class TestShouldUpdateToday:
    """Tests for should_update_today conditional logic."""

    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_returns_false_on_weekend(self):
        updater = self._make_updater()
        from zoneinfo import ZoneInfo
        _SH = ZoneInfo('Asia/Shanghai')
        with patch('data_manager.auto_data_updater.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2023, 6, 3, 16, 0, tzinfo=_SH)
            mock_dt.combine = datetime.combine
            result = updater.should_update_today()
        assert result is False

    def test_returns_false_if_already_updated_today(self):
        updater = self._make_updater()
        from zoneinfo import ZoneInfo
        _SH = ZoneInfo('Asia/Shanghai')
        mon = date(2023, 6, 5)  # Monday
        updater.last_update_time = mon
        updater.is_trading_day = lambda d: True
        updater.update_time = '00:00'  # 应已过更新时刻
        with patch('data_manager.auto_data_updater.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2023, 6, 5, 15, 0, tzinfo=_SH)  # 15:00 CST
            mock_dt.combine = datetime.combine
            result = updater.should_update_today()
        assert result is False

    def test_returns_false_before_update_time(self):
        updater = self._make_updater()
        from zoneinfo import ZoneInfo
        _SH = ZoneInfo('Asia/Shanghai')
        mon = date(2023, 6, 5)
        updater.last_update_time = None
        updater.is_trading_day = lambda d: True
        updater.update_time = '23:59'  # hasn't come yet
        with patch('data_manager.auto_data_updater.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2023, 6, 5, 9, 0, tzinfo=_SH)  # 09:00 CST, before 23:59
            mock_dt.combine = datetime.combine
            result = updater.should_update_today()
        assert result is False


# ---------------------------------------------------------------------------
# AutoDataUpdater.update_single_stock – interface not initialized
# ---------------------------------------------------------------------------

class TestUpdateSingleStockNoInterface:
    """update_single_stock when interface fails to initialize."""

    def _make_updater_no_interface(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            updater = AutoDataUpdater(':memory:')
            # Force initialize_interface to leave self.interface as None
            updater.interface = None
            updater.initialize_interface = lambda: None
            return updater

    def test_returns_dict_with_failure(self):
        updater = self._make_updater_no_interface()
        result = updater.update_single_stock('000001.SZ')
        assert isinstance(result, dict)
        assert result['stock_code'] == '000001.SZ'
        assert result['success'] is False

    def test_returns_error_message_when_no_interface(self):
        updater = self._make_updater_no_interface()
        result = updater.update_single_stock('600000.SH')
        assert 'message' in result
        assert result['message']  # non-empty

    def test_returns_zero_records_when_no_interface(self):
        updater = self._make_updater_no_interface()
        result = updater.update_single_stock('000001.SZ')
        assert result['records'] == 0


# ---------------------------------------------------------------------------
# AutoDataUpdater – initialize_data_manager / initialize_interface
# ---------------------------------------------------------------------------

class TestInitializeMethods:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_initialize_data_manager_failure_leaves_none(self):
        updater = self._make_updater()
        # 强制 importlib.import_module 抛异常，确保 data_manager 保持 None
        with patch('importlib.import_module', side_effect=ImportError("no module")):
            updater.initialize_data_manager()
        assert updater.data_manager is None

    def test_initialize_interface_failure_leaves_none(self):
        updater = self._make_updater()
        # 让 UnifiedDataInterface 构造抛异常，interface 保持 None
        with patch('data_manager.unified_data_interface.UnifiedDataInterface',
                   side_effect=Exception("connection refused")):
            updater.initialize_interface()
        assert updater.interface is None

    def test_initialize_interface_already_set_no_op(self):
        updater = self._make_updater()
        mock_iface = MagicMock()
        updater.interface = mock_iface
        updater.initialize_interface()
        # interface should not be replaced
        assert updater.interface is mock_iface


# ---------------------------------------------------------------------------
# AutoDataUpdater – update_single_stock with mocked interface
# ---------------------------------------------------------------------------

class TestUpdateSingleStockWithInterface:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            updater = AutoDataUpdater(':memory:')
            return updater

    def test_update_with_empty_plan_returns_no_update(self):
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.build_incremental_plan.return_value = []
        updater.interface = mock_iface
        result = updater.update_single_stock('600000.SH')
        assert isinstance(result, dict)
        assert result['success'] is False

    def test_update_with_skip_plan_returns_no_update(self):
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.build_incremental_plan.return_value = [{'mode': 'skip'}]
        updater.interface = mock_iface
        result = updater.update_single_stock('600000.SH')
        assert result['success'] is False

    def test_update_with_exception_returns_failure(self):
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.build_incremental_plan.side_effect = Exception("network error")
        updater.interface = mock_iface
        result = updater.update_single_stock('600000.SH')
        assert result['success'] is False
        assert 'message' in result


# ---------------------------------------------------------------------------
# AutoDataUpdater – start / stop / get_status
# ---------------------------------------------------------------------------

class TestStartStop:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_start_sets_running_true(self):
        updater = self._make_updater()
        updater.start()
        assert updater.running is True
        updater.stop()

    def test_start_creates_thread(self):
        updater = self._make_updater()
        updater.start()
        assert updater.thread is not None
        updater.stop()

    def test_start_idempotent(self):
        updater = self._make_updater()
        updater.start()
        t1 = updater.thread
        updater.start()  # second call is no-op when already running
        assert updater.thread is t1
        updater.stop()

    def test_stop_sets_running_false(self):
        updater = self._make_updater()
        updater.start()
        updater.stop()
        assert updater.running is False

    def test_stop_clears_thread(self):
        updater = self._make_updater()
        updater.start()
        updater.stop()
        assert updater.thread is None


class TestGetStatus:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_get_status_returns_dict(self):
        updater = self._make_updater()
        status = updater.get_status()
        assert isinstance(status, dict)

    def test_get_status_has_running_key(self):
        updater = self._make_updater()
        status = updater.get_status()
        assert 'running' in status
        assert status['running'] is False

    def test_get_status_has_expected_keys(self):
        updater = self._make_updater()
        status = updater.get_status()
        assert 'update_time' in status
        assert 'total_updates' in status
        assert 'last_update' in status


# ---------------------------------------------------------------------------
# AutoDataUpdater – run_update_task
# ---------------------------------------------------------------------------

class TestRunUpdateTask:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_run_update_task_no_update_needed(self):
        updater = self._make_updater()
        updater.should_update_today = lambda: False
        # Should not raise
        updater.run_update_task()

    def test_run_update_task_handles_exception(self):
        updater = self._make_updater()
        updater.should_update_today = MagicMock(side_effect=Exception("error"))
        # Should catch exception, not raise
        updater.run_update_task()


# ---------------------------------------------------------------------------
# AutoDataUpdater._get_all_stock_codes
# ---------------------------------------------------------------------------

class TestGetAllStockCodes:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_returns_codes_from_db(self):
        import pandas as pd
        updater = self._make_updater()
        mock_df = pd.DataFrame({'stock_code': ['000001.SZ', '600000.SH']})
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchdf.return_value = mock_df
        mock_mgr = MagicMock()
        mock_mgr.get_read_connection.return_value.__enter__ = lambda s: mock_con
        mock_mgr.get_read_connection.return_value.__exit__ = MagicMock(return_value=False)
        with patch('data_manager.duckdb_connection_pool.get_db_manager', return_value=mock_mgr):
            codes = updater._get_all_stock_codes()
        assert '000001.SZ' in codes
        assert '600000.SH' in codes

    def test_falls_back_to_board_loader_when_db_empty(self):
        import pandas as pd
        updater = self._make_updater()
        mock_df = pd.DataFrame({'stock_code': []})
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchdf.return_value = mock_df
        mock_mgr = MagicMock()
        mock_mgr.get_read_connection.return_value.__enter__ = lambda s: mock_con
        mock_mgr.get_read_connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_loader = MagicMock()
        mock_loader.get_board_stocks.return_value = ['300001.SZ', '300002.SZ']
        with patch('data_manager.duckdb_connection_pool.get_db_manager', return_value=mock_mgr), \
             patch('importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.BoardStocksLoader.return_value = mock_loader
            mock_import.return_value = mock_module
            codes = updater._get_all_stock_codes()
        assert '300001.SZ' in codes

    def test_returns_empty_on_db_exception(self):
        updater = self._make_updater()
        with patch('data_manager.duckdb_connection_pool.get_db_manager',
                   side_effect=Exception("DB init failed")):
            codes = updater._get_all_stock_codes()
        assert codes == []


# ---------------------------------------------------------------------------
# AutoDataUpdater.update_all_stocks
# ---------------------------------------------------------------------------

class TestUpdateAllStocks:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_no_data_manager_returns_failure(self):
        updater = self._make_updater()
        updater.initialize_data_manager = lambda: None
        updater.data_manager = None
        result = updater.update_all_stocks(['000001.SZ'])
        assert result.get('success') is False

    def test_all_success_counts_correctly(self):
        updater = self._make_updater()
        updater.initialize_data_manager = lambda: None
        updater.data_manager = MagicMock()
        updater.update_single_stock = MagicMock(return_value={
            'success': True, 'stock_code': 'X', 'records': 1, 'message': 'ok'
        })
        with patch('time.sleep'):
            result = updater.update_all_stocks(['000001.SZ', '600000.SH'])
        assert result['total'] == 2
        assert result['success'] == 2
        assert result['failed'] == 0

    def test_partial_failure_counted(self):
        updater = self._make_updater()
        updater.initialize_data_manager = lambda: None
        updater.data_manager = MagicMock()
        call_results = [
            {'success': True, 'stock_code': 'A', 'records': 1, 'message': 'ok'},
            {'success': False, 'stock_code': 'B', 'records': 0, 'message': 'fail'},
        ]
        updater.update_single_stock = MagicMock(side_effect=call_results)
        with patch('time.sleep'):
            result = updater.update_all_stocks(['000001.SZ', '600000.SH'])
        assert result['total'] == 2
        assert result['success'] == 1
        assert result['failed'] == 1
        assert updater.last_update_status == 'partial'

    def test_increments_total_updates(self):
        updater = self._make_updater()
        updater.initialize_data_manager = lambda: None
        updater.data_manager = MagicMock()
        updater.update_single_stock = MagicMock(return_value={
            'success': True, 'stock_code': 'X', 'records': 0, 'message': ''
        })
        assert updater.total_updates == 0
        with patch('time.sleep'):
            updater.update_all_stocks(['000001.SZ'])
        assert updater.total_updates == 1

    def test_uses_provided_stock_codes(self):
        updater = self._make_updater()
        updater.initialize_data_manager = lambda: None
        updater.data_manager = MagicMock()
        called_with = []

        def _fake_update(code):
            called_with.append(code)
            return {'success': True, 'stock_code': code, 'records': 0, 'message': ''}

        updater.update_single_stock = _fake_update
        with patch('time.sleep'):
            updater.update_all_stocks(['AAA', 'BBB'])
        assert called_with == ['AAA', 'BBB']


# ---------------------------------------------------------------------------
# AutoDataUpdater.manual_update
# ---------------------------------------------------------------------------

class TestManualUpdate:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_manual_update_routes_to_update_all_stocks(self):
        updater = self._make_updater()
        updater.initialize_data_manager = lambda: None
        updater.data_manager = MagicMock()
        updater.update_single_stock = MagicMock(return_value={
            'success': True, 'stock_code': 'X', 'records': 0, 'message': ''
        })
        with patch('time.sleep'):
            result = updater.manual_update(['000001.SZ'])
        assert 'total' in result
        assert result['total'] == 1

    def test_manual_update_none_codes_fetches_all(self):
        updater = self._make_updater()
        updater._get_all_stock_codes = MagicMock(return_value=['000001.SZ'])
        updater.initialize_data_manager = lambda: None
        updater.data_manager = MagicMock()
        updater.update_single_stock = MagicMock(return_value={
            'success': True, 'stock_code': 'X', 'records': 0, 'message': ''
        })
        with patch('time.sleep'):
            updater.manual_update(None)
        updater._get_all_stock_codes.assert_called_once()


# ---------------------------------------------------------------------------
# AutoDataUpdater._run_quarantine_replay_task
# ---------------------------------------------------------------------------

class TestRunQuarantineReplayTask:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_skips_when_interface_none(self):
        updater = self._make_updater()
        updater.initialize_interface = lambda: None
        updater.interface = None
        # Should return without raising
        updater._run_quarantine_replay_task()

    def test_logs_empty_queue(self):
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.run_quarantine_replay.return_value = {
            'processed': 0, 'succeeded': 0, 'failed': 0, 'dead_letter': 0
        }
        mock_iface.get_quarantine_status_counts.return_value = {'total': 0, 'dead_letter': 0}
        updater.initialize_interface = lambda: None
        updater.interface = mock_iface
        updater._run_quarantine_replay_task()
        mock_iface.run_quarantine_replay.assert_called_once_with(limit=50, max_retries=3)

    def test_logs_successful_replay(self):
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.run_quarantine_replay.return_value = {
            'processed': 10, 'succeeded': 9, 'failed': 1, 'dead_letter': 0
        }
        mock_iface.get_quarantine_status_counts.return_value = {'total': 100, 'dead_letter': 5}
        updater.initialize_interface = lambda: None
        updater.interface = mock_iface
        updater._run_quarantine_replay_task()
        mock_iface.run_quarantine_replay.assert_called_once()

    def test_critical_log_on_dead_letter_threshold_abs(self):
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.run_quarantine_replay.return_value = {
            'processed': 0, 'succeeded': 0, 'failed': 0, 'dead_letter': 0
        }
        # dead_letter=200 exceeds default threshold of 100
        mock_iface.get_quarantine_status_counts.return_value = {'total': 500, 'dead_letter': 200}
        updater.initialize_interface = lambda: None
        updater.interface = mock_iface
        import logging
        with patch.dict('os.environ', {'EASYXT_QUARANTINE_DEADLETTER_WARN': '100'}):
            with patch('data_manager.auto_data_updater.logger') as mock_logger:
                updater._run_quarantine_replay_task()
            mock_logger.critical.assert_called_once()

    def test_handles_get_counts_exception_gracefully(self):
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.run_quarantine_replay.return_value = {
            'processed': 3, 'succeeded': 3, 'failed': 0, 'dead_letter': 0
        }
        mock_iface.get_quarantine_status_counts.side_effect = Exception("DB unavailable")
        updater.initialize_interface = lambda: None
        updater.interface = mock_iface
        # Should not raise
        updater._run_quarantine_replay_task()


# ---------------------------------------------------------------------------
# AutoDataUpdater._run_financial_data_update_task
# ---------------------------------------------------------------------------

class TestRunFinancialDataUpdateTask:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def _mock_today(self, mock_dt, d):
        """Helper: make datetime.now(tz=_SH).date() return d."""
        mock_now = MagicMock()
        mock_now.date.return_value = d
        mock_dt.now.return_value = mock_now

    def test_skips_non_active_month(self):
        from datetime import date as real_date
        updater = self._make_updater()
        with patch('data_manager.auto_data_updater.datetime') as mock_dt:
            self._mock_today(mock_dt, real_date(2023, 3, 15))  # March not in FINANCIAL_MONTHS
            updater._run_financial_data_update_task()
        # If we reach here without side effects, the early return path was hit

    def test_skips_when_interface_none(self):
        from datetime import date as real_date
        updater = self._make_updater()
        updater.initialize_interface = lambda: None
        updater.interface = None
        with patch('data_manager.auto_data_updater.datetime') as mock_dt:
            self._mock_today(mock_dt, real_date(2023, 4, 15))  # April in FINANCIAL_MONTHS
            updater._run_financial_data_update_task()  # Should return early

    def test_tushare_fallback_when_qmt_unavailable(self):
        from datetime import date as real_date
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.qmt_available = False
        updater.initialize_interface = lambda: None
        updater.interface = mock_iface
        updater._get_all_stock_codes = MagicMock(return_value=['000001.SZ', '000002.SZ'])

        mock_saver = MagicMock()
        mock_saver.save_from_tushare.return_value = {'success': True, 'records': 5}

        with patch('data_manager.auto_data_updater.datetime') as mock_dt, \
             patch.dict('os.environ', {
                 'TUSHARE_TOKEN': 'fake_token_for_test',
                 'EASYXT_TUSHARE_TOKEN': '',
             }), \
             patch('data_manager.financial_data_saver.FinancialDataSaver', return_value=mock_saver), \
             patch('data_manager.duckdb_connection_pool.get_db_manager'):
            self._mock_today(mock_dt, real_date(2023, 4, 15))
            updater._run_financial_data_update_task()

        assert mock_saver.save_from_tushare.call_count == 2

    def test_tushare_fallback_skips_when_no_token(self):
        from datetime import date as real_date
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.qmt_available = False
        updater.initialize_interface = lambda: None
        updater.interface = mock_iface

        mock_saver = MagicMock()
        with patch('data_manager.auto_data_updater.datetime') as mock_dt, \
             patch.dict('os.environ', {
                 'TUSHARE_TOKEN': '',
                 'EASYXT_TUSHARE_TOKEN': '',
             }), \
             patch('data_manager.financial_data_saver.FinancialDataSaver', return_value=mock_saver):
            self._mock_today(mock_dt, real_date(2023, 4, 15))
            updater._run_financial_data_update_task()

        # No token → should skip without calling saver
        mock_saver.save_from_tushare.assert_not_called()

    def test_tushare_path_handles_saver_exception(self):
        from datetime import date as real_date
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.qmt_available = False
        updater.initialize_interface = lambda: None
        updater.interface = mock_iface
        updater._get_all_stock_codes = MagicMock(return_value=['000001.SZ'])

        mock_saver = MagicMock()
        mock_saver.save_from_tushare.side_effect = Exception("API timeout")

        with patch('data_manager.auto_data_updater.datetime') as mock_dt, \
             patch.dict('os.environ', {'TUSHARE_TOKEN': 'tok', 'EASYXT_TUSHARE_TOKEN': ''}), \
             patch('data_manager.financial_data_saver.FinancialDataSaver', return_value=mock_saver), \
             patch('data_manager.duckdb_connection_pool.get_db_manager'):
            self._mock_today(mock_dt, real_date(2023, 4, 15))
            # Should NOT raise even if saver throws
            updater._run_financial_data_update_task()

    def test_skips_when_no_stock_codes(self):
        from datetime import date as real_date
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.qmt_available = False
        updater.initialize_interface = lambda: None
        updater.interface = mock_iface
        updater._get_all_stock_codes = MagicMock(return_value=[])

        mock_saver = MagicMock()
        with patch('data_manager.auto_data_updater.datetime') as mock_dt, \
             patch.dict('os.environ', {'TUSHARE_TOKEN': 'tok', 'EASYXT_TUSHARE_TOKEN': ''}), \
             patch('data_manager.financial_data_saver.FinancialDataSaver', return_value=mock_saver):
            self._mock_today(mock_dt, real_date(2023, 4, 15))
            updater._run_financial_data_update_task()

        # No codes → saver never called
        mock_saver.save_from_tushare.assert_not_called()


# ---------------------------------------------------------------------------
# Module-level standalone functions
# ---------------------------------------------------------------------------

class TestModuleLevelHelpers:
    def test_run_audit_chain_check_handles_import_error(self):
        from data_manager.auto_data_updater import _run_audit_chain_check
        with patch('tools.audit_chain_integrity_check.run_integrity_check',
                   side_effect=ImportError("no module")):
            _run_audit_chain_check()  # Should not raise

    def test_run_cross_source_consistency_check_handles_exception(self):
        from data_manager.auto_data_updater import _run_cross_source_consistency_check
        # 让 DuckDB 连接池抛异常，验证外层 try/except 正常兜底
        with patch('data_manager.duckdb_connection_pool.get_db_manager',
                   side_effect=RuntimeError("no connection")):
            _run_cross_source_consistency_check()  # Should not raise

    def test_run_cross_source_consistency_check_logs_alert_on_bad_data(self):
        import pandas as pd
        from data_manager.auto_data_updater import _run_cross_source_consistency_check
        mock_mgr = MagicMock()
        mock_mgr.execute_read_query.return_value = pd.DataFrame(
            {'stock_code': [f'{i:06d}.SH' for i in range(20)]}
        )
        mock_ctrl = MagicMock()
        # 所有标的均超偏差阈值 → alert=True → logger.error
        mock_ctrl.cross_validate_sources.return_value = {
            'consistent': False, 'max_diff_pct': 5.0, 'error': None,
            'akshare_rows': 60, 'duckdb_rows': 60, 'compared_rows': 60,
            'consistency_rate': 0.8,
        }
        with patch('data_manager.duckdb_connection_pool.get_db_manager', return_value=mock_mgr), \
             patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'), \
             patch('gui_app.data_manager_controller.DataManagerController', return_value=mock_ctrl), \
             patch('data_manager.auto_data_updater.logger') as mock_logger:
            _run_cross_source_consistency_check()
        mock_logger.error.assert_called_once()

    def test_run_cross_source_consistency_check_logs_info_on_pass(self):
        import pandas as pd
        from data_manager.auto_data_updater import _run_cross_source_consistency_check
        mock_mgr = MagicMock()
        mock_mgr.execute_read_query.return_value = pd.DataFrame(
            {'stock_code': [f'{i:06d}.SH' for i in range(20)]}
        )
        mock_ctrl = MagicMock()
        # 所有标的数据一致 → alert=False → logger.info
        mock_ctrl.cross_validate_sources.return_value = {
            'consistent': True, 'max_diff_pct': 0.1, 'error': None,
            'akshare_rows': 60, 'duckdb_rows': 60, 'compared_rows': 60,
            'consistency_rate': 1.0,
        }
        with patch('data_manager.duckdb_connection_pool.get_db_manager', return_value=mock_mgr), \
             patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'), \
             patch('gui_app.data_manager_controller.DataManagerController', return_value=mock_ctrl), \
             patch('data_manager.auto_data_updater.logger') as mock_logger:
            _run_cross_source_consistency_check()
        mock_logger.info.assert_called_once()


# ---------------------------------------------------------------------------
# AutoDataUpdater.start – environment validation paths
# ---------------------------------------------------------------------------

class TestStartEnvValidation:
    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_start_raises_on_env_error(self):
        import pytest
        updater = self._make_updater()
        with patch('data_manager.validate_environment',
                   return_value={'duckdb': 'ERROR: file missing'}):
            with pytest.raises(RuntimeError):
                updater.start()
        assert updater.running is False

    def test_start_proceeds_with_warnings(self):
        updater = self._make_updater()
        with patch('data_manager.validate_environment',
                   return_value={'duckdb': 'WARN: size large', 'xtdata': 'OK'}):
            updater.start()
        assert updater.running is True
        updater.stop()

    def test_start_proceeds_when_validate_env_raises(self):
        updater = self._make_updater()
        with patch('data_manager.validate_environment',
                   side_effect=AttributeError("not callable")):
            updater.start()
        assert updater.running is True
        updater.stop()


# ===========================================================================
# Additional coverage: uncovered branches
# ===========================================================================


class TestInitializeDataManagerSuccess:
    """Lines 132-134: initialize_data_manager success path."""

    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_success_stores_data_manager(self):
        updater = self._make_updater()
        mock_dm = MagicMock()
        mock_dm_cls = MagicMock(return_value=mock_dm)
        mock_module = MagicMock()
        mock_module.DataManager = mock_dm_cls
        with patch('importlib.import_module', return_value=mock_module):
            updater.initialize_data_manager()
        assert updater.data_manager is mock_dm


class TestInitializeInterfaceSuccess:
    """Lines 144-145: initialize_interface success path."""

    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_success_stores_interface_and_calls_connect(self):
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface_cls = MagicMock(return_value=mock_iface)
        import data_manager.unified_data_interface as _udi_mod
        with patch.object(_udi_mod, 'UnifiedDataInterface', mock_iface_cls):
            updater.initialize_interface()
        assert updater.interface is mock_iface
        mock_iface.connect.assert_called_once_with(read_only=False)


class TestGetListingDateExceptionPaths:
    """Lines 285-286, 298-299: exception paths in get_listing_date."""

    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_xtquant_import_error_falls_back_to_default(self):
        """Lines 285-286: xtquant import raises → default '1990-01-01'."""
        updater = self._make_updater()
        updater.interface = None
        with patch.dict('os.environ', {'EASYXT_ENABLE_XT_LISTING_DATE': '1'}), \
             patch.dict(sys.modules, {'xtquant': None, 'xtquant.xtdata': None}):
            result = updater.get_listing_date('000001.SZ')
        assert result == '1990-01-01'

    def test_interface_con_execute_raises_falls_back(self):
        """Lines 298-299: interface.con.execute() raises → default '1990-01-01'."""
        updater = self._make_updater()
        mock_con = MagicMock()
        mock_con.execute.side_effect = RuntimeError("db error")
        mock_iface = MagicMock()
        mock_iface.con = mock_con
        updater.interface = mock_iface
        # 同时屏蔽 XTQuant，确保测试路径隔离到 DuckDB fallback 而不依赖本地连接
        with patch.dict(sys.modules, {'xtquant': None, 'xtquant.xtdata': None}):
            result = updater.get_listing_date('000001.SZ')
        assert result == '1990-01-01'


class TestUpdateAllPeriodsForStockException:
    """Lines 379-381: exception in get_stock_data recorded."""

    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_get_stock_data_raises_failure_recorded(self):
        updater = self._make_updater()
        mock_iface = MagicMock()
        mock_iface.get_stock_data.side_effect = RuntimeError("API down")
        updater.interface = mock_iface
        result = updater.update_all_periods_for_stock('000001.SZ', periods=['1d'])
        assert not result['periods']['1d']['success']
        assert 'API down' in result['periods']['1d']['message']


class TestBulkDownloadAutoMode:
    """Lines 423, 426: None periods/stock_codes."""

    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_none_periods_uses_all_periods(self):
        """Line 423: periods=None → ALL_PERIODS."""
        updater = self._make_updater()
        updater.interface = MagicMock()
        with patch.object(updater, 'update_all_periods_for_stock', return_value={
            'stock_code': '000001.SZ', 'success_periods': 1,
            'total_records': 10, 'periods': {}
        }) as mock_update:
            result = updater.bulk_download(stock_codes=['000001.SZ'], periods=None)
        assert result['total_stocks'] == 1
        # Called with ALL_PERIODS
        _, kwargs = mock_update.call_args
        assert kwargs.get('periods') == updater.ALL_PERIODS

    def test_none_stock_codes_fetches_from_db(self):
        """Line 426: stock_codes=None → _get_all_stock_codes() called."""
        updater = self._make_updater()
        updater.interface = MagicMock()
        with patch.object(updater, '_get_all_stock_codes',
                          return_value=['600000.SH']) as mock_get, \
             patch.object(updater, 'update_all_periods_for_stock', return_value={
                 'stock_code': '600000.SH', 'success_periods': 0,
                 'total_records': 0, 'periods': {}
             }):
            result = updater.bulk_download(stock_codes=None, periods=['1d'])
        mock_get.assert_called_once()
        assert result['total_stocks'] == 1


class TestBulkDownloadFailureAndProgress:
    """Lines 466-467, 472-473: stock failure + on_progress exception."""

    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_failed_stock_increments_failed_count(self):
        """Lines 466-467: success_periods=0 → failed_stocks incremented."""
        updater = self._make_updater()
        updater.interface = MagicMock()
        with patch.object(updater, 'update_all_periods_for_stock', return_value={
            'stock_code': '000001.SZ', 'success_periods': 0,
            'total_records': 0, 'periods': {}
        }):
            result = updater.bulk_download(
                stock_codes=['000001.SZ'], periods=['1d'])
        assert result['failed_stocks'] == 1
        assert result['success_stocks'] == 0

    def test_on_progress_exception_swallowed(self):
        """Lines 472-473: on_progress raises → swallowed."""
        updater = self._make_updater()
        updater.interface = MagicMock()

        def bad_progress(*args):
            raise RuntimeError("progress error")

        with patch.object(updater, 'update_all_periods_for_stock', return_value={
            'stock_code': '000001.SZ', 'success_periods': 1,
            'total_records': 5, 'periods': {}
        }):
            result = updater.bulk_download(
                stock_codes=['000001.SZ'], periods=['1d'],
                on_progress=bad_progress)
        assert result['success_stocks'] == 1


class TestBulkDownloadSignalBusFails:
    """Lines 482-483: signal_bus emit fails → swallowed."""

    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_signal_bus_emit_failure_swallowed(self):
        updater = self._make_updater()
        updater.interface = MagicMock()
        mock_sb = MagicMock()
        mock_sb.emit.side_effect = RuntimeError("bus error")
        mock_signal_bus_mod = MagicMock(signal_bus=mock_sb)
        mock_events_mod = MagicMock()
        with patch.object(updater, 'update_all_periods_for_stock', return_value={
            'stock_code': '000001.SZ', 'success_periods': 1,
            'total_records': 10, 'periods': {}
        }), \
        patch.dict(sys.modules, {
            'core.signal_bus': mock_signal_bus_mod,
            'core.events': mock_events_mod,
        }):
            result = updater.bulk_download(
                stock_codes=['000001.SZ'], periods=['1d'])
        assert result['total_stocks'] == 1

    def test_bulk_download_progress_event_emitted(self):
        """BULK_DOWNLOAD_PROGRESS 每下载一只股票广播一次到 signal_bus。"""
        updater = self._make_updater()
        updater.interface = MagicMock()
        mock_sb = MagicMock()
        mock_signal_bus_mod = MagicMock(signal_bus=mock_sb)
        mock_events = MagicMock()
        mock_events.BULK_DOWNLOAD_PROGRESS = "bulk_download_progress"
        mock_events_mod = MagicMock(Events=mock_events)
        with patch.object(updater, 'update_all_periods_for_stock', return_value={
            'stock_code': '000001.SZ', 'success_periods': 1,
            'total_records': 5, 'periods': {}
        }), \
        patch.dict(sys.modules, {
            'core.signal_bus': mock_signal_bus_mod,
            'core.events': mock_events_mod,
        }):
            updater.bulk_download(stock_codes=['000001.SZ'], periods=['1d'])
        # 至少有一次 emit 调用以 BULK_DOWNLOAD_PROGRESS 为第一参数
        progress_calls = [
            c for c in mock_sb.emit.call_args_list
            if c.args and c.args[0] == "bulk_download_progress"
        ]
        assert len(progress_calls) >= 1
        call_kw = progress_calls[0].kwargs
        assert call_kw.get("current") == 1
        assert call_kw.get("total") == 1
        assert call_kw.get("stock_code") == "000001.SZ"


class TestSaveCheckpointException:
    """Lines 523-528: json.dump fails → no crash."""

    def _make_updater(self, tmp_path):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=str(tmp_path / 'db.ddb')):
            from data_manager.auto_data_updater import AutoDataUpdater
            upd = AutoDataUpdater(str(tmp_path / 'db.ddb'))
        upd._checkpoint_path = tmp_path / 'checkpoint.json'
        return upd

    def test_json_dump_fails_no_raise(self, tmp_path):
        updater = self._make_updater(tmp_path)
        with patch('json.dump', side_effect=OSError("disk full")):
            updater._save_checkpoint('2026-01-01', 0, 10, 5, 1, [])
        # Must not raise


class TestLoadCheckpointException:
    """Lines 538-539: file corrupt → return {}."""

    def _make_updater(self, tmp_path):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=str(tmp_path / 'db.ddb')):
            from data_manager.auto_data_updater import AutoDataUpdater
            upd = AutoDataUpdater(str(tmp_path / 'db.ddb'))
        upd._checkpoint_path = tmp_path / 'checkpoint.json'
        return upd

    def test_corrupt_checkpoint_returns_empty_dict(self, tmp_path):
        updater = self._make_updater(tmp_path)
        updater._checkpoint_path.write_text("not valid json!!!{")
        result = updater._load_checkpoint('2026-01-01')
        assert result == {}


class TestUpdateAllStocksCheckpointSave:
    """Line 611: auto mode (stock_codes=None) saves checkpoint."""

    def _make_updater(self, tmp_path):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=str(tmp_path / 'db.ddb')):
            from data_manager.auto_data_updater import AutoDataUpdater
            upd = AutoDataUpdater(str(tmp_path / 'db.ddb'))
        upd._checkpoint_path = tmp_path / 'checkpoint.json'
        return upd

    def test_auto_mode_saves_checkpoint(self, tmp_path):
        updater = self._make_updater(tmp_path)
        updater.data_manager = MagicMock()  # Skip initialize_data_manager
        save_calls = []
        original_save = updater._save_checkpoint

        def tracking_save(*args, **kwargs):
            save_calls.append(True)
            return original_save(*args, **kwargs)

        updater._save_checkpoint = tracking_save
        with patch.object(updater, '_get_all_stock_codes',
                          return_value=['000001.SZ']), \
             patch.object(updater, 'update_single_stock',
                          return_value={'success': True, 'records': 5}):
            result = updater.update_all_stocks(stock_codes=None)
        assert len(save_calls) >= 1
        assert result['success'] == 1


class TestGetAllStockCodesEdgeCases:
    """Line 659: board loader returns non-list → return []."""

    def _make_updater(self):
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'):
            from data_manager.auto_data_updater import AutoDataUpdater
            return AutoDataUpdater(':memory:')

    def test_non_list_fresh_codes_returns_empty(self):
        import pandas as pd
        updater = self._make_updater()
        mock_manager = MagicMock()
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchdf.return_value = pd.DataFrame(
            {'stock_code': []})
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=mock_con)
        ctx.__exit__ = MagicMock(return_value=False)
        mock_manager.get_read_connection.return_value = ctx

        mock_loader = MagicMock()
        mock_loader.get_board_stocks.return_value = {'invalid': 'dict'}  # not a list
        mock_loader_cls = MagicMock(return_value=mock_loader)
        mock_module = MagicMock()
        mock_module.BoardStocksLoader = mock_loader_cls

        with patch('data_manager.duckdb_connection_pool.get_db_manager',
                   return_value=mock_manager), \
             patch('importlib.import_module', return_value=mock_module):
            codes = updater._get_all_stock_codes()
        assert codes == []
