"""Tests for SmartDataDetector and TradingCalendar in data_manager.smart_data_detector."""
import pytest
import pandas as pd
from datetime import date, timedelta
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# TradingCalendar – basic date logic (no network)
# ---------------------------------------------------------------------------

class TestTradingCalendarBasics:
    def _make_calendar_no_network(self):
        """Return a TradingCalendar with an empty _trade_calendar_days (no Tushare/AKShare)."""
        with patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_tushare', return_value=set()), \
             patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_akshare', return_value=set()):
            from data_manager.smart_data_detector import TradingCalendar
            return TradingCalendar()

    def test_weekends_are_not_trading_days(self):
        cal = self._make_calendar_no_network()
        # 2024-01-06 is Saturday
        assert not cal.is_trading_day(date(2024, 1, 6))
        # 2024-01-07 is Sunday
        assert not cal.is_trading_day(date(2024, 1, 7))

    def test_weekday_not_holiday_is_trading_day(self):
        cal = self._make_calendar_no_network()
        # Monday, not a major holiday
        assert cal.is_trading_day(date(2024, 3, 4))

    def test_get_trading_days_returns_list(self):
        cal = self._make_calendar_no_network()
        days = cal.get_trading_days(date(2024, 3, 4), date(2024, 3, 8))
        assert isinstance(days, list)
        # Mon-Fri week → at least 4 days (some might be holidays)
        assert len(days) >= 4

    def test_get_trading_days_excludes_weekends(self):
        cal = self._make_calendar_no_network()
        days = cal.get_trading_days(date(2024, 3, 4), date(2024, 3, 10))
        for d in days:
            assert d.weekday() < 5

    def test_get_missing_trading_days_empty_existing(self):
        cal = self._make_calendar_no_network()
        start = date(2024, 3, 4)
        end = date(2024, 3, 8)
        expected = cal.get_trading_days(start, end)
        missing = cal.get_missing_trading_days(start, end, [])
        assert sorted(missing) == sorted(expected)

    def test_get_missing_trading_days_fully_present(self):
        cal = self._make_calendar_no_network()
        start = date(2024, 3, 4)
        end = date(2024, 3, 8)
        all_days = cal.get_trading_days(start, end)
        missing = cal.get_missing_trading_days(start, end, all_days)
        assert missing == []

    def test_get_missing_trading_days_partial(self):
        cal = self._make_calendar_no_network()
        start = date(2024, 3, 4)  # Monday
        end = date(2024, 3, 8)    # Friday
        all_days = cal.get_trading_days(start, end)
        # Remove one day from existing
        existing = [d for d in all_days if d != date(2024, 3, 6)]
        missing = cal.get_missing_trading_days(start, end, existing)
        if date(2024, 3, 6) in all_days:
            assert date(2024, 3, 6) in missing
        else:
            assert missing == []


class TestTradingCalendarWithTushare:
    def test_uses_tushare_calendar_when_available(self):
        fake_days = {date(2024, 3, 4), date(2024, 3, 5), date(2024, 3, 6)}
        with patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_tushare', return_value=fake_days), \
             patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_akshare', return_value=set()):
            from data_manager.smart_data_detector import TradingCalendar
            cal = TradingCalendar()
        assert cal._trade_calendar_days == fake_days
        # is_trading_day uses _trade_calendar_days (after weekend filter)
        assert cal.is_trading_day(date(2024, 3, 4))
        assert not cal.is_trading_day(date(2024, 3, 1))  # Saturday

    def test_falls_back_to_akshare_when_tushare_empty(self):
        fake_days = {date(2024, 3, 4), date(2024, 3, 5)}
        with patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_tushare', return_value=set()), \
             patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_akshare', return_value=fake_days):
            from data_manager.smart_data_detector import TradingCalendar
            cal = TradingCalendar()
        assert cal._trade_calendar_days == fake_days

    def test_empty_both_sources_uses_builtin_calendar(self):
        with patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_tushare', return_value=set()), \
             patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_akshare', return_value=set()):
            from data_manager.smart_data_detector import TradingCalendar
            cal = TradingCalendar()
        assert cal._trade_calendar_days == set()
        # Fallback to chinese_calendar / built-in: weekday not holiday
        assert cal.is_trading_day(date(2024, 3, 4))

    def test_get_trading_days_uses_trade_calendar_days(self):
        fake_days = {date(2024, 3, 4), date(2024, 3, 5), date(2024, 3, 6),
                     date(2024, 3, 7), date(2024, 3, 8), date(2024, 3, 11)}
        with patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_tushare', return_value=fake_days), \
             patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_akshare', return_value=set()):
            from data_manager.smart_data_detector import TradingCalendar
            cal = TradingCalendar()
        result = cal.get_trading_days(date(2024, 3, 4), date(2024, 3, 8))
        # Should only return days that are in fake_days and within range
        assert set(result) == {date(2024, 3, 4), date(2024, 3, 5),
                                date(2024, 3, 6), date(2024, 3, 7), date(2024, 3, 8)}


class TestTradingCalendarFromTushare:
    def test_returns_empty_set_when_no_token(self):
        with patch.dict('os.environ', {'EASYXT_TUSHARE_TOKEN': '', 'TUSHARE_TOKEN': ''}):
            with patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_tushare', return_value=set()), \
                 patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_akshare', return_value=set()):
                from data_manager.smart_data_detector import TradingCalendar
                cal = TradingCalendar()
            result = cal._load_trade_calendar_from_tushare()
        assert result == set()

    def test_returns_empty_set_on_tushare_exception(self):
        with patch.dict('os.environ', {'TUSHARE_TOKEN': 'fake_token', 'EASYXT_TUSHARE_TOKEN': ''}):
            with patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_tushare', return_value=set()), \
                 patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_akshare', return_value=set()):
                from data_manager.smart_data_detector import TradingCalendar
                cal = TradingCalendar()
            with patch('tushare.pro_api', side_effect=Exception("API error")), \
                 patch('tushare.set_token'):
                result = cal._load_trade_calendar_from_tushare()
        assert result == set()

    def test_returns_empty_set_on_akshare_exception(self):
        with patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_tushare', return_value=set()), \
             patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_akshare', return_value=set()):
            from data_manager.smart_data_detector import TradingCalendar
            cal = TradingCalendar()
        with patch('akshare.tool_trade_date_hist_sina', side_effect=Exception("network error")):
            result = cal._load_trade_calendar_from_akshare()
        assert result == set()


# ---------------------------------------------------------------------------
# SmartDataDetector – setup helper
# ---------------------------------------------------------------------------

def _make_detector():
    """Create a SmartDataDetector with patched DuckDB path (no real file)."""
    with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'), \
         patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_tushare', return_value=set()), \
         patch('data_manager.smart_data_detector.TradingCalendar._load_trade_calendar_from_akshare', return_value=set()):
        from data_manager.smart_data_detector import SmartDataDetector
        detector = SmartDataDetector(':memory:')
    return detector


# ---------------------------------------------------------------------------
# SmartDataDetector.connect
# ---------------------------------------------------------------------------

class TestSmartDataDetectorConnect:
    def test_connect_succeeds_with_valid_manager(self):
        detector = _make_detector()
        mock_mgr = MagicMock()
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=MagicMock())
        cm.__exit__ = MagicMock(return_value=False)
        mock_mgr.get_read_connection.return_value = cm
        with patch('data_manager.duckdb_connection_pool.get_db_manager', return_value=mock_mgr):
            result = detector.connect()
        assert result is True
        assert detector.con is True

    def test_connect_fails_on_exception(self):
        detector = _make_detector()
        with patch('data_manager.duckdb_connection_pool.get_db_manager', side_effect=Exception("no db")):
            result = detector.connect()
        assert result is False
        assert detector._manager is None


# ---------------------------------------------------------------------------
# SmartDataDetector.detect_missing_data
# ---------------------------------------------------------------------------

class TestDetectMissingData:
    def _make_connected_detector(self, query_result_df=None):
        detector = _make_detector()
        detector.con = True  # mark as connected
        mock_mgr = MagicMock()
        mock_con = MagicMock()
        if query_result_df is None:
            query_result_df = pd.DataFrame()
        mock_con.execute.return_value.df.return_value = query_result_df
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=mock_con)
        cm.__exit__ = MagicMock(return_value=False)
        mock_mgr.get_read_connection.return_value = cm
        detector._manager = mock_mgr
        return detector

    def test_returns_empty_when_not_connected(self):
        detector = _make_detector()
        # con is None by default
        result = detector.detect_missing_data('000001.SZ', '2024-01-01', '2024-01-31')
        assert result == {}

    def test_returns_empty_on_invalid_start_date(self):
        detector = self._make_connected_detector()
        result = detector.detect_missing_data('000001.SZ', 'not-a-date', '2024-01-31')
        assert result == {}

    def test_returns_empty_on_reversed_dates(self):
        detector = self._make_connected_detector()
        result = detector.detect_missing_data('000001.SZ', '2024-01-31', '2024-01-01')
        assert result == {}

    def test_returns_report_when_no_existing_data(self):
        detector = self._make_connected_detector(pd.DataFrame())
        result = detector.detect_missing_data('000001.SZ', '2024-03-04', '2024-03-08')
        assert result['stock_code'] == '000001.SZ'
        assert result['existing_data']['count'] == 0
        assert result['missing_count'] > 0
        assert result['completeness_ratio'] == 0.0

    def test_returns_report_with_existing_data(self):
        dates = pd.date_range('2024-03-04', '2024-03-08', freq='B')
        df = pd.DataFrame({'date': dates})
        detector = self._make_connected_detector(df)
        result = detector.detect_missing_data('000001.SZ', '2024-03-04', '2024-03-08')
        assert result['stock_code'] == '000001.SZ'
        assert result['existing_data']['count'] == len(dates)
        assert isinstance(result['completeness_ratio'], float)

    def test_completeness_ratio_is_one_when_complete(self):
        # 使用内置日历，查询一个短范围并返回所有交易日
        detector = self._make_connected_detector()
        # Populate detector calendar with known days
        fake_days = {date(2024, 3, 4), date(2024, 3, 5), date(2024, 3, 6),
                     date(2024, 3, 7), date(2024, 3, 8)}
        detector.calendar._trade_calendar_days = fake_days
        dates = pd.DataFrame({'date': pd.to_datetime(sorted(fake_days))})
        # Replace mock to return all days
        mock_mgr = MagicMock()
        mock_con = MagicMock()
        mock_con.execute.return_value.df.return_value = dates
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=mock_con)
        cm.__exit__ = MagicMock(return_value=False)
        mock_mgr.get_read_connection.return_value = cm
        detector._manager = mock_mgr
        result = detector.detect_missing_data('000001.SZ', '2024-03-04', '2024-03-08')
        assert result['completeness_ratio'] == pytest.approx(1.0)
        assert result['missing_count'] == 0

    def test_handles_query_exception(self):
        detector = _make_detector()
        detector.con = True
        mock_mgr = MagicMock()
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("query error")
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=mock_con)
        cm.__exit__ = MagicMock(return_value=False)
        mock_mgr.get_read_connection.return_value = cm
        detector._manager = mock_mgr
        result = detector.detect_missing_data('000001.SZ', '2024-03-04', '2024-03-08')
        assert result == {}

    def test_manager_none_returns_empty(self):
        detector = _make_detector()
        detector.con = True
        detector._manager = None
        result = detector.detect_missing_data('000001.SZ', '2024-03-04', '2024-03-08')
        assert result == {}


# ---------------------------------------------------------------------------
# SmartDataDetector._group_continuous_dates
# ---------------------------------------------------------------------------

class TestGroupContinuousDates:
    def _detector(self):
        return _make_detector()

    def test_empty_input(self):
        d = self._detector()
        assert d._group_continuous_dates([]) == []

    def test_single_date(self):
        d = self._detector()
        result = d._group_continuous_dates([date(2024, 3, 4)])
        assert len(result) == 1
        assert result[0]['days'] == 1

    def test_consecutive_dates_form_one_segment(self):
        d = self._detector()
        dates = [date(2024, 3, 4), date(2024, 3, 5), date(2024, 3, 6)]
        result = d._group_continuous_dates(dates)
        assert len(result) == 1
        assert result[0]['start'] == date(2024, 3, 4)
        assert result[0]['end'] == date(2024, 3, 6)
        assert result[0]['days'] == 3

    def test_gap_creates_two_segments(self):
        d = self._detector()
        # Gap between Mar 8 and Mar 18 (10 days)
        dates = [date(2024, 3, 4), date(2024, 3, 5), date(2024, 3, 18), date(2024, 3, 19)]
        result = d._group_continuous_dates(dates)
        assert len(result) == 2
        assert result[0]['start'] == date(2024, 3, 4)
        assert result[1]['start'] == date(2024, 3, 18)


# ---------------------------------------------------------------------------
# SmartDataDetector.batch_detect_missing
# ---------------------------------------------------------------------------

class TestBatchDetectMissing:
    def test_returns_dict_keyed_by_stock_code(self):
        detector = _make_detector()
        detector.con = True
        mock_mgr = MagicMock()
        mock_con = MagicMock()
        mock_con.execute.return_value.df.return_value = pd.DataFrame()
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=mock_con)
        cm.__exit__ = MagicMock(return_value=False)
        mock_mgr.get_read_connection.return_value = cm
        detector._manager = mock_mgr
        codes = ['000001.SZ', '600519.SH']
        result = detector.batch_detect_missing(codes, '2024-03-04', '2024-03-08')
        assert set(result.keys()) == set(codes)

    def test_empty_stock_list_returns_empty_dict(self):
        detector = _make_detector()
        result = detector.batch_detect_missing([], '2024-03-04', '2024-03-08')
        assert result == {}


# ---------------------------------------------------------------------------
# SmartDataDetector.get_download_plan
# ---------------------------------------------------------------------------

class TestGetDownloadPlan:
    def _make_detector_with_reports(self, report_factory):
        detector = _make_detector()
        detector.batch_detect_missing = report_factory
        return detector

    def test_plan_structure(self):
        def mock_batch(codes, start, end):
            return {
                '000001.SZ': {'missing_count': 3, 'missing_segments': [{'start': date(2024, 3, 4), 'end': date(2024, 3, 6), 'days': 3}]},
                '600519.SH': {'missing_count': 0, 'missing_segments': []},
            }
        detector = self._make_detector_with_reports(mock_batch)
        plan = detector.get_download_plan(['000001.SZ', '600519.SH'], '2024-03-04', '2024-03-08')
        assert plan['total_stocks'] == 2
        assert plan['stocks_with_missing_data'] == 1
        assert plan['total_missing_days'] == 3
        assert len(plan['download_tasks']) == 1
        assert plan['download_tasks'][0]['stock_code'] == '000001.SZ'

    def test_plan_all_complete_no_tasks(self):
        def mock_batch(codes, start, end):
            return {c: {'missing_count': 0, 'missing_segments': []} for c in codes}
        detector = self._make_detector_with_reports(mock_batch)
        plan = detector.get_download_plan(['000001.SZ'], '2024-03-04', '2024-03-08')
        assert plan['stocks_with_missing_data'] == 0
        assert plan['total_missing_days'] == 0
        assert plan['download_tasks'] == []

    def test_plan_empty_stock_list(self):
        def mock_batch(codes, start, end):
            return {}
        detector = self._make_detector_with_reports(mock_batch)
        plan = detector.get_download_plan([], '2024-03-04', '2024-03-08')
        assert plan['total_stocks'] == 0
        assert plan['download_tasks'] == []


class TestReportPrinting:
    def test_print_missing_report_still_prints_when_detector_default_silent(self, capsys):
        detector = _make_detector()
        detector._stdout_enabled = False
        detector._logger = MagicMock()
        report = {
            'stock_code': '000001.SZ',
            'check_range': ('2024-03-04', '2024-03-08'),
            'expected_trading_days': 5,
            'existing_data': {
                'first_date': None,
                'last_date': None,
                'count': 0,
                'dates': [],
            },
            'missing_trading_days': [],
            'missing_segments': [],
            'missing_count': 0,
            'completeness_ratio': 1.0,
        }

        detector.print_missing_report(report)

        output = capsys.readouterr().out
        assert '数据缺失报告' in output
        assert '000001.SZ' in output
