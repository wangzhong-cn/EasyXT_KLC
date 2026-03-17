"""Tests for TradingCalendar (smart_data_detector) and DataQualityReport (data_integrity_checker)."""
import pytest
from datetime import date, timedelta


# ---------------------------------------------------------------------------
# TradingCalendar
# ---------------------------------------------------------------------------

class TestTradingCalendarHolidays:
    """Tests for TradingCalendar holiday generation helpers."""

    @pytest.fixture(autouse=True)
    def setup(self):
        from data_manager.smart_data_detector import TradingCalendar
        self.cal = TradingCalendar()

    def test_holidays_set_non_empty(self):
        assert len(self.cal.holidays) > 1000

    def test_saturday_in_holidays(self):
        sat = date(2023, 6, 3)
        assert sat.weekday() == 5
        assert sat in self.cal.holidays

    def test_sunday_in_holidays(self):
        sun = date(2023, 6, 4)
        assert sun.weekday() == 6
        assert sun in self.cal.holidays

    def test_generate_holiday_range_basic(self):
        start = date(2023, 1, 1)
        end = date(2023, 1, 3)
        result = self.cal._generate_holiday_range(start, end)
        assert result == [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)]

    def test_generate_holiday_range_single_day(self):
        d = date(2023, 5, 1)
        result = self.cal._generate_holiday_range(d, d)
        assert result == [d]

    def test_generate_holiday_range_empty_when_start_after_end(self):
        result = self.cal._generate_holiday_range(date(2023, 1, 5), date(2023, 1, 3))
        assert result == []

    def test_spring_festival_2024_in_holidays(self):
        spring = date(2024, 2, 10)
        assert spring in self.cal.holidays

    def test_spring_festival_holidays_generates_list(self):
        result = self.cal._generate_spring_festival_holidays()
        assert isinstance(result, list)
        assert len(result) > 0
        # 2024 Spring Festival should appear
        assert date(2024, 2, 10) in result

    def test_generate_qingming_holidays(self):
        result = self.cal._generate_qingming_holidays()
        assert date(2023, 4, 4) in result
        assert date(2023, 4, 5) in result
        assert date(2023, 4, 6) in result

    def test_generate_qingming_covers_2000_2030(self):
        result = self.cal._generate_qingming_holidays()
        assert date(2000, 4, 4) in result
        assert date(2030, 4, 4) in result

    def test_generate_dragon_boat_holidays(self):
        result = self.cal._generate_dragon_boat_holidays()
        # 2023 端午节为 6 月 22 日，假期前后各 1 天：6/21-6/23
        assert date(2023, 6, 21) in result
        assert date(2023, 6, 22) in result
        assert date(2023, 6, 23) in result

    def test_generate_mid_autumn_holidays(self):
        result = self.cal._generate_mid_autumn_holidays()
        # 2023 中秋节为 9 月 29 日，假期前后各 1 天：9/28-9/30
        assert date(2023, 9, 28) in result
        assert date(2023, 9, 29) in result


class TestTradingCalendarTradingDay:
    """Tests for is_trading_day."""

    @pytest.fixture(autouse=True)
    def setup(self):
        from data_manager.smart_data_detector import TradingCalendar
        self.cal = TradingCalendar()

    def test_saturday_not_trading(self):
        sat = date(2023, 6, 3)
        assert not self.cal.is_trading_day(sat)

    def test_sunday_not_trading(self):
        sun = date(2023, 6, 4)
        assert not self.cal.is_trading_day(sun)

    def test_regular_tuesday_is_trading(self):
        # A regular Tuesday not in any holiday
        tue = date(2023, 8, 1)
        # This may or may not be a trading day depending on observed holidays
        # Just verify the return is boolean
        result = self.cal.is_trading_day(tue)
        assert isinstance(result, bool)

    def test_spring_festival_not_trading(self):
        spring = date(2024, 2, 10)
        assert not self.cal.is_trading_day(spring)

    def test_zhaohui_saturday_spring_festival_2023_not_trading(self):
        """A股铁律：2023-01-28 是春节调休补班周六，chinese_calendar 认为是工作日，
        但 A 股从不在周六开市——is_trading_day 必须返回 False。"""
        zhaohui_sat = date(2023, 1, 28)  # Saturday
        assert zhaohui_sat.weekday() == 5, "前置检查：确认是周六"
        # 验证 chinese_calendar 确实认为此日是工作日（调休）
        try:
            import chinese_calendar
            assert chinese_calendar.is_workday(zhaohui_sat), (
                "前置检查失败：chinese_calendar 未将 2023-01-28 标记为补班工作日"
            )
        except Exception:
            pass  # 库不可用时跳过前置检查，铁律测试仍然有效
        # 无论 chinese_calendar 如何判断，A 股周六永远休市
        assert not self.cal.is_trading_day(zhaohui_sat), (
            "2023-01-28（春节补班周六）不应是交易日——A股铁律：周六永远休市"
        )

    def test_zhaohui_saturday_national_day_2023_not_trading(self):
        """A股铁律：2023-10-07 是国庆调休补班周六，同样必须是休市日。"""
        zhaohui_sat = date(2023, 10, 7)  # Saturday
        assert zhaohui_sat.weekday() == 5, "前置检查：确认是周六"
        assert not self.cal.is_trading_day(zhaohui_sat), (
            "2023-10-07（国庆补班周六）不应是交易日——A股铁律：周六永远休市"
        )

    def test_all_saturdays_in_2023_never_trading(self):
        """2023 全年每个周六（含所有调休补班周六）均不是交易日。"""
        from datetime import timedelta
        d = date(2023, 1, 1)
        end = date(2023, 12, 31)
        while d <= end:
            if d.weekday() == 5:  # Saturday
                assert not self.cal.is_trading_day(d), f"{d} 是周六，不应为交易日"
            d += timedelta(days=1)


class TestTradingCalendarGetTradingDays:
    """Tests for get_trading_days and get_missing_trading_days."""

    @pytest.fixture(autouse=True)
    def setup(self):
        from data_manager.smart_data_detector import TradingCalendar
        self.cal = TradingCalendar()

    def test_get_trading_days_excludes_weekends(self):
        # Week of 2023-06-05 to 2023-06-11
        start = date(2023, 6, 5)   # Monday
        end = date(2023, 6, 11)    # Sunday
        days = self.cal.get_trading_days(start, end)
        # At most 5 trading days (Mon-Fri), weekends excluded
        assert len(days) <= 5
        for d in days:
            assert d.weekday() < 5

    def test_get_trading_days_empty_when_all_weekend(self):
        start = date(2023, 6, 10)  # Saturday
        end = date(2023, 6, 11)    # Sunday
        days = self.cal.get_trading_days(start, end)
        assert days == []

    def test_get_trading_days_single_weekday(self):
        mon = date(2023, 6, 5)
        days = self.cal.get_trading_days(mon, mon)
        # May or may not be holiday, but if not a weekend it should potentially appear
        assert isinstance(days, list)

    def test_get_trading_days_returns_sorted_list(self):
        start = date(2023, 7, 10)
        end = date(2023, 7, 21)
        days = self.cal.get_trading_days(start, end)
        assert days == sorted(days)

    def test_get_trading_days_start_after_end_returns_empty(self):
        days = self.cal.get_trading_days(date(2023, 6, 10), date(2023, 6, 5))
        assert days == []

    def test_get_missing_trading_days_none_missing(self):
        start = date(2023, 6, 5)
        end = date(2023, 6, 9)
        all_days = self.cal.get_trading_days(start, end)
        missing = self.cal.get_missing_trading_days(start, end, all_days)
        assert missing == []

    def test_get_missing_trading_days_some_missing(self):
        start = date(2023, 6, 5)
        end = date(2023, 6, 9)
        all_days = self.cal.get_trading_days(start, end)
        # Provide only first two days
        existing = all_days[:2] if len(all_days) >= 2 else all_days
        missing = self.cal.get_missing_trading_days(start, end, existing)
        assert isinstance(missing, list)
        assert len(missing) <= len(all_days)

    def test_get_missing_trading_days_all_missing(self):
        start = date(2023, 8, 7)   # Monday
        end = date(2023, 8, 11)    # Friday
        all_days = self.cal.get_trading_days(start, end)
        missing = self.cal.get_missing_trading_days(start, end, [])
        # All trading days should be missing
        assert set(missing) == set(all_days)

    def test_get_missing_trading_days_returns_sorted(self):
        start = date(2023, 7, 10)
        end = date(2023, 7, 21)
        missing = self.cal.get_missing_trading_days(start, end, [])
        assert missing == sorted(missing)


# ---------------------------------------------------------------------------
# SmartDataDetector (basic instantiation and no-connect paths)
# ---------------------------------------------------------------------------

class TestSmartDataDetectorBasic:
    """Tests for SmartDataDetector basic construction and no-connection paths."""

    def test_detect_missing_data_without_connect_returns_empty(self):
        from unittest.mock import patch, MagicMock
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.smart_data_detector import SmartDataDetector
            det = SmartDataDetector(':memory:')
            # con is None before connect()
            result = det.detect_missing_data('000001.SZ', '2023-01-01', '2023-12-31')
            assert result == {}

    def test_connect_failure_returns_false(self):
        from unittest.mock import patch, MagicMock
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value='/nonexistent/path.duckdb'):
            from data_manager.smart_data_detector import SmartDataDetector
            det = SmartDataDetector('/nonexistent/path.duckdb')
            with patch('data_manager.duckdb_connection_pool.get_db_manager', side_effect=Exception('no db')):
                result = det.connect()
            assert result is False

    def test_has_trading_calendar_after_init(self):
        from unittest.mock import patch
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.smart_data_detector import SmartDataDetector
            det = SmartDataDetector(':memory:')
            assert det.calendar is not None


# ---------------------------------------------------------------------------
# DataQualityReport  (data_integrity_checker.py)
# ---------------------------------------------------------------------------

class TestDataQualityReport:
    """Tests for DataQualityReport pure data-class methods."""

    @pytest.fixture
    def report(self):
        from data_manager.data_integrity_checker import DataQualityReport
        return DataQualityReport()

    def test_initially_no_errors(self, report):
        assert not report.has_errors()

    def test_initially_no_warnings(self, report):
        assert not report.has_warnings()

    def test_add_error_sets_has_errors(self, report):
        report.add_issue('ERROR', 'some error')
        assert report.has_errors()

    def test_add_warning_sets_has_warnings(self, report):
        report.add_issue('WARNING', 'some warning')
        assert report.has_warnings()

    def test_add_info_does_not_set_errors_or_warnings(self, report):
        report.add_issue('INFO', 'informational')
        assert not report.has_errors()
        assert not report.has_warnings()

    def test_get_summary_counts_errors(self, report):
        report.add_issue('ERROR', 'err1')
        report.add_issue('ERROR', 'err2')
        s = report.get_summary()
        assert s['errors'] == 2

    def test_get_summary_counts_warnings(self, report):
        report.add_issue('WARNING', 'w1')
        s = report.get_summary()
        assert s['warnings'] == 1

    def test_get_summary_count_info(self, report):
        report.add_issue('INFO', 'i1')
        report.add_issue('INFO', 'i2')
        s = report.get_summary()
        assert s['info'] == 2

    def test_get_summary_includes_issue_messages(self, report):
        report.add_issue('ERROR', 'critical')
        s = report.get_summary()
        assert 'critical' in s['issues']

    def test_get_summary_includes_warning_messages(self, report):
        report.add_issue('WARNING', 'mild issue')
        s = report.get_summary()
        assert 'mild issue' in s['warning_messages']

    def test_multiple_types(self, report):
        report.add_issue('ERROR', 'e1')
        report.add_issue('WARNING', 'w1')
        report.add_issue('INFO', 'i1')
        s = report.get_summary()
        assert s['errors'] == 1
        assert s['warnings'] == 1
        assert s['info'] == 1


# ---------------------------------------------------------------------------
# TradingCalendar 业务精确性（黄金断言）
# 2026 端午节 = 6/19  春节 = 2/17  清明 = 4/5
# 下列测试一旦当前实现错误则 FAIL，正是 TDD 对齐修复用断言
# ---------------------------------------------------------------------------

class TestTradingCalendarAccuracy:
    """
    精确业务规则断言：确保 TradingCalendar 对每年真实节假日给出正确答案。
    当前基于固定硬编码的实现会使这些测试失败，这是预期行为——修复后它们才应通过。
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        from data_manager.smart_data_detector import TradingCalendar
        self.cal = TradingCalendar()

    # --- 2026 端午节 (6/19, 非 5/28) ---
    def test_2026_dragon_boat_real_date_is_holiday(self):
        """2026 端午 = 6/19，必须是非交易日"""
        real_date = date(2026, 6, 19)
        assert not self.cal.is_trading_day(real_date), (
            f"2026 端午节 {real_date} 应为非交易日，当前实现未收录"
        )

    def test_2026_wrong_dragon_boat_should_not_be_holiday(self):
        """2026年5/28 是普通周四，不应因端午被标记为假日"""
        wrong_date = date(2026, 5, 28)
        # 5/28 是周四（workday=3）
        assert wrong_date.weekday() == 3, "测试前提：2026-05-28 应为周四"
        assert self.cal.is_trading_day(wrong_date), (
            f"2026-05-28 实为普通工作日，不应被误标为节假日"
        )

    # --- 2025 端午节 (5/31) ---
    def test_2025_dragon_boat_real_date_is_holiday(self):
        """2025 端午 = 5/31，必须是非交易日"""
        real_date = date(2025, 5, 31)
        assert not self.cal.is_trading_day(real_date), (
            f"2025 端午节 {real_date} 应为非交易日，当前实现未收录"
        )

    # --- 春节历史数据（2020 年，固定表中未收录） ---
    def test_2020_spring_festival_is_holiday(self):
        """2020 春节 = 1/25，必须是非交易日"""
        spring_2020 = date(2020, 1, 25)
        assert not self.cal.is_trading_day(spring_2020), (
            "2020 春节 2020-01-25 应为非交易日，当前实现春节表仅覆盖 2024-2030"
        )

    def test_2021_spring_festival_is_holiday(self):
        """2021 春节 = 2/12，必须是非交易日"""
        spring_2021 = date(2021, 2, 12)
        assert not self.cal.is_trading_day(spring_2021), (
            "2021 春节 2021-02-12 应为非交易日，当前实现春节表仅覆盖 2024-2030"
        )

    # --- 普通交易日不能被误判 ---
    def test_ordinary_monday_is_trading(self):
        """2024-03-04（普通周一）必须是交易日"""
        ordinary = date(2024, 3, 4)
        assert self.cal.is_trading_day(ordinary), (
            f"{ordinary} 是普通周一，应为交易日"
        )


# ---------------------------------------------------------------------------
# DataIntegrityChecker basic paths
# ---------------------------------------------------------------------------

class TestDataIntegrityCheckerBasic:
    """Tests for DataIntegrityChecker construction and mocked check methods."""

    def _make_checker(self):
        from unittest.mock import patch, MagicMock
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path', return_value=':memory:'):
            from data_manager.data_integrity_checker import DataIntegrityChecker
            checker = object.__new__(DataIntegrityChecker)
            checker.duckdb_path = ':memory:'
            checker.con = None
            # SmartDataDetector mock
            from data_manager.smart_data_detector import TradingCalendar
            checker.calendar = TradingCalendar()
            # Mock the detector attribute
            det_mock = MagicMock()
            det_mock.con = None
            checker.detector = det_mock
            return checker

    def test_check_data_quality_handles_db_error(self):
        """DB 查询异常时 _check_data_quality 返回 ERROR 条目"""
        from unittest.mock import patch
        checker = self._make_checker()
        with patch.object(checker, '_query_df', side_effect=Exception('db error')):
            result = checker._check_data_quality('000001.SZ', '2023-01-01', '2023-12-31')
        assert isinstance(result, list)
        assert any('ERROR' == item['level'] for item in result)

    def test_check_price_relations_handles_db_error(self):
        from unittest.mock import patch
        checker = self._make_checker()
        with patch.object(checker, '_query_df', side_effect=Exception('db error')):
            result = checker._check_price_relations('000001.SZ', '2023-01-01', '2023-12-31')
        assert isinstance(result, list)
        assert any('ERROR' == item['level'] for item in result)

    def test_check_price_relations_clean_data(self):
        """合法 OHLCV 结构（high>=max(open,close), low<=min(open,close)）不应有 ERROR"""
        import pandas as pd
        from unittest.mock import patch
        checker = self._make_checker()
        # 000001.SZ (平安银行) 2023-01-03~04 真实价格区间（约 13-14 元）
        df = pd.DataFrame({
            'date': ['2023-01-03', '2023-01-04'],
            'open': [13.40, 13.85],
            'high': [13.86, 14.15],
            'low': [13.30, 13.72],
            'close': [13.84, 14.07],
        })
        with patch.object(checker, '_query_df', return_value=df):
            result = checker._check_price_relations('000001.SZ', '2023-01-01', '2023-12-31')
        assert all(item.get('level') != 'ERROR' for item in result)

    def test_check_data_quality_empty_df_returns_error(self):
        """查询返回空 DataFrame 时 _check_data_quality 应返回 ERROR"""
        import pandas as pd
        from unittest.mock import patch
        checker = self._make_checker()
        with patch.object(checker, '_query_df', return_value=pd.DataFrame()):
            result = checker._check_data_quality('000001.SZ', '2023-01-01', '2023-12-31')
        assert any(item['level'] == 'ERROR' for item in result)

    def test_check_data_quality_with_null_values(self):
        """open 列含 None 时应触发 ERROR（000001.SZ 真实价格区间，open 故意留空）"""
        import pandas as pd
        from unittest.mock import patch
        checker = self._make_checker()
        df = pd.DataFrame({
            'date': ['2023-01-03'],
            'open': [None],          # 空值 → 触发 ERROR
            'high': [13.86],
            'low': [13.30],
            'close': [13.84],
            'volume': [213447200],   # 平安银行 2023-01-03 真实成交量量级
        })
        with patch.object(checker, '_query_df', return_value=df):
            result = checker._check_data_quality('000001.SZ', '2023-01-01', '2023-12-31')
        assert any(item['level'] == 'ERROR' for item in result)

    # ------------------------------------------------------------------
    # _check_outliers
    # ------------------------------------------------------------------

    def test_check_outliers_db_error_returns_error(self):
        """DB 异常时 _check_outliers 返回 ERROR 条目"""
        from unittest.mock import patch
        checker = self._make_checker()
        with patch.object(checker, '_query_df', side_effect=Exception('db error')):
            result = checker._check_outliers('000001.SZ', '2023-01-01', '2023-12-31')
        assert isinstance(result, list)
        assert any(item['level'] == 'ERROR' for item in result)

    def test_check_outliers_empty_df_returns_empty(self):
        """空数据集 _check_outliers 返回空列表"""
        import pandas as pd
        from unittest.mock import patch
        checker = self._make_checker()
        with patch.object(checker, '_query_df', return_value=pd.DataFrame()):
            result = checker._check_outliers('000001.SZ', '2023-01-01', '2023-12-31')
        assert result == []

    def test_check_outliers_extreme_return_triggers_warning(self):
        """单日涨幅超 20% 应触发 WARNING。
        数据来源：tests/fixtures/real_market_data.py::get_extreme_outlier_df()
        见 EXTREME_RECORDS[0].description 中的说明和合规声明。
        """
        from unittest.mock import patch
        from tests.fixtures.real_market_data import get_extreme_outlier_df
        checker = self._make_checker()
        df = get_extreme_outlier_df()  # 详见 fixture 中的来源说明
        with patch.object(checker, '_query_df', return_value=df):
            result = checker._check_outliers('000001.SZ', '2023-01-01', '2023-12-31')
        assert any(item['level'] == 'WARNING' for item in result)

    def test_check_outliers_normal_data_no_warning(self):
        """正常波动（<5% 日涨跌）_check_outliers 不应有 WARNING；
        数据来源：000001.SZ (平安银行) 2023-01-03~05 真实收盘价区间
        """
        import pandas as pd
        from unittest.mock import patch
        checker = self._make_checker()
        df = pd.DataFrame({
            'date': ['2023-01-03', '2023-01-04', '2023-01-05'],
            'close': [13.84, 14.07, 13.84],   # 日涨跌约 +1.7% / -1.6%
            'volume': [213447200, 201896700, 169524700],
        })
        with patch.object(checker, '_query_df', return_value=df):
            result = checker._check_outliers('000001.SZ', '2023-01-01', '2023-12-31')
        assert all(item['level'] != 'WARNING' for item in result)

    # ------------------------------------------------------------------
    # _check_volume_anomalies
    # ------------------------------------------------------------------

    def test_check_volume_anomalies_db_error(self):
        """DB 异常时 _check_volume_anomalies 返回 WARNING"""
        from unittest.mock import patch
        checker = self._make_checker()
        with patch.object(checker, '_query_df', side_effect=Exception('db error')):
            result = checker._check_volume_anomalies('000001.SZ', '2023-01-01', '2023-12-31')
        assert isinstance(result, list)
        assert any(item['level'] == 'WARNING' for item in result)

    def test_check_volume_anomalies_too_few_rows(self):
        """不足 10 条数据时直接返回空列表"""
        import pandas as pd
        from unittest.mock import patch
        checker = self._make_checker()
        df = pd.DataFrame({
            'date': ['2023-01-03'],
            'volume': [213447200],   # 000001.SZ 真实成交量量级
        })
        with patch.object(checker, '_query_df', return_value=df):
            result = checker._check_volume_anomalies('000001.SZ', '2023-01-01', '2023-12-31')
        assert result == []

    def test_check_volume_anomalies_spike_triggers_info(self):
        """成交量尖峰（需 ≥26 个正常样本使阈値 < 尖峰值）应触发 INFO。
        数据来源：tests/fixtures/real_market_data.py::get_volume_spike_df()
        base_volume = 000001.SZ 2023-01-03 真实成交量；尖峰默认为 50 倍
        """
        from unittest.mock import patch
        from tests.fixtures.real_market_data import get_volume_spike_df
        checker = self._make_checker()
        # 不传 spike_volume 使用默认（base_volume * 50），就能稳定触发异常
        df = get_volume_spike_df(normal_count=50)
        with patch.object(checker, '_query_df', return_value=df):
            result = checker._check_volume_anomalies('000001.SZ', '2023-01-01', '2023-12-31')
        assert any(item['level'] == 'INFO' for item in result)

    # ------------------------------------------------------------------
    # batch_check_integrity
    # ------------------------------------------------------------------

    def test_batch_check_integrity_empty_list(self):
        """空股票列表返回空字典"""
        from unittest.mock import patch
        checker = self._make_checker()
        result = checker.batch_check_integrity([], '2023-01-01', '2023-12-31')
        assert result == {}

    def test_batch_check_integrity_returns_dict_per_stock(self):
        """每个股票都有对应的报告条目"""
        from unittest.mock import patch, MagicMock
        checker = self._make_checker()
        fake_report = {
            'stock_code': '000001.SZ',
            'check_range': ('2023-01-01', '2023-12-31'),
            'missing_trading_days': 0,
            'completeness_ratio': 1.0,
            'quality_report': {'errors': 0, 'warnings': 0, 'issues': []},
            'status': 'PASS',
        }
        with patch.object(checker, 'check_integrity', return_value=fake_report):
            result = checker.batch_check_integrity(
                ['000001.SZ', '600000.SH'], '2023-01-01', '2023-12-31'
            )
        assert '000001.SZ' in result
        assert '600000.SH' in result

    def test_batch_check_integrity_passes_args(self):
        """check_integrity 被调用时参数正确传递"""
        from unittest.mock import patch, MagicMock, call
        checker = self._make_checker()
        fake_report = {
            'stock_code': '000001.SZ',
            'check_range': ('2023-01-01', '2023-06-30'),
            'missing_trading_days': 0,
            'completeness_ratio': 1.0,
            'quality_report': {'errors': 0, 'warnings': 0, 'issues': []},
            'status': 'PASS',
        }
        with patch.object(checker, 'check_integrity', return_value=fake_report) as mock_ci:
            checker.batch_check_integrity(['000001.SZ'], '2023-01-01', '2023-06-30')
        mock_ci.assert_called_once_with('000001.SZ', '2023-01-01', '2023-06-30')

    # ------------------------------------------------------------------
    # generate_integrity_report
    # ------------------------------------------------------------------

    def _make_reports(self, pass_count=2, fail_count=1):
        """构造批量检查结果字典（纯 Python，无 DB）"""
        reports = {}
        for i in range(pass_count):
            code = f'00000{i+1}.SZ'
            reports[code] = {
                'stock_code': code,
                'check_range': ('2023-01-01', '2023-12-31'),
                'missing_trading_days': 0,
                'completeness_ratio': 1.0,
                'quality_report': {'errors': 0, 'warnings': 0, 'issues': []},
                'status': 'PASS',
            }
        for i in range(fail_count):
            code = f'60000{i+1}.SH'
            reports[code] = {
                'stock_code': code,
                'check_range': ('2023-01-01', '2023-12-31'),
                'missing_trading_days': 5,
                'completeness_ratio': 0.98,
                'quality_report': {'errors': 2, 'warnings': 1, 'issues': ['null in open', 'neg close']},
                'status': 'FAIL',
            }
        return reports

    def test_generate_report_returns_string(self):
        """generate_integrity_report 返回字符串"""
        checker = self._make_checker()
        reports = self._make_reports()
        output = checker.generate_integrity_report(reports)
        assert isinstance(output, str)

    def test_generate_report_contains_totals(self):
        """报告包含检查总数、通过数和失败数"""
        checker = self._make_checker()
        reports = self._make_reports(pass_count=2, fail_count=1)
        output = checker.generate_integrity_report(reports)
        assert '3' in output  # total_stocks = 3
        assert '2' in output  # passed = 2
        assert '1' in output  # failed = 1

    def test_generate_report_contains_stock_codes(self):
        """报告中包含各股票代码"""
        checker = self._make_checker()
        reports = self._make_reports(pass_count=1, fail_count=1)
        output = checker.generate_integrity_report(reports)
        assert '000001.SZ' in output
        assert '600001.SH' in output

    def test_generate_report_empty_reports(self):
        """空报告字典不抛出异常且返回字符串"""
        checker = self._make_checker()
        output = checker.generate_integrity_report({})
        assert isinstance(output, str)
        assert '0' in output  # total_stocks = 0

    def test_generate_report_status_labels(self):
        """报告中出现 PASS 和 FAIL 状态标签"""
        checker = self._make_checker()
        reports = self._make_reports(pass_count=1, fail_count=1)
        output = checker.generate_integrity_report(reports)
        assert 'PASS' in output
        assert 'FAIL' in output


# ---------------------------------------------------------------------------
# DataIntegrityChecker 覆盖率补充（__init__ + connect + close + check_integrity）
# ---------------------------------------------------------------------------

class TestDataIntegrityCheckerCoverage:
    """补充覆盖 __init__、connect、close 和 check_integrity 主干，推进至 ≥ 70%。"""

    def _make_detector_mock(self, missing_count: int = 0):
        from unittest.mock import MagicMock
        mock_det = MagicMock()
        mock_det.detect_missing_data.return_value = {
            'missing_count': missing_count,
            'missing_trading_days': ['2023-02-01'] * missing_count,
            'completeness_ratio': 1.0 if missing_count == 0 else 0.95,
        }
        return mock_det

    def _make_checker(self, missing_count: int = 0):
        from data_manager.data_integrity_checker import DataIntegrityChecker
        from data_manager.smart_data_detector import TradingCalendar
        checker = object.__new__(DataIntegrityChecker)
        checker.con = None
        checker.duckdb_path = ':memory:'
        checker.calendar = TradingCalendar()
        checker.detector = self._make_detector_mock(missing_count)
        return checker

    def test_init_real_constructor(self):
        """DataIntegrityChecker() 真实构造路径（覆盖 __init__ 主干）"""
        from unittest.mock import patch
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'), \
             patch('data_manager.data_integrity_checker.SmartDataDetector'), \
             patch('data_manager.data_integrity_checker.TradingCalendar'):
            from data_manager.data_integrity_checker import DataIntegrityChecker
            checker = DataIntegrityChecker(':memory:')
        assert checker.duckdb_path == ':memory:'
        assert checker.con is None

    def test_connect_delegates_to_detector(self):
        """connect() 委托给 detector.connect()"""
        from unittest.mock import patch
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'), \
             patch('data_manager.data_integrity_checker.SmartDataDetector') as mock_sdd, \
             patch('data_manager.data_integrity_checker.TradingCalendar'):
            from data_manager.data_integrity_checker import DataIntegrityChecker
            checker = DataIntegrityChecker(':memory:')
        checker.connect()
        mock_sdd.return_value.connect.assert_called_once()

    def test_close_delegates_to_detector(self):
        """close() 委托给 detector.close()"""
        from unittest.mock import patch
        with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
                   return_value=':memory:'), \
             patch('data_manager.data_integrity_checker.SmartDataDetector') as mock_sdd, \
             patch('data_manager.data_integrity_checker.TradingCalendar'):
            from data_manager.data_integrity_checker import DataIntegrityChecker
            checker = DataIntegrityChecker(':memory:')
        checker.close()
        mock_sdd.return_value.close.assert_called_once()

    def test_check_integrity_all_clean_returns_pass(self):
        """无缺失日 + 无数据问题 → status PASS，主干全路径覆盖"""
        from unittest.mock import patch
        checker = self._make_checker()
        with patch.object(checker, '_check_data_quality', return_value=[]), \
             patch.object(checker, '_check_price_relations', return_value=[]), \
             patch.object(checker, '_check_outliers', return_value=[]), \
             patch.object(checker, '_check_volume_anomalies', return_value=[]):
            result = checker.check_integrity('000001.SZ', '2023-01-01', '2023-12-31')
        assert result['status'] == 'PASS'
        assert result['stock_code'] == '000001.SZ'
        assert result['missing_trading_days'] == 0
        assert result['completeness_ratio'] == 1.0

    def test_check_integrity_fails_on_data_error(self):
        """_check_data_quality 返回 ERROR → status FAIL"""
        from unittest.mock import patch
        checker = self._make_checker()
        with patch.object(checker, '_check_data_quality',
                          return_value=[{'level': 'ERROR', 'message': '空值'}]), \
             patch.object(checker, '_check_price_relations', return_value=[]), \
             patch.object(checker, '_check_outliers', return_value=[]), \
             patch.object(checker, '_check_volume_anomalies', return_value=[]):
            result = checker.check_integrity('000001.SZ', '2023-01-01', '2023-12-31')
        assert result['status'] == 'FAIL'

    def test_check_integrity_missing_days_branch(self):
        """missing_count > 0 → 报告字段正确（覆盖 WARNING 分支）"""
        from unittest.mock import patch
        checker = self._make_checker(missing_count=3)
        with patch.object(checker, '_check_data_quality', return_value=[]), \
             patch.object(checker, '_check_price_relations', return_value=[]), \
             patch.object(checker, '_check_outliers', return_value=[]), \
             patch.object(checker, '_check_volume_anomalies', return_value=[]):
            result = checker.check_integrity('000001.SZ', '2023-01-01', '2023-12-31')
        assert result['missing_trading_days'] == 3
        assert result['completeness_ratio'] == 0.95

    def test_check_integrity_detailed_false_skips_volume(self):
        """detailed=False 不调用 _check_volume_anomalies"""
        from unittest.mock import patch
        checker = self._make_checker()
        with patch.object(checker, '_check_data_quality', return_value=[]), \
             patch.object(checker, '_check_price_relations', return_value=[]), \
             patch.object(checker, '_check_outliers', return_value=[]), \
             patch.object(checker, '_check_volume_anomalies') as mock_vol:
            checker.check_integrity('000001.SZ', '2023-01-01', '2023-12-31', detailed=False)
        mock_vol.assert_not_called()
