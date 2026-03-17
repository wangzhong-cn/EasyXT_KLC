"""Tests for FinancialDataSaver – pure data-preparation and _format_timetag paths."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, call


def _make_mock_db():
    """Return a mock db_manager that accepts all write queries."""
    mock = MagicMock()
    mock.execute_write_query.return_value = None
    mock.insert_dataframe.return_value = None
    return mock


def _make_saver(mock_db=None):
    """Instantiate FinancialDataSaver with a mock db_manager."""
    if mock_db is None:
        mock_db = _make_mock_db()
    from data_manager.financial_data_saver import FinancialDataSaver
    return FinancialDataSaver(mock_db), mock_db


# ---------------------------------------------------------------------------
# _create_tables (via construction)
# ---------------------------------------------------------------------------

class TestCreateTables:
    def test_constructor_calls_execute_write_query(self):
        saver, mock_db = _make_saver()
        assert mock_db.execute_write_query.called

    def test_creates_income_table(self):
        saver, mock_db = _make_saver()
        calls_text = ' '.join(str(c) for c in mock_db.execute_write_query.call_args_list)
        assert 'financial_income' in calls_text

    def test_creates_balance_table(self):
        saver, mock_db = _make_saver()
        calls_text = ' '.join(str(c) for c in mock_db.execute_write_query.call_args_list)
        assert 'financial_balance' in calls_text

    def test_creates_cashflow_table(self):
        saver, mock_db = _make_saver()
        calls_text = ' '.join(str(c) for c in mock_db.execute_write_query.call_args_list)
        assert 'financial_cashflow' in calls_text


# ---------------------------------------------------------------------------
# _format_timetag
# ---------------------------------------------------------------------------

class TestFormatTimetag:
    @pytest.fixture
    def saver(self):
        s, _ = _make_saver()
        return s

    def test_integer_8_digits(self, saver):
        result = saver._format_timetag(20230103)
        assert result == '2023-01-03'

    def test_float_8_digits(self, saver):
        result = saver._format_timetag(20231231.0)
        assert result == '2023-12-31'

    def test_string_iso_date(self, saver):
        result = saver._format_timetag('2023-06-15')
        assert result == '2023-06-15'

    def test_nan_returns_none(self, saver):
        result = saver._format_timetag(float('nan'))
        assert result is None

    def test_string_longer_than_10_truncated(self, saver):
        result = saver._format_timetag('2023-01-01 09:30:00')
        assert result == '2023-01-01'


# ---------------------------------------------------------------------------
# _prepare_income_data
# ---------------------------------------------------------------------------

class TestPrepareIncomeData:
    @pytest.fixture
    def saver(self):
        s, _ = _make_saver()
        return s

    def _make_income_df(self, n=2):
        return pd.DataFrame({
            'm_timetag': [20230331, 20221231],
            'm_anntime': [20230420, 20230130],
            'revenue': [1e9, 9e8],
            'operating_revenue': [1e9, 9e8],
            'total_operating_cost': [8e8, 7e8],
            'net_profit_incl_min_int_inc': [2e8, 1.8e8],
            'net_profit_excl_min_int_inc': [1.9e8, 1.7e8],
            'oper_profit': [2.1e8, 1.9e8],
            'tot_profit': [2.2e8, 2.0e8],
            's_fa_eps_basic': [0.5, 0.45],
            's_fa_eps_diluted': [0.49, 0.44],
        })[:n]

    def test_returns_list_of_dicts(self, saver):
        df = self._make_income_df()
        result = saver._prepare_income_data('000001.SZ', df)
        assert isinstance(result, list)
        assert len(result) == 2

    def test_stock_code_in_each_record(self, saver):
        df = self._make_income_df()
        result = saver._prepare_income_data('600000.SH', df)
        for rec in result:
            assert rec['stock_code'] == '600000.SH'

    def test_report_date_formatted(self, saver):
        df = self._make_income_df(1)
        result = saver._prepare_income_data('000001.SZ', df)
        assert result[0]['report_date'] == '2023-03-31'

    def test_data_source_is_qmt(self, saver):
        df = self._make_income_df(1)
        result = saver._prepare_income_data('000001.SZ', df)
        assert result[0]['data_source'] == 'QMT'

    def test_gross_profit_calculated(self, saver):
        df = self._make_income_df(1)
        result = saver._prepare_income_data('000001.SZ', df)
        expected_gp = 1e9 - 8e8
        assert result[0]['gross_profit'] == pytest.approx(expected_gp)

    def test_nan_timetag_skips_row(self, saver):
        df = pd.DataFrame({
            'm_timetag': [float('nan'), 20230331],
            'revenue': [1e9, 9e8],
        })
        result = saver._prepare_income_data('000001.SZ', df)
        # NaN row should be skipped
        assert len(result) <= 1

    def test_net_margin_calculated_when_revenue_positive(self, saver):
        df = self._make_income_df(1)
        result = saver._prepare_income_data('000001.SZ', df)
        assert result[0]['net_margin'] is not None

    def test_net_margin_none_when_revenue_zero(self, saver):
        df = pd.DataFrame({
            'm_timetag': [20230331],
            'revenue': [0],
            'operating_revenue': [0],
        })
        result = saver._prepare_income_data('000001.SZ', df)
        if result:
            assert result[0]['net_margin'] is None

    def test_empty_df_returns_empty_list(self, saver):
        result = saver._prepare_income_data('000001.SZ', pd.DataFrame())
        assert result == []


# ---------------------------------------------------------------------------
# _prepare_balance_data
# ---------------------------------------------------------------------------

class TestPrepareBalanceData:
    @pytest.fixture
    def saver(self):
        s, _ = _make_saver()
        return s

    def _make_balance_df(self):
        return pd.DataFrame({
            'm_timetag': [20230331],
            'm_anntime': [20230420],
            'tot_assets': [5e9],
            'tot_liab': [3e9],
            'total_equity': [2e9],
            'total_current_assets': [2e9],
            'total_current_liability': [1.5e9],
            'fix_assets': [1e9],
            'intang_assets': [2e8],
        })

    def test_returns_list(self, saver):
        df = self._make_balance_df()
        result = saver._prepare_balance_data('000001.SZ', df)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_debt_ratio_calculated(self, saver):
        df = self._make_balance_df()
        result = saver._prepare_balance_data('000001.SZ', df)
        expected = 3e9 / 5e9 * 100
        assert result[0]['debt_to_asset_ratio'] == pytest.approx(expected)

    def test_current_ratio_calculated(self, saver):
        df = self._make_balance_df()
        result = saver._prepare_balance_data('000001.SZ', df)
        expected = 2e9 / 1.5e9
        assert result[0]['current_ratio'] == pytest.approx(expected)

    def test_data_source_is_qmt(self, saver):
        df = self._make_balance_df()
        result = saver._prepare_balance_data('000001.SZ', df)
        assert result[0]['data_source'] == 'QMT'


# ---------------------------------------------------------------------------
# _prepare_cashflow_data
# ---------------------------------------------------------------------------

class TestPrepareCashflowData:
    @pytest.fixture
    def saver(self):
        s, _ = _make_saver()
        return s

    def _make_cashflow_df(self):
        return pd.DataFrame({
            'm_timetag': [20230331],
            'm_anntime': [20230420],
            'net_cash_flows_oper_act': [3e8],
            'net_cash_flows_inv_act': [-1e8],
            'net_cash_flows_fnc_act': [-5e7],
            'cash_cash_equ_beg_period': [1e9],
            'cash_cash_equ_end_period': [1.15e9],
        })

    def test_returns_list(self, saver):
        df = self._make_cashflow_df()
        result = saver._prepare_cashflow_data('000001.SZ', df)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_net_cash_flow_sum(self, saver):
        df = self._make_cashflow_df()
        result = saver._prepare_cashflow_data('000001.SZ', df)
        expected = 3e8 + (-1e8) + (-5e7)
        assert result[0]['net_cash_flow'] == pytest.approx(expected)

    def test_cash_begin_end_stored(self, saver):
        df = self._make_cashflow_df()
        result = saver._prepare_cashflow_data('000001.SZ', df)
        assert result[0]['cash_equivalents_begin'] == pytest.approx(1e9)
        assert result[0]['cash_equivalents_end'] == pytest.approx(1.15e9)


# ---------------------------------------------------------------------------
# save_from_qmt – high level
# ---------------------------------------------------------------------------

class TestSaveFromQmt:
    @pytest.fixture
    def saver_and_db(self):
        return _make_saver()

    def test_none_income_df_returns_success(self, saver_and_db):
        saver, _ = saver_and_db
        result = saver.save_from_qmt('000001.SZ', None)
        assert result['success'] is True
        assert result['income_count'] == 0

    def test_empty_income_df_returns_success(self, saver_and_db):
        saver, _ = saver_and_db
        result = saver.save_from_qmt('000001.SZ', pd.DataFrame())
        assert result['success'] is True

    def test_stock_code_in_result(self, saver_and_db):
        saver, _ = saver_and_db
        result = saver.save_from_qmt('600000.SH', None)
        assert result['stock_code'] == '600000.SH'

    def test_all_none_all_counts_zero(self, saver_and_db):
        saver, _ = saver_and_db
        result = saver.save_from_qmt('000001.SZ', None, None, None)
        assert result['income_count'] == 0
        assert result['balance_count'] == 0
        assert result['cashflow_count'] == 0


# ---------------------------------------------------------------------------
# save_from_tushare – Tushare 降级路径
# ---------------------------------------------------------------------------

class TestSaveFromTushare:
    """Tests for the Tushare fallback path in FinancialDataSaver."""

    @pytest.fixture
    def saver(self):
        s, _ = _make_saver()
        return s

    # ── token 缺失 ──────────────────────────────────────────────────────────

    def test_missing_token_returns_error_not_success(self, saver, monkeypatch):
        monkeypatch.delenv("EASYXT_TUSHARE_TOKEN", raising=False)
        monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
        result = saver.save_from_tushare("000001.SZ")
        assert result["success"] is False
        assert result["error"] is not None
        assert "EASYXT_TUSHARE_TOKEN" in result["error"]

    def test_missing_token_counts_all_zero(self, saver, monkeypatch):
        monkeypatch.delenv("EASYXT_TUSHARE_TOKEN", raising=False)
        monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
        result = saver.save_from_tushare("000001.SZ")
        assert result["income_count"] == 0
        assert result["balance_count"] == 0
        assert result["cashflow_count"] == 0

    def test_result_has_source_field(self, saver, monkeypatch):
        monkeypatch.delenv("EASYXT_TUSHARE_TOKEN", raising=False)
        monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
        result = saver.save_from_tushare("000001.SZ")
        assert result.get("source") == "tushare"

    def test_tushare_unavailable_returns_error(self, saver, monkeypatch):
        monkeypatch.setenv("EASYXT_TUSHARE_TOKEN", "dummy_token")
        # Simulate tushare ImportError by hiding the module
        import sys
        saved = sys.modules.get("tushare")
        sys.modules["tushare"] = None  # type: ignore[assignment]
        try:
            result = saver.save_from_tushare("000001.SZ")
            assert result["success"] is False
            assert result["error"] is not None
        finally:
            if saved is None:
                sys.modules.pop("tushare", None)
            else:
                sys.modules["tushare"] = saved

    # ── Tushare API 桩 ──────────────────────────────────────────────────────

    def _make_ts_income_df(self):
        return pd.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ"],
            "end_date": ["20231231", "20230930"],
            "ann_date": ["20240130", "20231030"],
            "revenue": [1e9, 9.5e8],
            "total_revenue": [1e9, 9.5e8],
            "total_operate_cost": [8e8, 7.5e8],
            "n_income": [2e8, 1.8e8],
            "n_income_attr_p": [1.9e8, 1.7e8],
            "operate_profit": [2.1e8, 1.9e8],
            "total_profit": [2.2e8, 2.0e8],
            "basic_eps": [0.5, 0.45],
            "diluted_eps": [0.49, 0.44],
        })

    def _make_ts_balance_df(self):
        return pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "end_date": ["20231231"],
            "ann_date": ["20240130"],
            "total_assets": [5e9],
            "total_liab": [3e9],
            "total_hldr_eqy_inc_min_int": [2e9],
            "total_cur_assets": [2e9],
            "total_cur_liab": [1.5e9],
            "fix_assets": [1e9],
            "intan_assets": [2e8],
        })

    def _make_ts_cashflow_df(self):
        return pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "end_date": ["20231231"],
            "ann_date": ["20240130"],
            "n_cashflow_act": [3e8],
            "n_cashflow_inv_act": [-1e8],
            "n_cash_flows_fnc_act": [-5e7],
            "c_cash_equ_beg_period": [1e9],
            "c_cash_equ_end_period": [1.15e9],
        })

    def _patch_tushare(self, monkeypatch, income_df, balance_df, cashflow_df):
        """Replace tushare module with a mock pro API."""
        import sys
        mock_pro = MagicMock()
        mock_pro.income.return_value = income_df
        mock_pro.balancesheet.return_value = balance_df
        mock_pro.cashflow.return_value = cashflow_df

        mock_ts = MagicMock()
        mock_ts.pro_api.return_value = mock_pro

        monkeypatch.setenv("EASYXT_TUSHARE_TOKEN", "dummy_token")
        monkeypatch.setitem(sys.modules, "tushare", mock_ts)
        return mock_ts, mock_pro

    def test_calls_pro_income_balancesheet_cashflow(self, saver, monkeypatch):
        _, mock_pro = self._patch_tushare(
            monkeypatch,
            self._make_ts_income_df(),
            self._make_ts_balance_df(),
            self._make_ts_cashflow_df(),
        )
        result = saver.save_from_tushare("000001.SZ")
        mock_pro.income.assert_called_once()
        mock_pro.balancesheet.assert_called_once()
        mock_pro.cashflow.assert_called_once()

    def test_success_when_data_returned(self, saver, monkeypatch):
        self._patch_tushare(
            monkeypatch,
            self._make_ts_income_df(),
            self._make_ts_balance_df(),
            self._make_ts_cashflow_df(),
        )
        result = saver.save_from_tushare("000001.SZ")
        assert result["success"] is True

    def test_income_count_correct(self, saver, monkeypatch):
        self._patch_tushare(
            monkeypatch,
            self._make_ts_income_df(),   # 2 rows
            pd.DataFrame(),
            pd.DataFrame(),
        )
        result = saver.save_from_tushare("000001.SZ")
        assert result["income_count"] == 2

    def test_data_source_field_is_tushare(self, saver, monkeypatch):
        """Records written to DB must carry data_source='Tushare'."""
        _, mock_pro = self._patch_tushare(
            monkeypatch,
            self._make_ts_income_df(),
            pd.DataFrame(),
            pd.DataFrame(),
        )
        saver.save_from_tushare("000001.SZ")
        # Check the DataFrame passed to insert_dataframe
        _, db = _make_saver.__wrapped__() if hasattr(_make_saver, "__wrapped__") else (None, None)
        # Verify via _prepare_income_data_tushare directly
        records = saver._prepare_income_data_tushare("000001.SZ", self._make_ts_income_df())
        assert all(r["data_source"] == "Tushare" for r in records)

    def test_date_format_conversion(self, saver, monkeypatch):
        """Tushare YYYYMMDD → YYYY-MM-DD for report_date."""
        records = saver._prepare_income_data_tushare("000001.SZ", self._make_ts_income_df())
        assert records[0]["report_date"] == "2023-12-31"
        assert records[1]["report_date"] == "2023-09-30"

    def test_announce_date_format_conversion(self, saver, monkeypatch):
        records = saver._prepare_income_data_tushare("000001.SZ", self._make_ts_income_df())
        assert records[0]["announce_date"] == "2024-01-30"

    def test_empty_income_df_still_succeeds_if_balance_has_data(self, saver, monkeypatch):
        self._patch_tushare(
            monkeypatch,
            pd.DataFrame(),
            self._make_ts_balance_df(),
            pd.DataFrame(),
        )
        result = saver.save_from_tushare("000001.SZ")
        assert result["success"] is True
        assert result["income_count"] == 0
        assert result["balance_count"] == 1

    def test_all_empty_dfs_success_false(self, saver, monkeypatch):
        self._patch_tushare(
            monkeypatch, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        )
        result = saver.save_from_tushare("000001.SZ")
        assert result["success"] is False

    def test_balance_debt_ratio_calculated(self, saver):
        records = saver._prepare_balance_data_tushare("000001.SZ", self._make_ts_balance_df())
        expected = 3e9 / 5e9 * 100
        assert records[0]["debt_to_asset_ratio"] == pytest.approx(expected)

    def test_cashflow_net_sum(self, saver):
        records = saver._prepare_cashflow_data_tushare("000001.SZ", self._make_ts_cashflow_df())
        expected = 3e8 + (-1e8) + (-5e7)
        assert records[0]["net_cash_flow"] == pytest.approx(expected)

    def test_ts_date_helper_yyyymmdd(self, saver):
        assert saver._ts_date("20231231") == "2023-12-31"

    def test_ts_date_helper_none(self, saver):
        assert saver._ts_date(None) is None

    def test_ts_date_helper_nan(self, saver):
        assert saver._ts_date(float("nan")) is None

    def test_fv_helper_returns_first_valid(self, saver):
        row = {"a": float("nan"), "b": 42.0}
        assert saver._fv(row, "a", "b") == pytest.approx(42.0)

    def test_fv_helper_default_zero(self, saver):
        row: dict = {}
        assert saver._fv(row, "x", "y") == 0.0
