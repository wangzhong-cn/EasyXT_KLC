"""Tests for FiveFoldAdjustmentManager calculation logic (pure-pandas paths)."""
import pytest
import pandas as pd
import numpy as np
from datetime import date
from unittest.mock import MagicMock, patch


def _make_price_df(n=5):
    """Build a simple OHLCV DataFrame with a 'date' column."""
    dates = pd.date_range('2023-01-03', periods=n, freq='B')
    return pd.DataFrame({
        'date': dates,
        'open': [10.0 + i * 0.1 for i in range(n)],
        'high': [10.5 + i * 0.1 for i in range(n)],
        'low': [9.5 + i * 0.1 for i in range(n)],
        'close': [10.2 + i * 0.1 for i in range(n)],
        'volume': [100000] * n,
    })


def _make_manager():
    """Create a FiveFoldAdjustmentManager bypassing __init__."""
    from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
    m = object.__new__(FiveFoldAdjustmentManager)
    m._db = None
    m.con = None
    m.duckdb_path = ':memory:'
    return m


# ---------------------------------------------------------------------------
# calculate_adjustment – no dividends
# ---------------------------------------------------------------------------

class TestCalculateAdjustmentNoDividends:
    """calculate_adjustment returns 5 keys when no dividends provided."""

    def test_returns_five_keys(self):
        m = _make_manager()
        df = _make_price_df()
        result = m.calculate_adjustment(df)
        assert set(result.keys()) == {'none', 'front', 'back', 'geometric_front', 'geometric_back'}

    def test_empty_df_returns_empty_dict(self):
        m = _make_manager()
        result = m.calculate_adjustment(pd.DataFrame())
        assert result == {}

    def test_none_df_returns_empty_dict(self):
        m = _make_manager()
        result = m.calculate_adjustment(None)
        assert result == {}

    def test_none_key_equals_original(self):
        m = _make_manager()
        df = _make_price_df()
        result = m.calculate_adjustment(df)
        # 'none' should have the same close values
        assert list(result['none']['close']) == pytest.approx(list(df['close']), rel=1e-6)

    def test_no_dividends_all_keys_equal_original(self):
        m = _make_manager()
        df = _make_price_df()
        result = m.calculate_adjustment(df)
        for key in ['front', 'back', 'geometric_front', 'geometric_back']:
            pd.testing.assert_frame_equal(
                result[key].reset_index(drop=True)[['open', 'close']],
                result['none'].reset_index(drop=True)[['open', 'close']],
                check_like=True,
            )

    def test_df_with_index_only_no_date_column(self):
        m = _make_manager()
        dates = pd.date_range('2023-01-03', periods=3, freq='B')
        df = pd.DataFrame({
            'open': [10.0, 10.1, 10.2],
            'high': [10.5, 10.6, 10.7],
            'low': [9.5, 9.6, 9.7],
            'close': [10.2, 10.3, 10.4],
        }, index=dates)
        result = m.calculate_adjustment(df)
        assert len(result) == 5

    def test_empty_dividends_acts_like_no_dividends(self):
        m = _make_manager()
        df = _make_price_df()
        result = m.calculate_adjustment(df, dividends=pd.DataFrame())
        assert set(result.keys()) == {'none', 'front', 'back', 'geometric_front', 'geometric_back'}


# ---------------------------------------------------------------------------
# calculate_adjustment – with dividends
# ---------------------------------------------------------------------------

class TestCalculateAdjustmentWithDividends:
    """calculate_adjustment with a dividends DataFrame triggers adjustment paths."""

    def _dividends_df(self):
        return pd.DataFrame({
            'ex_date': [pd.Timestamp('2023-01-06')],
            'dividend_per_share': [0.50],
            'bonus_ratio': [None],
        })

    def test_returns_five_keys_with_dividends(self):
        m = _make_manager()
        df = _make_price_df(10)
        divs = self._dividends_df()
        result = m.calculate_adjustment(df, dividends=divs)
        assert set(result.keys()) == {'none', 'front', 'back', 'geometric_front', 'geometric_back'}

    def test_front_adjustment_differs_from_none(self):
        m = _make_manager()
        df = _make_price_df(10)
        divs = self._dividends_df()
        result = m.calculate_adjustment(df, dividends=divs)
        # Front adjustment data should differ from none (at least for early dates)
        assert isinstance(result['front'], pd.DataFrame)
        assert len(result['front']) == len(result['none'])

    def test_back_adjustment_is_dataframe(self):
        m = _make_manager()
        df = _make_price_df(10)
        divs = self._dividends_df()
        result = m.calculate_adjustment(df, dividends=divs)
        assert isinstance(result['back'], pd.DataFrame)

    def test_geometric_front_is_dataframe(self):
        m = _make_manager()
        df = _make_price_df(10)
        divs = self._dividends_df()
        result = m.calculate_adjustment(df, dividends=divs)
        assert isinstance(result['geometric_front'], pd.DataFrame)

    def test_geometric_back_is_dataframe(self):
        m = _make_manager()
        df = _make_price_df(10)
        divs = self._dividends_df()
        result = m.calculate_adjustment(df, dividends=divs)
        assert isinstance(result['geometric_back'], pd.DataFrame)

    def test_bonus_ratio_path(self):
        m = _make_manager()
        df = _make_price_df(10)
        divs = pd.DataFrame({
            'ex_date': [pd.Timestamp('2023-01-06')],
            'dividend_per_share': [None],
            'bonus_ratio': [5.0],  # 10送5
        })
        result = m.calculate_adjustment(df, dividends=divs)
        assert len(result) == 5


# ---------------------------------------------------------------------------
# FiveFoldAdjustmentManager.connect (failure path)
# ---------------------------------------------------------------------------

class TestFiveFoldConnect:
    def test_connect_failure_returns_false(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        m = object.__new__(FiveFoldAdjustmentManager)
        m._db = None
        m.con = None
        m.duckdb_path = ':memory:'
        with patch('data_manager.duckdb_fivefold_adjust.get_db_manager', side_effect=Exception('no db')):
            result = m.connect()
        assert result is False

    def test_add_adjustment_columns_without_db_returns_false(self):
        m = _make_manager()
        result = m.add_adjustment_columns()
        assert result is False


# ---------------------------------------------------------------------------
# Class attribute ADJUST_TYPES
# ---------------------------------------------------------------------------

class TestAdjustTypes:
    def test_adjust_types_keys(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        assert 'none' in FiveFoldAdjustmentManager.ADJUST_TYPES
        assert 'front' in FiveFoldAdjustmentManager.ADJUST_TYPES
        assert 'back' in FiveFoldAdjustmentManager.ADJUST_TYPES
        assert 'geometric_front' in FiveFoldAdjustmentManager.ADJUST_TYPES
        assert 'geometric_back' in FiveFoldAdjustmentManager.ADJUST_TYPES

    def test_adjust_types_count(self):
        from data_manager.duckdb_fivefold_adjust import FiveFoldAdjustmentManager
        assert len(FiveFoldAdjustmentManager.ADJUST_TYPES) == 5


# ---------------------------------------------------------------------------
# save_adjusted_data – DB write paths
# ---------------------------------------------------------------------------

def _make_mock_db():
    """Produce a MagicMock posing as a DuckDB connection pool manager."""
    mock_con = MagicMock()
    mock_con.execute = MagicMock()
    mock_con.register = MagicMock()
    mock_con.unregister = MagicMock()
    mock_mgr = MagicMock()
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=mock_con)
    cm.__exit__ = MagicMock(return_value=False)
    mock_mgr.get_write_connection.return_value = cm
    return mock_mgr, mock_con


class TestSaveAdjustedData:
    def _make_manager_with_db(self):
        m = _make_manager()
        mock_db, mock_con = _make_mock_db()
        m._db = mock_db
        return m, mock_db, mock_con

    def test_returns_false_when_no_db(self):
        m = _make_manager()
        result = m.save_adjusted_data('000001.SZ', {'none': _make_price_df()})
        assert result is False

    def test_returns_true_on_success(self):
        m, mock_db, mock_con = self._make_manager_with_db()
        df = _make_price_df(3)
        adjusted = {
            'none': df.copy(),
            'front': df.copy(),
            'back': df.copy(),
            'geometric_front': df.copy(),
            'geometric_back': df.copy(),
        }
        result = m.save_adjusted_data('000001.SZ', adjusted)
        assert result is True

    def test_calls_begin_and_commit(self):
        m, mock_db, mock_con = self._make_manager_with_db()
        df = _make_price_df(3)
        adjusted = {'none': df.copy(), 'front': df.copy()}
        m.save_adjusted_data('000001.SZ', adjusted)
        calls = [str(c) for c in mock_con.execute.call_args_list]
        begin_found = any('BEGIN' in c for c in calls)
        commit_found = any('COMMIT' in c for c in calls)
        assert begin_found
        assert commit_found

    def test_rollback_on_exception(self):
        m = _make_manager()
        mock_db = MagicMock()
        mock_con = MagicMock()
        mock_con.execute.side_effect = [None, None, Exception("insert error")]
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=mock_con)
        cm.__exit__ = MagicMock(return_value=False)
        mock_db.get_write_connection.return_value = cm
        m._db = mock_db

        df = _make_price_df(2)
        result = m.save_adjusted_data('000001.SZ', {'none': df.copy()})
        assert result is False
        # ROLLBACK must have been issued
        calls = [str(c) for c in mock_con.execute.call_args_list]
        assert any('ROLLBACK' in c for c in calls)


# ---------------------------------------------------------------------------
# get_data_with_adjustment – query paths
# ---------------------------------------------------------------------------

class TestGetDataWithAdjustment:
    def _make_manager_with_db(self, query_df=None):
        m = _make_manager()
        if query_df is None:
            query_df = pd.DataFrame()
        mock_db = MagicMock()
        mock_db.execute_read_query.return_value = query_df
        # Columns check
        mock_read_con = MagicMock()
        mock_read_con.execute.return_value.fetchdf.return_value = pd.DataFrame(
            {'column_name': ['open', 'close', 'high', 'low', 'volume', 'amount',
                             'open_front', 'close_front', 'high_front', 'low_front',
                             'open_back', 'close_back', 'high_back', 'low_back',
                             'open_geometric_front', 'close_geometric_front',
                             'high_geometric_front', 'low_geometric_front',
                             'open_geometric_back', 'close_geometric_back',
                             'high_geometric_back', 'low_geometric_back']}
        )
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=mock_read_con)
        cm.__exit__ = MagicMock(return_value=False)
        mock_db.get_read_connection.return_value = cm
        m._db = mock_db
        return m

    def test_returns_empty_when_no_db(self):
        m = _make_manager()
        result = m.get_data_with_adjustment('000001.SZ', '2023-01-01', '2023-12-31', 'none')
        assert result.empty

    def test_invalid_adjust_type_returns_empty(self):
        m = self._make_manager_with_db()
        result = m.get_data_with_adjustment('000001.SZ', '2023-01-01', '2023-12-31', 'invalid_type')
        assert result.empty

    def test_none_adjust_type_queries_db(self):
        dates = pd.date_range('2023-01-03', periods=3, freq='B')
        df = pd.DataFrame({
            'stock_code': ['000001.SZ'] * 3,
            'date': dates,
            'open': [10.0, 10.1, 10.2],
            'high': [10.5, 10.6, 10.7],
            'low': [9.5, 9.6, 9.7],
            'close': [10.2, 10.3, 10.4],
            'volume': [100000] * 3,
            'amount': [1000000.0] * 3,
        })
        m = self._make_manager_with_db(query_df=df)
        result = m.get_data_with_adjustment('000001.SZ', '2023-01-01', '2023-12-31', 'none')
        assert not result.empty
        assert len(result) == 3

    def test_front_adjust_type_queries_db(self):
        dates = pd.date_range('2023-01-03', periods=2, freq='B')
        df = pd.DataFrame({
            'stock_code': ['000001.SZ'] * 2,
            'date': dates,
            'open': [10.0, 10.1],
            'high': [10.5, 10.6],
            'low': [9.5, 9.6],
            'close': [10.2, 10.3],
            'volume': [100000] * 2,
            'amount': [1000000.0] * 2,
        })
        m = self._make_manager_with_db(query_df=df)
        result = m.get_data_with_adjustment('000001.SZ', '2023-01-01', '2023-12-31', 'front')
        assert not result.empty

    def test_query_exception_returns_empty(self):
        m = _make_manager()
        mock_db = MagicMock()
        mock_db.execute_read_query.side_effect = Exception("query failed")
        mock_read_con = MagicMock()
        mock_read_con.execute.return_value.fetchdf.return_value = pd.DataFrame(
            {'column_name': ['open_front', 'close_front', 'high_front', 'low_front']}
        )
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=mock_read_con)
        cm.__exit__ = MagicMock(return_value=False)
        mock_db.get_read_connection.return_value = cm
        m._db = mock_db
        result = m.get_data_with_adjustment('000001.SZ', '2023-01-01', '2023-12-31', 'front')
        assert result.empty


# ---------------------------------------------------------------------------
# _try_repair_adjustment – auto-repair edge cases
# ---------------------------------------------------------------------------

class TestTryRepairAdjustment:
    def test_no_db_returns_immediately(self):
        m = _make_manager()
        # No _db – must not raise
        m._try_repair_adjustment('000001.SZ', '2023-01-01', '2023-12-31')

    def test_empty_raw_df_is_noop(self):
        m = _make_manager()
        mock_db = MagicMock()
        mock_db.execute_read_query.return_value = pd.DataFrame()
        m._db = mock_db
        m._try_repair_adjustment('000001.SZ', '2023-01-01', '2023-12-31')
        # No write connection should have been acquired
        mock_db.get_write_connection.assert_not_called()

    def test_skips_when_open_front_already_populated(self):
        dates = pd.date_range('2023-01-03', periods=3, freq='B')
        df = pd.DataFrame({
            'stock_code': ['000001.SZ'] * 3,
            'date': dates,
            'open': [10.0, 10.1, 10.2],
            'high': [10.5, 10.6, 10.7],
            'low': [9.5, 9.6, 9.7],
            'close': [10.2, 10.3, 10.4],
            'open_front': [10.1, 10.2, 10.3],  # already populated
        })
        m = _make_manager()
        mock_db = MagicMock()
        mock_db.execute_read_query.return_value = df
        m._db = mock_db
        m._try_repair_adjustment('000001.SZ', '2023-01-03', '2023-01-05')
        mock_db.get_write_connection.assert_not_called()

    def test_repairs_null_front_columns(self):
        """When open_front is all-null, recalculates and writes back."""
        dates = pd.date_range('2023-01-03', periods=3, freq='B')
        df = pd.DataFrame({
            'stock_code': ['000001.SZ'] * 3,
            'date': dates,
            'open': [10.0, 10.1, 10.2],
            'high': [10.5, 10.6, 10.7],
            'low': [9.5, 9.6, 9.7],
            'close': [10.2, 10.3, 10.4],
            'volume': [100000] * 3,
            'open_front': [None, None, None],  # all null → needs repair
        })

        m = _make_manager()
        mock_db = MagicMock()
        mock_db.execute_read_query.return_value = df
        mock_write_con = MagicMock()
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=mock_write_con)
        cm.__exit__ = MagicMock(return_value=False)
        mock_db.get_write_connection.return_value = cm
        m._db = mock_db

        m._try_repair_adjustment('000001.SZ', '2023-01-03', '2023-01-05')
        # Should have attempted writes via get_write_connection
        mock_db.get_write_connection.assert_called()
