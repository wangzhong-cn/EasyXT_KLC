"""
gui_app/widgets/table_models.py 纯逻辑方法测试。

PyQt5 sip 绑定不允许 object.__new__ 绕过 QAbstractTableModel /
QSortFilterProxyModel。解决方案：patch Qt 基类的 __init__ 为 no-op，
使 DataFrameTableModel.__init__ 的 Python 层能正常执行，再手动
设置所需状态。
"""

import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np
import pytest

from PyQt5.QtCore import Qt


# ---------------------------------------------------------------------------
# 存根辅助
# ---------------------------------------------------------------------------

def _make_model(df: "pd.DataFrame | None" = None):
    """
    patch QAbstractTableModel.__init__ 为 no-op，让 DataFrameTableModel.__new__
    可以正常执行 Python 层（设置 self._df、self._logger 等）。
    data_loaded 是 pyqtSignal，无法在未完整初始化时 emit，不做修改即可——
    我们的测试不会触发 set_data()。
    """
    from gui_app.widgets.table_models import DataFrameTableModel

    with patch("PyQt5.QtCore.QAbstractTableModel.__init__", return_value=None):
        # 绕过 sip C++ __new__ 检查，用 __new__ + 手动赋值代替
        m = DataFrameTableModel.__new__(DataFrameTableModel)
        m._df = df if df is not None else pd.DataFrame()
        m._logger = logging.getLogger("test_table_models")
    return m


def _make_proxy():
    """patch QSortFilterProxyModel.__init__ 为 no-op。"""
    from gui_app.widgets.table_models import SortFilterProxyModel

    with patch("PyQt5.QtCore.QSortFilterProxyModel.__init__", return_value=None):
        proxy = SortFilterProxyModel.__new__(SortFilterProxyModel)
        proxy._filter_text = ""
        proxy._filter_columns = None
        proxy.invalidateFilter = MagicMock()
    return proxy


def _sample_df(rows: int = 5) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": [f"00000{i}.SZ" for i in range(rows)],
            "price": [10.0 + i * 0.5 for i in range(rows)],
            "volume": [1000 * (i + 1) for i in range(rows)],
        }
    )


# ===========================================================================
# rowCount / columnCount / get_data
# ===========================================================================

class TestBasicAccessors:
    def test_row_count_empty(self):
        m = _make_model()
        assert m.rowCount() == 0

    def test_row_count_nonempty(self):
        m = _make_model(_sample_df(7))
        assert m.rowCount() == 7

    def test_column_count_empty(self):
        m = _make_model()
        assert m.columnCount() == 0

    def test_column_count_nonempty(self):
        m = _make_model(_sample_df(3))
        assert m.columnCount() == 3

    def test_get_data_returns_dataframe(self):
        df = _sample_df(4)
        m = _make_model(df)
        result = m.get_data()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4

    def test_get_data_is_same_object(self):
        df = _sample_df(3)
        m = _make_model(df)
        assert m.get_data() is df


# ===========================================================================
# get_row_data
# ===========================================================================

class TestGetRowData:
    def test_valid_row_returns_dict(self):
        m = _make_model(_sample_df(5))
        result = m.get_row_data(0)
        assert isinstance(result, dict)
        assert set(result.keys()) == {"code", "price", "volume"}

    def test_row_0_correct_value(self):
        m = _make_model(_sample_df(5))
        assert m.get_row_data(0)["price"] == pytest.approx(10.0)

    def test_last_row(self):
        m = _make_model(_sample_df(5))
        assert m.get_row_data(4)["price"] == pytest.approx(12.0)

    def test_negative_row_returns_empty(self):
        m = _make_model(_sample_df(5))
        assert m.get_row_data(-1) == {}

    def test_out_of_bounds_row_returns_empty(self):
        m = _make_model(_sample_df(5))
        assert m.get_row_data(5) == {}

    def test_empty_df_returns_empty(self):
        m = _make_model()
        assert m.get_row_data(0) == {}

    def test_single_row_df(self):
        df = pd.DataFrame({"a": [42], "b": [99]})
        m = _make_model(df)
        result = m.get_row_data(0)
        assert result["a"] == 42


# ===========================================================================
# get_column_data
# ===========================================================================

class TestGetColumnData:
    def test_valid_column_returns_list(self):
        m = _make_model(_sample_df(5))
        result = m.get_column_data(1)  # "price" column
        assert isinstance(result, list)
        assert len(result) == 5

    def test_first_column_values(self):
        m = _make_model(_sample_df(3))
        result = m.get_column_data(0)
        assert result[0] == "000000.SZ"

    def test_negative_column_returns_empty(self):
        m = _make_model(_sample_df(5))
        assert m.get_column_data(-1) == []

    def test_out_of_bounds_column_returns_empty(self):
        m = _make_model(_sample_df(5))
        assert m.get_column_data(3) == []

    def test_empty_df_returns_empty(self):
        m = _make_model()
        assert m.get_column_data(0) == []

    def test_price_column_is_floats(self):
        m = _make_model(_sample_df(5))
        result = m.get_column_data(1)
        assert all(isinstance(v, float) for v in result)


# ===========================================================================
# sort — 需 stub out beginResetModel / endResetModel
# ===========================================================================

class TestSort:
    @staticmethod
    def _prep(m) -> object:
        """注入 no-op Qt 方法，避免调用未初始化的 QAbstractTableModel C++ 层。"""
        m.beginResetModel = MagicMock()
        m.endResetModel = MagicMock()
        return m

    def test_sort_ascending(self):
        df = pd.DataFrame({"val": [3, 1, 2]})
        m = self._prep(_make_model(df))
        m.sort(0, Qt.AscendingOrder)
        assert list(m._df["val"]) == [1, 2, 3]

    def test_sort_descending(self):
        df = pd.DataFrame({"val": [3, 1, 2]})
        m = self._prep(_make_model(df))
        m.sort(0, Qt.DescendingOrder)
        assert list(m._df["val"]) == [3, 2, 1]

    def test_sort_negative_column_returns_early(self):
        m = self._prep(_make_model(_sample_df(3)))
        m.sort(-1, Qt.AscendingOrder)
        m.beginResetModel.assert_not_called()

    def test_sort_out_of_range_returns_early(self):
        m = self._prep(_make_model(_sample_df(3)))
        m.sort(100, Qt.AscendingOrder)
        m.beginResetModel.assert_not_called()


# ===========================================================================
# SortFilterProxyModel — set_filter_text / set_filter_columns
# ===========================================================================

class TestSortFilterProxyModelState:
    """测试内部状态变更，invalidateFilter 已被 MagicMock 替换。"""

    def test_set_filter_text_lowercases(self):
        proxy = _make_proxy()
        proxy.set_filter_text("HELLO")
        assert proxy._filter_text == "hello"

    def test_set_filter_text_strips_whitespace(self):
        proxy = _make_proxy()
        proxy.set_filter_text("  abc  ")
        assert proxy._filter_text == "abc"

    def test_set_filter_text_empty(self):
        proxy = _make_proxy()
        proxy.set_filter_text("")
        assert proxy._filter_text == ""

    def test_set_filter_text_calls_invalidate(self):
        proxy = _make_proxy()
        proxy.set_filter_text("x")
        proxy.invalidateFilter.assert_called_once()

    def test_set_filter_columns(self):
        proxy = _make_proxy()
        proxy.set_filter_columns([0, 2])
        assert proxy._filter_columns == [0, 2]

    def test_set_filter_columns_none_resets(self):
        proxy = _make_proxy()
        proxy.set_filter_columns([1])
        proxy.set_filter_columns(None)
        assert proxy._filter_columns is None

    def test_set_filter_columns_calls_invalidate(self):
        proxy = _make_proxy()
        proxy.set_filter_columns([1])
        proxy.invalidateFilter.assert_called_once()
