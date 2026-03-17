"""
gui_app/widgets/table_renderer.py 的纯逻辑方法测试。
LazyTableLoader 的无副作用方法（has_next / has_prev / get_page_info）
可以通过 object.__new__ + 手动设置内部状态直接测试。
"""

import pytest

from gui_app.widgets.table_renderer import LazyTableLoader


# ---------------------------------------------------------------------------
# 构建无 Qt 依赖的存根
# ---------------------------------------------------------------------------

def _make_loader(all_data: list, page: int = 0, page_size: int = 10) -> LazyTableLoader:
    """bypass __init__ (需要 QTableWidget)，直接设置内部状态。"""
    stub = object.__new__(LazyTableLoader)
    stub._all_data = all_data
    stub._current_page = page
    stub._page_size = page_size
    return stub


# ===========================================================================
# has_next
# ===========================================================================

class TestHasNext:
    def test_empty_data_no_next(self):
        loader = _make_loader([], page=0, page_size=10)
        assert loader.has_next() is False

    def test_less_than_one_page_no_next(self):
        loader = _make_loader(list(range(5)), page=0, page_size=10)
        assert loader.has_next() is False

    def test_exactly_one_page_no_next(self):
        loader = _make_loader(list(range(10)), page=0, page_size=10)
        assert loader.has_next() is False

    def test_more_than_one_page_has_next(self):
        loader = _make_loader(list(range(11)), page=0, page_size=10)
        assert loader.has_next() is True

    def test_on_last_page_no_next(self):
        loader = _make_loader(list(range(20)), page=1, page_size=10)
        assert loader.has_next() is False

    def test_on_first_of_three_pages(self):
        loader = _make_loader(list(range(25)), page=0, page_size=10)
        assert loader.has_next() is True

    def test_on_second_of_three_pages(self):
        loader = _make_loader(list(range(25)), page=1, page_size=10)
        assert loader.has_next() is True

    def test_on_third_of_three_pages(self):
        loader = _make_loader(list(range(25)), page=2, page_size=10)
        assert loader.has_next() is False


# ===========================================================================
# has_prev
# ===========================================================================

class TestHasPrev:
    def test_first_page_no_prev(self):
        loader = _make_loader(list(range(20)), page=0)
        assert loader.has_prev() is False

    def test_second_page_has_prev(self):
        loader = _make_loader(list(range(20)), page=1)
        assert loader.has_prev() is True

    def test_page_0_empty_data_no_prev(self):
        loader = _make_loader([], page=0)
        assert loader.has_prev() is False

    def test_large_page_has_prev(self):
        loader = _make_loader(list(range(100)), page=9)
        assert loader.has_prev() is True


# ===========================================================================
# get_page_info
# ===========================================================================

class TestGetPageInfo:
    def test_returns_string(self):
        loader = _make_loader(list(range(10)), page=0)
        result = loader.get_page_info()
        assert isinstance(result, str)

    def test_single_page_format(self):
        loader = _make_loader(list(range(5)), page=0, page_size=10)
        result = loader.get_page_info()
        assert "1/1" in result

    def test_first_of_three_pages(self):
        loader = _make_loader(list(range(25)), page=0, page_size=10)
        result = loader.get_page_info()
        assert "1/3" in result

    def test_second_of_three_pages(self):
        loader = _make_loader(list(range(25)), page=1, page_size=10)
        result = loader.get_page_info()
        assert "2/3" in result

    def test_total_count_in_info(self):
        loader = _make_loader(list(range(25)), page=0, page_size=10)
        result = loader.get_page_info()
        assert "25" in result

    def test_empty_data_page_info(self):
        loader = _make_loader([], page=0, page_size=10)
        result = loader.get_page_info()
        assert "0" in result  # 0 条记录

    def test_exactly_two_pages(self):
        loader = _make_loader(list(range(20)), page=1, page_size=10)
        result = loader.get_page_info()
        assert "2/2" in result

    def test_page_suffix_contains_chinese(self):
        loader = _make_loader(list(range(10)), page=0)
        result = loader.get_page_info()
        assert "页" in result


# ===========================================================================
# next_page / prev_page (state mutations, no Qt calls needed)
# ===========================================================================

class TestPageNavigation:
    def test_next_page_advances_current_page(self):
        loader = _make_loader(list(range(25)), page=0, page_size=10)
        # Manually override _show_page to avoid Qt calls
        loader._show_page = lambda p: setattr(loader, "_current_page", p)
        loader.next_page()
        assert loader._current_page == 1

    def test_next_page_at_last_page_stays(self):
        loader = _make_loader(list(range(10)), page=0, page_size=10)
        loader._show_page = lambda p: setattr(loader, "_current_page", p)
        loader.next_page()
        # No next page (exactly 10 items, page_size=10), should not advance
        assert loader._current_page == 0

    def test_prev_page_decrements_current_page(self):
        loader = _make_loader(list(range(25)), page=2, page_size=10)
        loader._show_page = lambda p: setattr(loader, "_current_page", p)
        loader.prev_page()
        assert loader._current_page == 1

    def test_prev_page_at_first_page_stays(self):
        loader = _make_loader(list(range(25)), page=0, page_size=10)
        loader._show_page = lambda p: setattr(loader, "_current_page", p)
        loader.prev_page()
        assert loader._current_page == 0
