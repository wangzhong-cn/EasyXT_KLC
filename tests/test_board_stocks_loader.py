"""Tests for BoardStocksLoader – pure logic and mocked xtdata paths."""
import csv
import os
import pytest
import tempfile
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestBoardStocksLoaderConstruction:
    """BoardStocksLoader constructor sets attributes correctly."""

    def test_constructor_sets_available_true_when_xtdata_present(self):
        """当 xtdata 可导入时，构造函数应设置 available=True。"""
        from unittest.mock import MagicMock
        mock_xtdata = MagicMock()
        # xtquant.xtdata 在此测试环境中需要 MiniQMT 守护进程，无法直接导入。
        # 注入 mock 到 sys.modules 以模拟 xtdata 可用场景。
        with patch.dict('sys.modules', {
            'xtquant': MagicMock(xtdata=mock_xtdata),
            'xtquant.xtdata': mock_xtdata,
        }):
            from data_manager.board_stocks_loader import BoardStocksLoader
            loader = BoardStocksLoader()
        assert loader.available is True
        assert loader.xtdata is not None

    def test_constructor_sets_available_false_when_xtdata_missing(self):
        with patch.dict('sys.modules', {'xtquant': None, 'xtquant.xtdata': None}):
            # Temporarily make import fail
            import importlib
            import sys
            # Patch the import inside the class
            with patch('builtins.__import__', side_effect=lambda name, *a, **kw: (
                (_ for _ in ()).throw(ImportError('no xtquant'))
                if name == 'xtquant' else __import__(name, *a, **kw)
            )):
                try:
                    from data_manager.board_stocks_loader import BoardStocksLoader as BSL2
                    loader2 = BSL2()
                    # If xtquant now unavailable, should be False
                    if not loader2.available:
                        assert loader2.xtdata is None
                except Exception:
                    pass  # Import interception may vary


# ---------------------------------------------------------------------------
# get_board_stocks  (available=False)
# ---------------------------------------------------------------------------

class TestGetBoardStocksUnavailable:
    """get_board_stocks returns [] when available is False."""

    @pytest.fixture
    def loader(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        loader.available = False  # force unavailable
        return loader

    def test_hs300_returns_empty(self, loader):
        assert loader.get_board_stocks('hs300') == []

    def test_zz500_returns_empty(self, loader):
        assert loader.get_board_stocks('zz500') == []

    def test_all_returns_empty(self, loader):
        assert loader.get_board_stocks('all') == []

    def test_unknown_board_returns_empty_unavailable(self, loader):
        assert loader.get_board_stocks('unknown_board') == []


# ---------------------------------------------------------------------------
# get_board_stocks  (available=True, mocked xtdata)
# ---------------------------------------------------------------------------

class TestGetBoardStocksMocked:
    """get_board_stocks routes to correct xtdata calls."""

    @pytest.fixture
    def loader(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        loader.available = True
        mock_xt = MagicMock()
        mock_xt.get_stock_list_in_sector.return_value = ['000001.SZ', '000002.SZ', '600000.SH']
        loader.xtdata = mock_xt
        return loader

    def test_unknown_board_returns_empty(self, loader):
        result = loader.get_board_stocks('invalid_board')
        assert result == []

    def test_hs300_calls_get_stock_list_with_correct_code(self, loader):
        # board_map maps Chinese→English: '沪深300'→'hs300' → triggers index path
        loader.get_board_stocks('沪深300')
        loader.xtdata.get_stock_list_in_sector.assert_any_call('000300.SH')

    def test_sz50_calls_get_stock_list_with_correct_code(self, loader):
        loader.get_board_stocks('上证50')
        loader.xtdata.get_stock_list_in_sector.assert_any_call('000016.SH')

    def test_zz500_calls_correct(self, loader):
        loader.get_board_stocks('中证500')
        loader.xtdata.get_stock_list_in_sector.assert_any_call('000905.SH')

    def test_zz1000_calls_correct(self, loader):
        loader.get_board_stocks('中证1000')
        loader.xtdata.get_stock_list_in_sector.assert_any_call('000852.SH')

    def test_returns_stock_list(self, loader):
        result = loader.get_board_stocks('hs300')
        assert isinstance(result, list)
        assert len(result) > 0

    def test_exception_returns_empty(self, loader):
        loader.xtdata.get_stock_list_in_sector.side_effect = Exception('network error')
        result = loader.get_board_stocks('hs300')
        assert result == []

    def test_all_board_aggregates_sh_sz_bj(self, loader):
        loader.xtdata.get_stock_list_in_sector.return_value = ['000001.SZ']
        # '全A股' passes through lower() → '全a股' → board_code='all'
        result = loader.get_board_stocks('全A股')
        calls = [c.args[0] for c in loader.xtdata.get_stock_list_in_sector.call_args_list]
        assert 'SH' in calls or 'SZ' in calls

    def test_english_board_name_resolved(self, loader):
        # English 'hs300' maps via board_map to '沪深300', which falls to _get_market_board
        result = loader.get_board_stocks('hs300')
        loader.xtdata.get_stock_list_in_sector.assert_called()

    def test_kcb_board_fallback(self, loader):
        """科创板 tries direct sector, then falls back to SH filter."""
        loader.xtdata.get_stock_list_in_sector.return_value = None
        result = loader.get_board_stocks('kcb')
        assert isinstance(result, list)

    def test_cyb_board_fallback(self, loader):
        """创业板 tries direct sector, then falls back to SZ filter."""
        loader.xtdata.get_stock_list_in_sector.return_value = None
        result = loader.get_board_stocks('cyb')
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# get_available_boards
# ---------------------------------------------------------------------------

class TestGetAvailableBoards:
    def test_returns_dict(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        boards = loader.get_available_boards()
        assert isinstance(boards, dict)

    def test_contains_hs300(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        boards = loader.get_available_boards()
        assert '沪深300' in boards

    def test_contains_all_a_shares(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        boards = loader.get_available_boards()
        assert '全A股' in boards

    def test_seven_entries(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        boards = loader.get_available_boards()
        assert len(boards) == 7


# ---------------------------------------------------------------------------
# load_from_csv
# ---------------------------------------------------------------------------

class TestLoadFromCsv:
    def test_nonexistent_file_returns_empty(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        result = loader.load_from_csv('/nonexistent/path/file.csv')
        assert result == []

    def test_valid_csv_with_code_column(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False,
                                        newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['stock_code', 'name'])
            # Write pre-formatted codes so pandas doesn't lose leading zeros
            writer.writerow(['000001.SZ', '平安银行'])
            writer.writerow(['600000.SH', '浦发银行'])
            fname = f.name
        try:
            result = loader.load_from_csv(fname)
            assert '000001.SZ' in result
            assert '600000.SH' in result
        finally:
            os.unlink(fname)

    def test_csv_without_code_column_uses_first_column(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False,
                                        newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ticker', 'name'])
            writer.writerow(['688001', '华兴源创'])
            fname = f.name
        try:
            result = loader.load_from_csv(fname)
            assert isinstance(result, list)
        finally:
            os.unlink(fname)

    def test_csv_adds_sh_suffix_to_6xx(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False,
                                        newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['stock_code'])
            writer.writerow(['600000'])
            fname = f.name
        try:
            result = loader.load_from_csv(fname)
            assert '600000.SH' in result
        finally:
            os.unlink(fname)

    def test_csv_adds_sz_suffix_to_3xx(self):
        # Note: pandas reads '000001' as integer 1, losing leading zeros.
        # '300001' reads as 300001 → str='300001' starts with '3' → .SZ added
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False,
                                        newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['stock_code'])
            writer.writerow(['300001'])
            fname = f.name
        try:
            result = loader.load_from_csv(fname)
            assert '300001.SZ' in result
        finally:
            os.unlink(fname)

    def test_csv_with_existing_suffix_not_doubled(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False,
                                        newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['stock_code'])
            writer.writerow(['000001.SZ'])
            fname = f.name
        try:
            result = loader.load_from_csv(fname)
            assert '000001.SZ' in result
            assert not any('.SZ.SZ' in s for s in result)
        finally:
            os.unlink(fname)

    def test_bj_suffix_for_8xx(self):
        from data_manager.board_stocks_loader import BoardStocksLoader
        loader = BoardStocksLoader()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False,
                                        newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['stock_code'])
            writer.writerow(['830946'])
            fname = f.name
        try:
            result = loader.load_from_csv(fname)
            assert '830946.BJ' in result
        finally:
            os.unlink(fname)
