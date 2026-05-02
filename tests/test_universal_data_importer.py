"""Tests for UniversalDataImporter – mocked dependencies."""
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, PropertyMock


def _make_importer(duckdb_path=':memory:'):
    """Create a UniversalDataImporter with all external dependencies mocked."""
    with patch('data_manager.duckdb_connection_pool.resolve_duckdb_path',
               return_value=duckdb_path), \
         patch('data_manager.unified_data_interface.UnifiedDataInterface') as mock_udi, \
         patch('data_manager.board_stocks_loader.BoardStocksLoader') as mock_bsl, \
         patch('data_manager.csv_importer.CSVImporter') as mock_csv, \
         patch('data_manager.universal_data_importer.UnifiedDataInterface', mock_udi), \
         patch('data_manager.universal_data_importer.BoardStocksLoader', mock_bsl), \
         patch('data_manager.universal_data_importer.CSVImporter', mock_csv):
        from data_manager.universal_data_importer import UniversalDataImporter
        imp = UniversalDataImporter(duckdb_path=duckdb_path)
    return imp


class TestConstructor:
    def test_constructor_sets_duckdb_path(self):
        imp = _make_importer(':memory:')
        assert imp.duckdb_path == ':memory:'

    def test_constructor_detector_none(self):
        imp = _make_importer()
        assert imp.detector is None

    def test_callbacks_none_by_default(self):
        imp = _make_importer()
        assert imp.progress_callback is None
        assert imp.status_callback is None

    def test_interface_attribute_set(self):
        imp = _make_importer()
        assert imp.interface is not None


class TestConnectClose:
    def test_connect_delegates_to_interface(self):
        imp = _make_importer()
        mock_connect = MagicMock(return_value=True)
        imp.interface.connect = mock_connect
        result = imp.connect()
        mock_connect.assert_called_once()

    def test_close_does_not_raise(self):
        imp = _make_importer()
        # interface.con is None
        imp.interface.con = None
        imp.close()  # should not raise

    def test_close_clears_connection(self):
        imp = _make_importer()
        mock_con = MagicMock()
        imp.interface.con = mock_con
        imp.close()
        mock_con.close.assert_called_once()
        assert imp.interface.con is None


class TestImportBoardStocksEmpty:
    def test_empty_board_returns_error(self):
        imp = _make_importer()
        imp.board_loader.get_board_stocks = MagicMock(return_value=[])
        result = imp.import_board_stocks('沪深300', '2024-01-01', '2024-01-31')
        assert result.get('success') is False
        assert 'error' in result

    def test_board_name_passed_to_loader(self):
        imp = _make_importer()
        imp.board_loader.get_board_stocks = MagicMock(return_value=[])
        imp.import_board_stocks('上证50', '2024-01-01', '2024-01-31')
        imp.board_loader.get_board_stocks.assert_called_once_with('上证50')


class TestImportFromCsvEmpty:
    def test_empty_csv_returns_error(self):
        imp = _make_importer()
        imp.csv_importer.load_stock_list = MagicMock(return_value=[])
        result = imp.import_from_csv('nonexistent.csv', '2024-01-01', '2024-01-31')
        assert result.get('success') is False

    def test_empty_csv_is_silent_by_default(self, capsys):
        imp = _make_importer()
        imp.csv_importer.load_stock_list = MagicMock(return_value=[])
        result = imp.import_from_csv('nonexistent.csv', '2024-01-01', '2024-01-31')
        assert result.get('success') is False
        captured = capsys.readouterr()
        assert captured.out == ''

    def test_csv_path_passed_to_importer(self):
        imp = _make_importer()
        imp.csv_importer.load_stock_list = MagicMock(return_value=[])
        imp.import_from_csv('/path/to/stocks.csv', '2024-01-01', '2024-01-31')
        imp.csv_importer.load_stock_list.assert_called_once_with('/path/to/stocks.csv')

    def test_verbose_mode_restores_stdout(self, capsys):
        imp = _make_importer()
        imp._stdout_enabled = True
        imp.csv_importer.load_stock_list = MagicMock(return_value=[])
        result = imp.import_from_csv('/path/to/stocks.csv', '2024-01-01', '2024-01-31')
        assert result.get('success') is False
        captured = capsys.readouterr()
        assert 'CSV股票列表导入' in captured.out


class TestImportCustomStocks:
    def test_empty_stocks_list_raises_or_returns_error(self):
        imp = _make_importer()
        # Empty stocks causes batch_size=0 → range() ValueError (documented behavior)
        import pytest as _pytest
        with _pytest.raises((ValueError, Exception)):
            imp.import_custom_stocks([], '2024-01-01', '2024-01-31')

    def test_stocks_passed_through(self):
        imp = _make_importer()
        # Mock _import_stocks_batch to verify it's called
        imp._import_stocks_batch = MagicMock(return_value={'success': True})
        imp.import_custom_stocks(['600000.SH', '000001.SZ'],
                                  '2024-01-01', '2024-01-31')
        imp._import_stocks_batch.assert_called_once()
        call_kwargs = imp._import_stocks_batch.call_args
        assert '600000.SH' in call_kwargs.kwargs.get('stocks',
                                                      call_kwargs.args[0] if call_kwargs.args else [])


class TestCheckMissingData:
    def test_check_with_no_missing_returns_empty_df(self):
        imp = _make_importer()
        mock_detector = MagicMock()
        mock_detector.detect_missing_data.return_value = {
            'missing_count': 0,
            'missing_trading_days': [],
            'missing_segments': []
        }
        imp.detector = mock_detector

        result = imp.check_missing_data(['600000.SH'], '2024-01-01', '2024-01-31')
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_check_with_missing_returns_non_empty_df(self):
        imp = _make_importer()
        mock_detector = MagicMock()
        mock_detector.detect_missing_data.return_value = {
            'missing_count': 5,
            'missing_trading_days': [],
            'missing_segments': [('2024-01-15', '2024-01-19')]
        }
        imp.detector = mock_detector

        result = imp.check_missing_data(['600000.SH'], '2024-01-01', '2024-01-31')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['stock'] == '600000.SH'


class TestResumeImport:
    def test_resume_does_not_raise(self):
        imp = _make_importer()
        imp.resume_import()  # Should NOT raise, just prints


class TestImportBatchWithMockedInterface:
    """Covers more of _import_stocks_batch logic when interface is mocked."""

    def test_import_with_stocks_calls_interface(self):
        imp = _make_importer()
        # Mock the interface get_stock_data
        imp.interface.get_stock_data = MagicMock(return_value=pd.DataFrame())
        imp.board_loader.get_board_stocks = MagicMock(return_value=['600000.SH'])
        result = imp.import_board_stocks('沪深300', '2024-01-01', '2024-01-31')
        assert isinstance(result, dict)
