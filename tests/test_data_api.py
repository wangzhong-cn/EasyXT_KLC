import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from easy_xt.data_api import (
    validate_period,
    get_supported_periods,
    get_recommended_stocks,
    auto_time_range,
    validate_stock_codes,
    DataAPI
)
from easy_xt.data_types import ConnectionError, DataError

def test_helpers():
    assert validate_period('1d')
    assert not validate_period('999d')
    assert '1d' in get_supported_periods()
    assert len(get_recommended_stocks(2)) == 2
    
    st, ed = auto_time_range(10)
    assert len(st) == 8
    assert len(ed) == 8
    
    ok, msg = validate_stock_codes("000001.SZ")
    assert ok
    ok, msg = validate_stock_codes("invalid")
    assert not ok

@pytest.fixture
def mock_xt():
    xt = MagicMock()
    # Mock client connection
    client = MagicMock()
    client.is_connected.return_value = True
    xt.get_client.return_value = client
    return xt

@pytest.fixture
def data_api(mock_xt):
    with patch("easy_xt.data_api.xt", mock_xt):
        api = DataAPI()
        api.xt = mock_xt
        api.connect()
        return api

def test_connect(data_api, mock_xt):
    assert data_api._connected

def test_get_price(data_api, mock_xt):
    # Mock K线返回格式 {code: df} 索引必须是时间
    df = pd.DataFrame({
        "open": [10.0, 10.5],
        "close": [10.5, 11.0],
        "volume": [1000, 2000]
    }, index=["20240101", "20240102"])
    mock_xt.get_market_data_ex.return_value = {"000001.SZ": df}
    
    res = data_api.get_price("000001.SZ", "20240101", "20240102")
    assert not res.empty
    assert "time" in res.columns
    assert "code" in res.columns
    
    # Mock Tick返回格式
    tick_data = pd.DataFrame([{"time": 1700000000000, "lastPrice": 10.5}])
    mock_xt.get_market_data_ex.return_value = {"000001.SZ": tick_data}
    res_tick = data_api.get_price("000001.SZ", period="tick")
    assert not res_tick.empty
    
def test_get_price_exceptions(data_api, mock_xt):
    # 测试异常抛出
    mock_xt.get_market_data_ex.return_value = {}
    with pytest.raises(DataError):
        data_api.get_price("000001.SZ")

def test_get_current_price(data_api, mock_xt):
    mock_xt.get_full_tick.return_value = {
        "000001.SZ": {"lastPrice": 10.5, "open": 10.0, "high": 11.0, "low": 9.5}
    }
    res = data_api.get_current_price("000001.SZ")
    assert not res.empty
    assert res.iloc[0]["price"] == 10.5

def test_get_financial_data(data_api, mock_xt):
    mock_xt.get_financial_data.return_value = {"000001.SZ": {"Balance": pd.DataFrame()}}
    res = data_api.get_financial_data("000001.SZ")
    assert "000001.SZ" in res

def test_get_stock_list(data_api, mock_xt):
    mock_xt.get_stock_list_in_sector.return_value = ["000001.SZ", "000002.SZ"]
    res = data_api.get_stock_list()
    assert len(res) > 0

def test_get_trading_dates(data_api, mock_xt):
    mock_xt.get_trading_dates.return_value = [1700000000000]
    res = data_api.get_trading_dates()
    assert len(res) == 1

def test_download_data(data_api, mock_xt):
    res = data_api.download_data("000001.SZ")
    assert res
    mock_xt.download_history_data.assert_called()

def test_download_history_data_batch(data_api, mock_xt):
    # mock verification
    mock_xt.get_local_data.return_value = {"000001.SZ": [1, 2, 3]}
    res = data_api.download_history_data_batch(["000001.SZ"])
    assert res.get("000001.SZ") is True
