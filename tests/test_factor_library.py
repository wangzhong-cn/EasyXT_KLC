import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch

from easy_xt.factor_library import EasyFactor, DuckDBDataReader

@pytest.fixture
def mock_market_data():
    """生成 200 天假数据，覆盖常见指标的计算窗口要求"""
    dates = pd.date_range(start="2024-01-01", periods=200, freq="B").strftime("%Y-%m-%d")
    np.random.seed(42)
    
    dfs = []
    for stock in ["000001.SZ", "600519.SH"]:
        base_price = 100.0
        returns = np.random.normal(0, 0.02, 200)
        closes = base_price * np.cumprod(1 + returns)
        
        df = pd.DataFrame({
            "date": dates,
            "stock_code": stock,
            "open": closes * 0.99,
            "high": closes * 1.05,
            "low": closes * 0.95,
            "close": closes,
            "volume": np.random.randint(1000, 10000, 200),
            "amount": np.random.randint(100000, 1000000, 200),
            "turnover": np.random.uniform(0.01, 0.1, 200)
        })
        dfs.append(df)
        
    return pd.concat(dfs, ignore_index=True)

class TestEasyFactor:
    @patch("easy_xt.factor_library.DuckDBDataReader")
    def test_get_factor_batch_and_analyze(self, mock_reader_cls, mock_market_data):
        # 配置 Mock Reader
        mock_reader = MagicMock()
        mock_reader.get_market_data.return_value = mock_market_data
        mock_reader.get_stock_list.return_value = ["000001.SZ", "600519.SH"]
        # mock_reader_cls 返回我们创建的 mock
        mock_reader_cls.return_value = mock_reader
        
        # 实例化 EasyFactor (屏蔽扩展模块，因为我们在测因子逻辑)
        ef = EasyFactor("dummy_path.ddb", enable_extended_modules=False)
        ef.duckdb_reader = mock_reader  # 强行注入
        
        # 针对 analyze_batch 的全面计算覆盖
        stocks = ["000001.SZ", "600519.SH"]
        ret = ef.analyze_batch(stocks, "2024-01-01", "2024-10-01")
        
        # 断言各因子字典都被填充
        assert "momentum" in ret
        assert not ret["momentum"].empty
        assert "volatility" in ret
        assert not ret["volatility"].empty
        assert "technical" in ret
        assert not ret["technical"].empty
        
        # 测试单一查询 (get_factor) -> 会调用 get_market_data_ex -> 内部再调 get_market_data
        rsi_df = ef.get_factor("000001.SZ", "rsi", "2024-01-01")
        assert not rsi_df.empty

        # 覆盖一个异常计算分支
        bad_factor = ef.get_factor("000001.SZ", "unknown_factor", "2024-01-01")
        assert bad_factor.empty

        # 测试 get_all_factors
        all_df = ef.get_all_factors("000001.SZ", "2024-01-01", "2024-10-01")
        assert not all_df.empty


