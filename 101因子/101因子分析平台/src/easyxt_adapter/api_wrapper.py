"""
EasyXT API包装器
提供统一的接口访问EasyXT功能
"""
import pandas as pd
from typing import List, Dict, Optional
import sys
import os

# 添加项目路径
project_path = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, project_path)

from .data_loader import EasyXTDataLoader


class EasyXTAPIWrapper:
    """EasyXT API包装器"""

    def __init__(self):
        self.data_loader = EasyXTDataLoader()

    def get_market_data(self, symbols: List[str], start_date: str, end_date: str,
                       fields: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取市场数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (格式: 'YYYY-MM-DD')
            end_date: 结束日期 (格式: 'YYYY-MM-DD')
            fields: 需要的字段列表

        Returns:
            pd.DataFrame: 市场数据，索引为[date, symbol]
        """
        return self.data_loader.load_data(symbols, start_date, end_date, fields)

    def get_universe(self, market: str = "a_stock") -> List[str]:
        """
        获取股票池（通过 QMT xtdata 获取真实股票列表）

        Args:
            market: 市场类型

        Returns:
            List[str]: 股票代码列表
        """
        try:
            from xtquant import xtdata
            if market == "hs300":
                stocks = xtdata.get_stock_list_in_sector("沪深300")
            elif market == "zz500":
                stocks = xtdata.get_stock_list_in_sector("中证500")
            else:
                stocks = xtdata.get_stock_list_in_sector("沪深A股")
            if stocks:
                return stocks
        except Exception as e:
            raise RuntimeError(f"无法从 QMT 获取股票池 (market={market}): {e}") from e
        raise RuntimeError(f"QMT 返回空股票池 (market={market})")

    def get_instrument_info(self, symbols: List[str]) -> pd.DataFrame:
        """
        获取证券信息（通过 QMT xtdata 获取真实证券信息）

        Args:
            symbols: 股票代码列表

        Returns:
            pd.DataFrame: 证券信息
        """
        try:
            from xtquant import xtdata
            data = []
            for symbol in symbols:
                detail = xtdata.get_instrument_detail(symbol)
                if detail:
                    info = {
                        'symbol': symbol,
                        'name': detail.get('InstrumentName', symbol),
                        'industry': detail.get('ProductID', ''),
                        'market': 'A股',
                        'currency': 'CNY',
                        'status': '交易' if detail.get('IsTrading', 1) else '停牌',
                    }
                else:
                    info = {
                        'symbol': symbol,
                        'name': symbol,
                        'industry': '',
                        'market': 'A股',
                        'currency': 'CNY',
                        'status': '未知',
                    }
                data.append(info)
            return pd.DataFrame(data)
        except Exception as e:
            raise RuntimeError(f"无法从 QMT 获取证券信息: {e}") from e


# 单例模式的EasyXT实例
easyxt_instance = EasyXTAPIWrapper()


def get_easyxt_instance() -> EasyXTAPIWrapper:
    """
    获取EasyXT实例

    Returns:
        EasyXTAPIWrapper: EasyXT API包装器实例
    """
    return easyxt_instance


# 测试代码
if __name__ == '__main__':
    api = get_easyxt_instance()

    # 测试获取股票池
    universe = api.get_universe("hs300")
    print(f'沪深300股票池: {universe}')

    # 测试获取市场数据
    symbols = universe[:3]  # 取前3只股票
    data = api.get_market_data(
        symbols=symbols,
        start_date='2023-01-01',
        end_date='2023-01-10'
    )

    print(f'获取数据形状: {data.shape}')
    print(f'数据预览:')
    print(data.head(10))

    # 测试获取证券信息
    info = api.get_instrument_info(symbols)
    print(f'证券信息:')
    print(info)