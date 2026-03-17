"""
数据提供者模块

包含各种数据源的实现：
- TdxDataProvider: 通达信数据接口
- ThsDataProvider: 同花顺数据接口
- EastmoneyDataProvider: 东方财富数据接口
"""

from .base_provider import BaseDataProvider

try:
    from .tdx_provider import TdxDataProvider
except Exception:
    TdxDataProvider = None
try:
    from .eastmoney_provider import EastmoneyDataProvider
except Exception:
    EastmoneyDataProvider = None
from .ths_provider import ThsDataProvider

__all__ = ['BaseDataProvider', 'ThsDataProvider']
if EastmoneyDataProvider is not None:
    __all__.append('EastmoneyDataProvider')
if TdxDataProvider is not None:
    __all__.append('TdxDataProvider')
