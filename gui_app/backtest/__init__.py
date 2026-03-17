"""
回测框架模块
基于微信文章回测架构设计，集成Backtrader专业回测引擎
"""

from .data_manager import DataManager
from .engine import AdvancedBacktestEngine
from .risk_analyzer import RiskAnalyzer

__all__ = [
    'AdvancedBacktestEngine',
    'DataManager',
    'RiskAnalyzer'
]
