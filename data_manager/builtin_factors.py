"""
内置因子库 — EasyXT 平台预注册的标准 A 股因子

使用方式（自动注册）::

    from data_manager.builtin_factors import register_all_builtin_factors
    register_all_builtin_factors()

或在应用启动时一次性调用，之后通过全局注册中心直接使用::

    from data_manager.factor_registry import factor_registry, factor_compute_engine
    result = factor_compute_engine.compute("momentum_20d", df)

因子分类
--------
momentum   : 动量类因子
volatility : 波动率类因子
volume     : 成交量类因子
technical  : 技术指标类（MA、RSI、MACD 等）
quality    : 财务质量类（需要财务数据，仅占位）
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _rolling_std(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=max(1, window // 2)).std()


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


# ---------------------------------------------------------------------------
# 动量类 (momentum)
# ---------------------------------------------------------------------------

def _momentum_20d(df: pd.DataFrame) -> pd.Series:
    """20 日价格动量：当日收盘 / 20 日前收盘 - 1"""
    return df["close"] / df["close"].shift(20) - 1


def _momentum_5d(df: pd.DataFrame) -> pd.Series:
    """5 日价格动量"""
    return df["close"] / df["close"].shift(5) - 1


def _momentum_60d(df: pd.DataFrame) -> pd.Series:
    """60 日价格动量（绕开最近 5 日，去除短期反转）"""
    return df["close"].shift(5) / df["close"].shift(60) - 1


def _roc_10d(df: pd.DataFrame) -> pd.Series:
    """10 日涨跌幅（Rate of Change）"""
    return df["close"].pct_change(10)


# ---------------------------------------------------------------------------
# 波动率类 (volatility)
# ---------------------------------------------------------------------------

def _volatility_20d(df: pd.DataFrame) -> pd.Series:
    """20 日已实现波动率（日收益率标准差）"""
    ret = df["close"].pct_change()
    return _rolling_std(ret, 20)


def _volatility_60d(df: pd.DataFrame) -> pd.Series:
    """60 日已实现波动率"""
    ret = df["close"].pct_change()
    return _rolling_std(ret, 60)


def _atr_14d(df: pd.DataFrame) -> pd.Series:
    """14 日平均真实波幅 (ATR)"""
    high = df["high"]
    low = df["low"]
    close_prev = df["close"].shift(1)
    tr = pd.concat(
        [high - low, (high - close_prev).abs(), (low - close_prev).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(14, min_periods=7).mean()


# ---------------------------------------------------------------------------
# 成交量类 (volume)
# ---------------------------------------------------------------------------

def _volume_ratio_20d(df: pd.DataFrame) -> pd.Series:
    """量比：当日成交量 / 20 日均量"""
    avg_vol = df["volume"].rolling(20, min_periods=10).mean()
    return df["volume"] / avg_vol.replace(0, np.nan)


def _turnover_20d_zscore(df: pd.DataFrame) -> pd.Series:
    """20 日换手率 Z-Score（需要 amount 列作为代理指标；若无则用 volume）"""
    col = "amount" if "amount" in df.columns else "volume"
    series = df[col].astype(float)
    roll_mean = series.rolling(20, min_periods=10).mean()
    roll_std = _rolling_std(series, 20)
    return (series - roll_mean) / roll_std.replace(0, np.nan)


def _obv(df: pd.DataFrame) -> pd.Series:
    """能量潮 OBV（On-Balance Volume）"""
    direction = np.sign(df["close"].diff())
    return (direction * df["volume"]).cumsum()


# ---------------------------------------------------------------------------
# 技术指标类 (technical)
# ---------------------------------------------------------------------------

def _ma_cross_5_20(df: pd.DataFrame) -> pd.Series:
    """MA5/MA20 金叉强度：MA5 - MA20（正值=多头排列）"""
    ma5 = df["close"].rolling(5, min_periods=3).mean()
    ma20 = df["close"].rolling(20, min_periods=10).mean()
    return (ma5 - ma20) / ma20


def _rsi_14(df: pd.DataFrame) -> pd.Series:
    """RSI(14)"""
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=13, adjust=False).mean()
    avg_loss = loss.ewm(com=13, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _macd_diff(df: pd.DataFrame) -> pd.Series:
    """MACD DIF（EMA12 - EMA26），方向性动量信号"""
    return _ema(df["close"], 12) - _ema(df["close"], 26)


def _bollinger_pct_b(df: pd.DataFrame) -> pd.Series:
    """布林带 %B = (close - lower) / (upper - lower)"""
    mid = df["close"].rolling(20, min_periods=10).mean()
    std = _rolling_std(df["close"], 20)
    upper = mid + 2 * std
    lower = mid - 2 * std
    width = (upper - lower).replace(0, np.nan)
    return (df["close"] - lower) / width


def _high_low_ratio_20d(df: pd.DataFrame) -> pd.Series:
    """20 日振幅归一：(high_max - low_min) / close"""
    high_max = df["high"].rolling(20, min_periods=10).max()
    low_min = df["low"].rolling(20, min_periods=10).min()
    return (high_max - low_min) / df["close"].replace(0, np.nan)


# ---------------------------------------------------------------------------
# 主注册函数
# ---------------------------------------------------------------------------

_BUILTIN_FACTORS = [
    # -------- momentum --------
    ("momentum_20d",        _momentum_20d,         "momentum",   "20日价格动量"),
    ("momentum_5d",         _momentum_5d,          "momentum",   "5日价格动量"),
    ("momentum_60d_skip5",  _momentum_60d,         "momentum",   "60日动量(跳过近5日)"),
    ("roc_10d",             _roc_10d,              "momentum",   "10日涨跌幅ROC"),
    # -------- volatility ------
    ("volatility_20d",      _volatility_20d,       "volatility", "20日已实现波动率"),
    ("volatility_60d",      _volatility_60d,       "volatility", "60日已实现波动率"),
    ("atr_14d",             _atr_14d,              "volatility", "14日ATR"),
    # -------- volume ----------
    ("volume_ratio_20d",    _volume_ratio_20d,     "volume",     "量比(当日/20日均量)"),
    ("turnover_zscore_20d", _turnover_20d_zscore,  "volume",     "20日换手率Z-Score"),
    ("obv",                 _obv,                  "volume",     "能量潮OBV"),
    # -------- technical -------
    ("ma_cross_5_20",       _ma_cross_5_20,        "technical",  "MA5/MA20金叉强度"),
    ("rsi_14",              _rsi_14,               "technical",  "RSI(14)"),
    ("macd_diff",           _macd_diff,            "technical",  "MACD DIF"),
    ("bollinger_pct_b",     _bollinger_pct_b,      "technical",  "布林带%B"),
    ("high_low_ratio_20d",  _high_low_ratio_20d,   "technical",  "20日振幅归一"),
]


def register_all_builtin_factors(registry=None) -> int:
    """
    将所有内置因子注册到因子注册中心。

    Args:
        registry: FactorRegistry 实例；为 None 时使用全局 factor_registry。

    Returns:
        成功注册的因子数量。
    """
    if registry is None:
        from data_manager.factor_registry import factor_registry as _global_registry
        registry = _global_registry

    count = 0
    for name, func, category, desc in _BUILTIN_FACTORS:
        try:
            registry.register_func(name, func, category=category, description=desc, version="1.0")
            count += 1
        except Exception:
            log.exception("注册内置因子 '%s' 失败", name)

    log.info("内置因子注册完成，共 %d 个", count)
    return count


# 模块导入时自动注册
try:
    register_all_builtin_factors()
except Exception:
    log.exception("自动注册内置因子失败")
