"""
绩效指标计算模块

提供标准量化回测绩效指标：
  - 年化夏普比率（Sharpe Ratio）
  - 卡玛比率（Calmar Ratio）
  - 最大回撤（Max Drawdown）
  - 年化复合增长率（CAGR）
  - 胜率（Win Rate）
  - 月度收益率（Monthly Returns Attribution）
"""

from __future__ import annotations

import math
from typing import Any, Dict

import numpy as np
import pandas as pd


def calc_sharpe(
    returns: pd.Series,
    ann_factor: float = 252,
    risk_free: float = 0.03,
) -> float:
    """年化夏普比率。

    Returns:
        float: 年化夏普，若标准差为 0 返回 0.0。
    """
    if len(returns) < 2:
        return 0.0
    daily_rf = (1.0 + risk_free) ** (1.0 / ann_factor) - 1.0
    excess = returns - daily_rf
    std = float(excess.std())
    if std == 0 or math.isnan(std):
        return 0.0
    return float(excess.mean() / std * np.sqrt(ann_factor))


def calc_max_drawdown(equity: pd.Series) -> float:
    """最大回撤（返回正数，如 0.15 表示 15%）。"""
    if equity.empty:
        return 0.0
    rolling_max = equity.cummax()
    drawdown = (equity - rolling_max) / rolling_max.replace(0, np.nan)
    mdd = float(-drawdown.min())
    return max(mdd, 0.0)


def calc_cagr(equity: pd.Series, ann_factor: float = 252) -> float:
    """复合年化增长率（CAGR）。"""
    if len(equity) < 2:
        return 0.0
    total_return = float(equity.iloc[-1]) / float(equity.iloc[0]) - 1.0
    n_periods = len(equity)
    try:
        return float((1.0 + total_return) ** (ann_factor / n_periods) - 1.0)
    except (OverflowError, ValueError):
        return 0.0


def calc_calmar(equity: pd.Series, ann_factor: float = 252) -> float:
    """卡玛比率 = CAGR / MaxDrawdown。"""
    mdd = calc_max_drawdown(equity)
    if mdd == 0.0:
        return 0.0
    cagr = calc_cagr(equity, ann_factor)
    return float(cagr / mdd)


def calc_win_rate(trades: pd.DataFrame) -> float:
    """按先入先出（FIFO）配对买卖，计算收益 > 0 的成对占比。"""
    if trades.empty:
        return 0.0
    buys = trades[trades["direction"] == "buy"].copy()
    sells = trades[trades["direction"] == "sell"].copy()
    if buys.empty or sells.empty:
        return 0.0
    try:
        paired_pnl: list[float] = []
        for code, sells_g in sells.groupby("code"):
            buys_g = buys[buys["code"] == code].reset_index(drop=True)
            if buys_g.empty:
                continue
            buy_prices = list(buys_g["price"])
            for sp in sells_g["price"]:
                if buy_prices:
                    bp = buy_prices.pop(0)
                    paired_pnl.append(float(sp) - float(bp))
        if not paired_pnl:
            return 0.0
        wins = sum(1 for p in paired_pnl if p > 0)
        return float(wins) / len(paired_pnl)
    except Exception:
        return 0.0


def calc_monthly_returns(equity: pd.Series) -> pd.Series:
    """月度收益率 Series（index 为 'YYYY-MM' 字符串）。"""
    if equity.empty or not isinstance(equity.index, pd.DatetimeIndex):
        return pd.Series(dtype=float)
    monthly = equity.resample("ME").last()
    monthly_ret = monthly.pct_change().dropna()
    monthly_ret.index = pd.Index([str(i)[:7] for i in monthly_ret.index])
    return monthly_ret


def calc_all_metrics(
    equity: pd.Series,
    trades: pd.DataFrame,
    initial_capital: float,
) -> Dict[str, Any]:
    """计算全部标准绩效指标，返回字典。

    Args:
        equity: 权益曲线（DatetimeIndex → float）
        trades: 成交记录 DataFrame（含 direction/code/price/volume 列）
        initial_capital: 初始本金

    Returns:
        dict，含 sharpe/calmar/max_drawdown/cagr/total_return/win_rate 等字段。
    """
    if equity.empty:
        return {
            "sharpe": 0.0,
            "calmar": 0.0,
            "max_drawdown": 0.0,
            "cagr": 0.0,
            "total_return": 0.0,
            "win_rate": 0.0,
            "trade_count": 0,
            "monthly_returns": {},
            "start_equity": float(initial_capital),
            "end_equity": float(initial_capital),
        }

    returns = equity.pct_change().dropna()

    sharpe = calc_sharpe(returns)
    mdd = calc_max_drawdown(equity)
    cagr = calc_cagr(equity)
    calmar = calc_calmar(equity)
    total_return = (
        float(equity.iloc[-1]) / float(initial_capital) - 1.0
        if initial_capital > 0
        else 0.0
    )
    win_rate = calc_win_rate(trades) if (trades is not None and not trades.empty) else 0.0
    trade_count = (
        len(trades[trades["direction"] == "sell"])
        if (trades is not None and not trades.empty and "direction" in trades.columns)
        else 0
    )
    monthly_rets = calc_monthly_returns(equity)

    return {
        "sharpe": round(sharpe, 4),
        "calmar": round(calmar, 4),
        "max_drawdown": round(mdd, 4),
        "cagr": round(cagr, 4),
        "total_return": round(total_return, 4),
        "win_rate": round(win_rate, 4),
        "trade_count": trade_count,
        "monthly_returns": monthly_rets.to_dict() if not monthly_rets.empty else {},
        "start_equity": float(equity.iloc[0]),
        "end_equity": float(equity.iloc[-1]),
    }
