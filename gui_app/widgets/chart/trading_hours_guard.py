"""
trading_hours_guard.py — A 股交易时段守卫

功能：
  - 判断当前系统时间是否在 A 股交易时段内
  - 提供 can_change_backend() 接口，供灰度开关使用
  - 提供 assert_not_trading() 装饰器，用于防止在交易时段执行危险操作

A 股交易时段：
  周一至周五
  集合竞价：09:15 – 09:25
  连续競價：09:30 – 11:30  / 13:00 – 15:00
  盘后竞价（科创/创业）：15:05 – 15:35  （可选）

本模块默认监控"连续竞价 + 集合竞价"两个窗口，不包含盘后。
"""
from __future__ import annotations

import functools
import logging
from datetime import datetime, time, timedelta
from typing import Any, Callable

log = logging.getLogger(__name__)


class TradingHoursGuard:
    """
    A 股交易时段判断工具（纯静态，无状态）。

    is_trading_day(dt)           → bool
    is_trading_time(dt)          → bool
    current_session()            → (bool, session_name)
    can_change_backend()         → (bool, reason)
    """

    # 集合竞价 + 连续竞价时段（含开始，不含结束边界外的操作窗口）
    _SESSIONS: list[tuple[time, time, str]] = [
        (time(9, 15), time(9, 25), "集合竞价"),
        (time(9, 30), time(11, 35), "上午连续竞价"),   # 留1分钟尾部余量
        (time(13, 0), time(15, 5), "下午连续竞价"),    # 留1分钟尾部余量
    ]

    @classmethod
    def is_trading_day(cls, dt: datetime | None = None) -> bool:
        """判断 dt 是否为交易日（仅检查周一~周五，不查法定节假日）。"""
        dt = dt or datetime.now()
        return dt.weekday() < 5  # Monday=0 … Friday=4

    @classmethod
    def is_trading_time(cls, dt: datetime | None = None) -> bool:
        """判断 dt 是否在任一交易时段内。"""
        dt = dt or datetime.now()
        if not cls.is_trading_day(dt):
            return False
        t = dt.time()
        return any(start <= t < end for start, end, _ in cls._SESSIONS)

    @classmethod
    def current_session(cls, dt: datetime | None = None) -> tuple[bool, str]:
        """
        返回 (in_session, session_name)。
        非交易时段时返回 (False, "")。
        """
        dt = dt or datetime.now()
        if not cls.is_trading_day(dt):
            return False, ""
        t = dt.time()
        for start, end, name in cls._SESSIONS:
            if start <= t < end:
                return True, name
        return False, ""

    @classmethod
    def can_change_backend(cls) -> tuple[bool, str]:
        """
        检查是否允许切换图表后端。

        Returns:
            (True, "")          → 允许
            (False, reason)     → 拒绝，reason 描述原因
        """
        in_session, name = cls.current_session()
        if in_session:
            return False, f"当前处于交易时段 [{name}]，禁止切换图表后端以保障交易稳定性"
        return True, ""

    @classmethod
    def minutes_to_next_session(cls) -> int | None:
        """
        返回距离下一个交易时段开始的分钟数。
        已在交易时段内返回 0；今日无更多时段返回 None。
        """
        now = datetime.now()
        if not cls.is_trading_day(now):
            return None
        t_now = now.time()
        for start, _, _ in cls._SESSIONS:
            if t_now < start:
                delta = datetime.combine(now.date(), start) - now
                return max(0, int(delta.total_seconds() / 60))
        return None


# ── 装饰器 ─────────────────────────────────────────────────────────────────────

def require_non_trading(fn: Callable[..., Any]) -> Callable[..., Any]:
    """
    装饰器：在交易时段内调用被装饰函数时抛出 RuntimeError。

    用于保护"热更新桥接脚本"、"切换后端"等危险操作。

    Example::

        @require_non_trading
        def switch_to_native_backend():
            ...
    """
    @functools.wraps(fn)
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        ok, reason = TradingHoursGuard.can_change_backend()
        if not ok:
            raise RuntimeError(f"[TradingHoursGuard] 操作被阻止: {reason}")
        return fn(*args, **kwargs)
    return _wrapper
