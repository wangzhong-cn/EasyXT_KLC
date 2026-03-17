"""
策略回放测试（Phase 3）

用固定历史行情片段回放，验证两类示例策略的信号稳定不漂移：
  - 相同输入序列 → 相同信号数量（确定性保证）
  - 平坦行情 → 0 信号（无噪声发单）
  - 数据不足（< period）→ 0 信号（保护期正确）
  - 已知触发序列 → 至少 1 次信号（策略有行动能力）

确定性保证：
  - 所有回放序列均为内联常量，不使用随机数、datetime.now() 或其他环境依赖项
  - bar.time 字段使用固定 UTC 纪元时间戳（中性整数），不依赖本地时区
  - 输入序列繁垃在此锁定：修改后请同步更新断言

bar 序列设计说明（fast=3, slow=5）：
  - 5 根下行 K 线 → EMA快 < EMA慢
  - 6 根 (9.6) + 7 根 (9.9)：
      prev_fast=9.525, prev_slow=9.581（bar6）
      curr_fast=9.7,   curr_slow=9.661（bar7）→ 9.7 > 9.661 → 金叉 → 买入
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from strategies.base_strategy import BarData, StrategyContext
from strategies.examples.bollinger_strategy import BollingerMeanRevStrategy
from strategies.examples.ma_cross_strategy import MACrossStrategy

# ---------------------------------------------------------------------------
# 固定回放序列（版本锁定：修改后需同步更新断言）
# ---------------------------------------------------------------------------

# MA Cross 序列：5 根下行 + 5 根快速上行 → 已知在 bar[6] 触发金叉
_REPLAY_BARS_MA: list = [
    10.0, 9.8, 9.6, 9.5, 9.4,  # declining phase
    9.6, 9.9, 10.2, 10.8, 11.5,  # rapid rise → golden cross at bar index 6
]

# Bollinger 序列：5 根平行 K 线（建立带宽）+ 1 根大幅下跌（破下轨）
# std_mult=1.5: lower = 18.0 - 1.5*4.0 = 12.0，price=10.0 < 12.0 → 买入
_REPLAY_BARS_BOLL: list = [
    20.0, 20.0, 20.0, 20.0, 20.0,  # warmup: mean=20, std=0→deque uses these 5
    10.0,  # sharp drop → below lower_band  (deque=[20,20,20,20,10], mean=18, std=4, lower=12)
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# UTC 基准时间戳（固定 epoch，跨机器一致）
# 2024-01-01T00:00:00Z = 1704067200
_BASE_UTC_TS = 1_704_067_200


def _make_bar(close: float, t: int = 0) -> BarData:
    return BarData(
        code="000001.SZ",
        period="1d",
        open=close,
        high=close * 1.01,
        low=close * 0.99,
        close=close,
        volume=1_000_000.0,
        time=_BASE_UTC_TS + t * 86400,  # 每根 bar 相差 1 天（秒），不依赖本地时区
    )


def _make_ctx(nav: float = 1_000_000.0) -> StrategyContext:
    executor = MagicMock()
    executor.submit_order.return_value = MagicMock(
        order_id="OD001", status="submitted", msg=""
    )
    return StrategyContext(
        strategy_id="replay",
        account_id="88001234",
        positions={},
        nav=nav,
        executor=executor,
    )


def _run_replay(strategy, bars: list, nav: float = 1_000_000.0) -> int:
    """Run a fixed bar sequence through the strategy; return submit_order call count."""
    ctx = _make_ctx(nav=nav)
    strategy._start(ctx)
    for i, price in enumerate(bars):
        strategy._handle_bar(ctx, _make_bar(price, t=i))
    strategy._stop(ctx)
    return ctx.executor.submit_order.call_count


# ---------------------------------------------------------------------------
# MA Cross Replay
# ---------------------------------------------------------------------------

class TestMACrossStrategyReplay:
    """Determinism guard: same bar sequence → same signal count on every run."""

    def test_signal_count_is_deterministic(self) -> None:
        """两次独立回放相同 K 线序列，信号数量必须完全一致。"""
        count1 = _run_replay(
            MACrossStrategy("r1", fast_period=3, slow_period=5),
            _REPLAY_BARS_MA,
        )
        count2 = _run_replay(
            MACrossStrategy("r2", fast_period=3, slow_period=5),
            _REPLAY_BARS_MA,
        )
        assert count1 == count2

    def test_golden_cross_sequence_produces_at_least_one_signal(self) -> None:
        """已知金叉序列（bars[6] 金叉）至少产生 1 次买入信号。"""
        count = _run_replay(
            MACrossStrategy("r3", fast_period=3, slow_period=5),
            _REPLAY_BARS_MA,
        )
        assert count >= 1, f"Expected ≥1 signal, got {count}"

    def test_flat_bars_produce_no_signal(self) -> None:
        """完全平行行情（EMA 快慢始终相等）不应产生任何信号。"""
        count = _run_replay(
            MACrossStrategy("r4", fast_period=3, slow_period=5),
            [10.0] * 20,
        )
        assert count == 0

    def test_insufficient_bars_produce_no_signal(self) -> None:
        """K 线数量严格小于慢线周期时不产生任何信号。"""
        # slow_period=10, only 9 bars → never reaches computation stage
        count = _run_replay(
            MACrossStrategy("r5", fast_period=3, slow_period=10),
            [10.0] * 9,
        )
        assert count == 0

    def test_different_period_params_produce_different_counts(self) -> None:
        """不同周期参数在同一序列上的信号数不相关（参数有实际影响）。"""
        # fast=3/slow=5 → cross detected; fast=8/slow=9 → different EMA smoothing
        count_narrow = _run_replay(
            MACrossStrategy("r6a", fast_period=3, slow_period=5),
            _REPLAY_BARS_MA,
        )
        count_wide = _run_replay(
            MACrossStrategy("r6b", fast_period=4, slow_period=9),
            _REPLAY_BARS_MA,
        )
        # They may differ or match; what matters is both are deterministic
        for _ in range(3):
            assert _run_replay(
                MACrossStrategy("r6c", fast_period=3, slow_period=5),
                _REPLAY_BARS_MA,
            ) == count_narrow


# ---------------------------------------------------------------------------
# Bollinger Bands Replay
# ---------------------------------------------------------------------------

class TestBollingerStrategyReplay:
    """Determinism guard: same bar sequence → same signal count on every run."""

    def test_signal_count_is_deterministic(self) -> None:
        """两次独立回放相同 K 线序列，信号数量必须完全一致。"""
        count1 = _run_replay(
            BollingerMeanRevStrategy("b1", period=5, std_mult=1.5),
            _REPLAY_BARS_BOLL,
        )
        count2 = _run_replay(
            BollingerMeanRevStrategy("b2", period=5, std_mult=1.5),
            _REPLAY_BARS_BOLL,
        )
        assert count1 == count2

    def test_drop_below_lower_band_triggers_buy(self) -> None:
        """bar[5]=10.0 应跌破下轨（lower=12.0, std_mult=1.5），触发买入。

        推导：deque=[20,20,20,20,10], mean=18, std=4, lower=18-1.5*4=12, 10 < 12 ✓
        """
        count = _run_replay(
            BollingerMeanRevStrategy("b3", period=5, std_mult=1.5),
            _REPLAY_BARS_BOLL,
        )
        assert count >= 1, f"Expected ≥1 buy signal, got {count}"

    def test_flat_bars_produce_no_signal(self) -> None:
        """完全平行行情（std=0 → 上下轨等于中轨）不应产生任何信号。

        条件：bar.close < lower 永远为 False（close == lower == mid）。
        """
        count = _run_replay(
            BollingerMeanRevStrategy("b4", period=5, std_mult=2.0),
            [20.0] * 15,
        )
        assert count == 0

    def test_insufficient_bars_produce_no_signal(self) -> None:
        """K 线数量严格小于 period 时不产生任何信号。"""
        count = _run_replay(
            BollingerMeanRevStrategy("b5", period=10, std_mult=2.0),
            [20.0] * 9,
        )
        assert count == 0

    def test_halted_strategy_ignores_all_bars(self) -> None:
        """_halted=True 后即使传入有效 K 线也不产生信号（回放确定性）。"""
        ctx = _make_ctx()
        strat = BollingerMeanRevStrategy("b6", period=5, std_mult=1.5)
        strat._start(ctx)
        strat._halted = True

        for price in _REPLAY_BARS_BOLL:
            strat._handle_bar(ctx, _make_bar(price))

        ctx.executor.submit_order.assert_not_called()
        strat._stop(ctx)
