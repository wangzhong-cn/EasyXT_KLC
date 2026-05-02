"""
原生事件驱动回测引擎

设计原则：
  - 无前视偏差：信号 bar 下单，下一 bar 开盘成交
  - 完全自主：零 backtrader 依赖
  - 治理接入：集成 RiskEngine 预交易风控 + AuditTrail 审计链
  - 数据就近：通过 UnifiedDataInterface 加载 DuckDB 本地数据（含在线回退）
"""

from __future__ import annotations

import logging
import sys
import uuid
from dataclasses import dataclass, field
from itertools import groupby
from pathlib import Path
from typing import Any, cast

import pandas as pd

# 保证无论从哪个目录启动都能找到项目包
_PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from strategies.base_strategy import (  # noqa: E402
    BarData,
    BaseStrategy,
    OrderData,
    StrategyContext,
    TickData,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 配置数据类
# ---------------------------------------------------------------------------


@dataclass
class BacktestConfig:
    """回测参数配置。"""

    initial_capital: float = 1_000_000.0  # 初始资金（元）
    commission_rate: float = 0.0003  # 手续费率（双向，万三）
    stamp_duty: float = 0.001  # 印花税（仅卖出）
    slippage_pct: float = 0.0002  # 滑点（按成交价的比例）
    min_trade_unit: int = 100  # 最小交易单位（股）
    fill_on: str = "next_open"  # 成交时机：next_open | current_close
    allow_short: bool = False  # A 股默认不允许做空
    # ── 多资产支持 ──
    asset_type: str = "stock"  # stock | future | option | convertible
    future_contract_multiplier: dict[str, int] = field(default_factory=dict)  # code → 合约乘数（如 {"rb2501.SF": 10}）
    # ── Tick 模式专用 ──
    tick_latency_ticks: int = 0  # tick 延迟（tick 数）
    tick_slippage_bps: float = 0.0  # tick 滑点（基点）
    tick_use_orderbook: bool = False  # 是否使用五档行情模拟成交
    tick_participation_rate: float = 1.0  # 参与率（部分成交）
    tick_orderbook_levels: int = 1  # 使用的盘口档位数
    tick_max_wait_ticks: int = 0  # 最大等待 tick 数（0=不超时）
    tick_cancel_retry_max: int = 0  # 撤单重试次数
    tick_cancel_retry_price_bps: float = 0.0  # 重试价格调整（基点）
    tick_cancel_retry_guard_bps: float = 0.0  # 重挂累计追价上限（基点）


# ---------------------------------------------------------------------------
# 输出结果
# ---------------------------------------------------------------------------


@dataclass
class BacktestResult:
    """回测输出结果。"""

    equity_curve: pd.Series  # DatetimeIndex → 净值
    trades: pd.DataFrame  # 成交记录表
    metrics: dict[str, Any]  # 绩效指标字典
    final_equity: float
    initial_capital: float
    strategy_id: str
    weight_history: list[dict[str, Any]] = None  # type: ignore[assignment]
    """持仓权重快照列表，每次再平衡后填充。格式：[{"datetime": pd.Timestamp, "weights": {code: float}}, ...]"""

    def __post_init__(self) -> None:
        if self.weight_history is None:
            self.weight_history = []


# ---------------------------------------------------------------------------
# 内部持仓管理
# ---------------------------------------------------------------------------


class _Position:
    """单标的持仓（FIFO 成本核算）。"""

    __slots__ = ("code", "quantity", "avg_cost")

    def __init__(self, code: str) -> None:
        self.code = code
        self.quantity: int = 0
        self.avg_cost: float = 0.0

    def buy(self, volume: int, price: float) -> None:
        if volume <= 0:
            return
        total_cost = self.quantity * self.avg_cost + volume * price
        self.quantity += volume
        self.avg_cost = total_cost / self.quantity if self.quantity > 0 else 0.0

    def sell(self, volume: int) -> None:
        self.quantity = max(self.quantity - volume, 0)
        if self.quantity == 0:
            self.avg_cost = 0.0

    def market_value(self, price: float) -> float:
        return self.quantity * price


# ---------------------------------------------------------------------------
# 轻量模拟执行器（供 StrategyContext.executor 使用）
# ---------------------------------------------------------------------------


class _Executor:
    """策略通过 context.executor.submit_order(...) 提交订单。"""

    def __init__(self, config: BacktestConfig) -> None:
        self._config = config
        self._submitted_orders: list[dict] = []

    def submit_order(
        self,
        code: str,
        volume: float,
        price: float,
        direction: str,
        signal_id: str = "",
        asset_type: str = "stock",
        offset: str = "open",
    ) -> str:
        """提交一笔订单（异步——将在下一 bar 开盘成交）。

        Args:
            code:        标的代码
            volume:      委托数量（股票=股，期货=手）
            price:       委托价格（参考价，实际按 fill_on 规则成交）
            direction:   "buy" | "sell"
            signal_id:   关联信号 ID（可选）
            asset_type:  资产类型（默认 stock，future/option/conversion 可选）
            offset:      期货开平标志（open/close/close_today/close_history）

        Returns:
            order_id 字符串（空字符串表示委托被过滤）
        """
        volume_int = int(volume // self._config.min_trade_unit) * self._config.min_trade_unit
        if volume_int <= 0:
            return ""
        order_id = uuid.uuid4().hex[:8]
        self._submitted_orders.append(
            {
                "order_id": order_id,
                "signal_id": signal_id,
                "code": str(code),
                "direction": str(direction).lower(),
                "volume": volume_int,
                "price": float(price),
                "status": "submitted",
                "filled_price": 0.0,
                "filled_volume": 0,
                "error_msg": "",
                "asset_type": asset_type,
                "offset": offset,
            }
        )
        return order_id


# ---------------------------------------------------------------------------
# 回测引擎主体
# ---------------------------------------------------------------------------


class BacktestEngine:
    """
    原生事件驱动回测引擎。

    无前视偏差（next-bar-open 成交）、零外部库依赖（无 backtrader）。

    集成点：
      - :class:`core.risk_engine.RiskEngine` — 每笔委托提交前预交易风控
      - :class:`core.audit_trail.AuditTrail`  — 成交审计链（strategy_id 前缀 ``bt:``）
    """

    def __init__(
        self,
        config: BacktestConfig | None = None,
        duckdb_path: str | None = None,
        risk_engine: Any | None = None,
        audit_trail: Any | None = None,
    ) -> None:
        self.config = config or BacktestConfig()
        self.duckdb_path = duckdb_path
        self._risk_engine = risk_engine
        self._audit_trail = audit_trail
        self._logger = log

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        strategy: BaseStrategy,
        codes: list[str],
        start_date: str,
        end_date: str,
        period: str = "1d",
        adjust: str = "qfq",
        factors_to_load: list[str] | None = None,
        preloaded_data: dict[str, pd.DataFrame] | None = None,
    ) -> BacktestResult:
        """运行回测，返回 :class:`BacktestResult`。

        Args:
            factors_to_load: 需要在每个 bar 注入策略上下文的因子名列表，例如
                ``["momentum_20d", "rsi_14"]``。
                引擎会优先从 DuckDB ``factor_values`` 表读取；若该标的/因子缺失，
                则现算并自动持久化，供下次复用。
                为 None 时不加载任何因子。
            preloaded_data: 预加载数据（tick 模式必需），形如 ``{code: DataFrame}``。
                DataFrame 需有 DatetimeIndex，列含 ``price``/``volume``，
                可选 ``ask1``/``bid1``/``ask1_vol``/``bid1_vol`` 等盘口列。
        """
        if period == "tick" and preloaded_data is not None:
            return self._run_tick(strategy, codes, preloaded_data)

        data = (
            self._load_data(codes, start_date, end_date, period, adjust)
            if preloaded_data is None
            else preloaded_data
        )
        if not data:
            raise ValueError(f"无法加载数据: codes={codes} {start_date}~{end_date} period={period}")

        # ── 预加载因子时间序列（索引对齐后转为 {code: {factor: Series}}） ──
        factor_series: dict[str, dict[str, pd.Series]] = {}
        if factors_to_load:
            factor_series = self._load_factors(codes, start_date, end_date, factors_to_load)

        # ── 构建统一时间轴 ─────────────────────────────────────────────
        events: list[tuple[pd.Timestamp, str, dict]] = []
        for code, df in data.items():
            for ts, row in df.iterrows():
                events.append((pd.Timestamp(cast(Any, ts)), code, row.to_dict()))
        events.sort(key=lambda x: x[0])

        # ── 运行时状态 ─────────────────────────────────────────────────
        cash: float = self.config.initial_capital
        positions: dict[str, _Position] = {}
        latest_prices: dict[str, float] = {}
        equity_points: list[tuple[pd.Timestamp, float]] = []
        trades_list: list[dict] = []
        pending_fill_queue: list[dict] = []  # 等待下一 bar 成交
        returns_history: list[float] = []
        weight_history: list[dict] = []  # 权重快照列表

        def _nav() -> float:
            return cash + sum(
                p.market_value(latest_prices.get(c, p.avg_cost))
                for c, p in positions.items()
                if p.quantity > 0
            )

        def _pos_values() -> dict[str, float]:
            return {
                c: p.market_value(latest_prices.get(c, p.avg_cost))
                for c, p in positions.items()
                if p.quantity > 0
            }

        def _factor_snapshot(ts: pd.Timestamp) -> dict[str, dict[str, float]]:
            """将当前 bar 时间点的因子值切片为 {code: {factor: float}} 字典。"""
            snapshot: dict[str, dict[str, float]] = {}
            for code, factors in factor_series.items():
                snapshot[code] = {}
                for fname, series in factors.items():
                    try:
                        val = float(series.loc[ts])
                        snapshot[code][fname] = val
                    except (KeyError, TypeError, ValueError):
                        # 该 bar 无因子数据——用 NaN 占位，策略自行判断
                        snapshot[code][fname] = float("nan")
            return snapshot

        def _build_ctx(executor: _Executor, ts: pd.Timestamp | None = None) -> StrategyContext:
            return StrategyContext(
                strategy_id=strategy.strategy_id,
                account_id="backtest",
                positions=_pos_values(),
                nav=_nav(),
                params={},
                executor=executor,
                risk_engine=self._risk_engine,
                audit_trail=self._audit_trail,
                factor_snapshot=_factor_snapshot(ts) if ts is not None else {},
            )

        # ── on_init ────────────────────────────────────────────────────
        _init_exec = _Executor(self.config)
        try:
            strategy.on_init(_build_ctx(_init_exec))
        except Exception:
            self._logger.exception("strategy.on_init 异常 [%s]", strategy.strategy_id)

        # ── 主循环 ─────────────────────────────────────────────────────
        for ts, bar_group_iter in groupby(events, key=lambda x: x[0]):
            bar_by_code: dict[str, dict] = {be[1]: be[2] for be in bar_group_iter}

            # STEP 1 — 以本 bar 开盘价成交上一 bar 挂出的订单 ──────────
            _trades_before_fill = len(trades_list)
            still_pending: list[dict] = []
            for order in pending_fill_queue:
                code = order["code"]
                bar = bar_by_code.get(code)
                if bar is None:
                    still_pending.append(order)
                    continue
                self._fill_order(order, bar, cash, positions, trades_list, ts)
                cash = order.get("_cash_after", cash)
                # notify strategy of fill
                od = OrderData(
                    order_id=order["order_id"],
                    signal_id=order.get("signal_id", ""),
                    code=code,
                    direction=order["direction"],
                    volume=order["volume"],
                    price=order.get("price", 0.0),
                    status=order["status"],
                    filled_volume=order.get("filled_volume", 0),
                    filled_price=order.get("filled_price", 0.0),
                    error_msg=order.get("error_msg", ""),
                )
                _notify_exec = _Executor(self.config)
                try:
                    strategy.on_order(_build_ctx(_notify_exec, ts), od)
                except Exception:
                    self._logger.exception("strategy.on_order 异常")
                # AuditTrail
                if self._audit_trail is not None and order["status"] == "filled":
                    try:
                        _sig_id = order.get("signal_id") or uuid.uuid4().hex
                        self._audit_trail.record_signal(
                            strategy_id=f"bt:{strategy.strategy_id}",
                            code=code,
                            direction=order["direction"],
                            price_hint=order.get("filled_price"),
                            volume_hint=order.get("filled_volume"),
                        )
                    except Exception:
                        pass

            pending_fill_queue = still_pending

            # STEP 2 — 更新最新价格 & 计算净值 ──────────────────────────
            for code, bar in bar_by_code.items():
                close_val = bar.get("close") or bar.get("Close") or 0.0
                if close_val and float(close_val) > 0:
                    latest_prices[code] = float(close_val)

            nav = _nav()
            equity_points.append((ts, nav))

            # 有成交发生时记录权重快照
            if len(trades_list) > _trades_before_fill and nav > 0:
                pos_vals = _pos_values()
                snap_w = {c: v / nav for c, v in pos_vals.items() if v > 0}
                if snap_w:
                    weight_history.append({"datetime": ts, "weights": snap_w})

            if len(equity_points) >= 2:
                prev_nav = equity_points[-2][1]
                if prev_nav > 0:
                    returns_history.append((nav - prev_nav) / prev_nav)

            # STEP 3 — 按 bar 驱动策略，收集新订单 ───────────────────────
            new_orders_this_bar: list[dict] = []
            for code, bar in bar_by_code.items():
                bar_data = BarData(
                    code=code,
                    period=period,
                    open=float(bar.get("open") or bar.get("Open") or 0.0),
                    high=float(bar.get("high") or bar.get("High") or 0.0),
                    low=float(bar.get("low") or bar.get("Low") or 0.0),
                    close=float(bar.get("close") or bar.get("Close") or 0.0),
                    volume=float(bar.get("volume") or bar.get("Volume") or 0.0),
                    time=ts,
                )
                bar_exec = _Executor(self.config)
                ctx_bar = _build_ctx(bar_exec, ts)

                # 盘前风控哨兵（volume=0 只检查持仓浓度 / 日内回撤）
                if self._risk_engine is not None:
                    try:
                        rr = self._risk_engine.check_pre_trade(
                            account_id="backtest",
                            code=code,
                            volume=0,
                            price=bar_data.close,
                            direction="buy",
                            positions=_pos_values(),
                            nav=nav,
                            returns=returns_history[-20:] if returns_history else None,
                            strategy_id=strategy.strategy_id,
                        )
                        if rr.action.value != "pass":
                            try:
                                strategy.on_risk(ctx_bar, rr)
                            except Exception:
                                self._logger.exception("strategy.on_risk 异常")
                    except Exception:
                        pass

                try:
                    strategy.on_bar(ctx_bar, bar_data)
                except Exception:
                    self._logger.exception("strategy.on_bar 异常 code=%s", code)

                new_orders_this_bar.extend(bar_exec._submitted_orders)

            # STEP 4 — 过风控门禁，加入待成交队列 ────────────────────────
            for order in new_orders_this_bar:
                blocked = False
                if self._risk_engine is not None:
                    try:
                        rr = self._risk_engine.check_pre_trade(
                            account_id="backtest",
                            code=order["code"],
                            volume=float(order["volume"]),
                            price=float(order.get("price") or latest_prices.get(order["code"], 0)),
                            direction=order["direction"],
                            positions=_pos_values(),
                            nav=nav,
                            returns=returns_history[-20:] if returns_history else None,
                            strategy_id=strategy.strategy_id,
                        )
                        if rr.blocked:
                            order["status"] = "rejected"
                            order["error_msg"] = f"risk:{rr.action.value}"
                            blocked = True
                            # 通知策略
                            _re_exec = _Executor(self.config)
                            try:
                                strategy.on_risk(_build_ctx(_re_exec, ts), rr)
                            except Exception:
                                pass
                    except Exception:
                        pass
                if not blocked:
                    pending_fill_queue.append(order)

        # ── on_stop ────────────────────────────────────────────────────
        _stop_exec = _Executor(self.config)
        try:
            strategy.on_stop(_build_ctx(_stop_exec))
        except Exception:
            self._logger.exception("strategy.on_stop 异常 [%s]", strategy.strategy_id)

        # ── 组装 BacktestResult ────────────────────────────────────────
        final_equity = cash + sum(
            p.market_value(latest_prices.get(c, p.avg_cost))
            for c, p in positions.items()
            if p.quantity > 0
        )

        equity_series = pd.Series(
            [e for _, e in equity_points],
            index=pd.DatetimeIndex([ts for ts, _ in equity_points]),
            name="equity",
            dtype=float,
        )
        trades_df = pd.DataFrame(trades_list) if trades_list else pd.DataFrame()

        from easyxt_backtest.performance import calc_all_metrics

        metrics = calc_all_metrics(equity_series, trades_df, self.config.initial_capital)

        # ── 组合风险分析（可选） ────────────────────────────────────────
        if self._risk_engine is not None:
            self._enrich_with_portfolio_risk(
                metrics,
                equity_series,
                positions,
                latest_prices,
                final_equity,
            )
            # R4: 将风控事件统计持久化至 DuckDB risk_events 表
            self._persist_risk_events_to_duckdb(
                strategy_id=strategy.strategy_id,
                risk_stats=self._risk_engine.get_risk_stats(),
            )

        return BacktestResult(
            equity_curve=equity_series,
            trades=trades_df,
            metrics=metrics,
            final_equity=final_equity,
            initial_capital=self.config.initial_capital,
            strategy_id=strategy.strategy_id,
            weight_history=weight_history,
        )

    # ------------------------------------------------------------------
    # Tick 模式
    # ------------------------------------------------------------------

    def _run_tick(
        self,
        strategy: BaseStrategy,
        codes: list[str],
        preloaded_data: dict[str, pd.DataFrame],
    ) -> BacktestResult:
        """Tick 模式回测：逐笔驱动策略，支持延迟、盘口、参与率等。"""
        cfg = self.config

        # ── 构建 tick 事件流 ──────────────────────────────────────────
        tick_events: list[tuple[pd.Timestamp, str, dict]] = []
        for code, df in preloaded_data.items():
            if code not in codes:
                continue
            for ts, row in df.iterrows():
                tick_events.append((pd.Timestamp(cast(Any, ts)), code, row.to_dict()))
        tick_events.sort(key=lambda x: x[0])

        # ── 运行时状态 ───────────────────────────────────────────────
        cash: float = cfg.initial_capital
        positions: dict[str, _Position] = {}
        latest_prices: dict[str, float] = {}
        equity_points: list[tuple[pd.Timestamp, float]] = []
        trades_list: list[dict] = []
        pending_queue: list[dict] = []
        tick_index = 0

        def _nav() -> float:
            return cash + sum(
                p.market_value(latest_prices.get(c, p.avg_cost))
                for c, p in positions.items()
                if p.quantity > 0
            )

        def _pos_values() -> dict[str, float]:
            return {
                c: p.market_value(latest_prices.get(c, p.avg_cost))
                for c, p in positions.items()
                if p.quantity > 0
            }

        def _build_ctx(executor: _Executor) -> StrategyContext:
            return StrategyContext(
                strategy_id=strategy.strategy_id,
                account_id="backtest",
                positions=_pos_values(),
                nav=_nav(),
                params={},
                executor=executor,
                risk_engine=self._risk_engine,
                audit_trail=self._audit_trail,
                factor_snapshot={},
            )

        # ── on_init ──────────────────────────────────────────────────
        _init_exec = _Executor(cfg)
        try:
            strategy.on_init(_build_ctx(_init_exec))
        except Exception:
            self._logger.exception("strategy.on_init 异常 [%s]", strategy.strategy_id)

        # ── 主循环 ───────────────────────────────────────────────────
        for ts, code, tick_dict in tick_events:
            tick_index += 1
            price = float(tick_dict.get("price") or tick_dict.get("lastPrice") or 0.0)
            tick_vol = float(tick_dict.get("volume") or tick_dict.get("lastVolume") or 0.0)

            if price > 0:
                latest_prices[code] = price

            # 本 tick 盘口流动性消耗跟踪（用于 orderbook 模式）
            liquidity_consumed: dict[str, float] = {}

            # STEP 1 — 尝试成交 pending 队列 ──────────────────────────
            still_pending: list[dict] = []
            for order in pending_queue:
                if order["code"] != code:
                    still_pending.append(order)
                    continue

                placed_tick = order.get("_tick_index", 0)
                latency = cfg.tick_latency_ticks
                waited = tick_index - placed_tick

                # 超时撤单
                if cfg.tick_max_wait_ticks > 0 and waited > cfg.tick_max_wait_ticks:
                    retry_count = order.get("_retry_count", 0)
                    if retry_count < cfg.tick_cancel_retry_max:
                        base_price = float(order.get("_origin_price") or order.get("price") or 0.0)
                        current_price = float(order.get("price") or 0.0)
                        adj = current_price * cfg.tick_cancel_retry_price_bps / 10_000
                        next_price = current_price
                        if order["direction"] == "buy":
                            next_price = current_price + adj
                        else:
                            next_price = current_price - adj
                        guard_bps = max(float(cfg.tick_cancel_retry_guard_bps), 0.0)
                        guard_blocked = False
                        if base_price > 0 and guard_bps > 0:
                            if order["direction"] == "buy":
                                max_price = base_price * (1.0 + guard_bps / 10_000)
                                guard_blocked = next_price > max_price
                            else:
                                min_price = base_price * (1.0 - guard_bps / 10_000)
                                guard_blocked = next_price < min_price
                        if guard_blocked:
                            order["status"] = "cancelled"
                            order["error_msg"] = "retry_guard_blocked"
                        else:
                            order["_retry_count"] = retry_count + 1
                            order["price"] = next_price
                            order["_tick_index"] = tick_index
                            still_pending.append(order)
                        od = OrderData(
                            order_id=order["order_id"],
                            signal_id=order.get("signal_id", ""),
                            code=code,
                            direction=order["direction"],
                            volume=order["volume"],
                            price=float(order.get("price") or 0.0),
                            status="cancelled",
                            filled_volume=0,
                            filled_price=0.0,
                            error_msg=str(order.get("error_msg") or "timeout"),
                        )
                        _re_exec = _Executor(cfg)
                        try:
                            strategy.on_order(_build_ctx(_re_exec), od)
                        except Exception:
                            pass
                        continue
                    else:
                        order["status"] = "cancelled"
                        order["error_msg"] = "timeout"
                        od = OrderData(
                            order_id=order["order_id"],
                            signal_id=order.get("signal_id", ""),
                            code=code,
                            direction=order["direction"],
                            volume=order["volume"],
                            price=order["price"],
                            status="cancelled",
                            filled_volume=0,
                            filled_price=0.0,
                            error_msg="timeout",
                        )
                        _re_exec = _Executor(cfg)
                        try:
                            strategy.on_order(_build_ctx(_re_exec), od)
                        except Exception:
                            pass
                        continue

                # 延迟未到（waited <= latency 表示还需等待）
                if waited <= latency:
                    still_pending.append(order)
                    continue

                # 尝试成交（支持部分成交）
                remaining = order.get("_remaining_volume", int(order["volume"]))
                fill_result = self._fill_tick_order(
                    order,
                    tick_dict,
                    cash,
                    positions,
                    trades_list,
                    ts,
                    cfg,
                    remaining_volume=remaining,
                    liquidity_consumed=liquidity_consumed,
                )
                cash = order.get("_cash_after", cash)

                if fill_result > 0:
                    # 有成交（部分或全部）
                    filled_vol = fill_result
                    order["_remaining_volume"] = remaining - filled_vol
                    if order["_remaining_volume"] <= 0:
                        order["status"] = "filled"
                    else:
                        order["status"] = "partial"

                    od = OrderData(
                        order_id=order["order_id"],
                        signal_id=order.get("signal_id", ""),
                        code=code,
                        direction=order["direction"],
                        volume=order["volume"],
                        price=order.get("price", 0.0),
                        status=order["status"],
                        filled_volume=order.get("_total_filled", filled_vol),
                        filled_price=order.get("filled_price", 0.0),
                        error_msg="",
                    )
                    _notify_exec = _Executor(cfg)
                    try:
                        strategy.on_order(_build_ctx(_notify_exec), od)
                    except Exception:
                        self._logger.exception("strategy.on_order 异常")
                    if self._audit_trail is not None and order["status"] == "filled":
                        try:
                            self._audit_trail.record_signal(
                                strategy_id=f"bt:{strategy.strategy_id}",
                                code=code,
                                direction=order["direction"],
                                price_hint=order.get("filled_price"),
                                volume_hint=filled_vol,
                            )
                        except Exception:
                            pass
                    if order["_remaining_volume"] > 0:
                        still_pending.append(order)
                elif fill_result < 0:
                    # 终态（rejected）
                    od = OrderData(
                        order_id=order["order_id"],
                        signal_id=order.get("signal_id", ""),
                        code=code,
                        direction=order["direction"],
                        volume=order["volume"],
                        price=order.get("price", 0.0),
                        status=order.get("status", "rejected"),
                        filled_volume=order.get("_total_filled", 0),
                        filled_price=order.get("filled_price", 0.0),
                        error_msg=order.get("error_msg", ""),
                    )
                    _notify_exec = _Executor(cfg)
                    try:
                        strategy.on_order(_build_ctx(_notify_exec), od)
                    except Exception:
                        pass
                else:
                    # 未成交，继续等待
                    still_pending.append(order)

            pending_queue = still_pending

            # STEP 2 — 更新净值 ───────────────────────────────────────
            nav = _nav()
            equity_points.append((ts, nav))

            # STEP 3 — 驱动策略 on_tick ──────────────────────────────
            tick_data = TickData(
                code=code,
                last=price,
                volume=tick_vol,
                time=ts,
                ask1=float(tick_dict.get("ask1") or tick_dict.get("askPrice1") or 0.0),
                bid1=float(tick_dict.get("bid1") or tick_dict.get("bidPrice1") or 0.0),
                ask1_vol=float(tick_dict.get("ask1_vol") or tick_dict.get("askVol1") or 0.0),
                bid1_vol=float(tick_dict.get("bid1_vol") or tick_dict.get("bidVol1") or 0.0),
                ask2=float(tick_dict.get("ask2") or 0.0),
                ask3=float(tick_dict.get("ask3") or 0.0),
                ask2_vol=float(tick_dict.get("ask2_vol") or 0.0),
                ask3_vol=float(tick_dict.get("ask3_vol") or 0.0),
            )
            tick_exec = _Executor(cfg)
            ctx_tick = _build_ctx(tick_exec)
            try:
                strategy.on_tick(ctx_tick, tick_data)
            except Exception:
                self._logger.exception("strategy.on_tick 异常 code=%s", code)

            # STEP 4 — 风控 + 加入 pending 队列 ──────────────────────
            for order in tick_exec._submitted_orders:
                blocked = False
                if self._risk_engine is not None:
                    try:
                        rr = self._risk_engine.check_pre_trade(
                            account_id="backtest",
                            code=order["code"],
                            volume=float(order["volume"]),
                            price=float(order.get("price") or price),
                            direction=order["direction"],
                            positions=_pos_values(),
                            nav=nav,
                            returns=None,
                            strategy_id=strategy.strategy_id,
                        )
                        if rr.blocked:
                            order["status"] = "rejected"
                            order["error_msg"] = f"risk:{rr.action.value}"
                            blocked = True
                            _re_exec = _Executor(cfg)
                            try:
                                strategy.on_risk(_build_ctx(_re_exec), rr)
                            except Exception:
                                pass
                    except Exception:
                        pass
                if not blocked:
                    order["_tick_index"] = tick_index
                    order["_retry_count"] = 0
                    order["_origin_price"] = float(order.get("price") or 0.0)
                    order["_remaining_volume"] = int(order["volume"])
                    order["_total_filled"] = 0
                    pending_queue.append(order)

        # ── on_stop ──────────────────────────────────────────────────
        _stop_exec = _Executor(cfg)
        try:
            strategy.on_stop(_build_ctx(_stop_exec))
        except Exception:
            self._logger.exception("strategy.on_stop 异常 [%s]", strategy.strategy_id)

        # ── 组装 BacktestResult ──────────────────────────────────────
        final_equity = cash + sum(
            p.market_value(latest_prices.get(c, p.avg_cost))
            for c, p in positions.items()
            if p.quantity > 0
        )
        equity_series = pd.Series(
            [e for _, e in equity_points],
            index=pd.DatetimeIndex([t for t, _ in equity_points]),
            name="equity",
            dtype=float,
        )
        trades_df = pd.DataFrame(trades_list) if trades_list else pd.DataFrame()

        from easyxt_backtest.performance import calc_all_metrics

        metrics = calc_all_metrics(equity_series, trades_df, cfg.initial_capital)

        return BacktestResult(
            equity_curve=equity_series,
            trades=trades_df,
            metrics=metrics,
            final_equity=final_equity,
            initial_capital=cfg.initial_capital,
            strategy_id=strategy.strategy_id,
        )

    def _fill_tick_order(
        self,
        order: dict,
        tick_dict: dict,
        cash: float,
        positions: dict[str, _Position],
        trades_list: list,
        ts: pd.Timestamp,
        cfg: BacktestConfig,
        remaining_volume: int = 0,
        liquidity_consumed: dict[str, float] | None = None,
    ) -> int:
        """尝试在当前 tick 成交 order。

        Returns:
            >0: 本次成交数量
            0: 未成交（继续等待）
            -1: 终态（rejected）
        """
        code = order["code"]
        direction = order["direction"]
        order_price = float(order.get("price") or 0.0)
        price = float(tick_dict.get("price") or tick_dict.get("lastPrice") or 0.0)
        tick_vol = float(tick_dict.get("volume") or tick_dict.get("lastVolume") or 0.0)

        if price <= 0:
            return 0

        if remaining_volume <= 0:
            remaining_volume = int(order["volume"])

        # ── 参与率限制 ──────────────────────────────────────────────
        max_fill = remaining_volume
        if cfg.tick_participation_rate < 1.0 and tick_vol > 0:
            max_fill = min(remaining_volume, int(tick_vol * cfg.tick_participation_rate))
            if max_fill <= 0:
                return 0

        # ── 盘口成交模拟 ────────────────────────────────────────────
        if cfg.tick_use_orderbook:
            ask1 = float(tick_dict.get("ask1") or tick_dict.get("askPrice1") or 0.0)
            bid1 = float(tick_dict.get("bid1") or tick_dict.get("bidPrice1") or 0.0)

            if direction == "buy":
                if order_price < ask1 or ask1 <= 0:
                    return 0
                liq_prefix = "ask"
            else:
                if order_price > bid1 or bid1 <= 0:
                    return 0
                liq_prefix = "bid"

            # 汇总多档流动性（受 order_price 价格过滤）
            lvl_key = f"{liq_prefix}1"
            consumed = liquidity_consumed.get(lvl_key, 0.0) if liquidity_consumed else 0.0
            total_liq = 0.0
            for lvl in range(1, cfg.tick_orderbook_levels + 1):
                px_key = f"{liq_prefix}{lvl}" if lvl > 1 else f"{liq_prefix}1"
                vol_key = f"{liq_prefix}{lvl}_vol" if lvl > 1 else f"{liq_prefix}1_vol"
                lvl_px = float(tick_dict.get(px_key) or tick_dict.get(f"{liq_prefix}Price{lvl}") or 0.0)
                lvl_vol = float(tick_dict.get(vol_key) or tick_dict.get(f"{liq_prefix}Vol{lvl}") or 0.0)
                if lvl_px <= 0:
                    continue
                # 买单：只统计 ≤ order_price 的档位；卖单：只统计 ≥ order_price 的档位
                if direction == "buy" and lvl_px > order_price:
                    continue
                if direction == "sell" and lvl_px < order_price:
                    continue
                total_liq += lvl_vol

            if total_liq > 0:
                avail_liq = max(total_liq - consumed, 0.0)
                if avail_liq <= 0:
                    return 0
                if cfg.tick_participation_rate < 1.0:
                    max_fill = min(max_fill, int(avail_liq * cfg.tick_participation_rate))
                else:
                    max_fill = min(max_fill, int(avail_liq))
                if max_fill <= 0:
                    return 0
            # total_liq == 0 时无盘口量数据，不限制流动性（_weighted_fill_price 内部处理）
            fill_price = self._weighted_fill_price(
                tick_dict, direction, max_fill, cfg.tick_orderbook_levels
            )

            # 更新流动性消耗
            if liquidity_consumed is not None:
                liquidity_consumed[lvl_key] = consumed + max_fill
        else:
            slippage = price * cfg.tick_slippage_bps / 10_000
            fill_price = price + slippage if direction == "buy" else price - slippage
            fill_price = max(fill_price, 0.01)

        # ── 资金/持仓检查 ───────────────────────────────────────────
        trade_value = fill_price * max_fill
        commission = trade_value * cfg.commission_rate

        if direction == "buy":
            total_cost = trade_value + commission
            if cash < total_cost:
                order["status"] = "rejected"
                order["error_msg"] = "insufficient_cash"
                order["_cash_after"] = cash
                return -1
            cash -= total_cost
            if code not in positions:
                positions[code] = _Position(code)
            positions[code].buy(max_fill, fill_price)
        else:
            pos = positions.get(code)
            avail = pos.quantity if pos else 0
            actual_vol = min(max_fill, avail)
            if actual_vol <= 0:
                order["status"] = "rejected"
                order["error_msg"] = "no_position"
                order["_cash_after"] = cash
                return -1
            stamp = trade_value * cfg.stamp_duty
            proceeds = fill_price * actual_vol - commission - stamp
            cash += proceeds
            positions[code].sell(actual_vol)
            max_fill = actual_vol

        order["filled_price"] = fill_price
        order["filled_volume"] = max_fill
        order["_cash_after"] = cash
        order["_total_filled"] = order.get("_total_filled", 0) + max_fill

        trades_list.append(
            {
                "datetime": ts,
                "code": code,
                "direction": direction,
                "volume": max_fill,
                "price": fill_price,
                "order_price": order_price,
                "commission": commission,
                "stamp": 0.0 if direction == "buy" else trade_value * cfg.stamp_duty,
            }
        )
        return max_fill

    @staticmethod
    def _weighted_fill_price(
        tick_dict: dict,
        direction: str,
        volume: int,
        levels: int,
    ) -> float:
        """多档盘口加权成交价。"""
        prefix = "ask" if direction == "buy" else "bid"
        total_cost = 0.0
        remaining = volume
        for lvl in range(1, levels + 1):
            px_key = f"{prefix}{lvl}" if lvl > 1 else f"{prefix}1"
            vol_key = f"{prefix}{lvl}_vol" if lvl > 1 else f"{prefix}1_vol"
            px = float(tick_dict.get(px_key) or tick_dict.get(f"{prefix}Price{lvl}") or 0.0)
            vol = float(tick_dict.get(vol_key) or tick_dict.get(f"{prefix}Vol{lvl}") or 0.0)
            if px <= 0:
                continue
            # 无量数据时假设无限流动性
            fill = remaining if vol <= 0 else min(remaining, int(vol))
            if fill <= 0:
                continue
            total_cost += px * fill
            remaining -= fill
            if remaining <= 0:
                break
        filled = volume - remaining
        if filled <= 0:
            return float(tick_dict.get("price") or tick_dict.get("lastPrice") or 0.0)
        return total_cost / filled

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------

    def _fill_order(
        self,
        order: dict,
        bar: dict,
        cash: float,
        positions: dict[str, _Position],
        trades_list: list,
        ts: pd.Timestamp,
    ) -> None:
        """就地修改 order 字典，更新 cash 与 positions，并追加成交记录。"""
        code = order["code"]
        direction = order["direction"]
        volume = int(order["volume"])
        cfg = self.config
        asset_type = order.get("asset_type", "stock")

        multiplier = cfg.future_contract_multiplier.get(code, 1)
        contract_vol = volume * multiplier

        open_price = float(bar.get("open") or bar.get("Open") or 0.0)
        if open_price <= 0:
            order["status"] = "rejected"
            order["error_msg"] = "invalid_open_price"
            order["_cash_after"] = cash
            return

        slippage = open_price * cfg.slippage_pct
        fill_price = open_price + slippage if direction == "buy" else open_price - slippage
        fill_price = max(fill_price, 0.01)

        trade_value = fill_price * contract_vol
        commission = trade_value * cfg.commission_rate
        stamp = 0.0

        if asset_type == "future":
            margin_rate = 0.12
            required_margin = trade_value * margin_rate
            if direction == "buy":
                if cash < required_margin + commission:
                    order["status"] = "rejected"
                    order["error_msg"] = "insufficient_margin"
                    order["_cash_after"] = cash
                    return
                cash -= required_margin + commission
                if code not in positions:
                    positions[code] = _Position(code)
                positions[code].buy(volume, fill_price)
            else:
                pos = positions.get(code)
                avail = pos.quantity if pos else 0
                actual_vol = min(volume, avail)
                if actual_vol <= 0:
                    order["status"] = "rejected"
                    order["error_msg"] = "no_position"
                    order["_cash_after"] = cash
                    return
                released_margin = fill_price * actual_vol * multiplier * margin_rate
                proceeds = released_margin - commission
                cash += proceeds
                positions[code].sell(actual_vol)
                contract_vol = actual_vol * multiplier
        elif direction == "buy":
            stamp = 0.0
            total_cost = trade_value + commission
            if cash < total_cost:
                order["status"] = "rejected"
                order["error_msg"] = "insufficient_cash"
                order["_cash_after"] = cash
                return
            cash -= total_cost
            if code not in positions:
                positions[code] = _Position(code)
            positions[code].buy(volume, fill_price)
        else:
            stamp = trade_value * cfg.stamp_duty
            pos = positions.get(code)
            avail = pos.quantity if pos else 0
            actual_vol = min(volume, avail)
            if actual_vol <= 0:
                order["status"] = "rejected"
                order["error_msg"] = "no_position"
                order["_cash_after"] = cash
                return
            proceeds = fill_price * actual_vol - commission - stamp
            cash += proceeds
            positions[code].sell(actual_vol)
            volume = actual_vol  # 实际成交量

        order["status"] = "filled"
        order["filled_price"] = fill_price
        order["filled_volume"] = volume
        order["_cash_after"] = cash

        executed_vol = contract_vol if asset_type == "future" else volume
        trades_list.append(
            {
                "datetime": ts,
                "code": code,
                "direction": direction,
                "volume": executed_vol,
                "price": fill_price,
                "commission": commission,
                "stamp": stamp if asset_type == "stock" else 0.0,
                "asset_type": asset_type,
            }
        )

    def _enrich_with_portfolio_risk(
        self,
        metrics: dict[str, Any],
        equity_series: pd.Series,
        positions: dict[str, _Position],
        latest_prices: dict[str, float],
        final_equity: float,
    ) -> None:
        """在 metrics 中追加 PortfolioRiskAnalyzer 结果（best-effort）。"""
        try:
            from core.portfolio_risk import PortfolioRiskAnalyzer
        except ImportError:
            return

        # 构造每日收益率
        if len(equity_series) < 3:
            return
        daily_returns = equity_series.pct_change().dropna().tolist()

        # 各标的组合数据
        portfolio: dict[str, dict[str, Any]] = {}
        for code, pos in positions.items():
            if pos.quantity <= 0:
                continue
            mv = pos.market_value(latest_prices.get(code, pos.avg_cost))
            portfolio[code] = {"nav": mv, "returns": daily_returns}

        if not portfolio:
            return

        try:
            analyzer = PortfolioRiskAnalyzer()
            var_result = analyzer.portfolio_var95(portfolio, total_nav=final_equity)
            metrics["portfolio_var95"] = var_result.portfolio_var95
            metrics["portfolio_var95_pct"] = var_result.portfolio_var95_pct
            metrics["portfolio_cvar95"] = var_result.portfolio_cvar95
            metrics["portfolio_cvar95_pct"] = var_result.portfolio_cvar95_pct
        except Exception:
            self._logger.debug("PortfolioRiskAnalyzer 计算失败，跳过")

    def _persist_risk_events_to_duckdb(
        self,
        strategy_id: str,
        risk_stats: dict[str, Any],
    ) -> None:
        """R4: 将回测结束后的风控事件统计写入 DuckDB ``risk_events`` 表。

        表结构（首次写入时自动创建）::

            risk_events (
                run_ts      TIMESTAMP,   -- 回测执行时间戳
                strategy_id VARCHAR,     -- 策略 ID
                account_id  VARCHAR,     -- 账户 ID
                action      VARCHAR,     -- pass / warn / limit / halt
                count       INTEGER      -- 该 action 出现次数
            )

        写入失败时仅记录 DEBUG 日志，不影响回测结果。
        """
        if not risk_stats or not self.duckdb_path:
            return
        try:
            from datetime import datetime

            import duckdb

            run_ts = datetime.utcnow().isoformat(timespec="seconds")
            rows: list[tuple] = []
            for account_id, counters in risk_stats.items():
                for action, count in counters.items():
                    if count > 0:
                        rows.append((run_ts, strategy_id, account_id, action, count))

            if not rows:
                return

            conn = duckdb.connect(self.duckdb_path)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS risk_events (
                        run_ts      VARCHAR,
                        strategy_id VARCHAR,
                        account_id  VARCHAR,
                        action      VARCHAR,
                        count       INTEGER
                    )
                """)
                conn.executemany("INSERT INTO risk_events VALUES (?, ?, ?, ?, ?)", rows)
                conn.commit()
                self._logger.debug(
                    "R4: 写入 risk_events %d 行 (strategy=%s)", len(rows), strategy_id
                )
            finally:
                conn.close()
        except Exception as exc:
            self._logger.debug("R4: risk_events 写入失败（不影响结果）: %s", exc)

    def _load_data(
        self,
        codes: list[str],
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
    ) -> dict[str, pd.DataFrame]:
        """通过 UnifiedDataInterface 加载数据（本地 DuckDB 优先，在线兜底）。"""
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface
        except ImportError:
            self._logger.error(
                "无法导入 UnifiedDataInterface，请确认 data_manager 包在 sys.path 中"
            )
            return {}

        ui = UnifiedDataInterface(duckdb_path=self.duckdb_path, silent_init=True)
        try:
            ui.connect(read_only=True)
        except Exception:
            self._logger.exception("回测数据连接失败")

        result: dict[str, pd.DataFrame] = {}
        for code in codes:
            try:
                df = ui.get_stock_data(
                    code,
                    start_date,
                    end_date,
                    period=period,
                    adjust=adjust,
                )
                if df is None or (hasattr(df, "empty") and df.empty):
                    self._logger.warning("空数据 %s %s~%s", code, start_date, end_date)
                    continue
                # 归一化为 DatetimeIndex
                if not isinstance(df.index, pd.DatetimeIndex):
                    for col in ("datetime", "date", "time"):
                        if col in df.columns:
                            df = df.set_index(col)
                            break
                df.index = pd.to_datetime(df.index, errors="coerce")
                df = df[df.index.notna()].sort_index()
                result[code] = df
            except Exception:
                self._logger.exception("数据加载失败: %s", code)

        try:
            ui.close()
        except Exception:
            pass

        return result

    def _load_factors(
        self,
        codes: list[str],
        start_date: str,
        end_date: str,
        factor_names: list[str],
    ) -> dict[str, dict[str, pd.Series]]:
        """预加载指定因子的历史时间序列，返回 ``{code: {factor_name: pd.Series}}``。

        优先从 DuckDB ``factor_values`` 表读取；若缺失则通过
        :meth:`~data_manager.unified_data_interface.UnifiedDataInterface.compute_and_save_factor`
        现算并持久化，方便后续回测复用。

        任何单个因子/股票的失败不会中断整体回测——该值置为空 Series 并记录警告。
        """
        result: dict[str, dict[str, pd.Series]] = {code: {} for code in codes}

        try:
            from data_manager.unified_data_interface import UnifiedDataInterface
        except ImportError:
            self._logger.warning("无法导入 UnifiedDataInterface，跳过因子预加载")
            return result

        from data_manager.builtin_factors import register_all_builtin_factors

        ui = UnifiedDataInterface(duckdb_path=self.duckdb_path, silent_init=True)
        try:
            ui.connect()
        except Exception:
            self._logger.exception("因子加载：数据库连接失败，跳过")
            return result

        # 确保内置因子已注册（幂等）
        try:
            register_all_builtin_factors()
        except Exception:
            self._logger.warning("内置因子注册失败（部分因子可能无法计算）")

        for code in codes:
            for fname in factor_names:
                try:
                    # 先尝试从 DuckDB 直接读取
                    series = ui.load_factor(code, fname, start_date, end_date)
                    if series is None or series.empty:
                        # 缓存未命中：现算并持久化
                        count = ui.compute_and_save_factor(fname, code, start_date, end_date)
                        if count > 0:
                            series = ui.load_factor(code, fname, start_date, end_date)
                        else:
                            series = pd.Series(dtype=float)
                    if series is None:
                        series = pd.Series(dtype=float)
                    result[code][fname] = series
                except Exception:
                    self._logger.warning(
                        "因子加载失败: code=%s factor=%s，该因子将返回空值", code, fname
                    )
                    result[code][fname] = pd.Series(dtype=float)

        try:
            ui.close()
        except Exception:
            pass

        return result
