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

    initial_capital: float = 1_000_000.0   # 初始资金（元）
    commission_rate: float = 0.0003         # 手续费率（双向，万三）
    stamp_duty: float = 0.001               # 印花税（仅卖出）
    slippage_pct: float = 0.0002            # 滑点（按成交价的比例）
    min_trade_unit: int = 100               # 最小交易单位（股）
    fill_on: str = "next_open"              # 成交时机：next_open | current_close
    allow_short: bool = False               # A 股默认不允许做空
    tick_slippage_bps: float = 0.0          # tick模式额外滑点（bp）
    tick_latency_ticks: int = 0             # tick模式延迟成交tick数
    tick_use_orderbook: bool = False        # tick模式按bid1/ask1撮合
    tick_participation_rate: float = 1.0    # tick模式单tick最大参与率（0~1）
    tick_orderbook_levels: int = 1          # tick模式盘口档位（1~5）
    tick_max_wait_ticks: int = -1           # tick模式最大等待tick数，超出后撤单（-1禁用）
    tick_cancel_retry_max: int = 0          # tick撤单后最大重挂次数
    tick_cancel_retry_price_bps: float = 0.0  # 每次重挂限价调整（bp）


# ---------------------------------------------------------------------------
# 输出结果
# ---------------------------------------------------------------------------


@dataclass
class BacktestResult:
    """回测输出结果。"""

    equity_curve: pd.Series                 # DatetimeIndex → 净值
    trades: pd.DataFrame                    # 成交记录表
    metrics: dict[str, Any]                 # 绩效指标字典
    final_equity: float
    initial_capital: float
    strategy_id: str
    weight_history: list[dict[str, Any]] = field(default_factory=list)  # type: ignore[assignment]
    """每次再平衡后的权重快照列表，每项 {"datetime": Timestamp, "weights": {code: weight}}。"""


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
    ) -> str:
        """提交一笔订单（异步——将在下一 bar 开盘成交）。

        Args:
            code:      股票代码
            volume:    委托数量（股）
            price:     委托价格（参考价，实际按 fill_on 规则成交）
            direction: "buy" | "sell"
            signal_id: 关联信号 ID（可选）

        Returns:
            order_id 字符串（空字符串表示委托被过滤）
        """
        # A 股最小交易单位约束
        volume_int = (
            int(volume // self._config.min_trade_unit) * self._config.min_trade_unit
        )
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
            preloaded_data: 外部已准备好的 OHLCV 数据（{code: DataFrame}），
                若提供则跳过 ``_load_data`` 内部数据源请求，直接使用该数据。
                用于 GUI 层复用已加载的行情，避免重复 I/O。
        """
        data = preloaded_data if preloaded_data is not None else self._load_data(
            codes, start_date, end_date, period, adjust
        )
        if not data:
            raise ValueError(
                f"无法加载数据: codes={codes} {start_date}~{end_date} period={period}"
            )

        # ── 预加载因子时间序列（索引对齐后转为 {code: {factor: Series}}） ──
        factor_series: dict[str, dict[str, pd.Series]] = {}
        if factors_to_load:
            factor_series = self._load_factors(
                codes, start_date, end_date, factors_to_load
            )

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
        pending_fill_queue: list[dict] = []   # 等待下一 bar 成交
        order_queue_seq = 0
        returns_history: list[float] = []
        weight_history: list[dict] = []        # 权重快照
        _prev_trades_count = 0  # 用于检测当前 bar 是否发生了成交

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
            still_pending: list[dict] = []
            tick_liquidity_state: dict[str, dict[str, Any]] = {}
            if period == "tick":
                unit = max(int(self.config.min_trade_unit), 1)
                rate = max(0.0, min(float(self.config.tick_participation_rate), 1.0))
                for code, bar in bar_by_code.items():
                    levels = max(1, min(int(self.config.tick_orderbook_levels), 5))
                    ask_levels: list[list[float]] = []
                    bid_levels: list[list[float]] = []
                    for lv in range(1, levels + 1):
                        ask_px = float(
                            bar.get(f"ask{lv}")
                            or bar.get(f"ask_price_{lv}")
                            or bar.get(f"askPrice{lv}")
                            or 0.0
                        )
                        ask_vol_raw = (
                            bar.get(f"ask{lv}_vol")
                            or bar.get(f"ask{lv}_volume")
                            or bar.get(f"askVol{lv}")
                            or bar.get(f"ask_vol_{lv}")
                        )
                        bid_px = float(
                            bar.get(f"bid{lv}")
                            or bar.get(f"bid_price_{lv}")
                            or bar.get(f"bidPrice{lv}")
                            or 0.0
                        )
                        bid_vol_raw = (
                            bar.get(f"bid{lv}_vol")
                            or bar.get(f"bid{lv}_volume")
                            or bar.get(f"bidVol{lv}")
                            or bar.get(f"bid_vol_{lv}")
                        )
                        if ask_px > 0:
                            ask_qty = -1 if ask_vol_raw is None else int((float(ask_vol_raw) // unit) * unit)
                            ask_levels.append([ask_px, float(ask_qty)])
                        if bid_px > 0:
                            bid_qty = -1 if bid_vol_raw is None else int((float(bid_vol_raw) // unit) * unit)
                            bid_levels.append([bid_px, float(bid_qty)])
                    if rate < 1.0:
                        bar_vol = float(
                            bar.get("volume")
                            or bar.get("Volume")
                            or bar.get("last_volume")
                            or bar.get("vol")
                            or 0.0
                        )
                        part_left = int((bar_vol * rate) // unit) * unit
                    else:
                        part_left = -1
                    tick_liquidity_state[code] = {
                        "ask_levels": ask_levels,
                        "bid_levels": bid_levels,
                        "part_left": part_left,
                    }
            if period == "tick":
                pending_iter = sorted(
                    pending_fill_queue,
                    key=lambda o: (
                        str(o.get("code") or ""),
                        0 if str(o.get("direction") or "") == "buy" else 1,
                        -(float(o.get("price") or 0.0)) if str(o.get("direction") or "") == "buy" else float(o.get("price") or 0.0),
                        int(o.get("_queue_seq") or 0),
                    ),
                )
            else:
                pending_iter = pending_fill_queue
            for order in pending_iter:
                code = order["code"]
                bar = bar_by_code.get(code)
                if bar is None:
                    still_pending.append(order)
                    continue
                if period == "tick":
                    latency_left = int(order.get("_latency_left", 0))
                    if latency_left > 0:
                        order["_latency_left"] = latency_left - 1
                        still_pending.append(order)
                        continue
                    max_wait = int(self.config.tick_max_wait_ticks)
                    wait_ticks = int(order.get("_wait_ticks", 0)) + 1
                    order["_wait_ticks"] = wait_ticks
                    if max_wait >= 0 and wait_ticks > max_wait:
                        order["status"] = "cancelled"
                        order["error_msg"] = "timeout_cancelled"
                        order["_cash_after"] = cash
                if order.get("status") == "cancelled":
                    cash = order.get("_cash_after", cash)
                else:
                    self._fill_order(
                        order, bar, cash, positions, trades_list, ts, period, tick_liquidity_state
                    )
                    cash = order.get("_cash_after", cash)
                if order.get("status") == "submitted":
                    still_pending.append(order)
                    continue
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
                if period == "tick" and order.get("status") == "cancelled":
                    retry_max = max(int(self.config.tick_cancel_retry_max), 0)
                    retry_count = int(order.get("_retry_count", 0))
                    if retry_count < retry_max:
                        retry_order = dict(order)
                        retry_order["status"] = "submitted"
                        retry_order["filled_price"] = 0.0
                        retry_order["filled_volume"] = 0
                        retry_order["error_msg"] = ""
                        retry_order["_wait_ticks"] = 0
                        retry_order["_latency_left"] = max(int(self.config.tick_latency_ticks), 0)
                        retry_order["_retry_count"] = retry_count + 1
                        retry_order["_queue_seq"] = order_queue_seq
                        order_queue_seq += 1
                        base_px = float(retry_order.get("price") or 0.0)
                        bps = float(self.config.tick_cancel_retry_price_bps)
                        if base_px > 0 and bps > 0:
                            if str(retry_order.get("direction") or "") == "buy":
                                retry_order["price"] = base_px * (1.0 + bps / 10000.0)
                            else:
                                retry_order["price"] = base_px * (1.0 - bps / 10000.0)
                        still_pending.append(retry_order)
                # AuditTrail
                if self._audit_trail is not None and order["status"] == "filled":
                    try:
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

            # STEP 2 — 更新最新价格 & 计算净值 ──────────────────────────────────────────────────────
            _had_fills = len(trades_list) > _prev_trades_count
            _prev_trades_count = len(trades_list)
            for code, bar in bar_by_code.items():
                close_val = (
                    bar.get("close")
                    or bar.get("Close")
                    or bar.get("price")
                    or bar.get("last")
                    or bar.get("last_price")
                    or 0.0
                )
                if close_val and float(close_val) > 0:
                    latest_prices[code] = float(close_val)

            nav = _nav()
            equity_points.append((ts, nav))

            # 在价格更新后记录权重快照（每次有成交时）
            if _had_fills and nav > 0:
                pos_vals = _pos_values()
                total_inv = sum(pos_vals.values())
                if total_inv > 0:
                    weight_history.append({
                        "datetime": ts,
                        "weights": {c: v / nav for c, v in pos_vals.items()},
                    })

            if len(equity_points) >= 2:
                prev_nav = equity_points[-2][1]
                if prev_nav > 0:
                    returns_history.append((nav - prev_nav) / prev_nav)

            # STEP 3 — 按 bar 驱动策略，收集新订单 ───────────────────────
            new_orders_this_bar: list[dict] = []
            for code, bar in bar_by_code.items():
                px = float(
                    bar.get("price")
                    or bar.get("last")
                    or bar.get("last_price")
                    or bar.get("close")
                    or bar.get("Close")
                    or 0.0
                )
                vol = float(
                    bar.get("volume")
                    or bar.get("Volume")
                    or bar.get("last_volume")
                    or bar.get("vol")
                    or 0.0
                )
                bar_data = BarData(
                    code=code,
                    period=period,
                    open=float(bar.get("open") or bar.get("Open") or px),
                    high=float(bar.get("high") or bar.get("High") or px),
                    low=float(bar.get("low") or bar.get("Low") or px),
                    close=float(bar.get("close") or bar.get("Close") or px),
                    volume=vol,
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
                    if period == "tick":
                        tick_data = TickData(
                            code=code,
                            last=bar_data.close,
                            volume=bar_data.volume,
                            bid1=float(bar.get("bid1") or bar.get("bid_price_1") or 0.0),
                            ask1=float(bar.get("ask1") or bar.get("ask_price_1") or 0.0),
                            time=ts,
                        )
                        strategy.on_tick(ctx_bar, tick_data)
                    else:
                        strategy.on_bar(ctx_bar, bar_data)
                except Exception:
                    self._logger.exception("strategy.on_bar/on_tick 异常 code=%s", code)

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
                    if period == "tick":
                        order["_queue_seq"] = order_queue_seq
                        order_queue_seq += 1
                        order["_latency_left"] = max(int(self.config.tick_latency_ticks), 0)
                        order["_wait_ticks"] = 0
                        order["_retry_count"] = 0
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
                metrics, equity_series, positions, latest_prices, final_equity,
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
        period: str = "1d",
        tick_liquidity_state: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        """就地修改 order 字典，更新 cash 与 positions，并追加成交记录。"""
        code = order["code"]
        direction = order["direction"]
        volume = int(order["volume"])
        cfg = self.config
        original_volume = volume

        # 成交价：开盘价 + 滑点
        open_price = float(
            bar.get("open")
            or bar.get("Open")
            or bar.get("price")
            or bar.get("last")
            or bar.get("last_price")
            or bar.get("close")
            or bar.get("Close")
            or 0.0
        )
        if period == "tick" and cfg.tick_use_orderbook:
            state = (tick_liquidity_state or {}).get(code, {})
            ask_levels = cast(list[list[float]], state.get("ask_levels", []))
            bid_levels = cast(list[list[float]], state.get("bid_levels", []))
            best_ask = float(ask_levels[0][0]) if ask_levels else float(bar.get("ask1") or bar.get("ask_price_1") or 0.0)
            best_bid = float(bid_levels[0][0]) if bid_levels else float(bar.get("bid1") or bar.get("bid_price_1") or 0.0)
            limit_price = float(order.get("price") or 0.0)
            if direction == "buy":
                if best_ask <= 0:
                    order["status"] = "submitted"
                    order["_cash_after"] = cash
                    return
                if limit_price > 0 and limit_price < best_ask:
                    order["status"] = "submitted"
                    order["_cash_after"] = cash
                    return
                open_price = best_ask
            else:
                if best_bid <= 0:
                    order["status"] = "submitted"
                    order["_cash_after"] = cash
                    return
                if limit_price > 0 and limit_price > best_bid:
                    order["status"] = "submitted"
                    order["_cash_after"] = cash
                    return
                open_price = best_bid

        if period == "tick":
            state = (tick_liquidity_state or {}).get(code, {})
            part_left = int(state.get("part_left", -1))
            cap_by_participation = volume if part_left < 0 else part_left
            desired_volume = max(min(volume, cap_by_participation), 0)
            cap_by_book = desired_volume
            avg_fill_from_book = 0.0
            if cfg.tick_use_orderbook:
                levels = cast(list[list[float]], state.get("ask_levels" if direction == "buy" else "bid_levels", []))
                limit_price = float(order.get("price") or 0.0)
                remain = desired_volume
                value_sum = 0.0
                filled = 0
                for lvl in levels:
                    px = float(lvl[0])
                    qty = float(lvl[1])
                    if direction == "buy" and limit_price > 0 and px > limit_price:
                        continue
                    if direction == "sell" and limit_price > 0 and px < limit_price:
                        continue
                    if remain <= 0:
                        break
                    lvl_cap = remain if qty < 0 else min(remain, int(qty))
                    if lvl_cap <= 0:
                        continue
                    value_sum += float(lvl_cap) * px
                    filled += int(lvl_cap)
                    remain -= int(lvl_cap)
                    if qty >= 0:
                        lvl[1] = max(float(qty) - float(lvl_cap), 0.0)
                cap_by_book = filled
                avg_fill_from_book = (value_sum / filled) if filled > 0 else 0.0
                if direction == "buy":
                    pass
                if direction == "sell":
                    pass
            volume = max(min(volume, cap_by_participation, cap_by_book), 0)
            if volume <= 0:
                order["status"] = "submitted"
                order["_cash_after"] = cash
                return
            if cfg.tick_use_orderbook and avg_fill_from_book > 0:
                open_price = avg_fill_from_book
        if open_price <= 0:
            order["status"] = "rejected"
            order["error_msg"] = "invalid_open_price"
            order["_cash_after"] = cash
            return

        slippage = open_price * cfg.slippage_pct
        if period == "tick" and cfg.tick_slippage_bps > 0:
            slippage = open_price * (cfg.tick_slippage_bps / 10000.0)
        fill_price = open_price + slippage if direction == "buy" else open_price - slippage
        fill_price = max(fill_price, 0.01)

        trade_value = fill_price * volume
        commission = trade_value * cfg.commission_rate

        if direction == "buy":
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
        else:  # sell
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
        if period == "tick" and tick_liquidity_state is not None and code in tick_liquidity_state:
            state = tick_liquidity_state[code]
            if int(state.get("part_left", -1)) >= 0:
                state["part_left"] = max(int(state.get("part_left", 0)) - int(volume), 0)
        remain = max(original_volume - volume, 0)
        if remain > 0 and period == "tick":
            order["status"] = "submitted"
            order["volume"] = remain
        order["_cash_after"] = cash

        trades_list.append(
            {
                "datetime": ts,
                "code": code,
                "direction": direction,
                "volume": volume,
                "price": fill_price,
                "order_price": float(order.get("price") or 0.0),
                "commission": commission,
                "stamp": stamp,
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
                conn.executemany(
                    "INSERT INTO risk_events VALUES (?, ?, ?, ?, ?)", rows
                )
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
            self._logger.error("无法导入 UnifiedDataInterface，请确认 data_manager 包在 sys.path 中")
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
                        count = ui.compute_and_save_factor(
                            fname, code, start_date, end_date
                        )
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
