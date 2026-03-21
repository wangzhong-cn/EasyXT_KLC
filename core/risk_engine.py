"""
风控引擎 MVP（Phase 1）

职责：
  - 逐笔预交易风控检查（pre-trade check）
  - 净敞口 / 单标的集中度 / 组合 HHI / 日内回撤 / VaR95 计算
  - 分级动作：PASS → WARN → LIMIT → HALT
"""

from __future__ import annotations

import json
import logging
import math
import threading
import uuid
from collections import deque
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DuckDB persistence helper
# ---------------------------------------------------------------------------

_CREATE_RISK_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS risk_events (
    event_id    VARCHAR PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    event_type  VARCHAR NOT NULL,
    symbol      VARCHAR NOT NULL,
    details_json JSON,
    severity    VARCHAR NOT NULL
)
"""


class _RiskEventDB:
    """薄层封装：将风控触发事件写入 DuckDB ``risk_events`` 表。

    仅在 ``db_path`` 非空时生效；不强依赖 duckdb（import 失败时静默降级）。
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = None
        try:
            import duckdb
            self._conn = duckdb.connect(db_path)
            self._conn.execute(_CREATE_RISK_EVENTS_TABLE)
            self._conn.commit()
        except Exception as exc:  # pragma: no cover
            log.warning("risk_events DuckDB 初始化失败，事件持久化已禁用: %s", exc)
            self._conn = None

    def insert(
        self,
        event_type: str,
        symbol: str,
        details: dict,
        severity: str,
    ) -> None:
        if self._conn is None:
            return
        try:
            self._conn.execute(
                "INSERT INTO risk_events VALUES (?, ?, ?, ?, ?, ?)",
                [
                    str(uuid.uuid4()),
                    datetime.now(tz=timezone.utc).isoformat(),
                    event_type,
                    symbol,
                    json.dumps(details, ensure_ascii=False),
                    severity,
                ],
            )
            self._conn.commit()
        except Exception as exc:  # pragma: no cover
            log.warning("risk_events 写入失败: %s", exc)

    def query_all(self) -> list:
        """返回全部事件行（测试 / 审计用）。"""
        if self._conn is None:
            return []
        rows = self._conn.execute(
            "SELECT event_id, ts, event_type, symbol, details_json, severity "
            "FROM risk_events ORDER BY ts"
        ).fetchall()
        return [
            {
                "event_id": r[0],
                "ts": r[1],
                "event_type": r[2],
                "symbol": r[3],
                "details_json": r[4],
                "severity": r[5],
            }
            for r in rows
        ]

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # pragma: no cover
                pass


# ---------------------------------------------------------------------------
# Enums & Value Types
# ---------------------------------------------------------------------------


class RiskAction(Enum):
    PASS = "pass"        # 正常，允许交易
    WARN = "warn"        # 触发预警，发事件但不拦截
    LIMIT = "limit"      # 限制开仓（可减仓，不可加仓）
    HALT = "halt"        # 强制停止（不可新开任何方向）


@dataclass
class RiskCheckResult:
    action: RiskAction
    reason: str = ""
    metrics: Dict[str, float] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.action == RiskAction.PASS

    @property
    def blocked(self) -> bool:
        return self.action in (RiskAction.LIMIT, RiskAction.HALT)


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------


@dataclass
class RiskThresholds:
    # 单标的集中度上限（占净值比例）
    concentration_limit: float = 0.30
    # 组合 HHI 上限（>0.18 视为过于集中）
    hhi_limit: float = 0.18
    # 日内最大回撤（超过则 HALT）
    intraday_drawdown_halt: float = 0.05
    # 日内回撤预警线
    intraday_drawdown_warn: float = 0.03
    # VaR95 占净值上限
    var95_limit: float = 0.02
    # 净敞口上限（多 - 空 / 净值）
    net_exposure_limit: float = 0.95
    # 每分钟最大下单次数（fat-finger 防护）
    max_orders_per_minute: int = 30
    # 单笔最大委托金额（0 表示不限制）
    max_single_order_value: float = 0.0


# ---------------------------------------------------------------------------
# Risk Engine
# ---------------------------------------------------------------------------


class RiskEngine:
    """
    无状态风控计算核心。外部依赖（持仓、账户净值、历史收益）由调用方注入，
    引擎本身不持有可变状态，方便单元测试。

    日内回撤状态通过 :meth:`update_daily_high` 和 :meth:`get_intraday_drawdown`
    维护在 ``_daily_high`` 字典里，可在每日开盘重置。
    """

    def __init__(
        self,
        thresholds: Optional[RiskThresholds] = None,
        db_path: Optional[str] = None,
    ) -> None:
        self.thresholds = thresholds or RiskThresholds()
        # account_id -> 日内高点
        self._daily_high: Dict[str, float] = {}
        # 分层阈值注册表：key = account_id 或 strategy_id
        self._thresholds_registry: Dict[str, RiskThresholds] = {}
        # 风控事件计数器：account_id -> {action_value -> count}
        self._risk_counters: Dict[str, Dict[str, int]] = {}
        # 可选 DuckDB 持久化
        self._event_db: Optional[_RiskEventDB] = (
            _RiskEventDB(db_path) if db_path else None
        )
        # 下单频率追踪：account_id -> deque[timestamp]
        self._order_timestamps: Dict[str, deque] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Pre-trade gate
    # ------------------------------------------------------------------

    def check_pre_trade(
        self,
        account_id: str,
        code: str,
        volume: float,
        price: float,
        direction: str,                    # "buy" | "sell"
        positions: Dict[str, float],       # code -> market_value
        nav: float,                        # 账户净值
        returns: Optional[List[float]] = None,  # 近期日收益率序列（VaR 计算用）
        strategy_id: str = "",             # 策略 ID（可选，用于分层阈值查找）
    ) -> RiskCheckResult:
        """完整的预交易风控检查。支持分层阈值：account_id专属 > strategy_id专属 > 全局默认。
        所有触发事件自动记入风控统计（get_risk_stats()）；非PASS事件持久化到 risk_events 表。"""
        result = self._do_check_pre_trade(
            account_id=account_id,
            code=code,
            volume=volume,
            price=price,
            direction=direction,
            positions=positions,
            nav=nav,
            returns=returns,
            strategy_id=strategy_id,
        )
        self._record_risk_event(account_id, result.action)
        if result.action != RiskAction.PASS and self._event_db is not None:
            self._event_db.insert(
                event_type=result.action.value,
                symbol=code,
                details={
                    "account_id": account_id,
                    "reason": result.reason,
                    "direction": direction,
                    "volume": volume,
                    "price": price,
                    "metrics": result.metrics,
                },
                severity=(
                    "critical" if result.action == RiskAction.HALT
                    else "warning" if result.action == RiskAction.WARN
                    else "error"
                ),
            )
        return result

    def _do_check_pre_trade(
        self,
        account_id: str,
        code: str,
        volume: float,
        price: float,
        direction: str,
        positions: Dict[str, float],
        nav: float,
        returns: Optional[List[float]] = None,
        strategy_id: str = "",
    ) -> RiskCheckResult:
        """
        内部检查实现（无副作用）。

        Args:
            account_id: 账户 ID，用于日内回撤跟踪。
            code: 标的代码。
            volume: 委托手数/股数（正数）。
            price: 委托价格。
            direction: "buy" 或 "sell"。
            positions: 当前持仓市值字典（code→市值）。
            nav: 账户总净值。
            returns: 近期日收益率序列，用于 VaR95 计算（可选）。
            strategy_id: 策略 ID，用于查找策略专属阈值（优先级低于 account_id）。
        """
        thresholds = self._resolve_thresholds(account_id, strategy_id)
        if nav <= 0:
            return RiskCheckResult(RiskAction.HALT, reason="净值非正，拒绝交易")

        trade_value = volume * price
        metrics: Dict[str, float] = {}

        # 0a. Fat-finger 单笔委托金额检查
        if thresholds.max_single_order_value > 0 and trade_value > thresholds.max_single_order_value:
            metrics["order_value"] = trade_value
            return RiskCheckResult(
                RiskAction.HALT,
                reason=f"单笔委托金额 {trade_value:.0f} 超限 {thresholds.max_single_order_value:.0f}",
                metrics=metrics,
            )

        # 0b. 下单频率检查
        if thresholds.max_orders_per_minute > 0:
            now = datetime.now(tz=timezone.utc).timestamp()
            with self._lock:
                ts_deque = self._order_timestamps.setdefault(account_id, deque())
                cutoff = now - 60.0
                while ts_deque and ts_deque[0] < cutoff:
                    ts_deque.popleft()
                if len(ts_deque) >= thresholds.max_orders_per_minute:
                    metrics["orders_per_minute"] = float(len(ts_deque))
                    return RiskCheckResult(
                        RiskAction.LIMIT,
                        reason=f"每分钟下单 {len(ts_deque)} 次超限 {thresholds.max_orders_per_minute}",
                        metrics=metrics,
                    )
                ts_deque.append(now)

        # 模拟成交后的新持仓（仅用于集中度/HHI 前瞻检查）
        projected = dict(positions)
        if direction == "buy":
            projected[code] = projected.get(code, 0.0) + trade_value
        else:
            projected[code] = max(0.0, projected.get(code, 0.0) - trade_value)

        metrics: Dict[str, float] = {}

        # 1. 净敞口
        net_exp = self.get_net_exposure(projected, nav)
        metrics["net_exposure"] = net_exp
        if direction == "buy" and net_exp > thresholds.net_exposure_limit:
            return RiskCheckResult(
                RiskAction.LIMIT,
                reason=f"净敞口 {net_exp:.1%} 超限 {thresholds.net_exposure_limit:.1%}",
                metrics=metrics,
            )

        # 2. 单标的集中度
        conc = self.get_concentration(projected, code, nav)
        metrics["concentration"] = conc
        if conc > thresholds.concentration_limit:
            return RiskCheckResult(
                RiskAction.LIMIT,
                reason=f"{code} 集中度 {conc:.1%} 超限 {thresholds.concentration_limit:.1%}",
                metrics=metrics,
            )

        # 3. HHI
        hhi = self.get_hhi(projected, nav)
        metrics["hhi"] = hhi
        if hhi > thresholds.hhi_limit:
            log.warning("组合 HHI=%.4f 超预警线 %.4f", hhi, thresholds.hhi_limit)
            # HHI 超限仅警告，不阻断（可在严格模式下改为 LIMIT）

        # 4. 日内回撤
        drawdown = self.get_intraday_drawdown(account_id, nav)
        metrics["intraday_drawdown"] = drawdown
        if drawdown >= thresholds.intraday_drawdown_halt:
            return RiskCheckResult(
                RiskAction.HALT,
                reason=f"日内回撤 {drawdown:.1%} 触发熔断线 {thresholds.intraday_drawdown_halt:.1%}",
                metrics=metrics,
            )
        if drawdown >= thresholds.intraday_drawdown_warn:
            log.warning("日内回撤 %.2f%% 触达预警线", drawdown * 100)
            return RiskCheckResult(
                RiskAction.WARN,
                reason=f"日内回撤 {drawdown:.1%} 触达预警",
                metrics=metrics,
            )

        # 5. VaR95（可选）
        if returns:
            var95 = self.calc_var95(returns)
            metrics["var95"] = var95
            if var95 > thresholds.var95_limit:
                log.warning("VaR95=%.4f 超限 %.4f", var95, thresholds.var95_limit)
                return RiskCheckResult(
                    RiskAction.WARN,
                    reason=f"VaR95 {var95:.2%} 超预警线",
                    metrics=metrics,
                )

        return RiskCheckResult(RiskAction.PASS, metrics=metrics)

    # ------------------------------------------------------------------
    # Stratified threshold registry
    # ------------------------------------------------------------------

    def register_thresholds(self, key: str, thresholds: RiskThresholds) -> None:
        """注册账户或策略专属阈值。key = account_id 或 strategy_id。"""
        self._thresholds_registry[key] = thresholds

    def _resolve_thresholds(
        self, account_id: str, strategy_id: str = ""
    ) -> RiskThresholds:
        """优先级：account_id 专属 > strategy_id 专属 > 全局默认。"""
        return (
            self._thresholds_registry.get(account_id)
            or (self._thresholds_registry.get(strategy_id) if strategy_id else None)
            or self.thresholds
        )

    @staticmethod
    def calibrate_thresholds_from_returns(
        returns: List[float],
        var95_safety_margin: float = 1.5,
        concentration_limit: float = 0.30,
        hhi_limit: float = 0.18,
    ) -> RiskThresholds:
        """
        根据历史收益率序列校准风控阈值。

        算法：
          var95_limit = 历史实际 VaR95 × safety_margin
                        （safety_margin > 1 防止阈值过严阐截正常交易）
          halt_level  = max(3σ 日波动率, 0.04)
          warn_level  = halt_level × 0.6

        Args:
            returns: 近 N 日收益率序列（日粒度）。
            var95_safety_margin: VaR95 阈值放宽倍数，默认 1.5 倍。
            concentration_limit: 单标的集中度上限（不从数据校准）。
            hhi_limit: HHI 上限（不从数据校准）。

        Returns:
            根据数据校准的 :class:`RiskThresholds` 实例。
        """
        import statistics

        if not returns:
            return RiskThresholds()

        # VaR95 内联 calc_var95 逻辑（避免在静态方法里调用实例方法）
        sorted_r = sorted(returns)
        idx = max(0, int(math.ceil(len(sorted_r) * 0.05)) - 1)
        var95_raw = abs(min(sorted_r[idx], 0.0))

        var95_limit = round(min(var95_raw * var95_safety_margin, 0.05), 4)
        std_dev = statistics.stdev(returns) if len(returns) > 1 else 0.02
        halt_level = round(max(std_dev * 3, 0.04), 4)
        warn_level = round(halt_level * 0.6, 4)

        return RiskThresholds(
            concentration_limit=concentration_limit,
            hhi_limit=hhi_limit,
            intraday_drawdown_halt=halt_level,
            intraday_drawdown_warn=warn_level,
            var95_limit=var95_limit,
        )

    # ------------------------------------------------------------------
    # Risk event statistics (for SLO monitoring)
    # ------------------------------------------------------------------

    def _record_risk_event(self, account_id: str, action: RiskAction) -> None:
        if account_id not in self._risk_counters:
            self._risk_counters[account_id] = {a.value: 0 for a in RiskAction}
        self._risk_counters[account_id][action.value] += 1

    def get_risk_stats(self, account_id: Optional[str] = None) -> Dict:
        """
        返回风控事件统计（用于 SLO 监控）。
        account_id=None 时返回所有账户的汇总。
        """
        if account_id:
            return dict(self._risk_counters.get(account_id, {}))
        return {k: dict(v) for k, v in self._risk_counters.items()}

    def reset_risk_stats(self, account_id: Optional[str] = None) -> None:
        """重置风控事件计数器（建议每日零点调用）。"""
        if account_id:
            self._risk_counters.pop(account_id, None)
        else:
            self._risk_counters.clear()

    # ------------------------------------------------------------------
    # Metric calculations (pure, stateless)
    # ------------------------------------------------------------------

    def get_net_exposure(self, positions: Dict[str, float], nav: float) -> float:
        """净敞口 = Σ多头市值 / 净值（空头按负市值处理）。"""
        if nav <= 0:
            return 0.0
        return sum(positions.values()) / nav

    def get_concentration(
        self, positions: Dict[str, float], code: str, nav: float
    ) -> float:
        """单标的集中度 = 该标的市值 / 净值。"""
        if nav <= 0:
            return 0.0
        return positions.get(code, 0.0) / nav

    def get_hhi(self, positions: Dict[str, float], nav: float) -> float:
        """
        Herfindahl-Hirschman Index（HHI）= Σ(wᵢ²)，
        wᵢ = 单标的市值 / 总持仓市值。
        HHI ∈ [1/N, 1]；完全分散 → 0，单一集中 → 1。
        """
        total = sum(v for v in positions.values() if v > 0)
        if total <= 0:
            return 0.0
        return sum((v / total) ** 2 for v in positions.values() if v > 0)

    def get_intraday_drawdown(self, account_id: str, current_nav: float) -> float:
        """
        日内回撤 = (日内高点 - 当前净值) / 日内高点。
        调用此方法会自动更新日内高点。
        """
        with self._lock:
            high = self._daily_high.get(account_id, current_nav)
            if current_nav > high:
                self._daily_high[account_id] = current_nav
                return 0.0
            self._daily_high[account_id] = high
        if high <= 0:
            return 0.0
        return (high - current_nav) / high

    def update_daily_high(self, account_id: str, nav: float) -> None:
        """手动更新日内高点（开盘时调用，以当日开盘净值初始化）。"""
        with self._lock:
            self._daily_high[account_id] = max(self._daily_high.get(account_id, nav), nav)

    def reset_daily_state(self, account_id: Optional[str] = None) -> None:
        """每日开盘前重置日内高点和下单频率计数器（account_id=None 则清空全部）。"""
        with self._lock:
            if account_id is None:
                self._daily_high.clear()
                self._order_timestamps.clear()
            else:
                self._daily_high.pop(account_id, None)
                self._order_timestamps.pop(account_id, None)

    @staticmethod
    def calc_var95(returns: List[float]) -> float:
        """
        历史模拟法 VaR95（绝对值，单日）。
        取收益率序列第 5% 分位的损失（正数表示亏损幅度）。
        """
        if not returns:
            return 0.0
        sorted_r = sorted(returns)
        idx = max(0, int(math.ceil(len(sorted_r) * 0.05)) - 1)
        worst = sorted_r[idx]
        return abs(min(worst, 0.0))
