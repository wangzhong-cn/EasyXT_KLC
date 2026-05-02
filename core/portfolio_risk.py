"""
组合级风控模块（Phase 2）

职责：
  - 多持仓组合的历史模拟 VaR / CVaR
  - 行业集中度分析（HHI、前 N 集中度）
  - 多账户敞口聚合
  - 与 RiskEngine 单标的风控形成两级体系

用法示例::

    from core.portfolio_risk import PortfolioRiskAnalyzer

    analyzer = PortfolioRiskAnalyzer()

    # 组合 VaR：多标的各自提供历史收益率序列
    portfolio = {
        "000001.SZ": {"nav": 50000.0, "returns": [-0.01, 0.02, ...]},
        "600519.SH": {"nav": 30000.0, "returns": [ 0.01, -0.03, ...]},
    }
    result = analyzer.portfolio_var95(portfolio, total_nav=200000.0)

    # 行业集中度
    positions = {"000001.SZ": 50000, "000002.SZ": 30000, "002415.SZ": 20000}
    sector_map = {"000001.SZ": "银行", "000002.SZ": "房地产", "002415.SZ": "科技"}
    concentration = analyzer.sector_concentration(positions, sector_map)
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 数据容器
# ---------------------------------------------------------------------------


@dataclass
class PortfolioVaRResult:
    """组合 VaR 计算结果。"""
    # 组合整体 VaR95（历史模拟法，单日绝对损失额）
    portfolio_var95: float
    # 组合整体 VaR95 占总净值比例
    portfolio_var95_pct: float
    # CVaR（条件风险值）：超过 VaR 阈值部分的平均损失
    portfolio_cvar95: float
    portfolio_cvar95_pct: float
    # 各标的 VaR 贡献（绝对金额）
    per_position_var95: Dict[str, float] = field(default_factory=dict)
    # 组合收益率序列（由各持仓加权合并）
    portfolio_returns: List[float] = field(default_factory=list)
    # 序列长度
    n_observations: int = 0


@dataclass
class SectorConcentrationResult:
    """行业集中度分析结果。"""
    # 各行业持仓市值占比
    sector_weights: Dict[str, float] = field(default_factory=dict)
    # HHI（按行业）
    hhi: float = 0.0
    # 前 N 行业集中度（前 3 行业合计占比）
    top3_concentration: float = 0.0
    # 最大单一行业占比
    max_sector_weight: float = 0.0
    # 最大单一行业名称
    max_sector_name: str = ""
    # 未分类（sector_map 未覆盖）标的合计占比
    unknown_weight: float = 0.0


@dataclass
class MultiAccountExposure:
    """多账户敞口聚合结果。"""
    # 各账户净值
    account_navs: Dict[str, float] = field(default_factory=dict)
    # 总净值
    total_nav: float = 0.0
    # 各账户多头持仓市值
    account_gross_long: Dict[str, float] = field(default_factory=dict)
    # 全平台合计多头敞口
    platform_gross_long: float = 0.0
    # 全平台合计净敞口占平台总净值比例
    platform_net_exposure_pct: float = 0.0
    # 各账户合并后各标的总持仓（code -> 市值）
    aggregated_positions: Dict[str, float] = field(default_factory=dict)
    # 合并持仓 HHI
    aggregated_hhi: float = 0.0


# ---------------------------------------------------------------------------
# 主类
# ---------------------------------------------------------------------------


class PortfolioRiskAnalyzer:
    """
    无状态组合风控计算器。所有输入由调用方注入，方便单元测试。
    线程安全（纯函数式，不持有可变状态）。
    """

    # ------------------------------------------------------------------
    # 1. 组合 VaR / CVaR（历史模拟法）
    # ------------------------------------------------------------------

    def portfolio_var95(
        self,
        positions: Dict[str, Dict],
        total_nav: float,
        correlation_method: str = "weighted_sum",
    ) -> PortfolioVaRResult:
        """
        多持仓组合历史模拟 VaR95。

        Args:
            positions: {
                symbol: {
                    "nav":     float,         # 该标的当前市值
                    "returns": list[float],   # 该标的日收益率序列（时间对齐）
                }
            }
            total_nav:            账户总净值（含现金）。
            correlation_method:   "weighted_sum"（保守，忽略相关性，适合散户用）|
                                  "portfolio"（利用组合收益序列，更精确，需序列等长）。

        Returns:
            PortfolioVaRResult
        """
        if not positions or total_nav <= 0:
            return PortfolioVaRResult(0.0, 0.0, 0.0, 0.0)

        per_position_var: Dict[str, float] = {}
        weighted_returns_map: Dict[str, List[float]] = {}

        for symbol, pos in positions.items():
            pos_nav = float(pos.get("nav", 0.0))
            returns = [float(r) for r in pos.get("returns", [])]
            if not returns or pos_nav <= 0:
                continue
            var95 = self._calc_var95(returns)
            per_position_var[symbol] = var95 * pos_nav   # 损失金额
            weight = pos_nav / total_nav
            weighted_returns_map[symbol] = [r * weight for r in returns]

        if not weighted_returns_map:
            return PortfolioVaRResult(0.0, 0.0, 0.0, 0.0, per_position_var)

        if correlation_method == "portfolio":
            # 需要序列等长，否则截断到最短
            min_len = min(len(v) for v in weighted_returns_map.values())
            portfolio_returns = [
                sum(weighted_returns_map[s][i] for s in weighted_returns_map)
                for i in range(min_len)
            ]
        else:
            # weighted_sum：每标的 weighted_return 序列可以不等长，取相同时间点叠加
            # 简化：使用各序列平均长度对齐的加总
            max_len = max(len(v) for v in weighted_returns_map.values())
            portfolio_returns = [0.0] * max_len
            for symbol, wr in weighted_returns_map.items():
                for i, r in enumerate(wr):
                    portfolio_returns[i] += r

        n = len(portfolio_returns)
        port_var95 = self._calc_var95(portfolio_returns) * total_nav
        port_cvar95 = self._calc_cvar95(portfolio_returns) * total_nav

        return PortfolioVaRResult(
            portfolio_var95=round(port_var95, 2),
            portfolio_var95_pct=round(port_var95 / total_nav, 6),
            portfolio_cvar95=round(port_cvar95, 2),
            portfolio_cvar95_pct=round(port_cvar95 / total_nav, 6),
            per_position_var95=per_position_var,
            portfolio_returns=portfolio_returns,
            n_observations=n,
        )

    # ------------------------------------------------------------------
    # 2. 行业集中度分析
    # ------------------------------------------------------------------

    def sector_concentration(
        self,
        positions: Dict[str, float],    # code -> 市值
        sector_map: Dict[str, str],     # code -> 行业名称
        top_n: int = 3,
    ) -> SectorConcentrationResult:
        """
        计算行业层面集中度。

        Args:
            positions:  {code: 市值}。
            sector_map: {code: 行业名称}。未出现在 sector_map 中的标的归入 "未分类"。
            top_n:      前 N 行业集中度阈值，默认 3。
        """
        total = sum(max(v, 0.0) for v in positions.values())
        if total <= 0:
            return SectorConcentrationResult()

        sector_nav: Dict[str, float] = {}
        unknown_nav = 0.0
        for code, mv in positions.items():
            if mv <= 0:
                continue
            sector = sector_map.get(code, "")
            if sector:
                sector_nav[sector] = sector_nav.get(sector, 0.0) + mv
            else:
                unknown_nav += mv

        sector_weights = {s: sector_nav[s] / total for s in sector_nav}
        unknown_weight = unknown_nav / total

        if not sector_weights:
            return SectorConcentrationResult(unknown_weight=unknown_weight)

        # HHI（按行业加权）
        hhi = sum(w ** 2 for w in sector_weights.values())

        # 前 N 集中度
        sorted_weights = sorted(sector_weights.values(), reverse=True)
        topn_conc = sum(sorted_weights[:top_n])

        max_sector = max(sector_weights, key=sector_weights.get)  # type: ignore[arg-type]

        return SectorConcentrationResult(
            sector_weights=sector_weights,
            hhi=round(hhi, 4),
            top3_concentration=round(topn_conc, 4),
            max_sector_weight=round(sector_weights[max_sector], 4),
            max_sector_name=max_sector,
            unknown_weight=round(unknown_weight, 4),
        )

    # ------------------------------------------------------------------
    # 3. 多账户敞口聚合
    # ------------------------------------------------------------------

    def aggregate_multi_account(
        self,
        accounts: Dict[str, Dict],
    ) -> MultiAccountExposure:
        """
        聚合多个账户的持仓和净值。

        Args:
            accounts: {
                account_id: {
                    "nav":       float,            # 账户净值
                    "positions": {code: 市值},     # 持仓
                }
            }

        Returns:
            MultiAccountExposure — 平台层面的净敞口和聚合持仓。
        """
        result = MultiAccountExposure()
        aggregated: Dict[str, float] = {}

        for acct_id, info in accounts.items():
            nav = float(info.get("nav", 0.0))
            positions = info.get("positions", {}) or {}
            result.account_navs[acct_id] = nav
            result.total_nav += nav

            gross_long = sum(max(v, 0.0) for v in positions.values())
            result.account_gross_long[acct_id] = gross_long
            result.platform_gross_long += gross_long

            for code, mv in positions.items():
                if mv > 0:
                    aggregated[code] = aggregated.get(code, 0.0) + mv

        result.aggregated_positions = aggregated

        if result.total_nav > 0:
            result.platform_net_exposure_pct = round(
                result.platform_gross_long / result.total_nav, 4
            )

        # 聚合持仓 HHI
        total_pos = sum(result.aggregated_positions.values())
        if total_pos > 0:
            result.aggregated_hhi = round(
                sum((v / total_pos) ** 2 for v in result.aggregated_positions.values()), 4
            )

        return result

    # ------------------------------------------------------------------
    # 4. Beta-to-market 估算（线性回归）
    # ------------------------------------------------------------------

    @staticmethod
    def estimate_beta(
        asset_returns: List[float],
        market_returns: List[float],
    ) -> float:
        """
        用最小二乘法估算资产相对市场的 Beta。

        Args:
            asset_returns:  资产日收益率序列。
            market_returns: 市场基准（如沪深 300）日收益率序列。

        Returns:
            beta 估计值（1.0 = 与市场同步）。
        """
        n = min(len(asset_returns), len(market_returns))
        if n < 5:
            return 1.0    # 样本不足，返回中性 Beta
        xs = market_returns[:n]
        ys = asset_returns[:n]
        mean_x = sum(xs) / n
        mean_y = sum(ys) / n
        cov = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
        var_x = sum((xs[i] - mean_x) ** 2 for i in range(n))
        if var_x == 0:
            return 1.0
        return cov / var_x

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------

    @staticmethod
    def _calc_var95(returns: List[float]) -> float:
        """历史模拟 VaR95（绝对比例，单日）。"""
        if not returns:
            return 0.0
        sorted_r = sorted(returns)
        idx = max(0, int(math.floor(len(sorted_r) * 0.05)) - 1)
        worst = sorted_r[idx]
        return abs(min(worst, 0.0))

    @staticmethod
    def _calc_cvar95(returns: List[float]) -> float:
        """
        条件风险值 CVaR95（Expected Shortfall）：超过 VaR 阈值的平均损失。
        """
        if not returns:
            return 0.0
        sorted_r = sorted(returns)
        cutoff_idx = max(1, int(math.floor(len(sorted_r) * 0.05)))
        tail = sorted_r[:cutoff_idx]
        avg_tail = sum(tail) / len(tail)
        return abs(min(avg_tail, 0.0))

    # ------------------------------------------------------------------
    # 优化权重风控校验
    # ------------------------------------------------------------------

    @staticmethod
    def check_optimal_weights(
        weights: Dict[str, float],
        max_single_weight: float = 0.3,
        max_hhi: float = 0.25,
    ) -> "OptimalWeightRiskCheck":
        """
        对优化器输出的权重进行风控校验。

        Args:
            weights:           各资产权重字典，合计应为 1.0。
            max_single_weight: 单资产最大权重限制（默认 30%）。
            max_hhi:           权重 HHI 上限（默认 0.25，即均匀分散）。

        Returns:
            OptimalWeightRiskCheck 实例。
        """
        if not weights:
            return OptimalWeightRiskCheck(feasible=False, warnings=["权重字典为空"])

        total = sum(weights.values())
        w_norm = {k: v / total for k, v in weights.items()} if total > 0 else weights

        max_w = max(w_norm.values())
        hhi = sum(v ** 2 for v in w_norm.values())

        warns: List[str] = []
        single_breach = max_w > max_single_weight + 1e-9
        hhi_breach = hhi > max_hhi + 1e-9

        if single_breach:
            warns.append(f"单仓占比超限 {max_w:.2f} > {max_single_weight:.2f}")
        if hhi_breach:
            warns.append(f"HHI 超限 {hhi:.2f} > {max_hhi:.2f}")

        return OptimalWeightRiskCheck(
            feasible=not (single_breach or hhi_breach),
            max_single_weight=max_w,
            weight_hhi=hhi,
            max_single_breach=single_breach,
            hhi_breach=hhi_breach,
            warnings=warns,
        )

    # ------------------------------------------------------------------
    # 便捷工厂
    # ------------------------------------------------------------------

    @classmethod
    def quick_var(
        cls,
        positions: Dict[str, Tuple[float, List[float]]],
        total_nav: float,
    ) -> PortfolioVaRResult:
        """
        简化调用入口：positions = {symbol: (市值, returns列表)}。

        示例::

            result = PortfolioRiskAnalyzer.quick_var(
                {"000001.SZ": (50000, [-0.01, 0.02, ...])},
                total_nav=200000,
            )
        """
        expanded = {
            sym: {"nav": nav, "returns": rets}
            for sym, (nav, rets) in positions.items()
        }
        return cls().portfolio_var95(expanded, total_nav)


# ---------------------------------------------------------------------------
# 优化权重风控校验
# ---------------------------------------------------------------------------


@dataclass
class OptimalWeightRiskCheck:
    """check_optimal_weights 的校验结果。"""

    feasible: bool = True
    """校验整体是否通过（True=无超限，False=至少一项超限）"""

    max_single_weight: float = 0.0
    """当前最大单仓权重"""

    weight_hhi: float = 0.0
    """权重 HHI 指数（集中度）"""

    max_single_breach: bool = False
    """是否存在单仓权重超限"""

    hhi_breach: bool = False
    """是否 HHI 超限"""

    warnings: List[str] = field(default_factory=list)  # type: ignore[assignment]
    """警告信息列表"""
