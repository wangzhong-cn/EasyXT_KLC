"""
tests/test_portfolio_risk.py
单元测试：core.portfolio_risk.PortfolioRiskAnalyzer
"""

import math
import pytest

from core.portfolio_risk import PortfolioRiskAnalyzer, PortfolioVaRResult


# ---------------------------------------------------------------------------
# 辅助数据
# ---------------------------------------------------------------------------

def _simple_returns(n: int = 200, seed_val: float = 0.005) -> list[float]:
    """生成简单周期性收益率序列（确定性，无随机依赖）。"""
    returns = []
    for i in range(n):
        returns.append(seed_val * math.sin(i * 0.3))
    # 加入几个尾部亏损，以便 VaR 计算有意义
    for j in range(0, n, 20):
        returns[j] = -0.03
    return returns


# ---------------------------------------------------------------------------
# PortfolioVaRResult — 基本字段
# ---------------------------------------------------------------------------


class TestPortfolioVaRResult:
    def test_default_fields(self):
        r = PortfolioVaRResult(
            portfolio_var95=1000.0,
            portfolio_var95_pct=0.01,
            portfolio_cvar95=1500.0,
            portfolio_cvar95_pct=0.015,
        )
        assert r.portfolio_var95 == 1000.0
        assert r.portfolio_var95_pct == 0.01
        assert r.per_position_var95 == {}
        assert r.portfolio_returns == []

    def test_cvar_geq_var(self):
        """CVaR 应 >= VaR。"""
        analyzer = PortfolioRiskAnalyzer()
        rets = _simple_returns(100)
        positions = {
            "A": {"nav": 100000.0, "returns": rets},
        }
        result = analyzer.portfolio_var95(positions, total_nav=100000.0)
        assert result.portfolio_cvar95 >= result.portfolio_var95


# ---------------------------------------------------------------------------
# portfolio_var95 — 核心逻辑
# ---------------------------------------------------------------------------


class TestPortfolioVar95:
    def setup_method(self):
        self.analyzer = PortfolioRiskAnalyzer()

    def test_empty_positions_returns_zero(self):
        result = self.analyzer.portfolio_var95({}, total_nav=100000.0)
        assert result.portfolio_var95 == 0.0
        assert result.portfolio_var95_pct == 0.0

    def test_zero_nav_returns_zero(self):
        result = self.analyzer.portfolio_var95(
            {"A": {"nav": 0.0, "returns": _simple_returns()}},
            total_nav=0.0,
        )
        assert result.portfolio_var95 == 0.0

    def test_single_position_var_is_nonneg(self):
        rets = _simple_returns(100)
        positions = {"000001.SZ": {"nav": 50000.0, "returns": rets}}
        result = self.analyzer.portfolio_var95(positions, total_nav=100000.0)
        assert result.portfolio_var95 >= 0.0
        assert result.portfolio_var95_pct <= 1.0

    def test_per_position_var_populated(self):
        rets = _simple_returns(100)
        positions = {
            "000001.SZ": {"nav": 50000.0, "returns": rets},
            "600519.SH": {"nav": 30000.0, "returns": rets},
        }
        result = self.analyzer.portfolio_var95(positions, total_nav=100000.0)
        assert "000001.SZ" in result.per_position_var95
        assert "600519.SH" in result.per_position_var95

    def test_n_observations_matches_returns(self):
        rets = _simple_returns(80)
        positions = {"A": {"nav": 80000.0, "returns": rets}}
        result = self.analyzer.portfolio_var95(positions, total_nav=100000.0)
        assert result.n_observations == 80

    def test_portfolio_var_pct_in_range(self):
        rets = _simple_returns(200)
        positions = {
            "A": {"nav": 60000.0, "returns": rets},
            "B": {"nav": 40000.0, "returns": rets},
        }
        result = self.analyzer.portfolio_var95(positions, total_nav=100000.0)
        assert 0.0 <= result.portfolio_var95_pct <= 1.0

    def test_portfolio_method_vs_weighted_sum(self):
        """'portfolio' 模式和 'weighted_sum' 模式均应正常运行且值非负。"""
        rets = _simple_returns(60)
        positions = {
            "A": {"nav": 50000.0, "returns": rets},
            "B": {"nav": 30000.0, "returns": rets},
        }
        r_ws = self.analyzer.portfolio_var95(
            positions, total_nav=80000.0, correlation_method="weighted_sum"
        )
        r_p = self.analyzer.portfolio_var95(
            positions, total_nav=80000.0, correlation_method="portfolio"
        )
        assert r_ws.portfolio_var95 >= 0.0
        assert r_p.portfolio_var95 >= 0.0

    def test_position_without_returns_skipped(self):
        """没有收益率序列的持仓不应导致报错。"""
        positions = {
            "A": {"nav": 50000.0, "returns": _simple_returns(50)},
            "B": {"nav": 30000.0, "returns": []},   # 空序列
        }
        result = self.analyzer.portfolio_var95(positions, total_nav=80000.0)
        assert result.portfolio_var95 >= 0.0
        assert "B" not in result.per_position_var95


# ---------------------------------------------------------------------------
# sector_concentration
# ---------------------------------------------------------------------------


class TestSectorConcentration:
    def setup_method(self):
        self.analyzer = PortfolioRiskAnalyzer()

    def test_basic_concentration(self):
        positions = {"A": 50000.0, "B": 30000.0, "C": 20000.0}
        sector_map = {"A": "银行", "B": "银行", "C": "科技"}
        result = self.analyzer.sector_concentration(positions, sector_map)
        assert abs(result.sector_weights["银行"] - 0.8) < 1e-6
        assert abs(result.sector_weights["科技"] - 0.2) < 1e-6

    def test_hhi_single_sector(self):
        """全部持仓属于同一行业时 HHI = 1。"""
        positions = {"A": 1000.0, "B": 2000.0}
        sector_map = {"A": "银行", "B": "银行"}
        result = self.analyzer.sector_concentration(positions, sector_map)
        assert abs(result.hhi - 1.0) < 1e-6

    def test_top3_concentration_le_total(self):
        positions = {f"S{i}": float(i + 1) * 1000 for i in range(10)}
        sector_map = {f"S{i}": f"行业{i % 4}" for i in range(10)}
        result = self.analyzer.sector_concentration(positions, sector_map)
        assert result.top3_concentration <= 1.0 + 1e-9

    def test_unknown_sector_captured(self):
        positions = {"A": 50000.0, "B": 50000.0}
        sector_map = {"A": "科技"}   # B 未覆盖
        result = self.analyzer.sector_concentration(positions, sector_map)
        assert abs(result.unknown_weight - 0.5) < 1e-6

    def test_empty_positions_returns_default(self):
        result = self.analyzer.sector_concentration({}, {})
        assert result.hhi == 0.0

    def test_max_sector(self):
        positions = {"A": 70000.0, "B": 20000.0, "C": 10000.0}
        sector_map = {"A": "银行", "B": "科技", "C": "消费"}
        result = self.analyzer.sector_concentration(positions, sector_map)
        assert result.max_sector_name == "银行"
        assert abs(result.max_sector_weight - 0.7) < 1e-6


# ---------------------------------------------------------------------------
# aggregate_multi_account
# ---------------------------------------------------------------------------


class TestAggregateMultiAccount:
    def setup_method(self):
        self.analyzer = PortfolioRiskAnalyzer()

    def test_total_nav(self):
        accounts = {
            "acct1": {"nav": 200000.0, "positions": {"A": 100000.0}},
            "acct2": {"nav": 100000.0, "positions": {"B": 80000.0}},
        }
        result = self.analyzer.aggregate_multi_account(accounts)
        assert result.total_nav == 300000.0

    def test_aggregated_positions(self):
        accounts = {
            "acct1": {"nav": 200000.0, "positions": {"A": 100000.0, "B": 50000.0}},
            "acct2": {"nav": 100000.0, "positions": {"A": 60000.0,  "C": 30000.0}},
        }
        result = self.analyzer.aggregate_multi_account(accounts)
        assert abs(result.aggregated_positions["A"] - 160000.0) < 1e-3
        assert abs(result.aggregated_positions["B"] - 50000.0) < 1e-3
        assert abs(result.aggregated_positions["C"] - 30000.0) < 1e-3

    def test_platform_exposure_pct(self):
        accounts = {
            "acct1": {"nav": 100000.0, "positions": {"A": 80000.0}},
        }
        result = self.analyzer.aggregate_multi_account(accounts)
        assert abs(result.platform_net_exposure_pct - 0.8) < 1e-6

    def test_aggregated_hhi_full_concentration(self):
        accounts = {
            "acct1": {"nav": 100000.0, "positions": {"A": 100000.0}},
        }
        result = self.analyzer.aggregate_multi_account(accounts)
        assert abs(result.aggregated_hhi - 1.0) < 1e-6

    def test_empty_accounts(self):
        result = self.analyzer.aggregate_multi_account({})
        assert result.total_nav == 0.0
        assert result.aggregated_positions == {}


# ---------------------------------------------------------------------------
# estimate_beta
# ---------------------------------------------------------------------------


class TestEstimateBeta:
    def test_perfect_correlation_beta_one(self):
        rets = _simple_returns(100)
        beta = PortfolioRiskAnalyzer.estimate_beta(rets, rets)
        assert abs(beta - 1.0) < 1e-9

    def test_double_beta(self):
        """资产收益 = 2 × 市场收益 → Beta ≈ 2。"""
        market = [0.01, -0.02, 0.03, -0.01, 0.02] * 20
        asset = [r * 2 for r in market]
        beta = PortfolioRiskAnalyzer.estimate_beta(asset, market)
        assert abs(beta - 2.0) < 1e-6

    def test_insufficient_data_returns_one(self):
        beta = PortfolioRiskAnalyzer.estimate_beta([0.01, -0.02], [0.01, -0.02])
        assert beta == 1.0  # 样本不足

    def test_zero_market_variance_returns_one(self):
        market = [0.0] * 50
        asset = [0.01] * 50
        beta = PortfolioRiskAnalyzer.estimate_beta(asset, market)
        assert beta == 1.0


# ---------------------------------------------------------------------------
# quick_var — 工厂方法
# ---------------------------------------------------------------------------


class TestQuickVar:
    def test_quick_var_basic(self):
        rets = _simple_returns(100)
        positions = {
            "A": (50000.0, rets),
            "B": (30000.0, rets),
        }
        result = PortfolioRiskAnalyzer.quick_var(positions, total_nav=100000.0)
        assert isinstance(result, PortfolioVaRResult)
        assert result.portfolio_var95 >= 0.0
        assert "A" in result.per_position_var95
        assert "B" in result.per_position_var95
