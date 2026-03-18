"""
tests/test_portfolio_risk.py
单元测试：core.portfolio_risk.PortfolioRiskAnalyzer
"""

import math
import pytest

from core.portfolio_risk import (
    PortfolioRiskAnalyzer,
    PortfolioVaRResult,
    SectorConcentrationResult,
    MultiAccountExposure,
)


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


# ---------------------------------------------------------------------------
# R7 补充：+20 个新测试
# ---------------------------------------------------------------------------


class TestPortfolioVar95Formulas:
    """验证 PortfolioVaRResult pct 字段与绝对值字段一致性。"""

    def test_var95_pct_equals_var_over_nav(self):
        rets = _simple_returns(50, seed_val=0.03)
        pos = {"A": {"nav": 60000.0, "returns": rets}, "B": {"nav": 40000.0, "returns": rets}}
        result = PortfolioRiskAnalyzer().portfolio_var95(pos, total_nav=100000.0)
        assert abs(result.portfolio_var95_pct - result.portfolio_var95 / 100000.0) < 1e-5

    def test_cvar95_pct_equals_cvar_over_nav(self):
        rets = _simple_returns(50, seed_val=0.03)
        pos = {"A": {"nav": 80000.0, "returns": rets}}
        result = PortfolioRiskAnalyzer().portfolio_var95(pos, total_nav=200000.0)
        assert abs(result.portfolio_cvar95_pct - result.portfolio_cvar95 / 200000.0) < 1e-5

    def test_cvar95_ge_var95(self):
        """CVaR95 ≥ VaR95（数学不变式）。"""
        rets = _simple_returns(100, seed_val=0.02)
        pos = {"A": {"nav": 50000.0, "returns": rets}}
        result = PortfolioRiskAnalyzer().portfolio_var95(pos, total_nav=100000.0)
        assert result.portfolio_cvar95 >= result.portfolio_var95

    def test_n_observations_matches_portfolio_returns_length(self):
        rets = _simple_returns(30, seed_val=0.02)
        pos = {"X": {"nav": 100000.0, "returns": rets}}
        result = PortfolioRiskAnalyzer().portfolio_var95(pos, total_nav=100000.0)
        assert result.n_observations == len(result.portfolio_returns) == 30

    def test_empty_positions_returns_zero_result(self):
        result = PortfolioRiskAnalyzer().portfolio_var95({}, total_nav=100000.0)
        assert result.portfolio_var95 == 0.0
        assert result.portfolio_var95_pct == 0.0


class TestPortfolioModeMinLen:
    """portfolio mode 截断到 min_len。"""

    def test_unequal_sequences_truncated(self):
        rets_long = [0.01, -0.02] * 50     # 100 samples
        rets_short = [-0.03, 0.01] * 10    # 20 samples
        pos = {
            "A": {"nav": 50000.0, "returns": rets_long},
            "B": {"nav": 50000.0, "returns": rets_short},
        }
        result = PortfolioRiskAnalyzer().portfolio_var95(
            pos, total_nav=100000.0, correlation_method="portfolio"
        )
        assert result.n_observations == 20  # min_len

    def test_weighted_sum_uses_max_len(self):
        """weighted_sum 使用最长序列，不截断。"""
        rets_long = [0.01] * 80
        rets_short = [-0.01] * 20
        pos = {
            "A": {"nav": 50000.0, "returns": rets_long},
            "B": {"nav": 50000.0, "returns": rets_short},
        }
        result = PortfolioRiskAnalyzer().portfolio_var95(
            pos, total_nav=100000.0, correlation_method="weighted_sum"
        )
        assert result.n_observations == 80  # max_len


class TestSectorConcentrationEdges:
    def test_all_unknown_sectors(self):
        """所有标的不在 sector_map → unknown_weight = 1.0。"""
        positions = {"A": 60000.0, "B": 40000.0}
        result = PortfolioRiskAnalyzer().sector_concentration(positions, {})
        assert result.unknown_weight == pytest.approx(1.0)
        assert result.sector_weights == {}

    def test_negative_positions_excluded(self):
        """负市值持仓被排除，不计入总市值。"""
        positions = {"A": 50000.0, "SHORT": -10000.0}
        sm = {"A": "银行"}
        result = PortfolioRiskAnalyzer().sector_concentration(positions, sm)
        # total should use only positive: 50_000
        assert result.sector_weights["银行"] == pytest.approx(1.0)
        assert result.unknown_weight == pytest.approx(0.0)

    def test_all_negative_positions_returns_default(self):
        """所有市值为负时返回空结果。"""
        positions = {"A": -5000.0, "B": -3000.0}
        result = PortfolioRiskAnalyzer().sector_concentration(positions, {"A": "科技"})
        assert isinstance(result, SectorConcentrationResult)
        assert result.top3_concentration == 0.0

    def test_top_n_equals_one(self):
        """top_n=1 → top3_concentration == max_sector_weight。"""
        positions = {"X": 60000.0, "Y": 40000.0}
        sm = {"X": "银行", "Y": "科技"}
        result = PortfolioRiskAnalyzer().sector_concentration(positions, sm, top_n=1)
        assert result.top3_concentration == pytest.approx(result.max_sector_weight)

    def test_top_n_exceeds_sector_count(self):
        """top_n 大于行业数时仍正常返回（sum 截断到实际数量）。"""
        positions = {"A": 50000.0}
        sm = {"A": "银行"}
        result = PortfolioRiskAnalyzer().sector_concentration(positions, sm, top_n=5)
        assert result.top3_concentration == pytest.approx(1.0)


class TestAggregateMultiAccountEdges:
    def test_negative_positions_excluded_from_gross_long(self):
        """空头持仓不计入 platform_gross_long。"""
        accounts = {
            "acc1": {"nav": 100000.0, "positions": {"LONG": 80000.0, "SHORT": -20000.0}},
        }
        result = PortfolioRiskAnalyzer().aggregate_multi_account(accounts)
        assert result.platform_gross_long == pytest.approx(80000.0)
        assert "SHORT" not in result.aggregated_positions

    def test_zero_nav_account(self):
        """零净值账户贡献 0，不影响 total_nav。"""
        accounts = {
            "real": {"nav": 100000.0, "positions": {}},
            "zero": {"nav": 0.0, "positions": {}},
        }
        result = PortfolioRiskAnalyzer().aggregate_multi_account(accounts)
        assert result.total_nav == pytest.approx(100000.0)

    def test_aggregated_hhi_two_equal_positions(self):
        """2 只等权聚合持仓 → HHI = 0.5。"""
        accounts = {
            "a": {"nav": 100000.0, "positions": {"X": 50000.0, "Y": 50000.0}},
        }
        result = PortfolioRiskAnalyzer().aggregate_multi_account(accounts)
        assert result.aggregated_hhi == pytest.approx(0.5, abs=1e-4)


class TestEstimateBetaEdges:
    def test_four_samples_returns_one(self):
        """n < 5 → 返回中性 Beta = 1.0。"""
        asset = [0.01, -0.02, 0.03, -0.01]
        market = [0.02, -0.01, 0.02, -0.02]
        beta = PortfolioRiskAnalyzer().estimate_beta(asset, market)
        assert beta == 1.0

    def test_five_samples_computes_beta(self):
        """n == 5 → 正常计算（不返回 1.0）。"""
        asset = [0.01, -0.02, 0.03, -0.01, 0.02]
        market = [0.01, -0.02, 0.03, -0.01, 0.02]  # identical → beta ≈ 1.0
        beta = PortfolioRiskAnalyzer().estimate_beta(asset, market)
        assert beta == pytest.approx(1.0)  # identical series → beta = 1

    def test_negative_beta(self):
        """资产与市场完全反向 → beta < 0。"""
        market = [0.01] * 10 + [-0.01] * 10
        asset = [-0.01] * 10 + [0.01] * 10
        beta = PortfolioRiskAnalyzer().estimate_beta(asset, market)
        assert beta < 0

    def test_zero_market_variance_returns_one(self):
        """市场收益率无变化（方差=0）时返回 1.0。"""
        market = [0.0] * 20
        asset = [0.01] * 20
        beta = PortfolioRiskAnalyzer().estimate_beta(asset, market)
        assert beta == 1.0


class TestQuickVarEdges:
    def test_empty_positions_returns_zero_result(self):
        result = PortfolioRiskAnalyzer.quick_var({}, total_nav=100000.0)
        assert isinstance(result, PortfolioVaRResult)
        assert result.portfolio_var95 == 0.0

    def test_single_position_matches_direct_call(self):
        rets = _simple_returns(50, seed_val=0.03)
        quick = PortfolioRiskAnalyzer.quick_var(
            {"A": (80000.0, rets)}, total_nav=100000.0
        )
        direct = PortfolioRiskAnalyzer().portfolio_var95(
            {"A": {"nav": 80000.0, "returns": rets}}, total_nav=100000.0
        )
        assert quick.portfolio_var95 == direct.portfolio_var95
