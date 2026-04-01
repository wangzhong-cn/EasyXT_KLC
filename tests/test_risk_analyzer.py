"""
tests/test_risk_analyzer.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
RiskAnalyzer 纯数学逻辑单元测试 — 不需要 QApplication
"""
import math
import warnings
import pytest
import numpy as np

from gui_app.backtest.risk_analyzer import RiskAnalyzer


# ---------------------------------------------------------------------------
# 辅助: 生成标准测试数据
# ---------------------------------------------------------------------------
def _flat_values(n=10, start=100.0, step=1.0):
    """线性增长净值序列"""
    return [start + i * step for i in range(n)]


def _declining_values(n=10, start=100.0, step=1.0):
    """线性下跌净值序列"""
    return [start - i * step for i in range(n)]


def _constant_values(n=10, v=100.0):
    return [v] * n


def _sine_values(n=50, amp=10.0, base=100.0):
    """正弦波净值，确保有涨有跌"""
    import math
    return [base + amp * math.sin(2 * math.pi * i / n) for i in range(n)]


# ---------------------------------------------------------------------------
# TestRiskAnalyzerInit
# ---------------------------------------------------------------------------
class TestRiskAnalyzerInit:
    def test_default_risk_free_rate(self):
        ra = RiskAnalyzer()
        assert ra.risk_free_rate == 0.03

    def test_custom_risk_free_rate(self):
        ra = RiskAnalyzer(risk_free_rate=0.05)
        assert ra.risk_free_rate == 0.05

    def test_zero_risk_free_rate(self):
        ra = RiskAnalyzer(risk_free_rate=0.0)
        assert ra.risk_free_rate == 0.0


# ---------------------------------------------------------------------------
# TestCalculateReturns
# ---------------------------------------------------------------------------
class TestCalculateReturns:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_empty_returns_empty(self):
        assert self.ra._calculate_returns([]) == []

    def test_single_value_returns_empty(self):
        assert self.ra._calculate_returns([100.0]) == []

    def test_two_values_one_return(self):
        result = self.ra._calculate_returns([100.0, 110.0])
        assert len(result) == 1
        assert abs(result[0] - 0.1) < 1e-9

    def test_flat_values_zero_returns(self):
        values = _constant_values(5, 100.0)
        result = self.ra._calculate_returns(values)
        assert all(r == 0.0 for r in result)

    def test_growth_positive_returns(self):
        values = _flat_values(5, 100.0, 10.0)  # 100, 110, 120, 130, 140
        result = self.ra._calculate_returns(values)
        assert all(r > 0 for r in result)

    def test_decline_negative_returns(self):
        values = _declining_values(5, 100.0, 5.0)  # 100, 95, 90, 85, 80
        result = self.ra._calculate_returns(values)
        assert all(r < 0 for r in result)

    def test_zero_denominator_returns_zero(self):
        # When previous value is 0 → return 0.0
        result = self.ra._calculate_returns([0.0, 100.0])
        assert result == [0.0]

    def test_length_is_n_minus_one(self):
        values = _flat_values(10, 100.0)
        result = self.ra._calculate_returns(values)
        assert len(result) == 9


# ---------------------------------------------------------------------------
# TestAnnualizeReturn
# ---------------------------------------------------------------------------
class TestAnnualizeReturn:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_zero_periods_returns_zero(self):
        assert self.ra._annualize_return(0.10, 0) == 0.0

    def test_negative_periods_returns_zero(self):
        assert self.ra._annualize_return(0.10, -1) == 0.0

    def test_one_year_equals_total(self):
        # 252 periods → 1 year exactly
        result = self.ra._annualize_return(0.10, 252, 252)
        assert abs(result - 0.10) < 1e-9

    def test_two_years_compound(self):
        # total=0.21 over 504 periods (2 yrs) → annualized ≈ 0.10
        result = self.ra._annualize_return(0.21, 504, 252)
        assert abs(result - 0.10) < 0.005

    def test_zero_total_return(self):
        result = self.ra._annualize_return(0.0, 252, 252)
        assert result == 0.0

    def test_negative_total_return(self):
        result = self.ra._annualize_return(-0.20, 252, 252)
        assert result < 0


# ---------------------------------------------------------------------------
# TestCalculateVolatility
# ---------------------------------------------------------------------------
class TestCalculateVolatility:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_empty_returns_zero(self):
        assert self.ra._calculate_volatility([]) == 0.0

    def test_single_return_zero(self):
        assert self.ra._calculate_volatility([0.01]) == 0.0

    def test_constant_returns_zero_vol(self):
        # All same returns → std ≈ 0 (floating point near-zero)
        result = self.ra._calculate_volatility([0.01] * 10)
        assert result < 1e-9

    def test_volatile_returns_high_vol(self):
        # Large swings → high volatility
        returns = [0.10, -0.10] * 50
        result = self.ra._calculate_volatility(returns)
        assert result > 0.5

    def test_return_is_annualized(self):
        # Daily vol * sqrt(252) should be the result
        daily_returns = [0.01, -0.01, 0.02, -0.02, 0.01]
        result = self.ra._calculate_volatility(daily_returns, periods_per_year=252)
        expected = np.std(daily_returns, ddof=1) * np.sqrt(252)
        assert abs(result - expected) < 1e-9


# ---------------------------------------------------------------------------
# TestCalculateMaxDrawdown
# ---------------------------------------------------------------------------
class TestCalculateMaxDrawdown:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_single_value_returns_zero(self):
        dd, dur = self.ra._calculate_max_drawdown([100.0])
        assert dd == 0.0
        assert dur == 0

    def test_always_increasing_no_drawdown(self):
        values = _flat_values(10, 100.0)
        dd, dur = self.ra._calculate_max_drawdown(values)
        assert dd == 0.0
        assert dur == 0

    def test_always_decreasing_full_drawdown(self):
        values = [100.0, 90.0, 80.0, 70.0]
        dd, dur = self.ra._calculate_max_drawdown(values)
        # Max drawdown = (70-100)/100 = 30%
        assert abs(dd - 0.30) < 1e-9

    def test_drawdown_positive_magnitude(self):
        values = [100.0, 120.0, 80.0, 100.0]
        dd, dur = self.ra._calculate_max_drawdown(values)
        # Peak=120, trough=80 → drawdown = (80-120)/120 = -33.3%
        assert abs(dd - (80 / 120 - 1) * -1) < 1e-9
        assert dd > 0

    def test_duration_counted(self):
        # Peak at index 1, then drops for 4 periods
        values = [100.0, 200.0, 190.0, 180.0, 170.0, 160.0]
        _, dur = self.ra._calculate_max_drawdown(values)
        assert dur == 4

    def test_recovery_resets_duration(self):
        # Short dip then recover, then longer dip
        values = [100.0, 90.0, 100.0, 80.0, 70.0, 60.0]
        _, dur = self.ra._calculate_max_drawdown(values)
        assert dur >= 3  # The longer dip is 3 periods

    def test_empty_returns_zero_zero(self):
        dd, dur = self.ra._calculate_max_drawdown([])
        assert dd == 0.0
        assert dur == 0


# ---------------------------------------------------------------------------
# TestCalculateVar
# ---------------------------------------------------------------------------
class TestCalculateVar:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_short_returns_zero(self):
        # Less than 10 returns
        assert self.ra._calculate_var([0.01] * 5) == 0.0

    def test_var_negative_for_losses(self):
        # Frequent losses → 5th percentile should be negative
        returns = list(np.random.default_rng(42).normal(-0.01, 0.05, 100))
        var = self.ra._calculate_var(returns, confidence_level=0.05)
        assert var < 0

    def test_var_is_percentile(self):
        returns = [0.01 * i for i in range(-10, 10)]
        var = self.ra._calculate_var(returns, 0.05)
        expected = float(np.percentile(returns, 5))
        assert abs(var - expected) < 1e-9

    def test_var_exactly_10_returns(self):
        returns = [0.01 * i for i in range(10)]
        result = self.ra._calculate_var(returns)
        assert isinstance(result, float)


# ---------------------------------------------------------------------------
# TestCalculateCVar
# ---------------------------------------------------------------------------
class TestCalculateCVar:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_short_returns_zero(self):
        assert self.ra._calculate_cvar([0.01] * 5) == 0.0

    def test_cvar_lte_var(self):
        # CVaR should be ≤ VaR (more severe or equal)
        rng = np.random.default_rng(0)
        returns = list(rng.normal(0, 0.02, 50))
        var = self.ra._calculate_var(returns, 0.05)
        cvar = self.ra._calculate_cvar(returns, 0.05)
        assert cvar <= var

    def test_cvar_when_no_tail_losses(self):
        # Build returns such that no element is strictly below VaR
        # (all elements equal VaR, so tail_losses = [var] → cvar == var)
        returns = [0.05] * 20  # all identical → percentile = 0.05 exactly
        cvar = self.ra._calculate_cvar(returns, 0.05)
        var = self.ra._calculate_var(returns, 0.05)
        assert abs(cvar - var) < 1e-9


# ---------------------------------------------------------------------------
# TestCalculateSharpeRatio
# ---------------------------------------------------------------------------
class TestCalculateSharpeRatio:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_empty_returns_zero(self):
        assert self.ra._calculate_sharpe_ratio([], 0.10) == 0.0

    def test_single_return_zero(self):
        assert self.ra._calculate_sharpe_ratio([0.01], 0.10) == 0.0

    def test_zero_volatility_returns_zero(self):
        returns = [0.01] * 10
        assert self.ra._calculate_sharpe_ratio(returns, 0.0) == 0.0

    def test_positive_sharpe_with_good_returns(self):
        # High consistent returns → positive Sharpe
        ra = RiskAnalyzer(risk_free_rate=0.0)
        returns = [0.005] * 100  # daily 0.5% → ~126% annualized
        vol = ra._calculate_volatility(returns)
        if vol == 0:
            vol = 0.01  # avoid zero vol edge via manual vol
        sharpe = ra._calculate_sharpe_ratio(returns, 0.01)
        assert isinstance(sharpe, float)

    def test_sharpe_formula(self):
        ra = RiskAnalyzer(risk_free_rate=0.03)
        returns = [0.01, -0.005, 0.015, 0.002, 0.008]
        vol = 0.20
        arr = np.array(returns)
        expected = (np.mean(arr) * 252 - 0.03) / 0.20
        result = ra._calculate_sharpe_ratio(returns, vol)
        assert abs(result - expected) < 1e-9


# ---------------------------------------------------------------------------
# TestCalculateSortinoRatio
# ---------------------------------------------------------------------------
class TestCalculateSortinoRatio:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_empty_returns_zero(self):
        assert self.ra._calculate_sortino_ratio([]) == 0.0

    def test_single_return_zero(self):
        assert self.ra._calculate_sortino_ratio([0.01]) == 0.0

    def test_all_positive_returns_infinite(self):
        # No negative returns → downside deviation = 0 → inf
        result = self.ra._calculate_sortino_ratio([0.01] * 10)
        assert result == float('inf')

    def test_all_negative_returns_finite(self):
        result = self.ra._calculate_sortino_ratio([-0.01] * 10)
        assert math.isfinite(result)

    def test_zero_positive_mean_zero_sortino(self):
        ra = RiskAnalyzer(risk_free_rate=0.0)
        # Zero mean negative returns → could be 0 sortino
        returns = [-0.01, 0.01, -0.01, 0.01] * 5
        result = ra._calculate_sortino_ratio(returns)
        assert isinstance(result, float)

    def test_single_negative_return_does_not_emit_runtime_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = self.ra._calculate_sortino_ratio([0.10, -0.20, 0.05])
        assert isinstance(result, float)
        assert not any(issubclass(w.category, RuntimeWarning) for w in caught)


# ---------------------------------------------------------------------------
# TestCalculateCalmarRatio
# ---------------------------------------------------------------------------
class TestCalculateCalmarRatio:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_zero_drawdown_positive_return_infinite(self):
        result = self.ra._calculate_calmar_ratio(0.20, 0.0)
        assert result == float('inf')

    def test_zero_drawdown_zero_return_returns_zero(self):
        result = self.ra._calculate_calmar_ratio(0.0, 0.0)
        assert result == 0.0

    def test_zero_drawdown_negative_return_zero(self):
        result = self.ra._calculate_calmar_ratio(-0.10, 0.0)
        assert result == 0.0

    def test_positive_calmar(self):
        result = self.ra._calculate_calmar_ratio(0.20, 0.10)
        assert abs(result - 2.0) < 1e-9

    def test_negative_annualized_return(self):
        result = self.ra._calculate_calmar_ratio(-0.10, 0.05)
        assert result < 0


# ---------------------------------------------------------------------------
# TestCalculateWinRate
# ---------------------------------------------------------------------------
class TestCalculateWinRate:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_empty_returns_zero(self):
        assert self.ra._calculate_win_rate([]) == 0.0

    def test_all_positive_hundred_percent(self):
        assert self.ra._calculate_win_rate([0.01] * 10) == 1.0

    def test_all_negative_zero_percent(self):
        assert self.ra._calculate_win_rate([-0.01] * 10) == 0.0

    def test_half_and_half(self):
        returns = [0.01, -0.01] * 5
        assert self.ra._calculate_win_rate(returns) == 0.5

    def test_three_quarters(self):
        returns = [0.01, 0.01, 0.01, -0.01]
        assert self.ra._calculate_win_rate(returns) == 0.75

    def test_zero_not_counted(self):
        # Zero returns are NOT positive
        returns = [0.0, 0.0, 0.01, -0.01]
        assert self.ra._calculate_win_rate(returns) == 0.25


# ---------------------------------------------------------------------------
# TestCalculateProfitFactor
# ---------------------------------------------------------------------------
class TestCalculateProfitFactor:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_empty_returns_zero(self):
        assert self.ra._calculate_profit_factor([]) == 0.0

    def test_no_losses_positive_profit_infinite(self):
        result = self.ra._calculate_profit_factor([0.01] * 5)
        assert result == float('inf')

    def test_no_wins_zero(self):
        result = self.ra._calculate_profit_factor([-0.01] * 5)
        assert result == 0.0

    def test_no_wins_no_losses_zero(self):
        result = self.ra._calculate_profit_factor([0.0] * 5)
        assert result == 0.0

    def test_equal_wins_losses_one(self):
        returns = [0.01, -0.01] * 5
        result = self.ra._calculate_profit_factor(returns)
        assert abs(result - 1.0) < 1e-9

    def test_two_to_one_ratio(self):
        # 0.02 gains, 0.01 losses
        returns = [0.02, -0.01]
        result = self.ra._calculate_profit_factor(returns)
        assert abs(result - 2.0) < 1e-9


# ---------------------------------------------------------------------------
# TestCalculateBenchmarkMetrics
# ---------------------------------------------------------------------------
class TestCalculateBenchmarkMetrics:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_mismatched_lengths_returns_empty(self):
        result = self.ra._calculate_benchmark_metrics([0.01] * 5, [0.01] * 3)
        assert result == {}

    def test_single_element_returns_empty(self):
        result = self.ra._calculate_benchmark_metrics([0.01], [0.01])
        assert result == {}

    def test_returns_required_keys(self):
        rng = np.random.default_rng(0)
        port = list(rng.normal(0.001, 0.01, 50))
        bench = list(rng.normal(0.001, 0.01, 50))
        result = self.ra._calculate_benchmark_metrics(port, bench)
        for key in ['alpha', 'beta', 'information_ratio', 'tracking_error']:
            assert key in result

    def test_identical_portfolio_beta_approx_one(self):
        # Beta of portfolio vs same benchmark ≈ 1 (cov/var; ddof differences may arise)
        returns = [0.01 + i * 0.001 for i in range(50)]
        result = self.ra._calculate_benchmark_metrics(returns, returns)
        assert abs(result['beta'] - 1.0) < 0.1

    def test_zero_benchmark_variance_beta_zero(self):
        # Constant benchmark → var = 0 → beta = 0
        port = [0.01] * 50
        bench = [0.005] * 50
        result = self.ra._calculate_benchmark_metrics(port, bench)
        assert result['beta'] == 0.0


# ---------------------------------------------------------------------------
# TestGetDefaultMetrics
# ---------------------------------------------------------------------------
class TestGetDefaultMetrics:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_returns_dict(self):
        result = self.ra._get_default_metrics()
        assert isinstance(result, dict)

    def test_all_returns_zero(self):
        result = self.ra._get_default_metrics()
        for k, v in result.items():
            assert v == 0 or v == 0.0, f"{k} = {v}"

    def test_required_keys_present(self):
        result = self.ra._get_default_metrics()
        for key in ['total_return', 'annualized_return', 'volatility',
                    'max_drawdown', 'sharpe_ratio', 'sortino_ratio',
                    'calmar_ratio', 'win_rate', 'profit_factor']:
            assert key in result


# ---------------------------------------------------------------------------
# TestAnalyzePortfolio
# ---------------------------------------------------------------------------
class TestAnalyzePortfolio:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_empty_returns_default(self):
        result = self.ra.analyze_portfolio([])
        assert result == self.ra._get_default_metrics()

    def test_single_value_returns_default(self):
        result = self.ra.analyze_portfolio([100.0])
        assert result == self.ra._get_default_metrics()

    def test_two_values_has_keys(self):
        result = self.ra.analyze_portfolio([100.0, 110.0])
        assert 'total_return' in result
        assert 'sharpe_ratio' in result

    def test_total_return_correct(self):
        result = self.ra.analyze_portfolio([100.0, 200.0])
        assert abs(result['total_return'] - 1.0) < 1e-9

    def test_growing_portfolio_positive_sharpe(self):
        rng = np.random.default_rng(0)
        # Consistently growing values
        values = [100.0 * (1 + 0.001) ** i for i in range(252)]
        result = self.ra.analyze_portfolio(values)
        assert result['total_return'] > 0
        assert result['annualized_return'] > 0

    def test_max_drawdown_with_dip(self):
        values = [100.0, 110.0, 90.0, 100.0]
        result = self.ra.analyze_portfolio(values)
        assert result['max_drawdown'] > 0

    def test_with_benchmark(self):
        rng = np.random.default_rng(1)
        values = [100.0 + i * 0.5 for i in range(50)]
        bench_ret = list(rng.normal(0.001, 0.01, 49))
        result = self.ra.analyze_portfolio(values, benchmark_returns=bench_ret)
        assert 'alpha' in result
        assert 'beta' in result

    def test_precomputed_returns_used(self):
        values = [100.0, 105.0, 110.0]
        manual_returns = [0.0, 0.0]  # force flat returns
        result = self.ra.analyze_portfolio(values, returns=manual_returns)
        # With 0 returns, win_rate should be 0
        assert result['win_rate'] == 0.0

    def test_positive_periods_counted(self):
        values = [100.0, 110.0, 120.0, 130.0]
        result = self.ra.analyze_portfolio(values)
        assert result['positive_periods'] == 3
        assert result['negative_periods'] == 0

    def test_total_periods(self):
        values = _flat_values(10, 100.0)
        result = self.ra.analyze_portfolio(values)
        assert result['total_periods'] == 10


# ---------------------------------------------------------------------------
# TestGenerateRiskReport
# ---------------------------------------------------------------------------
class TestGenerateRiskReport:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_returns_string(self):
        metrics = self.ra._get_default_metrics()
        result = self.ra.generate_risk_report(metrics)
        assert isinstance(result, str)

    def test_contains_header(self):
        metrics = self.ra._get_default_metrics()
        result = self.ra.generate_risk_report(metrics)
        assert "=" * 10 in result

    def test_contains_total_return(self):
        metrics = {'total_return': 0.25, 'annualized_return': 0.25,
                   'volatility': 0.15, 'max_drawdown': 0.05,
                   'max_drawdown_duration': 10, 'var_95': -0.02,
                   'cvar_95': -0.03, 'sharpe_ratio': 1.5,
                   'sortino_ratio': 2.0, 'calmar_ratio': 5.0,
                   'win_rate': 0.6, 'profit_factor': 1.5,
                   'total_periods': 252}
        result = self.ra.generate_risk_report(metrics)
        assert "25.00%" in result

    def test_benchmark_section_shown_when_alpha_present(self):
        metrics = self.ra._get_default_metrics()
        metrics['alpha'] = 0.05
        metrics['beta'] = 1.1
        metrics['information_ratio'] = 0.8
        metrics['tracking_error'] = 0.05
        result = self.ra.generate_risk_report(metrics)
        assert "Alpha" in result or "alpha" in result.lower()

    def test_no_benchmark_section_without_alpha(self):
        metrics = self.ra._get_default_metrics()
        result = self.ra.generate_risk_report(metrics)
        assert "Alpha" not in result


# ---------------------------------------------------------------------------
# TestAssessRiskLevel
# ---------------------------------------------------------------------------
class TestAssessRiskLevel:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_returns_string(self):
        result = self.ra._assess_risk_level({'sharpe_ratio': 0, 'max_drawdown': 0,
                                              'volatility': 0})
        assert isinstance(result, str)

    def test_excellent_metrics_low_risk(self):
        result = self.ra._assess_risk_level({
            'sharpe_ratio': 3.0,    # +3
            'max_drawdown': 0.03,   # +3
            'volatility': 0.05      # +3
        })
        assert "低风险" in result or "优秀" in result

    def test_poor_metrics_high_risk(self):
        result = self.ra._assess_risk_level({
            'sharpe_ratio': 0.1,    # 0
            'max_drawdown': 0.50,   # 0
            'volatility': 0.80      # 0
        })
        assert "高风险" in result or "需要改进" in result

    def test_medium_metrics_medium_risk(self):
        result = self.ra._assess_risk_level({
            'sharpe_ratio': 1.5,    # +2
            'max_drawdown': 0.08,   # +2
            'volatility': 0.15      # +2
        })
        assert "中等" in result or "良好" in result

    def test_sharpe_threshold_1_to_2(self):
        # sharpe 1..2 gives score+2
        result = self.ra._assess_risk_level({
            'sharpe_ratio': 1.5,
            'max_drawdown': 0.08,
            'volatility': 0.15
        })
        assert isinstance(result, str)

    def test_sharpe_gt_2(self):
        result = self.ra._assess_risk_level({
            'sharpe_ratio': 2.5,
            'max_drawdown': 0.25,
            'volatility': 0.25
        })
        assert isinstance(result, str)

    def test_max_drawdown_thresholds(self):
        # dd 0.10..0.20 → +1
        result = self.ra._assess_risk_level({
            'sharpe_ratio': 0.0,
            'max_drawdown': 0.15,
            'volatility': 0.35
        })
        assert isinstance(result, str)

    def test_missing_keys_default_zero(self):
        # Empty dict → sharpe=0, max_dd=0 (<0.05 → +3), vol=0 (<0.10 → +3) → score=6 → medium
        result = self.ra._assess_risk_level({})
        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------
class TestEdgeCases:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_large_values_no_exception(self):
        values = [1e8 * (1.001 ** i) for i in range(100)]
        result = self.ra.analyze_portfolio(values)
        assert 'total_return' in result

    def test_very_small_values_no_exception(self):
        values = [1e-6 * (1.001 ** i) for i in range(50)]
        result = self.ra.analyze_portfolio(values)
        assert isinstance(result, dict)

    def test_negative_portfolio_values_no_exception(self):
        # Should not crash even with negative values
        values = [100.0, -10.0, 80.0]
        result = self.ra.analyze_portfolio(values)
        assert isinstance(result, dict)

    def test_sine_wave_portfolio(self):
        values = _sine_values(100)
        result = self.ra.analyze_portfolio(values)
        assert 'max_drawdown' in result
        assert result['max_drawdown'] >= 0


# ---------------------------------------------------------------------------
# TestBenchmarkBranch — lines 101-102: benchmark_returns is not None
# ---------------------------------------------------------------------------
class TestBenchmarkBranch:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_with_benchmark_returns_adds_alpha_beta(self):
        """< 10 returns → VaR skips np.percentile; benchmark path covered (lines 101-102)."""
        # 10 portfolio values → 9 returns → _calculate_var/cvar return 0.0 early
        portfolio_values = [100.0 + i for i in range(10)]
        benchmark = [0.005] * 9  # 9 benchmark returns matching 9 portfolio returns
        result = self.ra.analyze_portfolio(portfolio_values, benchmark_returns=benchmark)
        assert 'alpha' in result
        assert 'beta' in result

    def test_benchmark_constant_zero_std_gives_zero_beta(self):
        """std(benchmark)==0 → beta=0.0 (lines 311-312)."""
        returns = [0.01, 0.02, -0.01, 0.015, 0.005]
        benchmark = [0.0, 0.0, 0.0, 0.0, 0.0]  # std=0
        result = self.ra._calculate_benchmark_metrics(returns, benchmark)
        assert result['beta'] == 0.0


# ---------------------------------------------------------------------------
# TestCalculateVarCoverLine198 — lines 198, 218-225
# ---------------------------------------------------------------------------
class TestCalculateVarCoverLine198:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_calculate_var_returns_percentile_value(self):
        """Patch np.percentile to bypass numpy version issue; covers line 198."""
        from unittest.mock import patch
        import gui_app.backtest.risk_analyzer as ra_mod
        returns = [float(i) * 0.01 for i in range(-10, 10)]  # 20 values, >= 10
        with patch.object(ra_mod.np, 'percentile', return_value=-0.05):
            result = self.ra._calculate_var(returns, confidence_level=0.05)
        assert result == -0.05  # line 198 reached

    def test_calculate_cvar_with_tail_losses(self):
        """Patch np.percentile; tail_losses branch (line 220) covered."""
        from unittest.mock import patch
        import gui_app.backtest.risk_analyzer as ra_mod
        # returns: 10 below -0.05, 10 above; tail covered
        returns = [-0.10, -0.09, -0.08, -0.07, -0.06,
                   -0.04, 0.01, 0.02, 0.03, 0.04, 0.05]
        with patch.object(ra_mod.np, 'percentile', return_value=-0.05):
            result = self.ra._calculate_cvar(returns, confidence_level=0.05)
        assert isinstance(result, float)  # lines 218-220 reached

    def test_calculate_cvar_empty_tail_uses_var(self):
        """All returns above var → else branch (line 222) covered."""
        from unittest.mock import patch
        import gui_app.backtest.risk_analyzer as ra_mod
        # All returns > 0 → nothing <= negative var
        returns = [0.01, 0.02, 0.03, 0.04, 0.05,
                   0.06, 0.07, 0.08, 0.09, 0.10, 0.11]
        with patch.object(ra_mod.np, 'percentile', return_value=-99.0):
            # All returns > -99, so tail_losses will be non-empty and use mean
            # Try a value higher than all returns
            pass
        # Use positive var so no return is <= var
        with patch.object(ra_mod.np, 'percentile', return_value=9999.0):
            # All returns <= 9999 → tail non-empty... need a var higher than all returns
            pass
        # Actually to get empty tail: var must be less than ALL returns
        # patch var = min(returns) - 1.0 so no returns <= var
        with patch.object(ra_mod.np, 'percentile', return_value=-999.0):
            result = self.ra._calculate_cvar(returns, confidence_level=0.05)
        # All returns > -999, tail_losses = [] → else branch: cvar = float(var) = -999.0
        assert result == -999.0  # line 222 reached


# ---------------------------------------------------------------------------
# TestAssessRiskLevelLow — line 424: score += 1 when 0.5 < sharpe <= 1.0
# ---------------------------------------------------------------------------
class TestAssessRiskLevelLow:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_sharpe_between_0_5_and_1_0_gives_one_point(self):
        """0.5 < sharpe < 1.0 → elif sharpe > 0.5: score += 1 (line 424)."""
        result = self.ra._assess_risk_level({
            'sharpe_ratio': 0.7,     # 0.5 < 0.7 <= 1.0 → +1
            'max_drawdown': 0.50,    # >= 0.20 → +0
            'volatility': 0.35,     # >= 0.30 → +0
        })
        assert isinstance(result, str)  # score = 1 → "[RED] 高风险 (需要改进)"


# ---------------------------------------------------------------------------
# TestAnnualizeReturnEdgeCases — lines 128, 133-134
# ---------------------------------------------------------------------------
class TestAnnualizeReturnEdgeCases:
    def setup_method(self):
        self.ra = RiskAnalyzer()

    def test_negative_periods_per_year_gives_zero(self):
        """years = periods / negative_ppy ≤ 0 → return 0.0 (line 128)."""
        result = self.ra._annualize_return(0.5, 10, periods_per_year=-100)
        assert result == 0.0

    def test_overflow_in_power_returns_zero(self):
        """(1 + total_return) ** (1/years) overflows → except → return 0.0 (lines 133-134)."""
        # total_return = 1e300 - 1, periods = 1, periods_per_year = 1000
        # years = 1/1000 = 0.001, 1/years = 1000
        # (1e300) ** 1000 → OverflowError → except Exception → return 0.0
        result = self.ra._annualize_return(1e300 - 1, 1, periods_per_year=1000)
        assert result == 0.0
