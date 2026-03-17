"""
tests/test_data_contract_validator.py

DataContractValidator 单元测试（L3/L4 硬门禁护栏）

覆盖范围：
  Class A - 空/None 输入快速失败
  Class B - 完好数据全部通过
  Class C - OHLC 关系违规（硬失败）
  Class D - 非正价格（硬失败）
  Class E - 关键列缺失（硬失败）
  Class F - 关键列 NaN 率超限（硬失败）
  Class G - 价格速度违规（硬失败 vs 软告警）
  Class H - 成交量零值软告警
  Class I - 列别名自动识别
  Class J - to_dict 输出结构
  Class K - Stage1 集成：data_contract_passed 传入 Stage1Result
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

# ── 路径注入 ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from data_manager.data_contract_validator import (
    OHLC_SANITY_MIN_PCT,
    POSITIVE_PRICE_MIN_PCT,
    NAN_RATIO_HARD_MAX,
    VELOCITY_HARD_LIMIT,
    VELOCITY_HARD_ROW_PCT,
    VOLUME_ZERO_SOFT_MAX,
    ContractValidationResult,
    ContractViolation,
    DataContractValidator,
)


# ─────────────────────────────────────── 测试工厂 ──────────────────────────────

def _make_clean_df(n: int = 50) -> pd.DataFrame:
    """返回一份完全合法的模拟日线 DataFrame（50 行）。"""
    import numpy as np
    rng = np.random.default_rng(42)
    base = 10.0
    closes = base + rng.normal(0, 0.3, n).cumsum()
    closes = abs(closes) + 5.0  # 确保正值
    opens = closes * rng.uniform(0.99, 1.01, n)
    highs = closes * rng.uniform(1.001, 1.02, n)
    lows  = closes * rng.uniform(0.98, 0.999, n)
    # 确保 OHLC 关系正确
    highs = [max(h, o, c) for h, o, c in zip(highs, opens, closes)]
    lows  = [min(l, o, c) for l, o, c in zip(lows, opens, closes)]
    return pd.DataFrame({
        "open":   opens,
        "high":   highs,
        "low":    lows,
        "close":  closes,
        "volume": rng.integers(1_000, 1_000_000, n).astype(float),
    })


# ──────────────────────────────────────────────────────────────────────────────
# Class A: 空数据快速失败
# ──────────────────────────────────────────────────────────────────────────────

class TestEmptyInput:
    def test_none_returns_fail(self):
        v = DataContractValidator()
        r = v.validate(None, "TEST", "mock")  # type: ignore[arg-type]
        assert r.pass_gate is False
        assert r.rows == 0
        assert any(x.check == "non_empty" for x in r.violations)

    def test_empty_df_returns_fail(self):
        v = DataContractValidator()
        r = v.validate(pd.DataFrame(), "000001.SZ", "mock")
        assert r.pass_gate is False
        assert any(x.severity == "hard" for x in r.violations)


# ──────────────────────────────────────────────────────────────────────────────
# Class B: 完好数据全部通过
# ──────────────────────────────────────────────────────────────────────────────

class TestCleanDataPasses:
    def test_clean_data_pass_gate(self):
        v = DataContractValidator()
        r = v.validate(_make_clean_df(100), "000001.SZ", "mock")
        assert r.pass_gate is True

    def test_clean_data_no_hard_violations(self):
        v = DataContractValidator()
        r = v.validate(_make_clean_df(100), "000001.SZ", "mock")
        hard = [x for x in r.violations if x.severity == "hard"]
        assert len(hard) == 0

    def test_clean_data_ohlc_sanity_100pct(self):
        v = DataContractValidator()
        r = v.validate(_make_clean_df(100), "000001.SZ", "mock")
        assert r.ohlc_sanity_pct == pytest.approx(1.0, abs=0.001)

    def test_clean_data_positive_price_100pct(self):
        v = DataContractValidator()
        r = v.validate(_make_clean_df(100), "000001.SZ", "mock")
        assert r.positive_price_pct == pytest.approx(1.0, abs=0.001)

    def test_clean_data_no_nan(self):
        v = DataContractValidator()
        r = v.validate(_make_clean_df(100), "000001.SZ", "mock")
        for col_nan in r.nan_ratios.values():
            assert col_nan == 0.0

    def test_result_symbol_and_source_preserved(self):
        v = DataContractValidator()
        r = v.validate(_make_clean_df(), "600519.SH", "akshare")
        assert r.symbol == "600519.SH"
        assert r.source == "akshare"


# ──────────────────────────────────────────────────────────────────────────────
# Class C: OHLC 关系违规（硬失败）
# ──────────────────────────────────────────────────────────────────────────────

class TestOHLCSanityFail:
    def _df_with_bad_ohlc(self, n: int = 100, bad_fraction: float = 0.05) -> pd.DataFrame:
        """前 bad_fraction 比例行的 high 改成比 close 还低。"""
        df = _make_clean_df(n).copy()
        bad_n = max(2, int(n * bad_fraction))
        df.loc[:bad_n - 1, "high"] = df.loc[:bad_n - 1, "close"] * 0.95  # high < close ← 违规
        return df

    def test_ohlc_fail_triggers_hard_violation(self):
        v = DataContractValidator()
        r = v.validate(self._df_with_bad_ohlc(100, bad_fraction=0.05), "TEST", "mock")
        assert r.pass_gate is False

    def test_ohlc_fail_check_name(self):
        v = DataContractValidator()
        r = v.validate(self._df_with_bad_ohlc(100, bad_fraction=0.05), "TEST", "mock")
        hard_checks = {v.check for v in r.violations if v.severity == "hard"}
        assert "ohlc_sanity" in hard_checks

    def test_ohlc_sanity_pct_below_threshold(self):
        v = DataContractValidator()
        r = v.validate(self._df_with_bad_ohlc(100, bad_fraction=0.05), "TEST", "mock")
        assert r.ohlc_sanity_pct < OHLC_SANITY_MIN_PCT

    def test_minor_ohlc_error_within_tolerance_passes(self):
        """单行违规 (< 1%) 在 100 行数据集中应通过硬门禁。"""
        df = _make_clean_df(200).copy()
        df.loc[0, "high"] = df.loc[0, "close"] * 0.95   # 仅 1 行，< 1%
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        # 1/200 = 0.5% < 1% → 应通过
        assert r.pass_gate is True


# ──────────────────────────────────────────────────────────────────────────────
# Class D: 非正价格（硬失败）
# ──────────────────────────────────────────────────────────────────────────────

class TestNegativePrice:
    def test_zero_close_triggers_fail(self):
        df = _make_clean_df(50).copy()
        df.loc[0:2, "close"] = 0.0      # 3 行零价格 → 6% > 1% → 硬失败
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.pass_gate is False

    def test_negative_open_triggers_fail(self):
        df = _make_clean_df(50).copy()
        df.loc[0:2, "open"] = -5.0      # 负价格
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.pass_gate is False

    def test_positive_price_pct_in_result(self):
        df = _make_clean_df(100).copy()
        df.loc[0:4, "close"] = -1.0     # 5 行负值 → 5%
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.positive_price_pct < POSITIVE_PRICE_MIN_PCT


# ──────────────────────────────────────────────────────────────────────────────
# Class E: 关键列缺失（硬失败）
# ──────────────────────────────────────────────────────────────────────────────

class TestMissingColumns:
    @pytest.mark.parametrize("drop_col", ["open", "high", "low", "close"])
    def test_missing_critical_column_fails(self, drop_col: str):
        df = _make_clean_df(50).drop(columns=[drop_col])
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.pass_gate is False

    @pytest.mark.parametrize("drop_col", ["open", "high", "low", "close"])
    def test_missing_column_in_violations(self, drop_col: str):
        df = _make_clean_df(50).drop(columns=[drop_col])
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        checks = {v.check for v in r.violations}
        assert f"column_exists_{drop_col}" in checks


# ──────────────────────────────────────────────────────────────────────────────
# Class F: 关键列 NaN 率超限（硬失败）
# ──────────────────────────────────────────────────────────────────────────────

class TestNanRateHardFail:
    def test_high_nan_rate_close_fails(self):
        df = _make_clean_df(50).copy()
        df.loc[0:2, "close"] = float("nan")  # 3/50 = 6% > 1%
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.pass_gate is False
        assert r.nan_ratios.get("close", 0) > NAN_RATIO_HARD_MAX

    def test_single_nan_within_tolerance(self):
        """1 行 NaN / 200 行 = 0.5% < 1% → 仍应通过。"""
        df = _make_clean_df(200).copy()
        df.loc[0, "close"] = float("nan")
        # 需要修复 OHLC 一行，因为 NaN close 导致 OHLC 检查时该行被 dropna 自动排除
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        # nan_ratios["close"] = 1/200 = 0.5% < 1% → no hard violation from NaN
        assert r.nan_ratios.get("close", 0) <= NAN_RATIO_HARD_MAX


# ──────────────────────────────────────────────────────────────────────────────
# Class G: 价格速度违规
# ──────────────────────────────────────────────────────────────────────────────

class TestPriceVelocity:
    def test_many_velocity_violations_hard_fail(self):
        """把 5% 的行改成日内涨幅 25% → 超过 1% 行占比 → 硬失败。"""
        df = _make_clean_df(100).copy()
        bad_rows = 5  # 5/100 = 5% > 1%
        for i in range(1, bad_rows + 1):
            df.loc[i, "close"] = df.loc[i - 1, "close"] * 1.30   # +30%
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.pass_gate is False
        checks = {x.check for x in r.violations if x.severity == "hard"}
        assert "price_velocity" in checks

    def test_single_velocity_violation_soft_warn(self):
        """单行速度违规（< 1%）仅产生软告警，不失败。"""
        df = _make_clean_df(200).copy()
        df.loc[10, "close"] = df.loc[9, "close"] * 1.5   # 单行 +50%
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        # 1/200 = 0.5% < 1% → 软告警，不是硬失败
        assert r.pass_gate is True
        soft_checks = {x.check for x in r.violations if x.severity == "soft"}
        assert "price_velocity_info" in soft_checks

    def test_velocity_violation_count_accurate(self):
        # row 10: +50% (violation), row 11: ~-33% rebound (also violation)
        # pct_change captures both the spike and the recovery, so >=1 is correct
        df = _make_clean_df(200).copy()
        df.loc[10, "close"] = df.loc[9, "close"] * 1.5
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.velocity_violation_count >= 1


# ──────────────────────────────────────────────────────────────────────────────
# Class H: 成交量零值软告警
# ──────────────────────────────────────────────────────────────────────────────

class TestVolumeZeroSoft:
    def test_high_zero_volume_soft_only(self):
        """10% 零成交量 → 软告警，不应触发硬失败。"""
        df = _make_clean_df(100).copy()
        df.loc[0:9, "volume"] = 0.0   # 10 行 = 10%
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.pass_gate is True   # 不阻断
        soft_checks = {x.check for x in r.violations if x.severity == "soft"}
        assert "volume_zero" in soft_checks

    def test_low_zero_volume_no_warn(self):
        """< 5% 零成交量 → 无告警。"""
        df = _make_clean_df(100).copy()
        df.loc[0:3, "volume"] = 0.0   # 4 行 = 4%
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        vol_checks = {x.check for x in r.violations}
        assert "volume_zero" not in vol_checks


# ──────────────────────────────────────────────────────────────────────────────
# Class I: 列别名自动识别
# ──────────────────────────────────────────────────────────────────────────────

class TestColumnAliases:
    def test_open_price_alias(self):
        df = _make_clean_df(50).rename(columns={"open": "open_price"})
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.pass_gate is True

    def test_close_price_alias(self):
        df = _make_clean_df(50).rename(columns={"close": "close_price"})
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.pass_gate is True

    def test_uppercase_columns(self):
        df = _make_clean_df(50).rename(columns={
            "open": "Open", "high": "High", "low": "Low", "close": "Close"
        })
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        assert r.pass_gate is True


# ──────────────────────────────────────────────────────────────────────────────
# Class J: to_dict 输出结构
# ──────────────────────────────────────────────────────────────────────────────

class TestToDictOutput:
    def test_to_dict_required_keys(self):
        v = DataContractValidator()
        r = v.validate(_make_clean_df(), "TEST", "mock")
        d = r.to_dict()
        required = {
            "symbol", "source", "rows",
            "ohlc_sanity_pct", "positive_price_pct", "nan_ratios",
            "velocity_violation_count", "velocity_violation_pct",
            "volume_zero_pct", "pass_gate", "violations",
        }
        assert required.issubset(set(d.keys()))

    def test_to_dict_pass_gate_true(self):
        v = DataContractValidator()
        r = v.validate(_make_clean_df(), "TEST", "mock")
        d = r.to_dict()
        assert d["pass_gate"] is True

    def test_to_dict_violations_list(self):
        v = DataContractValidator()
        r = v.validate(_make_clean_df(), "TEST", "mock")
        d = r.to_dict()
        assert isinstance(d["violations"], list)

    def test_to_dict_fail_has_violations(self):
        df = _make_clean_df(50).drop(columns=["close"])
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock")
        d = r.to_dict()
        assert d["pass_gate"] is False
        assert len(d["violations"]) > 0
        assert all("check" in x and "severity" in x for x in d["violations"])


# ──────────────────────────────────────────────────────────────────────────────
# Class K: Stage1 集成（data_contract_passed 字段）
# ──────────────────────────────────────────────────────────────────────────────

class TestStage1ContractIntegration:
    def test_stage1result_default_contract_passed(self):
        """Stage1Result 默认 data_contract_passed=True（不影响现有测试）。"""
        from dataclasses import fields
        from strategies.stage1_pipeline import Stage1Result
        field_names = {f.name for f in fields(Stage1Result)}
        assert "data_contract_passed" in field_names

    def test_stage1result_to_dict_includes_field(self):
        """to_dict() 输出中必须包含 data_contract_passed 键。"""
        from strategies.stage1_pipeline import (
            Stage1Result, DataAcceptanceResult,
            BacktestMetrics, InOutSampleResult, ParamSensitivityResult,
        )
        dummy_accept = DataAcceptanceResult(
            symbol="TEST", period="1d", date_range="2020~2021",
            expected_trading_days=240, actual_data_days=240,
            coverage_pct=100.0, max_gap_days=0,
            pass_board=True, failures=[],
        )
        dummy_bt = BacktestMetrics(
            total_return_pct=10.0, annualized_return_pct=5.0, sharpe_ratio=0.5,
            max_drawdown_pct=10.0, win_rate_pct=55.0, trade_count=10,
            years=2.0, period="full",
            calmar_ratio=0.5, cost_ratio_pct=0.5, market_assumption="A股",
        )
        dummy_io = InOutSampleResult(
            in_sample_period="2020~2021", out_sample_period="2021~2022",
            in_sharpe=0.5, out_sharpe=0.4, oos_ratio=0.8, pass_threshold=True,
        )
        dummy_sens = ParamSensitivityResult(
            base_params={"short_period": 5, "long_period": 20},
            sensitivity_table=[], max_change_pct=10.0, pass_threshold=True,
        )
        result = Stage1Result(
            strategy="test_strat", symbol="TEST", run_date="2026-01-01",
            start="2020-01-01", end="2022-01-01", oos_split="2021-01-01",
            stage1_pass=True, data_acceptance=dummy_accept,
            full_backtest=dummy_bt, in_sample=dummy_bt, out_of_sample=dummy_bt,
            in_out_comparison=dummy_io, param_sensitivity=dummy_sens,
            data_contract_passed=False,   # 测试 False 值
        )
        d = result.to_dict()
        assert "data_contract_passed" in d
        assert d["data_contract_passed"] is False


# ──────────────────────────────────────────────────────────────────────────────
# Class L: 非交易日行检查（P0 门禁补强）
# ──────────────────────────────────────────────────────────────────────────────

def _make_df_with_dates(dates: list) -> pd.DataFrame:
    """OHLCV 合法且含 datetime 列的 DataFrame 工厂函数。"""
    import numpy as np

    n = len(dates)
    rng = np.random.default_rng(7)
    closes = 10.0 + rng.random(n)
    return pd.DataFrame({
        "datetime": pd.to_datetime(dates),
        "open":   closes * 0.99,
        "high":   closes * 1.01,
        "low":    closes * 0.98,
        "close":  closes,
        "volume": np.ones(n) * 1000.0,
    })


class TestNonTradingDayGate:
    """非交易日数据入库拒绝门禁（§5 P0 补强）。

    使用 2024-08 的已知工作日/周末做断言基准，返回结果不依赖远程日历 API：
    A 股铁律——周六/周日 is_trading_day() 永远返回 False。
    """

    # 2024-08-19 Mon ~ 2024-08-23 Fri：非假日工作日
    _WEEKDAYS = ["2024-08-19", "2024-08-20", "2024-08-21", "2024-08-22", "2024-08-23"]
    # 2024-08-24 Sat, 2024-08-25 Sun：周末，时刻词
    _WEEKENDS = ["2024-08-24", "2024-08-25"]

    def test_weekday_only_data_passes_daily(self):
        """全工作日日线数据，§5 不应产生 non_trading_day 违规。"""
        df = _make_df_with_dates(self._WEEKDAYS)
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock", period="1d")
        assert r.pass_gate is True
        assert not any(x.check == "non_trading_day" for x in r.violations)

    def test_weekend_rows_rejected_with_hard_violation(self):
        """含周末行的日线 DataFrame 必须触发 non_trading_day 硬门禁。"""
        df = _make_df_with_dates(self._WEEKDAYS + self._WEEKENDS)
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock", period="1d")
        assert r.pass_gate is False
        ntd = [x for x in r.violations if x.check == "non_trading_day"]
        assert len(ntd) == 1
        assert ntd[0].severity == "hard"
        assert ntd[0].value == 2.0  # 两行周末

    def test_no_datetime_column_skips_check(self):
        """无 datetime/date 列时，§5 静默跳过，不产生误报。"""
        df = _make_clean_df(10)  # _make_clean_df 无日期列
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock", period="1d")
        assert not any(x.check == "non_trading_day" for x in r.violations)

    def test_intraday_periods_skip_check(self):
        """分钟/小时周期不做非交易日检查（允许跨分钟缓冲）。"""
        df = _make_df_with_dates(self._WEEKENDS)  # 只含周末日期
        v = DataContractValidator()
        for intraday in ("1m", "5m", "15m", "30m", "60m"):
            r = v.validate(df, "TEST", "mock", period=intraday)
            assert not any(x.check == "non_trading_day" for x in r.violations), (
                f"period={intraday} 不应触发非交易日检查"
            )

    def test_weekly_period_rejects_weekend(self):
        """周线数据中的周末行也应被拒绝（1w 同属检查范围）。"""
        df = _make_df_with_dates(self._WEEKDAYS[:2] + self._WEEKENDS)
        v = DataContractValidator()
        r = v.validate(df, "TEST", "mock", period="1w")
        assert r.pass_gate is False
        assert any(x.check == "non_trading_day" for x in r.violations)
