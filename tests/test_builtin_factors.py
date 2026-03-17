"""
内置因子库单元测试

针对 data_manager/builtin_factors.py 中 15 个纯函数因子进行覆盖测试。

数据来源
--------
所有测试均使用 tests/fixtures/real_market_data.py 中的真实 A 股历史行情。
主 fixture：000001.SZ（平安银行）2023-01-03 ~ 2023-06-02，共 100 个交易日。
数据来源：AKShare stock_zh_a_hist，可独立复核。

边界条件测试说明
----------------
TestRobustness 中部分测试会修改真实数据的某一列（如将 close 统一为第 1 行真实收盘价，
或将 volume 设为 0），以验证算法对极端市场状态（价格不变、成交量归零）的数学鲁棒性。
这类修改保留了真实数据的框架结构，仅变更目标列，目的是测试算法不变量（数学性质），
而非模拟市场行情，符合 development_rules.md 铁律 0 的精神。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# 公共 fixture — 真实 A 股历史行情
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def ohlcv() -> pd.DataFrame:
    """000001.SZ（平安银行）2023H1 真实日线 100 行，含 amount 代理列。

    来源：tests/fixtures/real_market_data.RECORDS_000001_SZ_2023H1
    amount = close × volume(手) × 100，近似成交额（元）。
    """
    from tests.fixtures.real_market_data import get_000001_sz_daily_full
    return get_000001_sz_daily_full()


@pytest.fixture(scope="module")
def ohlcv_short() -> pd.DataFrame:
    """000001.SZ 前 15 个交易日日线，用于测试 min_periods 边界。"""
    from tests.fixtures.real_market_data import get_000001_sz_daily_full
    return get_000001_sz_daily_full().head(15).copy()


# ---------------------------------------------------------------------------
# 辅助工具
# ---------------------------------------------------------------------------

def _check_series(result: pd.Series, df: pd.DataFrame) -> None:
    """断言因子输出的基本属性。"""
    assert isinstance(result, pd.Series), "返回值应为 pd.Series"
    assert len(result) == len(df), "长度应与输入 DataFrame 一致"
    assert result.dropna().shape[0] > 0, "应有至少 1 个有效非 NaN 值"


# ---------------------------------------------------------------------------
# 动量类因子测试
# ---------------------------------------------------------------------------

class TestMomentum:
    def test_momentum_20d_shape(self, ohlcv):
        from data_manager.builtin_factors import _momentum_20d
        _check_series(_momentum_20d(ohlcv), ohlcv)

    def test_momentum_20d_nan_prefix(self, ohlcv):
        from data_manager.builtin_factors import _momentum_20d
        # shift(20) → 前 20 行无基准价，应为 NaN
        assert _momentum_20d(ohlcv).iloc[:20].isna().all()

    def test_momentum_20d_value(self, ohlcv):
        """验证第 20 行（2023-01-30）相对第 0 行（2023-01-03）的真实动量。"""
        from data_manager.builtin_factors import _momentum_20d
        result = _momentum_20d(ohlcv)
        expected = ohlcv["close"].iloc[20] / ohlcv["close"].iloc[0] - 1
        assert abs(result.iloc[20] - expected) < 1e-10

    def test_momentum_5d_shape(self, ohlcv):
        from data_manager.builtin_factors import _momentum_5d
        result = _momentum_5d(ohlcv)
        _check_series(result, ohlcv)
        assert result.iloc[:5].isna().all()

    def test_momentum_5d_value(self, ohlcv):
        """验证第 5 行（2023-01-10，close=14.44）相对第 0 行（13.77）的真实动量。"""
        from data_manager.builtin_factors import _momentum_5d
        result = _momentum_5d(ohlcv)
        expected = ohlcv["close"].iloc[5] / ohlcv["close"].iloc[0] - 1
        assert abs(result.iloc[5] - expected) < 1e-10

    def test_momentum_60d_shape(self, ohlcv):
        from data_manager.builtin_factors import _momentum_60d
        _check_series(_momentum_60d(ohlcv), ohlcv)

    def test_roc_10d_shape(self, ohlcv):
        from data_manager.builtin_factors import _roc_10d
        result = _roc_10d(ohlcv)
        _check_series(result, ohlcv)
        assert result.iloc[:10].isna().all()

    def test_roc_10d_value(self, ohlcv):
        """验证第 10 行（2023-01-17）相对第 0 行（2023-01-03）的真实 10 日 ROC。"""
        from data_manager.builtin_factors import _roc_10d
        result = _roc_10d(ohlcv)
        expected = (ohlcv["close"].iloc[10] - ohlcv["close"].iloc[0]) / ohlcv["close"].iloc[0]
        assert abs(result.iloc[10] - expected) < 1e-10


# ---------------------------------------------------------------------------
# 波动率类因子测试
# ---------------------------------------------------------------------------

class TestVolatility:
    def test_volatility_20d_shape(self, ohlcv):
        from data_manager.builtin_factors import _volatility_20d
        _check_series(_volatility_20d(ohlcv), ohlcv)

    def test_volatility_20d_nonnegative(self, ohlcv):
        from data_manager.builtin_factors import _volatility_20d
        assert (_volatility_20d(ohlcv).dropna() >= 0).all()

    def test_volatility_60d_shape(self, ohlcv):
        from data_manager.builtin_factors import _volatility_60d
        _check_series(_volatility_60d(ohlcv), ohlcv)

    def test_atr_14d_shape(self, ohlcv):
        from data_manager.builtin_factors import _atr_14d
        _check_series(_atr_14d(ohlcv), ohlcv)

    def test_atr_14d_nonnegative(self, ohlcv):
        from data_manager.builtin_factors import _atr_14d
        assert (_atr_14d(ohlcv).dropna() >= 0).all()

    def test_atr_14d_short(self, ohlcv_short):
        """min_periods=7：15 行真实数据应能计算出部分有效值。"""
        from data_manager.builtin_factors import _atr_14d
        assert _atr_14d(ohlcv_short).dropna().shape[0] > 0


# ---------------------------------------------------------------------------
# 成交量类因子测试
# ---------------------------------------------------------------------------

class TestVolume:
    def test_volume_ratio_shape(self, ohlcv):
        from data_manager.builtin_factors import _volume_ratio_20d
        _check_series(_volume_ratio_20d(ohlcv), ohlcv)

    def test_volume_ratio_positive(self, ohlcv):
        from data_manager.builtin_factors import _volume_ratio_20d
        assert (_volume_ratio_20d(ohlcv).dropna() > 0).all()

    def test_volume_ratio_reasonable(self, ohlcv):
        """平安银行 2023H1 量比应在合理范围（0.05 ~ 20 之间）。"""
        from data_manager.builtin_factors import _volume_ratio_20d
        result = _volume_ratio_20d(ohlcv).dropna()
        assert result.max() < 20 and result.min() > 0.05

    def test_turnover_zscore_shape(self, ohlcv):
        from data_manager.builtin_factors import _turnover_20d_zscore
        _check_series(_turnover_20d_zscore(ohlcv), ohlcv)

    def test_turnover_zscore_without_amount(self, ohlcv):
        """无 amount 列时自动降级到 volume 列。"""
        from data_manager.builtin_factors import _turnover_20d_zscore
        df_no_amt = ohlcv.drop(columns=["amount"])
        _check_series(_turnover_20d_zscore(df_no_amt), df_no_amt)

    def test_obv_shape(self, ohlcv):
        from data_manager.builtin_factors import _obv
        _check_series(_obv(ohlcv), ohlcv)

    def test_obv_cumulative(self, ohlcv):
        """OBV 最后一行应与手工累积计算一致。"""
        from data_manager.builtin_factors import _obv
        result = _obv(ohlcv)
        direction = np.sign(ohlcv["close"].diff())
        expected_last = float((direction * ohlcv["volume"]).cumsum().iloc[-1])
        assert abs(result.iloc[-1] - expected_last) < 1e-6


# ---------------------------------------------------------------------------
# 技术指标类因子测试
# ---------------------------------------------------------------------------

class TestTechnical:
    def test_ma_cross_shape(self, ohlcv):
        from data_manager.builtin_factors import _ma_cross_5_20
        _check_series(_ma_cross_5_20(ohlcv), ohlcv)

    def test_ma_cross_dtype(self, ohlcv):
        from data_manager.builtin_factors import _ma_cross_5_20
        assert np.issubdtype(_ma_cross_5_20(ohlcv).dtype, np.floating)

    def test_rsi_range(self, ohlcv):
        """RSI(14) 应在 [0, 100] 之间（边界值在极端行情下合法）。"""
        from data_manager.builtin_factors import _rsi_14
        result = _rsi_14(ohlcv).dropna()
        assert (result >= 0).all() and (result <= 100).all()

    def test_rsi_shape(self, ohlcv):
        from data_manager.builtin_factors import _rsi_14
        _check_series(_rsi_14(ohlcv), ohlcv)

    def test_macd_diff_shape(self, ohlcv):
        from data_manager.builtin_factors import _macd_diff
        _check_series(_macd_diff(ohlcv), ohlcv)

    def test_macd_diff_no_nan_after_warmup(self, ohlcv):
        """EMA 从第 1 行起有值（ewm adjust=False），30 行后不应有 NaN。"""
        from data_manager.builtin_factors import _macd_diff
        assert _macd_diff(ohlcv).iloc[30:].isna().sum() == 0

    def test_bollinger_pct_b_shape(self, ohlcv):
        from data_manager.builtin_factors import _bollinger_pct_b
        _check_series(_bollinger_pct_b(ohlcv), ohlcv)

    def test_bollinger_pct_b_range_typical(self, ohlcv):
        """平安银行正常交易期间 %B 绝大多数在 (-1.0, 2.0) 之间。"""
        from data_manager.builtin_factors import _bollinger_pct_b
        result = _bollinger_pct_b(ohlcv).dropna()
        assert ((result > -1.0) & (result < 2.0)).mean() >= 0.9

    def test_high_low_ratio_shape(self, ohlcv):
        from data_manager.builtin_factors import _high_low_ratio_20d
        _check_series(_high_low_ratio_20d(ohlcv), ohlcv)

    def test_high_low_ratio_positive(self, ohlcv):
        from data_manager.builtin_factors import _high_low_ratio_20d
        assert (_high_low_ratio_20d(ohlcv).dropna() >= 0).all()


# ---------------------------------------------------------------------------
# 注册机制测试
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_register_all_returns_count(self):
        from data_manager.builtin_factors import register_all_builtin_factors
        n = register_all_builtin_factors()
        assert n == 15, f"预期 15 个因子已注册，实际注册了 {n} 个"

    def test_register_idempotent(self):
        """多次调用不应抛异常，注册数量保持 15。"""
        from data_manager.builtin_factors import register_all_builtin_factors
        n1 = register_all_builtin_factors()
        n2 = register_all_builtin_factors()
        assert n1 == n2 == 15

    def test_builtin_factors_list_length(self):
        from data_manager.builtin_factors import _BUILTIN_FACTORS
        assert len(_BUILTIN_FACTORS) == 15

    def test_factor_names_unique(self):
        from data_manager.builtin_factors import _BUILTIN_FACTORS
        names = [entry[0] for entry in _BUILTIN_FACTORS]
        assert len(names) == len(set(names)), "因子名称应唯一"

    def test_each_factor_callable(self):
        from data_manager.builtin_factors import _BUILTIN_FACTORS
        for name, fn, *_ in _BUILTIN_FACTORS:
            assert callable(fn), f"{name} 应可调用"

    def test_custom_registry(self):
        """可传入自定义 registry 对象完成注册。"""
        from unittest.mock import MagicMock
        from data_manager.builtin_factors import register_all_builtin_factors
        mock_registry = MagicMock()
        mock_registry.register_func.return_value = None
        n = register_all_builtin_factors(registry=mock_registry)
        assert n == 15
        assert mock_registry.register_func.call_count == 15

    def test_global_registry_has_rsi_14(self):
        """注册后全局 factor_registry 应包含 rsi_14。"""
        from data_manager.builtin_factors import register_all_builtin_factors
        from data_manager.factor_registry import factor_registry
        register_all_builtin_factors()
        assert "rsi_14" in factor_registry


# ---------------------------------------------------------------------------
# 边界条件 / 鲁棒性测试
# ---------------------------------------------------------------------------

class TestRobustness:
    def test_single_row(self):
        """单行真实数据输入不崩溃（结果可能全 NaN）。"""
        from tests.fixtures.real_market_data import get_000001_sz_daily_full
        from data_manager.builtin_factors import _momentum_20d, _rsi_14, _obv
        df1 = get_000001_sz_daily_full().head(1)
        for fn in [_momentum_20d, _rsi_14, _obv]:
            assert isinstance(fn(df1), pd.Series)

    def test_constant_close_volatility_zero(self):
        """数学不变量：收盘价不变 → 日收益率=0 → 滚动标准差=0（非负）。

        使用真实数据框架，将 close 列统一为第 1 行真实收盘价（2023-01-03，¥13.77），
        以验证算法在极端无波动场景下的数学性质。
        """
        from tests.fixtures.real_market_data import get_000001_sz_daily_full
        from data_manager.builtin_factors import _volatility_20d
        df = get_000001_sz_daily_full().head(40).copy()
        df["close"] = df["close"].iloc[0]  # 真实第一日收盘价 ¥13.77，保持不变
        result = _volatility_20d(df).dropna()
        assert (result >= 0).all()
        assert (result == 0).all()

    def test_zero_volume_volume_ratio(self):
        """边界：成交量为 0（停牌/熔断）时量比应返回 NaN，而非 inf。

        将真实数据的 volume 列清零，模拟 A 股停牌场景。
        """
        from tests.fixtures.real_market_data import get_000001_sz_daily_full
        from data_manager.builtin_factors import _volume_ratio_20d
        df = get_000001_sz_daily_full().head(40).copy()
        df["volume"] = 0.0
        assert _volume_ratio_20d(df).dropna().shape[0] == 0

    def test_obv_flat_price(self):
        """数学不变量：收盘价不变 → direction=0 → OBV 从第 2 行起累积为 0。

        使用真实数据框架，将 close 列统一为第 1 行真实收盘价（¥13.77）。
        """
        from tests.fixtures.real_market_data import get_000001_sz_daily_full
        from data_manager.builtin_factors import _obv
        df = get_000001_sz_daily_full().head(30).copy()
        df["close"] = df["close"].iloc[0]  # 真实第一日收盘价 ¥13.77
        result = _obv(df)
        # 第 0 行 diff=NaN → OBV[0]=NaN；之后 direction=0 → 值恒为 0
        assert result.iloc[1:].eq(0).all()
