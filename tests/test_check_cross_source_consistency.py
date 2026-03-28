"""跨源一致性抽检 compare_pair 单元测试。

数据来源：tests/fixtures/real_market_data.py（真实 A 股历史，符合铁律 0）。
"""

import pandas as pd

from tests.fixtures.real_market_data import (
    RECORDS_000001_SZ_2023Q1,
    RECORDS_600000_SH_2023Q1,
)
from tools.check_cross_source_consistency import CompareConfig, compare_pair


def _make_daily_df(records: list[tuple], indices: list[int] | None = None) -> pd.DataFrame:
    """从真实 fixture 记录中选取指定行，构造 datetime/close/volume DataFrame。"""
    rows = [records[i] for i in indices] if indices is not None else list(records)
    full = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    return full.rename(columns={"date": "datetime"})[["datetime", "close", "volume"]]


def test_compare_pair_no_common_date_returns_none():
    # 000001.SZ 第 0 行 (2023-01-03) vs 第 1 行 (2023-01-04)：日期不同 → None
    left = _make_daily_df(RECORDS_000001_SZ_2023Q1, [0])
    right = _make_daily_df(RECORDS_000001_SZ_2023Q1, [1])
    cfg = CompareConfig(close_diff_threshold_pct=0.5, volume_diff_threshold_pct=5.0)
    assert compare_pair(left, right, cfg) is None


def test_compare_pair_alerts_when_close_diff_exceeds_threshold():
    # 同日 2023-01-03：000001.SZ close=13.84 vs 600000.SH close=7.22
    # 相对偏差 ≈ 47.8%，远超 0.5% 阈值 → 触发 close_alert
    left = _make_daily_df(RECORDS_000001_SZ_2023Q1, [0])
    right = _make_daily_df(RECORDS_600000_SH_2023Q1, [0])
    cfg = CompareConfig(close_diff_threshold_pct=0.5, volume_diff_threshold_pct=5.0)
    result = compare_pair(left, right, cfg)
    assert result is not None
    assert result["close_alert"] is True
    assert result["alert"] is True


def test_compare_pair_alerts_when_volume_diff_exceeds_threshold():
    # 同日 2023-01-03：000001.SZ volume=213447200 vs 600000.SH volume=50234500
    # 相对偏差 ≈ 76.5%，远超 5% 阈值 → 触发 volume_alert
    left = _make_daily_df(RECORDS_000001_SZ_2023Q1, [0])
    right = _make_daily_df(RECORDS_600000_SH_2023Q1, [0])
    cfg = CompareConfig(close_diff_threshold_pct=100.0, volume_diff_threshold_pct=5.0)
    result = compare_pair(left, right, cfg)
    assert result is not None
    assert result["volume_alert"] is True
    assert result["alert"] is True


def test_compare_pair_no_alert_when_within_threshold():
    # 同一来源、同一记录对比自身 → 偏差 = 0% → 不触发告警
    left = _make_daily_df(RECORDS_000001_SZ_2023Q1, [0])
    right = _make_daily_df(RECORDS_000001_SZ_2023Q1, [0])
    cfg = CompareConfig(close_diff_threshold_pct=0.5, volume_diff_threshold_pct=5.0)
    result = compare_pair(left, right, cfg)
    assert result is not None
    assert result["alert"] is False
