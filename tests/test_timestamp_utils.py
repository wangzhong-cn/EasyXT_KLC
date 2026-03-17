"""P0 时间戳契约层回归测试
=============================
验收门槛：
  - qmt_ms_to_beijing：UTC epoch ms → 北京时间，偏差精确 +8h
  - dat_s_to_beijing：UTC epoch s → 北京时间，偏差精确 +8h
  - 同一标的同一天：1m 数据不因时区问题归属到前一天
  - 无 tzinfo（naive 断言）
"""
import numpy as np
import pandas as pd
import pytest

from data_manager.timestamp_utils import (
    UTC8_OFFSET_MS,
    UTC8_OFFSET_S,
    assert_no_tz,
    dat_s_to_beijing,
    qmt_ms_to_beijing,
)


class TestConstants:
    """契约常量不可被意外修改"""

    def test_utc8_offset_s(self):
        assert UTC8_OFFSET_S == 28_800

    def test_utc8_offset_ms(self):
        assert UTC8_OFFSET_MS == 28_800_000

    def test_ms_equals_s_times_1000(self):
        assert UTC8_OFFSET_MS == UTC8_OFFSET_S * 1000


class TestQmtMsToBeijing:
    """QMT API ms → 北京时间"""

    def test_0930_beijing(self):
        # 2024-01-02 09:30:00 CST  =  2024-01-02 01:30:00 UTC  =  1704159000 s
        # 验证：pd.to_datetime(1704159000*1000 + 28800000, unit='ms') == '2024-01-02 09:30:00'
        ts_ms = 1704159000 * 1000
        result = qmt_ms_to_beijing(pd.Series([ts_ms]))
        assert result.iloc[0] == pd.Timestamp("2024-01-02 09:30:00")

    def test_1500_close_beijing(self):
        # 2024-01-02 15:00:00 CST  =  2024-01-02 07:00:00 UTC  =  1704178800 s
        # 验证：pd.to_datetime(1704178800*1000 + 28800000, unit='ms') == '2024-01-02 15:00:00'
        ts_ms = 1704178800 * 1000
        result = qmt_ms_to_beijing(pd.Series([ts_ms]))
        assert result.iloc[0] == pd.Timestamp("2024-01-02 15:00:00")

    def test_exactly_8h_ahead_of_raw_utc(self):
        """与直接 unit='ms'（UTC naive）相比，结果恰好多 8 小时"""
        ts_ms = 1704159000 * 1000  # 2024-01-02 09:30 CST
        raw = pd.to_datetime(pd.Series([ts_ms]), unit="ms").iloc[0]
        beijing = qmt_ms_to_beijing(pd.Series([ts_ms])).iloc[0]
        assert (beijing - raw) == pd.Timedelta(hours=8)

    def test_naive_no_tzinfo(self):
        ts_ms = 1704159000 * 1000
        result = qmt_ms_to_beijing(pd.Series([ts_ms]))
        assert result.iloc[0].tzinfo is None

    def test_vectorized_multiple(self):
        ts_values = pd.Series([1704159000 * 1000, 1704178800 * 1000])
        result = qmt_ms_to_beijing(ts_values)
        assert len(result) == 2
        assert result.iloc[0] == pd.Timestamp("2024-01-02 09:30:00")
        assert result.iloc[1] == pd.Timestamp("2024-01-02 15:00:00")

    def test_date_attribution_correct(self):
        """开盘时间 09:30 CST 的日期归属必须是当天而非前一天（UTC naive 会错）"""
        ts_ms = 1704159000 * 1000  # 2024-01-02 09:30 CST
        result = qmt_ms_to_beijing(pd.Series([ts_ms]))
        assert result.iloc[0].date() == pd.Timestamp("2024-01-02").date()


class TestDatSToBeijing:
    """DAT 二进制 s → 北京时间"""

    def test_daily_date_correct(self):
        # 日线 DAT 通常存 00:00 CST = 前一天 16:00 UTC
        # 2024-01-02 00:00 CST = 2024-01-01 16:00 UTC = 1704124800 s
        ts_s = np.array([1704124800], dtype=np.uint32)
        result = dat_s_to_beijing(ts_s)
        assert result[0] == pd.Timestamp("2024-01-02 00:00:00")

    def test_without_offset_gives_wrong_date(self):
        """证明不加偏移量时日期会错一天（v4 验证实证翻版）"""
        ts_s = np.array([1704124800], dtype=np.uint32)  # 2024-01-01 16:00 UTC
        raw = pd.to_datetime(ts_s.astype(np.int64), unit="s")
        beijing = dat_s_to_beijing(ts_s)
        # 不加偏移：日期归属 2024-01-01（错）
        assert raw[0] == pd.Timestamp("2024-01-01 16:00:00")
        # 加偏移：日期归属 2024-01-02（正确）
        assert beijing[0] == pd.Timestamp("2024-01-02 00:00:00")

    def test_exactly_8h_ahead_of_raw_utc(self):
        ts_s = np.array([1704124800], dtype=np.uint32)
        raw = pd.to_datetime(ts_s.astype(np.int64), unit="s")
        beijing = dat_s_to_beijing(ts_s)
        assert (beijing[0] - raw[0]) == pd.Timedelta(hours=8)

    def test_naive_no_tzinfo(self):
        ts_s = np.array([1704124800], dtype=np.uint32)
        result = dat_s_to_beijing(ts_s)
        assert result[0].tzinfo is None

    def test_consistent_with_qmt_ms(self):
        """DAT 日线时间戳与 QMT 1d 时间戳应归属同一天"""
        # DAT: 2024-01-02 00:00 CST epoch s = (2024-01-01 16:00 UTC = 1704124800)
        dat_ts = np.array([1704124800], dtype=np.uint32)
        dat_result = dat_s_to_beijing(dat_ts)[0]
        # QMT 日线: 2024-01-02 09:30 CST epoch ms
        qmt_ts = pd.Series([1704159000 * 1000])
        qmt_result = qmt_ms_to_beijing(qmt_ts).iloc[0]
        # 日期归属一致
        assert dat_result.date() == qmt_result.date()


class TestAssertNoTz:
    def test_naive_passes(self):
        assert_no_tz(pd.Timestamp("2024-01-02 09:30:00"))  # 不应抛异常

    def test_nat_passes(self):
        assert_no_tz(pd.NaT)  # NaT 无 tzinfo，不应抛异常

    def test_aware_raises(self):
        from zoneinfo import ZoneInfo
        aware = pd.Timestamp("2024-01-02 09:30:00", tz=ZoneInfo("Asia/Shanghai"))
        with pytest.raises(ValueError, match="时间戳契约违反"):
            assert_no_tz(aware, label="test_aware")

    def test_utc_aware_raises(self):
        aware = pd.Timestamp("2024-01-02 01:30:00", tz="UTC")
        with pytest.raises(ValueError, match="时间戳契约违反"):
            assert_no_tz(aware, label="utc_aware")
