"""
tests/test_dat_binary_reader.py
================================
DAT 二进制直读器 + 相关修复的单元测试

覆盖范围：
  Class A: DATBinaryReader（无 DAT 文件时的优雅降级）
  Class B: _build_dat_path（路径构造逻辑）
  Class C: _symbol_to_market_code（代码解析）
  Class D: _read_dat_numpy（二进制解析正确性）
  Class E: read_dat 日期过滤
  Class F: DataSourceRegistry 质量门禁
  Class G: _check_missing_trading_days 修复验证（bdate_range 逻辑）
  Class H: DATBinarySource 健康状态
"""

import struct
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import pytest

# ─── 被测模块 ─────────────────────────────────────────────────────────────────
from data_manager.dat_binary_reader import (
    DATBinaryReader,
    _build_dat_path,
    _symbol_to_market_code,
    _read_dat_numpy,
    read_dat,
    HEADER_SIZE,
    RECORD_SIZE,
    UTC8_OFFSET,
)
from data_manager.datasource_registry import DataSourceRegistry, DATBinarySource


# ─── 工具函数：构造合法 DAT 文件 ──────────────────────────────────────────────

def _make_dat_file(
    dir_path: Path,
    n_records: int = 5,
    base_epoch: int = 1700000000,
    open_v: int = 13400,    # 000001.SZ 2023-01-03 开盘 13.40 元 × 1000（QMT 格式整数）
    close_v: int = 13840,   # 000001.SZ 2023-01-03 收盘 13.84 元 × 1000
    volume: int = 213447200,  # 000001.SZ 2023-01-03 真实成交量（股数）
) -> Path:
    """在 dir_path 写一个 DAT 文件，返回路径。"""
    dat_path = dir_path / "test.DAT"
    with dat_path.open("wb") as f:
        f.write(b"\x00" * HEADER_SIZE)   # 8 字节文件头
        for i in range(n_records):
            ts = base_epoch + i * 86400   # 每条 +1 天
            rec = struct.pack(
                "<IIIIIII36s",
                ts,
                open_v,
                open_v + 100,   # high
                open_v - 100,   # low
                close_v,
                0,              # pad
                volume,
                b"\x00" * 36,  # rest（metadata）
            )
            f.write(rec)
    return dat_path


def _make_dat_file_with_zero_volume(dir_path: Path) -> Path:
    """构造含零成交量记录的 DAT 文件（这些记录应被过滤）。"""
    dat_path = dir_path / "zero_vol.DAT"
    with dat_path.open("wb") as f:
        f.write(b"\x00" * HEADER_SIZE)
        # 有效记录
        ts_valid = 1700000000
        rec_valid = struct.pack("<IIIIIII36s", ts_valid, 10000, 10100, 9900, 10000, 0, 500, b"\x00" * 36)
        # 零成交量（应被过滤）
        rec_zero  = struct.pack("<IIIIIII36s", ts_valid + 86400, 10100, 10200, 10000, 10100, 0, 0, b"\x00" * 36)
        f.write(rec_valid)
        f.write(rec_zero)
    return dat_path


# ══════════════════════════════════════════════════════════════════════════════
# Class A: DATBinaryReader 无文件可用时的优雅降级
# ══════════════════════════════════════════════════════════════════════════════
class TestDATBinaryReaderNoFile:

    def test_no_qmt_base_returns_empty(self):
        """qmt_base=None（无 QMT 安装）时 get_data 返回空 DataFrame"""
        reader = DATBinaryReader(qmt_base=Path("/nonexistent/path/that/cannot/exist"))
        df = reader.get_data("600519.SH", "2023-01-01", "2023-12-31", "1d", "none")
        assert df.empty

    def test_is_available_false_when_no_base(self):
        reader = DATBinaryReader(qmt_base=Path("/nonexistent"))
        assert reader.is_available() is False

    def test_health_reports_unavailable(self):
        reader = DATBinaryReader(qmt_base=Path("/nonexistent"))
        h = reader.health()
        assert h["available"] is False

    def test_missing_dat_file_returns_empty(self, tmp_path):
        """DAT 文件路径不存在时返回空 DataFrame"""
        reader = DATBinaryReader(qmt_base=tmp_path)
        df = reader.get_data("600519.SH", "2023-01-01", "2023-12-31", "1d", "none")
        assert df.empty


# ══════════════════════════════════════════════════════════════════════════════
# Class B: _build_dat_path 路径构造
# ══════════════════════════════════════════════════════════════════════════════
class TestBuildDatPath:

    def test_returns_none_for_unsupported_period(self, tmp_path):
        path = _build_dat_path(tmp_path, "600519.SH", "15m")
        assert path is None

    def test_returns_none_when_file_not_exist(self, tmp_path):
        path = _build_dat_path(tmp_path, "600519.SH", "1d")
        assert path is None

    def test_returns_path_when_file_exists(self, tmp_path):
        """构造预期路径：datadir/SH/86400/600519.DAT"""
        target = tmp_path / "SH" / "86400"
        target.mkdir(parents=True)
        (target / "600519.DAT").write_bytes(b"\x00" * 8)  # 空文件头
        path = _build_dat_path(tmp_path, "600519.SH", "1d")
        assert path is not None
        assert path.name == "600519.DAT"

    def test_futures_path_uses_sf_market(self, tmp_path):
        target = tmp_path / "SF" / "86400"
        target.mkdir(parents=True)
        (target / "rb2510.DAT").write_bytes(b"\x00" * 8)
        path = _build_dat_path(tmp_path, "rb2510.SF", "1d")
        assert path is not None
        assert path.name == "rb2510.DAT"

    def test_1m_uses_period_60(self, tmp_path):
        target = tmp_path / "SH" / "60"
        target.mkdir(parents=True)
        (target / "600519.DAT").write_bytes(b"\x00" * 8)
        path = _build_dat_path(tmp_path, "600519.SH", "1m")
        assert path is not None


# ══════════════════════════════════════════════════════════════════════════════
# Class C: _symbol_to_market_code 解析
# ══════════════════════════════════════════════════════════════════════════════
class TestSymbolToMarketCode:

    def test_sh_stock(self):
        market, code = _symbol_to_market_code("600519.SH")
        assert market == "SH" and code == "600519"

    def test_sz_stock(self):
        market, code = _symbol_to_market_code("000001.SZ")
        assert market == "SZ" and code == "000001"

    def test_futures_sf(self):
        market, code = _symbol_to_market_code("rb2510.SF")
        assert market == "SF" and code == "rb2510"

    def test_no_suffix_starts_6_defaults_sh(self):
        market, code = _symbol_to_market_code("600519")
        assert market == "SH"

    def test_no_suffix_starts_0_defaults_sz(self):
        market, code = _symbol_to_market_code("000001")
        assert market == "SZ"

    def test_suffix_is_uppercased(self):
        market, _ = _symbol_to_market_code("600519.sh")
        assert market == "SH"


# ══════════════════════════════════════════════════════════════════════════════
# Class D: _read_dat_numpy 二进制解析正确性
# ══════════════════════════════════════════════════════════════════════════════
class TestReadDatNumpy:

    def test_empty_file_returns_empty_df(self, tmp_path):
        p = tmp_path / "empty.DAT"
        p.write_bytes(b"\x00" * HEADER_SIZE)
        df = _read_dat_numpy(p)
        assert df.empty

    def test_nonexistent_file_returns_empty_df(self, tmp_path):
        p = tmp_path / "phantom.DAT"
        df = _read_dat_numpy(p)
        assert df.empty

    def test_record_count_matches(self, tmp_path):
        _make_dat_file(tmp_path, n_records=5)
        df = _read_dat_numpy(tmp_path / "test.DAT")
        assert len(df) == 5

    def test_price_division_by_1000(self, tmp_path):
        """`open * 1000` 写入 → 读出应除以 1000"""
        _make_dat_file(tmp_path, open_v=10500, close_v=10600)
        df = _read_dat_numpy(tmp_path / "test.DAT")
        assert abs(df["open"].iloc[0] - 10.5) < 1e-6
        assert abs(df["close"].iloc[0] - 10.6) < 1e-6

    def test_volume_is_integer(self, tmp_path):
        _make_dat_file(tmp_path, volume=3000)
        df = _read_dat_numpy(tmp_path / "test.DAT")
        assert df["volume"].dtype == np.int64
        assert df["volume"].iloc[0] == 3000

    def test_zero_volume_records_filtered(self, tmp_path):
        """零成交量记录不得出现在输出中"""
        _make_dat_file_with_zero_volume(tmp_path)
        df = _read_dat_numpy(tmp_path / "zero_vol.DAT")
        assert len(df) == 1   # 只有 1 条有效记录

    def test_utc8_offset_applied(self, tmp_path):
        """时间戳应加 UTC8_OFFSET（+28800s），确保日期归属为北京时间"""
        # base_epoch = 2023-11-14 16:00:00 UTC = 2023-11-15 00:00:00 北京
        base_epoch = 1700006400  # 2023-11-15 00:00:00 UTC+8
        _make_dat_file(tmp_path, n_records=1, base_epoch=base_epoch - UTC8_OFFSET)
        df = _read_dat_numpy(tmp_path / "test.DAT")
        # date 应落在 2023-11-15
        assert df.index[0].date().isoformat() == "2023-11-15"

    def test_index_name_is_date(self, tmp_path):
        _make_dat_file(tmp_path)
        df = _read_dat_numpy(tmp_path / "test.DAT")
        assert df.index.name == "date"

    def test_columns_present(self, tmp_path):
        _make_dat_file(tmp_path)
        df = _read_dat_numpy(tmp_path / "test.DAT")
        assert set(df.columns) >= {"open", "high", "low", "close", "volume"}

    def test_ohlc_relationship(self, tmp_path):
        """high ≥ max(open, close), low ≤ min(open, close)"""
        _make_dat_file(tmp_path, open_v=10000, close_v=10100)
        df = _read_dat_numpy(tmp_path / "test.DAT")
        row = df.iloc[0]
        assert row["high"] >= max(row["open"], row["close"])
        assert row["low"]  <= min(row["open"], row["close"])


# ══════════════════════════════════════════════════════════════════════════════
# Class E: read_dat 日期过滤
# ══════════════════════════════════════════════════════════════════════════════
class TestReadDatDateFilter:

    def _setup_dat(self, tmp_path: Path, market: str = "SH") -> Path:
        """在 tmp_path/SH/86400/600519.DAT 写 10 条记录"""
        target = tmp_path / market / "86400"
        target.mkdir(parents=True)
        # base_epoch = 2023-01-01 北京 → 2022-12-31 16:00:00 UTC
        base_epoch_utc = 1672502400 - UTC8_OFFSET  # 2023-01-01 00:00 CST
        _make_dat_file(target, n_records=10, base_epoch=base_epoch_utc)
        # rename test.DAT → 600519.DAT
        (target / "test.DAT").rename(target / "600519.DAT")
        return tmp_path

    def test_no_filter_returns_all(self, tmp_path):
        base = self._setup_dat(tmp_path)
        df = read_dat("600519.SH", "1d", qmt_base=base)
        assert len(df) == 10

    def test_start_date_filters_early_rows(self, tmp_path):
        base = self._setup_dat(tmp_path)
        df = read_dat("600519.SH", "1d", start_date="2023-01-05", qmt_base=base)
        assert all(df.index >= pd.Timestamp("2023-01-05"))

    def test_end_date_filters_late_rows(self, tmp_path):
        base = self._setup_dat(tmp_path)
        df = read_dat("600519.SH", "1d", end_date="2023-01-05", qmt_base=base)
        assert all(df.index <= pd.Timestamp("2023-01-05"))

    def test_narrow_range_returns_subset(self, tmp_path):
        base = self._setup_dat(tmp_path)
        df = read_dat("600519.SH", "1d", start_date="2023-01-03", end_date="2023-01-06", qmt_base=base)
        assert len(df) <= 4

    def test_out_of_range_returns_empty(self, tmp_path):
        base = self._setup_dat(tmp_path)
        df = read_dat("600519.SH", "1d", start_date="2030-01-01", qmt_base=base)
        assert df.empty


# ══════════════════════════════════════════════════════════════════════════════
# Class F: DataSourceRegistry 质量门禁
# ══════════════════════════════════════════════════════════════════════════════
class TestRegistryQualityGate:

    def _good_df(self) -> pd.DataFrame:
        """使用 000001.SZ 2023 年 1 月真实日线数据（铁律 0：禁止伪造 OHLCV）。"""
        from tests.fixtures.real_market_data import RECORDS_000001_SZ_2023Q1
        records = RECORDS_000001_SZ_2023Q1[:5]
        dates = pd.DatetimeIndex([r[0] for r in records])
        return pd.DataFrame(
            {
                "open":   [r[1] for r in records],
                "high":   [r[2] for r in records],
                "low":    [r[3] for r in records],
                "close":  [r[4] for r in records],
                "volume": [r[5] for r in records],
            },
            index=dates,
        )

    def _all_zero_close_df(self) -> pd.DataFrame:
        dates = pd.date_range("2023-01-01", periods=5)
        return pd.DataFrame(
            {"open": [0]*5, "high": [0]*5, "low": [0]*5, "close": [0]*5, "volume": [0]*5},
            index=dates,
        )

    def test_good_data_returned(self):
        """正常数据直接返回"""
        class FakeSource:
            name = "fake"
            def get_data(self, *a): return self._df
            _df = None
        reg = DataSourceRegistry()
        src = FakeSource()
        src._df = self._good_df()
        reg.register("fake", src)
        df = reg.get_data("X", "2023-01-01", "2023-01-05", "1d", "none")
        assert not df.empty

    def test_zero_close_data_skipped(self):
        """全零 close 数据应被质量门禁过滤，返回空"""
        class ZeroSrc:
            name = "zero"
            def get_data(self, *a):
                dates = pd.date_range("2023-01-01", periods=5)
                return pd.DataFrame({"close": [0]*5}, index=dates)
        reg = DataSourceRegistry()
        reg.register("zero", ZeroSrc())
        df = reg.get_data("X", "2023-01-01", "2023-01-05", "1d", "none")
        assert df.empty

    def test_zero_close_fallthrough_to_next_source(self):
        """第一源 close=0 → 应跳过并返回第二源的有效数据"""
        class ZeroSrc:
            name = "zero"
            def get_data(self, *a):
                dates = pd.date_range("2023-01-01", periods=3)
                return pd.DataFrame({"close": [0]*3}, index=dates)

        class GoodSrc:
            name = "good"
            def get_data(self, *a):
                from tests.fixtures.real_market_data import RECORDS_000001_SZ_2023Q1
                records = RECORDS_000001_SZ_2023Q1[:3]
                dates = pd.DatetimeIndex([r[0] for r in records])
                return pd.DataFrame(
                    {
                        "open":   [r[1] for r in records],
                        "high":   [r[2] for r in records],
                        "low":    [r[3] for r in records],
                        "close":  [r[4] for r in records],
                        "volume": [r[5] for r in records],
                    },
                    index=dates,
                )

        reg = DataSourceRegistry()
        reg.register("zero", ZeroSrc())
        reg.register("good", GoodSrc())
        df = reg.get_data("X", "2023-01-01", "2023-01-03", "1d", "none",
                          preferred_sources=["zero", "good"])
        assert not df.empty and (df["close"] > 0).all()

    def test_no_close_column_always_passes(self):
        """无 close 列的数据跳过质量检查（兼容非 OHLC 数据）"""
        class NoCloseSrc:
            name = "nocls"
            def get_data(self, *a):
                dates = pd.date_range("2023-01-01", periods=3)
                return pd.DataFrame({"volume": [100]*3}, index=dates)
        reg = DataSourceRegistry()
        reg.register("nocls", NoCloseSrc())
        df = reg.get_data("X", "2023-01-01", "2023-01-03", "1d", "none")
        assert not df.empty


# ══════════════════════════════════════════════════════════════════════════════
# Class G: _check_missing_trading_days 修复验证
# ══════════════════════════════════════════════════════════════════════════════
class TestCheckMissingTradingDaysFixed:
    """
    验证修复后的 _check_missing_trading_days() 不再使用 250/365 近似。
    通过构造 UnifiedDataInterface 并调用私有方法进行白盒测试。
    """

    @pytest.fixture
    def udi(self):
        """无副作用地构造 UDI 实例（不连接 DuckDB）"""
        from data_manager.unified_data_interface import UnifiedDataInterface
        u = UnifiedDataInterface.__new__(UnifiedDataInterface)
        # 只注入必要的属性，避免触发 __init__ 里的数据库连接
        u._logger = __import__("logging").getLogger("test_udi")
        return u

    def test_empty_data_returns_9999(self, udi):
        result = udi._check_missing_trading_days(pd.DataFrame(), "2023-01-01", "2023-12-31")
        assert result == 9999

    def test_complete_year_no_missing(self, udi):
        """整年完整数据（含所有周一至周五）→ 应返回 0

        A 股铁律：周六/周日永远休市（调休补班的周六也不开市）。
        TradingCalendar.is_trading_day() 已加周末前置过滤，get_trading_days()
        返回的日期集合 ⊆ bdate_range（仅周一至周五），因此用 bdate_range
        构造的数据（超集）能覆盖所有预期交易日，结果必为 0。
        """
        dates = pd.bdate_range("2023-01-01", "2023-12-29")
        df = pd.DataFrame({"close": 1}, index=dates)
        result = udi._check_missing_trading_days(df, "2023-01-01", "2023-12-29")
        assert result == 0

    def test_partial_data_triggers_refetch(self, udi):
        """只有 30 天数据，请求 200 天 → 应检测到缺失"""
        dates = pd.bdate_range("2023-01-01", periods=30)
        df = pd.DataFrame({"close": 1}, index=dates)
        result = udi._check_missing_trading_days(df, "2023-01-01", "2023-12-31")
        assert result > 0

    def test_no_longer_uses_250_approximation(self, udi):
        """
        原 bug：200 天 / 预期 244 天 = 82% > 80% → 误判为完整。
        修复后：85% 阈值，200/244 = 82% < 85% → 正确检测为缺失。
        """
        # 构造约 200 天的数据
        dates = pd.bdate_range("2023-01-01", periods=200)
        df = pd.DataFrame({"close": 1}, index=dates)
        # 请求完整一年（预期 ~235-244 天）
        result = udi._check_missing_trading_days(df, "2023-01-01", "2023-12-31")
        # 修复前此处返回 0（漏报）；修复后应返回 > 0
        assert result > 0, "修复后应检测到 ~40 天缺失，不应返回 0"

    def test_start_after_end_returns_0(self, udi):
        """start > end 的无效区间不应崩溃"""
        df = pd.DataFrame({"close": [1]})
        result = udi._check_missing_trading_days(df, "2023-12-31", "2023-01-01")
        assert result == 0


# ══════════════════════════════════════════════════════════════════════════════
# Class H: DATBinarySource 健康状态
# ══════════════════════════════════════════════════════════════════════════════
class TestDATBinarySourceHealth:

    def test_health_reports_unavailable_without_base(self):
        reader = DATBinaryReader(qmt_base=Path("/nonexistent"))
        src = DATBinarySource(reader)
        h = src.health()
        assert h["available"] is False

    def test_health_reports_available_with_valid_base(self, tmp_path):
        reader = DATBinaryReader(qmt_base=tmp_path)
        src = DATBinarySource(reader)
        h = src.health()
        assert h["available"] is True

    def test_get_data_returns_empty_when_unavailable(self):
        reader = DATBinaryReader(qmt_base=Path("/nonexistent"))
        src = DATBinarySource(reader)
        df = src.get_data("600519.SH", "2023-01-01", "2023-12-31", "1d", "none")
        assert df.empty
