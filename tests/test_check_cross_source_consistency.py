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


# ---------------------------------------------------------------------------
# _read_from_tushare – 单元测试（打桩 tushare 模块）
# ---------------------------------------------------------------------------

import sys
from unittest.mock import MagicMock

from tools.check_cross_source_consistency import _read_from_tushare


def _make_ts_daily_df():
    """构造 Tushare pro.daily() 返回格式的 DataFrame。"""
    return pd.DataFrame({
        "ts_code": ["000001.SZ", "000001.SZ"],
        "trade_date": ["20230103", "20230104"],
        "open": [13.5, 13.7],
        "high": [14.0, 14.1],
        "low": [13.4, 13.6],
        "close": [13.84, 13.90],
        "pre_close": [13.5, 13.84],
        "change": [0.34, 0.06],
        "pct_chg": [2.52, 0.43],
        "vol": [213447.2, 180000.0],   # 单位手
        "amount": [29501000.0, 25000000.0],
    })


def _patch_tushare(monkeypatch, daily_df):
    mock_pro = MagicMock()
    mock_pro.daily.return_value = daily_df
    mock_ts = MagicMock()
    mock_ts.pro_api.return_value = mock_pro
    monkeypatch.setitem(sys.modules, "tushare", mock_ts)
    return mock_pro


def test_read_from_tushare_returns_dataframe(monkeypatch):
    mock_pro = _patch_tushare(monkeypatch, _make_ts_daily_df())
    result = _read_from_tushare("000001.SZ", "2023-01-01", "2023-01-31", "dummy_token")
    assert result is not None
    assert not result.empty
    assert "datetime" in result.columns
    assert "close" in result.columns
    assert "volume" in result.columns


def test_read_from_tushare_volume_converted(monkeypatch):
    """vol 单位手 × 100 = 股。"""
    _patch_tushare(monkeypatch, _make_ts_daily_df())
    result = _read_from_tushare("000001.SZ", "2023-01-01", "2023-01-31", "dummy_token")
    assert result is not None
    # 第一行 vol=213447.2 → volume=21344720
    assert abs(result.iloc[0]["volume"] - 21344720.0) < 1.0


def test_read_from_tushare_date_parsed(monkeypatch):
    """trade_date YYYYMMDD → datetime column。"""
    _patch_tushare(monkeypatch, _make_ts_daily_df())
    result = _read_from_tushare("000001.SZ", "2023-01-01", "2023-01-31", "dummy_token")
    assert result is not None
    assert result.iloc[0]["datetime"].year == 2023
    assert result.iloc[0]["datetime"].month == 1
    assert result.iloc[0]["datetime"].day == 3


def test_read_from_tushare_empty_df_returns_none(monkeypatch):
    _patch_tushare(monkeypatch, pd.DataFrame())
    result = _read_from_tushare("000001.SZ", "2023-01-01", "2023-01-31", "dummy_token")
    assert result is None


def test_read_from_tushare_import_error_returns_none(monkeypatch):
    """tushare 未安装时应返回 None 而非抛出异常。"""
    monkeypatch.setitem(sys.modules, "tushare", None)
    result = _read_from_tushare("000001.SZ", "2023-01-01", "2023-01-31", "token")
    assert result is None


def test_read_from_tushare_api_exception_returns_none(monkeypatch):
    """pro.daily() 抛出异常时返回 None。"""
    mock_pro = MagicMock()
    mock_pro.daily.side_effect = RuntimeError("network error")
    mock_ts = MagicMock()
    mock_ts.pro_api.return_value = mock_pro
    monkeypatch.setitem(sys.modules, "tushare", mock_ts)
    result = _read_from_tushare("000001.SZ", "2023-01-01", "2023-01-31", "token")
    assert result is None


# ---------------------------------------------------------------------------
# run_check_detailed – Tushare 第三方源集成
# ---------------------------------------------------------------------------

from unittest.mock import patch, call

from tools.check_cross_source_consistency import run_check_detailed


def _make_cfg(close_pct=0.5, vol_pct=5.0):
    return CompareConfig(close_diff_threshold_pct=close_pct, volume_diff_threshold_pct=vol_pct)


def _make_daily_result_df(close=10.0, volume=100000.0, date_str="2023-01-03"):
    return pd.DataFrame({"datetime": [pd.Timestamp(date_str)], "close": [close], "volume": [volume]})


def test_run_check_detailed_tushare_pair_keys_present(monkeypatch):
    """当 enable_tushare=True 时，pair_totals 应含 Tushare 相关 key。"""
    # 打桩 UnifiedDataInterface 以避免真实 DB 连接
    mock_ui = MagicMock()
    mock_ui.qmt_available = False
    mock_ui.akshare_available = False
    mock_ui._read_from_duckdb.return_value = _make_daily_result_df(10.0, 100000)
    mock_ui._read_from_qmt.return_value = None
    mock_ui._read_from_akshare.return_value = None
    mock_ui.connect.return_value = None
    mock_ui._check_qmt.return_value = None
    mock_ui._check_akshare.return_value = None
    mock_ui.con = None

    ts_df = _make_daily_result_df(10.0, 100000)

    with patch("data_manager.unified_data_interface.UnifiedDataInterface", return_value=mock_ui), \
         patch("tools.check_cross_source_consistency._load_candidates", return_value=["000001.SZ"]), \
         patch("tools.check_cross_source_consistency._read_from_tushare", return_value=ts_df):
        result = run_check_detailed(
            sample_size=1,
            start_date="2023-01-01",
            end_date="2023-01-31",
            cfg=_make_cfg(),
            seed=42,
            enable_qmt=False,
            enable_akshare=False,
            enable_tushare=True,
            tushare_token="dummy",
        )

    assert "duckdb_vs_tushare" in result["pair_totals"]
    assert "duckdb_vs_tushare" in result["pair_alerts"]


def test_run_check_detailed_tushare_no_alert_when_matching(monkeypatch):
    """DuckDB 与 Tushare 数据完全一致时，pair_alerts['duckdb_vs_tushare'] == 0。"""
    mock_ui = MagicMock()
    mock_ui.qmt_available = False
    mock_ui.akshare_available = False
    mock_ui._read_from_duckdb.return_value = _make_daily_result_df(10.0, 100000)
    mock_ui._read_from_qmt.return_value = None
    mock_ui._read_from_akshare.return_value = None
    mock_ui.connect.return_value = None
    mock_ui._check_qmt.return_value = None
    mock_ui._check_akshare.return_value = None
    mock_ui.con = None

    ts_df = _make_daily_result_df(10.0, 100000)

    with patch("data_manager.unified_data_interface.UnifiedDataInterface", return_value=mock_ui), \
         patch("tools.check_cross_source_consistency._load_candidates", return_value=["000001.SZ"]), \
         patch("tools.check_cross_source_consistency._read_from_tushare", return_value=ts_df):
        result = run_check_detailed(
            sample_size=1,
            start_date="2023-01-01",
            end_date="2023-01-31",
            cfg=_make_cfg(),
            seed=42,
            enable_qmt=False,
            enable_akshare=False,
            enable_tushare=True,
            tushare_token="dummy",
        )

    assert result["pair_alerts"]["duckdb_vs_tushare"] == 0


def test_run_check_detailed_tushare_alert_when_price_diverges(monkeypatch):
    """DuckDB close=10.0 vs Tushare close=15.0 → 差异 33%，超过 0.5% 阈值 → 告警。"""
    mock_ui = MagicMock()
    mock_ui.qmt_available = False
    mock_ui.akshare_available = False
    mock_ui._read_from_duckdb.return_value = _make_daily_result_df(10.0, 100000)
    mock_ui._read_from_qmt.return_value = None
    mock_ui._read_from_akshare.return_value = None
    mock_ui.connect.return_value = None
    mock_ui._check_qmt.return_value = None
    mock_ui._check_akshare.return_value = None
    mock_ui.con = None

    ts_df = _make_daily_result_df(15.0, 100000)  # 价格偏差 33%

    with patch("data_manager.unified_data_interface.UnifiedDataInterface", return_value=mock_ui), \
         patch("tools.check_cross_source_consistency._load_candidates", return_value=["000001.SZ"]), \
         patch("tools.check_cross_source_consistency._read_from_tushare", return_value=ts_df):
        result = run_check_detailed(
            sample_size=1,
            start_date="2023-01-01",
            end_date="2023-01-31",
            cfg=_make_cfg(close_pct=0.5),
            seed=42,
            enable_qmt=False,
            enable_akshare=False,
            enable_tushare=True,
            tushare_token="dummy",
        )

    assert result["pair_alerts"]["duckdb_vs_tushare"] >= 1
    assert result["symbol_alerts"] >= 1


def test_run_check_detailed_tushare_disabled_no_calls(monkeypatch):
    """enable_tushare=False 时，_read_from_tushare 不应被调用。"""
    mock_ui = MagicMock()
    mock_ui.qmt_available = False
    mock_ui.akshare_available = False
    mock_ui._read_from_duckdb.return_value = _make_daily_result_df(10.0, 100000)
    mock_ui._read_from_qmt.return_value = None
    mock_ui._read_from_akshare.return_value = None
    mock_ui.connect.return_value = None
    mock_ui._check_qmt.return_value = None
    mock_ui._check_akshare.return_value = None
    mock_ui.con = None

    with patch("data_manager.unified_data_interface.UnifiedDataInterface", return_value=mock_ui), \
         patch("tools.check_cross_source_consistency._load_candidates", return_value=["000001.SZ"]), \
         patch("tools.check_cross_source_consistency._read_from_tushare") as mock_ts_read:
        run_check_detailed(
            sample_size=1,
            start_date="2023-01-01",
            end_date="2023-01-31",
            cfg=_make_cfg(),
            seed=42,
            enable_qmt=False,
            enable_akshare=False,
            enable_tushare=False,
            tushare_token="",
        )

    mock_ts_read.assert_not_called()


def test_run_check_detailed_source_hits_includes_tushare_key(monkeypatch):
    """source_hits 字典必须含 'tushare' key。"""
    mock_ui = MagicMock()
    mock_ui.qmt_available = False
    mock_ui.akshare_available = False
    mock_ui._read_from_duckdb.return_value = None
    mock_ui._read_from_qmt.return_value = None
    mock_ui._read_from_akshare.return_value = None
    mock_ui.connect.return_value = None
    mock_ui._check_qmt.return_value = None
    mock_ui._check_akshare.return_value = None
    mock_ui.con = None

    with patch("data_manager.unified_data_interface.UnifiedDataInterface", return_value=mock_ui), \
         patch("tools.check_cross_source_consistency._load_candidates", return_value=["000001.SZ"]), \
         patch("tools.check_cross_source_consistency._read_from_tushare", return_value=None):
        result = run_check_detailed(
            sample_size=1,
            start_date="2023-01-01",
            end_date="2023-01-31",
            cfg=_make_cfg(),
            seed=42,
            enable_qmt=False,
            enable_akshare=False,
            enable_tushare=True,
            tushare_token="dummy",
        )

    assert "tushare" in result["source_hits"]
