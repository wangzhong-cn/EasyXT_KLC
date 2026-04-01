from __future__ import annotations

import json

import pandas as pd
import pytest

from data_manager.period_bar_builder import PERIOD_BAR_BUILDER_VERSION, PeriodBarBuilder


def _make_intraday_df() -> pd.DataFrame:
    am_auction = pd.date_range("2024-01-02 09:15:00", "2024-01-02 09:25:00", freq="1min")
    am = pd.date_range("2024-01-02 09:30:00", "2024-01-02 11:30:00", freq="1min")
    pm = pd.date_range("2024-01-02 13:00:00", "2024-01-02 15:00:00", freq="1min")
    ts = am_auction.append(am).append(pm)
    n = len(ts)
    prices = [10.0 + i * 0.01 for i in range(n)]
    return pd.DataFrame(
        {
            "time": ts,
            "open": prices,
            "high": [p + 0.02 for p in prices],
            "low": [p - 0.02 for p in prices],
            "close": prices,
            "volume": [100.0] * n,
        }
    )


def test_intraday_metadata_columns_and_session_profile():
    df_1m = _make_intraday_df()
    df_1d = pd.DataFrame(
        {
            "time": [pd.Timestamp("2024-01-02")],
            "open": [10.0],
            "high": [13.0],
            "low": [9.5],
            "close": [12.5],
            "volume": [float(df_1m["volume"].sum())],
        }
    )
    builder = PeriodBarBuilder(session_profile="CN_A_AUCTION")
    out = builder.build_intraday_bars(df_1m, period_minutes=10, daily_ref=df_1d)
    assert not out.empty
    assert (out["alignment"] == "left").all()
    assert (out["anchor"] == "daily_close").all()
    assert (out["session_profile"] == "CN_A_AUCTION").all()
    assert (out["session_profile_id"] == "CN_A_AUCTION").all()
    assert (out["session_profile_version"] == "legacy").all()
    assert (out["auction_policy"] == "unknown").all()
    assert (out["period_code"] == "10m").all()
    assert (out["period_family"] == "intraday").all()
    assert (out["period_registry_version"] == "legacy").all()
    assert (out["threshold_version"] == "legacy").all()
    assert (out["bar_builder_version"] == PERIOD_BAR_BUILDER_VERSION).all()


def test_intraday_metadata_versions_can_be_overridden():
    df_1m = _make_intraday_df()
    df_1d = pd.DataFrame(
        {
            "time": [pd.Timestamp("2024-01-02")],
            "open": [10.0],
            "high": [13.0],
            "low": [9.5],
            "close": [12.5],
            "volume": [float(df_1m["volume"].sum())],
        }
    )
    builder = PeriodBarBuilder(
        session_profile="CN_A_AUCTION",
        session_profile_version="2026.04.01",
        auction_policy="explicit_auction_session",
        period_registry_version="2026.04.01",
        default_threshold_version="2026.03.31",
    )
    out = builder.build_intraday_bars(
        df_1m,
        period_minutes=10,
        daily_ref=df_1d,
        period_code="1W",
        period_family="intraday",
        threshold_version="2026.04.02",
    )
    assert not out.empty
    assert (out["session_profile_version"] == "2026.04.01").all()
    assert (out["auction_policy"] == "explicit_auction_session").all()
    assert (out["period_code"] == "1W").all()
    assert (out["period_registry_version"] == "2026.04.01").all()
    assert (out["threshold_version"] == "2026.04.02").all()
    assert (out["bar_builder_version"] == PERIOD_BAR_BUILDER_VERSION).all()


def test_anchor_none_does_not_force_daily_close():
    df_1m = pd.DataFrame(
        {
            "time": pd.date_range("2024-01-02 09:30:00", "2024-01-02 09:34:00", freq="1min"),
            "open": [10.0, 10.1, 10.2, 10.3, 10.4],
            "high": [10.0, 10.1, 10.2, 10.3, 10.4],
            "low": [10.0, 10.1, 10.2, 10.3, 10.4],
            "close": [10.0, 10.1, 10.2, 10.3, 10.4],
            "volume": [100, 100, 100, 100, 100],
        }
    )
    daily = pd.DataFrame(
        {"time": [pd.Timestamp("2024-01-02")], "open": [10.0], "high": [12.0], "low": [9.0], "close": [12.0], "volume": [500]}
    )
    out = PeriodBarBuilder(anchor="none").build_intraday_bars(df_1m, period_minutes=5, daily_ref=daily)
    assert not out.empty
    assert float(out.iloc[-1]["close"]) == 10.4


def test_cross_validate_emits_jsonl_report(tmp_path):
    report_file = tmp_path / "period_validation_report.jsonl"
    df_1m = _make_intraday_df()
    df_1d = pd.DataFrame(
        {
            "time": [pd.Timestamp("2024-01-02")],
            "open": [10.0],
            "high": [13.0],
            "low": [9.5],
            "close": [12.5],
            "volume": [float(df_1m["volume"].sum())],
        }
    )
    builder = PeriodBarBuilder(validation_report_file=str(report_file))
    bars = builder.build_intraday_bars(df_1m, period_minutes=10, daily_ref=df_1d)
    vr = builder.cross_validate("10m", bars, daily_ref=df_1d)
    assert vr.is_valid
    assert report_file.exists()
    lines = report_file.read_text(encoding="utf-8").strip().splitlines()
    assert lines
    payload = json.loads(lines[-1])
    assert payload["period"] == "10m"
    assert payload["period_code"] == "10m"
    assert payload["period_family"] == "intraday"
    assert payload["alignment"] == "left"
    assert payload["anchor"] == "daily_close"
    assert payload["session_profile_version"] == "legacy"
    assert payload["bar_builder_version"] == PERIOD_BAR_BUILDER_VERSION


def test_cross_validate_error_report_details_schema_stable(tmp_path):
    report_file = tmp_path / "period_validation_report.jsonl"
    df_1m = _make_intraday_df()
    df_1d = pd.DataFrame(
        {
            "time": [pd.Timestamp("2024-01-02")],
            "open": [10.0],
            "high": [13.0],
            "low": [9.5],
            "close": [12.5],
            "volume": [float(df_1m["volume"].sum())],
        }
    )
    builder = PeriodBarBuilder(validation_report_file=str(report_file))
    bars = builder.build_intraday_bars(df_1m, period_minutes=10, daily_ref=df_1d)
    bad_daily = df_1d.copy()
    bad_daily.loc[:, "close"] = float(df_1d.iloc[0]["close"]) + 3.0
    vr = builder.cross_validate("10m", bars, daily_ref=bad_daily)
    assert vr.is_valid is False
    lines = report_file.read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(lines[-1])
    assert payload["is_valid"] is False
    assert isinstance(payload.get("details"), list) and payload["details"]
    for d in payload["details"]:
        assert {"metric", "actual", "expected", "delta"}.issubset(set(d.keys()))


# ---------------------------------------------------------------------------
# ValidationResult – to_dict / add_detail
# ---------------------------------------------------------------------------

def test_validation_result_to_dict_empty():
    from data_manager.period_bar_builder import ValidationResult
    vr = ValidationResult()
    d = vr.to_dict()
    assert d["is_valid"] is True
    assert d["errors"] == []
    assert d["warnings"] == []
    assert d["details"] == []


def test_validation_result_add_detail_and_to_dict():
    from data_manager.period_bar_builder import ValidationResult
    vr = ValidationResult()
    vr.add_detail({"date": "2024-01-02", "metric": "close_diff", "delta": 0.05})
    vr.add_error("收盘价未收敛")
    d = vr.to_dict()
    assert d["is_valid"] is False
    assert len(d["details"]) == 1
    assert d["details"][0]["metric"] == "close_diff"
    assert len(d["errors"]) == 1


def test_validation_result_add_detail_ignores_non_dict():
    from data_manager.period_bar_builder import ValidationResult
    vr = ValidationResult()
    vr.add_detail("not a dict")  # type: ignore[arg-type]
    assert vr.details == []


# ---------------------------------------------------------------------------
# PeriodBarBuilder.__init__ – invalid params raise ValueError
# ---------------------------------------------------------------------------

def test_invalid_alignment_raises():
    with pytest.raises(ValueError, match="unsupported alignment"):
        PeriodBarBuilder(alignment="right")


def test_invalid_anchor_raises():
    with pytest.raises(ValueError, match="unsupported anchor"):
        PeriodBarBuilder(anchor="market_open")


# ---------------------------------------------------------------------------
# build() – dispatcher paths for multiday and natural_calendar
# ---------------------------------------------------------------------------

def _make_1d_df(n=10, start="2024-01-02") -> pd.DataFrame:
    dates = pd.date_range(start, periods=n, freq="B")
    prices = [10.0 + i * 0.1 for i in range(n)]
    return pd.DataFrame({
        "time": dates,
        "open": prices,
        "high": [p + 0.5 for p in prices],
        "low": [p - 0.5 for p in prices],
        "close": prices,
        "volume": [100000.0] * n,
    })


def test_build_multiday_returns_dataframe():
    df_1d = _make_1d_df(n=20, start="2024-01-02")
    builder = PeriodBarBuilder()
    result = builder.build("5d", data_1d=df_1d, listing_date="2024-01-02")
    assert not result.empty
    assert "open" in result.columns
    assert "close" in result.columns


def test_build_multiday_empty_1d_returns_empty():
    builder = PeriodBarBuilder()
    result = builder.build("5d", data_1d=pd.DataFrame(), listing_date="2024-01-02")
    assert result.empty


def test_build_natural_calendar_week():
    df_1d = _make_1d_df(n=20, start="2024-01-02")
    builder = PeriodBarBuilder()
    result = builder.build("1w", data_1d=df_1d)
    assert not result.empty
    assert (result["alignment"] == "calendar_right").all()
    assert (result["anchor"] == "period_end").all()


def test_build_natural_calendar_empty_returns_empty():
    builder = PeriodBarBuilder()
    result = builder.build("1w", data_1d=pd.DataFrame())
    assert result.empty


def test_build_unknown_period_raises():
    builder = PeriodBarBuilder()
    with pytest.raises(ValueError):
        builder.build("99x", data_1d=_make_1d_df())


def test_build_base_period_raises():
    """BASE periods (1m/1d) cannot be built through this module."""
    builder = PeriodBarBuilder()
    with pytest.raises(ValueError, match="基础数据"):
        builder.build("1d", data_1d=_make_1d_df())


# ---------------------------------------------------------------------------
# cross_validate() – unknown period and empty bars early-return paths
# ---------------------------------------------------------------------------

def test_cross_validate_unknown_period_returns_warning():
    builder = PeriodBarBuilder()
    bars = _make_1d_df(n=5)
    vr = builder.cross_validate("UNKNOWN_XYZ", bars)
    assert not vr.is_valid or len(vr.warnings) > 0


def test_cross_validate_empty_bars_returns_warning():
    builder = PeriodBarBuilder()
    vr = builder.cross_validate("10m", pd.DataFrame())
    assert len(vr.warnings) > 0


# ---------------------------------------------------------------------------
# build_natural_calendar_bars – session_profile metadata
# ---------------------------------------------------------------------------

def test_build_natural_calendar_session_profile():
    df_1d = _make_1d_df(n=30, start="2024-01-02")
    builder = PeriodBarBuilder(session_profile="CN_A_AUCTION")
    result = builder.build_natural_calendar_bars(df_1d, freq="ME")
    assert not result.empty
    assert (result["session_profile"] == "CN_A_AUCTION").all()
    assert (result["period_code"] == "1M").all()
    assert (result["period_family"] == "natural_calendar").all()


# ---------------------------------------------------------------------------
# cross_validate with multiday – _validate_multiday_vs_daily path
# ---------------------------------------------------------------------------

def test_cross_validate_multiday_vs_daily():
    df_1d = _make_1d_df(n=20, start="2024-01-02")
    builder = PeriodBarBuilder()
    bars_5d = builder.build("5d", data_1d=df_1d, listing_date="2024-01-02")
    assert not bars_5d.empty
    vr = builder.cross_validate("5d", bars_5d, daily_ref=df_1d)
    # Should run _validate_multiday_vs_daily without error
    assert isinstance(vr.is_valid, bool)


# ---------------------------------------------------------------------------
# _validate_intraday_vs_daily – volume divergence warning path
# ---------------------------------------------------------------------------

def test_intraday_volume_divergence_adds_warning():
    """When intraday total volume differs from daily by >5%, a warning is added."""
    df_1m = _make_intraday_df()
    daily_volume = float(df_1m["volume"].sum()) * 2.0  # deliberately 100% more → triggers warning
    df_1d = pd.DataFrame({
        "time": [pd.Timestamp("2024-01-02")],
        "open": [10.0], "high": [13.0], "low": [9.5],
        "close": [df_1m["close"].iloc[-1]],  # same final close → no close error
        "volume": [daily_volume],
    })
    builder = PeriodBarBuilder()
    bars = builder.build_intraday_bars(df_1m, period_minutes=10, daily_ref=df_1d)
    vr = builder.cross_validate("10m", bars, daily_ref=df_1d)
    # Volume divergence should produce a warning or detail
    assert len(vr.warnings) > 0 or len(vr.details) > 0


def test_session_profile_file_override(tmp_path):
    profile_file = tmp_path / "session_profiles.json"
    profile_file.write_text(
        json.dumps({"CUSTOM_DAY": [["09:30", "09:34"]]}, ensure_ascii=False),
        encoding="utf-8",
    )
    df_1m = pd.DataFrame(
        {
            "time": pd.date_range("2024-01-02 09:30:00", "2024-01-02 09:34:00", freq="1min"),
            "open": [10, 10.1, 10.2, 10.3, 10.4],
            "high": [10, 10.1, 10.2, 10.3, 10.4],
            "low": [10, 10.1, 10.2, 10.3, 10.4],
            "close": [10, 10.1, 10.2, 10.3, 10.4],
            "volume": [100, 100, 100, 100, 100],
        }
    )
    out = PeriodBarBuilder(session_profile="CUSTOM_DAY", session_profile_file=str(profile_file)).build_intraday_bars(
        df_1m, period_minutes=5
    )
    assert not out.empty
    assert (out["session_profile"] == "CUSTOM_DAY").all()
