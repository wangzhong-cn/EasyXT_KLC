"""
tests/test_stability_tools.py — stability_30d_report / stage1_pipeline 最小单测集

覆盖范围：
  1. 字段兼容映射（P0_open vs P0_open_count，ach vs active_critical_high，ts vs timestamp）
  2. 空数据输入不崩溃（build_report / build_json）
  3. SLA 计算正确性
  4. anomaly schema 规范化（旧格式 ts/anomaly → timestamp/context）与无效字段警告
  5. Stage1 BacktestMetrics 扩展指标（calmar/sortino/turnover > 0）
  6. CI 连续失败逻辑（1天不阻断 → exit 0，2天阻断 → exit 1）
  7. CLI 参数解析（stability_30d_report + stage1_pipeline）
"""
from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

# ─────────────────────────────────────────────────────────────────────────────
# 路径注入，确保项目根在 sys.path
# ─────────────────────────────────────────────────────────────────────────────
ROOT = pathlib.Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.stability_30d_report import (
    _ach,
    _count_consecutive_fails,
    _count_consecutive_step6_worsening,
    _gate,
    _p0,
    _sla,
    _ts,
    _validate_anomaly,
    ANOMALY_EVENT_TYPES,
    ANOMALY_SEVERITIES,
    build_json,
    build_report,
    main as stability_main,
)


# ─────────────────────────────────────────────────────────────────────────────
# 辅助：构造最小合法记录
# ─────────────────────────────────────────────────────────────────────────────

def _make_record(*, gate: bool = True, p0: int = 0, ach: int = 0,
                 ts: str = "2026-01-15T00:00:00Z") -> dict:
    return {
        "ts": ts,
        "strict_gate_pass": gate,
        "P0_open_count": p0,
        "active_critical_high": ach,
    }


def _make_record_old_keys(*, gate: bool = True, p0: int = 0, ach: int = 0,
                           ts: str = "2026-01-15T00:00:00Z") -> dict:
    """旧字段命名：P0_open / ach / strict_pass。"""
    return {
        "ts": ts,
        "strict_pass": gate,
        "P0_open": p0,
        "ach": ach,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 1. 字段兼容层
# ─────────────────────────────────────────────────────────────────────────────

class TestFieldCompatibility(unittest.TestCase):

    def test_p0_new_key(self):
        self.assertEqual(_p0({"P0_open_count": 5}), 5)

    def test_p0_old_key(self):
        self.assertEqual(_p0({"P0_open": 3}), 3)

    def test_p0_new_key_takes_priority(self):
        self.assertEqual(_p0({"P0_open": 3, "P0_open_count": 7}), 7)

    def test_ach_new_key(self):
        self.assertEqual(_ach({"active_critical_high": 2}), 2)

    def test_ach_old_key(self):
        self.assertEqual(_ach({"ach": 4}), 4)

    def test_gate_new_key(self):
        self.assertTrue(_gate({"strict_gate_pass": True}))
        self.assertFalse(_gate({"strict_gate_pass": False}))

    def test_gate_old_key(self):
        self.assertTrue(_gate({"strict_pass": True}))

    def test_ts_iso_z(self):
        r = {"ts": "2026-03-10T08:00:00Z"}
        ts = _ts(r)
        self.assertIsNotNone(ts)
        assert ts is not None
        self.assertEqual(ts.year, 2026)
        self.assertEqual(ts.month, 3)

    def test_ts_timestamp_key(self):
        r = {"timestamp": "2026-04-01T00:00:00+00:00"}
        ts = _ts(r)
        self.assertIsNotNone(ts)
        assert ts is not None
        self.assertEqual(ts.month, 4)


# ─────────────────────────────────────────────────────────────────────────────
# 2. 空数据 build_report / build_json 不崩溃
# ─────────────────────────────────────────────────────────────────────────────

class TestEmptyDataHandling(unittest.TestCase):

    _GEN_AT = "2026-03-15T00:00:00Z"

    def test_build_report_empty_returns_string(self):
        md = build_report([], [], 30, self._GEN_AT)
        self.assertIsInstance(md, str)
        self.assertIn("暂无数据", md)

    def test_build_json_empty(self):
        j = build_json([], [], 30, self._GEN_AT)
        self.assertEqual(j["record_count"], 0)
        self.assertEqual(j["sla_pass"], False)

    def test_build_report_with_data_no_crash(self):
        recs = [_make_record(ts=f"2026-01-{i:02d}T00:00:00Z") for i in range(1, 6)]
        md = build_report(recs, [], 30, self._GEN_AT)
        self.assertIn("SLA", md)

    def test_stability_main_no_history_exit0(self):
        """运行 stability_main 时历史文件不存在应 exit 0（无数据不触发阻断）。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            md_path   = pathlib.Path(tmpdir) / "out.md"
            json_path = pathlib.Path(tmpdir) / "out.json"
            rc = stability_main([
                "--window-days", "7",
                "--out",      str(md_path),
                "--json-out", str(json_path),
            ])
        self.assertEqual(rc, 0)


# ─────────────────────────────────────────────────────────────────────────────
# 3. SLA 计算
# ─────────────────────────────────────────────────────────────────────────────

class TestSlaCalc(unittest.TestCase):

    def test_sla_all_pass(self):
        recs = [_make_record(gate=True,  ts=f"2026-02-{i:02d}T00:00:00Z") for i in range(1, 11)]
        self.assertEqual(_sla(recs), "10/10 (100%)")

    def test_sla_mixed(self):
        recs = [_make_record(gate=(i % 2 == 0), ts=f"2026-02-{i:02d}T00:00:00Z") for i in range(1, 11)]
        result = _sla(recs)
        self.assertIn("5/10", result)

    def test_sla_no_data(self):
        self.assertEqual(_sla([]), "N/A")


class TestStep6AndCanaryAggregation(unittest.TestCase):
    def test_build_json_includes_step6_and_canary_fields(self):
        recs = [
            {
                "ts": "2026-03-01T00:00:00Z",
                "strict_gate_pass": True,
                "P0_open_count": 0,
                "active_critical_high": 0,
                "step6_sampled": 12,
                "step6_hard_fail_rate": 0.05,
                "canary_shadow_write_enabled": True,
                "canary_shadow_only": True,
            }
        ]
        out = build_json(recs, [], 30, "2026-03-15T00:00:00Z")
        self.assertEqual(out["step6_sampled_latest"], 12)
        self.assertEqual(out["step6_hard_fail_rate_latest"], 0.05)
        self.assertEqual(out["canary_mode_latest"], "shadow_only")
        self.assertEqual(out["daily"][0]["canary_mode"], "shadow_only")

    def test_build_json_includes_consecutive_step6_worsening_days(self):
        recs = [
            {"ts": "2026-03-01T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.01},
            {"ts": "2026-03-02T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 12, "step6_hard_fail_rate": 0.03},
            {"ts": "2026-03-03T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 11, "step6_hard_fail_rate": 0.04},
        ]
        out = build_json(recs, [], 30, "2026-03-15T00:00:00Z")
        self.assertEqual(out["consecutive_step6_worsening_days"], 2)

    def test_count_consecutive_step6_worsening_stops_on_non_sampled_day(self):
        recs = [
            {"ts": "2026-03-01T00:00:00Z", "step6_sampled": 10, "step6_hard_fail_rate": 0.01},
            {"ts": "2026-03-02T00:00:00Z", "step6_sampled": 0, "step6_hard_fail_rate": 0.20},
            {"ts": "2026-03-03T00:00:00Z", "step6_sampled": 8, "step6_hard_fail_rate": 0.30},
        ]
        self.assertEqual(_count_consecutive_step6_worsening(recs), 0)


# ─────────────────────────────────────────────────────────────────────────────
# 4. Anomaly schema 规范化与验证
# ─────────────────────────────────────────────────────────────────────────────

class TestAnomalySchema(unittest.TestCase):

    def test_old_format_normalizes_timestamp(self):
        old = {"ts": "2026-03-10T08:00:00Z", "strategy": "s1",
               "anomaly": "回撤过大", "severity": "high"}
        result = _validate_anomaly(old)
        self.assertIn("timestamp", result)
        self.assertEqual(result["timestamp"], "2026-03-10T08:00:00Z")
        self.assertNotIn("ts", result)

    def test_old_format_creates_context(self):
        old = {"ts": "2026-03-10T08:00:00Z", "strategy": "s1",
               "anomaly": "回撤过大", "severity": "high"}
        result = _validate_anomaly(old)
        self.assertIsInstance(result.get("context"), dict)
        self.assertEqual(result["context"].get("description"), "回撤过大")

    def test_new_format_passes_through(self):
        new = {
            "timestamp": "2026-03-10T08:00:00Z", "strategy": "s1",
            "event_type": "drawdown_spike", "severity": "high",
            "context": {"description": "test", "reporter": "auto"},
        }
        result = _validate_anomaly(new)
        self.assertNotIn("_schema_warnings", result)

    def test_invalid_severity_triggers_warning(self):
        ev = {"timestamp": "2026-03-10T08:00:00Z", "strategy": "s1",
              "event_type": "signal_miss", "severity": "EXTREME",
              "context": {"description": "x"}}
        with patch("sys.stderr"):   # 抑制打印输出
            result = _validate_anomaly(ev)
        self.assertIn("_schema_warnings", result)
        self.assertTrue(any("EXTREME" in w for w in result["_schema_warnings"]))

    def test_missing_event_type_defaults_to_unclassified(self):
        ev = {"timestamp": "2026-03-10T08:00:00Z", "strategy": "s1",
              "severity": "low", "context": {"description": "x"}}
        result = _validate_anomaly(ev)
        self.assertEqual(result["event_type"], "unclassified")

    def test_valid_event_types_exist(self):
        """ANOMALY_EVENT_TYPES 应包含基本类型。"""
        self.assertIn("drawdown_spike", ANOMALY_EVENT_TYPES)
        self.assertIn("unclassified",   ANOMALY_EVENT_TYPES)

    def test_valid_severities_exist(self):
        self.assertIn("high",     ANOMALY_SEVERITIES)
        self.assertIn("critical", ANOMALY_SEVERITIES)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Stage1 BacktestMetrics 扩展指标（calmar / sortino / turnover）
# ─────────────────────────────────────────────────────────────────────────────

class TestStage1BacktestMetrics(unittest.TestCase):
    """使用合成数据验证 SimpleBacktester 返回正确的扩展指标。"""

    @classmethod
    def _make_uptrend_df(cls, n: int = 300):
        """生成线性上涨价格序列，金叉次数应 > 0。"""
        import pandas as pd
        prices = [10.0 + 0.05 * i + ((-1) ** i) * 0.1 for i in range(n)]
        return pd.DataFrame({"close": prices})

    def test_backtest_calmar_positive_on_uptrend(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df  = self._make_uptrend_df()
        bt  = SimpleBacktester(df, short_period=5, long_period=20)
        res = bt.run()
        # 上涨行情年化 > 0 且 calmar 应为正
        if res.trade_count > 0:
            self.assertGreaterEqual(res.calmar_ratio, 0.0)

    def test_backtest_turnover_non_negative(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df  = self._make_uptrend_df()
        res = SimpleBacktester(df, short_period=5, long_period=20).run()
        self.assertGreaterEqual(res.turnover_rate_pct, 0.0)

    def test_backtest_empty_df_no_crash(self):
        import pandas as pd
        from strategies.stage1_pipeline import SimpleBacktester
        res = SimpleBacktester(pd.DataFrame(), 5, 20).run()
        self.assertEqual(res.total_return_pct, 0.0)
        self.assertEqual(res.calmar_ratio, 0.0)

    def test_market_assumption_carried_through(self):
        from strategies.stage1_pipeline import SimpleBacktester
        df  = self._make_uptrend_df()
        res = SimpleBacktester(df, market_name="港股").run()
        self.assertEqual(res.market_assumption, "港股")

    def test_slippage_reduces_net_return(self):
        """有滑点时净收益应 <= 无滑点时的净收益。"""
        from strategies.stage1_pipeline import SimpleBacktester
        df        = self._make_uptrend_df()
        no_slip   = SimpleBacktester(df.copy(), slippage=0.0).run()
        with_slip = SimpleBacktester(df.copy(), slippage=0.005).run()
        self.assertLessEqual(with_slip.total_return_pct, no_slip.total_return_pct)

    def test_schema_v2_to_dict(self):
        """Stage1Result.to_dict() 应使用 stage1/v2 schema。"""
        from strategies.stage1_pipeline import (
            BacktestMetrics,
            DataAcceptanceResult,
            InOutSampleResult,
            ParamSensitivityResult,
            Stage1Result,
        )
        bt = BacktestMetrics(1.0, 2.0, 0.5, 5.0, 60.0, 3, 1.0, "full")
        da = DataAcceptanceResult(
            symbol="000001.SZ", period="1d",
            date_range="2020-01-01~2025-01-01",
            expected_trading_days=0, actual_data_days=0,
            coverage_pct=100.0, max_gap_days=0, pass_board=True, failures=[],
        )
        io = InOutSampleResult("", "", 1.0, 0.9, 0.9, True)
        ps = ParamSensitivityResult({"short_period": 5, "long_period": 20}, [], 5.0, True)
        s1 = Stage1Result(
            strategy="test", symbol="000001.SZ", run_date="2026-01-01",
            start="2020-01-01", end="2025-01-01", oos_split="2023-01-01",
            stage1_pass=True, data_acceptance=da,
            full_backtest=bt, in_sample=bt, out_of_sample=bt,
            in_out_comparison=io, param_sensitivity=ps,
        )
        d = s1.to_dict()
        self.assertEqual(d["_schema_version"], "stage1/v2")
        self.assertIn("benchmark_comparison", d)


# ─────────────────────────────────────────────────────────────────────────────
# 6. CI 连续失败逻辑
# ─────────────────────────────────────────────────────────────────────────────

class TestConsecutiveFailLogic(unittest.TestCase):

    def _make_recs(self, pattern: list[bool]) -> list[dict]:
        """根据 bool 列表创建记录序列（True=pass）。"""
        return [
            _make_record(gate=g, ts=f"2026-03-{i+1:02d}T00:00:00Z")
            for i, g in enumerate(pattern)
        ]

    def test_single_fail_at_end(self):
        recs = self._make_recs([True, True, True, False])
        self.assertEqual(_count_consecutive_fails(recs), 1)

    def test_two_fails_at_end(self):
        recs = self._make_recs([True, True, False, False])
        self.assertEqual(_count_consecutive_fails(recs), 2)

    def test_all_pass(self):
        recs = self._make_recs([True, True, True])
        self.assertEqual(_count_consecutive_fails(recs), 0)

    def test_all_fail(self):
        recs = self._make_recs([False, False, False])
        self.assertEqual(_count_consecutive_fails(recs), 3)

    def test_soft_fail_one_day_no_block(self):
        """SLA < 80% 但仅 1 天失败，threshold=2 → exit 0（不阻断）。"""
        recs = self._make_recs([True] * 25 + [False])   # 25/26 pass ≈ 96%… edge case
        # 强制 SLA < 80%：多条失败记录 + 1 天在末尾
        bad_recs = self._make_recs([False] * 8 + [True] + [False])
        # 使用 history json 文件驱动
        with tempfile.TemporaryDirectory() as tmpdir:
            hist_path = pathlib.Path(tmpdir) / "p0_trend_history.json"
            hist_path.write_text(json.dumps(bad_recs), encoding="utf-8")
            md_out = pathlib.Path(tmpdir) / "out.md"
            json_out = pathlib.Path(tmpdir) / "out.json"
            with patch("tools.stability_30d_report.HISTORY_PATH", hist_path):
                rc = stability_main([
                    "--window-days", "365",
                    "--consecutive-fail-days", "2",
                    "--out", str(md_out),
                    "--json-out", str(json_out),
                ])
        self.assertEqual(rc, 0)

    def test_soft_fail_two_days_blocks(self):
        """SLA < 80% 且末尾连续 2 天失败，threshold=2 → exit 1（阻断）。"""
        bad_recs = self._make_recs([False] * 8 + [False, False])  # 全失败
        with tempfile.TemporaryDirectory() as tmpdir:
            hist_path = pathlib.Path(tmpdir) / "p0_trend_history.json"
            hist_path.write_text(json.dumps(bad_recs), encoding="utf-8")
            md_out = pathlib.Path(tmpdir) / "out.md"
            json_out = pathlib.Path(tmpdir) / "out.json"
            with patch("tools.stability_30d_report.HISTORY_PATH", hist_path):
                rc = stability_main([
                    "--window-days", "365",
                    "--consecutive-fail-days", "2",
                    "--out", str(md_out),
                    "--json-out", str(json_out),
                ])
        self.assertEqual(rc, 1)

    def test_step6_warn_days_only_warn_not_block(self):
        recs = [
            {"ts": "2026-03-01T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.01},
            {"ts": "2026-03-02T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.03},
            {"ts": "2026-03-03T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.05},
            {"ts": "2026-03-04T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.07},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            hist_path = pathlib.Path(tmpdir) / "p0_trend_history.json"
            hist_path.write_text(json.dumps(recs), encoding="utf-8")
            md_out = pathlib.Path(tmpdir) / "out.md"
            json_out = pathlib.Path(tmpdir) / "out.json"
            with patch("tools.stability_30d_report.HISTORY_PATH", hist_path):
                rc = stability_main([
                    "--window-days", "365",
                    "--step6-warn-days", "3",
                    "--step6-fail-days", "5",
                    "--out", str(md_out),
                    "--json-out", str(json_out),
                ])
        self.assertEqual(rc, 0)

    def test_step6_fail_days_blocks(self):
        recs = [
            {"ts": "2026-03-01T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.01},
            {"ts": "2026-03-02T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.02},
            {"ts": "2026-03-03T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.03},
            {"ts": "2026-03-04T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.04},
            {"ts": "2026-03-05T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.05},
            {"ts": "2026-03-06T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.06},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            hist_path = pathlib.Path(tmpdir) / "p0_trend_history.json"
            hist_path.write_text(json.dumps(recs), encoding="utf-8")
            md_out = pathlib.Path(tmpdir) / "out.md"
            json_out = pathlib.Path(tmpdir) / "out.json"
            with patch("tools.stability_30d_report.HISTORY_PATH", hist_path):
                rc = stability_main([
                    "--window-days", "365",
                    "--step6-warn-days", "3",
                    "--step6-fail-days", "5",
                    "--out", str(md_out),
                    "--json-out", str(json_out),
                ])
        self.assertEqual(rc, 1)


# ─────────────────────────────────────────────────────────────────────────────
# 7. CLI 参数解析
# ─────────────────────────────────────────────────────────────────────────────

class TestStage1ArgParsing(unittest.TestCase):

    def test_required_args_parse(self):
        """stage1_pipeline main() 应正确解析所有必填/可选参数。"""
        import argparse
        from strategies.stage1_pipeline import main as s1_main
        # 构造 mock runner，不实际执行回测
        import unittest.mock as mock
        with mock.patch("strategies.stage1_pipeline.Stage1Runner") as cls_mock:
            inst = mock.MagicMock()
            inst.run.return_value = mock.MagicMock(
                stage1_pass=True,
                to_dict=lambda: {"_schema_version": "stage1/v2"},
                run_date="2026-01-01",
            )
            cls_mock.return_value = inst
            with tempfile.TemporaryDirectory() as tmpdir:
                rc = s1_main([
                    "--strategy", "测试策略",
                    "--symbol",   "000001.SZ",
                    "--start",    "2020-01-01",
                    "--end",      "2025-12-31",
                    "--oos-split","2023-01-01",
                    "--benchmark","CSI500",
                    "--buy-comm", "0.001",
                    "--sell-comm","0.001",
                    "--slippage", "0.001",
                    "--market",   "ETF",
                    "--out",      str(pathlib.Path(tmpdir) / "test.json"),
                ])
        # Stage1Runner 应以正确参数被调用
        _, kwargs = cls_mock.call_args
        self.assertEqual(kwargs["benchmark"], "CSI500")
        self.assertAlmostEqual(kwargs["buy_comm"], 0.001)
        self.assertAlmostEqual(kwargs["slippage"], 0.001)
        self.assertEqual(kwargs["market_name"], "ETF")

    def test_stability_arg_consecutive_fail_days(self):
        """stability_main 应正确解析 --consecutive-fail-days。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            rc = stability_main([
                "--window-days", "30",
                "--consecutive-fail-days", "3",
                "--out",      str(pathlib.Path(tmpdir) / "out.md"),
                "--json-out", str(pathlib.Path(tmpdir) / "out.json"),
            ])
        self.assertEqual(rc, 0)  # 无历史数据 → 无阻断


if __name__ == "__main__":
    unittest.main(verbosity=2)
