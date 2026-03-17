"""
test_stage1_batch.py — Stage 1 批量评测器回归测试

覆盖：
  - 三层参数覆盖（defaults → group → asset）
  - 置信度分级（HIGH / MEDIUM / LOW / INSUFFICIENT_DATA）
  - 可达率告警触发（外置阈值 per-group）
  - 成交可行性检查（FAIL / WARN / INFO）
  - 换月规则格式化
  - 失败原因分类 _categorize_failure_reason
  - 分组聚合 _aggregate_group（failure_reasons / accessibility_warn_pct）
  - SLIPPAGE_SENSITIVITY 完整性
"""
from __future__ import annotations

import pathlib
import sys

import pytest

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from strategies.stage1_batch import (  # noqa: E402
    SLIPPAGE_SENSITIVITY,
    AssetResult,
    _aggregate_group,
    _build_run_configs,
    _categorize_failure_reason,
    _check_tradeability,
    _compute_confidence,
    _format_rollover_policy,
)


# ─────────────────────────────────────────────────────────────────────────────
# 辅助：最小宇宙 YAML 结构
# ─────────────────────────────────────────────────────────────────────────────

def _mini_universe(asset_overrides: dict | None = None) -> dict:
    """构造最小化宇宙结构，用于参数覆盖测试。"""
    return {
        "defaults": {
            "start":       "2020-01-01",
            "end":         "2024-12-31",
            "short_period": 5,
            "long_period":  20,
            "thresholds":  {"accessibility_warn_pct": 80},
        },
        "groups": {
            "test_group": {
                "label":       "测试组",
                "slippage":    0.001,
                "long_period": 30,     # 覆盖 defaults
                "assets": [
                    {
                        "symbol":       "TEST.SZ",
                        "name":         "测试股票",
                        "short_period": 3,   # 覆盖 group/defaults
                        **(asset_overrides or {}),
                    }
                ],
            }
        },
    }


def _make_asset(
    group: str = "g",
    pass_: bool = True,
    error: str = "",
    td_pass: bool = True,
    flags: list[str] | None = None,
    sharpe: float = 1.0,
) -> AssetResult:
    return AssetResult(
        group=group, symbol="X", original_symbol="X", name="X",
        stage1_pass=pass_, summary="",
        sharpe_full=sharpe, annualized_pct=10.0, max_drawdown_pct=5.0,
        calmar_ratio=2.0, sortino_ratio=1.5, oos_ratio=0.9,
        error=error,
        tradeability_pass=td_pass,
        tradeability_flags=flags or [],
    )


# ─────────────────────────────────────────────────────────────────────────────
# 三层参数覆盖
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildRunConfigs:
    def test_defaults_applied(self):
        cfgs = _build_run_configs(_mini_universe())
        assert cfgs[0]["start"] == "2020-01-01"
        assert cfgs[0]["end"]   == "2024-12-31"

    def test_group_overrides_defaults(self):
        """group.long_period=30 应覆盖 defaults.long_period=20"""
        cfgs = _build_run_configs(_mini_universe())
        assert cfgs[0]["long_period"] == 30

    def test_asset_overrides_group_and_defaults(self):
        """asset.short_period=3 应覆盖 defaults.short_period=5"""
        cfgs = _build_run_configs(_mini_universe())
        assert cfgs[0]["short_period"] == 3

    def test_asset_level_slippage_override(self):
        cfgs = _build_run_configs(_mini_universe({"slippage": 0.0005}))
        assert cfgs[0]["slippage"] == 0.0005

    def test_continuous_symbol_wins_over_symbol(self):
        cfgs = _build_run_configs(_mini_universe({"continuous_symbol": "CONT_MAIN"}))
        assert cfgs[0]["actual_symbol"] == "CONT_MAIN"
        assert cfgs[0]["symbol"]        == "TEST.SZ"

    def test_no_continuous_symbol_uses_symbol(self):
        cfgs = _build_run_configs(_mini_universe())
        assert cfgs[0]["actual_symbol"] == "TEST.SZ"

    def test_group_key_set(self):
        cfgs = _build_run_configs(_mini_universe())
        assert cfgs[0]["group"] == "test_group"

    def test_label_inherited_from_group(self):
        cfgs = _build_run_configs(_mini_universe())
        assert cfgs[0]["label"] == "测试组"

    def test_thresholds_propagated_from_defaults(self):
        """thresholds 块应随 defaults → group 合并传入每个 asset cfg"""
        cfgs = _build_run_configs(_mini_universe())
        assert cfgs[0].get("thresholds", {}).get("accessibility_warn_pct") == 80

    def test_group_overrides_threshold(self):
        """group-level thresholds 覆盖 defaults thresholds"""
        universe = _mini_universe()
        universe["groups"]["test_group"]["thresholds"] = {"accessibility_warn_pct": 90}
        cfgs = _build_run_configs(universe)
        assert cfgs[0]["thresholds"]["accessibility_warn_pct"] == 90

    def test_multi_group_multi_asset_count(self):
        """两组各两个标的 → 共 4 条 config"""
        universe = {
            "defaults": {},
            "groups": {
                "g1": {"assets": [{"symbol": "A"}, {"symbol": "B"}]},
                "g2": {"assets": [{"symbol": "C"}, {"symbol": "D"}]},
            },
        }
        cfgs = _build_run_configs(universe)
        assert len(cfgs) == 4


# ─────────────────────────────────────────────────────────────────────────────
# 置信度分级
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeConfidence:
    def test_high(self):
        label, _ = _compute_confidence(total=5, error_count=0, pass_rate_pct=50.0)
        assert label == "HIGH"

    def test_medium_with_one_error(self):
        label, _ = _compute_confidence(total=4, error_count=1, pass_rate_pct=60.0)
        assert label == "MEDIUM"

    def test_low_two_valid(self):
        label, _ = _compute_confidence(total=2, error_count=0, pass_rate_pct=50.0)
        assert label == "LOW"

    def test_insufficient_no_valid(self):
        label, _ = _compute_confidence(total=2, error_count=2, pass_rate_pct=0.0)
        assert label == "INSUFFICIENT_DATA"

    def test_insufficient_empty(self):
        label, _ = _compute_confidence(total=0, error_count=0, pass_rate_pct=0.0)
        assert label == "INSUFFICIENT_DATA"

    def test_high_requires_zero_error(self):
        label, _ = _compute_confidence(total=5, error_count=1, pass_rate_pct=60.0)
        assert label != "HIGH"

    def test_high_pass_rate_too_low(self):
        label, _ = _compute_confidence(total=5, error_count=0, pass_rate_pct=5.0)
        assert label != "HIGH"

    def test_high_pass_rate_too_high(self):
        label, _ = _compute_confidence(total=5, error_count=0, pass_rate_pct=95.0)
        assert label != "HIGH"

    def test_medium_high_error_rate_falls_to_low(self):
        """error_rate=33% > 25% → 不满足 MEDIUM，valid_n=2 先检查 LOW"""
        label, _ = _compute_confidence(total=3, error_count=1, pass_rate_pct=50.0)
        # valid_n=2, error_rate=33%  → LOW (valid_n>=2 and error_rate<50)
        assert label == "LOW"


# ─────────────────────────────────────────────────────────────────────────────
# 成交可行性检查
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckTradeability:
    def test_slippage_ok_no_fail(self):
        cfg = {"slippage": 0.0005, "tradeability": {"slippage_tier": "equity"}}
        ok, flags = _check_tradeability(cfg)
        assert ok
        assert not any(f.startswith("FAIL") for f in flags)

    def test_slippage_hard_fail(self):
        """equity fail=0.005；0.006 → FAIL"""
        cfg = {"slippage": 0.006, "tradeability": {"slippage_tier": "equity"}}
        ok, flags = _check_tradeability(cfg)
        assert not ok
        assert any(f.startswith("FAIL") and "滑点" in f for f in flags)

    def test_slippage_warn_range(self):
        """equity warn=0.002；0.003 → WARN 但不 FAIL"""
        cfg = {"slippage": 0.003, "tradeability": {"slippage_tier": "equity"}}
        ok, flags = _check_tradeability(cfg)
        assert ok
        assert any(f.startswith("WARN") and "滑点" in f for f in flags)

    def test_missing_min_turnover_warns(self):
        cfg = {"slippage": 0.001, "tradeability": {"slippage_tier": "equity"}}
        _, flags = _check_tradeability(cfg)
        assert any("min_daily_turnover" in f for f in flags)

    def test_capacity_info_generated(self):
        cfg = {
            "slippage": 0.001,
            "tradeability": {
                "slippage_tier":          "equity",
                "min_daily_turnover_cny": 50_000_000,
                "max_participation_rate": 0.10,
            },
        }
        _, flags = _check_tradeability(cfg)
        assert any(f.startswith("INFO") and "容量" in f for f in flags)

    def test_participation_over_20pct_fails(self):
        cfg = {
            "slippage": 0.001,
            "tradeability": {
                "slippage_tier":        "equity",
                "max_participation_rate": 0.25,
            },
        }
        ok, flags = _check_tradeability(cfg)
        assert not ok
        assert any(f.startswith("FAIL") and "participation" in f.lower() for f in flags)

    def test_default_tier_equity_when_missing(self):
        """无 tradeability 配置 → 默认 equity tier；0.001 < 0.005 → OK"""
        ok, _ = _check_tradeability({"slippage": 0.001})
        assert ok

    def test_commodity_futures_tier_warn_not_fail(self):
        """commodity_futures fail=0.010；0.009 → WARN 不 FAIL"""
        cfg = {"slippage": 0.009, "tradeability": {"slippage_tier": "commodity_futures"}}
        ok, flags = _check_tradeability(cfg)
        assert ok
        assert any(f.startswith("WARN") for f in flags)

    def test_etf_tier_tighter_warn(self):
        """etf warn=0.001；0.002 → WARN"""
        cfg = {"slippage": 0.002, "tradeability": {"slippage_tier": "etf"}}
        ok, flags = _check_tradeability(cfg)
        assert ok
        assert any(f.startswith("WARN") for f in flags)


# ─────────────────────────────────────────────────────────────────────────────
# 换月规则格式化
# ─────────────────────────────────────────────────────────────────────────────

class TestFormatRolloverPolicy:
    def test_empty_for_non_futures(self):
        assert _format_rollover_policy({"slippage": 0.001}) == ""

    def test_full_policy_format(self):
        cfg = {
            "rollover": {
                "method":                 "days_before_expiry",
                "days_before_expiry":     5,
                "volume_ratio_threshold": 1.2,
                "avoid_open_on_rollover": True,
                "rollover_slippage_add":  0.001,
            }
        }
        result = _format_rollover_policy(cfg)
        assert "days_before_expiry" in result
        assert "到期前5日" in result
        assert "1.2" in result
        assert "换月日禁开仓" in result
        assert "0.10%" in result   # 0.001 * 100

    def test_continuous_symbol_triggers_format(self):
        """有 continuous_symbol 但无 rollover 块 → 返回非空（至少包含 method=unspecified）"""
        result = _format_rollover_policy({"continuous_symbol": "RB_MAIN"})
        assert result != ""

    def test_partial_rollover_no_crash(self):
        """rollover 块只有 method → 不崩溃"""
        cfg = {"rollover": {"method": "volume_ratio"}}
        result = _format_rollover_policy(cfg)
        assert "volume_ratio" in result


# ─────────────────────────────────────────────────────────────────────────────
# 失败原因分类
# ─────────────────────────────────────────────────────────────────────────────

class TestCategorizeFaultReason:
    def test_slippage_fault(self):
        assert _categorize_failure_reason("FAIL|滑点 0.006 超出容忍上限") == "流动性/成交约束"

    def test_participation_fault(self):
        assert _categorize_failure_reason("FAIL|participation 25% > 20%") == "流动性/成交约束"

    def test_data_gap_fault(self):
        assert _categorize_failure_reason("KeyError: 数据缺口过大") == "数据缺口"

    def test_coverage_fault(self):
        assert _categorize_failure_reason("coverage < 0.95") == "数据缺口"

    def test_rollover_fault(self):
        assert _categorize_failure_reason("换月映射找不到主力合约") == "换月映射缺失"

    def test_continuous_fault(self):
        assert _categorize_failure_reason("continuous symbol not found") == "换月映射缺失"

    def test_config_error(self):
        # 不含流动性/数据关键字，纯 config 错误
        assert _categorize_failure_reason("ValueError: 配置字段 yaml_path 无效") == "配置错误"

    def test_keyerror_config(self):
        # 'missing' 会触发数据缺口，改用不含 data-keywords 的 keyerror 消息
        assert _categorize_failure_reason("KeyError: 'yaml_path'") == "配置错误"

    def test_generic_runtime(self):
        assert _categorize_failure_reason("RuntimeError: 未知异常") == "回测异常"


# ─────────────────────────────────────────────────────────────────────────────
# 分组聚合
# ─────────────────────────────────────────────────────────────────────────────

class TestAggregateGroup:
    def test_pass_rate(self):
        results = [_make_asset(pass_=True)] * 3 + [_make_asset(pass_=False)]
        gs = _aggregate_group("g", "G", results, {})
        assert gs.pass_rate_pct == 75.0

    def test_accessibility_full(self):
        results = [_make_asset()] * 3
        gs = _aggregate_group("g", "G", results, {})
        assert gs.accessibility_pct == 100.0

    def test_accessibility_partial(self):
        """2 error / 4 total → 50%"""
        results = [_make_asset()] * 2 + [_make_asset(error="ERR")] * 2
        gs = _aggregate_group("g", "G", results, {})
        assert gs.accessibility_pct == 50.0

    def test_accessibility_warn_pct_from_threshold(self):
        gs = _aggregate_group("g", "G", [_make_asset()], {"accessibility_warn_pct": 90})
        assert gs.accessibility_warn_pct == 90.0

    def test_accessibility_warn_pct_default_80(self):
        gs = _aggregate_group("g", "G", [_make_asset()], {})
        assert gs.accessibility_warn_pct == 80.0

    def test_accessibility_warn_pct_none_threshold(self):
        gs = _aggregate_group("g", "G", [_make_asset()], None)
        assert gs.accessibility_warn_pct == 80.0

    def test_td_fail_count(self):
        results = [
            _make_asset(td_pass=False), _make_asset(td_pass=True),
            _make_asset(td_pass=False),
        ]
        gs = _aggregate_group("g", "G", results, {})
        assert gs.tradeability_fail_count == 2

    def test_failure_reasons_from_errors(self):
        results = [
            _make_asset(error="数据缺口"),
            _make_asset(error="数据缺口"),
            _make_asset(error="换月映射"),
        ]
        gs = _aggregate_group("g", "G", results, {})
        assert gs.failure_reasons.get("数据缺口", 0) == 2
        assert gs.failure_reasons.get("换月映射缺失", 0) == 1

    def test_failure_reasons_from_fail_flags(self):
        flags = ["FAIL|滑点 0.006 超出容忍上限", "WARN|optional warning", "INFO|info only"]
        results = [_make_asset(td_pass=False, flags=flags)]
        gs = _aggregate_group("g", "G", results, {})
        # 只有 FAIL flag 被计入 failure_reasons
        assert "流动性/成交约束" in gs.failure_reasons
        assert gs.failure_reasons.get("流动性/成交约束", 0) == 1

    def test_warn_flags_not_counted_in_failure_reasons(self):
        """WARN flag 不计入 failure_reasons"""
        flags = ["WARN|参与率偏高"]
        results = [_make_asset(td_pass=True, flags=flags)]
        gs = _aggregate_group("g", "G", results, {})
        assert not gs.failure_reasons  # 无 FAIL，无 error → 空

    def test_failure_reasons_sorted_descending(self):
        """出现最多的原因排第一"""
        results = [
            _make_asset(error="数据缺口"),
            _make_asset(error="数据缺口"),
            _make_asset(error="换月映射"),
        ]
        gs = _aggregate_group("g", "G", results, {})
        first_key = next(iter(gs.failure_reasons))
        assert gs.failure_reasons[first_key] == 2

    def test_confidence_high_when_conditions_met(self):
        """6 valid, 0 error, pass_rate=50% → HIGH"""
        results = [_make_asset(pass_=True)] * 3 + [_make_asset(pass_=False)] * 3
        gs = _aggregate_group("g", "G", results, {})
        assert gs.confidence == "HIGH"

    def test_verdict_go(self):
        results = [_make_asset(pass_=True, sharpe=1.5)] * 4
        gs = _aggregate_group("g", "G", results, {})
        assert gs.verdict == "GO"

    def test_verdict_nogo(self):
        results = [_make_asset(pass_=False, sharpe=-0.5)] * 4
        gs = _aggregate_group("g", "G", results, {})
        assert gs.verdict == "NO-GO"

    def test_empty_results_no_crash(self):
        gs = _aggregate_group("g", "G", [], {})
        assert gs.total == 0
        assert gs.accessibility_pct == 0.0
        assert gs.confidence == "INSUFFICIENT_DATA"


# ─────────────────────────────────────────────────────────────────────────────
# 可达率告警触发（外置阈值）
# ─────────────────────────────────────────────────────────────────────────────

class TestAccessibilityAlertLogic:
    """模拟 run_batch 中 gs.accessibility_pct < gs.accessibility_warn_pct 判断。"""

    def test_no_alert_above_default_threshold(self):
        results = [_make_asset()] * 4
        gs = _aggregate_group("g", "G", results, {"accessibility_warn_pct": 80})
        assert gs.accessibility_pct >= gs.accessibility_warn_pct

    def test_alert_triggered_50pct_below_80(self):
        """2 error / 4 total = 50% < 80%"""
        results = [_make_asset()] * 2 + [_make_asset(error="Err")] * 2
        gs = _aggregate_group("g", "G", results, {"accessibility_warn_pct": 80})
        assert gs.accessibility_pct < gs.accessibility_warn_pct

    def test_strict_threshold_90_triggers_at_75pct(self):
        """3/4 可达 = 75% < 90% 告警阈值"""
        results = [_make_asset()] * 3 + [_make_asset(error="Err")]
        gs = _aggregate_group("g", "G", results, {"accessibility_warn_pct": 90})
        assert gs.accessibility_pct < gs.accessibility_warn_pct
        assert gs.accessibility_warn_pct == 90.0

    def test_loose_threshold_60_no_alert_at_75pct(self):
        """3/4 可达 = 75% >= 60%，不触发告警"""
        results = [_make_asset()] * 3 + [_make_asset(error="Err")]
        gs = _aggregate_group("g", "G", results, {"accessibility_warn_pct": 60})
        assert gs.accessibility_pct >= gs.accessibility_warn_pct

    def test_index_futures_style_90pct_threshold(self):
        """模拟 index_futures 配置 90% 阈值; 全部可达时不触发"""
        results = [_make_asset()] * 3
        gs = _aggregate_group("index_futures", "股指期货", results, {"accessibility_warn_pct": 90})
        assert gs.accessibility_pct >= gs.accessibility_warn_pct  # 100% >= 90%


# ─────────────────────────────────────────────────────────────────────────────
# SLIPPAGE_SENSITIVITY 完整性
# ─────────────────────────────────────────────────────────────────────────────

class TestSlippageSensitivity:
    def test_all_required_tiers_present(self):
        required = {"equity", "etf", "commodity_futures", "index_futures"}
        assert required.issubset(set(SLIPPAGE_SENSITIVITY.keys()))

    def test_warn_strictly_less_than_fail(self):
        for tier, v in SLIPPAGE_SENSITIVITY.items():
            assert v["warn"] < v["fail"], f"{tier}: warn={v['warn']} must be < fail={v['fail']}"

    def test_all_values_positive(self):
        for tier, v in SLIPPAGE_SENSITIVITY.items():
            assert v["warn"] > 0
            assert v["fail"] > 0

    def test_equity_thresholds(self):
        assert SLIPPAGE_SENSITIVITY["equity"]["warn"] == 0.002
        assert SLIPPAGE_SENSITIVITY["equity"]["fail"] == 0.005

    def test_index_futures_thresholds(self):
        assert SLIPPAGE_SENSITIVITY["index_futures"]["warn"] == 0.001
        assert SLIPPAGE_SENSITIVITY["index_futures"]["fail"] == 0.003
