"""
风控配置加载器的单元测试
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from core.risk_config_loader import load_thresholds, load_risk_engine
from core.risk_engine import RiskEngine, RiskThresholds


class TestLoadThresholds:
    def test_from_dict_direct(self):
        t = load_thresholds({
            "concentration_limit": 0.25,
            "intraday_drawdown_halt": 0.04,
        })
        assert t.concentration_limit == 0.25
        assert t.intraday_drawdown_halt == 0.04
        assert t.hhi_limit == 0.18  # default untouched

    def test_from_dict_legacy_keys(self):
        t = load_thresholds({
            "max_position_ratio": 0.08,
            "stop_loss_ratio": 0.03,
            "max_total_exposure": 0.7,
        })
        assert t.concentration_limit == 0.08
        assert t.intraday_drawdown_warn == 0.03
        assert t.net_exposure_limit == 0.7

    def test_from_json_file(self, tmp_path):
        config = {
            "settings": {
                "risk": {
                    "concentration_limit": 0.20,
                    "var95_limit": 0.01,
                }
            }
        }
        f = tmp_path / "test_cfg.json"
        f.write_text(json.dumps(config), encoding="utf-8")
        t = load_thresholds(str(f))
        assert t.concentration_limit == 0.20
        assert t.var95_limit == 0.01

    def test_from_json_file_legacy_format(self, tmp_path):
        config = {
            "settings": {
                "risk": {
                    "max_position_ratio": 0.08,
                    "stop_loss_ratio": 0.03,
                    "max_total_exposure": 0.7,
                    "blacklist": ["ST*"],
                }
            }
        }
        f = tmp_path / "legacy_cfg.json"
        f.write_text(json.dumps(config), encoding="utf-8")
        t = load_thresholds(str(f))
        assert t.concentration_limit == 0.08
        assert t.net_exposure_limit == 0.7

    def test_missing_file_returns_defaults(self):
        t = load_thresholds("/nonexistent/path/xyz.json")
        assert t == RiskThresholds()

    def test_unknown_keys_ignored(self):
        t = load_thresholds({"foo_bar_baz": 999})
        assert t == RiskThresholds()


class TestLoadRiskEngine:
    def test_none_returns_default_engine(self):
        e = load_risk_engine(None)
        assert isinstance(e, RiskEngine)
        assert e.thresholds == RiskThresholds()

    def test_dict_creates_configured_engine(self):
        e = load_risk_engine({"concentration_limit": 0.15})
        assert isinstance(e, RiskEngine)
        assert e.thresholds.concentration_limit == 0.15

    def test_file_creates_configured_engine(self, tmp_path):
        config = {"risk": {"concentration_limit": 0.22}}
        f = tmp_path / "cfg.json"
        f.write_text(json.dumps(config), encoding="utf-8")
        e = load_risk_engine(str(f))
        assert e.thresholds.concentration_limit == 0.22


# ---------------------------------------------------------------------------
# R6: emergency_stop / stop_loss_ratio → HALT linkage
# ---------------------------------------------------------------------------

class TestR6EmergencyStop:
    """emergency_stop=True 将 intraday_drawdown_halt 设为 0.0，使引擎立即 HALT 任何交易。"""

    def test_emergency_stop_sets_halt_to_zero(self):
        t = load_thresholds({"emergency_stop": True})
        assert t.intraday_drawdown_halt == 0.0

    def test_emergency_stop_false_leaves_halt_unchanged(self):
        t = load_thresholds({"emergency_stop": False, "intraday_drawdown_halt": 0.05})
        assert t.intraday_drawdown_halt == 0.05

    def test_emergency_stop_engine_halts_all_trades(self):
        """emergency_stop=True 产生的引擎对所有交易返回 HALT。"""
        from core.risk_engine import RiskAction
        e = load_risk_engine({"emergency_stop": True})
        result = e.check_pre_trade(
            account_id="acc", code="000001.SZ", volume=100, price=10.0,
            direction="buy", positions={}, nav=100_000,
        )
        assert result.action == RiskAction.HALT

    def test_stop_loss_ratio_maps_to_halt_when_halt_absent(self):
        """stop_loss_ratio → intraday_drawdown_halt（当未明确配置 halt 阈值时）。"""
        t = load_thresholds({"stop_loss_ratio": 0.04})
        # legacy mapping: stop_loss_ratio → intraday_drawdown_warn
        assert t.intraday_drawdown_warn == 0.04
        # R6 linkage: also sets halt
        assert t.intraday_drawdown_halt == 0.04

    def test_explicit_halt_overrides_stop_loss_linkage(self):
        """显式 intraday_drawdown_halt 优先于 stop_loss_ratio 联动。"""
        t = load_thresholds({
            "stop_loss_ratio": 0.04,
            "intraday_drawdown_halt": 0.06,
        })
        assert t.intraday_drawdown_halt == 0.06  # explicit wins

    def test_emergency_stop_overrides_explicit_halt(self):
        """emergency_stop=True 覆盖显式 intraday_drawdown_halt 设置。"""
        t = load_thresholds({
            "emergency_stop": True,
            "intraday_drawdown_halt": 0.05,
        })
        # emergency_stop processes AFTER halt, so it wins
        assert t.intraday_drawdown_halt == 0.0

    def test_emergency_stop_from_json_file(self, tmp_path):
        """从 JSON 文件读取 emergency_stop=True 也能触发联动。"""
        config = {
            "settings": {
                "risk": {
                    "emergency_stop": True,
                    "concentration_limit": 0.25,
                }
            }
        }
        f = tmp_path / "emergency.json"
        f.write_text(json.dumps(config), encoding="utf-8")
        e = load_risk_engine(str(f))
        assert e.thresholds.intraday_drawdown_halt == 0.0
        assert e.thresholds.concentration_limit == 0.25
