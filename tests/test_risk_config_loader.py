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
