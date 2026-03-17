"""
tests/test_backend_config.py

ChartBackendConfig 完整行为覆盖测试。
所有测试均不依赖真实文件系统或 unified_config.json。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# 每个测试前重置单例，避免跨测试污染
import gui_app.widgets.chart.backend_config as bc_mod


@pytest.fixture(autouse=True)
def reset_singleton():
    """每个测试前后复位单例 + 清理环境变量。"""
    bc_mod._INSTANCE = None
    os.environ.pop("EASYXT_CHART_BACKEND", None)
    yield
    bc_mod._INSTANCE = None
    os.environ.pop("EASYXT_CHART_BACKEND", None)


def _make_cfg(**kwargs):
    """辅助：构造 ChartBackendConfig，默认最简配置。"""
    defaults = {
        "default_backend": "lwc_python",
        "native_lwc_whitelist": {"accounts": [], "strategies": []},
        "freeze_during_trading": False,
        "ws_handshake_timeout_s": 5.0,
    }
    defaults.update(kwargs)
    return bc_mod.ChartBackendConfig(defaults)


# ─────────────────────────────────────────────────────────────────────────────
# __init__ 及属性初始化
# ─────────────────────────────────────────────────────────────────────────────

class TestInit:
    def test_default_backend_set(self):
        cfg = _make_cfg(default_backend="lwc_python")
        assert cfg._default == "lwc_python"

    def test_whitelist_accounts_normalized(self):
        cfg = _make_cfg(native_lwc_whitelist={"accounts": ["  DEMO_001  "], "strategies": []})
        assert "demo_001" in cfg._wl_accounts

    def test_whitelist_strategies_normalized(self):
        cfg = _make_cfg(native_lwc_whitelist={"accounts": [], "strategies": ["Grid_V2"]})
        assert "grid_v2" in cfg._wl_strategies

    def test_freeze_trading_default_true(self):
        cfg = bc_mod.ChartBackendConfig({})
        assert cfg._freeze_trading is True

    def test_ws_timeout_default(self):
        cfg = bc_mod.ChartBackendConfig({})
        assert cfg.ws_handshake_timeout_s == 5.0

    def test_empty_whitelist_ok(self):
        cfg = _make_cfg()
        assert len(cfg._wl_accounts) == 0
        assert len(cfg._wl_strategies) == 0

    def test_none_whitelist_ok(self):
        cfg = bc_mod.ChartBackendConfig({"native_lwc_whitelist": None})
        assert len(cfg._wl_accounts) == 0


# ─────────────────────────────────────────────────────────────────────────────
# get_backend 优先级逻辑
# ─────────────────────────────────────────────────────────────────────────────

class TestGetBackend:
    def test_returns_default_when_no_override(self):
        cfg = _make_cfg(default_backend="lwc_python")
        assert cfg.get_backend() == "lwc_python"

    def test_env_override_lwc_python(self):
        cfg = _make_cfg(default_backend="native_lwc")
        os.environ["EASYXT_CHART_BACKEND"] = "lwc_python"
        assert cfg.get_backend() == "lwc_python"

    def test_env_override_native_lwc(self):
        cfg = _make_cfg(default_backend="lwc_python")
        os.environ["EASYXT_CHART_BACKEND"] = "native_lwc"
        assert cfg.get_backend() == "native_lwc"

    def test_env_override_invalid_ignored(self):
        cfg = _make_cfg(default_backend="lwc_python")
        os.environ["EASYXT_CHART_BACKEND"] = "bogus_backend"
        # 无效值不命中 → 走后续逻辑
        assert cfg.get_backend() == "lwc_python"

    def test_account_whitelist_returns_native(self):
        cfg = _make_cfg(
            native_lwc_whitelist={"accounts": ["demo_001"], "strategies": []},
            default_backend="lwc_python",
        )
        assert cfg.get_backend(account_id="demo_001") == "native_lwc"

    def test_account_whitelist_case_insensitive(self):
        cfg = _make_cfg(
            native_lwc_whitelist={"accounts": ["DEMO_001"], "strategies": []},
        )
        assert cfg.get_backend(account_id="demo_001") == "native_lwc"

    def test_strategy_whitelist_returns_native(self):
        cfg = _make_cfg(
            native_lwc_whitelist={"accounts": [], "strategies": ["grid_v2"]},
            default_backend="lwc_python",
        )
        assert cfg.get_backend(strategy_id="grid_v2") == "native_lwc"

    def test_strategy_whitelist_case_insensitive(self):
        cfg = _make_cfg(
            native_lwc_whitelist={"accounts": [], "strategies": ["Grid_V2"]},
        )
        assert cfg.get_backend(strategy_id="GRID_V2") == "native_lwc"

    def test_env_override_takes_priority_over_whitelist(self):
        cfg = _make_cfg(
            native_lwc_whitelist={"accounts": ["demo_001"], "strategies": []},
        )
        os.environ["EASYXT_CHART_BACKEND"] = "lwc_python"
        # env 优先于白名单
        assert cfg.get_backend(account_id="demo_001") == "lwc_python"

    def test_unknown_account_returns_default(self):
        cfg = _make_cfg(
            native_lwc_whitelist={"accounts": ["demo_001"], "strategies": []},
            default_backend="lwc_python",
        )
        assert cfg.get_backend(account_id="other") == "lwc_python"


# ─────────────────────────────────────────────────────────────────────────────
# can_switch_now
# ─────────────────────────────────────────────────────────────────────────────

class TestCanSwitchNow:
    def test_no_freeze_always_ok(self):
        cfg = _make_cfg(freeze_during_trading=False)
        ok, reason = cfg.can_switch_now()
        assert ok is True
        assert reason == ""

    def test_freeze_outside_session_ok(self):
        cfg = _make_cfg(freeze_during_trading=True)
        mock_guard = MagicMock()
        mock_guard.current_session.return_value = (False, "")
        with patch.dict("sys.modules", {
            "gui_app.widgets.chart.trading_hours_guard": MagicMock(TradingHoursGuard=mock_guard)
        }):
            # 直接 patch 模块路径
            with patch("gui_app.widgets.chart.backend_config.ChartBackendConfig.can_switch_now",
                       wraps=cfg.can_switch_now):
                # 无法直接 patch relative import，改为 patch trading_hours_guard
                pass
        # 直接 mock 内部 import
        with patch("gui_app.widgets.chart.backend_config.__builtins__"):
            pass
        # 使用 sys.modules patch
        fake_guard_cls = MagicMock()
        fake_guard_cls.current_session.return_value = (False, "")
        import sys
        mod_mock = MagicMock()
        mod_mock.TradingHoursGuard = fake_guard_cls
        with patch.dict(sys.modules, {"gui_app.widgets.chart.trading_hours_guard": mod_mock}):
            ok, reason = cfg.can_switch_now()
        assert ok is True
        assert reason == ""

    def test_freeze_inside_session_blocked(self):
        cfg = _make_cfg(freeze_during_trading=True)
        fake_guard_cls = MagicMock()
        fake_guard_cls.current_session.return_value = (True, "上午盘")
        import sys
        mod_mock = MagicMock()
        mod_mock.TradingHoursGuard = fake_guard_cls
        with patch.dict(sys.modules, {"gui_app.widgets.chart.trading_hours_guard": mod_mock}):
            ok, reason = cfg.can_switch_now()
        assert ok is False
        assert "上午盘" in reason


# ─────────────────────────────────────────────────────────────────────────────
# 白名单动态增删
# ─────────────────────────────────────────────────────────────────────────────

class TestWhitelistMutation:
    def test_add_account_to_whitelist(self):
        cfg = _make_cfg()
        cfg.add_account_to_whitelist("new_account")
        assert "new_account" in cfg._wl_accounts

    def test_add_account_normalized(self):
        cfg = _make_cfg()
        cfg.add_account_to_whitelist("  New_ACC  ")
        assert "new_acc" in cfg._wl_accounts

    def test_remove_account_from_whitelist(self):
        cfg = _make_cfg(native_lwc_whitelist={"accounts": ["demo"], "strategies": []})
        cfg.remove_account_from_whitelist("demo")
        assert "demo" not in cfg._wl_accounts

    def test_remove_nonexistent_account_no_error(self):
        cfg = _make_cfg()
        cfg.remove_account_from_whitelist("nonexistent")  # should not raise


# ─────────────────────────────────────────────────────────────────────────────
# to_dict
# ─────────────────────────────────────────────────────────────────────────────

class TestToDict:
    def test_to_dict_keys(self):
        cfg = _make_cfg()
        d = cfg.to_dict()
        assert "default_backend" in d
        assert "wl_accounts_count" in d
        assert "wl_strategies_count" in d
        assert "freeze_during_trading" in d
        assert "ws_handshake_timeout_s" in d
        assert "env_override" in d

    def test_to_dict_counts(self):
        cfg = _make_cfg(native_lwc_whitelist={"accounts": ["a", "b"], "strategies": ["s1"]})
        d = cfg.to_dict()
        assert d["wl_accounts_count"] == 2
        assert d["wl_strategies_count"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# _load_from_file
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadFromFile:
    def test_load_missing_file_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(bc_mod, "_CONFIG_PATH", tmp_path / "nonexistent.json")
        result = bc_mod._load_from_file()
        assert result == {}

    def test_load_valid_config(self, tmp_path, monkeypatch):
        cfg_file = tmp_path / "unified_config.json"
        cfg_file.write_text(json.dumps({
            "chart": {"engine": {"default_backend": "native_lwc"}}
        }), encoding="utf-8")
        monkeypatch.setattr(bc_mod, "_CONFIG_PATH", cfg_file)
        result = bc_mod._load_from_file()
        assert result["default_backend"] == "native_lwc"

    def test_load_invalid_json_returns_empty(self, tmp_path, monkeypatch):
        cfg_file = tmp_path / "unified_config.json"
        cfg_file.write_text("{invalid json}", encoding="utf-8")
        monkeypatch.setattr(bc_mod, "_CONFIG_PATH", cfg_file)
        result = bc_mod._load_from_file()
        assert result == {}

    def test_load_missing_chart_key_returns_empty(self, tmp_path, monkeypatch):
        cfg_file = tmp_path / "unified_config.json"
        cfg_file.write_text(json.dumps({"other": {}}), encoding="utf-8")
        monkeypatch.setattr(bc_mod, "_CONFIG_PATH", cfg_file)
        result = bc_mod._load_from_file()
        assert result == {}


# ─────────────────────────────────────────────────────────────────────────────
# 单例 + 热重载
# ─────────────────────────────────────────────────────────────────────────────

class TestSingleton:
    def test_get_returns_same_instance(self):
        with patch.object(bc_mod, "_load_from_file", return_value={}):
            a = bc_mod.get_chart_backend_config()
            b = bc_mod.get_chart_backend_config()
        assert a is b

    def test_reload_returns_new_instance(self):
        with patch.object(bc_mod, "_load_from_file", return_value={}):
            a = bc_mod.get_chart_backend_config()
            c = bc_mod.reload_chart_backend_config()
        assert a is not c

    def test_singleton_thread_safety(self):
        """并发初始化不能产生两个不同实例。"""
        instances = []
        with patch.object(bc_mod, "_load_from_file", return_value={}):
            def worker():
                instances.append(bc_mod.get_chart_backend_config())
            threads = [threading.Thread(target=worker) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        assert len(set(id(i) for i in instances)) == 1

    def test_inner_double_check_returns_existing_instance(self):
        """Trigger double-checked locking inner branch (line 177): _INSTANCE set by another
        thread between outer check and lock acquisition."""
        import gui_app.widgets.chart.backend_config as bc_mod2
        # Use a custom lock that sets _INSTANCE when __enter__ is called,
        # simulating a race where another thread initialised while we waited.
        pre_created = bc_mod2.ChartBackendConfig({})

        class _RaceLock:
            def __enter__(self):
                bc_mod2._INSTANCE = pre_created  # another thread already initialised
                return self
            def __exit__(self, *_):
                pass

        original_instance = bc_mod2._INSTANCE
        original_lock = bc_mod2._LOCK
        try:
            bc_mod2._INSTANCE = None
            bc_mod2._LOCK = _RaceLock()
            result = bc_mod2.get_chart_backend_config()
        finally:
            bc_mod2._INSTANCE = original_instance
            bc_mod2._LOCK = original_lock

        assert result is pre_created  # inner branch returned the pre-created instance
