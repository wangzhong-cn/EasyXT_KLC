import os
import sys
import json

from PyQt5.QtWidgets import QApplication

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.theme_manager import ThemeManager


def get_app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_theme_manager_toggle_and_apply():
    app = get_app()
    manager = ThemeManager()

    assert manager.current() == "light"
    manager.apply(app)
    assert app.styleSheet() == manager._themes["light"]

    manager.toggle_theme()
    manager.apply(app)
    assert manager.current() == "dark"
    assert app.styleSheet() == manager._themes["dark"]


class TestThemeManagerConfig:
    def test_config_path_nonexistent_returns_early(self):
        """不存在的 config_path → _load_config 早返回 (lines 109, 112-113)."""
        manager = ThemeManager(config_path="/nonexistent/path.json")
        assert manager.current() == "light"

    def test_config_path_valid_sets_dark_theme(self, tmp_path):
        """有效 JSON config 包含 dark 主题 → _current 更新 (lines 115-119)."""
        config = tmp_path / "config.json"
        config.write_text(json.dumps({"ui": {"theme": "dark"}}), encoding="utf-8")
        manager = ThemeManager(config_path=str(config))
        assert manager.current() == "dark"

    def test_config_path_valid_unknown_theme_ignored(self, tmp_path):
        """JSON 中主题名不在已知集合 → _current 保持默认 (line 118 false branch)."""
        config = tmp_path / "config.json"
        config.write_text(json.dumps({"ui": {"theme": "neon"}}), encoding="utf-8")
        manager = ThemeManager(config_path=str(config))
        assert manager.current() == "light"

    def test_config_path_invalid_json_swallows_exception(self, tmp_path):
        """损坏的 JSON → except 块不抛出 (lines 120-121)."""
        config = tmp_path / "config.json"
        config.write_text("not json!!!", encoding="utf-8")
        manager = ThemeManager(config_path=str(config))
        assert manager.current() == "light"

    def test_apply_with_explicit_theme_argument(self):
        """apply(theme='dark') 更新 _current (line 125)."""
        app = get_app()
        manager = ThemeManager()
        manager.apply(app, theme="dark")
        assert manager.current() == "dark"
