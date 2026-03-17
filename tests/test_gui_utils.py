"""
tests/test_gui_utils.py
~~~~~~~~~~~~~~~~~~~~~~~
Covers pure-logic modules that need no QApplication:
  - gui_app.widgets.chart_workspace.chart_datafeed  (9 stmts)
  - gui_app.theme.colors                            (1 stmt)
  - gui_app.theme                                   (19 stmts)
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from gui_app.widgets.chart_workspace.chart_datafeed import ChartDatafeed
from gui_app.theme.colors import COLORS
from gui_app.theme import apply_theme, get_color, load_stylesheet


# ===========================================================================
# ChartDatafeed
# ===========================================================================

class TestChartDatafeed:
    def test_initial_last_data_is_none(self):
        feed = ChartDatafeed()
        assert feed.last_data() is None

    def test_set_data_stores_dataframe(self):
        feed = ChartDatafeed()
        df = pd.DataFrame({"close": [10.0, 11.0]})
        feed.set_data(df)
        assert feed.last_data() is df

    def test_set_data_replaces_previous(self):
        feed = ChartDatafeed()
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"a": [2]})
        feed.set_data(df1)
        feed.set_data(df2)
        assert feed.last_data() is df2


# ===========================================================================
# theme.colors — importing already covers the 1-stmt module
# ===========================================================================

class TestThemeColors:
    def test_colors_has_dark_and_light(self):
        assert "dark" in COLORS
        assert "light" in COLORS

    def test_dark_has_background(self):
        assert "background" in COLORS["dark"]


# ===========================================================================
# gui_app.theme — load_stylesheet / get_color / apply_theme
# ===========================================================================

class TestLoadStylesheet:
    def test_unknown_theme_returns_empty_string(self):
        """not path branch (line ~9-10)."""
        result = load_stylesheet("nonexistent_theme_xyz")
        assert result == ""

    def test_dark_theme_returns_nonempty_string(self):
        """path exists branch — reads dark.qss (line ~11)."""
        result = load_stylesheet("dark")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_light_theme_returns_nonempty_string(self):
        result = load_stylesheet("light")
        assert isinstance(result, str)
        assert len(result) > 0


class TestGetColor:
    def test_dark_background_returns_correct_hex(self):
        color = get_color("dark", "background")
        assert color == COLORS["dark"]["background"]

    def test_light_accent_returns_correct_hex(self):
        color = get_color("light", "accent")
        assert color == COLORS["light"]["accent"]

    def test_unknown_theme_falls_back_to_dark(self):
        """falls back to COLORS["dark"] for unknown theme."""
        color = get_color("neon", "background")
        assert color == COLORS["dark"]["background"]

    def test_missing_key_returns_empty_string(self):
        color = get_color("dark", "nonexistent_key_xyz")
        assert color == ""


class TestApplyTheme:
    def test_apply_theme_with_dark_sets_stylesheet(self):
        """stylesheet is non-empty → app.setStyleSheet called (line ~16)."""
        app = MagicMock()
        selected = apply_theme(app, theme="dark")
        assert selected == "dark"
        app.setStyleSheet.assert_called_once()

    def test_apply_theme_default_uses_dark(self):
        """theme=None → selected='dark'."""
        app = MagicMock()
        selected = apply_theme(app)
        assert selected == "dark"

    def test_apply_theme_unknown_theme_no_setStyleSheet(self):
        """load_stylesheet returns '' → if stylesheet: is False → no setStyleSheet (line ~15 false branch)."""
        app = MagicMock()
        apply_theme(app, theme="unknown_xyz")
        app.setStyleSheet.assert_not_called()

    def test_apply_theme_returns_selected_name(self):
        app = MagicMock()
        result = apply_theme(app, theme="light")
        assert result == "light"
