import json
import logging
import os
from typing import Dict, Optional

from PyQt5.QtWidgets import QApplication


logger = logging.getLogger(__name__)


class ThemeManager:
    def __init__(self, config_path: Optional[str] = None) -> None:
        self._themes: Dict[str, str] = {
            "light": """
        QMainWindow {
            background-color: #f0f0f0;
        }
        QTabWidget::pane {
            border: 1px solid #c0c0c0;
            background-color: white;
        }
        QTabBar::tab {
            background-color: #e0e0e0;
            padding: 8px 16px;
            margin-right: 2px;
            border-top-left-radius: 5px;
            border-top-right-radius: 5px;
        }
        QTabBar::tab:selected {
            background-color: white;
            border-bottom: 2px solid #2196F3;
        }
        QGroupBox {
            font-weight: bold;
            border: 2px solid #cccccc;
            border-radius: 5px;
            margin-top: 1ex;
            padding-top: 10px;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 5px 0 5px;
        }
        QPushButton {
            padding: 6px 12px;
            border-radius: 4px;
            border: 1px solid #ccc;
            background-color: #f0f0f0;
        }
        QPushButton:hover {
            background-color: #e0e0e0;
        }
        QPushButton:pressed {
            background-color: #d0d0d0;
        }
    """,
            "dark": """
        QMainWindow {
            background-color: #0f172a;
        }
        QTabWidget::pane {
            border: 1px solid #334155;
            background-color: #0f172a;
        }
        QTabBar::tab {
            background-color: #1e293b;
            color: #cbd5e1;
            padding: 8px 16px;
            margin-right: 2px;
            border-top-left-radius: 5px;
            border-top-right-radius: 5px;
        }
        QTabBar::tab:selected {
            background-color: #334155;
            border-bottom: 2px solid #3b82f6;
        }
        QGroupBox {
            font-weight: bold;
            background-color: #1e293b;
            border: 1px solid #334155;
            border-radius: 5px;
            margin-top: 1ex;
            padding-top: 10px;
            color: #e2e8f0;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 5px 0 5px;
        }
        QPushButton {
            padding: 6px 12px;
            border-radius: 4px;
            border: 1px solid #475569;
            background-color: #3b82f6;
            color: white;
        }
        QPushButton:hover {
            background-color: #2563eb;
        }
        QPushButton:pressed {
            background-color: #1d4ed8;
        }
    """
        }
        self._current = "light"
        if config_path:
            self._load_config(config_path)

    def _load_config(self, config_path: str) -> None:
        if not os.path.exists(config_path):
            return
        try:
            with open(config_path, "r", encoding="utf-8") as file:
                config = json.load(file)
            theme = config.get("ui", {}).get("theme")
            if theme in self._themes:
                self._current = theme
        except Exception:
            logger.exception("Failed to load theme config")

    def apply(self, app: QApplication, theme: Optional[str] = None) -> None:
        if theme is not None:
            self._current = theme
        stylesheet = self._themes.get(self._current, "")
        app.setStyleSheet(stylesheet)

    def current(self) -> str:
        return self._current

    def toggle_theme(self) -> str:
        self._current = "dark" if self._current == "light" else "light"
        return self._current
