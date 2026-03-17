from pathlib import Path
from typing import Optional

from PyQt5.QtWidgets import QApplication

from .colors import COLORS

_BASE_DIR = Path(__file__).resolve().parent
_THEME_FILES = {
    "dark": _BASE_DIR / "dark.qss",
    "light": _BASE_DIR / "light.qss",
}


def load_stylesheet(theme: str) -> str:
    path = _THEME_FILES.get(theme)
    if not path or not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def apply_theme(app: QApplication, theme: Optional[str] = None) -> str:
    selected = theme or "dark"
    stylesheet = load_stylesheet(selected)
    if stylesheet:
        app.setStyleSheet(stylesheet)
    return selected


def get_color(theme: str, key: str) -> str:
    return COLORS.get(theme, COLORS["dark"]).get(key, "")
