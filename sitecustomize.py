from __future__ import annotations

import importlib.util
import os

_TRUTHY = {"1", "true", "yes", "on"}


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name, "")).strip().lower() in _TRUTHY


def _has_qt_binding() -> bool:
    for module_name in ("PyQt5", "PyQt6", "PySide6"):
        try:
            if importlib.util.find_spec(module_name) is not None:
                return True
        except (ImportError, ModuleNotFoundError, ValueError):
            continue
    return False


def _disable_pytest_qt_when_qt_missing() -> None:
    """在无 Qt 绑定的环境里，自动禁用 pytest-qt 插件。

    目的：允许 uv/.venv 这类“先跑非 GUI 测试、暂未安装 Qt”的环境正常进入 pytest，
    而不是在插件自动加载阶段直接失败。
    """
    if _env_truthy("EASYXT_FORCE_PYTEST_QT_PLUGIN"):
        return
    if _has_qt_binding():
        return

    current = str(os.environ.get("PYTEST_ADDOPTS", "") or "")
    os.environ.setdefault("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    required_plugins = (
        "-p anyio.pytest_plugin",
        "-p pytest_asyncio.plugin",
    )
    missing = [option for option in required_plugins if option not in current]
    if missing:
        current = f"{current} {' '.join(missing)}".strip()
        os.environ["PYTEST_ADDOPTS"] = current
    os.environ.setdefault("EASYXT_PYTEST_QT_PLUGIN_DISABLED", "1")


_disable_pytest_qt_when_qt_missing()
