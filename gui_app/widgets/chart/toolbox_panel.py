from __future__ import annotations

import importlib
import json
import logging
import os
import sys
from pathlib import Path


class _SymbolHolder:
    def __init__(self, value: str):
        self.value = value


def _native_drawings_dir() -> Path:
    """返回原生画线 JSON 文件的存储目录（与旧 toolbox 目录并列）。"""
    base = Path(os.environ.get("EASYXT_DRAWINGS_DIR", str(Path.home() / ".easyxt" / "drawings")))
    base.mkdir(parents=True, exist_ok=True)
    return base


def _safe_symbol(symbol: str) -> str:
    """把标的代码转为合法文件名（去掉 . 等特殊字符）。"""
    return symbol.replace(".", "_").replace("/", "_")


class ToolboxPanel:
    """
    画线工具栏适配层。

    legacy 路径（默认）: 封装 lwc-python fork 的 ToolBox，持久化由 fork 自身管理。
    native 路径: 当传入 native_adapter 时，接管 get_drawings / load_drawings，
                 以 JSON 文件（~/.easyxt/drawings/<symbol>.json）做持久化。
    """

    def __init__(self, chart, symbol: str = "", native_adapter=None):
        self.chart = chart
        self._logger = logging.getLogger(__name__)
        self._native = native_adapter  # NativeLwcChartAdapter | None
        self._current_symbol = symbol

        if self._native is not None:
            # ── native 路径：跳过旧 fork ToolBox ──────────────────────────
            self.toolbox = None
            # 若初始标的非空，立即加载持久化画线
            if symbol:
                self._native_load(symbol)
        else:
            # ── legacy 路径：旧 lwc-python fork ToolBox ───────────────────
            self._toolbox_cls = self._load_toolbox()
            self.toolbox = getattr(chart, "toolbox", None) or self._toolbox_cls(chart)
            self._symbol_holder = _SymbolHolder(symbol)
            try:
                self.toolbox.save_drawings_under(self._symbol_holder)
            except Exception:
                self._logger.exception("Failed to initialize toolbox drawings store")

    # ── native 持久化辅助 ──────────────────────────────────────────────────

    def _native_save(self, symbol: str) -> None:
        """将当前画线同步写入 JSON 文件。"""
        if not symbol or self._native is None:
            return
        try:
            drawings = self._native.get_drawings(timeout=3.0)
            path = _native_drawings_dir() / f"{_safe_symbol(symbol)}.json"
            path.write_text(json.dumps(drawings, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            self._logger.exception("ToolboxPanel: failed to save native drawings for %s", symbol)

    def _native_load(self, symbol: str) -> None:
        """从 JSON 文件恢复画线到图表。"""
        if not symbol or self._native is None:
            return
        try:
            path = _native_drawings_dir() / f"{_safe_symbol(symbol)}.json"
            drawings: list = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
            self._native.load_drawings(drawings)
        except Exception:
            self._logger.exception("ToolboxPanel: failed to load native drawings for %s", symbol)

    # ── 公开 API ───────────────────────────────────────────────────────────

    def _load_toolbox(self):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        external_lwc_path = os.path.join(project_root, "external", "lightweight-charts-python")
        if external_lwc_path not in sys.path:
            sys.path.insert(0, external_lwc_path)
        module = importlib.import_module("lightweight_charts.toolbox")
        return getattr(module, "ToolBox")

    def set_symbol(self, symbol: str) -> None:
        if not symbol:
            return
        if self._native is not None:
            # 先保存旧标的画线，再加载新标的画线
            if self._current_symbol and self._current_symbol != symbol:
                self._native_save(self._current_symbol)
            self._current_symbol = symbol
            self._native_load(symbol)
        else:
            self._current_symbol = symbol
            self._symbol_holder.value = symbol
            try:
                self.toolbox.load_drawings(symbol)
            except Exception:
                self._logger.exception("Failed to load toolbox drawings")

    def save_current(self) -> None:
        """主动保存当前标的画线（native 路径；legacy 路径由 fork 自动管理）。"""
        if self._native is not None and self._current_symbol:
            self._native_save(self._current_symbol)
