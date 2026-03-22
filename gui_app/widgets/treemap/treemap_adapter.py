"""
TreemapAdapter — ECharts 行情热图适配器

通过本地 WebSocket JSON-RPC 2.0 与 treemap-bridge.js 通信，
模式与 KLineChartAdapter / NativeLwcChartAdapter 保持一致。

生命周期::

    adapter = TreemapAdapter()
    widget, ok = adapter.initialize(parent)   # 返回 (QWidget, is_native: bool)
    if ok:
        adapter.set_data(sectors)             # 推送数据
        adapter.set_filter("电子")            # 板块过滤
        adapter.on_symbol_click(callback)     # 注册股票点击回调
        adapter.stop()                        # 清理
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

log = logging.getLogger(__name__)

# RPC 方法常量 (Python → JS)
M_SET_DATA   = "treemap.setData"
M_SET_FILTER = "treemap.setFilter"
M_APPLY_THEME = "treemap.applyTheme"
M_RESIZE     = "treemap.resize"

# RPC 事件常量 (JS → Python)
E_READY      = "treemap.ready"
E_CLICK      = "treemap.click"
E_HOVER      = "treemap.hover"


class TreemapAdapter:
    """
    ECharts Treemap 图表适配器。

    若 QWebEngineView 或 WebSocket 连接失败，initialize() 返回
    (fallback_label, False)；调用者通过 is_native==False 识别降级。
    """

    def __init__(self) -> None:
        self._bridge: Any | None = None
        self._webview: Any | None = None
        self._initialized: bool = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def initialize(self, parent=None, timeout: float = 5.0):
        """
        启动 WS 服务 → 构建 HTML → 等待握手。

        Returns:
            (QWidget, is_native: bool)
        """
        from gui_app.widgets.chart.ws_bridge import WsBridge

        try:
            from PyQt5.QtCore import QUrl
            from PyQt5.QtWebEngineWidgets import QWebEngineView
        except ImportError:
            log.warning("TreemapAdapter: QWebEngineView 不可用，返回占位符")
            return self._make_fallback(parent), False

        self._bridge = WsBridge()
        port = self._bridge.start()
        log.info("TreemapAdapter: WS 服务启动在端口 %d", port)

        self._webview = QWebEngineView(parent)
        native_dir = self._get_native_dir()
        base_url = QUrl.fromLocalFile(str(native_dir) + "/")
        self._webview.setHtml(self._build_html(port), base_url)

        connected = self._bridge.wait_connect(timeout=timeout)
        if not connected:
            log.warning(
                "TreemapAdapter: WS 握手超时 (%.1fs)，降级到占位符", timeout
            )
            self._bridge.stop()
            self._bridge = None
            self._webview.deleteLater()
            self._webview = None
            return self._make_fallback(parent), False

        self._initialized = True
        log.info("TreemapAdapter: 热图连接成功")
        return self._webview, True

    def stop(self) -> None:
        """释放 WebSocket 服务端资源。"""
        if self._bridge is not None:
            try:
                self._bridge.stop()
            except Exception:
                pass
            self._bridge = None
        self._initialized = False

    # ── Data API ──────────────────────────────────────────────────────────────

    def set_data(self, sectors: list[dict]) -> None:
        """
        推送板块/股票树形数据。

        sectors 格式::

            [
              {
                "name": "电子",
                "children": [
                  { "symbol": "600703.SH", "name": "三安光电",
                    "value": 50000000000, "pct_change": 2.34 },
                  ...
                ]
              },
              ...
            ]
        """
        if not self._initialized or self._bridge is None:
            return
        self._bridge.notify(M_SET_DATA, {"sectors": sectors})

    def set_filter(self, sector: str = "") -> None:
        """
        筛选指定板块（空字符串 = 全行业）。
        """
        if not self._initialized or self._bridge is None:
            return
        self._bridge.notify(M_SET_FILTER, {"sector": sector})

    def apply_theme(self, theme: str = "dark") -> None:
        """应用主题（'dark' | 'light'）。"""
        if not self._initialized or self._bridge is None:
            return
        self._bridge.notify(M_APPLY_THEME, {"theme": theme})

    def trigger_resize(self) -> None:
        """通知 JS 端触发 ECharts resize（父容器尺寸变化后调用）。"""
        if not self._initialized or self._bridge is None:
            return
        self._bridge.notify(M_RESIZE, {})

    # ── Event API ─────────────────────────────────────────────────────────────

    def on_symbol_click(self, callback: Callable[[str, str, float], None]) -> None:
        """
        注册股票点击回调。

        callback(symbol: str, name: str, pct_change: float)
        """
        if self._bridge is None:
            return
        self._bridge.on(E_CLICK, lambda params: callback(
            params.get("symbol", ""),
            params.get("name", ""),
            params.get("pct_change", 0.0),
        ))

    def on_symbol_hover(self, callback: Callable[[str, str, float], None]) -> None:
        """
        注册股票悬停回调。

        callback(symbol: str, name: str, pct_change: float)
        """
        if self._bridge is None:
            return
        self._bridge.on(E_HOVER, lambda params: callback(
            params.get("symbol", ""),
            params.get("name", ""),
            params.get("pct_change", 0.0),
        ))

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _get_native_dir() -> Path:
        return Path(__file__).parent.parent.parent / "chart_native"

    def _build_html(self, port: int) -> str:
        """生成 ECharts 热图页面 HTML（路径相对于 chart_native/）。"""
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>EasyXT Treemap</title>
  <style>
    html, body {{
      margin: 0; padding: 0;
      width: 100%; height: 100%;
      background: #0c0d0f;
      overflow: hidden;
      font-family: 'Microsoft YaHei', sans-serif;
    }}
    #chart {{
      width: 100%;
      /* 底部留出图例条高度 */
      height: calc(100% - 32px);
    }}
    #treemap-legend {{
      display: flex;
      align-items: center;
      height: 28px;
      padding: 0 8px;
      gap: 6px;
      background: #141518;
      border-top: 1px solid #333;
    }}
    .legend-bar {{
      flex: 1;
      height: 12px;
      border-radius: 3px;
    }}
    .legend-label {{
      font-size: 11px;
      color: #999;
      white-space: nowrap;
    }}
  </style>
</head>
<body>
  <div id="chart"></div>
  <script src="./lib/echarts.min.js"></script>
  <script src="./treemap-bridge.js"></script>
  <script>
    TreemapBridge.init(document.getElementById('chart'), {port});
  </script>
</body>
</html>"""

    @staticmethod
    def _make_fallback(parent):
        """创建降级占位符 QLabel。"""
        try:
            from PyQt5.QtCore import Qt
            from PyQt5.QtWidgets import QLabel
        except ImportError:
            return None
        lbl = QLabel("行情热图不可用（QWebEngineView 未安装）", parent)
        lbl.setAlignment(Qt.AlignCenter)
        lbl.setStyleSheet("color: #666; font-size: 14px; background: #0c0d0f;")
        return lbl
