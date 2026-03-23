"""
chart_adapter.py — 图表适配器抽象层 (Stage 1 + Stage 2.1 + Stage 2.2)

ChartAdapter          Protocol 接口定义
LwcPythonChartAdapter 封装当前 lightweight-charts-python fork（当前主链路）
NativeLwcChartAdapter WebSocket JSON-RPC 原生通道（Stage 2.2 画线 API 已实装）
create_chart_adapter  工厂函数；读取环境变量 EASYXT_CHART_BACKEND 选择后端

后端选择（优先级）：
  1. create_chart_adapter(backend="native_lwc") 显式参数
  2. os.environ["EASYXT_CHART_BACKEND"] = "native_lwc"
  3. 默认 "lwc_python"

NativeLwcChartAdapter 生命周期::

    adapter = NativeLwcChartAdapter()
    widget, is_native = adapter.initialize(parent_widget, timeout=5.0)
    # is_native=False → adapter 已自动降级到 lwc_python fallback
    layout.addWidget(widget)
    ...
    adapter.destroy()   # 退出时调用
"""
from __future__ import annotations

import logging
import os
import shutil
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

import pandas as pd

log = logging.getLogger(__name__)


# ── Protocol ──────────────────────────────────────────────────────────────────

class ChartAdapter(Protocol):
    def set_data(self, data: pd.DataFrame) -> None: ...
    def update_data(self, row: pd.Series) -> None: ...
    def marker(self, text: str) -> None: ...


# ── lwc_python adapter（当前主链路封装）────────────────────────────────────────

class LwcPythonChartAdapter:
    """封装 lightweight-charts-python fork 的 QtChart 对象。"""

    def __init__(self, chart) -> None:
        self._chart = chart

    def set_data(self, data: pd.DataFrame) -> None:
        self._chart.set(data)

    def update_data(self, row: pd.Series) -> None:
        try:
            self._chart.update(row)
        except ZeroDivisionError:
            chart_interval = getattr(self._chart, "_interval", None)
            log.warning("LwcPythonChartAdapter.update_data skipped due to invalid chart interval: %s", chart_interval)
        except TypeError:
            if getattr(self._chart, "_last_bar", "__missing__") is None:
                log.warning("LwcPythonChartAdapter.update_data recovered from empty last_bar")
                self._chart.set(pd.DataFrame([row]))
                return
            raise

    def marker(self, text: str) -> None:
        self._chart.marker(text=text)

    def destroy(self) -> None:
        """无需额外清理（由 QtChart 自身管理）。"""


# ── native_lwc adapter（Stage 2.1 最小可运行骨架）────────────────────────────

class NativeLwcChartAdapter:
    """
    原生 lightweight-charts v5.x 适配器，通过本地 WebSocket JSON-RPC 2.0
    与 gui_app/chart_native/chart-bridge.js 通信。

    若 WebSocket 握手超时（默认 5s），自动降级到 LwcPythonChartAdapter。
    """

    #: 项目内静态路径（单次解析）
    _LWC_JS_SRC: Path | None = None

    def __init__(self) -> None:
        self._bridge = None           # WsBridge 实例
        self._webview = None          # QWebEngineView
        self._initialized: bool = False
        self._fallback: LwcPythonChartAdapter | None = None
        self._rpc_seq: int = 0

    def _notify_v1(
        self,
        method: str,
        payload: dict,
        *,
        chart_id: str = "main",
        pane_id: str | None = None,
        source: str = "python",
    ) -> None:
        if not self._bridge:
            return
        self._rpc_seq += 1
        envelope: dict[str, object] = {
            "v": 1,
            "type": "chart.rpc",
            "chart_id": chart_id or "main",
            "method": method,
            "payload": payload or {},
            "seq": self._rpc_seq,
            "ts_ms": int(time.time() * 1000),
            "source": source,
        }
        if pane_id:
            envelope["pane_id"] = pane_id
        self._bridge.notify("chart.rpc", envelope)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def initialize(self, parent=None, timeout: float = 5.0):
        """
        启动 WS 服务→加载原生 HTML→等待握手。

        Returns:
            (QWidget, is_native: bool)
            is_native=False 时 widget 由 fallback lwc_python 提供，
            且 set_data / update_data / marker 均路由到 fallback。
        """
        from .ws_bridge import WsBridge

        try:
            from PyQt5.QtCore import QUrl
            from PyQt5.QtWebEngineWidgets import QWebEngineView
        except ImportError:
            log.warning("NativeLwcChartAdapter: QWebEngineView 不可用，fallback")
            return self._make_fallback(parent), False

        # 1. 启动 WebSocket 服务
        self._bridge = WsBridge()
        port = self._bridge.start()
        log.info("NativeLwcChartAdapter: WS 服务启动在端口 %d", port)

        # 2. 确保 lib/lightweight-charts.js 已就绪
        self._ensure_lwc_lib()

        # 3. 创建 WebEngineView + 注入动态 HTML
        self._webview = QWebEngineView(parent)
        native_dir = self._get_native_dir()
        html = self._build_html(port)
        base_url = QUrl.fromLocalFile(str(native_dir) + "/")
        self._webview.setHtml(html, base_url)

        # 4. 等待 JS chart.ready 握手（通过 WsBridge.wait_connect）
        #    注意：不能用 threading.Event.wait() 阻塞主线程，
        #    因为 QWebEngineView 需要事件循环来加载页面和运行 JS。
        #    改用 QEventLoop + QTimer 确保 Qt 事件循环持续运行。
        from PyQt5.QtCore import QEventLoop, QTimer
        connected = False
        _loop = QEventLoop()

        def _check():
            nonlocal connected
            if self._bridge and self._bridge.is_connected:
                connected = True
                _loop.quit()

        _poll = QTimer()
        _poll.timeout.connect(_check)
        _poll.start(50)  # 每 50ms 检查一次

        _timeout = QTimer()
        _timeout.setSingleShot(True)
        _timeout.timeout.connect(_loop.quit)
        _timeout.start(int(timeout * 1000))

        _loop.exec_()
        _poll.stop()
        _timeout.stop()
        if not connected:
            log.warning(
                "NativeLwcChartAdapter: WS 握手超时 (%.1fs)，自动降级 lwc_python",
                timeout,
            )
            self._bridge.stop()
            self._bridge = None
            self._webview.deleteLater()
            self._webview = None
            return self._make_fallback(parent), False

        self._initialized = True
        log.info("NativeLwcChartAdapter: 原生图表连接成功")
        return self._webview, True

    def destroy(self) -> None:
        """释放 WebSocket 服务和 WebView。应在 widget 销毁前调用。"""
        if self._bridge:
            self._bridge.stop()
            self._bridge = None
        if self._webview:
            self._webview.deleteLater()
            self._webview = None
        self._initialized = False

    # ── ChartAdapter interface ────────────────────────────────────────────────

    def set_data(self, data: pd.DataFrame) -> None:
        if not self._initialized or not self._bridge:
            if self._fallback:
                self._fallback.set_data(data)
            return
        from . import rpc_protocol as rpc
        try:
            self._bridge.notify(rpc.M_SET_DATA, rpc.build_set_data(data))
        except Exception:
            log.warning("NativeLwcChartAdapter.set_data: notify failed")
            if self._fallback:
                self._fallback.set_data(data)

    def update_data(self, row: pd.Series) -> None:
        if not self._initialized or not self._bridge:
            if self._fallback:
                self._fallback.update_data(row)
            return
        from . import rpc_protocol as rpc
        try:
            self._bridge.notify(rpc.M_UPDATE_BAR, rpc.build_update_bar(row))
        except Exception:
            log.warning("NativeLwcChartAdapter.update_data: notify failed")
            if self._fallback:
                self._fallback.update_data(row)

    def marker(self, text: str) -> None:
        if not self._initialized or not self._bridge:
            if self._fallback:
                self._fallback.marker(text)
            return
        from . import rpc_protocol as rpc
        try:
            self._bridge.notify(rpc.M_SET_MARKERS, rpc.build_set_markers(
                [{"time": None, "position": "aboveBar", "color": "#f68410",
                  "shape": "circle", "text": text}]
            ))
        except Exception:
            log.warning("NativeLwcChartAdapter.marker: notify failed")
            if self._fallback:
                self._fallback.marker(text)

    def create_indicator(
        self,
        name: str,
        *,
        is_stack: bool = False,
        pane_id: str | None = None,
        height: int | None = None,
        calc_params: list[int] | None = None,
        short_name: str | None = None,
    ) -> None:
        if not self._initialized or not self._bridge:
            return
        from . import rpc_protocol as rpc
        params: dict[str, object] = {"name": name, "isStack": bool(is_stack)}
        if pane_id:
            pane_options: dict[str, object] = {"id": pane_id}
            if height is not None:
                pane_options["height"] = int(height)
            params["paneOptions"] = pane_options
        if calc_params:
            params["calcParams"] = [int(x) for x in calc_params]
        if short_name:
            params["shortName"] = short_name
        try:
            self._bridge.notify(rpc.M_CREATE_INDICATOR, params)
        except Exception:
            log.warning("NativeLwcChartAdapter.create_indicator: notify failed")

    def add_indicator_from_data(
        self,
        indicator_id: str,
        data: pd.DataFrame,
        *,
        value_col: str,
        pane: str | None = None,
        style: dict[str, object] | None = None,
    ) -> None:
        if not self._initialized or not self._bridge:
            return
        if not indicator_id or data is None or data.empty:
            return
        if "time" not in data.columns or value_col not in data.columns:
            return
        from . import rpc_protocol as rpc
        records = data.loc[:, ["time", value_col]].to_dict("records")
        style_payload = dict(style or {})
        style_payload.setdefault("valueKey", value_col)
        params: dict[str, object] = {"id": indicator_id, "data": records, "style": style_payload}
        if pane:
            params["pane"] = pane
        try:
            self._bridge.notify(rpc.M_ADD_INDICATOR, params)
        except Exception:
            log.warning("NativeLwcChartAdapter.add_indicator_from_data: notify failed")

    def remove_indicator(self, *, pane_id: str, name: str | None = None) -> None:
        if not self._initialized or not self._bridge:
            return
        from . import rpc_protocol as rpc
        params: dict[str, object] = {"paneId": pane_id}
        if name:
            params["name"] = name
        try:
            self._bridge.notify(rpc.M_REMOVE_INDICATOR, params)
        except Exception:
            log.warning("NativeLwcChartAdapter.remove_indicator: notify failed")

    def apply_theme(self, theme: str) -> None:
        """KLine 路径主题应用：通过 RPC chart.applyTheme 发送配色方案。"""
        if not self._initialized or not self._bridge:
            return
        from . import rpc_protocol as rpc
        _DARK = {
            "backgroundColor": "#0f172a",
            "textColor": "#e2e8f0",
            "axisColor": "#334155",
            "crosshairColor": "rgba(59,130,246,0.7)",
            "gridColor": "#1e293b",
            "upColor": "#22c55e",
            "downColor": "#ef4444",
        }
        _LIGHT = {
            "backgroundColor": "#f8fafc",
            "textColor": "#0f172a",
            "axisColor": "#cbd5e1",
            "crosshairColor": "rgba(59,130,246,0.7)",
            "gridColor": "#e2e8f0",
            "upColor": "#16a34a",
            "downColor": "#dc2626",
        }
        palette = _DARK if theme == "dark" else _LIGHT
        try:
            self._bridge.notify(rpc.M_APPLY_THEME, rpc.build_apply_theme(palette))
        except Exception:
            log.warning("NativeLwcChartAdapter.apply_theme: notify failed")

    # ── Event registration (扩展接口，Stage 2.2 实装) ──────────────────────────

    def on_chart_click(self, callback: Callable[[dict], None]) -> None:
        if self._bridge:
            from . import rpc_protocol as rpc
            self._bridge.on(rpc.E_CHART_CLICK, callback)

    def on_crosshair_move(self, callback: Callable[[dict], None]) -> None:
        if self._bridge:
            from . import rpc_protocol as rpc
            self._bridge.on(rpc.E_CROSSHAIR_MOVE, callback)

    def on_range_changed(self, callback: Callable[[dict], None]) -> None:
        if self._bridge:
            from . import rpc_protocol as rpc
            self._bridge.on(rpc.E_RANGE_CHANGED, callback)

    def on_drawing_created(self, callback: Callable[[dict], None]) -> None:
        if self._bridge:
            from . import rpc_protocol as rpc
            self._bridge.on(rpc.E_DRAWING_CREATED, callback)

    def on_drawing_deleted(self, callback: Callable[[dict], None]) -> None:
        if self._bridge:
            from . import rpc_protocol as rpc
            self._bridge.on(rpc.E_DRAWING_DELETED, callback)

    def on_drawing_updated(self, callback: Callable[[dict], None]) -> None:
        if self._bridge:
            from . import rpc_protocol as rpc
            self._bridge.on(rpc.E_DRAWING_UPDATED, callback)

    # ── Drawing API (Stage 2.2) ───────────────────────────────────────────────

    def add_drawing(self, drawing_type: str, **kwargs) -> str | None:
        """
        添加画线并返回 drawing_id。

        hline:  add_drawing('hline', price=10.5, title='支撑位', style={'color':'#ef5350'})
        tline:  add_drawing('tline', time1='2024-01-02', price1=10.2,
                                     time2='2024-03-01', price2=11.8)
        vline:  add_drawing('vline', time='2024-06-01')
        """
        if not self._initialized or not self._bridge:
            return None
        from . import rpc_protocol as rpc
        try:
            if kwargs:
                params = rpc.build_add_drawing(drawing_type, **kwargs)
                self._bridge.notify(rpc.M_ADD_DRAWING, params)
            else:
                params = rpc.build_start_draw(drawing_type)
                self._bridge.notify(rpc.M_START_DRAW, params)
            return params["id"]
        except Exception:
            log.warning("NativeLwcChartAdapter.add_drawing: notify failed")
            return None

    def remove_drawing(self, drawing_id: str) -> None:
        """删除指定 id 的画线。"""
        if not self._initialized or not self._bridge:
            return
        from . import rpc_protocol as rpc
        try:
            self._bridge.notify(rpc.M_REMOVE_DRAWING, rpc.build_remove_drawing(drawing_id))
        except Exception:
            log.warning("NativeLwcChartAdapter.remove_drawing: notify failed")

    def load_drawings(self, drawings: list) -> None:
        """批量恢复画线（换标的时调用，传入持久化存储的画线列表）。"""
        if not self._initialized or not self._bridge:
            return
        from . import rpc_protocol as rpc
        try:
            self._bridge.notify(rpc.M_LOAD_DRAWINGS, rpc.build_load_drawings(drawings))
        except Exception:
            log.warning("NativeLwcChartAdapter.load_drawings: notify failed")

    def get_drawings(self, timeout: float = 3.0) -> list:
        """
        同步获取当前所有画线元数据（用于持久化，阻塞至多 timeout 秒）。

        Returns:
            list[dict]  画线元数据列表，超时时返回空列表。
        """
        if not self._initialized or not self._bridge:
            return []
        from . import rpc_protocol as rpc
        try:
            result = self._bridge.call_sync(rpc.M_GET_DRAWINGS, {}, timeout=timeout)
            return result if isinstance(result, list) else []
        except Exception:
            log.warning("NativeLwcChartAdapter.get_drawings: 超时或失败")
            return []

    # ── Timezone & Watermark (Sprint 4) ───────────────────────────────────────

    def set_timezone(self, timezone: str) -> None:
        """设置图表时区，如 'Asia/Shanghai', 'UTC' 等。"""
        if not self._initialized or not self._bridge:
            return
        from . import rpc_protocol as rpc
        try:
            self._bridge.notify(rpc.M_SET_TIMEZONE, {"timezone": timezone})
        except Exception:
            log.warning("NativeLwcChartAdapter.set_timezone: notify failed")

    def set_watermark(self, text: str) -> None:
        """设置图表水印文字（品种代码大字居中半透明显示）。"""
        if not self._initialized or not self._bridge:
            return
        from . import rpc_protocol as rpc
        try:
            self._bridge.notify(rpc.M_SET_WATERMARK, {"text": text})
        except Exception:
            log.warning("NativeLwcChartAdapter.set_watermark: notify failed")

    # ── Internal ──────────────────────────────────────────────────────────────

    def _make_fallback(self, parent):
        """创建 lwc_python fallback 图表，存入 self._fallback 以备数据路由。"""
        project_root = Path(__file__).parent.parent.parent.parent
        external = str(project_root / "external" / "lightweight-charts-python")
        if external not in sys.path:
            sys.path.insert(0, external)
        try:
            import importlib
            mod = importlib.import_module("lightweight_charts.widgets")
            QtChart = getattr(mod, "QtChart")
            chart = QtChart(parent, toolbox=True)
            self._fallback = LwcPythonChartAdapter(chart)
            return chart.get_webview()
        except Exception:
            log.exception("NativeLwcChartAdapter: fallback 创建失败")
            from PyQt5.QtWidgets import QLabel
            lbl = QLabel("图表初始化失败\n(native + fallback both failed)", parent)
            return lbl

    @classmethod
    def _get_native_dir(cls) -> Path:
        return Path(__file__).parent.parent.parent / "chart_native"

    @classmethod
    def _get_lwc_js_src(cls) -> Path:
        if cls._LWC_JS_SRC is None:
            project_root = Path(__file__).parent.parent.parent.parent
            cls._LWC_JS_SRC = (
                project_root
                / "external" / "lightweight-charts-python"
                / "lightweight_charts" / "js" / "lightweight-charts.js"
            )
        return cls._LWC_JS_SRC

    def _ensure_lwc_lib(self) -> None:
        """首次运行时将 lightweight-charts.js 复制到 chart_native/lib/。"""
        native_dir = self._get_native_dir()
        lib_dir = native_dir / "lib"
        target = lib_dir / "lightweight-charts.js"
        if not target.exists():
            lib_dir.mkdir(parents=True, exist_ok=True)
            src = self._get_lwc_js_src()
            if src.exists():
                shutil.copy2(src, target)
                log.debug("NativeLwcChartAdapter: 已复制 %s → %s", src, target)
            else:
                log.warning("NativeLwcChartAdapter: lightweight-charts.js 源文件不存在: %s", src)

    def _build_html(self, port: int) -> str:
        """生成带 WS 端口注入的图表页面 HTML（相对路径相对于 chart_native/）。"""
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>EasyXT Native Chart</title>
  <style>
    html, body {{
      margin: 0; padding: 0;
      width: 100%; height: 100%;
      background: #0c0d0f;
      overflow: hidden;
    }}
    #chart {{ width: 100%; height: 100%; }}
    #easyxt-labels {{
      position: absolute; top: 6px; left: 8px;
      font-family: sans-serif; font-size: 12px; color: #d8d9db;
      pointer-events: none; z-index: 10;
    }}
    #easyxt-symbol-label {{ font-weight: bold; margin-right: 8px; }}
    #easyxt-period-label  {{ color: #888; }}
  </style>
</head>
<body>
  <div id="easyxt-labels">
    <span id="easyxt-symbol-label"></span>
    <span id="easyxt-period-label"></span>
  </div>
  <div id="chart"></div>
  <script src="./lib/lightweight-charts.js"></script>
  <script src="./chart-bridge.js"></script>
  <script>
    // NativeLwcChartAdapter 注入 WS 端口
    ChartBridge.init(document.getElementById('chart'), {port});
  </script>
</body>
</html>"""


# ── KLineChart adapter（Sprint 1 — 路径 B: KLineChart v9.x）─────────────────────

class KLineChartAdapter(NativeLwcChartAdapter):
    """
    KLineChart v9.8.x 适配器。

    继承 NativeLwcChartAdapter 的全部 WsBridge / 事件 / 画线 API，
    仅覆盖 HTML 模板（klinecharts.min.js + kline-bridge.js + KlineBridge.init）
    和运行时库校验（klinecharts.min.js 已随项目提交，无需动态复制）。
    """

    def _ensure_lwc_lib(self) -> None:
        """klinecharts.min.js 已随项目提交到 chart_native/lib/，仅做存在性校验。"""
        lib = self._get_native_dir() / "lib" / "klinecharts.min.js"
        if not lib.exists():
            log.warning("KLineChartAdapter: klinecharts.min.js 缺失: %s", lib)

    def _build_html(self, port: int) -> str:
        """生成 KLineChart 专业图表页面 HTML（纯图表区，侧边栏由 Qt 面板承载）。"""
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>EasyXT KLineChart</title>
  <style>
    html, body {{
      margin: 0; padding: 0; width: 100%; height: 100%;
      background: #0c0d0f; overflow: hidden;
    }}
    #chart {{ width: 100%; height: 100%; }}
    #easyxt-labels {{
      position: absolute; top: 6px; left: 8px;
      font-family: sans-serif; font-size: 12px; color: #d8d9db;
      pointer-events: none; z-index: 10;
    }}
    #easyxt-symbol-label {{ font-weight: bold; margin-right: 8px; }}
    #easyxt-period-label  {{ color: #888; }}
    #easyxt-watermark {{
      position: absolute; top: 50%; left: 50%;
      transform: translate(-50%, -50%);
      font-family: sans-serif; font-size: 48px; font-weight: bold;
      color: rgba(255,255,255,0.04); pointer-events: none; z-index: 5;
      white-space: nowrap; user-select: none;
    }}
  </style>
</head>
<body>
  <div id="easyxt-watermark"></div>
  <div id="easyxt-labels">
    <span id="easyxt-symbol-label"></span>
    <span id="easyxt-period-label"></span>
  </div>
  <div id="chart"></div>
  <script src="./lib/klinecharts.min.js"></script>
  <script src="./kline-bridge.js"></script>
  <script>
    KlineBridge.init(document.getElementById('chart'), {port});
  </script>
</body>
</html>"""

    def notify_orderbook(self, quote: dict, *, chart_id: str = "main", pane_id: str = "orderbook") -> None:
        """推送五档行情数据至 HTML 桥（保留接口，实际侧边栏由 Qt 面板承载）。"""
        if self._initialized and self._bridge:
            self._notify_v1("orderbook.update", quote, chart_id=chart_id, pane_id=pane_id)

    def notify_trades_tick(self, tick: dict, *, chart_id: str = "main", pane_id: str = "trades") -> None:
        """推送单笔成交明细至 HTML 桥（保留接口，实际侧边栏由 Qt 面板承载）。"""
        if self._initialized and self._bridge:
            self._notify_v1("trades.addTick", tick, chart_id=chart_id, pane_id=pane_id)

    def notify_stats(self, stats: dict, *, chart_id: str = "main", pane_id: str = "stats") -> None:
        """推送关键数据至 HTML 桥（保留接口，实际侧边栏由 Qt 面板承载）。"""
        if self._initialized and self._bridge:
            self._notify_v1("stats.update", stats, chart_id=chart_id, pane_id=pane_id)


# ── Factory ───────────────────────────────────────────────────────────────────

def create_chart_adapter(
    chart=None,
    backend: str | None = None,
    parent=None,
    account_id: str | None = None,
    strategy_id: str | None = None,
):
    """
    工厂函数。

    lwc_python 模式（默认）:
        adapter = create_chart_adapter(chart=qt_chart_obj)

    native_lwc 模式:
        adapter = create_chart_adapter(backend="native_lwc", parent=parent_widget)
        widget, is_native = adapter.initialize(parent_widget, timeout=5.0)

    灰度模式（通过 account_id / strategy_id 进行白名单匹配）:
        adapter = create_chart_adapter(chart=qt_chart_obj, account_id="A001")
    """
    # ── 1. 优先级：显式传参 > 配置中心（含 env var）─────────────────────────
    if backend is not None:
        backend_key = backend.strip().lower()
    else:
        try:
            from .backend_config import get_chart_backend_config
            backend_key = get_chart_backend_config().get_backend(
                account_id=account_id, strategy_id=strategy_id
            )
        except Exception:
            # 配置中心不可用时降级到 env var
            backend_key = os.environ.get("EASYXT_CHART_BACKEND", "klinechart").strip().lower()

    # ── 2. native_lwc：先做交易时段冻结检查 ──────────────────────────────────
    if backend_key == "native_lwc":
        try:
            from .backend_config import get_chart_backend_config
            ok, reason = get_chart_backend_config().can_switch_now()
            if not ok:
                log.warning("create_chart_adapter: %s → 降级 lwc_python", reason)
                backend_key = "lwc_python"
        except Exception:
            pass  # 检查失败时不阻断，继续创建 native_lwc

    if backend_key == "klinechart":
        return KLineChartAdapter()

    if backend_key == "native_lwc":
        return NativeLwcChartAdapter()

    # ── 3. 默认 lwc_python ────────────────────────────────────────────────────
    if chart is None:
        raise ValueError("create_chart_adapter: lwc_python backend 需要传入 chart 对象")
    return LwcPythonChartAdapter(chart)
