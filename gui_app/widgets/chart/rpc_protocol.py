"""
chart/rpc_protocol.py — EasyXT 本地图表 JSON-RPC 2.0 协议定义

Python → JS 方法（Method）：
  chart.setData         批量写入 OHLCV K线数据（全量替换）
  chart.updateBar       增量更新最后一根 bar（实时行情）
  chart.setMarkers      替换全部标记（买卖信号点）
  chart.addIndicator    向指定窗格添加技术指标序列
  chart.updateIndicator 实时更新指标最后一个数据点
  chart.removeIndicator 移除指标
  chart.applyTheme      应用主题配色
  chart.resize          调整图表尺寸
  chart.setSymbolText   更新顶栏标的显示
  chart.setPeriodText   更新顶栏周期显示
  chart.addDrawing      添加画线（返回 drawing_id）
  chart.removeDrawing   删除画线
  chart.getDrawings     获取所有画线（call_sync 调用，用于持久化）
  chart.loadDrawings    从持久化加载画线
  chart.takeScreenshot  截图（call_sync 调用，返回 base64 PNG）
  chart.fitContent      自动适配可视范围

JS → Python 事件（Event）：
  chart.click           用户单击图表 {time, price}
  chart.crosshairMove   十字线移动 {time, price, open, high, low, close, volume}
  chart.rangeChanged    可视时间范围变化 {from, to}
  chart.symbolSearch    顶栏搜索框输入 {query}
  chart.drawingCreated  画线完成 {id, type, points}
  chart.drawingUpdated  画线拖动调整 {id, points}
  chart.drawingDeleted  画线删除 {id}
  chart.ready           JS 图表初始化完成（握手确认）{}
"""
from __future__ import annotations

import pandas as pd

# ── Method names ─────────────────────────────────────────────────────────────
M_SET_DATA = "chart.setData"
M_UPDATE_BAR = "chart.updateBar"
M_SET_MARKERS = "chart.setMarkers"
M_ADD_INDICATOR = "chart.addIndicator"
M_UPDATE_INDICATOR = "chart.updateIndicator"
M_REMOVE_INDICATOR = "chart.removeIndicator"
M_APPLY_THEME = "chart.applyTheme"
M_RESIZE = "chart.resize"
M_SET_SYMBOL = "chart.setSymbolText"
M_SET_PERIOD = "chart.setPeriodText"
M_ADD_DRAWING = "chart.addDrawing"
M_REMOVE_DRAWING = "chart.removeDrawing"
M_GET_DRAWINGS = "chart.getDrawings"  # call_sync
M_LOAD_DRAWINGS = "chart.loadDrawings"
M_TAKE_SCREENSHOT = "chart.takeScreenshot"  # call_sync → base64 PNG
M_FIT_CONTENT = "chart.fitContent"

# ── Event names ───────────────────────────────────────────────────────────────
E_CHART_CLICK = "chart.click"
E_CROSSHAIR_MOVE = "chart.crosshairMove"
E_RANGE_CHANGED = "chart.rangeChanged"
E_SYMBOL_SEARCH = "chart.symbolSearch"
E_DRAWING_CREATED = "chart.drawingCreated"
E_DRAWING_UPDATED = "chart.drawingUpdated"
E_DRAWING_DELETED = "chart.drawingDeleted"
E_READY = "chart.ready"

# ── JSON-RPC 2.0 error codes ──────────────────────────────────────────────────
# Standard codes
ERR_PARSE_ERROR = -32700
ERR_INVALID_REQUEST = -32600
ERR_METHOD_NOT_FOUND = -32601
ERR_INVALID_PARAMS = -32602
ERR_INTERNAL = -32603
# Application codes
ERR_CHART_NOT_READY = -32000   # 图表未完成初始化
ERR_TIMEOUT = -32001            # 操作超时
ERR_SERIES_NOT_FOUND = -32002   # 指定 series/indicator 不存在
ERR_DRAWING_NOT_FOUND = -32003  # 指定画线 ID 不存在

# ── Param builders ────────────────────────────────────────────────────────────

_OHLCV_COLS = ("time", "open", "high", "low", "close", "volume")


def build_set_data(df: pd.DataFrame) -> dict:
    """将 OHLCV DataFrame 转换为 setData 参数。只传必要列，降低序列化开销。"""
    cols = [c for c in _OHLCV_COLS if c in df.columns]
    return {
        "bars": df[cols].to_dict("records"),
        "fitContent": True,
    }


def build_update_bar(row: pd.Series) -> dict:
    """单行 Series → updateBar 参数。"""
    return {"bar": {k: v for k, v in row.items() if k in _OHLCV_COLS}}


def build_set_markers(markers: list[dict]) -> dict:
    return {"markers": markers}


def build_add_indicator(
    indicator_id: str,
    pane: str,
    data: pd.DataFrame,
    style: dict,
) -> dict:
    """添加指标到指定窗格（pane）。data 需包含 time 列。"""
    time_cols = [c for c in data.columns]
    return {
        "id": indicator_id,
        "pane": pane,
        "data": data[time_cols].to_dict("records"),
        "style": style,
    }


def build_update_indicator(indicator_id: str, bar: dict) -> dict:
    return {"id": indicator_id, "bar": bar}


def build_apply_theme(theme: dict) -> dict:
    return {"theme": theme}


def build_add_drawing(
    drawing_type: str,
    style: dict | None = None,
    *,
    # hline
    price: float | None = None,
    title: str = "",
    axis_label: bool = True,
    # tline
    time1: str | int | None = None,
    price1: float | None = None,
    time2: str | int | None = None,
    price2: float | None = None,
    # vline
    time: str | int | None = None,
    # caller-supplied id (optional; JS will use it as-is)
    drawing_id: str | None = None,
) -> dict:
    """
    构造 chart.addDrawing 参数。

    - hline  水平价格线: price=...
    - tline  趋势线:     time1, price1, time2, price2
    - vline  垂直时间线: time=...
    """
    import uuid
    params: dict = {
        "id": drawing_id or str(uuid.uuid4()),
        "type": drawing_type,
        "style": style or {},
    }
    if drawing_type == "hline":
        params["price"] = price
        params["title"] = title
        params["axisLabel"] = axis_label
    elif drawing_type == "tline":
        params["time1"] = time1
        params["price1"] = price1
        params["time2"] = time2
        params["price2"] = price2
    elif drawing_type == "vline":
        params["time"] = time
    return params


def build_remove_drawing(drawing_id: str) -> dict:
    return {"id": drawing_id}


def build_load_drawings(drawings: list[dict]) -> dict:
    return {"drawings": drawings}
