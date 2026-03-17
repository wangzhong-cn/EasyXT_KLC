from __future__ import annotations

import logging
from typing import Protocol, cast

import pandas as pd


# ---------------------------------------------------------------------------
# 全局周期→DuckDB表映射 (供 _ChartDataLoadThread / _FallbackSymbolThread 共用)
# 派生周期 (15m/30m/60m/1w/1M) 指向实际聚合源表，而非不存在的物理表
# ---------------------------------------------------------------------------
PERIOD_TABLE_MAP: dict[str, str] = {
    "1m": "stock_1m",
    "5m": "stock_5m",
    "15m": "stock_1m",
    "30m": "stock_1m",
    "60m": "stock_1m",
    "1d": "stock_daily",
    "1w": "stock_daily",
    "1M": "stock_daily",
    "tick": "stock_tick",
}

PERIOD_DATE_COL_MAP: dict[str, tuple[str, str]] = {
    "1m": ("stock_1m", "datetime"),
    "5m": ("stock_5m", "datetime"),
    "15m": ("stock_1m", "datetime"),
    "30m": ("stock_1m", "datetime"),
    "60m": ("stock_1m", "datetime"),
    "1d": ("stock_daily", "date"),
    "1w": ("stock_daily", "date"),
    "1M": ("stock_daily", "date"),
    "tick": ("stock_tick", "datetime"),
}


class _OverlayLine(Protocol):
    def show_data(self) -> None:
        ...

    def hide_data(self) -> None:
        ...

    def update(self, data: pd.Series) -> None:
        ...

    def set(self, data: pd.DataFrame) -> None:
        ...


class SubchartManager:
    """管理所有子图 (MACD / RSI / VOL / KDJ) 及主图叠加指标 (MA / BOLL)"""

    def __init__(self, chart):
        self.chart = chart
        self.window = chart.win
        # --- MACD ---
        self.macd_chart = None
        self.macd_line = None
        self.macd_signal = None
        self.macd_hist = None
        # --- RSI ---
        self.rsi_chart = None
        self.rsi_line = None
        # --- VOL ---
        self.vol_chart = None
        self.vol_hist = None
        # --- KDJ ---
        self.kdj_chart = None
        self.kdj_k_line = None
        self.kdj_d_line = None
        self.kdj_j_line = None
        # --- MA (叠加在主图) ---
        self._ma_lines: dict[int, _OverlayLine] = {}  # period -> line object
        # --- BOLL (叠加在主图) ---
        self._boll_upper = None
        self._boll_mid = None
        self._boll_lower = None

        # 可见性开关
        self.macd_visible = True
        self.rsi_visible = True
        self.vol_visible = False
        self.kdj_visible = False
        self.ma_visible = False
        self.ma_periods: list[int] = [5, 10, 20, 60]
        self.boll_visible = False

        self.macd_height = 0.2
        self.rsi_height = 0.2
        self.vol_height = 0.15
        self.kdj_height = 0.18
        self._logger = logging.getLogger(__name__)

    # ------------------------------------------------------------------ 公开 API
    def setup_default(self):
        self._ensure_subcharts()
        self._apply_layout()

    def set_visibility(
        self,
        macd: bool | None = None,
        rsi: bool | None = None,
        vol: bool | None = None,
        kdj: bool | None = None,
        ma: bool | None = None,
        boll: bool | None = None,
    ):
        if macd is not None:
            self.macd_visible = macd
        if rsi is not None:
            self.rsi_visible = rsi
        if vol is not None:
            self.vol_visible = vol
        if kdj is not None:
            self.kdj_visible = kdj
        if ma is not None:
            self.ma_visible = ma
        if boll is not None:
            self.boll_visible = boll
        self._ensure_subcharts()
        self._ensure_overlays()
        self._apply_layout()

    # ------------------------------------------------------------------ 子图创建
    def _ensure_subcharts(self):
        try:
            # MACD
            if self.macd_visible and self.macd_chart is None:
                self.macd_chart = self.window.create_subchart(
                    width=1.0,
                    height=self.macd_height,
                    sync_id=self.chart.id,
                    sync_crosshairs_only=False,
                )
                self.macd_line = self.macd_chart.create_line(name="macd", color="rgba(59,130,246,0.9)")
                self.macd_signal = self.macd_chart.create_line(name="signal", color="rgba(248,113,113,0.9)")
                self.macd_hist = self.macd_chart.create_histogram(
                    name="hist",
                    color="rgba(148,163,184,0.6)",
                    price_line=False,
                    price_label=False,
                    scale_margin_top=0.2,
                    scale_margin_bottom=0.0,
                )
            # RSI
            if self.rsi_visible and self.rsi_chart is None:
                self.rsi_chart = self.window.create_subchart(
                    width=1.0,
                    height=self.rsi_height,
                    sync_id=self.chart.id,
                    sync_crosshairs_only=False,
                )
                self.rsi_line = self.rsi_chart.create_line(name="rsi", color="rgba(16,185,129,0.9)")
            # VOL
            if self.vol_visible and self.vol_chart is None:
                self.vol_chart = self.window.create_subchart(
                    width=1.0,
                    height=self.vol_height,
                    sync_id=self.chart.id,
                    sync_crosshairs_only=False,
                )
                self.vol_hist = self.vol_chart.create_histogram(
                    name="volume",
                    color="rgba(99,102,241,0.5)",
                    price_line=False,
                    price_label=False,
                    scale_margin_top=0.1,
                    scale_margin_bottom=0.0,
                )
            # KDJ
            if self.kdj_visible and self.kdj_chart is None:
                self.kdj_chart = self.window.create_subchart(
                    width=1.0,
                    height=self.kdj_height,
                    sync_id=self.chart.id,
                    sync_crosshairs_only=False,
                )
                self.kdj_k_line = self.kdj_chart.create_line(name="K", color="rgba(59,130,246,0.9)")
                self.kdj_d_line = self.kdj_chart.create_line(name="D", color="rgba(248,113,113,0.9)")
                self.kdj_j_line = self.kdj_chart.create_line(name="J", color="rgba(250,204,21,0.9)")
        except Exception:
            self._logger.exception("Failed to setup subcharts")

    # ------------------------------------------------------------------ 主图叠加
    _MA_COLORS = {
        5: "rgba(250,204,21,0.85)",    # 黄
        10: "rgba(59,130,246,0.85)",    # 蓝
        20: "rgba(248,113,113,0.85)",   # 红
        60: "rgba(16,185,129,0.85)",    # 绿
    }

    def _ensure_overlays(self):
        """创建 / 显示 / 隐藏主图叠加线 (MA / BOLL)
        用 hide_data/show_data (applyOptions visible) 代替 delete()，
        避免 delete() 内部访问 legend._lines 抛 JS 异常导致 removeSeries 不执行。
        """
        try:
            # --- MA ---
            if self.ma_visible:
                for p in self.ma_periods:
                    if p not in self._ma_lines:
                        color = self._MA_COLORS.get(p, "rgba(180,180,180,0.7)")
                        self._ma_lines[p] = cast(_OverlayLine, self.chart.create_line(
                            name=f"MA{p}", color=color,
                            price_line=False, price_label=False,
                        ))
                    else:
                        self._ma_lines[p].show_data()   # 恢复隐藏的线
            else:
                for line in self._ma_lines.values():
                    try:
                        line.hide_data()                # 隐藏但保留对象
                    except Exception:
                        pass
            # --- BOLL ---
            if self.boll_visible:
                if self._boll_mid is None:
                    self._boll_mid = self.chart.create_line(
                        name="BOLL_MID", color="rgba(250,204,21,0.7)",
                        price_line=False, price_label=False,
                    )
                    self._boll_upper = self.chart.create_line(
                        name="BOLL_UP", color="rgba(59,130,246,0.5)",
                        price_line=False, price_label=False,
                    )
                    self._boll_lower = self.chart.create_line(
                        name="BOLL_DN", color="rgba(59,130,246,0.5)",
                        price_line=False, price_label=False,
                    )
                else:
                    for attr in ("_boll_upper", "_boll_mid", "_boll_lower"):
                        line = getattr(self, attr, None)
                        if line is not None:
                            line.show_data()
            else:
                for attr in ("_boll_upper", "_boll_mid", "_boll_lower"):
                    line = getattr(self, attr, None)
                    if line is not None:
                        try:
                            line.hide_data()
                        except Exception:
                            pass
        except Exception:
            self._logger.exception("Failed to ensure overlays")

    # ------------------------------------------------------------------ 布局
    def _apply_layout(self):
        total = 1.0
        macd_h = self.macd_height if self.macd_visible else 0.0
        rsi_h = self.rsi_height if self.rsi_visible else 0.0
        vol_h = self.vol_height if self.vol_visible else 0.0
        kdj_h = self.kdj_height if self.kdj_visible else 0.0
        main_h = max(total - macd_h - rsi_h - vol_h - kdj_h, 0.3)
        self.chart.resize(height=main_h)
        if self.macd_chart is not None:
            self.macd_chart.resize(height=macd_h)
        if self.rsi_chart is not None:
            self.rsi_chart.resize(height=rsi_h)
        if self.vol_chart is not None:
            self.vol_chart.resize(height=vol_h)
        if self.kdj_chart is not None:
            self.kdj_chart.resize(height=kdj_h)

    # ------------------------------------------------------------------ 数据更新
    def update(self, data: pd.DataFrame):
        if data is None or data.empty:
            return
        try:
            self.setup_default()
            if "time" not in data.columns:
                return
            if self.macd_visible:
                macd_df, signal_df, hist_df = self._build_macd_df(data)
                if self.macd_line is not None and macd_df is not None:
                    self.macd_line.set(macd_df)
                if self.macd_signal is not None and signal_df is not None:
                    self.macd_signal.set(signal_df)
                if self.macd_hist is not None and hist_df is not None:
                    self.macd_hist.set(hist_df)
            if self.rsi_visible:
                rsi_df = self._build_rsi_df(data)
                if self.rsi_line is not None and rsi_df is not None:
                    self.rsi_line.set(rsi_df)
            if self.vol_visible:
                vol_df = self._build_vol_df(data)
                if self.vol_hist is not None and vol_df is not None:
                    self.vol_hist.set(vol_df)
            if self.kdj_visible:
                k_df, d_df, j_df = self._build_kdj_df(data)
                if self.kdj_k_line is not None and k_df is not None:
                    self.kdj_k_line.set(k_df)
                if self.kdj_d_line is not None and d_df is not None:
                    self.kdj_d_line.set(d_df)
                if self.kdj_j_line is not None and j_df is not None:
                    self.kdj_j_line.set(j_df)
            if self.ma_visible:
                self._update_ma(data)
            if self.boll_visible:
                self._update_boll(data)
        except Exception:
            self._logger.exception("Failed to update subcharts")

    def compute_all(self, data: pd.DataFrame) -> dict:
        """线程安全: 在后台线程计算所有指标 DataFrame，不调用任何 chart API"""
        results: dict = {}
        if data is None or data.empty or "time" not in data.columns:
            return results
        try:
            if self.macd_visible:
                results["macd"] = self._build_macd_df(data)
            if self.rsi_visible:
                results["rsi"] = self._build_rsi_df(data)
            if self.vol_visible:
                results["vol"] = self._build_vol_df(data)
            if self.kdj_visible:
                results["kdj"] = self._build_kdj_df(data)
            if self.ma_visible:
                close = pd.to_numeric(data["close"], errors="coerce")
                ma_results = {}
                for p in self.ma_periods:
                    ma = close.rolling(p, min_periods=1).mean()
                    ma_df = pd.DataFrame({"time": data["time"], f"MA{p}": ma})
                    ma_df = ma_df.dropna(subset=["time", f"MA{p}"])
                    if not ma_df.empty:
                        ma_results[p] = ma_df
                results["ma"] = ma_results
            if self.boll_visible:
                close = pd.to_numeric(data["close"], errors="coerce")
                mid = close.rolling(20, min_periods=1).mean()
                std = close.rolling(20, min_periods=1).std()
                upper = mid + 2 * std
                lower = mid - 2 * std
                results["boll"] = {
                    "mid": pd.DataFrame({"time": data["time"], "BOLL_MID": mid}).dropna(),
                    "upper": pd.DataFrame({"time": data["time"], "BOLL_UP": upper}).dropna(),
                    "lower": pd.DataFrame({"time": data["time"], "BOLL_DN": lower}).dropna(),
                }
        except Exception:
            self._logger.exception("compute_all failed")
        return results

    def compute_last_bar(self, data: pd.DataFrame) -> dict:
        """线程安全: 仅计算最后一行指标值，用于增量 .update() (Fix 55)"""
        results: dict = {}
        if data is None or len(data) < 2 or "time" not in data.columns:
            return results
        try:
            last_time = data["time"].iloc[-1]
            if self.macd_visible:
                close = pd.to_numeric(data["close"], errors="coerce")
                ema_fast = close.ewm(span=12, adjust=False).mean()
                ema_slow = close.ewm(span=26, adjust=False).mean()
                macd = ema_fast - ema_slow
                signal = macd.ewm(span=9, adjust=False).mean()
                hist = macd - signal
                results["macd"] = (
                    {"time": last_time, "macd": float(macd.iloc[-1])},
                    {"time": last_time, "signal": float(signal.iloc[-1])},
                    {"time": last_time, "hist": float(hist.iloc[-1])},
                )
            if self.rsi_visible and len(data) >= 15:
                close = pd.to_numeric(data["close"], errors="coerce")
                delta = close.diff()
                gain = delta.where(delta > 0, 0.0)
                loss = -delta.where(delta < 0, 0.0)
                avg_gain = gain.rolling(14).mean()
                avg_loss = loss.rolling(14).mean()
                rs = avg_gain / avg_loss.replace(0, pd.NA)
                rsi = 100 - (100 / (1 + rs))
                val = rsi.iloc[-1]
                if pd.notna(val):
                    results["rsi"] = {"time": last_time, "rsi": float(val)}
            if self.vol_visible and "volume" in data.columns:
                vol = pd.to_numeric(data["volume"], errors="coerce").fillna(0)
                results["vol"] = {"time": last_time, "volume": float(vol.iloc[-1])}
            if self.kdj_visible and len(data) >= 9:
                high = pd.to_numeric(data["high"], errors="coerce")
                low = pd.to_numeric(data["low"], errors="coerce")
                close = pd.to_numeric(data["close"], errors="coerce")
                low_n = low.rolling(9, min_periods=1).min()
                high_n = high.rolling(9, min_periods=1).max()
                rsv = (close - low_n) / (high_n - low_n).replace(0, pd.NA) * 100
                k = rsv.ewm(com=2, adjust=False).mean()
                d = k.ewm(com=2, adjust=False).mean()
                j = 3 * k - 2 * d
                results["kdj"] = (
                    {"time": last_time, "K": float(k.iloc[-1])},
                    {"time": last_time, "D": float(d.iloc[-1])},
                    {"time": last_time, "J": float(j.iloc[-1])},
                )
            if self.ma_visible:
                close = pd.to_numeric(data["close"], errors="coerce")
                ma_results = {}
                for p in self.ma_periods:
                    ma = close.rolling(p, min_periods=1).mean()
                    ma_results[p] = {"time": last_time, f"MA{p}": float(ma.iloc[-1])}
                results["ma"] = ma_results
            if self.boll_visible and len(data) >= 20:
                close = pd.to_numeric(data["close"], errors="coerce")
                mid = close.rolling(20, min_periods=1).mean()
                std = close.rolling(20, min_periods=1).std()
                upper = mid + 2 * std
                lower = mid - 2 * std
                results["boll"] = {
                    "mid": {"time": last_time, "BOLL_MID": float(mid.iloc[-1])},
                    "upper": {"time": last_time, "BOLL_UP": float(upper.iloc[-1])},
                    "lower": {"time": last_time, "BOLL_DN": float(lower.iloc[-1])},
                }
        except Exception:
            self._logger.exception("compute_last_bar failed")
        return results

    def _safe_update(self, line, row_series: "pd.Series"):
        """安全更新副图线条：data 为空时改用 set() 初始化"""
        if line is None:
            return
        try:
            if line.data is None or line.data.empty:
                line.set(pd.DataFrame([row_series]))
            else:
                line.update(row_series)
        except (IndexError, KeyError):
            try:
                line.set(pd.DataFrame([row_series]))
            except Exception:
                pass

    def apply_last_bar(self, results: dict):
        """主线程: 仅 .update(单行) 每个指标线条，避免 .set(全量) (Fix 55)"""
        try:
            self.setup_default()
            if "macd" in results:
                macd_row, signal_row, hist_row = results["macd"]
                self._safe_update(self.macd_line, pd.Series(macd_row))
                self._safe_update(self.macd_signal, pd.Series(signal_row))
                self._safe_update(self.macd_hist, pd.Series(hist_row))
            if "rsi" in results:
                self._safe_update(self.rsi_line, pd.Series(results["rsi"]))
            if "vol" in results:
                self._safe_update(self.vol_hist, pd.Series(results["vol"]))
            if "kdj" in results:
                k_row, d_row, j_row = results["kdj"]
                self._safe_update(self.kdj_k_line, pd.Series(k_row))
                self._safe_update(self.kdj_d_line, pd.Series(d_row))
                self._safe_update(self.kdj_j_line, pd.Series(j_row))
            if "ma" in results:
                for p, row in results["ma"].items():
                    line = self._ma_lines.get(p)
                    self._safe_update(line, pd.Series(row))
            if "boll" in results:
                boll = results["boll"]
                self._safe_update(self._boll_mid, pd.Series(boll["mid"]))
                self._safe_update(self._boll_upper, pd.Series(boll["upper"]))
                self._safe_update(self._boll_lower, pd.Series(boll["lower"]))
        except Exception:
            self._logger.exception("apply_last_bar failed")

    def apply_precomputed(self, results: dict):
        """主线程: 将预计算结果应用到图表线条 (仅 WebView IPC，无 pandas 计算)"""
        try:
            self.setup_default()
            if "macd" in results:
                macd_df, signal_df, hist_df = results["macd"]
                if self.macd_line is not None and macd_df is not None:
                    self.macd_line.set(macd_df)
                if self.macd_signal is not None and signal_df is not None:
                    self.macd_signal.set(signal_df)
                if self.macd_hist is not None and hist_df is not None:
                    self.macd_hist.set(hist_df)
            if "rsi" in results:
                rsi_df = results["rsi"]
                if self.rsi_line is not None and rsi_df is not None:
                    self.rsi_line.set(rsi_df)
            if "vol" in results:
                vol_df = results["vol"]
                if self.vol_hist is not None and vol_df is not None:
                    self.vol_hist.set(vol_df)
            if "kdj" in results:
                k_df, d_df, j_df = results["kdj"]
                if self.kdj_k_line is not None and k_df is not None:
                    self.kdj_k_line.set(k_df)
                if self.kdj_d_line is not None and d_df is not None:
                    self.kdj_d_line.set(d_df)
                if self.kdj_j_line is not None and j_df is not None:
                    self.kdj_j_line.set(j_df)
            if "ma" in results:
                for p, ma_df in results["ma"].items():
                    line = self._ma_lines.get(p)
                    if line is not None:
                        line.set(ma_df)
            if "boll" in results:
                boll = results["boll"]
                if self._boll_mid is not None and not boll["mid"].empty:
                    self._boll_mid.set(boll["mid"])
                if self._boll_upper is not None and not boll["upper"].empty:
                    self._boll_upper.set(boll["upper"])
                if self._boll_lower is not None and not boll["lower"].empty:
                    self._boll_lower.set(boll["lower"])
        except Exception:
            self._logger.exception("apply_precomputed failed")
    def _build_macd_df(self, data: pd.DataFrame):
        close = pd.to_numeric(data["close"], errors="coerce")
        ema_fast = close.ewm(span=12, adjust=False).mean()
        ema_slow = close.ewm(span=26, adjust=False).mean()
        macd = ema_fast - ema_slow
        signal = macd.ewm(span=9, adjust=False).mean()
        hist = macd - signal
        macd_df = pd.DataFrame({"time": data["time"], "macd": macd})
        signal_df = pd.DataFrame({"time": data["time"], "signal": signal})
        hist_df = pd.DataFrame({"time": data["time"], "hist": hist})
        macd_df = macd_df.dropna(subset=["time", "macd"])
        signal_df = signal_df.dropna(subset=["time", "signal"])
        hist_df = hist_df.dropna(subset=["time", "hist"])
        return macd_df, signal_df, hist_df

    def _build_rsi_df(self, data: pd.DataFrame) -> pd.DataFrame | None:
        close = pd.to_numeric(data["close"], errors="coerce")
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        # min_periods=1 保证前14根K线不产生NaN，使RSI序列与主图时间轴完全对齐
        avg_gain = gain.rolling(14, min_periods=1).mean()
        avg_loss = loss.rolling(14, min_periods=1).mean()
        rs = avg_gain / avg_loss.replace(0, float("nan"))
        # 当avg_gain=avg_loss=0时(首根K线平盘) rs=nan，填充中性值50
        rsi = (100 - (100 / (1 + rs))).fillna(50.0)
        rsi_df = pd.DataFrame({"time": data["time"], "rsi": rsi})
        # 只剔除时间列无效行，不再因rsi=NaN而截断序列
        rsi_df = rsi_df.dropna(subset=["time"])
        if rsi_df.empty:
            return None
        return rsi_df

    def _build_vol_df(self, data: pd.DataFrame) -> pd.DataFrame | None:
        if "volume" not in data.columns:
            return None
        vol = pd.to_numeric(data["volume"], errors="coerce").fillna(0)
        vol_df = pd.DataFrame({"time": data["time"], "volume": vol})
        vol_df = vol_df.dropna(subset=["time"])
        if vol_df.empty:
            return None
        return vol_df

    def _build_kdj_df(self, data: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3):
        high = pd.to_numeric(data["high"], errors="coerce")
        low = pd.to_numeric(data["low"], errors="coerce")
        close = pd.to_numeric(data["close"], errors="coerce")
        low_n = low.rolling(n, min_periods=1).min()
        high_n = high.rolling(n, min_periods=1).max()
        rsv = (close - low_n) / (high_n - low_n).replace(0, pd.NA) * 100
        k = rsv.ewm(com=m1 - 1, adjust=False).mean()
        d = k.ewm(com=m2 - 1, adjust=False).mean()
        j = 3 * k - 2 * d
        k_df = pd.DataFrame({"time": data["time"], "K": k}).dropna(subset=["time", "K"])
        d_df = pd.DataFrame({"time": data["time"], "D": d}).dropna(subset=["time", "D"])
        j_df = pd.DataFrame({"time": data["time"], "J": j}).dropna(subset=["time", "J"])
        return (k_df if not k_df.empty else None,
                d_df if not d_df.empty else None,
                j_df if not j_df.empty else None)

    def _update_ma(self, data: pd.DataFrame):
        close = pd.to_numeric(data["close"], errors="coerce")
        for p, line in self._ma_lines.items():
            ma = close.rolling(p, min_periods=1).mean()
            ma_df = pd.DataFrame({"time": data["time"], f"MA{p}": ma})
            ma_df = ma_df.dropna(subset=["time", f"MA{p}"])
            if not ma_df.empty:
                line.set(ma_df)

    def _update_boll(self, data: pd.DataFrame, n: int = 20, k: int = 2):
        close = pd.to_numeric(data["close"], errors="coerce")
        mid = close.rolling(n, min_periods=1).mean()
        std = close.rolling(n, min_periods=1).std()
        upper = mid + k * std
        lower = mid - k * std
        if self._boll_mid is not None:
            df = pd.DataFrame({"time": data["time"], "BOLL_MID": mid}).dropna()
            if not df.empty:
                self._boll_mid.set(df)
        if self._boll_upper is not None:
            df = pd.DataFrame({"time": data["time"], "BOLL_UP": upper}).dropna()
            if not df.empty:
                self._boll_upper.set(df)
        if self._boll_lower is not None:
            df = pd.DataFrame({"time": data["time"], "BOLL_DN": lower}).dropna()
            if not df.empty:
                self._boll_lower.set(df)
