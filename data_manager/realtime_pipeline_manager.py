#!/usr/bin/env python3
import os
import time
from collections import deque
from typing import Any, Optional

import pandas as pd


class RealtimePipelineManager:
    def __init__(self, max_queue: int = 256, flush_interval_ms: int = 200):
        self.max_queue = max(32, int(max_queue))
        self.flush_interval_s = max(0.05, float(flush_interval_ms) / 1000.0)
        # 读取环境变量，但添加边界校验
        self._drop_rate_threshold = max(
            0.001, min(0.999, float(os.environ.get("EASYXT_RT_DROP_THRESHOLD", "0.1")))
        )
        self._window_seconds = max(
            1.0, min(3600.0, float(os.environ.get("EASYXT_RT_WINDOW_SECONDS", "60")))
        )
        self._alert_sustain_s = max(
            0.1, min(600.0, float(os.environ.get("EASYXT_RT_ALERT_SUSTAIN_S", "5")))
        )

        # 添加滞回阈值，用于避免抖动
        self._recovery_threshold = self._drop_rate_threshold * 0.6  # 恢复阈值为告警阈值的60%

        self._queue: deque[dict[str, Any]] = deque()
        self._symbol: str = ""
        self._period: str = "1d"
        self._last_data: pd.DataFrame = pd.DataFrame()
        self._last_total_volume: Optional[float] = None
        self._last_flush_ts: float = 0.0
        self._dropped_quotes: int = 0
        self._total_quotes: int = 0
        self._window_quotes: deque[float] = deque()
        self._window_dropped: deque[float] = deque()
        self._window_exceed_since: Optional[float] = None
        self._sustained_alert: bool = False

    def configure(self, symbol: str, period: str, last_data: Optional[pd.DataFrame]) -> None:
        symbol = str(symbol or "").strip()
        period = str(period or "1d").strip()
        if symbol != self._symbol or period != self._period:
            self._queue.clear()
            self._last_total_volume = None
            self._window_quotes.clear()
            self._window_dropped.clear()
            self._total_quotes = 0
            self._dropped_quotes = 0
            self._window_exceed_since = None
            self._sustained_alert = False
        self._symbol = symbol
        self._period = period
        if last_data is None or last_data.empty:
            self._last_data = pd.DataFrame()
        else:
            # Fix 56: 只保留最后 5 行而非全量拷贝，避免大 DataFrame 的 O(n) copy
            self._last_data = last_data.tail(5).copy()

    def enqueue_quote(self, quote: dict[str, Any]) -> None:
        if not isinstance(quote, dict):
            return
        now = time.monotonic()
        self._total_quotes += 1
        self._window_quotes.append(now)
        if len(self._queue) >= self.max_queue:
            self._queue.popleft()
            self._dropped_quotes += 1
            self._window_dropped.append(now)
        self._queue.append(quote)
        self._trim_window(now)

    def flush(self, force: bool = False) -> Optional[dict[str, Any]]:
        if not self._queue:
            return None
        now = time.monotonic()
        if not force and (now - self._last_flush_ts) < self.flush_interval_s:
            return None
        self._last_flush_ts = now

        quote = self._queue[-1]
        self._queue.clear()
        price = float(quote.get("price") or 0)
        if price <= 0:
            return None

        if self._last_data is None or self._last_data.empty:
            bar = self._build_bar_from_quote(quote, self._period)
            if bar is None:
                return None
            self._last_data = pd.DataFrame([bar])
            self._last_total_volume = float(quote.get("volume") or 0)
            return {
                "action": "init",
                "bar": bar,
                "data": self._last_data.copy(),
                "quote": quote,
                "metrics": self.metrics(),
            }

        bar = self._apply_quote_to_series(quote)
        if bar is None:
            return None
        return {
            "action": "update",
            "bar": bar,
            "data": None,  # Fix 56: 不再返回全量拷贝，调用方自行维护 last_data
            "quote": quote,
            "metrics": self.metrics(),
        }

    def metrics(self) -> dict[str, Any]:
        now = time.monotonic()
        self._trim_window(now)
        drop_rate = 0.0
        if self._total_quotes > 0:
            drop_rate = self._dropped_quotes / self._total_quotes
        window_total = len(self._window_quotes)
        window_drop_rate = 0.0
        if window_total > 0:
            window_drop_rate = len(self._window_dropped) / window_total

        # 使用滞回逻辑来避免反复抖动
        window_exceeded = window_total > 0 and window_drop_rate > self._drop_rate_threshold
        window_recovered = window_total > 0 and window_drop_rate < self._recovery_threshold

        if window_exceeded:
            if self._window_exceed_since is None:
                self._window_exceed_since = now
            if (now - self._window_exceed_since) >= self._alert_sustain_s:
                self._sustained_alert = True
        elif window_recovered and self._sustained_alert:
            # 只有在告警已激活且当前恢复时才清除告警
            self._window_exceed_since = None
            self._sustained_alert = False

        return {
            "queue_len": len(self._queue),
            "dropped_quotes": self._dropped_quotes,
            "flush_interval_ms": int(self.flush_interval_s * 1000),
            "max_queue": self.max_queue,
            "drop_rate": round(drop_rate * 100, 2),  # 丢包率百分比
            "drop_rate_threshold": round(self._drop_rate_threshold * 100, 2),
            "drop_rate_threshold_exceeded": drop_rate > self._drop_rate_threshold,
            "window_drop_rate": round(window_drop_rate * 100, 2),
            "window_total_quotes": window_total,
            "window_seconds": int(self._window_seconds),
            "window_threshold_exceeded": window_exceeded,
            "sustained_drop_alert": self._sustained_alert,
            "alert_sustain_s": int(self._alert_sustain_s),
            "recovery_threshold": round(self._recovery_threshold * 100, 2),  # 恢复阈值
        }

    def _trim_window(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._window_quotes and self._window_quotes[0] < cutoff:
            self._window_quotes.popleft()
        while self._window_dropped and self._window_dropped[0] < cutoff:
            self._window_dropped.popleft()

    @staticmethod
    def _compute_bar_time(now: pd.Timestamp, period: str) -> str:
        """Fix 58: 统一计算所有周期的 bar_time，支持 1m/5m/15m/30m/60m/1d/1w/1M
        始终返回字符串，确保与 last_data['time'] 的字符串值对比时类型一致。"""
        if period in ("1d", "1w", "1M"):
            return now.strftime("%Y-%m-%d")
        if period == "1m":
            return now.floor("min").strftime("%Y-%m-%d %H:%M:%S")
        if period == "5m":
            return now.floor("5min").strftime("%Y-%m-%d %H:%M:%S")
        if period == "15m":
            return now.floor("15min").strftime("%Y-%m-%d %H:%M:%S")
        if period == "30m":
            return now.floor("30min").strftime("%Y-%m-%d %H:%M:%S")
        if period == "60m":
            return now.floor("60min").strftime("%Y-%m-%d %H:%M:%S")
        return now.strftime("%Y-%m-%d %H:%M:%S")

    def _build_bar_from_quote(self, quote: dict[str, Any], period: str) -> Optional[dict[str, Any]]:
        price = float(quote.get("price") or 0)
        if price <= 0:
            return None
        now = pd.Timestamp.now()
        bar_time: Any = self._compute_bar_time(now, period)
        open_price = float(quote.get("open") or price)
        high = max(price, float(quote.get("high") or price))
        low = min(price, float(quote.get("low") or price))
        volume = float(quote.get("volume") or 0)
        return {
            "time": bar_time,
            "open": open_price,
            "high": high,
            "low": low,
            "close": price,
            "volume": volume,
        }

    def _apply_quote_to_series(self, quote: dict[str, Any]) -> Optional[dict[str, Any]]:
        price = float(quote.get("price") or 0)
        if price <= 0:
            return None

        now = pd.Timestamp.now()
        bar_time: Any = self._compute_bar_time(now, self._period)

        total_volume = float(quote.get("volume") or 0)
        if self._last_total_volume is None or total_volume < self._last_total_volume:
            self._last_total_volume = total_volume
        volume_delta = max(total_volume - (self._last_total_volume or 0), 0)
        self._last_total_volume = total_volume

        last_row = self._last_data.iloc[-1].copy()
        last_time = last_row.get("time")
        if last_time == bar_time:
            high = max(float(last_row["high"]), price, float(quote.get("high") or price))
            low = min(float(last_row["low"]), price, float(quote.get("low") or price))
            open_price = float(last_row["open"])
            volume = float(last_row.get("volume") or 0) + volume_delta
            updated = {
                "time": bar_time,
                "open": open_price,
                "high": high,
                "low": low,
                "close": price,
                "volume": volume,
            }
            for col, value in updated.items():
                self._last_data.at[self._last_data.index[-1], col] = value
        else:
            open_price = float(quote.get("open") or price)
            high = max(price, float(quote.get("high") or price))
            low = min(price, float(quote.get("low") or price))
            updated = {
                "time": bar_time,
                "open": open_price,
                "high": high,
                "low": low,
                "close": price,
                "volume": volume_delta,
            }
            self._last_data = pd.concat(
                [self._last_data, pd.DataFrame([updated])], ignore_index=True
            )
        return updated

    def update_config(
        self,
        drop_rate_threshold: Optional[float] = None,
        window_seconds: Optional[float] = None,
        alert_sustain_s: Optional[float] = None,
        flush_interval_ms: Optional[float] = None,
        max_queue: Optional[float] = None,
    ):
        """动态更新配置参数，实现热更新"""
        if drop_rate_threshold is not None:
            # 边界校验
            self._drop_rate_threshold = max(0.001, min(0.999, float(drop_rate_threshold)))
            # 恢复阈值也相应更新
            self._recovery_threshold = self._drop_rate_threshold * 0.6

        if window_seconds is not None:
            # 边界校验
            self._window_seconds = max(1.0, min(3600.0, float(window_seconds)))

        if alert_sustain_s is not None:
            # 边界校验
            self._alert_sustain_s = max(0.1, min(600.0, float(alert_sustain_s)))

        if flush_interval_ms is not None:
            # 边界校验
            self.flush_interval_s = max(0.05, float(flush_interval_ms) / 1000.0)

        if max_queue is not None:
            # 边界校验并更新最大队列长度
            new_max_queue = max(32, int(max_queue))
            if new_max_queue != self.max_queue:
                self.max_queue = new_max_queue
                # 如果当前队列长度超过新设置的限制，需要清理队列
                while len(self._queue) > self.max_queue:
                    self._queue.popleft()
                    self._dropped_quotes += 1

        # 重置窗口状态，以避免使用旧参数造成的不一致
        now = time.monotonic()
        self._window_quotes.clear()
        self._window_dropped.clear()
        self._window_exceed_since = None
        self._sustained_alert = False

    def get_config(self) -> dict[str, Any]:
        return {
            "drop_rate_threshold": self._drop_rate_threshold,
            "recovery_threshold": self._recovery_threshold,
            "window_seconds": self._window_seconds,
            "alert_sustain_s": self._alert_sustain_s,
            "flush_interval_ms": int(self.flush_interval_s * 1000),
            "max_queue": self.max_queue,
        }
