"""
结构监控面板 (StructureMonitorPanel)

N 字结构实时监控 + 回撤仪表板

功能：
  - 输入股票代码 + 周期 → 触发历史扫描（LocalRightsMappingEngine + StructureEngine）
  - 结构表：展示所有已识别 N 字结构（P0/P1/P2/P3 + 方向 + 折返深度 + 状态）
  - 信号表：展示结构化信号（LONG/SHORT/EXIT + 止损价 + 止损距离）
  - 回撤仪表板：当前回撤% / 最大回撤% / Calmar 比率（公理锚定，不可主观修改）

架构说明：
  - 数据层：unified_data_interface.py → close_raw（不复权）
  - 映射层：LocalRightsMappingEngine → close_mapped
  - 结构层：StructureEngine（公理1/2/3）→ NStructure 列表
  - 信号层：SignalGenerator + DrawdownTracker → StructuredSignal 列表
"""

from __future__ import annotations

import json
import logging
import os
from types import SimpleNamespace
from typing import Optional

from PyQt5.QtCore import QThread, Qt, pyqtSignal
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QComboBox,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QProgressBar,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from core.events import Events
from core.signal_bus import signal_bus

log = logging.getLogger(__name__)

# ── 颜色常量 ──────────────────────────────────────────────────────────────────
_C_UP = QColor(0xC8, 0xE6, 0xC9)      # 浅绿（上涨结构）
_C_DOWN = QColor(0xFF, 0xCC, 0xBC)    # 浅橙（下跌结构）
_C_EXIT = QColor(0xFF, 0xF9, 0xC4)    # 浅黄（EXIT 信号）
_C_REVERSED = QColor(0xE1, 0xBE, 0xE7)  # 浅紫（反转）
_C_HEADER = QColor(0xF5, 0xF5, 0xF5)

_PERIODS = ["1d", "5m", "1m", "15m", "30m", "60m", "5d", "10d"]
_DATA_MODES = [
    ("local_scan", "本地扫描"),
    ("api_read_only", "API只读"),
]
_STATUS_FILTERS = [
    ("", "全部状态"),
    ("active", "active"),
    ("closed", "closed"),
    ("reversed", "reversed"),
]
_SIGNAL_FILTERS = [
    ("", "全部信号"),
    ("LONG", "LONG"),
    ("SHORT", "SHORT"),
    ("EXIT", "EXIT"),
    ("HOLD", "HOLD"),
]
_BAYES_GROUP_STRATEGIES = [
    ("adaptive", "自适应Bayes"),
    ("fixed", "固定Bayes"),
]
_PAGE_LIMITS = [50, 100, 200, 500]


# ── 后台扫描线程 ──────────────────────────────────────────────────────────────

class _ScanThread(QThread):
    """在后台线程中完成：数据读取 → 局部除权映射 → 结构扫描 → 信号生成。"""

    scan_done = pyqtSignal(list, list, float, float, object)  # structures, signals, dd, max_dd, calmar

    def __init__(
        self,
        code: str,
        interval: str,
        *,
        group_strategy: str = "adaptive",
        min_observations: int = 3,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.code = code
        self.interval = interval
        self.group_strategy = group_strategy
        self.min_observations = min_observations
        self._error: Optional[str] = None
        self._close_wait_timeout_ms = 200

    error_occurred = pyqtSignal(str)

    def run(self) -> None:
        try:
            if self.isInterruptionRequested():
                return
            import pandas as pd
            from datetime import datetime, timedelta
            from data_manager.structure_bayesian_baseline import StructureBayesianBaseline
            from data_manager.unified_data_interface import UnifiedDataInterface
            from data_manager.local_rights_mapping import LocalRightsMappingEngine
            from data_manager.structure_engine import StructureEngine, PricePoint
            from data_manager.signal_generator import SignalGenerator, DrawdownTracker

            # 1. 读取原始行情（不复权），默认取近 3 年
            today = datetime.today()
            end_date = today.strftime("%Y-%m-%d")
            start_date = (today - timedelta(days=3 * 365)).strftime("%Y-%m-%d")

            udi = UnifiedDataInterface()
            if self.isInterruptionRequested():
                return
            raw_df = udi.get_stock_data(
                self.code,
                start_date=start_date,
                end_date=end_date,
                period=self.interval,
                adjust="none",
                auto_save=False,
            )
            if raw_df is None or raw_df.empty:
                self.error_occurred.emit(f"无法获取 {self.code} 的行情数据")
                return

            # 规范化列名：统一为 trade_date + close_raw
            df = raw_df.copy()
            if "trade_time" in df.columns:
                df = df.rename(columns={"trade_time": "trade_date"})
            elif "date" in df.columns:
                df = df.rename(columns={"date": "trade_date"})

            for src, dst in [("open", "open_raw"), ("high", "high_raw"),
                              ("low", "low_raw"), ("close", "close_raw"),
                              ("volume", "volume")]:
                if src in df.columns and dst not in df.columns:
                    df = df.rename(columns={src: dst})

            if "close_raw" not in df.columns:
                self.error_occurred.emit("行情数据缺少 close 列")
                return

            # trade_date 转为毫秒整数
            if df["trade_date"].dtype == object or str(df["trade_date"].dtype).startswith("datetime"):
                df["trade_date"] = pd.to_datetime(df["trade_date"]).astype("int64") // 1_000_000
            else:
                df["trade_date"] = df["trade_date"].astype("int64")

            df = df.sort_values("trade_date").reset_index(drop=True)
            df["close_raw"] = df["close_raw"].astype(float)
            if self.isInterruptionRequested():
                return

            # 2. 局部除权映射（无除权数据时 rights_factor=1.0，close_mapped=close_raw）
            engine_mapping = LocalRightsMappingEngine()
            mapped_df = engine_mapping.map(df, ex_rights_events=[])  # 首期不接入除权数据
            if self.isInterruptionRequested():
                return

            # 3. 结构扫描（公理 1/2/3）
            tracker = DrawdownTracker()
            gen = SignalGenerator(
                code=self.code,
                interval=self.interval,
                drawdown_tracker=tracker,
            )
            struct_engine = StructureEngine(
                on_structure_created=gen.on_structure_event,
                on_structure_extended=gen.on_structure_event,
                on_structure_reversed=gen.on_structure_event,
            )
            structures = struct_engine.scan(mapped_df)
            signals = gen.signals
            if self.isInterruptionRequested():
                return
            StructureBayesianBaseline().annotate_structure_objects(
                structures,
                signals,
                code=self.code,
                interval=self.interval,
                group_by=("code", "interval", "direction"),
                group_strategy=self.group_strategy,
                min_observations=self.min_observations,
            )
            dd_pct = tracker.update(
                float(mapped_df["close_mapped"].iloc[-1]),
                int(mapped_df["trade_date"].iloc[-1]),
            )
            max_dd = tracker.max_drawdown_pct
            calmar = tracker.calmar()

            self.scan_done.emit(structures, signals, dd_pct, max_dd, calmar)

        except Exception as e:
            log.exception("_ScanThread 异常")
            self.error_occurred.emit(str(e))


class _ApiReadThread(QThread):
    """从 EasyXT API 读取结构/信号只读快照，避免 GUI 侧重新扫描。"""

    scan_done = pyqtSignal(list, list, float, float, object)
    metadata_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(
        self,
        code: str,
        interval: str,
        *,
        host: str = "",
        port: int | None = None,
        api_token: str = "",
        status_filter: str = "",
        signal_type_filter: str = "",
        limit: int = 100,
        offset: int = 0,
        group_strategy: str = "adaptive",
        min_observations: int = 3,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.code = code
        self.interval = interval
        self.host = host
        self.port = port
        self.api_token = api_token
        self.status_filter = status_filter
        self.signal_type_filter = signal_type_filter
        self.limit = limit
        self.offset = offset
        self.group_strategy = group_strategy
        self.min_observations = min_observations
        self._close_wait_timeout_ms = 1000

    def run(self) -> None:
        try:
            if self.isInterruptionRequested():
                return
            from urllib.error import HTTPError, URLError
            from urllib.parse import urlencode
            from urllib.request import Request, urlopen

            host = self._normalize_host(
                self.host
                or str(os.environ.get("EASYXT_STRUCTURE_MONITOR_API_HOST", "") or "")
                or str(os.environ.get("EASYXT_API_HOST", "") or "")
                or "127.0.0.1"
            )
            port = self.port or int(str(os.environ.get("EASYXT_API_PORT", "8765") or "8765"))
            timeout_s = float(
                str(os.environ.get("EASYXT_STRUCTURE_MONITOR_API_TIMEOUT_S", "6") or "6")
            )
            token = self.api_token.strip() or str(os.environ.get("EASYXT_API_TOKEN", "") or "").strip()

            def _request_json(path: str) -> dict:
                url = f"http://{host}:{port}{path}"
                headers = {"Accept": "application/json"}
                if token:
                    headers["X-API-Token"] = token
                req = Request(url, headers=headers, method="GET")
                try:
                    with urlopen(req, timeout=timeout_s) as resp:
                        payload = resp.read().decode("utf-8", errors="replace")
                except HTTPError as exc:
                    detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
                    raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
                except URLError as exc:
                    raise RuntimeError(str(exc.reason or exc)) from exc
                try:
                    return json.loads(payload)
                except Exception as exc:
                    raise RuntimeError(f"API 返回非 JSON: {payload[:160]}") from exc

            query_params: list[tuple[str, object]] = [
                ("code", self.code),
                ("interval", self.interval),
                ("limit", self.limit),
                ("offset", self.offset),
                ("include_bayes_meta", 1),
                ("group_strategy", self.group_strategy),
                ("min_observations", self.min_observations),
            ]
            if self.status_filter:
                query_params.append(("status", self.status_filter))
            query = urlencode(query_params, doseq=True)
            structures_body = _request_json(f"/api/v1/structures/?{query}")
            if self.isInterruptionRequested():
                return
            signal_params: list[tuple[str, object]] = [
                ("code", self.code),
                ("interval", self.interval),
                ("limit", self.limit),
                ("offset", self.offset),
            ]
            if self.signal_type_filter:
                signal_params.append(("signal_type", self.signal_type_filter))
            signals_body = _request_json(f"/api/v1/signals/?{urlencode(signal_params, doseq=True)}")
            if self.isInterruptionRequested():
                return

            posterior_items: list[dict] = []
            summary_items: list[dict] = []
            try:
                posterior_params: list[tuple[str, object]] = [
                    ("code", self.code),
                    ("interval", self.interval),
                    ("group_by", "code"),
                    ("group_by", "interval"),
                    ("group_by", "direction"),
                    ("group_strategy", self.group_strategy),
                    ("min_observations", self.min_observations),
                ]
                if self.status_filter:
                    posterior_params.append(("status", self.status_filter))
                if self.signal_type_filter:
                    posterior_params.append(("signal_type", self.signal_type_filter))
                posterior_body = _request_json(
                    f"/api/v1/structures/bayesian-baseline?{urlencode(posterior_params, doseq=True)}"
                )
                posterior_items = list(posterior_body.get("items") or [])
                summary_body = _request_json(
                    f"/api/v1/structures/bayesian-baseline/summary?{urlencode(posterior_params, doseq=True)}"
                )
                summary_items = list(summary_body.get("items") or [])
            except Exception as exc:
                log.debug("_ApiReadThread Bayesian preview 失败，退回已持久化区间: %s", exc)

            structures = self._convert_structures(
                list(structures_body.get("items") or []),
                posterior_items,
            )
            signals = self._convert_signals(list(signals_body.get("items") or []))
            self.metadata_ready.emit(
                {
                    "returned": int(structures_body.get("returned") or len(structures)),
                    "offset": self.offset,
                    "limit": self.limit,
                    "posterior_items": posterior_items,
                    "summary_items": summary_items,
                    "group_strategy": self.group_strategy,
                    "min_observations": self.min_observations,
                }
            )
            dd_values = [float(sig.drawdown_pct) for sig in signals if sig.drawdown_pct is not None]
            calmar_values = [sig.calmar_snapshot for sig in signals if sig.calmar_snapshot is not None]
            current_dd = float(dd_values[-1]) if dd_values else 0.0
            max_dd = max(dd_values) if dd_values else 0.0
            calmar = calmar_values[-1] if calmar_values else None
            self.scan_done.emit(structures, signals, current_dd, max_dd, calmar)

        except Exception as e:
            log.exception("_ApiReadThread 异常")
            self.error_occurred.emit(str(e))

    @staticmethod
    def _normalize_host(host: str) -> str:
        text = str(host or "").strip()
        if text in ("", "0.0.0.0", "::"):
            return "127.0.0.1"
        return text

    @staticmethod
    def _convert_structures(items: list[dict], posterior_items: list[dict]) -> list[object]:
        posterior_by_bucket = {
            (row.get("interval"), row.get("direction")): row
            for row in posterior_items
        }
        structures: list[object] = []
        for item in items:
            layer4 = dict(item.get("layer4") or {})
            if layer4.get("bayes_lower") is None or layer4.get("bayes_upper") is None:
                bucket = posterior_by_bucket.get((item.get("interval"), item.get("direction")))
                if bucket is not None:
                    layer4["bayes_lower"] = bucket.get("bayes_lower")
                    layer4["bayes_upper"] = bucket.get("bayes_upper")
            points = item.get("points") or {}

            def _pt(name: str) -> object:
                payload = points.get(name) or {}
                price = payload.get("price")
                return SimpleNamespace(
                    ts=payload.get("ts"),
                    price=float(price) if price is not None else 0.0,
                )

            p0 = _pt("p0")
            p1 = _pt("p1")
            p2 = _pt("p2")
            p3 = _pt("p3")
            structures.append(
                SimpleNamespace(
                    struct_id=item.get("structure_id"),
                    direction=item.get("direction"),
                    status=item.get("status"),
                    p0=p0,
                    p1=p1,
                    p2=p2,
                    p3=p3,
                    retrace_ratio=item.get("retrace_ratio"),
                    bayes_lower=layer4.get("bayes_lower"),
                    bayes_upper=layer4.get("bayes_upper"),
                    posterior_mean=layer4.get("posterior_mean"),
                    observation_count=layer4.get("observation_count"),
                    continuation_count=layer4.get("continuation_count"),
                    reversal_count=layer4.get("reversal_count"),
                    bayes_group_level=layer4.get("bayes_group_level"),
                    bayes_group_key=layer4.get("bayes_group_key"),
                    stop_loss_price=p2.price,
                    stop_loss_distance=abs(p3.price - p2.price),
                )
            )
        return structures

    @staticmethod
    def _convert_signals(items: list[dict]) -> list[object]:
        signals: list[object] = []
        for item in items:
            risk = item.get("risk") or {}
            drawdown_pct = risk.get("drawdown_pct")
            calmar_snapshot = risk.get("calmar_snapshot")
            signals.append(
                SimpleNamespace(
                    signal_id=item.get("signal_id"),
                    structure_id=item.get("structure_id"),
                    signal_ts=item.get("signal_ts"),
                    signal_type=item.get("signal_type"),
                    trigger_price=float(item.get("trigger_price") or 0.0),
                    stop_loss_price=float(risk.get("stop_loss_price") or 0.0),
                    stop_loss_distance=float(risk.get("stop_loss_distance") or 0.0),
                    drawdown_pct=(float(drawdown_pct) if drawdown_pct is not None else None),
                    calmar_snapshot=(
                        float(calmar_snapshot) if calmar_snapshot is not None else None
                    ),
                )
            )
        return signals


class _BayesianApiThread(QThread):
    """触发 Bayesian preview / apply / summary，避免阻塞 GUI。"""

    completed = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(
        self,
        *,
        action: str,
        code: str,
        interval: str,
        host: str = "",
        port: int | None = None,
        api_token: str = "",
        status_filter: str = "",
        signal_type_filter: str = "",
        group_strategy: str = "adaptive",
        min_observations: int = 3,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.action = action
        self.code = code
        self.interval = interval
        self.host = host
        self.port = port
        self.api_token = api_token
        self.status_filter = status_filter
        self.signal_type_filter = signal_type_filter
        self.group_strategy = group_strategy
        self.min_observations = min_observations
        self._close_wait_timeout_ms = 1000

    def run(self) -> None:
        try:
            if self.isInterruptionRequested():
                return
            from urllib.error import HTTPError, URLError
            from urllib.parse import urlencode
            from urllib.request import Request, urlopen

            host = _ApiReadThread._normalize_host(
                self.host
                or str(os.environ.get("EASYXT_STRUCTURE_MONITOR_API_HOST", "") or "")
                or str(os.environ.get("EASYXT_API_HOST", "") or "")
                or "127.0.0.1"
            )
            port = self.port or int(str(os.environ.get("EASYXT_API_PORT", "8765") or "8765"))
            timeout_s = float(
                str(os.environ.get("EASYXT_STRUCTURE_MONITOR_API_TIMEOUT_S", "6") or "6")
            )
            token = self.api_token.strip() or str(os.environ.get("EASYXT_API_TOKEN", "") or "").strip()

            def _request_json(path: str, *, method: str) -> dict:
                url = f"http://{host}:{port}{path}"
                headers = {"Accept": "application/json"}
                if token:
                    headers["X-API-Token"] = token
                req = Request(url, headers=headers, method=method)
                try:
                    with urlopen(req, timeout=timeout_s) as resp:
                        payload = resp.read().decode("utf-8", errors="replace")
                except HTTPError as exc:
                    detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
                    raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
                except URLError as exc:
                    raise RuntimeError(str(exc.reason or exc)) from exc
                return json.loads(payload)

            params: list[tuple[str, object]] = [
                ("code", self.code),
                ("interval", self.interval),
                ("group_by", "code"),
                ("group_by", "interval"),
                ("group_by", "direction"),
                ("group_strategy", self.group_strategy),
                ("min_observations", self.min_observations),
            ]
            if self.status_filter:
                params.append(("status", self.status_filter))
            if self.signal_type_filter:
                params.append(("signal_type", self.signal_type_filter))
            query = urlencode(params, doseq=True)

            if self.action == "apply":
                result = _request_json(
                    f"/api/v1/structures/bayesian-baseline/apply?{query}",
                    method="POST",
                )
            else:
                result = _request_json(
                    f"/api/v1/structures/bayesian-baseline?{query}",
                    method="GET",
                )
            if self.isInterruptionRequested():
                return
            summary = _request_json(
                f"/api/v1/structures/bayesian-baseline/summary?{query}",
                method="GET",
            )
            result["summary"] = summary
            result["action"] = self.action
            self.completed.emit(result)
        except Exception as exc:
            log.exception("_BayesianApiThread 异常")
            self.error_occurred.emit(str(exc))


class _StructureDetailThread(QThread):
    """按 structure_id 拉取结构详情、最新信号和审计摘要。"""

    completed = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(
        self,
        *,
        structure_id: str,
        host: str = "",
        port: int | None = None,
        api_token: str = "",
        audit_limit: int = 20,
        group_strategy: str = "adaptive",
        min_observations: int = 3,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.structure_id = structure_id
        self.host = host
        self.port = port
        self.api_token = api_token
        self.audit_limit = audit_limit
        self.group_strategy = group_strategy
        self.min_observations = min_observations
        self._close_wait_timeout_ms = 1000

    def run(self) -> None:
        try:
            if self.isInterruptionRequested():
                return
            from urllib.error import HTTPError, URLError
            from urllib.parse import urlencode
            from urllib.request import Request, urlopen

            host = _ApiReadThread._normalize_host(
                self.host
                or str(os.environ.get("EASYXT_STRUCTURE_MONITOR_API_HOST", "") or "")
                or str(os.environ.get("EASYXT_API_HOST", "") or "")
                or "127.0.0.1"
            )
            port = self.port or int(str(os.environ.get("EASYXT_API_PORT", "8765") or "8765"))
            timeout_s = float(
                str(os.environ.get("EASYXT_STRUCTURE_MONITOR_API_TIMEOUT_S", "6") or "6")
            )
            token = self.api_token.strip() or str(os.environ.get("EASYXT_API_TOKEN", "") or "").strip()
            params = urlencode(
                {
                    "audit_limit": self.audit_limit,
                    "include_bayes_meta": 1,
                    "group_strategy": self.group_strategy,
                    "min_observations": self.min_observations,
                }
            )
            url = f"http://{host}:{port}/api/v1/structures/{self.structure_id}/detail?{params}"
            headers = {"Accept": "application/json"}
            if token:
                headers["X-API-Token"] = token
            req = Request(url, headers=headers, method="GET")
            try:
                with urlopen(req, timeout=timeout_s) as resp:
                    payload = resp.read().decode("utf-8", errors="replace")
            except HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
                raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
            except URLError as exc:
                raise RuntimeError(str(exc.reason or exc)) from exc
            if self.isInterruptionRequested():
                return
            self.completed.emit(json.loads(payload))
        except Exception as exc:
            log.exception("_StructureDetailThread 异常")
            self.error_occurred.emit(str(exc))


# ── 主面板 ────────────────────────────────────────────────────────────────────

class StructureMonitorPanel(QWidget):
    """结构监控面板 — N 字结构识别结果展示 + 回撤仪表板。"""

    def __init__(
        self,
        parent=None,
        *,
        test_mode: Optional[bool] = None,
        allow_test_scan: bool = False,
        data_mode: str = "local_scan",
    ) -> None:
        super().__init__(parent)
        self.test_mode = (
            bool(os.environ.get("PYTEST_CURRENT_TEST"))
            if test_mode is None
            else bool(test_mode)
        )
        self._allow_test_scan = allow_test_scan
        self._data_mode = self._normalize_data_mode(data_mode)
        self._scan_thread: Optional[_ScanThread] = None
        self._bayes_action_thread: Optional[_BayesianApiThread] = None
        self._detail_thread: Optional[_StructureDetailThread] = None
        self._structures: list = []
        self._signals: list = []
        self._page_offset = 0
        self._page_limit = 100
        self._last_api_returned = 0
        self._last_bayes_summary: list[dict] = []
        self._setup_ui()

    # ── UI 构建 ───────────────────────────────────────────────────────────────

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        # 顶部工具栏
        root.addWidget(self._build_toolbar())

        # 状态 / 进度条
        self._status_label = QLabel("就绪")
        self._status_label.setStyleSheet("color:#555; font-size:12px;")
        self._progress = QProgressBar()
        self._progress.setFixedHeight(6)
        self._progress.setTextVisible(False)
        self._progress.setRange(0, 0)  # indeterminate
        self._progress.setVisible(False)
        status_row = QHBoxLayout()
        status_row.addWidget(self._status_label)
        status_row.addWidget(self._progress)
        root.addLayout(status_row)
        self._api_hint_label = QLabel("只读摘要: —")
        self._api_hint_label.setStyleSheet("color:#607D8B; font-size:11px;")
        root.addWidget(self._api_hint_label)

        # 回撤仪表板
        root.addWidget(self._build_drawdown_dashboard())

        # 主内容区（结构表 | 信号表）
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self._build_structure_table())
        splitter.addWidget(self._build_signal_table())
        splitter.setSizes([600, 400])
        root.addWidget(splitter, stretch=1)
        root.addWidget(self._build_structure_detail_panel())
        self._on_data_mode_changed(self._data_mode_combo.currentIndex())

    def _build_toolbar(self) -> QWidget:
        bar = QWidget()
        layout = QHBoxLayout(bar)
        layout.setContentsMargins(0, 0, 0, 0)

        layout.addWidget(QLabel("股票代码:"))
        self._code_edit = QLineEdit("000001.SZ")
        self._code_edit.setFixedWidth(120)
        layout.addWidget(self._code_edit)

        layout.addWidget(QLabel("数据源:"))
        self._data_mode_combo = QComboBox()
        for value, label in _DATA_MODES:
            self._data_mode_combo.addItem(label, value)
        idx = self._data_mode_combo.findData(self._data_mode)
        self._data_mode_combo.setCurrentIndex(max(idx, 0))
        self._data_mode_combo.setFixedWidth(90)
        self._data_mode_combo.currentIndexChanged.connect(self._on_data_mode_changed)
        layout.addWidget(self._data_mode_combo)

        layout.addWidget(QLabel("结构状态:"))
        self._status_filter_combo = QComboBox()
        for value, label in _STATUS_FILTERS:
            self._status_filter_combo.addItem(label, value)
        self._status_filter_combo.setFixedWidth(92)
        self._status_filter_combo.currentIndexChanged.connect(self._on_readonly_filter_changed)
        layout.addWidget(self._status_filter_combo)

        layout.addWidget(QLabel("周期:"))
        self._period_combo = QComboBox()
        self._period_combo.addItems(_PERIODS)
        self._period_combo.setFixedWidth(80)
        layout.addWidget(self._period_combo)

        layout.addWidget(QLabel("突破阈值:"))
        self._break_edit = QLineEdit("0.001")
        self._break_edit.setFixedWidth(70)
        layout.addWidget(self._break_edit)

        layout.addWidget(QLabel("信号过滤:"))
        self._signal_filter_combo = QComboBox()
        for value, label in _SIGNAL_FILTERS:
            self._signal_filter_combo.addItem(label, value)
        self._signal_filter_combo.setFixedWidth(88)
        self._signal_filter_combo.currentIndexChanged.connect(self._on_readonly_filter_changed)
        layout.addWidget(self._signal_filter_combo)

        layout.addWidget(QLabel("每页:"))
        self._page_limit_combo = QComboBox()
        for item in _PAGE_LIMITS:
            self._page_limit_combo.addItem(str(item), item)
        self._page_limit_combo.setCurrentIndex(_PAGE_LIMITS.index(self._page_limit))
        self._page_limit_combo.setFixedWidth(68)
        self._page_limit_combo.currentIndexChanged.connect(self._on_page_limit_changed)
        layout.addWidget(self._page_limit_combo)

        layout.addWidget(QLabel("Bayes:"))
        self._bayes_group_strategy_combo = QComboBox()
        default_strategy = str(
            os.environ.get("EASYXT_STRUCTURE_MONITOR_BAYES_GROUP_STRATEGY", "adaptive")
            or "adaptive"
        )
        for value, label in _BAYES_GROUP_STRATEGIES:
            self._bayes_group_strategy_combo.addItem(label, value)
        idx = self._bayes_group_strategy_combo.findData(default_strategy)
        self._bayes_group_strategy_combo.setCurrentIndex(max(idx, 0))
        self._bayes_group_strategy_combo.setFixedWidth(104)
        self._bayes_group_strategy_combo.currentIndexChanged.connect(self._on_readonly_filter_changed)
        layout.addWidget(self._bayes_group_strategy_combo)

        self._bayes_min_obs_edit = QLineEdit(
            str(os.environ.get("EASYXT_STRUCTURE_MONITOR_BAYES_MIN_OBS", "3") or "3")
        )
        self._bayes_min_obs_edit.setFixedWidth(40)
        layout.addWidget(self._bayes_min_obs_edit)

        self._scan_btn = QPushButton("开始扫描")
        self._scan_btn.setFixedWidth(90)
        self._scan_btn.clicked.connect(self._start_scan)
        layout.addWidget(self._scan_btn)

        self._bayes_preview_btn = QPushButton("Bayes预览")
        self._bayes_preview_btn.setFixedWidth(82)
        self._bayes_preview_btn.clicked.connect(lambda: self._start_bayesian_action("preview"))
        layout.addWidget(self._bayes_preview_btn)

        self._bayes_apply_btn = QPushButton("Bayes写回")
        self._bayes_apply_btn.setFixedWidth(82)
        self._bayes_apply_btn.clicked.connect(lambda: self._start_bayesian_action("apply"))
        layout.addWidget(self._bayes_apply_btn)

        self._prev_page_btn = QPushButton("上一页")
        self._prev_page_btn.setFixedWidth(66)
        self._prev_page_btn.clicked.connect(self._load_prev_page)
        layout.addWidget(self._prev_page_btn)

        self._next_page_btn = QPushButton("下一页")
        self._next_page_btn.setFixedWidth(66)
        self._next_page_btn.clicked.connect(self._load_next_page)
        layout.addWidget(self._next_page_btn)

        self._page_label = QLabel("第 1 页")
        self._page_label.setStyleSheet("color:#78909C; font-size:11px;")
        layout.addWidget(self._page_label)

        layout.addStretch()
        return bar

    def _build_drawdown_dashboard(self) -> QGroupBox:
        box = QGroupBox("回撤仪表板（公理锚定，止损锚点 = P2）")
        box.setStyleSheet("QGroupBox { font-weight: bold; font-size: 12px; }")
        box.setFixedHeight(96)
        grid = QGridLayout(box)
        grid.setSpacing(10)

        labels = [
            ("当前回撤", "—", "#E53935"),
            ("历史最大回撤", "—", "#BF360C"),
            ("Calmar 比率", "—", "#1565C0"),
            ("当前结构方向", "—", "#2E7D32"),
            ("当前止损价 (P2)", "—", "#6A1B9A"),
            ("止损距离", "—", "#E65100"),
            ("当前贝叶斯区间", "—", "#283593"),
            ("当前贝叶斯均值", "—", "#3949AB"),
            ("Bayes 桶级别", "—", "#00897B"),
            ("Bayes 样本数", "—", "#5D4037"),
        ]
        self._dd_labels: dict[str, QLabel] = {}
        for col, (title, init_val, color) in enumerate(labels):
            t_lbl = QLabel(title)
            t_lbl.setStyleSheet("color:#777; font-size:11px;")
            t_lbl.setAlignment(Qt.AlignCenter)
            v_lbl = QLabel(init_val)
            v_lbl.setStyleSheet(f"color:{color}; font-size:16px; font-weight:bold;")
            v_lbl.setAlignment(Qt.AlignCenter)
            grid.addWidget(t_lbl, 0, col)
            grid.addWidget(v_lbl, 1, col)
            self._dd_labels[title] = v_lbl

        return box

    def _build_structure_table(self) -> QGroupBox:
        box = QGroupBox("N 字结构列表（公理1/2/3 识别结果）")
        box.setStyleSheet("QGroupBox { font-weight: bold; font-size: 12px; }")
        layout = QVBoxLayout(box)

        headers = [
            "方向", "P0 价", "P1 价", "P2 价（止损）",
            "P3 价（驱动）", "折返深度", "Bayes 下界", "Bayes 上界", "状态",
        ]
        self._struct_table = QTableWidget(0, len(headers))
        self._struct_table.setHorizontalHeaderLabels(headers)
        self._struct_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._struct_table.setAlternatingRowColors(True)
        self._struct_table.horizontalHeader().setStretchLastSection(True)
        self._struct_table.verticalHeader().hide()
        self._struct_table.setSelectionBehavior(QTableWidget.SelectRows)
        self._struct_table.itemSelectionChanged.connect(self._on_structure_selection_changed)
        layout.addWidget(self._struct_table)
        return box

    def _build_structure_detail_panel(self) -> QGroupBox:
        box = QGroupBox("结构详情联动（选中结构后查看审计 / Layer 4 / 最新信号）")
        box.setStyleSheet("QGroupBox { font-weight: bold; font-size: 12px; }")
        box.setFixedHeight(220)
        layout = QVBoxLayout(box)
        self._detail_header_label = QLabel("未选中结构")
        self._detail_header_label.setStyleSheet("color:#ECEFF1; font-weight:bold; font-size:12px;")
        self._detail_bayes_label = QLabel("Layer 4: —")
        self._detail_bayes_label.setStyleSheet("color:#90CAF9; font-size:11px;")
        self._detail_signal_label = QLabel("最新信号: —")
        self._detail_signal_label.setStyleSheet("color:#A5D6A7; font-size:11px;")
        self._detail_audit_summary_label = QLabel("审计摘要: —")
        self._detail_audit_summary_label.setStyleSheet("color:#FFCC80; font-size:11px;")
        layout.addWidget(self._detail_header_label)
        layout.addWidget(self._detail_bayes_label)
        layout.addWidget(self._detail_signal_label)
        layout.addWidget(self._detail_audit_summary_label)

        self._audit_table = QTableWidget(0, 5)
        self._audit_table.setHorizontalHeaderLabels(["事件", "时间", "状态", "方向", "快照摘要"])
        self._audit_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._audit_table.setAlternatingRowColors(True)
        self._audit_table.horizontalHeader().setStretchLastSection(True)
        self._audit_table.verticalHeader().hide()
        self._audit_table.setSelectionBehavior(QTableWidget.SelectRows)
        layout.addWidget(self._audit_table)
        return box

    def _build_signal_table(self) -> QGroupBox:
        box = QGroupBox("结构化信号（LONG / SHORT / EXIT）")
        box.setStyleSheet("QGroupBox { font-weight: bold; font-size: 12px; }")
        layout = QVBoxLayout(box)

        headers = [
            "信号类型", "触发价", "止损价 (P2)",
            "止损距离", "回撤 %", "Calmar",
        ]
        self._signal_table = QTableWidget(0, len(headers))
        self._signal_table.setHorizontalHeaderLabels(headers)
        self._signal_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._signal_table.setAlternatingRowColors(True)
        self._signal_table.horizontalHeader().setStretchLastSection(True)
        self._signal_table.verticalHeader().hide()
        self._signal_table.setSelectionBehavior(QTableWidget.SelectRows)
        layout.addWidget(self._signal_table)
        return box

    # ── 扫描逻辑 ─────────────────────────────────────────────────────────────

    def _start_scan(self) -> None:
        if self._scan_thread is not None and self._scan_thread.isRunning():
            return

        if self.test_mode and not self._allow_test_scan:
            self._status_label.setText("测试模式：跳过真实扫描")
            return

        code = self._code_edit.text().strip()
        interval = self._period_combo.currentText()
        data_mode = self._current_data_mode()

        if not code:
            self._status_label.setText("请输入股票代码")
            return

        self._scan_btn.setEnabled(False)
        self._progress.setVisible(True)
        action_text = "正在从 API 读取" if data_mode == "api_read_only" else "正在扫描"
        self._status_label.setText(f"{action_text} {code} {interval} ...")
        self._struct_table.setRowCount(0)
        self._signal_table.setRowCount(0)

        if data_mode == "api_read_only":
            thread = _ApiReadThread(
                code,
                interval,
                status_filter=self._current_status_filter(),
                signal_type_filter=self._current_signal_filter(),
                limit=self._current_page_limit(),
                offset=self._page_offset,
                group_strategy=self._current_bayes_group_strategy(),
                min_observations=self._current_min_observations(),
                parent=self,
            )
            thread.metadata_ready.connect(self._on_api_metadata)
        else:
            thread = _ScanThread(
                code,
                interval,
                group_strategy=self._current_bayes_group_strategy(),
                min_observations=self._current_min_observations(),
                parent=self,
            )
        thread.scan_done.connect(self._on_scan_done)
        thread.error_occurred.connect(self._on_scan_error)
        thread.finished.connect(lambda: self._scan_btn.setEnabled(True))
        thread.finished.connect(lambda: self._progress.setVisible(False))
        self._scan_thread = thread
        thread.start()

    @staticmethod
    def _normalize_data_mode(data_mode: str) -> str:
        allowed = {item[0] for item in _DATA_MODES}
        return data_mode if data_mode in allowed else "local_scan"

    def _current_data_mode(self) -> str:
        if hasattr(self, "_data_mode_combo") and self._data_mode_combo is not None:
            mode = self._data_mode_combo.currentData()
            if isinstance(mode, str):
                return self._normalize_data_mode(mode)
        return self._data_mode

    def _current_status_filter(self) -> str:
        value = self._status_filter_combo.currentData()
        return value if isinstance(value, str) else ""

    def _current_signal_filter(self) -> str:
        value = self._signal_filter_combo.currentData()
        return value if isinstance(value, str) else ""

    def _current_page_limit(self) -> int:
        value = self._page_limit_combo.currentData()
        return int(value) if value is not None else self._page_limit

    def _current_bayes_group_strategy(self) -> str:
        value = self._bayes_group_strategy_combo.currentData()
        return value if isinstance(value, str) else "adaptive"

    def _current_min_observations(self) -> int:
        text = self._bayes_min_obs_edit.text().strip()
        try:
            return max(1, int(text))
        except Exception:
            self._bayes_min_obs_edit.setText("3")
            return 3

    def _update_readonly_controls(self) -> None:
        readonly = self._data_mode == "api_read_only"
        for widget in (
            self._status_filter_combo,
            self._signal_filter_combo,
            self._page_limit_combo,
            self._bayes_group_strategy_combo,
            self._bayes_min_obs_edit,
            self._bayes_preview_btn,
            self._bayes_apply_btn,
            self._prev_page_btn,
            self._next_page_btn,
            self._page_label,
        ):
            widget.setEnabled(readonly)
        if readonly:
            self._update_page_label()
        else:
            self._api_hint_label.setText("只读摘要: 本地扫描模式")

    def _reset_api_pagination(self) -> None:
        self._page_offset = 0
        self._last_api_returned = 0
        self._update_page_label()

    def _update_page_label(self) -> None:
        page = self._page_offset // max(1, self._current_page_limit()) + 1
        self._page_label.setText(f"第 {page} 页")
        self._prev_page_btn.setEnabled(self._data_mode == "api_read_only" and self._page_offset > 0)
        self._next_page_btn.setEnabled(
            self._data_mode == "api_read_only"
            and self._last_api_returned >= self._current_page_limit() > 0
        )

    def _on_readonly_filter_changed(self, _index: int) -> None:
        self._reset_api_pagination()
        if self._data_mode == "api_read_only":
            self._api_hint_label.setText("只读摘要: 条件已更新，点击“刷新只读”重新查询")

    def _on_page_limit_changed(self, _index: int) -> None:
        self._page_limit = self._current_page_limit()
        self._reset_api_pagination()

    def _load_prev_page(self) -> None:
        if self._data_mode != "api_read_only":
            return
        self._page_offset = max(0, self._page_offset - self._current_page_limit())
        self._update_page_label()
        self._start_scan()

    def _load_next_page(self) -> None:
        if self._data_mode != "api_read_only":
            return
        if self._last_api_returned < self._current_page_limit():
            return
        self._page_offset += self._current_page_limit()
        self._update_page_label()
        self._start_scan()

    def _on_data_mode_changed(self, _index: int) -> None:
        mode = self._current_data_mode()
        self._data_mode = mode
        if not hasattr(self, "_scan_btn"):
            return
        if mode == "api_read_only":
            self._scan_btn.setText("刷新只读")
            if hasattr(self, "_status_label"):
                self._status_label.setText("就绪（API 只读模式）")
            self._api_hint_label.setText("只读摘要: 等待查询")
        else:
            self._scan_btn.setText("开始扫描")
            if hasattr(self, "_status_label"):
                self._status_label.setText("就绪")
            self._api_hint_label.setText("只读摘要: 本地扫描模式")
        self._update_readonly_controls()

    def _on_scan_done(
        self,
        structures: list,
        signals: list,
        dd_pct: float,
        max_dd: float,
        calmar,
    ) -> None:
        self._structures = structures
        self._signals = signals
        self._last_api_returned = len(structures) if self._data_mode == "api_read_only" else 0
        self._update_page_label()
        self._struct_table.clearSelection()
        self._clear_structure_detail("请选择一行结构查看审计与 Layer 4 详情")

        # ── 更新仪表板 ──
        self._dd_labels["当前回撤"].setText(f"{dd_pct:.2f}%")
        self._dd_labels["历史最大回撤"].setText(f"{max_dd:.2f}%")
        self._dd_labels["Calmar 比率"].setText(
            f"{calmar:.2f}" if calmar is not None else "—"
        )

        if structures:
            last = structures[-1]
            direction_cn = "↑ 上涨" if last.direction == "up" else "↓ 下跌"
            self._dd_labels["当前结构方向"].setText(direction_cn)
            self._dd_labels["当前止损价 (P2)"].setText(f"{last.stop_loss_price:.4f}")
            self._dd_labels["止损距离"].setText(f"{last.stop_loss_distance:.4f}")
            if last.bayes_lower is not None and last.bayes_upper is not None:
                self._dd_labels["当前贝叶斯区间"].setText(
                    f"{last.bayes_lower:.3f} ~ {last.bayes_upper:.3f}"
                )
            else:
                self._dd_labels["当前贝叶斯区间"].setText("—")
            self._dd_labels["当前贝叶斯均值"].setText(
                f"{last.posterior_mean:.3f}" if getattr(last, "posterior_mean", None) is not None else "—"
            )
            self._dd_labels["Bayes 桶级别"].setText(
                str(getattr(last, "bayes_group_level", None) or "—")
            )
            obs_count = getattr(last, "observation_count", None)
            self._dd_labels["Bayes 样本数"].setText(
                str(int(obs_count)) if obs_count is not None else "—"
            )
        else:
            for k in (
                "当前结构方向",
                "当前止损价 (P2)",
                "止损距离",
                "当前贝叶斯区间",
                "当前贝叶斯均值",
                "Bayes 桶级别",
                "Bayes 样本数",
            ):
                self._dd_labels[k].setText("—")

        # ── 填充结构表 ──
        self._struct_table.setRowCount(len(structures))
        for row, s in enumerate(structures):
            color = _C_UP if s.direction == "up" else _C_DOWN
            cells = [
                ("↑ 上涨" if s.direction == "up" else "↓ 下跌"),
                f"{s.p0.price:.4f}",
                f"{s.p1.price:.4f}",
                f"{s.p2.price:.4f}",
                f"{s.p3.price:.4f}",
                f"{s.retrace_ratio:.3f}" if s.retrace_ratio is not None else "—",
                f"{s.bayes_lower:.3f}" if s.bayes_lower is not None else "—",
                f"{s.bayes_upper:.3f}" if s.bayes_upper is not None else "—",
                s.status,
            ]
            for col, text in enumerate(cells):
                item = QTableWidgetItem(text)
                item.setBackground(
                    _C_REVERSED if s.status == "reversed" else color
                )
                item.setTextAlignment(Qt.AlignCenter)
                self._struct_table.setItem(row, col, item)
        self._struct_table.scrollToBottom()

        # ── 填充信号表 ──
        self._signal_table.setRowCount(len(signals))
        for row, sig in enumerate(signals):
            if sig.signal_type == "LONG":
                color = _C_UP
            elif sig.signal_type == "SHORT":
                color = _C_DOWN
            elif sig.signal_type == "EXIT":
                color = _C_EXIT
            else:
                color = _C_REVERSED

            cells = [
                sig.signal_type,
                f"{sig.trigger_price:.4f}",
                f"{sig.stop_loss_price:.4f}",
                f"{sig.stop_loss_distance:.4f}",
                f"{sig.drawdown_pct:.2f}%" if sig.drawdown_pct is not None else "—",
                f"{sig.calmar_snapshot:.2f}" if sig.calmar_snapshot is not None else "—",
            ]
            for col, text in enumerate(cells):
                item = QTableWidgetItem(text)
                item.setBackground(color)
                item.setTextAlignment(Qt.AlignCenter)
                self._signal_table.setItem(row, col, item)
        self._signal_table.scrollToBottom()

        self._status_label.setText(
            f"扫描完成：{len(structures)} 个结构，{len(signals)} 条信号"
        )

    def _on_scan_error(self, msg: str) -> None:
        friendly = self._format_api_error(msg) if self._data_mode == "api_read_only" else msg
        self._status_label.setText(f"错误: {friendly}")
        if self._data_mode == "api_read_only":
            self._api_hint_label.setText(f"只读摘要: {friendly}")
        log.error("StructureMonitorPanel 扫描错误: %s", msg)

    def _on_api_metadata(self, payload: dict) -> None:
        self._last_api_returned = int(payload.get("returned") or 0)
        self._update_page_label()
        self._last_bayes_summary = list(payload.get("summary_items") or [])
        if self._last_bayes_summary:
            top = self._last_bayes_summary[0]
            self._api_hint_label.setText(
                "只读摘要: "
                f"strategy={payload.get('group_strategy')} "
                f"level={top.get('bayes_group_level')} "
                f"结构={int(top.get('structure_count') or 0)} "
                f"后验均值={float(top.get('mean_posterior_mean') or 0.0):.3f} "
                f"审计均值={float(top.get('mean_audit_event_count') or 0.0):.2f}"
            )
        elif self._data_mode == "api_read_only":
            self._api_hint_label.setText("只读摘要: 当前条件下无 Bayes 摘要")

    def _start_bayesian_action(self, action: str) -> None:
        if self._data_mode != "api_read_only":
            self._api_hint_label.setText("只读摘要: 仅 API 只读模式支持 Bayes 预览/写回")
            return
        if self._bayes_action_thread is not None and self._bayes_action_thread.isRunning():
            return
        code = self._code_edit.text().strip()
        interval = self._period_combo.currentText()
        if not code:
            self._status_label.setText("请输入股票代码")
            return
        self._bayes_preview_btn.setEnabled(False)
        self._bayes_apply_btn.setEnabled(False)
        self._api_hint_label.setText(
            "只读摘要: 正在执行 Bayes 写回..." if action == "apply" else "只读摘要: 正在预览 Bayes..."
        )
        thread = _BayesianApiThread(
            action=action,
            code=code,
            interval=interval,
            status_filter=self._current_status_filter(),
            signal_type_filter=self._current_signal_filter(),
            group_strategy=self._current_bayes_group_strategy(),
            min_observations=self._current_min_observations(),
            parent=self,
        )
        thread.completed.connect(self._on_bayesian_action_done)
        thread.error_occurred.connect(self._on_scan_error)
        thread.finished.connect(lambda: self._bayes_preview_btn.setEnabled(self._data_mode == "api_read_only"))
        thread.finished.connect(lambda: self._bayes_apply_btn.setEnabled(self._data_mode == "api_read_only"))
        self._bayes_action_thread = thread
        thread.start()

    def _on_bayesian_action_done(self, payload: dict) -> None:
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        top_items = list(summary.get("items") or [])
        action = str(payload.get("action") or "preview")
        if top_items:
            top = top_items[0]
            self._api_hint_label.setText(
                "只读摘要: "
                f"{action} 完成 level={top.get('bayes_group_level')} "
                f"结构={int(top.get('structure_count') or 0)} "
                f"后验均值={float(top.get('mean_posterior_mean') or 0.0):.3f}"
            )
        else:
            self._api_hint_label.setText(f"只读摘要: {action} 完成，但无摘要结果")
        if action == "apply":
            updated = int(payload.get("updated") or 0)
            self._status_label.setText(f"Bayesian 写回完成：updated={updated}")
            self._start_scan()
        else:
            returned = int(payload.get("returned") or 0)
            self._status_label.setText(f"Bayesian 预览完成：{returned} 个桶")

    def _clear_structure_detail(self, reason: str = "未选中结构") -> None:
        self._detail_header_label.setText(reason)
        self._detail_bayes_label.setText("Layer 4: —")
        self._detail_signal_label.setText("最新信号: —")
        self._detail_audit_summary_label.setText("审计摘要: —")
        self._audit_table.setRowCount(0)

    def _on_structure_selection_changed(self) -> None:
        row = self._struct_table.currentRow()
        if row < 0 or row >= len(self._structures):
            self._clear_structure_detail("未选中结构")
            return
        structure = self._structures[row]
        self._render_selected_structure_detail(structure)
        struct_id = getattr(structure, "struct_id", None)
        if self._data_mode == "api_read_only" and struct_id:
            self._load_structure_detail(str(struct_id))
        elif self._data_mode != "api_read_only":
            self._detail_audit_summary_label.setText("审计摘要: 本地扫描模式默认不拉取审计日志")

    def _render_selected_structure_detail(self, structure: object) -> None:
        struct_id = getattr(structure, "struct_id", "—") or "—"
        direction = getattr(structure, "direction", "—") or "—"
        status = getattr(structure, "status", "—") or "—"
        self._detail_header_label.setText(
            f"结构 #{struct_id} | direction={direction} | status={status}"
        )
        posterior_mean = getattr(structure, "posterior_mean", None)
        bayes_lower = getattr(structure, "bayes_lower", None)
        bayes_upper = getattr(structure, "bayes_upper", None)
        observation_count = getattr(structure, "observation_count", None)
        continuation_count = getattr(structure, "continuation_count", None)
        reversal_count = getattr(structure, "reversal_count", None)
        group_level = getattr(structure, "bayes_group_level", None) or "—"
        group_key = getattr(structure, "bayes_group_key", None) or "—"
        mean_text = f"{posterior_mean:.3f}" if posterior_mean is not None else "—"
        lower_text = f"{bayes_lower:.3f}" if bayes_lower is not None else "—"
        upper_text = f"{bayes_upper:.3f}" if bayes_upper is not None else "—"
        self._detail_bayes_label.setText(
            "Layer 4: "
            f"mean={mean_text} interval={lower_text}~{upper_text} "
            f"obs={observation_count if observation_count is not None else '—'} "
            f"cont/rev={continuation_count if continuation_count is not None else '—'}/"
            f"{reversal_count if reversal_count is not None else '—'} "
            f"group={group_level} key={group_key}"
        )
        latest_signal = self._find_latest_signal_for_structure(struct_id)
        if latest_signal is not None:
            dd = getattr(latest_signal, "drawdown_pct", None)
            calmar = getattr(latest_signal, "calmar_snapshot", None)
            dd_text = f"{dd:.2f}%" if dd is not None else "—"
            calmar_text = f"{calmar:.2f}" if calmar is not None else "—"
            self._detail_signal_label.setText(
                "最新信号: "
                f"{getattr(latest_signal, 'signal_type', '—')} "
                f"trigger={getattr(latest_signal, 'trigger_price', 0.0):.4f} "
                f"stop={getattr(latest_signal, 'stop_loss_price', 0.0):.4f} "
                f"dd={dd_text} calmar={calmar_text}"
            )
        else:
            self._detail_signal_label.setText("最新信号: 当前结构未匹配到信号")

    def _find_latest_signal_for_structure(self, structure_id: object) -> object | None:
        if not structure_id:
            return None
        matches = [
            sig for sig in self._signals
            if str(getattr(sig, "structure_id", "")) == str(structure_id)
        ]
        if not matches:
            return None
        matches.sort(
            key=lambda sig: (
                getattr(sig, "signal_ts", -1),
                1 if getattr(sig, "signal_type", "") == "EXIT" else 0,
                str(getattr(sig, "signal_id", "")),
            ),
            reverse=True,
        )
        return matches[0]

    def _load_structure_detail(self, structure_id: str) -> None:
        if self._detail_thread is not None and self._detail_thread.isRunning():
            self._detail_thread.requestInterruption()
            self._detail_thread.quit()
        self._detail_audit_summary_label.setText("审计摘要: 正在加载结构详情…")
        thread = _StructureDetailThread(
            structure_id=structure_id,
            audit_limit=20,
            group_strategy=self._current_bayes_group_strategy(),
            min_observations=self._current_min_observations(),
            parent=self,
        )
        thread.completed.connect(self._on_structure_detail_loaded)
        thread.error_occurred.connect(self._on_structure_detail_error)
        self._detail_thread = thread
        thread.start()

    def _on_structure_detail_loaded(self, payload: dict) -> None:
        structure = payload.get("structure") if isinstance(payload.get("structure"), dict) else {}
        layer4 = structure.get("layer4") if isinstance(structure, dict) else {}
        summary = payload.get("audit_summary") if isinstance(payload.get("audit_summary"), dict) else {}
        last_event_type = summary.get("last_event_type") or "N/A"
        last_event_ts = summary.get("last_event_ts") or "N/A"
        self._detail_audit_summary_label.setText(
            "审计摘要: "
            f"events={int(summary.get('audit_event_count') or 0)} "
            f"create/extend/reverse={int(summary.get('create_event_count') or 0)}/"
            f"{int(summary.get('extend_event_count') or 0)}/{int(summary.get('reverse_event_count') or 0)} "
            f"last={last_event_type}@{last_event_ts}"
        )
        if isinstance(layer4, dict) and layer4.get("posterior_mean") is not None:
            self._detail_bayes_label.setText(
                "Layer 4: "
                f"mean={float(layer4.get('posterior_mean') or 0.0):.3f} "
                f"interval={float(layer4.get('bayes_lower') or 0.0):.3f}~{float(layer4.get('bayes_upper') or 0.0):.3f} "
                f"obs={int(layer4.get('observation_count') or 0)} "
                f"cont/rev={int(layer4.get('continuation_count') or 0)}/{int(layer4.get('reversal_count') or 0)} "
                f"group={layer4.get('bayes_group_level') or '—'} key={layer4.get('bayes_group_key') or '—'}"
            )
        latest_signal = payload.get("latest_signal") if isinstance(payload.get("latest_signal"), dict) else None
        if latest_signal is not None:
            risk = latest_signal.get("risk") if isinstance(latest_signal.get("risk"), dict) else {}
            drawdown = risk.get("drawdown_pct")
            calmar = risk.get("calmar_snapshot")
            dd_text = f"{float(drawdown):.2f}%" if drawdown is not None else "—"
            calmar_text = f"{float(calmar):.2f}" if calmar is not None else "—"
            self._detail_signal_label.setText(
                "最新信号: "
                f"{latest_signal.get('signal_type') or '—'} "
                f"trigger={float(latest_signal.get('trigger_price') or 0.0):.4f} "
                f"stop={float(risk.get('stop_loss_price') or 0.0):.4f} "
                f"dd={dd_text} calmar={calmar_text}"
            )
        self._set_audit_items(list(payload.get("audit_items") or []))

    def _on_structure_detail_error(self, msg: str) -> None:
        self._detail_audit_summary_label.setText(f"审计摘要: {self._format_api_error(msg)}")

    def _set_audit_items(self, items: list[dict]) -> None:
        self._audit_table.setRowCount(len(items))
        for row, item in enumerate(items):
            snapshot = item.get("snapshot") if isinstance(item.get("snapshot"), dict) else {}
            summary = (
                f"status={snapshot.get('status', '—')} retrace={snapshot.get('retrace_ratio', '—')}"
                if snapshot
                else "—"
            )
            cells = [
                str(item.get("event_type") or "—"),
                str(item.get("event_ts") or "—"),
                str(snapshot.get("status") or "—") if snapshot else "—",
                str(snapshot.get("direction") or "—") if snapshot else "—",
                summary,
            ]
            for col, text in enumerate(cells):
                widget = QTableWidgetItem(text)
                widget.setTextAlignment(Qt.AlignCenter)
                self._audit_table.setItem(row, col, widget)
        self._audit_table.scrollToTop()

    def _format_api_error(self, msg: str) -> str:
        text = str(msg or "").strip()
        lowered = text.lower()
        port = str(os.environ.get("EASYXT_API_PORT", "8765") or "8765")
        if "connection refused" in lowered or "10061" in lowered:
            return f"API 服务未启动（127.0.0.1:{port} 拒绝连接），请先启动 EasyXT 服务"
        if "timed out" in lowered or "timeout" in lowered:
            return f"API 请求超时（端口 {port}），请检查服务是否卡住或本机负载过高"
        if "http 401" in lowered or "http 403" in lowered:
            return "API 鉴权失败，请检查 EASYXT_API_TOKEN / DEV_MODE 配置"
        if "api 返回非 json" in lowered:
            return "API 返回异常内容，请检查服务端路由或日志输出"
        return text

    @staticmethod
    def _safe_thread_wait(thread: object, timeout_ms: int) -> bool:
        wait = getattr(thread, "wait", None)
        if not callable(wait):
            return True
        try:
            return bool(wait(timeout_ms))
        except TypeError:
            return bool(wait())
        except Exception:
            return False

    def closeEvent(self, event) -> None:  # type: ignore[override]
        try:
            for thread in (self._scan_thread, self._bayes_action_thread, self._detail_thread):
                if thread is None or not thread.isRunning():
                    continue
                thread.requestInterruption()
                thread.quit()
                wait_ms = int(getattr(thread, "_close_wait_timeout_ms", 200) or 200)
                if not self._safe_thread_wait(thread, wait_ms):
                    log.warning(
                        "StructureMonitorPanel closeEvent: %s 未在 %sms 内退出，强制终止",
                        type(thread).__name__,
                        wait_ms,
                    )
                    thread.terminate()
                    self._safe_thread_wait(thread, 500 if wait_ms >= 1000 else 200)
                    try:
                        signal_bus.emit(
                            Events.THREAD_FORCED_TERMINATE,
                            thread_name=type(thread).__name__,
                            component="structure_monitor_panel",
                        )
                    except Exception:
                        pass
            self._scan_thread = None
            self._bayes_action_thread = None
            self._detail_thread = None
        finally:
            super().closeEvent(event)
