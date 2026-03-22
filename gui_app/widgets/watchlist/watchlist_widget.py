from __future__ import annotations

import csv
import getpass
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from PyQt5.QtCore import QSettings, QSortFilterProxyModel, Qt, QTimer, pyqtSignal
from PyQt5.QtWidgets import (
    QComboBox,
    QDialog,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QTableView,
    QVBoxLayout,
)

from .watchlist_delegate import WatchlistDelegate
from .watchlist_group import WatchlistGroupManager
from .watchlist_model import WatchlistModel
from .watchlist_toolbar import WatchlistToolbar


class _WatchlistProxyModel(QSortFilterProxyModel):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._search_text = ""
        self._type_filter = "全部"

    def set_search_text(self, text: str) -> None:
        self._search_text = str(text or "").strip().lower()
        self.invalidateFilter()

    def set_type_filter(self, text: str) -> None:
        self._type_filter = str(text or "全部").strip()
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row: int, source_parent) -> bool:
        model = self.sourceModel()
        if model is None:
            return True
        raw_value = getattr(model, "raw_value", None)
        if callable(raw_value) and self._type_filter != "全部":
            market = str(raw_value(source_row, "market") or "")
            mapped = {"A股": "CN", "港股": "HK", "美股": "US"}.get(self._type_filter, "")
            if mapped and market != mapped:
                return False
        if not self._search_text:
            return True
        columns = getattr(model, "COLUMNS", [])
        for _, key in columns:
            value = str(raw_value(source_row, key) if callable(raw_value) else "").lower()
            if self._search_text in value:
                return True
        return False


class WatchlistWidget(QFrame):
    symbol_selected = pyqtSignal(str)
    _ACTION_LOG_LIMIT = 30

    def __init__(self, parent=None, *, state_key: str = "main", enable_fullscreen: bool = True) -> None:
        super().__init__(parent)
        self.setMinimumWidth(320)
        self._current_symbol = ""
        self._current_group = "默认"
        self._current_action_filter = "全部"
        self._current_time_filter = "全部"
        self._quotes_cache: dict[str, dict[str, Any]] = {}
        self._state_key = state_key
        self._enable_fullscreen = enable_fullscreen
        self._fullscreen_dialog: QDialog | None = None
        self._fullscreen_widget: WatchlistWidget | None = None
        self.group_manager = WatchlistGroupManager()
        self.group_manager.subscribe_actions(self._on_group_action)
        self.destroyed.connect(lambda *_: self.group_manager.unsubscribe_actions(self._on_group_action))
        self.toolbar = WatchlistToolbar(self)
        self.toolbar.set_groups(self.group_manager.group_names())
        self.toolbar.search_changed.connect(self._on_search_changed)
        self.toolbar.type_changed.connect(self._on_type_changed)
        self.toolbar.color_mode_changed.connect(self._on_color_mode_changed)
        self.toolbar.group_changed.connect(self._on_group_changed)
        self.toolbar.add_group_clicked.connect(self._on_add_group)
        self.toolbar.remove_group_clicked.connect(self._on_remove_group)
        self.toolbar.fullscreen_clicked.connect(self._toggle_fullscreen_view)
        self.toolbar.fullscreen_btn.setVisible(enable_fullscreen)

        self.table = QTableView(self)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setSelectionMode(QTableView.SingleSelection)
        self.table.setSortingEnabled(True)
        self.table.verticalHeader().setVisible(False)

        self.model = WatchlistModel()
        self.proxy = _WatchlistProxyModel(self)
        self.proxy.setSourceModel(self.model)
        self.table.setModel(self.proxy)
        self.table.setItemDelegate(WatchlistDelegate(self.table))
        self.table.clicked.connect(self._on_row_clicked)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(True)
        self.table.setColumnWidth(0, 100)
        self.table.setColumnWidth(1, 95)
        for col in range(2, 12):
            self.table.setColumnWidth(col, 72)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(6)
        layout.addWidget(self.toolbar)
        layout.addWidget(self.table, 1)

        # --- 审计日志：可折叠面板，默认收起 ---
        self._audit_toggle_btn = QPushButton("分组日志 ▸", self)
        self._audit_toggle_btn.setCheckable(True)
        self._audit_toggle_btn.setChecked(False)
        self._audit_toggle_btn.setStyleSheet(
            "QPushButton { background:transparent; color:#8a95a5; font-size:10px;"
            " border:none; padding:0; text-align:left; }"
            "QPushButton:hover { color:#cfd8dc; }"
        )
        self._audit_toggle_btn.clicked.connect(self._toggle_audit_panel)
        layout.addWidget(self._audit_toggle_btn)

        self._audit_panel = QFrame(self)
        audit_layout = QVBoxLayout(self._audit_panel)
        audit_layout.setContentsMargins(0, 2, 0, 0)
        audit_layout.setSpacing(4)

        self.action_log_toolbar = QHBoxLayout()
        self.action_log_toolbar.setContentsMargins(0, 0, 0, 0)
        self.action_log_toolbar.setSpacing(6)
        self.action_filter_combo = QComboBox(self)
        self.action_filter_combo.setMinimumWidth(120)
        self.action_filter_combo.currentTextChanged.connect(self._on_action_filter_changed)
        self.action_log_toolbar.addWidget(self.action_filter_combo)
        self.action_time_combo = QComboBox(self)
        self.action_time_combo.addItems(["全部", "今天", "7天", "30天"])
        self.action_time_combo.setMinimumWidth(80)
        self.action_time_combo.currentTextChanged.connect(self._on_action_time_filter_changed)
        self.action_log_toolbar.addWidget(self.action_time_combo)
        self.action_export_btn = QPushButton("导出CSV", self)
        self.action_export_btn.clicked.connect(self.export_action_log_csv)
        self.action_log_toolbar.addWidget(self.action_export_btn)
        self.action_clear_btn = QPushButton("清理日志", self)
        self.action_clear_btn.clicked.connect(self._clear_action_log)
        self.action_log_toolbar.addWidget(self.action_clear_btn)
        self.action_archive_combo = QComboBox(self)
        self.action_archive_combo.addItems(["全部归档", "7天前归档", "30天前归档"])
        self.action_archive_combo.setMinimumWidth(95)
        self.action_log_toolbar.addWidget(self.action_archive_combo)
        self.action_archive_btn = QPushButton("执行归档", self)
        self.action_archive_btn.clicked.connect(self._archive_action_logs)
        self.action_log_toolbar.addWidget(self.action_archive_btn)
        self.action_verify_btn = QPushButton("完整性校验", self)
        self.action_verify_btn.clicked.connect(self._verify_action_log_integrity)
        self.action_log_toolbar.addWidget(self.action_verify_btn)
        self.action_verify_file_btn = QPushButton("校验CSV", self)
        self.action_verify_file_btn.clicked.connect(self._verify_exported_csv_file)
        self.action_log_toolbar.addWidget(self.action_verify_file_btn)
        self.action_log_toolbar.addStretch(1)
        audit_layout.addLayout(self.action_log_toolbar)

        self.action_log_list = QListWidget(self)
        self.action_log_list.setMaximumHeight(120)
        self.action_log_list.setStyleSheet(
            "QListWidget { background:#171a24; color:#b8c2d0; border:1px solid #2a3240; font-size:10px; }"
            "QListWidget::item { padding:2px 4px; }"
        )
        audit_layout.addWidget(self.action_log_list)

        self._audit_panel.setVisible(False)
        layout.addWidget(self._audit_panel)

        self.model.set_symbols([])
        self._flash_timer = QTimer(self)
        self._flash_timer.setInterval(120)
        self._flash_timer.timeout.connect(self._on_flash_tick)
        self._flash_timer.start()
        self._load_state()
        self._on_color_mode_changed(self.toolbar.color_combo.currentText())
        self._on_group_changed(self._current_group)
        self._refresh_action_filter_options()
        self._refresh_action_log()

    def _toggle_audit_panel(self, checked: bool) -> None:
        self._audit_panel.setVisible(checked)
        self._audit_toggle_btn.setText("分组日志 ▾" if checked else "分组日志 ▸")
        self._save_state()

    def set_current_symbol(self, symbol: str) -> None:
        self._current_symbol = str(symbol or "").strip()
        if not self._current_symbol:
            return
        if self._current_symbol not in self.group_manager.symbols(self._current_group):
            symbols = self.group_manager.symbols(self._current_group)
            symbols.append(self._current_symbol)
            self.group_manager.set_symbols(self._current_group, symbols, source=self._state_key)
            self._on_group_changed(self._current_group)
        quote = self._quotes_cache.get(self._current_symbol, {"name": self._current_symbol})
        self.model.upsert_quote(self._current_symbol, quote)
        self._sync_fullscreen_widget_symbol(self._current_symbol)

    def set_status(self, text: str) -> None:
        self.toolbar.search_edit.setPlaceholderText(text or "搜索代码/名称")

    def update_orderbook(self, quote: dict[str, Any]) -> None:
        symbol = str(quote.get("symbol") or self._current_symbol or "").strip()
        if not symbol:
            return
        price = quote.get("price") or quote.get("last_price") or quote.get("close")
        prev_close = quote.get("prev_close") or quote.get("last_close")
        open_price = quote.get("open")
        high = quote.get("high")
        low = quote.get("low")
        bid1 = quote.get("bid1")
        ask1 = quote.get("ask1")
        volume = quote.get("volume")
        change = None
        change_pct = None
        try:
            if price not in (None, "") and prev_close not in (None, "", 0):
                change = float(price) - float(prev_close)
                change_pct = change / float(prev_close) * 100.0
        except Exception:
            pass
        payload = {
            "symbol": symbol,
            "name": quote.get("name") or symbol,
            "price": price,
            "change": change,
            "change_pct": change_pct,
            "bid1": bid1,
            "ask1": ask1,
            "prev_close": prev_close,
            "open": open_price,
            "high": high,
            "low": low,
            "volume": volume,
        }
        self._quotes_cache[symbol] = payload
        if self._fullscreen_widget is not None:
            self._fullscreen_widget.update_orderbook(dict(payload))
        if symbol not in self.group_manager.symbols(self._current_group):
            return
        self.model.upsert_quote(
            symbol,
            payload,
        )

    def _on_search_changed(self, text: str) -> None:
        self.proxy.set_search_text(text)
        self._save_state()
        if self._fullscreen_widget is not None:
            self._fullscreen_widget.toolbar.search_edit.setText(str(text or ""))

    def _on_type_changed(self, text: str) -> None:
        value = str(text or "全部")
        if self.toolbar.type_combo.currentText() != value:
            idx_local = self.toolbar.type_combo.findText(value)
            if idx_local >= 0:
                self.toolbar.type_combo.setCurrentIndex(idx_local)
        self.proxy.set_type_filter(value)
        self._save_state()
        if self._fullscreen_widget is not None:
            idx = self._fullscreen_widget.toolbar.type_combo.findText(value)
            if idx >= 0:
                self._fullscreen_widget.toolbar.type_combo.setCurrentIndex(idx)

    def _on_color_mode_changed(self, text: str) -> None:
        value = str(text or "红涨绿跌")
        if self.toolbar.color_combo.currentText() != value:
            idx_local = self.toolbar.color_combo.findText(value)
            if idx_local >= 0:
                self.toolbar.color_combo.setCurrentIndex(idx_local)
        self.model.set_color_mode(value)
        self._save_state()
        if self._fullscreen_widget is not None:
            idx = self._fullscreen_widget.toolbar.color_combo.findText(value)
            if idx >= 0:
                self._fullscreen_widget.toolbar.color_combo.setCurrentIndex(idx)

    def _on_group_changed(self, group: str) -> None:
        name = str(group or "").strip() or "默认"
        if name not in self.group_manager.group_names():
            self.group_manager.add_group(name, source=self._state_key)
            self.toolbar.set_groups(self.group_manager.group_names())
        self._current_group = name
        rows = []
        for sym in self.group_manager.symbols(self._current_group):
            q = dict(self._quotes_cache.get(sym, {}))
            q.setdefault("symbol", sym)
            q.setdefault("name", sym)
            rows.append(q)
        self.model.set_symbols(rows)
        self._save_state()
        if self._fullscreen_widget is not None and self._fullscreen_widget._current_group != self._current_group:
            idx = self._fullscreen_widget.toolbar.group_combo.findText(self._current_group)
            if idx >= 0:
                self._fullscreen_widget.toolbar.group_combo.setCurrentIndex(idx)

    def _on_add_group(self) -> None:
        name, ok = QInputDialog.getText(self, "新建分组", "分组名称")
        if not ok:
            return
        group = str(name or "").strip()
        if not group:
            return
        if group in self.group_manager.group_names():
            QMessageBox.information(self, "提示", "分组已存在")
            return
        self.group_manager.add_group(group, source=self._state_key)
        self.toolbar.set_groups(self.group_manager.group_names())
        self._save_state()
        idx = self.toolbar.group_combo.findText(group)
        if idx >= 0:
            self.toolbar.group_combo.setCurrentIndex(idx)

    def _on_remove_group(self) -> None:
        group = self.toolbar.group_combo.currentText().strip()
        if not group or group == "默认":
            return
        if QMessageBox.question(self, "删除分组", f"确认删除分组“{group}”？") != QMessageBox.Yes:
            return
        self.group_manager.remove_group(group, source=self._state_key)
        self.toolbar.set_groups(self.group_manager.group_names())
        self._on_group_changed(self.toolbar.group_combo.currentText() or "默认")
        self._save_state()

    def _on_row_clicked(self, proxy_index) -> None:
        if not proxy_index.isValid():
            return
        src = self.proxy.mapToSource(proxy_index)
        symbol = str(self.model.raw_value(src.row(), "symbol") or "").strip()
        if symbol:
            self.symbol_selected.emit(symbol)
            self._sync_fullscreen_widget_symbol(symbol)

    def _on_flash_tick(self) -> None:
        if self.model.rowCount() <= 0:
            return
        self.table.viewport().update()

    def _toggle_fullscreen_view(self) -> None:
        if not self._enable_fullscreen:
            return
        if self._fullscreen_dialog is not None:
            self._fullscreen_dialog.close()
            self._fullscreen_dialog = None
            self._fullscreen_widget = None
            return
        dialog = QDialog(self, Qt.Window)
        dialog.setWindowTitle("报价列表")
        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(0, 0, 0, 0)
        mirror = WatchlistWidget(dialog, state_key=f"{self._state_key}_fullscreen", enable_fullscreen=False)
        mirror.symbol_selected.connect(self.symbol_selected.emit)
        layout.addWidget(mirror)
        self._fullscreen_dialog = dialog
        self._fullscreen_widget = mirror
        self._sync_to_fullscreen_widget()
        dialog.finished.connect(lambda _code=0: self._clear_fullscreen_ref())
        dialog.showMaximized()

    def _sync_to_fullscreen_widget(self) -> None:
        if self._fullscreen_widget is None:
            return
        target = self._fullscreen_widget
        target.toolbar.set_groups(self.group_manager.group_names())
        idx_group = target.toolbar.group_combo.findText(self._current_group)
        if idx_group >= 0:
            target.toolbar.group_combo.setCurrentIndex(idx_group)
        idx_type = target.toolbar.type_combo.findText(self.toolbar.type_combo.currentText())
        if idx_type >= 0:
            target.toolbar.type_combo.setCurrentIndex(idx_type)
        idx_color = target.toolbar.color_combo.findText(self.toolbar.color_combo.currentText())
        if idx_color >= 0:
            target.toolbar.color_combo.setCurrentIndex(idx_color)
        target.toolbar.search_edit.setText(self.toolbar.search_edit.text())
        for quote in self._quotes_cache.values():
            target.update_orderbook(dict(quote))
        if self._current_symbol:
            target.set_current_symbol(self._current_symbol)

    def _sync_fullscreen_widget_symbol(self, symbol: str) -> None:
        if self._fullscreen_widget is not None and symbol:
            self._fullscreen_widget.set_current_symbol(symbol)

    def _clear_fullscreen_ref(self) -> None:
        self._fullscreen_dialog = None
        self._fullscreen_widget = None

    def get_group_action_log(self) -> list[dict]:
        return self.group_manager.get_action_log()

    def _state_prefix(self) -> str:
        return f"watchlist/{self._state_key}"

    def _load_state(self) -> None:
        settings = QSettings("EasyXT", "WatchlistWidget")
        group = settings.value(f"{self._state_prefix()}/group", "默认", type=str)
        typ = settings.value(f"{self._state_prefix()}/type", "全部", type=str)
        color = settings.value(f"{self._state_prefix()}/color_mode", "红涨绿跌", type=str)
        search = settings.value(f"{self._state_prefix()}/search", "", type=str)
        action_filter = settings.value(f"{self._state_prefix()}/action_filter", "全部", type=str)
        time_filter = settings.value(f"{self._state_prefix()}/time_filter", "全部", type=str)
        if group and group not in self.group_manager.group_names():
            self.group_manager.add_group(group)
        self.toolbar.set_groups(self.group_manager.group_names())
        idx_group = self.toolbar.group_combo.findText(group or "默认")
        if idx_group >= 0:
            self.toolbar.group_combo.setCurrentIndex(idx_group)
        idx_type = self.toolbar.type_combo.findText(typ or "全部")
        if idx_type >= 0:
            self.toolbar.type_combo.setCurrentIndex(idx_type)
        idx_color = self.toolbar.color_combo.findText(color or "红涨绿跌")
        if idx_color >= 0:
            self.toolbar.color_combo.setCurrentIndex(idx_color)
        if search:
            self.toolbar.search_edit.setText(search)
        self._current_action_filter = str(action_filter or "全部")
        idx_time = self.action_time_combo.findText(str(time_filter or "全部"))
        if idx_time >= 0:
            self.action_time_combo.setCurrentIndex(idx_time)
        self._current_time_filter = str(time_filter or "全部")
        audit_open = settings.value(f"{self._state_prefix()}/audit_open", False, type=bool)
        self._audit_toggle_btn.setChecked(audit_open)
        self._toggle_audit_panel(audit_open)

    def _save_state(self) -> None:
        settings = QSettings("EasyXT", "WatchlistWidget")
        settings.setValue(f"{self._state_prefix()}/group", self._current_group)
        settings.setValue(f"{self._state_prefix()}/type", self.toolbar.type_combo.currentText())
        settings.setValue(f"{self._state_prefix()}/color_mode", self.toolbar.color_combo.currentText())
        settings.setValue(f"{self._state_prefix()}/search", self.toolbar.search_edit.text())
        settings.setValue(f"{self._state_prefix()}/action_filter", self._current_action_filter)
        settings.setValue(f"{self._state_prefix()}/time_filter", self._current_time_filter)
        settings.setValue(f"{self._state_prefix()}/audit_open", self._audit_toggle_btn.isChecked())

    def _on_group_action(self, entry: dict) -> None:
        _ = entry
        self._refresh_action_filter_options()
        self._refresh_action_log()
        self.toolbar.set_groups(self.group_manager.group_names())
        if self._current_group not in self.group_manager.group_names():
            self._current_group = "默认"
        idx = self.toolbar.group_combo.findText(self._current_group)
        if idx >= 0:
            self.toolbar.group_combo.setCurrentIndex(idx)

    def _refresh_action_log(self) -> None:
        logs = self._get_filtered_action_logs()
        self.action_log_list.clear()
        for entry in logs:
            self.action_log_list.addItem(self._format_action_entry(entry))
        if self.action_log_list.count() > 0:
            self.action_log_list.scrollToBottom()

    def _append_action_log_entry(self, entry: dict) -> None:
        text = self._format_action_entry(entry)
        self.action_log_list.addItem(QListWidgetItem(text))
        while self.action_log_list.count() > self._ACTION_LOG_LIMIT:
            self.action_log_list.takeItem(0)
        self.action_log_list.scrollToBottom()

    def _refresh_action_filter_options(self) -> None:
        all_actions = {"全部"}
        for e in self.group_manager.get_action_log():
            action = str(e.get("action") or "").strip()
            if action:
                all_actions.add(action)
        current = self._current_action_filter if self._current_action_filter in all_actions else "全部"
        self.action_filter_combo.blockSignals(True)
        self.action_filter_combo.clear()
        self.action_filter_combo.addItems(sorted(all_actions, key=lambda x: (x != "全部", x)))
        idx = self.action_filter_combo.findText(current)
        if idx >= 0:
            self.action_filter_combo.setCurrentIndex(idx)
        self.action_filter_combo.blockSignals(False)
        self._current_action_filter = current

    def _on_action_filter_changed(self, text: str) -> None:
        self._current_action_filter = str(text or "全部")
        self._save_state()
        self._refresh_action_log()

    def _on_action_time_filter_changed(self, text: str) -> None:
        self._current_time_filter = str(text or "全部")
        self._save_state()
        self._refresh_action_log()

    def _clear_action_log(self) -> None:
        if QMessageBox.question(self, "清理日志", "确认清理本地分组动作日志文件？") != QMessageBox.Yes:
            return
        self.group_manager.clear_action_log()
        self._refresh_action_filter_options()
        self._refresh_action_log()

    def _archive_action_logs(self) -> None:
        mode = self.action_archive_combo.currentText()
        days = 0
        if mode == "7天前归档":
            days = 7
        elif mode == "30天前归档":
            days = 30
        count = self.group_manager.archive_uncompressed_logs(days)
        QMessageBox.information(
            self,
            "归档完成",
            f"模式：{mode}\n压缩文件数：{count}",
        )

    def _verify_action_log_integrity(self) -> dict[str, str]:
        logs = self._get_filtered_action_logs()
        digest = self._build_integrity_digest(logs)
        result = {
            "count": str(len(logs)),
            "digest": digest,
            "action_filter": self._current_action_filter,
            "time_filter": self._current_time_filter,
        }
        QMessageBox.information(
            self,
            "完整性校验",
            f"记录数：{result['count']}\n"
            f"动作筛选：{result['action_filter']}\n"
            f"时间筛选：{result['time_filter']}\n"
            f"SHA256：{result['digest']}",
        )
        return result

    def _verify_exported_csv_file(self) -> None:
        csv_path, _ = QFileDialog.getOpenFileName(
            self,
            "选择导出CSV文件",
            str(self.group_manager.file_path.parent),
            "CSV Files (*.csv);;All Files (*)",
        )
        path = str(csv_path or "").strip()
        if not path:
            return
        result = self.verify_csv_with_sidecar(path)
        if result["ok"]:
            QMessageBox.information(self, "校验成功", f"文件：{path}\nSHA256：{result['actual']}")
        else:
            QMessageBox.warning(
                self,
                "校验失败",
                f"文件：{path}\n期望：{result['expected']}\n实际：{result['actual']}\n原因：{result['reason']}",
            )

    def _get_filtered_action_logs(self) -> list[dict]:
        logs = self.group_manager.get_action_log()[-self._ACTION_LOG_LIMIT :]
        if self._current_action_filter and self._current_action_filter != "全部":
            logs = [e for e in logs if str(e.get("action") or "") == self._current_action_filter]
        if self._current_time_filter and self._current_time_filter != "全部":
            logs = [e for e in logs if self._in_time_window(e, self._current_time_filter)]
        return logs

    @staticmethod
    def _in_time_window(entry: dict, time_filter: str, now: datetime | None = None) -> bool:
        if not time_filter or time_filter == "全部":
            return True
        ts = str(entry.get("ts") or "").strip()
        if not ts:
            return False
        try:
            ts_dt = datetime.fromisoformat(ts)
        except Exception:
            return False
        now_dt = now or datetime.now()
        if time_filter == "今天":
            return ts_dt.date() == now_dt.date()
        if time_filter == "7天":
            return ts_dt >= now_dt - timedelta(days=7)
        if time_filter == "30天":
            return ts_dt >= now_dt - timedelta(days=30)
        return True

    def export_action_log_csv(self, file_path: str | None = None) -> str | None:
        interactive = not file_path
        logs = self._get_filtered_action_logs()
        digest = self._build_integrity_digest(logs)
        target = file_path
        if not target:
            default_name = self._build_default_export_name(digest_prefix=digest[:8])
            target, _ = QFileDialog.getSaveFileName(
                self,
                "导出分组动作日志",
                default_name,
                "CSV Files (*.csv);;All Files (*)",
            )
        target = str(target or "").strip()
        if not target:
            return None
        with open(target, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "ts",
                    "action",
                    "group",
                    "payload",
                    "source_view",
                    "action_filter",
                    "time_filter",
                    "operator",
                    "exported_at",
                ],
            )
            writer.writeheader()
            exported_at = datetime.now().isoformat(timespec="seconds")
            operator = getpass.getuser()
            for e in logs:
                payload = e.get("payload") or {}
                writer.writerow(
                    {
                        "ts": e.get("ts") or "",
                        "action": e.get("action") or "",
                        "group": e.get("group") or "",
                        "payload": json.dumps(payload, ensure_ascii=False),
                        "source_view": str(payload.get("source") or ""),
                        "action_filter": self._current_action_filter,
                        "time_filter": self._current_time_filter,
                        "operator": operator,
                        "exported_at": exported_at,
                    }
                )
        file_digest = self._file_sha256(target)
        sha_path = self._write_sha256_sidecar(target, file_digest, data_digest=digest)
        if interactive:
            QMessageBox.information(
                self,
                "导出完成",
                f"已导出 {len(logs)} 条日志\n"
                f"动作筛选：{self._current_action_filter}\n"
                f"时间筛选：{self._current_time_filter}\n"
                f"文件：{target}\n"
                f"校验：{sha_path}",
            )
        return target

    def _build_default_export_name(self, digest_prefix: str = "") -> str:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = self.group_manager.file_path.parent
        prefix = f"{digest_prefix}_" if digest_prefix else ""
        return str(Path(base) / f"{prefix}watchlist_action_log_{stamp}.csv")

    @staticmethod
    def _write_sha256_sidecar(csv_path: str, digest: str, data_digest: str = "") -> str:
        path = Path(csv_path)
        sidecar = path.with_name(f"{path.name}.sha256")
        lines = [f"{digest} *{path.name}\n"]
        if data_digest:
            lines.append(f"# data_digest={data_digest}\n")
        sidecar.write_text("".join(lines), encoding="utf-8")
        return str(sidecar)

    @staticmethod
    def _file_sha256(file_path: str) -> str:
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    @classmethod
    def verify_csv_with_sidecar(cls, csv_path: str) -> dict[str, str | bool]:
        path = Path(csv_path)
        sidecar = path.with_name(f"{path.name}.sha256")
        if not path.exists():
            return {"ok": False, "expected": "", "actual": "", "reason": "csv_not_found"}
        if not sidecar.exists():
            return {"ok": False, "expected": "", "actual": "", "reason": "sha256_not_found"}
        raw = sidecar.read_text(encoding="utf-8").splitlines()
        first = raw[0].strip() if raw else ""
        expected = first.split(" ")[0] if first else ""
        actual = cls._file_sha256(str(path))
        ok = bool(expected) and expected.lower() == actual.lower()
        return {"ok": ok, "expected": expected, "actual": actual, "reason": "ok" if ok else "digest_mismatch"}

    @staticmethod
    def _build_integrity_digest(logs: list[dict]) -> str:
        normalized = []
        for e in logs:
            normalized.append(
                {
                    "ts": e.get("ts") or "",
                    "action": e.get("action") or "",
                    "group": e.get("group") or "",
                    "payload": e.get("payload") or {},
                }
            )
        text = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _format_action_entry(entry: dict) -> str:
        ts = str(entry.get("ts") or "--")
        action = str(entry.get("action") or "--")
        group = str(entry.get("group") or "--")
        payload = entry.get("payload") or {}
        size = payload.get("size")
        size_txt = f" size={size}" if size is not None else ""
        return f"{ts} | {action} | {group}{size_txt}"
