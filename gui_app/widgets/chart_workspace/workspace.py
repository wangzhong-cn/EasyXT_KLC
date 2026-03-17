import importlib
import sys
from pathlib import Path
from typing import Optional

from PyQt5.QtCore import Qt, QThread, QTimer, pyqtSignal
from PyQt5.QtWidgets import QApplication, QLabel, QSplitter, QVBoxLayout, QWidget

try:
    from gui_app.widgets.chart_workspace.chart_panel import ChartPanel
except ModuleNotFoundError:
    _project_root = str(Path(__file__).resolve().parents[3])
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)
    from gui_app.widgets.chart_workspace.chart_panel import ChartPanel


class _OperationPanelLoader(QThread):
    loaded = pyqtSignal(object)
    error = pyqtSignal(str)

    def run(self):
        try:
            panel_cls = importlib.import_module(
                "gui_app.widgets.operation_panel.panel"
            ).OperationPanel
            self.loaded.emit(panel_cls)
        except Exception as e:
            self.error.emit(str(e))


class ChartWorkspace(QWidget):
    def __init__(self, parent=None, show_operation_panel: bool = True):
        super().__init__(parent)
        self.show_operation_panel = show_operation_panel
        self.chart_panel = ChartPanel(self)
        self.operation_panel = None
        self._operation_placeholder: Optional[QLabel] = None
        if self.show_operation_panel:
            self._operation_placeholder = QLabel("操作面板加载中...")
            self._operation_placeholder.setAlignment(Qt.AlignCenter)
            self._operation_placeholder.setStyleSheet("color: #888; font-size: 14px;")
        self._splitter = None
        self._min_chart_height = 320
        self._min_operation_height = 220
        self._load_thread = None
        self._panel_loaded = False
        self._init_ui()
        self._connect_signals()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        if not self.show_operation_panel:
            layout.addWidget(self.chart_panel, 1)
            return
        splitter = QSplitter(Qt.Vertical)
        splitter.setHandleWidth(6)
        splitter.setOpaqueResize(True)
        splitter.setStyleSheet("QSplitter::handle{background:#444444;}")
        splitter.addWidget(self.chart_panel)
        if self._operation_placeholder is not None:
            splitter.addWidget(self._operation_placeholder)
        splitter.setStretchFactor(0, 7)
        splitter.setStretchFactor(1, 3)
        splitter.setCollapsible(0, False)
        splitter.setCollapsible(1, False)
        self.chart_panel.setMinimumHeight(self._min_chart_height)
        if self._operation_placeholder is not None:
            self._operation_placeholder.setMinimumHeight(self._min_operation_height)
        splitter.setSizes([800, 340])
        layout.addWidget(splitter, 1)
        self._splitter = splitter

    def showEvent(self, event):
        super().showEvent(event)
        if not self.show_operation_panel:
            return
        QTimer.singleShot(0, self._apply_split_sizes)
        if not self._panel_loaded:
            QTimer.singleShot(500, self._ensure_operation_panel)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._apply_split_sizes()

    def _apply_split_sizes(self):
        if not self._splitter:
            return
        total = self._splitter.size().height()
        if total <= 0:
            total = self.height()
        if total <= 0:
            return
        min_top = self._min_chart_height
        min_bottom = self._min_operation_height
        if total < min_top + min_bottom:
            min_bottom = max(total - min_top, 0)
        top_size = max(int(total * 0.7), min_top)
        bottom_size = max(total - top_size, min_bottom)
        if top_size + bottom_size > total:
            bottom_size = max(total - top_size, 0)
        if top_size < min_top:
            top_size = min_top
            bottom_size = max(total - top_size, 0)
        if bottom_size < min_bottom:
            bottom_size = min_bottom
            top_size = max(total - bottom_size, min_top)
        self._splitter.setSizes([top_size, bottom_size])

    def _connect_signals(self):
        if self.operation_panel is not None:
            self.chart_panel.symbol_changed.connect(self.operation_panel.on_symbol_changed)
            self.operation_panel.symbol_selected.connect(self.chart_panel.load_symbol)
            self.operation_panel.order_submitted.connect(self.chart_panel.mark_order)

    def _ensure_operation_panel(self):
        if not self.show_operation_panel:
            return
        if self.operation_panel is not None or self._panel_loaded:
            return
        self._panel_loaded = True
        self._load_thread = _OperationPanelLoader()
        self._load_thread.loaded.connect(self._on_operation_panel_loaded)
        self._load_thread.error.connect(self._on_operation_panel_error)
        self._load_thread.start()

    def _on_operation_panel_loaded(self, panel_cls):
        try:
            self.operation_panel = panel_cls(self)
            if self._splitter is not None:
                index = self._splitter.indexOf(self._operation_placeholder) if self._operation_placeholder is not None else -1
                if index >= 0:
                    self._splitter.replaceWidget(index, self.operation_panel)
            if self._operation_placeholder:
                self._operation_placeholder.deleteLater()
                self._operation_placeholder = None
            self.operation_panel.setMinimumHeight(self._min_operation_height)
            self._connect_signals()
        except Exception as e:
            if self._operation_placeholder:
                self._operation_placeholder.setText(f"面板加载失败: {e}")

    def _on_operation_panel_error(self, error_msg: str):
        if self._operation_placeholder:
            self._operation_placeholder.setText(f"面板加载失败: {error_msg}")

    def preheat(self):
        QTimer.singleShot(0, self.chart_panel._ensure_workspace)
        if self.show_operation_panel:
            QTimer.singleShot(500, self._ensure_operation_panel)

    def closeEvent(self, event):
        try:
            if self._load_thread is not None and self._load_thread.isRunning():
                self._load_thread.requestInterruption()
                self._load_thread.quit()
                self._load_thread.wait(500)
        finally:
            super().closeEvent(event)


def _run_standalone():
    app = QApplication.instance()
    owns_app = app is None
    if app is None:
        app = QApplication(sys.argv)
    window = ChartWorkspace(show_operation_panel=True)
    window.resize(1400, 900)
    window.show()
    if owns_app:
        return app.exec_()
    return 0


if __name__ == "__main__":
    raise SystemExit(_run_standalone())
