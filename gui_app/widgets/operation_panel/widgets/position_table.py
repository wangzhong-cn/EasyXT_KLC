from PyQt5.QtWidgets import QVBoxLayout, QWidget

from gui_app.enhanced.operation_panel.position_panel import PositionPanel


class PositionTable(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._panel = PositionPanel()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._panel)

    def update_positions(self, positions: list[dict]):
        self._panel.update_positions(positions)

    def clear_data(self):
        self._panel.clear_data()
