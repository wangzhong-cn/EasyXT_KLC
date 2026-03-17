from PyQt5.QtWidgets import QVBoxLayout, QWidget

from core.events import Events
from core.signal_bus import signal_bus
from gui_app.widgets.operation_panel.widgets.position_table import PositionTable


class PositionTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.position_table = PositionTable()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.position_table)
        signal_bus.subscribe(Events.POSITION_UPDATED, self._on_position_updated)

    def update_positions(self, positions: list[dict]):
        self.position_table.update_positions(positions)

    def clear_data(self):
        self.position_table.clear_data()

    def _on_position_updated(self, positions: list[dict], **kwargs):
        if positions is None:
            return
        self.update_positions(positions)
