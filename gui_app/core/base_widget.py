from PyQt5.QtWidgets import QWidget

from gui_app.core.signal_bus import signal_bus


class BaseWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.signal_bus = signal_bus
