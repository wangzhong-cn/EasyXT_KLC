from PyQt5.QtWidgets import QVBoxLayout, QWidget

from core.events import Events
from core.signal_bus import signal_bus
from gui_app.widgets.operation_panel.widgets.account_info import AccountInfo


class AccountTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.account_info = AccountInfo()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.account_info)
        signal_bus.subscribe(Events.ACCOUNT_UPDATED, self._on_account_updated)

    def update_account(self, account_info: dict):
        self.account_info.update_account_info(account_info)

    def _on_account_updated(self, account_info: dict, **kwargs):
        if account_info is None:
            return
        self.account_info.update_account_info(account_info)
