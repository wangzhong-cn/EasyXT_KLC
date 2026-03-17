from PyQt5.QtWidgets import QVBoxLayout, QWidget

from gui_app.enhanced.operation_panel.account_panel import AccountPanel


class AccountInfo(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._panel = AccountPanel()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._panel)

    def update_account_info(self, account_info: dict):
        self._panel.update_account_info(account_info)

    def update_rejection_stats(self, stats: dict):
        self._panel.update_rejection_stats(stats)

    def clear_data(self):
        self._panel.clear_data()
