from PyQt5.QtWidgets import QVBoxLayout, QWidget

from gui_app.widgets.operation_panel.widgets.order_form import OrderForm


class TradeTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.order_form = OrderForm()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.order_form)
        self.order_submitted = self.order_form.order_submitted

    def set_symbol(self, symbol: str):
        self.order_form.set_symbol(symbol)
