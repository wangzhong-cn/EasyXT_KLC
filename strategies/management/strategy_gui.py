#!/usr/bin/env python3
"""
策略管理GUI界面 - 阶段5用户交互组件
提供策略创建、配置、回测、监控的图形界面
"""

import logging
import sys

from PyQt5.QtCore import QDate, Qt, pyqtSignal
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QComboBox,
    QDateEdit,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from .backtest_engine import backtest_engine
from .strategy_manager import strategy_manager


class StrategyCreationDialog(QDialog):
    """策略创建对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("创建新策略")
        self.setModal(True)
        self.resize(600, 500)
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # 基础信息
        basic_group = QGroupBox("基础信息")
        basic_layout = QFormLayout(basic_group)

        self.name_edit = QLineEdit()
        self.type_combo = QComboBox()
        self.type_combo.addItems(["趋势跟踪", "均值回归", "因子选股", "网格交易", "条件单", "跨周期对冲"])
        self.base_strategy_combo = QComboBox()
        self.base_strategy_combo.addItems(["MA_Cross", "RSI_Reversion", "Momentum_Factor", "Grid_Basic", "Conditional_Break"])

        basic_layout.addRow("策略名称:", self.name_edit)
        basic_layout.addRow("策略类型:", self.type_combo)
        basic_layout.addRow("基础策略:", self.base_strategy_combo)

        self.period_combo = QComboBox()
        self.period_combo.addItems(["1d", "1m", "5m", "15m", "30m", "1h"])
        basic_layout.addRow("数据周期:", self.period_combo)

        # 参数配置
        param_group = QGroupBox("策略参数")
        param_layout = QFormLayout(param_group)

        self.fast_period_spin = QSpinBox()
        self.fast_period_spin.setRange(1, 100)
        self.fast_period_spin.setValue(5)

        self.slow_period_spin = QSpinBox()
        self.slow_period_spin.setRange(1, 200)
        self.slow_period_spin.setValue(20)

        self.adjust_combo = QComboBox()
        self.adjust_combo.addItems(["不复权", "前复权", "后复权"])

        param_layout.addRow("快速周期:", self.fast_period_spin)
        param_layout.addRow("慢速周期:", self.slow_period_spin)
        param_layout.addRow("复权方式:", self.adjust_combo)

        # 风控配置
        risk_group = QGroupBox("风险控制")
        risk_layout = QFormLayout(risk_group)

        self.max_position_spin = QDoubleSpinBox()
        self.max_position_spin.setRange(0.01, 0.2)
        self.max_position_spin.setValue(0.2)
        self.max_position_spin.setSingleStep(0.05)

        self.daily_stop_loss_spin = QDoubleSpinBox()
        self.daily_stop_loss_spin.setRange(0.01, 0.03)
        self.daily_stop_loss_spin.setValue(0.03)

        self.max_drawdown_spin = QDoubleSpinBox()
        self.max_drawdown_spin.setRange(0.05, 0.15)
        self.max_drawdown_spin.setValue(0.15)

        risk_layout.addRow("最大仓位:", self.max_position_spin)
        risk_layout.addRow("单日止损:", self.daily_stop_loss_spin)
        risk_layout.addRow("最大回撤:", self.max_drawdown_spin)

        # 标的配置
        symbol_group = QGroupBox("交易标的")
        symbol_layout = QVBoxLayout(symbol_group)

        self.symbols_edit = QTextEdit()
        self.symbols_edit.setPlaceholderText("每行一个股票代码，如：\n000001.SZ\n000002.SZ")
        self.symbols_edit.setMaximumHeight(100)
        symbol_layout.addWidget(self.symbols_edit)

        backtest_group = QGroupBox("回测设置")
        backtest_layout = QFormLayout(backtest_group)
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDate(QDate.currentDate().addYears(-1))
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDate(QDate.currentDate())
        backtest_layout.addRow("开始日期:", self.start_date_edit)
        backtest_layout.addRow("结束日期:", self.end_date_edit)

        cost_group = QGroupBox("交易成本")
        cost_layout = QFormLayout(cost_group)
        self.commission_spin = QDoubleSpinBox()
        self.commission_spin.setDecimals(6)
        self.commission_spin.setRange(0.0, 0.01)
        self.commission_spin.setValue(0.0003)
        self.tax_spin = QDoubleSpinBox()
        self.tax_spin.setDecimals(6)
        self.tax_spin.setRange(0.0, 0.01)
        self.tax_spin.setValue(0.001)
        self.slippage_spin = QDoubleSpinBox()
        self.slippage_spin.setDecimals(2)
        self.slippage_spin.setRange(0.0, 50.0)
        self.slippage_spin.setValue(0.0)
        cost_layout.addRow("手续费率:", self.commission_spin)
        cost_layout.addRow("印花税率:", self.tax_spin)
        cost_layout.addRow("滑点(bps):", self.slippage_spin)

        # 按钮区域
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)

        # 布局组装
        layout.addWidget(basic_group)
        layout.addWidget(param_group)
        layout.addWidget(risk_group)
        layout.addWidget(symbol_group)
        layout.addWidget(backtest_group)
        layout.addWidget(cost_group)
        layout.addWidget(button_box)

    def get_config_data(self) -> dict:
        """获取配置数据"""
        type_mapping = {
            "趋势跟踪": "trend",
            "均值回归": "reversion",
            "因子选股": "factor",
            "网格交易": "grid",
            "条件单": "conditional",
            "跨周期对冲": "hedge"
        }

        adjust_mapping = {
            "不复权": "none",
            "前复权": "front",
            "后复权": "back"
        }

        symbols = [s.strip() for s in self.symbols_edit.toPlainText().split('\n') if s.strip()]

        return {
            "strategy_name": self.name_edit.text(),
            "strategy_type": type_mapping[self.type_combo.currentText()],
            "base_strategy": self.base_strategy_combo.currentText(),
            "parameters": {
                "fast_period": self.fast_period_spin.value(),
                "slow_period": self.slow_period_spin.value(),
                "adjust": adjust_mapping[self.adjust_combo.currentText()]
            },
            "risk_controls": {
                "max_position": self.max_position_spin.value(),
                "daily_stop_loss": self.daily_stop_loss_spin.value(),
                "max_drawdown": self.max_drawdown_spin.value()
            },
            "symbols": symbols,
            "period": self.period_combo.currentText(),
            "backtest_range": {
                "start": self.start_date_edit.date().toString("yyyy-MM-dd"),
                "end": self.end_date_edit.date().toString("yyyy-MM-dd")
            },
            "trading_cost": {
                "commission": self.commission_spin.value(),
                "tax": self.tax_spin.value(),
                "slippage_bps": self.slippage_spin.value()
            }
        }


class StrategyManagementWidget(QWidget):
    """策略管理主界面"""

    strategy_created = pyqtSignal(str)  # 策略创建信号
    backtest_started = pyqtSignal(str)  # 回测开始信号

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
        self.load_strategies()
        self.logger = logging.getLogger(__name__)

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # 标题
        title_label = QLabel("策略管理")
        title_font = QFont()
        title_font.setPointSize(16)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(title_label)

        # 操作按钮
        button_layout = QHBoxLayout()

        self.create_btn = QPushButton("创建策略")
        self.create_btn.clicked.connect(self.create_strategy)

        self.backtest_btn = QPushButton("执行回测")
        self.backtest_btn.clicked.connect(self.run_backtest)

        self.delete_btn = QPushButton("删除策略")
        self.delete_btn.clicked.connect(self.delete_strategy)

        button_layout.addWidget(self.create_btn)
        button_layout.addWidget(self.backtest_btn)
        button_layout.addWidget(self.delete_btn)
        button_layout.addStretch()

        layout.addLayout(button_layout)

        # 策略列表表格
        self.strategy_table = QTableWidget()
        self.strategy_table.setColumnCount(6)
        self.strategy_table.setHorizontalHeaderLabels([
            "策略ID", "策略名称", "策略类型", "创建时间", "版本", "标的数量"
        ])
        self.strategy_table.setSelectionBehavior(QTableWidget.SelectRows)
        layout.addWidget(self.strategy_table)

    def load_strategies(self):
        """加载策略列表"""
        strategies = strategy_manager.list_strategies()
        self.strategy_table.setRowCount(len(strategies))

        for row, strategy in enumerate(strategies):
            self.strategy_table.setItem(row, 0, QTableWidgetItem(strategy['strategy_id']))
            self.strategy_table.setItem(row, 1, QTableWidgetItem(strategy['strategy_name']))
            self.strategy_table.setItem(row, 2, QTableWidgetItem(strategy['strategy_type']))
            self.strategy_table.setItem(row, 3, QTableWidgetItem(strategy['created_at']))
            self.strategy_table.setItem(row, 4, QTableWidgetItem(str(strategy.get('version', 1))))
            self.strategy_table.setItem(row, 5, QTableWidgetItem(str(strategy['symbols_count'])))

        self.strategy_table.resizeColumnsToContents()

    def create_strategy(self):
        """创建新策略"""
        dialog = StrategyCreationDialog(self)
        if dialog.exec_() == QDialog.Accepted:
            try:
                config_data = dialog.get_config_data()
                if not config_data["strategy_name"].strip():
                    QMessageBox.warning(self, "提示", "策略名称不能为空")
                    return
                if not config_data["symbols"]:
                    QMessageBox.warning(self, "提示", "请至少输入一个标的代码")
                    return
                strategy_id = strategy_manager.create_strategy(config_data)

                QMessageBox.information(self, "成功", f"策略创建成功！\n策略ID: {strategy_id}")
                self.load_strategies()
                self.strategy_created.emit(strategy_id)

            except Exception as e:
                self.logger.error("策略创建失败: %s", e)
                QMessageBox.critical(self, "错误", f"策略创建失败: {e}")

    def run_backtest(self):
        """执行回测"""
        selected_items = self.strategy_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "警告", "请先选择一个策略")
            return

        strategy_item = self.strategy_table.item(selected_items[0].row(), 0)
        if strategy_item is None:
            QMessageBox.warning(self, "警告", "未找到策略ID")
            return
        strategy_id = strategy_item.text()
        strategy_config = strategy_manager.get_strategy(strategy_id)

        if not strategy_config:
            QMessageBox.critical(self, "错误", "策略配置不存在")
            return

        try:
            # 执行回测
            result = backtest_engine.run_backtest(strategy_config)

            QMessageBox.information(self, "回测完成",
                f"回测执行成功！\n"
                f"总收益率: {result.performance_metrics.get('total_return', 0):.2%}\n"
                f"最大回撤: {result.performance_metrics.get('max_drawdown', 0):.2%}"
            )

            self.backtest_started.emit(strategy_id)

        except Exception as e:
            self.logger.error("回测执行失败: %s", e)
            QMessageBox.critical(self, "回测失败", f"回测执行失败: {e}")

    def delete_strategy(self):
        """删除策略"""
        selected_items = self.strategy_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "警告", "请先选择一个策略")
            return

        strategy_id_item = self.strategy_table.item(selected_items[0].row(), 0)
        strategy_name_item = self.strategy_table.item(selected_items[0].row(), 1)
        if strategy_id_item is None or strategy_name_item is None:
            QMessageBox.warning(self, "警告", "未找到策略信息")
            return
        strategy_id = strategy_id_item.text()
        strategy_name = strategy_name_item.text()

        reply = QMessageBox.question(self, "确认删除",
            f"确定要删除策略 '{strategy_name}' 吗？此操作不可恢复。",
            QMessageBox.Yes | QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            if strategy_manager.delete_strategy(strategy_id):
                QMessageBox.information(self, "成功", "策略删除成功")
                self.load_strategies()
            else:
                self.logger.error("策略删除失败: %s", strategy_id)
                QMessageBox.critical(self, "错误", "策略删除失败")


if __name__ == "__main__":
    from PyQt5.QtWidgets import QApplication

    app = QApplication(sys.argv)

    widget = StrategyManagementWidget()
    widget.show()

    sys.exit(app.exec_())
