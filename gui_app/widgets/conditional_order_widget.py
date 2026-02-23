#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
条件单GUI组件
提供条件单的可视化配置、管理和监控界面
"""

import sys
import os
import importlib.util
from datetime import datetime, timedelta
from typing import Optional

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QLineEdit, QPushButton, QTextEdit,
    QTableWidget, QTableWidgetItem, QSpinBox, QDoubleSpinBox, QComboBox,
    QSplitter, QFrame, QMessageBox,
    QFormLayout, QScrollArea, QDateTimeEdit, QDateEdit
)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QDateTime
from PyQt5.QtGui import QColor, QTextCursor

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

EASYXT_AVAILABLE = importlib.util.find_spec("easy_xt") is not None


class ConditionalOrderWidget(QWidget):
    """条件单GUI组件"""

    log_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.orders = []  # 存储所有条件单
        self.order_counter = 0  # 条件单计数器
        self.monitored_orders = set()  # 已启动监控的条件单ID集合
        self.trade_api = None  # AdvancedTradeAPI实例
        self._trade_initialized = False  # 交易API是否已初始化
        self.init_ui()
        self.setup_timer()
        self.init_trade_connection()  # 自动初始化交易连接

    def init_ui(self):
        """初始化用户界面"""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # 创建分割器
        splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(splitter)

        # 上半部分：条件单配置
        config_widget = self.create_config_panel()
        splitter.addWidget(config_widget)

        # 下半部分：条件单管理
        manage_widget = self.create_manage_panel()
        splitter.addWidget(manage_widget)

        # 设置分割比例
        splitter.setSizes([350, 400])

    def create_config_panel(self) -> QWidget:
        """创建配置面板"""
        # 使用滚动区域包裹整个配置面板
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setSpacing(12)  # 减小垂直间距
        layout.setContentsMargins(10, 10, 10, 10)

        # 条件单类型选择
        type_group = QGroupBox("条件单类型")
        type_layout = QFormLayout(type_group)
        type_layout.setSpacing(12)  # 行间距12px
        type_layout.setContentsMargins(15, 20, 15, 15)  # 边距
        # 设置标签和输入框之间的水平间距
        type_layout.setHorizontalSpacing(18)  # 标签与输入框间距18px
        type_layout.setVerticalSpacing(15)  # 行间距15px

        self.order_type_combo = QComboBox()
        self.order_type_combo.setMinimumWidth(200)  # 设置最小宽度200px
        self.order_type_combo.addItems([
            "价格条件单",
            "时间条件单",
            "涨跌幅条件单",
            "止盈止损单"
        ])
        self.order_type_combo.currentIndexChanged.connect(self.on_order_type_changed)
        type_layout.addRow("条件单类型:", self.order_type_combo)

        layout.addWidget(type_group)

        # 条件配置区域（不滚动，直接显示）
        condition_group = QGroupBox("条件配置")
        self.condition_layout = QFormLayout(condition_group)
        self.condition_layout.setSpacing(12)  # 行间距12px
        self.condition_layout.setContentsMargins(15, 15, 15, 15)  # 边距
        # 设置标签和输入框之间的水平间距
        self.condition_layout.setHorizontalSpacing(18)  # 标签与输入框间距18px
        self.condition_layout.setVerticalSpacing(15)  # 行间距15px
        self.create_condition_ui(self.condition_layout)

        layout.addWidget(condition_group)

        # 动作配置
        action_group = QGroupBox("触发动作")
        action_layout = QFormLayout(action_group)
        action_layout.setSpacing(12)  # 行间距12px
        action_layout.setContentsMargins(15, 20, 15, 15)  # 边距
        # 设置标签和输入框之间的水平间距
        action_layout.setHorizontalSpacing(18)  # 标签与输入框间距18px
        action_layout.setVerticalSpacing(15)  # 行间距15px

        self.action_type_combo = QComboBox()
        self.action_type_combo.setMinimumWidth(180)  # 设置最小宽度180px
        self.action_type_combo.addItems(["买入", "卖出"])
        action_layout.addRow("操作类型:", self.action_type_combo)

        self.account_combo = QComboBox()
        self.account_combo.setMinimumWidth(200)
        action_layout.addRow("账户:", self.account_combo)

        self.stock_code_edit = QLineEdit("511090.SH")
        self.stock_code_edit.setMinimumWidth(200)  # 设置最小宽度200px
        action_layout.addRow("股票代码:", self.stock_code_edit)

        self.order_quantity_spin = QSpinBox()
        self.order_quantity_spin.setMinimumWidth(180)  # 设置最小宽度180px
        self.order_quantity_spin.setRange(100, 100000)
        self.order_quantity_spin.setValue(100)
        self.order_quantity_spin.setSingleStep(100)
        action_layout.addRow("数量(股):", self.order_quantity_spin)

        self.order_price_spin = QDoubleSpinBox()
        self.order_price_spin.setMinimumWidth(180)  # 设置最小宽度180px
        self.order_price_spin.setRange(0, 9999.99)  # 允许输入0表示市价
        self.order_price_spin.setValue(0)  # 默认市价
        self.order_price_spin.setDecimals(2)
        self.order_price_spin.setSpecialValueText("市价单")  # 0显示为"市价单"
        action_layout.addRow("价格:", self.order_price_spin)

        layout.addWidget(action_group)

        # 有效期设置
        expiry_group = QGroupBox("有效期设置")
        expiry_layout = QFormLayout(expiry_group)
        expiry_layout.setSpacing(12)  # 行间距12px
        expiry_layout.setContentsMargins(15, 20, 15, 15)  # 边距
        # 设置标签和输入框之间的水平间距
        expiry_layout.setHorizontalSpacing(18)  # 标签与输入框间距18px
        expiry_layout.setVerticalSpacing(15)  # 行间距15px

        self.valid_date_edit = QDateEdit()
        self.valid_date_edit.setMinimumWidth(200)  # 设置最小宽度200px
        self.valid_date_edit.setDate(datetime.now().date() + timedelta(days=1))
        self.valid_date_edit.setCalendarPopup(True)
        expiry_layout.addRow("有效日期:", self.valid_date_edit)

        self.valid_time_edit = QDateTimeEdit()
        self.valid_time_edit.setMinimumWidth(250)  # 设置最小宽度250px
        self.valid_time_edit.setDateTime(
            QDateTime.currentDateTime().addDays(1)
        )
        self.valid_time_edit.setDisplayFormat("yyyy-MM-dd hh:mm:ss")
        expiry_layout.addRow("有效期至:", self.valid_time_edit)

        layout.addWidget(expiry_group)

        # 按钮区域
        button_layout = QHBoxLayout()

        self.create_order_btn = QPushButton("➕ 创建条件单")
        self.create_order_btn.setMinimumSize(0, 0)
        self.create_order_btn.setStyleSheet("""
            QPushButton {
                background-color: #0066cc;
                color: white;
                border: none;
                border-radius: 5px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #0077ee;
            }
        """)
        self.create_order_btn.clicked.connect(self.create_order)

        self.clear_form_btn = QPushButton("🔄 清空表单")
        self.clear_form_btn.setMinimumSize(0, 0)
        self.clear_form_btn.clicked.connect(self.clear_form)

        button_layout.addWidget(self.create_order_btn)
        button_layout.addWidget(self.clear_form_btn)
        button_layout.addStretch()

        layout.addLayout(button_layout)

        # 添加弹性空间
        layout.addStretch()

        scroll.setWidget(panel)

        # 返回滚动区域而不是面板
        return scroll

    def create_manage_panel(self) -> QWidget:
        """创建管理面板"""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setSpacing(15)  # 增加垂直间距到15

        # 条件单列表
        list_group = QGroupBox("活跃条件单")
        list_layout = QVBoxLayout(list_group)
        list_layout.setSpacing(10)  # 增加列表内部间距到10

        self.order_table = QTableWidget(0, 7)
        self.order_table.setHorizontalHeaderLabels([
            "ID", "类型", "股票", "条件", "动作", "状态", "操作"
        ])
        header = self.order_table.horizontalHeader()
        if header:
            header.setStretchLastSection(True)
        self.order_table.setAlternatingRowColors(True)
        self.order_table.setMinimumHeight(0)
        self.order_table.cellClicked.connect(self.on_order_clicked)
        list_layout.addWidget(self.order_table)

        # 列表操作按钮
        list_button_layout = QHBoxLayout()

        self.refresh_btn = QPushButton("🔄 刷新")
        self.refresh_btn.clicked.connect(self.refresh_order_list)
        list_button_layout.addWidget(self.refresh_btn)

        self.delete_order_btn = QPushButton("🗑 删除选中")
        self.delete_order_btn.clicked.connect(self.delete_selected_order)
        list_button_layout.addWidget(self.delete_order_btn)

        self.disable_order_btn = QPushButton("⏸ 禁用选中")
        self.disable_order_btn.clicked.connect(self.disable_selected_order)
        list_button_layout.addWidget(self.disable_order_btn)

        self.enable_order_btn = QPushButton("▶ 启用选中")
        self.enable_order_btn.clicked.connect(self.enable_selected_order)
        list_button_layout.addWidget(self.enable_order_btn)

        list_button_layout.addStretch()

        list_layout.addLayout(list_button_layout)
        layout.addWidget(list_group)

        # 触发历史记录
        history_group = QGroupBox("触发历史")
        history_layout = QVBoxLayout(history_group)
        history_layout.setSpacing(10)  # 增加内部间距到10

        self.history_table = QTableWidget(0, 5)
        self.history_table.setHorizontalHeaderLabels([
            "时间", "条件单ID", "条件", "触发价格", "执行结果"
        ])
        header = self.history_table.horizontalHeader()
        if header:
            header.setStretchLastSection(True)
        self.history_table.setAlternatingRowColors(True)
        self.history_table.setMinimumHeight(0)
        history_layout.addWidget(self.history_table)

        layout.addWidget(history_group)

        # 日志输出
        log_group = QGroupBox("运行日志")
        log_layout = QVBoxLayout(log_group)
        log_layout.setSpacing(10)  # 增加内部间距到10
        log_layout.setContentsMargins(8, 8, 8, 8)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(0)
        self.log_text.setMaximumHeight(16777215)
        self.log_text.setStyleSheet("""
            QTextEdit {
                font-family: 'Consolas', 'Courier New', monospace;
                font-size: 12px;
                background-color: #1e1e1e;
                color: #d4d4d4;
                border: 1px solid #444;
            }
        """)
        log_layout.addWidget(self.log_text)

        layout.addWidget(log_group)

        return panel

    def create_condition_ui(self, parent_widget):
        """创建条件配置UI（根据选择的类型）"""
        order_type = self.order_type_combo.currentText()

        if "价格条件单" in order_type:
            self.create_price_condition_ui(parent_widget)
        elif "时间条件单" in order_type:
            self.create_time_condition_ui(parent_widget)
        elif "涨跌幅条件单" in order_type:
            self.create_change_condition_ui(parent_widget)
        elif "止盈止损" in order_type:
            self.create_stop_condition_ui(parent_widget)

    def create_price_condition_ui(self, layout):
        """创建价格条件UI"""
        self.condition_direction_combo = QComboBox()
        self.condition_direction_combo.setMinimumWidth(200)  # 设置最小宽度200px
        self.condition_direction_combo.addItems([
            "价格大于等于",
            "价格小于等于",
            "价格突破"
        ])
        layout.addRow("触发条件:", self.condition_direction_combo)

        self.target_price_spin = QDoubleSpinBox()
        self.target_price_spin.setMinimumWidth(180)  # 设置最小宽度180px
        self.target_price_spin.setRange(0.01, 9999.99)
        self.target_price_spin.setValue(100.0)
        self.target_price_spin.setDecimals(2)
        layout.addRow("目标价格:", self.target_price_spin)

    def create_time_condition_ui(self, layout):
        """创建时间条件UI"""
        self.trigger_time_edit = QDateTimeEdit()
        self.trigger_time_edit.setMinimumWidth(250)  # 设置最小宽度250px
        self.trigger_time_edit.setDateTime(QDateTime.currentDateTime())
        self.trigger_time_edit.setDisplayFormat("yyyy-MM-dd hh:mm:ss")
        layout.addRow("触发时间:", self.trigger_time_edit)

        self.trigger_type_combo = QComboBox()
        self.trigger_type_combo.setMinimumWidth(200)  # 设置最小宽度200px
        self.trigger_type_combo.addItems([
            "立即执行",
            "在集合竞价执行"
        ])
        layout.addRow("执行方式:", self.trigger_type_combo)

    def create_change_condition_ui(self, layout):
        """创建涨跌幅条件UI"""
        self.change_direction_combo = QComboBox()
        self.change_direction_combo.setMinimumWidth(200)  # 设置最小宽度200px
        self.change_direction_combo.addItems([
            "涨幅超过",
            "跌幅超过",
            "涨幅回落",
            "跌幅反弹"
        ])
        layout.addRow("触发条件:", self.change_direction_combo)

        self.change_threshold_spin = QDoubleSpinBox()
        self.change_threshold_spin.setMinimumWidth(180)  # 设置最小宽度180px
        self.change_threshold_spin.setRange(-20.0, 20.0)
        self.change_threshold_spin.setValue(2.0)
        self.change_threshold_spin.setDecimals(2)
        self.change_threshold_spin.setSuffix("%")
        layout.addRow("涨跌幅阈值:", self.change_threshold_spin)

        self.reference_price_combo = QComboBox()
        self.reference_price_combo.setMinimumWidth(200)  # 设置最小宽度200px
        self.reference_price_combo.addItems([
            "前收盘价",
            "今日开盘价",
            "指定价格"
        ])
        layout.addRow("基准价格:", self.reference_price_combo)

        self.ref_price_spin = QDoubleSpinBox()
        self.ref_price_spin.setMinimumWidth(180)  # 设置最小宽度180px
        self.ref_price_spin.setRange(0.01, 9999.99)
        self.ref_price_spin.setValue(100.0)
        self.ref_price_spin.setDecimals(2)
        layout.addRow("指定基准:", self.ref_price_spin)

    def create_stop_condition_ui(self, layout):
        """创建止盈止损UI"""
        self.stop_type_combo = QComboBox()
        self.stop_type_combo.setMinimumWidth(200)  # 设置最小宽度200px
        self.stop_type_combo.addItems([
            "止盈单",
            "止损单",
            "止盈止损"
        ])
        layout.addRow("类型:", self.stop_type_combo)

        self.stop_loss_price_spin = QDoubleSpinBox()
        self.stop_loss_price_spin.setMinimumWidth(180)  # 设置最小宽度180px
        self.stop_loss_price_spin.setRange(0.01, 9999.99)
        self.stop_loss_price_spin.setValue(95.0)
        self.stop_loss_price_spin.setDecimals(2)
        layout.addRow("止损价:", self.stop_loss_price_spin)

        self.stop_profit_price_spin = QDoubleSpinBox()
        self.stop_profit_price_spin.setMinimumWidth(180)  # 设置最小宽度180px
        self.stop_profit_price_spin.setRange(0.01, 9999.99)
        self.stop_profit_price_spin.setValue(110.0)
        self.stop_profit_price_spin.setDecimals(2)
        layout.addRow("止盈价:", self.stop_profit_price_spin)

    def on_order_type_changed(self, index):
        """条件单类型改变事件"""
        # 清空旧的条件UI
        while self.condition_layout.count():
            item = self.condition_layout.takeAt(0)
            if item.widget():
                item.widget().setParent(None)

        # 重新创建条件UI
        self.create_condition_ui(self.condition_layout)

    def get_condition_description(self) -> str:
        """获取条件描述"""
        order_type = self.order_type_combo.currentText()
        desc = f"{order_type} - "

        if "价格条件" in order_type:
            direction = self.condition_direction_combo.currentText()
            price = self.target_price_spin.value()
            desc += f"{direction} {price:.2f}元"

        elif "时间条件" in order_type:
            time_str = self.trigger_time_edit.dateTime().toString("yyyy-MM-dd hh:mm:ss")
            desc += f"在 {time_str} 触发"

        elif "涨跌幅" in order_type:
            direction = self.change_direction_combo.currentText()
            threshold = self.change_threshold_spin.value()
            desc += f"{direction} {threshold:.2f}%"

        elif "止盈止损" in order_type:
            stop_type = self.stop_type_combo.currentText()
            desc += f"{stop_type}"
            if "止盈" in stop_type or "止盈止损" in stop_type:
                profit = self.stop_profit_price_spin.value()
                desc += f" (止盈价: {profit:.2f})"
            if "止损" in stop_type or "止盈止损" in stop_type:
                loss = self.stop_loss_price_spin.value()
                desc += f" (止损价: {loss:.2f})"

        return desc

    def create_order(self):
        """创建条件单"""
        try:
            # 获取基本信息
            order_type = self.order_type_combo.currentText()
            stock_code = self.stock_code_edit.text()
            action = self.action_type_combo.currentText()
            quantity = self.order_quantity_spin.value()
            price = self.order_price_spin.value()
            account_id = ""
            if hasattr(self, "account_combo"):
                account_id = self.account_combo.currentData() or self.account_combo.currentText()
                account_id = str(account_id).strip()

            if not stock_code:
                QMessageBox.warning(self, "输入错误", "请输入股票代码")
                return
            if not account_id:
                QMessageBox.warning(self, "输入错误", "请选择账户")
                return

            # 获取有效期
            expiry_str = self.valid_time_edit.dateTime().toString("yyyy-MM-dd hh:mm:ss")
            try:
                expiry_time = datetime.strptime(expiry_str, "%Y-%m-%d %H:%M:%S")
                if expiry_time <= datetime.now():
                    QMessageBox.warning(
                        self,
                        "有效期错误",
                        f"有效期必须晚于当前时间！\n\n当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"设置的有效期: {expiry_str}\n\n请重新设置有效期。"
                    )
                    return
            except Exception as e:
                QMessageBox.warning(self, "有效期错误", f"有效期格式错误: {str(e)}")
                return

            # 创建条件单对象
            self.order_counter += 1
            order = {
                'id': f"CO{self.order_counter:04d}",
                'type': order_type,
                'stock_code': stock_code,
                'account_id': account_id,
                'action': action,
                'quantity': quantity,
                'price': price,
                'condition': self.get_condition_description(),
                'expiry': expiry_str,
                'status': '等待中',
                'created_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            # 添加到列表
            self.orders.append(order)

            # 更新显示
            self.update_order_table()

            # 日志输出
            self.log("=" * 60)
            self.log(f"创建条件单成功: {order['id']}")
            self.log(f"  类型: {order['type']}")
            self.log(f"  股票: {order['stock_code']}")
            self.log(f"  账户: {order['account_id']}")
            self.log(f"  条件: {order['condition']}")
            self.log(f"  动作: {order['action']} {order['quantity']}股 @ {order['price']:.2f}")
            self.log(f"  有效期至: {order['expiry']}")
            self.log("=" * 60)

            QMessageBox.information(self, "创建成功",
                f"条件单已创建！\n\n"
                f"条件单ID: {order['id']}\n"
                f"类型: {order['type']}\n"
                f"条件: {order['condition']}\n\n"
                f"请在命令行窗口监控执行情况。"
            )

        except Exception as e:
            QMessageBox.critical(self, "创建失败", f"无法创建条件单:\n{str(e)}")
            import traceback
            traceback.print_exc()

    def update_order_table(self):
        """更新条件单表格"""
        self.order_table.setRowCount(len(self.orders))

        for row, order in enumerate(self.orders):
            # ID
            self.order_table.setItem(row, 0, QTableWidgetItem(order['id']))

            # 类型
            order_type = order['type']
            if "价格条件" in order_type:
                type_str = "价格"
            elif "时间条件" in order_type:
                type_str = "时间"
            elif "涨跌幅" in order_type:
                type_str = "涨跌幅"
            elif "止盈止损" in order_type:
                type_str = "止盈止损"
            else:
                type_str = order_type[:4]
            self.order_table.setItem(row, 1, QTableWidgetItem(type_str))

            # 股票
            self.order_table.setItem(row, 2, QTableWidgetItem(order['stock_code']))

            # 条件
            condition = order['condition']
            if len(condition) > 30:
                condition = condition[:30] + "..."
            self.order_table.setItem(row, 3, QTableWidgetItem(condition))

            # 动作
            action_str = f"{order['action']}{order['quantity']}股"
            self.order_table.setItem(row, 4, QTableWidgetItem(action_str))

            # 状态
            status = order['status']
            status_item = QTableWidgetItem(status)
            if status == "等待中":
                status_item.setForeground(QColor(0, 150, 0))
            elif status == "已触发":
                status_item.setForeground(QColor(0, 0, 255))
            elif status == "已过期":
                status_item.setForeground(QColor(150, 150, 150))
            self.order_table.setItem(row, 5, status_item)

            # 操作
            btn_widget = QWidget()
            btn_layout = QHBoxLayout(btn_widget)
            btn_layout.setContentsMargins(5, 2, 5, 2)

            view_btn = QPushButton("查看")
            view_btn.clicked.connect(lambda checked, r=row: self.view_order(r))
            btn_layout.addWidget(view_btn)

            self.order_table.setCellWidget(row, 6, btn_widget)

    def view_order(self, row):
        """查看条件单详情"""
        order = self.orders[row]

        details = f"""
条件单详情

ID: {order['id']}
类型: {order['type']}
股票代码: {order['stock_code']}
条件: {order['condition']}
动作: {order['action']} {order['quantity']}股 @ {order['price']:.2f}
有效期至: {order['expiry']}
状态: {order['status']}
创建时间: {order['created_time']}
        """

        QMessageBox.information(self, f"条件单详情 - {order['id']}", details)

    def on_order_clicked(self, row, col):
        """表格项点击事件"""
        if col == 6:  # 操作列
            pass  # 操作由按钮处理
        else:
            self.view_order(row)

    def delete_selected_order(self):
        """删除选中的条件单"""
        current_row = self.order_table.currentRow()
        if current_row < 0:
            QMessageBox.warning(self, "未选择", "请先选择要删除的条件单")
            return

        order = self.orders[current_row]

        reply = QMessageBox.question(
            self,
            "确认删除",
            f"确定要删除条件单 {order['id']} 吗？",
            QMessageBox.Yes | QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            del self.orders[current_row]
            self.update_order_table()
            self.log(f"条件单已删除: {order['id']}")

    def disable_selected_order(self):
        """禁用选中的条件单"""
        current_row = self.order_table.currentRow()
        if current_row < 0:
            QMessageBox.warning(self, "未选择", "请先选择要禁用的条件单")
            return

        self.orders[current_row]['status'] = '已禁用'
        self.update_order_table()
        self.log(f"条件单已禁用: {self.orders[current_row]['id']}")

    def enable_selected_order(self):
        """启用选中的条件单"""
        current_row = self.order_table.currentRow()
        if current_row < 0:
            QMessageBox.warning(self, "未选择", "请先选择要启用的条件单")
            return

        self.orders[current_row]['status'] = '等待中'
        self.update_order_table()
        self.log(f"条件单已启用: {self.orders[current_row]['id']}")

    def refresh_order_list(self):
        """刷新条件单列表"""
        self.update_order_table()
        self.log("条件单列表已刷新")

    def clear_form(self):
        """清空表单"""
        self.stock_code_edit.clear()
        self.order_quantity_spin.setValue(100)
        self.order_price_spin.setValue(100.0)
        self.log("表单已清空")

    def setup_timer(self):
        """设置定时器"""
        # 监控定时器
        self.monitor_timer = QTimer()
        self.monitor_timer.timeout.connect(self.monitor_orders)
        self.monitor_timer.start(5000)  # 每5秒检查一次

    def init_trade_connection(self):
        """初始化交易连接"""
        if not EASYXT_AVAILABLE:
            self.log("提示: EasyXT不可用，条件单功能受限")
            return

        try:
            import easy_xt
            import json
            import os

            # 读取统一配置文件
            config_file = os.path.join(
                os.path.dirname(__file__), '..', '..', 'config', 'unified_config.json'
            )
            if not os.path.exists(config_file):
                self.log("提示: 未找到统一配置文件 (config/unified_config.json)")
                return

            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # 获取QMT路径和账户ID
            settings = config.get('settings', {})
            account_config = settings.get('account', {})

            userdata_path = account_config.get('qmt_path', '')
            account_id = account_config.get('account_id', '')
            account_type = account_config.get('account_type', 'STOCK')

            if not userdata_path:
                self.log("提示: 统一配置文件中未设置QMT路径 (settings.account.qmt_path)")
                return

            self.log("正在初始化交易连接...")
            self.log(f"  QMT路径: {userdata_path}")
            if account_id:
                self.log(f"  账户ID: {account_id}")
            else:
                self.log("  账户ID: 未设置")

            # 获取扩展API实例
            self.trade_api = easy_xt.get_extended_api()

            # 初始化交易服务
            if hasattr(self.trade_api, 'init_trade'):
                result = self.trade_api.init_trade(userdata_path)
                if result:
                    self._trade_initialized = True
                    self.log("✓ 交易服务连接成功")
                else:
                    self.log("✗ 交易服务连接失败")
                    return

            # 添加账户
            trader = getattr(self.trade_api.trade_api, 'trader', None)
            logged_accounts = []
            if trader and hasattr(trader, 'query_account_infos'):
                infos = trader.query_account_infos() or []
                for info in infos:
                    acc_id = getattr(info, "account_id", None)
                    if acc_id is None:
                        continue
                    logged_accounts.append({
                        "account_id": str(acc_id),
                        "account_type": getattr(info, "account_type", None)
                    })

            added_ids = []
            logged_map = {a["account_id"]: a for a in logged_accounts}
            if logged_accounts:
                self.log(f"✓ 已登录账户: {', '.join([a['account_id'] for a in logged_accounts])}")
                for account in logged_accounts:
                    acc_id = account.get("account_id")
                    acc_type = account.get("account_type", account_type)
                    if not acc_id:
                        continue
                    if self.trade_api.add_account(acc_id, acc_type):
                        added_ids.append(acc_id)
                if added_ids:
                    if account_id and account_id in added_ids:
                        selected_id = account_id
                    else:
                        selected_id = added_ids[0]
                        if account_id:
                            self.log(f"⚠️ 配置账户不可用，已切换到: {selected_id}")
                    account_id = selected_id
            elif account_id:
                if self.trade_api.add_account(account_id, account_type):
                    added_ids.append(account_id)
                    self.log(f"✓ 已添加账户: {account_id} ({account_type})")
                else:
                    self.log(f"✗ 添加账户失败: {account_id}")
            else:
                self.log("✗ 未检测到账户，无法添加")

            if account_id:
                selected_type = account_type
                if account_id in logged_map:
                    selected_type = logged_map[account_id].get("account_type", selected_type)
                account_config['account_id'] = account_id
                if selected_type is not None:
                    try:
                        from xtquant import xtconstant
                        account_config['account_type'] = xtconstant.ACCOUNT_TYPE_DICT.get(selected_type, selected_type)
                    except Exception:
                        account_config['account_type'] = selected_type
                config['settings']['account'] = account_config
                try:
                    with open(config_file, 'w', encoding='utf-8') as f:
                        json.dump(config, f, ensure_ascii=False, indent=2)
                    self.log(f"✓ 已更新默认账户配置: {account_id}")
                except Exception as e:
                    self.log(f"✗ 更新默认账户配置失败: {e}")

            if hasattr(self, "account_combo"):
                self.account_combo.clear()
                if logged_accounts:
                    for account in logged_accounts:
                        acc_id = account.get("account_id")
                        acc_type = account.get("account_type")
                        if not acc_id:
                            continue
                        label = str(acc_id)
                        if acc_type is not None:
                            label = f"{label} ({acc_type})"
                        self.account_combo.addItem(label, str(acc_id))
                elif added_ids:
                    for acc_id in added_ids:
                        self.account_combo.addItem(str(acc_id), str(acc_id))
                preferred_id = account_id or "1678070127"
                idx = self.account_combo.findData(str(preferred_id))
                if idx >= 0:
                    self.account_combo.setCurrentIndex(idx)

        except Exception as e:
            self.log(f"初始化交易连接时出错: {str(e)}")
            import traceback
            traceback.print_exc()

    def monitor_orders(self):
        """监控条件单并自动触发"""
        if not EASYXT_AVAILABLE:
            return

        try:

            for order in self.orders:
                # 跳过已触发、已禁用或已过期的条件单
                if order['status'] not in ['等待中']:
                    continue

                # 检查是否过期
                try:
                    expiry_time = datetime.strptime(order['expiry'], "%Y-%m-%d %H:%M:%S")
                    if datetime.now() > expiry_time:
                        order['status'] = '已过期'
                        self.log(f"条件单已过期: {order['id']}")
                        self.update_order_table()
                        continue
                except Exception:
                    pass

                # 根据条件单类型进行监控
                order_type = order['type']
                stock_code = order['stock_code']

                # 获取当前价格
                current_price = self._get_current_price(stock_code)
                if current_price is None or current_price <= 0:
                    continue

                # 检查是否触发条件
                triggered = False

                if "价格条件单" in order_type:
                    triggered = self._check_price_condition(order, current_price)
                elif "涨跌幅条件单" in order_type:
                    triggered = self._check_change_condition(order, current_price)
                elif "时间条件单" in order_type:
                    triggered = self._check_time_condition(order)
                elif "止盈止损单" in order_type:
                    triggered = self._check_stop_condition(order, current_price)

                # 如果触发条件满足，执行交易
                if triggered:
                    self._execute_order(order, current_price)

        except Exception as e:
            self.log(f"监控条件单时出错: {str(e)}")

    def _get_current_price(self, stock_code: str) -> Optional[float]:
        """获取股票当前价格"""
        try:
            from xtquant import xtdata
            from easy_xt.utils import StockCodeUtils

            normalized_code = StockCodeUtils.normalize_code(stock_code)

            # 尝试使用get_full_tick获取实时价格
            tick_data = xtdata.get_full_tick([normalized_code])
            if tick_data and normalized_code in tick_data:
                tick_info = tick_data[normalized_code]
                if tick_info and 'lastPrice' in tick_info:
                    return float(tick_info['lastPrice'])
                elif tick_info and 'price' in tick_info:
                    return float(tick_info['price'])

            # 如果失败，尝试get_market_data
            current_data = xtdata.get_market_data(
                stock_list=[normalized_code],
                period='tick',
                count=1
            )

            if current_data and isinstance(current_data, dict) and normalized_code in current_data:
                data_array = current_data[normalized_code]
                if hasattr(data_array, '__len__') and len(data_array) > 0:
                    first_item = data_array[0]
                    if hasattr(first_item, 'lastPrice'):
                        return float(first_item['lastPrice'])

            return None
        except Exception as e:
            print(f"获取{stock_code}当前价格失败: {str(e)}")
            return None

    def _check_price_condition(self, order: dict, current_price: float) -> bool:
        """检查价格条件"""
        try:
            condition = order['condition']
            # 解析条件，例如："价格条件单 - 价格大于等于 5.00元"
            if "价格大于等于" in condition:
                import re
                match = re.search(r'(\d+\.?\d*)元', condition)
                if match:
                    target_price = float(match.group(1))
                    return current_price >= target_price

            elif "价格小于等于" in condition:
                import re
                match = re.search(r'(\d+\.?\d*)元', condition)
                if match:
                    target_price = float(match.group(1))
                    return current_price <= target_price

            elif "价格突破" in condition:
                import re
                match = re.search(r'(\d+\.?\d*)元', condition)
                if match:
                    target_price = float(match.group(1))
                    # 突破通常指从下向上突破
                    return current_price > target_price

            return False
        except Exception as e:
            print(f"检查价格条件失败: {str(e)}")
            return False

    def _check_change_condition(self, order: dict, current_price: float) -> bool:
        """检查涨跌幅条件"""
        try:
            # 需要获取基准价格
            # 这里简化处理，假设基准价格已存储
            # 实际需要根据reference_price_combo获取
            return False
        except Exception:
            return False

    def _check_time_condition(self, order: dict) -> bool:
        """检查时间条件"""
        try:
            condition = order['condition']
            # 解析触发时间，例如："时间条件单 - 在 2026-01-27 16:30:00 触发"
            import re
            match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', condition)
            if match:
                trigger_time = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
                return datetime.now() >= trigger_time
            return False
        except Exception:
            return False

    def _check_stop_condition(self, order: dict, current_price: float) -> bool:
        """检查止盈止损条件"""
        try:
            condition = order['condition']
            # 解析止盈止损价格
            import re
            has_stop_loss = "止损价" in condition
            has_stop_profit = "止盈价" in condition

            if has_stop_loss:
                match = re.search(r'止损价: (\d+\.?\d*)', condition)
                if match:
                    stop_loss_price = float(match.group(1))
                    if current_price <= stop_loss_price:
                        return True

            if has_stop_profit:
                match = re.search(r'止盈价: (\d+\.?\d*)', condition)
                if match:
                    stop_profit_price = float(match.group(1))
                    if current_price >= stop_profit_price:
                        return True

            return False
        except Exception:
            return False

    def _execute_order(self, order: dict, current_price: float):
        """执行订单"""
        try:
            # 检查交易API是否已初始化
            if self.trade_api is None or not self._trade_initialized:
                self.log("提示: 交易API未初始化，请检查配置文件中的QMT路径")
                self.add_to_history(order, current_price, "交易服务未连接")
                return

            # 检查trade_api是否存在
            if not hasattr(self.trade_api, 'trade_api') or self.trade_api.trade_api is None:
                self.log("提示: trade_api未初始化")
                self.add_to_history(order, current_price, "交易服务未连接")
                return

            # 检查是否已添加账户
            if not hasattr(self.trade_api.trade_api, 'accounts') or not self.trade_api.trade_api.accounts:
                self.log("提示: 未添加交易账户，请先在'网格交易'中配置账户")
                self.add_to_history(order, current_price, "未添加交易账户")
                return

            account_id = order.get('account_id')
            accounts = self.trade_api.trade_api.accounts or {}
            if not account_id and hasattr(self, "account_combo"):
                account_id = self.account_combo.currentData() or self.account_combo.currentText()
            account_id = str(account_id).strip() if account_id else ""
            if not account_id or account_id not in accounts:
                if accounts:
                    account_id = list(accounts.keys())[0]
                else:
                    self.log("提示: 未添加交易账户")
                    self.add_to_history(order, current_price, "未添加交易账户")
                    return

            # 确定订单类型
            action = order['action']
            order_type = 'buy' if action == '买入' else 'sell'

            # 确定下单价格（0表示市价）
            order_price = order['price'] if order['price'] > 0 else current_price
            price_type = 'limit' if order['price'] > 0 else 'market'

            # 执行下单
            if order_type == 'buy':
                order_id = self.trade_api.trade_api.buy(
                    account_id=account_id,
                    code=order['stock_code'],
                    volume=order['quantity'],
                    price=order_price,
                    price_type=price_type
                )
            else:
                order_id = self.trade_api.trade_api.sell(
                    account_id=account_id,
                    code=order['stock_code'],
                    volume=order['quantity'],
                    price=order_price,
                    price_type=price_type
                )

            if order_id:
                order['status'] = '已触发'
                self.update_order_table()
                self.log(f"✓ 条件单触发成功: {order['id']}, 委托号: {order_id}")

                # 添加到触发历史
                self.add_to_history(order, current_price, f"委托成功: {order_id}")
            else:
                self.log(f"✗ 条件单触发失败: {order['id']}, 下单失败")
                self.add_to_history(order, current_price, "下单失败")

        except Exception as e:
            self.log(f"✗ 执行条件单失败: {str(e)}")
            self.add_to_history(order, current_price, f"执行异常: {str(e)}")
            import traceback
            traceback.print_exc()

    def add_to_history(self, order: dict, trigger_price: float, result: str):
        """添加到触发历史"""
        row = self.history_table.rowCount()
        self.history_table.insertRow(row)

        timestamp = datetime.now().strftime("%H:%M:%S")
        self.history_table.setItem(row, 0, QTableWidgetItem(timestamp))
        self.history_table.setItem(row, 1, QTableWidgetItem(order['id']))

        condition = order['condition']
        if len(condition) > 20:
            condition = condition[:20] + "..."
        self.history_table.setItem(row, 2, QTableWidgetItem(condition))

        self.history_table.setItem(row, 3, QTableWidgetItem(f"{trigger_price:.2f}"))
        self.history_table.setItem(row, 4, QTableWidgetItem(result))

    def log(self, message: str):
        """输出日志"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_message = f"[{timestamp}] {message}"
        self.log_text.append(log_message)
        self.log_text.moveCursor(QTextCursor.End)


# 导出类
__all__ = ['ConditionalOrderWidget']
