#!/usr/bin/env python3
"""
条件单GUI组件
提供条件单的可视化配置、管理和监控界面
"""

import importlib.util
import os
import sys
import threading
from datetime import datetime, timedelta
from types import ModuleType
from typing import Optional

from PyQt5.QtCore import QDateTime, Qt, QThread, QTimer, pyqtSignal
from PyQt5.QtGui import QColor, QTextCursor
from PyQt5.QtWidgets import (
    QComboBox,
    QDateEdit,
    QDateTimeEdit,
    QDoubleSpinBox,
    QFormLayout,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

EASYXT_AVAILABLE = importlib.util.find_spec("easy_xt") is not None


class _TradeInitThread(QThread):
    finished = pyqtSignal(object)
    failed = pyqtSignal(str)

    def __init__(self):
        super().__init__()

    def run(self):
        if not EASYXT_AVAILABLE:
            self.finished.emit({"logs": ["提示: EasyXT不可用，条件单功能受限"], "ok": False})
            return
        try:
            import json
            import easy_xt

            logs = []
            config_file = os.path.join(
                os.path.dirname(__file__), "..", "..", "config", "unified_config.json"
            )
            if not os.path.exists(config_file):
                self.finished.emit({"logs": ["提示: 未找到统一配置文件 (config/unified_config.json)"], "ok": False})
                return
            with open(config_file, encoding="utf-8") as f:
                config = json.load(f)
            settings = config.get("settings", {})
            account_config = settings.get("account", {})
            userdata_path = account_config.get("qmt_path", "")
            account_id = account_config.get("account_id", "")
            env_account_id = os.environ.get("EASYXT_ACCOUNT_ID", "").strip()
            if env_account_id:
                account_id = env_account_id
            if isinstance(account_id, str) and account_id.startswith("__REPLACE_"):
                account_id = ""
            account_type = account_config.get("account_type", "STOCK")
            if not userdata_path:
                self.finished.emit({"logs": ["提示: 统一配置文件中未设置QMT路径 (settings.account.qmt_path)"], "ok": False})
                return

            logs.append("正在初始化交易连接...")
            logs.append(f"  QMT路径: {userdata_path}")
            logs.append(f"  账户ID: {account_id if account_id else '未设置'}")

            # get_extended_api() 内部已使用 _xt_init_lock 保护，无需手动获取
            try:
                trade_api = easy_xt.get_extended_api()
                if isinstance(trade_api, ModuleType) or not hasattr(trade_api, "trade_api"):
                    from easy_xt.extended_api import ExtendedAPI

                    trade_api = ExtendedAPI()

                if hasattr(trade_api, "init_trade"):
                    result = trade_api.init_trade(userdata_path)
                    if not result:
                        self.finished.emit({"logs": logs + ["✗ 交易服务连接失败"], "ok": False})
                        return
                    logs.append("✓ 交易服务连接成功")

                raw_trade_api = getattr(trade_api, "trade_api", None)
                trader = getattr(raw_trade_api, "trader", None)
                logged_accounts = []
                if trader and hasattr(trader, "query_account_infos"):
                    infos = trader.query_account_infos() or []
                    for info in infos:
                        acc_id = getattr(info, "account_id", None)
                        if acc_id is None:
                            continue
                        logged_accounts.append(
                            {
                                "account_id": str(acc_id),
                                "account_type": getattr(info, "account_type", None),
                            }
                        )

                added_ids = []
                if logged_accounts:
                    logs.append(f"✓ 已登录账户: {', '.join([a['account_id'] for a in logged_accounts])}")
                    for account in logged_accounts:
                        acc_id = account.get("account_id")
                        acc_type = account.get("account_type", account_type)
                        if acc_id and trade_api.add_account(acc_id, acc_type):
                            added_ids.append(acc_id)
                    if added_ids:
                        if account_id and account_id in added_ids:
                            selected_id = account_id
                        else:
                            selected_id = added_ids[0]
                            if account_id:
                                logs.append(f"⚠️ 配置账户不可用，已切换到: {selected_id}")
                        account_id = selected_id
                elif account_id:
                    if trade_api.add_account(account_id, account_type):
                        added_ids.append(account_id)
                        logs.append(f"✓ 已添加账户: {account_id} ({account_type})")
                    else:
                        logs.append(f"✗ 添加账户失败: {account_id}")
                else:
                    logs.append("✗ 未检测到账户，无法添加")
            finally:
                pass

            self.finished.emit(
                {
                    "ok": True,
                    "logs": logs,
                    "trade_api": trade_api,
                    "logged_accounts": logged_accounts,
                    "added_ids": added_ids,
                    "account_id": account_id,
                }
            )
        except Exception as e:
            self.failed.emit(f"初始化交易连接时出错: {e}")


class _PriceMonitorThread(QThread):
    """后台价格采样线程：批量获取所有待监控股票的最新价，避免在 UI 主线程同步调用 xtdata"""

    prices_ready = pyqtSignal(dict)  # {order_id: float or None}

    def __init__(self, orders: list):
        super().__init__()
        # 仅保存必要数据，避免持有 widget 引用
        self._tasks: list[tuple[str, str]] = [
            (order["id"], order["stock_code"]) for order in orders
        ]

    def run(self):
        if not EASYXT_AVAILABLE:
            self.prices_ready.emit({})
            return

        results: dict[str, Optional[float]] = {}
        try:
            import easy_xt
            from easy_xt.utils import StockCodeUtils
            broker = easy_xt.get_xtquant_broker()
        except Exception:
            self.prices_ready.emit({})
            return

        # 归一化并去重，批量拉取
        code_to_orders: dict[str, list[str]] = {}
        for order_id, stock_code in self._tasks:
            try:
                normalized = StockCodeUtils.normalize_code(stock_code)
            except Exception:
                normalized = stock_code
            code_to_orders.setdefault(normalized, []).append(order_id)

        unique_codes = list(code_to_orders.keys())
        price_map: dict[str, float] = {}

        # 主路径：get_full_tick 批量拉取
        try:
            tick_data = broker.get_full_tick(unique_codes) or {}
            for code, tick_info in tick_data.items():
                if not tick_info:
                    continue
                if "lastPrice" in tick_info:
                    price_map[code] = float(tick_info["lastPrice"])
                elif "price" in tick_info:
                    price_map[code] = float(tick_info["price"])
        except Exception as exc:
            print(f"[PriceMonitorThread] get_full_tick failed: {exc}")

        # 兜底：对仍缺价格的 code 用 get_market_data
        missing = [c for c in unique_codes if c not in price_map]
        if missing:
            try:
                current_data = broker.get_market_data(
                    stock_list=missing, period="tick", count=1
                )
                if current_data and isinstance(current_data, dict):
                    for code, data_array in current_data.items():
                        if hasattr(data_array, "__len__") and len(data_array) > 0:
                            first_item = data_array[0]
                            if hasattr(first_item, "lastPrice"):
                                try:
                                    price_map[code] = float(first_item["lastPrice"])
                                except Exception:
                                    pass
            except Exception as exc:
                print(f"[PriceMonitorThread] get_market_data fallback failed: {exc}")

        # 将价格映射回 order_id
        for code, order_ids in code_to_orders.items():
            price = price_map.get(code)
            for oid in order_ids:
                results[oid] = price

        self.prices_ready.emit(results)


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
        self._trade_init_thread: Optional[QThread] = None
        self._price_monitor_thread: Optional[QThread] = None
        self.init_ui()
        self.setup_timer()
        self.init_trade_connection()  # 自动初始化交易连接（异步）

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
                item.widget().deleteLater()

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
        # 监控定时器：触发后台价格采样，不在 UI 线程同步拉取行情
        self.monitor_timer = QTimer()
        self.monitor_timer.timeout.connect(self._start_price_monitor)
        self.monitor_timer.start(5000)  # 每5秒检查一次

    def init_trade_connection(self):
        """初始化交易连接"""
        if self._trade_init_thread is not None and self._trade_init_thread.isRunning():
            return
        self._trade_init_thread = _TradeInitThread()
        self._trade_init_thread.finished.connect(self._on_trade_init_finished)
        self._trade_init_thread.failed.connect(self._on_trade_init_failed)
        self._trade_init_thread.start()

    def _on_trade_init_finished(self, result: dict):
        self._trade_init_thread = None
        for line in result.get("logs", []):
            self.log(line)
        if not result.get("ok"):
            return
        self.trade_api = result.get("trade_api")
        self._trade_initialized = True
        account_id = result.get("account_id", "")
        logged_accounts = result.get("logged_accounts", [])
        added_ids = result.get("added_ids", [])
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

    def _on_trade_init_failed(self, message: str):
        self._trade_init_thread = None
        self.log(message)

    def _start_price_monitor(self):
        """定时器回调：先做纯CPU的过期检查，再把行情拉取派到后台线程"""
        if not EASYXT_AVAILABLE:
            return
        # 上一批尚未完成，跳过本轮（避免积压）
        if self._price_monitor_thread is not None and self._price_monitor_thread.isRunning():
            return

        pending = []
        updated = False
        for order in self.orders:
            if order["status"] not in ["等待中"]:
                continue
            # 过期检查（纯 CPU，主线程安全）
            try:
                expiry_time = datetime.strptime(order["expiry"], "%Y-%m-%d %H:%M:%S")
                if datetime.now() > expiry_time:
                    order["status"] = "已过期"
                    self.log(f"条件单已过期: {order['id']}")
                    updated = True
                    continue
            except Exception:
                pass
            pending.append(order)

        if updated:
            self.update_order_table()
        if not pending:
            return

        self._price_monitor_thread = _PriceMonitorThread(pending)
        self._price_monitor_thread.prices_ready.connect(self._on_prices_ready)
        self._price_monitor_thread.start()

    def _on_prices_ready(self, prices: dict):
        """后台价格采样完成回调（UI 主线程执行，无 I/O）"""
        self._price_monitor_thread = None
        try:
            updated = False
            for order in self.orders:
                if order["status"] not in ["等待中"]:
                    continue
                order_type = order["type"]
                order_id = order["id"]

                # 时间条件单不依赖价格
                if "时间条件单" in order_type:
                    if self._check_time_condition(order):
                        self._execute_order(order, 0.0)
                        updated = True
                    continue

                current_price = prices.get(order_id)
                if current_price is None or current_price <= 0:
                    continue

                triggered = False
                if "价格条件单" in order_type:
                    triggered = self._check_price_condition(order, current_price)
                elif "涨跌幅条件单" in order_type:
                    triggered = self._check_change_condition(order, current_price)
                elif "止盈止损单" in order_type:
                    triggered = self._check_stop_condition(order, current_price)

                if triggered:
                    self._execute_order(order, current_price)
                    updated = True

            if updated:
                self.update_order_table()
        except Exception as e:
            self.log(f"处理价格监控结果时出错: {e}")

    def monitor_orders(self):
        """[已废弃] 保留供外部兼容调用，实际逻辑已迁移至 _start_price_monitor"""
        self._start_price_monitor()

    def _get_current_price(self, stock_code: str) -> Optional[float]:
        """获取股票当前价格"""
        try:
            import easy_xt
            from easy_xt.utils import StockCodeUtils

            normalized_code = StockCodeUtils.normalize_code(stock_code)
            broker = easy_xt.get_xtquant_broker()

            # 尝试使用get_full_tick获取实时价格
            tick_data = broker.get_full_tick([normalized_code])
            if tick_data and normalized_code in tick_data:
                tick_info = tick_data[normalized_code]
                if tick_info and 'lastPrice' in tick_info:
                    return float(tick_info['lastPrice'])
                elif tick_info and 'price' in tick_info:
                    return float(tick_info['price'])

            # 如果失败，尝试get_market_data
            current_data = broker.get_market_data(
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
        """执行订单（校验在主线程，交易 I/O 推后台线程）"""
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

            # 交易 I/O 在后台线程执行，避免阻塞主线程
            threading.Thread(
                target=self._execute_order_bg,
                args=(order, current_price, account_id),
                daemon=True,
            ).start()

        except Exception as e:
            self.log(f"✗ 执行条件单失败: {str(e)}")
            self.add_to_history(order, current_price, f"执行异常: {str(e)}")
            import traceback
            traceback.print_exc()

    def _execute_order_bg(self, order: dict, current_price: float, account_id: str):
        """后台线程：执行交易 API 调用，结果通过 QTimer 回主线程更新 UI"""
        try:
            trade_api = self.trade_api
            if trade_api is None or not hasattr(trade_api, "trade_api") or trade_api.trade_api is None:
                QTimer.singleShot(0, lambda: self._apply_execute_error(order, current_price, "交易API未初始化"))
                return
            action = order['action']
            order_type = 'buy' if action == '买入' else 'sell'
            order_price = order['price'] if order['price'] > 0 else current_price
            price_type = 'limit' if order['price'] > 0 else 'market'

            if order_type == 'buy':
                order_id = trade_api.trade_api.buy(
                    account_id=account_id,
                    code=order['stock_code'],
                    volume=order['quantity'],
                    price=order_price,
                    price_type=price_type
                )
            else:
                order_id = trade_api.trade_api.sell(
                    account_id=account_id,
                    code=order['stock_code'],
                    volume=order['quantity'],
                    price=order_price,
                    price_type=price_type
                )

            # UI 更新回主线程
            QTimer.singleShot(0, lambda: self._apply_execute_result(order, current_price, order_id))

        except Exception as e:
            err_text = str(e)
            QTimer.singleShot(
                0,
                lambda err=err_text: self._apply_execute_error(order, current_price, err),
            )

    def _apply_execute_result(self, order: dict, current_price: float, order_id):
        """主线程：应用交易结果到 UI"""
        if order_id:
            order['status'] = '已触发'
            self.update_order_table()
            self.log(f"✓ 条件单触发成功: {order['id']}, 委托号: {order_id}")
            self.add_to_history(order, current_price, f"委托成功: {order_id}")
        else:
            self.log(f"✗ 条件单触发失败: {order['id']}, 下单失败")
            self.add_to_history(order, current_price, "下单失败")

    def _apply_execute_error(self, order: dict, current_price: float, error_msg: str):
        """主线程：显示交易执行异常"""
        self.log(f"✗ 执行条件单失败: {error_msg}")
        self.add_to_history(order, current_price, f"执行异常: {error_msg}")

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

    def closeEvent(self, event):
        try:
            if hasattr(self, "monitor_timer") and self.monitor_timer is not None:
                self.monitor_timer.stop()
            if self._price_monitor_thread is not None and self._price_monitor_thread.isRunning():
                self._price_monitor_thread.requestInterruption()
                self._price_monitor_thread.quit()
                self._price_monitor_thread.wait(1000)
            if self._trade_init_thread is not None and self._trade_init_thread.isRunning():
                self._trade_init_thread.requestInterruption()
                self._trade_init_thread.quit()
                self._trade_init_thread.wait(1000)
        finally:
            super().closeEvent(event)


# 导出类
__all__ = ['ConditionalOrderWidget']
