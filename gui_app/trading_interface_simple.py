#!/usr/bin/env python3
"""
简洁交易界面 - 基于您喜欢的UI设计
模仿专业交易软件的简洁风格
"""

import json
import os
import sys
from datetime import datetime

from PyQt5.QtCore import QThread, QTimer, pyqtSignal
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QStatusBar,
    QVBoxLayout,
    QWidget,
)

from core.events import Events
from core.signal_bus import signal_bus
from gui_app.enhanced.operation_panel import (
    AccountPanel,
    BasicOrderValidator,
    BlacklistValidator,
    ConcentrationValidator,
    DailyLossValidator,
    OrderData,
    OrderPanel,
    PositionPanel,
    RiskOrderValidator,
    ValidatorChain,
)

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

EASYXT_AVAILABLE = True


class TradingInterface(QMainWindow):
    """简洁交易界面主窗口"""

    # 信号定义
    account_updated = pyqtSignal(dict)
    position_updated = pyqtSignal(list)

    def __init__(self):
        super().__init__()
        self.account_id = None
        self.account_type = "STOCK"
        self.userdata_path = None
        self.session_id = None
        self.is_connected = False
        self.account_info = {}
        self.positions = []
        self.max_position_ratio = 0.3
        self.max_concentration_ratio = 0.1
        self.max_daily_loss_ratio = 0.05
        self.blacklist_patterns = []
        self.max_daily_orders = 100
        self.daily_order_count = 0
        self.daily_order_date = datetime.now().date()
        self.rejection_total = 0
        self.rejection_by_reason = {}
        self.rejection_by_symbol = {}
        self._api_init_thread = None
        self._refresh_thread = None
        self._refresh_in_progress = False

        self.easyxt = None

        self.load_account_config()
        self.order_validator = self._build_order_validator()
        self.init_ui()
        self.setup_timer()
        self.setup_style()
        self._connect_events()
        QTimer.singleShot(0, self._init_api_async)

    def _init_api_async(self):
        if not EASYXT_AVAILABLE or self._api_init_thread is not None:
            return

        class _ApiInitThread(QThread):
            ready = pyqtSignal(object)
            error = pyqtSignal(str)

            def run(self):
                try:
                    import easy_xt

                    api = easy_xt.get_api()
                    self.ready.emit(api)
                except Exception as exc:
                    self.error.emit(str(exc))

        thread = _ApiInitThread(self)
        thread.ready.connect(self._on_api_ready)
        thread.error.connect(self._on_api_error)
        self._api_init_thread = thread
        thread.start()

    def _on_api_ready(self, api):
        self.easyxt = api
        if self._api_init_thread is not None:
            self._api_init_thread.quit()
            # Fix 58: 不在主线程 wait()，改用 deleteLater 延迟清理
            self._api_init_thread.deleteLater()
            self._api_init_thread = None
        QTimer.singleShot(0, self.connect_to_trading)

    def _on_api_error(self, message: str):
        if self._api_init_thread is not None:
            self._api_init_thread.quit()
            self._api_init_thread.deleteLater()
            self._api_init_thread = None
        self.status_bar.showMessage(f"交易接口初始化失败: {message}")

    def _connect_events(self):
        signal_bus.subscribe(Events.CHART_DATA_LOADED, self.update_stock_code)
        signal_bus.subscribe(Events.ORDER_REQUESTED, self._handle_order_request)
        signal_bus.subscribe(Events.ORDER_BATCH_REQUESTED, self._handle_batch_request)

    def update_stock_code(self, symbol: str, **kwargs):
        if not symbol:
            return
        self.order_panel.set_stock_code(symbol)

    def _handle_order_request(
        self,
        symbol: str,
        side: str,
        price: float,
        volume: int,
        source: str = "grid",
        **kwargs,
    ):
        return self.submit_unified_order(symbol, side, price, volume, source=source)

    def _handle_batch_request(self, orders: list[dict], source: str = "batch", **kwargs):
        return self.submit_batch_orders(orders, source=source)

    def init_ui(self):
        """初始化用户界面"""
        self.setWindowTitle("量化交易系统")
        self.setGeometry(100, 100, 800, 600)

        # 创建中央窗口
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # 主布局
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(5)
        main_layout.setContentsMargins(5, 5, 5, 5)

        # 顶部状态栏
        self.create_top_status_bar(main_layout)

        # 账户信息区域
        self.create_account_info_section(main_layout)

        # 交易操作区域
        self.create_trading_section(main_layout)

        # 持仓列表区域
        self.create_position_section(main_layout)

        # 底部状态栏
        self.create_status_bar()

    def create_top_status_bar(self, parent_layout):
        """创建顶部状态栏"""
        top_frame = QFrame()
        top_frame.setFrameStyle(QFrame.StyledPanel)
        top_frame.setFixedHeight(40)

        top_layout = QHBoxLayout(top_frame)
        top_layout.setContentsMargins(10, 5, 10, 5)

        # 实盘交易标签
        self.trading_mode_label = QLabel("📊 实盘交易")
        self.trading_mode_label.setFont(QFont("微软雅黑", 10, QFont.Bold))

        # 连接状态标签
        self.connection_status_label = QLabel("🔴 未连接")
        self.connection_status_label.setFont(QFont("微软雅黑", 9))

        # 连接交易服务按钮
        self.connect_btn = QPushButton("🔌 连接交易服务")
        self.connect_btn.setFixedSize(120, 30)
        self.connect_btn.clicked.connect(self.toggle_connection)
        self.connect_btn.setVisible(True)

        top_layout.addWidget(self.trading_mode_label)
        top_layout.addStretch()
        top_layout.addWidget(self.connection_status_label)
        top_layout.addWidget(self.connect_btn)

        parent_layout.addWidget(top_frame)

    def create_account_info_section(self, parent_layout):
        self.account_panel = AccountPanel()
        self.account_panel.account_changed.connect(self.on_account_changed)
        parent_layout.addWidget(self.account_panel)
        self._update_rejection_display()

    def create_trading_section(self, parent_layout):
        self.order_panel = OrderPanel()
        self.order_panel.order_requested.connect(self.on_order_requested)
        parent_layout.addWidget(self.order_panel)

    def create_position_section(self, parent_layout):
        self.position_panel = PositionPanel()
        parent_layout.addWidget(self.position_panel)

    def create_status_bar(self):
        """创建底部状态栏"""
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

        # 添加状态信息
        self.status_bar.showMessage("就绪")

        # 添加时间标签
        self.time_label = QLabel()
        self.status_bar.addPermanentWidget(self.time_label)

    def setup_timer(self):
        """设置定时器"""
        # 更新时间定时器
        self.time_timer = QTimer()
        self.time_timer.timeout.connect(self.update_time)
        self.time_timer.start(1000)  # 每秒更新

        # 数据更新定时器
        self.data_timer = QTimer()
        self.data_timer.timeout.connect(self.update_data)
        self.data_timer.start(5000)  # 每5秒更新

    def setup_style(self):
        """设置界面样式"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f0f0f0;
            }
            QGroupBox {
                font-weight: bold;
                border: 2px solid #cccccc;
                border-radius: 5px;
                margin-top: 1ex;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
            QTableWidget {
                gridline-color: #d0d0d0;
                background-color: white;
                alternate-background-color: #f8f8f8;
            }
            QTableWidget::item {
                padding: 5px;
            }
            QComboBox, QSpinBox, QDoubleSpinBox {
                padding: 5px;
                border: 1px solid #ccc;
                border-radius: 3px;
            }
            QFrame {
                background-color: #e8e8e8;
                border: 1px solid #ccc;
            }
        """)

    def load_account_config(self):
        """加载账户配置"""
        config_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        unified_path = os.path.join(config_root, 'config', 'unified_config.json')
        gui_config_path = os.path.join(os.path.dirname(__file__), 'config.json')

        if os.path.exists(unified_path):
            try:
                with open(unified_path, encoding='utf-8') as f:
                    unified_config = json.load(f)
                account_config = unified_config.get('settings', {}).get('account', {})
                risk_config = unified_config.get('settings', {}).get('risk', {})
                system_qmt = unified_config.get('system', {}).get('qmt', {})
                account_id = account_config.get('account_id')
                env_account_id = os.environ.get('EASYXT_ACCOUNT_ID', '').strip()
                if env_account_id:
                    account_id = env_account_id
                if isinstance(account_id, str) and account_id.startswith("__REPLACE_"):
                    account_id = ""
                self.account_id = account_id or self.account_id
                self.account_type = account_config.get('account_type', self.account_type)
                qmt_path = account_config.get('qmt_path')
                self.session_id = system_qmt.get('session_id', self.session_id)
                self.max_position_ratio = risk_config.get("max_position_ratio", self.max_position_ratio)
                self.max_concentration_ratio = risk_config.get("max_concentration_ratio", self.max_concentration_ratio)
                self.max_daily_loss_ratio = risk_config.get("stop_loss_ratio", self.max_daily_loss_ratio)
                self.blacklist_patterns = risk_config.get("blacklist", self.blacklist_patterns)

                if qmt_path:
                    if qmt_path.lower().endswith('.exe'):
                        base_dir = os.path.dirname(os.path.dirname(qmt_path))
                        self.userdata_path = os.path.join(base_dir, 'userdata_mini')
                    else:
                        self.userdata_path = qmt_path
            except Exception:
                pass

        if not self.userdata_path and os.path.exists(gui_config_path):
            try:
                with open(gui_config_path, encoding='utf-8') as f:
                    gui_config = json.load(f)
                qmt_config = gui_config.get('qmt', {})
                self.userdata_path = qmt_config.get('userdata_path', self.userdata_path)
            except Exception:
                pass

    def toggle_connection(self):
        """切换连接状态"""
        if not self.is_connected:
            self.connect_to_trading()
        else:
            self.disconnect_from_trading()

    def connect_to_trading(self):
        """连接到交易服务。当 easyxt 不可用或未传入时自动切换为模拟模式。"""
        try:
            if not EASYXT_AVAILABLE or not self.easyxt:
                # 模拟连接模式：加载演示数据，供开发/测试/离线环境使用
                self.is_connected = True
                self.connection_status_label.setText("🟡 模拟模式")
                self.connect_btn.setText("🔌 断开连接")
                self.status_bar.showMessage("运行于模拟模式（无实盘连接）")
                self.load_demo_data()
                return

            if not self.userdata_path or not self.account_id:
                return

            trade_connected = self.easyxt.init_trade(self.userdata_path, self.session_id)
            if not trade_connected:
                return

            logged_accounts = self._get_logged_accounts()
            if logged_accounts:
                added_ids = self._add_logged_accounts(logged_accounts)
                if added_ids:
                    self._select_preferred_account(logged_accounts)
                    self.update_account_combo(logged_accounts)
                    self.is_connected = True
                    self.connection_status_label.setText("🟢 已连接")
                    self.connect_btn.setText("🔌 断开连接")
                    self.status_bar.showMessage("交易服务连接成功")
                    self.refresh_account_info()
            else:
                account_added = self.easyxt.add_account(self.account_id, self.account_type)
                if account_added:
                    self.update_account_combo([])

                self.is_connected = True
                self.connection_status_label.setText("🟢 已连接")
                self.connect_btn.setText("🔌 断开连接")
                self.status_bar.showMessage("交易服务连接成功")

                self.refresh_account_info()

        except Exception as e:
            self.status_bar.showMessage(f"连接失败: {str(e)}")

    def disconnect_from_trading(self):
        """断开交易服务连接"""
        try:
            if EASYXT_AVAILABLE and self.easyxt:
                self.easyxt.trade.disconnect()

            self.is_connected = False
            self.connection_status_label.setText("🔴 未连接")
            self.status_bar.showMessage("已断开连接")

            # 清空数据
            self.clear_data()

        except Exception as e:
            QMessageBox.warning(self, "断开连接", f"断开连接时出错: {str(e)}")

    def _get_logged_accounts(self):
        accounts = []
        try:
            trader = getattr(self.easyxt.trade, "trader", None) if self.easyxt else None
            if trader and hasattr(trader, "query_account_infos"):
                infos = trader.query_account_infos()
                if infos:
                    for info in infos:
                        account_id = getattr(info, "account_id", None)
                        if account_id is None:
                            continue
                        accounts.append({
                            "account_id": str(account_id),
                            "account_type": getattr(info, "account_type", None)
                        })
        except Exception:
            return []
        return accounts

    def _add_logged_accounts(self, accounts):
        added_ids = []
        api = self.easyxt
        if api is None:
            return added_ids
        for account in accounts:
            account_id = account.get("account_id")
            account_type = account.get("account_type")
            if not account_id:
                continue
            if api.add_account(account_id, account_type):
                added_ids.append(str(account_id))
        return added_ids

    def _select_preferred_account(self, accounts, preferred_id="1678070127"):
        api = self.easyxt
        if api is None:
            return
        logged_ids = {str(a.get("account_id")) for a in accounts if a.get("account_id")}
        if preferred_id in logged_ids:
            self.account_id = preferred_id
            return
        candidates = []
        for account in accounts:
            account_id = account.get("account_id")
            account_type = account.get("account_type")
            if not account_id:
                continue
            trade_api = getattr(api, "trade", None)
            if trade_api is None:
                continue
            asset = trade_api.get_account_asset(account_id) or {}
            total_asset = asset.get('total_asset', 0) or 0
            market_value = asset.get('market_value', 0) or 0
            positions_df = trade_api.get_positions(account_id)
            pos_count = len(positions_df) if positions_df is not None else 0
            candidates.append((pos_count, market_value, total_asset, account_id, account_type))
        if candidates:
            best = max(candidates, key=lambda x: (x[0], x[1], x[2]))
            self.account_id = str(best[3])
            if best[4]:
                self.account_type = best[4]
            self.status_bar.showMessage(f"已自动选择账户: {self.account_id}")

    def update_account_combo(self, accounts):
        if not hasattr(self, "account_panel"):
            return
        self.account_panel.set_accounts(accounts, self.account_id)

    def on_account_changed(self, account_id: str):
        if not self.is_connected:
            return
        if not account_id:
            return
        self.account_id = str(account_id)
        self.refresh_account_info()

    def refresh_account_info(self):
        """刷新账户信息"""
        if not self.is_connected or not EASYXT_AVAILABLE or not self.easyxt:
            return
        if not self.account_id:
            return
        if self._refresh_in_progress:
            return

        class _AccountRefreshThread(QThread):
            ready = pyqtSignal(dict, list)
            error = pyqtSignal(str)

            def __init__(self, api, account_id):
                super().__init__()
                self._api = api
                self._account_id = account_id

            def run(self):
                try:
                    asset = self._api.trade.get_account_asset(self._account_id)
                    account_info = {
                        'total_asset': 0,
                        'available_cash': 0,
                        'market_value': 0,
                        'today_pnl': 0
                    }
                    if asset:
                        account_info = {
                            'total_asset': asset.get('total_asset', 0),
                            'available_cash': asset.get('cash', 0),
                            'market_value': asset.get('market_value', 0),
                            'today_pnl': asset.get('today_pnl', 0)
                        }

                    positions_df = self._api.trade.get_positions(self._account_id)
                    positions = []
                    if positions_df is not None and not positions_df.empty:
                        for _, row in positions_df.iterrows():
                            positions.append({
                                'stock_code': row.get('code', ''),
                                'volume': row.get('volume', 0),
                                'available_volume': row.get('can_use_volume', 0),
                                'cost_price': row.get('open_price', 0)
                            })
                    self.ready.emit(account_info, positions)
                except Exception as exc:
                    self.error.emit(str(exc))

        self._refresh_in_progress = True
        thread = _AccountRefreshThread(self.easyxt, self.account_id)
        thread.ready.connect(self._on_account_refresh_ready)
        thread.error.connect(self._on_account_refresh_error)
        self._refresh_thread = thread
        thread.start()

    def _on_account_refresh_ready(self, account_info: dict, positions: list):
        self.account_info = account_info
        self.positions = positions
        self.update_account_display(account_info)
        self.update_position_display(positions)
        self._finish_refresh_thread()

    def _on_account_refresh_error(self, message: str):
        print(f"刷新账户信息失败: {message}")
        self._finish_refresh_thread()

    def _finish_refresh_thread(self):
        self._refresh_in_progress = False
        if self._refresh_thread is not None:
            self._refresh_thread.quit()
            # Fix 58: 不在主线程 wait()，改用 deleteLater 延迟清理
            self._refresh_thread.deleteLater()
            self._refresh_thread = None

    def _refresh_account_info_sync(self):
        """兼容保留：必要时可用于同步刷新"""
        try:
            if self.is_connected and EASYXT_AVAILABLE and self.easyxt:
                asset = self.easyxt.trade.get_account_asset(self.account_id)
                if asset:
                    account_info = {
                        'total_asset': asset.get('total_asset', 0),
                        'available_cash': asset.get('cash', 0),
                        'market_value': asset.get('market_value', 0),
                        'today_pnl': asset.get('today_pnl', 0)
                    }
                else:
                    account_info = {
                        'total_asset': 0,
                        'available_cash': 0,
                        'market_value': 0,
                        'today_pnl': 0
                    }
                self.account_info = account_info
                self.update_account_display(account_info)

                positions_df = self.easyxt.trade.get_positions(self.account_id)
                positions = []
                if positions_df is not None and not positions_df.empty:
                    for _, row in positions_df.iterrows():
                        positions.append({
                            'stock_code': row.get('code', ''),
                            'volume': row.get('volume', 0),
                            'available_volume': row.get('can_use_volume', 0),
                            'cost_price': row.get('open_price', 0)
                        })
                self.positions = positions
                self.update_position_display(positions)

        except Exception as e:
            print(f"刷新账户信息失败: {e}")

    def load_demo_data(self):
        """加载演示数据"""
        # 模拟账户数据
        demo_account = {
            'total_asset': 100000.00,
            'available_cash': 50000.00,
            'market_value': 50000.00,
            'today_pnl': 1500.00
        }
        self.account_info = demo_account
        self.update_account_display(demo_account)

        # 模拟持仓数据
        demo_positions = [
            {'stock_code': '000001.SZ', 'volume': 1000, 'available_volume': 1000, 'cost_price': 12.50},
            {'stock_code': '600000.SH', 'volume': 500, 'available_volume': 500, 'cost_price': 8.80},
        ]
        self.positions = demo_positions
        self.update_position_display(demo_positions)

    def update_account_display(self, account_info):
        self.account_panel.update_account_info(account_info)
        self.account_updated.emit(account_info)
        signal_bus.emit(Events.ACCOUNT_UPDATED, account_info=account_info)

    def update_position_display(self, positions):
        self.position_panel.update_positions(positions)
        self.position_updated.emit(positions)
        signal_bus.emit(Events.POSITION_UPDATED, positions=positions)

    def clear_data(self):
        """清空数据显示"""
        self.account_panel.clear_data()
        self.position_panel.clear_data()
        self._reset_rejection_stats()

    def place_order_signal(self, stock_code: str, side: str, price: float, volume: int) -> bool:
        return self.submit_unified_order(stock_code, side, price, volume, source="signal")

    def on_order_requested(self, side: str, stock_code: str, price: float, volume: int):
        self.submit_unified_order(stock_code, side, price, volume, source="manual")

    def buy_stock(self, stock_code=None, price=None, volume=None):
        """买入股票"""
        stock_code = stock_code or self.order_panel.stock_combo.currentText()
        volume = volume if volume is not None else self.order_panel.volume_spin.value()
        price = price if price is not None else self.order_panel.price_spin.value()
        self.submit_unified_order(stock_code, "buy", price, volume, source="manual")

    def sell_stock(self, stock_code=None, price=None, volume=None):
        """卖出股票"""
        stock_code = stock_code or self.order_panel.stock_combo.currentText()
        volume = volume if volume is not None else self.order_panel.volume_spin.value()
        price = price if price is not None else self.order_panel.price_spin.value()
        self.submit_unified_order(stock_code, "sell", price, volume, source="manual")

    def submit_unified_order(self, stock_code: str, side: str, price: float, volume: int, source: str) -> bool:
        if not self.is_connected:
            if source == "manual":
                QMessageBox.warning(self, "未连接", "请先连接交易服务")
            elif hasattr(self, "status_bar") and self.status_bar:
                self.status_bar.showMessage("未连接交易服务", 5000)
            return False
        validation = self.validate_order(stock_code, side, price, volume)
        if not validation.is_valid:
            self._record_rejection(stock_code, validation)
            if source == "manual":
                QMessageBox.warning(self, "订单校验失败", validation.error_message or "校验失败")
            elif hasattr(self, "status_bar") and self.status_bar:
                self.status_bar.showMessage(f"订单校验失败: {validation.error_message}", 5000)
            return False
        try:
            if EASYXT_AVAILABLE and self.easyxt:
                if side == "buy":
                    result = self.easyxt.trade.buy(self.account_id, stock_code, volume, price, "limit")
                else:
                    result = self.easyxt.trade.sell(self.account_id, stock_code, volume, price, "limit")
                if result:
                    self.refresh_account_info()
                    if source == "manual":
                        QMessageBox.information(
                            self,
                            "交易成功",
                            f"{'买入' if side == 'buy' else '卖出'}订单已提交\\n{stock_code} {volume}股 @{price}",
                        )
                    elif hasattr(self, "status_bar") and self.status_bar:
                        self.status_bar.showMessage(f"自动交易 {side} {stock_code} {volume} @{price}", 5000)
                    signal_bus.emit(
                        Events.ORDER_SUBMITTED,
                        side=side,
                        symbol=stock_code,
                        price=price,
                        volume=volume,
                    )
                    self._track_order()
                    return True
                if source == "manual":
                    QMessageBox.warning(self, "交易失败", "订单提交失败")
                elif hasattr(self, "status_bar") and self.status_bar:
                    self.status_bar.showMessage("自动交易下单失败", 5000)
                return False
            if source == "manual":
                QMessageBox.information(
                    self,
                    "模拟交易",
                    f"模拟{'买入' if side == 'buy' else '卖出'}: {stock_code}\\n数量: {volume}股\\n价格: {price}",
                )
            elif hasattr(self, "status_bar") and self.status_bar:
                self.status_bar.showMessage(f"模拟交易 {side} {stock_code} {volume} @{price}", 5000)
            signal_bus.emit(Events.ORDER_SUBMITTED, side=side, symbol=stock_code, price=price, volume=volume)
            self._track_order()
            return True
        except Exception as e:
            if source == "manual":
                QMessageBox.critical(self, "交易错误", f"下单失败: {str(e)}")
            elif hasattr(self, "status_bar") and self.status_bar:
                self.status_bar.showMessage("自动交易异常", 5000)
            return False

    def submit_batch_orders(self, orders: list[dict], source: str = "batch"):
        results = []
        for order in orders:
            symbol = order.get("symbol") or order.get("stock_code") or ""
            side = order.get("side") or ""
            price = order.get("price", 0)
            volume = order.get("volume", 0)
            results.append(self.submit_unified_order(symbol, side, price, volume, source=source))
        return {
            "total": len(results),
            "success": sum(1 for result in results if result),
            "failed": sum(1 for result in results if not result),
            "results": results,
        }

    def update_time(self):
        """更新时间显示"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.setText(current_time)

    def _build_order_validator(self):
        return ValidatorChain([
            BasicOrderValidator(),
            BlacklistValidator(self.blacklist_patterns),
            DailyLossValidator(
                get_daily_loss=self.get_daily_loss,
                get_total_asset=self.get_total_asset,
                max_daily_loss_ratio=self.max_daily_loss_ratio,
            ),
            ConcentrationValidator(
                get_position_value=self.get_position_value,
                get_total_asset=self.get_total_asset,
                max_concentration_ratio=self.max_concentration_ratio,
            ),
            RiskOrderValidator(
                get_available_cash=self.get_available_cash,
                get_total_asset=self.get_total_asset,
                get_position_volume=self.get_position_volume,
                get_daily_order_count=self.get_daily_order_count,
                max_position_ratio=self.max_position_ratio,
                max_daily_orders=self.max_daily_orders,
            ),
        ])

    def _reset_daily_order_count_if_needed(self):
        today = datetime.now().date()
        if self.daily_order_date != today:
            self.daily_order_date = today
            self.daily_order_count = 0

    def _track_order(self):
        self._reset_daily_order_count_if_needed()
        self.daily_order_count += 1

    def _reset_rejection_stats(self):
        self.rejection_total = 0
        self.rejection_by_reason = {}
        self.rejection_by_symbol = {}
        self._update_rejection_display()

    def _record_rejection(self, symbol: str, validation):
        code = validation.error_code or "UNKNOWN"
        self.rejection_total += 1
        self.rejection_by_reason[code] = self.rejection_by_reason.get(code, 0) + 1
        self.rejection_by_symbol[symbol] = self.rejection_by_symbol.get(symbol, 0) + 1
        self._update_rejection_display()

    def _update_rejection_display(self):
        if not hasattr(self, "account_panel"):
            return
        self.account_panel.update_rejection_stats({
            "total": self.rejection_total,
            "reasons": self.rejection_by_reason,
            "symbols": self.rejection_by_symbol,
        })

    def get_available_cash(self) -> float:
        return float(self.account_info.get("available_cash", 0) or 0)

    def get_total_asset(self) -> float:
        return float(self.account_info.get("total_asset", 0) or 0)

    def get_position_volume(self, symbol: str) -> int:
        for pos in self.positions:
            if pos.get("stock_code") == symbol:
                return int(pos.get("available_volume", 0) or 0)
        return 0

    def get_position_value(self, symbol: str) -> float:
        for pos in self.positions:
            if pos.get("stock_code") == symbol:
                volume = float(pos.get("available_volume", 0) or 0)
                price = float(pos.get("cost_price", 0) or 0)
                return volume * price
        return 0.0

    def get_daily_loss(self) -> float:
        today_pnl = float(self.account_info.get("today_pnl", 0) or 0)
        return max(0.0, -today_pnl)

    def get_daily_order_count(self) -> int:
        self._reset_daily_order_count_if_needed()
        return self.daily_order_count

    def validate_order(self, symbol: str, side: str, price: float, volume: int):
        order = OrderData(symbol=symbol, side=side, price=float(price), volume=int(volume))
        return self.order_validator.validate(order)

    def update_data(self):
        """定时更新数据"""
        if self.is_connected:
            self.refresh_account_info()

    def closeEvent(self, event):
        """关闭事件"""
        # 停止定时器
        if hasattr(self, 'time_timer'):
            self.time_timer.stop()
        if hasattr(self, 'data_timer'):
            self.data_timer.stop()
        if self.is_connected:
            self.disconnect_from_trading()
        event.accept()


def main():
    """主函数"""
    app = QApplication(sys.argv)

    # 设置应用程序属性
    app.setApplicationName("量化交易系统")
    app.setApplicationVersion("1.0")

    # 设置字体
    font = QFont("微软雅黑", 9)
    app.setFont(font)

    # 创建主窗口
    window = TradingInterface()
    window.show()

    # 运行应用程序
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
