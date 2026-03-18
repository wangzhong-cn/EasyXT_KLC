#!/usr/bin/env python3
"""
DuckDB 数据管理界面
提供可视化的数据管理功能

参考文档：duckdb.docx
数据管理模块整体功能展示：
整个界面布局非常直观。
- 顶部是工具栏，集中了导入、定时补充和统计信息三大核心功能。
- 左侧是树形列表，按市场和股票分类展示资产。
- 右侧则是核心操作区，上半部分用于设置查询条件，下半部分展示查询结果。
- 底部状态栏则会实时反馈操作进度。
"""

import importlib
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from PyQt5.QtCore import (
    QAbstractTableModel,
    QByteArray,
    QDate,
    QSettings,
    QSortFilterProxyModel,
    Qt,
    QThread,
    QTimer,
    pyqtSignal,
)
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableView,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from core.events import Events
from core.signal_bus import signal_bus
from data_manager.duckdb_connection_pool import resolve_duckdb_path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'data_manager'))

duckdb = importlib.import_module("duckdb")
pd = importlib.import_module("pandas")
DataIntegrityChecker = getattr(importlib.import_module("data_integrity_checker"), "DataIntegrityChecker")
UniversalDataImporter = getattr(importlib.import_module("universal_data_importer"), "UniversalDataImporter")
if TYPE_CHECKING:
    import pandas as pd

# 导入连接管理器
try:
    from data_manager.duckdb_connection_pool import get_db_manager
    DB_MANAGER_AVAILABLE = True
except ImportError:
    DB_MANAGER_AVAILABLE = False


class DataQueryThread(QThread):
    """数据查询工作线程 - 使用只读连接"""

    data_ready = pyqtSignal(object)
    error_occurred = pyqtSignal(str)

    def __init__(self, duckdb_path, query):
        super().__init__()
        self.duckdb_path = duckdb_path
        self.query = query

    def run(self):
        try:
            manager = get_db_manager(self.duckdb_path)
            df = manager.execute_read_query(self.query)

            self.data_ready.emit(df)
        except Exception as e:
            self.error_occurred.emit(str(e))


class DataFrameTableModel(QAbstractTableModel):
    def __init__(self, df=None, columns=None, headers=None):
        super().__init__()
        self.df = df if df is not None else pd.DataFrame()
        self.columns = columns or list(self.df.columns)
        self.headers = headers or list(self.columns)

    def set_dataframe(self, df, columns, headers):
        self.beginResetModel()
        self.df = df if df is not None else pd.DataFrame()
        self.columns = list(columns)
        self.headers = list(headers)
        self.endResetModel()

    def rowCount(self, parent=None):
        return 0 if self.df is None else len(self.df)

    def columnCount(self, parent=None):
        return len(self.columns)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or self.df is None:
            return None
        row = index.row()
        col_name = self.columns[index.column()]
        try:
            value = self.df.iloc[row][col_name]
        except Exception:
            return None
        if role == Qt.UserRole:
            return None if pd.isna(value) else value
        if role != Qt.DisplayRole:
            return None
        if pd.isna(value):
            return ""
        if col_name in {"open", "high", "low", "close"}:
            try:
                return f"{float(value):.2f}"
            except Exception:
                return str(value)
        if col_name == "volume":
            try:
                return f"{int(value):,}"
            except Exception:
                return str(value)
        if col_name == "amount":
            try:
                return f"{float(value):,.0f}"
            except Exception:
                return str(value)
        return str(value)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            if 0 <= section < len(self.headers):
                return self.headers[section]
            return None
        return str(section + 1)


class DataUpdateThread(QThread):
    """数据更新工作线程"""

    progress_updated = pyqtSignal(int, str)
    update_completed = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(self, duckdb_path, stock_codes, start_date, end_date, period):
        super().__init__()
        self.duckdb_path = duckdb_path
        self.stock_codes = stock_codes
        self.start_date = start_date
        self.end_date = end_date
        self.period = period

    def run(self):
        try:
            total = len(self.stock_codes)
            if total == 0:
                self.update_completed.emit({
                    'total': 0,
                    'success': 0,
                    'failed': 0,
                    'skipped': 0,
                    'verify': []
                })
                return

            self.progress_updated.emit(0, "准备导入数据...")
            importer = UniversalDataImporter(duckdb_path=self.duckdb_path)
            importer.connect()
            try:
                import_result = importer.import_custom_stocks(
                    stocks=self.stock_codes,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    period=self.period
                )
            finally:
                importer.close()

            verify_results = []
            verify_skipped = False
            if self.period == '1d':
                self.progress_updated.emit(70, "导入完成，开始校验...")
                checker = DataIntegrityChecker(self.duckdb_path)
                checker.connect()
                try:
                    for i, stock_code in enumerate(self.stock_codes, 1):
                        progress = 70 + int(i / total * 30)
                        self.progress_updated.emit(progress, f"校验 {stock_code} ({i}/{total})")
                        report = checker.check_integrity(
                            stock_code,
                            self.start_date,
                            self.end_date,
                            detailed=False
                        )
                        verify_results.append(report)
                finally:
                    checker.close()
            else:
                verify_skipped = True

            self.update_completed.emit({
                'total': import_result.get('total', total),
                'success': import_result.get('success', 0),
                'failed': import_result.get('failed', 0),
                'skipped': import_result.get('skipped', 0),
                'verify': verify_results,
                'verify_skipped': verify_skipped,
                'period': self.period
            })
        except Exception as e:
            self.error_occurred.emit(str(e))


class DuckDBDataManagerWidget(QWidget):
    """
    DuckDB 数据管理界面

    功能：
    1. 数据查询（支持复权）
    2. 数据导入
    3. 数据完整性检查
    4. 统计信息
    5. 定时补充
    """

    def __init__(self):
        super().__init__()

        self.duckdb_path = resolve_duckdb_path()
        self.con = None
        self.current_query_df = None
        self.view_column_widths = {}
        self.view_sort_state = {}
        self.view_column_order = {}
        self.view_snapshots = {}
        self.view_named_snapshots = {}
        self.view_default_snapshots = {}
        self.current_view_mode = None

        self.load_view_column_widths()
        self.load_view_sort_state()
        self.load_view_column_order()
        self.load_view_snapshots()
        self.load_view_named_snapshots()
        self.load_view_default_snapshots()
        self.init_ui()
        self.current_view_mode = self.view_combo.currentText()
        self.ensure_default_view_snapshot(self.current_view_mode)
        if not self.apply_view_snapshot(self.current_view_mode):
            self.apply_view_column_widths(self.current_view_mode)
            self.apply_view_sort_state(self.current_view_mode)
            self.apply_view_column_order(self.current_view_mode)
        self.refresh_snapshot_combo()
        self.load_data_tree()
        self.load_statistics(show_dialog=False)

    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        # 标题
        title = QLabel("DuckDB 数据管理")
        title.setFont(QFont("Arial", 16, QFont.Bold))
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        # 工具栏
        toolbar_layout = QHBoxLayout()
        self.import_btn = QPushButton("📥 导入数据")
        self.check_btn = QPushButton("🔍 完整性检查")
        self.stats_btn = QPushButton("📊 统计信息")
        self.refresh_btn = QPushButton("🔄 刷新")

        for btn in [self.import_btn, self.check_btn, self.stats_btn, self.refresh_btn]:
            btn.setStyleSheet("""
                QPushButton {
                    padding: 8px 16px;
                    font-size: 12px;
                    border: 1px solid #ccc;
                    border-radius: 4px;
                    background-color: #f5f5f5;
                }
                QPushButton:hover {
                    background-color: #e0e0e0;
                }
            """)
            toolbar_layout.addWidget(btn)

        toolbar_layout.addStretch()
        layout.addLayout(toolbar_layout)

        # 主内容区（左右分割）
        splitter = QSplitter(Qt.Horizontal)

        # 左侧：树形列表
        left_panel = self.create_tree_panel()
        splitter.addWidget(left_panel)

        # 右侧：查询和结果
        right_panel = self.create_query_panel()
        splitter.addWidget(right_panel)

        # 设置分割比例（左30%，右70%）
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 7)

        layout.addWidget(splitter)

        # 底部：状态栏
        self.status_label = QLabel("就绪")
        self.status_label.setStyleSheet("padding: 5px; background-color: #f0f0f0; border-radius: 3px;")
        layout.addWidget(self.status_label)

        # 连接信号
        self.import_btn.clicked.connect(self.import_data)
        self.check_btn.clicked.connect(self.check_integrity)
        self.stats_btn.clicked.connect(lambda: self.load_statistics(show_dialog=True))
        self.refresh_btn.clicked.connect(self.refresh_all)
        self.query_btn.clicked.connect(self.execute_query)
        self.view_combo.currentTextChanged.connect(self.on_view_mode_changed)

        # 连接树形列表的信号
        self.data_tree.itemClicked.connect(self.on_tree_item_clicked)

    def create_tree_panel(self) -> QWidget:
        """创建左侧树形面板"""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 5, 0)

        # 搜索框
        search_layout = QHBoxLayout()
        search_label = QLabel("搜索:")
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("输入代码或名称...")
        search_layout.addWidget(search_label)
        search_layout.addWidget(self.search_edit)
        layout.addLayout(search_layout)

        # 树形列表
        self.data_tree = QTreeWidget()
        self.data_tree.setHeaderLabels(["名称", "数据量"])
        self.data_tree.setColumnWidth(0, 200)
        self.data_tree.setColumnWidth(1, 80)
        layout.addWidget(self.data_tree)

        return panel

    def create_query_panel(self) -> QWidget:
        """创建右侧查询面板"""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(5, 0, 0, 0)

        # 查询条件区
        query_group = QGroupBox("查询条件")
        query_layout = QGridLayout()

        # 股票代码
        query_layout.addWidget(QLabel("股票代码:"), 0, 0)
        self.stock_code_edit = QLineEdit()
        query_layout.addWidget(self.stock_code_edit, 0, 1)

        # 复权类型
        query_layout.addWidget(QLabel("复权类型:"), 0, 2)
        self.adjust_combo = QComboBox()
        self.adjust_combo.addItems([
            "不复权",
            "前复权",
            "后复权",
            "等比前复权",
            "等比后复权"
        ])
        query_layout.addWidget(self.adjust_combo, 0, 3)

        # 日期范围
        query_layout.addWidget(QLabel("开始日期:"), 1, 0)
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDate(QDate.currentDate().addMonths(-3))
        query_layout.addWidget(self.start_date_edit, 1, 1)

        query_layout.addWidget(QLabel("结束日期:"), 1, 2)
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDate(QDate.currentDate())
        query_layout.addWidget(self.end_date_edit, 1, 3)

        query_layout.addWidget(QLabel("导入周期:"), 2, 0)
        self.period_combo = QComboBox()
        self.period_combo.addItems([
            "日线(1d)",
            "分钟(1m)",
            "5分钟(5m)",
            "Tick(tick)"
        ])
        query_layout.addWidget(self.period_combo, 2, 1)

        query_layout.addWidget(QLabel("查询周期:"), 2, 2)
        self.query_period_combo = QComboBox()
        self.query_period_combo.addItems([
            "日线(1d)",
            "分钟(1m)",
            "5分钟(5m)",
            "Tick(tick)"
        ])
        query_layout.addWidget(self.query_period_combo, 2, 3)

        query_layout.addWidget(QLabel("视图模式:"), 3, 0)
        self.view_combo = QComboBox()
        self.view_combo.addItems([
            "时间排序视图 (时间+标的)",
            "标的排序视图 (标的+时间)"
        ])
        query_layout.addWidget(self.view_combo, 3, 1)

        query_layout.addWidget(QLabel("视图快照:"), 4, 0)
        self.snapshot_combo = QComboBox()
        query_layout.addWidget(self.snapshot_combo, 4, 1)
        self.save_snapshot_btn = QPushButton("保存快照")
        query_layout.addWidget(self.save_snapshot_btn, 4, 2)
        self.restore_default_btn = QPushButton("恢复默认")
        query_layout.addWidget(self.restore_default_btn, 4, 3)
        self.rename_snapshot_btn = QPushButton("重命名")
        query_layout.addWidget(self.rename_snapshot_btn, 5, 2)
        self.delete_snapshot_btn = QPushButton("删除快照")
        query_layout.addWidget(self.delete_snapshot_btn, 5, 3)
        self.export_snapshot_btn = QPushButton("导出快照")
        query_layout.addWidget(self.export_snapshot_btn, 6, 2)
        self.import_snapshot_btn = QPushButton("导入快照")
        query_layout.addWidget(self.import_snapshot_btn, 6, 3)

        query_group.setLayout(query_layout)
        layout.addWidget(query_group)

        # 查询按钮
        button_layout = QHBoxLayout()
        self.query_btn = QPushButton("🔎 查询")
        self.query_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        button_layout.addWidget(self.query_btn)
        button_layout.addStretch()
        layout.addLayout(button_layout)

        # 查询结果区
        result_group = QGroupBox("查询结果")
        result_layout = QVBoxLayout()

        self.result_model = DataFrameTableModel(pd.DataFrame(), [], [])
        self.result_proxy = QSortFilterProxyModel()
        self.result_proxy.setSourceModel(self.result_model)
        self.result_proxy.setSortRole(Qt.UserRole)
        self.result_table = QTableView()
        self.result_table.setModel(self.result_proxy)
        self.result_table.setSortingEnabled(True)
        self.result_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.result_table.setSelectionMode(QAbstractItemView.SingleSelection)
        header = self.result_table.horizontalHeader()
        if header:
            header.setSectionResizeMode(QHeaderView.Interactive)
            header.setStretchLastSection(True)
            header.setSectionsMovable(True)
            header.sectionResized.connect(self.on_column_resized)
            header.sortIndicatorChanged.connect(self.on_sort_indicator_changed)
            header.sectionMoved.connect(self.on_column_moved)
        result_layout.addWidget(self.result_table)

        self.snapshot_combo.currentTextChanged.connect(self.on_snapshot_selected)
        self.save_snapshot_btn.clicked.connect(self.on_save_snapshot)
        self.restore_default_btn.clicked.connect(self.on_restore_default_snapshot)
        self.rename_snapshot_btn.clicked.connect(self.on_rename_snapshot)
        self.delete_snapshot_btn.clicked.connect(self.on_delete_snapshot)
        self.export_snapshot_btn.clicked.connect(self.on_export_snapshot)
        self.import_snapshot_btn.clicked.connect(self.on_import_snapshot)

        result_group.setLayout(result_layout)
        layout.addWidget(result_group)

        return panel

    def load_data_tree(self):
        """加载数据树形列表"""
        try:
            manager = get_db_manager(self.duckdb_path)
            df = manager.execute_read_query("""
                    SELECT
                        CASE
                            WHEN stock_code LIKE '%.SH' THEN '上海'
                            WHEN stock_code LIKE '%.SZ' THEN '深圳'
                            WHEN stock_code LIKE '%.BJ' THEN '北交所'
                            ELSE '其他'
                        END as market,
                        stock_code,
                        COUNT(*) as count
                    FROM stock_daily
                    GROUP BY market, stock_code
                    ORDER BY market, stock_code
                """)

            # 构建树
            self.data_tree.clear()

            markets = {}
            for _, row in df.iterrows():
                market = row.get('market')
                stock_code = row.get('stock_code')
                count = row.get('count')

                if not market or pd.isna(market):
                    market = "其他"
                if not stock_code or pd.isna(stock_code):
                    continue
                if count is None or pd.isna(count):
                    count = 0

                if market not in markets:
                    markets[market] = []

                markets[market].append((str(stock_code), int(count)))

            # 添加到树
            for market_name, stocks in sorted(markets.items(), key=lambda item: str(item[0])):
                market_item = QTreeWidgetItem([market_name, ""])
                market_item.setExpanded(True)

                for stock_code, count in sorted(stocks, key=lambda item: item[0]):
                    stock_item = QTreeWidgetItem([stock_code, str(count)])
                    market_item.addChild(stock_item)

                self.data_tree.addTopLevelItem(market_item)

        except Exception as e:
            self.status_label.setText(f"数据树加载失败: {e}")

    def on_tree_item_clicked(self, item: QTreeWidgetItem, column: int):
        """树形列表项点击事件"""
        text = item.text(0)

        # 如果是股票代码（包含点）
        if '.' in text:
            self.stock_code_edit.setText(text)
            signal_bus.emit(Events.SYMBOL_SELECTED, symbol=text)

    def execute_query(self):
        """执行查询"""
        stock_text = self.stock_code_edit.text().strip()
        if not stock_text:
            QMessageBox.warning(self, "提示", "请输入股票代码")
            return
        stock_codes = [s for s in stock_text.replace("，", ",").replace(" ", ",").split(",") if s]
        if not stock_codes:
            QMessageBox.warning(self, "提示", "请输入股票代码")
            return

        start_date = self.start_date_edit.date().toString('yyyy-MM-dd')
        end_date = self.end_date_edit.date().toString('yyyy-MM-dd')

        # 获取复权类型
        adjust_index = self.adjust_combo.currentIndex()
        adjust_types = ['', 'front', 'back', 'geometric_front', 'geometric_back']
        adjust_type = adjust_types[adjust_index]

        period_map = {
            "日线(1d)": ("stock_daily", "date", "日期"),
            "分钟(1m)": ("stock_1m", "datetime", "时间"),
            "5分钟(5m)": ("stock_5m", "datetime", "时间"),
            "Tick(tick)": ("stock_tick", "datetime", "时间")
        }
        table_name, time_field, time_label = period_map.get(
            self.query_period_combo.currentText(),
            ("stock_daily", "date", "日期")
        )

        # 构建查询
        if adjust_type == '' or adjust_type == 'none':
            price_cols = ['open', 'high', 'low', 'close']
        else:
            col_mapping = {
                'front': ['open_front', 'high_front', 'low_front', 'close_front'],
                'back': ['open_back', 'high_back', 'low_back', 'close_back'],
                'geometric_front': ['open_geometric_front', 'high_geometric_front',
                                   'low_geometric_front', 'close_geometric_front'],
                'geometric_back': ['open_geometric_back', 'high_geometric_back',
                                  'low_geometric_back', 'close_geometric_back'],
            }
            price_cols = col_mapping.get(adjust_type, ['open', 'high', 'low', 'close'])

        if time_field == "date":
            time_expr = "date::DATE"
        else:
            time_expr = "datetime::TIMESTAMP"

        is_batch = len(stock_codes) > 1
        if is_batch:
            parts = []
            for code in stock_codes:
                safe_code = code.replace("'", "''")
                parts.append(
                    f"""
                    SELECT
                        '{safe_code}' as stock,
                        {time_expr} as time,
                        {price_cols[0]}::DOUBLE as open,
                        {price_cols[1]}::DOUBLE as high,
                        {price_cols[2]}::DOUBLE as low,
                        {price_cols[3]}::DOUBLE as close,
                        volume::BIGINT as volume,
                        amount::DOUBLE as amount
                    FROM {table_name}
                    WHERE stock_code = '{safe_code}'
                      AND {time_field} >= '{start_date}'
                      AND {time_field} <= '{end_date}'
                    """
                )
            query = "\nUNION ALL\n".join(parts) + f"\nORDER BY {time_field}, stock"
        else:
            safe_code = stock_codes[0].replace("'", "''")
            query = f"""
                SELECT
                    {time_expr} as time,
                    {price_cols[0]}::DOUBLE as open,
                    {price_cols[1]}::DOUBLE as high,
                    {price_cols[2]}::DOUBLE as low,
                    {price_cols[3]}::DOUBLE as close,
                    volume::BIGINT as volume,
                    amount::DOUBLE as amount
                FROM {table_name}
                WHERE stock_code = '{safe_code}'
                  AND {time_field} >= '{start_date}'
                  AND {time_field} <= '{end_date}'
                ORDER BY {time_field}
            """

        # 显示等待状态
        self.status_label.setText("正在查询数据...")
        if is_batch:
            columns = ["stock", "time", "open", "high", "low", "close", "volume", "amount"]
            headers = ["标的", time_label, "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        else:
            columns = ["time", "open", "high", "low", "close", "volume", "amount"]
            headers = [time_label, "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        self.result_model.set_dataframe(pd.DataFrame(), columns, headers)

        # 在线程中执行查询
        self.query_thread = DataQueryThread(self.duckdb_path, query)
        self.query_thread.data_ready.connect(self.on_query_result)
        self.query_thread.error_occurred.connect(self.on_query_error)
        self.query_thread.start()

    def on_query_result(self, df: Any):
        """查询结果回调"""
        self.current_query_df = df.copy()
        self.display_query_result(df)

    def _result_column_count(self) -> int:
        model = self.result_table.model()
        if model is None:
            return 0
        return int(model.columnCount())

    def on_view_mode_changed(self, _: str):
        if self.current_view_mode is not None:
            column_count = self._result_column_count()
            self.view_column_widths[self.current_view_mode] = [
                self.result_table.columnWidth(i) for i in range(column_count)
            ]
            header = self.result_table.horizontalHeader()
            self.view_sort_state[self.current_view_mode] = {
                "section": header.sortIndicatorSection(),
                "order": int(header.sortIndicatorOrder()),
                "enabled": self.result_table.isSortingEnabled(),
            }
            self.view_column_order[self.current_view_mode] = header.saveState().toHex().data().decode()
            self.update_view_snapshot(self.current_view_mode)
            self.save_view_column_widths()
            self.save_view_sort_state()
            self.save_view_column_order()
            self.save_view_snapshots()
        self.current_view_mode = self.view_combo.currentText()
        self.ensure_default_view_snapshot(self.current_view_mode)
        if not self.apply_view_snapshot(self.current_view_mode):
            self.apply_view_column_widths(self.current_view_mode)
            self.apply_view_sort_state(self.current_view_mode)
            self.apply_view_column_order(self.current_view_mode)
        self.refresh_snapshot_combo()
        if self.current_query_df is None:
            return
        self.display_query_result(self.current_query_df)

    def display_query_result(self, df: Any):
        vertical_scroll = self.result_table.verticalScrollBar().value()
        horizontal_scroll = self.result_table.horizontalScrollBar().value()
        header = self.result_table.horizontalHeader()
        column_count = self._result_column_count()
        column_widths = [self.result_table.columnWidth(i) for i in range(column_count)]
        sorting_enabled = self.result_table.isSortingEnabled()
        sort_section = header.sortIndicatorSection()
        sort_order = header.sortIndicatorOrder()
        current_key = None
        current_index = self.result_table.currentIndex()
        if current_index.isValid():
            source_index = self.result_proxy.mapToSource(current_index)
            if source_index.isValid() and self.result_model.df is not None:
                row = source_index.row()
                is_batch_view = "stock" in self.result_model.df.columns
                try:
                    if is_batch_view:
                        current_key = (
                            self.result_model.df.iloc[row]["stock"],
                            self.result_model.df.iloc[row]["time"],
                        )
                    else:
                        current_key = ("", self.result_model.df.iloc[row]["time"])
                except Exception:
                    current_key = None
        view_mode = self.view_combo.currentText()
        is_batch = "stock" in df.columns
        if is_batch:
            if view_mode == "标的排序视图 (标的+时间)":
                df = df.sort_values(["stock", "time"])
            else:
                df = df.sort_values(["time", "stock"])
        else:
            if "time" in df.columns:
                df = df.sort_values(["time"])

        if is_batch:
            columns = ["stock", "time", "open", "high", "low", "close", "volume", "amount"]
        else:
            columns = ["time", "open", "high", "low", "close", "volume", "amount"]
        headers = self.result_model.headers if self.result_model.headers else columns
        self.result_model.set_dataframe(df, columns, headers)
        match_row = None
        if current_key is not None and not df.empty:
            try:
                if is_batch:
                    mask = (df["stock"] == current_key[0]) & (df["time"] == current_key[1])
                else:
                    mask = df["time"] == current_key[1]
                matches = df.index[mask]
                if len(matches) > 0:
                    match_row = int(matches[0])
            except Exception:
                match_row = None

        target_widths = self.view_column_widths.get(view_mode, column_widths)
        current_count = self._result_column_count()
        for i, width in enumerate(target_widths):
            if i < current_count:
                self.result_table.setColumnWidth(i, width)
        self.result_table.setSortingEnabled(sorting_enabled)
        if 0 <= sort_section < current_count:
            header.setSortIndicator(sort_section, sort_order)
        if match_row is not None:
            source_index = self.result_model.index(match_row, 0)
            proxy_index = self.result_proxy.mapFromSource(source_index)
            if proxy_index.isValid():
                self.result_table.setCurrentIndex(proxy_index)
                self.result_table.scrollTo(proxy_index)
        self.result_table.verticalScrollBar().setValue(vertical_scroll)
        self.result_table.horizontalScrollBar().setValue(horizontal_scroll)
        self.status_label.setText(f"查询完成，共 {len(df)} 条记录")

    def load_view_column_widths(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        raw = settings.value("view_column_widths", "")
        if not raw:
            return
        try:
            data = json.loads(raw)
        except Exception:
            return
        if isinstance(data, dict):
            self.view_column_widths = {
                key: [int(value) for value in values]
                for key, values in data.items()
                if isinstance(values, list)
            }

    def save_view_column_widths(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        settings.setValue("view_column_widths", json.dumps(self.view_column_widths, ensure_ascii=False))

    def apply_view_column_widths(self, view_mode: str):
        widths = self.view_column_widths.get(view_mode)
        if not widths:
            return
        column_count = self._result_column_count()
        for i, width in enumerate(widths):
            if i < column_count:
                self.result_table.setColumnWidth(i, width)

    def on_column_resized(self, *_):
        if self.current_view_mode is None:
            return
        column_count = self._result_column_count()
        self.view_column_widths[self.current_view_mode] = [
            self.result_table.columnWidth(i) for i in range(column_count)
        ]
        self.update_view_snapshot(self.current_view_mode)
        self.save_view_column_widths()
        self.save_view_snapshots()

    def load_view_column_order(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        raw = settings.value("view_column_order", "")
        if not raw:
            return
        try:
            data = json.loads(raw)
        except Exception:
            return
        if isinstance(data, dict):
            self.view_column_order = {
                key: value for key, value in data.items() if isinstance(value, str)
            }

    def save_view_column_order(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        settings.setValue("view_column_order", json.dumps(self.view_column_order, ensure_ascii=False))

    def apply_view_column_order(self, view_mode: str):
        state = self.view_column_order.get(view_mode)
        if not state:
            return
        header = self.result_table.horizontalHeader()
        if header is None:
            return
        data = QByteArray.fromHex(state.encode())
        if not data.isEmpty():
            header.restoreState(data)

    def on_column_moved(self, *_):
        if self.current_view_mode is None:
            return
        header = self.result_table.horizontalHeader()
        self.view_column_order[self.current_view_mode] = header.saveState().toHex().data().decode()
        column_count = self._result_column_count()
        self.view_column_widths[self.current_view_mode] = [
            self.result_table.columnWidth(i) for i in range(column_count)
        ]
        self.update_view_snapshot(self.current_view_mode)
        self.save_view_column_widths()
        self.save_view_column_order()
        self.save_view_snapshots()

    def on_snapshot_selected(self, name: str):
        if self.current_view_mode is None or not name or name == "（未选择）":
            return
        snapshot = self.view_named_snapshots.get(self.current_view_mode, {}).get(name)
        if not snapshot:
            return
        self.apply_view_snapshot_data(snapshot)
        self.view_snapshots[self.current_view_mode] = snapshot
        self.save_view_snapshots()

    def on_save_snapshot(self):
        if self.current_view_mode is None:
            return
        name, ok = QInputDialog.getText(self, "保存快照", "快照名称:")
        if not ok:
            return
        name = name.strip()
        if not name:
            return
        snapshot = self.get_current_snapshot()
        if self.current_view_mode not in self.view_named_snapshots:
            self.view_named_snapshots[self.current_view_mode] = {}
        self.view_named_snapshots[self.current_view_mode][name] = snapshot
        self.save_view_named_snapshots()
        self.refresh_snapshot_combo()
        self.snapshot_combo.setCurrentText(name)

    def on_restore_default_snapshot(self):
        if self.current_view_mode is None:
            return
        self.ensure_default_view_snapshot(self.current_view_mode)
        snapshot = self.view_default_snapshots.get(self.current_view_mode)
        if not snapshot:
            return
        self.apply_view_snapshot_data(snapshot)
        self.view_snapshots[self.current_view_mode] = snapshot
        self.save_view_snapshots()
        self.snapshot_combo.setCurrentText("（未选择）")

    def on_rename_snapshot(self):
        if self.current_view_mode is None:
            return
        current_name = self.snapshot_combo.currentText()
        if not current_name or current_name == "（未选择）":
            return
        name, ok = QInputDialog.getText(self, "重命名快照", "新的快照名称:", text=current_name)
        if not ok:
            return
        name = name.strip()
        if not name or name == current_name:
            return
        snapshot_map = self.view_named_snapshots.get(self.current_view_mode, {})
        if current_name not in snapshot_map:
            return
        snapshot_map[name] = snapshot_map.pop(current_name)
        self.view_named_snapshots[self.current_view_mode] = snapshot_map
        self.save_view_named_snapshots()
        self.refresh_snapshot_combo()
        self.snapshot_combo.setCurrentText(name)

    def on_delete_snapshot(self):
        if self.current_view_mode is None:
            return
        current_name = self.snapshot_combo.currentText()
        if not current_name or current_name == "（未选择）":
            return
        result = QMessageBox.question(self, "删除快照", f"确定删除快照「{current_name}」吗？")
        if result != QMessageBox.Yes:
            return
        snapshot_map = self.view_named_snapshots.get(self.current_view_mode, {})
        if current_name in snapshot_map:
            snapshot_map.pop(current_name, None)
            self.view_named_snapshots[self.current_view_mode] = snapshot_map
            self.save_view_named_snapshots()
            self.refresh_snapshot_combo()
            self.snapshot_combo.setCurrentText("（未选择）")

    def on_export_snapshot(self):
        if self.current_view_mode is None:
            return
        snapshot_name = self.snapshot_combo.currentText()
        if not snapshot_name or snapshot_name == "（未选择）":
            QMessageBox.warning(self, "导出快照", "请选择要导出的快照")
            return
        filename, _ = QFileDialog.getSaveFileName(
            self,
            "导出快照",
            f"snapshot_{snapshot_name}.json",
            "JSON文件 (*.json)",
        )
        if not filename:
            return
        try:
            export_file = self.export_snapshot_to_json(snapshot_name, filename)
            QMessageBox.information(self, "导出成功", f"快照已导出到:\n{export_file}")
        except Exception as exc:
            QMessageBox.critical(self, "导出失败", f"导出失败: {exc}")

    def export_snapshot_to_json(self, snapshot_name: str, filepath: str) -> str:
        snapshot_config = self.get_snapshot_config(snapshot_name)
        export_data = {
            "metadata": {
                "snapshot_name": snapshot_name,
                "export_time": datetime.now().isoformat(),
                "version": "1.0",
                "software": "EasyXT Data Manager",
            },
            "configuration": snapshot_config,
            "query_conditions": {
                "stock_codes": self.stock_code_edit.text(),
                "start_date": self.start_date_edit.date().toString("yyyy-MM-dd"),
                "end_date": self.end_date_edit.date().toString("yyyy-MM-dd"),
                "query_period": self.query_period_combo.currentText(),
                "view_mode": self.view_combo.currentText(),
            },
        }
        with open(filepath, "w", encoding="utf-8") as file:
            json.dump(export_data, file, ensure_ascii=False, indent=2)
        return filepath

    def on_import_snapshot(self):
        if self.current_view_mode is None:
            return
        filename, _ = QFileDialog.getOpenFileName(
            self,
            "导入快照",
            "",
            "JSON文件 (*.json)",
        )
        if not filename:
            return
        try:
            snapshot_name = self.import_snapshot_from_json(filename)
            if snapshot_name:
                QMessageBox.information(self, "导入成功", "快照导入成功")
                self.refresh_snapshot_combo()
                self.snapshot_combo.setCurrentText(snapshot_name)
            else:
                QMessageBox.warning(self, "导入失败", "快照文件格式不正确")
        except Exception as exc:
            QMessageBox.critical(self, "导入失败", f"导入失败: {exc}")

    def import_snapshot_from_json(self, filepath: str) -> str:
        with open(filepath, encoding="utf-8") as file:
            import_data = json.load(file)
        if not self.validate_import_data(import_data):
            return ""
        snapshot_name = import_data["metadata"]["snapshot_name"]
        config = import_data["configuration"]
        self.save_snapshot_config(snapshot_name, config)
        return snapshot_name

    def validate_import_data(self, import_data: dict) -> bool:
        if not isinstance(import_data, dict):
            return False
        metadata = import_data.get("metadata")
        configuration = import_data.get("configuration")
        if not isinstance(metadata, dict) or not isinstance(configuration, dict):
            return False
        if "snapshot_name" not in metadata:
            return False
        if not self.is_valid_snapshot_config(configuration):
            return False
        return True

    def is_valid_snapshot_config(self, config: dict) -> bool:
        if "widths" not in config or "order_state" not in config or "sort" not in config:
            return False
        if not isinstance(config.get("widths"), list):
            return False
        if not isinstance(config.get("order_state"), str):
            return False
        if not isinstance(config.get("sort"), dict):
            return False
        return True

    def get_snapshot_config(self, snapshot_name: str) -> dict:
        if self.current_view_mode is None:
            return {}
        snapshot_map = self.view_named_snapshots.get(self.current_view_mode, {})
        return snapshot_map.get(snapshot_name, {})

    def save_snapshot_config(self, snapshot_name: str, config: dict):
        if self.current_view_mode is None:
            return
        if self.current_view_mode not in self.view_named_snapshots:
            self.view_named_snapshots[self.current_view_mode] = {}
        self.view_named_snapshots[self.current_view_mode][snapshot_name] = config
        self.save_view_named_snapshots()

    def load_view_snapshots(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        raw = settings.value("view_snapshots", "")
        if not raw:
            return
        try:
            data = json.loads(raw)
        except Exception:
            return
        if isinstance(data, dict):
            self.view_snapshots = {
                key: value for key, value in data.items() if isinstance(value, dict)
            }

    def save_view_snapshots(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        settings.setValue("view_snapshots", json.dumps(self.view_snapshots, ensure_ascii=False))

    def load_view_named_snapshots(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        raw = settings.value("view_named_snapshots", "")
        if not raw:
            return
        try:
            data = json.loads(raw)
        except Exception:
            return
        if isinstance(data, dict):
            self.view_named_snapshots = {
                key: value
                for key, value in data.items()
                if isinstance(value, dict)
            }

    def save_view_named_snapshots(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        settings.setValue("view_named_snapshots", json.dumps(self.view_named_snapshots, ensure_ascii=False))

    def load_view_default_snapshots(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        raw = settings.value("view_default_snapshots", "")
        if not raw:
            return
        try:
            data = json.loads(raw)
        except Exception:
            return
        if isinstance(data, dict):
            self.view_default_snapshots = {
                key: value
                for key, value in data.items()
                if isinstance(value, dict)
            }

    def save_view_default_snapshots(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        settings.setValue("view_default_snapshots", json.dumps(self.view_default_snapshots, ensure_ascii=False))

    def ensure_default_view_snapshot(self, view_mode: str):
        if view_mode in self.view_default_snapshots:
            return
        snapshot = self.get_current_snapshot()
        for i in range(self.view_combo.count()):
            mode = self.view_combo.itemText(i)
            if mode not in self.view_default_snapshots:
                self.view_default_snapshots[mode] = snapshot
        self.save_view_default_snapshots()

    def refresh_snapshot_combo(self):
        if self.current_view_mode is None:
            return
        self.snapshot_combo.blockSignals(True)
        self.snapshot_combo.clear()
        self.snapshot_combo.addItem("（未选择）")
        names = sorted(self.view_named_snapshots.get(self.current_view_mode, {}).keys())
        for name in names:
            self.snapshot_combo.addItem(name)
        self.snapshot_combo.blockSignals(False)

    def get_current_snapshot(self) -> dict:
        header = self.result_table.horizontalHeader()
        column_count = self._result_column_count()
        return {
            "widths": [self.result_table.columnWidth(i) for i in range(column_count)],
            "order_state": header.saveState().toHex().data().decode(),
            "sort": {
                "section": header.sortIndicatorSection(),
                "order": int(header.sortIndicatorOrder()),
                "enabled": self.result_table.isSortingEnabled(),
            },
        }

    def update_view_snapshot(self, view_mode: str):
        self.view_snapshots[view_mode] = self.get_current_snapshot()

    def apply_view_snapshot(self, view_mode: str) -> bool:
        snapshot = self.view_snapshots.get(view_mode)
        if not snapshot:
            return False
        return self.apply_view_snapshot_data(snapshot)

    def apply_view_snapshot_data(self, snapshot: dict) -> bool:
        header = self.result_table.horizontalHeader()
        order_state = snapshot.get("order_state")
        if isinstance(order_state, str) and order_state:
            data = QByteArray.fromHex(order_state.encode())
            if not data.isEmpty():
                header.restoreState(data)
        widths = snapshot.get("widths")
        if isinstance(widths, list):
            column_count = self._result_column_count()
            for i, width in enumerate(widths):
                if i < column_count:
                    self.result_table.setColumnWidth(i, int(width))
        sort_state = snapshot.get("sort")
        if isinstance(sort_state, dict):
            enabled = sort_state.get("enabled")
            section = sort_state.get("section")
            order = sort_state.get("order")
            if isinstance(enabled, bool):
                self.result_table.setSortingEnabled(enabled)
            if isinstance(section, int) and isinstance(order, int):
                if 0 <= section < self._result_column_count():
                    header.setSortIndicator(section, Qt.SortOrder(order))
                    self.result_proxy.sort(section, Qt.SortOrder(order))
        return True

    def load_view_sort_state(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        raw = settings.value("view_sort_state", "")
        if not raw:
            return
        try:
            data = json.loads(raw)
        except Exception:
            return
        if isinstance(data, dict):
            self.view_sort_state = {
                key: value
                for key, value in data.items()
                if isinstance(value, dict)
            }

    def save_view_sort_state(self):
        settings = QSettings("EasyXT", "DuckDBDataManagerWidget")
        settings.setValue("view_sort_state", json.dumps(self.view_sort_state, ensure_ascii=False))

    def apply_view_sort_state(self, view_mode: str):
        state = self.view_sort_state.get(view_mode)
        if not state:
            return
        section = state.get("section")
        order = state.get("order")
        enabled = state.get("enabled")
        if isinstance(enabled, bool):
            self.result_table.setSortingEnabled(enabled)
        if isinstance(section, int) and isinstance(order, int):
            header = self.result_table.horizontalHeader()
            if 0 <= section < self._result_column_count():
                header.setSortIndicator(section, Qt.SortOrder(order))
                self.result_proxy.sort(section, Qt.SortOrder(order))

    def on_sort_indicator_changed(self, section: int, order: int):
        if self.current_view_mode is None:
            return
        self.result_proxy.sort(section, Qt.SortOrder(order))
        self.view_sort_state[self.current_view_mode] = {
            "section": section,
            "order": int(order),
            "enabled": self.result_table.isSortingEnabled(),
        }
        self.update_view_snapshot(self.current_view_mode)
        self.save_view_sort_state()
        self.save_view_snapshots()

    def on_query_error(self, error_msg: str):
        """查询错误回调"""
        QMessageBox.critical(self, "查询错误", error_msg)
        self.status_label.setText("查询失败")

    def load_statistics(self, show_dialog: bool = False):
        """加载统计信息"""
        try:
            manager = get_db_manager(self.duckdb_path)
            stats = manager.execute_read_query("""
                SELECT
                    COUNT(DISTINCT stock_code) as stock_count,
                    COUNT(*) as total_records,
                    MIN(date) as first_date,
                    MAX(date) as last_date
                FROM stock_daily
            """)

            if not stats.empty:
                row = stats.iloc[0]
                msg = (
                    f"标的数量: {row['stock_count']:,} | "
                    f"总记录数: {row['total_records']:,} | "
                    f"日期范围: {row['first_date']} ~ {row['last_date']}"
                )
                self.status_label.setText(msg)
                if show_dialog:
                    QMessageBox.information(self, "统计信息", msg)

        except Exception as e:
            self.status_label.setText(f"统计信息加载失败: {e}")

    def check_integrity(self):
        """检查数据完整性"""
        stock_code = self.stock_code_edit.text().strip()

        if not stock_code:
            QMessageBox.warning(self, "提示", "请先选择股票")
            return

        start_date = self.start_date_edit.date().toString('yyyy-MM-dd')
        end_date = self.end_date_edit.date().toString('yyyy-MM-dd')

        # 创建检查器
        checker = DataIntegrityChecker(self.duckdb_path)
        checker.connect()

        # 执行检查
        report = checker.check_integrity(stock_code, start_date, end_date, detailed=True)

        # 显示结果
        msg = f"""数据完整性检查结果

标的: {stock_code}
检查范围: {start_date} ~ {end_date}

缺失交易日: {report['missing_trading_days']}
数据完整度: {report['completeness_ratio']*100:.2f}%
状态: {report['status']}

错误数: {report['quality_report']['errors']}
警告数: {report['quality_report']['warnings']}
"""

        QMessageBox.information(self, "完整性检查", msg)
        checker.close()

    def import_data(self):
        """导入数据"""
        stock_text = self.stock_code_edit.text().strip()
        if not stock_text:
            QMessageBox.warning(self, "提示", "请输入股票代码（支持逗号分隔）")
            return

        stock_codes = [s for s in stock_text.replace("，", ",").replace(" ", ",").split(",") if s]
        start_date = self.start_date_edit.date().toString('yyyy-MM-dd')
        end_date = self.end_date_edit.date().toString('yyyy-MM-dd')

        self.import_btn.setEnabled(False)
        self.check_btn.setEnabled(False)
        self.status_label.setText("开始导入数据...")

        period_map = {
            "日线(1d)": "1d",
            "分钟(1m)": "1m",
            "5分钟(5m)": "5m",
            "Tick(tick)": "tick"
        }
        period = period_map.get(self.period_combo.currentText(), "1d")

        self.update_thread = DataUpdateThread(
            self.duckdb_path,
            stock_codes,
            start_date,
            end_date,
            period
        )
        self.update_thread.progress_updated.connect(self.on_update_progress)
        self.update_thread.update_completed.connect(self.on_update_completed)
        self.update_thread.error_occurred.connect(self.on_update_error)
        self.update_thread.start()

    def refresh_all(self):
        """刷新所有数据"""
        self.status_label.setText("正在刷新...")
        self.load_data_tree()
        self.load_statistics(show_dialog=False)
        self.status_label.setText("刷新完成")

    def on_update_progress(self, progress: int, message: str):
        self.status_label.setText(f"{message} ({progress}%)")

    def on_update_completed(self, result: dict):
        self.import_btn.setEnabled(True)
        self.check_btn.setEnabled(True)
        verify = result.get("verify", [])
        verify_summary = [
            f"{item.get('stock_code')}: 完整度 {item.get('completeness_ratio', 0) * 100:.2f}%"
            for item in verify
        ]
        verify_skipped = result.get("verify_skipped", False)
        period = result.get("period", "1d")
        msg = (
            f"导入完成\n"
            f"总计: {result.get('total', 0)}\n"
            f"成功: {result.get('success', 0)}\n"
            f"跳过: {result.get('skipped', 0)}\n"
            f"失败: {result.get('failed', 0)}\n"
            f"周期: {period}"
        )
        if verify_summary:
            msg = msg + "\n\n校验摘要:\n" + "\n".join(verify_summary)
        if verify_skipped:
            msg = msg + "\n\n校验摘要:\n当前周期不支持完整性校验"
        QMessageBox.information(self, "导入完成", msg)
        stock_text = self.stock_code_edit.text().strip()
        stock_codes = [s for s in stock_text.replace("，", ",").replace(" ", ",").split(",") if s]
        should_query = False
        if len(stock_codes) > 1:
            selected = self.show_stock_selection_dialog(stock_codes)
            if selected:
                self.stock_code_edit.setText(",".join(selected))
                should_query = True
        elif stock_codes:
            self.stock_code_edit.setText(stock_codes[0])
            should_query = True
        self.query_period_combo.setCurrentText(self.period_combo.currentText())
        if should_query:
            self.status_label.setText("导入完成，自动查询中...")
            QTimer.singleShot(100, self.execute_query)
        self.refresh_all()

    def on_update_error(self, error_msg: str):
        self.import_btn.setEnabled(True)
        self.check_btn.setEnabled(True)
        QMessageBox.critical(self, "导入失败", error_msg)
        self.status_label.setText("导入失败")

    def show_stock_selection_dialog(self, stock_codes: list[str]) -> list[str] | None:
        dialog = QDialog(self)
        dialog.setWindowTitle("选择要查询的标的")
        dialog.resize(300, 400)

        layout = QVBoxLayout(dialog)
        label = QLabel(f"导入了 {len(stock_codes)} 个标的，请选择要查询的标的：")
        layout.addWidget(label)

        search_layout = QHBoxLayout()
        search_label = QLabel("搜索:")
        search_edit = QLineEdit()
        search_edit.setPlaceholderText("输入标的代码...")
        search_layout.addWidget(search_label)
        search_layout.addWidget(search_edit)
        layout.addLayout(search_layout)

        select_all_checkbox = QCheckBox("全选")
        layout.addWidget(select_all_checkbox)

        list_widget = QListWidget()
        list_widget.setSelectionMode(QListWidget.MultiSelection)
        for code in stock_codes:
            list_widget.addItem(code)
        list_widget.setCurrentRow(0)
        layout.addWidget(list_widget)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)

        def apply_filter(text: str):
            keyword = text.lower().strip()
            for i in range(list_widget.count()):
                item = list_widget.item(i)
                if item is not None:
                    item.setHidden(keyword not in item.text().lower())
            update_select_all_state()

        def toggle_select_all(state: int):
            checked = state == Qt.Checked
            for i in range(list_widget.count()):
                item = list_widget.item(i)
                if item is not None and not item.isHidden():
                    item.setSelected(checked)

        def update_select_all_state():
            visible_items = []
            for i in range(list_widget.count()):
                item = list_widget.item(i)
                if item is not None and not item.isHidden():
                    visible_items.append(item)
            if not visible_items:
                select_all_checkbox.blockSignals(True)
                select_all_checkbox.setCheckState(Qt.Unchecked)
                select_all_checkbox.blockSignals(False)
                return
            all_selected = all(item.isSelected() for item in visible_items)
            select_all_checkbox.blockSignals(True)
            select_all_checkbox.setCheckState(Qt.Checked if all_selected else Qt.Unchecked)
            select_all_checkbox.blockSignals(False)

        search_edit.textChanged.connect(apply_filter)
        select_all_checkbox.stateChanged.connect(toggle_select_all)
        list_widget.itemSelectionChanged.connect(update_select_all_state)

        if dialog.exec_() == QDialog.Accepted:
            selected_items = list_widget.selectedItems()
            if selected_items:
                return [item.text() for item in selected_items]
        return None

    def closeEvent(self, event):
        try:
            threads = [
                getattr(self, 'query_thread', None),
                getattr(self, 'update_thread', None),
            ]
            for t in threads:
                if t and t.isRunning():
                    t.requestInterruption()
                    t.quit()
                    t0 = time.monotonic()
                    finished = t.wait(1000)
                    elapsed_ms = int((time.monotonic() - t0) * 1000)
                    status = "已退出" if finished else "超时未退出"
                    print(f"[closeEvent] DuckDBDataManagerWidget - {t.__class__.__name__}: {status} ({elapsed_ms}ms)")
        finally:
            super().closeEvent(event)


if __name__ == "__main__":
    import sys

    from PyQt5.QtWidgets import QApplication

    app = QApplication(sys.argv)
    widget = DuckDBDataManagerWidget()
    widget.resize(1200, 800)
    widget.setWindowTitle("DuckDB 数据管理")
    widget.show()
    sys.exit(app.exec_())
