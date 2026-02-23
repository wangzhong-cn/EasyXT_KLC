#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

import sys
import importlib
from typing import TYPE_CHECKING, Any
from pathlib import Path

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QGroupBox, QLabel, QPushButton, QTableWidget, QTableWidgetItem, QHeaderView, QComboBox,
    QSplitter, QMessageBox,
    QDateEdit, QTreeWidget, QTreeWidgetItem,
    QLineEdit
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QDate
from PyQt5.QtGui import QFont

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'data_manager'))

duckdb = importlib.import_module("duckdb")
pd = importlib.import_module("pandas")
DataIntegrityChecker = getattr(importlib.import_module("data_integrity_checker"), "DataIntegrityChecker")
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
            if DB_MANAGER_AVAILABLE:
                # 使用连接管理器（只读模式）
                manager = get_db_manager(self.duckdb_path)
                df = manager.execute_read_query(self.query)
            else:
                # 回退到直接连接（使用只读模式）
                con = duckdb.connect(self.duckdb_path, read_only=True)
                try:
                    df = con.execute(self.query).df()
                finally:
                    con.close()

            self.data_ready.emit(df)
        except Exception as e:
            self.error_occurred.emit(str(e))


class DataUpdateThread(QThread):
    """数据更新工作线程"""

    progress_updated = pyqtSignal(int, str)
    update_completed = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(self, duckdb_path, stock_codes, start_date, end_date):
        super().__init__()
        self.duckdb_path = duckdb_path
        self.stock_codes = stock_codes
        self.start_date = start_date
        self.end_date = end_date

    def run(self):
        try:
            # TODO: 实现数据更新逻辑
            # 这里调用 import_bonds_to_duckdb.py 中的函数
            total = len(self.stock_codes)

            for i, stock_code in enumerate(self.stock_codes, 1):
                self.progress_updated.emit(
                    int(i / total * 100),
                    f"更新 {stock_code} ({i}/{total})"
                )
                # 模拟更新
                self.msleep(100)

            self.update_completed.emit({
                'total': total,
                'success': total,
                'failed': 0
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

        self.duckdb_path = r'D:/StockData/stock_data.ddb'
        self.con = None

        self.init_ui()
        self.load_data_tree()
        self.load_statistics()

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
        self.stats_btn.clicked.connect(self.load_statistics)
        self.refresh_btn.clicked.connect(self.refresh_all)
        self.query_btn.clicked.connect(self.execute_query)

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

        self.result_table = QTableWidget()
        self.result_table.setColumnCount(7)
        self.result_table.setHorizontalHeaderLabels([
            "日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额"
        ])
        header = self.result_table.horizontalHeader()
        if header:
            header.setSectionResizeMode(QHeaderView.Stretch)
        result_layout.addWidget(self.result_table)

        result_group.setLayout(result_layout)
        layout.addWidget(result_group)

        return panel

    def load_data_tree(self):
        """加载数据树形列表"""
        try:
            # 使用连接管理器或只读连接
            if DB_MANAGER_AVAILABLE:
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
            else:
                con = duckdb.connect(self.duckdb_path, read_only=True)
                try:
                    df = con.execute("""
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
                    """).df()
                finally:
                    con.close()

            # 构建树
            self.data_tree.clear()

            markets = {}
            for _, row in df.iterrows():
                market = row['market']
                stock_code = row['stock_code']
                count = row['count']

                if market not in markets:
                    markets[market] = []

                markets[market].append((stock_code, count))

            # 添加到树
            for market_name, stocks in sorted(markets.items()):
                market_item = QTreeWidgetItem([market_name, ""])
                market_item.setExpanded(True)

                for stock_code, count in sorted(stocks):
                    stock_item = QTreeWidgetItem([stock_code, str(count)])
                    market_item.addChild(stock_item)

                self.data_tree.addTopLevelItem(market_item)

        except Exception as e:
            QMessageBox.warning(self, "错误", f"加载数据树失败: {e}")

    def on_tree_item_clicked(self, item: QTreeWidgetItem, column: int):
        """树形列表项点击事件"""
        text = item.text(0)

        # 如果是股票代码（包含点）
        if '.' in text:
            self.stock_code_edit.setText(text)

    def execute_query(self):
        """执行查询"""
        stock_code = self.stock_code_edit.text().strip()
        if not stock_code:
            QMessageBox.warning(self, "提示", "请输入股票代码")
            return

        start_date = self.start_date_edit.date().toString('yyyy-MM-dd')
        end_date = self.end_date_edit.date().toString('yyyy-MM-dd')

        # 获取复权类型
        adjust_index = self.adjust_combo.currentIndex()
        adjust_types = ['', 'front', 'back', 'geometric_front', 'geometric_back']
        adjust_type = adjust_types[adjust_index]

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

        query = f"""
            SELECT
                date::DATE as date,
                {price_cols[0]}::DOUBLE as open,
                {price_cols[1]}::DOUBLE as high,
                {price_cols[2]}::DOUBLE as low,
                {price_cols[3]}::DOUBLE as close,
                volume::BIGINT as volume,
                amount::DOUBLE as amount
            FROM stock_daily
            WHERE stock_code = '{stock_code}'
              AND date >= '{start_date}'
              AND date <= '{end_date}'
            ORDER BY date
        """

        # 显示等待状态
        self.status_label.setText("正在查询数据...")
        self.result_table.setRowCount(0)

        # 在线程中执行查询
        self.query_thread = DataQueryThread(self.duckdb_path, query)
        self.query_thread.data_ready.connect(self.on_query_result)
        self.query_thread.error_occurred.connect(self.on_query_error)
        self.query_thread.start()

    def on_query_result(self, df: Any):
        """查询结果回调"""
        self.result_table.setRowCount(len(df))

        for i, (_, row) in enumerate(df.iterrows()):
            self.result_table.setItem(i, 0, QTableWidgetItem(str(row['date'])))
            self.result_table.setItem(i, 1, QTableWidgetItem(f"{row['open']:.2f}"))
            self.result_table.setItem(i, 2, QTableWidgetItem(f"{row['high']:.2f}"))
            self.result_table.setItem(i, 3, QTableWidgetItem(f"{row['low']:.2f}"))
            self.result_table.setItem(i, 4, QTableWidgetItem(f"{row['close']:.2f}"))
            self.result_table.setItem(i, 5, QTableWidgetItem(f"{int(row['volume'])}"))
            self.result_table.setItem(i, 6, QTableWidgetItem(f"{row['amount']:.0f}" if pd.notna(row['amount']) else ""))

        self.status_label.setText(f"查询完成，共 {len(df)} 条记录")

    def on_query_error(self, error_msg: str):
        """查询错误回调"""
        QMessageBox.critical(self, "查询错误", error_msg)
        self.status_label.setText("查询失败")

    def load_statistics(self):
        """加载统计信息"""
        try:
            # 使用连接管理器或只读连接
            if DB_MANAGER_AVAILABLE:
                manager = get_db_manager(self.duckdb_path)
                stats = manager.execute_read_query("""
                    SELECT
                        COUNT(DISTINCT stock_code) as stock_count,
                        COUNT(*) as total_records,
                        MIN(date) as first_date,
                        MAX(date) as last_date
                    FROM stock_daily
                """)
            else:
                con = duckdb.connect(self.duckdb_path, read_only=True)
                try:
                    stats = con.execute("""
                        SELECT
                            COUNT(DISTINCT stock_code) as stock_count,
                            COUNT(*) as total_records,
                            MIN(date) as first_date,
                            MAX(date) as last_date
                        FROM stock_daily
                    """).fetchdf()
                finally:
                    con.close()

            if not stats.empty:
                row = stats.iloc[0]
                msg = (
                    f"标的数量: {row['stock_count']:,} | "
                    f"总记录数: {row['total_records']:,} | "
                    f"日期范围: {row['first_date']} ~ {row['last_date']}"
                )
                self.status_label.setText(msg)
                QMessageBox.information(self, "统计信息", msg)

        except Exception as e:
            QMessageBox.warning(self, "错误", f"加载统计信息失败: {e}")

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
        QMessageBox.information(self, "导入数据",
            "数据导入功能\n\n"
            "请使用命令行工具：\n"
            "  python import_bonds_to_duckdb.py\n\n"
            "或者运行自动更新服务：\n"
            "  python data_manager/auto_data_updater.py --start"
        )

    def refresh_all(self):
        """刷新所有数据"""
        self.status_label.setText("正在刷新...")
        self.load_data_tree()
        self.load_statistics()
        self.status_label.setText("刷新完成")


if __name__ == "__main__":
    from PyQt5.QtWidgets import QApplication
    import sys

    app = QApplication(sys.argv)
    widget = DuckDBDataManagerWidget()
    widget.resize(1200, 800)
    widget.setWindowTitle("DuckDB 数据管理")
    widget.show()
    sys.exit(app.exec_())
