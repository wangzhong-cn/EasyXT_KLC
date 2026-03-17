#!/usr/bin/env python3
"""
虚拟化表格模型 - 高性能大数据展示

功能：
1. QAbstractTableModel 封装，支持大数据
2. 虚拟滚动 - 只渲染可见行
3. 排序/筛选 - 通过 QSortFilterProxyModel

使用示例：
    from gui_app.widgets.table_models import DataFrameTableModel

    model = DataFrameTableModel(df)
    view.setModel(model)

    # 添加排序代理
    from PyQt5.QtWidgets import QSortFilterProxyModel
    proxy = QSortFilterProxyModel()
    proxy.setSourceModel(model)
    view.setModel(proxy)
"""

import logging
from typing import Any, Optional

import numpy as np
import pandas as pd
from PyQt5.QtCore import QAbstractTableModel, QModelIndex, QSortFilterProxyModel, Qt, pyqtSignal
from PyQt5.QtWidgets import QAbstractItemView, QHeaderView, QTableView


def create_virtual_table_view(
    dataframe: Optional[pd.DataFrame] = None,
    columns: Optional[list[str]] = None,
    sortable: bool = True,
    editable: bool = False,
) -> tuple[QTableView, "DataFrameTableModel"]:
    """
    创建高性能虚拟化表格视图

    返回: (QTableView, DataFrameTableModel)

    使用示例:
        view, model = create_virtual_table_view(df, columns=['列1', '列2'])
        layout.addWidget(view)

        # 后续更新数据
        model.set_data(new_df)
    """
    model = DataFrameTableModel(dataframe)
    view = QTableView()
    view.setModel(model)

    # 配置视图
    view.setAlternatingRowColors(True)
    view.setSortingEnabled(sortable)
    view.setEditTriggers(
        QAbstractItemView.NoEditTriggers if not editable else QAbstractItemView.AllEditTriggers
    )
    view.setSelectionBehavior(QAbstractItemView.SelectRows)
    header = view.horizontalHeader()
    if header is not None:
        header.setStretchLastSection(True)

    # 自动调整列宽
    header = view.horizontalHeader()
    if header:
        header.setSectionResizeMode(QHeaderView.Interactive)

    return view, model


class DataFrameTableModel(QAbstractTableModel):
    """
    DataFrame 表格模型 - 高性能虚拟化

    信号：
        data_loaded: 数据加载完成 (row_count, column_count)
    """

    data_loaded = pyqtSignal(int, int)

    def __init__(self, dataframe: Optional[pd.DataFrame] = None):
        super().__init__()
        self._df = dataframe if dataframe is not None else pd.DataFrame()
        self._logger = logging.getLogger(__name__)

    def set_data(self, dataframe: pd.DataFrame):
        """设置数据"""
        self.beginResetModel()
        self._df = dataframe if dataframe is not None else pd.DataFrame()
        self.endResetModel()
        self.data_loaded.emit(len(self._df), len(self._df.columns))

    def get_data(self) -> pd.DataFrame:
        """获取数据"""
        return self._df

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """返回行数"""
        return len(self._df)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """返回列数"""
        return len(self._df.columns)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        """获取单元格数据"""
        if not index.isValid():
            return None

        if role == Qt.DisplayRole or role == Qt.EditRole:
            value = self._df.iloc[index.row(), index.column()]
            if pd.isna(value):
                return ""
            if isinstance(value, (float, np.floating)):
                # 格式化浮点数
                if abs(value) < 0.01 or abs(value) > 10000:
                    return f"{value:.4f}"
                return f"{value:.2f}"
            return str(value)

        return None

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole
    ) -> Any:
        """获取表头数据"""
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._df.columns[section])
            elif orientation == Qt.Vertical:
                return str(section + 1)
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        """返回单元格标志"""
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder):
        """排序"""
        if column < 0 or column >= len(self._df.columns):
            return

        self.beginResetModel()

        col_name = str(self._df.columns[column])
        ascending = order == Qt.AscendingOrder

        try:
            self._df = self._df.sort_values(by=col_name, ascending=ascending)
        except Exception as e:
            self._logger.warning(f"排序失败: {e}")

        self.endResetModel()

    def get_row_data(self, row: int) -> dict[str, Any]:
        """获取行数据"""
        if row < 0 or row >= len(self._df):
            return {}
        return self._df.iloc[row].to_dict()

    def get_column_data(self, column: int) -> list[Any]:
        """获取列数据"""
        if column < 0 or column >= len(self._df.columns):
            return []
        return self._df.iloc[:, column].tolist()


class SortFilterProxyModel(QSortFilterProxyModel):
    """
    排序筛选代理模型 - 支持模糊搜索和列筛选
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._filter_text = ""
        self._filter_columns = None  # None 表示所有列

    def set_filter_text(self, text: str):
        """设置过滤文本"""
        self._filter_text = text.strip().lower()
        self.invalidateFilter()

    def set_filter_columns(self, columns: list):
        """设置过滤的列"""
        self._filter_columns = columns
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex) -> bool:
        """过滤AcceptsRow"""
        if not self._filter_text:
            return True

        model = self.sourceModel()
        if model is None:
            return True

        # 检查所有列或指定列
        columns_to_check = (
            range(model.columnCount()) if self._filter_columns is None else self._filter_columns
        )

        for col in columns_to_check:
            if col >= model.columnCount():
                continue
            index = model.index(source_row, col, source_parent)
            data = model.data(index, Qt.DisplayRole)
            if data and self._filter_text in str(data).lower():
                return True

        return False
