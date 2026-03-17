#!/usr/bin/env python3
"""
表格批量渲染器 - 避免一次性渲染导致UI卡顿

功能：
1. 分批渲染 - 每批渲染N行后交回事件循环
2. 延迟加载 - 使用 QTimer 实现
3. 支持 QTableWidget 和 QTableView

使用示例：
    from gui_app.widgets.table_renderer import TableRenderer

    # 创建渲染器
    renderer = TableRenderer(self.trades_table)

    # 分批渲染数据
    renderer.render_batch(data_list, batch_size=100)
"""

import logging
from typing import Any, Callable, Optional

from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QTableView, QTableWidget, QTableWidgetItem


class TableRenderer:
    """
    表格批量渲染器

    使用分批渲染避免UI卡顿
    """

    def __init__(
        self,
        table: Optional[QTableWidget] = None,
        table_view: Optional[QTableView] = None,
        batch_size: int = 100,
        render_delay_ms: int = 10,
    ):
        self._logger = logging.getLogger(__name__)
        self._table = table
        self._table_view = table_view
        self._model = None
        self._batch_size = batch_size
        self._render_delay_ms = render_delay_ms

        # 如果是 QTableView，获取 model
        if self._table_view is not None and hasattr(self._table_view, "setModel"):
            # 尝试导入 DataFrameTableModel
            try:
                from gui_app.widgets.table_models import DataFrameTableModel

                self._model_class: Optional[type[DataFrameTableModel]] = DataFrameTableModel
            except ImportError:
                self._model_class = None

        # 渲染状态
        self._render_data: list[list[Any]] = []
        self._render_callbacks: list[Callable] = []
        self._render_timer: Optional[QTimer] = None
        self._render_index = 0

    def render_batch(
        self,
        data: list[list[Any]],
        row_colors: Optional[list[Any]] = None,
        cell_styles: Optional[list[list[dict[str, Any]]]] = None,
    ):
        """
        分批渲染数据

        参数：
            data: 行列表，每行是列值列表
            row_colors: 每行的背景色 (可选)
            cell_styles: 每个单元格的样式 (可选)
        """
        if not data:
            return

        self._render_data = data
        self._render_index = 0
        self._row_colors = row_colors or []
        self._cell_styles = cell_styles or []

        # 如果是 QTableWidget
        if self._table is not None:
            self._render_batch_widget()
        # 如果是 QTableView
        elif self._table_view is not None and self._model_class is not None:
            self._render_batch_view()

    def _render_batch_widget(self):
        """分批渲染 QTableWidget"""
        table = self._table
        if table is None:
            return
        table.setRowCount(len(self._render_data))
        self._render_next_batch_widget()

    def _render_next_batch_widget(self):
        """继续渲染下一批"""
        if self._render_index >= len(self._render_data):
            # 渲染完成
            return
        table = self._table
        if table is None:
            return

        # 禁用排序以提高性能
        table.setSortingEnabled(False)

        # 渲染一批
        end = min(self._render_index + self._batch_size, len(self._render_data))

        for i in range(self._render_index, end):
            row_data = self._render_data[i]
            for j, value in enumerate(row_data):
                item = QTableWidgetItem(str(value) if value is not None else "")

                # 设置行颜色
                if i < len(self._row_colors) and self._row_colors[i]:
                    item.setBackground(self._row_colors[i])

                # 设置单元格样式
                if i < len(self._cell_styles) and j < len(self._cell_styles[i]):
                    style = self._cell_styles[i][j]
                    if "background" in style:
                        item.setBackground(style["background"])
                    if "foreground" in style:
                        item.setForeground(style["foreground"])

                table.setItem(i, j, item)

        self._render_index = end

        # 安排下一批渲染
        if self._render_index < len(self._render_data):
            QTimer.singleShot(self._render_delay_ms, self._render_next_batch_widget)
        else:
            # 渲染完成，重新启用排序
            table.setSortingEnabled(True)
            self._logger.debug(f"Rendered {len(self._render_data)} rows in batches")

    def _render_batch_view(self):
        """渲染 QTableView (使用 DataFrameTableModel)"""
        if not self._model_class:
            return
        table_view = self._table_view
        if table_view is None:
            return

        # 转换为 DataFrame
        import pandas as pd

        df = pd.DataFrame(self._render_data)

        # 创建或更新模型
        if table_view.model() is None:
            model = self._model_class(df)
            table_view.setModel(model)
        else:
            model = table_view.model()
            if model is not None and hasattr(model, "set_data"):
                model_any: Any = model
                model_any.set_data(df)

        self._logger.debug(f"Rendered {len(self._render_data)} rows to QTableView")

    def cancel(self):
        """取消渲染"""
        self._render_data = []
        self._render_index = 0


class LazyTableLoader:
    """
    延迟加载器 - 数据量大时先显示部分，后台加载剩余

    使用示例:
        loader = LazyTableLoader(table)
        loader.load_async(query_func, callback)
    """

    def __init__(self, table: QTableWidget, page_size: int = 100):
        self._table = table
        self._page_size = page_size
        self._logger = logging.getLogger(__name__)
        self._all_data: list[list[Any]] = []
        self._current_page = 0

    def set_data(self, data: list[list[Any]]):
        """设置数据并显示第一页"""
        self._all_data = data
        self._current_page = 0
        self._show_page(0)

    def _show_page(self, page: int):
        """显示指定页"""
        start = page * self._page_size
        end = min(start + self._page_size, len(self._all_data))

        if start >= len(self._all_data):
            return

        page_data = self._all_data[start:end]

        self._table.setRowCount(len(page_data))

        for i, row_data in enumerate(page_data):
            for j, value in enumerate(row_data):
                item = QTableWidgetItem(str(value) if value is not None else "")
                self._table.setItem(i, j, item)

        self._current_page = page

    def next_page(self):
        """显示下一页"""
        if (self._current_page + 1) * self._page_size < len(self._all_data):
            self._show_page(self._current_page + 1)

    def prev_page(self):
        """显示上一页"""
        if self._current_page > 0:
            self._show_page(self._current_page - 1)

    def has_next(self) -> bool:
        return (self._current_page + 1) * self._page_size < len(self._all_data)

    def has_prev(self) -> bool:
        return self._current_page > 0

    def get_page_info(self) -> str:
        total_pages = (len(self._all_data) + self._page_size - 1) // self._page_size
        return f"{self._current_page + 1}/{total_pages} 页 (共 {len(self._all_data)} 条)"
