#!/usr/bin/env python3
"""
本地数据管理GUI组件
提供本地数据的下载、管理和查看功能
"""

import importlib
import importlib.util
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd
from PyQt5.QtCore import (
    QAbstractTableModel,
    QDate,
    QModelIndex,
    QSortFilterProxyModel,
    Qt,
    QThread,
    QTimer,
    QVariant,
    pyqtSignal,
)
from PyQt5.QtGui import QTextCursor
from PyQt5.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateEdit,
    QDialog,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSplitter,
    QTableView,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

# 添加项目路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

widgets_path = os.path.join(project_root, 'gui_app', 'widgets')
if widgets_path not in sys.path:
    sys.path.insert(0, widgets_path)

class _PandasTableModel(QAbstractTableModel):
    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self._df = df.reset_index(drop=False).copy()
        self._columns = list(self._df.columns)

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self._df)

    def columnCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self._columns)

    def data(self, index: QModelIndex, role=Qt.DisplayRole):
        if not index.isValid():
            return QVariant()
        if role == Qt.DisplayRole:
            try:
                val = self._df.iat[index.row(), index.column()]
                if pd.isna(val):
                    return ""
                return str(val)
            except Exception:
                return ""
        return QVariant()

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return QVariant()
        if orientation == Qt.Horizontal:
            if 0 <= section < len(self._columns):
                return self._columns[section]
        else:
            return str(section + 1)
        return QVariant()

_ALLOWED_WRITE_TABLES: frozenset[str] = frozenset({
    "stock_daily", "stock_1m", "stock_5m", "stock_15m", "stock_30m", "stock_60m",
    "stock_tick", "stock_transaction", "data_ingestion_status",
    "custom_period_bars", "risk_events", "strategy_snapshots",
})
_VALID_KLINE_PERIODS: frozenset[str] = frozenset({"1d", "1m", "5m", "15m", "30m", "60m"})


def _get_table_columns(con, table_name: str) -> list[str]:
    if table_name not in _ALLOWED_WRITE_TABLES:
        return []
    try:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",
            [table_name],
        ).fetchall()
        if rows:
            return [row[0] for row in rows]
    except Exception:
        pass
    try:
        rows = con.execute(
            f"SELECT column_name FROM pragma_table_info('{table_name}')"  # noqa: S608 table_name validated above
        ).fetchall()
        return [row[0] for row in rows]
    except Exception:
        return []

def _align_dataframe_to_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    aligned = df.copy()
    for col in columns:
        if col not in aligned.columns:
            aligned[col] = None
    aligned = aligned.reindex(columns=columns)
    return aligned


def _build_stock_daily_delete_sql(stock_codes: list[str], columns: list[str], period: str = '1d', adjust_type: str = 'none') -> tuple[str, list]:
    placeholders = ', '.join(['?' for _ in stock_codes])
    where_clauses = [f"stock_code IN ({placeholders})"]
    params: list = list(stock_codes)
    if 'period' in columns:
        where_clauses.append("period = ?")
        params.append(period)
    if 'adjust_type' in columns:
        where_clauses.append("adjust_type = ?")
        params.append(adjust_type)
    return "DELETE FROM stock_daily WHERE " + " AND ".join(where_clauses), params  # noqa: S608


def _upsert_ingestion_status(
    con,
    stock_code: str,
    period: str,
    start_date: str,
    end_date: str,
    source: str,
    status: str,
    record_count: int,
    error_message: Optional[str]
) -> None:
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS data_ingestion_status (
                stock_code VARCHAR NOT NULL,
                period VARCHAR NOT NULL,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                source VARCHAR,
                status VARCHAR,
                record_count INTEGER,
                error_message VARCHAR,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (stock_code, period)
            )
        """)
        con.execute(
            "DELETE FROM data_ingestion_status WHERE stock_code = ? AND period = ?",
            [stock_code, period]
        )
        con.execute(
            """
            INSERT INTO data_ingestion_status (
                stock_code,
                period,
                start_date,
                end_date,
                source,
                status,
                record_count,
                error_message,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                stock_code,
                period,
                start_date,
                end_date,
                source,
                status,
                int(record_count),
                error_message
            ]
        )
    except Exception:
        return


def _import_duckdb_manager():
    module_path = os.path.join(project_root, "data_manager", "duckdb_connection_pool.py")
    spec = importlib.util.spec_from_file_location("_easyxt_duckdb_connection_pool", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载模块: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.get_db_manager


_tables_ensured = False  # 模块级单次标志，避免每个线程反复开写连接


def _ensure_duckdb_tables() -> bool:
    global _tables_ensured
    if _tables_ensured:
        return True
    if not UNIFIED_INTERFACE_AVAILABLE:
        return False
    interface = None
    try:
        interface = UnifiedDataInterface(_get_duckdb_path())
        if not interface.connect(read_only=False):
            return False
        interface._ensure_tables_exist()
        _tables_ensured = True
        return True
    except Exception:
        return False
    finally:
        if interface:
            interface.close()

BatchFinancialSaveThread: Any = None
if importlib.util.find_spec("advanced_data_viewer_widget") is not None:
    module = importlib.import_module("advanced_data_viewer_widget")
    BatchFinancialSaveThread = getattr(module, "BatchFinancialSaveThread", None)
    BATCH_SAVE_AVAILABLE = BatchFinancialSaveThread is not None
else:
    BATCH_SAVE_AVAILABLE = False

try:
    from data_manager.unified_data_interface import UnifiedDataInterface
    UNIFIED_INTERFACE_AVAILABLE = True
except ImportError:
    UNIFIED_INTERFACE_AVAILABLE = False

_resolve_duckdb_path: Optional[Callable[[Optional[str]], str]] = None
try:
    from data_manager.duckdb_connection_pool import resolve_duckdb_path as _resolve_duckdb_path
except Exception:
    _resolve_duckdb_path = None

resolve_duckdb_path: Optional[Callable[[Optional[str]], str]] = (
    _resolve_duckdb_path if callable(_resolve_duckdb_path) else None
)


def _get_duckdb_path() -> str:
    if resolve_duckdb_path is not None:
        return resolve_duckdb_path(None)
    return r"D:/StockData/stock_data.ddb"


class DataDownloadThread(QThread):
    """数据下载线程"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)  # current, total
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, task_type, symbols, start_date, end_date, data_type='daily'):
        super().__init__()
        self.task_type = task_type  # 'download_stocks', 'download_bonds', 'update_data'
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.data_type = data_type  # 'daily', '1min', '5min', 'tick'
        self._is_running = True

    def run(self):
        """运行下载任务"""
        try:
            if self.task_type == 'download_stocks':
                self._download_stocks()
            elif self.task_type == 'download_bonds':
                self._download_bonds()
            elif self.task_type == 'update_data':
                self._update_data()
            elif self.task_type == 'backfill_history':
                self._backfill_history()
            elif self.task_type == 'download_tick':
                self._download_tick()
            elif self.task_type == 'download_transaction':
                self._download_transaction()
        except Exception as e:
            import traceback
            error_msg = f"下载失败: {str(e)}\n{traceback.format_exc()}"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)

    def _download_stocks(self):
        """下载股票数据"""
        try:
            # 导入本地数据管理器
            factor_platform_path = Path(__file__).parents[2] / "101因子" / "101因子分析平台" / "src"
            if str(factor_platform_path) not in sys.path:
                sys.path.insert(0, str(factor_platform_path))

            from data_manager import LocalDataManager
            duckdb_manager = None
            duckdb_columns = None
            duckdb_buffer = []
            duckdb_batch_size = 50
            try:
                get_db_manager = _import_duckdb_manager()
                duckdb_manager = get_db_manager(_get_duckdb_path())
            except Exception:
                duckdb_manager = None

            manager = LocalDataManager()
            self.log_signal.emit("✅ 数据管理器初始化成功")

            # 如果没有指定股票列表，获取全部A股
            if not self.symbols:
                self.log_signal.emit("📊 正在获取A股列表...")
                self.symbols = manager.get_all_stocks_list(
                    include_st=True,
                    include_sz=True,
                    include_bj=True,
                    exclude_st=True,
                    exclude_delisted=True
                )
                self.log_signal.emit(f"✅ 获取到 {len(self.symbols)} 只A股")

            total = len(self.symbols)
            success_count = 0
            failed_count = 0
            failed_list = []  # 记录失败的股票及原因

            for i, symbol in enumerate(self.symbols):
                if not self._is_running:
                    self.log_signal.emit("⚠️ 用户中断下载")
                    break

                try:
                    self.progress_signal.emit(i + 1, total)

                    # 下载数据
                    df = manager._fetch_from_source(symbol, self.start_date, self.end_date)

                    if df.empty:
                        failed_count += 1
                        failed_list.append(f"{symbol} - 数据为空")
                        continue

                    # 保存数据
                    success, file_size = manager.storage.save_data(df, symbol, 'daily')

                    if success:
                        manager.metadata.update_data_version(
                            symbol=symbol,
                            symbol_type='stock',
                            start_date=str(df.index.min().date()),
                            end_date=str(df.index.max().date()),
                            record_count=len(df),
                            file_size=file_size
                        )
                        success_count += 1
                        if duckdb_manager is not None:
                            date_series = pd.to_datetime(df.index).strftime('%Y-%m-%d')
                            df_processed = pd.DataFrame({
                                'stock_code': symbol,
                                'symbol_type': 'stock',
                                'date': date_series,
                                'period': '1d',
                                'open': df['open'] if 'open' in df.columns else df.iloc[:, 0],
                                'high': df['high'] if 'high' in df.columns else df.iloc[:, 0],
                                'low': df['low'] if 'low' in df.columns else df.iloc[:, 0],
                                'close': df['close'] if 'close' in df.columns else df.iloc[:, 0],
                                'volume': df['volume'].astype('int64') if 'volume' in df.columns else 0,
                                'amount': df['amount'] if 'amount' in df.columns else 0,
                                'adjust_type': 'none',
                                'factor': 1.0,
                                'created_at': datetime.now(),
                                'updated_at': datetime.now()
                            })
                            for col in ['open', 'high', 'low', 'close']:
                                df_processed[f'{col}_front'] = df_processed[col]
                                df_processed[f'{col}_back'] = df_processed[col]
                                df_processed[f'{col}_geometric_front'] = df_processed[col]
                                df_processed[f'{col}_geometric_back'] = df_processed[col]
                            duckdb_buffer.append(df_processed)
                            if len(duckdb_buffer) >= duckdb_batch_size:
                                try:
                                    df_all = pd.concat(duckdb_buffer, ignore_index=True)
                                    with duckdb_manager.get_write_connection() as con:
                                        if duckdb_columns is None:
                                            duckdb_columns = _get_table_columns(con, 'stock_daily')
                                        if not duckdb_columns:
                                            raise ValueError("stock_daily 表不存在或字段为空")
                                        delete_sql, delete_params = _build_stock_daily_delete_sql(
                                            df_all['stock_code'].unique().tolist(),
                                            duckdb_columns,
                                            period='1d',
                                            adjust_type='none'
                                        )
                                        con.execute(delete_sql, delete_params)
                                        df_aligned = _align_dataframe_to_columns(df_all, duckdb_columns)
                                        con.register('temp_bulk', df_aligned)
                                        cols_sql = ", ".join(duckdb_columns)
                                        con.execute(f"INSERT INTO stock_daily ({cols_sql}) SELECT {cols_sql} FROM temp_bulk")  # noqa: S608
                                        con.unregister('temp_bulk')
                                        for stock_code in df_all['stock_code'].unique().tolist():
                                            df_symbol = df_all[df_all['stock_code'] == stock_code]
                                            _upsert_ingestion_status(
                                                con,
                                                stock_code,
                                                '1d',
                                                str(df_symbol['date'].min()),
                                                str(df_symbol['date'].max()),
                                                'local_download',
                                                'success',
                                                len(df_symbol),
                                                None
                                            )
                                    duckdb_buffer = []
                                except Exception as e:
                                    self.log_signal.emit(f"⚠️ DuckDB写入失败: {str(e)[:60]}")
                    else:
                        failed_count += 1
                        failed_list.append(f"{symbol} - 保存失败")

                    # 每下载100只股票输出一次日志
                    if (i + 1) % 100 == 0:
                        self.log_signal.emit(f"📊 进度: {i + 1}/{total} | 成功: {success_count} | 失败: {failed_count}")

                except Exception as e:
                    failed_count += 1
                    failed_list.append(f"{symbol} - {str(e)[:50]}")
                    continue

            manager.close()
            if duckdb_manager is not None and duckdb_buffer:
                try:
                    df_all = pd.concat(duckdb_buffer, ignore_index=True)
                    with duckdb_manager.get_write_connection() as con:
                        if duckdb_columns is None:
                            duckdb_columns = _get_table_columns(con, 'stock_daily')
                        if not duckdb_columns:
                            raise ValueError("stock_daily 表不存在或字段为空")
                        delete_sql, delete_params = _build_stock_daily_delete_sql(
                            df_all['stock_code'].unique().tolist(),
                            duckdb_columns,
                            period='1d',
                            adjust_type='none'
                        )
                        con.execute(delete_sql, delete_params)
                        df_aligned = _align_dataframe_to_columns(df_all, duckdb_columns)
                        con.register('temp_bulk', df_aligned)
                        cols_sql = ", ".join(duckdb_columns)
                        con.execute(f"INSERT INTO stock_daily ({cols_sql}) SELECT {cols_sql} FROM temp_bulk")  # noqa: S608
                        con.unregister('temp_bulk')
                        for stock_code in df_all['stock_code'].unique().tolist():
                            df_symbol = df_all[df_all['stock_code'] == stock_code]
                            _upsert_ingestion_status(
                                con,
                                stock_code,
                                '1d',
                                str(df_symbol['date'].min()),
                                str(df_symbol['date'].max()),
                                'local_download',
                                'success',
                                len(df_symbol),
                                None
                            )
                        for stock_code in df_all['stock_code'].unique().tolist():
                            df_symbol = df_all[df_all['stock_code'] == stock_code]
                            _upsert_ingestion_status(
                                con,
                                stock_code,
                                '1d',
                                str(df_symbol['date'].min()),
                                str(df_symbol['date'].max()),
                                'local_download',
                                'success',
                                len(df_symbol),
                                None
                            )
                except Exception as e:
                    self.log_signal.emit(f"⚠️ DuckDB写入失败: {str(e)[:60]}")

            result = {
                'total': total,
                'success': success_count,
                'failed': failed_count,
                'failed_list': failed_list,
                'task_type': 'download_stocks'
            }

            self.finished_signal.emit(result)
            self.log_signal.emit(f"✅ 下载完成! 总数: {total}, 成功: {success_count}, 失败: {failed_count}")

            # 输出失败清单
            if failed_list:
                self.log_signal.emit("")
                self.log_signal.emit("=" * 70)
                self.log_signal.emit("  失败清单:")
                for failed_item in failed_list:
                    self.log_signal.emit(f"    ✗ {failed_item}")
                self.log_signal.emit("=" * 70)

        except Exception as e:
            import traceback
            error_msg = f"下载股票数据失败: {str(e)}\n{traceback.format_exc()}"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)

    def _download_bonds(self):
        """下载可转债数据"""
        try:
            factor_platform_path = Path(__file__).parents[2] / "101因子" / "101因子分析平台" / "src"
            if str(factor_platform_path) not in sys.path:
                sys.path.insert(0, str(factor_platform_path))

            from data_manager import LocalDataManager
            duckdb_manager = None
            duckdb_columns = None
            duckdb_buffer = []
            duckdb_batch_size = 50
            try:
                get_db_manager = _import_duckdb_manager()
                duckdb_manager = get_db_manager(_get_duckdb_path())
            except Exception:
                duckdb_manager = None

            manager = LocalDataManager()
            self.log_signal.emit("✅ 数据管理器初始化成功")

            # 如果没有指定可转债列表，获取全部可转债
            if not self.symbols:
                self.log_signal.emit("📊 正在获取可转债列表...")
                self.symbols = manager.get_all_convertible_bonds_list()
                self.log_signal.emit(f"✅ 获取到 {len(self.symbols)} 只可转债")

            total = len(self.symbols)
            success_count = 0
            failed_count = 0
            failed_list = []  # 记录失败的可转债及原因

            for i, symbol in enumerate(self.symbols):
                if not self._is_running:
                    self.log_signal.emit("⚠️ 用户中断下载")
                    break

                try:
                    self.progress_signal.emit(i + 1, total)

                    # 下载数据
                    df = manager._fetch_from_source(symbol, self.start_date, self.end_date)

                    if df.empty:
                        failed_count += 1
                        failed_list.append(f"{symbol} - 数据为空")
                        continue

                    # 保存数据
                    success, file_size = manager.storage.save_data(df, symbol, 'daily')

                    if success:
                        manager.metadata.update_data_version(
                            symbol=symbol,
                            symbol_type='bond',
                            start_date=str(df.index.min().date()),
                            end_date=str(df.index.max().date()),
                            record_count=len(df),
                            file_size=file_size
                        )
                        success_count += 1
                        if duckdb_manager is not None:
                            date_series = pd.to_datetime(df.index).strftime('%Y-%m-%d')
                            df_processed = pd.DataFrame({
                                'stock_code': symbol,
                                'symbol_type': 'bond',
                                'date': date_series,
                                'period': '1d',
                                'open': df['open'] if 'open' in df.columns else df.iloc[:, 0],
                                'high': df['high'] if 'high' in df.columns else df.iloc[:, 0],
                                'low': df['low'] if 'low' in df.columns else df.iloc[:, 0],
                                'close': df['close'] if 'close' in df.columns else df.iloc[:, 0],
                                'volume': df['volume'].astype('int64') if 'volume' in df.columns else 0,
                                'amount': df['amount'] if 'amount' in df.columns else 0,
                                'adjust_type': 'none',
                                'factor': 1.0,
                                'created_at': datetime.now(),
                                'updated_at': datetime.now()
                            })
                            for col in ['open', 'high', 'low', 'close']:
                                df_processed[f'{col}_front'] = df_processed[col]
                                df_processed[f'{col}_back'] = df_processed[col]
                                df_processed[f'{col}_geometric_front'] = df_processed[col]
                                df_processed[f'{col}_geometric_back'] = df_processed[col]
                            duckdb_buffer.append(df_processed)
                            if len(duckdb_buffer) >= duckdb_batch_size:
                                try:
                                    df_all = pd.concat(duckdb_buffer, ignore_index=True)
                                    with duckdb_manager.get_write_connection() as con:
                                        if duckdb_columns is None:
                                            duckdb_columns = _get_table_columns(con, 'stock_daily')
                                        if not duckdb_columns:
                                            raise ValueError("stock_daily 表不存在或字段为空")
                                        delete_sql, delete_params = _build_stock_daily_delete_sql(
                                            df_all['stock_code'].unique().tolist(),
                                            duckdb_columns,
                                            period='1d',
                                            adjust_type='none'
                                        )
                                        con.execute(delete_sql, delete_params)
                                        df_aligned = _align_dataframe_to_columns(df_all, duckdb_columns)
                                        con.register('temp_bulk', df_aligned)
                                        cols_sql = ", ".join(duckdb_columns)
                                        con.execute(f"INSERT INTO stock_daily ({cols_sql}) SELECT {cols_sql} FROM temp_bulk")  # noqa: S608
                                        con.unregister('temp_bulk')
                                        for stock_code in df_all['stock_code'].unique().tolist():
                                            df_symbol = df_all[df_all['stock_code'] == stock_code]
                                            _upsert_ingestion_status(
                                                con,
                                                stock_code,
                                                '1d',
                                                str(df_symbol['date'].min()),
                                                str(df_symbol['date'].max()),
                                                'local_download',
                                                'success',
                                                len(df_symbol),
                                                None
                                            )
                                    duckdb_buffer = []
                                except Exception as e:
                                    self.log_signal.emit(f"⚠️ DuckDB写入失败: {str(e)[:60]}")
                    else:
                        failed_count += 1
                        failed_list.append(f"{symbol} - 保存失败")

                    # 每下载50只可转债输出一次日志
                    if (i + 1) % 50 == 0:
                        self.log_signal.emit(f"📊 进度: {i + 1}/{total} | 成功: {success_count} | 失败: {failed_count}")

                except Exception as e:
                    failed_count += 1
                    failed_list.append(f"{symbol} - {str(e)[:50]}")
                    continue

            manager.close()
            if duckdb_manager is not None and duckdb_buffer:
                try:
                    df_all = pd.concat(duckdb_buffer, ignore_index=True)
                    with duckdb_manager.get_write_connection() as con:
                        if duckdb_columns is None:
                            duckdb_columns = _get_table_columns(con, 'stock_daily')
                        if not duckdb_columns:
                            raise ValueError("stock_daily 表不存在或字段为空")
                        delete_sql, delete_params = _build_stock_daily_delete_sql(
                            df_all['stock_code'].unique().tolist(),
                            duckdb_columns,
                            period='1d',
                            adjust_type='none'
                        )
                        con.execute(delete_sql, delete_params)
                        df_aligned = _align_dataframe_to_columns(df_all, duckdb_columns)
                        con.register('temp_bulk', df_aligned)
                        cols_sql = ", ".join(duckdb_columns)
                        con.execute(f"INSERT INTO stock_daily ({cols_sql}) SELECT {cols_sql} FROM temp_bulk")  # noqa: S608
                        con.unregister('temp_bulk')
                except Exception as e:
                    self.log_signal.emit(f"⚠️ DuckDB写入失败: {str(e)[:60]}")

            result = {
                'total': total,
                'success': success_count,
                'failed': failed_count,
                'failed_list': failed_list,
                'task_type': 'download_bonds'
            }

            self.finished_signal.emit(result)
            self.log_signal.emit(f"✅ 下载完成! 总数: {total}, 成功: {success_count}, 失败: {failed_count}")

            # 输出失败清单
            if failed_list:
                self.log_signal.emit("")
                self.log_signal.emit("=" * 70)
                self.log_signal.emit("  失败清单:")
                for failed_item in failed_list:
                    self.log_signal.emit(f"    ✗ {failed_item}")
                self.log_signal.emit("=" * 70)

        except Exception as e:
            import traceback
            error_msg = f"下载可转债数据失败: {str(e)}\n{traceback.format_exc()}"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)

    def _download_tick(self):
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface
            factor_platform_path = Path(__file__).parents[2] / "101因子" / "101因子分析平台" / "src"
            if str(factor_platform_path) not in sys.path:
                sys.path.insert(0, str(factor_platform_path))
            from data_manager import LocalDataManager
            manager = LocalDataManager()
            if not self.symbols:
                self.log_signal.emit("📊 正在获取A股列表...")
                self.symbols = manager.get_all_stocks_list(
                    include_st=True,
                    include_sz=True,
                    include_bj=True,
                    exclude_st=True,
                    exclude_delisted=True
                )
                self.log_signal.emit(f"✅ 获取到 {len(self.symbols)} 只A股")
            queue = deque([(symbol, 0) for symbol in self.symbols])
            total = len(self.symbols)
            success_count = 0
            failed_count = 0
            failed_samples: list[dict[str, Any]] = []
            failed_reasons: dict[str, int] = {}
            max_retries = 2
            interface = UnifiedDataInterface(duckdb_path=_get_duckdb_path(), silent_init=True)
            interface.connect(read_only=False)
            idx = 0
            start_ts = time.time()
            last_log = start_ts
            while queue:
                if not self._is_running:
                    break
                symbol, attempt = queue.popleft()
                idx += 1
                self.progress_signal.emit(min(idx, total), total)
                reason = None
                try:
                    ok = interface.ingest_tick_data(
                        symbol, self.start_date, self.end_date, aggregate_1m=True
                    )
                    if ok:
                        success_count += 1
                    else:
                        reason = "empty_or_unavailable"
                except Exception as e:
                    reason = f"exception:{str(e)[:60]}"
                if reason:
                    if attempt < max_retries:
                        queue.append((symbol, attempt + 1))
                        self.log_signal.emit(f"🔁 Tick重试 {symbol} (第{attempt + 1}次)")
                    else:
                        failed_count += 1
                        failed_samples.append(
                            {
                                "stock_code": symbol,
                                "reason": reason,
                                "attempt": attempt + 1,
                                "start_date": self.start_date,
                                "end_date": self.end_date,
                                "task_type": "download_tick",
                            }
                        )
                        failed_reasons[reason] = failed_reasons.get(reason, 0) + 1
                now = time.time()
                if now - last_log >= 2:
                    elapsed = max(now - start_ts, 1.0)
                    speed = idx / elapsed
                    remaining = max(total - min(idx, total), 0)
                    eta = int(remaining / speed) if speed > 0 else 0
                    self.log_signal.emit(
                        f"📊 Tick进度: {min(idx, total)}/{total} | 成功: {success_count} | 失败: {failed_count} | 速度: {speed:.2f}/s | ETA: {eta}s"
                    )
                    last_log = now
            interface.close()
            failed_path = None
            if failed_samples:
                failed_path = self._export_failed_samples(failed_samples, "tick")
                self.log_signal.emit(f"📄 Tick失败样本已导出: {failed_path}")
            if failed_reasons:
                self.log_signal.emit(f"🧾 Tick失败原因统计: {failed_reasons}")
            result = {
                'total': total,
                'success': success_count,
                'failed': failed_count,
                'task_type': 'download_tick',
                'failed_samples_path': failed_path,
                'failed_reasons': failed_reasons
            }
            self.finished_signal.emit(result)
        except Exception as e:
            import traceback
            error_msg = f"下载Tick失败: {str(e)}\n{traceback.format_exc()}"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)

    def _download_transaction(self):
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface
            factor_platform_path = Path(__file__).parents[2] / "101因子" / "101因子分析平台" / "src"
            if str(factor_platform_path) not in sys.path:
                sys.path.insert(0, str(factor_platform_path))
            from data_manager import LocalDataManager
            manager = LocalDataManager()
            if not self.symbols:
                self.log_signal.emit("📊 正在获取A股列表...")
                self.symbols = manager.get_all_stocks_list(
                    include_st=True,
                    include_sz=True,
                    include_bj=True,
                    exclude_st=True,
                    exclude_delisted=True
                )
                self.log_signal.emit(f"✅ 获取到 {len(self.symbols)} 只A股")
            queue = deque([(symbol, 0) for symbol in self.symbols])
            total = len(self.symbols)
            success_count = 0
            failed_count = 0
            failed_samples: list[dict[str, Any]] = []
            failed_reasons: dict[str, int] = {}
            max_retries = 2
            interface = UnifiedDataInterface(duckdb_path=_get_duckdb_path(), silent_init=True)
            interface.connect(read_only=False)
            idx = 0
            start_ts = time.time()
            last_log = start_ts
            while queue:
                if not self._is_running:
                    break
                symbol, attempt = queue.popleft()
                idx += 1
                self.progress_signal.emit(min(idx, total), total)
                reason = None
                fallback = False
                try:
                    ok, fallback = interface.ingest_transaction_data(
                        symbol, self.start_date, self.end_date, aggregate_1m=True
                    )
                    if ok:
                        success_count += 1
                        if fallback:
                            self.log_signal.emit(f"⚠️ Level2不可用，已降级为tick转换: {symbol}")
                    else:
                        reason = "empty_or_unavailable"
                except Exception as e:
                    reason = f"exception:{str(e)[:60]}"
                if reason:
                    if attempt < max_retries:
                        queue.append((symbol, attempt + 1))
                        self.log_signal.emit(f"🔁 分笔重试 {symbol} (第{attempt + 1}次)")
                    else:
                        failed_count += 1
                        failed_samples.append(
                            {
                                "stock_code": symbol,
                                "reason": reason,
                                "attempt": attempt + 1,
                                "start_date": self.start_date,
                                "end_date": self.end_date,
                                "task_type": "download_transaction",
                                "fallback_used": fallback,
                            }
                        )
                        failed_reasons[reason] = failed_reasons.get(reason, 0) + 1
                now = time.time()
                if now - last_log >= 2:
                    elapsed = max(now - start_ts, 1.0)
                    speed = idx / elapsed
                    remaining = max(total - min(idx, total), 0)
                    eta = int(remaining / speed) if speed > 0 else 0
                    self.log_signal.emit(
                        f"📊 分笔进度: {min(idx, total)}/{total} | 成功: {success_count} | 失败: {failed_count} | 速度: {speed:.2f}/s | ETA: {eta}s"
                    )
                    last_log = now
            interface.close()
            failed_path = None
            if failed_samples:
                failed_path = self._export_failed_samples(failed_samples, "transaction")
                self.log_signal.emit(f"📄 分笔失败样本已导出: {failed_path}")
            if failed_reasons:
                self.log_signal.emit(f"🧾 分笔失败原因统计: {failed_reasons}")
            result = {
                'total': total,
                'success': success_count,
                'failed': failed_count,
                'task_type': 'download_transaction',
                'failed_samples_path': failed_path,
                'failed_reasons': failed_reasons
            }
            self.finished_signal.emit(result)
        except Exception as e:
            import traceback
            error_msg = f"下载分笔成交失败: {str(e)}\n{traceback.format_exc()}"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)

    def _export_failed_samples(self, samples: list[dict[str, Any]], tag: str) -> Optional[str]:
        try:
            if not samples:
                return None
            log_dir = os.path.join(Path(__file__).parents[2], "logs")
            os.makedirs(log_dir, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(log_dir, f"failed_{tag}_{ts}.csv")
            pd.DataFrame(samples).to_csv(file_path, index=False, encoding="utf-8-sig")
            return file_path
        except Exception:
            return None

    def _get_latest_failed_csv(self) -> Optional[str]:
        try:
            log_dir = os.path.join(Path(__file__).parents[2], "logs")
            if not os.path.exists(log_dir):
                return None
            candidates = []
            for name in os.listdir(log_dir):
                if name.startswith("failed_") and name.endswith(".csv"):
                    candidates.append(os.path.join(log_dir, name))
            if not candidates:
                return None
            candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            return candidates[0]
        except Exception:
            return None

    def _parse_task_type_from_path(self, path: str) -> str:
        base = os.path.basename(path)
        if "transaction" in base:
            return "download_transaction"
        if "tick" in base:
            return "download_tick"
        return "download_tick"

    def _build_symbols_from_csv(self, file_path: str) -> list[str]:
        try:
            df = pd.read_csv(file_path)
            if "stock_code" in df.columns:
                return [str(code).strip() for code in df["stock_code"].dropna().unique().tolist()]
        except Exception:
            pass
        return []

    def _update_data(self):
        """更新数据（增量）- 使用DuckDB存储，批量处理避免连接冲突"""
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface
            get_db_manager = _import_duckdb_manager()

            self.log_signal.emit("✅ 数据管理器初始化成功")

            # 获取DuckDB管理器
            manager = get_db_manager(_get_duckdb_path())
            interface = UnifiedDataInterface(duckdb_path=_get_duckdb_path(), silent_init=True)
            interface.connect(read_only=False)

            # 查找需要更新的股票（落后超过0天，包括今天的数据）
            # 说明：落后0天表示今天的数据可能还没收盘，落后1天表示昨天数据缺失
            query = """
                SELECT
                    stock_code,
                    MAX(date) as latest_date,
                    DATEDIFF('day', MAX(date), CURRENT_DATE) as days_behind
                FROM stock_daily
                GROUP BY stock_code
                HAVING DATEDIFF('day', MAX(date), CURRENT_DATE) > 0
                ORDER BY days_behind DESC
            """

            df_stocks = manager.execute_read_query(query)

            if df_stocks.empty:
                self.log_signal.emit("✅ 所有数据都是最新的，无需更新")
                self.finished_signal.emit({'total': 0, 'success': 0, 'failed': 0, 'task_type': 'update_data'})
                return

            stock_codes = df_stocks['stock_code'].tolist()
            self.log_signal.emit(f"📊 发现 {len(stock_codes)} 只股票需要更新")

            total = len(stock_codes)
            success_count = 0
            failed_count = 0
            skipped_count = 0
            failed_list = []

            for i, stock_code in enumerate(stock_codes):
                if not self._is_running:
                    self.log_signal.emit("⚠️ 用户中断更新")
                    break

                try:
                    self.progress_signal.emit(i + 1, total)

                    # 进度显示
                    if (i + 1) % 100 == 0 or i == 0:
                        self.log_signal.emit(f"  📈 进度: {i+1}/{total} ({(i+1)/total*100:.1f}%)")

                    stock_data = df_stocks[df_stocks['stock_code'] == stock_code].iloc[0]
                    latest_date = pd.to_datetime(stock_data['latest_date'], errors='coerce')
                    if latest_date is None or pd.isna(latest_date):
                        skipped_count += 1
                        continue
                    start_date = latest_date.strftime('%Y-%m-%d')
                    end_date = datetime.now().strftime('%Y-%m-%d')
                    plan = interface.build_incremental_plan(
                        stock_code=stock_code,
                        start_date=start_date,
                        end_date=end_date,
                        period='1d'
                    )
                    if not plan:
                        skipped_count += 1
                        continue
                    total_records = 0
                    for item in plan:
                        mode = item.get("mode")
                        if mode == "skip":
                            continue
                        df = interface.get_stock_data(
                            stock_code=stock_code,
                            start_date=item.get("start_date", start_date),
                            end_date=item.get("end_date", end_date),
                            period='1d',
                            auto_save=True
                        )
                        if df is not None and not df.empty:
                            total_records += len(df)
                    if total_records > 0:
                        success_count += 1
                    else:
                        skipped_count += 1

                except Exception as e:
                    self.log_signal.emit(f"  [{i+1}/{total}] {stock_code}: ✗ 错误 - {str(e)[:50]}")
                    failed_count += 1
                    failed_list.append(f"{stock_code} - {str(e)[:30]}")

            # 输出结果
            result = {
                'total': total,
                'success': success_count,
                'failed': failed_count,
                'skipped': skipped_count,
                'failed_list': failed_list,
                'task_type': 'update_data'
            }

            self.finished_signal.emit(result)
            self.log_signal.emit(f"✅ 更新完成! 总数: {total}, 成功: {success_count}, 跳过: {skipped_count}, 失败: {failed_count}")

            # 输出失败清单
            if failed_list:
                self.log_signal.emit("")
                self.log_signal.emit("=" * 70)
                self.log_signal.emit("  失败清单:")
                for failed_item in failed_list[:20]:  # 只显示前20个
                    self.log_signal.emit(f"    ✗ {failed_item}")
                if len(failed_list) > 20:
                    self.log_signal.emit(f"    ... 还有 {len(failed_list) - 20} 只")
                self.log_signal.emit("=" * 70)

        except ImportError as e:
            error_msg = f"导入模块失败: {str(e)}\n请确保 data_manager.duckdb_connection_pool 模块可用"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)
        except Exception as e:
            import traceback
            error_msg = f"更新数据失败: {str(e)}\n{traceback.format_exc()}"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)

    def _backfill_history(self):
        """补充历史数据（从2018年开始）"""
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface
            get_db_manager = _import_duckdb_manager()

            self.log_signal.emit("✅ 数据管理器初始化成功")

            # 获取DuckDB管理器
            manager = get_db_manager(_get_duckdb_path())
            interface = UnifiedDataInterface(duckdb_path=_get_duckdb_path(), silent_init=True)
            interface.connect(read_only=False)

            # 查询所有股票及其最早日期
            query = """
                SELECT
                    stock_code,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date
                FROM stock_daily
                GROUP BY stock_code
                ORDER BY stock_code
            """

            df_stocks = manager.execute_read_query(query)

            if df_stocks.empty:
                self.log_signal.emit("⚠️ 数据库中没有数据，请先下载A股数据")
                self.finished_signal.emit({'total': 0, 'success': 0, 'failed': 0, 'task_type': 'backfill_history'})
                return

            # 筛选需要补充历史的股票（最早日期晚于2018-06-01）
            cutoff_date = pd.to_datetime('2018-06-01')
            df_stocks['earliest_date'] = pd.to_datetime(df_stocks['earliest_date'], errors='coerce')
            df_stocks = df_stocks[df_stocks['earliest_date'].notna()]
            needs_backfill = df_stocks[df_stocks['earliest_date'] > cutoff_date]

            if needs_backfill.empty:
                self.log_signal.emit("✅ 所有股票都有完整历史数据")
                self.finished_signal.emit({'total': 0, 'success': 0, 'failed': 0, 'task_type': 'backfill_history'})
                return

            stock_codes = needs_backfill['stock_code'].tolist()
            self.log_signal.emit(f"📊 发现 {len(stock_codes)} 只股票需要补充历史数据")

            total = len(stock_codes)
            success_count = 0
            failed_count = 0
            failed_list = []

            for i, stock_code in enumerate(stock_codes):
                try:
                    # 进度显示
                    if (i + 1) % 100 == 0:
                        self.log_signal.emit(f"📊 进度: {i+1}/{total} ({(i+1)/total*100:.1f}%)")

                    row = needs_backfill[needs_backfill['stock_code'] == stock_code].iloc[0]
                    earliest_date = pd.to_datetime(row['earliest_date'], errors='coerce')
                    if earliest_date is None or pd.isna(earliest_date):
                        failed_count += 1
                        failed_list.append(f"{stock_code} - earliest_date无效")
                        continue
                    start_date = cutoff_date.strftime('%Y-%m-%d')
                    end_date = earliest_date.strftime('%Y-%m-%d')
                    plan = interface.build_incremental_plan(
                        stock_code=stock_code,
                        start_date=start_date,
                        end_date=end_date,
                        period='1d'
                    )
                    if not plan:
                        continue
                    total_records = 0
                    for item in plan:
                        mode = item.get("mode")
                        if mode == "skip":
                            continue
                        df = interface.get_stock_data(
                            stock_code=stock_code,
                            start_date=item.get("start_date", start_date),
                            end_date=item.get("end_date", end_date),
                            period='1d',
                            auto_save=True
                        )
                        if df is not None and not df.empty:
                            total_records += len(df)
                    if total_records > 0:
                        success_count += 1
                    else:
                        failed_count += 1
                        failed_list.append(f"{stock_code} - 数据为空")

                except Exception as e:
                    self.log_signal.emit(f"  [{i+1}/{total}] {stock_code}: ✗ 错误 - {str(e)[:50]}")
                    failed_count += 1
                    failed_list.append(f"{stock_code} - {str(e)[:30]}")

            self.log_signal.emit(f"📥 历史数据收集完成: {success_count} 只股票成功")

            result = {
                'total': total,
                'success': success_count,
                'failed': failed_count,
                'failed_list': failed_list,
                'task_type': 'backfill_history'
            }

            self.finished_signal.emit(result)
            self.log_signal.emit(f"✅ 历史数据补充完成! 总数: {total}, 成功: {success_count}, 失败: {failed_count}")

            # 输出失败清单
            if failed_list:
                self.log_signal.emit("")
                self.log_signal.emit("=" * 70)
                self.log_signal.emit("  失败清单:")
                for failed_item in failed_list:
                    self.log_signal.emit(f"    ✗ {failed_item}")
                self.log_signal.emit("=" * 70)

        except Exception as e:
            import traceback
            error_msg = f"补充历史数据失败: {str(e)}\n{traceback.format_exc()}"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)

    def stop(self):
        """停止下载"""
        self._is_running = False
        self.quit()
        # Fix 58: 不在主线程 wait(3000)，让线程自行退出后由 Qt 清理


class SingleStockDownloadThread(QThread):
    """单个标的下载线程"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)  # current, total
    finished_signal = pyqtSignal(dict)  # {'success': bool, 'symbol': str, 'record_count': int, 'file_size': float}
    error_signal = pyqtSignal(str)

    def __init__(self, stock_code, start_date, end_date, period='1d'):
        super().__init__()
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        self.period = period  # '1d', '1m', '5m', '15m', '30m', '60m', 'tick'
        self._is_running = True

    def run(self):
        """运行下载任务"""
        try:
            from xtquant import xtdata

            # 检查DuckDB管理器是否可用
            try:
                get_db_manager = _import_duckdb_manager()
                manager = get_db_manager(_get_duckdb_path())
                self.log_signal.emit("[OK] 数据管理器初始化成功")
            except ImportError:
                self.error_signal.emit("DuckDB管理器不可用，请确保data_manager.duckdb_connection_pool模块存在")
                return
            except Exception as e:
                self.error_signal.emit(f"DuckDB管理器初始化失败: {e}")
                return

            self.log_signal.emit(f"[INFO] 正在下载 {self.stock_code}...")
            self.log_signal.emit(f"   数据周期: {self.period}")
            self.log_signal.emit(f"   日期范围: {self.start_date} ~ {self.end_date}")

            # 转换日期格式
            start_dt = datetime.strptime(self.start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(self.end_date, '%Y-%m-%d')
            start_str = start_dt.strftime('%Y%m%d')
            end_str = end_dt.strftime('%Y%m%d')

            # 映射周期到QMT API格式
            period_map = {
                '1d': '1d',
                '1m': '1m',
                '5m': '5m',
                '15m': '15m',
                '30m': '30m',
                '60m': '60m',
                'tick': 'tick',
                'l2transaction': 'l2transaction'
            }
            qmt_period = period_map.get(self.period, '1d')

            # 下载数据
            # 统一使用get_market_data_ex获取数据（支持日线和分钟线）
            # 计算需要获取的数据条数
            start_dt = datetime.strptime(self.start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(self.end_date, '%Y-%m-%d')
            days_diff = (end_dt - start_dt).days + 1

            if self.period == '1d':
                # 日线：直接获取天数，加20天缓冲
                count = max(days_diff + 20, 30)
                self.log_signal.emit(f"📡 正在从QMT获取日线数据（约{days_diff}个交易日）...")
            elif self.period == 'tick':
                # tick数据：需要先下载历史数据
                self.log_signal.emit("📥 正在下载tick历史数据...")
                try:
                    # 对于tick数据，需要先使用download_history_data下载
                    # 注意：tick数据下载需要指定到秒
                    start_time_str = start_dt.strftime('%Y%m%d') + "000000"
                    end_time_str = end_dt.strftime('%Y%m%d') + "235959"

                    # 调用下载函数
                    xtdata.download_history_data(
                        stock_code=self.stock_code,
                        period='tick',
                        start_time=start_time_str,
                        end_time=end_time_str
                    )
                    self.log_signal.emit("✓ tick数据下载完成")
                except Exception as e:
                    self.log_signal.emit(f"⚠ tick数据下载警告: {str(e)}")
                    self.log_signal.emit("  继续尝试读取本地数据...")

                # 下载后尝试读取，设置较大的count
                count = 100000
                self.log_signal.emit("📡 正在读取已下载的tick数据...")
            elif self.period == 'l2transaction':
                count = 100000
                self.log_signal.emit("📡 正在读取分笔成交数据...")
            else:
                # 分钟线：估算每天的条数
                if self.period == '1m':
                    count_per_day = 240  # 4小时 * 60分钟
                elif self.period == '5m':
                    count_per_day = 48
                elif self.period == '15m':
                    count_per_day = 16
                elif self.period == '30m':
                    count_per_day = 8
                else:  # 60m
                    count_per_day = 4

                count = days_diff * count_per_day
                # 限制最大条数，避免数据量过大
                count = min(count, 50000)
                self.log_signal.emit(f"📡 正在从QMT获取{self.period}分钟线数据（最多{count}条）...")

            # 使用count参数获取数据（QMT API支持的方式）
            if self.period == 'tick':
                # tick数据需要指定字段列表
                data = xtdata.get_market_data_ex(
                    field_list=['time', 'lastPrice', 'volume', 'amount', 'func_type', 'openInt'],
                    stock_list=[self.stock_code],
                    period=qmt_period,
                    start_time=start_str,
                    end_time=end_str,
                    count=count
                )
            elif self.period == 'l2transaction':
                data = xtdata.get_market_data_ex(
                    field_list=['time', 'price', 'volume', 'amount', 'bsflag', 'trade_id'],
                    stock_list=[self.stock_code],
                    period=qmt_period,
                    start_time=start_str,
                    end_time=end_str,
                    count=count
                )
            else:
                data = xtdata.get_market_data_ex(
                    stock_list=[self.stock_code],
                    period=qmt_period,
                    count=count
                )

            if isinstance(data, dict) and self.stock_code in data:
                df = data[self.stock_code]
                if df.empty:
                    if self.period == 'l2transaction':
                        self.log_signal.emit("⚠️ Level2分笔为空，尝试降级为tick转换")
                        data = xtdata.get_market_data_ex(
                            field_list=['time', 'lastPrice', 'volume', 'amount', 'func_type', 'openInt'],
                            stock_list=[self.stock_code],
                            period='tick',
                            start_time=start_str,
                            end_time=end_str,
                            count=count
                        )
                        if isinstance(data, dict) and self.stock_code in data:
                            df = data[self.stock_code]
                    if df.empty:
                        self.error_signal.emit(f"没有获取到 {self.stock_code} 的数据，请检查代码和日期范围")
                        return
            else:
                self.error_signal.emit(f"没有获取到 {self.stock_code} 的数据，请检查代码和日期范围")
                return

            # 根据日期范围过滤数据
            self.log_signal.emit("🔍 正在过滤日期范围...")
            df['datetime'] = pd.to_datetime(df['time'], unit='ms')

            if self.period == '1d':
                # 日线：只保留日期范围内的数据
                df = df[(df['datetime'] >= start_dt) & (df['datetime'] <= end_dt)]
            else:
                # 分钟线/tick：只保留日期范围内的数据（精确到分钟/秒）
                # 使用当天的23:59:59作为结束时间
                end_dt_dt = datetime.combine(end_dt, dt_time(23, 59, 59))
                df = df[(df['datetime'] >= start_dt) & (df['datetime'] <= end_dt_dt)]

            if df.empty:
                self.error_signal.emit("在指定日期范围内没有数据，请检查日期设置")
                return

            record_count = len(df)
            self.log_signal.emit(f"📊 获取到 {record_count} 条数据")

            # 转换数据格式
            self.log_signal.emit("💾 正在保存到DuckDB...")

            # 转换为标准格式
            if self.period == 'l2transaction':
                time_series = pd.to_datetime(df['time'], unit='ms')
                price_col = 'price' if 'price' in df.columns else 'lastPrice'
                bs_col = 'bsflag' if 'bsflag' in df.columns else 'bsFlag' if 'bsFlag' in df.columns else None
                trade_col = 'trade_id' if 'trade_id' in df.columns else 'tradeId' if 'tradeId' in df.columns else None
                df_processed = pd.DataFrame({
                    'stock_code': self.stock_code,
                    'symbol_type': 'stock' if (self.stock_code.startswith('0') or self.stock_code.startswith('3') or self.stock_code.startswith('6')) else 'etf',
                    'datetime': time_series,
                    'period': 'tick',
                    'price': df[price_col] if price_col in df.columns else 0,
                    'volume': df['volume'].astype('int64') if 'volume' in df.columns else 0,
                    'amount': df['amount'] if 'amount' in df.columns else 0,
                    'bs_flag': df[bs_col] if bs_col else None,
                    'trade_id': df[trade_col] if trade_col else 0,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                })
                table_name = 'stock_transaction'
                with manager.get_write_connection() as con:
                    con.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            stock_code VARCHAR(20),
                            symbol_type VARCHAR(10),
                            datetime TIMESTAMP,
                            period VARCHAR(10),
                            price DOUBLE,
                            volume BIGINT,
                            amount DOUBLE,
                            bs_flag VARCHAR,
                            trade_id BIGINT,
                            created_at TIMESTAMP,
                            updated_at TIMESTAMP
                        )
                    """)
                with manager.get_write_connection() as con:
                    con.register('temp_data', df_processed)
                    con.execute(
                        "DELETE FROM stock_transaction WHERE stock_code = ? AND datetime >= ? AND datetime <= ?",
                        [self.stock_code, str(start_dt), str(end_dt)],
                    )
                    con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_data")  # noqa: S608
                    con.unregister('temp_data')
                try:
                    series = df_processed.set_index('datetime')['price'].astype(float)
                    ohlc = series.resample('1min').ohlc()
                    vol = df_processed.set_index('datetime')['volume'].resample('1min').sum()
                    amt = df_processed.set_index('datetime')['amount'].resample('1min').sum()
                    bars = pd.concat([ohlc, vol.rename('volume'), amt.rename('amount')], axis=1).dropna(how='all')
                    bars = bars.reset_index().rename(columns={'datetime': 'datetime'})
                    bars['stock_code'] = self.stock_code
                    bars['symbol_type'] = 'stock' if (self.stock_code.startswith('0') or self.stock_code.startswith('3') or self.stock_code.startswith('6')) else 'etf'
                    bars['period'] = '1m'
                    bars['adjust_type'] = 'none'
                    bars['factor'] = 1.0
                    bars['created_at'] = datetime.now()
                    bars['updated_at'] = datetime.now()
                    with manager.get_write_connection() as con:
                        con.execute("""
                            CREATE TABLE IF NOT EXISTS stock_1m (
                                stock_code VARCHAR(20),
                                symbol_type VARCHAR(10),
                                datetime TIMESTAMP,
                                period VARCHAR(10),
                                open DOUBLE,
                                high DOUBLE,
                                low DOUBLE,
                                close DOUBLE,
                                volume BIGINT,
                                amount DOUBLE,
                                adjust_type VARCHAR,
                                factor DOUBLE,
                                created_at TIMESTAMP,
                                updated_at TIMESTAMP
                            )
                        """)
                        columns = _get_table_columns(con, 'stock_1m')
                        df_aligned = _align_dataframe_to_columns(bars, columns)
                        con.register('temp_1m', df_aligned)
                        con.execute(
                            "DELETE FROM stock_1m WHERE stock_code = ? AND datetime >= ? AND datetime <= ?",
                            [self.stock_code, str(start_dt), str(end_dt_dt)],
                        )
                        cols_sql = ", ".join(columns)
                        con.execute(f"INSERT INTO stock_1m ({cols_sql}) SELECT {cols_sql} FROM temp_1m")  # noqa: S608
                        con.unregister('temp_1m')
                    self.log_signal.emit("✅ 分笔聚合1m写入完成")
                except Exception as e:
                    self.log_signal.emit(f"⚠️ 分笔聚合1m失败: {str(e)[:60]}")
                result = {
                    'success': True,
                    'symbol': self.stock_code,
                    'record_count': len(df_processed),
                    'file_size': len(df_processed) * 0.0001
                }
                self.finished_signal.emit(result)
                self.log_signal.emit(f"[OK] {self.stock_code} 下载完成!")
                return
            if self.period == 'tick':
                # tick数据处理（字段结构不同）
                time_series = pd.to_datetime(df['time'], unit='ms')

                df_processed = pd.DataFrame({
                    'stock_code': self.stock_code,
                    'symbol_type': 'stock' if (self.stock_code.startswith('0') or self.stock_code.startswith('3') or self.stock_code.startswith('6')) else 'etf',
                    'datetime': time_series,
                    'period': 'tick',
                    'lastPrice': df['lastPrice'] if 'lastPrice' in df.columns else 0,
                    'volume': df['volume'].astype('int64') if 'volume' in df.columns else 0,
                    'amount': df['amount'] if 'amount' in df.columns else 0,
                    'func_type': df['func_type'] if 'func_type' in df.columns else 0,
                    'openInt': df['openInt'] if 'openInt' in df.columns else 0,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                })

                table_name = 'stock_tick'

                # 确保stock_tick表存在
                with manager.get_write_connection() as con:
                    con.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            stock_code VARCHAR(20),
                            symbol_type VARCHAR(10),
                            datetime TIMESTAMP,
                            period VARCHAR(10),
                            lastPrice DOUBLE,
                            volume BIGINT,
                            amount DOUBLE,
                            func_type INTEGER,
                            openInt DOUBLE,
                            created_at TIMESTAMP,
                            updated_at TIMESTAMP
                        )
                    """)

                # 保存tick数据
                with manager.get_write_connection() as con:
                    con.register('temp_data', df_processed)
                    # 删除该股票在日期范围内的旧数据
                    con.execute(
                        "DELETE FROM stock_tick WHERE stock_code = ? AND datetime >= ? AND datetime <= ?",
                        [self.stock_code, str(start_dt), str(end_dt)],
                    )
                    # 插入新数据
                    con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_data")  # noqa: S608
                    con.unregister('temp_data')

                self.log_signal.emit(f"✅ 已保存 {len(df_processed)} 条tick记录到DuckDB")

                try:
                    series = df_processed.set_index('datetime')['lastPrice'].astype(float)
                    ohlc = series.resample('1min').ohlc()
                    vol = df_processed.set_index('datetime')['volume'].resample('1min').sum()
                    amt = df_processed.set_index('datetime')['amount'].resample('1min').sum()
                    bars = pd.concat([ohlc, vol.rename('volume'), amt.rename('amount')], axis=1).dropna(how='all')
                    bars = bars.reset_index().rename(columns={'datetime': 'datetime'})
                    bars['stock_code'] = self.stock_code
                    bars['symbol_type'] = 'stock' if (self.stock_code.startswith('0') or self.stock_code.startswith('3') or self.stock_code.startswith('6')) else 'etf'
                    bars['period'] = '1m'
                    bars['adjust_type'] = 'none'
                    bars['factor'] = 1.0
                    bars['created_at'] = datetime.now()
                    bars['updated_at'] = datetime.now()
                    with manager.get_write_connection() as con:
                        con.execute("""
                            CREATE TABLE IF NOT EXISTS stock_1m (
                                stock_code VARCHAR(20),
                                symbol_type VARCHAR(10),
                                datetime TIMESTAMP,
                                period VARCHAR(10),
                                open DOUBLE,
                                high DOUBLE,
                                low DOUBLE,
                                close DOUBLE,
                                volume BIGINT,
                                amount DOUBLE,
                                adjust_type VARCHAR,
                                factor DOUBLE,
                                created_at TIMESTAMP,
                                updated_at TIMESTAMP
                            )
                        """)
                        columns = _get_table_columns(con, 'stock_1m')
                        df_aligned = _align_dataframe_to_columns(bars, columns)
                        con.register('temp_1m', df_aligned)
                        con.execute(
                            "DELETE FROM stock_1m WHERE stock_code = ? AND datetime >= ? AND datetime <= ?",
                            [self.stock_code, str(start_dt), str(end_dt_dt)],
                        )
                        cols_sql = ", ".join(columns)
                        con.execute(f"INSERT INTO stock_1m ({cols_sql}) SELECT {cols_sql} FROM temp_1m")  # noqa: S608
                        con.unregister('temp_1m')
                    self.log_signal.emit("✅ Tick聚合1m写入完成")
                except Exception as e:
                    self.log_signal.emit(f"⚠️ Tick聚合1m失败: {str(e)[:60]}")

                result = {
                    'success': True,
                    'symbol': self.stock_code,
                    'record_count': len(df_processed),
                    'file_size': len(df_processed) * 0.0001
                }

                self.finished_signal.emit(result)
                self.log_signal.emit(f"[OK] {self.stock_code} 下载完成!")
                return

            if 'time' in df.columns:
                # QMT返回的数据格式
                # 日线：使用DATE类型（字符串YYYY-MM-DD）
                # 分钟线：使用TIMESTAMP类型（直接保存datetime对象）
                time_series = pd.to_datetime(df['time'], unit='ms')
                if self.period == '1d':
                    date_series = time_series.dt.strftime('%Y-%m-%d')
                    time_field = {'date': date_series}
                else:
                    time_field = {'datetime': time_series}

                df_processed = pd.DataFrame({
                    'stock_code': self.stock_code,
                    'symbol_type': 'stock' if (self.stock_code.startswith('0') or self.stock_code.startswith('3') or self.stock_code.startswith('6')) else 'etf',
                    'period': self.period,
                    'open': df['open'],
                    'high': df['high'],
                    'low': df['low'],
                    'close': df['close'],
                    'volume': df['volume'].astype('int64') if 'volume' in df.columns else 0,
                    'amount': df['amount'] if 'amount' in df.columns else 0,
                    'adjust_type': 'none',
                    'factor': 1.0,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                })
                df_processed = pd.concat([df_processed, pd.DataFrame(time_field)], axis=1)

                # 添加复权列（全部使用原始价格）
                for col in ['open', 'high', 'low', 'close']:
                    df_processed[f'{col}_front'] = df_processed[col]
                    df_processed[f'{col}_back'] = df_processed[col]
                    df_processed[f'{col}_geometric_front'] = df_processed[col]
                    df_processed[f'{col}_geometric_back'] = df_processed[col]

                # 保存到DuckDB
                if self.period == '1d':
                    table_name = 'stock_daily'
                else:
                    if self.period not in _VALID_KLINE_PERIODS:
                        raise ValueError(f"不支持的K线周期: {self.period!r}")
                    table_name = f'stock_{self.period}'

                with manager.get_write_connection() as con:
                    if self.period != '1d':
                        con.execute(f"""
                            CREATE TABLE IF NOT EXISTS {table_name} (
                                stock_code VARCHAR NOT NULL,
                                symbol_type VARCHAR NOT NULL,
                                datetime TIMESTAMP NOT NULL,
                                period VARCHAR NOT NULL,
                                open DECIMAL(18, 6),
                                high DECIMAL(18, 6),
                                low DECIMAL(18, 6),
                                close DECIMAL(18, 6),
                                volume BIGINT,
                                amount DECIMAL(18, 6),
                                adjust_type VARCHAR DEFAULT 'none',
                                factor DECIMAL(18, 6) DEFAULT 1.0,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                PRIMARY KEY (stock_code, datetime, period, adjust_type)
                            )
                        """)
                    columns = _get_table_columns(con, table_name)
                    if not columns:
                        raise ValueError(f"数据表不存在或字段为空: {table_name}")
                    df_aligned = _align_dataframe_to_columns(df_processed, columns)
                    con.register('temp_data', df_aligned)
                    cols_sql = ", ".join(columns)
                    con.execute(f"DELETE FROM {table_name} WHERE stock_code = ?", [self.stock_code])  # noqa: S608
                    con.execute(f"INSERT INTO {table_name} ({cols_sql}) SELECT {cols_sql} FROM temp_data")  # noqa: S608
                    con.unregister('temp_data')

                self.log_signal.emit(f"✅ 已保存 {len(df_processed)} 条记录到DuckDB")

                result = {
                    'success': True,
                    'symbol': self.stock_code,
                    'record_count': len(df_processed),
                    'file_size': len(df_processed) * 0.0001  # 估算
                }

                self.finished_signal.emit(result)
                self.log_signal.emit(f"[OK] {self.stock_code} 下载完成!")

            else:
                self.error_signal.emit("数据格式不正确")

        except Exception as e:
            import traceback
            error_msg = f"[ERROR] 下载失败: {str(e)}\n{traceback.format_exc()}"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)

    def stop(self):
        """停止下载"""
        self._is_running = False
        self.quit()
        # Fix 58: 不在主线程 wait(3000)，让线程自行退出后由 Qt 清理


class VerifyDataThread(QThread):
    """验证数据完整性线程"""
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(dict)

    def __init__(self, stock_code):
        super().__init__()
        self.stock_code = stock_code

    def run(self):
        """运行验证任务"""
        try:
            _ensure_duckdb_tables()
            get_db_manager = _import_duckdb_manager()
            manager = get_db_manager(_get_duckdb_path())
            with manager.get_read_connection() as con:
                # 检查1分钟数据
                has_1min = False
                records_1min = 0
                start_1min = ''
                end_1min = ''
                try:
                    result = con.execute(
                        "SELECT COUNT(*) as cnt, MIN(date) as start_date, MAX(date) as end_date"
                        " FROM stock_1m WHERE stock_code = ?",
                        [self.stock_code],
                    ).fetchone()
                    if result and result[0] > 0:
                        has_1min = True
                        records_1min = result[0]
                        start_1min = str(result[1]) if result[1] else ''
                        end_1min = str(result[2]) if result[2] else ''
                        self.log_signal.emit(f"✓ 1分钟数据: {records_1min:,} 条 ({start_1min} ~ {end_1min})")
                except Exception:
                    pass

                # 检查日线数据
                has_daily = False
                records_daily = 0
                start_daily = ''
                end_daily = ''
                try:
                    result = con.execute(
                        "SELECT COUNT(*) as cnt, MIN(date) as start_date, MAX(date) as end_date"
                        " FROM stock_daily WHERE stock_code = ?",
                        [self.stock_code],
                    ).fetchone()
                    if result and result[0] > 0:
                        has_daily = True
                        records_daily = result[0]
                        start_daily = str(result[1]) if result[1] else ''
                        end_daily = str(result[2]) if result[2] else ''
                        self.log_signal.emit(f"✓ 日线数据: {records_daily:,} 条 ({start_daily} ~ {end_daily})")
                except Exception:
                    pass

                # 检查tick数据
                has_tick = False
                records_tick = 0
                start_tick = ''
                end_tick = ''
                try:
                    result = con.execute(
                        "SELECT COUNT(*) as cnt, MIN(datetime) as start_time, MAX(datetime) as end_time"
                        " FROM stock_tick WHERE stock_code = ?",
                        [self.stock_code],
                    ).fetchone()
                    if result and result[0] > 0:
                        has_tick = True
                        records_tick = result[0]
                        start_tick = str(result[1]) if result[1] else ''
                        end_tick = str(result[2]) if result[2] else ''
                        self.log_signal.emit(f"✓ Tick数据: {records_tick:,} 条 ({start_tick} ~ {end_tick})")
                except Exception:
                    pass

            result = {
                'stock': self.stock_code,
                'has_1min': has_1min,
                'has_daily': has_daily,
                'has_tick': has_tick,
                'records_1min': records_1min,
                'records_daily': records_daily,
                'records_tick': records_tick,
                'start_1min': start_1min,
                'end_1min': end_1min,
                'start_daily': start_daily,
                'end_daily': end_daily,
                'start_tick': start_tick,
                'end_tick': end_tick
            }

            self.finished_signal.emit(result)

        except Exception as e:
            self.log_signal.emit(f"✗ 验证失败: {e}")
            result = {
                'stock': self.stock_code,
                'has_1min': False,
                'has_daily': False,
                'records_1min': 0,
                'records_daily': 0,
                'start_1min': '',
                'end_1min': '',
                'start_daily': '',
                'end_daily': ''
            }
            self.finished_signal.emit(result)


class FinancialDataDownloadThread(QThread):
    """QMT财务数据下载线程"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)  # current, total
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, stock_list=None, table_list=None, start_time=None, end_time=None):
        super().__init__()
        # 默认下载常用股票
        self.stock_list = stock_list or ["000001.SZ", "600519.SH", "511380.SH", "512100.SH"]
        # 默认下载主要财务报表
        self.table_list = table_list or ["Balance", "Income", "CashFlow"]
        # 默认时间范围：最近3年
        from datetime import datetime
        if end_time is None:
            end_time = datetime.now()
        else:
            end_time = datetime.strptime(end_time, '%Y%m%d')

        if start_time is None:
            start_time = end_time - timedelta(days=365*3)  # 默认3年
        else:
            start_time = datetime.strptime(start_time, '%Y%m%d')

        self.start_time = start_time.strftime('%Y%m%d')
        self.end_time = end_time.strftime('%Y%m%d')
        self._is_running = True

    def run(self):
        """运行下载任务"""
        try:
            from xtquant import xtdata

            self.log_signal.emit("=" * 70)
            self.log_signal.emit("  【QMT财务数据下载】")
            self.log_signal.emit("=" * 70)

            # 步骤0: 过滤ETF和指数
            self.log_signal.emit("【步骤0】过滤ETF和指数")
            self.log_signal.emit("-" * 70)

            filtered_stock_list = []
            etf_count = 0
            index_count = 0
            stock_count = 0

            for stock_code in self.stock_list:
                try:
                    # 获取股票类型信息
                    type_info = xtdata.get_instrument_type(stock_code)

                    # 判断类型
                    if isinstance(type_info, dict):
                        if type_info.get('stock', False):
                            # 是股票
                            filtered_stock_list.append(stock_code)
                            stock_count += 1
                            self.log_signal.emit(f"[OK] {stock_code}: 股票")
                        elif type_info.get('etf', False) or type_info.get('fund', False):
                            # 是ETF或基金
                            etf_count += 1
                            self.log_signal.emit(f"[SKIP] {stock_code}: ETF/基金（无财务报表）")
                        elif type_info.get('index', False):
                            # 是指数
                            index_count += 1
                            self.log_signal.emit(f"[SKIP] {stock_code}: 指数（无财务报表）")
                        else:
                            # 未知类型，尝试下载
                            self.log_signal.emit(f"[INFO] {stock_code}: 类型未知，将尝试下载")
                            filtered_stock_list.append(stock_code)
                            stock_count += 1
                    else:
                        # 如果返回的不是字典，尝试下载
                        self.log_signal.emit(f"[INFO] {stock_code}: 类型={type_info}，将尝试下载")
                        filtered_stock_list.append(stock_code)
                        stock_count += 1

                except Exception:
                    # 如果获取类型失败，也尝试下载
                    self.log_signal.emit(f"[WARN] {stock_code}: 无法获取类型信息，将尝试下载")
                    filtered_stock_list.append(stock_code)
                    stock_count += 1

            self.log_signal.emit("")
            self.log_signal.emit(f"[统计] 原始数量: {len(self.stock_list)}")
            self.log_signal.emit(f"  - 股票: {stock_count} 只（将下载）")
            self.log_signal.emit(f"  - ETF/基金: {etf_count} 只（已跳过）")
            self.log_signal.emit(f"  - 指数: {index_count} 只（已跳过）")
            self.log_signal.emit("")

            if not filtered_stock_list:
                self.log_signal.emit("[INFO] 没有需要下载财务数据的股票")
                result = {
                    'total': len(self.stock_list),
                    'success': 0,
                    'failed': 0,
                    'skipped': len(self.stock_list),
                    'task_type': 'financial_data'
                }
                self.finished_signal.emit(result)
                return

            # 更新股票列表为过滤后的列表
            self.stock_list = filtered_stock_list
            total_stocks = len(self.stock_list)

            self.log_signal.emit(f"[INFO] 准备下载 {total_stocks} 只股票的财务数据")
            self.log_signal.emit(f"[INFO] 数据表: {', '.join(self.table_list)}")
            self.log_signal.emit(f"[INFO] 时间范围: {self.start_time} ~ {self.end_time}")
            self.log_signal.emit("")

            success_count = 0
            failed_count = 0
            failed_list = []  # 记录失败的股票及原因

            # 步骤1: 下载财务数据
            self.log_signal.emit("【步骤1】下载财务数据到QMT本地")
            self.log_signal.emit("-" * 70)

            try:
                self.log_signal.emit(f"[INFO] 正在下载 {self.stock_list} 的财务数据...")
                result = xtdata.download_financial_data(
                    stock_list=self.stock_list,
                    table_list=self.table_list
                )

                if result is None or result == '':
                    self.log_signal.emit("[OK] 财务数据下载完成")
                else:
                    self.log_signal.emit(f"[返回] {result}")

            except Exception as e:
                error_msg = f"[ERROR] 下载失败: {e}"
                self.log_signal.emit(error_msg)
                self.error_signal.emit(error_msg)
                return

            # 步骤2: 读取并验证数据
            self.log_signal.emit("")
            self.log_signal.emit("【步骤2】读取并验证财务数据")
            self.log_signal.emit("-" * 70)

            for i, stock_code in enumerate(self.stock_list):
                if not self._is_running:
                    self.log_signal.emit("[WARN] 用户中断下载")
                    break

                try:
                    self.progress_signal.emit(i + 1, total_stocks)
                    self.log_signal.emit(f"[{i+1}/{total_stocks}] {stock_code}:")

                    # 读取财务数据（添加时间范围参数）
                    result = xtdata.get_financial_data(
                        stock_list=[stock_code],
                        table_list=self.table_list,
                        start_time=self.start_time,
                        end_time=self.end_time,
                        report_type='report_time'
                    )

                    # 处理返回结果（可能是dict或DataFrame）
                    total_records = 0

                    if isinstance(result, dict):
                        # 字典格式：{stock_code: {table_name: data}}
                        if stock_code in result:
                            stock_data = result[stock_code]

                            for table_name in self.table_list:
                                if table_name in stock_data:
                                    table_data = stock_data[table_name]
                                    if isinstance(table_data, pd.DataFrame):
                                        record_count = len(table_data)
                                        total_records += record_count
                                        self.log_signal.emit(f"    [OK] {table_name}: {record_count} 条记录")
                                    elif isinstance(table_data, dict):
                                        record_count = len(table_data)
                                        total_records += record_count
                                        self.log_signal.emit(f"    [OK] {table_name}: {record_count} 条记录")
                                    elif isinstance(table_data, list):
                                        record_count = len(table_data)
                                        total_records += record_count
                                        self.log_signal.emit(f"    [OK] {table_name}: {record_count} 条记录")
                        else:
                            self.log_signal.emit(f"    [WARN] {stock_code} 不在返回结果中")

                    elif isinstance(result, pd.DataFrame):
                        # DataFrame格式：直接是数据
                        record_count = len(result)
                        total_records += record_count
                        self.log_signal.emit(f"    [OK] 财务数据: {record_count} 条记录")
                        self.log_signal.emit(f"    [INFO] 列: {list(result.columns)[:5]}...")

                    if total_records > 0:
                        success_count += 1
                        self.log_signal.emit(f"    [OK] 共 {total_records} 条财务数据")
                    else:
                        failed_count += 1
                        failed_list.append(f"{stock_code} - 数据为空")
                        self.log_signal.emit("    [WARN] 没有获取到财务数据")

                except Exception as e:
                    failed_count += 1
                    failed_list.append(f"{stock_code} - {str(e)[:50]}")
                    self.log_signal.emit(f"    [ERROR] {e}")
                    continue

            # 完成
            result = {
                'total': total_stocks,
                'success': success_count,
                'failed': failed_count,
                'failed_list': failed_list,
                'skipped': etf_count + index_count,
                'task_type': 'financial_data'
            }

            self.finished_signal.emit(result)

            self.log_signal.emit("")
            self.log_signal.emit("=" * 70)
            self.log_signal.emit("  下载完成!")
            self.log_signal.emit(f"  有效股票: {total_stocks} 只")
            self.log_signal.emit(f"  成功: {success_count} 只")
            self.log_signal.emit(f"  失败: {failed_count} 只")
            if etf_count + index_count > 0:
                self.log_signal.emit(f"  跳过: {etf_count + index_count} 只（ETF/指数无财务数据）")
            self.log_signal.emit("=" * 70)

        except ImportError:
            error_msg = "[ERROR] 导入xtquant失败，请确保QMT已安装并运行"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)
        except Exception as e:
            import traceback
            error_msg = f"[ERROR] 财务数据下载失败: {str(e)}\n{traceback.format_exc()}"
            self.log_signal.emit(error_msg)
            self.error_signal.emit(error_msg)

    def stop(self):
        """停止下载"""
        self._is_running = False
        self.quit()
        # Fix 58: 不在主线程 wait(3000)，让线程自行退出后由 Qt 清理


class LocalStatsLoadThread(QThread):
    """后台加载本地数据统计，避免阻塞UI线程"""

    result_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(self, duckdb_path: str):
        super().__init__()
        self.duckdb_path = duckdb_path

    def run(self):
        try:
            get_db_manager = _import_duckdb_manager()
            manager = get_db_manager(self.duckdb_path)

            def _table_exists(con, table_name: str) -> bool:
                try:
                    result = con.execute(
                        """
                        SELECT COUNT(*) FROM information_schema.tables
                        WHERE table_name = ?
                        """,
                        [table_name],
                    ).fetchone()
                    return bool(result and int(result[0]) > 0)
                except Exception:
                    return False

            with manager.get_read_connection() as con:
                has_daily = _table_exists(con, "stock_daily")
                has_symbol_type = False
                stats_daily = None
                if has_daily:
                    columns = _get_table_columns(con, "stock_daily")
                    has_symbol_type = "symbol_type" in columns
                    if has_symbol_type:
                        stats_daily = con.execute(
                            """
                            SELECT
                                COUNT(DISTINCT stock_code) as stock_count,
                                SUM(CASE WHEN symbol_type = 'stock' THEN 1 ELSE 0 END) as stock_only,
                                SUM(CASE WHEN symbol_type = 'etf' THEN 1 ELSE 0 END) as etf_count,
                                COUNT(*) as total_records,
                                MAX(date) as latest_date
                            FROM stock_daily
                            """
                        ).fetchone()
                    else:
                        stats_daily = con.execute(
                            """
                            SELECT
                                COUNT(DISTINCT stock_code) as stock_count,
                                COUNT(*) as total_records,
                                MAX(date) as latest_date
                            FROM stock_daily
                            """
                        ).fetchone()

                minute_tables = ["stock_1m", "stock_5m", "stock_15m", "stock_30m", "stock_60m"]
                minute_records = 0
                for table in minute_tables:
                    try:
                        if not _table_exists(con, table):
                            continue
                        result = con.execute(
                            f"""
                            SELECT COUNT(*) as records
                            FROM {table}
                            """  # noqa: S608
                        ).fetchone()
                        if result:
                            minute_records += int(result[0] or 0)
                    except Exception:
                        pass

            if has_symbol_type:
                total_symbols = stats_daily[0] if stats_daily else 0
                stock_count = stats_daily[1] if stats_daily else 0
                daily_records = stats_daily[3] if stats_daily else 0
                latest_date = str(stats_daily[4]) if stats_daily and stats_daily[4] else "N/A"
            else:
                total_symbols = stats_daily[0] if stats_daily else 0
                stock_count = total_symbols
                daily_records = stats_daily[1] if stats_daily else 0
                latest_date = str(stats_daily[2]) if stats_daily and stats_daily[2] else "N/A"

            total_records = daily_records + minute_records
            size_mb = total_records * 0.0001
            self.result_ready.emit(
                {
                    "total_symbols": int(total_symbols or 0),
                    "stock_count": int(stock_count or 0),
                    "total_bonds": 0,
                    "total_records": int(total_records or 0),
                    "size_mb": float(size_mb or 0.0),
                    "latest_date": latest_date,
                }
            )
        except Exception as e:
            self.error_occurred.emit(str(e))


class LocalDataManagerWidget(QWidget):
    """本地数据管理组件"""

    def __init__(self):
        super().__init__()
        self.download_thread = None
        self.duckdb_storage = None
        self.duckdb_con = None  # 添加DuckDB连接属性
        self._stats_thread = None
        self.init_ui()

    def init_ui(self):
        """初始化界面"""
        layout = QVBoxLayout(self)

        # 创建主分割器
        splitter = QSplitter(Qt.Horizontal)
        layout.addWidget(splitter)

        # 左侧面板 - 数据列表和操作
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_panel.setMinimumWidth(500)

        # 右侧面板 - 日志
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_panel.setMinimumWidth(400)

        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 1)

        # ========== 左侧面板 ==========

        # 统计信息组
        stats_group = QGroupBox("📊 数据统计 (DuckDB)")
        stats_layout = QGridLayout()
        stats_group.setLayout(stats_layout)
        left_layout.addWidget(stats_group)

        self.total_symbols_label = QLabel("标的总数: 0")
        self.total_stocks_label = QLabel("股票数量: 0")
        self.total_bonds_label = QLabel("可转债数量: 0")
        self.total_records_label = QLabel("总记录数: 0")
        self.total_size_label = QLabel("存储大小: 0 MB")
        self.latest_date_label = QLabel("最新日期: N/A")

        stats_layout.addWidget(self.total_symbols_label, 0, 0)
        stats_layout.addWidget(self.total_stocks_label, 0, 1)
        stats_layout.addWidget(self.total_bonds_label, 1, 0)
        stats_layout.addWidget(self.total_records_label, 1, 1)
        stats_layout.addWidget(self.total_size_label, 2, 0)
        stats_layout.addWidget(self.latest_date_label, 2, 1)

        stats_layout.addWidget(self.total_symbols_label, 0, 0)
        stats_layout.addWidget(self.total_stocks_label, 0, 1)
        stats_layout.addWidget(self.total_bonds_label, 1, 0)
        stats_layout.addWidget(self.total_records_label, 1, 1)
        stats_layout.addWidget(self.total_size_label, 2, 0)
        stats_layout.addWidget(self.latest_date_label, 2, 1)

        # 数据操作组
        action_group = QGroupBox("📥 数据下载")
        action_layout = QGridLayout()
        action_group.setLayout(action_layout)
        left_layout.addWidget(action_group)

        # 日期范围选择
        action_layout.addWidget(QLabel("开始日期:"), 0, 0)
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDate(QDate.currentDate().addYears(-10))
        action_layout.addWidget(self.start_date_edit, 0, 1)

        action_layout.addWidget(QLabel("结束日期:"), 0, 2)
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDate(QDate.currentDate())
        action_layout.addWidget(self.end_date_edit, 0, 3)

        # 下载数据类型选择
        data_type_layout = QHBoxLayout()
        self.data_type_combo = QComboBox()
        self.data_type_combo.addItems(["日线数据", "1分钟数据", "5分钟数据", "15分钟数据", "30分钟数据", "60分钟数据", "Tick数据"])
        data_type_layout.addWidget(QLabel("数据类型:"))
        data_type_layout.addWidget(self.data_type_combo)
        data_type_layout.addStretch()
        action_layout.addLayout(data_type_layout, 1, 0, 1, 4)

        # 下载按钮
        btn_layout = QHBoxLayout()

        self.download_stocks_btn = QPushButton("📥 下载A股数据")
        self.download_stocks_btn.clicked.connect(self.download_stocks)
        self.download_stocks_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        btn_layout.addWidget(self.download_stocks_btn)

        self.download_bonds_btn = QPushButton("📥 下载可转债数据")
        self.download_bonds_btn.clicked.connect(self.download_bonds)
        self.download_bonds_btn.setStyleSheet("""
            QPushButton {
                background-color: #2196F3;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #0b7dda;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        btn_layout.addWidget(self.download_bonds_btn)

        self.update_data_btn = QPushButton("🔄 一键补充数据")
        self.update_data_btn.clicked.connect(self.update_data)
        self.update_data_btn.setStyleSheet("""
            QPushButton {
                background-color: #FF9800;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #e68900;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        btn_layout.addWidget(self.update_data_btn)

        # 补充历史数据按钮
        self.backfill_data_btn = QPushButton("📜 补充历史数据")
        self.backfill_data_btn.clicked.connect(self.backfill_historical_data)
        self.backfill_data_btn.setStyleSheet("""
            QPushButton {
                background-color: #9C27B0;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #7B1FA2;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        btn_layout.addWidget(self.backfill_data_btn)

        self.download_tick_btn = QPushButton("⏱ 下载Tick")
        self.download_tick_btn.clicked.connect(self.download_tick_data)
        self.download_tick_btn.setStyleSheet("""
            QPushButton {
                background-color: #3F51B5;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #303F9F;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        btn_layout.addWidget(self.download_tick_btn)

        self.download_transaction_btn = QPushButton("🧾 下载分笔成交")
        self.download_transaction_btn.clicked.connect(self.download_transaction_data)
        self.download_transaction_btn.setStyleSheet("""
            QPushButton {
                background-color: #795548;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #5D4037;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        btn_layout.addWidget(self.download_transaction_btn)

        action_layout.addLayout(btn_layout, 2, 0, 1, 4)

        # 进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        action_layout.addWidget(self.progress_bar, 3, 0, 1, 4)

        # 停止按钮
        self.stop_btn = QPushButton("⏹️ 停止下载")
        self.stop_btn.clicked.connect(self.stop_download)
        self.stop_btn.setVisible(False)
        self.stop_btn.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #da190b;
            }
        """)
        action_layout.addWidget(self.stop_btn, 4, 0, 1, 4)

        # ========== 快速操作区域 ==========
        quick_action_group = QGroupBox("⚡ 快速操作")
        quick_action_layout = QGridLayout()
        quick_action_group.setLayout(quick_action_layout)
        left_layout.addWidget(quick_action_group)

        # 快速操作按钮
        other_action_layout = QHBoxLayout()

        self.verify_data_btn = QPushButton("🔍 验证数据完整性")
        self.verify_data_btn.clicked.connect(self.verify_data_integrity)
        self.verify_data_btn.setStyleSheet("""
            QPushButton {
                background-color: #607D8B;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #546E7A;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        other_action_layout.addWidget(self.verify_data_btn)

        self.retry_failed_btn = QPushButton("♻️ 失败样本重试")
        self.retry_failed_btn.clicked.connect(self.retry_failed_samples)
        self.retry_failed_btn.setStyleSheet("""
            QPushButton {
                background-color: #8BC34A;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #7CB342;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        other_action_layout.addWidget(self.retry_failed_btn)

        self.import_failed_btn = QPushButton("📂 导入CSV重跑")
        self.import_failed_btn.clicked.connect(self.import_failed_samples)
        self.import_failed_btn.setStyleSheet("""
            QPushButton {
                background-color: #009688;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #00796B;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        other_action_layout.addWidget(self.import_failed_btn)

        self.show_failures_btn = QPushButton("📈 失败原因图")
        self.show_failures_btn.clicked.connect(self.show_failure_chart)
        self.show_failures_btn.setStyleSheet("""
            QPushButton {
                background-color: #E91E63;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #C2185B;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        other_action_layout.addWidget(self.show_failures_btn)

        other_action_layout.addStretch()
        quick_action_layout.addLayout(other_action_layout, 0, 0, 1, 4)

        # ========== QMT财务数据下载区域 ==========
        financial_group = QGroupBox("💰 QMT财务数据")
        financial_layout = QGridLayout()
        financial_group.setLayout(financial_layout)
        left_layout.addWidget(financial_group)

        # 第一行：股票列表选择
        financial_layout.addWidget(QLabel("股票列表:"), 0, 0)

        self.financial_stock_combo = QComboBox()
        self.financial_stock_combo.addItems([
            "默认股票列表 (4只)",
            "自定义股票列表",
            "全部A股（谨慎使用）",
            "沪深300成分股",
            "中证500成分股",
            "中证1000成分股"
        ])
        financial_layout.addWidget(self.financial_stock_combo, 0, 1, 1, 3)

        # 第二行：数据表选择
        financial_layout.addWidget(QLabel("数据表:"), 1, 0)

        # 使用复选框让用户选择数据表
        table_check_layout = QHBoxLayout()

        self.financial_balance_check = QCheckBox("资产负债表")
        self.financial_balance_check.setChecked(True)
        table_check_layout.addWidget(self.financial_balance_check)

        self.financial_income_check = QCheckBox("利润表")
        self.financial_income_check.setChecked(True)
        table_check_layout.addWidget(self.financial_income_check)

        self.financial_cashflow_check = QCheckBox("现金流量表")
        self.financial_cashflow_check.setChecked(True)
        table_check_layout.addWidget(self.financial_cashflow_check)

        self.financial_cap_check = QCheckBox("股本结构")
        table_check_layout.addWidget(self.financial_cap_check)

        table_check_layout.addStretch()
        financial_layout.addLayout(table_check_layout, 1, 1, 1, 3)

        # 第三行：下载按钮
        self.financial_download_btn = QPushButton("💰 下载QMT财务数据")
        self.financial_download_btn.clicked.connect(self.download_financial_data)
        self.financial_download_btn.setStyleSheet("""
            QPushButton {
                background-color: #00BCD4;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #0097A7;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        financial_layout.addWidget(self.financial_download_btn, 2, 0, 1, 2)

        # 保存到DuckDB按钮
        self.financial_save_btn = QPushButton("💾 保存到DuckDB")
        self.financial_save_btn.clicked.connect(self.save_financial_to_duckdb)
        self.financial_save_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        financial_layout.addWidget(self.financial_save_btn, 2, 2, 1, 2)

        # 添加说明标签
        financial_note = QLabel("说明: 下载财务数据后，点击「保存到DuckDB」可永久存储")
        financial_note.setStyleSheet("color: #666; font-size: 9pt; padding: 5px;")
        financial_layout.addWidget(financial_note, 3, 0, 1, 4)


        # ========== 手动下载单个标的区域 ==========
        manual_group = QGroupBox("🎯 手动下载单个标的（支持分钟线）")
        manual_layout = QGridLayout()
        manual_group.setLayout(manual_layout)
        left_layout.addWidget(manual_group)

        # 第一行：股票代码输入
        manual_layout.addWidget(QLabel("股票/ETF代码:"), 0, 0)
        self.stock_code_input = QLineEdit()
        self.stock_code_input.setPlaceholderText("例如: 512100.SH 或 159915.SZ")
        manual_layout.addWidget(self.stock_code_input, 0, 1, 1, 3)

        # 第二行：常用ETF快捷按钮
        etf_label = QLabel("常用ETF:")
        etf_label.setStyleSheet("font-weight: bold; color: #2196F3;")
        manual_layout.addWidget(etf_label, 1, 0)

        etf_button_layout = QHBoxLayout()
        common_etfs = [
            ("511380.SH", "可转债ETF"),
            ("512100.SH", "中证1000"),
            ("510300.SH", "沪深300"),
            ("510500.SH", "中证500"),
            ("159915.SZ", "深证ETF")
        ]

        for code, name in common_etfs:
            etf_btn = QPushButton(f"{code}")
            etf_btn.setToolTip(f"{name}")
            etf_btn.clicked.connect(lambda checked, c=code: self.stock_code_input.setText(c))
            etf_btn.setStyleSheet("""
                QPushButton {
                    background-color: #E3F2FD;
                    color: #1976D2;
                    border: 1px solid #2196F3;
                    padding: 4px 8px;
                    border-radius: 3px;
                    font-size: 9pt;
                }
                QPushButton:hover {
                    background-color: #BBDEFB;
                }
            """)
            etf_button_layout.addWidget(etf_btn)

        etf_button_layout.addStretch()
        manual_layout.addLayout(etf_button_layout, 1, 1, 1, 3)

        # 第三行：数据类型选择
        manual_layout.addWidget(QLabel("数据类型:"), 2, 0)
        self.manual_data_type_combo = QComboBox()
        self.manual_data_type_combo.addItems([
            "日线数据",
            "1分钟数据",
            "5分钟数据",
            "15分钟数据",
            "30分钟数据",
            "60分钟数据",
            "Tick数据",
            "分笔成交"
        ])
        manual_layout.addWidget(self.manual_data_type_combo, 2, 1)

        # 日期范围
        manual_layout.addWidget(QLabel("日期范围:"), 2, 2)
        date_range_layout = QHBoxLayout()

        self.manual_start_date_edit = QDateEdit()
        self.manual_start_date_edit.setCalendarPopup(True)
        self.manual_start_date_edit.setDate(QDate.currentDate().addMonths(-3))
        self.manual_start_date_edit.setDisplayFormat("yyyy-MM-dd")
        date_range_layout.addWidget(self.manual_start_date_edit)

        date_range_layout.addWidget(QLabel("~"))

        self.manual_end_date_edit = QDateEdit()
        self.manual_end_date_edit.setCalendarPopup(True)
        self.manual_end_date_edit.setDate(QDate.currentDate())
        self.manual_end_date_edit.setDisplayFormat("yyyy-MM-dd")
        date_range_layout.addWidget(self.manual_end_date_edit)

        manual_layout.addLayout(date_range_layout, 2, 3)

        # 第四行：下载按钮
        self.manual_download_btn = QPushButton("⬇️ 下载单个标的")
        self.manual_download_btn.clicked.connect(self.download_single_stock)
        self.manual_download_btn.setStyleSheet("""
            QPushButton {
                background-color: #9C27B0;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #7B1FA2;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        manual_layout.addWidget(self.manual_download_btn, 3, 0, 1, 4)

        # 说明标签
        manual_note = QLabel("💡 提示：分钟线数据建议只下载最近1-3个月，避免数据量过大")
        manual_note.setStyleSheet("color: #FF9800; font-size: 9pt; padding: 5px;")
        manual_layout.addWidget(manual_note, 4, 0, 1, 4)

        # ========== 右侧面板 ==========

        # 日志组
        log_group = QGroupBox("📝 操作日志")
        log_layout = QVBoxLayout()
        log_group.setLayout(log_layout)
        right_layout.addWidget(log_group)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet("""
            QTextEdit {
                font-family: 'Consolas', 'Monaco', monospace;
                font-size: 10pt;
                background-color: #1e1e1e;
                color: #d4d4d4;
            }
        """)
        log_layout.addWidget(self.log_text)

        # 清空日志按钮
        clear_log_btn = QPushButton("🗑️ 清空日志")
        clear_log_btn.clicked.connect(self.log_text.clear)
        log_layout.addWidget(clear_log_btn)

        # 初始日志
        self.log("本地数据管理组件已加载")
        self.log("提示：首次使用请先下载数据")

        # 异步加载DuckDB统计数据，避免切换标签页时主线程卡顿
        QTimer.singleShot(100, self.load_duckdb_statistics)

    def log(self, message):
        """输出日志"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.log_text.append(f"[{timestamp}] {message}")
        # 滚动到底部
        cursor = self.log_text.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_text.setTextCursor(cursor)

    def load_duckdb_statistics(self):
        """异步从DuckDB加载统计数据"""
        if self._stats_thread is not None and self._stats_thread.isRunning():
            return
        self._stats_thread = LocalStatsLoadThread(_get_duckdb_path())
        self._stats_thread.result_ready.connect(self._on_duckdb_statistics_loaded)
        self._stats_thread.error_occurred.connect(self._on_duckdb_statistics_error)
        self._stats_thread.start()

    def _on_duckdb_statistics_loaded(self, stats: dict[str, Any]):
        total_symbols = int(stats.get("total_symbols", 0) or 0)
        stock_count = int(stats.get("stock_count", 0) or 0)
        total_bonds = int(stats.get("total_bonds", 0) or 0)
        total_records = int(stats.get("total_records", 0) or 0)
        size_mb = float(stats.get("size_mb", 0.0) or 0.0)
        latest_date = str(stats.get("latest_date", "N/A") or "N/A")

        self.total_symbols_label.setText(f"标的总数: {total_symbols:,}")
        self.total_stocks_label.setText(f"股票数量: {stock_count:,}")
        self.total_bonds_label.setText(f"可转债数量: {total_bonds:,}")
        self.total_records_label.setText(f"总记录数: {total_records:,}")
        self.total_size_label.setText(f"存储大小: {size_mb:.2f} MB")
        self.latest_date_label.setText(f"最新日期: {latest_date}")

    def _on_duckdb_statistics_error(self, error_message: str):
        self.log(f"[ERROR] 加载统计数据失败: {error_message}")

    def download_single_stock(self):
        """下载单个标的的数据"""
        # 获取输入的股票代码
        stock_code = self.stock_code_input.text().strip()

        if not stock_code:
            QMessageBox.warning(self, "提示", "请输入股票/ETF代码")
            return

        # 标准化代码格式
        stock_code = stock_code.upper()

        # 验证代码格式
        if '.' not in stock_code:
            # 如果没有后缀，尝试自动添加
            if stock_code.startswith('6') or stock_code.startswith('5'):
                stock_code = stock_code + '.SH'
            elif stock_code.startswith('0') or stock_code.startswith('3') or stock_code.startswith('1'):
                stock_code = stock_code + '.SZ'

        # 获取日期范围
        start_date = self.manual_start_date_edit.date().toString("yyyy-MM-dd")
        end_date = self.manual_end_date_edit.date().toString("yyyy-MM-dd")

        # 获取数据类型
        data_type_text = self.manual_data_type_combo.currentText()
        period_map = {
            "日线数据": "1d",
            "1分钟数据": "1m",
            "5分钟数据": "5m",
            "15分钟数据": "15m",
            "30分钟数据": "30m",
            "60分钟数据": "60m",
            "Tick数据": "tick",
            "分笔成交": "l2transaction"
        }
        period = period_map.get(data_type_text, "1d")

        self.log(f"🎯 开始下载单个标的: {stock_code}")
        self.log(f"   数据类型: {data_type_text}")
        self.log(f"   日期范围: {start_date} ~ {end_date}")
        self.log("   说明: 下载数据为【不复权】的原始数据，查看时可选择复权类型")

        # 禁用按钮
        self.manual_download_btn.setEnabled(False)

        # 创建下载线程（不传递复权参数，只下载原始数据）
        self.download_thread = SingleStockDownloadThread(
            stock_code=stock_code,
            start_date=start_date,
            end_date=end_date,
            period=period
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.finished_signal.connect(self.on_single_download_finished)
        self.download_thread.error_signal.connect(self.on_single_download_error)
        self.download_thread.start()

    def on_single_download_finished(self, result):
        """单个标的下载完成"""
        self.manual_download_btn.setEnabled(True)

        stock_code = result.get('symbol', '')
        success = result.get('success', False)
        record_count = result.get('record_count', 0)
        file_size = result.get('file_size', 0)

        if success:
            self.log(f"✅ {stock_code} 下载成功!")
            self.log(f"   记录数: {record_count} 条")
            self.log(f"   文件大小: {file_size:.2f} MB")

            QMessageBox.information(self, "下载成功",
                f"{stock_code} 下载成功!\n\n记录数: {record_count} 条\n文件大小: {file_size:.2f} MB")

        else:
            self.log(f"❌ {stock_code} 下载失败")

    def on_single_download_error(self, error_msg):
        """单个标的下载出错"""
        self.manual_download_btn.setEnabled(True)
        QMessageBox.critical(self, "下载失败", error_msg)

    def download_financial_data(self):
        """下载QMT财务数据"""
        if self.download_thread and self.download_thread.isRunning():
            QMessageBox.warning(self, "提示", "已有下载任务正在运行")
            return

        # 获取股票列表
        stock_selection = self.financial_stock_combo.currentText()

        if "默认股票列表" in stock_selection:
            stock_list = ["000001.SZ", "600519.SH", "511380.SH", "512100.SH"]
        elif "自定义股票列表" in stock_selection:
            # 弹出输入对话框让用户输入股票列表
            text, ok = QInputDialog.getText(
                self, "输入股票列表",
                "请输入股票代码，用逗号分隔:\n例如: 000001.SZ,600519.SH,511380.SH"
            )
            if not ok or not text.strip():
                return
            stock_list = [s.strip() for s in text.split(',')]
        elif "全部A股" in stock_selection:
            # 警告用户
            reply = QMessageBox.question(
                self, "确认下载",
                "即将下载全部A股的财务数据，这可能需要较长时间。\n\n确定要继续吗？",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.No:
                return
            # 获取全部A股列表（后台线程，㊺修复）
            all_stocks = self._get_sector_stocks('沪深A股')
            if not all_stocks:
                QMessageBox.warning(self, "错误", "获取股票列表失败")
                return
            stock_list = all_stocks[:100]
            QMessageBox.information(self, "提示", "为避免下载时间过长，限制为前100只股票")
        elif "沪深300" in stock_selection:
            stock_list = self._get_sector_stocks('沪深300') or ["000001.SZ", "600519.SH", "511380.SH"]
        elif "中证500" in stock_selection:
            stock_list = self._get_sector_stocks('中证500') or ["000001.SZ", "600519.SH", "511380.SH"]
        elif "中证1000" in stock_selection:
            stock_list = self._get_sector_stocks('中证1000') or ["000001.SZ", "600519.SH", "511380.SH"]
        else:
            stock_list = ["000001.SZ", "600519.SH", "511380.SH"]

        # 获取数据表列表
        table_list = []
        if self.financial_balance_check.isChecked():
            table_list.append("Balance")
        if self.financial_income_check.isChecked():
            table_list.append("Income")
        if self.financial_cashflow_check.isChecked():
            table_list.append("CashFlow")
        if self.financial_cap_check.isChecked():
            table_list.append("Capitalization")

        if not table_list:
            QMessageBox.warning(self, "提示", "请至少选择一个数据表")
            return

        self.log("💰 开始下载QMT财务数据")
        self.log(f"   股票数量: {len(stock_list)}")
        self.log(f"   数据表: {', '.join(table_list)}")

        # 创建下载线程
        self.download_thread = FinancialDataDownloadThread(
            stock_list=stock_list,
            table_list=table_list
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.progress_signal.connect(self.update_progress)
        self.download_thread.finished_signal.connect(self.on_financial_download_finished)
        self.download_thread.error_signal.connect(self.on_financial_download_error)
        self.download_thread.start()

        self._set_download_state(True)

    def on_financial_download_finished(self, result):
        """财务数据下载完成"""
        self._set_download_state(False)
        self.progress_bar.setVisible(False)

        total = result.get('total', 0)
        success = result.get('success', 0)
        failed = result.get('failed', 0)
        skipped = result.get('skipped', 0)

        msg = "QMT财务数据下载完成！\n\n"
        msg += f"有效股票: {total} 只\n"
        msg += f"成功: {success} 只\n"
        msg += f"失败: {failed} 只"
        if skipped > 0:
            msg += f"\n跳过: {skipped} 只（ETF/指数无财务数据）"

        if failed > 0:
            QMessageBox.warning(self, "下载完成", msg)
        else:
            QMessageBox.information(self, "下载完成", msg)

    def save_financial_to_duckdb(self):
        """保存财务数据到DuckDB"""
        # 检查模块是否可用
        if not BATCH_SAVE_AVAILABLE:
            QMessageBox.warning(self, "功能不可用",
                "批量保存财务数据模块不可用。\n\n请确保 advanced_data_viewer_widget.py 文件存在且可导入。")
            return

        # 获取股票列表
        stock_selection = self.financial_stock_combo.currentText()

        if "默认股票列表" in stock_selection:
            stock_list = ["000001.SZ", "600519.SH", "511380.SH", "512100.SH"]
        elif "自定义股票列表" in stock_selection:
            text, ok = QInputDialog.getText(
                self, "输入股票列表",
                "请输入股票代码，用逗号分隔:\n例如: 000001.SZ,600519.SH"
            )
            if not ok or not text.strip():
                return
            stock_list = [s.strip() for s in text.split(',')]
        elif "沪深300" in stock_selection:
            stock_list = self._get_sector_stocks('沪深300') or ["000001.SZ", "600519.SH"]
        elif "中证500" in stock_selection:
            stock_list = self._get_sector_stocks('中证500') or ["000001.SZ", "600519.SH"]
        elif "中证1000" in stock_selection:
            stock_list = self._get_sector_stocks('中证1000') or ["000001.SZ", "600519.SH"]
        elif "全部A股" in stock_selection:
            reply = QMessageBox.question(
                self, "确认保存",
                "即将保存全部A股的财务数据到DuckDB，这可能需要较长时间。\n\n确定要继续吗？",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.No:
                return
            stock_list = self._get_sector_stocks('沪深A股')
            if not stock_list:
                QMessageBox.warning(self, "错误", "获取股票列表失败")
                return
        else:
            stock_list = ["000001.SZ", "600519.SH"]

        self.log("💾 开始保存财务数据到DuckDB")
        self.log(f"   股票数量: {len(stock_list)}")

        # 创建保存线程
        self.save_thread = BatchFinancialSaveThread(stock_list)
        self.save_thread.log_signal.connect(self.log)
        self.save_thread.progress_signal.connect(self.update_progress)
        self.save_thread.finished_signal.connect(self.on_financial_save_finished)
        self.save_thread.error_signal.connect(self.on_financial_save_error)
        self.save_thread.start()

        self._set_download_state(True)

    def on_financial_save_finished(self, result):
        """财务数据保存完成"""
        self._set_download_state(False)
        self.progress_bar.setVisible(False)

        total = result.get('total', 0)
        success = result.get('success', 0)
        failed = result.get('failed', 0)

        msg = "财务数据保存完成！\n\n"
        msg += f"总数: {total} 只\n"
        msg += f"成功: {success} 只\n"
        msg += f"失败: {failed} 只"

        if failed > 0:
            QMessageBox.warning(self, "保存完成", msg)
        else:
            QMessageBox.information(self, "保存完成", msg)

        # 重新加载数据信息
        self.load_duckdb_statistics()

    def on_financial_save_error(self, error_msg):
        """财务数据保存出错"""
        self._set_download_state(False)
        self.progress_bar.setVisible(False)
        QMessageBox.critical(self, "保存失败", error_msg)

    def download_single_financial(self):
        """下载单只股票的财务数据"""
        financial_stock_input = getattr(self, "financial_stock_input", None)
        if financial_stock_input is None or not hasattr(financial_stock_input, "text"):
            QMessageBox.warning(self, "提示", "当前界面未启用单股财务输入框")
            return
        stock_code = str(financial_stock_input.text()).strip()

        if not stock_code:
            QMessageBox.warning(self, "提示", "请输入股票代码")
            return

        # 标准化代码格式
        stock_code = stock_code.upper()

        # 验证代码格式
        if '.' not in stock_code:
            # 如果没有后缀，尝试自动添加
            if stock_code.startswith('6') or stock_code.startswith('5'):
                stock_code = stock_code + '.SH'
            elif stock_code.startswith('0') or stock_code.startswith('3') or stock_code.startswith('1'):
                stock_code = stock_code + '.SZ'

        # 获取数据表列表
        table_list = []
        if self.financial_balance_check.isChecked():
            table_list.append("Balance")
        if self.financial_income_check.isChecked():
            table_list.append("Income")
        if self.financial_cashflow_check.isChecked():
            table_list.append("CashFlow")
        if self.financial_cap_check.isChecked():
            table_list.append("Capitalization")

        if not table_list:
            QMessageBox.warning(self, "提示", "请至少选择一个数据表")
            return

        self.log(f"💰 开始下载 {stock_code} 的财务数据")
        self.log(f"   数据表: {', '.join(table_list)}")

        # 创建下载线程
        self.download_thread = FinancialDataDownloadThread(
            stock_list=[stock_code],
            table_list=table_list
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.progress_signal.connect(self.update_progress)
        self.download_thread.finished_signal.connect(self.on_single_financial_finished)
        self.download_thread.error_signal.connect(self.on_financial_download_error)
        self.download_thread.start()

        self._set_download_state(True)

    def on_single_financial_finished(self, result):
        """单只股票财务数据下载完成"""
        self._set_download_state(False)
        self.progress_bar.setVisible(False)

        total = result.get('total', 0)
        success = result.get('success', 0)
        failed = result.get('failed', 0)
        skipped = result.get('skipped', 0)

        msg = "财务数据下载完成！\n\n"
        msg += f"有效股票: {total} 只\n"
        msg += f"成功: {success} 只"
        if failed > 0:
            msg += f"\n失败: {failed} 只"
        if skipped > 0:
            msg += f"\n跳过: {skipped} 只（ETF/指数）"

        if failed > 0:
            QMessageBox.warning(self, "下载完成", msg)
        else:
            QMessageBox.information(self, "下载完成", msg)

        # 刷新财务数据统计
        self.refresh_financial_stats()

    def refresh_financial_stats(self):
        """刷新财务数据统计（后台线程执行 xtdata 调用，㊺修复）"""
        self.log("[INFO] 正在统计已下载的财务数据...")
        threading.Thread(target=self._refresh_financial_stats_bg, daemon=True).start()

    def _refresh_financial_stats_bg(self):
        try:
            from xtquant import xtdata

            test_stocks = ["000001.SZ", "600519.SH", "511380.SH", "512100.SH"]
            table_list = ["Balance", "Income", "CashFlow"]

            total_count = 0
            stock_count = 0

            for stock_code in test_stocks:
                try:
                    result = xtdata.get_financial_data(
                        stock_list=[stock_code],
                        table_list=table_list,
                        start_time="20200101",
                        end_time="20260130",
                        report_type='report_time'
                    )

                    if isinstance(result, dict) and stock_code in result:
                        stock_data = result[stock_code]
                        count = 0
                        for table_name in table_list:
                            if table_name in stock_data:
                                table_data = stock_data[table_name]
                                if isinstance(table_data, dict):
                                    count += len(table_data)
                                elif hasattr(table_data, '__len__'):
                                    count += len(table_data)

                        if count > 0:
                            stock_count += 1
                            total_count += count

                except Exception:
                    continue

            self.log(f"[OK] 财务数据统计更新完成: {stock_count}只股票, {total_count}条记录")

        except Exception as e:
            self.log(f"[ERROR] 统计财务数据失败: {e}")

    def view_financial_data(self):
        """查看选中股票的财务数据"""
        data_table = getattr(self, "data_table", None)
        if data_table is None:
            QMessageBox.warning(self, "提示", "当前界面没有可用的数据表格")
            return
        # 获取选中的行
        selected_items = data_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "提示", "请先在列表中选择一只股票")
            return

        # 获取股票代码
        row = data_table.currentRow()
        code_item = data_table.item(row, 0)
        if not code_item:
            return

        stock_code = code_item.text()

        self.log(f"[INFO] 查看 {stock_code} 的财务数据")

        # 提示用户使用数据查看器
        QMessageBox.information(
            self,
            "查看财务数据",
            f"「查看财务数据」功能已迁移到「📈 数据查看器」标签页\n\n"
            f"请在「📈 数据查看器」标签页中：\n"
            f"1. 选择股票: {stock_code}\n"
            f"2. 点击「💰 加载财务数据」按钮\n\n"
            f"新功能支持查看更详细的财务指标数据。"
        )

    def _get_latest_failed_csv(self) -> Optional[str]:
        try:
            log_dir = os.path.join(Path(__file__).parents[2], "logs")
            if not os.path.exists(log_dir):
                return None
            candidates: list[str] = []
            for name in os.listdir(log_dir):
                if name.startswith("failed_") and name.endswith(".csv"):
                    candidates.append(os.path.join(log_dir, name))
            if not candidates:
                return None
            candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            return candidates[0]
        except Exception:
            return None

    def _parse_task_type_from_path(self, path: str) -> str:
        base = os.path.basename(path)
        if "transaction" in base:
            return "download_transaction"
        if "tick" in base:
            return "download_tick"
        return "download_tick"

    def _build_symbols_from_csv(self, file_path: str) -> list[str]:
        try:
            df = pd.read_csv(file_path)
            if "stock_code" in df.columns:
                return [str(code).strip() for code in df["stock_code"].dropna().unique().tolist()]
        except Exception:
            pass
        return []

    def on_financial_download_error(self, error_msg):
        """财务数据下载出错"""
        self._set_download_state(False)
        self.progress_bar.setVisible(False)
        QMessageBox.critical(self, "下载失败", error_msg)

    @staticmethod
    def _get_sector_stocks(sector_name: str) -> list:
        """在后台线程中获取板块股票列表，避免主线程阻塞（㊺修复）"""
        from concurrent.futures import ThreadPoolExecutor
        def _fetch():
            from xtquant import xtdata
            return xtdata.get_stock_list_in_sector(sector_name) or []
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(_fetch)
                return future.result(timeout=10)
        except Exception:
            return []

    def download_stocks(self):
        """下载A股数据"""
        if self.download_thread and self.download_thread.isRunning():
            QMessageBox.warning(self, "提示", "已有下载任务正在运行")
            return

        start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
        end_date = self.end_date_edit.date().toString("yyyy-MM-dd")

        self.log(f"📥 开始下载A股数据 ({start_date} ~ {end_date})")

        self.download_thread = DataDownloadThread(
            task_type='download_stocks',
            symbols=None,  # 自动获取全部A股
            start_date=start_date,
            end_date=end_date
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.progress_signal.connect(self.update_progress)
        self.download_thread.finished_signal.connect(self.on_download_finished)
        self.download_thread.error_signal.connect(self.on_download_error)
        self.download_thread.start()

        self._set_download_state(True)

    def download_bonds(self):
        """下载可转债数据"""
        if self.download_thread and self.download_thread.isRunning():
            QMessageBox.warning(self, "提示", "已有下载任务正在运行")
            return

        start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
        end_date = self.end_date_edit.date().toString("yyyy-MM-dd")

        self.log(f"📥 开始下载可转债数据 ({start_date} ~ {end_date})")

        self.download_thread = DataDownloadThread(
            task_type='download_bonds',
            symbols=None,  # 自动获取全部可转债
            start_date=start_date,
            end_date=end_date
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.progress_signal.connect(self.update_progress)
        self.download_thread.finished_signal.connect(self.on_download_finished)
        self.download_thread.error_signal.connect(self.on_download_error)
        self.download_thread.start()

        self._set_download_state(True)

    def update_data(self):
        """一键补充数据"""
        if self.download_thread and self.download_thread.isRunning():
            QMessageBox.warning(self, "提示", "已有下载任务正在运行")
            return

        self.log("🔄 开始补充数据...")

        self.download_thread = DataDownloadThread(
            task_type='update_data',
            symbols=None,
            start_date=None,
            end_date=None
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.progress_signal.connect(self.update_progress)
        self.download_thread.finished_signal.connect(self.on_download_finished)
        self.download_thread.error_signal.connect(self.on_download_error)
        self.download_thread.start()

        self._set_download_state(True)

    def backfill_historical_data(self):
        """补充历史数据（获取2018年以来的完整数据）"""
        reply = QMessageBox.question(
            self, "确认操作",
            "此操作将为所有股票补充2018年以来的完整历史数据。\n\n"
            "可能需要较长时间，确定要继续吗？",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply == QMessageBox.No:
            return

        if self.download_thread and self.download_thread.isRunning():
            QMessageBox.warning(self, "提示", "已有下载任务正在运行")
            return

        self.log("📜 开始补充历史数据（2018年起）...")

        self.download_thread = DataDownloadThread(
            task_type='backfill_history',
            symbols=None,
            start_date='20180101',
            end_date=None
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.progress_signal.connect(self.update_progress)
        self.download_thread.finished_signal.connect(self.on_download_finished)
        self.download_thread.error_signal.connect(self.on_download_error)
        self.download_thread.start()

        self._set_download_state(True)

    def download_tick_data(self):
        if self.download_thread and self.download_thread.isRunning():
            QMessageBox.warning(self, "提示", "已有下载任务正在运行")
            return
        start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
        end_date = self.end_date_edit.date().toString("yyyy-MM-dd")
        reply = QMessageBox.question(
            self,
            "确认操作",
            f"将下载Tick数据并写入DuckDB。\n日期范围: {start_date} ~ {end_date}\n\n数据量较大，确定继续吗？",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply == QMessageBox.No:
            return
        self.log(f"⏱ 开始下载Tick数据 ({start_date} ~ {end_date})")
        self.download_thread = DataDownloadThread(
            task_type='download_tick',
            symbols=None,
            start_date=start_date,
            end_date=end_date
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.progress_signal.connect(self.update_progress)
        self.download_thread.finished_signal.connect(self.on_download_finished)
        self.download_thread.error_signal.connect(self.on_download_error)
        self.download_thread.start()
        self._set_download_state(True)

    def download_transaction_data(self):
        if self.download_thread and self.download_thread.isRunning():
            QMessageBox.warning(self, "提示", "已有下载任务正在运行")
            return
        start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
        end_date = self.end_date_edit.date().toString("yyyy-MM-dd")
        reply = QMessageBox.question(
            self,
            "确认操作",
            f"将下载分笔成交数据并写入DuckDB。\n日期范围: {start_date} ~ {end_date}\n\n数据量较大，确定继续吗？",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply == QMessageBox.No:
            return
        self.log(f"🧾 开始下载分笔成交 ({start_date} ~ {end_date})")
        self.download_thread = DataDownloadThread(
            task_type='download_transaction',
            symbols=None,
            start_date=start_date,
            end_date=end_date
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.progress_signal.connect(self.update_progress)
        self.download_thread.finished_signal.connect(self.on_download_finished)
        self.download_thread.error_signal.connect(self.on_download_error)
        self.download_thread.start()
        self._set_download_state(True)

    def update_progress(self, current, total):
        """更新进度"""
        self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)
        pct = (current / total) * 100 if total > 0 else 0
        self.progress_bar.setFormat(f"{current}/{total} ({pct:.1f}%)")

    def on_download_finished(self, result):
        """下载完成"""
        self._set_download_state(False)
        self.progress_bar.setVisible(False)

        total = result.get('total', 0)
        success = result.get('success', 0)
        failed = result.get('failed', 0)

        msg = f"下载完成！\n总数: {total}\n成功: {success}\n失败: {failed}"

        if failed > 0:
            QMessageBox.warning(self, "下载完成", msg)
        else:
            QMessageBox.information(self, "下载完成", msg)

        # 重新加载数据信息
        self.load_duckdb_statistics()

    def on_download_error(self, error_msg):
        """下载出错"""
        self._set_download_state(False)
        self.progress_bar.setVisible(False)
        QMessageBox.critical(self, "下载失败", error_msg)

    def stop_download(self):
        """停止下载"""
        if self.download_thread and self.download_thread.isRunning():
            self.log("⏹️ 正在停止下载...")
            self.download_thread.stop()

    def _set_download_state(self, is_downloading):
        """设置下载状态"""
        self.download_stocks_btn.setEnabled(not is_downloading)
        self.download_bonds_btn.setEnabled(not is_downloading)
        self.update_data_btn.setEnabled(not is_downloading)
        self.backfill_data_btn.setEnabled(not is_downloading)
        self.download_tick_btn.setEnabled(not is_downloading)
        self.download_transaction_btn.setEnabled(not is_downloading)
        self.manual_download_btn.setEnabled(not is_downloading)
        self.verify_data_btn.setEnabled(not is_downloading)
        self.retry_failed_btn.setEnabled(not is_downloading)
        self.import_failed_btn.setEnabled(not is_downloading)
        self.show_failures_btn.setEnabled(not is_downloading)
        self.financial_download_btn.setEnabled(not is_downloading)
        self.stop_btn.setVisible(is_downloading)
        self.progress_bar.setVisible(is_downloading)

        if is_downloading:
            self.progress_bar.setValue(0)

    def verify_data_integrity(self):
        """验证数据完整性"""
        # 创建一个带输入选项的对话框
        dialog = QInputDialog(self)
        dialog.setWindowTitle("验证数据完整性")
        dialog.setLabelText("请输入要验证的股票代码:")
        dialog.setTextValue("511380.SH")  # 默认值
        dialog.setInputMode(QInputDialog.TextInput)

        ok = dialog.exec_()
        stock_code = dialog.textValue().strip()

        if ok and stock_code:
            # 自动格式化代码
            if '.' not in stock_code:
                # 自动添加交易所后缀
                if stock_code.startswith(('5', '6')):
                    stock_code = stock_code + '.SH'
                elif stock_code.startswith(('0', '1', '3')):
                    stock_code = stock_code + '.SZ'

            self.log(f"🔍 验证 {stock_code} 数据完整性...")

            # 创建验证线程
            self.verify_thread = VerifyDataThread(stock_code)
            self.verify_thread.log_signal.connect(self.log)
            self.verify_thread.finished_signal.connect(self.on_verify_finished)
            self.verify_thread.start()

    def on_verify_finished(self, result):
        """验证完成"""
        stock = result.get('stock', 'N/A')
        has_1min = result.get('has_1min', False)
        has_daily = result.get('has_daily', False)
        has_tick = result.get('has_tick', False)
        records_1min = result.get('records_1min', 0)
        records_daily = result.get('records_daily', 0)
        records_tick = result.get('records_tick', 0)
        start_1min = result.get('start_1min', '')
        end_1min = result.get('end_1min', '')
        start_daily = result.get('start_daily', '')
        end_daily = result.get('end_daily', '')
        start_tick = result.get('start_tick', '')
        end_tick = result.get('end_tick', '')

        msg = f"{stock} 数据验证结果:\n\n"
        msg += f"1分钟数据: {'✓ 存在' if has_1min else '✗ 不存在'}"
        if has_1min:
            msg += f"\n   记录数: {records_1min:,} 条"
            msg += f"\n   时间范围: {start_1min} ~ {end_1min}"
        else:
            msg += "\n"

        msg += f"\n日线数据: {'✓ 存在' if has_daily else '✗ 不存在'}"
        if has_daily:
            msg += f"\n   记录数: {records_daily:,} 条"
            msg += f"\n   时间范围: {start_daily} ~ {end_daily}"

        msg += f"\nTick数据: {'✓ 存在' if has_tick else '✗ 不存在'}"
        if has_tick:
            msg += f"\n   记录数: {records_tick:,} 条"
            msg += f"\n   时间范围: {start_tick} ~ {end_tick}"

        if has_1min or has_daily or has_tick:
            QMessageBox.information(self, "验证完成", msg)
        else:
            QMessageBox.warning(self, "验证完成", msg + "\n⚠️ 该股票没有本地数据，请先下载")

    def retry_failed_samples(self):
        if self.download_thread and self.download_thread.isRunning():
            QMessageBox.warning(self, "提示", "已有下载任务正在运行")
            return
        latest = self._get_latest_failed_csv()
        if not latest:
            QMessageBox.information(self, "提示", "未找到失败样本CSV")
            return
        symbols = self._build_symbols_from_csv(latest)
        if not symbols:
            QMessageBox.information(self, "提示", "失败样本CSV为空或格式不正确")
            return
        task_type = self._parse_task_type_from_path(latest)
        reply = QMessageBox.question(
            self,
            "确认操作",
            f"将重试失败样本，共 {len(symbols)} 只标的。\n\n文件: {latest}",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply == QMessageBox.No:
            return
        self.log(f"♻️ 开始重试失败样本: {len(symbols)} 只标的")
        self.download_thread = DataDownloadThread(
            task_type=task_type,
            symbols=symbols,
            start_date=self.start_date_edit.date().toString("yyyy-MM-dd"),
            end_date=self.end_date_edit.date().toString("yyyy-MM-dd")
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.progress_signal.connect(self.update_progress)
        self.download_thread.finished_signal.connect(self.on_download_finished)
        self.download_thread.error_signal.connect(self.on_download_error)
        self.download_thread.start()
        self._set_download_state(True)

    def import_failed_samples(self):
        if self.download_thread and self.download_thread.isRunning():
            QMessageBox.warning(self, "提示", "已有下载任务正在运行")
            return
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择失败样本CSV", "", "CSV文件 (*.csv)"
        )
        if not file_path:
            return
        symbols = self._build_symbols_from_csv(file_path)
        if not symbols:
            QMessageBox.information(self, "提示", "CSV为空或格式不正确")
            return
        task_type = self._parse_task_type_from_path(file_path)
        reply = QMessageBox.question(
            self,
            "确认操作",
            f"将导入CSV并重跑，共 {len(symbols)} 只标的。\n\n文件: {file_path}",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply == QMessageBox.No:
            return
        self.log(f"📂 导入CSV重跑: {len(symbols)} 只标的")
        self.download_thread = DataDownloadThread(
            task_type=task_type,
            symbols=symbols,
            start_date=self.start_date_edit.date().toString("yyyy-MM-dd"),
            end_date=self.end_date_edit.date().toString("yyyy-MM-dd")
        )
        self.download_thread.log_signal.connect(self.log)
        self.download_thread.progress_signal.connect(self.update_progress)
        self.download_thread.finished_signal.connect(self.on_download_finished)
        self.download_thread.error_signal.connect(self.on_download_error)
        self.download_thread.start()
        self._set_download_state(True)

    def show_failure_chart(self):
        latest = self._get_latest_failed_csv()
        if not latest:
            QMessageBox.information(self, "提示", "未找到失败样本CSV")
            return
        try:
            df = pd.read_csv(latest)
        except Exception:
            QMessageBox.information(self, "提示", "无法读取失败样本CSV")
            return
        if "reason" not in df.columns:
            QMessageBox.information(self, "提示", "CSV缺少reason列")
            return
        reason_counts = df["reason"].fillna("unknown").value_counts().to_dict()
        total = sum(reason_counts.values())
        summary = "\n".join([f"{k}: {v}" for k, v in reason_counts.items()])
        QMessageBox.information(
            self,
            "失败原因统计",
            f"文件: {latest}\n总计: {total}\n\n{summary}"
        )

    def closeEvent(self, event):
        try:
            # verify_thread 含多个串行 DuckDB 查询，给更长的等待窗口
            thread_timeouts = [
                (self.download_thread, 1000),
                (self._stats_thread, 1000),
                (getattr(self, 'save_thread', None), 1000),
                (getattr(self, 'verify_thread', None), 3000),
            ]
            for t, timeout_ms in thread_timeouts:
                try:
                    if t and t.isRunning():
                        t.requestInterruption()
                        t.quit()
                        t0 = time.monotonic()
                        finished = t.wait(timeout_ms)
                        elapsed_ms = int((time.monotonic() - t0) * 1000)
                        status = "已退出" if finished else "超时未退出"
                        print(f"[closeEvent] LocalDataManagerWidget - {t.__class__.__name__}: {status} ({elapsed_ms}ms)")
                except Exception:
                    pass  # 测试 mock 或非 QThread 对象，跳过清理
        finally:
            super().closeEvent(event)


class DataViewerDialog(QDialog):
    """数据查看对话框 - 支持复权"""

    def __init__(self, stock_code: str, adjust: str, parent=None):
        super().__init__(parent)
        self.stock_code = stock_code
        self.adjust = adjust
        self.setWindowTitle(f"查看数据 - {stock_code} ({adjust}) [DuckDB]")
        self.setMinimumSize(0, 0)
        self.init_ui()
        self.load_data()

    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)

        # 顶部信息
        info_layout = QHBoxLayout()

        # 股票代码
        code_label = QLabel(f"股票代码: <b>{self.stock_code}</b>")
        code_label.setStyleSheet("font-size: 12pt;")
        info_layout.addWidget(code_label)

        # 复权类型
        adjust_names = {"none": "不复权", "qfq": "前复权", "hfq": "后复权"}
        adjust_label = QLabel(f"复权类型: <b>{adjust_names.get(self.adjust, self.adjust)}</b>")
        adjust_label.setStyleSheet("font-size: 12pt;")
        info_layout.addWidget(adjust_label)

        info_layout.addStretch()

        # 导出按钮
        export_btn = QPushButton("📊 导出CSV")
        export_btn.clicked.connect(self.export_csv)
        export_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 5px 12px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        info_layout.addWidget(export_btn)

        # 关闭按钮
        close_btn = QPushButton("✖ 关闭")
        close_btn.clicked.connect(self.accept)
        close_btn.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                color: white;
                border: none;
                padding: 5px 12px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #da190b;
            }
        """)
        info_layout.addWidget(close_btn)

        layout.addLayout(info_layout)

        # 数据表格
        self.data_table = QTableView()
        self.data_table.setAlternatingRowColors(True)
        self.data_table.setSortingEnabled(True)
        layout.addWidget(self.data_table)

        # 统计信息
        self.stats_label = QLabel()
        self.stats_label.setStyleSheet("font-size: 10pt; color: #666;")
        layout.addWidget(self.stats_label)

    def load_data(self):
        """加载数据"""
        try:
            get_db_manager = _import_duckdb_manager()
            manager = get_db_manager(_get_duckdb_path())

            db_path = Path(_get_duckdb_path())

            if not db_path.exists():
                self.stats_label.setText(f"❌ 数据库不存在: {db_path}")
                model = _PandasTableModel(pd.DataFrame({"错误": [f"数据库不存在:\n{db_path}"]}))
                self.data_table.setModel(model)
                return

            with manager.get_read_connection() as con:
                adjust_map = {
                    "none": "none",
                    "qfq": "front",
                    "hfq": "back"
                }
                duckdb_adjust = adjust_map.get(self.adjust, "none")

                columns = _get_table_columns(con, 'stock_daily')
                if not columns:
                    self.stats_label.setText("❌ stock_daily 表不存在或无列")
                    return

                conditions = ["stock_code = ?"]
                params: list = [self.stock_code]
                if 'period' in columns:
                    conditions.append("period = ?")
                    params.append('1d')
                if 'adjust_type' in columns:
                    conditions.append("adjust_type = ?")
                    params.append(duckdb_adjust)
                where_clause = " AND ".join(conditions)

                query = f"SELECT date, open, high, low, close, volume, amount FROM stock_daily WHERE {where_clause} ORDER BY date"  # noqa: S608

                df = con.execute(query, params).df()

            if df.empty:
                self.stats_label.setText(f"❌ 未找到 {self.stock_code} 的数据")
                model = _PandasTableModel(pd.DataFrame({"提示": [f"未找到 {self.stock_code} 的数据\n请先下载该股票的数据"]}))
                self.data_table.setModel(model)
                return

            # 设置日期为索引
            df.set_index('date', inplace=True)

            # 显示数据
            self._display_data(df)

        except Exception as e:
            self.stats_label.setText(f"❌ 加载失败: {str(e)}")
            import traceback
            traceback.print_exc()
            model = _PandasTableModel(pd.DataFrame({"错误": [f"加载数据失败:\n{str(e)}"]}))
            self.data_table.setModel(model)

    def _display_data(self, df):
        """显示数据到表格"""
        model = _PandasTableModel(df)
        proxy = QSortFilterProxyModel(self)
        proxy.setSourceModel(model)
        self.data_table.setModel(proxy)
        self.data_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        # 更新统计信息
        stats = f"总记录数: {len(df):,} 条"
        if not df.empty:
            latest_price = df['close'].iloc[-1]
            stats += f" | 最新价: {latest_price:.2f}"

            if len(df) >= 2:
                start_price = df['close'].iloc[0]
                total_return = (latest_price / start_price - 1) * 100
                stats += f" | 区间涨跌: {total_return:+.2f}%"

        self.stats_label.setText(stats)

    def export_csv(self):
        """导出为CSV"""
        try:
            get_db_manager = _import_duckdb_manager()
            manager = get_db_manager(_get_duckdb_path())

            # 映射复权类型
            adjust_map = {
                "none": "none",
                "qfq": "front",
                "hfq": "back"
            }
            duckdb_adjust = adjust_map.get(self.adjust, "none")

            with manager.get_read_connection() as con:
                columns = _get_table_columns(con, 'stock_daily')
                if not columns:
                    self.stats_label.setText("❌ stock_daily 表不存在或无列")
                    return

                conditions = ["stock_code = ?"]
                params: list = [self.stock_code]
                if 'period' in columns:
                    conditions.append("period = ?")
                    params.append('1d')
                if 'adjust_type' in columns:
                    conditions.append("adjust_type = ?")
                    params.append(duckdb_adjust)
                where_clause = " AND ".join(conditions)

                query = f"SELECT date, open, high, low, close, volume, amount FROM stock_daily WHERE {where_clause} ORDER BY date"  # noqa: S608
                df = con.execute(query, params).df()

            # 设置日期为索引
            df.set_index('date', inplace=True)

            # 选择保存路径
            default_name = f"{self.stock_code}_{self.adjust}_duckdb_data.csv"
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "导出CSV",
                default_name,
                "CSV文件 (*.csv)"
            )

            if file_path:
                df.to_csv(file_path, encoding='utf-8-sig')
                QMessageBox.information(self, "成功", f"数据已导出到:\n{file_path}")

        except Exception as e:
            QMessageBox.critical(self, "错误", f"导出失败: {str(e)}")


class _FinancialFetchThread(QThread):
    """后台获取 xtdata 财务数据，避免主线程阻塞（㊺修复）"""
    data_ready = pyqtSignal(str, object)  # (table_name, result_dict_or_None)
    error_occurred = pyqtSignal(str)

    def __init__(self, stock_code: str, table_name: str):
        super().__init__()
        self._stock_code = stock_code
        self._table_name = table_name

    def run(self):
        try:
            from xtquant import xtdata
            xtdata.download_financial_data(
                stock_list=[self._stock_code], table_list=[self._table_name]
            )
            result = xtdata.get_financial_data(
                stock_list=[self._stock_code], table_list=[self._table_name],
                start_time="20200101", end_time="20260130", report_type='report_time',
            )
            self.data_ready.emit(self._table_name, result)
        except Exception as exc:
            self.error_occurred.emit(str(exc))


class FinancialDataViewerDialog(QDialog):
    """财务数据查看对话框"""

    def __init__(self, stock_code: str, parent=None):
        super().__init__(parent)
        self.stock_code = stock_code
        self._fetch_thread: Optional[QThread] = None
        self.setWindowTitle(f"查看财务数据 - {stock_code}")
        self.setMinimumSize(0, 0)
        self.init_ui()
        self.load_data()

    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout(self)

        # 顶部信息
        info_layout = QHBoxLayout()

        # 股票代码
        code_label = QLabel(f"股票代码: <b>{self.stock_code}</b>")
        code_label.setStyleSheet("font-size: 12pt;")
        info_layout.addWidget(code_label)

        # 数据表选择
        info_layout.addWidget(QLabel("数据表:"))
        self.table_combo = QComboBox()
        self.table_combo.addItems(["Balance (资产负债表)", "Income (利润表)", "CashFlow (现金流量表)", "Capitalization (股本结构)"])
        self.table_combo.currentIndexChanged.connect(self.load_data)
        info_layout.addWidget(self.table_combo)

        info_layout.addStretch()

        # 导出CSV按钮
        export_btn = QPushButton("📊 导出CSV")
        export_btn.clicked.connect(self.export_financial_csv)
        export_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 5px 12px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        info_layout.addWidget(export_btn)

        # 刷新按钮
        refresh_btn = QPushButton("🔄 刷新")
        refresh_btn.clicked.connect(self.load_data)
        refresh_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 5px 12px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        info_layout.addWidget(refresh_btn)

        # 关闭按钮
        close_btn = QPushButton("✖ 关闭")
        close_btn.clicked.connect(self.accept)
        close_btn.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                color: white;
                border: none;
                padding: 5px 12px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #da190b;
            }
        """)
        info_layout.addWidget(close_btn)

        layout.addLayout(info_layout)

        # 数据表格
        self.data_table = QTableView()
        self.data_table.setAlternatingRowColors(True)
        self.data_table.setSortingEnabled(True)
        layout.addWidget(self.data_table)

        # 统计信息
        self.stats_label = QLabel()
        self.stats_label.setStyleSheet("font-size: 10pt; color: #666;")
        layout.addWidget(self.stats_label)

    def load_data(self):
        """加载数据 — 后台线程执行 xtdata 调用（㊺修复）"""
        table_text = self.table_combo.currentText()
        table_map = {
            "Balance (资产负债表)": "Balance",
            "Income (利润表)": "Income",
            "CashFlow (现金流量表)": "CashFlow",
            "Capitalization (股本结构)": "Capitalization"
        }
        table_name = table_map.get(table_text, "Balance")

        self.data_table.setModel(_PandasTableModel(pd.DataFrame()))
        self.stats_label.setText("正在加载数据...")

        if self._fetch_thread and self._fetch_thread.isRunning():
            self._fetch_thread.requestInterruption()
        thread = _FinancialFetchThread(self.stock_code, table_name)
        thread.data_ready.connect(self._on_financial_data_ready)
        thread.error_occurred.connect(lambda msg: self._on_financial_error(msg))
        self._fetch_thread = thread
        thread.start()

    def _on_financial_data_ready(self, table_name: str, result):
        if isinstance(result, dict) and self.stock_code in result:
            stock_data = result[self.stock_code]
            if table_name in stock_data:
                table_data = stock_data[table_name]
                if isinstance(table_data, pd.DataFrame):
                    self._display_dataframe(table_data)
                elif isinstance(table_data, dict):
                    self._display_dict(table_data)
                else:
                    self.stats_label.setText(f"数据类型: {type(table_data)}")
            else:
                self.stats_label.setText(f"未找到 {table_name} 表数据")
        else:
            self.stats_label.setText("未找到财务数据")

    def _on_financial_error(self, msg: str):
        self.stats_label.setText(f"加载失败: {msg}")

    def _display_dataframe(self, df):
        """显示DataFrame"""
        model = _PandasTableModel(df)
        proxy = QSortFilterProxyModel(self)
        proxy.setSourceModel(model)
        self.data_table.setModel(proxy)
        self.data_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        # 更新统计信息
        total = len(df)
        self.stats_label.setText(f"总记录数: {total} 条")

    def _display_dict(self, data):
        """显示字典数据"""
        # 将字典转换为表格
        keys = list(data.keys())
        values = []
        for key in keys:
            value = data[key]
            value_str = str(value) if not isinstance(value, (list, dict)) else f"{type(value).__name__}({len(value)})"
            values.append(value_str)
        df = pd.DataFrame({"字段名": keys, "值": values})
        model = _PandasTableModel(df)
        proxy = QSortFilterProxyModel(self)
        proxy.setSourceModel(model)
        self.data_table.setModel(proxy)
        self.data_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.data_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.stats_label.setText(f"字段数量: {len(keys)} 个")

    def export_financial_csv(self):
        """导出财务数据为CSV — 先选路径再后台获取数据（㊺修复）"""
        table_text = self.table_combo.currentText()
        table_map = {
            "Balance (资产负债表)": "Balance",
            "Income (利润表)": "Income",
            "CashFlow (现金流量表)": "CashFlow",
            "Capitalization (股本结构)": "Capitalization"
        }
        table_name = table_map.get(table_text, "Balance")

        default_name = f"{self.stock_code}_{table_name}_财务数据.csv"
        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出财务数据CSV", default_name, "CSV文件 (*.csv)"
        )
        if not file_path:
            return

        self.stats_label.setText("正在导出数据...")

        def _do_export():
            try:
                from xtquant import xtdata
                xtdata.download_financial_data(
                    stock_list=[self.stock_code], table_list=[table_name]
                )
                result = xtdata.get_financial_data(
                    stock_list=[self.stock_code], table_list=[table_name],
                    start_time="20200101", end_time="20260130",
                    report_type='report_time',
                )
                if isinstance(result, dict) and self.stock_code in result:
                    stock_data = result[self.stock_code]
                    if table_name in stock_data:
                        table_data = stock_data[table_name]
                        if isinstance(table_data, pd.DataFrame):
                            df = table_data
                        elif isinstance(table_data, dict):
                            df = pd.DataFrame.from_dict(table_data, orient='index').T
                        else:
                            return
                        df.to_csv(file_path, encoding='utf-8-sig', index=True)
            except Exception:
                pass

        threading.Thread(target=_do_export, daemon=True).start()





if __name__ == '__main__':
    from PyQt5.QtWidgets import QApplication

    app = QApplication(sys.argv)
    window = LocalDataManagerWidget()
    window.setWindowTitle("本地数据管理")
    window.resize(1200, 800)
    window.show()
    sys.exit(app.exec_())
