#!/usr/bin/env python3
"""
定时自动数据补充模块
实现每日收盘后自动更新数据的功能

参考文档：duckdb.docx
定时数据补充模块：
通过内置的定时补充功能，你可以设定在每日收盘后（例如 15:30）自动运行补数任务。
系统会智能判断当前是否为交易日：
如果是，则自动拉取当日最新行情并入库；
如果是非交易日，则自动跳过。

配合系统托盘驻留功能，定时任务可以在后台静默运行，不干扰你的正常工作。
你完全无需感知，第二天打开软件时，数据已经是最新状态。
"""

import json
import logging
import importlib
import os
import sys
import threading
import time
from datetime import date, datetime
from datetime import time as dt_time
from zoneinfo import ZoneInfo

_SH = ZoneInfo('Asia/Shanghai')
from pathlib import Path
from typing import Optional

import duckdb
schedule = importlib.import_module("schedule")

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from smart_data_detector import TradingCalendar

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_auto_updater.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def _shift_time(hhmm: str, minutes: int) -> str:
    """将 'HH:MM' 字符串向后偏移 minutes 分钟，返回新 'HH:MM' 字符串。"""
    h, m = map(int, hhmm.split(":"))
    total = h * 60 + m + minutes
    return f"{(total // 60) % 24:02d}:{total % 60:02d}"


def _run_audit_chain_check() -> None:
    """每日收盘后的审计链完整性抽检（由 schedule 调用）。"""
    try:
        from tools.audit_chain_integrity_check import run_integrity_check
        run_integrity_check(strict=False)
    except Exception:
        logger.exception("审计链抽检任务执行失败")


def _run_cross_source_consistency_check() -> None:
    """每日更新完成后的跨数据源一致性抽检（由 schedule 调用）。"""
    try:
        from tools.check_cross_source_consistency import run_check
        report = run_check(sample_size=30, threshold=0.02, emit_alert=True)
        if report.get("alert"):
            logger.error(
                "跨源一致性告警: %d/%d 标的偏差超 2%% — %s",
                report.get("bad", 0),
                report.get("checked", 0),
                [d["code"] for d in report.get("details", [])[:5]],
            )
        else:
            logger.info(
                "跨源一致性校验通过: %d 标的抽检通过", report.get("checked", 0)
            )
    except Exception:
        logger.exception("跨源一致性抽检任务执行失败")


class AutoDataUpdater:
    """
    自动数据更新器

    功能：
    1. 定时任务（每日收盘后自动运行）
    2. 智能判断交易日
    3. 自动下载当日数据并入库
    4. 支持后台运行
    """

    def __init__(self,
                 duckdb_path: Optional[str] = None,
                 update_time: str = '15:30'):
        """
        初始化自动更新器

        Args:
            duckdb_path: DuckDB 数据库路径
            update_time: 每日更新时间（默认 15:30，收盘后）
        """
        from data_manager.duckdb_connection_pool import resolve_duckdb_path

        self.duckdb_path = resolve_duckdb_path(duckdb_path)
        self.update_time = update_time
        self.calendar = TradingCalendar()
        self.running = False
        self.thread = None
        self.data_manager = None
        self.interface = None

        # 统计信息
        self.last_update_time = None
        self.last_update_status = None
        self.total_updates = 0

        # 断点续传检查点文件（存放在 DuckDB 同目录下）
        self._checkpoint_path = Path(self.duckdb_path).parent / "data_updater_checkpoint.json"

    def initialize_data_manager(self):
        """延迟初始化 DataManager（避免循环导入）"""
        if self.data_manager is None:
            try:
                dm_module = importlib.import_module("gui_app.backtest.data_manager")
                dm_cls = getattr(dm_module, "DataManager")
                self.data_manager = dm_cls()
                logger.info("DataManager 初始化成功")
            except Exception as e:
                logger.error(f"DataManager 初始化失败: {e}")
                self.data_manager = None

    def initialize_interface(self):
        if self.interface is None:
            try:
                from data_manager.unified_data_interface import UnifiedDataInterface
                self.interface = UnifiedDataInterface(duckdb_path=self.duckdb_path, silent_init=True)
                self.interface.connect(read_only=False)
                logger.info("UnifiedDataInterface 初始化成功")
            except Exception as e:
                logger.error(f"UnifiedDataInterface 初始化失败: {e}")
                self.interface = None

    def is_trading_day(self, check_date: Optional[date] = None) -> bool:
        """
        判断是否为交易日

        Args:
            check_date: 要检查的日期，默认为今天

        Returns:
            是否为交易日
        """
        if check_date is None:
            check_date = datetime.now(tz=_SH).date()

        return self.calendar.is_trading_day(check_date)

    def should_update_today(self) -> bool:
        """
        判断今天是否需要更新数据

        条件：
        1. 是交易日
        2. 当前时间已过设定时间
        3. 今天的数据还没有更新过

        Returns:
            是否需要更新
        """
        today = datetime.now(tz=_SH).date()

        # 检查是否为交易日
        if not self.is_trading_day(today):
            logger.info(f"今天 {today} 不是交易日，跳过更新")
            return False

        # 检查是否已过设定时间
        now = datetime.now(tz=_SH)
        update_hour, update_minute = map(int, self.update_time.split(':'))
        update_time_today = datetime.combine(today, dt_time(update_hour, update_minute), _SH)

        if now < update_time_today:
            logger.info(f"未到设定更新时间 {self.update_time}，当前时间 {now.strftime('%H:%M')}")
            return False

        # 检查今天是否已经更新过
        if self.last_update_time == today:
            logger.info(f"今天 {today} 已更新过，跳过")
            return False

        return True

    def update_single_stock(self, stock_code: str) -> dict:
        """
        更新单只股票的数据

        Args:
            stock_code: 股票代码

        Returns:
            更新结果
        """
        result = {
            'stock_code': stock_code,
            'success': False,
            'message': '',
            'records': 0
        }

        if self.interface is None:
            self.initialize_interface()
        if self.interface is None:
            result['message'] = 'UnifiedDataInterface 未初始化'
            return result

        try:
            today_str = datetime.now(tz=_SH).date().strftime('%Y-%m-%d')
            plan = self.interface.build_incremental_plan(
                stock_code=stock_code,
                start_date=today_str,
                end_date=today_str,
                period='1d'
            )
            total_records = 0
            if not plan:
                result['message'] = '无更新计划'
                return result
            for item in plan:
                mode = item.get("mode")
                if mode == "skip":
                    continue
                df = self.interface.get_stock_data(
                    stock_code=stock_code,
                    start_date=item.get("start_date", today_str),
                    end_date=item.get("end_date", today_str),
                    period='1d',
                    auto_save=True
                )
                if df is not None and not df.empty:
                    total_records += len(df)
            if total_records == 0:
                result['message'] = '无数据'
                return result
            result['success'] = True
            result['records'] = total_records
            result['message'] = f'更新成功，{total_records} 条记录'

            logger.info(f"{stock_code}: {result['message']}")

        except Exception as e:
            result['message'] = f'更新失败: {e}'
            logger.error(f"{stock_code}: {result['message']}")

        return result

    # ── 全周期批量下载 ─────────────────────────────────────────────────────────

    #: 默认全量入库周期（可由调用方覆盖）
    ALL_PERIODS: list[str] = ["1d", "1m", "5m"]

    def get_listing_date(self, stock_code: str) -> str:
        """获取股票/期货上市首个交易日。

        优先级：XTQuant get_instrument_detail → DuckDB stock_daily 最早记录 → '1990-01-01'。
        返回 'YYYY-MM-DD' 字符串。
        """
        # 1. XTQuant 在线获取（OpenDate 为股票 IPO 日，CreateDate 为期货上市日）
        if os.environ.get("EASYXT_ENABLE_XT_LISTING_DATE", "0") in ("1", "true", "True"):
            try:
                from xtquant import xtdata
                detail = xtdata.get_instrument_detail(stock_code)
                if detail:
                    raw = detail.get("OpenDate") or detail.get("CreateDate")
                    if raw:
                        s = str(int(raw)).strip()
                        if len(s) == 8:
                            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
            except Exception:
                pass

        # 2. DuckDB stock_daily 最早记录
        if self.interface is not None and getattr(self.interface, "con", None) is not None:
            try:
                row = self.interface.con.execute(
                    "SELECT MIN(date) AS d FROM stock_daily WHERE stock_code = ? AND period = '1d'",
                    [stock_code],
                ).df()
                import pandas as _pd
                if not row.empty and _pd.notna(row["d"].iloc[0]):
                    return _pd.to_datetime(row["d"].iloc[0]).strftime("%Y-%m-%d")
            except Exception:
                pass

        # 3. 兜底：中国最早上市时间（1990 年沪深交易所成立）
        return "1990-01-01"

    def update_all_periods_for_stock(
        self,
        stock_code: str,
        periods: Optional[list[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict:
        """下载并入库单只股票的所有指定周期数据。

        Args:
            stock_code: 股票代码，如 '000001.SZ'。
            periods: 要下载的周期列表，默认 ALL_PERIODS。
            start_date: 起始日期（含），格式 'YYYY-MM-DD'；None 表示尽量拉全历史。
            end_date: 结束日期（含），格式 'YYYY-MM-DD'；None 表示今日。

        Returns:
            {
              'stock_code': str,
              'periods': {period: {'success': bool, 'records': int, 'message': str}},
              'total_records': int,
              'success_periods': int,
            }
        """
        if periods is None:
            periods = self.ALL_PERIODS

        if self.interface is None:
            self.initialize_interface()
        if self.interface is None:
            return {
                'stock_code': stock_code,
                'periods': {},
                'total_records': 0,
                'success_periods': 0,
                'message': 'UnifiedDataInterface 未初始化',
            }

        today_str = datetime.now(tz=_SH).date().strftime('%Y-%m-%d')
        if end_date is None:
            end_date = today_str
        # 默认起始日期：1d 从上市首日起完整下载（刚性约束），分钟线近 1 年
        _listing_date = self.get_listing_date(stock_code)  # 动态获取上市首日
        _default_start_intraday = (
            datetime.now(tz=_SH).replace(year=datetime.now(tz=_SH).year - 1)
            .strftime('%Y-%m-%d')
        )

        period_results: dict[str, dict] = {}
        total_records = 0
        success_periods = 0

        for period in periods:
            if start_date is None:
                # 1d 必须从上市首日下载完整历史（多日自定义周期左对齐的刚性前提）
                _start = _listing_date if period == '1d' else _default_start_intraday
            else:
                _start = start_date

            pr: dict = {'success': False, 'records': 0, 'message': ''}
            try:
                df = self.interface.get_stock_data(
                    stock_code=stock_code,
                    start_date=_start,
                    end_date=end_date,
                    period=period,
                    auto_save=True,
                )
                if df is not None and not df.empty:
                    pr['success'] = True
                    pr['records'] = len(df)
                    pr['message'] = f'{len(df)} 条'
                    total_records += len(df)
                    success_periods += 1
                else:
                    pr['message'] = '无数据'
            except Exception as exc:
                pr['message'] = f'失败: {exc}'
                logger.error("%s|%s 下载失败: %s", stock_code, period, exc)

            period_results[period] = pr
            logger.info("%s|%s: %s", stock_code, period, pr['message'])

        return {
            'stock_code': stock_code,
            'periods': period_results,
            'total_records': total_records,
            'success_periods': success_periods,
        }

    def bulk_download(
        self,
        stock_codes: Optional[list[str]] = None,
        periods: Optional[list[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        on_progress=None,
        stop_event: Optional[threading.Event] = None,
    ) -> dict:
        """批量下载全部标的、全周期数据并入 DuckDB。

        Args:
            stock_codes: 要下载的股票代码列表；None 表示从数据库或内置列表自动获取全部。
            periods: 要下载的周期列表；None 表示 ALL_PERIODS。
            start_date: 起始日期；None 表示各周期默认历史起点。
            end_date: 结束日期；None 表示今日。
            on_progress: 可选回调 fn(current, total, stock_code, period, status)，
                         每完成一只股票时调用一次。
            stop_event: threading.Event，置位后终止任务。

        Returns:
            {
              'total_stocks': int,
              'success_stocks': int,
              'failed_stocks': int,
              'total_records': int,
              'results': list[dict],
            }
        """
        if periods is None:
            periods = self.ALL_PERIODS

        if stock_codes is None:
            stock_codes = self._get_all_stock_codes()

        logger.info("bulk_download 开始: %d 只股票, 周期=%s", len(stock_codes), periods)

        if self.interface is None:
            self.initialize_interface()
        if self.interface is None:
            return {
                'total_stocks': len(stock_codes),
                'success_stocks': 0,
                'failed_stocks': len(stock_codes),
                'total_records': 0,
                'results': [],
                'message': 'UnifiedDataInterface 未初始化',
            }

        results: list[dict] = []
        success_stocks = 0
        failed_stocks = 0
        total_records = 0
        total = len(stock_codes)

        for idx, code in enumerate(stock_codes):
            if stop_event is not None and stop_event.is_set():
                logger.info("bulk_download 检测到停止信号，已处理 %d/%d", idx, total)
                break

            stock_result = self.update_all_periods_for_stock(
                stock_code=code,
                periods=periods,
                start_date=start_date,
                end_date=end_date,
            )
            results.append(stock_result)
            rec = stock_result.get('total_records', 0)
            total_records += rec
            if stock_result.get('success_periods', 0) > 0:
                success_stocks += 1
                status = 'ok'
            else:
                failed_stocks += 1
                status = 'failed'

            if on_progress is not None:
                try:
                    on_progress(idx + 1, total, code, ','.join(periods), status)
                except Exception:
                    pass

        # 下载完成后广播事件（不依赖 Qt，用懒导入避免循环引用）
        try:
            from core.signal_bus import signal_bus as _sb
            from core.events import Events as _Ev
            _sb.emit(_Ev.DATA_INGESTION_COMPLETE,
                     stock_codes=stock_codes, periods=periods,
                     success_count=success_stocks, failed_count=failed_stocks)
        except Exception as _emit_err:
            logger.debug("广播 DATA_INGESTION_COMPLETE 失败: %s", _emit_err)

        logger.info(
            "bulk_download 完成: 成功=%d 失败=%d 总记录=%d",
            success_stocks, failed_stocks, total_records,
        )
        return {
            'total_stocks': total,
            'success_stocks': success_stocks,
            'failed_stocks': failed_stocks,
            'total_records': total_records,
            'results': results,
        }

    # ── 断点续传辅助方法 ──────────────────────────────────────────────────────

    def _save_checkpoint(
        self,
        batch_date: str,
        last_index: int,
        total: int,
        success_count: int,
        failed_count: int,
        failed_stocks: list[str],
    ) -> None:
        """原子写入当前批次进度到检查点文件。"""
        data = {
            "batch_date": batch_date,
            "last_index": last_index,
            "total": total,
            "success_count": success_count,
            "failed_count": failed_count,
            "failed_stocks": failed_stocks,
            "saved_at": datetime.now(tz=_SH).isoformat(),
        }
        tmp = self._checkpoint_path.with_suffix(".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            tmp.replace(self._checkpoint_path)
        except Exception as e:
            logger.warning("检查点保存失败: %s", e)
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    def _load_checkpoint(self, batch_date: str) -> dict:
        """加载检查点，仅当 batch_date 与当前日期一致时返回有效数据。"""
        try:
            if self._checkpoint_path.exists():
                with open(self._checkpoint_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if data.get("batch_date") == batch_date:
                    return data
        except Exception as e:
            logger.warning("检查点加载失败: %s", e)
        return {}

    def update_all_stocks(self, stock_codes: Optional[list[str]] = None) -> dict:
        """
        更新所有股票的数据

        Args:
            stock_codes: 要更新的股票列表，None 表示更新全部

        Returns:
            更新结果汇总
        """
        logger.info("=" * 60)
        logger.info("开始自动数据更新")
        logger.info("=" * 60)

        # 断点续传：仅全量自动模式（stock_codes=None）支持
        _is_auto_mode = stock_codes is None
        if stock_codes is None:
            # 从数据库获取所有股票代码
            stock_codes = self._get_all_stock_codes()
            logger.info(f"从数据库获取到 {len(stock_codes)} 只股票")

        # 确保 DataManager 已初始化
        self.initialize_data_manager()

        if self.data_manager is None:
            logger.error("无法初始化 DataManager，取消更新")
            return {'success': False, 'message': 'DataManager 初始化失败'}

        # 断点续传：加载当日检查点
        batch_date = datetime.now(tz=_SH).date().isoformat()
        start_from = 0
        success_count = 0
        failed_count = 0
        failed_stocks_list: list[str] = []
        if _is_auto_mode:
            ckpt = self._load_checkpoint(batch_date)
            if ckpt and ckpt.get("last_index", -1) >= 0:
                start_from = ckpt["last_index"] + 1
                success_count = ckpt.get("success_count", 0)
                failed_count = ckpt.get("failed_count", 0)
                failed_stocks_list = ckpt.get("failed_stocks", [])
                logger.info(
                    "断点续传：从第 %d 只股票继续（共 %d 只，已完成 %d 只）",
                    start_from + 1, len(stock_codes), start_from,
                )

        # 更新每只股票
        _CHECKPOINT_EVERY = 50  # 每处理 N 只股票保存一次检查点
        results = []

        for i, stock_code in enumerate(stock_codes):
            if i < start_from:
                continue

            logger.info(f"[{i + 1}/{len(stock_codes)}] 更新 {stock_code}...")

            result = self.update_single_stock(stock_code)
            results.append(result)

            if result['success']:
                success_count += 1
            else:
                failed_count += 1
                failed_stocks_list.append(stock_code)

            # 每 N 只或最后一只时保存检查点（仅全量自动模式）
            if _is_auto_mode and (
                (i + 1) % _CHECKPOINT_EVERY == 0 or i == len(stock_codes) - 1
            ):
                self._save_checkpoint(
                    batch_date, i, len(stock_codes),
                    success_count, failed_count, failed_stocks_list,
                )

            # 避免请求过于频繁
            time.sleep(0.1)

        # 更新统计信息
        self.last_update_time = datetime.now(tz=_SH).date()
        self.last_update_status = 'success' if failed_count == 0 else 'partial'
        self.total_updates += 1

        # 打印汇总
        logger.info("=" * 60)
        logger.info("更新完成")
        logger.info(f"总计: {len(stock_codes)} 只")
        logger.info(f"成功: {success_count}")
        logger.info(f"失败: {failed_count}")
        logger.info("=" * 60)

        return {
            'total': len(stock_codes),
            'success': success_count,
            'failed': failed_count,
            'results': results
        }

    def _get_all_stock_codes(self) -> list[str]:
        """从数据库获取所有股票代码"""
        try:
            from data_manager.duckdb_connection_pool import get_db_manager
            manager = get_db_manager(self.duckdb_path)
            with manager.get_read_connection() as con:
                df = con.execute("""
                    SELECT DISTINCT stock_code
                    FROM stock_daily
                    ORDER BY stock_code
                """).fetchdf()
            codes = df['stock_code'].tolist()
            if codes:
                return codes
            loader_module = importlib.import_module("data_manager.board_stocks_loader")
            loader_cls = getattr(loader_module, "BoardStocksLoader")
            loader = loader_cls()
            fresh_codes = loader.get_board_stocks("all")
            if isinstance(fresh_codes, list):
                return fresh_codes
            return []

        except Exception as e:
            logger.error(f"获取股票代码失败: {e}")
            return []

    def run_update_task(self):
        """执行更新任务"""
        try:
            logger.info("定时任务触发")

            # 判断是否需要更新
            if not self.should_update_today():
                return

            # 执行更新
            self.update_all_stocks()

        except Exception as e:
            logger.error(f"更新任务执行失败: {e}", exc_info=True)

    def _run_quarantine_replay_task(self) -> None:
        """定时执行隔离队列重放，并上报成功率（由 schedule 调用）。"""
        try:
            self.initialize_interface()
            if self.interface is None:
                logger.warning("quarantine replay 跳过：UnifiedDataInterface 未初始化")
                return
            result = self.interface.run_quarantine_replay(limit=50, max_retries=3)
            q_counts = {}
            try:
                q_counts = self.interface.get_quarantine_status_counts()
            except Exception:
                q_counts = {}
            dead_letter_total = int((q_counts or {}).get("dead_letter", 0) or 0)
            quarantine_total = int((q_counts or {}).get("total", 0) or 0)
            dl_abs_warn = int(os.environ.get("EASYXT_QUARANTINE_DEADLETTER_WARN", "100") or 100)
            dl_ratio_warn = float(os.environ.get("EASYXT_QUARANTINE_DEADLETTER_RATIO_WARN", "0.01") or 0.01)
            dl_ratio = (dead_letter_total / quarantine_total) if quarantine_total > 0 else 0.0
            if result["processed"] > 0:
                total = result["processed"]
                ok = result["succeeded"]
                rate = ok / total * 100 if total else 0.0
                logger.info(
                    "quarantine replay 完成: processed=%d succeeded=%d failed=%d "
                    "dead_letter=%d success_rate=%.1f%% total_dead_letter=%d total_quarantine=%d",
                    total, ok, result["failed"], result["dead_letter"], rate,
                    dead_letter_total, quarantine_total,
                )
            else:
                logger.debug("quarantine replay: 队列为空，无待处理项")
            if dead_letter_total >= dl_abs_warn or dl_ratio >= dl_ratio_warn:
                logger.critical(
                    "quarantine dead_letter 超阈值: dead_letter_total=%d total=%d ratio=%.2f%% "
                    "threshold_abs=%d threshold_ratio=%.2f%%",
                    dead_letter_total,
                    quarantine_total,
                    dl_ratio * 100.0,
                    dl_abs_warn,
                    dl_ratio_warn * 100.0,
                )
        except Exception:
            logger.exception("quarantine replay 定时任务执行失败")

    def _run_financial_data_update_task(self) -> None:
        """季报财务数据更新（收盘后 20 分钟，仅季报披露活跃月份执行）。

        活跃月份（A 股定期报告披露窗口）：
          1-2 月（三季报补充/半年报）, 4 月（年报）, 8 月（半年报）, 10-11 月（三季报）
        数据源：优先 QMT xtdata.get_financial_data；QMT 不可用时跳过。
        """
        try:
            today = datetime.now(tz=_SH).date()
            FINANCIAL_MONTHS = {1, 2, 4, 8, 10, 11}
            if today.month not in FINANCIAL_MONTHS:
                logger.debug(
                    "财务数据更新: 当前月份 %d 不在季报披露期，跳过", today.month
                )
                return

            self.initialize_interface()
            if self.interface is None:
                logger.warning("财务数据更新: UnifiedDataInterface 未初始化，跳过")
                return

            from data_manager.financial_data_saver import FinancialDataSaver
            from data_manager.duckdb_connection_pool import get_db_manager
            import pandas as pd

            db_mgr = get_db_manager(self.duckdb_path)
            saver = FinancialDataSaver(db_mgr)

            stock_codes = self._get_all_stock_codes()
            if not stock_codes:
                logger.info("财务数据更新: 无股票代码，跳过")
                return

            max_stocks = int(os.environ.get("EASYXT_FINANCIAL_BATCH_SIZE", "50") or 50)
            batch = stock_codes[:max_stocks]

            qmt_ok = getattr(self.interface, "qmt_available", False)
            if not qmt_ok:
                # ── Tushare 降级路径 ────────────────────────────────────────
                ts_token = (
                    os.environ.get("EASYXT_TUSHARE_TOKEN", "").strip()
                    or os.environ.get("TUSHARE_TOKEN", "").strip()
                )
                if not ts_token:
                    logger.info(
                        "财务数据更新: QMT 不可用且 EASYXT_TUSHARE_TOKEN 未设置，跳过"
                    )
                    return
                logger.info(
                    "财务数据更新: QMT 不可用，降级使用 Tushare（批量=%d）", len(batch)
                )
                updated = 0
                skipped = 0
                for code in batch:
                    try:
                        result = saver.save_from_tushare(code)
                        if result.get("success"):
                            updated += 1
                        else:
                            logger.debug(
                                "财务数据更新(Tushare) %s 未写入: %s",
                                code, result.get("error"),
                            )
                            skipped += 1
                    except Exception as exc:
                        logger.debug("财务数据更新(Tushare) %s 异常: %s", code, exc)
                        skipped += 1
                logger.info(
                    "财务数据更新完成(Tushare): 成功=%d 跳过=%d 总=%d (月份=%d)",
                    updated, skipped, len(batch), today.month,
                )
                return
            # ── QMT 主路径 ──────────────────────────────────────────────────
            updated = 0
            skipped = 0
            for code in batch:
                try:
                    from xtquant import xtdata  # type: ignore[import]
                    raw = xtdata.get_financial_data(
                        stock_list=[code],
                        table_list=["Income", "Balance", "CashFlow"],
                        start_time="",
                        end_time="",
                    )
                    stock_raw = raw.get(code, {}) if raw else {}
                    income_df = stock_raw.get("Income", pd.DataFrame())
                    balance_df = stock_raw.get("Balance", pd.DataFrame())
                    cashflow_df = stock_raw.get("CashFlow", pd.DataFrame())
                    result = saver.save_from_qmt(code, income_df, balance_df, cashflow_df)
                    if result.get("success"):
                        updated += 1
                    else:
                        skipped += 1
                except Exception as exc:
                    logger.debug("财务数据更新 %s 跳过: %s", code, exc)
                    skipped += 1

            logger.info(
                "财务数据更新完成: 成功=%d 跳过=%d 总=%d (月份=%d)",
                updated, skipped, len(batch), today.month,
            )
        except Exception:
            logger.exception("财务数据更新任务执行失败")

    def start(self):
        """启动定时更新服务"""
        if self.running:
            logger.warning("定时更新服务已在运行")
            return

        # ── P1: 启动前环境完整性校验（fail-fast）─────────────────────────────
        try:
            from data_manager import validate_environment
            _env = validate_environment(raise_on_error=False)
            _errors = {k: v for k, v in _env.items() if v.startswith("ERROR")}
            _warns = {k: v for k, v in _env.items() if v.startswith("WARN")}
            if _errors:
                err_list = "; ".join(f"{k}: {v}" for k, v in _errors.items())
                logger.critical("启动环境校验失败，终止数据更新服务: %s", err_list)
                raise RuntimeError(f"环境校验失败，请修复后重试 — {err_list}")
            if _warns:
                logger.warning("启动环境校验有告警（不影响启动）: %s", _warns)
            else:
                logger.info("启动环境校验通过")
        except RuntimeError:
            raise  # 原样向上抛出，终止启动
        except Exception as _e:
            logger.warning("validate_environment 调用异常，跳过校验: %s", _e)

        logger.info(f"启动定时更新服务，更新时间: {self.update_time}")

        # 设置定时任务
        schedule.every().day.at(self.update_time).do(self.run_update_task)
        # 收盘后 5 分钟进行审计链完整性抽检（独立于数据更新）
        _audit_time = _shift_time(self.update_time, minutes=5)
        schedule.every().day.at(_audit_time).do(_run_audit_chain_check)
        logger.info("审计链抽检任务已注册，执行时间: %s", _audit_time)
        # 收盘后 10 分钟进行跨数据源一致性抽检
        _cross_time = _shift_time(self.update_time, minutes=10)
        schedule.every().day.at(_cross_time).do(_run_cross_source_consistency_check)
        logger.info("跨源一致性抽检任务已注册，执行时间: %s", _cross_time)
        # 收盘后 15 分钟执行一次 quarantine replay（处置当日新增隔离项）
        _replay_time = _shift_time(self.update_time, minutes=15)
        schedule.every().day.at(_replay_time).do(self._run_quarantine_replay_task)
        logger.info("quarantine replay 每日任务已注册，执行时间: %s", _replay_time)
        # 每小时执行一次 quarantine replay（处理盘中/非收盘时段积压）
        schedule.every().hour.do(self._run_quarantine_replay_task)
        logger.info("quarantine replay 每小时任务已注册")
        # 收盘后 20 分钟更新季报财务数据（仅季报披露活跃月份实际执行）
        _financial_time = _shift_time(self.update_time, minutes=20)
        schedule.every().day.at(_financial_time).do(self._run_financial_data_update_task)
        logger.info("季报财务数据更新任务已注册，执行时间: %s", _financial_time)

        # 启动后台线程
        self.running = True
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()

        logger.info("定时更新服务已启动（后台运行）")

    def _run_scheduler(self):
        """运行调度器（在后台线程中）"""
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次

    def stop(self):
        """停止定时更新服务"""
        logger.info("停止定时更新服务")
        self.running = False

        if self.thread:
            self.thread.join(timeout=5)
            self.thread = None

        schedule.clear()

    def get_status(self) -> dict:
        """获取更新服务状态"""
        return {
            'running': self.running,
            'update_time': self.update_time,
            'last_update': str(self.last_update_time) if self.last_update_time else None,
            'last_status': self.last_update_status,
            'total_updates': self.total_updates,
            'is_trading_day': self.is_trading_day(),
            'should_update': self.should_update_today()
        }

    def manual_update(self, stock_codes: Optional[list[str]] = None) -> dict:
        """
        手动触发更新（用于测试）

        Args:
            stock_codes: 要更新的股票列表

        Returns:
            更新结果
        """
        logger.info("手动触发数据更新")
        self.initialize_data_manager()
        return self.update_all_stocks(stock_codes)


def test_auto_updater():
    """测试自动更新功能"""
    print("=" * 60)
    print("自动数据更新测试")
    print("=" * 60)
    print()

    # 创建更新器
    updater = AutoDataUpdater(update_time='15:30')

    # 显示状态
    print("当前状态:")
    status = updater.get_status()
    for key, value in status.items():
        print(f"  {key}: {value}")
    print()

    # 判断是否应该更新
    print("判断是否应该更新:")
    should = updater.should_update_today()
    print(f"  应该更新: {should}")
    print(f"  原因: {'是交易日且已到设定时间' if should else '不是交易日或未到设定时间'}")
    print()

    # 手动触发一次更新（测试）
    print("手动触发更新（测试）:")
    result = updater.manual_update(['511380.SH', '511880.SH'])
    print(f"  更新结果: 成功 {result['success']}, 失败 {result['failed']}")
    print()

    # 显示更新后的状态
    print("更新后的状态:")
    status = updater.get_status()
    for key, value in status.items():
        print(f"  {key}: {value}")

    print()
    print("[OK] 测试完成")


def start_auto_update_service():
    """启动自动更新服务（生产环境使用）"""
    print("=" * 60)
    print("启动自动数据更新服务")
    print("=" * 60)
    print()

    updater = AutoDataUpdater(update_time='15:30')

    try:
        updater.start()

        print("服务已启动，按 Ctrl+C 停止")

        # 保持主线程运行
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n收到停止信号")
        updater.stop()
        print("服务已停止")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--start':
        # 启动服务
        start_auto_update_service()
    else:
        # 运行测试
        test_auto_updater()
