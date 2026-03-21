#!/usr/bin/env python
"""精准补录脚本：只处理 DuckDB 中缺失数据的 ETF/股票。

用法: python tools/补录缺失.py
"""
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("backfill")


def main():
    from xtquant import xtdata
    xtdata.enable_hello = False
    from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path
    from data_manager.auto_data_updater import AutoDataUpdater

    db_path = resolve_duckdb_path()
    mgr = get_db_manager(db_path)

    # 获取全部 ETF 代码
    all_etf = sorted(xtdata.get_stock_list_in_sector("沪深ETF"))
    log.info("全部 ETF: %d 只", len(all_etf))

    # 查询已入库的代码
    with mgr.get_read_connection() as con:
        existing = set(
            con.execute("SELECT DISTINCT stock_code FROM stock_daily").fetchdf()["stock_code"].tolist()
        )
    log.info("DuckDB 已有 stock_daily: %d 只标的", len(existing))

    # 找出缺失的
    missing = [c for c in all_etf if c not in existing]
    log.info("缺失 ETF: %d 只, 将尝试补录", len(missing))

    if not missing:
        log.info("无需补录")
        return

    updater = AutoDataUpdater()
    updater.initialize_interface()
    if updater.interface is None:
        log.error("初始化失败")
        return

    start = time.time()
    result = updater.bulk_download(
        stock_codes=missing,
        periods=["1d", "5m"],
        start_date="20100101",
    )
    elapsed = time.time() - start

    log.info(
        "补录完成: 成功=%d 失败=%d 记录=%d 耗时=%.1fs",
        result["success_stocks"], result["failed_stocks"],
        result["total_records"], elapsed,
    )

    # 验证
    with mgr.get_read_connection() as con:
        for t in ["stock_daily", "stock_5m", "custom_period_bars"]:
            try:
                rc = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                sc = con.execute(f"SELECT COUNT(DISTINCT stock_code) FROM {t}").fetchone()[0]
                log.info("  %s: %s 行, %s 只标的", t, f"{rc:,}", f"{sc:,}")
            except Exception as e:
                log.info("  %s: %s", t, e)


if __name__ == "__main__":
    main()
