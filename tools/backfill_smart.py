#!/usr/bin/env python
"""预筛 + 精准补录：先用 xtdata 秒级判断哪些 ETF 有数据，再只补录那些。

用法: python tools/backfill_smart.py
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
log = logging.getLogger("smart-backfill")


def main():
    from xtquant import xtdata
    xtdata.enable_hello = False
    from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path
    from data_manager.auto_data_updater import AutoDataUpdater

    db_path = resolve_duckdb_path()
    mgr = get_db_manager(db_path)

    # 1. 全部 ETF 代码
    all_etf = sorted(xtdata.get_stock_list_in_sector("沪深ETF"))
    log.info("全部 ETF: %d 只", len(all_etf))

    # 2. 查 DuckDB 已有代码
    with mgr.get_read_connection() as con:
        existing = set(
            con.execute("SELECT DISTINCT stock_code FROM stock_daily").fetchdf()["stock_code"].tolist()
        )
    log.info("DuckDB 已有 stock_daily: %d 只标的", len(existing))

    missing = [c for c in all_etf if c not in existing]
    log.info("缺失 ETF: %d 只", len(missing))
    if not missing:
        log.info("无需补录")
        return

    # 3. 用 xtdata 快速预筛（只拉最近 5 天日线看有没有）
    log.info("正在用 xtdata 预筛有数据的标的...")
    have_data = []
    no_data = []
    for i, code in enumerate(missing, 1):
        try:
            df = xtdata.get_market_data_ex(
                field_list=["close"],
                stock_list=[code],
                period="1d",
                count=5,
            )
            if code in df and len(df[code]) > 0:
                have_data.append(code)
            else:
                no_data.append(code)
        except Exception:
            no_data.append(code)
        if i % 200 == 0:
            log.info("  预筛进度: %d/%d, 有数据=%d", i, len(missing), len(have_data))

    log.info("预筛完成: 有数据=%d, 无数据=%d", len(have_data), len(no_data))

    if not have_data:
        log.info("全部缺失 ETF 在 xtdata 中均无数据，无法补录")
        return

    log.info("将补录 %d 只 ETF: %s ...", len(have_data), have_data[:10])

    # 4. 补录
    updater = AutoDataUpdater()
    updater.initialize_interface()
    if updater.interface is None:
        log.error("初始化失败")
        return

    start = time.time()
    result = updater.bulk_download(
        stock_codes=have_data,
        periods=["1d", "5m"],
        start_date="20100101",
    )
    elapsed = time.time() - start

    log.info(
        "补录完成: 成功=%d 失败=%d 记录=%d 耗时=%.1fs",
        result["success_stocks"], result["failed_stocks"],
        result["total_records"], elapsed,
    )

    # 5. 验证
    with mgr.get_read_connection() as con:
        for t in ["stock_daily", "stock_5m", "custom_period_bars"]:
            try:
                rc = con.execute("SELECT COUNT(*) FROM " + t).fetchone()[0]
                sc = con.execute("SELECT COUNT(DISTINCT stock_code) FROM " + t).fetchone()[0]
                log.info("  %s: %s 行, %s 只标的", t, f"{rc:,}", f"{sc:,}")
            except Exception as e:
                log.info("  %s: %s", t, e)

    # 6. 特别检查持仓
    holdings = ["511380.SH", "512890.SH", "513090.SH", "518880.SH"]
    with mgr.get_read_connection() as con:
        log.info("=== 持仓检查 ===")
        for h in holdings:
            try:
                cnt = con.execute(
                    "SELECT COUNT(*) FROM stock_daily WHERE stock_code = ?", [h]
                ).fetchone()[0]
                log.info("  %s: %d 条日线", h, cnt)
            except Exception as e:
                log.info("  %s: %s", h, e)


if __name__ == "__main__":
    main()
