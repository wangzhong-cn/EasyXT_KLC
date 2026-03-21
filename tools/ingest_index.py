#!/usr/bin/env python
"""指数全量入库脚本（xtdata 预筛 + 分批）。

用法: python tools/ingest_index.py
"""
import json
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ingest_index.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("ingest-index")


def main():
    from xtquant import xtdata
    xtdata.enable_hello = False
    from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path
    from data_manager.auto_data_updater import AutoDataUpdater

    db_path = resolve_duckdb_path()
    mgr = get_db_manager(db_path)

    # 1. 获取指数代码
    codes = sorted(xtdata.get_stock_list_in_sector("沪深指数"))
    log.info("全部指数: %d 只", len(codes))

    # 2. DuckDB 已有
    with mgr.get_read_connection() as con:
        existing = set(
            con.execute("SELECT DISTINCT stock_code FROM stock_daily").fetchdf()["stock_code"].tolist()
        )
    log.info("DuckDB 已有 stock_daily: %d 只标的", len(existing))

    missing = [c for c in codes if c not in existing]
    log.info("缺失指数: %d 只", len(missing))

    if not missing:
        log.info("无需处理")
        return

    # 3. xtdata 预筛
    log.info("正在用 xtdata 预筛...")
    have_data = []
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
        except Exception:
            pass
        if i % 200 == 0:
            log.info("  预筛进度: %d/%d, 有数据=%d", i, len(missing), len(have_data))

    log.info("预筛完成: 有数据=%d, 无数据=%d", len(have_data), len(missing) - len(have_data))

    if not have_data:
        log.info("全部缺失指数在 xtdata 中均无数据")
        return

    # 4. 分批入库
    BATCH = 100
    total_batches = (len(have_data) + BATCH - 1) // BATCH
    log.info("将分 %d 批入库 %d 只指数", total_batches, len(have_data))

    updater = AutoDataUpdater()
    updater.initialize_interface()
    if updater.interface is None:
        log.error("初始化失败")
        return

    t0 = time.time()
    total_success = 0
    total_failed = 0
    total_records = 0

    for batch_idx in range(total_batches):
        batch = have_data[batch_idx * BATCH : (batch_idx + 1) * BATCH]
        log.info("=== 批次 %d/%d: %d 只 [%s ... %s] ===",
                 batch_idx + 1, total_batches, len(batch), batch[0], batch[-1])

        result = updater.bulk_download(
            stock_codes=batch,
            periods=["1d", "5m"],
            start_date="20100101",
        )

        total_success += result["success_stocks"]
        total_failed += result["failed_stocks"]
        total_records += result["total_records"]

        elapsed = time.time() - t0
        remaining = (total_batches - batch_idx - 1) * (elapsed / (batch_idx + 1))
        log.info(
            "批次完成: 成功=%d 失败=%d 记录=%d | 累计: 成功=%d 失败=%d ETA=%.0fm",
            result["success_stocks"], result["failed_stocks"], result["total_records"],
            total_success, total_failed, remaining / 60,
        )

    log.info("=== 指数入库全部完成 ===")
    log.info("累计: 成功=%d 失败=%d 记录=%d 耗时=%.1fs",
             total_success, total_failed, total_records, time.time() - t0)

    # 5. 验证
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
