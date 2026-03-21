#!/usr/bin/env python
"""A股全量入库脚本（xtdata 预筛 + 分批写入 + 断点续传）。

用法: python tools/ingest_ashare.py
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
        logging.FileHandler("ingest_ashare.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("ingest-ashare")

CHECKPOINT = Path("data/ashare_ingest_ckpt.json")


def load_ckpt():
    if CHECKPOINT.exists():
        return json.loads(CHECKPOINT.read_text(encoding="utf-8"))
    return {"done_codes": [], "success": 0, "failed": 0, "records": 0}


def save_ckpt(ckpt):
    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT.write_text(json.dumps(ckpt, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    from xtquant import xtdata
    xtdata.enable_hello = False
    from data_manager.auto_data_updater import AutoDataUpdater
    from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

    db_path = resolve_duckdb_path()
    mgr = get_db_manager(db_path)

    # 1. 获取全部 A 股代码
    sh_stocks = xtdata.get_stock_list_in_sector("沪深A股")
    log.info("全部 A 股: %d 只", len(sh_stocks))

    # 2. DuckDB 已有
    with mgr.get_read_connection() as con:
        existing = set(
            con.execute("SELECT DISTINCT stock_code FROM stock_daily").fetchdf()["stock_code"].tolist()
        )
    log.info("DuckDB 已有 stock_daily: %d 只标的", len(existing))

    # checkpoint 已完成的也排除
    ckpt = load_ckpt()
    done_set = set(ckpt["done_codes"])
    log.info("Checkpoint 已完成: %d 只", len(done_set))

    missing = sorted([c for c in sh_stocks if c not in existing and c not in done_set])
    log.info("待处理 A 股: %d 只", len(missing))

    if not missing:
        log.info("无需处理")
        return

    # 3. xtdata 预筛
    log.info("正在用 xtdata 预筛 (拉最近5天日线)...")
    have_data = []
    no_data_codes = []
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
                no_data_codes.append(code)
        except Exception:
            no_data_codes.append(code)
        if i % 1000 == 0:
            log.info("  预筛进度: %d/%d, 有数据=%d", i, len(missing), len(have_data))

    log.info("预筛完成: 有数据=%d, 无数据=%d", len(have_data), len(no_data_codes))
    # 无数据的标记为已完成（避免重复筛）
    ckpt["done_codes"].extend(no_data_codes)
    save_ckpt(ckpt)

    if not have_data:
        log.info("全部待处理 A 股在 xtdata 中均无数据")
        return

    # 4. 分批入库
    BATCH = 200
    total_batches = (len(have_data) + BATCH - 1) // BATCH
    log.info("将分 %d 批入库 %d 只 A 股", total_batches, len(have_data))

    updater = AutoDataUpdater()
    updater.initialize_interface()
    if updater.interface is None:
        log.error("初始化失败")
        return

    ingest_lock = Path(str(db_path) + ".ingest.lock")

    def _rotate_interface_connection() -> None:
        if updater.interface is None:
            return
        ingest_lock.unlink(missing_ok=True)  # 释放：删哨兵，此时 DB 文件无持久写锁
        try:
            updater.interface._close_duckdb_connection()
        except Exception:
            pass
        time.sleep(5.0)
        if not updater.interface.connect(read_only=False):
            raise RuntimeError("批次连接重建失败")
        ingest_lock.touch()  # 重新持有：建哨兵

    t0 = time.time()
    total_success = ckpt["success"]
    total_failed = ckpt["failed"]
    total_records = ckpt["records"]

    ingest_lock.touch()  # 入库开始
    for batch_idx in range(total_batches):
        batch = have_data[batch_idx * BATCH : (batch_idx + 1) * BATCH]
        log.info("=== 批次 %d/%d: %d 只 [%s ... %s] ===",
                 batch_idx + 1, total_batches, len(batch), batch[0], batch[-1])

        bt0 = time.time()
        result = updater.bulk_download(
            stock_codes=batch,
            periods=["1d", "5m"],
            start_date="20100101",
        )
        bt1 = time.time()

        total_success += result["success_stocks"]
        total_failed += result["failed_stocks"]
        total_records += result["total_records"]

        ckpt["done_codes"].extend(batch)
        ckpt["success"] = total_success
        ckpt["failed"] = total_failed
        ckpt["records"] = total_records
        save_ckpt(ckpt)

        elapsed = bt1 - bt0
        overall = bt1 - t0
        remaining = (total_batches - batch_idx - 1) * (overall / (batch_idx + 1))
        log.info(
            "批次完成: 成功=%d 失败=%d 记录=%d 耗时=%.0fs | 累计: 成功=%d 失败=%d ETA=%.0fm",
            result["success_stocks"], result["failed_stocks"], result["total_records"],
            elapsed, total_success, total_failed, remaining / 60,
        )
        if batch_idx < total_batches - 1:
            _rotate_interface_connection()
    ingest_lock.unlink(missing_ok=True)  # 入库完成清理

    log.info("=== A 股入库全部完成 ===")
    log.info("累计: 成功=%d 失败=%d 记录=%d 耗时=%.1fs",
             total_success, total_failed, total_records, time.time() - t0)

    # 5. 验证
    with mgr.get_read_connection() as con:
        for t in ["stock_daily", "stock_5m", "custom_period_bars"]:
            try:
                rc = con.execute("SELECT COUNT(*) FROM " + t).fetchone()[0]
                sc = con.execute("SELECT COUNT(DISTINCT stock_code) FROM " + t).fetchone()[0]
                log.info("  %s: %s 行, %s 只标的", t, f"{rc:,}", f"{sc:,}")
            except Exception as e:
                log.info("  %s: %s", t, e)


if __name__ == "__main__":
    main()
