#!/usr/bin/env python
"""全量数据入库脚本 — 分阶段断点续传。

用法:
    python tools/full_bulk_ingest.py                    # 全量（A股+ETF+指数）
    python tools/full_bulk_ingest.py --stage etf        # 仅ETF
    python tools/full_bulk_ingest.py --stage a_share    # 仅A股
    python tools/full_bulk_ingest.py --resume            # 从检查点续传
    python tools/full_bulk_ingest.py --batch-size 200   # 每批200只

阶段顺序:  etf → a_share → index
每阶段完成后自动写检查点到 data/full_ingest_checkpoint.json
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# 项目根目录
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("full_bulk_ingest")

CHECKPOINT_FILE = ROOT / "data" / "full_ingest_checkpoint.json"
STAGES = ["etf", "a_share", "index"]


def get_stock_codes(stage: str) -> list[str]:
    """按阶段获取股票代码列表。"""
    from xtquant import xtdata
    xtdata.enable_hello = False

    if stage == "etf":
        return sorted(xtdata.get_stock_list_in_sector("沪深ETF"))
    elif stage == "a_share":
        return sorted(xtdata.get_stock_list_in_sector("沪深A股"))
    elif stage == "index":
        return sorted(xtdata.get_stock_list_in_sector("沪深指数"))
    else:
        raise ValueError(f"未知阶段: {stage}")


def save_checkpoint(stage: str, last_idx: int, total: int,
                    success: int, failed: int, failed_codes: list[str]):
    data = {
        "stage": stage,
        "last_idx": last_idx,
        "total": total,
        "success": success,
        "failed": failed,
        "failed_codes": failed_codes[-100:],  # 保留最近100个
        "saved_at": datetime.now().isoformat(),
    }
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(CHECKPOINT_FILE)


def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def run_stage(stage: str, batch_size: int, resume_idx: int = 0):
    """执行单个阶段的数据入库。"""
    from data_manager.auto_data_updater import AutoDataUpdater

    codes = get_stock_codes(stage)
    total = len(codes)
    log.info("=== 阶段 [%s] 开始: %d 只标的, 从第 %d 只继续 ===", stage, total, resume_idx)

    updater = AutoDataUpdater()
    updater.initialize_interface()
    if updater.interface is None:
        log.error("UnifiedDataInterface 初始化失败, 退出")
        return False

    success = 0
    failed = 0
    failed_codes: list[str] = []
    total_records = 0
    stage_start = time.time()

    for i in range(resume_idx, total, batch_size):
        batch = codes[i:i + batch_size]
        batch_start = time.time()
        log.info("[%s] 批次 %d-%d / %d (%d只)", stage, i, i + len(batch) - 1, total, len(batch))

        result = updater.bulk_download(
            stock_codes=batch,
            periods=["1d", "5m"],
            start_date="20100101",
        )

        batch_success = result.get("success_stocks", 0)
        batch_failed = result.get("failed_stocks", 0)
        batch_records = result.get("total_records", 0)

        success += batch_success
        failed += batch_failed
        total_records += batch_records

        # 收集失败代码
        for r in result.get("results", []):
            if r.get("success_periods", 0) == 0:
                failed_codes.append(r.get("stock_code", "?"))

        batch_elapsed = time.time() - batch_start
        total_elapsed = time.time() - stage_start
        remaining = total - (i + len(batch))
        avg_per_stock = total_elapsed / max(i + len(batch) - resume_idx, 1)
        eta_s = remaining * avg_per_stock

        log.info(
            "[%s] 进度: %d/%d (成功=%d 失败=%d 记录=%d) "
            "本批 %.1fs 总 %.0fs ETA %.0fs (%.1f分钟)",
            stage, i + len(batch), total,
            success, failed, total_records,
            batch_elapsed, total_elapsed, eta_s, eta_s / 60,
        )

        # 每批保存检查点
        save_checkpoint(stage, i + len(batch), total, success, failed, failed_codes)

    total_elapsed = time.time() - stage_start
    log.info(
        "=== 阶段 [%s] 完成: 成功=%d 失败=%d 记录=%d 耗时=%.0fs ===",
        stage, success, failed, total_records, total_elapsed,
    )
    return True


def verify_results():
    """验证DuckDB中的入库结果。"""
    import duckdb
    from data_manager.duckdb_connection_pool import resolve_duckdb_path
    db_path = resolve_duckdb_path()
    if not Path(db_path).exists():
        log.warning("DuckDB 文件不存在: %s", db_path)
        return
    con = duckdb.connect(str(db_path), read_only=True)
    tables = ["stock_daily", "stock_5m", "custom_period_bars", "data_ingestion_status"]
    for t in tables:
        try:
            row_count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            stock_count = con.execute(
                f"SELECT COUNT(DISTINCT stock_code) FROM {t}"
            ).fetchone()[0]
            log.info("  %s: %s 行, %s 只标的", t, f"{row_count:,}", f"{stock_count:,}")
        except Exception as e:
            log.info("  %s: %s", t, e)
    con.close()


def main():
    parser = argparse.ArgumentParser(description="全量数据入库")
    parser.add_argument("--stage", choices=STAGES + ["all"], default="all",
                        help="执行哪个阶段 (默认: all)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="每批处理的标的数 (默认: 100)")
    parser.add_argument("--resume", action="store_true",
                        help="从检查点续传")
    args = parser.parse_args()

    log.info("全量数据入库脚本启动 batch_size=%d", args.batch_size)

    if args.stage == "all":
        stages_to_run = STAGES
    else:
        stages_to_run = [args.stage]

    resume_stage = None
    resume_idx = 0

    if args.resume:
        cp = load_checkpoint()
        if cp:
            resume_stage = cp.get("stage")
            resume_idx = cp.get("last_idx", 0)
            log.info("加载检查点: stage=%s, last_idx=%d", resume_stage, resume_idx)

    overall_start = time.time()

    for stage in stages_to_run:
        if resume_stage and stage != resume_stage:
            # 跳过已完成的阶段（在检查点阶段之前的）
            if STAGES.index(stage) < STAGES.index(resume_stage):
                log.info("跳过已完成阶段: %s", stage)
                continue
            start_idx = 0
        elif stage == resume_stage:
            start_idx = resume_idx
        else:
            start_idx = 0

        ok = run_stage(stage, args.batch_size, start_idx)
        if not ok:
            log.error("阶段 %s 失败, 中止", stage)
            break
        # 阶段完成后清除resume状态
        resume_stage = None
        resume_idx = 0

    total_time = time.time() - overall_start
    log.info("=== 全部完成: 总耗时 %.1f 分钟 (%.1f 小时) ===", total_time / 60, total_time / 3600)

    log.info("--- 入库验证 ---")
    verify_results()


if __name__ == "__main__":
    main()
