#!/usr/bin/env python
"""指数全量入库脚本（xtdata 预筛 + 分批）。

用法: python tools/ingest_index.py
"""
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
INGEST_LOCK_STALE_S = 6 * 3600


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _cleanup_stale_ingest_lock(lock_path: Path, stale_s: int = INGEST_LOCK_STALE_S) -> bool:
    if not lock_path.exists():
        return False
    now = time.time()
    try:
        content = lock_path.read_text(encoding="utf-8", errors="ignore").strip()
    except OSError:
        content = ""
    pid = -1
    ts = 0.0
    if "|" in content:
        p, t = content.split("|", 1)
        try:
            pid = int(p)
        except ValueError:
            pid = -1
        try:
            ts = float(t)
        except ValueError:
            ts = 0.0
    age_s = max(0.0, now - ts) if ts > 0 else float(stale_s + 1)
    alive = _pid_exists(pid)
    if (not alive) or age_s > stale_s:
        lock_path.unlink(missing_ok=True)
        return True
    return False


def _write_ingest_lock(lock_path: Path) -> None:
    lock_path.write_text(f"{os.getpid()}|{time.time():.3f}", encoding="utf-8")


def main():
    from xtquant import xtdata
    xtdata.enable_hello = False
    from data_manager.auto_data_updater import AutoDataUpdater
    from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

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

    init_batch = max(1, int(os.environ.get("EASYXT_INDEX_INIT_BATCH", "100") or 100))
    min_batch = max(1, int(os.environ.get("EASYXT_INDEX_MIN_BATCH", "10") or 10))
    max_batch = max(min_batch, int(os.environ.get("EASYXT_INDEX_MAX_BATCH", "100") or 100))
    target_batch_sec = max(10.0, float(os.environ.get("EASYXT_INDEX_TARGET_BATCH_SEC", "90") or 90))
    batch_size = min(max_batch, max(min_batch, init_batch))
    log.info(
        "动态批次入库指数: 总数=%d, 初始=%d, 区间=[%d,%d], 目标耗时=%.0fs",
        len(have_data), batch_size, min_batch, max_batch, target_batch_sec,
    )

    updater = AutoDataUpdater()
    updater.initialize_interface()
    if updater.interface is None:
        log.error("初始化失败")
        return

    ingest_lock = Path(str(db_path) + ".ingest.lock")
    _cleanup_stale_ingest_lock(ingest_lock)

    def _rotate_interface_connection() -> None:
        if updater.interface is None:
            return
        ingest_lock.unlink(missing_ok=True)
        try:
            updater.interface._close_duckdb_connection()
        except Exception:
            pass
        time.sleep(5.0)
        if not updater.interface.connect(read_only=False):
            raise RuntimeError("批次连接重建失败")
        _write_ingest_lock(ingest_lock)

    def _next_batch_size(current: int, elapsed_sec: float) -> int:
        if elapsed_sec > target_batch_sec * 1.2:
            return max(min_batch, int(current * 0.7))
        if elapsed_sec < target_batch_sec * 0.5:
            return min(max_batch, int(current * 1.2))
        return current

    t0 = time.time()
    total_success = 0
    total_failed = 0
    total_records = 0

    _write_ingest_lock(ingest_lock)
    try:
        cursor = 0
        batch_idx = 0
        while cursor < len(have_data):
            batch_idx += 1
            batch = have_data[cursor : cursor + batch_size]
            log.info(
                "=== 批次 %d: %d 只 [%s ... %s] | 已完成=%d/%d ===",
                batch_idx, len(batch), batch[0], batch[-1], cursor, len(have_data),
            )

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

            cursor += len(batch)
            batch_elapsed = bt1 - bt0
            elapsed = bt1 - t0
            avg_batch_sec = elapsed / batch_idx
            remaining_items = max(0, len(have_data) - cursor)
            remaining_batches = (remaining_items + batch_size - 1) // batch_size
            remaining = remaining_batches * avg_batch_sec
            log.info(
                "批次完成: 成功=%d 失败=%d 记录=%d 耗时=%.0fs | 累计: 成功=%d 失败=%d ETA=%.0fm",
                result["success_stocks"], result["failed_stocks"], result["total_records"],
                batch_elapsed, total_success, total_failed, remaining / 60,
            )
            prev_batch_size = batch_size
            batch_size = _next_batch_size(batch_size, batch_elapsed)
            if batch_size != prev_batch_size:
                log.info("调整批次大小: %d -> %d (本批耗时 %.0fs)", prev_batch_size, batch_size, batch_elapsed)
            if cursor < len(have_data):
                _rotate_interface_connection()
    finally:
        ingest_lock.unlink(missing_ok=True)

    log.info("=== 指数入库全部完成 ===")
    log.info("累计: 成功=%d 失败=%d 记录=%d 耗时=%.1fs",
             total_success, total_failed, total_records, time.time() - t0)

    # 5. 验证
    with mgr.get_read_connection() as con:
        for t in ["stock_daily", "stock_5m", "custom_period_bars"]:
            try:
                rc_row = con.execute("SELECT COUNT(*) FROM " + t).fetchone()
                sc_row = con.execute("SELECT COUNT(DISTINCT stock_code) FROM " + t).fetchone()
                rc = int(rc_row[0]) if rc_row else 0
                sc = int(sc_row[0]) if sc_row else 0
                log.info("  %s: %s 行, %s 只标的", t, f"{rc:,}", f"{sc:,}")
            except Exception as e:
                log.info("  %s: %s", t, e)


if __name__ == "__main__":
    main()
