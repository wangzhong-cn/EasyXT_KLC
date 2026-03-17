#!/usr/bin/env python3
"""
audit_chain_integrity_check.py — 审计链完整性日常抽检任务

用途：
  每日收盘后（建议 15:35）由 schedule / crontab / CI 调用。
  跑 AuditTrail.verify_chain_integrity()，将结果写入 logs/audit_chain_YYYYMMDD.json，
  发现异常时经 signal_bus 广播 AUDIT_CHAIN_ALERT 事件，并以非 0 退出（CI 可感知）。

用法：
  python tools/audit_chain_integrity_check.py                 # 检查今日
  python tools/audit_chain_integrity_check.py --date 20260307 # 指定日期（仅用于报告文件名）
  python tools/audit_chain_integrity_check.py --strict        # 发现异常时 exit(1)

与 AutoDataUpdater 集成（auto_data_updater.py）：
  from tools.audit_chain_integrity_check import run_integrity_check
  schedule.every().day.at("15:35").do(run_integrity_check)

退出码：
  0 — 完整性验证通过（ok=True，无 tampered，无 chain_break）
  1 — 发现篡改或断链（--strict 模式），或运行时异常
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# 确保项目根在 sys.path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

log = logging.getLogger("audit_chain_check")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_ROOT / "logs" / "audit_chain_check.log", encoding="utf-8"),
    ],
)

# 确保 logs/ 目录存在
(_ROOT / "logs").mkdir(exist_ok=True)

# 连续失败升级配置
_STATE_FILE = _ROOT / "logs" / "audit_chain_state.json"   # 持久化连续失败计数
_ESCALATE_WARN_AFTER   = 2   # 失败达到这个次数后发出严重告警
_ESCALATE_BLOCK_AFTER  = 3   # 失败达到这个次数后升级为阻断级（禁止 L1 发布）


def _load_state() -> dict:
    """加载持久化连续失败状态。"""
    if _STATE_FILE.exists():
        try:
            return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"consecutive_failures": 0, "last_failure_at": None, "escalated": False}


def _save_state(state: dict) -> None:
    """持久化连续失败状态。"""
    try:
        _STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        log.warning("状态文件写入失败（非致命）")


def _handle_failure_escalation(all_ok: bool) -> dict:
    """
    根据本次检查结果更新连续失败状态。

    Returns:
        state dict，其中 'escalated' 为 True 表示已升级为阻断级。
    """
    state = _load_state()

    if all_ok:
        if state["consecutive_failures"] > 0:
            log.info("审计链恢复正常，连续失败计数重置（原失败 %d 次）", state["consecutive_failures"])
        state["consecutive_failures"] = 0
        state["last_failure_at"] = None
        state["escalated"] = False
    else:
        state["consecutive_failures"] += 1
        state["last_failure_at"] = datetime.now().isoformat(timespec="seconds")

        n = state["consecutive_failures"]
        if n >= _ESCALATE_BLOCK_AFTER:
            state["escalated"] = True
            log.critical(
                "审计链连续失败 %d 次！已升级为[阻断级]——禁止任何等级发布直到审计链修复。"
                "解除方式：修复审计链异常后删除 %s",
                n, _STATE_FILE,
            )
        elif n >= _ESCALATE_WARN_AFTER:
            log.error(
                "审计链连续失败 %d 次！警告级升高——再失败 %d 次将升级为阻断级。",
                n, _ESCALATE_BLOCK_AFTER - n,
            )
        else:
            log.warning("审计链失败 %d 次（连续失败 %d 次后升级为阻断级）", n, _ESCALATE_BLOCK_AFTER)

    _save_state(state)
    return state


def run_integrity_check(date_str: str | None = None, strict: bool = False) -> bool:
    """
    执行审计链完整性检查。

    Args:
        date_str: 报告日期（格式 YYYYMMDD），默认今天。
        strict:   True 时发现异常则 exit(1)（CI 模式）。

    Returns:
        True  — 完整性正常
        False — 发现异常
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    log.info("=== 审计链完整性检查开始 date=%s ===", date_str)

    try:
        from core.audit_trail import AuditTrail
        trail = AuditTrail()
        result = trail.verify_chain_integrity()
    except Exception:
        log.exception("AuditTrail 初始化或 verify_chain_integrity 执行失败")
        state = _handle_failure_escalation(all_ok=False)
        if strict or state.get("escalated", False):
            sys.exit(1)
        return False

    # ── 写入 JSON 报告 ────────────────────────────────────────────────────────
    report_path = _ROOT / "logs" / f"audit_chain_{date_str}.json"
    report = {
        "date": date_str,
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "ok": result.get("ok", False),
        "detail": {k: v for k, v in result.items() if k != "ok"},
    }
    try:
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        log.info("报告已写入 %s", report_path)
    except OSError:
        log.warning("报告写入失败（非致命）: %s", report_path)

    # ── 摘要日志 ─────────────────────────────────────────────────────────────
    all_ok = result.get("ok", False)
    for table in ("signals", "orders", "fills"):
        tbl_data = result.get(table, {})
        if not tbl_data:
            continue
        tampered = tbl_data.get("tampered", 0)
        breaks = tbl_data.get("chain_breaks", 0)
        log.info(
            "  %-8s total=%-5d tampered=%-3d chain_breaks=%-3d",
            table, tbl_data.get("total", 0), tampered, breaks,
        )

    # ── 连续失败升级 ──────────────────────────────────────────────────────────
    state = _handle_failure_escalation(all_ok)
    is_escalated = state.get("escalated", False)
    consecutive_n = state.get("consecutive_failures", 0)

    # ── 异常时广播告警 ────────────────────────────────────────────────────────
    if not all_ok:
        tampered_summary = {
            t: result.get(t, {}).get("tampered_ids", [])
            for t in ("signals", "orders", "fills")
        }
        chain_break_summary = {
            t: result.get(t, {}).get("chain_break_ids", [])
            for t in ("signals", "orders", "fills")
        }
        log.error(
            "审计链完整性异常！tampered=%s  chain_breaks=%s",
            tampered_summary, chain_break_summary,
        )
        try:
            from core.events import Events
            from core.signal_bus import signal_bus
            signal_bus.emit(
                Events.AUDIT_ENTRY_CREATED,
                audit_ok=False,
                tampered=tampered_summary,
                chain_breaks=chain_break_summary,
                consecutive_failures=consecutive_n,
                escalated=is_escalated,
                report_path=str(report_path),
            )
        except Exception:
            log.warning("signal_bus 广播失败（非 GUI 环境下正常）")

        if strict or is_escalated:
            sys.exit(1)
        return False

    log.info("=== 审计链完整性检查通过（ok=True） ===")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="审计链完整性日常抽检")
    parser.add_argument("--date", help="报告日期 YYYYMMDD（默认今天）")
    parser.add_argument(
        "--strict", action="store_true",
        help="发现异常时 exit(1)（用于 CI / 定时任务告警）",
    )
    args = parser.parse_args()
    run_integrity_check(date_str=args.date, strict=args.strict)


if __name__ == "__main__":
    main()
