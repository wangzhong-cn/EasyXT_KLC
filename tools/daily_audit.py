"""
daily_audit.py — 日一致性审计报表（信号/委托/成交三方差异核对）

功能：
  1. 扫描指定日期的 TradeExecutor_YYYYMMDD.log（来自 strategies/**）
  2. 解析三类关键事件：
     - SIGNAL  : 策略发出的交易信号（尚不记录，仅占位）
     - ORDER   : 委托提交事件（"订单详情: 股票=..."）
     - FILL    : 成交回报事件（"成交"/"已成交"）
  3. 三方 diff：信号 vs 委托 vs （未来的）PnL 记录
  4. 输出 JSON 报告到 logs/audit_YYYYMMDD.json
  5. --strict 模式：有差异时以非 0 退出（CI gate）

典型用法：
    python tools/daily_audit.py
    python tools/daily_audit.py --date 20260224
    python tools/daily_audit.py --date 20260224 --strict
    python tools/daily_audit.py --log-dir strategies/xueqiu_follow/logs

格式假设（来自 TradeExecutor 日志）：
  YYYY-MM-DD HH:MM:SS - TradeExecutor - INFO - 开始执行订单: <ORDER_ID>
  YYYY-MM-DD HH:MM:SS - TradeExecutor - INFO - 订单详情: 股票=<CODE>, 方向=<buy|sell>, 数量=<N>, 价格=<P>, 委托类型=<KIND>
  YYYY-MM-DD HH:MM:SS - TradeExecutor - INFO - 成交: <ORDER_ID> ...  (如有)
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Any

log = logging.getLogger("daily_audit")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# 项目根目录
_ROOT = Path(__file__).resolve().parents[1]

# ── 日志行正则 ─────────────────────────────────────────────────────────────────

# 通用前缀：2026-02-24 15:57:57 - TradeExecutor - INFO - ...
_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
    r"\s+-\s+(?P<logger>\S+)\s+-\s+(?P<level>\w+)\s+-\s+(?P<msg>.+)$"
)

# 委托提交：开始执行订单: order_xxx
_ORDER_START_RE = re.compile(r"开始执行订单[：:]\s*(?P<order_id>\S+)")

# 订单详情
_ORDER_DETAIL_RE = re.compile(
    r"订单详情[：:]\s*股票=(?P<code>[^,]+),\s*方向=(?P<side>[^,]+),\s*"
    r"数量=(?P<qty>[^,]+),\s*价格=(?P<price>[^,]+),\s*委托类型=(?P<kind>.+)$"
)

# 成交确认（多种可能的格式）
_FILL_RE = re.compile(
    r"(成交|已成交|全部成交|部分成交)[：:]?\s*(?P<order_id>\S+)?(?P<extra>.*)"
)

# 撤单
_CANCEL_RE = re.compile(r"(撤单|已撤销|订单取消)[：:]?\s*(?P<order_id>\S+)?")

# 执行器关闭（边界）
_CLOSE_RE = re.compile(r"(关闭交易执行器|交易执行器已关闭)")


# ── 数据模型 ──────────────────────────────────────────────────────────────────

@dataclass
class OrderRecord:
    order_id: str
    ts_submit: str = ""
    code: str = ""
    side: str = ""
    qty: str = ""
    price: str = ""
    kind: str = ""
    status: str = "submitted"   # submitted | filled | cancelled | partial
    ts_fill: str = ""
    fill_extra: str = ""
    source_file: str = ""


@dataclass
class AuditReport:
    date: str
    generated_at: str
    source_files: list[str] = field(default_factory=list)
    orders_submitted: int = 0
    orders_filled: int = 0
    orders_cancelled: int = 0
    orders_no_feedback: int = 0
    fill_rate: float = 0.0
    orders: list[dict[str, Any]] = field(default_factory=list)
    anomalies: list[str] = field(default_factory=list)
    summary: str = ""


# ── 核心解析逻辑 ──────────────────────────────────────────────────────────────

def parse_trade_executor_log(log_path: Path) -> list[OrderRecord]:
    """解析单个 TradeExecutor_YYYYMMDD.log 文件，返回 OrderRecord 列表。"""
    records: dict[str, OrderRecord] = {}
    last_order_id: str | None = None

    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.rstrip()
                m = _LINE_RE.match(line)
                if not m:
                    continue
                ts, msg = m.group("ts"), m.group("msg")

                # 委托提交
                om = _ORDER_START_RE.search(msg)
                if om:
                    oid = om.group("order_id")
                    if oid not in records:
                        records[oid] = OrderRecord(
                            order_id=oid,
                            ts_submit=ts,
                            source_file=log_path.name,
                        )
                    last_order_id = oid
                    continue

                # 订单详情
                dm = _ORDER_DETAIL_RE.search(msg)
                if dm and last_order_id and last_order_id in records:
                    r = records[last_order_id]
                    r.code = dm.group("code").strip()
                    r.side = dm.group("side").strip()
                    r.qty = dm.group("qty").strip()
                    r.price = dm.group("price").strip()
                    r.kind = dm.group("kind").strip()
                    continue

                # 成交
                fm = _FILL_RE.search(msg)
                if fm:
                    oid = fm.group("order_id") or last_order_id or ""
                    oid = oid.strip()
                    if oid in records:
                        records[oid].status = "filled"
                        records[oid].ts_fill = ts
                        records[oid].fill_extra = (fm.group("extra") or "").strip()
                    elif last_order_id and last_order_id in records:
                        records[last_order_id].status = "filled"
                        records[last_order_id].ts_fill = ts
                    continue

                # 撤单
                cm = _CANCEL_RE.search(msg)
                if cm:
                    oid = (cm.group("order_id") or last_order_id or "").strip()
                    if oid in records:
                        records[oid].status = "cancelled"
                    elif last_order_id and last_order_id in records:
                        records[last_order_id].status = "cancelled"

    except OSError as e:
        log.warning("无法读取日志文件 %s: %s", log_path, e)

    return list(records.values())


def _find_log_files(search_dirs: list[Path], date_str: str) -> list[Path]:
    """在 search_dirs 中递归查找 TradeExecutor_YYYYMMDD*.log 文件。"""
    found: list[Path] = []
    for d in search_dirs:
        pattern = str(d / "**" / f"TradeExecutor_{date_str}*.log")
        matched = glob.glob(pattern, recursive=True)
        found.extend(Path(p) for p in matched)
    return sorted(set(found))


def run_audit(
    audit_date: str,
    search_dirs: list[Path],
    strict: bool = False,
) -> AuditReport:
    """
    执行审计，返回 AuditReport。

    audit_date : "YYYYMMDD" 格式
    search_dirs: 搜索根目录列表
    strict     : True 时若有异常则以 sys.exit(1) 终止
    """
    log_files = _find_log_files(search_dirs, audit_date)
    log.info("找到日志文件 %d 个: %s", len(log_files), [f.name for f in log_files])

    all_records: list[OrderRecord] = []
    for lf in log_files:
        all_records.extend(parse_trade_executor_log(lf))

    # 去重（同一 order_id 可能出现在多次尝试中，取最终状态）
    deduped: dict[str, OrderRecord] = {}
    for r in all_records:
        existing = deduped.get(r.order_id)
        if existing is None:
            deduped[r.order_id] = r
        else:
            # 有成交记录优先
            if r.status == "filled":
                existing.status = "filled"
                existing.ts_fill = r.ts_fill

    orders = list(deduped.values())
    submitted = len(orders)
    filled = sum(1 for o in orders if o.status == "filled")
    cancelled = sum(1 for o in orders if o.status == "cancelled")
    no_feedback = sum(1 for o in orders if o.status == "submitted")
    fill_rate = filled / submitted if submitted else 0.0

    anomalies: list[str] = []
    # 异常 1：有委托但无任何反馈（可能是超时、服务重启等）
    for o in orders:
        if o.status == "submitted" and not o.fill_extra:
            anomalies.append(
                f"订单 {o.order_id} ({o.code} {o.side} {o.qty}@{o.price}) "
                f"提交于 {o.ts_submit}，无成交/撤单回报"
            )

    # 异常 2：成交但缺少价格信息（数据不完整）
    for o in orders:
        if o.status == "filled" and not o.price:
            anomalies.append(f"订单 {o.order_id} 成交但缺少价格信息")

    summary_parts = [
        f"委托 {submitted} 笔",
        f"成交 {filled} 笔",
        f"撤单 {cancelled} 笔",
        f"无反馈 {no_feedback} 笔",
        f"成交率 {fill_rate:.1%}",
    ]
    if anomalies:
        summary_parts.append(f"⚠️ 异常 {len(anomalies)} 条")

    report = AuditReport(
        date=audit_date,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        source_files=[str(f) for f in log_files],
        orders_submitted=submitted,
        orders_filled=filled,
        orders_cancelled=cancelled,
        orders_no_feedback=no_feedback,
        fill_rate=round(fill_rate, 4),
        orders=[asdict(o) for o in orders],
        anomalies=anomalies,
        summary=", ".join(summary_parts),
    )

    if strict and anomalies:
        log.error("严格模式：发现 %d 条异常，退出码 1", len(anomalies))
        _save_report(report)
        sys.exit(1)

    return report


def _save_report(report: AuditReport) -> Path:
    out_dir = _ROOT / "logs"
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / f"audit_{report.date}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(asdict(report), f, ensure_ascii=False, indent=2)
    log.info("审计报告已写入: %s", out_file)
    return out_file


# ── CLI 入口 ──────────────────────────────────────────────────────────────────

def _today_str() -> str:
    return date.today().strftime("%Y%m%d")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="EasyXT 日一致性审计工具（信号/委托/成交三方差异）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python tools/daily_audit.py                          # 审计今天
  python tools/daily_audit.py --date 20260224          # 审计指定日期
  python tools/daily_audit.py --date 20260224 --strict # 有异常时 exit(1)
  python tools/daily_audit.py --log-dir strategies/xueqiu_follow/logs
""",
    )
    parser.add_argument(
        "--date", default=_today_str(),
        help="审计日期 YYYYMMDD（默认：今天）",
    )
    parser.add_argument(
        "--log-dir", action="append", dest="log_dirs",
        help="额外搜索目录（可重复）；默认搜索 strategies/** 和 logs/",
    )
    parser.add_argument(
        "--strict", action="store_true",
        help="有差异时以 exit(1) 终止（CI gate）",
    )
    parser.add_argument(
        "--output", default=None,
        help="报告输出路径（默认: logs/audit_YYYYMMDD.json）",
    )
    args = parser.parse_args(argv)

    # 默认搜索目录
    default_dirs = [
        _ROOT / "strategies",
        _ROOT / "logs",
    ]
    extra_dirs = [Path(d) for d in (args.log_dirs or [])]
    search_dirs = default_dirs + extra_dirs

    report = run_audit(
        audit_date=args.date,
        search_dirs=search_dirs,
        strict=args.strict,
    )

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(asdict(report), f, ensure_ascii=False, indent=2)
        log.info("报告已写入: %s", out)
    else:
        _save_report(report)

    # 控制台摘要
    print(f"\n=== 审计报告 {report.date} ===")
    print(f"  {report.summary}")
    if report.anomalies:
        print(f"  异常明细:")
        for a in report.anomalies:
            print(f"    - {a}")
    if not report.orders:
        print("  （未找到交易日志，可能本日无交易）")
    print()

    return 1 if (args.strict and report.anomalies) else 0


if __name__ == "__main__":
    sys.exit(main())
