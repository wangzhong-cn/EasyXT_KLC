from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

from data_manager.golden_1d_audit import BackfillStatus, Golden1dAuditor, SymbolAuditSummary
from data_manager.governance_metadata import build_governance_snapshot

logger = logging.getLogger(__name__)

RepairPlanStatus = Literal["noop", "queued", "manual_review", "blocked", "failed"]

_HARD_BLOCKER_PATTERNS = (
    "non_trading_day",
    "非交易日数据",
    "同日重复",
    "重复 1d 记录",
)

_INVARIANT_PATTERN = re.compile(r"^(?P<trade_date>\d{4}-\d{2}-\d{2}):\s*1m→1d")


def _new_repair_task_list() -> list[RepairTask]:
    return []


def _new_string_list() -> list[str]:
    return []


def _new_snapshot_dict() -> dict[str, Any]:
    return {}


@dataclass
class RepairTask:
    stock_code: str
    period: str
    start_date: str
    end_date: str
    reason: str
    priority_hint: int | None = None
    current_symbol: str = ""
    gap_length: int | None = None


@dataclass
class RepairPlan:
    symbol: str
    generated_at: str
    status: RepairPlanStatus
    tasks: list[RepairTask] = field(default_factory=_new_repair_task_list)
    blocker_issues: list[str] = field(default_factory=_new_string_list)
    notes: list[str] = field(default_factory=_new_string_list)
    summary_snapshot: dict[str, Any] = field(default_factory=_new_snapshot_dict)


@dataclass
class RepairExecutionResult:
    symbol: str
    status: BackfillStatus
    queued_tasks: int = 0
    failed_tasks: int = 0
    blocker_issues: list[str] = field(default_factory=_new_string_list)
    notes: list[str] = field(default_factory=_new_string_list)


@dataclass
class StoredRepairPlanSnapshot:
    symbol: str
    generated_at: str | None
    plan_status: str
    queued_tasks: int = 0
    failed_tasks: int = 0
    blocker_issues: list[str] = field(default_factory=_new_string_list)
    notes: list[str] = field(default_factory=_new_string_list)
    tasks: list[RepairTask] = field(default_factory=_new_repair_task_list)
    summary_snapshot: dict[str, Any] = field(default_factory=_new_snapshot_dict)

    @property
    def task_count(self) -> int:
        return len(self.tasks)


class Golden1DRepairOrchestrator:
    """黄金标准 1D 后台修复编排器。

    设计目标：
    - 审计层只判定，不直接修库
    - 读链路不自愈，后台修复默认开启
    - 自动修复仅处理“缺失/落后/可重建”问题
    - 脏数据/跨源冲突/合约硬失败进入 manual_review 或 blocked
    """

    def __init__(
        self,
        auditor: Golden1dAuditor | None = None,
        interface: Any | None = None,
    ) -> None:
        self.auditor = auditor or Golden1dAuditor()
        self.interface = interface
        self._ensure_plan_table()

    def _ensure_plan_table(self) -> None:
        Path(self.auditor.audit_db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.auditor.audit_db_path)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS golden_1d_repair_plan (
                plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                plan_status TEXT NOT NULL,
                queued_tasks INTEGER DEFAULT 0,
                failed_tasks INTEGER DEFAULT 0,
                blocker_issues TEXT,
                notes TEXT,
                tasks_json TEXT,
                summary_snapshot TEXT
            )
            """
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _decode_json_list(raw: str | None) -> list[str]:
        if not raw:
            return []
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if isinstance(value, list):
            decoded = cast(list[Any], value)
            return [str(item) for item in decoded]
        return []

    @staticmethod
    def _decode_tasks(raw: str | None) -> list[RepairTask]:
        if not raw:
            return []
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(value, list):
            return []
        decoded = cast(list[Any], value)
        tasks: list[RepairTask] = []
        for item in decoded:
            if not isinstance(item, dict):
                continue
            payload = cast(dict[str, Any], item)
            try:
                tasks.append(
                    RepairTask(
                        stock_code=str(payload.get("stock_code") or ""),
                        period=str(payload.get("period") or "1d"),
                        start_date=str(payload.get("start_date") or ""),
                        end_date=str(payload.get("end_date") or ""),
                        reason=str(payload.get("reason") or ""),
                        priority_hint=(
                            int(payload["priority_hint"])
                            if payload.get("priority_hint") is not None
                            else None
                        ),
                        current_symbol=str(payload.get("current_symbol") or ""),
                        gap_length=(
                            int(payload["gap_length"])
                            if payload.get("gap_length") is not None
                            else None
                        ),
                    )
                )
            except Exception:
                continue
        return tasks

    @staticmethod
    def _decode_summary_snapshot(raw: str | None) -> dict[str, Any]:
        if not raw:
            return {}
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return cast(dict[str, Any], value) if isinstance(value, dict) else {}

    def _row_to_snapshot(self, row: sqlite3.Row) -> StoredRepairPlanSnapshot:
        return StoredRepairPlanSnapshot(
            symbol=str(row["symbol"]),
            generated_at=row["generated_at"],
            plan_status=str(row["plan_status"]),
            queued_tasks=int(row["queued_tasks"] or 0),
            failed_tasks=int(row["failed_tasks"] or 0),
            blocker_issues=self._decode_json_list(row["blocker_issues"]),
            notes=self._decode_json_list(row["notes"]),
            tasks=self._decode_tasks(row["tasks_json"]),
            summary_snapshot=self._decode_summary_snapshot(row["summary_snapshot"]),
        )

    def get_latest_plan(self, symbol: str) -> StoredRepairPlanSnapshot | None:
        symbol = str(symbol or "").strip()
        if not symbol:
            return None
        conn = sqlite3.connect(self.auditor.audit_db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT symbol, generated_at, plan_status, queued_tasks, failed_tasks,
                   blocker_issues, notes, tasks_json, summary_snapshot
            FROM golden_1d_repair_plan
            WHERE symbol = ?
            ORDER BY generated_at DESC, plan_id DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        conn.close()
        if row is None:
            return None
        return self._row_to_snapshot(row)

    def list_recent_plans(
        self,
        symbol: str | None = None,
        limit: int = 20,
    ) -> list[StoredRepairPlanSnapshot]:
        query = (
            "SELECT symbol, generated_at, plan_status, queued_tasks, failed_tasks, "
            "blocker_issues, notes, tasks_json, summary_snapshot "
            "FROM golden_1d_repair_plan"
        )
        params: list[Any] = []
        if symbol:
            query += " WHERE symbol = ?"
            params.append(symbol)
        query += " ORDER BY generated_at DESC, plan_id DESC LIMIT ?"
        params.append(max(1, int(limit)))

        conn = sqlite3.connect(self.auditor.audit_db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [self._row_to_snapshot(row) for row in rows]

    @staticmethod
    def _parse_date(raw: str | None) -> date | None:
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            return None

    @staticmethod
    def _format_date(value: date) -> str:
        return value.strftime("%Y-%m-%d")

    @staticmethod
    def _extract_invariant_dates(issues: list[Any]) -> list[str]:
        trade_dates: list[str] = []
        for issue in issues or []:
            match = _INVARIANT_PATTERN.match(str(issue))
            if match:
                trade_dates.append(match.group("trade_date"))
        return sorted(set(trade_dates))

    @staticmethod
    def _contains_hard_blocker(summary: SymbolAuditSummary) -> list[str]:
        raw_issues = cast(list[Any], summary.issues or [])
        issues: list[str] = [str(item) for item in raw_issues]
        blockers = [
            issue
            for issue in issues
            if any(pattern in issue for pattern in _HARD_BLOCKER_PATTERNS)
        ]
        if summary.backfill_status == "failed":
            blockers.insert(0, "存在合约硬门禁失败，自动补齐已被阻断")
        invariant_dates = Golden1DRepairOrchestrator._extract_invariant_dates(raw_issues)
        cross_source_mismatch = (
            summary.cross_source_status == "degraded"
            and summary.cross_source_fields_passed < summary.cross_source_fields_total
            and summary.missing_days <= 0
            and not summary.has_listing_gap
            and not invariant_dates
        )
        if cross_source_mismatch:
            blockers.append("跨源字段冲突，需人工复核后再决定是否清洗/回填")
        return blockers

    @staticmethod
    def _should_schedule_incremental(summary: SymbolAuditSummary) -> bool:
        local_last = Golden1DRepairOrchestrator._parse_date(summary.local_last_date)
        if local_last is None:
            return False
        stale_days = max(
            1,
            int(os.environ.get("EASYXT_GOLDEN_1D_INCREMENTAL_STALE_DAYS", "3") or 3),
        )
        return (date.today() - local_last).days >= stale_days

    @staticmethod
    def _merge_tasks(tasks: list[RepairTask]) -> list[RepairTask]:
        merged: dict[tuple[str, str], RepairTask] = {}
        for task in tasks:
            key = (task.stock_code, task.period)
            existing = merged.get(key)
            if existing is None:
                merged[key] = task
                continue
            start = min(existing.start_date, task.start_date)
            end = max(existing.end_date, task.end_date)
            reasons = [part for part in existing.reason.split(",") if part]
            if task.reason not in reasons:
                reasons.append(task.reason)
            merged[key] = RepairTask(
                stock_code=task.stock_code,
                period=task.period,
                start_date=start,
                end_date=end,
                reason=",".join(reasons),
                priority_hint=min(
                    existing.priority_hint if existing.priority_hint is not None else 999,
                    task.priority_hint if task.priority_hint is not None else 999,
                ),
                current_symbol=task.current_symbol or existing.current_symbol,
                gap_length=max(existing.gap_length or 0, task.gap_length or 0) or None,
            )
        return list(merged.values())

    def plan_summary(self, summary: SymbolAuditSummary, current_symbol: str = "") -> RepairPlan:
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        blocker_issues = self._contains_hard_blocker(summary)
        tasks: list[RepairTask] = []
        notes: list[str] = []
        today = date.today()

        listing_date = self._parse_date(summary.listing_date)
        local_first = self._parse_date(summary.local_first_date)
        local_last = self._parse_date(summary.local_last_date)
        raw_issues = cast(list[Any], summary.issues or [])
        invariant_dates = self._extract_invariant_dates(raw_issues)

        if summary.has_listing_gap and listing_date and local_first:
            gap_end = local_first - timedelta(days=1)
            if listing_date <= gap_end:
                tasks.append(
                    RepairTask(
                        stock_code=summary.symbol,
                        period="1d",
                        start_date=self._format_date(listing_date),
                        end_date=self._format_date(gap_end),
                        reason="listing_gap_backfill",
                        priority_hint=10,
                        current_symbol=current_symbol,
                        gap_length=summary.listing_gap_days or summary.missing_days or 1,
                    )
                )
                notes.append("检测到 listing gap，已生成上市首日至本地首条前一日的 1d 回填任务")

        if summary.actual_trading_days == 0 and listing_date:
            tasks.append(
                RepairTask(
                    stock_code=summary.symbol,
                    period="1d",
                    start_date=self._format_date(listing_date),
                    end_date=self._format_date(today),
                    reason="bootstrap_local_daily",
                    priority_hint=15,
                    current_symbol=current_symbol,
                    gap_length=summary.expected_trading_days or 1,
                )
            )
            notes.append("本地 1d 数据为空，已生成全历史 bootstrap 回填任务")

        if summary.missing_days > 0 and local_last:
            missing_start = local_first or listing_date or local_last
            tasks.append(
                RepairTask(
                    stock_code=summary.symbol,
                    period="1d",
                    start_date=self._format_date(missing_start),
                    end_date=self._format_date(local_last),
                    reason="missing_days_backfill",
                    priority_hint=20,
                    current_symbol=current_symbol,
                    gap_length=summary.missing_days,
                )
            )
            notes.append("检测到缺失交易日，已生成 1d 缺口回填任务")

        if invariant_dates:
            tasks.extend(
                [
                    RepairTask(
                        stock_code=summary.symbol,
                        period="1m",
                        start_date=min(invariant_dates),
                        end_date=max(invariant_dates),
                        reason="invariant_repair_1m",
                        priority_hint=30,
                        current_symbol=current_symbol,
                        gap_length=len(invariant_dates),
                    ),
                    RepairTask(
                        stock_code=summary.symbol,
                        period="1d",
                        start_date=min(invariant_dates),
                        end_date=max(invariant_dates),
                        reason="invariant_repair_1d",
                        priority_hint=31,
                        current_symbol=current_symbol,
                        gap_length=len(invariant_dates),
                    ),
                ]
            )
            notes.append("检测到 1m→1d 不变量失败，已生成 1m/1d 定向修复任务")

        if self._should_schedule_incremental(summary):
            lookback_days = max(
                1,
                int(os.environ.get("EASYXT_GOLDEN_1D_INCREMENTAL_LOOKBACK_DAYS", "7") or 7),
            )
            window_start = self._format_date(today - timedelta(days=lookback_days))
            window_end = self._format_date(today)
            for period, priority in (("1d", 40), ("1m", 41), ("5m", 42)):
                tasks.append(
                    RepairTask(
                        stock_code=summary.symbol,
                        period=period,
                        start_date=window_start,
                        end_date=window_end,
                        reason="stale_window_backfill",
                        priority_hint=priority,
                        current_symbol=current_symbol,
                        gap_length=lookback_days,
                    )
                )
            notes.append("检测到最新窗口滞后，已生成最近窗口增量补齐任务")

        merged_tasks = self._merge_tasks(tasks)

        if blocker_issues:
            status: RepairPlanStatus = "blocked" if merged_tasks else "manual_review"
            merged_tasks = []
            notes.append("存在污染/冲突类问题，自动补齐已阻断，等待人工治理")
        elif merged_tasks:
            status = "queued"
        elif summary.golden_status == "golden":
            status = "noop"
            notes.append("当前已满足黄金标准，无需额外修复任务")
        else:
            status = "manual_review"
            notes.append("当前无可自动修复项，建议进入人工复核")

        return RepairPlan(
            symbol=summary.symbol,
            generated_at=generated_at,
            status=status,
            tasks=merged_tasks,
            blocker_issues=blocker_issues[:10],
            notes=notes[:10],
            summary_snapshot={
                "golden_status": summary.golden_status,
                "backfill_status": summary.backfill_status,
                "missing_days": summary.missing_days,
                "duplicate_days": summary.duplicate_days,
                "has_listing_gap": summary.has_listing_gap,
                "listing_gap_days": summary.listing_gap_days,
                "cross_source_status": summary.cross_source_status,
                "cross_source_fields_passed": summary.cross_source_fields_passed,
                "cross_source_fields_total": summary.cross_source_fields_total,
                "governance": build_governance_snapshot(
                    symbol=summary.symbol,
                    trade_date=summary.last_audited_at or generated_at,
                    periods=sorted({task.period for task in merged_tasks}) or ["1d"],
                ),
            },
        )

    def _ensure_interface(self) -> Any | None:
        if self.interface is not None:
            return self.interface
        try:
            from data_manager.unified_data_interface import UnifiedDataInterface

            self.interface = UnifiedDataInterface(
                duckdb_path=self.auditor.duckdb_path,
                eager_init=False,
                silent_init=True,
            )
            self.interface.connect(read_only=False)
            return self.interface
        except Exception:
            logger.exception("初始化 UnifiedDataInterface 失败，无法执行 Golden 1D 补齐计划")
            self.interface = None
            return None

    def _save_plan_record(self, plan: RepairPlan, result: RepairExecutionResult) -> None:
        conn = sqlite3.connect(self.auditor.audit_db_path)
        conn.execute(
            """
            INSERT INTO golden_1d_repair_plan
            (symbol, generated_at, plan_status, queued_tasks, failed_tasks,
             blocker_issues, notes, tasks_json, summary_snapshot)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                plan.symbol,
                plan.generated_at,
                result.status,
                result.queued_tasks,
                result.failed_tasks,
                json.dumps(result.blocker_issues, ensure_ascii=False),
                json.dumps(result.notes, ensure_ascii=False),
                json.dumps([asdict(task) for task in plan.tasks], ensure_ascii=False),
                json.dumps(plan.summary_snapshot, ensure_ascii=False),
            ),
        )
        conn.commit()
        conn.close()

    def _persist_repair_receipt(self, plan: RepairPlan, result: RepairExecutionResult) -> None:
        iface = self._ensure_interface()
        if iface is None:
            return
        if plan.tasks:
            periods = sorted({task.period for task in plan.tasks})
            start_date = min(task.start_date for task in plan.tasks)
            end_date = max(task.end_date for task in plan.tasks)
            primary_period = periods[0]
            reason = ",".join(sorted({task.reason for task in plan.tasks}))
        else:
            periods = ["1d"]
            start_date = plan.generated_at[:10]
            end_date = plan.generated_at[:10]
            primary_period = "1d"
            reason = plan.status
        try:
            iface.record_repair_receipt(
                stock_code=plan.symbol,
                period=primary_period,
                start_date=start_date,
                end_date=end_date,
                reason=reason,
                status=result.status,
                task_count=len(plan.tasks),
                queued_tasks=result.queued_tasks,
                failed_tasks=result.failed_tasks,
            )
        except Exception:
            logger.exception("写入 repair_receipt 失败: %s", plan.symbol)

    def execute_plan(self, plan: RepairPlan) -> RepairExecutionResult:
        if plan.status == "noop":
            result = RepairExecutionResult(
                symbol=plan.symbol,
                status="complete",
                notes=plan.notes,
            )
            self.auditor.update_backfill_status(plan.symbol, "complete")
            self._save_plan_record(plan, result)
            self._persist_repair_receipt(plan, result)
            return result

        if plan.status in {"manual_review", "blocked"}:
            final_status: BackfillStatus = "manual_review" if plan.status == "manual_review" else "blocked"
            result = RepairExecutionResult(
                symbol=plan.symbol,
                status=final_status,
                blocker_issues=plan.blocker_issues,
                notes=plan.notes,
            )
            note = plan.blocker_issues[0] if plan.blocker_issues else (plan.notes[0] if plan.notes else None)
            self.auditor.update_backfill_status(plan.symbol, final_status, note=note)
            self._save_plan_record(plan, result)
            self._persist_repair_receipt(plan, result)
            return result

        iface = self._ensure_interface()
        if iface is None:
            result = RepairExecutionResult(
                symbol=plan.symbol,
                status="failed",
                failed_tasks=len(plan.tasks),
                blocker_issues=["后台修复接口不可用，任务未能入队"],
                notes=plan.notes,
            )
            self.auditor.update_backfill_status(plan.symbol, "failed", note=result.blocker_issues[0])
            self._save_plan_record(plan, result)
            self._persist_repair_receipt(plan, result)
            return result

        queued = 0
        failed = 0
        for task in plan.tasks:
            try:
                ok = iface.schedule_backfill(
                    stock_code=task.stock_code,
                    start_date=task.start_date,
                    end_date=task.end_date,
                    period=task.period,
                    priority=task.priority_hint,
                    reason=task.reason,
                    current_symbol=task.current_symbol,
                    gap_length=task.gap_length,
                )
            except Exception:
                logger.exception(
                    "Golden 1D repair task schedule failed: %s %s %s~%s",
                    task.stock_code,
                    task.period,
                    task.start_date,
                    task.end_date,
                )
                ok = False
            if ok:
                queued += 1
            else:
                failed += 1

        final_status: BackfillStatus
        if queued > 0 and failed == 0:
            final_status = "queued"
        elif queued > 0:
            final_status = "blocked"
        else:
            final_status = "failed"

        note = None
        if final_status == "queued":
            note = f"后台补齐任务已入队: queued={queued}"
        elif final_status == "blocked":
            note = f"后台补齐任务部分入队: queued={queued}, failed={failed}"
        else:
            note = "后台补齐任务入队失败"

        result = RepairExecutionResult(
            symbol=plan.symbol,
            status=final_status,
            queued_tasks=queued,
            failed_tasks=failed,
            blocker_issues=plan.blocker_issues,
            notes=plan.notes,
        )
        self.auditor.update_backfill_status(plan.symbol, final_status, note=note)
        self._save_plan_record(plan, result)
        self._persist_repair_receipt(plan, result)
        return result

    def audit_and_schedule(
        self,
        symbol: str,
        force_full: bool = False,
        current_symbol: str = "",
    ) -> RepairExecutionResult:
        summary = self.auditor.audit_symbol(symbol, force_full=force_full)
        plan = self.plan_summary(summary, current_symbol=current_symbol)
        return self.execute_plan(plan)
