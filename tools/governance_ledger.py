"""
tools/governance_ledger.py — 豁免治理台账

每次豁免生效时，将关键审计字段以 JSONL 格式追加写入
logs/governance_ledger.jsonl，供合规审计和抽检脚本使用。

台账格式（每行一条 JSON）：
{
  "ts": "2026-03-07T23:41:37",       // 写入时间
  "approval_id": "OA-20260307-001",  // 审批单号
  "phase": 0,                        // 适用阶段
  "effective_target": 0.27,          // 豁免后目标
  "expires": "2026-03-21",           // 到期日
  "approvers": ["team-lead@..."],    // 审批人列表
  "reason_excerpt": "Phase 0 引导...",// reason 前100字符（不存全文，防泄露）
  "commit_sha": "b5c2aa893039",      // HEAD commit
  "pipeline_id": "local",            // CI pipeline ID
  "env": "dev"                       // 运行环境
}
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

try:
    from tools.config import LEDGER as _LCFG
except ImportError:
    from config import LEDGER as _LCFG  # type: ignore[no-redef]

_logger = logging.getLogger("ci_gate.governance_ledger")

_ROOT = Path(__file__).resolve().parents[1]
_LEDGER_FILE = _ROOT / "logs" / "governance_ledger.jsonl"
_ARCHIVE_INDEX_FILE = _ROOT / "logs" / _LCFG.archive_index_filename

# 向后兼容：其他模块若直接引用这两个常量仍可正常工作
_REASON_MAX_LEN: int = _LCFG.reason_max_len
_RETENTION_DAYS: int = _LCFG.retention_days


def _get_commit_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short=12", "HEAD"],
            text=True, cwd=str(_ROOT), timeout=5,
        ).strip()
    except Exception:
        return "unknown"


def _get_pipeline_id() -> str:
    for var in ("CI_PIPELINE_ID", "GITHUB_RUN_ID"):
        val = os.environ.get(var, "")
        if val:
            return val
    return "local"


def _get_env() -> str:
    return os.environ.get("CI_ENV", os.environ.get("ENVIRONMENT", "dev"))


def _compute_prev_hash() -> str:
    """
    读取台账文件末尾一条有效记录，计算其 SHA-256 哈希值作为链式字段。
    空文件或首条记录返回字面量 'genesis'。
    链式字段的作用：任何对历史记录的事后篡改都会导致后续记录的 prev_hash 对不上，
    便于审计脚本做一致性校验。
    """
    if not _LEDGER_FILE.exists() or _LEDGER_FILE.stat().st_size == 0:
        return "genesis"
    try:
        last_line = ""
        with _LEDGER_FILE.open("r", encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if stripped:
                    last_line = stripped
        return hashlib.sha256(last_line.encode("utf-8")).hexdigest() if last_line else "genesis"
    except OSError:
        return "genesis"


def archive_old_records(retention_days: int = _RETENTION_DAYS) -> int:
    """
    将超过 ``retention_days`` 天的台账记录归档到同目录的月度压缩历史文件中，
    主台账只保留近期记录，避免文件无限增长影响读取/合规审计性能。
    归档完成同时更新追溯索引文件（governance_ledger_archive_index.json）。

    归档策略：
    - 归档文件名：``governance_ledger_archive_<YYYYMM>.jsonl.gz``
    - 归档文件按当前月份追加（月末自动切换新文件）
    - 写入失败时静默忽略，主台账保持不变
    - 返回归档记录数（0 表示无需归档或操作失败）
    """
    if not _LEDGER_FILE.exists():
        return 0
    cutoff = datetime.now() - timedelta(days=retention_days)
    recent_lines: list[str] = []
    archive_lines: list[str] = []
    try:
        for raw in _LEDGER_FILE.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                ts = datetime.fromisoformat(record.get("ts", ""))
                (archive_lines if ts < cutoff else recent_lines).append(line)
            except (json.JSONDecodeError, ValueError):
                recent_lines.append(line)  # 解析失败的行保守保留
    except OSError:
        return 0

    if not archive_lines:
        return 0

    archive_filename = f"governance_ledger_archive_{datetime.now().strftime('%Y%m')}.jsonl.gz"
    archive_file = _LEDGER_FILE.parent / archive_filename
    try:
        # 追加到归档压缩文件
        existing: list[str] = []
        if archive_file.exists():
            with gzip.open(str(archive_file), "rt", encoding="utf-8") as gz:
                existing = [ln.strip() for ln in gz if ln.strip()]
        with gzip.open(str(archive_file), "wt", encoding="utf-8") as gz:
            for ln in existing + archive_lines:
                gz.write(ln + "\n")
        # 重写主台账，仅保留近期记录
        with _LEDGER_FILE.open("w", encoding="utf-8") as fh:
            for ln in recent_lines:
                fh.write(ln + "\n")
    except OSError:
        return 0

    # 更新追溯索引
    _update_archive_index(archive_file, archive_filename, existing, archive_lines)
    _logger.info("归档台账：%s 条记录 → %s", len(archive_lines), archive_filename)
    return len(archive_lines)


def _update_archive_index(
    archive_file: Path,
    archive_filename: str,
    existing_lines: list[str],
    new_lines: list[str],
) -> None:
    """
    更新归档追溯索引文件（governance_ledger_archive_index.json）。

    索引格式：
    [
      {
        "archive_file": "governance_ledger_archive_202603.jsonl.gz",
        "record_count":  50,
        "ts_start":      "2026-01-01T00:00:00",
        "ts_end":        "2026-02-28T23:59:59",
        "file_sha256":   "<hex>",
        "updated_at":    "2026-03-08T10:00:00"
      },
      ...
    ]

    索引用途：
    - 审计时无需解压所有归档，直接通过时间范围定位对应归档文件
    - file_sha256 可校验归档文件是否被篡改
    """
    all_lines = existing_lines + new_lines
    ts_values: list[str] = []
    for ln in all_lines:
        try:
            ts_values.append(json.loads(ln).get("ts", ""))
        except (json.JSONDecodeError, KeyError):
            pass
    ts_sorted = sorted(t for t in ts_values if t)

    # 计算归档文件的 SHA-256（归档已写入磁盘）
    try:
        file_sha256 = hashlib.sha256(archive_file.read_bytes()).hexdigest()
    except OSError:
        file_sha256 = "unknown"

    entry = {
        "schema_version": _LCFG.archive_index_schema_version,
        "archive_file":  archive_filename,
        "record_count":  len(all_lines),
        "ts_start":      ts_sorted[0] if ts_sorted else "",
        "ts_end":        ts_sorted[-1] if ts_sorted else "",
        "file_sha256":   file_sha256,
        "updated_at":    datetime.now().isoformat(timespec="seconds"),
    }

    # 读入已有索引，更新或新增对应条目
    index: list[dict] = []
    try:
        if _ARCHIVE_INDEX_FILE.exists():
            index = json.loads(_ARCHIVE_INDEX_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        index = []

    updated = False
    for i, item in enumerate(index):
        if item.get("archive_file") == archive_filename:
            index[i] = entry
            updated = True
            break
    if not updated:
        index.append(entry)

    try:
        _ARCHIVE_INDEX_FILE.write_text(
            json.dumps(index, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError:
        pass  # 索引写入失败不阻断主流程


def append_waiver_use(waiver: dict) -> None:
    """
    将一次豁免使用记录追加写入台账文件。

    参数 waiver 为 _read_coverage_waiver() 返回的字典，
    包含 approval_id, effective_target, expires, reason, approver, phase 等字段。
    """
    _LEDGER_FILE.parent.mkdir(exist_ok=True)

    reason_full: str = waiver.get("reason", "")
    reason_excerpt = reason_full[:_REASON_MAX_LEN] + ("…" if len(reason_full) > _REASON_MAX_LEN else "")

    # approvers 可能是逗号分隔字符串（多人）
    approvers_raw: str = waiver.get("approver", "")
    approvers = [a.strip() for a in approvers_raw.split(",") if a.strip()]

    record = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "approval_id": waiver.get("approval_id", "N/A"),
        "phase": waiver.get("phase", "?"),
        "effective_target": waiver.get("effective_target"),
        "expires": waiver.get("expires"),
        "expires_in_days": waiver.get("expires_in_days"),
        "approvers": approvers,
        "reason_excerpt": reason_excerpt,
        "commit_sha": _get_commit_sha(),
        "pipeline_id": _get_pipeline_id(),
        "env": _get_env(),
        # 链式哈希：前一条记录的 SHA-256，防止事后篡改历史记录
        "prev_hash": _compute_prev_hash(),
    }
    try:
        with _LEDGER_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        _logger.info("新增台账记录：approval_id=%s phase=%s", record["approval_id"], record["phase"])
    except OSError:
        pass  # 台账写入失败不应阻断主流程，仅静默忽略
