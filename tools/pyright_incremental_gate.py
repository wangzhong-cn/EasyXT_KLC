#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


DIAG_PATTERN = re.compile(
    r"^(.*?):(\d+):(\d+) - (error|warning): (.*?)(?:\s+\((report[^)]+)\))?$"
)


@dataclass
class Diagnostic:
    file: str
    line: int
    col: int
    severity: str
    message: str
    rule: str

    @property
    def fingerprint(self) -> str:
        payload = f"{self.file}|{self.line}|{self.rule}|{self.message}"
        return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()


def parse_diagnostics(text: str) -> list[Diagnostic]:
    result: list[Diagnostic] = []
    for line in text.splitlines():
        m = DIAG_PATTERN.match(line)
        if not m:
            continue
        file, ln, col, severity, message, rule = m.groups()
        result.append(
            Diagnostic(
                file=file,
                line=int(ln),
                col=int(col),
                severity=severity,
                message=message,
                rule=rule or "<none>",
            )
        )
    return result


def run_pyright(project: str) -> tuple[int, str]:
    npx = shutil.which("npx")
    npm = shutil.which("npm")
    if npx:
        cmd = [npx, "--yes", "pyright", "--project", project]
    elif npm:
        cmd = [npm, "exec", "--yes", "pyright", "--", "--project", project]
    else:
        raise RuntimeError("npx/npm 未找到，请先安装 Node.js 或将其加入 PATH")
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="ignore")
    output = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    return p.returncode, output


def load_baseline(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"fingerprints": []}
    return json.loads(path.read_text(encoding="utf-8"))


def build_report(
    all_diags: list[Diagnostic],
    new_diags: list[Diagnostic],
    removed_count: int,
    baseline_count: int,
) -> dict[str, Any]:
    by_rule: dict[str, int] = {}
    by_severity: dict[str, int] = {}
    for d in all_diags:
        by_rule[d.rule] = by_rule.get(d.rule, 0) + 1
        by_severity[d.severity] = by_severity.get(d.severity, 0) + 1
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_current": len(all_diags),
        "total_baseline": baseline_count,
        "new_count": len(new_diags),
        "removed_count": removed_count,
        "by_rule": dict(sorted(by_rule.items(), key=lambda x: x[1], reverse=True)),
        "by_severity": by_severity,
        "new_items": [asdict(d) for d in new_diags],
    }


def write_md(report: dict[str, Any], output: Path) -> None:
    lines: list[str] = []
    lines.append("# Pyright Incremental Gate Report")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    lines.append(f"| Current total | {report['total_current']} |")
    lines.append(f"| Baseline total | {report['total_baseline']} |")
    lines.append(f"| New diagnostics | {report['new_count']} |")
    lines.append(f"| Removed diagnostics | {report['removed_count']} |")
    lines.append("")
    lines.append("## New Diagnostics")
    lines.append("")
    if not report["new_items"]:
        lines.append("No new diagnostics.")
    else:
        lines.append("| Severity | Rule | File | Line | Message |")
        lines.append("|---|---|---|---:|---|")
        for item in report["new_items"][:200]:
            msg = item["message"].replace("|", "/")
            lines.append(
                f"| {item['severity']} | {item['rule']} | {item['file']} | {item['line']} | {msg} |"
            )
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="pyrightconfig.main.json")
    parser.add_argument("--baseline", default=".ci/pyright_main_baseline.json")
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--update-baseline", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--reason", default="")
    parser.add_argument("--actor", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--ref", default="")
    args = parser.parse_args()

    artifacts_dir = Path(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    baseline_path = Path(args.baseline)
    baseline_path.parent.mkdir(parents=True, exist_ok=True)

    _, output = run_pyright(args.project)
    all_diags = parse_diagnostics(output)

    baseline = load_baseline(baseline_path)
    baseline_set = set(baseline.get("fingerprints", []))
    current_set = {d.fingerprint for d in all_diags}
    new_diags = [d for d in all_diags if d.fingerprint not in baseline_set]
    removed_count = len([fp for fp in baseline_set if fp not in current_set])

    report = build_report(all_diags, new_diags, removed_count, len(baseline_set))
    (artifacts_dir / "pyright_incremental_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    write_md(report, artifacts_dir / "pyright_incremental_report.md")
    (artifacts_dir / "pyright_output.txt").write_text(output, encoding="utf-8")

    if args.update_baseline:
        reason = (args.reason or "").strip()
        if not reason:
            print("[FAIL] baseline 更新必须提供 --reason（Issue/PR 链接或编号）")
            return 1
        new_baseline = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "project": args.project,
            "count": len(current_set),
            "update_reason": reason,
            "updated_by": args.actor,
            "run_id": args.run_id,
            "ref": args.ref,
            "fingerprints": sorted(current_set),
        }
        baseline_path.write_text(json.dumps(new_baseline, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] baseline updated: {baseline_path} ({len(current_set)})")
        return 0

    if args.strict and new_diags:
        print(f"[FAIL] new diagnostics: {len(new_diags)}")
        return 1
    print(
        f"[OK] current={len(all_diags)} baseline={len(baseline_set)} new={len(new_diags)} removed={removed_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
