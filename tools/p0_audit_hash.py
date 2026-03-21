"""
p0_audit_hash.py — 审计产物哈希记录工具

在发布流程或夜间巡检后运行，生成所有门禁产物的 SHA-256 哈希，
写入 artifacts/p0_audit_hash.json，便于后续追责和复盘：
  - 确认产物未被篡改
  - 锁定发布时刻的精确快照版本
  - 作为 git-blame 之外的独立审计链

用法：
  python tools/p0_audit_hash.py             # 记录当前哈希
  python tools/p0_audit_hash.py --verify    # 对比上次记录，检测变更

Release 流程建议：
  1. python tools/p0_gate_check.py --strict --json --enforce-allowlist-expiry > artifacts/p0_metrics_latest.json
  2. python tools/p0_audit_hash.py          # 锁定哈希
  3. 将 artifacts/p0_audit_hash.json 纳入发布 tag 的 release notes
"""
import argparse
import hashlib
import json
import pathlib
import sys
from datetime import datetime, timezone

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"

# 需要哈希记录的产物文件（相对于 PROJECT_ROOT）
AUDIT_FILES = [
    "artifacts/p0_metrics_latest.json",
    "artifacts/p0_metrics_final_verify.json",
    "artifacts/p0_release_evidence.md",
    ".p0_baseline.json",
]

OUTPUT_FILE = ARTIFACTS_DIR / "p0_audit_hash.json"


def _sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _collect_hashes() -> dict:
    result: dict[str, str | None] = {}
    for rel in AUDIT_FILES:
        p = PROJECT_ROOT / rel
        result[rel] = _sha256(p) if p.exists() else None
    return result


def record() -> None:
    hashes = _collect_hashes()
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 读取门禁 JSON 中的 gate 元信息（如果存在）
    gate_meta: dict = {}
    gate_file = PROJECT_ROOT / "artifacts" / "p0_metrics_latest.json"
    if gate_file.exists():
        try:
            g = json.loads(gate_file.read_text(encoding="utf-8"))
            gate_meta = {
                "strict_gate_pass": g.get("strict_gate_pass"),
                "P0_open_count": g.get("P0_open_count"),
                "active_critical_high": g.get("active_critical_high"),
                "gate_version": g.get("gate_version"),
                "gate_contract_valid": g.get("gate_contract_valid"),
                "gate_contract_version": g.get("gate_contract_version"),
            }
        except (json.JSONDecodeError, KeyError):
            pass

    doc = {
        "_recorded_at": now_str,
        "_gate_snapshot": gate_meta,
        "files": {
            rel: {"sha256": h, "exists": h is not None}
            for rel, h in hashes.items()
        },
    }

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")

    missing = [r for r, h in hashes.items() if h is None]
    print(f"[OK] 审计哈希已记录: {OUTPUT_FILE.relative_to(PROJECT_ROOT)}")
    print(f"     时间: {now_str}")
    print(f"     文件数: {len(hashes) - len(missing)} / {len(hashes)}")
    if missing:
        for m in missing:
            print(f"     [SKIP] 文件不存在: {m}")
    if gate_meta:
        print(f"     门禁状态: strict_gate_pass={gate_meta.get('strict_gate_pass')}  active_critical_high={gate_meta.get('active_critical_high')}")


def verify() -> None:
    if not OUTPUT_FILE.exists():
        print("[FAIL] 未找到历史哈希记录，请先运行: python tools/p0_audit_hash.py", file=sys.stderr)
        sys.exit(1)

    stored = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
    current = _collect_hashes()
    recorded_at = stored.get("_recorded_at", "unknown")

    print(f"[INFO] 对比基准: {recorded_at}")

    changed: list[str] = []
    missing_now: list[str] = []
    appeared: list[str] = []

    for rel, cur_hash in current.items():
        stored_entry = stored.get("files", {}).get(rel, {})
        stored_hash = stored_entry.get("sha256")
        stored_exists = stored_entry.get("exists", False)

        if stored_hash is None and cur_hash is not None:
            appeared.append(rel)
        elif cur_hash is None and stored_exists:
            missing_now.append(rel)
        elif cur_hash != stored_hash:
            changed.append(rel)

    if not changed and not missing_now:
        print(f"[OK]   所有 {len(current)} 个产物哈希与记录一致，无篡改")
        if appeared:
            for a in appeared:
                print(f"       [NEW] 新增文件（历史记录中不存在）: {a}")
    else:
        if changed:
            print("[WARN] 以下文件哈希已变更（可能是正常更新或篡改）：", file=sys.stderr)
            for c in changed:
                print(f"       {c}", file=sys.stderr)
        if missing_now:
            print("[FAIL] 以下文件已消失：", file=sys.stderr)
            for m in missing_now:
                print(f"       {m}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="P0 审计产物哈希记录")
    parser.add_argument("--verify", action="store_true", help="对比上次记录，检测变更")
    args = parser.parse_args()
    if args.verify:
        verify()
    else:
        record()


if __name__ == "__main__":
    main()
