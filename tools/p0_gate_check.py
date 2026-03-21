"""
P0 门禁检查脚本 v2 — EasyXT 数据基础设施放行铁门槛
=======================================================
用途：在 CI / 发布前运行，任一 P0 项新增违规则以非零退出码阻断流程。

使用方法：
  python tools/p0_gate_check.py --summary            # 全部 P0 状态汇总（含修复建议）
  python tools/p0_gate_check.py --strict             # 严格模式：任一 P0 fail 则退出码=1
  python tools/p0_gate_check.py --new-only           # PR 模式：仅检查新增违规（对比 baseline）
  python tools/p0_gate_check.py --check credential   # 仅检查凭据扫描
  python tools/p0_gate_check.py --check timestamp    # 仅检查时间戳合约（含修复建议）
  python tools/p0_gate_check.py --check publish      # 仅检查原子发布
  python tools/p0_gate_check.py --check sql          # 仅检查 SQL 注入（AST 级）
  python tools/p0_gate_check.py --check xtdata       # 仅检查 xtdata 裸导入
  python tools/p0_gate_check.py --check sla          # 仅检查最新 SLA 日报门禁
  python tools/p0_gate_check.py --check fake_ohlcv   # 仅检查测试中伪造 OHLCV（红线扫描，warn 级）
  python tools/p0_gate_check.py --save-baseline      # 保存当前违规为 baseline（首次运行或批量接受技术债）
  python tools/p0_gate_check.py --json               # JSON 输出（CI 解析用）

Windows 本地管道（PowerShell）需设 UTF-8 环境：
  $env:PYTHONUTF8=1; python tools/p0_gate_check.py --json | python -X utf8 -c "..."
Linux/CI 环境（GitHub Actions / bash）无需额外配置，UTF-8 默认即可。

CI 阻断矩阵：
  PR 阶段（--new-only）：仅阻断本次 PR 新增违规，存量技术债不阻断
  夜间巡检（--strict）  ：全量扫描，任一 P0 fail 即阻断

放行铁门槛（发布前必须全部通过）：
  P0_open_count            == 0
  strict_pass              == true
  credential_scan          == pass
  sql_injection_scan       == pass
  timestamp_contract_check == pass
  xtdata_import_check      == pass
  snapshot_publish_atomic  == pass
  sla_daily_gate           == pass
"""
from __future__ import annotations

import argparse
import ast
import datetime
import json
import os
import pathlib
import re
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Literal

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
BASELINE_FILE = PROJECT_ROOT / ".p0_baseline.json"

# 脚本版本号：与 baseline 签名绑定，跨版本使用旧 baseline 时会发出警告
SCRIPT_VERSION = "2.4.0"

try:
    from tools.release_rag_policy import gate_detail_tag, header_rag_status, parse_gate_detail_tag, period_validation_summary
except Exception:
    try:
        from release_rag_policy import gate_detail_tag, header_rag_status, parse_gate_detail_tag, period_validation_summary
    except Exception:
        gate_detail_tag = None
        header_rag_status = None
        parse_gate_detail_tag = None
        period_validation_summary = None
try:
    from tools.duckdb_crash_signature_gate import scan_duckdb_crash_signatures
except Exception:
    try:
        from duckdb_crash_signature_gate import scan_duckdb_crash_signatures
    except Exception:
        scan_duckdb_crash_signatures = None


def _get_git_commit() -> str:
    """获取当前 git commit hash（短格式），用于 baseline 签名。失败时返回 'unknown'。"""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=3,
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except Exception:
        return "unknown"

# ─────────────────────────────────────────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────────────────────────────────────────

Status = Literal["pass", "fail", "warn", "skip"]
Severity = Literal["critical", "high", "medium", "low"]


@dataclass
class Violation:
    location: str       # "path/to/file.py:42"
    message: str        # 人可读描述
    severity: Severity = "high"
    fix_hint: str = ""  # 自动修复建议（单行）

    def key(self) -> str:
        """用于 baseline 对比的唯一键（路径:行号，不含可变消息内容）"""
        return self.location

    def to_dict(self) -> dict:
        d = {"location": self.location, "message": self.message, "severity": self.severity}
        if self.fix_hint:
            d["fix_hint"] = self.fix_hint
        return d


@dataclass
class CheckResult:
    name: str
    status: Status
    detail: str = ""
    violations: list[Violation] = field(default_factory=list)
    # 被 allowlist 过滤掉的条目数
    suppressed: int = 0

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "violations": [v.to_dict() for v in self.violations[:20]],
            "suppressed": self.suppressed,
        }

    # 兼容旧代码，提供 violations 的字符串列表形式
    def violation_lines(self) -> list[str]:
        return [f"{v.location}: {v.message}" for v in self.violations]


@dataclass
class AllowEntry:
    """
    Allowlist 条目（治理规范约束）：
      pattern   — violation.key() 的前缀或精确值
      reason    — 豁免原因（必填；空值在 --strict 时阻断 CI）
      owner     — 负责人或团队（必填；空值在 --strict 时阻断 CI）
      expire    — 到期日 YYYY-MM-DD；推荐值: 新增/续期时设为 today + 12 个月。
                  '2099-12-31' 已停用，Release Gate 将把该哨兵值视为需复审条目。
      issue_ref — 关联 issue/PR 编号或 URL（续期时必须填写，保证审计可追踪）。
                  例: 'https://github.com/org/repo/issues/42' 或 '#42'
    """
    pattern: str
    reason: str
    owner: str
    expire: str       # YYYY-MM-DD，推荐: today + 12 个月
    issue_ref: str = ""  # 关联 issue/PR；续期时必须填写


# ─────────────────────────────────────────────────────────────────────────────
# Allowlist（已知技术债，暂时豁免）
# 每条条目必须填写 reason / owner / expire，CI 在 --strict 时检查空值。
# 修复对应违规后请立即删除该条目，避免 allowlist 成为永久垃圾桶。
# ─────────────────────────────────────────────────────────────────────────────

ALLOWLIST: dict[str, list[AllowEntry]] = {
    "credential_scan": [
        AllowEntry(
            pattern="tools/p0_gate_check.py",
            reason="KNOWN_CREDENTIALS 为检测用字面量定义，非真实凭据",
            owner="team",
            expire="2027-01-01",
            issue_ref="#cred-fp-gate-check",
        ),
        AllowEntry(
            pattern="tools/fault_drill.py",
            reason="压测工具 acc_drill 占位值，不连接真实账户",
            owner="team",
            expire="2027-01-01",
            issue_ref="#cred-fp-fault-drill",
        ),
        AllowEntry(
            pattern="tools/parse_tdx_zixg.py",
            reason="账户ID 字段为中文注释占位符，非真实凭据",
            owner="team",
            expire="2027-01-01",
            issue_ref="#cred-fp-parse-tdx",
        ),
        AllowEntry(
            pattern="config/production_template.json",
            reason="模板文件中的已知QMT测试账户凭据（test1234！），仅本地开发环境，不与外部生产共享",
            owner="team",
            expire="2027-01-01",
            issue_ref="#cred-local-config-test1234",
        ),
        AllowEntry(
            pattern="config/real_trading.json",
            reason="本地实盘配置文件中的已知QMT测试账户凭据（test1234！），不提交公共仓库",
            owner="team",
            expire="2027-01-01",
            issue_ref="#cred-local-config-test1234",
        ),
        AllowEntry(
            pattern="config/unified_config.json",
            reason="本地统一配置文件中的已知QMT测试账户凭据（test1234！），不提交公共仓库",
            owner="team",
            expire="2027-01-01",
            issue_ref="#cred-local-config-test1234",
        ),
        AllowEntry(
            pattern="config/xueqiu_config.json",
            reason="本地雪球配置文件中的已知QMT测试账户凭据（test1234！），不提交公共仓库",
            owner="team",
            expire="2027-01-01",
            issue_ref="#cred-local-config-test1234",
        ),
        AllowEntry(
            pattern="tools/test_qmt_connection.py",
            reason="QMT连接测试脚本中的已知测试账户ID（test1101），非生产账户，不连接真实资产",
            owner="team",
            expire="2027-01-01",
            issue_ref="#cred-local-test-qmt",
        ),
    ],
    # 示例：已知技术债，在修复 sprint 内豁免（修复后必须删除）
    # "timestamp_contract_check": [
    #     AllowEntry(
    #         pattern="data_manager/auto_data_updater.py:132",
    #         reason="Sprint-3 存量技术债，2026-04-01 前修复，参见 issue #42",
    #         owner="backend-team",
    #         expire="2026-04-01",
    #     ),
    # ],
    "timestamp_contract_check": [],
    "sql_injection_scan": [
        AllowEntry(
            pattern="tools/_check_duckdb.py",
            reason="诊断工具表名来自DuckDB系统表枚举(information_schema.tables)，使用SQL标识符双引号转义，无外部输入；WHERE子句已参数化",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-check-duckdb",
        ),
        AllowEntry(
            pattern="tools/_ultimate_crossval_v3_fast.py",
            reason="print语句中'from SKIP_MARKETS'为误报，非SQL上下文",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-crossval",
        ),
        AllowEntry(
            pattern="tools/create_user_block.py",
            reason="print示例代码中含Python import语句'from easy_xt...'被误判为SQL关键词，非SQL上下文",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-create-user",
        ),
        AllowEntry(
            pattern="tools/_diagnose_v4.py",
            reason="print输出'datetime.fromtimestamp'/'utcfromtimestamp'中'from'被误判为SQL关键词，非SQL上下文",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-diagnose-v4",
        ),
        AllowEntry(
            pattern="tools/_fix_money_flow_sql.py",
            reason="修复脚本中正则表达式字符串本身包含SQL模板片段（'DELETE FROM'），用于AST替换，非执行SQL",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-fix-money-flow",
        ),
        AllowEntry(
            pattern="tools/p0_gate_check.py",
            reason="门禁脚本自身output消息包含'DELETE+INSERT'用于描述原子性规则，非SQL执行上下文",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-p0-gate-check",
        ),
        AllowEntry(
            pattern="tools/p0_trend_update.py",
            reason="Markdown模板中含文件名'p0_trend_update.py'，'update'被误判为SQL关键词，非SQL上下文",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-p0-trend-update",
        ),
        AllowEntry(
            pattern="tools/pyright_incremental_gate.py",
            reason="print语句中f-string打印文件路径变量，非SQL执行上下文",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-pyright-gate",
        ),
        AllowEntry(
            pattern="tools/stability_30d_report.py",
            reason="字符串注释'30天回归稳定性报告'被扫描器误判，非SQL上下文",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-stability-report",
        ),
        AllowEntry(
            pattern="tools/stability_evidence_board.py",
            reason="print语句中f-string打印文件路径变量，非SQL执行上下文",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-stability-board",
        ),
        AllowEntry(
            pattern="tools/strategy_impact_baseline_manager.py",
            reason="print语句中f-string打印文件路径变量，非SQL执行上下文",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-fp-baseline-mgr",
        ),
        AllowEntry(
            pattern="data_manager/unified_data_interface.py",
            reason="shadow表名来自_SAFE_TABLE_NAMES硬编码白名单校验（见_write_shadow_copy），无外部输入路径",
            owner="team",
            expire="2027-01-01",
            issue_ref="#sql-whitelist-shadow-table",
        ),
    ],
    "snapshot_publish_atomic": [],
    "xtdata_import_check": [],
}


def is_suppressed(check_name: str, violation: Violation) -> bool:
    """检查一条违规是否在 allowlist 中（已豁免），路径规范化后匹配。"""
    allowed = ALLOWLIST.get(check_name, [])
    # 统一用正斜杠比较，避免 Windows 反斜杠 vs 正斜杠不一致
    key = violation.key().replace("\\", "/")
    for entry in allowed:
        pat = entry.pattern.replace("\\", "/").rstrip(":")
        if key == pat or key.startswith(pat + ":") or key.startswith(pat + "/"):
            return True
    return False


def apply_allowlist(check_name: str, violations: list[Violation]) -> tuple[list[Violation], int]:
    """过滤 allowlist 中的违规，返回 (活跃违规, 被压制数量)"""
    active = [v for v in violations if not is_suppressed(check_name, v)]
    suppressed = len(violations) - len(active)
    return active, suppressed


# ─────────────────────────────────────────────────────────────────────────────
# Baseline（PR 模式：仅阻断新增违规）
# ─────────────────────────────────────────────────────────────────────────────

def load_baseline() -> dict[str, set[str]]:
    """加载 baseline 文件，返回 {check_name: {violation_key, ...}}；跨版本/过期时打印警告。"""
    if not BASELINE_FILE.exists():
        return {}
    try:
        raw = json.loads(BASELINE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    meta = raw.get("_meta", {})
    # 版本校验
    saved_ver = meta.get("script_version", "unknown")
    if saved_ver != SCRIPT_VERSION:
        # --json 模式必须保证 stdout 是纯 JSON，所有告警统一打到 stderr
        print(
            f"[WARN] Baseline 由脚本 v{saved_ver} 生成，当前版本 v{SCRIPT_VERSION}。"
            " 建议重新 --save-baseline 以同步签名。",
            file=sys.stderr,
        )
    # 过期策略：baseline 超过有效期后自动警告，要求刷新
    expires_at_str = meta.get("expires_at", "")
    if expires_at_str:
        try:
            exp = datetime.datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
            if datetime.datetime.now(datetime.timezone.utc) > exp:
                print(
                    f"[WARN] Baseline 已于 {expires_at_str} 过期（有效期 14 天）。"
                    " --new-only 仍可运行，但存量对比可能失准，请立即重新 --save-baseline 刷新。",
                    file=sys.stderr,
                )
        except ValueError:
            pass
    return {k: set(v) for k, v in raw.items() if not k.startswith("_")}


def save_baseline(results: list[CheckResult]) -> None:
    """将当前违规集合保存为 baseline，附带签名+过期元数据防止跨版本/长期误用。"""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    expires_at = (now_utc + datetime.timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    data: dict = {
        "_meta": {
            "script_version": SCRIPT_VERSION,
            "git_commit": _get_git_commit(),
            "saved_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "expires_at": expires_at,
            "violation_counts": {r.name: len(r.violations) for r in results},
        }
    }
    data.update({r.name: [v.key() for v in r.violations] for r in results})
    BASELINE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    meta = data["_meta"]
    print(
        f"[OK] Baseline 已保存: {BASELINE_FILE.relative_to(PROJECT_ROOT)}"
        f"  (v{meta['script_version']} | commit={meta['git_commit']} | {meta['saved_at']})"
    )


def filter_new_violations(
    results: list[CheckResult],
    baseline: dict[str, set[str]],
) -> list[CheckResult]:
    """仅保留相对 baseline 新增的违规（PR 模式）"""
    filtered = []
    for r in results:
        known = baseline.get(r.name, set())
        new_viols = [v for v in r.violations if v.key() not in known]
        new_status: Status = "fail" if new_viols else ("pass" if r.status == "pass" else "warn")
        filtered.append(CheckResult(
            name=r.name,
            status=new_status,
            detail=f"{r.detail} | 新增: {len(new_viols)}, 存量(豁免): {len(r.violations) - len(new_viols)}",
            violations=new_viols,
            suppressed=r.suppressed,
        ))
    return filtered


# ─────────────────────────────────────────────────────────────────────────────
# 时间戳修复建议库
# ─────────────────────────────────────────────────────────────────────────────

# 每种危险模式对应的 (描述, 修复建议)
TIMESTAMP_FIX_HINTS: list[tuple[re.Pattern, str, str]] = [
    (
        re.compile(r"datetime\.fromtimestamp\([^,)]+/\s*1000\s*\)"),
        "datetime.fromtimestamp(x/1000) 无时区",
        "改为: datetime.fromtimestamp(x/1000, tz=ZoneInfo('Asia/Shanghai'))",
    ),
    (
        re.compile(r"datetime\.fromtimestamp\([^,)]+\)(?!\s*#.*tz)"),
        "datetime.fromtimestamp() 无时区",
        "改为: datetime.fromtimestamp(ts, tz=ZoneInfo('Asia/Shanghai'))",
    ),
    (
        re.compile(r"pd\.to_datetime\([^)]+unit=['\"]ms['\"]\s*\)(?!.*utc=True)"),
        "pd.to_datetime(unit=ms) 无 utc=True",
        "改为: pd.to_datetime(ts, unit='ms', utc=True).dt.tz_convert('Asia/Shanghai')",
    ),
    (
        re.compile(r"datetime\.now\(\s*\)"),
        "datetime.now() 无时区",
        "改为: datetime.now(tz=ZoneInfo('Asia/Shanghai'))",
    ),
    (
        re.compile(r"date\.today\(\s*\)"),
        "date.today() 依赖系统时区",
        "改为: datetime.now(tz=ZoneInfo('Asia/Shanghai')).date()",
    ),
]


# ─────────────────────────────────────────────────────────────────────────────
# SQL 注入：输入来源分级（A=外部直达，B=内部查询）
# ─────────────────────────────────────────────────────────────────────────────

# Tier A 文件特征：外部请求参数直接进入 SQL（优先修复，Batch A）
_EXTERNAL_SQL_PATTERNS = frozenset({
    "api_server", "trade_api", "routes", "handler",
    "endpoint", "gateway", "server", "api",
})


def _sql_source_tier(filepath: pathlib.Path) -> str:
    """
    判断 SQL 注入漏洞点的输入来源层级：
      A-外部：API 参数/UI 参数/配置输入直达 SQL → 优先修复（Batch A）
      B-内部：审计日志/内部统计查询 → 可延后（Batch B）
    """
    for part in filepath.parts:
        if part.lower() in _EXTERNAL_SQL_PATTERNS:
            return "A-外部"
    if filepath.stem.lower() in _EXTERNAL_SQL_PATTERNS:
        return "A-外部"
    return "B-内部"


# ─────────────────────────────────────────────────────────────────────────────
# AST 级 SQL 注入检测辅助
# ─────────────────────────────────────────────────────────────────────────────

def _is_fstring_sql_injection(node: ast.JoinedStr) -> bool:
    """
    判断一个 f-string AST 节点是否构成 SQL 注入风险：
    f-string 中包含 SQL 关键词（WHERE/DELETE/INSERT/SELECT）且含变量插值。
    使用单词边界匹配，避免 selected/deleted/updated/fromtimestamp 等误报。
    """
    _SQL_KW_RE = re.compile(
        r'\b(?:where|delete|insert|select|update|drop|from)\b'
    )
    # 收集 f-string 的常量部分（SQL 骨架）
    sql_skeleton = ""
    has_interpolation = False
    for part in node.values:
        if isinstance(part, ast.Constant) and isinstance(part.value, str):
            sql_skeleton += part.value.lower()
        elif isinstance(part, ast.FormattedValue):
            has_interpolation = True
    if not has_interpolation:
        return False
    # 判断是否包含 SQL 关键词（单词边界匹配，防止 selected/deleted 误报）
    return bool(_SQL_KW_RE.search(sql_skeleton))


def _ast_scan_sql_injections(src: str, filepath: pathlib.Path) -> list[Violation]:
    """
    AST 级扫描：找出文件中所有含 SQL 关键词的 f-string 变量插值。
    比 regex 更精确：跨行 f-string、括号内换行都能正确识别。
    """
    violations = []
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []  # 无法解析的文件跳过

    rel = filepath.relative_to(PROJECT_ROOT)
    for node in ast.walk(tree):
        # ast.JoinedStr 是 f-string 的 AST 类型
        if isinstance(node, ast.JoinedStr):
            if _is_fstring_sql_injection(node):
                # 进一步确认：插值变量来自外部（非枚举常量）
                for part in node.values:
                    if isinstance(part, ast.FormattedValue):
                        # 如果插值的是函数调用返回值（如 _get_table_name()）风险较低（medium）
                        severity: Severity = "high"
                        if isinstance(part.value, ast.Call):
                            severity = "medium"
                        # 拼出原始代码片段（用行号定位）
                        line_no = getattr(node, "lineno", 0)
                        # 取原始行作为上下文
                        lines = src.splitlines()
                        snippet = lines[line_no - 1].strip()[:80] if line_no > 0 else ""
                        tier = _sql_source_tier(filepath)
                        violations.append(Violation(
                            location=f"{rel}:{line_no}",
                            message=f"[{tier}] f-string SQL 插值（AST检测）: {snippet}",
                            severity=severity,
                            fix_hint="改为参数化: con.execute('SELECT ... WHERE col = ?', [value])",
                        ))
                        break  # 同一 f-string 只报一次
    return violations


# ─────────────────────────────────────────────────────────────────────────────
# 检查函数
# ─────────────────────────────────────────────────────────────────────────────

def check_credential_scan() -> CheckResult:
    """P0-S1: 扫描明文凭据（含 allowlist 过滤）"""
    KNOWN_CREDENTIALS = ["test1101", "test1234"]
    DANGER_PATTERN = re.compile(
        r"""(ACCOUNT_ID|PASSWORD|password|account_id)\s*=\s*["'][^"'${}]{4,}["']"""
    )
    raw_violations: list[Violation] = []
    scan_paths = [
        PROJECT_ROOT / "tools",
        PROJECT_ROOT / "config",
        PROJECT_ROOT / "data_manager",
        PROJECT_ROOT / "easy_xt",
        PROJECT_ROOT / "core",
    ]
    for root in scan_paths:
        for f in root.rglob("*"):
            if f.suffix not in (".py", ".json", ".yaml", ".yml", ".toml", ".env"):
                continue
            try:
                src = f.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            rel = f.relative_to(PROJECT_ROOT)
            lines = src.splitlines()

            for i, line in enumerate(lines, 1):
                # 检查1: 已知凭据字符串
                for cred in KNOWN_CREDENTIALS:
                    if cred in line and "os.environ" not in line and "${" not in line and "#" not in line.lstrip()[:3]:
                        raw_violations.append(Violation(
                            location=f"{rel}:{i}",
                            message=f"含已知凭据字面量 '{cred}': {line.strip()[:60]}",
                            severity="critical",
                            fix_hint=f"改为: os.environ.get('EASYXT_{cred.upper()}', '')",
                        ))
                # 检查2: 赋值模式
                m = DANGER_PATTERN.search(line)
                if m and "os.environ" not in line and "${" not in line:
                    raw_violations.append(Violation(
                        location=f"{rel}:{i}",
                        message=f"疑似明文凭据赋值: {m.group()[:60]}",
                        severity="critical",
                        fix_hint="改为环境变量: os.environ.get('EASYXT_CREDENTIAL', '')",
                    ))

    # 去重（同一位置可能被多个模式匹配）
    seen: set[str] = set()
    unique: list[Violation] = []
    for v in raw_violations:
        if v.location not in seen:
            seen.add(v.location)
            unique.append(v)
    unique.sort(key=lambda v: v.location)

    violations, suppressed = apply_allowlist("credential_scan", unique)
    status: Status = "fail" if violations else "pass"
    detail = (
        f"发现 {len(violations)} 处明文凭据"
        + (f"（另有 {suppressed} 处已豁免）" if suppressed else "")
        if violations else "未发现明文凭据"
    )
    return CheckResult("credential_scan", status, detail, violations, suppressed)


def check_sql_injection() -> CheckResult:
    """
    P0-S2: SQL 注入检测（AST 级 + regex 双重扫描）
    - AST 级：精确识别跨行 f-string，区分 high/medium severity
    - regex 级：兜底捕获 AST 解析失败的文件（语法错误等）
    """
    src_dirs = ["data_manager", "easy_xt", "core", "tools"]
    raw_violations: list[Violation] = []

    # regex 兜底（仅针对 AST 无法解析的文件）
    FALLBACK_PATTERN = re.compile(r"f['\"].*(?:WHERE|DELETE|INSERT|SELECT).*'\{", re.IGNORECASE)

    for d in src_dirs:
        root = PROJECT_ROOT / d
        if not root.exists():
            continue
        for f in root.rglob("*.py"):
            try:
                src = f.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue

            # 优先 AST 扫描
            ast_hits = _ast_scan_sql_injections(src, f)
            if ast_hits:
                raw_violations.extend(ast_hits)
            else:
                # AST 无命中时，regex 兜底（可能是跨文件拼接或 AST 解析失败）
                rel = f.relative_to(PROJECT_ROOT)
                for i, line in enumerate(src.splitlines(), 1):
                    if FALLBACK_PATTERN.search(line) and not line.strip().startswith("#"):
                        raw_violations.append(Violation(
                            location=f"{rel}:{i}",
                            message=f"SQL f-string 插值（regex兜底）: {line.strip()[:80]}",
                            severity="high",
                            fix_hint="改为参数化: con.execute('... WHERE col = ?', [value])",
                        ))

    violations, suppressed = apply_allowlist("sql_injection_scan", raw_violations)
    # 按 severity 排序：critical > high > medium > low
    SEV_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    violations.sort(key=lambda v: (SEV_ORDER.get(v.severity, 9), v.location))

    status: Status = "fail" if violations else "pass"
    high_count = sum(1 for v in violations if v.severity in ("critical", "high"))
    detail = (
        f"发现 {len(violations)} 处 SQL 注入（{high_count} 高危）"
        + (f"，另 {suppressed} 处已豁免" if suppressed else "")
        if violations else "SQL 注入检查通过（AST 级）"
    )
    return CheckResult("sql_injection_scan", status, detail, violations, suppressed)


def check_timestamp_contract() -> CheckResult:
    """
    P0-D1: 时间戳合约检查（含自动修复建议）
    每条违规附带具体的替换代码建议，让整改速度翻倍。
    """
    src_dirs = ["data_manager", "easy_xt", "core"]
    raw_violations: list[Violation] = []

    for d in src_dirs:
        root = PROJECT_ROOT / d
        if not root.exists():
            continue
        for f in root.rglob("*.py"):
            try:
                src = f.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            rel = f.relative_to(PROJECT_ROOT)
            lines = src.splitlines()
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                # 跳过注释行
                if stripped.startswith("#"):
                    continue
                for pat, desc, fix_hint in TIMESTAMP_FIX_HINTS:
                    if pat.search(line):
                        # 避免对已修复代码（含 ZoneInfo / timezone / tz= ）的误报
                        if "ZoneInfo" in line or "timezone" in line or "tz=" in line:
                            continue
                        # date.today() 在测试文件中是可接受的
                        if "date.today" in line and ("test_" in str(f) or f.name.startswith("test")):
                            continue
                        raw_violations.append(Violation(
                            location=f"{rel}:{i}",
                            message=f"[{desc}]  {stripped[:70]}",
                            severity="high",
                            fix_hint=fix_hint,
                        ))
                        break  # 同一行只报一次（第一个匹配的模式）

    violations, suppressed = apply_allowlist("timestamp_contract_check", raw_violations)

    status: Status = "fail" if violations else "pass"
    detail = (
        f"发现 {len(violations)} 处时区不安全操作"
        + (f"（另 {suppressed} 已豁免）" if suppressed else "")
        if violations else "时间戳合约检查通过"
    )
    return CheckResult("timestamp_contract_check", status, detail, violations, suppressed)


def check_xtdata_import() -> CheckResult:
    """P0-D2: 检测裸 `import xtdata`"""
    DANGER_PATTERN = re.compile(r"^\s*import xtdata\b", re.MULTILINE)
    src_dirs = ["data_manager", "easy_xt", "core", "tools", "strategies"]
    raw_violations: list[Violation] = []

    for d in src_dirs:
        root = PROJECT_ROOT / d
        if not root.exists():
            continue
        for f in root.rglob("*.py"):
            try:
                src = f.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            rel = f.relative_to(PROJECT_ROOT)
            for m in DANGER_PATTERN.finditer(src):
                line_no = src[: m.start()].count("\n") + 1
                raw_violations.append(Violation(
                    location=f"{rel}:{line_no}",
                    message="裸 `import xtdata`，QMT 可用性永远为 False",
                    severity="high",
                    fix_hint="改为: import xtquant.xtdata as xt",
                ))

    violations, suppressed = apply_allowlist("xtdata_import_check", raw_violations)
    status: Status = "fail" if violations else "pass"
    detail = (
        f"发现 {len(violations)} 处裸 import xtdata" if violations
        else "xtdata 导入规范检查通过"
    )
    return CheckResult("xtdata_import_check", status, detail, violations, suppressed)


def check_atomic_publish() -> CheckResult:
    """P0-D3: 检测 DELETE+INSERT 无事务包裹"""
    src_dirs = ["data_manager"]
    raw_violations: list[Violation] = []

    for d in src_dirs:
        root = PROJECT_ROOT / d
        if not root.exists():
            continue
        for f in root.rglob("*.py"):
            try:
                src = f.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            has_delete = "DELETE FROM" in src
            has_insert = "INSERT INTO" in src
            has_tx = any(kw in src for kw in ("BEGIN TRANSACTION", "con.execute(\"BEGIN", "COMMIT", "ROLLBACK"))
            if has_delete and has_insert and not has_tx:
                rel = f.relative_to(PROJECT_ROOT)
                raw_violations.append(Violation(
                    location=str(rel),
                    message="含 DELETE+INSERT 但无事务包裹，崩溃时数据半删半写",
                    severity="high",
                    fix_hint=(
                        "用 BEGIN TRANSACTION / COMMIT / ROLLBACK 包裹:\n"
                        "    con.execute('BEGIN TRANSACTION')\n"
                        "    try: con.execute('DELETE ...'); con.execute('INSERT ...')\n"
                        "         con.execute('COMMIT')\n"
                        "    except: con.execute('ROLLBACK'); raise"
                    ),
                ))

    violations, suppressed = apply_allowlist("snapshot_publish_atomic", raw_violations)
    status: Status = "fail" if violations else "pass"
    detail = (
        f"发现 {len(violations)} 个文件非原子 DELETE+INSERT" if violations
        else "原子发布检查通过"
    )
    return CheckResult("snapshot_publish_atomic", status, detail, violations, suppressed)


def check_schema_version() -> CheckResult:
    """P1: 血缘字段集成状态（warn 级，不阻断）"""
    target = PROJECT_ROOT / "data_manager" / "unified_data_interface.py"
    if not target.exists():
        return CheckResult("schema_version_check", "skip", "unified_data_interface.py 不存在")
    src = target.read_text(encoding="utf-8", errors="ignore")
    missing_fields = [f for f in ("schema_version", "ingest_run_id", "raw_hash", "source") if f not in src]
    if missing_fields:
        violations = [
            Violation(location="data_manager/unified_data_interface.py", message=f"缺失血缘字段: {missing_fields}",
                      severity="low", fix_hint="参见诊断报告附录 F — 数据血缘字段标准")
        ]
        return CheckResult("schema_version_check", "warn",
                           f"血缘字段未集成: {missing_fields}", violations)
    return CheckResult("schema_version_check", "pass", "血缘字段已集成")


# Release 模式标志：由 main() 通过 --enforce-allowlist-expiry 设置
# 过期 allowlist 条目 → critical（阻断 CI），默认 → high（仅 warn）
_enforce_allowlist_expiry: bool = False


def check_allowlist_governance() -> CheckResult:
    """
    P0-G1: Allowlist 治理检查
    每条豁免必须有 reason/owner/expire；
    空 reason/owner → critical（CI --strict 阻断）；
    条目过期 → high（默认 warn）或 critical（--enforce-allowlist-expiry 时阻断）。
    """
    issues: list[Violation] = []
    today = datetime.date.today()
    for check_name, entries in ALLOWLIST.items():
        for entry in entries:
            loc = f"ALLOWLIST[{check_name}]/{entry.pattern}"
            # 空 reason = 明确禁止，CI 硬阻断
            if not entry.reason.strip():
                issues.append(Violation(
                    location=loc,
                    message="allowlist 条目缺少 reason（禁止空置，CI 将阻断）",
                    severity="critical",
                    fix_hint="填写豁免原因，例: reason='Sprint-3 技术债，2026-06-01 前修复'",
                ))
            # 空 owner = 无人负责，CI 必须阻断
            if not entry.owner.strip():
                issues.append(Violation(
                    location=loc,
                    message="allowlist 条目缺少 owner（禁止空置，CI 将阻断）",
                    severity="critical",
                    fix_hint="填写负责人，例: owner='zhangsan' 或 owner='backend-team'",
                ))
            # 过期检查（--enforce-allowlist-expiry 时升级为 critical，触发 CI 硬阻断）
            # '2099-12-31' 为已停用的"永久豁免"哨兵，新条目应使用真实年度到期日
            if entry.expire and entry.expire != "2099-12-31":
                try:
                    exp_date = datetime.date.fromisoformat(entry.expire)
                    if today > exp_date:
                        expiry_sev: Severity = "critical" if _enforce_allowlist_expiry else "high"
                        issues.append(Violation(
                            location=loc,
                            message=f"allowlist 条目已过期 (expire={entry.expire})，对应违规应已修复，请删除此条目",
                            severity=expiry_sev,
                            fix_hint="确认违规已修复后从 ALLOWLIST 中删除此 AllowEntry",
                        ))
                except ValueError:
                    issues.append(Violation(
                        location=loc,
                        message=f"expire 格式无效 '{entry.expire}'（应为 YYYY-MM-DD）",
                        severity="medium",
                        fix_hint="正确格式: expire='2026-06-30'",
                    ))
            # issue_ref 检查：格式校验 + 缺失检测（advisory，medium，不阻断 CI）
            # 合法格式: '#123'  |  'https://...'  |  'http://...'
            _ref = entry.issue_ref.strip()
            if not _ref:
                issues.append(Violation(
                    location=loc,
                    message="allowlist 条目缺少 issue_ref（续期时必须附 issue/PR 链接）",
                    severity="medium",
                    fix_hint="填写关联链接，例: issue_ref='#42'、issue_ref='#my-fp-tag' 或 issue_ref='https://github.com/org/repo/issues/42'",
                ))
            elif not (
                re.match(r'^#[\w-]+$', _ref)
                or re.match(r'^https?://', _ref)
            ):
                issues.append(Violation(
                    location=loc,
                    message=f"issue_ref 格式无效 '{_ref}'（应为 '#tag'/'#123' 或 'https://...'）",
                    severity="medium",
                    fix_hint="合法格式: '#42'、'#my-fp-tag' 或 'https://github.com/org/repo/issues/42'",
                ))
    if not issues:
        return CheckResult("allowlist_governance", "pass", "Allowlist 治理规范（所有条目均有 reason/owner/expire）")
    critical_count = sum(1 for v in issues if v.severity == "critical")
    status: Status = "fail" if critical_count > 0 else "warn"
    return CheckResult(
        "allowlist_governance", status,
        f"Allowlist 治理问题 {len(issues)} 条（{critical_count} 条空字段将阻断 CI）",
        issues,
    )


def check_sla_gate() -> CheckResult:
    db_path = os.environ.get("EASYXT_DUCKDB_PATH", "")
    try:
        from data_manager.duckdb_connection_pool import resolve_duckdb_path

        resolved = resolve_duckdb_path(db_path if db_path else None)
    except Exception:
        resolved = db_path or "d:/stockdata/stock_data.ddb"
    if not pathlib.Path(resolved).exists():
        return CheckResult(
            "sla_daily_gate",
            "fail",
            f"SLA 数据库不存在: {resolved}",
            [
                Violation(
                    location="data_quality_sla_daily",
                    message="缺少SLA数据库文件，无法执行日级门禁",
                    severity="high",
                    fix_hint="先运行: python tools/governance_jobs.py --job sla",
                )
            ],
        )
    try:
        import duckdb

        con = duckdb.connect(resolved, read_only=True)
        try:
            row = con.execute(
                """
                SELECT report_date, completeness, consistency, lag_p95_ms, trust_score, gate_pass
                FROM data_quality_sla_daily
                ORDER BY report_date DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()
    except Exception as e:
        return CheckResult(
            "sla_daily_gate",
            "fail",
            f"SLA 查询失败: {e}",
            [
                Violation(
                    location="data_quality_sla_daily",
                    message=f"SLA 表查询异常: {e}",
                    severity="high",
                    fix_hint="确认 data_quality_sla_daily 已建表，并先运行 governance_jobs --job sla",
                )
            ],
        )
    if not row:
        return CheckResult(
            "sla_daily_gate",
            "fail",
            "SLA 表无记录",
            [
                Violation(
                    location="data_quality_sla_daily",
                    message="没有任何日级SLA记录",
                    severity="high",
                    fix_hint="先运行: python tools/governance_jobs.py --job sla",
                )
            ],
        )
    report_date, completeness, consistency, lag_p95_ms, trust_score, gate_pass = row
    violations: list[Violation] = []
    c = float(completeness or 0.0)
    s = float(consistency or 0.0)
    lag = None if lag_p95_ms is None else float(lag_p95_ms)
    if c < 0.995:
        violations.append(
            Violation(
                location=f"data_quality_sla_daily:{report_date}",
                message=f"completeness={c:.4f} < 0.995",
                severity="high",
                fix_hint="检查 write_audit_log 的 expected/actual 差异并回放隔离数据",
            )
        )
    if s < 0.998:
        violations.append(
            Violation(
                location=f"data_quality_sla_daily:{report_date}",
                message=f"consistency={s:.4f} < 0.998",
                severity="high",
                fix_hint="排查 source_conflict_audit 并修正跨源冲突仲裁策略",
            )
        )
    if lag is not None and lag >= 2000.0:
        violations.append(
            Violation(
                location=f"data_quality_sla_daily:{report_date}",
                message=f"lag_p95_ms={lag:.2f} >= 2000",
                severity="high",
                fix_hint="检查 realtime 写入链路和 checkpoint 频率",
            )
        )
    if not bool(gate_pass):
        violations.append(
            Violation(
                location=f"data_quality_sla_daily:{report_date}",
                message="gate_pass=false",
                severity="high",
                fix_hint="修复 completeness/consistency/lag 指标后重新生成日报",
            )
        )
    status: Status = "fail" if violations else "pass"
    detail = (
        f"SLA最新日报 {report_date} 未达标（trust_score={float(trust_score or 0.0):.4f}）"
        if violations
        else f"SLA最新日报通过 {report_date}（trust_score={float(trust_score or 0.0):.4f}）"
    )
    return CheckResult("sla_daily_gate", status, detail, violations)


def check_duckdb_write_probe() -> CheckResult:
    db_path = os.environ.get("EASYXT_DUCKDB_PATH", "")
    try:
        from data_manager.duckdb_connection_pool import resolve_duckdb_path
        resolved = resolve_duckdb_path(db_path if db_path else None)
    except Exception:
        resolved = db_path or "d:/stockdata/stock_data.ddb"
    db_file = pathlib.Path(resolved)
    parent = db_file.parent
    violations: list[Violation] = []
    if not parent.exists():
        violations.append(
            Violation(
                location=str(parent),
                message=f"DuckDB 目录不存在: {parent}",
                severity="high",
                fix_hint="创建数据目录并确保服务账户可写",
            )
        )
    if not db_file.exists():
        violations.append(
            Violation(
                location=str(db_file),
                message=f"DuckDB 文件不存在: {db_file}",
                severity="high",
                fix_hint="先初始化数据库文件或修正 EASYXT_DUCKDB_PATH",
            )
        )
    if violations:
        return CheckResult("duckdb_write_probe", "fail", "DuckDB 路径基础检查失败", violations)
    try:
        import duckdb
        con = duckdb.connect(str(db_file), read_only=False)
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute("CREATE TABLE IF NOT EXISTS __p0_write_probe(id BIGINT, ts TIMESTAMP)")
            con.execute("INSERT INTO __p0_write_probe VALUES (1, NOW())")
            con.execute("DELETE FROM __p0_write_probe WHERE id = 1")
            con.execute("COMMIT")
            con.execute("CHECKPOINT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            con.close()
    except Exception as e:
        return CheckResult(
            "duckdb_write_probe",
            "fail",
            f"DuckDB 写入探针失败: {e}",
            [
                Violation(
                    location=str(db_file),
                    message=f"写入/提交/检查点失败: {e}",
                    severity="high",
                    fix_hint="排查并发占用、目录权限、磁盘可用空间与 .wal/.write.lock 状态",
                )
            ],
        )
    return CheckResult("duckdb_write_probe", "pass", f"DuckDB 写入探针通过: {db_file}")


def check_duckdb_crash_signature_gate() -> CheckResult:
    if not callable(scan_duckdb_crash_signatures):
        return CheckResult("duckdb_crash_signature_gate", "skip", "duckdb_crash_signature_gate 不可用")
    extra = os.environ.get("EASYXT_DUCKDB_CRASH_SCAN_EXTRA_PATHS", "")
    extra_paths = [p.strip() for p in str(extra).split(";") if p.strip()]
    max_age_raw = os.environ.get("EASYXT_DUCKDB_CRASH_SCAN_MAX_AGE_HOURS", "24")
    try:
        max_age_hours: float | None = float(max_age_raw) if str(max_age_raw).strip() else None
    except Exception:
        max_age_hours = 24.0
    baseline_path = os.environ.get("EASYXT_DUCKDB_CRASH_BASELINE", "artifacts/duckdb_crash_baseline.json")
    report = scan_duckdb_crash_signatures(
        extra_paths=extra_paths,
        max_age_hours=max_age_hours,
        baseline_path=baseline_path,
    )
    hit_count = int(report.get("hit_count", 0) or 0)
    if hit_count <= 0:
        return CheckResult(
            "duckdb_crash_signature_gate",
            "pass",
            f"DuckDB 崩溃签名扫描通过（files={int(report.get('files_scanned', 0) or 0)}）",
        )
    violations: list[Violation] = []
    for row in list(report.get("hits", []) or [])[:20]:
        if not isinstance(row, dict):
            continue
        violations.append(
            Violation(
                location=f"{row.get('file', '')}:{row.get('line', '')}",
                message=f"DuckDB 崩溃签名命中: {row.get('message', '')}",
                severity="high",
                fix_hint="检查 checkpoint 线程、并发写入与退出阶段连接回收；复现后更新 DuckDB 专项策略",
            )
        )
    return CheckResult(
        "duckdb_crash_signature_gate",
        "fail",
        f"DuckDB 崩溃签名命中 {hit_count} 条",
        violations,
    )


def check_realtime_quote_contract() -> CheckResult:
    violations: list[Violation] = []

    main_window = PROJECT_ROOT / "gui_app" / "main_window.py"
    if not main_window.exists():
        return CheckResult("realtime_quote_contract_check", "fail", "main_window.py 不存在", [
            Violation(
                location="gui_app/main_window.py",
                message="入口文件缺失，无法验证实时行情启动契约",
                severity="high",
                fix_hint="恢复 gui_app/main_window.py 并重跑门禁",
            )
        ])
    main_src = main_window.read_text(encoding="utf-8", errors="ignore")
    idx_sys_path = main_src.find("sys.path.insert(0, project_path)")
    idx_guard_import = main_src.find("from gui_app.widgets.chart.trading_hours_guard import TradingHoursGuard")
    if idx_guard_import >= 0 and (idx_sys_path < 0 or idx_guard_import < idx_sys_path):
        violations.append(
            Violation(
                location="gui_app/main_window.py",
                message="TradingHoursGuard 导入出现在 sys.path 注入之前，直接运行会 ModuleNotFoundError",
                severity="high",
                fix_hint="先注入 project_path 到 sys.path，再导入 gui_app 包内模块",
            )
        )

    ws_file = PROJECT_ROOT / "gui_app" / "widgets" / "kline_chart_workspace.py"
    if not ws_file.exists():
        return CheckResult("realtime_quote_contract_check", "fail", "kline_chart_workspace.py 不存在", [
            Violation(
                location="gui_app/widgets/kline_chart_workspace.py",
                message="K 线工作区文件缺失，无法验证实时行情/五档契约",
                severity="high",
                fix_hint="恢复 gui_app/widgets/kline_chart_workspace.py 后重跑门禁",
            )
        ])
    ws_src = ws_file.read_text(encoding="utf-8", errors="ignore")

    required_tokens = [
        "def _normalize_realtime_quote(",
        "quote = self._normalize_realtime_quote(quote)",
        "lastPrice",
        "askPrice",
    ]
    for token in required_tokens:
        if token not in ws_src:
            violations.append(
                Violation(
                    location="gui_app/widgets/kline_chart_workspace.py",
                    message=f"缺少实时行情归一化契约片段: {token}",
                    severity="high",
                    fix_hint="确保实时消息在入管道与更新五档前完成统一字段映射",
                )
            )
    if 'raw.get(f"sell{level}")' not in ws_src:
        violations.append(
            Violation(
                location="gui_app/widgets/kline_chart_workspace.py",
                message="缺少 sellN -> askN 映射契约",
                severity="high",
                fix_hint="在实时归一化中补齐 sell{level} 到 ask{level} 的字段映射",
            )
        )
    if 'raw.get(f"buy{level}")' not in ws_src:
        violations.append(
            Violation(
                location="gui_app/widgets/kline_chart_workspace.py",
                message="缺少 buyN -> bidN 映射契约",
                severity="high",
                fix_hint="在实时归一化中补齐 buy{level} 到 bid{level} 的字段映射",
            )
        )

    status: Status = "fail" if violations else "pass"
    detail = "实时行情接收与五档字段归一化契约通过" if not violations else f"发现 {len(violations)} 处实时契约缺口"
    return CheckResult("realtime_quote_contract_check", status, detail, violations)


def check_intraday_bar_semantic_guard() -> CheckResult:
    db_path = os.environ.get("EASYXT_DUCKDB_PATH", "")
    try:
        from data_manager.duckdb_connection_pool import resolve_duckdb_path
        resolved = resolve_duckdb_path(db_path if db_path else None)
    except Exception:
        resolved = db_path or "d:/stockdata/stock_data.ddb"
    db_file = pathlib.Path(resolved)
    if not db_file.exists():
        return CheckResult(
            "intraday_bar_semantic_guard",
            "fail",
            f"DuckDB 文件不存在: {db_file}",
            [
                Violation(
                    location=str(db_file),
                    message="无法执行日内K线语义守卫（数据库文件缺失）",
                    severity="high",
                    fix_hint="初始化数据库并确认 EASYXT_DUCKDB_PATH 指向有效文件",
                )
            ],
        )
    violations: list[Violation] = []
    lookback_days = int(os.environ.get("EASYXT_INTRADAY_GUARD_LOOKBACK_DAYS", "5"))
    jump_limit = float(os.environ.get("EASYXT_INTRADAY_BAR_JUMP_LIMIT", "0.25"))
    range_limit = float(os.environ.get("EASYXT_INTRADAY_BAR_RANGE_LIMIT", "0.25"))
    max_allowed = int(os.environ.get("EASYXT_INTRADAY_ANOMALY_MAX", "0"))
    since_dt = (datetime.datetime.now() - datetime.timedelta(days=max(1, lookback_days))).strftime("%Y-%m-%d %H:%M:%S")
    try:
        import duckdb
        con = duckdb.connect(str(db_file), read_only=True)
        try:
            for table_name, period in (("stock_1m", "1m"), ("stock_5m", "5m")):
                exists = con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                    [table_name],
                ).fetchone()[0]
                if not exists:
                    violations.append(
                        Violation(
                            location=table_name,
                            message=f"缺少 {table_name}，无法执行 {period} 语义校验",
                            severity="high",
                            fix_hint="补齐分钟线表并确保行情写入任务正常运行",
                        )
                    )
                    continue
                invalid_ohlc = int(
                    con.execute(
                        f"""
                        SELECT COUNT(*) FROM {table_name}
                        WHERE period = ?
                          AND datetime >= ?
                          AND (
                            open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
                            OR high < low
                            OR high < GREATEST(open, close)
                            OR low > LEAST(open, close)
                          )
                        """,
                        [period, since_dt],
                    ).fetchone()[0]
                )
                if invalid_ohlc > max_allowed:
                    violations.append(
                        Violation(
                            location=f"{table_name}:{period}",
                            message=f"近{lookback_days}天存在 {invalid_ohlc} 条OHLC语义错误",
                            severity="high",
                            fix_hint="执行分钟线重放并启用隔离表回补，确保 high/low 覆盖 open/close",
                        )
                    )
                range_spike = int(
                    con.execute(
                        f"""
                        SELECT COUNT(*) FROM {table_name}
                        WHERE period = ?
                          AND datetime >= ?
                          AND ABS(open) > 1e-9
                          AND (high - low) / ABS(open) > ?
                        """,
                        [period, since_dt, range_limit],
                    ).fetchone()[0]
                )
                if range_spike > max_allowed:
                    violations.append(
                        Violation(
                            location=f"{table_name}:{period}",
                            message=f"近{lookback_days}天存在 {range_spike} 条振幅>{range_limit:.0%} 异常K线",
                            severity="high",
                            fix_hint="检查实时合成bar字段映射，禁止将日高低注入日内bar",
                        )
                    )
                jump_spike = int(
                    con.execute(
                        f"""
                        WITH seq AS (
                            SELECT
                                stock_code,
                                datetime,
                                close,
                                LAG(close) OVER (PARTITION BY stock_code ORDER BY datetime) AS prev_close
                            FROM {table_name}
                            WHERE period = ?
                              AND datetime >= ?
                        )
                        SELECT COUNT(*)
                        FROM seq
                        WHERE prev_close IS NOT NULL
                          AND ABS(prev_close) > 1e-9
                          AND ABS(close - prev_close) / ABS(prev_close) > ?
                        """,
                        [period, since_dt, jump_limit],
                    ).fetchone()[0]
                )
                if jump_spike > max_allowed:
                    violations.append(
                        Violation(
                            location=f"{table_name}:{period}",
                            message=f"近{lookback_days}天存在 {jump_spike} 条相邻bar跳变>{jump_limit:.0%}",
                            severity="high",
                            fix_hint="核查复权与实时拼接边界，执行异常点隔离后重建分钟线",
                        )
                    )
                out_of_session = int(
                    con.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE period = ?
                          AND datetime >= ?
                          AND (
                            EXTRACT('dow' FROM datetime) IN (0, 6)
                            OR (
                                CAST(datetime AS TIME) NOT BETWEEN TIME '09:30:00' AND TIME '11:30:00'
                                AND CAST(datetime AS TIME) NOT BETWEEN TIME '13:00:00' AND TIME '15:00:00'
                            )
                          )
                        """,
                        [period, since_dt],
                    ).fetchone()[0]
                )
                if out_of_session > max_allowed:
                    violations.append(
                        Violation(
                            location=f"{table_name}:{period}",
                            message=f"近{lookback_days}天存在 {out_of_session} 条盘后/非交易时段伪bar",
                            severity="high",
                            fix_hint="启用交易时段水位线，按 quote 时间戳对齐，丢弃盘后分钟bar",
                        )
                    )
        finally:
            con.close()
    except Exception as e:
        return CheckResult(
            "intraday_bar_semantic_guard",
            "fail",
            f"日内语义守卫执行失败: {e}",
            [
                Violation(
                    location=str(db_file),
                    message=f"语义守卫查询失败: {e}",
                    severity="high",
                    fix_hint="确认 DuckDB 可读并检查 stock_1m/stock_5m 表结构",
                )
            ],
        )
    status: Status = "fail" if violations else "pass"
    detail = "日内K线语义守卫通过" if not violations else f"发现 {len(violations)} 项日内K线异常"
    return CheckResult("intraday_bar_semantic_guard", status, detail, violations)


def check_governance_nightly_jobs() -> CheckResult:
    metrics_path = PROJECT_ROOT / "artifacts" / "governance_metrics_latest.json"
    require_metrics = os.environ.get("EASYXT_REQUIRE_GOVERNANCE_NIGHTLY", "0") in ("1", "true", "True")
    if not metrics_path.exists():
        if require_metrics:
            return CheckResult(
                "governance_nightly_jobs_check",
                "fail",
                "缺少 governance_metrics_latest.json",
                [
                    Violation(
                        location=str(metrics_path),
                        message="nightly 结果文件缺失",
                        severity="high",
                        fix_hint="先执行 nightly 治理作业：python tools/governance_jobs.py --job all",
                    )
                ],
            )
        return CheckResult("governance_nightly_jobs_check", "skip", "未发现 governance_metrics_latest.json（本地跳过）")
    try:
        raw = metrics_path.read_text(encoding="utf-8", errors="ignore")
        payload = json.loads(raw)
    except Exception:
        payload = None
        try:
            for line in reversed(raw.splitlines()):
                s = line.strip()
                if not s:
                    continue
                if not s.startswith("{"):
                    continue
                obj = json.loads(s)
                if isinstance(obj, dict):
                    payload = obj
                    break
        except Exception:
            payload = None
    if not isinstance(payload, dict):
        e = "json_object_not_found"
        return CheckResult(
            "governance_nightly_jobs_check",
            "fail",
            f"nightly 结果解析失败: {e}",
            [
                Violation(
                    location=str(metrics_path),
                    message=f"JSON 解析失败: {e}",
                    severity="high",
                    fix_hint="修复治理作业输出并重新生成 governance_metrics_latest.json",
                )
            ],
        )
    late = payload.get("late_event_replay") if isinstance(payload, dict) else {}
    late = late if isinstance(late, dict) else {}
    rebuild = payload.get("multiperiod_rebuild") if isinstance(payload, dict) else {}
    rebuild = rebuild if isinstance(rebuild, dict) else {}
    rebuild_receipt_check = payload.get("multiperiod_rebuild_receipt_check") if isinstance(payload, dict) else {}
    rebuild_receipt_check = rebuild_receipt_check if isinstance(rebuild_receipt_check, dict) else {}
    watermark_quality = payload.get("watermark_quality") if isinstance(payload, dict) else {}
    watermark_quality = watermark_quality if isinstance(watermark_quality, dict) else {}
    watermark_approval = payload.get("watermark_profile_approval") if isinstance(payload, dict) else {}
    watermark_approval = watermark_approval if isinstance(watermark_approval, dict) else {}
    warn_block = os.environ.get("EASYXT_WM_APPROVAL_WARN_BLOCK", "0") in ("1", "true", "True")
    violations: list[Violation] = []
    late_failed = int(late.get("failed", 0) or 0)
    late_dead = int(late.get("dead_letter", 0) or 0)
    if late_failed > 0 or late_dead > 0:
        violations.append(
            Violation(
                location="artifacts/governance_metrics_latest.json:late_event_replay",
                message=f"late_event_replay failed={late_failed} dead_letter={late_dead}",
                severity="high",
                fix_hint="先运行 python tools/governance_jobs.py --job late_replay --strict-late-replay 清零失败",
            )
        )
    rebuild_failed = int(rebuild.get("failed", 0) or 0)
    rebuild_ok = bool(rebuild.get("ok", False))
    if rebuild_failed > 0 or not rebuild_ok:
        violations.append(
            Violation(
                location="artifacts/governance_metrics_latest.json:multiperiod_rebuild",
                message=f"multiperiod_rebuild ok={rebuild_ok} failed={rebuild_failed}",
                severity="high",
                fix_hint="执行 python tools/governance_jobs.py --job rebuild --strict-rebuild 并修复失败周期",
            )
        )
    receipt_valid = bool(rebuild_receipt_check.get("valid", False))
    if not receipt_valid:
        violations.append(
            Violation(
                location="artifacts/governance_metrics_latest.json:multiperiod_rebuild_receipt_check",
                message="multiperiod_rebuild_receipt_check.valid=False",
                severity="high",
                fix_hint="执行 python tools/governance_jobs.py --job rebuild --strict-rebuild 并确认审计回执生成成功",
            )
        )
    wm_pass = bool(watermark_quality.get("q_score_pass", False))
    wm_today = watermark_quality.get("today") if isinstance(watermark_quality.get("today"), dict) else {}
    wm_q = float(wm_today.get("q_score", 0.0) or 0.0) if isinstance(wm_today, dict) else 0.0
    wm_floor = float(watermark_quality.get("q_score_floor", 0.0) or 0.0)
    if not wm_pass:
        violations.append(
            Violation(
                location="artifacts/governance_metrics_latest.json:watermark_quality",
                message=f"watermark q_score={wm_q:.4f} < floor={wm_floor:.4f}",
                severity="high",
                fix_hint="检查 realtime_watermark_events.jsonl 的晚到与乱序事件并回放修复",
            )
        )
    appr_valid = bool(watermark_approval.get("valid", False))
    appr_required = bool(watermark_approval.get("required", False))
    if appr_required and not appr_valid:
        violations.append(
            Violation(
                location="artifacts/governance_metrics_latest.json:watermark_profile_approval",
                message=f"watermark profile approval invalid reason={watermark_approval.get('reason')}",
                severity="high",
                fix_hint="补充 EASYXT_WM_APPROVAL_ID/EASYXT_WM_APPROVER 或切换非需审批模板",
            )
        )
    appr_risk = str(watermark_approval.get("risk_level") or "").lower()
    if warn_block and appr_risk == "warn":
        violations.append(
            Violation(
                location="artifacts/governance_metrics_latest.json:watermark_profile_approval",
                message=f"watermark profile approval risk_level={appr_risk}",
                severity="high",
                fix_hint="处理即将过期/用量临界审批单，或关闭 EASYXT_WM_APPROVAL_WARN_BLOCK",
            )
        )
    status: Status = "fail" if violations else "pass"
    detail = "nightly late_replay/rebuild 作业通过" if not violations else f"nightly 作业失败 {len(violations)} 项"
    return CheckResult("governance_nightly_jobs_check", status, detail, violations)


def check_period_validation_report() -> CheckResult:
    report_file = pathlib.Path(
        os.environ.get(
            "EASYXT_PERIOD_VALIDATION_REPORT_PATH",
            str(PROJECT_ROOT / "artifacts" / "period_validation_report.jsonl"),
        )
    )
    if not report_file.is_absolute():
        report_file = (PROJECT_ROOT / report_file).resolve()
    block_fail = os.environ.get("EASYXT_PERIOD_VALIDATION_FAIL_BLOCK", "1") in ("1", "true", "True")
    if not report_file.exists():
        if block_fail:
            return CheckResult(
                "period_validation_report_check",
                "fail",
                "缺少周期校验报告 period_validation_report.jsonl",
                [
                    Violation(
                        location=str(report_file),
                        message="周期校验报告缺失",
                        severity="high",
                        fix_hint="执行派生周期构建与 cross_validate，生成周期校验报告",
                    )
                ],
            )
        return CheckResult("period_validation_report_check", "skip", "未发现周期校验报告（阻断开关关闭）")
    try:
        lines = report_file.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception as e:
        return CheckResult(
            "period_validation_report_check",
            "fail",
            f"周期校验报告读取失败: {e}",
            [
                Violation(
                    location=str(report_file),
                    message=f"读取失败: {e}",
                    severity="high",
                    fix_hint="检查报告文件编码与权限",
                )
            ],
        )
    failures: list[dict] = []
    malformed = 0
    schema_invalid = 0
    for raw in lines[-500:]:
        s = raw.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            malformed += 1
            continue
        if not isinstance(obj, dict):
            malformed += 1
            continue
        details = obj.get("details", [])
        if details is None:
            details = []
        if not isinstance(details, list):
            schema_invalid += 1
            continue
        required_detail_keys = {"metric", "actual", "expected", "delta"}
        for item in details:
            if not isinstance(item, dict):
                schema_invalid += 1
                break
            if not required_detail_keys.issubset(set(item.keys())):
                schema_invalid += 1
                break
        if bool(obj.get("is_valid", True)) is False:
            failures.append(obj)
    violations: list[Violation] = []
    if malformed > 0:
        violations.append(
            Violation(
                location=str(report_file),
                message=f"周期校验报告存在 {malformed} 行非法JSON",
                severity="medium",
                fix_hint="修复报告生成器，保证每行均为合法 JSON 对象",
            )
        )
    if schema_invalid > 0:
        violations.append(
            Violation(
                location=str(report_file),
                message=f"周期校验报告存在 {schema_invalid} 行 details schema 非法",
                severity="high" if block_fail else "medium",
                fix_hint="确保 details 为列表，且每个条目包含 metric/actual/expected/delta",
            )
        )
    if failures:
        for item in failures[-5:]:
            period = str(item.get("period") or "unknown")
            detail = ""
            if isinstance(item.get("errors"), list) and item.get("errors"):
                detail = str(item["errors"][0])
            elif isinstance(item.get("warnings"), list) and item.get("warnings"):
                detail = str(item["warnings"][0])
            violations.append(
                Violation(
                    location=f"{report_file}:period={period}",
                    message=f"周期校验失败 period={period} detail={detail[:160]}",
                    severity="high" if block_fail else "medium",
                    fix_hint="修复对应周期构建规则并重新执行 cross_validate",
                )
            )
    if failures and block_fail:
        return CheckResult(
            "period_validation_report_check",
            "fail",
            f"周期校验报告失败 {len(failures)} 条",
            violations,
        )
    if schema_invalid > 0 and block_fail:
        return CheckResult(
            "period_validation_report_check",
            "fail",
            f"周期校验报告 details schema 非法 {schema_invalid} 条",
            violations,
        )
    if failures:
        return CheckResult(
            "period_validation_report_check",
            "warn",
            f"周期校验报告失败 {len(failures)} 条（阻断开关关闭）",
            violations,
        )
    status: Status = "warn" if (malformed > 0 or schema_invalid > 0) else "pass"
    if malformed == 0 and schema_invalid == 0:
        detail = "周期校验报告通过"
    elif malformed > 0 and schema_invalid == 0:
        detail = "周期校验报告通过，但存在非法JSON行"
    elif malformed == 0:
        detail = "周期校验报告通过，但存在 details schema 非法行"
    else:
        detail = "周期校验报告通过，但存在非法JSON与details schema非法行"
    return CheckResult("period_validation_report_check", status, detail, violations)


def check_watchdog_slo_gate() -> CheckResult:
    """读取主线程延迟日志，连续 SLO 违规次数达阈值则阻断。"""
    threshold = int(os.environ.get("EASYXT_WATCHDOG_SLO_CONSECUTIVE_FAIL_THRESHOLD", "3") or 3)
    log_path = pathlib.Path(
        os.environ.get("EASYXT_WATCHDOG_LOG_PATH", "") or str(PROJECT_ROOT / "logs" / "main_thread_latency.log")
    )
    if not log_path.is_absolute():
        log_path = (PROJECT_ROOT / log_path).resolve()
    if not log_path.exists():
        return CheckResult("watchdog_slo_gate", "skip", "主线程延迟日志不存在，跳过 SLO 门禁")
    try:
        lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception as exc:
        return CheckResult(
            "watchdog_slo_gate",
            "fail",
            f"主线程延迟日志读取失败: {exc}",
            [Violation(location=str(log_path), message=str(exc), severity="high", fix_hint="检查日志文件权限")],
        )
    # 从末尾统计连续 slo_violation=true 条目
    # 优先使用最新条目的 consecutive_violations 字段（watchdog 守护线程内准确追踪）；
    # 回退方案：反向扫描，但若相邻条目时间戳间隔 > 5 分钟则视为独立违规簇并中止计数。
    consecutive = 0
    last_ts: datetime.datetime | None = None
    _max_gap_s = 300  # 相邻窗口最大允许间隔（秒）；60s 窗口间隔，留 5× 缓冲
    last_valid_obj: dict | None = None
    for raw in reversed(lines[-200:]):
        s = raw.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            break
        if not isinstance(obj, dict):
            break
        # 解析时间戳以检测跨会话间隙
        curr_ts: datetime.datetime | None = None
        try:
            curr_ts = datetime.datetime.fromisoformat(obj.get("ts", ""))
        except Exception:
            pass
        if last_ts is not None and curr_ts is not None:
            gap = abs((last_ts - curr_ts).total_seconds())
            if gap > _max_gap_s:
                break  # 跨越独立会话，不应合并计数
        if obj.get("slo_violation") is True:
            # 若有可信的会话级计数字段，直接从最新条目读取
            if last_valid_obj is None:
                last_valid_obj = obj
                if isinstance(obj.get("consecutive_violations"), int):
                    consecutive = obj["consecutive_violations"]
                    break
            consecutive += 1
        else:
            break
        last_ts = curr_ts
    if consecutive >= threshold:
        return CheckResult(
            "watchdog_slo_gate",
            "fail",
            f"主线程 p99 延迟 SLO 连续违规 {consecutive} 次（阈值={threshold}）",
            [
                Violation(
                    location=str(log_path),
                    message=f"连续 {consecutive} 个 60s 窗口 p99 > SLO",
                    severity="high",
                    fix_hint="排查主线程阻塞（DB 查询/UI 渲染/网络 IO），降低 GIL 争用",
                )
            ],
        )
    return CheckResult(
        "watchdog_slo_gate",
        "pass",
        f"主线程延迟 SLO 正常（当前连续违规={consecutive}，阈值={threshold}）",
    )


# ─────────────────────────────────────────────────────────────────────────────
# 红线扫描：测试中伪造 OHLCV 检测
# ─────────────────────────────────────────────────────────────────────────────

# 常见伪造行情模式——匹配 tests/ 下 Python 文件中硬编码的 OHLCV 字段赋值
_FAKE_OHLCV_PATTERNS = [
    # 'open': [数字, ...] / 'close': [数字, ...] / 'high' / 'low' / 'volume'
    re.compile(
        r"""['"](?:open|high|low|close|volume)['"]\s*:\s*\[\s*\d+""",
        re.IGNORECASE,
    ),
    # DataFrame({'close': [10, 15]})
    re.compile(
        r"""DataFrame\s*\(\s*\{[^}]*['"](?:open|high|low|close|volume)['"]\s*:\s*\[""",
        re.IGNORECASE,
    ),
]

# 白名单路径后缀——这些文件/目录中的匹配不视为违规
_FAKE_OHLCV_EXEMPTIONS = {
    "tests/fixtures/real_market_data.py",     # 真实数据 fixture
    "tests/fixtures/",                        # fixture 目录
    "tests/conftest.py",                      # 共享 fixture
}


def check_fake_ohlcv_in_tests() -> CheckResult:
    """
    红线巡检：扫描 tests/ 中硬编码的伪造 OHLCV 行情数据。
    当前为 warn 级（不阻断），用于早期预警。

    依据：development_rules.md 铁律 0 补充——红线 vs 白线
    """
    tests_root = PROJECT_ROOT / "tests"
    if not tests_root.exists():
        return CheckResult("fake_ohlcv_scan", "skip", "tests/ 目录不存在")

    raw_violations: list[Violation] = []

    for py_file in tests_root.rglob("*.py"):
        rel = py_file.relative_to(PROJECT_ROOT).as_posix()
        # 跳过豁免路径
        if any(rel.startswith(ex) or rel == ex for ex in _FAKE_OHLCV_EXEMPTIONS):
            continue
        try:
            src = py_file.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for i, line in enumerate(src.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            for pat in _FAKE_OHLCV_PATTERNS:
                if pat.search(stripped):
                    raw_violations.append(Violation(
                        location=f"{rel}:{i}",
                        message=f"疑似伪造 OHLCV: {stripped[:80]}",
                        severity="medium",
                        fix_hint="改用 tests/fixtures/real_market_data.py 中的真实数据，或确认此处属于白线（基础设施桩）",
                    ))
                    break  # 同一行不重复报

    # 当前为 warn 级，不阻断
    if raw_violations:
        status: Status = "warn"
        detail = f"发现 {len(raw_violations)} 处疑似伪造 OHLCV（warn 级，暂不阻断）"
    else:
        status = "pass"
        detail = "测试代码红线扫描通过：未发现伪造 OHLCV"
    return CheckResult("fake_ohlcv_scan", status, detail, raw_violations)


# ─────────────────────────────────────────────────────────────────────────────
# 注册表
# ─────────────────────────────────────────────────────────────────────────────

ALL_CHECKS: dict[str, object] = {
    "credential":  check_credential_scan,
    "sql":         check_sql_injection,
    "timestamp":   check_timestamp_contract,
    "xtdata":      check_xtdata_import,
    "publish":     check_atomic_publish,
    "sla":         check_sla_gate,
    "duckdb_write": check_duckdb_write_probe,
    "duckdb_crash": check_duckdb_crash_signature_gate,
    "realtime":    check_realtime_quote_contract,
    "intraday_bar": check_intraday_bar_semantic_guard,
    "governance_nightly": check_governance_nightly_jobs,
    "period_validation": check_period_validation_report,
    "schema":      check_schema_version,
    "allowlist":   check_allowlist_governance,
    "watchdog_slo": check_watchdog_slo_gate,
    "fake_ohlcv": check_fake_ohlcv_in_tests,
}

P0_CHECKS = {"credential", "sql", "timestamp", "xtdata", "publish", "sla", "duckdb_write", "duckdb_crash", "realtime", "intraday_bar", "governance_nightly", "period_validation", "allowlist", "watchdog_slo"}
P0_RESULT_NAMES = {
    "credential_scan",
    "sql_injection_scan",
    "timestamp_contract_check",
    "xtdata_import_check",
    "snapshot_publish_atomic",
    "sla_daily_gate",
    "duckdb_write_probe",
    "duckdb_crash_signature_gate",
    "realtime_quote_contract_check",
    "intraday_bar_semantic_guard",
    "governance_nightly_jobs_check",
    "period_validation_report_check",
    "allowlist_governance",
    "watchdog_slo_gate",
}


def run_all_checks() -> list[CheckResult]:
    return [fn() for fn in ALL_CHECKS.values()]  # type: ignore[operator]


def _p0_names() -> set[str]:
    return set(P0_RESULT_NAMES)


def _extract_duckdb_write_probe_detail(results: list[CheckResult]) -> dict[str, object]:
    target = next((r for r in results if r.name == "duckdb_write_probe"), None)
    if target is None:
        return {
            "status": "missing",
            "db_path": "",
            "error_type": "check_missing",
            "message": "duckdb_write_probe 未执行",
            "recommended_action": "运行完整门禁: python tools/p0_gate_check.py --strict --json",
        }
    db_path = ""
    error_type = ""
    message = target.detail
    recommended_action = ""
    if target.violations:
        first = target.violations[0]
        db_path = str(first.location or "")
        message = str(first.message or target.detail)
        recommended_action = str(first.fix_hint or "")
        m = re.search(r"([A-Za-z_]+Error)", message)
        if m:
            error_type = m.group(1)
        elif "cannot open file" in message.lower():
            error_type = "cannot_open_file"
        elif "permission denied" in message.lower():
            error_type = "permission_denied"
        else:
            error_type = "duckdb_write_failure"
    return {
        "status": target.status,
        "db_path": db_path,
        "error_type": error_type,
        "message": message,
        "recommended_action": recommended_action,
    }


def _extract_intraday_bar_semantic_detail(results: list[CheckResult]) -> dict[str, object]:
    target = next((r for r in results if r.name == "intraday_bar_semantic_guard"), None)
    if target is None:
        return {
            "status": "missing",
            "message": "intraday_bar_semantic_guard 未执行",
            "anomaly_count": 0,
            "recommended_action": "运行完整门禁: python tools/p0_gate_check.py --strict --json",
        }
    return {
        "status": target.status,
        "message": target.detail,
        "anomaly_count": len(target.violations),
        "recommended_action": (target.violations[0].fix_hint if target.violations else ""),
    }


def _extract_governance_nightly_detail(results: list[CheckResult]) -> dict[str, object]:
    target = next((r for r in results if r.name == "governance_nightly_jobs_check"), None)
    if target is None:
        return {
            "status": "missing",
            "message": "governance_nightly_jobs_check 未执行",
            "failed_items": 0,
            "recommended_action": "运行完整门禁: python tools/p0_gate_check.py --strict --json",
        }
    return {
        "status": target.status,
        "message": target.detail,
        "failed_items": len(target.violations),
        "recommended_action": (target.violations[0].fix_hint if target.violations else ""),
    }


def _extract_watermark_quality_detail() -> dict[str, object]:
    metrics_path = PROJECT_ROOT / "artifacts" / "governance_metrics_latest.json"
    if not metrics_path.exists():
        return {
            "status": "missing",
            "today_q_score": 0.0,
            "q_score_floor": 0.0,
            "q_score_pass": False,
            "today_late_score": 0.0,
            "today_ooo_score": 0.0,
            "today_lateness_score": 0.0,
            "q_score_mean_7d": 0.0,
            "q_score_vol_7d": 0.0,
            "late_score_mean_7d": 0.0,
            "late_score_vol_7d": 0.0,
            "ooo_score_mean_7d": 0.0,
            "ooo_score_vol_7d": 0.0,
            "lateness_score_mean_7d": 0.0,
            "lateness_score_vol_7d": 0.0,
            "trend": [],
        }
    try:
        raw = metrics_path.read_text(encoding="utf-8", errors="ignore")
        payload = None
        for line in reversed(raw.splitlines()):
            s = line.strip()
            if not s or not s.startswith("{"):
                continue
            obj = json.loads(s)
            if isinstance(obj, dict):
                payload = obj
                break
        if payload is None:
            payload = json.loads(raw)
    except Exception:
        payload = {}
    wm = payload.get("watermark_quality") if isinstance(payload, dict) else {}
    wm = wm if isinstance(wm, dict) else {}
    today = wm.get("today") if isinstance(wm.get("today"), dict) else {}
    trend = wm.get("trend") if isinstance(wm.get("trend"), list) else []
    return {
        "status": str(wm.get("status") or "missing"),
        "today_q_score": float(today.get("q_score", 0.0) or 0.0) if isinstance(today, dict) else 0.0,
        "today_late_score": float(today.get("late_score", 0.0) or 0.0) if isinstance(today, dict) else 0.0,
        "today_ooo_score": float(today.get("ooo_score", 0.0) or 0.0) if isinstance(today, dict) else 0.0,
        "today_lateness_score": float(today.get("lateness_score", 0.0) or 0.0) if isinstance(today, dict) else 0.0,
        "q_score_floor": float(wm.get("q_score_floor", 0.0) or 0.0),
        "q_score_pass": bool(wm.get("q_score_pass", False)),
        "profile": str(wm.get("profile") or "balanced"),
        "weights": wm.get("weights") if isinstance(wm.get("weights"), dict) else {},
        "q_score_mean_7d": float(wm.get("q_score_mean_7d", 0.0) or 0.0),
        "q_score_vol_7d": float(wm.get("q_score_vol_7d", 0.0) or 0.0),
        "late_score_mean_7d": float(wm.get("late_score_mean_7d", 0.0) or 0.0),
        "late_score_vol_7d": float(wm.get("late_score_vol_7d", 0.0) or 0.0),
        "ooo_score_mean_7d": float(wm.get("ooo_score_mean_7d", 0.0) or 0.0),
        "ooo_score_vol_7d": float(wm.get("ooo_score_vol_7d", 0.0) or 0.0),
        "lateness_score_mean_7d": float(wm.get("lateness_score_mean_7d", 0.0) or 0.0),
        "lateness_score_vol_7d": float(wm.get("lateness_score_vol_7d", 0.0) or 0.0),
        "trend": trend[-7:],
    }


def _extract_watermark_profile_audit_detail() -> dict[str, object]:
    metrics_path = PROJECT_ROOT / "artifacts" / "governance_metrics_latest.json"
    if not metrics_path.exists():
        return {"status": "missing", "count": 0, "recent": []}
    try:
        raw = metrics_path.read_text(encoding="utf-8", errors="ignore")
        payload = None
        for line in reversed(raw.splitlines()):
            s = line.strip()
            if not s or not s.startswith("{"):
                continue
            obj = json.loads(s)
            if isinstance(obj, dict):
                payload = obj
                break
        if payload is None:
            payload = json.loads(raw)
    except Exception:
        payload = {}
    audit = payload.get("watermark_profile_audit") if isinstance(payload, dict) else {}
    audit = audit if isinstance(audit, dict) else {}
    recent = audit.get("recent") if isinstance(audit.get("recent"), list) else []
    compact = []
    for it in recent[-5:]:
        if not isinstance(it, dict):
            continue
        compact.append(
            {
                "ts": str(it.get("ts") or ""),
                "action": str(it.get("action") or ""),
                "profile": str(it.get("profile") or ""),
                "success": bool(it.get("success", False)),
                "message": str(it.get("message") or ""),
            }
        )
    return {"status": str(audit.get("status") or "missing"), "count": int(audit.get("count", 0) or 0), "recent": compact}


def _extract_watermark_profile_approval_detail() -> dict[str, object]:
    metrics_path = PROJECT_ROOT / "artifacts" / "governance_metrics_latest.json"
    if not metrics_path.exists():
        return {"required": False, "valid": False, "reason": "missing"}
    try:
        raw = metrics_path.read_text(encoding="utf-8", errors="ignore")
        payload = None
        for line in reversed(raw.splitlines()):
            s = line.strip()
            if not s or not s.startswith("{"):
                continue
            obj = json.loads(s)
            if isinstance(obj, dict):
                payload = obj
                break
        if payload is None:
            payload = json.loads(raw)
    except Exception:
        payload = {}
    appr = payload.get("watermark_profile_approval") if isinstance(payload, dict) else {}
    appr = appr if isinstance(appr, dict) else {}
    return {
        "required": bool(appr.get("required", False)),
        "valid": bool(appr.get("valid", False)),
        "release_env": str(appr.get("release_env") or ""),
        "profile": str(appr.get("profile") or ""),
        "approval_id": str(appr.get("approval_id") or ""),
        "approver": str(appr.get("approver") or ""),
        "reason": str(appr.get("reason") or ""),
        "registry_path": str(appr.get("registry_path") or ""),
        "approved_at": str(appr.get("approved_at") or ""),
        "expires_at": str(appr.get("expires_at") or ""),
        "approval_max_age_days": int(appr.get("approval_max_age_days", 0) or 0),
        "days_to_expire": int(appr.get("days_to_expire", 0) or 0),
        "signature_required": bool(appr.get("signature_required", False)),
        "signature_valid": bool(appr.get("signature_valid", False)),
        "signatures_required": int(appr.get("signatures_required", 0) or 0),
        "signatures_valid_count": int(appr.get("signatures_valid_count", 0) or 0),
        "max_uses": int(appr.get("max_uses", 0) or 0),
        "used_count": int(appr.get("used_count", 0) or 0),
        "remaining_uses": int(appr.get("remaining_uses", 0) or 0),
        "usage_log_file": str(appr.get("usage_log_file") or ""),
        "warnings": appr.get("warnings") if isinstance(appr.get("warnings"), list) else [],
        "risk_level": str(appr.get("risk_level") or "unknown"),
        "missing_fields": appr.get("missing_fields") if isinstance(appr.get("missing_fields"), list) else [],
    }


def _extract_period_validation_detail(results: list[CheckResult]) -> dict[str, object]:
    target = next((r for r in results if r.name == "period_validation_report_check"), None)
    if target is None:
        return {
            "status": "missing",
            "failed_items": 0,
            "recommended_action": "运行完整门禁: python tools/p0_gate_check.py --strict --json",
            "message": "period_validation_report_check 未执行",
        }
    return {
        "status": target.status,
        "failed_items": len(target.violations),
        "recommended_action": (target.violations[0].fix_hint if target.violations else ""),
        "message": target.detail,
    }


def _load_artifact_json(name: str) -> dict[str, object]:
    path = PROJECT_ROOT / "artifacts" / name
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _build_gate_contract_summary(
    *,
    strict_gate_pass: bool,
    p0_open_count: int,
    active_critical_high: int,
    period_validation_detail: dict[str, object],
) -> dict[str, object]:
    if not callable(gate_detail_tag) or not callable(header_rag_status) or not callable(parse_gate_detail_tag) or not callable(period_validation_summary):
        return {
            "gate_detail_tag": "",
            "gate_contract_valid": False,
            "gate_contract_version": 0,
            "gate_contract_error": "release_rag_policy_unavailable",
            "gate_contract_rag": "",
        }
    stability_evidence = _load_artifact_json("stability_evidence_30d.json")
    peak_release_gate = _load_artifact_json("peak_release_gate_latest.json")
    pv_failed, pv_max = period_validation_summary(stability_evidence, peak_release_gate)
    if pv_failed == 0 and pv_max == 0:
        pv_failed = int(period_validation_detail.get("failed_items", 0) or 0)
    peak_level = str(peak_release_gate.get("level", "") or "").lower()
    rag_status = header_rag_status(
        bool(strict_gate_pass),
        int(p0_open_count or 0),
        int(active_critical_high or 0),
        peak_level,
        int(pv_failed),
        int(pv_max),
    )
    gate_tag = gate_detail_tag(
        rag_status,
        int(pv_failed),
        int(pv_max),
        message=str(period_validation_detail.get("message") or ""),
        action=str(period_validation_detail.get("recommended_action") or ""),
    )
    parsed = parse_gate_detail_tag(gate_tag)
    return {
        "gate_detail_tag": gate_tag,
        "gate_contract_valid": bool(parsed.get("ok", False)),
        "gate_contract_version": int(parsed.get("version", 0) or 0),
        "gate_contract_error": str(parsed.get("error", "") or ""),
        "gate_contract_rag": str(parsed.get("rag", "") or ""),
    }


def _build_json_output(results: list[CheckResult], *, new_only: bool) -> dict[str, object]:
    p0_names = _p0_names()
    sev_counts: dict[str, int] = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    for r in results:
        for v in r.violations:
            sev_counts[v.severity] = sev_counts.get(v.severity, 0) + 1
    active_ch = sum(
        1 for r in results
        for v in r.violations
        if r.name in p0_names and v.severity in ("critical", "high")
    )
    p0_fail_count = sum(1 for r in results if r.name in p0_names and r.status == "fail")
    _today_d = datetime.date.today()
    _due_90_cutoff = _today_d + datetime.timedelta(days=90)
    al_total = sum(len(v) for v in ALLOWLIST.values())
    al_expired = 0
    al_due_90d = 0
    for _entries in ALLOWLIST.values():
        for _e in _entries:
            try:
                _exp_d = datetime.date.fromisoformat(_e.expire)
                if _exp_d < _today_d:
                    al_expired += 1
                elif _exp_d <= _due_90_cutoff:
                    al_due_90d += 1
            except (ValueError, AttributeError):
                pass
    period_validation_detail = _extract_period_validation_detail(results)
    strict_gate_pass = p0_fail_count == 0 and active_ch == 0
    output = {
        "script_version": SCRIPT_VERSION,
        "mode": "new_only" if new_only else "full",
        "P0_open_count": p0_fail_count,
        "active_critical_high": active_ch,
        "strict_gate_pass": strict_gate_pass,
        "strict_pass": all(
            r.status in ("pass", "warn", "skip")
            for r in results if r.name in p0_names
        ),
        "severity_counts": sev_counts,
        "allowlist_total": al_total,
        "allowlist_expired": al_expired,
        "allowlist_due_90d": al_due_90d,
        "duckdb_write_probe_detail": _extract_duckdb_write_probe_detail(results),
        "intraday_bar_semantic_detail": _extract_intraday_bar_semantic_detail(results),
        "governance_nightly_detail": _extract_governance_nightly_detail(results),
        "watermark_quality_detail": _extract_watermark_quality_detail(),
        "watermark_profile_audit_detail": _extract_watermark_profile_audit_detail(),
        "watermark_profile_approval_detail": _extract_watermark_profile_approval_detail(),
        "period_validation_detail": period_validation_detail,
        "checks": [r.to_dict() for r in results],
    }
    contract_summary = _build_gate_contract_summary(
        strict_gate_pass=strict_gate_pass,
        p0_open_count=p0_fail_count,
        active_critical_high=active_ch,
        period_validation_detail=period_validation_detail,
    )
    output.update(contract_summary)
    gate_contract_valid = bool(output.get("gate_contract_valid", False))
    output["contract_health"] = "HEALTHY" if gate_contract_valid else "BROKEN"
    output["strict_gate_pass"] = bool(strict_gate_pass and gate_contract_valid)
    return output


def _persist_latest_metrics(output: dict[str, object]) -> None:
    try:
        artifacts_dir = PROJECT_ROOT / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        out_file = artifacts_dir / "p0_metrics_latest.json"
        out_file.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# 输出
# ─────────────────────────────────────────────────────────────────────────────

SEV_LABEL = {"critical": "CRIT", "high": "HIGH", "medium": "MED ", "low": "LOW "}


def print_summary(results: list[CheckResult], verbose: bool = False, new_only: bool = False) -> None:
    p0_names = _p0_names()
    result_by_name = {r.name: r for r in results}
    p0_fails = [r for r in results if r.name in p0_names and r.status == "fail"]
    strict_pass = not p0_fails
    STATUS_ICON = {"pass": "[OK]  ", "fail": "[FAIL]", "warn": "[WARN]", "skip": "[SKIP]"}
    MODE_TAG = " (PR新增模式)" if new_only else ""

    print("\n" + "=" * 72)
    print(f"  EasyXT P0 门禁检查报告{MODE_TAG}")
    print("=" * 72)

    for r in results:
        icon = STATUS_ICON.get(r.status, "[?]   ")
        level = " [P0]" if r.name in p0_names else " [P1]"
        sup_note = f" | 豁免:{r.suppressed}" if r.suppressed else ""
        print(f"  {icon} {r.name:<36s}{level}  {r.detail}{sup_note}")

        show_viols = r.violations if verbose else r.violations[:3]
        for v in show_viols:
            sev = SEV_LABEL.get(v.severity, "    ")
            print(f"         [{sev}] {v.location}: {v.message[:65]}")
            if v.fix_hint and ("\n" not in v.fix_hint):
                print(f"                >> {v.fix_hint}")
            elif v.fix_hint:
                # 多行修复建议缩进输出
                for hint_line in v.fix_hint.splitlines():
                    print(f"                >> {hint_line}")
        if not verbose and len(r.violations) > 3:
            print(f"         ... 还有 {len(r.violations) - 3} 处（--verbose 查看全部）")

    print("=" * 72)
    print(f"  P0_open_count           = {len(p0_fails)}")
    print(f"  strict_pass             = {'true' if strict_pass else 'false'}")
    for name in sorted(p0_names):
        r = result_by_name.get(name)
        if r:
            print(f"  {r.name:<36s} = {r.status}")
    print("=" * 72)
    if strict_pass:
        print("  [READY] 全部 P0 门禁通过，可以发布！")
    else:
        print("  [BLOCK] P0 门禁未通过，禁止发布。请修复后重新运行。")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# CI 阻断矩阵说明（嵌入 --help）
# ─────────────────────────────────────────────────────────────────────────────

CI_MATRIX_HELP = """
CI 三层流水线矩阵：
  PR 阶段    --new-only --strict --json
             仅阻断本次新增违规，存量技术债不阻断；stdout 纯 JSON（警告在 stderr）
  夜间巡检   --strict --json
             全量扫描，P0 fail 或 P0 范围内 critical/high > 0 均阻断
  Release    --strict --json --enforce-allowlist-expiry
             在夜间基础上，过期 allowlist 条目升级为 critical 硬阻断
  本地验收   --summary
             人工查看，含修复建议，退出码=0

硬门禁联动规则（--strict 时双重阻断）：
  P0_open_count == 0  且  P0 范围 active_critical_high == 0  → 放行
  任一 P0 fail  或  P0 范围有 critical/high 活跃违规          → 阻断（防降级绕过）

典型 GitHub Actions 片段：
  PR check job:
    run: python tools/p0_gate_check.py --new-only --strict --json
    # JSON 字段: .strict_gate_pass == true  .active_critical_high == 0

  Nightly job:
    run: python tools/governance_jobs.py --job all --strict-sla --strict-dead-letter
    run: python tools/p0_gate_check.py --strict --json

  Release job:
    run: python tools/p0_gate_check.py --strict --json --enforce-allowlist-expiry
    # 过期豁免条目 → critical → 阻断，确保发布前 allowlist 已清理

查看 SQL 修复优先级（Batch A 外部输入优先）：
  Windows PowerShell:
    python tools/p0_gate_check.py --check sql --verbose 2>&1 | Select-String "A-外部"
  Linux / bash:
    python tools/p0_gate_check.py --check sql --verbose 2>&1 | grep "A-外部"

保存当前状态为 baseline（首次接入 CI 或批量接受技术债；有效期 14 天）：
    python tools/p0_gate_check.py --save-baseline

安装 pre-commit 凭据扫描钩子（本地拦截，减少回滚成本）：
    python tools/pre_commit_scan.py --install
"""


# ─────────────────────────────────────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    # Windows 控制台默认 CP936，强制 UTF-8 以保证 --json 管道可靠传输
    stdout_reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(stdout_reconfigure):
        stdout_reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(
        description="EasyXT P0 门禁检查 v2 — 含 severity 分级 / AST 检测 / 修复建议 / CI 矩阵",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=CI_MATRIX_HELP,
    )
    parser.add_argument("--check", choices=list(ALL_CHECKS.keys()), help="仅运行指定单项检查")
    parser.add_argument("--summary", action="store_true", help="全部检查 + 汇总报告（含修复建议）")
    parser.add_argument("--strict", action="store_true", help="任一 P0 fail 则退出码=1")
    parser.add_argument("--new-only", action="store_true", dest="new_only",
                        help="PR 模式：对比 baseline，仅阻断新增违规")
    parser.add_argument("--save-baseline", action="store_true", dest="save_baseline",
                        help="将当前违规保存为 baseline（用于 --new-only 对比）")
    parser.add_argument("--json", action="store_true", help="JSON 格式输出（CI 解析用）")
    parser.add_argument("--verbose", action="store_true", help="展示全部 violation 详情")
    parser.add_argument(
        "--enforce-allowlist-expiry", action="store_true", dest="enforce_allowlist_expiry",
        help="Release 模式：allowlist 过期条目升级为 critical（CI 硬阻断），默认仅 warn",
    )
    parser.add_argument(
        "--list-allowlisted-files", action="store_true", dest="list_allowlisted_files",
        help="输出 allowlist 覆盖的文件路径列表（JSON 数组），供 CI 变更触发复核使用",
    )
    args = parser.parse_args()

    # 仅列出 allowlist 覆盖文件：供 CI 步骤检测 PR 变更是否触碰豁免边界
    if args.list_allowlisted_files:
        files = sorted({
            e.pattern.replace("\\", "/")
            for entries in ALLOWLIST.values()
            for e in entries
            if e.pattern
        })
        print(json.dumps(files, ensure_ascii=False))
        return 0

    # Release 模式：过期 allowlist 条目触发 critical 阻断（覆盖模块级默认值）
    global _enforce_allowlist_expiry
    _enforce_allowlist_expiry = args.enforce_allowlist_expiry

    # 运行检查
    if args.check:
        results = [ALL_CHECKS[args.check]()]  # type: ignore[operator]
    else:
        results = run_all_checks()

    # 保存 baseline
    if args.save_baseline:
        save_baseline(results)
        return 0

    # PR 模式：对比 baseline 过滤存量
    if args.new_only:
        baseline = load_baseline()
        if not baseline:
            print(
                "[WARN] 未找到 baseline 文件，将使用全量违规（建议先运行 --save-baseline）",
                file=sys.stderr,
            )
        results = filter_new_violations(results, baseline)

    # 输出
    output = _build_json_output(results, new_only=args.new_only)
    _persist_latest_metrics(output)
    if args.json:
        print(json.dumps(output, ensure_ascii=False, indent=2))
    else:
        print_summary(results, verbose=args.verbose, new_only=args.new_only)

    # ── 退出码（硬门禁联动规则）─────────────────────────────────────────────
    # --strict 同时满足两个条件才放行：
    #   1) P0 fail = 0（所有 P0 检查通过）
    #   2) critical/high 活跃违规 = 0（防止降级到 medium 的策略性绕过）
    if args.strict:
        p0_names = _p0_names()
        p0_fail = any(r.name in p0_names and r.status == "fail" for r in results)
        # 仅统计 P0 检查（与 JSON 输出字段 active_critical_high 语义一致）
        active_ch = sum(
            1 for r in results
            for v in r.violations
            if r.name in p0_names and v.severity in ("critical", "high")
        )
        if p0_fail or active_ch > 0:
            if not args.json:
                failed_names = [r.name for r in results if r.name in p0_names and r.status == "fail"]
                print(
                    f"[BLOCK] P0_fail={int(p0_fail)} | "
                    f"active_critical/high={active_ch} | "
                    f"failed_checks={failed_names or 'none'}",
                    file=sys.stderr,
                )
            return 1
        return 0
    if args.check:
        return 0 if results[0].status in ("pass", "warn", "skip") else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
