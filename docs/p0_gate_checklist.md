# EasyXT P0 门禁检查清单 — 固化验收命令

> **版本**: v1.0 | **日期**: 2026-03-09
> **配套报告**: [data_infrastructure_diagnosis_v2.md](./data_infrastructure_diagnosis_v2.md)（v2.1）
> **执行脚本**: [tools/p0_gate_check.py](../tools/p0_gate_check.py)
> **适用阶段**: 每次 PR 合并前 / 版本发布前 / 生产部署前

---

## 放行铁门槛（全部通过方可发布）

```
P0_open_count            == 0
strict_pass              == true
timestamp_contract_check == pass
credential_scan          == pass
snapshot_publish_atomic  == pass
sla_daily_gate           == pass
duckdb_write_probe       == pass
realtime_quote_contract_check == pass
```

**一键全量验收**（推荐 CI 使用）：
```powershell
# 以 myenv 环境运行（Python 3.11.14），退出码：0=通过 1=阻断
& "C:\Users\wangzhong\miniconda3\envs\myenv\python.exe" tools/p0_gate_check.py --strict --summary
```

**JSON 输出（CI 解析用）**：
```powershell
& "C:\Users\wangzhong\miniconda3\envs\myenv\python.exe" tools/p0_gate_check.py --strict --json
```

**夜间巡检（建议）**：
```powershell
& "C:\Users\wangzhong\miniconda3\envs\myenv\python.exe" tools/governance_jobs.py --job all --strict-sla --strict-dead-letter
& "C:\Users\wangzhong\miniconda3\envs\myenv\python.exe" tools/p0_gate_check.py --strict --json
```

**CI 工作流（已固化）**：
```text
.github/workflows/nightly-governance.yml
```

---

## P0 检查项详细清单

### P0-S1: 凭据扫描（`credential_scan`）

**问题根因**: `tools/qmt_data_manager.py:L29-L33` 和 `config/unified_config.json:L30-L32` 含明文账户/密码。

| 步骤 | 命令 | 期望输出 |
|------|------|---------|
| 单项检查 | `python tools/p0_gate_check.py --check credential` | `credential_scan = pass` |
| 人工验证 | `Select-String -Path tools\qmt_data_manager.py -Pattern "PASSWORD\s*=" \| Select LineNumber,Line` | 行内含 `os.environ`，无字符串字面量 |
| 配置验证 | `python -c "import json; c=json.load(open('config/unified_config.json')); print(c.get('password',''))"` | 输出 `${EASYXT_PASSWORD}` 或空 |

**fail-fast 条件**: 输出中出现 `PASSWORD = "..."`（字面量）→ CI 阻断。

**修复动作**:
```python
# tools/qmt_data_manager.py
import os
ACCOUNT_ID = os.environ.get('EASYXT_ACCOUNT_ID', '')
PASSWORD   = os.environ.get('EASYXT_PASSWORD', '')
if not ACCOUNT_ID or not PASSWORD:
    raise RuntimeError("缺少凭据环境变量 EASYXT_ACCOUNT_ID / EASYXT_PASSWORD")
```

---

### P0-S2: SQL 注入扫描（`sql_injection_scan`）

**问题根因**: 11 处 SQL 字符串插值（见诊断报告附录 A），DuckDB 允许多语句执行。

| 步骤 | 命令 | 期望输出 |
|------|------|---------|
| 单项检查 | `python tools/p0_gate_check.py --check sql` | `sql_injection_scan = pass` |
| 人工复核 | `Get-ChildItem -Recurse -Filter *.py data_manager,easy_xt \| Select-String -Pattern "f['\"].*WHERE.*'\{"` | 无输出（0 matches） |

**fail-fast 条件**: 输出行数 > 0 → CI 阻断。

**修复模板**:
```python
# 修复前（有注入风险）：
con.execute(f"SELECT * FROM stock_daily WHERE stock_code = '{stock_code}'")

# 修复后（参数化）：
con.execute("SELECT * FROM stock_daily WHERE stock_code = ?", [stock_code])
```

---

### P0-D1: 时间戳合约（`timestamp_contract_check`）

**问题根因**: `datetime.fromtimestamp(x/1000)` 依赖系统时区，Docker/UTC 服务器日期偏移一天。

| 步骤 | 命令 | 期望输出 |
|------|------|---------|
| 单项检查 | `python tools/p0_gate_check.py --check timestamp` | `timestamp_contract_check = pass` |
| 人工复核 | `Get-ChildItem -Recurse -Filter *.py data_manager,easy_xt,core \| Select-String -Pattern "datetime\.fromtimestamp\([^,)]+/\s*1000\s*\)"` | 无输出 |

**fail-fast 条件**: 出现无时区 `datetime.fromtimestamp(x/1000)` 模式 → CI 阻断。

**修复模板**:
```python
# 修复前（时区依赖系统）：
df['date'] = df.index.map(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d'))

# 修复后（显式北京时间）：
from zoneinfo import ZoneInfo
SHANGHAI = ZoneInfo("Asia/Shanghai")
df['date'] = df.index.map(
    lambda x: datetime.fromtimestamp(x / 1000, tz=SHANGHAI).strftime('%Y-%m-%d')
)
```

---

### P0-D2: xtdata 导入规范（`xtdata_import_check`）

**问题根因**: `easy_xt/triple_source_manager.py:L93` 使用 `import xtdata` 而非 `import xtquant.xtdata`，导致 QMT 被永远标记为不可用。

| 步骤 | 命令 | 期望输出 |
|------|------|---------|
| 单项检查 | `python tools/p0_gate_check.py --check xtdata` | `xtdata_import_check = pass` |
| 人工复核 | `Get-ChildItem -Recurse -Filter *.py \| Select-String -Pattern "^\s*import xtdata\b"` | 无输出 |

**fail-fast 条件**: 出现裸 `import xtdata` → CI 阻断。

**修复动作**:
```python
# easy_xt/triple_source_manager.py L93 修复：
def _check_qmt(self) -> bool:
    try:
        import xtquant.xtdata   # ← 修正
        return True
    except ImportError:
        return False
    except Exception:
        return False
```

---

### P0-D3: 原子发布（`snapshot_publish_atomic`）

**问题根因**: 4 个文件中 DELETE+INSERT 无事务，崩溃时数据半删半写。

| 步骤 | 命令 | 期望输出 |
|------|------|---------|
| 单项检查 | `python tools/p0_gate_check.py --check publish` | `snapshot_publish_atomic = pass` |
| 人工复核 | `Get-ChildItem -Recurse -Filter *.py data_manager \| Select-String -Pattern "DELETE FROM"` | 每处结果所在文件同时含 `TRANSACTION` 或 `BEGIN` |

**fail-fast 条件**: 含 DELETE+INSERT 的文件未使用事务 → CI 阻断。

**修复模板**:
```python
# 修复前（非原子）：
con.execute(f"DELETE FROM {table} WHERE stock_code = '{stock_code}'")
con.execute(f"INSERT INTO {table} SELECT * FROM df")

# 修复后（原子）：
con.execute("BEGIN TRANSACTION")
try:
    con.execute("DELETE FROM ? WHERE stock_code = ?", [table, stock_code])
    con.execute(f"INSERT INTO {table} SELECT * FROM df")
    con.execute("COMMIT")
except Exception:
    con.execute("ROLLBACK")
    raise
```

---

## 查看全量详情

```powershell
# 全部 P0 检查，展示所有 violation（不截断）：
& "C:\Users\wangzhong\miniconda3\envs\myenv\python.exe" `
  tools/p0_gate_check.py --summary --verbose

# JSON 格式输出（适合脚本解析）：
& "C:\Users\wangzhong\miniconda3\envs\myenv\python.exe" `
  tools/p0_gate_check.py --strict --json | python -m json.tool
```

---

## 72 小时放行路线图

| 天次 | 目标 | 关键验收命令 | 期望结果 |
|------|------|------------|---------|
| **Day 0** | 凭据清零 + SQL 注入修复 | `python tools/p0_gate_check.py --check credential` | `credential_scan == pass` |
| **Day 0** | SQL 注入全清 | `python tools/p0_gate_check.py --check sql` | `sql_injection_scan == pass` |
| **Day 1** | DAT 提升 + xtdata 修正 | `python tools/p0_gate_check.py --check xtdata` | `xtdata_import_check == pass` |
| **Day 2** | 时间戳合约执行 | `python tools/p0_gate_check.py --check timestamp` | `timestamp_contract_check == pass` |
| **Day 3** | 原子发布全覆盖 | `python tools/p0_gate_check.py --check publish` | `snapshot_publish_atomic == pass` |
| **Day 3** | 全量放行 | `python tools/p0_gate_check.py --strict --summary` | `P0_open_count == 0, strict_pass == true` |

---

## 环境说明

| 环境 | Python | xtquant | 适用场景 |
|------|--------|---------|---------|
| `myenv` | 3.11.14 | ✅ 可用 | 生产运行、QMT 数据获取 |
| `base` | 3.13.x | ❌ 不可用 | 纯 Python 开发调试 |

**运行门禁检查必须使用 myenv**（确保 xtquant 导入测试有效）：
```powershell
# 激活 myenv 后运行：
conda activate myenv
python tools/p0_gate_check.py --strict --summary
```

---

*本文档与 `data_infrastructure_diagnosis_v2.md` v2.1 配套，作为独立可执行验收清单。*
*最后更新: 2026-03-09*
