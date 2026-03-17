## 双源离线对账（QMT vs AKShare）

### 目标

- 用离线审计确认多源数据在同一时间窗的一致性
- 对差异超阈值标的输出红线告警
- 产出可追溯 JSON 与周报 Markdown

### 配置文件

- [data_reconciliation_audit.json](file:///d:/EasyXT_KLC/config/data_reconciliation_audit.json)

### 本地运行

```bash
python tools/data_reconciliation_audit.py --config config/data_reconciliation_audit.json --out-dir artifacts
```

严格模式（有失败标的即返回非零）：

```bash
python tools/data_reconciliation_audit.py --config config/data_reconciliation_audit.json --out-dir artifacts --strict
```

使用 qmt311 快捷入口：

```bash
powershell -ExecutionPolicy Bypass -File run_reconciliation.ps1
```

### 红线阈值

- `close_rel_p95_max`
- `volume_rel_p95_max`
- `overlap_ratio_min`

### 产物

- `artifacts/source_reconciliation_*.json`
- `artifacts/source_reconciliation_*.md`
- `artifacts/source_reconciliation_latest.json`
- `artifacts/source_reconciliation_latest.md`
- `artifacts/governance_source_reconciliation_latest.json`
- `artifacts/p0_trend_history.json`（已注入 `checks.source_reconciliation` 与 `reconciliation` 字段）

### CI 工作流

- [source-reconciliation-audit.yml](file:///d:/EasyXT_KLC/.github/workflows/source-reconciliation-audit.yml)
- 支持 `workflow_dispatch` 手动触发：
  - `strict_mode=true`：有失败标的即任务失败
  - `strict_mode=false`：任务成功但保留失败明细到 artifact
