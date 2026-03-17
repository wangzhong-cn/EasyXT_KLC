# P0 治理月度回顾模板

> 用法：每月末（或每月第一个周一）按此模板拉取数据、填写结论、归档到 `artifacts/`。
> 预计耗时：15 分钟（含数据拉取命令）。

---

## 基本信息

| 字段 | 值 |
|------|----|
| 回顾月份 | YYYY-MM |
| 主持人 | |
| 数据截止日 | YYYY-MM-DD |
| 报告生成时间 | _(自动填写)_ |

---

## 第一节：关键指标（拉取命令 < 2 分钟）

```bash
# 1. 运行全量巡检，获取当月最终状态
python tools/p0_gate_check.py --strict --json > artifacts/p0_monthly_YYYYMM.json

# 2. 刷新趋势看板
python tools/p0_trend_update.py --gate-json artifacts/p0_monthly_YYYYMM.json

# 3. 生成近 30 天周报
python tools/p0_weekly_report.py --window-days 30 --out artifacts/p0_monthly_report_YYYYMM.md
```

### 1.1 稳态指标

| 指标 | 目标 | 本月实际 | 达标 |
|------|------|----------|------|
| `strict_pass=true` 天数占比（SLA） | ≥ 95% | | |
| 月末 `P0_open_count` | = 0 | | |
| 月末 `active_critical_high` | = 0 | | |

### 1.2 Allowlist 健康度

| 指标 | 上月末 | 本月末 | 变化 |
|------|--------|--------|------|
| `allowlist_total`（条目总量） | | | |
| `allowlist_expired`（过期条目） | | | Δ |
| `allowlist_due_90d`（90天内到期） | | | |
| `allowlist_touched_pr_count`（月内触碰豁免文件 PR 数） | — | | |

> **健康目标**：`allowlist_total` 逐月收缩或持平；`allowlist_expired=0`；`allowlist_due_90d` 提前处理。

### 1.3 active_critical_high 周均值趋势

> 从 `artifacts/p0_trend_history.json` 取本月 4 周数据，计算周均值。

| 周次 | 日期区间 | `active_critical_high` 均值 | 趋势 |
|------|----------|-----------------------------|------|
| W1 | MM-DD ~ MM-DD | | |
| W2 | MM-DD ~ MM-DD | | |
| W3 | MM-DD ~ MM-DD | | |
| W4 | MM-DD ~ MM-DD | | |
| **月均** | | | ↑/→/↓ |

---

## 第二节：Allowlist 条目逐条复审

> 对所有 `expire` 在本月末后 90 天内的条目，必须在本节作出三选一决策。
> 命令：`python tools/p0_gate_check.py --check allowlist --verbose`

| 条目 | 当前 expire | issue_ref | 决策 | 负责人 | 新 expire / 备注 |
|------|------------|-----------|------|--------|-----------------|
| `ALLOWLIST[xxx]/path` | YYYY-MM-DD | #? | 续期/删除/参数化 | | |

### 决策说明

- **续期**：违规尚未修复，延期 12 个月，必须填写 `issue_ref`（新 issue 或复用旧 issue）
- **删除**：对应代码已修复，从 `ALLOWLIST` 删除此 `AllowEntry`
- **参数化改造**：把误报根因消除（如把硬编码改为参数输入），从根本上消除对豁免的依赖

> `issue_ref` 合法格式：`'#123'` 或 `'https://github.com/org/repo/issues/123'`

---

## 第三节：SLO 回顾

### 3.1 本月 SLO 事件

| 日期 | 事件类型 | 持续时间 | 根因 | 已解决 |
|------|----------|----------|------|--------|
| | SLO 停滞 / 门禁 FAIL | | | ✅/❌ |

### 3.2 Allowlist 触碰 PR 明细（本月）

> 从 `artifacts/p0_allowlist_touch_events.json` 中筛选本月记录。

| PR # | 日期 | 触碰文件 | 是否有受控豁免标签 | 审批人已确认 |
|------|------|----------|--------------------|-------------|
| | | | ✅/❌ | ✅/❌ |

---

## 第四节：下月行动计划

| 优先级 | 行动 | 负责人 | 截止日 |
|--------|------|--------|--------|
| P0 | 清理即将到期的 allowlist 条目（90天内） | | |
| P0 | 确保 strict_pass 持续保持 true | | |
| P1 | | | |

---

## 第五节：治理趋势摘要（可选，管理层呈现用）

```
本月治理状态：[HEALTHY / AT RISK / DEGRADED]

核心结论（1–3 句话）：
-
-
-

与上月相比：
- allowlist 总量：+0 / -0（净变化）
- 豁免压力（触碰 PR 数）：N/A → N
- SLA 达成率：XX%
```

---

## 归档清单

完成本次回顾后，请将以下文件归档：

- [ ] `artifacts/p0_monthly_YYYYMM.json` — 门禁原始数据
- [ ] `artifacts/p0_monthly_report_YYYYMM.md` — 自动生成周报
- [ ] 本模板填写版本（重命名为 `docs/reviews/p0_review_YYYYMM.md`）
- [ ] 更新 `artifacts/p0_trend_latest.md`（趋势看板）

---

_模板版本: v1.0 | 维护: tools/p0_gate_check.py + p0_weekly_report.py_
