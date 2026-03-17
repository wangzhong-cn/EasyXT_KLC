# 策略门禁软转硬落地节奏

## 目标

在不阻塞业务放量的前提下，把数据门禁逐步提升为策略发布硬门禁。

## 当前状态

| 阶段 | 开始日期 | 状态 | 备注 |
|------|----------|------|------|
| Stage 0 | — | ✅ 已完成 | 门禁基础建设完毕，`strict_pass=true` |
| Stage 1 | 2026-03-10 | 🔄 **进行中** | 软检查 + 人工审批，计数连续 10 交易日 |
| Stage 2 | — | ⏳ 待启动 | 硬门禁白名单试运行 |
| Stage 3 | — | ⏳ 待启动 | 全量硬阻断 |

**Stage 1 连续计数起点：2026-03-10（第 1 / 10 交易日）**

## 分阶段策略

### Stage 0（✅ 已完成）

- 门禁状态：`strict_gate_pass=true`
- 策略接入方式：软检查（只告警，不阻断）
- 要求：每次策略发布附门禁 JSON 结果

### Stage 1（模拟盘周期）

- 持续时间：2-4 周
- 门禁策略：软检查 + 人工审批
- 退出条件：
  - 连续 10 个交易日 `strict_gate_pass=true`
  - 无 P0 回归
  - 周报 `active_critical_high` 维持 0

### Stage 2（硬门禁试运行）

- 范围：指定策略组（白名单）
- 门禁策略：不满足即阻断发布
- 退出条件：
  - 连续 2 周硬门禁无误阻塞
  - 无因门禁误报导致的回滚事件

### Stage 3（全量硬门禁）

- 范围：所有策略发布入口
- 门禁策略：硬阻断 + 审计留痕
- 常态要求：
  - PR：`--new-only --strict --json`
  - Nightly：`--strict --json`
  - Release：`--strict --json --enforce-allowlist-expiry`

## 发布前检查清单

- `python tools/p0_baseline_verify.py`
- `python tools/p0_gate_check.py --strict --json`
- `python tools/p0_audit_hash.py`
- `python tools/p0_trend_update.py`

## 责任分工

- 数据平台：门禁脚本与规则维护
- 策略团队：策略发布前自检与解释
- 发布经理：Release Gate 最终签核

## 例外处理

- 紧急修复允许走受控豁免（标签或 PR 复选框）
- 例外必须落审计事件并在周报复盘
