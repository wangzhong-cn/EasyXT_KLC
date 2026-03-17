# EasyXT 数据基础设施 SLO/SLI 规范（模板）

> 版本：v1.0（模板）
> 适用范围：`data_manager` / `easy_xt.realtime_data` / `easyxt_backtest` 的数据链路治理
> 目标：将“可用”提升为“可审计、可度量、可追责、可持续优化”的下一代量化数据基座

---

## 1. 评分模型与判分标准

### 1.1 总分结构（10 分制）

| 控制域 | 权重 | 达标定义（摘要） |
|---|---:|---|
| 数据正确性 | 2.0 | 交易日边界、OHLC 合法、复权口径一致、跨源一致性 |
| 时效性 | 1.2 | 新鲜度、端到端延迟（p95/p99）可控 |
| 可用性与容灾 | 1.4 | 主备切换、断路器策略、失败恢复能力 |
| 一致性与血缘 | 1.2 | 采集来源、哈希、时间戳、链路可追溯 |
| 门禁与隔离重放 | 1.2 | 硬门禁、隔离、自动重放、死信可控 |
| 配置与环境治理 | 1.0 | 启动 fail-fast、配置 schema 校验 |
| 可观测性与运维 | 1.0 | health、metrics、告警、审计报表 |
| 测试与变更治理 | 1.0 | CI gate + nightly + 回归基线稳定 |

### 1.2 分档解释

| 分数区间 | 等级 | 含义 |
|---|---|---|
| 0.0 ~ 5.9 | 不可生产 | 存在系统性脏数据/漏检风险 |
| 6.0 ~ 7.4 | 可用但有中高风险 | 关键链路可用，闭环不完整 |
| 7.5 ~ 8.9 | 生产可控 | 主要风险受控，具备可恢复性 |
| 9.0 ~ 10.0 | 下一代平台级 | 具备自动化闭环和持续优化能力 |

---

## 2. 核心 SLI 指标字典（必须项）

### 2.1 数据正确性（Correctness）

| 指标 ID | 公式 | 目标阈值 | 严重级别 |
|---|---|---|---|
| `sli_non_trading_day_rate` | 非交易日 K 线条数 / 总条数 | `<= 1e-6` | P0 |
| `sli_ohlc_violation_rate` | OHLC 逻辑违规条数 / 总条数 | `<= 1e-5` | P0 |
| `sli_non_positive_price_rate` | 非正价格条数 / 总条数 | `<= 1e-6` | P0 |
| `sli_duplicate_bar_rate` | 重复时间戳条数 / 总条数 | `<= 1e-5` | P1 |
| `sli_cross_source_basis_bp` | `abs(close_A-close_B)/close_B*10000` | 日线 `<= 2bp` | P1 |

### 2.2 时效性（Freshness & Latency）

| 指标 ID | 定义 | 目标阈值 | 严重级别 |
|---|---|---|---|
| `sli_freshness_breach_rate` | `now-last_update > SLA` 的比例 | `< 0.1%` | P1 |
| `sli_ingest_latency_p95_ms` | 采集到入库端到端延迟 p95 | 1m 周期 `< 3000ms` | P1 |
| `sli_ingest_latency_p99_ms` | 采集到入库端到端延迟 p99 | 1m 周期 `< 8000ms` | P1 |

### 2.3 可用性与容灾（Availability）

| 指标 ID | 公式 | 目标阈值 | 严重级别 |
|---|---|---|---|
| `sli_ingest_success_rate` | success / (success + failed) | `>= 99.9%` | P0 |
| `sli_cb_open_ratio` | 断路器 OPEN 时长 / 总时长 | `< 1%` | P1 |
| `sli_failover_mttr_s` | 主源失败至备源恢复时间 | `< 30s`（分钟线） | P1 |

### 2.4 门禁与隔离重放（Gate & Replay）

| 指标 ID | 定义 | 目标阈值 | 严重级别 |
|---|---|---|---|
| `sli_gate_leak_rate` | 门禁拒绝后仍入库比例 | `= 0` | P0 |
| `sli_quarantine_backlog` | 当前隔离待处理条数 | 趋势受控 | P1 |
| `sli_quarantine_replay_success_rate` | replay 成功 / replay 总数 | `>= 95%` | P1 |
| `sli_dead_letter_ratio` | dead_letter / quarantine 总数 | `< 1%` | P1 |

### 2.5 策略影响稳定性（Strategy Impact）

| 指标 ID | 定义 | 建议阈值 | 严重级别 |
|---|---|---|---|
| `sli_backtest_return_delta` | 基准数据与线上数据回测收益差 | `abs(ΔR) < 3%` | P1 |
| `sli_backtest_mdd_delta` | 最大回撤差值 | `abs(ΔMDD) < 1.5%` | P1 |
| `sli_backtest_sharpe_sign` | Sharpe 符号一致性 | 不允许反转 | P0 |

---

## 3. SLO 目标声明（模板）

### 3.1 月度 SLO

- `SLO-C1`：`sli_non_trading_day_rate` 月均 `<= 1e-6`
- `SLO-A1`：`sli_ingest_success_rate` 月均 `>= 99.9%`
- `SLO-G1`：`sli_gate_leak_rate = 0`
- `SLO-R1`：`sli_quarantine_replay_success_rate >= 95%`
- `SLO-L1`：`sli_ingest_latency_p95_ms < 3000`（1m 周期）

### 3.2 错误预算（Error Budget）

- 可用性错误预算：`1 - 99.9% = 0.1%`
- 预算消耗策略：
  - 消耗 > 50%：冻结非紧急变更
  - 消耗 > 80%：仅允许修复类变更
  - 消耗 > 100%：触发变更审查与回滚机制

---

## 4. 告警阈值草案（可直接转 Prometheus/OTel）

### 4.1 优先级定义

- `P0`：影响数据正确性或导致策略方向反转
- `P1`：影响可用性、时效性或恢复能力
- `P2`：影响观测与运维效率

### 4.2 规则草案

| 告警 ID | 触发条件 | 持续时间 | 等级 | 处置建议 |
|---|---|---:|---|---|
| `alert_gate_leak` | `sli_gate_leak_rate > 0` | 1m | P0 | 立即阻断写入并排查门禁 |
| `alert_non_trading_day_detected` | `sli_non_trading_day_rate > 1e-6` | 5m | P0 | 锁定来源、触发隔离与回放 |
| `alert_ingest_success_drop` | `sli_ingest_success_rate < 99.5%` | 10m | P1 | 检查主源与备源健康 |
| `alert_cb_open_long` | `sli_cb_open_ratio > 5%` | 30m | P1 | 调整阈值并排查上游故障 |
| `alert_quarantine_backlog_spike` | `sli_quarantine_backlog` 日环比 > 100% | 30m | P1 | 提升 replay 并分析根因 |
| `alert_replay_success_low` | `sli_quarantine_replay_success_rate < 90%` | 30m | P1 | 检查重放逻辑/权限/源可用性 |
| `alert_latency_p99_breach` | `sli_ingest_latency_p99_ms > 8000` | 15m | P1 | 限流 + 异步解耦 + 重试策略 |

---

## 5. 实施要件清单（Definition of Done）

### 5.1 平台级必须项（全部完成才可评为 9.0+）

- 启动校验 fail-fast 全入口接入（GUI/CLI/服务）
- 多源分钟线冗余（主备至少 2 源）
- 回读二次合约验证（可配置采样率）
- quarantine 自动重放任务（日/小时级）
- 数据血缘字段完整（source/hash/event_time/ingestion_id）
- 统一 health 端点输出（source、CB、backlog、SLO）
- CI + nightly + 生产巡检三层门禁
- 策略影响门禁（Δ收益、Δ回撤、Sharpe 符号）

### 5.2 验收门槛

- 连续 14 天 `sli_gate_leak_rate = 0`
- 连续 14 天 `sli_ingest_success_rate >= 99.9%`
- 连续 14 天 `sli_quarantine_backlog` 无失控增长
- 回测基线门禁连续通过（无符号反转）

---

## 6. Prometheus 指标命名建议

| 语义 | 建议指标名 | 类型 |
|---|---|---|
| 门禁拒绝总数 | `easyxt_gate_reject_total` | Counter |
| 门禁漏入库总数 | `easyxt_gate_leak_total` | Counter |
| 入库成功总数 | `easyxt_ingest_success_total` | Counter |
| 入库失败总数 | `easyxt_ingest_failed_total` | Counter |
| 隔离队列长度 | `easyxt_quarantine_backlog` | Gauge |
| 重放成功总数 | `easyxt_quarantine_replay_success_total` | Counter |
| 重放失败总数 | `easyxt_quarantine_replay_failed_total` | Counter |
| 断路器状态 | `easyxt_source_cb_state` (`0/1/2`) | Gauge |
| 入库延迟 | `easyxt_ingest_latency_ms` | Histogram |
| 数据新鲜度 | `easyxt_data_freshness_seconds` | Gauge |

---

## 7. 周报模板（治理例会）

- 本周 SLO 总览：达标/未达标项
- Top3 告警来源：按次数与影响排序
- 隔离与重放：新增、清理、死信情况
- 断路器行为：OPEN 次数、恢复时长
- 策略影响：回测偏移摘要（ΔR、ΔMDD、ΔSharpe）
- 下周动作：责任人、截止日期、验收标准

---

## 8. 版本演进建议

- v1.0：先完成指标采集与可观测
- v1.1：补齐自动重放与健康端点
- v1.2：引入跨源一致性与策略影响门禁
- v2.0：实现按资产类别分层 SLO（股票/期货/指数/期权）

