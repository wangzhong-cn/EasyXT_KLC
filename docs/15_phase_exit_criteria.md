# 阶段退出条件规范

> 本文档是当前仓库 **Phase 退出条件** 的独立规范来源。
>
> 自 2026-03-31 起，`tools/check_phase_exit.py` 不再把 `architecture_roadmap_direction2.md` 作为硬依赖入口。

## 适用范围

本文档用于约束：

- 何时允许宣布某个阶段完成
- 何时禁止进入下一阶段
- CI / 本地守门脚本需要检查哪些量化条件

## 当前阶段退出条件总表

| 阶段 | 进入前提 | 退出条件（满足全部才算完成） | 禁止进入下阶段的条件 |
| ---- | -------- | ---------------------------- | -------------------- |
| **Phase 0** | 无 | ① 全量测试覆盖 ≥ 40%；② `pytest tests/ -q` exit 0；③ 核心路径无 bare-except | 覆盖率不足或关键 bare-except 未修复 |
| **Phase 1** | Phase 0 满足 | ① 风控引擎上线且预交易拒单率可测量；② 策略基类标准化；③ 回撤熔断 D-3 演练通过；④ 月度误拦截率 ≤ 10% | 风控引擎未达标 / 演练未通过 / 全量测试失败 |
| **Phase 2** | Phase 0 满足（可并行） | ① T+1 对账引擎上线；② `verify_chain_integrity()` 为 `ok=True`；③ `batch_hash` 链式验证通过率 = 100% | 审计链存在 tampered 或断链 |
| **Phase 3** | Phase 1 + Phase 2 满足 | ① SLO 仪表盘输出 Prometheus 指标；② Error Budget 联动发布门禁上线；③ `release_gate()` 在 CI 中生效 | error budget 消耗 > 80% |
| **方向3 Phase A** | Phase 1-3 全部完成 | ① FastAPI server 负载测试通过；② LWC 通过 WS 消费数据、无 DuckDB 直连依赖；③ schema contract 全绿 | 前置阶段未完成或系统不稳定 |
| **方向3 Phase B** | Phase A 完成 + QMT 云 API 可用 | ① 策略在 K8s 稳定运行 2 周；② Kafka 替代 signal_bus 后测试通过 | Phase A 未完成或外部条件未满足 |

## 当前脚本已落地的检查范围

`tools/check_phase_exit.py` 当前已经代码化的阶段为：

- **Phase 0**
- **Phase 1**
- **Phase 2**

其余阶段目前仍以文档为主，待后续量化指标完全落地后，再继续脚本化。

## Phase 0 当前脚本检查项

- `pytest` 是否已全量通过
- `coverage.xml` 的总覆盖率是否达到下限
- 目录覆盖率配额是否达标
- 核心模块覆盖率是否达标
- 核心路径是否存在 bare-except

## Phase 1 当前脚本检查项

- 延续 Phase 0 的核心质量门禁
- `core/risk_engine.py` 是否存在
- `ThreadLifecycleMixin` 落地率是否合规
- `docs/drill_reports/open_incidents.md` 是否存在未关闭事故

## Phase 2 当前脚本检查项

- 延续基础测试通过要求
- `verify_chain_integrity()` 是否返回 `ok=True`
- `core/audit_trail.py` 覆盖率是否达到要求
- `open_incidents.md` 是否清零

## 推荐使用方式

- 本地检查：`../tools/check_phase_exit.py --phase 0 --report-only`
- CI 检查：由上游步骤先跑测试与 coverage，再把结果喂给 `check_phase_exit.py`

## 与仓库实现的对应关系

- 守门脚本：`../tools/check_phase_exit.py`
- 审计链：`../core/audit_trail.py`
- 线程生命周期检查：`../tools/check_thread_lifecycle.py`
- 演练未关闭事件：`drill_reports/open_incidents.md`

## 备注

> `architecture_roadmap_direction2.md` 的完整正文已归档到 `archive/architecture_roadmap_direction2_v1.md`，原路径仅保留兼容说明页；
> 阶段退出条件的当前维护入口，已经切到本文档。
