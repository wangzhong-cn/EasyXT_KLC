# 稳定性回归门禁

## 一键执行

```powershell
powershell -ExecutionPolicy Bypass -File tools/run_stability_regression.ps1
```

可选参数：

```powershell
powershell -ExecutionPolicy Bypass -File tools/run_stability_regression.ps1 -Runs 5 -Group all
```

## 分组

- derived_period_chain：派生周期链路
- write_observability_chain：写入后验与可观测性链路
- fallback_recovery_chain：回退与恢复链路
- convergence_contract_chain：收敛契约链路
- thread_lifecycle_chain：线程生命周期与图表防崩链路
- timestamp_contract_chain：事件时间字段契约链路

## 产物

- `artifacts/stability_regression_gate_latest.json`

## 判定标准

- 连续运行 N 次（默认 3）全部返回码为 0
- 同一分组各次运行的 passed/failed 计数一致
- 任一分组出现波动或失败，门禁失败（退出码 1）
