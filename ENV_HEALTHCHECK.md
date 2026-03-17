# ENV_HEALTHCHECK — 环境诊断与修复指南

## 一键检查

```bash
python tools/check_env.py          # 全量检查（含 pytest 发现）
python tools/check_env.py --quick  # 仅检查 import，跳过 pytest 发现
```

检查通过示例输出：

```
=======================================================
 EasyXT 环境健康检查
=======================================================

[1] Python 版本
  [OK]   Python 3.11.9  (需要 >= 3.9)

[2] 必选依赖
  [OK]   required: pandas
  [OK]   required: numpy
  [OK]   required: pytest

[3] 可选依赖
  [OK]   optional: duckdb
  ...

[4] artifacts/ 目录
  [OK]   artifacts/ 目录可写

[5] pytest 测试用例发现
  [OK]   pytest 发现: tests/test_stability_tools.py  37 selected

=======================================================
 结果: ✅ 全部通过
=======================================================
```

---

## 已知问题：`myenv` DLL 加载失败

### 症状

```
Process exited with code -1073741515
```

Windows exit code `-1073741515` = `0xC0000135` = `STATUS_DLL_NOT_FOUND`。

即 `myenv\Scripts\python.exe` 启动时找不到依赖的 C Runtime / VC++ Redistributable DLL。

### 根本原因

| 原因 | 说明 |
|------|------|
| Conda 环境迁移/重建 | `myenv` 如果是从 conda base 直接复制或 `--copies` 创建，VC++ DLL 路径可能断链 |
| VC++ Redistributable 缺失 | 系统未安装与 Python 编译版本匹配的 VC++ 2015-2022 运行库 |
| numpy/pandas C 扩展 `.pyd` 编译版本不匹配 | Python 小版本升级后旧 `.pyd` 文件失效 |

### 修复选项

**方案 A（推荐）：使用 conda base 系统 Python**

```bash
# 确认 conda base Python 可正常运行
conda activate base
python --version
python -c "import pandas, numpy; print('OK')"
python -m pytest tests/test_stability_tools.py -v
```

ci / 本地开发统一使用 conda base，绕过 `myenv` 的 DLL 问题。

**方案 B：重建 myenv**

```bash
# 删除旧 venv，重新创建
rmdir /s /q myenv
python -m venv myenv
myenv\Scripts\activate
pip install -r requirements.txt
python tools/check_env.py
```

**方案 C：修复 VC++ Redistributable**

下载并安装 [Visual C++ Redistributable 2015-2022 (x64)](https://aka.ms/vs/17/release/vc_redist.x64.exe)，
然后在 myenv 中重装二进制包：

```bash
myenv\Scripts\activate
pip install --force-reinstall numpy pandas scipy
python tools/check_env.py
```

---

## 最小回归集

以下测试文件在每次 nightly CI 中必须通过，确保关键增强不回退：

| 文件 | 覆盖模块 |
|------|----------|
| `tests/test_stability_tools.py` | `stability_30d_report`, `stage1_pipeline`, `governance_strategy_dashboard` |

本地运行：

```bash
python -m pytest tests/test_stability_tools.py -v --tb=short
```

---

## 依赖速查

| 包 | 用途 | 必须 |
|----|------|------|
| `pandas` | 数据处理 | ✅ |
| `numpy` | 数值计算 | ✅ |
| `pytest` | 测试运行 | ✅ |
| `duckdb` | 本地数据库 | 推荐 |
| `scipy` | 统计计算（Calmar/Sortino） | 可选 |
| `matplotlib` | 回测图表 | 可选 |

---

## Stage1 Schema 版本说明

> 详见 `strategies/stage1_pipeline.py: Stage1Result.to_dict()` 中的 `_schema_migration_note`。

| 字段 | v1 | v2 |
|------|----|-----|
| `calmar_ratio` | ❌ 无 | ✅ BacktestMetrics 字段（默认 0.0） |
| `sortino_ratio` | ❌ 无 | ✅ BacktestMetrics 字段（默认 0.0） |
| `benchmark_comparison` | ❌ 无 | ✅ 顶层键（缺失时 `available=False`）|
| `benchmark_source` | ❌ 无 | ✅ BenchmarkComparison 审计字段 |
| `bench_data_range` | ❌ 无 | ✅ BenchmarkComparison 审计字段 |

**Dashboard 读取老 v1 文件时请使用**：

```python
bm = result.get("benchmark_comparison", {"available": False})
```
