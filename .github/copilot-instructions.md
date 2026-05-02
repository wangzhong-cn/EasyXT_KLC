# EasyXT_KLC 工作区指引

## 先看这些入口文档

- 文档导航：`docs/00_README_FIRST.md`
- 架构总览：`docs/01_architecture_design.md`
- 开发规范：`docs/04_development_standards.md`
- GUI 线程/退出安全：`docs/05_thread_exit_safety_spec.md`
- 项目级红线与测试边界：`development_rules.md`
- 环境变量参考：`config/ENV_REFERENCE.md`
- Windows / DLL / 环境问题：`ENV_HEALTHCHECK.md`

## 代码风格

- 这是一个以 Python 为主的 Windows 量化项目；默认遵循 PEP 8、4 空格缩进、每行 100 字符、标准库/第三方/本地模块分组导入。
- 新增或修改的 Python 代码优先补充类型注解和简短文档字符串；参考 `docs/04_development_standards.md` 与 `mypy.ini`。
- 静态检查以 `ruff`、`mypy`、`bandit` 和 pre-commit 规则为准；相关配置见 `pyproject.toml`、`.pre-commit-config.yaml`、`mypy.ini`。
- 优先复用现有实现路径，不要为同一职责再造一套平行模块。

## 架构与目录边界

- `core/`：基础设施、导入/线程/事件总线等全局约束。涉及 xtquant/xtdata 时，先看 `core/xtdata_lock.py` 和 `core/xtquant_import.py`。
- `data_manager/`：统一数据接口、DuckDB、补数、数据治理。数据口径优先沿用 `data_manager/unified_data_interface.py` 现有链路。
- `easy_xt/`：交易与数据 API 封装。
- `gui_app/`：PyQt5 GUI。涉及线程、图表、关闭流程、实时连接时，以 `gui_app/widgets/kline_chart_workspace.py` 为参考实现。
- `easyxt_backtest/`：回测引擎、策略运行与绩效分析。
- `strategies/`：策略目录；优先新增而不是重写已有策略。
- `external/`、`xtquant_backup/`、`myenv/`：默认视为外部/环境目录，除非任务明确要求，否则不要修改。

## 构建与测试

- Windows 下优先使用 conda 环境运行 Python 和 pytest；直接调用 `myenv\Scripts\python.exe` 可能触发 DLL 问题。先参考 `ENV_HEALTHCHECK.md`。
- 默认测试遵循 `pytest.ini`：
  - 常规回归优先使用：`conda run --no-capture-output -n myenv python -m pytest tests/ -q --tb=short -m "not slow and not gui and not integration"`
  - 仅在明确需要外部系统时才运行 `@pytest.mark.integration`，并显式加 `--run-integration`
- 修改测试前先看 `tests/conftest.py`：测试会话默认关闭自动 checkpoint、QMT 在线模式和 WS 行情，并清理线程/环境污染。
- 涉及 DuckDB、QMT、GUI 线程或实时链路的修改，优先补充针对性回归测试，而不是只跑无关子集。

## 项目特有约定

- **严禁伪造市场数据。** 不要在测试或工具代码中硬编码 OHLCV、成交量、收益率等业务数据。涉及行情/业务数据的测试请遵守 `development_rules.md`，优先使用 `tests/fixtures/real_market_data.py` 或真实空数据场景。
- **所有 xtdata 调用都必须走统一锁/执行器。** 新增 xtquant/xtdata 访问时，复用 `core/xtdata_lock.py` 暴露的共享机制，不要自建独立锁。
- **xtquant 导入要走静默包装。** 需要提前导入 xtquant 时，优先复用 `core/xtquant_import.py`，避免导入期噪音和已知 warning 污染输出。
- **GUI/QThread 改动必须遵守线程退出安全规范。** 修改 `gui_app/` 中的线程、关闭流程、实时连接逻辑前，先读 `docs/05_thread_exit_safety_spec.md`；测试环境必须保留 `test_mode` / `PYTEST_CURRENT_TEST` 守卫。
- **保持 helper 模块默认静默。** 对 importer / detector / runtime helper 类模块，不要重新引入无条件 `print(...)`；优先沿用“logger + `verbose=True` / 环境变量显式开启 stdout”的现有模式。
- **改动环境变量、数据回退或可靠性逻辑前先查文档。** 优先参考 `config/ENV_REFERENCE.md`，避免引入与现有 `EASYXT_*` 开关冲突的新行为。
- **保持向后兼容。** 这个仓库大量文档都强调“扩展而非替换”；对 GUI、策略、配置和数据路径的改动，优先兼容旧行为。

## 什么时候用仓库内技能

- 回测、策略验证、绩效分析：`backtest-workflow`
- K 线图黑屏、图表加载失败、后端桥接异常：`kline-chart-debug`
- xtquant 崩溃、DLL 缺失、`bsonobj.cpp` / 并发问题：`xtquant-crash-debug`
- helper 模块 stdout 噪音治理：`stdout-noise-cleanup`
- 跑测试、分析覆盖率、看回归子集：`test-coverage`
