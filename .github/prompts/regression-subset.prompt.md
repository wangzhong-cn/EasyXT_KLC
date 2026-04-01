---
description: "Run the smallest useful regression subset for a changed area in EasyXT_KLC. Use when validating data_manager, gui_app, easy_xt, backtest, xtquant, DuckDB, or chart-related changes without jumping straight to the full suite."
name: "EasyXT Regression Subset"
argument-hint: "描述改动范围，例如：data_manager/unified_data_interface.py、gui_app/main_window.py、DuckDB checkpoint、K线图 backend"
agent: "agent"
---
根据我给出的改动范围、文件路径或问题描述，为 `EasyXT_KLC` 执行**最小但有代表性的回归验证**。

执行要求：

1. 先识别改动涉及的模块（如 `data_manager`、`gui_app`、`easy_xt`、`easyxt_backtest`、图表/xtquant/DuckDB）。
2. 优先选择与改动最相关的现有测试文件、现成 VS Code task 或最小 pytest 子集；不要一上来跑全量测试。
3. 遵守工作区与测试指令：
   - 参考 [工作区指引](../copilot-instructions.md)
   - 参考 [Hermetic 测试指引](../instructions/tests-hermetic.instructions.md)
4. Windows 下优先使用 conda 方式运行测试；默认排除 `slow`、`gui`、`integration`，除非我明确要求。
5. 如果仓库中已有高度匹配的 task，优先运行 task；否则再构造最小 pytest 命令。
6. 运行后给出简洁结论：
   - 运行了哪些测试/任务
   - 通过/失败情况
   - 如果失败，指出最可能与本次改动相关的首要失败点
   - 建议的下一步验证（如是否需要扩大到更大回归子集或全量）
7. 如果改动涉及以下高风险区域，额外提醒对应专项风险：
   - `xtquant` / `xtdata` / 崩溃：并发、锁、DLL、线程亲和性
   - `gui_app` / `QThread`：`test_mode` 守卫、`closeEvent`、线程退出安全
   - `data_manager` / DuckDB：checkpoint、只读连接、环境变量门控

输出格式尽量简洁，优先给出“本次最值得跑的验证”和“下一步是否要扩大范围”。
