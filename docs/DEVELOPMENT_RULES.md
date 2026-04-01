# docs/DEVELOPMENT_RULES 兼容页

> 当前仓库真正的项目级红线与测试边界，已经统一收敛到根目录 [`development_rules.md`](../development_rules.md)。

## 你应该阅读哪个文件

- 主规则文档：[`../development_rules.md`](../development_rules.md)
- 测试隔离配套：[`../tests/conftest.py`](../tests/conftest.py)
- 当前文档治理基线：[`13_docs_governance_matrix.md`](13_docs_governance_matrix.md)

## 为什么这个文件被收敛

旧版 `docs/DEVELOPMENT_RULES.md` 与根目录 `development_rules.md` 长期并存，
但当前真正被仓库指令、CI 和测试规范引用的是**根目录版本**。

为了避免继续出现“同名双规则、口径不一致”的问题，
这里现在仅保留为**兼容入口页**。

## 当前约定

> 如果你要更新项目红线、测试边界、fake market data 禁令或 hermetic 规则，
> 请修改根目录 `development_rules.md`，不要在这里维护第二份正文。
