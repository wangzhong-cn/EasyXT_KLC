# EasyXT 迁移指南

> 🔄 从旧版本迁移到新版本的完整指南

**版本**: v3.0
**最后更新**: 2026-02-23
**难度**: ⭐⭐☆☆☆ (简单)

---

## 📋 目录

1. [迁移前准备](#迁移前准备)
2. [迁移步骤](#迁移步骤)
3. [新功能使用](#新功能使用)
4. [常见问题](#常见问题)
5. [回滚方案](#回滚方案)

---

## 迁移前准备

> ⚠️ **重要提醒**：本文保留了部分“经典 GUI → 新模块”的旧迁移说明，
> 但从 2026-03-31 起，仓库当前默认路线已经切换为：
> **Tauri 增量替换 + SQLite(WAL) 主写状态 + DuckDB 只读影子**。
> 如果你只打算跟进当前主线，请优先看下方“2026-03 迁移路线校正”，不要直接照搬本文后半段旧路径。

## 2026-03 迁移路线校正

> 本文档原先偏向“经典 GUI → 新模块”的迁移说明。自 2026-03-31 起，当前默认迁移路线调整为：

- **前端**：Tauri 增量替换
- **主写状态**：SQLite (WAL)
- **只读分析**：DuckDB
- **零运维扩展**：SQLite 分片 + DuckDB 多文件联邦查询

在执行任何新迁移前，建议优先阅读：

1. [Tauri 增量替换蓝图](05_tauri_incremental_replacement_blueprint.md)
2. [双引擎状态契约](06_dual_engine_state_contract.md)
3. [gui_app Legacy Freeze](08_gui_app_legacy_freeze.md)

### 当前推荐迁移顺序

1. 冻结对 PyQt 深层交互的继续扩张
2. 建立 Tauri P0 可观察前端（结构列表 / 详情 / Bayesian / 同步状态）
3. 把主写状态收敛到 SQLite WAL
4. 把 DuckDB 收敛为只读影子与分析视图
5. 通过应用层分片继续扩展，不引入外部数据库服务

### ✅ 检查清单

在开始迁移前，请确认：

- [ ] **Git 仓库已同步**
  ```bash
  cd D:\EasyXT_KLC
  git pull origin main
  ```

- [ ] **现有功能正常工作**
  ```bash
  # 测试旧版主窗口
  python gui_app/main_window.py

  # 测试交易界面
  python gui_app/trading_interface_simple.py
  ```

- [ ] **配置文件已备份**
  ```bash
  # 导出当前配置
  copy config\unified_config.json config\unified_config.json.backup
  ```

- [ ] **阅读相关文档**
  - ✅ [架构设计文档](01_architecture_design.md)
  - ✅ [模块概览文档](02_modules_overview.md)

### 📦 环境要求

| 组件 | 版本要求 | 检查方式 |
|------|----------|----------|
| **Python** | >= 3.9 | `python --version` |
| **PyQt5** | >= 5.15 | `pip show PyQt5` |
| **Git** | >= 2.0 | `git --version` |
| **DuckDB** | >= 0.9.0 | `pip show duckdb` (新增) |

---

## 迁移步骤

### 步骤 1: 同步 Git 仓库

```bash
cd D:\EasyXT_KLC
git pull origin main
```

**说明**:
- ✅ Git 已提供完整的版本控制
- ✅ 所有历史版本都可以追溯
- ✅ 无需额外的本地备份

### 步骤 2: 验证现有功能

```bash
# 1. 测试主窗口
python gui_app/main_window.py

# 2. 测试交易界面
python gui_app/trading_interface_simple.py

# 3. 测试回测功能
# (通过主窗口访问)
```

**预期结果**:
- ✅ 所有现有功能正常工作
- ✅ 界面无异常
- ✅ 数据加载正常

### 步骤 3: 安装新增依赖（可选）

```bash
# 新增模块的依赖（如使用新功能）
pip install duckdb>=0.9.0
pip install pandas>=1.5.0
pip install numpy>=1.23.0
```

**说明**:
- ⚠️ 仅在使用新功能时需要
- ✅ 旧功能不需要新增依赖

### 步骤 4: 探索新功能（可选）

```bash
# 访问新功能文档
start docs\02_modules_overview.md

# 查看模块示例
start docs\modules\
```

---

## 新功能使用

### 交易管理模块

#### 快速下单

```python
# 方式 1: 使用经典交易界面（保留）
python gui_app/trading_interface_simple.py

# 方式 2: 使用新交易管理面板（新增，可选）
# (通过新平台入口访问)
python quant_platform/main.py
```

#### 图表点击下单

1. 打开图表工作台
2. 点击图表上的价格
3. 自动填充到下单面板
4. 确认下单

### 数据管理模块

#### DuckDB 数据库管理

```python
# 访问数据管理面板
# (通过新平台入口访问)
python quant_platform/main.py

# 选择"数据管理"标签页
# 管理 DuckDB 数据库
```

#### 数据下载

1. 打开"数据管理"面板
2. 选择"数据下载"标签
3. 配置下载参数
4. 开始下载

### 策略管理模块

#### 策略库管理

```python
# 所有现有策略保持不变
# strategies/ 目录原位置保留

# 访问策略管理面板
python quant_platform/main.py

# 选择"策略管理"标签页
# 浏览策略库
```

#### 回测分析

1. 打开"策略管理"面板
2. 选择策略
3. 配置参数
4. 运行回测
5. 查看结果

---

## 常见问题

### Q1: 迁移后旧功能还能用吗？

**A**: ✅ **完全可以！**

所有现有功能都 100% 保留：
- ✅ `gui_app/main_window.py` - 保留
- ✅ `gui_app/trading_interface_simple.py` - 保留
- ✅ `gui_app/widgets/` - 所有 Widget 保留
- ✅ `strategies/` - 所有策略保留
- ✅ `101 因子/` - 所有因子保留

### Q2: 如何回滚到旧版本？

**A**: 使用 Git 轻松回滚：

```bash
# 查看提交历史
git log --oneline

# 回滚到指定版本
git checkout <commit-hash>

# 或者重置到某个版本
git reset --hard <commit-hash>
```

### Q3: 新旧版本可以同时使用吗？

**A**: ✅ **可以！**

新旧版本是并行的：
- 旧版本：`python gui_app/main_window.py`
- 新版本：`python quant_platform/main.py` (未来)

可以同时运行，互不影响。

### Q4: 配置文件需要修改吗？

**A**: ❌ **不需要！**

所有配置文件向后兼容：
- ✅ `config/unified_config.json` - 继续有效
- ✅ 现有配置保持不变
- ✅ 新增配置可选使用

### Q5: 数据会丢失吗？

**A**: ❌ **不会！**

数据安全保护：
- ✅ 所有数据文件原位置保留
- ✅ DuckDB 是新增功能，不影响现有数据
- ✅ Git 版本控制，可追溯

### Q6: 迁移需要多长时间？

**A**: ⏱️ **5-10 分钟**

- 同步 Git: 1-2 分钟
- 验证功能：3-5 分钟
- 安装依赖（可选）: 2-3 分钟
- 探索新功能：自行决定

---

## 回滚方案

### 使用 Git 回滚

#### 查看提交历史

```bash
git log --oneline
```

**示例输出**:
```
d08c7b8 docs: 创建核心文档体系
59b51aa feat: 添加新功能
abc1234 fix: 修复 Bug
...
```

#### 回滚到指定版本

```bash
# 回滚到某个提交
git checkout <commit-hash>

# 例如回滚到创建文档前
git checkout 59b51aa
```

#### 重置当前分支

```bash
# ⚠️ 警告：这会丢弃当前更改
git reset --hard <commit-hash>
```

### 恢复配置文件

```bash
# 如果修改了配置文件，可以恢复
git checkout config/unified_config.json
```

### 恢复代码文件

```bash
# 恢复单个文件
git checkout <file-path>

# 恢复整个目录
git checkout gui_app/
```

---

## 迁移检查清单

### ✅ 迁移前

- [ ] Git 仓库已同步
- [ ] 现有功能测试通过
- [ ] 配置文件已备份
- [ ] 已阅读相关文档

### ✅ 迁移后

- [ ] 旧功能正常工作
- [ ] 新功能可以访问
- [ ] 配置文件有效
- [ ] 数据完整

### ✅ 可选步骤

- [ ] 安装新增依赖
- [ ] 测试新功能
- [ ] 阅读详细文档
- [ ] 参与社区讨论

---

## 获取帮助

### 文档资源

- 📖 [架构设计文档](01_architecture_design.md)
- 📖 [模块概览文档](02_modules_overview.md)
- 📖 [开发规范文档](04_development_standards.md)

### 社区支持

- 💬 知识星球：quant-king299
- 🐛 Issue: Gitee Issue 追踪
- 📧 Email: quant-king299@proton.me

### 反馈建议

欢迎通过以下方式反馈：

1. **提交 Issue**: Gitee 仓库 Issue 区
2. **社区讨论**: 知识星球社区
3. **邮件联系**: quant-king299@proton.me

---

## 总结

### 迁移要点

✅ **Git 同步** - 无需本地备份
✅ **旧功能保留** - 100% 保留
✅ **新旧并行** - 可自由选择
✅ **配置兼容** - 向后兼容
✅ **数据安全** - Git 版本控制

### 迁移时间

⏱️ **总计**: 5-10 分钟
- 同步 Git: 1-2 分钟
- 验证功能：3-5 分钟
- 可选步骤：自行决定

### 下一步

1. 📖 阅读 [模块概览文档](02_modules_overview.md)
2. 🎯 开始使用新功能
3. 💬 参与社区讨论
4. 📝 反馈使用体验

---

**EasyXT 量化交易平台**
*让量化交易更简单，让策略开发更高效*

---

## 附录：Git 常用命令

### 查看状态

```bash
git status
```

### 查看历史

```bash
git log --oneline
```

### 拉取最新代码

```bash
git pull origin main
```

### 推送本地更改

```bash
git add .
git commit -m "描述"
git push origin main
```

### 回滚操作

```bash
# 撤销未提交的更改
git checkout <file>

# 撤销最近的提交
git reset --soft HEAD~1

# 硬重置（⚠️ 会丢弃更改）
git reset --hard <commit>
```
