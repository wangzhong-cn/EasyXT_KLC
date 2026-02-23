# EasyXT 文档中心 - 快速导航

> 📚 一站式文档导航页面

**最后更新**: 2026-02-23  
**文档版本**: v3.0  
**仓库**: https://gitee.com/TradersTV/easy-xt_-klc

---

## 🎯 快速导航

### 第一次使用？

| 文档 | 阅读时间 | 适合人群 |
|------|----------|----------|
| [📖 文档导航](00_README_FIRST.md) | 2 分钟 | 所有用户 |
| [🚀 快速入门](user_manual/quickstart.md) | 5 分钟 | 新用户 |
| [📥 安装指南](user_manual/installation.md) | 10 分钟 | 新用户 |

### 开发者必读

| 文档 | 重要性 | 状态 |
|------|--------|------|
| [🏗️ 架构设计](01_architecture_design.md) | ⭐⭐⭐⭐⭐ | ✅ 已完成 |
| [📦 模块概览](02_modules_overview.md) | ⭐⭐⭐⭐⭐ | ✅ 已完成 |
| [👨‍💻 开发规范](04_development_standards.md) | ⭐⭐⭐⭐⭐ | ✅ 已完成 |
| [🔄 迁移指南](03_migration_guide.md) | ⭐⭐⭐⭐ | ✅ 已完成 |

---

## 📚 文档分类

### 核心文档 ✅

| 编号 | 文档 | 描述 | 状态 |
|------|------|------|------|
| 00 | [文档导航](00_README_FIRST.md) | 文档中心导航 | ✅ 完成 |
| 01 | [架构设计](01_architecture_design.md) | 整体架构和技术路线 | ✅ 完成 |
| 02 | [模块概览](02_modules_overview.md) | 三大核心模块详解 | ✅ 完成 |
| 03 | [迁移指南](03_migration_guide.md) | 从旧版到新版迁移 | ✅ 完成 |
| 04 | [开发规范](04_development_standards.md) | 代码开发和贡献规范 | ✅ 完成 |

### 模块文档 📦

| 模块 | 文档 | 状态 |
|------|------|------|
| **交易管理** | [modules/trading_module.md](modules/trading_module.md) | 📝 规划中 |
| **数据管理** | [modules/data_module.md](modules/data_module.md) | 📝 规划中 |
| **策略管理** | [modules/strategy_module.md](modules/strategy_module.md) | 📝 规划中 |

### 技术文档 🔧

| 文档 | 描述 | 状态 |
|------|------|------|
| [technical/api_reference.md](technical/api_reference.md) | 完整 API 接口文档 | 📝 规划中 |
| [technical/database_design.md](technical/database_design.md) | DuckDB 数据库设计 | 📝 规划中 |
| [technical/ui_design_standards.md](technical/ui_design_standards.md) | 界面设计规范 | 📝 规划中 |
| [technical/performance_optimization.md](technical/performance_optimization.md) | 性能优化指南 | 📝 规划中 |

### 用户手册 📖

| 文档 | 描述 | 状态 |
|------|------|------|
| [user_manual/installation.md](user_manual/installation.md) | 安装和配置指南 | 📝 规划中 |
| [user_manual/quickstart.md](user_manual/quickstart.md) | 5 分钟快速上手 | 📝 规划中 |
| [user_manual/tutorial.md](user_manual/tutorial.md) | 详细使用教程 | 📝 规划中 |
| [user_manual/faq.md](user_manual/faq.md) | FAQ 和问题排查 | 📝 规划中 |

---

## 🗂️ 项目文件结构

```
D:\EasyXT_KLC\
├── 📁 docs/                           # 📚 文档中心
│   ├── 00_README_FIRST.md            # 文档导航
│   ├── 01_architecture_design.md     # 架构设计 ✅
│   ├── 02_modules_overview.md        # 模块概览 ✅
│   ├── 03_migration_guide.md         # 迁移指南 ✅
│   ├── 04_development_standards.md   # 开发规范 ✅
│   ├── modules/                      # 模块文档 📝
│   ├── technical/                    # 技术文档 📝
│   └── user_manual/                  # 用户手册 📝
│
├── 📁 gui_app/                        # 🖥️ GUI 应用（保留）
│   ├── main_window.py                # 主窗口 ✅
│   ├── trading_interface_simple.py   # 交易界面 ✅
│   └── widgets/                      # Widget 组件 ✅
│
├── 📁 strategies/                     # 📈 策略库（保留）
│   ├── grid_trading/                 # 网格策略 ✅
│   ├── trend_following/              # 趋势跟踪 ✅
│   └── ...                           # 其他策略 ✅
│
├── 📁 101 因子/                        # 🔬 因子库（保留）
│   └── 101 因子分析平台/              # 因子分析平台 ✅
│
├── 📁 easy_xt/                        # 🔌 API 封装（保留）
├── 📁 data_manager/                   # 💾 数据管理（保留）
└── 📁 quant_platform/                 # 🆕 新平台（规划中）
```

---

## 🛡️ 保护性原则

### 绝对保护

✅ **所有现有功能 100% 保留**
- `gui_app/` 目录所有文件保留
- `strategies/` 目录所有策略保留
- `101 因子/` 目录所有因子保留
- `easy_xt/` 目录所有 API 保留

✅ **所有操作界面不丢失**
- 主窗口界面保留
- 交易界面保留
- 回测界面保留
- 所有 Widget 组件保留

✅ **所有配置文件向后兼容**
- 现有配置文件继续有效
- 新增配置可选使用
- 新旧配置可以共存

### 渐进式升级

🔄 **封装而非替换** - 新功能封装现有功能  
🔄 **扩展而非修改** - 新界面作为现有界面的增强  
🔄 **并行而非覆盖** - 新旧版本并行运行  
🔄 **可选而非强制** - 用户可选择使用新旧界面  

---

## 📊 文档完成度

### 已完成 ✅

- ✅ 文档导航 (00_README_FIRST.md)
- ✅ 架构设计 (01_architecture_design.md)
- ✅ 模块概览 (02_modules_overview.md)
- ✅ 迁移指南 (03_migration_guide.md)
- ✅ 开发规范 (04_development_standards.md)

### 规划中 📝

- 📝 交易管理模块详细文档
- 📝 数据管理模块详细文档
- 📝 策略管理模块详细文档
- 📝 API 参考文档
- 📝 数据库设计文档
- 📝 UI 设计规范
- 📝 性能优化指南
- 📝 安装指南
- 📝 快速入门
- 📝 使用教程
- 📝 FAQ

---

## 🔗 相关链接

### 仓库链接

- **Gitee**: https://gitee.com/TradersTV/easy-xt_-klc
- **GitHub**: https://github.com/quant-king299/EasyXT

### 社区链接

- **知识星球**: quant-king299
- **官方网站**: ptqmt.com

### 文档链接

- **Gitee 文档**: https://gitee.com/TradersTV/easy-xt_-klc/tree/main/docs
- **GitHub 文档**: https://github.com/quant-king299/EasyXT/tree/main/docs

---

## 📞 支持与反馈

### 获取帮助

1. **查看文档**: 本目录下的完整文档
2. **查看示例**: `examples/` 目录下的示例代码
3. **提交 Issue**: Gitee 仓库 Issue 区
4. **社区讨论**: 知识星球社区

### 反馈建议

欢迎通过以下方式反馈：

- 📧 Email: quant-king299@proton.me
- 💬 知识星球：quant-king299
- 🐛 Issue: Gitee Issue 追踪

---

## 📝 更新日志

### 2026-02-23 - 文档中心上线

✅ 创建核心文档体系
- 文档导航
- 架构设计
- 模块概览
- 迁移指南
- 开发规范

✅ 推送到 Gitee 仓库
- Commit: `docs: 创建核心文档体系`
- Commit: `docs: 添加迁移指南和开发规范`

### 下一步计划

📝 创建模块详细文档  
📝 创建技术文档  
📝 创建用户手册  
📝 创建示例代码  

---

## 🎯 阅读建议

### 新用户

1. 📖 [文档导航](00_README_FIRST.md) - 了解文档结构
2. 📖 [快速入门](user_manual/quickstart.md) - 5 分钟上手
3. 📖 [安装指南](user_manual/installation.md) - 安装配置
4. 📖 [使用教程](user_manual/tutorial.md) - 详细教程

### 开发者

1. 📖 [架构设计](01_architecture_design.md) - 了解整体架构
2. 📖 [模块概览](02_modules_overview.md) - 了解核心模块
3. 📖 [开发规范](04_development_standards.md) - 代码规范
4. 📖 [API 参考](technical/api_reference.md) - 接口文档

### 老用户迁移

1. 📖 [迁移指南](03_migration_guide.md) - 迁移步骤
2. 📖 [模块概览](02_modules_overview.md) - 新功能说明
3. 📖 [常见问题](user_manual/faq.md) - 问题排查

---

**EasyXT 量化交易平台**  
*让量化交易更简单，让策略开发更高效*

---

**最后更新**: 2026-02-23  
**维护者**: EasyXT 团队  
**联系方式**: quant-king299@proton.me
