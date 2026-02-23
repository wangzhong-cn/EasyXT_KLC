# EasyXT 文档中心

> 📚 完整的量化交易平台文档体系

**最后更新**: 2026-02-23  
**版本**: v3.0 (重构规划版)  
**仓库**: https://gitee.com/TradersTV/easy-xt_-klc

---

## 📖 文档导航

### 🎯 快速开始

| 文档 | 描述 | 适合人群 |
|------|------|----------|
| [架构设计](01_architecture_design.md) | 整体架构和技术路线 | 开发者、架构师 |
| [模块说明](02_modules_overview.md) | 三大核心模块详解 | 所有用户 |
| [迁移指南](03_migration_guide.md) | 从旧版到新版迁移 | 老用户 |
| [开发规范](04_development_standards.md) | 代码开发和贡献规范 | 开发者 |

### 📦 核心模块文档

| 模块 | 文档 | 状态 |
|------|------|------|
| **交易管理模块** | [交易管理文档](modules/trading_module.md) | ✅ 规划完成 |
| **数据管理模块** | [数据管理文档](modules/data_module.md) | ✅ 规划完成 |
| **策略管理模块** | [策略管理文档](modules/strategy_module.md) | ✅ 规划完成 |

### 🔧 技术文档

| 文档 | 描述 |
|------|------|
| [API 参考](technical/api_reference.md) | 完整 API 接口文档 |
| [数据库设计](technical/database_design.md) | DuckDB 数据库设计 |
| [UI 设计规范](technical/ui_design_standards.md) | 界面设计规范 |
| [性能优化指南](technical/performance_optimization.md) | 性能优化最佳实践 |

### 📚 用户手册

| 文档 | 描述 |
|------|------|
| [安装指南](user_manual/installation.md) | 安装和配置指南 |
| [快速入门](user_manual/quickstart.md) | 5 分钟快速上手 |
| [使用教程](user_manual/tutorial.md) | 详细使用教程 |
| [常见问题](user_manual/faq.md) | FAQ 和问题排查 |

---

## 🗂️ 项目结构说明

```
D:\EasyXT_KLC\
├── docs/                           # 📚 文档中心（本目录）
│   ├── 00_README_FIRST.md         # 文档导航（本文件）
│   ├── 01_architecture_design.md  # 架构设计
│   ├── 02_modules_overview.md     # 模块概览
│   ├── 03_migration_guide.md      # 迁移指南
│   ├── 04_development_standards.md # 开发规范
│   ├── modules/                   # 模块文档
│   ├── technical/                 # 技术文档
│   └── user_manual/               # 用户手册
│
├── gui_app/                        # 🖥️ GUI 应用（保留）
│   ├── main_window.py             # 主窗口（保留）
│   ├── trading_interface_simple.py # 交易界面（保留）
│   └── widgets/                   # Widget 组件（保留）
│
├── strategies/                     # 📈 策略库（保留）
│   ├── grid_trading/              # 网格策略
│   ├── trend_following/           # 趋势跟踪
│   ├── conditional_orders/        # 条件单
│   └── ...                        # 其他策略
│
├── 101 因子/                        # 🔬 因子库（保留）
│   └── 101 因子分析平台/            # 因子分析平台
│
├── easy_xt/                        # 🔌 API 封装（保留）
├── data_manager/                   # 💾 数据管理（保留）
└── quant_platform/                 # 🆕 新平台（规划中）
    ├── modules/                   # 模块实现
    ├── integrations/              # 集成层
    └── config/                    # 配置
```

---

## 🛡️ 保护性原则

本次重构遵循严格的保护性原则：

### ✅ 绝对保护

1. **所有现有功能 100% 保留**
   - ✅ `gui_app/` 目录所有文件保留
   - ✅ `strategies/` 目录所有策略保留
   - ✅ `101 因子/` 目录所有因子保留
   - ✅ `easy_xt/` 目录所有 API 保留

2. **所有操作界面不丢失**
   - ✅ 主窗口界面保留
   - ✅ 交易界面保留
   - ✅ 回测界面保留
   - ✅ 所有 Widget 组件保留

3. **所有配置文件向后兼容**
   - ✅ 现有配置文件继续有效
   - ✅ 新增配置可选使用
   - ✅ 新旧配置可以共存

### 🔄 渐进式升级

1. **封装而非替换** - 新功能封装现有功能
2. **扩展而非修改** - 新界面作为现有界面的增强
3. **并行而非覆盖** - 新旧版本并行运行
4. **可选而非强制** - 用户可选择使用新旧界面

---

## 📋 版本说明

### v3.0 (重构规划版) - 当前版本

**核心改进**:
- 🆕 新增三大核心模块（交易/数据/策略）
- 🆕 图表交易一体化设计
- 🆕 DuckDB 数据库深度集成
- 🆕 因子库统一管理
- 🛡️ 保护性重构，所有现有功能保留

**兼容性**:
- ✅ 完全兼容 v2.x 所有功能
- ✅ 新旧版本可以并行运行
- ✅ 配置文件向后兼容

### v2.x (经典版) - 稳定版本

**核心功能**:
- ✅ GUI 交易界面
- ✅ 策略回测框架
- ✅ 网格交易策略
- ✅ JQ2QMT 集成
- ✅ 雪球跟单策略

**状态**: 持续维护，Bug 修复

---

## 🔗 相关链接

- **Gitee 仓库**: https://gitee.com/TradersTV/easy-xt_-klc
- **GitHub 仓库**: https://github.com/quant-king299/EasyXT
- **知识星球**: quant-king299
- **官方网站**: ptqmt.com

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

### 2026-02-23
- ✅ 创建完整文档体系
- ✅ 完成架构设计文档
- ✅ 完成三大模块文档
- ✅ 完成迁移指南
- 🔄 开始代码重构

### 2026-02-20
- ✅ 确定保护性重构原则
- ✅ 完成技术路线规划
- ✅ Git 仓库配置完成

---

**EasyXT 量化交易平台**  
*让量化交易更简单，让策略开发更高效*

---

## 📚 文档阅读顺序建议

### 第一次使用

1. 📖 [快速入门](user_manual/quickstart.md) - 5 分钟上手
2. 📖 [安装指南](user_manual/installation.md) - 安装和配置
3. 📖 [使用教程](user_manual/tutorial.md) - 详细教程

### 开发者

1. 📖 [架构设计](01_architecture_design.md) - 了解整体架构
2. 📖 [开发规范](04_development_standards.md) - 代码规范
3. 📖 [API 参考](technical/api_reference.md) - 接口文档

### 老用户迁移

1. 📖 [迁移指南](03_migration_guide.md) - 迁移步骤
2. 📖 [模块说明](02_modules_overview.md) - 新功能说明
3. 📖 [常见问题](user_manual/faq.md) - 问题排查

---

**开始阅读**: [→ 架构设计文档](01_architecture_design.md)
