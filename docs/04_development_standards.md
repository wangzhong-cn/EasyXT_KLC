# EasyXT 开发规范

> 👨‍💻 代码开发和贡献的完整规范

**版本**: v3.0  
**最后更新**: 2026-02-23  
**状态**: ✅ 规划完成

---

## 📋 目录

1. [代码规范](#代码规范)
2. [项目结构](#项目结构)
3. [Git 工作流](#git-工作流)
4. [文档规范](#文档规范)
5. [测试规范](#测试规范)
6. [贡献指南](#贡献指南)

---

## 代码规范

### Python 编码规范

#### 基本原则

遵循 **PEP 8** Python 编码规范：

- ✅ 使用 4 个空格缩进
- ✅ 每行不超过 100 个字符
- ✅ 导入按顺序：标准库 → 第三方 → 本地模块
- ✅ 函数和类使用文档字符串

#### 命名规范

```python
# 类名：大驼峰命名法 (PascalCase)
class TradingModule:
    pass

# 函数和变量：小写 + 下划线 (snake_case)
def get_stock_data():
    pass

stock_code = "000001.SZ"

# 常量：全大写 + 下划线
MAX_RETRY_COUNT = 3
DEFAULT_TIMEOUT = 30

# 私有变量：单下划线前缀
_internal_counter = 0

# 模块级别私有：双下划线前缀
__module_private = 0
```

#### 类型注解

```python
from typing import Optional, List, Dict, Any

# 函数参数和返回值使用类型注解
def get_price(
    symbol: str,
    period: str = "1d",
    count: Optional[int] = None
) -> pd.DataFrame:
    """获取股票价格数据"""
    pass

# 类属性类型注解
class TradingModule:
    symbol: str
    price: float
    orders: List[Dict[str, Any]]
```

#### 文档字符串

```python
def calculate_sma(data: pd.DataFrame, period: int = 20) -> pd.Series:
    """
    计算简单移动平均线 (SMA)
    
    Args:
        data: 包含收盘价的数据框，需要有 'close' 列
        period: SMA 周期，默认 20 天
    
    Returns:
        pd.Series: SMA 序列
    
    Raises:
        ValueError: 如果数据框不包含 'close' 列
    
    Example:
        >>> df = pd.DataFrame({'close': [1, 2, 3, 4, 5]})
        >>> sma = calculate_sma(df, period=3)
        >>> print(sma)
    """
    if 'close' not in data.columns:
        raise ValueError("数据框必须包含 'close' 列")
    
    return data['close'].rolling(window=period).mean()
```

### 保护性原则

#### ⚠️ 不可违背的规则

```python
"""
保护性原则 - 所有开发者必须遵守

1. 不删除现有文件
   ❌ 禁止：删除 gui_app/ 中的任何文件
   ✅ 允许：在 enhanced/ 目录下新增文件

2. 不修改现有策略
   ❌ 禁止：修改 strategies/ 中的策略文件
   ✅ 允许：在 strategies/ 下创建新的策略子目录

3. 不移动因子库文件
   ❌ 禁止：移动 101 因子/ 中的任何文件
   ✅ 允许：在 quant_platform/ 中创建因子管理器

4. 保持向后兼容
   ❌ 禁止：修改现有配置文件结构
   ✅ 允许：新增可选配置项
"""
```

#### 代码审查清单

在提交代码前，请确认：

- [ ] 遵循 PEP 8 规范
- [ ] 添加了类型注解
- [ ] 编写了文档字符串
- [ ] 通过了现有测试
- [ ] 添加了新功能的测试
- [ ] 更新了相关文档
- [ ] 没有删除现有功能
- [ ] 保持了向后兼容

---

## 项目结构

### 目录组织

```
D:\EasyXT_KLC\
├── 📁 docs/                           # 文档中心
│   ├── 00_README_FIRST.md            # 文档导航
│   ├── 01_architecture_design.md     # 架构设计
│   ├── 02_modules_overview.md        # 模块概览
│   ├── 03_migration_guide.md         # 迁移指南
│   ├── 04_development_standards.md   # 开发规范（本文件）
│   ├── modules/                      # 模块文档
│   ├── technical/                    # 技术文档
│   └── user_manual/                  # 用户手册
│
├── 📁 gui_app/                        # GUI 应用（保留）
│   ├── main_window.py                # 主窗口（保留）
│   ├── trading_interface_simple.py   # 交易界面（保留）
│   ├── widgets/                      # Widget 组件（保留）
│   └── enhanced/                     # 增强版（新增）
│       ├── workspace/                # 新工作区
│       ├── operation_panel/          # 操作面板
│       └── adapters/                 # 适配器
│
├── 📁 strategies/                     # 策略库（保留）
│   ├── grid_trading/                 # 网格策略
│   ├── trend_following/              # 趋势跟踪
│   └── ...                           # 其他策略
│
├── 📁 101 因子/                        # 因子库（保留）
│   └── 101 因子分析平台/              # 因子分析平台
│
├── 📁 quant_platform/                 # 新平台（新增）
│   ├── main.py                       # 统一入口
│   ├── modules/                      # 模块实现
│   ├── integrations/                 # 集成层
│   └── config/                       # 配置
│
├── 📁 easy_xt/                        # API 封装（保留）
├── 📁 data_manager/                   # 数据管理（保留）
└── 📁 core/                           # 核心配置（保留）
```

### 文件命名规范

#### Python 文件

```
✅ 正确:
- trading_module.py       # 小写 + 下划线
- chart_panel.py
- data_manager.py

❌ 错误:
- TradingModule.py        # 不要使用大驼峰
- chart-panel.py          # 不要使用连字符
- chartPanel.py           # 不要使用驼峰
```

#### 文档文件

```
✅ 正确:
- 00_README_FIRST.md      # 编号 + 大写 + 下划线
- 01_architecture_design.md
- modules_overview.md

❌ 错误:
- readme.md               # 不要全小写
- ArchitectureDesign.md   # 不要大驼峰
```

#### 测试文件

```
✅ 正确:
- test_trading_module.py  # test_前缀
- test_data_manager.py

❌ 错误:
- trading_module_test.py  # 不要用_test 后缀
```

---

## Git 工作流

### 分支策略

```
main (主分支)
├── develop (开发分支)
│   ├── feature/trading-module (功能分支)
│   ├── feature/data-module (功能分支)
│   └── feature/strategy-module (功能分支)
├── bugfix/fix-login-issue (Bug 修复)
└── hotfix/critical-fix (紧急修复)
```

### 提交规范

#### Commit Message 格式

```
<type>(<scope>): <subject>

<body>

<footer>
```

#### Type 类型

| 类型 | 说明 | 示例 |
|------|------|------|
| **feat** | 新功能 | `feat(trading): 添加图表点击下单` |
| **fix** | Bug 修复 | `fix(data): 修复数据加载 Bug` |
| **docs** | 文档更新 | `docs: 添加架构设计文档` |
| **style** | 代码格式 | `style: 格式化代码` |
| **refactor** | 重构 | `refactor: 重构交易模块` |
| **test** | 测试 | `test: 添加单元测试` |
| **chore** | 构建/工具 | `chore: 更新依赖` |

#### 提交示例

```bash
# 新功能
git commit -m "feat(trading): 添加图表点击下单功能

- 实现图表价格点击获取
- 自动填充到下单面板
- 支持快速确认下单

Closes #123"

# Bug 修复
git commit -m "fix(data): 修复数据加载超时问题

- 增加超时重试机制
- 优化数据缓存

Fixes #456"

# 文档更新
git commit -m "docs: 添加核心文档体系

- 创建架构设计文档
- 创建模块概览文档
- 创建迁移指南"
```

### 推送规范

```bash
# 1. 拉取最新代码
git pull origin main

# 2. 创建功能分支
git checkout -b feature/trading-module

# 3. 开发并提交
git add .
git commit -m "feat(trading): 添加交易模块"

# 4. 推送到远程
git push origin feature/trading-module

# 5. 创建 Pull Request
# (在 Gitee 上创建)
```

---

## 文档规范

### Markdown 格式

#### 标题层级

```markdown
# 一级标题 (文档标题)
## 二级标题 (主要章节)
### 三级标题 (子章节)
#### 四级标题 (详细内容)
```

#### 代码块

````markdown
```python
# Python 代码示例
def hello():
    print("Hello, EasyXT!")
```

```bash
# Shell 命令
git pull origin main
```
````

#### 表格

```markdown
| 模块 | 功能 | 状态 |
|------|------|------|
| 交易管理 | 交易下单 | ✅ 完成 |
| 数据管理 | 数据下载 | 🆕 新增 |
| 策略管理 | 回测分析 | ✅ 完成 |
```

### 文档结构

每个文档应包含：

1. **标题和元信息**
   ```markdown
   # 文档标题
   
   > 简短描述
   
   **版本**: v3.0  
   **最后更新**: 2026-02-23
   ```

2. **目录**
   ```markdown
   ## 📋 目录
   1. [章节 1](#章节 1)
   2. [章节 2](#章节 2)
   ```

3. **正文内容**
   - 使用清晰的标题
   - 包含代码示例
   - 使用表格整理信息
   - 添加必要的图表

4. **总结**
   ```markdown
   ## 总结
   
   ### 关键点
   - 要点 1
   - 要点 2
   
   ### 下一步
   1. 步骤 1
   2. 步骤 2
   ```

---

## 测试规范

### 单元测试

#### 测试文件结构

```python
"""
test_trading_module.py
交易模块单元测试
"""
import unittest
from unittest.mock import Mock, patch
from gui_app.enhanced.operation_panel.trade_management import TradingModule


class TestTradingModule(unittest.TestCase):
    """交易模块测试类"""
    
    def setUp(self):
        """测试前准备"""
        self.trading_module = TradingModule()
    
    def test_buy_order(self):
        """测试买入订单"""
        order_data = {
            'symbol': '000001.SZ',
            'side': 'buy',
            'price': 10.50,
            'volume': 100
        }
        result = self.trading_module.submit_order(order_data)
        self.assertTrue(result)
    
    def test_sell_order(self):
        """测试卖出订单"""
        order_data = {
            'symbol': '000001.SZ',
            'side': 'sell',
            'price': 10.50,
            'volume': 100
        }
        result = self.trading_module.submit_order(order_data)
        self.assertTrue(result)
    
    def test_invalid_symbol(self):
        """测试无效股票代码"""
        order_data = {
            'symbol': 'INVALID',
            'side': 'buy',
            'price': 10.50,
            'volume': 100
        }
        with self.assertRaises(ValueError):
            self.trading_module.submit_order(order_data)


if __name__ == '__main__':
    unittest.main()
```

### 集成测试

```python
"""
test_integration.py
集成测试
"""
import unittest
from easy_xt import get_api
from gui_app.widgets.kline_chart_workspace import KLineChartWorkspace


class TestEasyXTIntegration(unittest.TestCase):
    """EasyXT 集成测试"""
    
    def test_api_connection(self):
        """测试 API 连接"""
        api = get_api()
        api.init_data()
        self.assertTrue(api._data_connected)
    
    def test_get_price(self):
        """测试获取价格数据"""
        api = get_api()
        data = api.get_price('000001.SZ', count=100)
        self.assertFalse(data.empty)
    
    def test_chart_workspace(self):
        """测试图表工作台"""
        workspace = KLineChartWorkspace()
        self.assertIsNotNone(workspace.chart)


if __name__ == '__main__':
    unittest.main()
```

### 测试覆盖率

目标测试覆盖率：

| 模块 | 目标覆盖率 | 当前覆盖率 |
|------|-----------|-----------|
| **交易管理** | > 80% | 待测试 |
| **数据管理** | > 80% | 待测试 |
| **策略管理** | > 80% | 待测试 |
| **核心 API** | > 90% | 待测试 |

---

## 贡献指南

### 贡献流程

1. **Fork 项目**
   - 在 Gitee 上 Fork 项目

2. **创建分支**
   ```bash
   git checkout -b feature/my-feature
   ```

3. **开发功能**
   - 遵循代码规范
   - 编写测试
   - 更新文档

4. **提交代码**
   ```bash
   git add .
   git commit -m "feat: 添加新功能"
   git push origin feature/my-feature
   ```

5. **创建 Pull Request**
   - 在 Gitee 上创建 PR
   - 描述功能改进
   - 等待代码审查

### 代码审查标准

审查员会检查：

- ✅ 代码质量
  - 遵循 PEP 8
  - 类型注解完整
  - 文档字符串清晰

- ✅ 功能完整性
  - 功能按预期工作
  - 测试覆盖充分
  - 没有破坏现有功能

- ✅ 文档完整性
  - 更新了相关文档
  - 添加了使用示例
  - 更新了 API 文档

### 贡献者权益

- ✅ 在 README.md 中列出贡献者
- ✅ 获得社区认可
- ✅ 参与项目决策
- ✅ 获取最新功能优先体验权

---

## 安全规范

### 代码安全

```python
# ✅ 安全做法
def execute_order(order_data):
    """执行订单前进行风控检查"""
    if not risk_check(order_data):
        raise ValueError("风控检查失败")
    return _execute(order_data)

# ❌ 危险做法
def execute_order(order_data):
    """直接执行订单，无检查"""
    return _execute(order_data)
```

### 数据安全

```python
# ✅ 安全做法
def save_config(config):
    """保存配置时加密敏感信息"""
    encrypted = encrypt_sensitive_data(config)
    with open('config.json', 'w') as f:
        json.dump(encrypted, f)

# ❌ 危险做法
def save_config(config):
    """明文保存配置"""
    with open('config.json', 'w') as f:
        json.dump(config, f)
```

---

## 总结

### 核心规范

✅ **代码规范** - 遵循 PEP 8，使用类型注解  
✅ **Git 规范** - 使用功能分支，编写清晰的提交信息  
✅ **文档规范** - 结构清晰，包含示例  
✅ **测试规范** - 单元测试 + 集成测试  
✅ **安全规范** - 风控检查，数据加密  

### 下一步

1. 📖 阅读 [架构设计文档](01_architecture_design.md)
2. 📖 阅读 [模块概览文档](02_modules_overview.md)
3. 🎯 开始开发功能
4. 💬 参与社区讨论

---

**EasyXT 量化交易平台**  
*让量化交易更简单，让策略开发更高效*
