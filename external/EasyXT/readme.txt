XTQuant Asset V1.0  XTQuant 资产 V1.0 最新发行版  对比   quant-king299 发布于 2025年10月18日 · 111 个提交 在此发行版之后进入 main 分支 v1.0.0 a52cdcd 包含运行依赖的 XTQuant 资源文件（xtquant.rar）
用于 根目录 环境部署
使用方法：将 xtquant.rar 解压覆盖至根目录xtquant
版本声明：XTQuant 版本非常多，本项目以本次发布的 xtquant 为准；如有本地不同版本，建议下载后覆盖使用




安装   环境要求   • 64 位 Python（建议 3.9+） • 已安装并登录的 QMT 客户端（标准版或迷你版） • Windows 系统（QMT 限制）  通过 pip 从 GitHub 安装（推荐用于生产环境）   推荐固定到稳定标签 v1.0.0： # 可选：创建虚拟环境
python -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install -U pip setuptools wheel
pip install "git+https://github.com/quant-king299/EasyXT.git@v1.0.0"   国内镜像（依赖走镜像，源码仍从 GitHub 拉取）： pip install -i https://pypi.tuna.tsinghua.edu.cn/simple "git+https://github.com/quant-king299/EasyXT.git@v1.0.0"   验证安装： python - << 'PY'
import easy_xt
print("easy_xt import OK:", easy_xt.__name__)
from easy_xt import get_api
api = get_api()
print("get_api OK:", type(api))
PY   说明：pip 仅安装 Python 包，不会安装 QMT/xtquant，本地需自备。 必装的 xtquant 特殊版本（强制）：请到以下 Release 页面下载附件 xtquant.rar，解压后覆盖到本项目根目录下的 xtquant/ 目录（若不存在则直接解压到根目录会创建该目录）： https://github.com/quant-king299/EasyXT/releases/tag/v1.0.0 为什么必须使用这一份 xtquant：券商侧随各自版本发布，不会与迅投官方保持一致节奏；不同券商包的 xtquant 版本、接口和行为差异会导致本项目运行报错。为确保一致性与稳定性，本项目仅支持上述 Release 附件中的 xtquant 版本，使用高/低其它版本都可能出现连接失败、字段缺失、接口不兼容等错误。 一键下载并解压（PowerShell，推荐）： $url = "https://github.com/quant-king299/EasyXT/releases/download/v1.0.0/xtquant.rar"
$dest = "$PWD\xtquant.rar"
Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing
if (Test-Path "$env:ProgramFiles\7-Zip\7z.exe") {
  & "$env:ProgramFiles\7-Zip\7z.exe" x -y "$dest" -o"$PWD"
} elseif (Get-Command 7z.exe -ErrorAction SilentlyContinue) {
  7z x -y "$dest" -o"$PWD"
} else {
  Write-Host "未检测到 7-Zip，请手动解压 $dest 到项目根目录（或安装 7-Zip 后重试）"
}
Remove-Item $dest -ErrorAction SilentlyContinue
# 验证目录：应出现 $PWD\xtquant 目录
if (Test-Path "$PWD\xtquant") { Write-Host "xtquant 安装完成" } else { Write-Host "xtquant 目录未找到，请检查解压是否成功" }   注：PowerShell 无法原生解压 .rar，需本机已安装 7-Zip（https://www.7-zip.org/）。如无 7-Zip，请手动解压 xtquant.rar 到项目根目录。  项目源码方式安装（可选）   # 克隆项目到本地   git clone https://github.com/quant-king299/EasyXT.git
cd EasyXT   # 安装依赖到Python环境   方式一：直接安装依赖到系统Python环境 pip install -r requirements.txt   方式二：创建虚拟环境安装（推荐） # 创建虚拟环境
python -m venv .venv
# 激活虚拟环境（Windows）
.\.venv\Scripts\activate
# 安装依赖
pip install -r requirements.txt   方式三：通过pip从GitHub直接安装（推荐用于生产环境） # 可选：创建虚拟环境
python -m venv .venv
.\.venv\Scripts\activate

# 更新pip并安装
python -m pip install -U pip setuptools wheel
pip install "git+https://github.com/quant-king299/EasyXT.git@v1.0.0"   🔧 配置   配置 QMT 路径（雪球跟单）   编辑：strategies/xueqiu_follow/config/unified_config.json 关键键名：settings.account.qmt_path（若同时存在 account.qmt_path，两处保持一致）。 示例（Windows JSON 需双反斜杠或用正斜杠）： {
  "settings": {
    "account": {
      "qmt_path": "D:\\国金证券QMT交易端\\userdata_mini",
      "account_id": "你的交易账号ID"
    }
  }
}   如何判断“正确目录”： • 必须是 QMT 的 userdata 或 userdata_mini 目录本身 • 目录内通常包含 xtquant, log, cfg 等子目录 • 常见错写：0MT（应为 QMT）、userdata mini（应为 userdata_mini）  📚 快速开始   基础数据获取   from easy_xt import EasyXT

# 创建API实例
api = EasyXT()

# 初始化数据服务
api.init_data()

# 获取股票价格
data = api.get_price('000001.SZ', count=100)
print(data.head())   简单交易示例   # 初始化交易服务
api.init_trade(USERDATA_PATH)
api.add_account(ACCOUNT_ID)

# 买入股票
order_id = api.buy(
    account_id=ACCOUNT_ID,
    code='000001.SZ',
    volume=100,
    price_type='market'
)   运行雪球跟单   方式一：批处理脚本（Windows） .\strategies\xueqiu_follow\启动雪球跟单.bat   方式二：Python 入口脚本 python strategies\xueqiu_follow\start_xueqiu_follow_easyxt.py   📖 学习路径   初学者路径   1. 01_基础入门.py - 学习基本的数据获取和API使用 2. 02_交易基础.py - 掌握基础交易操作 3. 05_数据周期详解.py - 了解不同数据周期的使用  进阶路径   4. 03_高级交易.py - 学习高级交易功能 5. 04_策略开发.py - 开发量化交易策略 6. 06_扩展API学习实例.py - 掌握扩展功能  实战路径   7. 07_qstock数据获取学习案例.py - 真实数据获取 8. 08_数据获取与交易结合案例.py - 数据与交易结合 9. 10_qstock真实数据交易案例_修复交易服务版.py - 完整实战案例  🏗️ 项目结构   miniqmt扩展/
├── easy_xt/                    # 核心API模块
│   ├── __init__.py
│   ├── api.py                  # 主API接口
│   ├── data_api.py            # 数据接口
│   ├── trade_api.py           # 交易接口
│   ├── advanced_trade_api.py  # 高级交易接口
│   └── utils.py               # 工具函数
├── 学习实例/                   # 学习案例
│   ├── 01_基础入门.py
│   ├── 02_交易基础.py
│   ├── 03_高级交易.py
│   └── ...
├── config/                     # 配置文件
│   ├── config_template.py
│   └── config.py
├── data/                       # 数据存储目录
├── logs/                       # 日志目录
├── xtquant/                    # QMT相关文件
├── gui_app/                    # GUI应用（可选）
├── requirements.txt            # 依赖列表
├── README.md                   # 项目说明
└── .gitignore                  # Git忽略文件
   ⚠️  风险提示   1. 投资风险: 量化交易存在投资风险，请谨慎操作 2. 测试环境: 建议先在模拟环境中测试策略 3. 资金管理: 合理控制仓位，设置止损止盈 4. 合规要求: 遵守相关法律法规和交易所规则