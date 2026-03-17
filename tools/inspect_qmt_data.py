"""
QMT 本地数据目录探查 + 账户连接测试
运行: python tools/inspect_qmt_data.py
"""
from __future__ import annotations

import os
import sys

# ---------------------------------------------------------------------------
# 路径配置
# ---------------------------------------------------------------------------
QMT_EXE = r"D:\申万宏源策略量化交易终端\bin.x64\XtItClient.exe"
QMT_USERDATA = r"D:\申万宏源策略量化交易终端\userdata_mini"
ACCOUNT_ID = os.environ.get("EASYXT_ACCOUNT_ID", "")

# ---------------------------------------------------------------------------
# 1. 路径存在性核查
# ---------------------------------------------------------------------------
print("=" * 60)
print("[1] QMT 路径核查")
print(f"  XtItClient.exe : {'✓' if os.path.exists(QMT_EXE) else '✗ 不存在!'} {QMT_EXE}")
print(f"  userdata_mini  : {'✓' if os.path.exists(QMT_USERDATA) else '✗ 不存在!'} {QMT_USERDATA}")

# ---------------------------------------------------------------------------
# 2. userdata_mini 关键子目录
# ---------------------------------------------------------------------------
print("\n[2] userdata_mini 结构")
if os.path.exists(QMT_USERDATA):
    items = sorted(os.listdir(QMT_USERDATA))
    important = [f for f in items if "mutex" not in f and "down_queue" not in f]
    for name in important:
        full = os.path.join(QMT_USERDATA, name)
        if os.path.isdir(full):
            try:
                count = len(os.listdir(full))
                print(f"  [DIR]  {name:<20} ({count} 项)")
            except PermissionError:
                print(f"  [DIR]  {name:<20} (无权限)")
        else:
            size = os.path.getsize(full)
            print(f"  [FILE] {name:<20} ({size} bytes)")

# ---------------------------------------------------------------------------
# 3. datas 目录 -- 市场品种概览
# ---------------------------------------------------------------------------
datas_dir = os.path.join(QMT_USERDATA, "datas")
print("\n[3] datas/ 品种目录（市场分类）")
if os.path.exists(datas_dir):
    markets = sorted(os.listdir(datas_dir))
    for mkt in markets:
        mkt_path = os.path.join(datas_dir, mkt)
        if not os.path.isdir(mkt_path):
            continue
        try:
            sub = os.listdir(mkt_path)
            item_count = len(sub)
            # 看子目录里有哪些数据类型
            types = set()
            for s in sub[:50]:
                sp = os.path.join(mkt_path, s)
                if os.path.isdir(sp):
                    types.update(os.listdir(sp)[:5])
            print(f"  {mkt:<10} {item_count:>6} 个品种  类型样本: {sorted(types)[:6]}")
        except PermissionError:
            print(f"  {mkt:<10} 无权限")
else:
    print("  datas/ 不存在")

# ---------------------------------------------------------------------------
# 4. datadir 目录 -- 账户/合约信息
# ---------------------------------------------------------------------------
datadir = os.path.join(QMT_USERDATA, "datadir")
print("\n[4] datadir/ 内容")
if os.path.exists(datadir):
    for name in sorted(os.listdir(datadir)):
        full = os.path.join(datadir, name)
        if os.path.isdir(full):
            try:
                print(f"  [DIR]  {name}  ({len(os.listdir(full))} 项)")
            except PermissionError:
                print(f"  [DIR]  {name}  (无权限)")
        else:
            print(f"  [FILE] {name}  ({os.path.getsize(full)} bytes)")
else:
    print("  datadir/ 不存在")

# ---------------------------------------------------------------------------
# 5. xtquant 导入测试
# ---------------------------------------------------------------------------
print("\n[5] xtquant 模块导入测试")
# 尝试从 QMT 安装的 python 目录加载 xtquant
qmt_python_dirs = [
    r"D:\申万宏源策略量化交易终端\bin.x64",
    r"D:\申万宏源策略量化交易终端\python",
    r"D:\申万宏源策略量化交易终端\mpython",
]
for d in qmt_python_dirs:
    if os.path.exists(d) and d not in sys.path:
        sys.path.insert(0, d)
        print(f"  已添加到 sys.path: {d}")

# 也加项目内 xtquant stub
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
stub_xtquant = os.path.join(project_root, "xtquant")
if os.path.exists(stub_xtquant) and project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    import xtquant
    print(f"  xtquant 导入成功，路径: {xtquant.__file__}")
    print(f"  __path__: {xtquant.__path__}")
except ImportError as e:
    print(f"  xtquant 导入失败: {e}")

try:
    from xtquant import xtdata
    print(f"  xtdata 导入成功")
except ImportError as e:
    print(f"  xtdata 导入失败（需要 QMT 本地 Python 组件）: {e}")

try:
    from xtquant import xttrader
    print(f"  xttrader 导入成功")
except ImportError as e:
    print(f"  xttrader 导入失败（需要 QMT 本地 Python 组件）: {e}")

# ---------------------------------------------------------------------------
# 6. 通过 EasyXT config 验证配置加载
# ---------------------------------------------------------------------------
print("\n[6] EasyXT config 配置验证")
try:
    sys.path.insert(0, project_root)
    from easy_xt.config import config
    print(f"  qmt_path:       {config.get('settings.account.qmt_path')}")
    print(f"  userdata_path:  {config.get('settings.account.qmt_userdata_path')}")
    print(f"  account_id:     {config.get('settings.account.account_id')}")
    print(f"  password:       {'已配置' if config.get('settings.account.password') else '未配置'}")
except Exception as e:
    print(f"  配置加载失败: {e}")

print("\n" + "=" * 60)
print("探查完成。如需连接 QMT 交易端，请先启动 XtItClient.exe 并登录。")
