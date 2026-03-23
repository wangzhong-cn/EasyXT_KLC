#!/usr/bin/env pwsh
# =============================================================================
# git_history_purge.ps1
# 用途：从 Git 完整历史中彻底删除泄露的凭据文件，并强制推送覆盖远程。
#
# !! 执行前必读 !!
# 1. 先在浏览器完成以下操作（不可逆，必须先做）：
#    - Gitee → 设置 → 私人令牌 → 撤销 a8946879cc448dd9361047b24ffef6e5
#    - 雪球 → 账户安全 → 退出全部设备（使 xq_a_token / xq_r_token 失效）
#    - 申请一个新的 Gitee 私人令牌，替换下方 $NEW_GITEE_TOKEN
# 2. 通知所有协作者：此操作会重写历史，他们需要重新 clone 或 fetch --force
# 3. 如果 GitHub 仓库为公开（Public），考虑先临时设为私有再操作
# =============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$NEW_GITEE_TOKEN,
    [string]$GITEE_USER = "TradersTV",
    [string]$GITEE_REPO = "easy-xt_-klc",
    [string]$GITHUB_REMOTE_URL = "https://github.com/wangzhong-cn/EasyXT_KLC.git"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$REPO_ROOT = Split-Path -Parent $PSScriptRoot

# ─────────────────────────────────────────────────────────────────────────────
# Step 0: 安全确认
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "========================================" -ForegroundColor Yellow
Write-Host " Git 历史敏感文件清洗脚本" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""
Write-Host ">>> 目标仓库: $REPO_ROOT" -ForegroundColor Cyan
Write-Host ">>> 将从全部历史删除以下文件:" -ForegroundColor Cyan
Write-Host "    - config/xueqiu_config.json" -ForegroundColor Red
Write-Host "    - config/real_trading.json" -ForegroundColor Red
Write-Host ""
$confirm = Read-Host "确认已撤销旧凭据并持有新 Gitee Token？输入 YES 继续"
if ($confirm -ne "YES") {
    Write-Host "已取消。" -ForegroundColor Red
    exit 1
}

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: 备份当前仓库
# ─────────────────────────────────────────────────────────────────────────────
$BACKUP_DIR = "${REPO_ROOT}_BACKUP_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
Write-Host ""
Write-Host ">>> [1/6] 备份仓库 → $BACKUP_DIR" -ForegroundColor Green
Copy-Item -Path $REPO_ROOT -Destination $BACKUP_DIR -Recurse -Force
Write-Host "    备份完成" -ForegroundColor Green

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: 提交当前工作区修改（安全修复）
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host ">>> [2/6] 提交未提交的安全修复..." -ForegroundColor Green
Set-Location $REPO_ROOT
$status = git status --porcelain
if ($status) {
    git add -A
    git commit -m "chore(security): apply Day-1 security audit fixes

- Bounded asyncio queue in push_service.py
- Remove shell=True injection in panel.py
- TOCTOU fix in duckdb_connection_pool.py
- SQL identifier whitelist in duckdb_connection_pool.py
- SQL injection whitelist in unified_data_interface.py
- pickle.load removed from cache_manager.py
- api_server.py: empty-token warning + EASYXT_REQUIRE_AUTH enforcement
- Hardcoded path removed from duckdb_connection_pool.py
- mypy.ini version 3.9→3.11
- pre-commit: bandit + git-secrets hooks added
- .gitignore: block xueqiu/real_trading/monitor config files"
    Write-Host "    提交完成" -ForegroundColor Green
} else {
    Write-Host "    工作区干净，跳过提交" -ForegroundColor Yellow
}

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: 从 Git 历史中删除敏感文件
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host ">>> [3/6] 运行 git filter-repo 清洗历史..." -ForegroundColor Green
Write-Host "    目标文件: config/xueqiu_config.json, config/real_trading.json" -ForegroundColor Cyan

# filter-repo 需要 Python 环境中已安装
python -m git_filter_repo `
    --path "config/xueqiu_config.json" `
    --path "config/real_trading.json" `
    --invert-paths `
    --force

Write-Host "    filter-repo 完成" -ForegroundColor Green

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: 确认敏感文件已从历史消失
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host ">>> [4/6] 验证清洗结果..." -ForegroundColor Green
$remaining = git log --oneline --all -- config/xueqiu_config.json
if ($remaining) {
    Write-Host "    ⚠ 警告：仍有 commit 包含 xueqiu_config.json！" -ForegroundColor Red
    Write-Host $remaining
    exit 1
} else {
    Write-Host "    ✅ xueqiu_config.json 已从全部历史中消除" -ForegroundColor Green
}

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: 重建远程（filter-repo 会删除 remote）
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host ">>> [5/6] 重建 Git 远程..." -ForegroundColor Green

# 注意：新 Token 不能硬编码进脚本/版本控制
$GITEE_URL = "https://${GITEE_USER}:${NEW_GITEE_TOKEN}@gitee.com/${GITEE_USER}/${GITEE_REPO}.git"

git remote add origin $GITEE_URL
git remote add github $GITHUB_REMOTE_URL

# 验证 remote 存在（不打印 token）
$remotes = git remote -v | ForEach-Object { $_ -replace ":[^@]*@", ":<TOKEN>@" }
Write-Host "    当前远程（token 已脱敏）："
$remotes | ForEach-Object { Write-Host "    $_" }

# ─────────────────────────────────────────────────────────────────────────────
# Step 6: 强制推送
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host ">>> [6/6] 强制推送覆盖远程历史..." -ForegroundColor Green
Write-Host "    推送到 Gitee (origin)..."
git push origin --all --force
git push origin --tags --force

Write-Host "    推送到 GitHub..."
git push github --all --force
git push github --tags --force

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " 清洗完成 ✅" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "后续操作（手动）：" -ForegroundColor Yellow
Write-Host "  1. 通知所有协作者执行: git fetch --prune && git reset --hard origin/main"
Write-Host "  2. 检查 GitHub Actions secrets 中是否有 GITEE_TOKEN，及时更新"
Write-Host "  3. 将新 Gitee token 仅存于 GitHub Actions secret，不得写入任何文件"
Write-Host "  4. 考虑对 GitHub 仓库启用 secret scanning alerts"
