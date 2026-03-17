#!/usr/bin/env pwsh
# run_stage1.ps1 — Stage 1 单标的运行入口（强制使用 Python 3.11 / qmt311）
# 用法：
#   .\run_stage1.ps1 --strategy dual_ma --symbol 000001.SZ --start 2024-01-01 --end 2025-12-31 --oos-split 2025-01-01
#   .\run_stage1.ps1 --strategy dual_ma --symbol 000001.SZ --start 2024-01-01 --end 2025-12-31 --oos-split 2025-01-01 --benchmark CSI300

$PYTHON_QMT311 = "C:\Users\wangzhong\miniconda3\envs\qmt311\python.exe"

if (-not (Test-Path $PYTHON_QMT311)) {
    Write-Error "[ERROR] 找不到 qmt311 Python 解释器: $PYTHON_QMT311"
    Write-Error "请确认已创建：conda create -n qmt311 python=3.11"
    exit 1
}

$ver = & $PYTHON_QMT311 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
if ($ver -ne "3.11") {
    Write-Error "[ERROR] qmt311 环境 Python 版本异常：$ver（期望 3.11）"
    exit 1
}

$env:PYTHONPATH = $PSScriptRoot
Write-Host "[INFO] 使用解释器: $PYTHON_QMT311 (Python $ver)" -ForegroundColor Cyan
Write-Host "[INFO] PYTHONPATH  : $env:PYTHONPATH" -ForegroundColor Cyan

& $PYTHON_QMT311 -m strategies.stage1_pipeline @args
exit $LASTEXITCODE
