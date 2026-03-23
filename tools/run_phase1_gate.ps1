# Phase-1 gate: optimizer + strategy + backtest chain validation
# Usage: powershell -ExecutionPolicy Bypass -File tools/run_phase1_gate.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = "C:\Users\wangzhong\miniconda3\envs\myenv\python.exe"

function Fail([string]$msg) {
    Write-Host ""
    Write-Host "[GATE FAIL] $msg" -ForegroundColor Red
    exit 1
}

function Pass([string]$msg) {
    Write-Host "[OK] $msg" -ForegroundColor Green
}

Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host " Phase-1 Gate" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "--- Step 1: ruff lint ---" -ForegroundColor Yellow

$ruff_targets = @(
    "core/portfolio_risk.py",
    "strategies/management/optimizer_strategy.py",
    "strategies/management/__init__.py",
    "easyxt_backtest/engine.py"
)

& $python -m ruff check @ruff_targets
if ($LASTEXITCODE -ne 0) { Fail "ruff check failed" }
Pass "ruff: 0 errors"

Write-Host ""
Write-Host "--- Step 2: pytest 24 tests ---" -ForegroundColor Yellow

$test_files = @(
    "tests/test_portfolio_optimizer.py",
    "tests/test_portfolio_risk_optimizer_check.py",
    "tests/test_optimize_and_rebalance.py",
    "tests/test_optimizer_strategy.py"
)

& $python -m pytest @test_files -q --tb=short --no-header
if ($LASTEXITCODE -ne 0) { Fail "pytest: some tests failed" }
Pass "pytest: all passed"

Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host " Phase-1 Gate PASSED" -ForegroundColor Green
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""