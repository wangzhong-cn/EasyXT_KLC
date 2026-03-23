$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "[optimizer-ci-subset] running deterministic pytest subset..."
python -m pytest `
  tests/test_portfolio_optimizer.py `
  tests/test_portfolio_risk_optimizer_check.py `
  tests/test_optimize_and_rebalance.py `
  tests/test_optimizer_strategy.py `
  -q

Write-Host "[optimizer-ci-subset] done."
