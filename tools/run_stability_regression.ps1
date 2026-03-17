param(
  [int]$Runs = 3,
  [string]$Group = "all"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = "C:/Users/wangzhong/miniconda3/envs/myenv/python.exe"
if (-not (Test-Path $python)) {
  $python = "python"
}

& $python -m py_compile tools/stability_regression_gate.py
& $python tools/stability_regression_gate.py --runs $Runs --group $Group
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}
Write-Host "stability gate passed (runs=$Runs, group=$Group)"
