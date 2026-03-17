$PYTHON_QMT311 = "C:\Users\wangzhong\miniconda3\envs\qmt311\python.exe"

if (-not (Test-Path $PYTHON_QMT311)) {
    Write-Error "[ERROR] 找不到 qmt311 Python 解释器: $PYTHON_QMT311"
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

& $PYTHON_QMT311 tools/data_reconciliation_audit.py --config config/data_reconciliation_audit.json --out-dir artifacts @args
exit $LASTEXITCODE
