param(
  [int]$AnchorFakeOhlcv = 452,
  [int]$AnchorPassed = 4704,
  [string]$DateText = "",
  [switch]$WriteJson
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = "C:/Users/wangzhong/miniconda3/envs/myenv/python.exe"
if (-not (Test-Path $python)) {
  $python = "python"
}

$gateProbeCode = @'
import json, re, subprocess, sys
try:
    res = subprocess.run([sys.executable, "tools/p0_gate_check.py", "--json"], capture_output=True, text=True, encoding="utf-8", errors="ignore")
    raw = (res.stdout or "")
    if not raw.strip():
        raw = (res.stderr or "")
    s = raw.find("{")
    e = raw.rfind("}")
    if s < 0 or e <= s:
        raise RuntimeError("gate_json_not_found")
    data = json.loads(raw[s:e+1])
    checks = data.get("checks", []) if isinstance(data, dict) else []
    def _find(name):
        for item in checks:
            if isinstance(item, dict) and item.get("name") == name:
                return item
        return {}
    watchdog = _find("watchdog_slo_gate")
    fake = _find("fake_ohlcv_scan")
    wd = 0
    fd = 0
    wd_detail = str(watchdog.get("detail", ""))
    fd_detail = str(fake.get("detail", ""))
    m = re.search(r"连续违规=(\d+)", wd_detail)
    if m:
        wd = int(m.group(1))
    m2 = re.search(r"发现\s+(\d+)\s+处", fd_detail)
    if m2:
        fd = int(m2.group(1))
    print(json.dumps({
        "strict_pass": bool(data.get("strict_pass", False)),
        "p0_open_count": int(data.get("P0_open_count", 0)),
        "watchdog_consecutive": wd,
        "fake_ohlcv_count": fd,
        "gate_error": ""
    }, ensure_ascii=False))
except Exception as e:
    print(json.dumps({
        "strict_pass": False,
        "p0_open_count": 1,
        "watchdog_consecutive": 0,
        "fake_ohlcv_count": 0,
        "gate_error": str(e)
    }, ensure_ascii=False))
'@
$gateMiniRaw = & $python -X utf8 -c $gateProbeCode
if ($LASTEXITCODE -ne 0 -or -not $gateMiniRaw) {
  throw "p0_gate_check 采样失败"
}
$gateObj = (($gateMiniRaw -join "`n").Trim()) | ConvertFrom-Json
$watchdogConsecutive = [int]$gateObj.watchdog_consecutive
$fakeCount = [int]$gateObj.fake_ohlcv_count
$fakeDelta = $fakeCount - $AnchorFakeOhlcv

$pytestRaw = & $python -m pytest tests/ -q --tb=short 2>&1
if ($LASTEXITCODE -ne 0) {
  throw "pytest 运行失败"
}
$pytestText = ($pytestRaw -join "`n")
$summary = [regex]::Match($pytestText, "(\d+)\s+passed(?:,\s+(\d+)\s+skipped)?(?:,\s+(\d+)\s+failed)?(?:,\s+(\d+)\s+errors?)?")
if (-not $summary.Success) {
  throw "无法解析 pytest 汇总"
}
$passed = [int]$summary.Groups[1].Value
$skipped = if ($summary.Groups[2].Success) { [int]$summary.Groups[2].Value } else { 0 }
$failed = if ($summary.Groups[3].Success) { [int]$summary.Groups[3].Value } else { 0 }
$errors = if ($summary.Groups[4].Success) { [int]$summary.Groups[4].Value } else { 0 }
$passedDelta = $passed - $AnchorPassed

if ([string]::IsNullOrWhiteSpace($DateText)) {
  $DateText = (Get-Date).ToString("yyyy-MM-dd")
}

$statusNote = "无异常"
if ($gateObj.gate_error) {
  $statusNote = "gate_error: $($gateObj.gate_error)"
} elseif (-not [bool]$gateObj.strict_pass) {
  $statusNote = "strict_pass=False"
} elseif ($watchdogConsecutive -ge 3) {
  $statusNote = "watchdog_consecutive>=3，触发快照归档"
} elseif ($fakeDelta -gt 0) {
  $statusNote = "fake_ohlcv 增量>0，需定位来源"
} elseif ($failed -gt 0 -or $errors -gt 0) {
  $statusNote = "pytest 出现 failed/errors"
}

$block = @(
  "### T+N ($DateText)",
  "strict_pass:             $($gateObj.strict_pass)",
  "watchdog_consecutive:    $watchdogConsecutive / 3",
  "fake_ohlcv_count:        $fakeCount",
  "fake_ohlcv_delta:        $fakeDelta",
  "测试基线:                $passed passed / $skipped skipped / $failed failed / $errors errors",
  "passed_delta_vs_anchor:  $passedDelta",
  "备注:                    $statusNote"
) -join "`n"

Write-Host $block

if ($WriteJson) {
  $outDir = Join-Path $root "artifacts\observation"
  New-Item -ItemType Directory -Force -Path $outDir | Out-Null
  $outFile = Join-Path $outDir ("daily_observation_" + (Get-Date).ToString("yyyyMMdd_HHmmss") + ".json")
  $payload = [ordered]@{
    date = $DateText
    strict_pass = [bool]$gateObj.strict_pass
    p0_open_count = [int]$gateObj.P0_open_count
    gate_error = [string]$gateObj.gate_error
    watchdog_consecutive = $watchdogConsecutive
    fake_ohlcv_count = $fakeCount
    fake_ohlcv_delta = $fakeDelta
    passed = $passed
    skipped = $skipped
    failed = $failed
    errors = $errors
    passed_delta_vs_anchor = $passedDelta
    note = $statusNote
  }
  ($payload | ConvertTo-Json -Depth 4) | Set-Content -Path $outFile -Encoding UTF8
  Write-Host "json_written: $outFile"
}
