param(
  [string]$ProtocolFile = "artifacts/OBSERVATION_PROTOCOL_20260321.md",
  [string]$DateText = "",
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$obsArgs = @("-File", "tools/daily_observation.ps1", "-WriteJson")
if (-not [string]::IsNullOrWhiteSpace($DateText)) {
  $obsArgs += @("-DateText", $DateText)
}
pwsh @obsArgs | Out-Null

$obsDir = Join-Path $root "artifacts\observation"
$latest = Get-ChildItem -Path $obsDir -Filter "daily_observation_*.json" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($null -eq $latest) {
  throw "未找到 observation json 快照"
}

$payload = Get-Content -Path $latest.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
$gateErr = ""
if ($payload.PSObject.Properties.Name -contains "gate_error") {
  $gateErr = [string]$payload.gate_error
}
if (-not [string]::IsNullOrWhiteSpace($gateErr)) {
  throw "daily_observation 采样失败: $gateErr"
}
$d = [string]$payload.date
if ([string]::IsNullOrWhiteSpace($d)) {
  $d = (Get-Date).ToString("yyyy-MM-dd")
}

$appendBlock = @(
  "",
  "### T+N ($d) — 自动追加",
  "",
  '```',
  "strict_pass:          $($payload.strict_pass)",
  "watchdog_consecutive: $($payload.watchdog_consecutive) / 3",
  "fake_ohlcv_count:     $($payload.fake_ohlcv_count)",
  "fake_ohlcv_delta:     $($payload.fake_ohlcv_delta)",
  "测试基线:             $($payload.passed) passed / $($payload.skipped) skipped / $($payload.failed) failed / $($payload.errors) errors",
  "passed_delta_vs_anchor: $($payload.passed_delta_vs_anchor)",
  "备注:                 $($payload.note)",
  "source_json:          $($latest.Name)",
  '```'
) -join "`n"

if ($DryRun) {
  Write-Host $appendBlock
  exit 0
}

$protocolPath = Join-Path $root $ProtocolFile
if (-not (Test-Path $protocolPath)) {
  throw "协议文件不存在: $protocolPath"
}
Add-Content -Path $protocolPath -Value $appendBlock -Encoding UTF8
Write-Host "appended_to: $protocolPath"
