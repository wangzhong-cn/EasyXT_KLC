param(
    [Parameter(Mandatory = $true)]
    [string]$Allowlist,
    [string]$Repo = ""
)

$gh = Get-Command gh -ErrorAction SilentlyContinue
if (-not $gh) {
    Write-Error "未检测到 gh CLI，请先安装并执行 gh auth login"
    exit 1
}

$argsList = @("variable", "set", "BASELINE_UPDATE_ALLOWLIST", "--body", $Allowlist)
if ($Repo -and $Repo.Trim().Length -gt 0) {
    $argsList += @("--repo", $Repo.Trim())
}

& gh @argsList
if ($LASTEXITCODE -ne 0) {
    Write-Error "设置 BASELINE_UPDATE_ALLOWLIST 失败"
    exit $LASTEXITCODE
}

Write-Host "[OK] BASELINE_UPDATE_ALLOWLIST 已设置: $Allowlist"
