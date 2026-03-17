param(
    [string]$Url = "http://127.0.0.1:8000",
    [int]$IntervalSec = 30,
    [int]$CriticalThreshold = 2,
    [string]$LogFile = "gray_monitor.log",
    [switch]$DryRunOnce
)

# 统一 UTF-8 输出，避免中文日志乱码
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$OutputEncoding = [System.Text.UTF8Encoding]::new()
$env:PYTHONUTF8 = "1"

$consecutive = 0

function Write-Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ss') $msg"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

Write-Log "=== 灰度监控启动 url=$Url interval=${IntervalSec}s critical_threshold=$CriticalThreshold dry_run_once=$($DryRunOnce.IsPresent) ==="

while ($true) {
    try {
        $resp = Invoke-RestMethod -Uri "$Url/health" -TimeoutSec 5 -ErrorAction Stop

        $ws = $resp.checks.ws
        $alert = $ws.drop_alert
        $dr1m = $ws.drop_rate_1m
        $latAvg = $ws.publish_latency_ms
        $latMax = $ws.publish_latency_max_ms
        $qLen = $ws.queue_len
        $strats = $resp.strategies_running
        $symbols = ($resp.ws_symbols -join ",")
        if (-not $symbols) { $symbols = "(none)" }

        Write-Log ("drop_rate_1m=$dr1m drop_alert=$alert " +
                   "lat_avg=$latAvg lat_max=$latMax queue_len=$qLen " +
                   "strategies=$strats symbols=$symbols")

        if ($alert -eq "critical") {
            $consecutive++
            Write-Log ">>> CRITICAL #$consecutive / $CriticalThreshold"

            if ($consecutive -ge $CriticalThreshold) {
                Write-Log "!!! 连续 $CriticalThreshold 次 critical — 触发处置 !!!"
                Write-Log "    lat_max=$latMax queue_len=$qLen → 参考决策矩阵执行回滚或踢连接"
                $consecutive = 0
            }
        } else {
            if ($consecutive -gt 0) {
                Write-Log "--- alert 已恢复为 $alert，重置连续计数"
            }
            $consecutive = 0
        }
    } catch {
        Write-Log "health_unreachable error=$($_.Exception.Message)"
        Write-Log "hint=请确认服务已启动: uvicorn core.api_server:app --host 127.0.0.1 --port 8000"
    }

    if ($DryRunOnce) {
        Write-Log "=== DryRunOnce 已执行 1 次采样，脚本退出 ==="
        break
    }

    Start-Sleep -Seconds $IntervalSec
}
