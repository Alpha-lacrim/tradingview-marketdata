param(
    [Parameter(Mandatory = $true)]
    [ValidateSet(17, 34, 64, 100, 128, 256)] [int] $Count,
    [ValidateSet(17, 34, 64, 100, 128, 256)] [int] $RollbackCount = 34,
    [ValidateRange(0, 6)] [int] $RollbackConnections = 0,
    [ValidateRange(0, 12)] [int] $RollbackConcurrency = 0,
    [ValidateRange(0, 8)] [int] $RollbackGraceSeconds = 2,
    [string] $RollbackImage = 'tradingview-app:ordinary-scale',
    [ValidateRange(0, 6)] [int] $Connections = 0,
    [ValidateRange(0, 12)] [int] $Concurrency = 0,
    [string] $Image = 'tradingview-app:ordinary-scale',
    [ValidateRange(0, 8)] [int] $GraceSeconds = 2,
    [string] $DeploymentDirectory = ''
)
$ErrorActionPreference = 'Stop'
# Legacy schema-0004 combined deployment only. This is not the split-process deployer.
# Never run this helper as part of integration tests.
$sourceDirectory = Split-Path -Parent $PSScriptRoot
$appBefore = (docker inspect tradingview-app-1 | ConvertFrom-Json)[0]
$dbBefore = (docker inspect tradingview-db-1 | ConvertFrom-Json)[0]
if (-not $DeploymentDirectory) {
    $DeploymentDirectory = $appBefore.Config.Labels.'com.docker.compose.project.working_dir'
    if (-not $DeploymentDirectory -or -not (Test-Path -LiteralPath $DeploymentDirectory)) {
        throw 'Existing Compose working directory is unavailable; pass its verified path explicitly.'
    }
}
$collectors = @(docker ps --filter label=com.docker.compose.service=app --format '{{.Names}}')
if ($collectors.Count -ne 1 -or $collectors[0] -ne 'tradingview-app-1') {
    throw 'Expected exactly one existing ordinary app; investigate before switching.'
}
if ($appBefore.Config.Labels.'com.docker.compose.project' -ne 'tradingview' -or
    $dbBefore.Config.Labels.'com.docker.compose.project' -ne 'tradingview' -or
    $dbBefore.Mounts[0].Name -ne 'tradingview_marketdata_pg') {
    throw 'Unexpected project/database volume; no action taken.'
}
$systemIdentifier = docker exec tradingview-db-1 psql -U marketdata -d marketdata -At -c 'SELECT system_identifier FROM pg_control_system()'
if ($LASTEXITCODE -ne 0 -or $systemIdentifier.Trim() -ne '7693817192740921379') {
    throw 'Unexpected production PostgreSQL system identifier; no action taken.'
}
$legacySchema = docker exec tradingview-db-1 psql -U marketdata -d marketdata -At -c 'SELECT version_num FROM alembic_version'
if ($LASTEXITCODE -ne 0 -or $legacySchema.Trim() -ne '0004') {
    throw 'Legacy switch requires schema 0004; use the reviewed split-process migration/rollback procedure.'
}
$legacyBase = docker compose -p tradingview --project-directory $DeploymentDirectory -f (Join-Path $DeploymentDirectory 'compose.yaml') config --format json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $legacyBase.services.collector -or
    $legacyBase.services.app.environment.APPLICATION_ROLE -eq 'api') {
    throw 'Legacy switch cannot operate on a split-process Compose project.'
}

function Get-CollectorConfiguration([int] $SelectedCount, [int] $Sockets = 0, [int] $Requests = 0,
    [string] $SelectedImage = 'tradingview-app:ordinary-scale', [int] $Grace = 2) {
    $env:TRADINGVIEW_INSTRUMENT_COUNT = "$SelectedCount"
    $env:TRADINGVIEW_SCALE_IMAGE = $SelectedImage
    $env:TRADINGVIEW_SCALE_GRACE_SECONDS = "$Grace"
    $connections = switch ($SelectedCount) { 64 { 2 } 100 { 4 } 128 { 4 } 256 { 4 } default { 1 } }
    if ($Sockets -gt 0) { $connections = $Sockets }
    $env:TRADINGVIEW_SCALE_CONNECTIONS = "$connections"
    $env:TRADINGVIEW_SCALE_CONCURRENCY = "$(2 * $connections)"
    if ($Requests -gt 0) { $env:TRADINGVIEW_SCALE_CONCURRENCY = "$Requests" }
    $arguments = @('-p', 'tradingview', '--project-directory', $DeploymentDirectory,
        '-f', (Join-Path $DeploymentDirectory 'compose.yaml'),
        '-f', (Join-Path $DeploymentDirectory 'compose.tradingview.yaml'))
    if ($SelectedCount -ne 17) {
        $arguments += @('-f', (Join-Path $DeploymentDirectory 'compose.tradingview-pilot.yaml'))
    }
    $override = if ($SelectedCount -le 34) { 'compose.tradingview-rollback-image.yaml' } else { 'compose.tradingview-legacy-scale.yaml' }
    return $arguments + @('-f', (Join-Path $sourceDirectory $override))
}

$desired = Get-CollectorConfiguration $Count $Connections $Concurrency $Image $GraceSeconds
$composeConfig = docker compose @desired config --format json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'Compose configuration validation failed before stop.' }
$image = $composeConfig.services.app.image
$imageCheck = docker image inspect $image | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $imageCheck) { throw 'Required image unavailable before stop.' }
# Preserve the old app while waiting for a bounded early-minute idle switch window.
# A late startup can overrun the next UTC target even when its cycle body is fast.
$switchWindowReady = $false
$windowEnd = if ($Count -le 100) { 40 } else { 30 }
for ($windowAttempt = 0; $windowAttempt -lt 35; $windowAttempt++) {
    $second = [DateTime]::UtcNow.Second
    if ($second -ge 15 -and $second -le $windowEnd) {
        $currentProviders = Invoke-RestMethod 'http://localhost:8000/providers' -TimeoutSec 3
        $retrieval = $currentProviders[0].candle_retrieval
        $busyWorkers = @($retrieval.workers.PSObject.Properties | Where-Object {
            $_.Value.state -ne 'disconnected'
        })
        $currentCycle = $currentProviders[0].last_candle_cycle
        $currentMinute = [DateTime]::UtcNow
        $currentMinute = $currentMinute.AddTicks(-($currentMinute.Ticks % [TimeSpan]::TicksPerMinute))
        $completedCurrent = $currentCycle.bucket_end -and
            ([DateTimeOffset]::Parse($currentCycle.bucket_end).UtcDateTime -ge $currentMinute)
        if ($completedCurrent -and $null -ne $currentCycle.cycle_duration_seconds -and
            -not $retrieval.active_requests -and $busyWorkers.Count -eq 0) {
            $switchWindowReady = $true; break
        }
    }
    Start-Sleep -Seconds 2
}
if (-not $switchWindowReady) { throw 'No early-minute idle switch window; old collector remains running.' }
$startedAt = [DateTime]::UtcNow.ToString('o')
try {
    docker stop --timeout 35 tradingview-app-1
    if ($LASTEXITCODE -ne 0) { throw 'Could not stop the sole collector.' }
    docker compose @desired up -d --no-deps --no-build app
    if ($LASTEXITCODE -ne 0) { throw 'App replacement failed.' }
    $ready = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        try {
            $providers = Invoke-RestMethod 'http://localhost:8000/providers' -TimeoutSec 3
            if (@($providers[0].symbols.PSObject.Properties).Count -eq $Count) { $ready = $true; break }
        } catch { }
        Start-Sleep -Seconds 2
    }
    if (-not $ready) { throw 'Selected provider registry did not become ready within 60 seconds.' }
    $dbAfter = (docker inspect tradingview-db-1 | ConvertFrom-Json)[0]
    if ($dbAfter.Id -ne $dbBefore.Id -or $dbAfter.Mounts[0].Name -ne $dbBefore.Mounts[0].Name) {
        throw 'Database identity changed unexpectedly.'
    }
    $appAfter = (docker inspect tradingview-app-1 | ConvertFrom-Json)[0]
    $result = [ordered]@{ count = $Count;
        connections = $composeConfig.services.app.environment.CANDLE_POOL_CONNECTIONS;
        concurrency = $composeConfig.services.app.environment.CANDLE_POOL_CONCURRENCY;
        grace_seconds = $composeConfig.services.app.environment.CANDLE_FETCH_GRACE_SECONDS;
        switch_started_at = $startedAt;
        ready_at = [DateTime]::UtcNow.ToString('o'); previous_container = $appBefore.Id;
        app_container = $appAfter.Id; image = $appAfter.Image; database_container = $dbAfter.Id;
        volume = $dbAfter.Mounts[0].Name; system_identifier = $systemIdentifier.Trim() }
    $result | ConvertTo-Json | Set-Content -Encoding utf8 (Join-Path $sourceDirectory "artifacts/activation-$Count-$env:TRADINGVIEW_SCALE_CONNECTIONS-$env:TRADINGVIEW_SCALE_CONCURRENCY.json")
    $result | ConvertTo-Json
} catch {
    $reason = $_
    docker stop --timeout 35 tradingview-app-1
    $fallback = Get-CollectorConfiguration $RollbackCount $RollbackConnections $RollbackConcurrency $RollbackImage $RollbackGraceSeconds
    docker compose @fallback up -d --no-deps --no-build app
    if ($LASTEXITCODE -ne 0) { throw "Switch failed and fallback restart failed: $reason" }
    throw "Switch failed; restored $RollbackCount configuration: $reason"
}
