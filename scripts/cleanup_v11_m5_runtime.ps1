# Cleanup for this session's independent WSL runtime, never Docker Desktop.
# Refuses any distribution/path/container/volume outside the reviewed M5 inventory.
$ErrorActionPreference = 'Stop'
$workspaceRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..')).TrimEnd('\')
$runtimeRoot = Join-Path $workspaceRoot 'artifacts\v11-m5-runtime'
$expectedBase = [IO.Path]::GetFullPath((Join-Path $runtimeRoot 'wsl')).TrimEnd('\')
$distroName = 'marketdata-v11-m5-test'
$registration = @(Get-ChildItem 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss' | ForEach-Object { Get-ItemProperty -LiteralPath $_.PSPath } | Where-Object { $_.DistributionName -eq $distroName })
if ($registration.Count -ne 1) { throw 'Expected exactly one task WSL registration' }
$actualBase = [IO.Path]::GetFullPath(($registration[0].BasePath -replace '^\\\\\?\\','')).TrimEnd('\')
if (-not $actualBase.Equals($expectedBase,[StringComparison]::OrdinalIgnoreCase)) { throw 'Task distribution path mismatch; nothing removed' }
if (-not $actualBase.StartsWith($runtimeRoot + '\',[StringComparison]::OrdinalIgnoreCase)) { throw 'Task path escapes runtime workspace' }
$active = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'v11_qualification_process.py' -and $_.CommandLine -match 'v11-m5-' })
if ($active.Count -ne 0) { throw 'Task application processes remain; stop them before cleanup' }
$dockerPath = '/tmp/marketdata-v11-m5/docker/docker'
$socket = 'unix:///tmp/marketdata-v11-m5/docker.sock'
function Invoke-TaskDocker {
    param([string[]]$Arguments)
    $raw = & wsl.exe -d $distroName -u root -- $dockerPath -H $socket @Arguments
    if ($LASTEXITCODE -ne 0) { throw 'Task Docker command failed' }
    return $raw
}
$info = ConvertFrom-Json -InputObject ((Invoke-TaskDocker -Arguments @('info','--format','{{json .}}')) -join [Environment]::NewLine)
if ($info.DockerRootDir -ne '/tmp/marketdata-v11-m5/data') { throw 'Unexpected daemon root' }
$containerNames = @('marketdata-v11-m5-destination-pg16-test','marketdata-v11-m5-source-pg16-test','marketdata-v11-m5-destination-ts-test','marketdata-v11-m5-source-ts-test')
$volumeNames = @($containerNames | ForEach-Object { $_ + '-data' })
$containers = ConvertFrom-Json -InputObject ((Invoke-TaskDocker -Arguments (@('inspect') + $containerNames)) -join [Environment]::NewLine)
if ($containers.Count -ne 4) { throw 'Unexpected container inventory' }
foreach ($container in $containers) {
    if ($containerNames -notcontains $container.Name.TrimStart('/')) { throw 'Unknown container' }
    if ($container.Config.Labels.'marketdata.task' -ne 'v11-m5') { throw 'Missing task label' }
    foreach ($mount in $container.Mounts) {
        if ($mount.Type -eq 'tmpfs') { continue }
        if ($mount.Type -ne 'volume' -or $volumeNames -notcontains $mount.Name) { throw 'Unrelated mount' }
    }
}
$volumes = ConvertFrom-Json -InputObject ((Invoke-TaskDocker -Arguments (@('volume','inspect') + $volumeNames)) -join [Environment]::NewLine)
foreach ($volume in $volumes) {
    if ($volumeNames -notcontains $volume.Name -or $volume.Labels.'marketdata.task' -ne 'v11-m5') { throw 'Unrelated volume' }
    if (-not $volume.Mountpoint.StartsWith('/tmp/marketdata-v11-m5/data/volumes/')) { throw 'Volume escapes own daemon root' }
}
$evidence = [ordered]@{ verified_at=[DateTime]::UtcNow.ToString('o'); distribution=$distroName; verified_absolute_path=$actualBase; daemon_root=$info.DockerRootDir; containers=@($containers | ForEach-Object { [ordered]@{name=$_.Name;id=$_.Id;image=$_.Image;mounts=$_.Mounts;command=$_.Config.Cmd;network=$_.HostConfig.NetworkMode} }); volumes=@($volumes | Select-Object Name,Mountpoint,Labels) }
[IO.File]::WriteAllText((Join-Path $runtimeRoot 'infrastructure-final.json'),($evidence | ConvertTo-Json -Depth 20),[Text.UTF8Encoding]::new($false))
foreach ($name in $containerNames) { Invoke-TaskDocker -Arguments @('rm','-f',$name) }
foreach ($name in $volumeNames) { Invoke-TaskDocker -Arguments @('volume','rm',$name) }
& wsl.exe --terminate $distroName
if ($LASTEXITCODE -ne 0) { throw 'Task distribution termination failed' }
& wsl.exe --unregister $distroName
if ($LASTEXITCODE -ne 0) { throw 'Task distribution unregister failed' }
[IO.File]::WriteAllText((Join-Path $runtimeRoot 'cleanup-completed.json'),(@{completed_at=[DateTime]::UtcNow.ToString('o');containers_removed=$containerNames;volumes_removed=$volumeNames;distribution_removed=$distroName;verified_absolute_path=$actualBase} | ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
