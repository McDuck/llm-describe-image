# Deploy the recognition review service to a configured remote Linux host.
[CmdletBinding()]
param(
    [string]$RemoteHost = "gpu-host.example.invalid",
    [string]$RemoteUser = "deploy",
    [string]$RemotePath = "projects/recognition-review",
    [string]$IdentityFile = (Join-Path $env:USERPROFILE ".ssh\id_ed25519_describe_media"),
    [string]$KnownHostsFile = (Join-Path $env:USERPROFILE ".ssh\known_hosts"),
    [string]$OutputDir = "/mnt/media-output",
    [int]$ReviewPort = 8080
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
foreach ($requiredFile in @($IdentityFile, $KnownHostsFile)) {
    if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
        throw "Required file was not found: $requiredFile"
    }
}
foreach ($command in @("scp", "ssh")) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "Required command was not found: $command"
    }
}

$sshOptions = @(
    "-i", $IdentityFile,
    "-o", "UserKnownHostsFile=$KnownHostsFile",
    "-o", "StrictHostKeyChecking=yes",
    "-o", "BatchMode=yes"
)
$target = "$RemoteUser@$RemoteHost"

& ssh @sshOptions $target "mkdir -p -- '$RemotePath/recognition_review/api' '$RemotePath/recognition_review/ui' '$RemotePath/recognition_review/config'"
if ($LASTEXITCODE -ne 0) { throw "Could not create the remote review directory." }

$reviewFiles = @(Join-Path $repoRoot "recognition_review/docker-compose.yml")
$apiFiles = @(Join-Path $repoRoot "recognition_review/api/server.py"; Join-Path $repoRoot "recognition_review/api/Dockerfile")
$uiFiles = @(
    Join-Path $repoRoot "recognition_review/ui/Dockerfile"
    Join-Path $repoRoot "recognition_review/ui/nginx.conf"
    Join-Path $repoRoot "recognition_review/ui/index.html"
    Join-Path $repoRoot "recognition_review/ui/app.js"
    Join-Path $repoRoot "recognition_review/ui/app.css"
)

& scp @sshOptions $reviewFiles "$target`:$RemotePath/recognition_review/"
& scp @sshOptions $apiFiles "$target`:$RemotePath/recognition_review/api/"
& scp @sshOptions $uiFiles "$target`:$RemotePath/recognition_review/ui/"
if ($LASTEXITCODE -ne 0) { throw "Review deployment file transfer failed." }

$remoteCommand = "cd -- '$RemotePath/recognition_review' && printf 'OUTPUT_DIR=%s\nREVIEW_PORT=%s\n' '$OutputDir' '$ReviewPort' > config/.env && docker compose --env-file config/.env -f docker-compose.yml up -d --build && docker compose --env-file config/.env -f docker-compose.yml ps"
& ssh @sshOptions $target $remoteCommand
if ($LASTEXITCODE -ne 0) { throw "Remote review deployment failed." }

Write-Host "Recognition review UI deployed at http://${RemoteHost}:$ReviewPort"
