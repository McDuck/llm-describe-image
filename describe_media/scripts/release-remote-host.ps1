# Deploy application files to a configured remote Linux host.
[CmdletBinding()]
param(
    [string]$RemoteHost = "gpu-host.example.invalid",
    [string]$RemoteUser = "deploy",
    [string]$RemotePath = "projects/describe-media",
    [string]$IdentityFile = (Join-Path $env:USERPROFILE ".ssh\id_ed25519_describe_media"),
    [string]$KnownHostsFile = (Join-Path $env:USERPROFILE ".ssh\known_hosts"),
    [string]$InputDir = "/mnt/media-input",
    [string]$OutputDir = "/mnt/media-output",
    [string]$OpenAIApiBase = "https://api.openai.com/v1",
    [switch]$NoBuild,
    [switch]$NoRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$describeMediaRoot = Join-Path $repoRoot "describe_media"
$excludedNames = @(
    ".git",
    ".env",
    ".venv",
    ".venv-1",
    ".venv-recognition",
    ".venv-recognition-directml",
    ".pytest_cache",
    "__pycache__",
    "sample-output",
    ".private"
)

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

$releaseItems = @($describeMediaRoot)
if ($releaseItems.Count -eq 0) {
    throw "No files found to release from $repoRoot"
}

$sshOptions = @(
    "-i", $IdentityFile,
    "-o", "UserKnownHostsFile=$KnownHostsFile",
    "-o", "StrictHostKeyChecking=yes",
    "-o", "BatchMode=yes"
)
$target = "{0}@{1}:{2}/" -f $RemoteUser, $RemoteHost, $RemotePath

Write-Host "Copying application files to $RemoteUser@$RemoteHost`:$RemotePath (preserving remote config/.env)..."
& scp -r @sshOptions @releaseItems $target
if ($LASTEXITCODE -ne 0) {
    throw "File transfer failed with exit code $LASTEXITCODE."
}

$remoteEnvCommand = "cd -- '$RemotePath' && test -f describe_media/config/.env && sed -i -E 's|^INPUT_DIR=.*|INPUT_DIR=$InputDir|; s|^OUTPUT_DIR=.*|OUTPUT_DIR=$OutputDir|; s|^OPENAI_API_BASE=.*|OPENAI_API_BASE=$OpenAIApiBase|' describe_media/config/.env"
Write-Host "Setting the Linux input and output mount paths..."
& ssh @sshOptions "$RemoteUser@$RemoteHost" $remoteEnvCommand
if ($LASTEXITCODE -ne 0) {
    throw "Remote config/.env update failed with exit code $LASTEXITCODE."
}

if ($NoRun) {
    Write-Host "Release completed; the app was not started because -NoRun was specified."
    return
}

$upArguments = if ($NoBuild) { "up -d describe_media" } else { "up -d --build describe_media" }
$remoteCommand = "cd -- '$RemotePath' && docker compose --env-file describe_media/config/.env -f describe_media/docker-compose.yml $upArguments && docker compose --env-file describe_media/config/.env -f describe_media/docker-compose.yml ps"

Write-Host "Starting the Docker Compose app..."
& ssh @sshOptions "$RemoteUser@$RemoteHost" $remoteCommand
if ($LASTEXITCODE -ne 0) {
    throw "Remote Docker Compose command failed with exit code $LASTEXITCODE."
}

Write-Host "Release completed and the app is running."
