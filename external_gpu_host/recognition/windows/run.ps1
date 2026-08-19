[CmdletBinding()]
param(
    [string]$Host = "127.0.0.1",
    [int]$Port = 5002,
    [string]$Model = $(if ($env:RECOGNITION_MODEL) { $env:RECOGNITION_MODEL } else { "buffalo_l" }),
    [string]$Token = $env:RECOGNITION_API_TOKEN,
    [string]$Python = "py"
)

$apiToken = $Token
if (-not $apiToken) {
    throw "RECOGNITION_API_TOKEN is required. Set it in the environment or pass -Token."
}

$worker = Join-Path $PSScriptRoot "..\worker\server.py"
& $Python $worker --host $Host --port $Port --model $Model --provider DmlExecutionProvider --token $apiToken
exit $LASTEXITCODE
