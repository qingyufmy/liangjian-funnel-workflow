param(
    [ValidateSet('run-due', 'run-morning', 'run-close', 'run-monitor')]
    [string]$Command = 'run-due'
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $projectRoot '.venv\Scripts\python.exe'
$logDirectory = Join-Path $projectRoot 'outputs\scheduler'

if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Project virtual environment is missing: $python"
}

New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
$env:LIANGJIAN_ROOT = $projectRoot
$stamp = Get-Date -Format 'yyyy-MM-dd'
$logPath = Join-Path $logDirectory "scheduler-$stamp.log"

$result = & $python -m liangjian_funnel $Command 2>&1
$exitCode = $LASTEXITCODE
$result | ForEach-Object { "$(Get-Date -Format o) $_" } | Add-Content -LiteralPath $logPath -Encoding UTF8
exit $exitCode
