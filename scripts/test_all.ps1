param(
    [string]$PythonPath,
    [string]$OutputDirectory
)
$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
if (-not $PythonPath) { $PythonPath = Join-Path $taskRoot '.venv/Scripts/python.exe' }
if (-not (Test-Path -LiteralPath $PythonPath -PathType Leaf)) { throw "Python missing: $PythonPath" }
$taskArguments = @((Join-Path $PSScriptRoot 'full_test_baseline.py'), '--python', $PythonPath)
if ($OutputDirectory) { $taskArguments += @('--output', $OutputDirectory) }
& $PythonPath @taskArguments
exit $LASTEXITCODE
