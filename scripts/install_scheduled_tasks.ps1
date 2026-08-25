param()

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot 'run_due.ps1'
$python = Join-Path $projectRoot '.venv\Scripts\python.exe'

if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) {
    throw "Runner is missing: $runner"
}
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Run .\.venv\Scripts\python.exe -m pip install -e '.[dev]' first."
}

$taskCommand = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$runner`""
$definitions = @(
    @('/Create', '/F', '/TN', 'LiangjianAStockResearchMorning', '/TR', $taskCommand, '/SC', 'DAILY', '/ST', '09:25'),
    @('/Create', '/F', '/TN', 'LiangjianAStockResearchClose', '/TR', $taskCommand, '/SC', 'DAILY', '/ST', '15:10'),
    @('/Create', '/F', '/TN', 'LiangjianAStockMonitor', '/TR', $taskCommand, '/SC', 'DAILY', '/ST', '09:25')
)

foreach ($arguments in $definitions) {
    & schtasks.exe @arguments | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create scheduled task $($arguments[4])"
    }
}

# schtasks.exe cannot express a daily trigger whose minute repetition resets
# each day.  Create a normal daily trigger first, then add the documented XML
# repetition interval/duration while preserving the generated principal and
# action settings for the current Windows account.
[xml]$monitorXml = Export-ScheduledTask -TaskName 'LiangjianAStockMonitor'
$namespace = $monitorXml.DocumentElement.NamespaceURI
$trigger = $monitorXml.Task.Triggers.CalendarTrigger
$repetition = $monitorXml.CreateElement('Repetition', $namespace)
$interval = $monitorXml.CreateElement('Interval', $namespace)
$interval.InnerText = 'PT1M'
$duration = $monitorXml.CreateElement('Duration', $namespace)
$duration.InnerText = 'PT5H45M'
$stop = $monitorXml.CreateElement('StopAtDurationEnd', $namespace)
$stop.InnerText = 'true'
[void]$repetition.AppendChild($interval)
[void]$repetition.AppendChild($duration)
[void]$repetition.AppendChild($stop)
[void]$trigger.PrependChild($repetition)
Register-ScheduledTask -TaskName 'LiangjianAStockMonitor' -Xml $monitorXml.OuterXml -Force | Out-Null

Write-Output 'Scheduled tasks installed. The internal Shanghai scheduler skips weekends, lunch, duplicate dispatches and stale monitor catch-up.'
