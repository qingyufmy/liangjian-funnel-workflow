param([switch]$Uninstall)
$ErrorActionPreference = 'Stop'
$a4TaskName = 'Liangjian-A4-External-Watchdog'
if ($Uninstall) {
    Unregister-ScheduledTask -TaskName $a4TaskName -Confirm:$false
    return
}
$a4Root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$a4Python = Join-Path $a4Root '.venv\Scripts\python.exe'
$a4Script = Join-Path $a4Root 'scripts\watch_a4_runtime.py'
$a4Webhook = Join-Path $a4Root 'state\a4_watchdog\lark_webhook.json'
foreach ($a4Path in @($a4Python, $a4Script, $a4Webhook)) {
    if (-not (Test-Path -LiteralPath $a4Path -PathType Leaf)) { throw "Missing watchdog dependency: $a4Path" }
}
$a4PythonWindowless = Join-Path $a4Root '.venv\Scripts\pythonw.exe'
if (-not (Test-Path -LiteralPath $a4PythonWindowless)) { throw 'pythonw.exe is required for hidden execution' }
$a4Action = New-ScheduledTaskAction -Execute $a4PythonWindowless -Argument ('"' + $a4Script + '"') -WorkingDirectory $a4Root
$a4Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)
$a4Principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
$a4Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Seconds 55) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $a4TaskName -Action $a4Action -Trigger $a4Trigger -Principal $a4Principal -Settings $a4Settings -Description 'Read-only A4 health probe outside VM; Lark outage/recovery alerts; no trading or restart actions.' -Force | Select-Object TaskName,State
