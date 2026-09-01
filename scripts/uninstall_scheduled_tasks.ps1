param()

$ErrorActionPreference = 'Stop'
foreach ($name in 'LiangjianAStockResearchPremarket', 'LiangjianAStockResearchMorning', 'LiangjianAStockResearchClose', 'LiangjianAStockMonitor') {
    & schtasks.exe /Delete /F /TN $name 2>$null
}
