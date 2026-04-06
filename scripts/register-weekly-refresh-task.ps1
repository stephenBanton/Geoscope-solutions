param(
  [string]$TaskName = 'GeoScope-Weekly-Refresh',
  [string]$Day = 'SUN',
  [string]$StartTime = '02:00',
  [string]$DataRoot = 'H:\databae\geodata',
  [int]$GeocodeLimit = 1000,
  [switch]$IncludePbf
)

$ErrorActionPreference = 'Stop'

$scriptPath = Join-Path $PSScriptRoot 'weekly-refresh.ps1'
if (-not (Test-Path $scriptPath)) {
  throw "Weekly refresh script not found: $scriptPath"
}

$cmd = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" -DataRoot `"$DataRoot`" -GeocodeLimit $GeocodeLimit"
if ($IncludePbf) {
  $cmd += ' -IncludePbf'
}

& schtasks.exe /Create /F /TN $TaskName /SC WEEKLY /D $Day /ST $StartTime /TR $cmd | Out-Null

Write-Host "Task registered: $TaskName" -ForegroundColor Green
Write-Host "Schedule      : WEEKLY on $Day at $StartTime" -ForegroundColor Green
Write-Host "Data root     : $DataRoot" -ForegroundColor Green
Write-Host "Command       : $cmd" -ForegroundColor Green
