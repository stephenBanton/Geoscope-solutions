param(
  [string]$TaskName = "GeoScope-DBeaver-Export",
  [string]$Interval = "HOURLY",
  [string]$StartTime = "08:00",
  [string]$ScriptPath = "C:\Users\Admin\Desktop\WEBSITE\geoscope\scripts\export-to-dbeaver.ps1"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $ScriptPath)) {
  throw "Export script not found: $ScriptPath"
}

$command = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""

# Create/replace a user-level task that runs even when elevated rights are not available.
& schtasks.exe /Create /F /TN $TaskName /SC $Interval /ST $StartTime /TR $command | Out-Null

Write-Host "Task registered: $TaskName" -ForegroundColor Green
Write-Host "Schedule: $Interval starting at $StartTime" -ForegroundColor Green
Write-Host "Command: $command" -ForegroundColor Green
