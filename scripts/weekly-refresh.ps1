param(
  [string]$DataRoot = 'H:\databae\geodata',
  [switch]$Recursive = $true,
  [int]$GeocodeLimit = 1000,
  [switch]$IncludePbf
)

$ErrorActionPreference = 'Stop'

$repoRoot = Join-Path $PSScriptRoot '..'
Set-Location $repoRoot

$logDir = Join-Path $PSScriptRoot 'logs'
if (-not (Test-Path $logDir)) {
  New-Item -Path $logDir -ItemType Directory | Out-Null
}

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logFile = Join-Path $logDir "weekly-refresh_$stamp.log"

function Write-Log {
  param([string]$Message)
  $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
  $line | Tee-Object -FilePath $logFile -Append
}

function Invoke-Step {
  param(
    [string]$Title,
    [scriptblock]$Action
  )

  Write-Log "START: $Title"
  try {
    & $Action
    Write-Log "DONE : $Title"
  } catch {
    Write-Log "FAIL : $Title :: $($_.Exception.Message)"
  }
}

Write-Log '============================================================'
Write-Log 'GeoScope weekly refresh started'
Write-Log "DataRoot=$DataRoot Recursive=$Recursive GeocodeLimit=$GeocodeLimit IncludePbf=$IncludePbf"

$resolvedDataRoot = $DataRoot
if (-not (Test-Path $resolvedDataRoot)) {
  if (Test-Path 'H:\databae') {
    $resolvedDataRoot = 'H:\databae'
    Write-Log "Data root fallback to $resolvedDataRoot"
  } else {
    throw "Data root not found: $DataRoot"
  }
}

Invoke-Step -Title 'Fill missing databases (local + configured downloads)' -Action {
  node scripts/fill-missing-dbs.js --source-root "$resolvedDataRoot" --download-root "downloads\\missing"
}

Invoke-Step -Title 'Bulk import all local data (CSV + vector, no PBF by default)' -Action {
  $args = @('-ExecutionPolicy', 'Bypass', '-File', (Join-Path $PSScriptRoot 'auto-import-all.ps1'), '-DataRoot', $resolvedDataRoot)
  if ($Recursive) { $args += '-Recursive' }
  if (-not $IncludePbf) { $args += '-SkipPbf' }
  powershell @args
}

Invoke-Step -Title 'Normalize categories' -Action {
  node scripts/normalize-categories.js
}

Invoke-Step -Title 'Geocode pending addresses batch' -Action {
  node scripts/geocode-pending.js --limit $GeocodeLimit
}

Invoke-Step -Title 'Report current missing database coverage' -Action {
  node scripts/check-missing-dbs.js
}

Write-Log 'GeoScope weekly refresh finished'
Write-Log "Log file: $logFile"
Write-Log '============================================================'
