param(
  [string]$ApiBase = "http://localhost:6001",
  [string]$Email = "admin@geoscope.com",
  [string]$Password = "1234",
  [string]$OutputDir = "C:\temp\geoscope-export",
  [string]$CsvFileName = "environmental-sites.csv",
  [string]$SqlFileName = "environmental-sites-copy.sql",
  [switch]$OpenDBeaver
)

$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) {
  Write-Host "[GeoScope] $Message" -ForegroundColor Cyan
}

function Test-ApiHealth([string]$BaseUrl) {
  try {
    $null = Invoke-WebRequest -Uri "$BaseUrl/health" -TimeoutSec 4
    return $true
  } catch {
    return $false
  }
}

function Start-BackendIfNeeded([string]$BaseUrl) {
  if (Test-ApiHealth $BaseUrl) {
    Write-Step "Backend is already online."
    return
  }

  $repoRoot = Split-Path -Parent $PSScriptRoot
  Write-Step "Backend is offline. Starting server.js..."
  Start-Process -FilePath "node" -ArgumentList "server.js" -WorkingDirectory $repoRoot | Out-Null

  for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Milliseconds 750
    if (Test-ApiHealth $BaseUrl) {
      Write-Step "Backend started successfully."
      return
    }
  }

  throw "Backend did not start in time at $BaseUrl"
}

function Get-AuthToken([string]$BaseUrl, [string]$UserEmail, [string]$UserPassword) {
  $body = @{ email = $UserEmail; password = $UserPassword } | ConvertTo-Json
  $login = Invoke-RestMethod -Uri "$BaseUrl/auth/login" -Method Post -ContentType "application/json" -Body $body -TimeoutSec 12
  if (-not $login.token) {
    throw "Login succeeded but no token was returned."
  }
  return $login.token
}

function Resolve-DBeaverPath {
  $candidates = @(
    "$env:ProgramFiles\DBeaver\dbeaver.exe",
    "$env:ProgramFiles\DBeaver Community\dbeaver.exe",
    "$env:LOCALAPPDATA\DBeaver\dbeaver.exe",
    "$env:LOCALAPPDATA\Programs\DBeaver\dbeaver.exe"
  )

  foreach ($path in $candidates) {
    if (Test-Path $path) { return $path }
  }

  return $null
}

Write-Step "Preparing output folder: $OutputDir"
New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

Start-BackendIfNeeded $ApiBase

Write-Step "Authenticating as $Email"
$token = Get-AuthToken -BaseUrl $ApiBase -UserEmail $Email -UserPassword $Password
$headers = @{ Authorization = "Bearer $token" }

$csvPath = Join-Path $OutputDir $CsvFileName
$sqlPath = Join-Path $OutputDir $SqlFileName
$csvPathForServer = ($csvPath -replace "\\", "/")
$csvPathEscaped = [System.Uri]::EscapeDataString($csvPathForServer)

Write-Step "Downloading CSV export..."
Invoke-WebRequest -Uri "$ApiBase/data/export/dbeaver/environmental-sites.csv" -Headers $headers -OutFile $csvPath -TimeoutSec 60

Write-Step "Downloading PostgreSQL COPY SQL script..."
Invoke-WebRequest -Uri "$ApiBase/data/export/postgres/environmental-sites-copy.sql?csvPath=$csvPathEscaped" -Headers $headers -OutFile $sqlPath -TimeoutSec 60

Write-Step "Export complete."
Write-Host "CSV: $csvPath" -ForegroundColor Green
Write-Host "SQL: $sqlPath" -ForegroundColor Green

if ($OpenDBeaver) {
  $dbeaverPath = Resolve-DBeaverPath
  if ($dbeaverPath) {
    Write-Step "Launching DBeaver..."
    Start-Process -FilePath $dbeaverPath | Out-Null
    Start-Sleep -Seconds 2
    Start-Process -FilePath $sqlPath | Out-Null
  } else {
    Write-Host "DBeaver executable not found. Open SQL file manually in DBeaver:" -ForegroundColor Yellow
    Write-Host $sqlPath -ForegroundColor Yellow
  }
}
