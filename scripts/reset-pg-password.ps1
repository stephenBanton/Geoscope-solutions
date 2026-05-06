# =============================================================================
# GeoScope — PostgreSQL Password Reset (Run as Administrator)
# =============================================================================
# This script:
#   1. Temporarily sets pg_hba.conf to trust (no password required)
#   2. Restarts PostgreSQL
#   3. Sets a new password for the postgres user
#   4. Restores secure authentication
#   5. Updates your .env file automatically
# =============================================================================

param(
    [string]$NewPassword = "GeoScope2026!"
)

$PG_VERSION   = "17"
$PG_SERVICE   = "postgresql-x64-17"
$PG_BIN       = "C:\Program Files\PostgreSQL\$PG_VERSION\bin"
$PG_DATA      = "C:\Program Files\PostgreSQL\$PG_VERSION\data"
$HBA_FILE     = "$PG_DATA\pg_hba.conf"
$HBA_BACKUP   = "$PG_DATA\pg_hba.conf.backup"
$ENV_FILE     = "$PSScriptRoot\..\env"
$PSQL         = "$PG_BIN\psql.exe"

# ── Check running as admin ────────────────────────────────────────────────────
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]"Administrator")) {
    Write-Host ""
    Write-Host "ERROR: Must run as Administrator." -ForegroundColor Red
    Write-Host "Right-click PowerShell → Run as Administrator, then run this script again." -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "=== GeoScope PostgreSQL Password Reset ===" -ForegroundColor Cyan
Write-Host "  New password will be: $NewPassword"
Write-Host ""

# ── Backup pg_hba.conf ────────────────────────────────────────────────────────
Write-Host "1. Backing up pg_hba.conf..." -ForegroundColor Yellow
Copy-Item $HBA_FILE $HBA_BACKUP -Force
Write-Host "   Backup saved to $HBA_BACKUP" -ForegroundColor Green

# ── Set pg_hba.conf to trust ─────────────────────────────────────────────────
Write-Host "2. Setting pg_hba.conf to trust mode..." -ForegroundColor Yellow
$hbaContent = @"
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# IPv4 local connections (temporarily trust for password reset):
host    all             all             127.0.0.1/32            trust
# IPv6 local connections:
host    all             all             ::1/128                 trust
# Local socket
local   all             all                                     trust
"@
Set-Content -Path $HBA_FILE -Value $hbaContent -Encoding UTF8
Write-Host "   Done." -ForegroundColor Green

# ── Restart PostgreSQL ────────────────────────────────────────────────────────
Write-Host "3. Restarting PostgreSQL service..." -ForegroundColor Yellow
Restart-Service $PG_SERVICE -Force
Start-Sleep -Seconds 3
Write-Host "   Service restarted." -ForegroundColor Green

# ── Set password ─────────────────────────────────────────────────────────────
Write-Host "4. Setting new password for postgres user..." -ForegroundColor Yellow
$sql = "ALTER USER postgres WITH PASSWORD '$NewPassword';"
& $PSQL -U postgres -d postgres -c $sql
if ($LASTEXITCODE -ne 0) {
    Write-Host "   Failed to set password. Check if PostgreSQL is running." -ForegroundColor Red
    exit 1
}
Write-Host "   Password set successfully." -ForegroundColor Green

# ── Restore pg_hba.conf to scram-sha-256 ─────────────────────────────────────
Write-Host "5. Restoring secure pg_hba.conf..." -ForegroundColor Yellow
$secureHba = @"
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# IPv4 local connections:
host    all             all             127.0.0.1/32            scram-sha-256
# IPv6 local connections:
host    all             all             ::1/128                 scram-sha-256
# Local socket (Windows — use md5 for compatibility):
host    all             all             localhost               scram-sha-256
"@
Set-Content -Path $HBA_FILE -Value $secureHba -Encoding UTF8

# ── Restart PostgreSQL again ──────────────────────────────────────────────────
Restart-Service $PG_SERVICE -Force
Start-Sleep -Seconds 3
Write-Host "   Secure config restored and service restarted." -ForegroundColor Green

# ── Update .env ───────────────────────────────────────────────────────────────
Write-Host "6. Updating .env file..." -ForegroundColor Yellow
$envPath = Resolve-Path "$PSScriptRoot\..\env" -ErrorAction SilentlyContinue
# Try .env (with dot)
$envPath2 = "$PSScriptRoot\..\.env"
if (Test-Path $envPath2) {
    $envContent = Get-Content $envPath2 -Raw
    $envContent = $envContent -replace 'PG_PASSWORD=.*', "PG_PASSWORD=$NewPassword"
    Set-Content -Path $envPath2 -Value $envContent -NoNewline
    Write-Host "   .env updated at $envPath2" -ForegroundColor Green
} else {
    Write-Host "   .env not found — manually set PG_PASSWORD=$NewPassword in your .env" -ForegroundColor Yellow
}

# ── Done ─────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== Done! ===" -ForegroundColor Cyan
Write-Host "  PostgreSQL password: $NewPassword" -ForegroundColor Green
Write-Host "  .env updated: PG_PASSWORD=$NewPassword" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "  1. In DBeaver: enter password '$NewPassword' and Test Connection"
Write-Host "  2. Create database 'geoscope' in DBeaver"
Write-Host "  3. Run schema.sql in DBeaver"
Write-Host "  4. Run: node scripts/auto-import.ps1 <your-csv-folder>"
Write-Host ""
