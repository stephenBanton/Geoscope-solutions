# =============================================================================
# GeoScope - Auto CSV Importer for PostgreSQL
# =============================================================================
# Scans a folder for .csv files and imports each one into PostgreSQL.
# Detects the database type automatically from the filename.
#
# Usage:
#   .\scripts\auto-import.ps1 -CsvFolder "C:\Downloads\GIS Data"
#   .\scripts\auto-import.ps1 -CsvFolder "C:\Downloads\GIS Data" -Recursive
#   .\scripts\auto-import.ps1 -CsvFile "C:\Downloads\rcra_florida.csv"
# =============================================================================

param(
    [string]$CsvFolder  = "",
    [string]$CsvFile    = "",
    [switch]$Recursive  = $false,
    [switch]$SpatialOnly = $true,
    [string]$ServerDir  = "$PSScriptRoot\.."
)

Set-Location $ServerDir

function Get-UniqueFilesByHash {
    param([object[]]$Files)

    $seenHashes = @{}
    $uniqueFiles = New-Object System.Collections.Generic.List[object]

    foreach ($file in ($Files | Sort-Object FullName -Unique)) {
        try {
            $hash = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash
        } catch {
            $hash = "PATH::$($file.FullName.ToLowerInvariant())"
        }

        if ($seenHashes.ContainsKey($hash)) {
            Write-Host "       SKIP Duplicate file content: $($file.FullName)" -ForegroundColor DarkYellow
            Write-Host "            Same as: $($seenHashes[$hash])" -ForegroundColor DarkGray
            continue
        }

        $seenHashes[$hash] = $file.FullName
        $uniqueFiles.Add($file)
    }

    return $uniqueFiles
}

function Get-DbInfo {
    param([string]$FileName)

    $f = $FileName.ToLower()

    # contamination
    if ($f -match 'rcra.*lqg|lqg')               { return @{ db = 'RCRA LQG';           cat = 'contamination' } }
    if ($f -match 'rcra.*sqg|sqg')               { return @{ db = 'RCRA SQG';           cat = 'contamination' } }
    if ($f -match 'rcra.*tsdf|tsdf')             { return @{ db = 'RCRA TSDF';          cat = 'contamination' } }
    if ($f -match 'rcra.*vsqg|vsqg')             { return @{ db = 'RCRA VSQG';          cat = 'contamination' } }
    if ($f -match 'rcra')                        { return @{ db = 'RCRA LQG';           cat = 'contamination' } }
    if ($f -match 'npl.*proposed|proposed.*npl') { return @{ db = 'PROPOSED NPL';      cat = 'contamination' } }
    if ($f -match 'npl.*delist|delist.*npl')     { return @{ db = 'DELISTED NPL';      cat = 'contamination' } }
    if ($f -match '\bnpl\b|superfund|cercl')    { return @{ db = 'NPL';               cat = 'contamination' } }
    if ($f -match 'corracts?')                   { return @{ db = 'CORRACTS';          cat = 'contamination' } }
    if ($f -match 'lust')                        { return @{ db = 'EPA LUST';          cat = 'contamination' } }
    if ($f -match '\bust\b')                    { return @{ db = 'EPA UST';           cat = 'contamination' } }
    if ($f -match 'pfas.*npl')                   { return @{ db = 'PFAS NPL';          cat = 'contamination' } }
    if ($f -match 'pfas.*fed|pfas.*federal')     { return @{ db = 'PFAS FEDERAL SITES'; cat = 'contamination' } }
    if ($f -match 'pfas.*tris')                  { return @{ db = 'PFAS TRIS';         cat = 'contamination' } }
    if ($f -match 'pfas.*spill')                 { return @{ db = 'PFAS SPILLS';       cat = 'contamination' } }
    if ($f -match 'pfas')                        { return @{ db = 'PFAS NPL';          cat = 'contamination' } }
    if ($f -match '\btris\b')                   { return @{ db = 'TRIS';              cat = 'contamination' } }
    if ($f -match 'pcb')                         { return @{ db = 'PCB TRANSFORMER';   cat = 'contamination' } }
    if ($f -match 'mgp|manifest.*gas|gas.*plant'){ return @{ db = 'MGP';               cat = 'contamination' } }
    if ($f -match 'brownfield')                  { return @{ db = 'BROWNFIELDS';       cat = 'contamination' } }

    # regulatory
    if ($f -match 'echo')                        { return @{ db = 'ECHO';              cat = 'regulatory' } }
    if ($f -match 'npdes')                       { return @{ db = 'NPDES';             cat = 'regulatory' } }
    if ($f -match 'air.*facilit|air.*permit')    { return @{ db = 'AIR FACILITY';      cat = 'regulatory' } }
    if ($f -match '\brmp\b')                    { return @{ db = 'RMP';               cat = 'regulatory' } }
    if ($f -match 'icis')                        { return @{ db = 'ICIS';              cat = 'regulatory' } }
    if ($f -match '\bdod\b|defense')            { return @{ db = 'DOD';               cat = 'regulatory' } }
    if ($f -match 'fuds')                        { return @{ db = 'FUDS';              cat = 'regulatory' } }
    if ($f -match 'federal.*facilit')            { return @{ db = 'FEDERAL FACILITY';  cat = 'regulatory' } }

    # hydrology
    if ($f -match 'flood.*dfirm|dfirm')          { return @{ db = 'FLOOD DFIRM';       cat = 'hydrology' } }
    if ($f -match 'flood.*q3|q3.*flood')         { return @{ db = 'FLOOD Q3';          cat = 'hydrology' } }
    if ($f -match 'flood')                       { return @{ db = 'FLOOD DFIRM';       cat = 'hydrology' } }
    if ($f -match 'wetland|nwi')                 { return @{ db = 'WETLANDS NWI';      cat = 'hydrology' } }
    if ($f -match 'stormwater')                  { return @{ db = 'STORMWATER';        cat = 'hydrology' } }

    # geology
    if ($f -match 'mine.*oper|oper.*mine')       { return @{ db = 'MINE OPERATIONS';   cat = 'geology' } }
    if ($f -match '\bmine')                      { return @{ db = 'MINES';             cat = 'geology' } }
    if ($f -match 'radon')                       { return @{ db = 'RADON EPA';         cat = 'geology' } }
    if ($f -match 'coal.*ash')                   { return @{ db = 'COAL ASH EPA';      cat = 'geology' } }
    if ($f -match 'asbestos')                    { return @{ db = 'ASBESTOS NOA';      cat = 'geology' } }
    if ($f -match 'ssurgo')                      { return @{ db = 'SSURGO';            cat = 'geology' } }
    if ($f -match 'statsgo')                     { return @{ db = 'STATSGO';           cat = 'geology' } }
    if ($f -match 'geolog')                      { return @{ db = 'USGS GEOLOGIC AGE'; cat = 'geology' } }

    # receptors
    if ($f -match 'school.*public|public.*school')  { return @{ db = 'SCHOOLS PUBLIC';  cat = 'receptors' } }
    if ($f -match 'school.*private|private.*school'){ return @{ db = 'SCHOOLS PRIVATE'; cat = 'receptors' } }
    if ($f -match 'school')                          { return @{ db = 'SCHOOLS PUBLIC';  cat = 'receptors' } }
    if ($f -match 'hospital')                        { return @{ db = 'HOSPITALS';       cat = 'receptors' } }
    if ($f -match 'daycare|day.*care')               { return @{ db = 'DAYCARE';         cat = 'receptors' } }
    if ($f -match 'nursing')                         { return @{ db = 'NURSING HOMES';   cat = 'receptors' } }
    if ($f -match 'college|universit')               { return @{ db = 'COLLEGES';        cat = 'receptors' } }
    if ($f -match 'prison|correctional')             { return @{ db = 'PRISONS';         cat = 'receptors' } }
    if ($f -match 'pipeline')                        { return @{ db = 'PIPELINES';       cat = 'other' } }

    return @{ db = ''; cat = '' }
}

function Test-HasLatLonHeaders {
    param([string]$CsvPath)

    try {
        $header = Get-Content -Path $CsvPath -TotalCount 1
        if (-not $header) { return $false }

        $cols = $header.Split(',') | ForEach-Object { $_.Trim('"').Trim().ToLower() }

        $hasLat = ($cols -contains 'latitude') -or ($cols -contains 'lat') -or ($cols -contains 'latitude83') -or ($cols -contains 'y_coord') -or ($cols -contains 'dec_lat')
        $hasLon = ($cols -contains 'longitude') -or ($cols -contains 'lon') -or ($cols -contains 'long') -or ($cols -contains 'longitude83') -or ($cols -contains 'x_coord') -or ($cols -contains 'dec_long')

        return ($hasLat -and $hasLon)
    } catch {
        return $false
    }
}

$csvFiles = @()

if ($CsvFile -ne '') {
    if (Test-Path $CsvFile) {
        $csvFiles = @(Get-Item $CsvFile)
    } else {
        Write-Host "File not found: $CsvFile" -ForegroundColor Red
        exit 1
    }
} elseif ($CsvFolder -ne '') {
    if (-not (Test-Path $CsvFolder)) {
        Write-Host "Folder not found: $CsvFolder" -ForegroundColor Red
        exit 1
    }

    if ($Recursive) {
        $csvFiles = Get-ChildItem -Path $CsvFolder -Filter '*.csv' -Recurse
    } else {
        $csvFiles = Get-ChildItem -Path $CsvFolder -Filter '*.csv'
    }
} else {
    Write-Host ''
    Write-Host 'Usage:' -ForegroundColor Yellow
    Write-Host '  .\scripts\auto-import.ps1 -CsvFolder "C:\Downloads\GIS Data"'
    Write-Host '  .\scripts\auto-import.ps1 -CsvFile "C:\Downloads\rcra_florida.csv"'
    Write-Host '  .\scripts\auto-import.ps1 -CsvFolder "C:\Downloads" -Recursive'
    Write-Host ''
    exit 0
}

if ($csvFiles.Count -eq 0) {
    Write-Host 'No CSV files found.' -ForegroundColor Yellow
    exit 0
}

$originalCsvCount = $csvFiles.Count
$csvFiles = @(Get-UniqueFilesByHash -Files $csvFiles)

Write-Host ''
Write-Host '=== GeoScope Auto CSV Importer ===' -ForegroundColor Cyan
Write-Host "  Found $originalCsvCount CSV file(s)"
if ($originalCsvCount -ne $csvFiles.Count) {
    Write-Host "  Unique after duplicate-file filtering: $($csvFiles.Count)" -ForegroundColor Cyan
}
if ($SpatialOnly) {
    Write-Host '  SpatialOnly mode: ON (files without lat/lon headers are skipped)'
}
Write-Host ''

$total = $csvFiles.Count
$success = 0
$failed = 0
$skipped = 0
$i = 0

foreach ($file in $csvFiles) {
    $i++
    $info = Get-DbInfo -FileName $file.Name

    Write-Host "[$i/$total] $($file.Name)" -ForegroundColor White

    if ($SpatialOnly -and -not (Test-HasLatLonHeaders -CsvPath $file.FullName)) {
        $skipped++
        Write-Host '       SKIP Missing lat/lon headers' -ForegroundColor DarkYellow
        Write-Host ''
        continue
    }

    if ($info.db -ne '') {
        Write-Host "       -> Detected: $($info.db) ($($info.cat))" -ForegroundColor DarkGray
        node scripts/import-csv.js "$($file.FullName)" "$($info.db)" "$($info.cat)"
    } else {
        Write-Host '       -> No match, importing with auto-detection' -ForegroundColor DarkYellow
        node scripts/import-csv.js "$($file.FullName)"
    }

    if ($LASTEXITCODE -eq 0) {
        $success++
        Write-Host '       OK Done' -ForegroundColor Green
    } else {
        $failed++
        Write-Host '       FAIL Failed (check errors above)' -ForegroundColor Red
    }

    Write-Host ''
}

Write-Host '=== Import Complete ===' -ForegroundColor Cyan
Write-Host "  Success : $success / $total" -ForegroundColor Green
if ($failed -gt 0) {
    Write-Host "  Failed  : $failed / $total" -ForegroundColor Red
}
if ($skipped -gt 0) {
    Write-Host "  Skipped : $skipped / $total" -ForegroundColor Yellow
}
Write-Host ''
