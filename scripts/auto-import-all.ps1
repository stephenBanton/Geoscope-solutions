param(
  [string]$DataRoot = 'H:\databae',
  [switch]$Recursive = $true,
  [switch]$SkipCsv,
  [switch]$SkipVector,
  [switch]$SkipPbf,
  [switch]$SpatialOnly = $true
)

$ErrorActionPreference = 'Stop'

$serverRoot = Join-Path $PSScriptRoot '..'
Set-Location $serverRoot

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
      Write-Host "Skipping duplicate file content: $($file.FullName)" -ForegroundColor DarkYellow
      Write-Host "  Same as: $($seenHashes[$hash])" -ForegroundColor DarkGray
      continue
    }

    $seenHashes[$hash] = $file.FullName
    $uniqueFiles.Add($file)
  }

  return $uniqueFiles
}

function Get-EnvValue {
  param(
    [string]$Path,
    [string]$Key,
    [string]$Default = ''
  )

  if (-not (Test-Path $Path)) {
    return $Default
  }

  $line = Get-Content $Path | Where-Object { $_ -match "^$Key=" } | Select-Object -First 1
  if (-not $line) {
    return $Default
  }

  return (($line -replace "^$Key=", '').Trim())
}

function Get-SafeTableName {
  param([string]$Name)
  $safe = $Name.ToLower() -replace '[^a-z0-9]+', '_'
  $safe = $safe.Trim('_')
  if ($safe.Length -gt 50) {
    $safe = $safe.Substring(0, 50)
  }
  if ([string]::IsNullOrWhiteSpace($safe)) {
    return 'raw_unknown'
  }
  return "raw_$safe"
}

if (-not (Test-Path $DataRoot)) {
  throw "Data folder not found: $DataRoot"
}

$envPath = Join-Path $serverRoot '.env'
$pgHost = Get-EnvValue -Path $envPath -Key 'PG_HOST' -Default 'localhost'
$pgPort = Get-EnvValue -Path $envPath -Key 'PG_PORT' -Default '5432'
$pgDb = Get-EnvValue -Path $envPath -Key 'PG_DATABASE' -Default 'geoscope'
$pgUser = Get-EnvValue -Path $envPath -Key 'PG_USER' -Default 'postgres'
$pgPass = Get-EnvValue -Path $envPath -Key 'PG_PASSWORD' -Default ''

$ogr2ogrCandidates = @(
  'C:\Program Files\QGIS 3.44.8\bin\ogr2ogr.exe',
  'C:\Program Files\QGIS 3.44.8\apps\gdal\bin\ogr2ogr.exe',
  'C:\OSGeo4W\bin\ogr2ogr.exe'
)

$ogrinfoCandidates = @(
  'C:\Program Files\QGIS 3.44.8\bin\ogrinfo.exe',
  'C:\Program Files\QGIS 3.44.8\apps\gdal\bin\ogrinfo.exe',
  'C:\OSGeo4W\bin\ogrinfo.exe'
)

$ogr2ogr = $null
$ogrinfo = $null
$gdalData = $null
foreach ($c in $ogr2ogrCandidates) {
  if (Test-Path $c) { $ogr2ogr = $c; break }
}
foreach ($c in $ogrinfoCandidates) {
  if (Test-Path $c) { $ogrinfo = $c; break }
}

$gdalCandidates = @(
  'C:\Program Files\QGIS 3.44.8\apps\gdal\share\gdal',
  'C:\OSGeo4W\share\gdal'
)
foreach ($gc in $gdalCandidates) {
  if (Test-Path $gc) {
    $gdalData = $gc
    break
  }
}

$projLib = $null
$projCandidates = @(
  'C:\Program Files\QGIS 3.44.8\share\proj',
  'C:\OSGeo4W\share\proj'
)
foreach ($pc in $projCandidates) {
  if (Test-Path $pc) {
    $projLib = $pc
    break
  }
}

if ((-not $SkipVector) -and (-not $ogr2ogr -or -not $ogrinfo)) {
  throw 'ogr2ogr/ogrinfo not found. Install QGIS or OSGeo4W first.'
}

$env:PGPASSWORD = $pgPass
$env:GDAL_DATA = if ($gdalData) { $gdalData } else { $env:GDAL_DATA }
$env:PROJ_LIB = if ($projLib) { $projLib } else { $env:PROJ_LIB }
$pgConn = "PG:host=$pgHost port=$pgPort dbname=$pgDb user=$pgUser password=$pgPass"

Write-Host "Data root      : $DataRoot" -ForegroundColor Cyan
Write-Host "PostgreSQL DB  : $pgDb@$pgHost`:$pgPort" -ForegroundColor Cyan
Write-Host "GDAL_DATA      : $($env:GDAL_DATA)" -ForegroundColor Cyan
Write-Host "PROJ_LIB       : $($env:PROJ_LIB)" -ForegroundColor Cyan
Write-Host "ogr2ogr        : $ogr2ogr" -ForegroundColor Cyan
Write-Host "Include CSV    : $(-not $SkipCsv)" -ForegroundColor Cyan
Write-Host "Include Vector : $(-not $SkipVector)" -ForegroundColor Cyan
Write-Host "Include PBF    : $(-not $SkipPbf)" -ForegroundColor Cyan
Write-Host ''

$ok = 0
$fail = 0
$skip = 0

try {
  if (-not $SkipCsv) {
    Write-Host 'Step 1/3: CSV import pass' -ForegroundColor Yellow
    $csvArgs = @('-CsvFolder', $DataRoot)
    if ($Recursive) { $csvArgs += '-Recursive' }
    if ($SpatialOnly) { $csvArgs += '-SpatialOnly' }

    & powershell -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'auto-import.ps1') @csvArgs
    if ($LASTEXITCODE -ne 0) {
      Write-Host 'CSV pass completed with some failures.' -ForegroundColor DarkYellow
    }
    Write-Host ''
  }

  if (-not $SkipVector) {
    Write-Host 'Step 2/3: Non-CSV vector import pass' -ForegroundColor Yellow

    $filePatterns = @('*.shp', '*.geojson', '*.json', '*.kml', '*.kmz', '*.gpkg')
    $vectorFiles = @()
    foreach ($p in $filePatterns) {
      if ($Recursive) {
        $vectorFiles += Get-ChildItem -Path $DataRoot -Filter $p -File -Recurse -ErrorAction SilentlyContinue
      } else {
        $vectorFiles += Get-ChildItem -Path $DataRoot -Filter $p -File -ErrorAction SilentlyContinue
      }
    }

    $originalVectorCount = $vectorFiles.Count
    $vectorFiles = @(Get-UniqueFilesByHash -Files $vectorFiles)
    if ($originalVectorCount -ne $vectorFiles.Count) {
      Write-Host "Filtered vector duplicates: $originalVectorCount -> $($vectorFiles.Count) unique files" -ForegroundColor Cyan
    }

    $fgdbDirs = @()
    if ($Recursive) {
      $fgdbDirs = Get-ChildItem -Path $DataRoot -Directory -Filter '*.gdb' -Recurse -ErrorAction SilentlyContinue
    } else {
      $fgdbDirs = Get-ChildItem -Path $DataRoot -Directory -Filter '*.gdb' -ErrorAction SilentlyContinue
    }

    foreach ($vf in $vectorFiles) {
      $base = [System.IO.Path]::GetFileNameWithoutExtension($vf.Name)
      $tbl = Get-SafeTableName -Name $base
      Write-Host "Importing vector file -> public.$tbl : $($vf.FullName)"

      & $ogr2ogr -f PostgreSQL $pgConn $vf.FullName -nln "public.$tbl" -nlt PROMOTE_TO_MULTI -lco GEOMETRY_NAME=geom -lco OVERWRITE=YES -lco PRECISION=NO -overwrite -s_srs EPSG:4326 -t_srs EPSG:4326 -skipfailures -progress
      if ($LASTEXITCODE -eq 0) {
        $ok++
      } else {
        $fail++
        Write-Host "Failed: $($vf.FullName)" -ForegroundColor Red
      }
    }

    foreach ($gdb in $fgdbDirs) {
      Write-Host "Scanning GDB layers: $($gdb.FullName)"
      $layers = & $ogrinfo -ro -so $gdb.FullName 2>$null | Select-String -Pattern '^[0-9]+:' | ForEach-Object {
        ($_.Line -split ':', 2)[1].Trim()
      }

      if (-not $layers -or $layers.Count -eq 0) {
        $skip++
        continue
      }

      foreach ($layer in $layers) {
        $tbl = Get-SafeTableName -Name ("$($gdb.BaseName)_$layer")
        Write-Host "Importing GDB layer -> public.$tbl : $layer"
        & $ogr2ogr -f PostgreSQL $pgConn $gdb.FullName -nln "public.$tbl" -nlt PROMOTE_TO_MULTI -lco GEOMETRY_NAME=geom -lco OVERWRITE=YES -lco PRECISION=NO -overwrite -t_srs EPSG:4326 -skipfailures -progress $layer
        if ($LASTEXITCODE -eq 0) {
          $ok++
        } else {
          $fail++
          Write-Host "Failed layer: $layer ($($gdb.Name))" -ForegroundColor Red
        }
      }
    }

    Write-Host ''
  }

  if (-not $SkipPbf) {
    Write-Host 'Step 3/3: OSM PBF import pass' -ForegroundColor Yellow
    $pbfFiles = @()
    if ($Recursive) {
      $pbfFiles = Get-ChildItem -Path $DataRoot -Filter '*.pbf' -File -Recurse -ErrorAction SilentlyContinue
    } else {
      $pbfFiles = Get-ChildItem -Path $DataRoot -Filter '*.pbf' -File -ErrorAction SilentlyContinue
    }

    $originalPbfCount = $pbfFiles.Count
    $pbfFiles = @(Get-UniqueFilesByHash -Files $pbfFiles)
    if ($originalPbfCount -ne $pbfFiles.Count) {
      Write-Host "Filtered PBF duplicates: $originalPbfCount -> $($pbfFiles.Count) unique files" -ForegroundColor Cyan
    }

    foreach ($pf in $pbfFiles) {
      Write-Host "Importing PBF via import-osm-pbf.ps1: $($pf.FullName)"
      & powershell -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'import-osm-pbf.ps1') -PbfPath $pf.FullName -Mode append -PGHost $pgHost -Port ([int]$pgPort) -Database $pgDb -User $pgUser -Password $pgPass
      if ($LASTEXITCODE -eq 0) {
        $ok++
      } else {
        $fail++
        Write-Host "Failed PBF: $($pf.FullName)" -ForegroundColor Red
      }
    }

    Write-Host ''
  }

  Write-Host '=== All Imports Complete ===' -ForegroundColor Cyan
  Write-Host "Succeeded: $ok" -ForegroundColor Green
  Write-Host "Failed   : $fail" -ForegroundColor Red
  Write-Host "Skipped  : $skip" -ForegroundColor Yellow
}
finally {
  Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}
