param(
  [string]$PbfPath = "H:\databae\north-america-latest.osm.pbf",
  [ValidateSet('create','append')]
  [string]$Mode = 'append',
  [string]$PGHost = 'localhost',
  [int]$Port = 5432,
  [string]$Database = 'geoscope',
  [string]$User = 'postgres',
  [string]$Password = '2030'
)

$ErrorActionPreference = 'Stop'

if (!(Test-Path $PbfPath)) {
  throw "PBF file not found: $PbfPath"
}

if ($PbfPath -like '*.crdownload') {
  throw "File appears incomplete (.crdownload): $PbfPath"
}

$osm2pgsql = Get-Command osm2pgsql -ErrorAction SilentlyContinue
$ogr2ogr = $null
$gdalData = $null

if (-not $osm2pgsql) {
  $ogrCandidates = @(
    'C:\Program Files\QGIS 3.44.8\bin\ogr2ogr.exe',
    'C:\Program Files\QGIS 3.44.8\apps\gdal\bin\ogr2ogr.exe',
    'C:\OSGeo4W\bin\ogr2ogr.exe'
  )
  foreach ($candidate in $ogrCandidates) {
    if (Test-Path $candidate) {
      $ogr2ogr = $candidate
      break
    }
  }

  if ($ogr2ogr) {
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
    
    $projCandidates = @(
      'C:\Program Files\QGIS 3.44.8\share\proj',
      'C:\OSGeo4W\share\proj'
    )
    $projLib = $null
    foreach ($pc in $projCandidates) {
      if (Test-Path $pc) {
        $projLib = $pc
        break
      }
    }
  }
}

if ($osm2pgsql) {
  Write-Host "Using osm2pgsql: $($osm2pgsql.Source)"
} elseif ($ogr2ogr) {
  Write-Host "osm2pgsql not found; using ogr2ogr fallback: $ogr2ogr" -ForegroundColor Yellow
} else {
  Write-Host "Neither osm2pgsql nor ogr2ogr was found." -ForegroundColor Yellow
  Write-Host "Install osm2pgsql or QGIS (includes ogr2ogr), then rerun this script." -ForegroundColor Yellow
  exit 1
}

Write-Host "Import mode: $Mode"
Write-Host "Source PBF: $PbfPath"

$env:PGPASSWORD = $Password
try {
  if ($osm2pgsql) {
    $modeArg = if ($Mode -eq 'create') { '--create' } else { '--append' }

    & osm2pgsql `
      $modeArg `
      --slim `
      --hstore `
      --latlong `
      --cache 2000 `
      --number-processes 4 `
      -H $PGHost `
      -P $Port `
      -U $User `
      -d $Database `
      "$PbfPath"

    if ($LASTEXITCODE -ne 0) {
      throw "osm2pgsql import failed with exit code $LASTEXITCODE"
    }
  } else {
    if ($gdalData) {
      $env:GDAL_DATA = $gdalData
    }
    if ($projLib) {
      $env:PROJ_LIB = $projLib
    }

    $pgConn = "PG:host=$PGHost port=$Port dbname=$Database user=$User password=$Password"
    $modeArg = if ($Mode -eq 'create') { '-overwrite' } else { '-append' }

    & $ogr2ogr `
      -f PostgreSQL `
      $pgConn `
      "$PbfPath" `
      -dialect SQLITE `
      -sql "SELECT osm_id, name, highway, other_tags, geometry FROM lines WHERE highway IS NOT NULL" `
      -nln osm_lines_raw `
      -lco GEOMETRY_NAME=geom `
      -nlt LINESTRING `
      $modeArg `
      -progress

    if ($LASTEXITCODE -ne 0) {
      throw "ogr2ogr OSM import failed with exit code $LASTEXITCODE"
    }
  }

  Write-Host "OSM raw import completed. Loading roads into area_features..." -ForegroundColor Cyan
  Push-Location (Join-Path $PSScriptRoot '..')
  node .\scripts\load-osm-roads.js
  if ($LASTEXITCODE -ne 0) {
    throw "Road loading script failed with exit code $LASTEXITCODE"
  }
  Pop-Location

  Write-Host "Done. OSM roads are available in PostgreSQL table: area_features" -ForegroundColor Green
}
finally {
  Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}
