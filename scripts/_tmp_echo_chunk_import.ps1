$src = 'H:\databae\geodata\ECHO_EXPORTER.csv'
$chunkDir = 'H:\databae\geodata\_chunks_echo'
$rowsPerChunk = 250000

if (!(Test-Path $chunkDir)) { New-Item -ItemType Directory -Path $chunkDir | Out-Null }
Get-ChildItem $chunkDir -Filter 'ECHO_EXPORTER_part*.csv' -ErrorAction SilentlyContinue | Remove-Item -Force

$reader = [System.IO.StreamReader]::new($src)
$header = $reader.ReadLine()
$part = 1
$row = 0
$writer = $null

function New-ChunkWriter([int]$p, [string]$dir, [string]$h) {
  $path = Join-Path $dir ("ECHO_EXPORTER_part{0:D3}.csv" -f $p)
  $w = [System.IO.StreamWriter]::new($path, $false, [System.Text.Encoding]::UTF8)
  $w.WriteLine($h)
  return $w
}

$writer = New-ChunkWriter -p $part -dir $chunkDir -h $header
while (($line = $reader.ReadLine()) -ne $null) {
  if ($row -gt 0 -and ($row % $rowsPerChunk) -eq 0) {
    $writer.Flush(); $writer.Dispose()
    $part++
    $writer = New-ChunkWriter -p $part -dir $chunkDir -h $header
  }
  $writer.WriteLine($line)
  $row++
}
$writer.Flush(); $writer.Dispose(); $reader.Dispose()
Write-Host "CHUNKS_CREATED=$part ROWS=$row"

$ok=0; $fail=0
$chunks = Get-ChildItem $chunkDir -Filter 'ECHO_EXPORTER_part*.csv' | Sort-Object Name
foreach ($c in $chunks) {
  Write-Host "\n=== Importing $($c.Name) ==="
  node .\scripts\import-csv.js "$($c.FullName)" "ECHO" "regulatory"
  if ($LASTEXITCODE -eq 0) { $ok++ } else { $fail++ }
}
Write-Host "\nECHO_CHUNK_IMPORT_OK=$ok FAIL=$fail"
