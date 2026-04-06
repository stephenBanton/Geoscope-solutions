$ErrorActionPreference = 'Stop'

$health = Invoke-WebRequest -Uri 'http://localhost:6001/health' -TimeoutSec 8 -ErrorAction SilentlyContinue
if(-not $health){ Write-Output 'HEALTH_FAIL'; exit 1 }

$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$project = "Florida Test $stamp"
$clientPayload = @{
  project_name = $project
  client_company = 'Florida QA Co'
  recipient_email_1 = 'nyangelos4@gmail.com'
  address = 'Miami, Florida'
  latitude = '25.7617'
  longitude = '-80.1918'
  notes = 'Automated Florida order test'
} | ConvertTo-Json

$clientRes = Invoke-WebRequest -Uri 'http://localhost:6001/client-orders' -Method Post -ContentType 'application/json' -Body $clientPayload -TimeoutSec 20
$clientJson = $clientRes.Content | ConvertFrom-Json
$createdId = if($clientJson.id){$clientJson.id}else{$clientJson.order_id}
Write-Output ('CREATE_HTTP=' + $clientRes.StatusCode + ';ORDER_ID=' + $createdId + ';PROJECT=' + $project)

$loginBody='{"email":"analyst@geoscope.com","password":"1234"}'
$login = Invoke-WebRequest -Uri 'http://localhost:6001/auth/login' -Method Post -ContentType 'application/json' -Body $loginBody -TimeoutSec 12
$token = ($login.Content | ConvertFrom-Json).token
$headers=@{ Authorization = "Bearer $token" }

$orders = Invoke-RestMethod -Uri 'http://localhost:6001/orders' -Headers $headers -TimeoutSec 20
$target = @($orders) | Where-Object { ($_.project_name -eq $project) -or ($_.id -eq $createdId) -or ($_.order_id -eq $createdId) } | Select-Object -First 1
if(-not $target){ Write-Output 'ORDER_LOOKUP_FAIL'; exit 1 }
$orderId = if($target.id){$target.id}else{$target.order_id}
Write-Output ('ANALYST_ORDER_FOUND=' + $orderId)

$gis = Invoke-RestMethod -Uri 'http://localhost:6001/nearby-search?lat=25.7617&lng=-80.1918&radius=1' -TimeoutSec 40
$sites = @($gis.results) | Select-Object -First 40 | ForEach-Object {
  $miles = [math]::Round(([double]$_.distance_m / 1609.344), 2)
  @{
    id = $_.id
    name = $_.site_name
    database = $_.database
    address = $_.address
    distance = $miles.ToString() + ' mi'
    status = $_.status
    elevation = 'N/A'
    direction = 'N/A'
    lat = $_.lat
    lng = $_.lng
  }
}
Write-Output ('GIS_TOTAL=' + $gis.summary.total)

$reportBody = @{
  order_id = $orderId
  paid = $true
  project_name = $target.project_name
  client_name = $(if($target.client_name){$target.client_name}else{$target.client_company})
  address = $target.address
  latitude = $(if($target.latitude){[double]$target.latitude}else{25.7617})
  longitude = $(if($target.longitude){[double]$target.longitude}else{-80.1918})
  summary = 'Florida order automated analyst test report.'
  environmentalData = @{
    environmentalSites = $sites
    floodZones = @()
    schools = @()
    governmentRecords = @()
    rainfall = @()
  }
} | ConvertTo-Json -Depth 8

try {
  $gen = Invoke-WebRequest -Uri 'http://localhost:6001/generate-report' -Method Post -Headers $headers -ContentType 'application/json' -Body $reportBody -TimeoutSec 180
  $download = ''
  try {
    $genJson = $gen.Content | ConvertFrom-Json
    $download = $genJson.downloadUrl
  } catch {
    $download = 'unparsed'
  }
  Write-Output ('GENERATE_HTTP=' + $gen.StatusCode + ';DOWNLOAD=' + $download)
} catch {
  if($_.Exception.Response){
    $code = [int]$_.Exception.Response.StatusCode.value__
    $body = ''
    try {
      $sr=New-Object IO.StreamReader($_.Exception.Response.GetResponseStream())
      $body = $sr.ReadToEnd()
    } catch {
      $body = $_.Exception.Message
    }
    Write-Output ('GENERATE_FAIL_HTTP=' + $code + ';BODY=' + $body)
  } else {
    Write-Output ('GENERATE_FAIL=' + $_.Exception.Message)
  }
  exit 1
}

$dl = Invoke-WebRequest -Uri ("http://localhost:6001/download/" + $orderId) -Headers $headers -Method Get -TimeoutSec 30 -ErrorAction SilentlyContinue
if($dl){ Write-Output ('DOWNLOAD_HTTP=' + $dl.StatusCode) } else { Write-Output 'DOWNLOAD_HTTP=NO_RESPONSE' }
