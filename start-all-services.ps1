$root = Split-Path -Parent $MyInvocation.MyCommand.Path

$services = @(
    @{ Name = "api-gateway"; Path = Join-Path $root "api-gateway"; Url = "http://localhost:8000" },
    @{ Name = "CMS"; Path = Join-Path $root "CMS"; Url = "http://localhost:8080" },
    @{ Name = "cms_adapter"; Path = Join-Path $root "cms_adapter"; Url = "http://localhost:8003" },
    @{ Name = "mock-ros-cloud"; Path = Join-Path $root "mock-ros-cloud"; Url = "http://localhost:8082" },
    @{ Name = "mock-wms-system"; Path = Join-Path $root "mock-wms-system"; Url = "http://localhost:8081" },
    @{ Name = "order_intake"; Path = Join-Path $root "order_intake"; Url = "http://localhost:8001" },
    @{ Name = "order_processing"; Path = Join-Path $root "order_processing"; Url = "http://localhost:8002" },
    @{ Name = "notification-tracking-service"; Path = Join-Path $root "notification-tracking-service"; Url = "http://localhost:8007" },
    @{ Name = "ros-adapter"; Path = Join-Path $root "ros-adapter"; Url = "http://localhost:8005" },
    @{ Name = "route-driver-service"; Path = Join-Path $root "route-driver-service"; Url = "http://localhost:8006" },
    @{ Name = "wms_adapter"; Path = Join-Path $root "wms_adapter"; Url = "http://localhost:8004" }
)

foreach ($service in $services) {
    $command = @"
Set-Location -Path '$($service.Path)'
.\mvnw.cmd spring-boot:run
"@

    Write-Host "Starting $($service.Name)... [$($service.Url)]"
    Start-Process powershell -ArgumentList '-NoExit','-Command', $command -WorkingDirectory $root
    Start-Sleep -Seconds 2
}

Write-Host "All services started in separate terminals."
