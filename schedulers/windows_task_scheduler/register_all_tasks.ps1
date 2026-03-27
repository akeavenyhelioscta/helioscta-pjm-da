# Register All Scheduled Tasks
# Finds and executes all .ps1 task scripts in subdirectories.
# Run as Administrator: powershell -ExecutionPolicy Bypass -File register_all_tasks.ps1

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$excludedScripts = @("register_all_tasks.ps1", "delete_all_tasks.ps1")
$allScripts = Get-ChildItem -Path $scriptDir -Filter "*.ps1" -Recurse | Where-Object { $_.Name -notin $excludedScripts }

Write-Host "Found $($allScripts.Count) task scripts to register:" -ForegroundColor Cyan
Write-Host ""

$successful = 0
$failed = 0

foreach ($script in $allScripts) {
    $relativePath = $script.FullName.Replace($scriptDir, "").TrimStart("\")
    Write-Host "Registering: $relativePath" -ForegroundColor Yellow

    try {
        & $script.FullName
        Write-Host "  Success" -ForegroundColor Green
        $successful++
    }
    catch {
        Write-Host "  Failed: $_" -ForegroundColor Red
        $failed++
    }
    Write-Host ""
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Registration Complete" -ForegroundColor Cyan
Write-Host "  Successful: $successful" -ForegroundColor Green
Write-Host "  Failed: $failed" -ForegroundColor $(if ($failed -gt 0) { "Red" } else { "Green" })
Write-Host "========================================" -ForegroundColor Cyan
