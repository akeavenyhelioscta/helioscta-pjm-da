$condaPath = "$env:USERPROFILE\miniconda3\Scripts\activate.bat"
$backendDir = "$env:USERPROFILE\Documents\github\helioscta-pjm-da\backend"

$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"call `"$condaPath`" helioscta-pjm-da && cd /d `"$backendDir`" && python -m src.scripts.refresh_cache --ttl 1`"" `
    -WorkingDirectory $backendDir

# Run every 30 minutes, 24/7
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes 30)

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

Register-ScheduledTask `
    -TaskName "Refresh Cache (PJM DA)" `
    -Description "Refresh local parquet data cache for PJM DA backend" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -TaskPath "\PJM-DA\" `
    -Force
