$PSNativeCommandUseErrorActionPreference = $false
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonScript = Join-Path $scriptDir 'copilot_session_close_reminder.py'
python $pythonScript
exit $LASTEXITCODE