$PSNativeCommandUseErrorActionPreference = $false
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonScript = Join-Path $scriptDir 'copilot_post_edit_reminder.py'
python $pythonScript
exit $LASTEXITCODE