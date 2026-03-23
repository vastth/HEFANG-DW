@echo off
setlocal

python "%~dp0copilot_session_close_reminder.py"
exit /b %ERRORLEVEL%