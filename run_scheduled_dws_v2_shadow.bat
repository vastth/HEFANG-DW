@echo off
REM ========================================================================
REM 何方珠宝 - DWS v2 Shadow Run 调度脚本（Windows）
REM 用于 Windows 任务计划程序触发 scheduled_dws_v2_shadow.py
REM ========================================================================

chcp 65001 >nul
cd /d %~dp0

echo ========================================================================
echo   何方珠宝 DWS v2 Shadow Run
echo   开始时间: %date% %time%
echo ========================================================================

python scheduled_dws_v2_shadow.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✓ DWS v2 Shadow Run 执行成功
    echo ========================================================================
    exit /b 0
) else (
    echo.
    echo ✗ DWS v2 Shadow Run 执行失败，错误码: %ERRORLEVEL%
    echo ========================================================================
    exit /b %ERRORLEVEL%
)