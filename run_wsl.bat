@echo off
REM Windows batch script to run the project in WSL
REM This is a convenience wrapper for Windows users

echo Running project in WSL (Windows Subsystem for Linux)...
echo.

REM Change to the directory where this batch file is located first
cd /d "%~dp0"

REM Use the run_wsl.sh script which handles paths correctly
wsl bash run_wsl.sh
