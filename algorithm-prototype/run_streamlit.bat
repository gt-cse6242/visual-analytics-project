@echo off
REM Wrapper that forwards to central setup/windows Streamlit runner
cd /d "%~dp0"
call ..\setup\windows\run_streamlit.bat %*
