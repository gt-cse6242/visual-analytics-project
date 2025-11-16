@echo off
title Yelp Algorithm Prototype - Streamlit UI

REM Change to project root (two levels up from this file)
cd /d "%~dp0\..\.."

echo 🚀 Starting Streamlit UI for Yelp Algorithm Prototype...

REM Check or create virtual environment
if not exist .venv (
	echo 🔧 Creating virtual environment (.venv)...
	python -m venv .venv
)

REM Activate environment
call .venv\Scripts\activate

REM Install dependencies if needed (prototype requirements)
echo 📦 Installing dependencies...
pip install -r algorithm-prototype\requirements.txt --quiet || echo "Warning: pip install failed (continuing)"

REM Run Streamlit
echo 🌐 Launching app at http://localhost:8501 ...
streamlit run algorithm-prototype\app_streamlit.py

pause
