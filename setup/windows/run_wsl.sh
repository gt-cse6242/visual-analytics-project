#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
VENV_PATH="$HOME/.venv-visual-analytics"

if [ ! -d "$VENV_PATH" ]; then
    echo "Error: Virtual environment not found at $VENV_PATH"
    echo "Please run setup_wsl.sh first"
    exit 1
fi

echo "Activating virtual environment..."
source "$VENV_PATH/bin/activate"

cd "$PROJECT_ROOT"

echo "Running Yelp pipeline..."
python run_all.py
