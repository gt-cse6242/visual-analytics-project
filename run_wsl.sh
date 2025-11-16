#!/bin/bash
# Helper script to run the Yelp pipeline in WSL
# Automatically activates the virtual environment and runs run_all.py
#
# Usage from PowerShell:
#   wsl bash run_wsl.sh
#
# Or from inside WSL:
#   bash run_wsl.sh

# Activate virtual environment
VENV_PATH="$HOME/.venv-visual-analytics"

if [ ! -d "$VENV_PATH" ]; then
    echo "Error: Virtual environment not found at $VENV_PATH"
    echo "Please run setup_wsl.sh first"
    exit 1
fi

echo "Activating virtual environment..."
source "$VENV_PATH/bin/activate"

# Navigate to the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Running Yelp pipeline..."
python run_all.py
