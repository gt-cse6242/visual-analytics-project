#!/bin/bash
set -euo pipefail

## Find repository root by searching for .git so script works when files are under data-pipeline/
SEARCH_DIR="$(cd "$(dirname "$0") && pwd)"
while [ "$SEARCH_DIR" != "/" ] && [ ! -d "$SEARCH_DIR/.git" ]; do
    SEARCH_DIR="$(dirname "$SEARCH_DIR")"
done
if [ -d "$SEARCH_DIR/.git" ]; then
    PROJECT_ROOT="$SEARCH_DIR"
else
    PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
fi

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
# If pipeline files were moved to data-pipeline/, run from that directory so imports like
# `from jobs import ...` resolve against the pipeline-local folders (jobs/, enriched/).
if [ -d "$PROJECT_ROOT/data-pipeline" ]; then
    echo "Detected data-pipeline/ directory; running pipeline from there"
    cd "$PROJECT_ROOT/data-pipeline"
fi

python run_all.py
