#!/bin/bash
# Setup script for running PySpark in WSL (Ubuntu)
# This avoids Windows username path issues by running in a Linux environment
#
# Usage from PowerShell:
#   wsl bash setup_wsl.sh
#
# Or from inside WSL:
#   cd "/mnt/c/Users/<your-windows-user>/path/to/visual-analytics-project"
#   bash setup_wsl.sh

set -e  # Exit on error

echo "=== Setting up PySpark environment in WSL ==="
echo ""

# Determine project root by searching upward for a .git folder.
SEARCH_DIR="$(pwd)"
while [ "$SEARCH_DIR" != "/" ] && [ ! -d "$SEARCH_DIR/.git" ]; do
	SEARCH_DIR="$(dirname "$SEARCH_DIR")"
done
if [ -d "$SEARCH_DIR/.git" ]; then
	PROJECT_ROOT="$SEARCH_DIR"
else
	PROJECT_ROOT="$(pwd)"
fi

# Update package lists
echo "Step 1/6: Updating package lists..."
sudo apt-get update -qq

# Install Java 11 (required for PySpark)
echo "Step 2/6: Installing Java 11..."
sudo apt-get install -y openjdk-11-jdk-headless

# Verify Java installation
echo ""
echo "Java installed:"
java -version
echo ""

# Install Python pip and venv if not already installed
echo "Step 3/6: Installing Python tools..."
sudo apt-get install -y python3-pip python3-venv

# Create virtual environment in WSL home directory (clean Linux paths)
VENV_PATH="$HOME/.venv-visual-analytics"
echo "Step 4/6: Creating virtual environment at $VENV_PATH..."
python3 -m venv "$VENV_PATH"

# Activate venv and install requirements
echo "Step 5/6: Installing Python packages..."
source "$VENV_PATH/bin/activate"
pip install --upgrade pip

# If data-pipeline has requirements.txt, install from there, otherwise install minimal set
DP_REQ="$PROJECT_ROOT/data-pipeline/requirements.txt"
if [ -f "$DP_REQ" ]; then
	echo "Installing packages from $DP_REQ"
	pip install -r "$DP_REQ"
else
	echo "Installing core packages (pyspark, pandas, pyarrow, spacy, etc.)"
	pip install pyspark==3.5.* pandas pyarrow wordcloud matplotlib spacy
fi

# Download spacy model
echo "Downloading spacy language model..."
python -m spacy download en_core_web_sm || true

echo ""
echo "=== ✓ Setup Complete! ==="
echo ""
echo "Virtual environment created at: $VENV_PATH"
echo ""
echo "To run the project in WSL:"
echo "  1. Start WSL:        wsl"
echo "  2. Go to project:    cd <your-project-path>"
echo "  3. Activate venv:    source $VENV_PATH/bin/activate"
echo "  4. Run scripts:      python run_all.py"
echo ""
echo "Or simply run from PowerShell in the project directory:" 
echo "  run_wsl.bat"
echo ""
