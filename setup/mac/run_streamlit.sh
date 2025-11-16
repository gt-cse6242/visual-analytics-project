#!/usr/bin/env bash
set -euo pipefail

# Run the Streamlit app for the algorithm prototype from the project root.
PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$PROJECT_ROOT"

echo "🚀 Starting Streamlit UI for Yelp Algorithm Prototype..."

# Ensure virtual environment exists (try python3.10 then fallback to python3)
if [ ! -d ".venv" ]; then
	echo "🔧 Creating virtual environment (.venv)..."
	if command -v python3.10 >/dev/null 2>&1; then
		python3.10 -m venv .venv
	else
		python3 -m venv .venv
	fi
fi

# Activate environment
# shellcheck source=/dev/null
source .venv/bin/activate

# Install dependencies for the prototype if needed
echo "📦 Installing prototype dependencies (if missing)..."
pip install -r algorithm-prototype/requirements.txt --quiet || true

echo "🌐 Launching Streamlit at http://localhost:8501 ..."
streamlit run algorithm-prototype/app_streamlit.py
