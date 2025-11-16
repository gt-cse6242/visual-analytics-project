#!/usr/bin/env bash
set -euo pipefail

# Run the Streamlit app for the algorithm prototype from the project root.
# Resolve the script directory (works no matter the current working directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Find repository root by walking up from the script directory until we find a .git folder.
CUR_DIR="$SCRIPT_DIR"
PROJECT_ROOT=""
while [ "$CUR_DIR" != "/" ] && [ -z "$PROJECT_ROOT" ]; do
	if [ -d "$CUR_DIR/.git" ] || [ -f "$CUR_DIR/.git" ]; then
		PROJECT_ROOT="$CUR_DIR"
		break
	fi
	CUR_DIR="$(dirname "$CUR_DIR")"
done
# Fallback: assume repo root is two levels up (compatible with some deployments)
if [ -z "$PROJECT_ROOT" ]; then
	PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi
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
REQ_FILE="$PROJECT_ROOT/algorithm-prototype/requirements.txt"
echo "📦 Installing prototype dependencies (if missing)..."
if [ -f "$REQ_FILE" ]; then
	pip install -r "$REQ_FILE" --quiet || true
else
	echo "⚠️  Requirements file not found: $REQ_FILE"
	echo "   If you intended to install dependencies, create that file or run pip manually inside the venv."
fi

# Ensure streamlit is available in the venv; try to install it if missing so the script can continue.
if ! command -v streamlit >/dev/null 2>&1; then
	echo "⚠️  'streamlit' not found in the activated venv. Attempting to install streamlit..."
	python -m pip install streamlit --quiet || echo "Failed to install streamlit automatically; please install it inside .venv"
fi

echo "🌐 Launching Streamlit at http://localhost:8501 ..."
streamlit run algorithm-prototype/app_streamlit.py
