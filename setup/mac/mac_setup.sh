#!/usr/bin/env bash
set -euo pipefail

# mac_setup.sh
# Create/use a Python venv, install requirements, download spaCy model,
# create a .env file with PYSPARK env vars, and run the Spark smoke test.
#
# Usage:
#   ./mac_setup.sh                 # run setup and smoke test (non-interactive)
#   ./mac_setup.sh --export        # also append PYSPARK vars to ~/.zshrc
#   ./mac_setup.sh --run-all       # after setup, run the full pipeline (only if Yelp dataset present)
#   ./mac_setup.sh --export --run-all

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
VENV="$PROJECT_ROOT/.venv"
PYTHON_CMD="${PYTHON:-python3}"

# Flags
DO_EXPORT=false
DO_RUN_ALL=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --export)
      DO_EXPORT=true
      shift
      ;;
    --run-all)
      DO_RUN_ALL=true
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--export] [--run-all]"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Usage: $0 [--export] [--run-all]"
      exit 2
      ;;
  esac
done

echo "Project root: $PROJECT_ROOT"
echo "Using python command: $PYTHON_CMD"

if ! command -v "$PYTHON_CMD" >/dev/null 2>&1; then
  echo "Error: $PYTHON_CMD not found. Install Python 3.10+ and re-run." >&2
  exit 1
fi

if [ ! -x "$VENV/bin/python" ]; then
  echo "Creating virtualenv at $VENV"
  "$PYTHON_CMD" -m venv "$VENV"
else
  echo "Virtualenv already exists at $VENV"
fi

echo "Activating virtualenv for setup steps..."
# shellcheck source=/dev/null
source "$VENV/bin/activate"

echo "Upgrading pip..."
python -m pip install --upgrade pip

if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
  echo "Installing requirements from requirements.txt"
  python -m pip install -r "$PROJECT_ROOT/requirements.txt"
else
  echo "No requirements.txt found in project root; skipping pip install."
fi

echo "Installing spaCy English model (en_core_web_sm)"
python -m spacy download en_core_web_sm || true

echo "Checking Java..."
if command -v java >/dev/null 2>&1; then
  java -version 2>&1 | sed -n '1p'
else
  echo "Java not found. Install Java 11, for example: brew install openjdk@11" >&2
fi

# Create a simple .env file with PYSPARK settings so the user can source it.
ENVFILE="$PROJECT_ROOT/.env"
PYSPARK_PY="$VENV/bin/python"
cat > "$ENVFILE" <<EOF
export PYSPARK_PYTHON="$PYSPARK_PY"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PY"
EOF

echo "Wrote PYSPARK env vars to $ENVFILE"

if [ "$DO_EXPORT" = true ]; then
  echo "Appending PYSPARK vars to ~/.zshrc"
  printf "\n# Visual Analytics project PYSPARK settings\nexport PYSPARK_PYTHON=\"%s\"\nexport PYSPARK_DRIVER_PYTHON=\"%s\"\n" "$PYSPARK_PY" "$PYSPARK_PY" >> "$HOME/.zshrc"
  echo "Appended to ~/.zshrc";
fi

echo "Running Spark smoke test (test_spark.py) with venv Python set for driver and worker..."
PYSPARK_PYTHON="$PYSPARK_PY" PYSPARK_DRIVER_PYTHON="$PYSPARK_PY" python "$PROJECT_ROOT/test_spark.py"

# Optionally run full pipeline if requested and dataset files are present
if [ "$DO_RUN_ALL" = true ]; then
  REVIEWS_JSON="$PROJECT_ROOT/yelp_dataset/yelp_academic_dataset_review.json"
  BUSINESS_JSON="$PROJECT_ROOT/yelp_dataset/yelp_academic_dataset_business.json"

  if [ -f "$REVIEWS_JSON" ] && [ -f "$BUSINESS_JSON" ]; then
    echo "Found Yelp dataset files; running full pipeline (run_all.py). This may take a long time..."
    PYSPARK_PYTHON="$PYSPARK_PY" PYSPARK_DRIVER_PYTHON="$PYSPARK_PY" python "$PROJECT_ROOT/run_all.py"
  else
    echo "--run-all requested but Yelp dataset files not found in $PROJECT_ROOT/yelp_dataset/" >&2
    echo "Expected: $REVIEWS_JSON and $BUSINESS_JSON" >&2
    echo "Skipping run_all.py. Place the Yelp JSON files in yelp_dataset/ and re-run with --run-all if you want the full pipeline to run."
  fi
fi

echo "Setup complete. To use the environment in a new shell:"
echo "  cd $PROJECT_ROOT"
echo "  source .venv/bin/activate"
echo "  # optionally: source .env to set PYSPARK env vars for spark runs"
echo "  # run pipeline: python run_all.py (ensure Yelp dataset files are in yelp_dataset/)"

exit 0
