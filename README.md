# Visual Analytics Project

## Getting Started

1. Start with `data-pipeline/README.md` to understand data acquisition, preprocessing, and storage.
2. Proceed to `algorithm-prototype/README.md` for model logic, experimentation, and evaluation.
3. Open up localhost:8501 for the streamlit app

## Order Rationale

Data pipeline outputs are prerequisites for the prototype algorithms.

## Quick Links

- [Data Pipeline README](data-pipeline/README.md)
- [Algorithm Prototype README](algorithm-prototype/README.md)

## Next Steps

After both, integrate results into visualization or deployment layers as needed.

## Troubleshooting & Debugging

If the setup or pipeline doesn't run with the instructions in this repo, try these focused checks in order. These are the most common issues we've seen (Python minor-version mismatch, Java, PySpark driver/worker mismatch, or spaCy wheel/build problems).

Checklist (quick):

- Confirm the Python version used for the repo virtualenv is 3.10.x (recommended). Many binary wheels (numpy, spaCy) are published for stable Python minors and can fail to build from source on newer, very recent Python versions.
- Ensure Java 11 is installed and on PATH (OpenJDK 11 is required for Spark compatibility with our tests).
- If Spark jobs fail with a Python-version-mismatch error, make sure the same Python executable is used for both driver and workers (set PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON to the venv python).

Useful commands / checks

- Check Python version (should be 3.10.x):

```bash
python3.10 --version || python --version
```

- If a sub-venv was accidentally created inside `data-pipeline/` (we've seen `data-pipeline/.venv` created with an incompatible Python), remove it and re-run setup from the repo root with the intended Python:

```bash
# from repo root
rm -rf data-pipeline/.venv   # remove accidental venv
PYTHON=python3.10 ./data-pipeline/setup/mac/mac_setup.sh --run-all
```

- If pip starts building numpy or spaCy from source (long C/C++ compile logs), that's a sign the chosen Python doesn't have prebuilt wheels for your platform; switch to Python 3.10 and recreate the venv as shown above.

- Check Java is available and the right major version:

```bash
java -version
# expect OpenJDK 11.x (e.g. "openjdk version \"11.0.xx\"")
```

- Spark driver/worker Python mismatch: set these env vars to point to the repo venv python before running Spark tasks (or add them to your shell):

```bash
export PYSPARK_PYTHON=/full/path/to/your/repo/.venv/bin/python
export PYSPARK_DRIVER_PYTHON=/full/path/to/your/repo/.venv/bin/python
# then run pipeline (from repo root):
cd data-pipeline
. /full/path/to/your/repo/.venv/bin/activate
python test_spark.py   # quick smoke test
python run_all.py      # full pipeline
```

- spaCy model issues: if a step complains about missing models, manually install the small English model:

```bash
. /full/path/to/your/repo/.venv/bin/activate
python -m pip install -U pip
python -m pip install spacy
python -m spacy download en_core_web_sm
python -m spacy validate
```

- Where to look for outputs and logs (quick):
  - Parquet outputs: `parquet/` (e.g., `parquet/yelp_review`, `parquet/yelp_review_enriched`, `parquet/absa_restaurant_parquet_1000`)
  - Wordcloud / artifacts: `out/` (e.g., `out/review_wordcloud.png`)

If the above don't resolve the issue, collect these items and open an issue or attach them to a PR request:

- Full console log (the failing step and any Python/compilation errors).
- The output of `python --version` and `which python` (or `python3.10 --version`).
- The output of `java -version`.
- Whether a local venv was accidentally created inside `data-pipeline/` (if so, remove it as shown above).

We're happy to help — if you open an issue include the items above and a short note about which setup script and command you ran.
