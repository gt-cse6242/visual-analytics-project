## Yelp Visual Analytics (Concise README)

Turn the Yelp Open Dataset into analysis-ready Parquet, join business attributes, preview samples, and optionally run aspect-based sentiment analysis (ABSA) and a wordcloud.

## Prerequisites

- Java 11 (JDK 11)
- Python 3.10
- Yelp Open Dataset files placed in `yelp_dataset/`:
  - `yelp_academic_dataset_review.json` (required)
  - `yelp_academic_dataset_business.json` (required)

## Setup

1. Create a virtual environment and install packages

```powershell
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

2. (Windows) Ensure Java 11 is installed and recognized

```powershell
java -version     # should show version 11.x
```

If Java isn’t found, install Adoptium Temurin JDK 11 and (if needed) set JAVA_HOME to the JDK 11 folder.

## Quickstart (one command)

From the repo root with your venv activated and Yelp dataset JSONs present in `yelp_dataset/`:

```powershell
python run_all.py
```

What it does:

- Convert reviews JSON → Parquet (`parquet/yelp_review/`)
- Join business attributes → `parquet/yelp_review_enriched/`
- Read and sample enriched data
- Run ABSA on 1,000 restaurant reviews → `parquet/absa_restaurant_parquet_1000/` (+ CSV)
- Generate a wordcloud PNG → `out/review_wordcloud.png`

## Optional: run individual steps

Step 1 — Convert reviews JSON to Parquet

```powershell
python jobs/yelp_review.py
# or with paths
python jobs/yelp_review.py --input yelp_dataset/yelp_academic_dataset_review.json --output parquet/yelp_review
```

Step 2 — Join business onto reviews

```powershell
python enriched/join_reviews_with_business.py
# or with paths
python enriched/join_reviews_with_business.py --reviews parquet/yelp_review --business-json yelp_dataset/yelp_academic_dataset_business.json --output parquet/yelp_review_enriched
```

Step 3 — Inspect enriched Parquet (schema, counts, sample)

```powershell
python enriched/read_joined_reviews.py
# restrict to years and adjust sample size
python enriched/read_joined_reviews.py --years 2021 2022 --sample 10
```

Optional — Wordcloud from review text

```powershell
# default reads parquet/yelp_review (autogenerates if missing unless --no-generate)
python enriched/make_review_wordcloud.py --years 2022 --top 200 --output out/wordcloud_2022.png
```

## ABSA notes (extract_aspects.py)

`run_all.py` calls ABSA with:

- Input: `parquet/yelp_review_enriched`
- Filters to restaurants via `biz_categories` containing "restaurants"
- Limits to 1,000 reviews by default (`data_size = 1000` inside `extract_aspects.py`)
- Output: `parquet/absa_restaurant_parquet_1000/` and a CSV inside that folder

To increase coverage, edit `extract_aspects.py` and raise/remove the `data_size` limit.

## Troubleshooting

- JAVA_GATEWAY_EXITED
  - Install Java 11, ensure `java -version` works; set JAVA_HOME if needed.
- ModuleNotFoundError: pyspark or spacy
  - Activate the venv, then `python -m pip install -r requirements.txt` and `python -m spacy download en_core_web_sm`.
- No module named 'en_core_web_sm'
  - Run `python -m spacy download en_core_web_sm` (with venv active).
- Parquet path not found
  - Run the earlier steps or `python run_all.py` to generate outputs first.
- Memory issues
  - Keep the ABSA sample at 1,000; or adjust Spark memory configs if increasing size.

## Project structure (key files)

- `run_all.py` — Orchestrates: convert → join → sample → ABSA
- `jobs/yelp_review.py` — Convert reviews JSON → Parquet (partitioned by year)
- `enriched/join_reviews_with_business.py` — Left-join business attributes onto reviews → enriched Parquet
- `enriched/read_joined_reviews.py` — Inspect enriched Parquet (schema/counts/sample)
- `enriched/make_review_wordcloud.py` — Build a wordcloud (or bar chart fallback)
- `extract_aspects.py` — Aspect-based sentiment extraction (restaurant-focused)
- `test_spark.py` — Quick Spark smoke test

## Requirements (summary)

See `requirements.txt` for exact versions. Main libs: PySpark 3.5.x, pandas, pyarrow, spaCy (+ model `en_core_web_sm`), wordcloud, matplotlib.

## Optional: quick Spark smoke test

```powershell
python test_spark.py
```

Expected output includes the Spark version and a tiny 2-row DataFrame.
