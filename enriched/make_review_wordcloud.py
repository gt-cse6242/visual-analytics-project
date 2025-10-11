"""
Generate a word cloud of the most common words from the Yelp review text.

Sources:
- Reviews Parquet (default: parquet/yelp_review), partitioned by review_year

Output:
- PNG image (default: out/review_wordcloud.png)

CLI examples:
- python enriched/make_review_wordcloud.py
- python enriched/make_review_wordcloud.py --years 2022 --top 200 --output out/wordcloud_2022.png
- python enriched/make_review_wordcloud.py --reviews parquet/yelp_review --min-length 3 --top 300

Notes:
- Uses PySpark to tokenize, remove stopwords, and count words; only the top-K are collected to the driver.
- Requires `wordcloud` and `matplotlib` installed in your Python environment.
  Install: pip install wordcloud matplotlib
"""
import argparse
import os
import sys
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    regexp_replace,
    split,
    size,
    explode,
    length as spark_length,
)
from pyspark.ml.feature import StopWordsRemover


DEFAULT_REVIEWS_PARQUET = "parquet/yelp_review"
DEFAULT_ENRICHED_PARQUET = "parquet/yelp_review_enriched"


def _resolve_path(path_like: str, anchor_file: str) -> str:
    """Return absolute path for a file/dir that works from any CWD.

    If path exists relative to CWD, use it; else resolve relative to repo root
    (parent of the anchor_file's folder).
    """
    p = Path(path_like)
    if p.is_absolute() and p.exists():
        return str(p)
    if p.exists():
        return str(p.resolve())
    repo_root = Path(anchor_file).resolve().parents[1]
    return str((repo_root / p).resolve())


def _parquet_has_files(dir_path: str) -> bool:
    p = Path(dir_path)
    if not p.exists():
        return False
    # Look for any parquet file recursively (e.g., in partition subfolders)
    try:
        next(p.rglob("*.parquet"))
        return True
    except StopIteration:
        # Some writers may emit files without .parquet extension, e.g., part-*
        try:
            next(p.rglob("part-*"))
            return True
        except StopIteration:
            return False


def build_wordcounts(
    spark: SparkSession,
    reviews_parquet: str,
    years: List[int] | None,
    min_length: int,
    top_k: int,
):
    # Read reviews (optionally specific year partitions)
    if years:
        paths = [str(Path(reviews_parquet) / f"review_year={y}") for y in years]
        df = spark.read.parquet(*paths)
    else:
        df = spark.read.parquet(reviews_parquet)

    # Normalize text and split into words
    # Keep only alphabetic characters; collapse non-letters to spaces
    df_words = (
        df.select(col("text").cast("string").alias("text"))
          .na.drop(subset=["text"])  # drop rows with null text
          .withColumn("text_l", lower(col("text")))
          .withColumn("text_l", regexp_replace(col("text_l"), "[^a-z]", " "))
          .withColumn("words", split(col("text_l"), r"\s+"))
    )

    # Remove stopwords using Spark's built-in list
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    df_filtered = remover.transform(df_words).drop("text", "text_l", "words")

    # Explode and filter short/empty tokens
    tokens = (
        df_filtered
        .where(size(col("filtered")) > 0)
        .select(explode(col("filtered")).alias("word"))
        .where((spark_length(col("word")) >= min_length) & (col("word") != ""))
    )

    # Count and return top-K
    top = tokens.groupBy("word").count().orderBy(col("count").desc()).limit(top_k)
    return [(row["word"], int(row["count"])) for row in top.collect()]


def make_wordcloud(freqs: list[tuple[str, int]], output_path: str, width=1600, height=900, background="white"):
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # First, try true wordcloud rendering
    try:
        from wordcloud import WordCloud
        import matplotlib.pyplot as plt

        freq_dict = dict(freqs)
        wc = WordCloud(width=width, height=height, background_color=background, collocations=False)
        wc.generate_from_frequencies(freq_dict)

        plt.figure(figsize=(width/100, height/100), dpi=100)
        plt.imshow(wc, interpolation="bilinear")
        plt.axis("off")
        plt.tight_layout(pad=0)
        plt.savefig(str(out_path), dpi=150)
        plt.close()
        return str(out_path)

    except ImportError as e:
        # Fallback: render a horizontal bar chart with matplotlib only
        try:
            import matplotlib.pyplot as plt
        except ImportError as e2:
            raise SystemExit(
                "Missing dependencies for rendering. Install at least matplotlib, or for true word clouds also install wordcloud:\n"
                "  python -m pip install matplotlib wordcloud\n"
                f"Original error: {e2}"
            )

        print("wordcloud not installed; falling back to a top-N bar chart.")
        top_show = min(len(freqs), 50)  # keep chart readable
        words, counts = zip(*freqs[:top_show]) if top_show > 0 else ([], [])

        plt.figure(figsize=(14, max(6, top_show * 0.25)))
        y_pos = list(range(top_show))
        plt.barh(y_pos, counts, color="steelblue")
        plt.yticks(y_pos, words)
        plt.gca().invert_yaxis()
        plt.xlabel("Count")
        plt.title(f"Top {top_show} words")
        plt.tight_layout()
        plt.savefig(str(out_path), dpi=150)
        plt.close()
        return str(out_path)


def _try_generate_reviews_parquet(anchor_file: str) -> str | None:
    """Attempt to generate reviews Parquet by invoking jobs.yelp_review.main.

    Returns the absolute output directory path if generation likely succeeded, else None.
    """
    repo_root = Path(anchor_file).resolve().parents[1]
    reviews_json = repo_root / "yelp_dataset/yelp_academic_dataset_review.json"
    out_dir = repo_root / "parquet/yelp_review"

    if not reviews_json.exists():
        print(f"Reviews JSON not found at: {reviews_json}")
        return None

    # Make package importable
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    try:
        from jobs import yelp_review as yr
    except Exception as e:
        print(f"Could not import jobs.yelp_review to generate parquet: {e}")
        return None

    try:
        print("Generating reviews Parquet via jobs/yelp_review.py ...")
        yr.main(input_path=str(reviews_json), output_path=str(out_dir))
    except Exception as e:
        print(f"Generation failed: {e}")
        return None

    # Re-check
    return str(out_dir) if _parquet_has_files(str(out_dir)) else None


def main():
    parser = argparse.ArgumentParser(description="Generate a word cloud from Yelp review text")
    parser.add_argument("--reviews", default=DEFAULT_REVIEWS_PARQUET, help="Path to reviews parquet root")
    parser.add_argument("--years", nargs="*", type=int, default=None, help="Optional list of years to include")
    parser.add_argument("--top", type=int, default=200, help="Number of most frequent words to include")
    parser.add_argument("--min-length", type=int, default=3, help="Minimum word length to keep")
    parser.add_argument("--output", default="out/review_wordcloud.png", help="Output PNG path")
    parser.add_argument("--background", default="white", help="Background color (e.g., white, black)")
    parser.add_argument("--no-generate", action="store_true", help="Do not auto-generate reviews parquet if missing")
    parser.add_argument("--prefer-enriched", action="store_true", help="Try enriched parquet first if reviews is missing")
    args = parser.parse_args()

    # Resolve paths robustly
    anchor = __file__
    reviews_path = _resolve_path(args.reviews, anchor_file=anchor)
    # Ensure we have data: prefer provided path; else optionally generate; else use enriched; else exit
    if _parquet_has_files(reviews_path):
        print(f"Found existing parquet files at: {reviews_path}; skipping generation.")
    else:
        print(f"No parquet files detected at: {reviews_path}.")
        enriched_path = _resolve_path(DEFAULT_ENRICHED_PARQUET, anchor_file=anchor)

        # Optionally prefer enriched first when reviews is missing
        if args.prefer_enriched and _parquet_has_files(enriched_path):
            print("--prefer-enriched set and enriched parquet is available; using it.")
            reviews_path = enriched_path
        elif not args.no_generate:
            print("Attempting to auto-generate reviews parquet (no --no-generate flag).")
            generated = _try_generate_reviews_parquet(anchor_file=anchor)
            if generated:
                reviews_path = generated
            elif _parquet_has_files(enriched_path):
                print("Generation unavailable; using enriched parquet for text source.")
                reviews_path = enriched_path
            else:
                raise SystemExit(
                    "No review data found. The script attempted to generate reviews parquet but failed.\n"
                    "Please create input first:\n"
                    "  1) python jobs/yelp_review.py\n"
                    "  2) (optional) python enriched/join_reviews_with_business.py\n"
                    "Then re-run the wordcloud script."
                )
        else:
            print("--no-generate set; will not attempt auto-generation.")
            if _parquet_has_files(enriched_path):
                print("Using enriched parquet for text source.")
                reviews_path = enriched_path
            else:
                raise SystemExit(
                    "No review data found and --no-generate was specified. Create inputs first:\n"
                    "  1) python jobs/yelp_review.py\n"
                    "  2) (optional) python enriched/join_reviews_with_business.py\n"
                )

    print(f"Using reviews parquet at: {reviews_path}")

    # Spark session (create only after data is ensured to avoid multiple SparkContexts)
    spark = (
        SparkSession.builder
        .appName("MakeReviewWordcloud")
        .config("spark.driver.memory", "4g")
        # Ensure driver and executors use this same Python (your .venv)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    if args.years:
        print(f"Restricting to years: {args.years}")

    freqs = build_wordcounts(
        spark=spark,
        reviews_parquet=reviews_path,
        years=args.years,
        min_length=args.min_length,
        top_k=args.top,
    )

    print(f"Top {len(freqs)} words (sample): {freqs[:10]}")
    out_file = make_wordcloud(freqs, output_path=args.output, background=args.background)
    print(f"Saved wordcloud to: {out_file}")

    spark.stop()


if __name__ == "__main__":
    main()
