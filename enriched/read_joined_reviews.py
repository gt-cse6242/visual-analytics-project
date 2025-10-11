"""
Read and inspect the enriched Yelp reviews Parquet (reviews joined with business).

Default input:  parquet/yelp_review_enriched

CLI examples:
    python read_joined_reviews.py                # read all years
    python read_joined_reviews.py --years 2018 2019 --sample 20
    python read_joined_reviews.py --parquet-dir parquet/yelp_review_enriched

Shows:
- Schema, partition count, total rows
- A sample including joined biz_* columns
- Example aggregation: top cities by recent 5-star reviews
"""
import argparse
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

PARQUET_DIR_DEFAULT = "parquet/yelp_review_enriched"


def _resolve_parquet_dir(parquet_dir: str) -> str:
    """Return an absolute path to the parquet dir that works from any CWD.

    Resolution order:
    1) If the provided path is absolute and exists, use it.
    2) If a relative path exists from the current working directory, use that.
    3) Otherwise, resolve relative to the repo root (the parent of this file's folder).
    """
    p = Path(parquet_dir)
    if p.is_absolute() and p.exists():
        return str(p)
    if p.exists():  # relative to CWD
        return str(p.resolve())
    repo_root = Path(__file__).resolve().parents[1]
    return str((repo_root / p).resolve())


def main(parquet_dir: str = PARQUET_DIR_DEFAULT, years=None, sample_rows: int = 10):
    t0 = time.time()

    spark = (
        SparkSession.builder
        .appName("ReadEnrichedYelpReviews")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Resolve path so running from subfolders (e.g., enriched/) still works
    resolved_dir = _resolve_parquet_dir(parquet_dir)
    print(f"Using enriched parquet at: {resolved_dir}")

    # Load
    if years is None:
        df = spark.read.parquet(resolved_dir)
    else:
        paths = [str(Path(resolved_dir) / f"review_year={y}") for y in years]
        df = spark.read.parquet(*paths)

    print("\nSchema:")
    df.printSchema()

    if "review_year" in df.columns and years is not None:
        df = df.filter(col("review_year").isin(years))

    print(f"\nInput partitions: {df.rdd.getNumPartitions()}")

    t1 = time.time()
    n = df.count()
    t2 = time.time()
    print(f"\nRows: {n:,}")
    print(f"Load+count time: {(t2 - t0)/60:.2f} min (count took {(t2 - t1):.1f}s)")

    # Sample
    print("\nSample rows (joined fields included):")
    sample_cols = [
        # core review fields
        "review_id", "user_id", "business_id",
        "stars", "useful", "funny", "cool",
        "text", "date", "review_ts", "review_year",
        # business fields (will only select those that exist)
        "biz_name", "biz_address", "biz_city", "biz_state", "biz_postal_code",
        "biz_latitude", "biz_longitude", "biz_stars", "biz_review_count",
        "biz_is_open", "biz_categories"
    ]
    # Build selection with a truncated `text` column (25 chars) and id columns (10 chars) for now
    select_exprs = []
    for c in sample_cols:
        if c in df.columns:
            if c == "text":
                select_exprs.append(substring(col("text"), 1, 25).alias("text"))
            elif c in {"review_id", "user_id", "business_id"}:
                select_exprs.append(substring(col(c), 1, 10).alias(c))
            else:
                select_exprs.append(col(c))
    df.select(*select_exprs).show(sample_rows, truncate=False)

    # Example: Top cities by recent 5-star review count
    if all(c in df.columns for c in ["review_year", "stars", "biz_city"]):
        recent5 = df.filter((col("review_year") >= 2018) & (col("stars") == 5))
        recent5.groupBy("biz_city").count().orderBy(col("count").desc()).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect enriched Yelp reviews parquet")
    parser.add_argument("--parquet-dir", default=PARQUET_DIR_DEFAULT, help="Path to enriched reviews parquet root")
    parser.add_argument("--years", nargs="*", type=int, default=None, help="Optional list of years to read/show")
    parser.add_argument("--sample", type=int, default=10, help="Number of sample rows to display")
    args = parser.parse_args()

    main(parquet_dir=args.parquet_dir, years=args.years, sample_rows=args.sample)
