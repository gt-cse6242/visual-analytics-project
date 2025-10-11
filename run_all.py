"""
Run the Yelp reviews pipeline end-to-end.

Steps:
    1) Convert reviews JSON -> Parquet (partitioned by review_year)
    2) Join business data onto reviews
    3) Read enriched Parquet and print a sample + tiny aggregation

Usage examples:
    python run_all.py                            # run all steps
    python run_all.py --skip-review              # skip step 1
    python run_all.py --skip-review --skip-join  # only read/sample
    python run_all.py --years 2018 2019 --sample 20

Notes:
    Requires PySpark, Java 11, and the Yelp dataset in yelp_dataset/.
"""
import argparse
import time
import sys
import subprocess

# Step modules
from jobs import yelp_review as reviews_step
from enriched import join_reviews_with_business as join_step

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

ENRICHED_PARQUET = "parquet/yelp_review_enriched"


def read_and_sample(years=None, sample_rows=10):
    t0 = time.time()
    spark = (
        SparkSession.builder
        .appName("ReadEnrichedYelpReviews-Pipeline")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load partitions
    if years:
        paths = [f"{ENRICHED_PARQUET}/review_year={y}" for y in years]
        df = spark.read.parquet(*paths)
    else:
        df = spark.read.parquet(ENRICHED_PARQUET)

    print("\nSchema:")
    df.printSchema()

    if years:
        df = df.filter(col("review_year").isin(years))

    print(f"\nInput partitions: {df.rdd.getNumPartitions()}")

    t1 = time.time()
    n = df.count()
    t2 = time.time()
    print(f"\nRows: {n:,}")
    print(f"Load+count time: {(t2 - t0)/60:.2f} min (count took {(t2 - t1):.1f}s)")

    # Sample
    print("\nSample rows (with business fields):")
    sample_cols = [
        "review_id", "user_id", "business_id", "stars", "date", "review_year",
        "biz_name", "biz_city", "biz_state", "biz_stars", "biz_review_count"
    ]
    df.select(*[c for c in sample_cols if c in df.columns]).show(sample_rows, truncate=False)

    # Tiny example aggregation
    if all(c in df.columns for c in ["review_year", "stars", "biz_city"]):
        recent5 = df.filter((col("review_year") >= 2018) & (col("stars") == 5))
        print("\nTop cities by recent 5-star review count:")
        recent5.groupBy("biz_city").count().orderBy(col("count").desc()).show(10, truncate=False)

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Run Yelp review pipeline end-to-end")
    parser.add_argument("--skip-review", action="store_true", help="Skip converting reviews JSON to Parquet")
    parser.add_argument("--skip-join", action="store_true", help="Skip joining business onto reviews")
    parser.add_argument("--years", nargs="*", type=int, default=None, help="Optional list of years to read/show")
    parser.add_argument("--sample", type=int, default=10, help="Number of sample rows to display")
    # Wordcloud options
    parser.add_argument("--make-wordcloud", action="store_true", help="Generate a wordcloud (or bar chart fallback) from review text")
    parser.add_argument("--wordcloud-output", default="out/review_wordcloud.png", help="Output PNG path for the wordcloud/bar chart")
    parser.add_argument("--wordcloud-top", type=int, default=200, help="Number of words to include in the wordcloud/bar chart")
    args = parser.parse_args()

    if not args.skip_review:
        print("\n=== Step 1/3: Convert reviews JSON -> Parquet ===")
        reviews_step.main()
    else:
        print("\n=== Step 1/3: Skipped reviews conversion ===")

    if not args.skip_join:
        print("\n=== Step 2/3: Join reviews with business ===")
        join_step.main()
    else:
        print("\n=== Step 2/3: Skipped join ===")

    print("\n=== Step 3/3: Read enriched Parquet and sample ===")
    read_and_sample(years=args.years, sample_rows=args.sample)

    # Optional Step 4: Generate wordcloud using a separate module invocation to avoid CLI arg conflicts
    if args.make_wordcloud:
        print("\n=== Step 4: Generate wordcloud from reviews text ===")
        cmd = [
            sys.executable,
            "-m",
            "enriched.make_review_wordcloud",
            "--reviews", "parquet/yelp_review_enriched",  # use enriched output from this run
            "--output", args.wordcloud_output,
            "--top", str(args.wordcloud_top),
            "--no-generate",  # avoid regeneration since pipeline already ran
        ]
        if args.years:
            cmd.extend(["--years", *[str(y) for y in args.years]])
        print("Running:", " ".join(cmd))
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print("Wordcloud generation failed:", e)


if __name__ == "__main__":
    main()
