"""
Run the Yelp pipeline end-to-end (convert -> join -> sample -> ABSA -> wordcloud).

Usage:
    python run_all.py

Notes:
    - Requires Java 11, PySpark, spaCy + en_core_web_sm
    - Expects Yelp JSON under yelp_dataset/
"""

import os
import tempfile
import time
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from jobs import yelp_review as reviews_step
from enriched import join_reviews_with_business as join_step
import extract_aspects as aspects_step
from enriched import make_review_wordcloud as wc

REVIEWS_JSON = "../yelp_dataset/yelp_academic_dataset_review.json"
REVIEWS_PARQUET = "../parquet/yelp_review"
ENRICHED_PARQUET = "../parquet/yelp_review_enriched"
WORDCLOUD_OUTPUT = "../out/review_wordcloud.png"
WORDCLOUD_TOP = 200


def _safe_tmpdir() -> str:
    tmpdir = os.path.join(tempfile.gettempdir(), "spark-local")
    os.makedirs(tmpdir, exist_ok=True)
    return tmpdir


def read_and_sample(sample_rows=10):
    t0 = time.time()
    tmpdir = _safe_tmpdir()
    spark = (
        SparkSession.builder
        .appName("ReadEnrichedYelpReviews-Pipeline")
        .config("spark.local.dir", tmpdir)
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={tmpdir}")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={tmpdir}")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(ENRICHED_PARQUET)

    print("\nSchema:")
    df.printSchema()

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


def run_wordcloud(top_k: int = WORDCLOUD_TOP, output_png: str = WORDCLOUD_OUTPUT):
    tmpdir = _safe_tmpdir()
    spark = (
        SparkSession.builder
        .appName("MakeReviewWordcloud-Pipeline")
        .config("spark.local.dir", tmpdir)
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={tmpdir}")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={tmpdir}")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    freqs = wc.build_wordcounts(
        spark=spark,
        reviews_parquet=ENRICHED_PARQUET,  # enriched also has `text`
        years=None,
        min_length=3,
        top_k=top_k,
    )
    out_file = wc.make_wordcloud(freqs, output_path=output_png)
    print(f"Saved wordcloud to: {out_file}")
    spark.stop()


def main():
    print("\n" + "="*60)
    print("YELP REVIEWS PIPELINE - Running all steps")
    print("="*60)

    # Step 1: Convert reviews JSON -> Parquet (partitioned by review_year)
    print("\n=== Step 1/5: Convert reviews JSON -> Parquet ===")
    reviews_step.main(input_path=REVIEWS_JSON, output_path=REVIEWS_PARQUET)

    # Step 2: Join business attributes onto reviews -> enriched Parquet
    print("\n=== Step 2/5: Join reviews with business ===")
    join_step.main(
        reviews_parquet=REVIEWS_PARQUET,
        business_json="../yelp_dataset/yelp_academic_dataset_business.json",
        output_path=ENRICHED_PARQUET,
    )

    # Step 3: Inspect enriched Parquet (schema, counts, sample, tiny aggregation)
    print("\n=== Step 3/5: Read enriched Parquet and sample ===")
    read_and_sample(sample_rows=10)

    # Step 4: Extract aspect-based sentiment (ABSA) from restaurant reviews (sampled)
    print("\n=== Step 4/5: Extract aspect-based sentiment (ABSA) ===")
    aspects_step.extract_aspect_from_text(
        input_path=ENRICHED_PARQUET,
        text_col="text",
        meta_cols=["business_id","biz_name","stars","date","biz_categories"],
        out_path="../parquet/absa_restaurant_parquet",
        seeds=aspects_step.RESTAURANT_SEEDS,
        aspects=aspects_step.ASPECTS,
        spacy_model="en_core_web_sm",
        repartition_n=1
    )

    # Step 5: Generate a wordcloud PNG from review text
    print("\n=== Step 5/5: Generate wordcloud ===")
    run_wordcloud()

    print("\n" + "="*60)
    print("PIPELINE COMPLETE!")
    print("="*60)


if __name__ == "__main__":
    main()
