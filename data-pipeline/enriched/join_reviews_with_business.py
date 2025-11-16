"""
Enrich review Parquet with business attributes by joining on business_id.

Inputs (defaults):
- Reviews Parquet: parquet/yelp_review (produced by yelp_review.py)
- Business JSON:  yelp_dataset/yelp_academic_dataset_business.json

Output (default):
- Parquet: parquet/yelp_review_enriched partitioned by review_year

Behavior:
- Selects a curated set of top-level business fields and aliases them with a `biz_` prefix
  to avoid name collisions (e.g., biz_name, biz_city, biz_stars, ...).
- Uses a left join to keep all reviews; business fields are null if not matched.
- Broadcasts the business side to speed up the join when feasible.

Usage:
    python join_reviews_with_business.py
    python join_reviews_with_business.py --reviews parquet/yelp_review --business-json yelp_dataset/... --output parquet/yelp_review_enriched
"""
import argparse
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

REVIEWS_PARQUET = "parquet/yelp_review"                # produced by yelp_review.py
BUSINESS_JSON   = "yelp_dataset/yelp_academic_dataset_business.json"
OUTPUT_PARQUET  = "parquet/yelp_review_enriched"       # will be created/overwritten


def select_business_columns(df_biz):
    """
    Select a curated set of top-level business columns and alias them with a biz_ prefix
    to avoid name collisions with review columns. We avoid nested objects.
    """
    wanted = [
        ("business_id", "business_id"),  # keep join key name
        ("name", "biz_name"),
        ("city", "biz_city"),
        ("state", "biz_state"),
        ("postal_code", "biz_postal_code"),
        ("latitude", "biz_latitude"),
        ("longitude", "biz_longitude"),
        ("stars", "biz_stars"),  # business average stars
        ("review_count", "biz_review_count"),
        ("is_open", "biz_is_open"),
        ("categories", "biz_categories"),
        ("address", "biz_address"),  # included if available in this dataset version
    ]

    # Only select columns that actually exist to make the script robust across dataset versions
    existing = []
    for src, alias in wanted:
        if src in df_biz.columns:
            if src == alias:
                existing.append(col(src))
            else:
                existing.append(col(src).alias(alias))

    return df_biz.select(*existing)


def main(reviews_parquet: str = REVIEWS_PARQUET, business_json: str = BUSINESS_JSON, output_path: str = OUTPUT_PARQUET):
    t0 = time.time()
    cores = max(1, os.cpu_count() or 4)

    spark = (
        SparkSession.builder
        .appName("JoinReviewsWithBusiness")
        .config("spark.driver.memory", "8g")
        .config("spark.local.dir", "/tmp")
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .config("spark.sql.shuffle.partitions", str(max(200, cores * 3)))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load reviews parquet (must exist)
    reviews = spark.read.parquet(reviews_parquet)

    # Load business JSON and select/alias useful columns
    biz = spark.read.json(business_json)
    biz_sel = select_business_columns(biz)

    # Prefer a broadcast hash join since business table is relatively small compared to reviews
    joined = reviews.join(broadcast(biz_sel), on="business_id", how="left")

    # Write enriched reviews, keeping the same partitioning by review_year
    (
        joined.write
        .mode("overwrite")
        .option("maxRecordsPerFile", 200_000)
        .partitionBy("review_year")
    .parquet(output_path)
    )

    elapsed = time.time() - t0
    print(f"Wrote enriched reviews to: {output_path}")
    print(f"Total time: {elapsed/60:.2f} minutes ({elapsed:.1f} seconds)")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Join reviews with business JSON -> enriched parquet")
    parser.add_argument("--reviews", default=REVIEWS_PARQUET, help="Path to reviews parquet root")
    parser.add_argument("--business-json", default=BUSINESS_JSON, help="Path to business JSON file")
    parser.add_argument("--output", default=OUTPUT_PARQUET, help="Output enriched parquet directory root")
    args = parser.parse_args()
    main(reviews_parquet=args.reviews, business_json=args.business_json, output_path=args.output)
