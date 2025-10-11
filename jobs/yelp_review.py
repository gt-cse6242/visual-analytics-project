"""
Convert Yelp reviews JSON to Parquet, partitioned by review_year.

Default input:  yelp_dataset/yelp_academic_dataset_review.json (line-delimited JSON)
Default output: parquet/yelp_review/review_year=YYYY/*.parquet (Snappy)

Usage:
    python yelp_review.py
    python yelp_review.py --input yelp_dataset/... --output parquet/yelp_review

Notes:
- Derives review_ts from `date` and review_year = year(review_ts).
- Configured for local runs with ~8g driver memory and reasonable parallelism.
"""
import argparse
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year

INPUT = "yelp_dataset/yelp_academic_dataset_review.json"
OUTPUT = "parquet/yelp_review"  # will be created/overwritten


def main(input_path: str = INPUT, output_path: str = OUTPUT):
    t0 = time.time()
    cores = max(1, os.cpu_count() or 4)

    spark = (
        SparkSession.builder
        .appName("YelpReview->Parquet")
        # reading large JSON locally
        .config("spark.driver.memory", "8g")
        .config("spark.local.dir", "/tmp")
        # parallel read: ~128 MB input splits
        .config("spark.sql.files.maxPartitionBytes", "128m")
        # reasonable shuffle parallelism
        .config("spark.sql.shuffle.partitions", str(max(200, cores * 3)))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # parquet
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read the line-delimited JSON reviews
    df = spark.read.json(input_path)

    # The Yelp review has a "date" string like "2018-07-07 22:18:00"
    # Derive timestamp and year for partitioning
    df2 = (
        df.withColumn("review_ts", to_timestamp(col("date")))
          .withColumn("review_year", year(col("review_ts")))
    )

    # Keep all original columns + derived
    all_cols = df.columns + ["review_ts", "review_year"]
    out = df2.select(*[c for c in all_cols if c in df2.columns])

    # Repartition by year to co-locate and produce year-partitioned files
    out = out.repartition("review_year")

    # Write Parquet partitioned by review_year
    (
        out.write
        .mode("overwrite")
        .option("maxRecordsPerFile", 200_000)
        .partitionBy("review_year")
    .parquet(output_path)
    )

    elapsed = time.time() - t0
    print(f"Wrote Parquet to: {output_path}")
    print(f"Total time: {elapsed/60:.2f} minutes ({elapsed:.1f} seconds)")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Yelp reviews JSON -> Parquet")
    parser.add_argument("--input", default=INPUT, help="Path to reviews JSON file")
    parser.add_argument("--output", default=OUTPUT, help="Output parquet directory root")
    args = parser.parse_args()
    main(input_path=args.input, output_path=args.output)
