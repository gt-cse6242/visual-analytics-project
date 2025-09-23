from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year
import os, time

INPUT  = "yelp_dataset/yelp_academic_dataset_user.json"
OUTPUT = "parquet/yelp_user"   # will be created/overwritten

def main():
    t0 = time.time()
    cores = max(1, os.cpu_count() or 4)

    spark = (
        SparkSession.builder
        .appName("YelpUser->Parquet")
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

    df = spark.read.json(INPUT)

    # derive year for partitioning
    df2 = (
        df.withColumn("yelping_since_ts", to_timestamp(col("yelping_since")))
          .withColumn("yelping_year", year(col("yelping_since_ts")))
    )

    # keep all original columns + derived
    all_cols = df.columns + ["yelping_since_ts", "yelping_year"]
    out = df2.select(*[c for c in all_cols if c in df2.columns])

    # Ensure rows for the same year tend to land together
    # (total partition count = spark.sql.shuffle.partitions)
    out = out.repartition("yelping_year")

    # Cap records per parquet file so each file stays reasonably sized.
    # Adjust up/down depending on row width (200k is a sensible default).
    (out.write
        .mode("overwrite")
        .option("maxRecordsPerFile", 200_000)
        .partitionBy("yelping_year")
        .parquet(OUTPUT)
    )

    elapsed = time.time() - t0
    print(f"Wrote Parquet to: {OUTPUT}")
    print(f"Total time: {elapsed/60:.2f} minutes ({elapsed:.1f} seconds)")

    spark.stop()

if __name__ == "__main__":
    main()