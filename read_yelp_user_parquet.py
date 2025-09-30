from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

PARQUET_DIR = "parquet/yelp_user"   # folder you wrote earlier
YEARS = None                         # e.g., YEARS = [2010, 2011, 2012]  or None for all

def main():
    t0 = time.time()
    spark = (
        SparkSession.builder
        .appName("ReadYelpUserParquet")
        .config("spark.driver.memory", "4g")  # reading is lighter; bump if needed
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1) Load
    if YEARS is None:
        # Loads all partitions
        df = spark.read.parquet(PARQUET_DIR)
    else:
        # Partition-pruned load (reads only the folders matching these years)
        paths = [f"{PARQUET_DIR}/yelping_year={y}" for y in YEARS]
        df = spark.read.parquet(*paths)

    # 2) Inspect
    print("\nSchema:")
    df.printSchema()

    # Partition column may come in as int; if missing (non-partitioned data), this is a no-op
    if "yelping_year" in df.columns:
        # Optional: filter again at query-time (also prunes if not already pruned)
        if YEARS is not None:
            df = df.filter(col("yelping_year").isin(YEARS))

    # Show how many tasks we’re using
    print(f"\nInput partitions: {df.rdd.getNumPartitions()}")

    # Count rows (forces read)
    t1 = time.time()
    n = df.count()
    t2 = time.time()
    print(f"\nRows: {n:,}")
    print(f"Load+count time: {(t2 - t0)/60:.2f} min (count took {(t2 - t1):.1f}s)")

    # 3) Quick sample peek
    print("\nSample rows:")
    df.select(
        *[c for c in ["user_id","name","yelping_since","review_count","average_stars","yelping_year","fans"] if c in df.columns]
    ).show(10, truncate=False)

    # 4) Example: only recent users
    if "yelping_year" in df.columns:
        recent = df.filter(col("yelping_year") >= 2015)
        print(f"\nRecent users (>=2015): {recent.count():,}")

    spark.stop()

if __name__ == "__main__":
    main()
