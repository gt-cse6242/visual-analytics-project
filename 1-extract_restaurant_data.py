from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lower
from pyspark.sql import Row
import sys

# ========= set up the pyspark session =========
spark = (
    SparkSession.builder
    .appName("ABSA-PySpark")
    # make sure executors use your venv python
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)

    # give driver/executor a bit more heap
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")

    # read smaller file splits to reduce per-task memory
    .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))  # 64 MB (default 128 MB)
    .config("spark.sql.files.openCostInBytes", str(8 * 1024 * 1024))     # helps create more splits

    # Parquet reader: smaller vectorized batches (or disable if needed)
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.columnarReaderBatchSize", "1024")          # default ~4096; lower uses less heap
    # If still OOM, try disabling vectorization:
    # .config("spark.sql.parquet.enableVectorizedReader", "false")

    # make Python workers reusable (fewer forks)
    .config("spark.python.worker.reuse", "true")

    # fewer rows per shuffle task (helps local dev)
    .config("spark.sql.shuffle.partitions", "100")
    .getOrCreate()
)


# Edit these if you want to run this file directly (no argparse)
input_path = "parquet/yelp_review_enriched"
print(f"\n========== Loading input from {input_path}) ===========")
df = spark.read.parquet(input_path)
print(f"[info] read Parquet ← {input_path} with {df.count()} rows")

# ========= filter the data to just restaurants =========
df_restaurant = df.withColumn("biz_categories", lower(df["biz_categories"]))\
            .filter(col("biz_categories").like("%restaurants"))
print(f"Full business review count: {df.count()}")
print(f"Restaurant business review count: {df_restaurant.count()}")
df_restaurant.printSchema()

# ========= write out the restaurant data =========
out_path = "parquet/yelp_review_restaurant"
print(f"\n========== Save to {out_path} ==========")
df_restaurant.write.mode("overwrite").parquet(f"{out_path}")
print(f"[info] wrote Parquet → {out_path}")


