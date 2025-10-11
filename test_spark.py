"""
Minimal Spark smoke test. Creates a tiny DataFrame and shows it.

Usage:
    python test_spark.py
Expected:
    Prints Spark version and a 2-row table.
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("local-test")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)

df = spark.createDataFrame([(1, "apple"), (2, "banana")], ["id", "fruit"])
df.show()

spark.stop()
