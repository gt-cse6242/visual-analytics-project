'''
========= description ========================
Used to exam any parquet file
- input_path  : parquet file of your choice
- out_path    : xxxx.csv
'''

from pyspark.sql import SparkSession, functions as F, types as T

# 0. Start Spark
spark = (
    SparkSession.builder
    .appName("AspectTermBucketing")
    .config("spark.sql.shuffle.partitions", "4")
    .master("local[2]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


df = spark.read.parquet("parquet/yelp_review_restaurant_review_level_scoring").orderBy("review_id")

df.printSchema()

df.show(200, truncate=False)

out_path = "xxxx.csv"
print(f"\n========== Save to {out_path} ==========")
df.limit(1000).toPandas().to_csv(out_path, index=False)

