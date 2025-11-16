from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("local-test")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)

# Read JSON file into dataframe
df = spark.read.json("yelp_dataset/yelp_academic_dataset_business.json")
df.printSchema()
df.show()

df.select(
    col("attributes").getField("AcceptsInsurance").alias("AcceptsInsurance").isNotNull()
).show()


spark.stop()
