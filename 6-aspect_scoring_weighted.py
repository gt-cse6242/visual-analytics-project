#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
========= description ========================
Aggregate per-review aspect scores to restaurant-level vectors using reviewer weights.
- input_path  : parquet/yelp_review_restaurant_review_level_scoring
- input_weight: parquet/yelp_user_weights
- out_path    : 
        parquet/yelp_review_restaurant_with_user_weighted_and_restaurant_level_scoring
        final_aspect_sentiment_results.csv
'''

import time
import os
from pyspark.sql import SparkSession, functions as F
import sys

def main():
    t0 = time.time()

    spark = (
    SparkSession.builder
    .appName("AspectScoringWeighted")
    # make sure executors use your venv python
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)

    # give driver/executor a bit more heap
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .config("spark.driver.maxResultSize", "6g") 

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
    spark.sparkContext.setLogLevel("ERROR")

    # -----------------------------
    # 1) Load inputs
    # -----------------------------
    input_path = "parquet/yelp_review_restaurant_review_level_scoring"                     # <- baseline scored reviews
    input_weight = "parquet/yelp_user_weights"       # <- from Part I

    print("\n========== Load inputs ==========")
    print(f"Scored reviews: {os.path.abspath(input_path)}")
    print(f"Reviewer weights: {os.path.abspath(input_weight)}")

    df = spark.read.parquet(input_path)
    df.printSchema()
    df_weights = spark.read.parquet(input_weight)

    # # sanity columns
    # need_review_cols = {"business_id", "user_id",
    #                     "aspect_food", "aspect_service", "aspect_price", "aspect_amb"}
    # missing = need_review_cols - set(df.columns)
    # if missing:
    #     raise ValueError(f"Missing required columns in scored reviews: {missing}")

    # need_weight_cols = {"user_id", "w_user"}
    # missing_w = need_weight_cols - set(df_weights.columns)
    # if missing_w:
    #     raise ValueError(f"Missing required columns in reviewer weights: {missing_w}")

    print(f"✅ Review rows: {df.count():,}")
    print(f"✅ Weight rows: {df_weights.count():,}")

    # -----------------------------
    # 2) Join weights; default to 1.0 if user missing
    # -----------------------------
    df_joined = (
        df.join(df_weights.select("user_id", "w_user"), on="user_id", how="left")
          .withColumn("w_user", F.coalesce(F.col("w_user"), F.lit(1.0)))
    )

    # -----------------------------
    # 3) Weighted aggregation per review
    # -----------------------------
    df_review_with_scoring = (df_joined.groupBy("review_id",
                                                "sentence", 
                                                "user_id",
                                                "business_id",
                                                "biz_name",
                                                "biz_categories",
                                                "biz_city",
                                                "biz_state",
                                                "date",
                                                "stars",
                                                "aspect",).agg(
        # weighted numerators
        F.sum(F.col("w_user") * F.col("positive_aspect_ratio")).alias("weighted_positive_aspect_ratio"),
        # denominator
        F.sum(F.col("w_user")).alias("w_user_den"),
        # basic counts
        F.count(F.lit(1)).alias("review_count"),
        F.countDistinct("user_id").alias("user_account"),
        # sum up the positive and negative count
        F.sum(F.col("positive_aspect_count")).alias("positive_aspect_count_sum"),
        F.sum(F.col("negative_aspect_count")).alias("negative_aspect_count_sum"),
    )
    # weighted means
    .withColumn("weighted_mean", F.when(F.col("w_user_den") > 0, F.col("weighted_positive_aspect_ratio")/F.col("w_user_den")).otherwise(F.lit(0.0)))
    )

    # df_review_with_scoring.orderBy("review_id").limit(10000).toPandas().to_csv("6-aspect_scoring_intermediate.csv", index=False)

    # Calculate the mean of the 'value' column
    mean_value = df_review_with_scoring.agg(F.mean("weighted_positive_aspect_ratio")).head()[0]

    # Normalize the 'value' column by subtracting its mean
    df_normalized = df_review_with_scoring.withColumn("normalized_weighted_positive_aspect_ratio", F.col("weighted_positive_aspect_ratio") - mean_value)

    # df_normalized = df_review_with_scoring.withColumnRenamed("weighted_positive_aspect_ratio", "normalized_weighted_positive_aspect_ratio")
    df_normalized.printSchema()

    # -----------------------------
    # 5) Optional: create restaurant level sentiment score.
    # -----------------------------
    # df_biz_with_scoring = (df_normalized.groupBy("business_id","biz_categories","biz_name","biz_city","biz_state","aspect").agg(
    #     # weighted numerators
    #     F.sum(F.col("w_user") * F.col("positive_aspect_ratio")).alias("weighted_positive_aspect_ratio"),
    #     # denominator
    #     F.sum(F.col("w_user")).alias("w_user_den"),
    #     # basic counts
    #     F.count(F.lit(1)).alias("review_count"),
    #     F.countDistinct("user_id").alias("user_account"),
    #     # sum up the positive and negative count
    #     F.sum(F.col("positive_aspect_count")).alias("positive_aspect_count_sum"),
    #     F.sum(F.col("negative_aspect_count")).alias("negative_aspect_count_sum"),
    # )
    # # weighted means
    # .withColumn("weighted_mean", F.when(F.col("w_user_den") > 0, F.col("weighted_positive_aspect_ratio")/F.col("w_user_den")).otherwise(F.lit(0.0)))
    # )
    

    # -----------------------------
    # 5) Optional: enforce min review count per restaurant
    # (kept same as your baseline; adjust if needed)
    # -----------------------------
    # MIN_REV = 10
    # print(f"✅ before min {MIN_REV} reviews filter: {df_normalized.count()} restaurants")
    # df_normalized_filtered = df_normalized.filter(F.col("review_count") >= MIN_REV)
    # print(f"✅ After min {MIN_REV} reviews filter: {df_normalized_filtered.count()} restaurants")

    df_normalized = df_normalized.withColumnRenamed("sentence", "text")\
        .withColumnRenamed("sentence", "text")\
        .withColumnRenamed("stars", "review_stars")\
        .withColumnRenamed("biz_name", "name")\
        .withColumnRenamed("biz_city", "city")\
        .withColumnRenamed("biz_state", "state")\
        .withColumnRenamed("biz_categories", "categories")\
        .withColumnRenamed("biz_name", "name")\
        .withColumnRenamed("weighted_positive_aspect_ratio","sentiment_score_before_normalization")\
        .withColumnRenamed("normalized_weighted_positive_aspect_ratio","sentiment_score")\
        .orderBy("business_id","review_id")
        
    df_normalized = df_normalized.select("business_id",
                                         "name",
                                         "city",
                                         "review_stars",
                                         "state",
                                         "categories",
                                         "review_id",
                                         "user_id",
                                         "text",
                                         "date",
                                         "review_count",
                                         "aspect",
                                         "sentiment_score",
                                         "sentiment_score_before_normalization",
                                         "positive_aspect_count_sum",
                                         "negative_aspect_count_sum")

    # df_normalized.printSchema()
    print(f"number of reviews {df_normalized.count()}")

    # join the dataframe back to the business and review data
    enriched_path = "parquet/yelp_review_enriched"
    df_enriched = spark.read.parquet(enriched_path).select("business_id","biz_stars").dropDuplicates()
    df_enriched = df_enriched.withColumnRenamed("biz_stars", "business_stars")
    df_normalized_with_biz_star = df_normalized.join(df_enriched, on="business_id", how="left")

    # calculate the average star from the review
    df_normalized_with_biz_star.printSchema()

    df_normalized_with_biz_star.toPandas().to_csv("6-aspect_scoring_weighted.csv", index=False) #.filter(F.col("review_id")=="--13tiFLodEng6H8C55doQ")

    out_path = "parquet/yelp_review_restaurant_restaurant_level_scoring_with_user_credibility"
    print(f"\n========== Save to {out_path} ==========")
    df_normalized_with_biz_star.write.mode("overwrite").parquet(out_path)


    spark.stop()
    print(f"✅ Done in {time.time()-t0:.2f}s")

if __name__ == "__main__":
    main()
