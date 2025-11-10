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

def main():
    t0 = time.time()

    spark = (
        SparkSession.builder
        .appName("AggregateAspectsWeighted")
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
    # 3) Weighted aggregation per business
    # -----------------------------
    df_biz_with_scoring = (df_joined.groupBy("business_id","biz_categories","biz_name","aspect").agg(
        # weighted numerators
        F.sum(F.col("w_user") * F.col("positive_aspect_ratio")).alias("weighted_positive_aspect_ratio"),
        # denominator
        F.sum(F.col("w_user")).alias("w_user_den"),
        # basic counts
        F.count(F.lit(1)).alias("n_reviews"),
        F.countDistinct("user_id").alias("n_users"),
        # sum up the positive and negative count
        F.sum(F.col("positive_aspect_count")).alias("positive_aspect_count_sum"),
        F.sum(F.col("negative_aspect_count")).alias("negative_aspect_count_sum"),
    )
    # weighted means
    .withColumn("weighted_mean", F.when(F.col("w_user_den") > 0, F.col("weighted_positive_aspect_ratio")/F.col("w_user_den")).otherwise(F.lit(0.0)))
    )
    

    # -----------------------------
    # 4) Optional: enforce min review count per restaurant
    # (kept same as your baseline; adjust if needed)
    # -----------------------------
    MIN_REV = 10
    print(f"✅ before min {MIN_REV} reviews filter: {df_biz_with_scoring.count():,} restaurants")
    df_biz_with_scoring_filtered = df_biz_with_scoring.filter(F.col("n_reviews") >= MIN_REV)
    df_biz_with_scoring_filtered.printSchema()
    print(f"✅ After min {MIN_REV} reviews filter: {df_biz_with_scoring_filtered.count():,} restaurants")


    out_path = "parquet/yelp_review_restaurant_restaurant_level_scoring_with_user_credibility"
    print(f"\n========== Save to {out_path} ==========")
    df_biz_with_scoring_filtered.write.mode("overwrite").parquet(out_path)

    df_biz_with_scoring_filtered.toPandas().to_csv("final_aspect_sentiment_results.csv", index=False)


    spark.stop()
    print(f"✅ Done in {time.time()-t0:.2f}s")

if __name__ == "__main__":
    main()
