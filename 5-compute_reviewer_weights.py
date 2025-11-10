#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
========= description ========================
input_path : parquet/yelp_review_restaurant_review_level_scoring
out_path   : parquet/yelp_user_weights

Compute reviewer credibility weights (w_user) from review variance.
==============================================
'''

import time
import os
from pyspark.sql import SparkSession, functions as F

def main():
    t0 = time.time()

    spark = (
        SparkSession.builder
        .appName("ComputeReviewerWeights")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # -----------------------------
    # 1) Load baseline-scored reviews (sample)
    # -----------------------------
    input_path = "parquet/yelp_review_restaurant_review_level_scoring"  # <- baseline output
    print(f"\n========== Loading input from {input_path}) ===========")
    df = spark.read.parquet(input_path)
    print(f"[info] read Parquet ← {input_path} with {df.count()} rows")
    df.printSchema()

    required_cols = {"user_id", "stars"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in input: {missing}")

    n_rows = df.count()
    print(f"Loaded {n_rows:,} reviews")

    # -----------------------------
    # 2) Compute reviewer weights
    #     w_user = 1 / (1 + Var_user(review_stars))
    # -----------------------------
    print("\n========== Computing reviewer credibility weights ==========")
    t1 = time.time()
    reviewer_stats = (
        df.groupBy("user_id")
          .agg(
              F.var_pop("stars").alias("var_review_stars"),
              F.count(F.lit(1)).alias("user_review_count_in_sample")
          )
          # Handle edge cases
          # If a user has only one review, Spark can’t compute a variance (it’s null), so this replaces it with 0. 
          # → A single-review user is treated as fully credible (weight = 1.0).
          .withColumn("var_review_stars", F.coalesce(F.col("var_review_stars"), F.lit(0.0)))
          .withColumn("w_user", 1.0 / (1.0 + F.col("var_review_stars"))) # Compute the credibility weight
          # When variance = 0 → w=1/(1+0)=1.0 → fully credible.
          # When variance = 3 → w=1/(1+3)=0.25 → down-weighted.
    )

    print(f"Reviewer rows: {reviewer_stats.count():,} (computed in {time.time()-t1:.2f}s)")
    reviewer_stats.select("user_id", "user_review_count_in_sample", "var_review_stars", "w_user").show(5, truncate=False)

    # -----------------------------
    # 3) Save weights to a NEW folder
    # -----------------------------
    out_dir = "parquet/yelp_user_weights"
    print(f"\n💾 Writing reviewer weights to {out_dir}")
    reviewer_stats.write.mode("overwrite").parquet(out_dir)
    print(f"Done in {time.time()-t0:.2f}s")

    spark.stop()

if __name__ == "__main__":
    main()
