# Step modules
from jobs import yelp_review as reviews_step
from enriched import join_reviews_with_business as join_step
import json
from load_ontology import build_comprehensive_ontology
from aspect_mapper import AspectMapper

import argparse
from typing import Iterable, Dict, List, Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lower, pandas_udf, PandasUDFType
from pyspark.sql import Row
import sys

import pandas as pd

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

# ========= rebuild ontology + mapper on this executor ========= 
ontology_json_path = "input_ontology.json"
with open(ontology_json_path, "r") as f:
    ONTLOLOGY = json.load(f)
sc = spark.sparkContext
ontology_spec_bc = sc.broadcast(ONTLOLOGY)

# Globals on executor side
_mapper_singleton = {"mapper": None}

@pandas_udf(
    "matched boolean, " +
    "category string, category_score double, category_trigger string, " +
    "sub_aspect string, sub_aspect_score double, sub_aspect_trigger string",
    PandasUDFType.SCALAR,
)
def map_aspect_udf(phrase_series: pd.Series) -> pd.DataFrame:
    """
    phrase_series: pd.Series of aspect span strings from Spark.
    returns columns for each phrase.
    """
    # Lazy-init per executor
    if _mapper_singleton["mapper"] is None:
        local_ontology = build_comprehensive_ontology(ontology_spec_bc.value)
        _mapper_singleton["mapper"] = AspectMapper(local_ontology)

    mapper = _mapper_singleton["mapper"]

    out_matched = []
    out_cat = []
    out_cat_score = []
    out_cat_trigger = []
    out_sub = []
    out_sub_score = []
    out_sub_trigger = []

    for phrase in phrase_series.fillna(""):
        res = mapper.map_phrase(phrase)

        out_matched.append(bool(res["matched"]))
        out_cat.append(res["category"] if res["category"] else None)
        out_cat_score.append(float(res["category_score"]) if res["category_score"] is not None else None)
        out_cat_trigger.append(res["category_trigger"] if res["category_trigger"] else None)
        out_sub.append(res["sub_aspect"] if res["sub_aspect"] else None)
        out_sub_score.append(float(res["sub_aspect_score"]) if res["sub_aspect_score"] is not None else None)
        out_sub_trigger.append(res["sub_aspect_trigger"] if res["sub_aspect_trigger"] else None)

    return pd.DataFrame({
        "matched": out_matched,
        "category": out_cat,
        "category_score": out_cat_score,
        "category_trigger": out_cat_trigger,
        "sub_aspect": out_sub,
        "sub_aspect_score": out_sub_score,
        "sub_aspect_trigger": out_sub_trigger,
    })


        

def extract_aspect_from_text(
    input_path: str,
    text_col: str,
    ontology_json_path: str,
    meta_cols: Optional[List[str]] = None,   # e.g., ["business_id","stars","date"]
    out_path: Optional[str] = None,          # if None, returns a DataFrame instead of writing
    repartition_n: Optional[int] = 1
):
    """
    Extracts aspects from text data using SpaCy and PySpark.
    
    This function processes text data to identify aspects and their associated opinions,
    
    Args:
        input_path (str): Path to input data file Parquet format
        text_col (str, optional): Name of the column containing review text.
        meta_cols (List[str], optional): Additional columns to keep in output. Defaults to None.
        out_path (str, optional): Path to save output Parquet file. If None, returns DataFrame.
        seeds (Dict): Dictionary of aspect  and their associated seed words.
        aspects (List[str]): List of target aspects to extract.
        spacy_model (str, optional): Name of SpaCy model to use. Defaults to "en_core_web_sm".
        repartition_n (int, optional): Number of partitions for output. Defaults to 1.
    
    Returns:
        Optional[pyspark.sql.DataFrame]: If out_path is None, returns DataFrame with extracted aspects.
        Otherwise writes to Parquet and returns None.
    """

    # ========= load the enriched data =========
    meta_cols = meta_cols or []

    df = spark.read.parquet(input_path)

    # Assuming 'df' is your PySpark DataFrame
    null_count = df.filter(col("aspect_seed").isNull()).count()

    print(f"Number of nulls in 'aspect_seed': {null_count}")

    # ========= For testing, limit data size =========
    data_size = 10000
    # df = df_restaurant.limit(data_size)

    # =========  For full data, use =========
    # df = df_restaurant
    # df.show()

    # ========= process the data to extract aspects =========
    # Use the process_partition function with the dataframe
    result_df = df.select(
        "aspect_seed",
        map_aspect_udf(col("aspect_seed")).alias("mapped")
    ).select(
        "aspect_seed",
        col("mapped.matched").alias("matched"),
        col("mapped.category").alias("category"),
        col("mapped.category_score").alias("category_score"),
        col("mapped.category_trigger").alias("category_trigger"),
        col("mapped.sub_aspect").alias("sub_aspect"),
        col("mapped.sub_aspect_score").alias("sub_aspect_score"),
        col("mapped.sub_aspect_trigger").alias("sub_aspect_trigger"),
    )

    result_df.show()

    # ========= Repartition if needed =========
    if repartition_n:
        result_df = result_df.repartition(repartition_n)
    

    # ========= output the results =========
    result_df.write.mode("overwrite").parquet(f"{out_path}_{data_size if str(data_size) else ''}")
    print(f"[info] wrote Parquet → {out_path}_{data_size if str(data_size) else ''}")
    result_df.toPandas().to_csv(f"{out_path}_{data_size if str(data_size) else ''}/restaurant_reviews_with_aspect_catagorized.csv")
    spark.stop()

    return 


# ------------ MAIN FUNCTION ------------
if __name__ == "__main__":

    ENRICHED_PARQUET = "parquet/absa_restaurant_parquet_10000"

    # Edit these if you want to run this file directly (no argparse)
    INPUT_PATH = ENRICHED_PARQUET
    TEXT_COL   = "text"
    META_COLS  = ["business_id","biz_name","stars","date","biz_categories"]   # put your columns here (or [])
    OUT_PATH   = f"parquet/restaurant_reviews_with_aspect_catagorized"  # or set to None to return DF
    SPACY_MODEL = "en_core_web_sm"
    REPARTITION = 1
    ONTOLOGY_JSON_PATH = "input_ontology.json"



    # run the extraction
    extract_aspect_from_text(
        input_path=INPUT_PATH,
        text_col=TEXT_COL,
        meta_cols=META_COLS,
        out_path=OUT_PATH,
        ontology_json_path=ONTOLOGY_JSON_PATH,
        repartition_n=REPARTITION
    )
