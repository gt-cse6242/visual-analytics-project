# Step modules
from jobs import yelp_review as reviews_step
from enriched import join_reviews_with_business as join_step


import argparse
from typing import Iterable, Dict, List, Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lower
from pyspark.sql import Row
import spacy
import sys


# ------------ NLP HELPER FUNCTIONS ------------
_NLP = {"obj": None}  # mutable container for SpaCy model

def get_nlp(spacy_model: str = "en_core_web_sm"):
    """
    Lazy-loads and returns a SpaCy NLP model.
    
    Args:
        spacy_model (str): Name of the SpaCy model to load
    
    Returns:
        spacy.lang: Loaded SpaCy model with sentencizer pipe enabled.
    """
    if _NLP["obj"] is None:
        nlp = spacy.load(spacy_model, disable=["ner"])
        if "sentencizer" not in nlp.pipe_names:
            nlp.add_pipe("sentencizer", first=True)
        _NLP["obj"] = nlp
    return _NLP["obj"]

def normalize_aspect(tok) -> str:
    """
    Normalizes an aspect token by including its compound modifiers.
    
    Args:
        tok (spacy.tokens.Token): The token to normalize
        
    Returns:
        str: Normalized aspect text including compound words, lowercase
    """
    parts = []
    for left in tok.lefts:
        if left.dep_ == "compound":
            parts.append(left.text)
    parts.append(tok.text)
    for right in tok.rights:
        if right.dep_ == "compound":
            parts.append(right.text)
    return " ".join(parts).lower()

def lemmas(aspect: str):
    """
    Reduce the words to their root forms
    
    Args:
        aspect (str): The aspect term to lemmatize
        
    Returns:
        list: List of lemmatized tokens from the aspect
    """
    import re
    WORD = re.compile(r"[a-zA-Z][a-zA-Z\-']+")
    toks = [t.lower() for t in WORD.findall(aspect)]
    lem = []
    for t in toks:
        if t.endswith("ies") and len(t) > 3: lem.append(t[:-3] + "y")
        elif t.endswith("s") and not t.endswith("ss"): lem.append(t[:-1])
        else: lem.append(t)
    return lem

def extract_pairs_from_sentence(sent):
    """
    Extracts aspect-opinion pairs from a sentence using dependency parsing.
    
    This function implements three strategies to find aspect-opinion pairs:
    1. Adjectival modifiers of nouns (e.g., "great food")
    2. Copular constructions with adjective complements (e.g., "service was excellent")
    3. Verbs with noun arguments and adjectival modifiers
    
    Args:
        sent (spacy.tokens.Span): A SpaCy sentence span to analyze
        
    Returns:
        list: List of dictionaries containing aspect-opinion pairs
    """
    pairs = []

    # Iterate through tokens in the sentence
    # Adjectival modifier (amod) -> noun: e.g. "great" food
    for tok in sent:
        if tok.pos_ == "NOUN":
            for child in tok.children:
                if child.dep_ == "amod" and child.pos_ == "ADJ":
                    pairs.append((tok, child))

    # 2) adjective compliment (acomp) with nominal subject (nsubj/nsubjpass): The food is "great"
    for tok in sent:
        if tok.dep_ == "acomp" and tok.pos_ == "ADJ":
            head = tok.head
            for child in head.children:
                if child.dep_ in ("nsubj", "nsubjpass") and child.pos_ in {"NOUN","PROPN"}:
                    pairs.append((child, tok))

    # 3) VERB with NOUN arg and nearby ADJ child: "love the spicy food"
    for tok in sent:
        if tok.pos_ == "VERB":
            noun_dependents = [c for c in tok.children if c.pos_ in {"NOUN","PROPN"} and c.dep_ in ("dobj","nsubj","nsubjpass","pobj")]
            adj_children = [c for c in tok.children if c.pos_ == "ADJ"]
            for n in noun_dependents:
                for a in adj_children:
                    pairs.append((n, a))


    # normalize the tokens and remove duplicates
    out = []
    aspect_opinion_pair = set()
    for a_tok, o_tok in pairs:
        a = normalize_aspect(a_tok)
        if not a:
            continue
        o = o_tok.lemma_.lower()
        key = (a, o, a_tok.i, o_tok.i)
        if key in aspect_opinion_pair:
            continue
        aspect_opinion_pair.add(key)
        out.append({"aspect": a, "opinion": o})
    return out



def route_category(aspect: str, seeds: Dict, targets: List[str]) -> Optional[str]:
    """
    Categorizes an aspect into predefined restaurant-related categories.
    
    Uses seed vocabularies to match aspects to categories like 'food',
    'service', 'environment', or 'price'.
    
    Args:
        aspect (str): The aspect term to categorize
        seeds (Dict): Dictionary of categories and their seed words
        targets (List[str]): List of valid target categories
        
    Returns:
        Optional[str]: Category name if matched, None if no match
    """
    lem = lemmas(aspect)
    for cat, vocab in seeds.items():
        if any(t in vocab for t in lem):
            return cat if cat in targets else None
    a = " ".join(lem)
    for cat, vocab in seeds.items():
        if any((" " in v and v in a) for v in vocab):
            return cat if cat in targets else None
    return None

def process_partition(rows: Iterable[Row], text_col: str, meta_cols: List[str], 
                     seeds: Dict, targets: List[str], spacy_model: str) -> Iterable[Row]:
    """
    Processes a partition of rows to extract aspect-based sentiment.
    
    This is the main processing function used by PySpark to parallelize
    the aspect extraction across multiple workers.
    
    Args:
        rows (Iterable[Row]): Iterator of PySpark Row objects containing text
        text_col (str): Name of the column containing text to analyze
        meta_cols (List[str]): Additional columns to include in output
        seeds (Dict): Dictionary of categories and their seed words
        targets (List[str]): List of valid target categories
        spacy_model (str): Name of SpaCy model to use
        
    Returns:
        Iterable[Row]: Iterator of processed rows with extracted aspects
    """
    nlp = get_nlp(spacy_model)
    for r in rows:
        text = r[text_col]
        if not isinstance(text, str) or not text.strip():
            continue
        doc = nlp(text)
        # process each sentence
        for sent in doc.sents:
            # Remove stop words
            original_text = sent.text.strip()

            filtered_text = [token.text for token in sent if not token.is_stop and not token.is_punct]

            # extract the filtered text into a new spacy doc
            filtered_text = " ".join(filtered_text)
            sent = nlp(filtered_text)[:]

            pairs = extract_pairs_from_sentence(sent)
            if not pairs:
                continue
            for p in pairs:
                cat = route_category(p["aspect"], seeds, targets)
                if cat is None:
                    continue
                yield Row(
                    sentence=original_text,
                    aspect=p["aspect"],
                    opinion=p["opinion"],
                    category=cat,
                    **{c: r[c] for c in meta_cols} if meta_cols else {}
                )

# ------------ MAIN FUNCTION ------------
def extract_aspect_from_text(
    input_path: str,
    text_col: str,
    meta_cols: Optional[List[str]] = None,   # e.g., ["business_id","stars","date"]
    out_path: Optional[str] = None,          # if None, returns a DataFrame instead of writing
    seeds = Dict,
    aspects = List[str],
    spacy_model: str = "en_core_web_sm",
    repartition_n: Optional[int] = 1
):
    """
    Extracts aspect-based sentiment analysis (ABSA) from text data using SpaCy and PySpark.
    
    This function processes text data to identify aspects (nouns) and their associated opinions (adjectives)
    specifically focused on restaurant-related content. It uses natural language processing to extract
    meaningful sentiment pairs and categorizes them into predefined aspects of restaurant reviews.
    
    Args:
        input_path (str): Path to input data file Parquet format
        text_col (str, optional): Name of the column containing review text.
        meta_cols (List[str], optional): Additional columns to keep in output. Defaults to None.
        out_path (str, optional): Path to save output Parquet file. If None, returns DataFrame.
        seeds (Dict): Dictionary of aspect categories and their associated seed words.
        aspects (List[str]): List of target aspect categories to extract.
        spacy_model (str, optional): Name of SpaCy model to use. Defaults to "en_core_web_sm".
        repartition_n (int, optional): Number of partitions for output. Defaults to 1.
    
    Returns:
        Optional[pyspark.sql.DataFrame]: If out_path is None, returns DataFrame with extracted aspects.
        Otherwise writes to Parquet and returns None.
    """

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

    # ========= load the enriched data =========
    meta_cols = meta_cols or []

    df = spark.read.parquet(input_path)
   
    # Keep only needed columns
    keep = [text_col] + meta_cols
    df = df.select(*keep)

    # ========= filter the data to just restaurants =========
    df_restaurant = df.withColumn("biz_categories", lower(df["biz_categories"]))\
                .filter(col("biz_categories").like("%restaurants"))
    print(f"Full business review count: {df.count()}")
    print(f"Restaurant business review count: {df_restaurant.count()}")

    # ========= For testing, limit data size =========
    data_size = 1000
    df = df_restaurant.limit(data_size)

    # =========  For full data, use =========
    # df = df_restaurant
    # df.show()

    # ========= process the data to extract aspects =========
    # Use the process_partition function with the dataframe
    rdd = df.rdd.mapPartitions(
        lambda rows: process_partition(rows, text_col, meta_cols or [], seeds, aspects, spacy_model)
    )

    # convert RDD back to DataFrame and save results in a pyspark dataframe
    schema_fields = [
        StructField("sentence", StringType(), True),
        StructField("aspect_seeds", StringType(), True),
        StructField("aspect_opinion", StringType(), True),
        StructField("aspect", StringType(), True),
    ] + [StructField(c, StringType(), True) for c in meta_cols]
    schema = StructType(schema_fields)

    result_df = spark.createDataFrame(rdd, schema=schema)
    result_df.show()

    # ========= Repartition if needed =========
    if repartition_n:
        result_df = result_df.repartition(repartition_n)
    

    # ========= output the results =========
    result_df.write.mode("overwrite").parquet(f"{out_path}_{data_size if str(data_size) else ''}")
    print(f"[info] wrote Parquet → {out_path}_{data_size if str(data_size) else ''}")
    result_df.toPandas().to_csv(f"{out_path}_{data_size if str(data_size) else ''}/restaurant_reviews_with_aspect_extracted.csv")
    spark.stop()

    return 



if __name__ == "__main__":

    ENRICHED_PARQUET = "parquet/yelp_review_enriched"

    # Edit these if you want to run this file directly (no argparse)
    INPUT_PATH = ENRICHED_PARQUET
    TEXT_COL   = "text"
    META_COLS  = ["business_id","biz_name","stars","date","biz_categories"]   # put your columns here (or [])
    OUT_PATH   = f"parquet/absa_restaurant_parquet"  # or set to None to return DF
    SPACY_MODEL = "en_core_web_sm"
    REPARTITION = 1

    # Map word Seeds to their target aspect categories
    RESTAURANT_SEEDS = {
        "food": {
            "food","dish","course","meal","taste","flavor","flavour","spice","seasoning","freshness",
            "portion","serving","menu","appetizer","entree","dessert","pasta","sushi","burger","pizza",
            "steak","noodle","soup","salad","bread","coffee","tea","drink","beverage","wine","cocktail",
            "temperature","texture","presentation","sauce","vegan","vegetarian","gluten-free"
        },
        "service": {
            "service","server","waiter","waitress","staff","waitstaff","host","hostess","bartender",
            "manager","attitude","attentive","responsive","rude","friendly","professional",
            "checkin","refill","timing","speed","slow","rush","tip","tipping"
        },
        "environment": {
            "environment","ambience","ambiance","atmosphere","vibe","decor","music","noise","noisy",
            "quiet","lighting","seating","table","booth","patio","view","crowded","space","spacious",
            "cleanliness","clean","dirty","restroom","bathroom","parking","temperature","ac","air",
            "smell","odor"
        },
        "price": {
            "price","cost","value","expensive","cheap","overpriced","affordable","bill","check",
            "portion-for-price","deal","discount","happy hour","fees","surcharge"
        },
    }

    # target aspects to extract
    ASPECTS = {"food","service","environment","price"}

    # run the extraction
    extract_aspect_from_text(
        input_path=INPUT_PATH,
        text_col=TEXT_COL,
        meta_cols=META_COLS,
        out_path=OUT_PATH,
        seeds = RESTAURANT_SEEDS,
        aspects = ASPECTS,
        spacy_model=SPACY_MODEL,
        repartition_n=REPARTITION
    )

