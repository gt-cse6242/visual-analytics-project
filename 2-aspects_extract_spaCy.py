import argparse
from typing import Iterable, Dict, List, Optional
from matplotlib import text
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
    Extracts aspect-opinion pairs from a sentence using dependency parsing. This function is used in function 'process_partition'.
    
    Args:
        sent (spacy.tokens.Span): A SpaCy sentence span to analyze
        
    Returns:
        list: List of dictionaries containing aspect-opinion pairs
    """

    def expand_noun_span(head):
        """ Return a clean aspect span around a noun: includes compounds/det on the left.
        amod: adjective modifier
        compound:
        det:
        poss:
        nummod: 
        """
        left = [t for t in head.lefts if t.dep_ in ("compound","amod","det","poss","nummod")]
        # pull in 'of'-phrase: 'cup of coffee'
        rights = []
        for r in head.rights:
            if r.dep_ == "prep" and r.lemma_ in {"of","for","at","in","on"}:
                rights.append(r)
                rights += [t for t in r.subtree if t.i > r.i]
        tokens = left + [head] + rights
        tokens = sorted(set(tokens), key=lambda t: t.i)
        return head.doc[tokens[0].i : tokens[-1].i + 1]

    def expand_adj_span(adj):
        """ Return opinion span with degree adv + negation.
        adv: adverb
        negation: negation token
        advmodL adverb modifier
        """
        # get all of the left token left to the adverb
        left = list(adj.lefts)
        # go thru the left token lists and extract advmod and pre-defiend intensifier
        advmods = [t for t in left if t.dep_ == "advmod" or (t.pos_=="ADV" and t.lemma_.lower() in INTENSIFIERS)]
        # go thru the left token lists and extract negation token and pre-defined nagation words
        negs = [t for t in adj.subtree if t.dep_ == "neg" or t.lemma_.lower() in NEG_WORDS]
        # reorganized the extracted token lists based on the token position id
        toks = sorted({*advmods, adj, *negs}, key=lambda t: t.i)
        # TODO: i don't know what this line of code does
        return adj.doc[toks[0].i : toks[-1].i + 1]

    def is_negated(token):
        """ check whether a token is a negation. iF yes return true, otherwise return false. """
        return any(t.dep_ == "neg" or t.lemma_.lower() in NEG_WORDS for t in token.subtree)

    def conj_expand(token, pos_set):
        """ Return token and its siblings of the same POS (e.g. tacos and burritos)."""
        out = [token]
        for sib in token.conjuncts:
            if sib.pos_ in pos_set:
                out.append(sib)
        return out

    def add_pair(pairs, aspect_span, opinion_span):
        """ Add the pair to the output dictionary """
        a = aspect_span.text.strip()
        o = opinion_span.text.strip()
        if a and o:
            pairs.append({
                "aspect": a,
                "opinion": o,
                "negated": any(w.lower_ in NEG_WORDS for w in opinion_span) or is_negated(opinion_span.root),
            })


    INTENSIFIERS = {"very","so","too","extremely","really","quite","super","pretty","fairly","highly","slightly","somewhat","kinda","sorta"}
    NEG_WORDS = {"no","not","n't","never","hardly","barely","scarcely","nothing",}

    pairs = []
    # 1) amod: "great food", "friendly staff"
    for n in sent:
        if n.pos_ in {"NOUN","PROPN"}:
            nouns = conj_expand(n, {"NOUN","PROPN"})
            for noun in nouns:
                for child in noun.children:
                    if child.dep_ == "amod" and child.pos_ == "ADJ":
                        # expand adj conj, e.g., "quick and attentive service"
                        adjs = conj_expand(child, {"ADJ"})
                        for adj in adjs:
                            add_pair(pairs, expand_noun_span(noun), expand_adj_span(adj))

    # - 2) copular / predicate adjectives:
    # -- Case A: ADJ with 'cop' and nsubj  -> "service is slow"
    # extract the list of adjs from the sentences
    adjs =[t for t in sent if t.pos_=="ADJ" and any(c.dep_=="cop" for c in t.children)]
    for adj in adjs:
        # extract the subject that the adjective is describing
        subjects = [c for c in adj.children if c.dep_ in ("nsubj","nsubjpass")]
        # extract todo:
        if not subjects and adj.head.dep_ == "relcl" and adj.head.head.pos_ in {"NOUN","PROPN"}:
            subjects = [adj.head.head]  # "service that was slow"
        subs = []
        # extract the noun and its siblings
        for s in subjects:
            subs += conj_expand(s, {"NOUN","PROPN"})
        # add the noun as the seed and adjective as the opinion
        for s in subs:
            add_pair(pairs, expand_noun_span(s), expand_adj_span(adj))

    # Case B: acomp on a verb with subject -> "the fries were good"
    for v in [t for t in sent if t.pos_ in {"VERB","AUX"}]:
        # todo: how is this different from the case A??
        adjs = [c for c in v.children if c.dep_ == "acomp" and c.pos_ == "ADJ"]
        subs = [c for c in v.children if c.dep_ in ("nsubj","nsubjpass") and c.pos_ in {"NOUN","PROPN"}]
        # share subjects across conjunct verbs
        if v.dep_ == "conj" and not subs:
            subs = [c for c in v.head.children if c.dep_ in ("nsubj","nsubjpass")]
        subj_expanded = []
        for s in subs:
            subj_expanded += conj_expand(s, {"NOUN","PROPN"})
        for a in adjs:
            for s in subj_expanded:
                add_pair(pairs, expand_noun_span(s), expand_adj_span(a))

    # --- 3) Verb + object + result/state adj: "made the noodles soggy"
    for v in [t for t in sent if t.pos_ == "VERB"]:
        dobjs = [c for c in v.children if c.dep_ in ("dobj","obj")]
        # Adjectival xcomp or complement attached to the verb
        xcomps = [c for c in v.children if c.dep_ in ("xcomp","ccomp") and c.pos_=="ADJ"]
        for obj in dobjs:
            objs = conj_expand(obj, {"NOUN","PROPN"})
            # Pattern: direct adj child of object via amod (e.g., "love spicy food")
            for o in objs:
                amods = [c for c in o.children if c.dep_=="amod" and c.pos_=="ADJ"]
                for a in amods:
                    add_pair(pairs, expand_noun_span(o), expand_adj_span(a))
                # Verb->xcomp ADJ: "made noodles soggy"
                for a in xcomps:
                    add_pair(pairs, expand_noun_span(o), expand_adj_span(a))

    # --- 4) Relative clauses with adjective inside: "service that was painfully slow"
    for rc in [t for t in sent if t.dep_ == "relcl" and t.head.pos_ in {"NOUN","PROPN"}]:
        # collect predicate adjectives under the relative clause verb
        adjs = [d for d in rc.subtree if d.pos_=="ADJ" and (d.dep_=="acomp" or any(c.dep_=="cop" for c in d.children))]
        for a in adjs:
            add_pair(pairs, expand_noun_span(rc.head), expand_adj_span(a))

    # --- 5) Coordinated adjectives to one subject: "tacos were fresh and tasty"
    for v in [t for t in sent if t.pos_ in {"VERB","AUX"}]:
        subs = [c for c in v.children if c.dep_ in ("nsubj","nsubjpass") and c.pos_ in {"NOUN","PROPN"}]
        if not subs and v.dep_ == "conj":
            subs = [c for c in v.head.children if c.dep_ in ("nsubj","nsubjpass")]
        pred_adj_heads = [c for c in v.children if c.dep_ in ("acomp","attr") and c.pos_=="ADJ"]
        for head_adj in pred_adj_heads:
            for s in subs:
                # expand both subject and adj coordination
                for s2 in conj_expand(s, {"NOUN","PROPN"}):
                    for a2 in conj_expand(head_adj, {"ADJ"}):
                        add_pair(pairs, expand_noun_span(s2), expand_adj_span(a2))

    seen = set()
    uniq = []
    for p in pairs:
        key = (p["aspect"].lower(), p["opinion"].lower(), p["negated"])
        if key not in seen:
            uniq.append(p)
            seen.add(key)
    return uniq

def process_partition(rows: Iterable[Row], 
                        text_col: str, 
                        meta_cols: List[str], 
                        spacy_model: str) -> Iterable[Row]: 
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
            # save the orignal text
            original_text = sent.text.strip()

            pairs = extract_pairs_from_sentence(sent)
            if not pairs:
                continue
            for p in pairs:
                # Process the text with spaCy
                aspect_seeds = nlp(p["aspect"])

                # Remove stop words
                filtered_tokens = [token for token in aspect_seeds if not token.is_stop]

                # Reconstruct the text without stop words
                filtered_text = " ".join([token.text for token in filtered_tokens])

                yield Row(
                    sentence=original_text,
                    aspect_seed=filtered_text,
                    aspect_opinion=p["opinion"],
                    aspect_negated=p["negated"],
                    **{c: r[c] for c in meta_cols} if meta_cols else {}
                )


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
    .config("spark.sql.parquet.columnarReaderBatchSize", "4096")          # default ~4096; lower uses less heap
    # If still OOM, try disabling vectorization:
    # .config("spark.sql.parquet.enableVectorizedReader", "false")

    # make Python workers reusable (fewer forks)
    .config("spark.python.worker.reuse", "true")

    # fewer rows per shuffle task (helps local dev)
    .config("spark.sql.shuffle.partitions", "100")
    .getOrCreate()
)




input_path = "parquet/yelp_review_restaurant"
text_col   = "text"
meta_cols  = ["review_id","business_id","stars","user_id","biz_name","biz_categories"] 
spacy_model = "en_core_web_sm"
repartition_n = 1

print(f"\n========== Loading input from {input_path}) ===========")
df = spark.read.parquet(input_path)
print(f"[info] read Parquet ← {input_path} with {df.count()} rows")
df.printSchema()

# Keep only needed columns
keep = [text_col] + meta_cols
df = df.select(*keep)

# ========= filter the data to just restaurants =========
# df_restaurant = df.withColumn("categories", lower(df["categories"]))\
#             .filter(col("categories").like("%restaurants"))
# print(f"Full business review count: {df.count()}")
# print(f"Restaurant business review count: {df_restaurant.count()}")

# ========= For testing, limit data size =========
# data_size = 10000
# df = df.limit(data_size)


"""
Aspect seed lexicon for domain-specific Aspect-Based Sentiment Analysis (ABSA).
Each top-level key is a target aspect (food, service, amb, price),
and each value is a Python set of seed lemmas commonly found in Yelp-style reviews.
"""


print("\n========== process the data to extract aspects =================================")
# Use the process_partition function with the dataframe
rdd = df.rdd.mapPartitions(
    lambda rows: process_partition(rows, 
                                    text_col, 
                                    meta_cols or [], 
                                    spacy_model)
)

# convert RDD back to DataFrame and save results in a pyspark dataframe
schema_fields = [
    StructField("sentence", StringType(), True),
    StructField("aspect_seed", StringType(), True),
    StructField("aspect_opinion", StringType(), True),
    StructField("aspect_negated", StringType(), True)
] + [StructField(c, StringType(), True) for c in meta_cols]
schema = StructType(schema_fields)

# run the final conversion
result_df = spark.createDataFrame(rdd, schema=schema)
# result_df.show()

# result_df = result_df.limit(10000)

print(f"I'm here")

# # ========= Repartition if needed =========
# if repartition_n:
#     result_df = result_df.repartition(repartition_n)

out_path   = f"parquet/yelp_review_restaurant_with_aspect_seeds_extracted" 
print(f"\n========== Save to {out_path} ==========")
result_df.write.mode("overwrite").parquet(f"{out_path}")
print(f"[info] wrote Parquet → {out_path}")
# result_df.limit(20000).toPandas().to_csv(f"{out_path}_{data_size if str(data_size) else ''}/../restaurant_reviews_with_aspect_extracted.csv")

spark.stop()


