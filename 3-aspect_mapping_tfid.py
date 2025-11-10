'''
========= description ========================
input_path : parquet/yelp_review_restaurant
out_path   : parquet/yelp_review_restaurant_with_aspect_seeds_extracted

Map the aspect seeds to pre-defined aspect: food, service, ambience, and price
1. Apply term frequency and inverse document frequency weighing scheme
to characters in the aspect seeds and build a unique term list from the review
2. use TfidfVectorizer to fit on both unique terms and pre-defined aspect prototypes (dictionary). 
3. use cosine similarity to compare the unique terms to aspects prototypes. 
4. fine-tune threshold to find the best mapping mechanism. 
5. Back up: Use lexicon-based mapping for the aspect seeds that was not mapped successfully to a pre-defined aspect.
==============================================
'''

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql import Row
from typing import Iterable, Dict, List, Optional
import numpy as np
import pandas as pd
import re
from sklearn.feature_extraction.text import TfidfVectorizer
import sys

from pyspark.sql.functions import col, lower

print("\n========== Load the Aspect MAPPINGS ===========================================")
MAPPINGS = {
    "food": [
        "food", "meal", "cuisine", "dish", "plate", "course", "appetizer", "entree", "dessert", "snack",
        "ingredient", "seasoning", "spice", "herb", "sauce", "dressing", "condiment", "dip", "spread", "garnish",
        "taste", "flavor", "aroma", "texture", "fresh", "frozen", "organic", "vegan", "vegetarian", "gluten-free", "dairy-free",
        "sweet", "savory", "spicy", "sour", "salty", "bitter", "umami", "tangy", "rich", "mild", "smoky", "zesty", "bland", "flavorful",
        "meat", "beef", "pork", "chicken", "turkey", "lamb", "veal", "sausage", "bacon", "ham", "steak", "ribeye", "brisket", "chorizo", "charcuterie", "prosciutto",
        "fish", "seafood", "shrimp", "crab", "lobster", "calamari", "oyster", "clam", "scallop", "sushi", "sashimi", "tuna", "salmon", "cod", "trout",
        "egg", "breakfast", "brunch", "omelet", "benedict", "hash brown", "eggplant", "quiche", "bagels", "toast", "pancake", "waffle", "french toast",
        "bread", "bun", "baguette", "roll", "pita", "flatbread", "tortilla", "sandwich", "burger", "sliders", "wrap", "panini", "sammie",
        "pasta", "spaghetti", "fettuccine", "lasagna", "ravioli", "mac cheese", "noodles", "ramen", "pho", "dim sum", "dumpling", "fusion",
        "rice", "risotto", "paella", "biryani", "fried rice", "jambalaya", "curry", "tikka masala", "enchiladas", "burrito", "tacos", "quesadilla", "nachos",
        "vegetable", "veggie", "greens", "salad", "kale", "spinach", "asparagus", "broccoli", "cauliflower", "carrot", "beet", "lettuce", "tomato", "onion", "pepper", "mushroom", "olive", "pickles",
        "fruit", "banana", "berries", "mango", "apple", "pineapple", "orange", "lemon", "lime", "cranberry", "melon", "smoothie", "juice",
        "dessert", "cake", "pie", "tart", "pastry", "donut", "cookie", "brownie", "pudding", "ice cream", "gelato", "sorbet", "custard", "mousse", "cheesecake",
        "beverage", "drink", "coffee", "espresso", "latte", "cappuccino", "mocha", "cold brew", "tea", "loose leaf", "milkshake", "soda", "juice", "boba",
        "beer", "wine", "whiskey", "rum", "tequila", "vodka", "gin", "champagne", "cocktail", "martini", "mimosa",
        "baking", "fried", "grilled", "roasted", "smoked", "seared", "sautéed", "steamed", "boiled", "poached", "raw", "crispy", "crunchy", "tender", "creamy", "juicy",
        "cultural", "mexican", "italian", "chinese", "japanese", "korean", "thai", "indian", "greek", "mediterranean", "middle eastern", "peruvian", "french", "american", "southern", "cajun", "new orleans", "bbq", "barbeque",
        "presentation", "plating", "portion", "garnish", "fresh", "homemade", "authentic", "artisanal", "craft", "local", "farm-to-table", "seasonal", "tasty", "delicious", "yummy"
    ],
    "service": [
        "service", "staff", "server", "waiter", "waitress", "bartender", "barista", "host", "hostess", "management", "team",
        "attentive", "friendly", "polite", "helpful", "responsive", "accommodating", "knowledgeable", "professional", "respectful", "welcoming", "cheerful",
        "rude", "unhelpful", "inattentive", "dismissive", "distracted", "slow", "careless", "neglectful",
        "quick", "fast", "prompt", "efficient", "timely", "smooth", "organized",
        "reservation", "booking", "queue", "line", "waitlist", "seating", "order accuracy", "mistake", "apology", "compensation",
        "customer", "guest", "client", "patron", "regular", "visitor",
        "communication", "interaction", "attitude", "behavior", "responsiveness",
        "complimentary", "free", "refill", "upgrade", "special request", "substitution",
        "delivery", "takeaway", "curbside", "online order", "pickup", "dine-in", "to-go",
        "management", "manager", "owner", "complaint", "response", "feedback", "apology", "fix"
    ],
    "ambience": [
        "ambience", "atmosphere", "vibe", "energy", "mood", "feeling",
        "decor", "design", "aesthetic", "interior", "exterior", "furniture", "layout", "arrangement",
        "lighting", "dim", "bright", "neon", "natural", "warm", "cool", "harsh", "cozy", "romantic", "elegant", "modern", "rustic", "vintage", "industrial", "minimalist", "classy", "casual", "quirky", "artsy",
        "music", "background", "jukebox", "live band", "dj", "playlist", "volume", "noise", "loud", "quiet", "silent", "echo", "acoustics",
        "smell", "aroma", "fragrance", "odor", "clean", "fresh", "stale", "smoky", "musty",
        "temperature", "warm", "cold", "freezing", "hot", "comfortable",
        "space", "room", "area", "seating", "booth", "table", "bar", "patio", "terrace", "balcony", "garden", "rooftop", "outdoor", "indoor", "open", "airy", "cramped", "crowded", "empty", "spacious",
        "cleanliness", "tidy", "messy", "dirty", "spotless", "sanitized", "maintained",
        "clientele", "crowd", "people", "family", "couples", "group", "friends", "women", "men", "kids", "students", "locals", "tourists",
        "comfort", "relaxing", "cozy", "intimate", "formal", "fine dining", "casual", "diner", "lounge", "club", "bar", "café", "coffee shop", "bistro", "restaurant",
        "parking", "easy", "valet", "limited", "street", "garage", "distance", "accessible",
        "artwork", "decor", "candle", "plants", "flowers", "centerpiece", "stage", "dance", "karaoke", "projector", "tv", "screens",
        "event", "occasion", "birthday", "date night", "celebration", "party", "gathering", "meeting", "business dinner",
        "environment", "sustainable", "eco-friendly", "green", "calm", "peaceful", "lively", "buzzing", "busy"
    ],
    "price": [
        "price", "cost", "value", "worth", "bill", "charge", "fee", "total", "check",
        "expensive", "pricey", "overpriced", "premium", "high-end", "luxury", "upscale",
        "cheap", "low-cost", "affordable", "budget-friendly", "reasonable", "fair", "economical", "discounted",
        "deal", "discount", "promotion", "offer", "special", "happy hour", "combo",
        "portion", "size", "quantity", "shareable", "filling", "generous", "small", "large",
        "worthwhile", "worth-it", "overpriced", "underpriced", "value-for-money", "bang-for-buck",
        "cash", "card", "credit", "debit", "payment", "tip", "gratuity", "service charge", "included", "tax", "hidden fee",
        "menu", "pricing", "price range", "bill split", "check-in", "deposit"
    ]
}

MAPPINGS = {k: " ".join(v) for k, v in MAPPINGS.items()}
ASPECTS = list(MAPPINGS.keys())

for k, v in MAPPINGS.items():
    print(f"{k}: {v[:80]} ...")  # preview
    print()

# --------------------------------------------------------
# Helpers
# --------------------------------------------------------
def l2_normalize_csr(m):
    """Row-wise L2 normalize a CSR matrix, safe for zero rows."""
    # ||row||_2
    row_norm = np.sqrt(m.power(2).sum(axis=1)).A.ravel() + 1e-12
    inv = 1.0 / row_norm
    # scale rows
    return m.multiply(inv[:, None])

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

def process_row(row: Iterable[Row]) -> Iterable[Row]:
    """
    Process a partition of rows to map aspect seeds to predefined aspects.
    
    Args:
        rows (Iterable[Row]): An iterable of Spark Rows containing aspect seeds.
        
    Returns:
        Iterable[Row]: An iterable of Spark Rows with mapped aspects.
    """
    
    d = row.asDict()  

    if row.aspect is None or len(row.aspect) == 0:
        d['aspect'] = route_aspect(row['aspect_seed'], MAPPINGS, ASPECTS)
        return Row(**d)
    return Row(**d)

def route_aspect(aspect_seed: str, seeds: Dict[str, List[str]], targets: List[str]) -> Optional[str]:
    """
    Categorizes an aspect_seeds into predefined restaurant-related aspects. This function is used in function 'process_partition'
    
    Uses seed vocabularies to match aspects_seeds to aspects like 'food',
    'service', 'environment', or 'price'.
    
    Args:
        aspect (str): a review's aspects_seeds term mapped to aspects
        seeds (Dict): Dictionary of aspects and their seed words
        targets (List[str]): List of valid target aspects
        
    Returns:
        Optional[str]: aspect name if matched, None if no match
    """
    lem = lemmas(aspect_seed)
    for cat, vocab in seeds.items():
        if any(t in vocab for t in lem):
            return cat if cat in targets else None
    a = " ".join(lem)
    for cat, vocab in seeds.items():
        if any((" " in v and v in a) for v in vocab):
            return cat if cat in targets else None
    return None


# --------------------------------------------------------
# Start Spark Session
# --------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("AspectTermBucketing-CharGram")
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

# --------------------------------------------------------
# read in the data
# --------------------------------------------------------
input_path = "parquet/yelp_review_restaurant_with_aspect_seeds_extracted"  # sample from hayley_yelp_absa_extract_aspects_spaCy_v2.py output
print(f"\n========== input from {input_path}) ==============================")
df = spark.read.parquet(input_path)
df.printSchema()
print(f"Total rows in input DF: {df.count()}")

# --------------------------------------------------------
# Map the aspect seeds to aspect buckets 
# via char-gram TF-IDF + Cosine Similarity
# --------------------------------------------------------
print("\n========== Extract unique terms from df with aspect seeds ======================")
unique_terms = (
    df.select(lower(col("aspect_seed")).alias("term_lc"))
      .where(F.col("term_lc").isNotNull() & (F.length("term_lc") > 0))
      .distinct()
      .rdd
      .map(lambda r: r["term_lc"].strip())
      .filter(lambda s: len(s) > 0)
      .collect()
)
print(f"Unique aspect terms collected: {len(unique_terms)}")


print("\n========== Build char-gram TF-IDF vectorizer and score =========================")
prototype_texts = [MAPPINGS[a] for a in ASPECTS]

# Char-gram vectorizer: robust to typos/morphology
# analyzer="char_wb" ensures ngrams don't cross whitespace boundaries
vec = TfidfVectorizer(
    analyzer="char_wb",
    ngram_range=(3, 5),
    lowercase=True,
    sublinear_tf=True,
    min_df=1
)

# Fit on vocab derived from both unique terms and prototypes
vec.fit(unique_terms + prototype_texts)

# Transform prototypes and normalize
P = vec.transform(prototype_texts)  # shape [A, D]
P = l2_normalize_csr(P)

# Transform unique terms and normalize
U = vec.transform(unique_terms)     # shape [N, D]
U = l2_normalize_csr(U)

# Cosine similarity is dot product of L2-normalized rows
S = U @ P.T  # shape [N, A]; can be sparse COO/CSR depending on backend

# Ensure dense numpy arrays for argmax and max values
# argmax on sparse -> returns matrix; convert explicitly to ndarray
best_idx = np.array(S.argmax(axis=1)).ravel()

# Get max similarity per row as 1D array
if hasattr(S, "toarray"):
    best_sim = np.array(S.max(axis=1).toarray()).ravel()
else:
    best_sim = np.array(S.max(axis=1)).ravel()

THRESH_LEX = 0.04  # tune as needed

# assign label if similarity above threshold
assigned = [
    ASPECTS[int(i)] if float(s) >= THRESH_LEX else ""
    for i, s in zip(best_idx, best_sim)
]

# Create mapping DataFrame (driver → Spark)
df_panda_mapping = pd.DataFrame({
    "term_lc": unique_terms,
    "aspect": assigned,
    "sim_lex": best_sim
})
# (optional) keep top-k debugging columns
# mapping_pdf["best_label"] = [ASPECTS[i] for i in best_idx]

df_spark_mapping = spark.createDataFrame(df_panda_mapping)

print("\n========== Apply Mapping  ==============================================")
# --------------------------------------------------------
# Join mapping back to the original DF
# --------------------------------------------------------
df_term_bucket = (
    df.withColumn("term_lc", lower(col("aspect_seed")))
      .join(df_spark_mapping.select("term_lc", "aspect"), on="term_lc", how="left")
)

df_term_bucket = df_term_bucket.drop("term_lc")
df_term_bucket.show(1, truncate=False)

# use lexical matching to fill in any missing aspects
print("\n========== Fill in missing aspects via lexical matching ========================")
# Convert DataFrame to RDD
rdd_term_bucket = df_term_bucket.rdd

# Apply the processing function to each partition
transformed_rdd = rdd_term_bucket.map(process_row)
df_output = transformed_rdd.toDF()

# Show the output DataFrame
df_output.show(truncate=False)

# cast aspect_negated to boolean
df_output = df_output.withColumn("aspect_negated", F.col("aspect_negated").cast("boolean"))

# drop duplicates
df_output = df_output.dropDuplicates()
print(f"Total rows in output DF after duplicates are dropped: {df_output.count()}")


df_output.printSchema()


out_path = "parquet/yelp_review_restaurant_with_extracted_aspects"
print(f"\n========== Save to {out_path} ==========")
df_output.write.mode("overwrite").parquet(out_path)

spark.stop()
