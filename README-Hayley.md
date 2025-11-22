### 1. Follow instruction in README.md to set up the envrionment

### 2. Import and Clean the Data Set
```
python run_all.py
```

Imported and cleaned dataset will be stored in 
```
parquet/yelp_review_enriched
```


### 4. Run pipeline End to End with full datasets

Extract only Restaurant data from parquet/yelp_review_enriched
```
python 1-extract_restaurant_data.py
```

Extract the aspect seeds from the each review.
```
python 2-aspects_extract_spaCy.py
```

Map extracted aspect seeds to aspect categories using TF-IDF and cosine similarity
```
python 3-aspect_mapping_tfid.py
```

Score the sentiment using extracted aspect opinions
```
python 4-aspect_scoring.py
```

Compute the reviewer credibility
```
python 5-compute_reviewer_weights
```

Filter the review based on reviewer credibility
```
python 6-aspect_scoring_weighted.py
```

The final dataset is stored in the following two format:
6-aspect_scoring_weighted.csv 
parquet/yelp_review_restaurant_restaurant_level_scoring_with_user_credibility"


### Other scripts and file:
Used to read in parquet files and exam the data
```
python read_in_data.py
```

Used to compare TF-IDF mapping with baseline mapping.
```
python 3.1-aspect_mapping_tfid_evaluate
```

Contains the evaluation resutls (manual labeling and excel sheet calculation)
```
3.1-aspect_mapping_tfid_evaluate_manually_labeled.csv
``` 