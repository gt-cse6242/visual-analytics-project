# visual-analytics-project

## PySpark Setup on macOS (Python 3.10 + Java 11)

These are the steps and environment exports we used to get PySpark running on macOS with Python 3.10 and Java 11.  
Follow these instructions if you need to replicate the setup locally.

---

### 1. Install dependencies

- Install Java 11 (via Homebrew)

```
brew install openjdk@11
```

- symlink to JDK 11

```
sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Library/Java/JavaVirtualMachines/openjdk-11.jdk
```

### 2. Install PySpark, pandas, pyarrow for Python 3.10

```
python3.10 -m pip install "pyspark==3.5.*" pandas pyarrow
python3.10 -c "import pyspark; print(pyspark.__version__)"
```

### 3. Configure Environment Variables

Add the following to your `~/.zshrc` so they are automatically applied when you open a new terminal:

- Ensure Spark uses Java 11

```
export JAVA_HOME="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

- Force Spark to use Python 3.10 (driver and worker must match)

```
export PYSPARK_PYTHON=python3.10
export PYSPARK_DRIVER_PYTHON=python3.10
```

```
source ~/.zshrc
```

### 4. Check your settings

```
java -version        # should show openjdk 11.x
echo $JAVA_HOME
python3.10 -V        # should show Python 3.10.x
echo $PYSPARK_PYTHON $PYSPARK_DRIVER_PYTHON
```

### 5. Quick Smoke Test

Run test_spark.py

```
python3.10 test_spark.py
```

### 6. Expected:

```
Spark version: 3.5.6
+---+------+
| id| fruit|
+---+------+
|  1| apple|
|  2|banana|
+---+------+
```

---

## PySpark Setup on Windows (Python 3.10 + Java 11)

These are the steps and environment variable settings we used to get PySpark running on Windows 10/11 with Python 3.10 and Java 11.
Follow these instructions if you need to replicate the setup locally.

### 1. Install dependencies

- Install Java 11 (OpenJDK)  
  Download from [Adoptium Temurin JDK 11](https://adoptium.net/temurin/releases/?version=11) and install.
  During installation, check the box “Set JAVA_HOME variable” if available.

- Install Python 3.10
  Download from Python.org or use the Windows Store.
  Make sure to check “Add Python to PATH” during installation.

### 2. Install PySpark, pandas, pyarrow for Python 3.10

- Open Command Prompt (cmd) or PowerShell and run:

```
python -m pip install "pyspark==3.5.*" pandas pyarrow
python -c "import pyspark; print(pyspark.__version__)"
```

### 3. Configure Environment Variables

- You need to set environment variables so Spark knows which Java and Python to use.
  Open Start Menu → Edit the system environment variables → Environment Variables…
  Under System variables, add or edit:
- Ensure Spark uses Java 11:

```
JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-11.x.x
PATH = %JAVA_HOME%\bin;%PATH%
```

- Force Spark to use Python 3.10 (driver and worker must match):

```
PYSPARK_PYTHON = python
PYSPARK_DRIVER_PYTHON = python
```

### 4. Check your settings

- Run these in a new terminal:

```
java -version         # should show openjdk 11.x
echo %JAVA_HOME%      # should print your Java 11 install path
python -V             # should show Python 3.10.x
echo %PYSPARK_PYTHON% %PYSPARK_DRIVER_PYTHON%
```

### 5. Quick Smoke Test

- Create test_spark.py with the following:

```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.createDataFrame([(1, "apple"), (2, "banana")], ["id", "fruit"])
df.show()
spark.stop()
```

run:

```
python test_spark.py
```

### 6. Expected:

```
Spark version: 3.5.6
+---+------+
| id| fruit|
+---+------+
|  1| apple|
|  2|banana|
+---+------+
```

---

### 7. Import the yelp dataset

Download and import the dataset from https://business.yelp.com/data/resources/open-dataset/ into folder. Run yelp_business.py to import the data.

```
yelp_dataset/
```

---

## Project structure and pipeline (added)

We organized the processing scripts into two folders to keep ingestion and enrichment separate:

- jobs/
  - yelp_review.py — Convert reviews JSON to Parquet (partitioned by review_year)
  - yelp_user.py — Convert users JSON to Parquet (partitioned by yelping_year)
  - yelp_business.py — Simple inspector for business JSON (schema/sample)
- enriched/
  - join_reviews_with_business.py — Left-join business attributes onto reviews and write enriched Parquet
  - read_joined_reviews.py — Reader/inspector for the enriched Parquet
  - make_review_wordcloud.py — Build a word cloud PNG from the review text
- run_all.py — Orchestrates the end-to-end pipeline (convert → join → sample)

Parquet outputs (created by the scripts):

- parquet/yelp_review/
- parquet/yelp_review_enriched/

## Usage (quickstart)

From the repo root:

- Convert reviews JSON → Parquet
  - python jobs/yelp_review.py
- Join business onto reviews to produce enriched Parquet
  - python enriched/join_reviews_with_business.py
- Inspect enriched output (schema, counts, sample/aggregation)
  - python enriched/read_joined_reviews.py --years 2022 --sample 10
- Create a review text wordcloud image (requires wordcloud + matplotlib)
  - python -m pip install wordcloud matplotlib
  - python enriched/make_review_wordcloud.py --years 2022 --top 200 --output out/wordcloud_2022.png
- Run all steps in one go
  - python run_all.py

Notes:

- You can pass input/output options to the job scripts via CLI flags (see each script's help).
- The reader supports selecting years: --years 2018 2019, and sample size: --sample 20.

## Recent changes (behavior)

- Path resolution in enriched/read_joined_reviews.py
  - The reader now resolves paths relative to the project root when needed, so it works whether you run it from the repo root or from the enriched/ folder.
- Sample output display tweaks in enriched/read_joined_reviews.py
  - Text column is truncated to 25 characters in the printed sample (display only).
  - ID columns review_id, user_id, business_id are truncated to 10 characters in the printed sample (display only).
  - These truncations do not modify the underlying data or affect aggregations.

## Environment recap

- Python 3.10, Java 11, PySpark 3.5.x
- Ensure JAVA_HOME points to JDK 11 and both PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON point to Python 3.10 (see setup above).

## CLI flags and examples

Below are the most common flags for each script with copyable examples.

jobs/yelp_review.py

- Flags: --input <reviews_json> --output <parquet_dir>
- Examples:
  - python jobs/yelp_review.py
  - python jobs/yelp_review.py --input yelp_dataset/yelp_academic_dataset_review.json --output parquet/yelp_review

enriched/join_reviews_with_business.py

- Flags: --reviews <reviews_parquet_root> --business-json <business_json> --output <enriched_parquet_dir>
- Examples:
  - python enriched/join_reviews_with_business.py
  - python enriched/join_reviews_with_business.py --reviews parquet/yelp_review --business-json yelp_dataset/yelp_academic_dataset_business.json --output parquet/yelp_review_enriched

enriched/read_joined_reviews.py

- Flags: --parquet-dir <enriched_parquet_root> --years <Y1> <Y2> ... --sample <N>
- Examples:
  - python enriched/read_joined_reviews.py
  - python enriched/read_joined_reviews.py --years 2019 2020 2021 --sample 15
  - python enriched/read_joined_reviews.py --parquet-dir parquet/yelp_review_enriched --years 2022 --sample 10

run_all.py (orchestrator)

- Flags: --skip-review --skip-join --years <Y1> <Y2> ... --sample <N>
- Examples:
  - python run_all.py
  - python run_all.py --skip-review
  - python run_all.py --skip-review --skip-join --years 2022 --sample 20

Notes:

- For --years, provide one or more years separated by spaces (e.g., --years 2018 2019 2020).
- The reader resolves relative paths from either the repo root or the current folder, so both repo-root and enriched/ invocations work.
- Wordcloud script counts tokens in Spark, then renders locally using wordcloud/matplotlib.

---

## Classmates quickstart (simple setup)

Option A: Minimal, no virtualenv (fastest to try)

- Install Java 11 (if not installed) and ensure JAVA_HOME is set (see “PySpark Setup on macOS/Windows” above).
- Install Python packages globally/user-level:
  - python -m pip install -r requirements.txt
- Run the pipeline and samples from the repo root:
  - python jobs/yelp_review.py
  - python enriched/join_reviews_with_business.py
  - python enriched/read_joined_reviews.py --years 2022 --sample 10
  - python enriched/make_review_wordcloud.py --years 2022 --top 200 --output out/wordcloud_2022.png

Option B: Use a virtualenv (more isolated)

- python3 -m venv .venv
- source .venv/bin/activate # macOS/Linux
- python -m pip install -r requirements.txt
- Run the same commands as above.

Notes

- If Spark complains about Java, double-check JAVA_HOME and java -version (should be 11.x).
- If imports fail, confirm you installed to the same Python you’re using: python -m pip show pyspark wordcloud matplotlib.
