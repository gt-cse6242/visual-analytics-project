## Yelp Visual Analytics

Turn the Yelp Open Dataset into analysis-ready Parquet, join business attributes, preview samples, and optionally run aspect-based sentiment analysis (ABSA) and a wordcloud.

---

## 📋 Prerequisites

- **Java 11 (JDK 11)** - Required for PySpark
- **Python 3.10+** - For running the scripts
- **Yelp Open Dataset** - Place these files in `yelp_dataset/`:
  - `yelp_academic_dataset_review.json` (required)
  - `yelp_academic_dataset_business.json` (required)

---

## 🚀 Quick Start

### For Mac/Linux Users

1. **Install Prerequisites**

   ```bash
   # Install Java 11 (if not already installed)
   # Mac:
   brew install openjdk@11

   # Linux (Ubuntu/Debian):
   sudo apt-get install openjdk-11-jdk
   ```

2. **Setup Python Environment**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python -m spacy download en_core_web_sm
   ```

3. **Run the Pipeline**
   ```bash
   python run_all.py
   ```

### For Windows Users (WSL Required)

**⚠️ Important:** PySpark does not work reliably on native Windows, especially with usernames containing special characters (spaces, apostrophes, etc.). **Use WSL (Windows Subsystem for Linux)** instead.

#### First Time: Install WSL

If you don't have WSL installed:

1. **Install WSL** (PowerShell as Administrator):

   ```powershell
   wsl --install
   ```

   This installs WSL2 with Ubuntu by default.

2. **Restart your computer** when prompted.

3. **Set up Ubuntu**: Launch "Ubuntu" from Start menu, create a username and password.

#### Setup the Project in WSL

1. **Navigate to project** (PowerShell in project directory):

   ```powershell
   cd path\to\visual-analytics-project
   ```

2. **Run setup script** (one-time setup):

   ```powershell
   wsl bash setup_wsl.sh
   ```

   - Enter your WSL/Ubuntu password when prompted
   - Installs Java 11, Python packages, and spacy model
   - Takes 5-10 minutes

3. **Run the pipeline**:

   ```powershell
   run_wsl.bat
   ```

   Or alternatively:

   ```powershell
   wsl bash run_wsl.sh
   ```

**That's it!** The batch file automatically handles paths and environment activation.

---

## 📦 What `run_all.py` Does

The pipeline runs 5 steps automatically:

## 📦 What `run_all.py` Does

The pipeline runs 5 steps automatically:

1. **Convert reviews JSON → Parquet** (`parquet/yelp_review/`)
2. **Join business attributes → enriched Parquet** (`parquet/yelp_review_enriched/`)
3. **Read and sample enriched data** (displays schema, counts, sample)
4. **Run ABSA on 1,000 restaurant reviews** → `parquet/absa_restaurant_parquet/` + CSV
5. **Generate a wordcloud PNG** → `out/review_wordcloud.png`

---

## 🔧 Optional: Run Individual Steps

### Mac/Linux

**Step 1 — Convert reviews JSON to Parquet**

```bash
python jobs/yelp_review.py
# or with custom paths
python jobs/yelp_review.py --input yelp_dataset/yelp_academic_dataset_review.json --output parquet/yelp_review
```

**Step 2 — Join business onto reviews**

```bash
python enriched/join_reviews_with_business.py
# or with custom paths
python enriched/join_reviews_with_business.py --reviews parquet/yelp_review --business-json yelp_dataset/yelp_academic_dataset_business.json --output parquet/yelp_review_enriched
```

**Step 3 — Inspect enriched Parquet**

```bash
python enriched/read_joined_reviews.py
# restrict to specific years
python enriched/read_joined_reviews.py --years 2021 2022 --sample 10
```

**Step 4 — Generate wordcloud**

```bash
python enriched/make_review_wordcloud.py --years 2022 --top 200 --output out/wordcloud_2022.png
```

### Windows (WSL)

Same commands, but prefix with WSL activation:

```powershell
wsl bash -c "source ~/.venv-visual-analytics/bin/activate && cd <project-path> && python jobs/yelp_review.py"
```

Or open WSL terminal and run commands directly after activating venv.

---

## 🎯 ABSA Configuration

The ABSA step (`extract_aspects.py`) is configured to:

- Read from `parquet/yelp_review_enriched`
- Filter to restaurants via `biz_categories` containing "restaurants"
- Limit to **1,000 reviews** by default (for reasonable runtime)
- Output to `parquet/absa_restaurant_parquet/`

To process more reviews, edit `data_size` in `extract_aspects.py`.

---

## 🐛 Troubleshooting

**Windows: PySpark fails with socket/path errors**

- Your Windows username likely contains special characters (apostrophes, spaces)
- **Solution:** Use WSL (see setup above) - this is the only reliable way
- Native Windows PySpark requires usernames without special characters

**JAVA_GATEWAY_EXITED**

- Install Java 11: `java -version` should show version 11.x
- Mac: `brew install openjdk@11`
- Linux: `sudo apt-get install openjdk-11-jdk-headless`
- Windows (WSL): Run `setup_wsl.sh` which installs Java automatically

**ModuleNotFoundError: pyspark or spacy**

- Activate virtual environment first
- Mac/Linux: `source .venv/bin/activate`
- Windows (WSL): Virtual environment is activated automatically by `run_wsl.bat`
- Then: `pip install -r requirements.txt && python -m spacy download en_core_web_sm`

**No module named 'en_core_web_sm'**

- Run: `python -m spacy download en_core_web_sm` (with venv active)

**Parquet path not found**

- Run earlier steps first or use `python run_all.py` to generate all outputs

**Memory issues**

- Keep ABSA sample at 1,000 reviews
- Or adjust Spark memory configs in individual scripts if increasing sample size

**WSL: "wsl command not found" (Windows)**

- Install WSL first: `wsl --install` in PowerShell as Administrator
- Restart computer after installation

---

## 📁 Project Structure

- **`run_all.py`** — Main pipeline orchestrator (convert → join → sample → ABSA → wordcloud)
- **`run_wsl.sh`** / **`run_wsl.bat`** — Windows WSL helper scripts
- **`setup_wsl.sh`** — One-time WSL environment setup (Java, Python, packages)
- **`jobs/yelp_review.py`** — Convert reviews JSON → Parquet (partitioned by year)
- **`enriched/join_reviews_with_business.py`** — Join business attributes onto reviews
- **`enriched/read_joined_reviews.py`** — Inspect enriched Parquet (schema/counts/sample)
- **`enriched/make_review_wordcloud.py`** — Generate wordcloud from review text
- **`extract_aspects.py`** — Aspect-based sentiment extraction (restaurant-focused)
- **`test_spark.py`** — Quick Spark smoke test

---

## 📚 Requirements

Main dependencies (see `requirements.txt` for versions):

- **PySpark 3.5.x** — Distributed data processing
- **pandas** — Data manipulation
- **pyarrow** — Parquet file support
- **spaCy** — NLP (+ `en_core_web_sm` model)
- **wordcloud** — Word cloud generation
- **matplotlib** — Visualization

---

## ✅ Quick Spark Smoke Test

Verify PySpark is working:

**Mac/Linux:**

```bash
python test_spark.py
```

**Windows (WSL):**

```powershell
wsl bash -c "source ~/.venv-visual-analytics/bin/activate && cd <project-path> && python test_spark.py"
```

Expected output:

```
Spark version: 3.5.7
+---+------+
| id| fruit|
+---+------+
|  1| apple|
|  2|banana|
+---+------+
```

---

## 💡 Tips

- **First run takes longer**: Parquet conversion and ABSA are compute-intensive
- **Subsequent runs are faster**: Parquet files are cached
- **Adjust ABSA sample size**: Edit `data_size` in `extract_aspects.py` for more/fewer reviews
- **Windows users**: Always use WSL - it's faster and more reliable than native Windows
- **Check output folders**: `parquet/` contains all data, `out/` contains visualizations
