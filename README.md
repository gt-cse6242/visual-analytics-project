# visual-analytics-project

## PySpark Setup on macOS (Python 3.10 + Java 11)

These are the steps and environment exports we used to get PySpark running on macOS with Python 3.10 and Java 11.  
Follow these instructions if you need to replicate the setup locally.

---

### 1. Install dependencies

* Install Java 11 (via Homebrew)
brew install openjdk@11

* symlink to JDK 11
sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Library/Java/JavaVirtualMachines/openjdk-11.jdk

### 2. Install PySpark, pandas, pyarrow for Python 3.10
python3.10 -m pip install "pyspark==3.5.*" pandas pyarrow
python3.10 -c "import pyspark; print(pyspark.__version__)"

### 3. Configure Environment Variables

Add the following to your `~/.zshrc` so they are automatically applied when you open a new terminal:

* Ensure Spark uses Java 11
export JAVA_HOME="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

* Force Spark to use Python 3.10 (driver and worker must match)
export PYSPARK_PYTHON=python3.10
export PYSPARK_DRIVER_PYTHON=python3.10

source ~/.zshrc

### 4. Check your settings
java -version        # should show openjdk 11.x
echo $JAVA_HOME
python3.10 -V        # should show Python 3.10.x
echo $PYSPARK_PYTHON $PYSPARK_DRIVER_PYTHON

### 5. Quick Smoke Test
Run test_spark.py -> python3.10 test_spark.py

### 6. Expected:
Spark version: 3.5.6
+---+------+

| id| fruit|

+---+------+

|  1| apple|

|  2|banana|

+---+------+



