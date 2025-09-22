# visual-analytics-project

## PySpark Setup on macOS (Python 3.10 + Java 11)

These are the steps and environment exports we used to get PySpark running on macOS with Python 3.10 and Java 11.  
Follow these instructions if you need to replicate the setup locally.

---

### 1. Install dependencies

* Install Java 11 (via Homebrew)
```
brew install openjdk@11
```

* symlink to JDK 11
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

* Ensure Spark uses Java 11
```
export JAVA_HOME="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

* Force Spark to use Python 3.10 (driver and worker must match)
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

------------------------------------------------------------------------------------
## PySpark Setup on Windows (Python 3.10 + Java 11)

These are the steps and environment variable settings we used to get PySpark running on Windows 10/11 with Python 3.10 and Java 11.
Follow these instructions if you need to replicate the setup locally.

### 1. Install dependencies

* Install Java 11 (OpenJDK)  
  Download from [Adoptium Temurin JDK 11](https://adoptium.net/temurin/releases/?version=11) and install.
  During installation, check the box “Set JAVA_HOME variable” if available.

* Install Python 3.10
  Download from Python.org or use the Windows Store.
  Make sure to check “Add Python to PATH” during installation.

### 2. Install PySpark, pandas, pyarrow for Python 3.10
* Open Command Prompt (cmd) or PowerShell and run:
```
python -m pip install "pyspark==3.5.*" pandas pyarrow
python -c "import pyspark; print(pyspark.__version__)"
```

### 3. Configure Environment Variables
* You need to set environment variables so Spark knows which Java and Python to use.
  Open Start Menu → Edit the system environment variables → Environment Variables…
  Under System variables, add or edit:
* Ensure Spark uses Java 11:
```
JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-11.x.x
PATH = %JAVA_HOME%\bin;%PATH%
```
* Force Spark to use Python 3.10 (driver and worker must match):
```
PYSPARK_PYTHON = python
PYSPARK_DRIVER_PYTHON = python
```

### 4. Check your settings
 * Run these in a new terminal:
```
java -version         # should show openjdk 11.x
echo %JAVA_HOME%      # should print your Java 11 install path
python -V             # should show Python 3.10.x
echo %PYSPARK_PYTHON% %PYSPARK_DRIVER_PYTHON%
```

### 5. Quick Smoke Test
  * Create test_spark.py with the following:
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

------------------------------------------------------------------------------------

### 7. Import the yelp dataset
Download and import the dataset from https://business.yelp.com/data/resources/open-dataset/ into folder. Run yelp_business.py to import the data. 
```
yelp_dataset/ 
```