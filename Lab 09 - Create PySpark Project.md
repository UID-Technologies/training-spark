# **Lab 09 - Architecting a Complete PySpark Data Engineering Project**

---

##  Description

In this hands-on lab, you will build a **complete PySpark project from scratch** using industry best practices.

This lab focuses on creating a **well-structured, maintainable data processing pipeline** that demonstrates professional development patterns commonly used in real-world data engineering projects.

You’ll:

* Work with multiple data formats (**CSV** and **JSON**)
* Implement **data transformation logic** in reusable modules
* Use **Spark SQL** for complex queries
* Establish a **robust testing framework**
* Manage configurations with **YAML**
* Document your project properly

---

##  Objectives

By the end of this lab, you will be able to:

1. **Project Structure** → Design and implement a professional PySpark project structure with separation of concerns
2. **Data Processing** → Load, clean, and transform data from multiple sources (CSV + JSON)
3. **Modular Development** → Create reusable transformation functions in separate modules
4. **Spark SQL Integration** → Implement complex data queries using Spark SQL
5. **Data Output** → Export processed data in optimized formats (Parquet)
6. **Testing Framework** → Develop unit tests with `pytest`
7. **Configuration Management** → Use `YAML` for config-driven development
8. **Documentation** → Provide clear README and setup instructions

---

##  Steps Overview

You will create a small PySpark project (Windows-friendly) using **Jupyter Notebooks** that:

* Loads **CSV** + **JSON** data
* Cleans + transforms data via reusable functions
* Runs **Spark SQL**
* Outputs results as **Parquet**
* Has **unit tests with pytest**

---

## **Step 1: Project structure**

Extract the scaffold (or create manually):

```
pyspark-notebook-lab/
├─ notebooks/              # Jupyter notebooks
│   └─ 01_pyspark_lab.ipynb
│
├─ src/                    # Source code
│   ├─ __init__.py
│   ├─ transformations.py  # Transform logic
│   └─ utils.py            # Small helpers (optional)
│
├─ data/                   # Data storage
│   ├─ raw/                # Input data
│   │   ├─ customers.csv
│   │   └─ orders.jsonl
│   └─ processed/          # Output data
│
├─ conf/                   # Configuration
│   └─ config.yaml
│
├─ tests/                  # Tests
│   └─ test_transformations.py
│
├─ requirements.txt        # Python deps
└─ README.md               # Project guide

```

---

## **Step 2: File contents**
Create a required files and save it approprate folder

### `requirements.txt`

```text
pyspark==3.5.1
jupyterlab
ipykernel
pandas
pyarrow
pyyaml
pytest
python-dotenv
```

---

### `data/raw/customers.csv`

```csv
customer_id,first_name,last_name,email,country
1,Varun,Gupta,varun@example.com,India
2,Sara,Lee,sara@example.com,Finland
3,Rahul,Kumar,rahul@example.com,India
4,Anna,Koivu,anna@example.com,Finland
```

---

### `data/raw/orders.jsonl`

```json
{"order_id": 101, "customer_id": 1, "amount": 120.5, "status": "paid", "order_date": "2025-09-01"}
{"order_id": 102, "customer_id": 1, "amount": 299.0, "status": "paid", "order_date": "2025-09-10"}
{"order_id": 103, "customer_id": 2, "amount": 75.0, "status": "refunded", "order_date": "2025-09-12"}
{"order_id": 104, "customer_id": 3, "amount": 199.99, "status": "paid", "order_date": "2025-09-15"}
```

---

## **Step 3: Create a Config file**

### `conf/config.yaml`

```yaml
project:
  name: PySpark Notebook Lab
  owner: "ABC Company"

io:
  customers_csv: "data/raw/customers.csv"
  orders_jsonl: "data/raw/orders.jsonl"
  output_parquet: "data/processed/customer_sales"

spark:
  master: "local[*]"
  app_name: "pyspark-notebook-lab"
  configs:
    spark.sql.execution.arrow.pyspark.enabled: "true"
```

---

## **Step 4: Create Transformation Python Script**

###  `src/transformations.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_customers(df: DataFrame) -> DataFrame:
    """Trim names and lowercase emails."""
    return (
        df.withColumn("first_name", F.trim("first_name"))
          .withColumn("last_name", F.trim("last_name"))
          .withColumn("email", F.lower(F.col("email")))
    )

def join_customers_orders(customers: DataFrame, orders: DataFrame) -> DataFrame:
    return customers.join(orders, on="customer_id", how="left")

def compute_customer_sales(joined: DataFrame) -> DataFrame:
    return (
        joined.filter(F.col("status") == F.lit("paid"))
              .groupBy("customer_id", "first_name", "last_name", "country")
              .agg(F.round(F.sum("amount"), 2).alias("total_amount"),
                   F.countDistinct("order_id").alias("paid_orders"))
              .orderBy(F.col("total_amount").desc_nulls_last())
    )
```

---

## **Step 5: Create Python file for Testing**

###  `tests/test_transformations.py`

```python
import pytest
from pyspark.sql import SparkSession, Row
from src.transformations import clean_customers, join_customers_orders, compute_customer_sales

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .appName("pyspark-tests")
             .master("local[*]")
             .getOrCreate())
    yield spark
    spark.stop()

def test_clean_customers(spark):
    df = spark.createDataFrame([Row(first_name=" Varun ", last_name=" Gupta ", email="VARUN@EXAMPLE.COM")])
    out = clean_customers(df).first()
    assert out.first_name == "Varun"
    assert out.last_name == "Gupta"
    assert out.email == "varun@example.com"

def test_compute_customer_sales(spark):
    customers = spark.createDataFrame([Row(customer_id=1, first_name="Varun", last_name="Gupta", email="v@x", country="India")])
    orders = spark.createDataFrame([
        Row(order_id=101, customer_id=1, amount=100.0, status="paid", order_date="2025-09-01"),
        Row(order_id=102, customer_id=1, amount=200.0, status="refunded", order_date="2025-09-02"),
    ])
    joined = join_customers_orders(customers, orders)
    agg = compute_customer_sales(joined).first()
    assert agg.total_amount == 100.0
    assert agg.paid_orders == 1
```

---

## **Step 6: Create Python Notebook**

### `notebooks/01_pyspark_lab.ipynb`

Key cells (auto-created):

```markdown
# PySpark Notebook Lab

Steps:
1. Validate environment
2. Create SparkSession
3. Load CSV & JSON
4. Clean/Transform using `src/transformations.py`
5. Run Spark SQL
6. Write output to Parquet
```

```python
# Create SparkSession
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("pyspark-notebook-lab")
         .master("local[*]")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .getOrCreate())
```

```python
# Load data from config
import yaml
cfg = yaml.safe_load(open("conf/config.yaml"))
customers_df = spark.read.option("header", True).csv(cfg["io"]["customers_csv"])
orders_df    = spark.read.json(cfg["io"]["orders_jsonl"])
customers_df.show()
orders_df.show()
```

```python
# Use reusable transformations
from src.transformations import clean_customers, join_customers_orders, compute_customer_sales

cleaned = clean_customers(customers_df)
joined  = join_customers_orders(cleaned, orders_df)
result  = compute_customer_sales(joined)
result.show()
```

```python
# Save results
result.write.mode("overwrite").parquet(cfg["io"]["output_parquet"])
```

### or

```
from pathlib import Path
import os
import shutil
import sys

import pyspark
import yaml
from pyspark.sql import SparkSession

repo_root = Path.cwd()
if not (repo_root / "config" / "config.yaml").exists() and (repo_root.parent / "config" / "config.yaml").exists():
    repo_root = repo_root.parent

os.chdir(repo_root)
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

java_exe = shutil.which("java")
if not java_exe:
    raise RuntimeError("Java is not available on PATH for the notebook kernel.")

spark_home = Path(pyspark.__file__).resolve().parent
spark_submit = spark_home / "bin" / "spark-submit.cmd"
if not spark_submit.exists():
    raise RuntimeError(f"PySpark launcher not found at {spark_submit}")

os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME") or str(Path(java_exe).resolve().parent.parent)
os.environ["SPARK_HOME"] = str(spark_home)
os.environ.setdefault("PYSPARK_ALLOW_INSECURE_GATEWAY", "1")

cfg = yaml.safe_load((repo_root / "config" / "config.yaml").read_text(encoding="utf-8"))

builder = SparkSession.builder.appName(cfg["spark"]["app_name"]).master(cfg["spark"]["master"])
for key, value in cfg["spark"]["configs"].items():
    builder = builder.config(key, value)

spark = builder.getOrCreate()
spark
```

```
# Load data from config
from pathlib import Path

customers_path = repo_root / cfg["io"]["customers_csv"]
orders_path = repo_root / cfg["io"]["orders_jsonl"]
if not orders_path.exists() and orders_path.with_suffix(".json").exists():
    orders_path = orders_path.with_suffix(".json")

customers_df = spark.read.option("header", True).csv(str(customers_path))
orders_df = spark.read.json(str(orders_path))
customers_df.show()
orders_df.show()
```

```
# Use reusable transformations
import sys, importlib
for mod in list(sys.modules):
    if mod.startswith('src'):
        del sys.modules[mod]
importlib.invalidate_caches()

from src.transformations import clean_customers, join_customers_orders, compute_customer_sales

cleaned = clean_customers(customers_df)
joined  = join_customers_orders(cleaned, orders_df)
result  = compute_customer_sales(joined)
result.show()
```

```
# Save results
# On Windows, Spark's native Parquet writer requires winutils.exe (Hadoop).
# pyarrow is already installed, so we write via pandas/pyarrow to bypass that dependency.
import shutil

output_path = repo_root / cfg["io"]["output_parquet"]

# Clear any previous run
if output_path.exists():
    shutil.rmtree(output_path)
output_path.mkdir(parents=True, exist_ok=True)

# Convert to pandas and write a single Parquet file with pyarrow
result.toPandas().to_parquet(
    str(output_path / "part-00000.parquet"),
    index=False,
    engine="pyarrow",
)

print(f"Parquet written to: {output_path}")
```

---

## **Step 7: Create README**
### `README.md`

````markdown
# PySpark Notebook Lab (Windows)

## Setup
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
jupyter lab
````
---


## **Step 8: Run**

Open `notebooks/01_pyspark_lab.ipynb` and run cells.

## Tests

```powershell
pytest -q
```

## Notes

* Works with bundled PySpark (no separate Spark install needed).
* Output goes to `data/processed/customer_sales/`.



---

## **Step 9: Step-by-Step Execution**

1. **Open PowerShell** → `cd` into project folder  
2. **Create virtualenv** → `python -m venv .venv ; .\.venv\Scripts\Activate.ps1`  
3. **Install deps** → `pip install -r requirements.txt`  
4. **Launch Jupyter** → `jupyter lab`  
5. **Open Notebook** → `notebooks/01_pyspark_lab.ipynb`  
6. **Run cells in order** → from env check → Spark → transformations → output  
7. **Check output** → Parquet files inside `data/processed/customer_sales/`  
8. **(Optional) Run tests** → `pytest -q`  

---

