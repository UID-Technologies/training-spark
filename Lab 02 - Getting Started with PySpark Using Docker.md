# Lab 02 - Getting Started with PySpark Using Docker

---

## Objective

By the end of this lab, you will be able to:

1. Run PySpark using Docker
2. Start a Spark container
3. Execute PySpark commands
4. Read CSV data using Spark
5. Perform basic transformations
6. Save processed output

---

## Prerequisites

Install these tools:

1. **Docker Desktop**
2. **VS Code**
3. Basic knowledge of:
   * Python
   * SQL
   * Command line

Verify Docker installation:

```bash
docker --version
```

---

## Step 1: Create Project Folder

```bash
mkdir pyspark-docker-lab
cd pyspark-docker-lab
```

Create folders:

```bash
mkdir data scripts output
```

Final structure:

```text
pyspark-docker-lab/
|
|-- data/
|-- scripts/
`-- output/
```

---

## Step 2: Create Sample CSV File

Create file:

```bash
data/employees.csv
```

Add this content:

```csv
id,name,department,salary,city
1,Amit,IT,70000,Bangalore
2,Neha,HR,50000,Delhi
3,Rahul,IT,85000,Pune
4,Sara,Finance,65000,Mumbai
5,John,HR,45000,Delhi
6,Priya,Finance,90000,Bangalore
```

---

## Step 3: Pull PySpark Docker Image

Use the official Spark image:

```bash
docker pull apache/spark:latest
```

Check image:

```bash
docker images
```

---

## Step 4: Run Spark Container

### For Windows PowerShell

```powershell
docker run -it --rm -v ${PWD}/data:/opt/spark/work-dir/data -v ${PWD}/scripts:/opt/spark/work-dir/scripts -v ${PWD}/output:/opt/spark/work-dir/output  apache/spark:latest /bin/bash
```

### For Linux/Mac

```bash
docker run -it --rm \
  -v $(pwd)/data:/opt/spark/work-dir/data \
  -v $(pwd)/scripts:/opt/spark/work-dir/scripts \
  -v $(pwd)/output:/opt/spark/work-dir/output \
  apache/spark:latest /bin/bash
```

You are now inside the Spark container.

---

## Step 5: Start PySpark Shell

Inside the container, run:

```bash
/opt/spark/bin

ls -l pyspark

chmod +x pyspark
```

```bash
./pyspark
```

You should see the PySpark shell.

---

## Step 6: Create Your First Spark DataFrame

Inside PySpark shell:

```python
data = [
    (1, "Amit", "IT", 70000),
    (2, "Neha", "HR", 50000),
    (3, "Rahul", "IT", 85000)
]

columns = ["id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)

df.show()
```

Expected output:

```text
+---+-----+----------+------+
| id| name|department|salary|
+---+-----+----------+------+
|  1| Amit|        IT| 70000|
|  2| Neha|        HR| 50000|
|  3|Rahul|        IT| 85000|
+---+-----+----------+------+
```

---

## Step 7: Check Schema

```python
df.printSchema()
```

Expected output:

```text
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- department: string (nullable = true)
 |-- salary: long (nullable = true)
```

---

## Step 8: Basic DataFrame Operations

### Select Columns

```python
df.select("name", "salary").show()
```

### Filter Data

```python
df.filter(df.salary > 60000).show()
```

### Add New Column

```python
from pyspark.sql.functions import col

df_bonus = df.withColumn("bonus", col("salary") * 0.10)

df_bonus.show()
```

### Group By

```python
df.groupBy("department").count().show()
```

---

## Step 9: Read CSV File from Docker Volume

Exit PySpark shell first:

```python
exit()
```

Start again if needed:

```bash
pyspark
```

Read CSV file:

```python
employees_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/opt/spark/work-dir/data/employees.csv")

employees_df.show()
```

---

## Step 10: Analyze CSV Data

### Show Schema

```python
employees_df.printSchema()
```

### Count Records

```python
employees_df.count()
```

### Filter IT Employees

```python
employees_df.filter(employees_df.department == "IT").show()
```

### Average Salary by Department

```python
employees_df.groupBy("department").avg("salary").show()
```

### Highest Salary

```python
employees_df.orderBy(employees_df.salary.desc()).show()
```

---

## Step 11: Save Output as CSV

```python
result_df = employees_df.groupBy("department").avg("salary")

result_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/opt/spark/work-dir/output/avg_salary_by_department")
```

Exit PySpark:

```python
exit()
```

Now check your local machine folder:

```text
pyspark-docker-lab/output/avg_salary_by_department/
```

You will see Spark output files.

---

## Step 12: Create a PySpark Script

Create file:

```bash
scripts/employee_analysis.py
```

Add this code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder \
    .appName("EmployeeAnalysis") \
    .getOrCreate()

employees_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/opt/spark/work-dir/data/employees.csv")

print("Original Data")
employees_df.show()

high_salary_df = employees_df.filter(col("salary") > 60000)

print("Employees with salary greater than 60000")
high_salary_df.show()

avg_salary_df = employees_df.groupBy("department") \
    .agg(avg("salary").alias("average_salary"))

print("Average salary by department")
avg_salary_df.show()

avg_salary_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/opt/spark/work-dir/output/avg_salary_report")

spark.stop()
```

---

## Step 13: Run PySpark Script in Docker

Start container again if needed:

```powershell
docker run -it --rm `
  -v ${PWD}/data:/opt/spark/work-dir/data `
  -v ${PWD}/scripts:/opt/spark/work-dir/scripts `
  -v ${PWD}/output:/opt/spark/work-dir/output `
  apache/spark:latest /bin/bash
```

Run the script:

```bash
spark-submit /opt/spark/work-dir/scripts/employee_analysis.py
```

Check output folder:

```text
output/avg_salary_report/
```

---

## Step 14: Use Docker Compose

Create file:

```bash
docker-compose.yml
```

Add this:

```yaml
services:
  spark:
    image: apache/spark:latest
    container_name: pyspark-lab
    volumes:
      - ./data:/opt/spark/work-dir/data
      - ./scripts:/opt/spark/work-dir/scripts
      - ./output:/opt/spark/work-dir/output
    working_dir: /opt/spark/work-dir
    command: /bin/bash
    stdin_open: true
    tty: true
```

Run:

```bash
docker compose run --rm spark
```

Inside container:

```bash
spark-submit scripts/employee_analysis.py
```

---

## Step 15: Practice Exercises

### Exercise 1: Filter by City

Show employees from Delhi.

```python
employees_df.filter(col("city") == "Delhi").show()
```

### Exercise 2: Add Annual Bonus

Add a new column called `annual_bonus`.

```python
employees_df.withColumn("annual_bonus", col("salary") * 0.15).show()
```

### Exercise 3: Department Salary Summary

Generate:

1. Average salary
2. Maximum salary
3. Minimum salary

```python
from pyspark.sql.functions import avg, max, min

employees_df.groupBy("department") \
    .agg(
        avg("salary").alias("average_salary"),
        max("salary").alias("max_salary"),
        min("salary").alias("min_salary")
    ).show()
```

### Exercise 4: Save IT Employees

```python
it_df = employees_df.filter(col("department") == "IT")

it_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/opt/spark/work-dir/output/it_employees")
```

---

## Common Issues

### Issue 1: Docker command not recognized

Docker Desktop is not installed or not running.

### Issue 2: File not found

Check the mounted path:

```bash
ls /opt/spark/work-dir/data
```

### Issue 3: Permission issue on output folder

Delete old output folder and rerun:

```bash
rm -rf /opt/spark/work-dir/output/avg_salary_report
```

### Issue 4: CSV output has multiple files

This is normal in Spark. Spark writes distributed output as multiple part files.

Example:

```text
part-00000-xxxx.csv
_SUCCESS
```

---

## Final Lab Outcome

After completing this lab, you should understand:

1. How to run PySpark using Docker
2. How to use PySpark shell
3. How to create Spark DataFrames
4. How to read CSV files
5. How to filter, group, and transform data
6. How to run PySpark scripts using `spark-submit`
7. How to save Spark output files

---
