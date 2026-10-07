---
duration: 2h
category:
  - name: LAB
components:
  - name: SPARK
  - name: ICEBERG
  - name: S3
platforms:
  - name: LINUX
resources:
  - title: PySpark DataFrame API (official documentation)
    url: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
  - title: Spark SQL, DataFrames and Datasets Guide
    url: https://spark.apache.org/docs/latest/sql-programming-guide.html
  - title: Hadoop-AWS module, S3A configuration
    url: https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
  - title: Apache Iceberg, Spark configuration
    url: https://iceberg.apache.org/docs/latest/spark-configuration
  - title: Apache Iceberg, multi-engine support and runtime jars
    url: https://iceberg.apache.org/multi-engine-support/
revisions:
  - date: 2026-09-25
    comment: Initial page
    author: mori@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: Bronze to silver with Spark DataFrames

This lab is a Python script version of [lab-pyspark-silver](./lab-pyspark-silver.md), and is used only if the PySpark shell is not working correctly.

## Objectives

- Connect a PySpark `SparkSession` to the S3-compatible object storage of Onyxia
- Read the bronze CSV datasets (`users.csv`, `orders.csv`) directly from S3
- Build the same silver layer as in the dbt lab, with the **DataFrame API** and with **Spark SQL**
- Write the silver layer to S3 as Parquet files
- Write the silver layer to S3 as Iceberg tables, and inspect what an Iceberg table is made of
- Compare the two approaches, dbt and Spark

## Prerequisites

- The `vscode-pyspark` Onyxia service and the project of the [uv lab](../03.object-storage/lab-1-uv.md)
- The `bronze/users.csv` and `bronze/orders.csv` objects uploaded at the end of the [S3 lab](../03.object-storage/lab-2-s3.md)
- The [dbt lab](./lab-dbt.md), which builds the same silver layer in SQL. Its output in `silver/` is used for the comparison at the end of this lab. If you removed it, run `uv run dbt build` again from the `lab_medallion` directory.
- A network access to Maven Central: the Iceberg library is downloaded when the Spark session starts

## Environment

The lab is run with Python scripts in VS Code. Each `python` block of this page is a piece of script that needs to be added or updated in the existed Python script. Each `bash` block is a command to run in a terminal. Please follow the instructions in every section.

Move into the project and set the environment variables used in the previous labs.

```bash
# Define the name of your repo/directory accordingly
GIT_REPO_NAME=<git-repo-name>
# Environment setup
cd /home/onyxia/work/$GIT_REPO_NAME
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
echo "$LAB_BUCKET_NAME"
#> user-gollum
```

Check that the datasets are present in the bronze layer:

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
#> 2026-09-14 11:02:10     331568 orders.csv
#> 2026-09-14 11:02:09       7351 users.csv
```

If they are missing, generate and upload them again:

```bash
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3 --profile 'default' cp orders.csv "s3://$LAB_BUCKET_NAME/bronze/orders.csv"
```

Then, in VS Code, use the kernel picker in the top-right corner of the notebook and select **Python (lab-pyspark uv)**. The versions of PySpark and of the Spark/Hadoop libraries already available in the `vscode-pyspark` image (`$SPARK_HOME/jars`) should be compatible. Check them, and note the Spark and Scala versions:

```bash
uv run pyspark --version
ls $SPARK_HOME/jars | grep -i hadoop-aws
```

If `hadoop-aws` is missing, the S3A configuration below fails with a `ClassNotFoundException`. In that case, add the matching jar with `spark.jars.packages` when building the session.

Iceberg is a library added to Spark. Its runtime jar is named after the Spark and Scala versions, `iceberg-spark-runtime-<spark version>_<scala version>`, for example `iceberg-spark-runtime-4.0_2.13` for Spark 4.0 with Scala 2.13. Pick the artifact matching your versions and a release of Iceberg which supports them, from the [Iceberg documentation](https://iceberg.apache.org/multi-engine-support/).

## SparkSession with S3 access

Onyxia issues temporary STS credentials as environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) and the endpoint of the object storage (`AWS_S3_ENDPOINT`). The DuckDB lab relied on the `s3_onyxia_connection` secret being picked up automatically. Spark's Hadoop S3A connector needs the same information configured explicitly, including the `TemporaryAWSCredentialsProvider`, which is required because the default provider ignores the session token.

The session also declares an Iceberg catalog named `lab`, used at the end of the lab. It must be configured now: the libraries of a session are loaded when it starts. The catalog is of type `hadoop`, a directory in the bucket, which needs no server.

```python
# Illustration of a Spark session configuration
spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
```

The session runs in `local[*]` mode: the driver and the executors share a single JVM, on the machine of the Onyxia service. The code of this lab does not change on a cluster, only the way the session is created.

Questions:

- The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?
- Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?

## Bronze layer

Read the two CSV files directly from S3 with the DataFrame reader. `inferSchema=True` triggers an extra pass over the file to detect column types, which is acceptable given the small size of the datasets.

```bash
cat << 'PY' > lab-spark-bronze.py
import os
from pyspark.sql import SparkSession

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]

# Adapt to the Spark and Scala versions of your environment
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True, multiLine=True
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True, multiLine=True
)

users_bronze.printSchema()
users_bronze.show(3, truncate=False)
PY
```

The `address` column spans 2 lines: the street, then the city, the state and the zip code. Some addresses, such as `DPO AP 09617`, are military addresses without a city, the same subtlety handled in the dbt lab.

```python
# Update the PySpark script to show 3 rows with only the address column.
users_bronze.select("address").show(3, truncate=False)
```

## Silver layer

The transformations are the ones of the dbt lab: `stg_users` and `stg_orders`, with the same columns and the same rules. Only the engine changes.

### Users, DataFrame API

The columns are renamed with consistent conventions, cast to their types, and the address is split and parsed.

```bash
cat << 'PY' > lab-spark-silver.py
import os

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]

# Adapt to the Spark and Scala versions of your environment
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True, multiLine=True
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True, multiLine=True
)

users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)

stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)

# Unlike SQL's nullif(), regexp_extract() returns an empty string, not NULL, when there
# is no match. Left as an empty string, "city" would silently count as a group in a
# later groupBy() instead of being excluded like a NULL.
stg_users = stg_users.withColumn(
    "city", F.when(F.col("city") == "", None).otherwise(F.col("city"))
)

stg_users.show(5, truncate=False)
PY
```

### Orders, DataFrame API with window deduplication

The generator does not produce duplicates, but ingestion processes do. Deduplication with a window function makes the model robust to a replay of the bronze layer, exactly like the `qualify row_number() ...` clause of the dbt lab.

```python
# Update the lib in lab-spark-silver.py
from pyspark.sql.window import Window
```

```python
# Update the following script in lab-spark-silver.py
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)

dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())

stg_orders = (
    orders_typed
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

stg_orders.show(5)
print(f"orders_typed: {orders_typed.count()} rows, stg_orders: {stg_orders.count()} rows")
```

`partitionBy()` in a window function is a **wide** transformation: rows are shuffled across executors so that every row of a given `order_id` lands on the same partition before `row_number()` can be computed.

### The same transformation with Spark SQL

Register the DataFrame as a temporary view and express the deduplication as a SQL query instead. Both approaches compile to the same Catalyst plan.

```python
# Update the following script in lab-spark-silver.py, you may comment/remove the previous .show() or print() function
# to avoid complicated output
orders_typed.createOrReplaceTempView("orders_typed")

stg_orders_sql = spark.sql('''
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *,
               row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
''')

stg_orders_sql.show(5)
```

```python
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("-----------------------------------------")
print("DataFrame API and Spark SQL results match")
print("-----------------------------------------")
```

A quick analytical query on the silver users, purely in SQL, to check the address parsing:

```python
stg_users.createOrReplaceTempView("stg_users")

spark.sql('''
    SELECT state,
           count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users
    GROUP BY state
    ORDER BY users DESC
''').show(10)
```

Questions:

- Compare the DataFrame and SQL cells above. Which one would you choose for a one-off exploration, and which for a model reused by several colleagues? Why?
- `regexp_extract()` returned an empty string instead of `NULL` on no match. Where else in this notebook could that same gotcha silently produce a wrong result if left unhandled?

## Write the silver layer as Parquet

Like the `external` models of the dbt lab, Spark writes the DataFrame content to Parquet files in the bucket. Each output is a directory of part-files, one per output partition, not a single object.

```python
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
```

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/silver/" --recursive
```

If you completed the dbt lab, the listing also shows `stg_users.parquet` and `stg_orders.parquet`, single objects.

Questions:

- List the objects written under `silver/users.parquet/`. How many part-files are there, and why?
- `coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?

Read the Parquet files back and check that no rows were lost or duplicated:

```python
users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")

assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("-----------------------------------------")
print("Row counts validated")
print("-----------------------------------------")
```

## Write the silver layer as Iceberg tables

A Parquet directory is only a set of files. **Iceberg** is a table format: it stores the data in Parquet files, and adds metadata files which describe the table, its schema and its snapshots. Writes are committed as a whole, so a reader never sees a half-written table.

Spark finds Iceberg tables through the `lab` catalog declared in the session. A table is named `<catalog>.<namespace>.<table>`. Create the `silver` namespace and write the same DataFrames:

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")

stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
```

List the objects in the bucket:

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/warehouse/" --recursive
```

Under `warehouse/silver/orders/`, the `data/` directory contains the Parquet files, and the `metadata/` directory contains a `v1.metadata.json` file, the manifests and the snapshot files, in Avro. The `version-hint.text` file tells a reader which version of the table is the current one.

Iceberg exposes its metadata as tables which can be queried with SQL:

```python
spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
""").show(truncate=False)

spark.sql("""
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lab.silver.orders.files
""").show(truncate=False)
```

Read the tables back through the catalog and validate them:

```python
users_iceberg = spark.table("lab.silver.users")
orders_iceberg = spark.table("lab.silver.orders")

assert users_iceberg.count() == users_bronze.count()
assert orders_iceberg.count() == stg_orders.count()
print("-----------------------------------------")
print("Iceberg row counts validated")
print("-----------------------------------------")
```

Questions:

- Compare `silver/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?
- A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?
- With this catalog, the pointer to the current version of a table is a file in the bucket. What can go wrong if two jobs commit at the same time, and what would a catalog service add?

## Compare with the dbt lab

Both labs build the same silver layer. Compare the results first.

Count the rows and sum the quantities in both outputs with the DuckDB CLI. The two lines of each query must show the same values:

```bash
duckdb -c "
  SELECT 'dbt' AS engine, count(*) AS orders, sum(quantity) AS quantity
  FROM 's3://$LAB_BUCKET_NAME/silver/dbt/stg_orders.parquet'
  UNION ALL
  SELECT 'spark', count(*), sum(quantity)
  FROM 's3://$LAB_BUCKET_NAME/silver/spark/orders.parquet/*.parquet'
"
duckdb -c "
  SELECT 'dbt' AS engine, count(*) AS users
  FROM 's3://$LAB_BUCKET_NAME/silver/dbt/stg_users.parquet'
  UNION ALL
  SELECT 'spark', count(*)
  FROM 's3://$LAB_BUCKET_NAME/silver/spark/users.parquet/*.parquet'
"
```

Compare the schemas, in particular the type of `order_id` and `ordered_at`:

```bash
duckdb -c "DESCRIBE SELECT * FROM 's3://$LAB_BUCKET_NAME/silver/stg_orders.parquet'"
duckdb -c "DESCRIBE SELECT * FROM 's3://$LAB_BUCKET_NAME/silver/orders.parquet/*.parquet'"
```

Questions:

- What differs between the two schemas? Spark has no `UUID` type: what did you do about it, and what is the consequence for the consumers of the silver layer?
- Compare the output layout: one object per model with dbt, one directory of part-files with Spark. Which one is easier for a consumer to read? Which one scales?
- The dbt lab validates the silver layer with the `unique`, `not_null`, `accepted_values` and `relationships` tests. What did this lab use instead, and what would you need to write to get the same guarantees?
- `ref()` and `source()` give dbt the dependency graph of the models. What plays this role in the notebook?
- Compare the time to build the silver layer with `dbt build` and with the cells of this lab. Why are these numbers not directly comparable?
- For each of these cases, would you choose dbt with DuckDB or Spark:
  - A daily batch of 10 GB?
  - A daily batch of 10 TB?
  - A team of analysts who only know SQL?

## Exercises

1. Add a `birthdate_is_valid` boolean column to `stg_users`, `False` when a user's birthdate is later than their first order date. This requires joining `stg_users` and `stg_orders`. Write it once with the DataFrame API and once with Spark SQL, and check that the two agree. Compare with the fix of the dbt lab.
2. Rewrite the `zip_code` extraction as a plain Python UDF, then as a `pandas_udf`. Time both with `%%timeit` on the full `users_bronze` DataFrame and compare against the `regexp_extract()` version above.
3. The bronze CSVs are read from S3 on every run of this notebook. Cache `users_bronze` and `orders_bronze` with `.cache()` after the first read, and check the **Spark UI** (`http://localhost:4040`) to see the effect on later stages.
4. Append one new order to `lab.silver.orders` with `writeTo(...).append()`. List the snapshots of the table, then read the table as it was before the append, with `VERSION AS OF` in Spark SQL. Do the same with the Parquet directory and explain the difference.

## Cleanup

Remove the objects of the silver layer and of the Iceberg warehouse. The objects of the bronze layer are kept for the
next modules.

```bash
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/silver/" --recursive
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/warehouse/" --recursive
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
#> 2026-09-14 11:02:10     331568 bronze/orders.csv
#> 2026-09-14 11:02:09       7351 bronze/users.csv
```

Stop the Spark session:

```python
spark.stop()
```

---

_The content of this document, including all text, images, and associated materials, is the exclusive property of
Adaltas and is protected by applicable copyright laws. Unauthorized distribution, reproduction, or sharing of this
content, in whole or in part, is strictly prohibited without the express written consent of the author(s). Any
violation of this restriction may result in legal action and the imposition of penalties as prescribed by law._
