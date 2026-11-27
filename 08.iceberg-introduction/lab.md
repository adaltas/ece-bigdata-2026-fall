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

# Lab: Explore Iceberg tables with Spark

## Objectives

- Understand how an Iceberg table is stored in object storage
- Create and query an Iceberg table with Spark
- Explore Iceberg metadata and snapshots
- Observe how Iceberg handles updates and schema evolution
- Use time travel to query previous table versions
- Compare an Iceberg table with a plain Parquet directory

## Prerequisites

- The `vscode-pyspark` Onyxia service with image: `inseefrlab/onyxia-vscode-pyspark:py3.13.15-spark4.1.1` instead of default image which is using Spark version 4.3.

![setup spark version 4.1.1](./assets/onyxia-vscode-spark-image.png)

- The `stg_users` and `stg_orders` DataFrames created in that lab
- A network connection to Maven Central

## Environment

The lab is run in a PySpark shell. Each `python` block of this page is a cell of the notebook. Each `bash` block is a command to run in a terminal.

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

Check that the Iceberg datasets are present in the bronze layer:

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/warehouse/" --recursive
#> 2026-09-25 10:18:08          0 warehouse/silver/
#> 2026-10-05 11:07:09     143651 warehouse/silver/orders/data/00000-24-951755ee-23d3-41f5-b06b-f197d20d5c12-0-00001.parquet
#> 2026-10-05 11:07:10       7343 warehouse/silver/orders/metadata/0e2a6bda-c591-4cae-96e6-496c54a894f2-m0.avro
#> 2026-10-05 11:07:10       4453 warehouse/silver/orders/metadata/snap-3142678218950516150-1-0e2a6bda-c591-4cae-96e6-496c54a894f2.avro
#> 2026-10-05 11:07:10       1913 warehouse/silver/orders/metadata/v1.metadata.json
#> ...
```

If they are missing, redo the [Spark DataFrame lab](./lab-pyspark-silver.md).

Then, in VS Code, use the kernel picker in the top-right corner of the notebook and select **Python (lab-pyspark uv)**. The versions of PySpark and of the Spark/Hadoop libraries already available in the `vscode-pyspark` image (`$SPARK_HOME/jars`) should be compatible. Check them, and note the Spark and Scala versions:

```bash
uv run pyspark --version
ls $SPARK_HOME/jars | grep -i hadoop-aws
```

If `hadoop-aws` is missing, the S3A configuration below fails with a `ClassNotFoundException`. In that case, add the matching jar with `spark.jars.packages` when building the session.

Iceberg is a library added to Spark. Its runtime jar is named after the Spark and Scala versions, `iceberg-spark-runtime-<spark version>_<scala version>`, for example `iceberg-spark-runtime-4.1_2.13` for Spark 4.1 with Scala 2.13. Pick the artifact matching your versions and a release of Iceberg which supports them, from the [Iceberg documentation](https://iceberg.apache.org/multi-engine-support/).

## SparkSession with S3 access

Onyxia issues temporary STS credentials as environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) and the endpoint of the object storage (`AWS_S3_ENDPOINT`). The DuckDB lab relied on the `s3_onyxia_connection` secret being picked up automatically. Spark's Hadoop S3A connector needs the same information configured explicitly, including the `TemporaryAWSCredentialsProvider`, which is required because the default provider ignores the session token.

The session also declares an Iceberg catalog named `lab`, used at the end of the lab. It must be configured now: the libraries of a session are loaded when it starts. The catalog is of type `hadoop`, a directory in the bucket, which needs no server.

Launch PySpark shell.

```bash
pyspark --master local[*] --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
```

The session runs in `local[*]` mode: the driver and the executors share a single JVM, on the machine of the Onyxia service. The code of this lab does not change on a cluster, only the way the session is created.

```python
import os
from pyspark.sql import SparkSession

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]

# Adapt to the Spark and Scala versions of your environment
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

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
spark
```

Verify the catalog:

```python
spark.sql("SHOW NAMESPACES IN lab").show()
```

## Create an Iceberg table

If an Iceberg table is not created from the [previous "## Write the silver layer as Iceberg tables" section](../07.sparksql-and-dataframes/lab-pyspark-silver.md), create a namespace and write the silver orders DataFrame as an Iceberg table:

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")

stg_orders.writeTo(
    "lab.silver.orders"
).createOrReplace()
```

Query the table through the catalog:

```python
spark.sql("""
    SELECT count(*) AS orders,
           sum(quantity) AS quantity
    FROM lab.silver.orders
""").show()
#> +------+--------+
#> |orders|quantity|
#> +------+--------+
#> |  5000|   14976|
#> +------+--------+
```

Unlike a Parquet directory, `lab.silver.orders` is a table known by the Iceberg catalog.

## Inspect the files

List the objects created in the warehouse:

```bash
aws s3 --profile default ls \
  "s3://$LAB_BUCKET_NAME/warehouse/silver/orders/" --recursive
```

Identify:

- the `data/` directory containing Parquet files
- the `metadata/` directory containing Iceberg metadata
- manifest files describing data files
- snapshot metadata describing table versions

Questions:

- Which files contain the actual rows?
- Which files describe the table?
- Why does an Iceberg table need more than the Parquet files?

## Explore Iceberg metadata

Iceberg exposes metadata through special metadata tables:

```python
spark.sql("""
    SELECT *
    FROM lab.silver.orders.snapshots
""").show(truncate=False)
```

Inspect the files belonging to the table:

```python
spark.sql("""
    SELECT file_path,
           file_format,
           record_count,
           file_size_in_bytes
    FROM lab.silver.orders.files
""").show(truncate=False)
```

Questions:

- How many snapshots does the table currently have?
- How many data files belong to the current table?
- What information does the `snapshots` table contain that the Parquet files do not?

## Create a second snapshot

Read the silver orders from the Iceberg table, so this section does not depend on the DataFrames of the previous lab:

```python
from pyspark.sql import functions as F

stg_orders = spark.table("lab.silver.orders")
stg_orders.show(5)
```

Create three new orders from existing rows, with new order IDs so the table keeps one row per order, and append them:

```python
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))

new_orders.writeTo(
    "lab.silver.orders"
).append()
```

Check the snapshots again:

```python
spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").show(truncate=False)
```

Check the number of rows:

```python
spark.sql("""
    SELECT count(*) FROM lab.silver.orders
""").show()
```

Questions:

- How many snapshots are there now?
- Did the original Parquet files disappear?
- Why does Iceberg create a new snapshot instead of simply replacing the table?

## Time travel

Retrieve the snapshot IDs:

```python
snapshots = spark.sql("""
    SELECT snapshot_id
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").collect()

snapshots
```

Use the first snapshot to query the table as it existed before the append:

```python
first_snapshot = snapshots[0]["snapshot_id"]

spark.sql(f"""
    SELECT count(*)
    FROM lab.silver.orders
    VERSION AS OF {first_snapshot}
""").show()
```

Compare it with the current table:

```python
spark.sql("""
    SELECT count(*)
    FROM lab.silver.orders
""").show()
```

Questions:

- What does time travel read?
- Why can Iceberg access an older version without making a complete copy of the table?
- What would happen if the old snapshot's metadata and data files were physically deleted?

## Schema evolution

Add a column to the table:

```python
spark.sql("""
    ALTER TABLE lab.silver.orders
    ADD COLUMN source STRING
""")
```

Inspect the schema:

```python
spark.sql("""
    DESCRIBE lab.silver.orders
""").show()
```

Write data using the new schema:

```python
spark.sql("""
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM lab.silver.orders
    LIMIT 1
""")
```

Questions:

- Did the existing Parquet files have to be rewritten?
- Where is the new schema recorded?
- Why is schema evolution a table-level operation rather than simply changing a Parquet file?

## Compare Iceberg with Parquet

The Spark lab also produced:

```text
silver/spark/orders.parquet/
```

Compare it with:

```text
warehouse/silver/orders/
```

Questions:

- Which one is simply a collection of Parquet files?
- Which one has table metadata and snapshots?
- How would you safely replace a Parquet directory while another query is reading it?
- How does Iceberg make the table state visible atomically?
- Why is a catalog needed once several engines need to share the same Iceberg table?

## Exercises

1. Create `lab.silver.users` as an Iceberg table.
2. Append a new batch of users and inspect the snapshots.
3. Query the table before and after the append using time travel.
4. Add a `country` column and write new data using the evolved schema.
5. Compare the S3 layout of the users and orders tables.
6. Explain the difference between **Parquet**, **Iceberg**, and an **Iceberg catalog**.

## Cleanup

Remove the Iceberg warehouse after completing the exercises:

```bash
aws s3 --profile default rm \
  "s3://$LAB_BUCKET_NAME/warehouse/" --recursive
```

Stop Spark:

```python
spark.stop()
```
