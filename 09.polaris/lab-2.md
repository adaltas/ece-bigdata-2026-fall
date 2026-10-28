---
duration: 2h
category:
  - name: LAB
components:
  - name: SPARK
  - name: ICEBERG
  - name: POLARIS
  - name: S3
platforms:
  - name: LINUX
resources:
  - title: Apache Polaris documentation
    url: https://polaris.apache.org/
  - title: Polaris access control (securable objects, privileges, roles)
    url: https://polaris.apache.org/releases/1.8.0/managing-security/access-control/
  - title: Apache Iceberg, Spark configuration
    url: https://iceberg.apache.org/docs/latest/spark-configuration
  - title: Apache Iceberg, REST catalog specification
    url: https://iceberg.apache.org/rest-catalog-spec/
revisions:
  - date: 2026-10-21
    comment: Updated the lab to use the final RBAC model with dev and analyst principals
    author: mori@adaltas.com
  - date: 2026-10-08
    comment: Initial draft, Polaris configuration to be adapted once the cluster is deployed
    author: mori@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: Govern Iceberg tables with Polaris

## Objectives

- Connect a Spark session to a Polaris catalog through the Iceberg REST API
- Register an existing Iceberg table from the Spark lab in Polaris, without copying its data
- Create tables through Polaris, and see the files they produce
- Compare the operations permitted to the `dev` and `analyst` principals on the same catalog
- Understand what Polaris enforces, and what it cannot enforce on this storage

## Prerequisites

- The [Spark DataFrame lab](../07.sparksql-and-dataframes/lab-pyspark-silver.md)
- The `vscode-pyspark` Onyxia service and the project of the lab
- The [Polaris deployment lab](./lab-1-deployment.md) completed
- The `vscode-pyspark` Onyxia service with limits of `cpu: 2000m`, `memory 2500Mi`.

## Environment

Navigate to the project directory and define the environment variables. Note that the Polaris catalog name is the `<client-id>`.

```bash
GIT_REPO_NAME=<git-repo-name>
cd /home/onyxia/work/$GIT_REPO_NAME
eval "$(aws configure export-credentials --profile default --format env)"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export AWS_REGION="us-east-1"
echo "$LAB_BUCKET_NAME"
#> user-gollum
set -a; . /home/onyxia/.polaris-credentials.env; set +a
echo $POLARIS_DEV_CREDENTIAL
#> ccfc...:c72bfea3e8619...
echo $POLARIS_ANALYST_CREDENTIAL
#> e9c2...:48db20d1d282a...
```

## RBAC chain

The following table defines the final RBAC model for the lab. Each principal is assigned a principal role, which in turn is granted a catalog role containing the privileges required for its tasks.

| Principal | Principal role  | Catalog role          | Catalog       | Privileges                                                                   |
| --------- | --------------- | --------------------- | ------------- | ---------------------------------------------------------------------------- |
| `dev`     | `data_engineer` | `catalog_contributor` | `<client-id>` | `CATALOG_MANAGE_ACCESS`, `CATALOG_MANAGE_METADATA`, `CATALOG_MANAGE_CONTENT` |
| `analyst` | `data_analyst`  | `catalog_reader`      | `<client-id>` | `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES`        |

The RBAC definition is already done with the bootstrap job:

1. The internal catalog `<client-id>`, with the base location and allowed location `s3://<client-id>/warehouse/`.
2. The principals `dev` and `analyst`.
3. The reusable principal roles `data_engineer` and `data_analyst`.
4. `data_engineer` is assigned to principal `dev`, and `data_analyst` is assigned to principal `analyst`.
5. The catalog roles `catalog_contributor` and `catalog_reader` in catalog `<client-id>`, and they are granted the privileges listed in the table above.
6. `catalog_contributor` is granted to principal role `data_engineer`, and `catalog_reader` granted to principal role `data_analyst`.

## SparkSession with the Polaris catalog

Launch the PySpark shell with the Iceberg runtime, as in the Spark lab. The Spark session declares two catalogs: `lab`, the Hadoop catalog used in the previous lab, and `polaris`, the REST catalog that connects to the student's catalog managed by Polaris. The names `lab` and `polaris` are Spark catalog identifiers; these Spark catalog identifiers are distinct from the catalog managed by Polaris.

```bash
pyspark --master local[*] --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

The `POLARIS_PRINCIPAL` variable selects the principal whose credentials are used by the Spark session. Start with `dev`. To run the analyst checks, change it to `analyst` and restart the spark session, as described in **Read as the analyst**.

```python
import os
from pyspark.sql import SparkSession

POLARIS_PRINCIPAL = "dev"  # "dev" or "analyst"
POLARIS_CREDENTIAL = os.environ[f"POLARIS_{POLARIS_PRINCIPAL.upper()}_CREDENTIAL"]

BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-polaris")
    .master("local[*]")
    # S3 access, identical to the Spark lab
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION"),
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # Hadoop catalog of the Spark lab
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    # Polaris catalog, REST API, principal chosen by POLARIS_PRINCIPAL
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
    .config("spark.sql.catalog.polaris.credential", POLARIS_CREDENTIAL)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    # No credential vending on Ceph RGW: files are read with the S3A keys above
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("Polaris principal:", POLARIS_PRINCIPAL)
spark
```

Check if the Hadoop configuration for S3A connector class. Stop `spark.stop()` the session and recreate a new one if the output is `None`.

```bash
print(spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl", None))
#> org.apache.hadoop.fs.s3a.S3AFileSystem
```

Check that the two catalogs are reachable:

```python
spark.sql("SHOW NAMESPACES IN lab").show()
spark.sql("SHOW NAMESPACES IN polaris").show()
```

The first command lists the namespaces in the Hadoop catalog. The second queries Polaris and lists the namespaces available in your catalog.

Questions:

- Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?
- Where is the credential used: in the Spark session, in the bucket, or in a request to Polaris?

## Register the Silver table in Polaris

The table `lab.silver.orders` exists in the bucket, with its metadata, but Polaris does not know about it. Registration adds a reference to the table's current metadata file in Polaris without copying the data files.

First, open another terminal and locate the current metadata file. The `version-hint.text` file contains the number of the latest version:

```bash
aws s3 --profile default cp \
  "s3://$LAB_BUCKET_NAME/warehouse/silver/orders/metadata/version-hint.text" -
#> 1
aws s3 --profile default ls \
  "s3://$LAB_BUCKET_NAME/warehouse/silver/orders/metadata/" | grep metadata.json
#> 2026-10-08 10:12:44       1913 v1.metadata.json
```

Replace `1` below with the version you read. Using the `dev` principal, register the table in Polaris and then query it:

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")

spark.sql(f"""
    CALL polaris.system.register_table(
      table => 'silver.orders',
      metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v1.metadata.json'
    )
""").show()

spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
```

The count should match the table from the Spark lab: 5,000 orders. The table is now accessible through both the lab and polaris catalogs. Initially, both reference the same table metadata, but their catalog metadata pointers are managed independently. The Spark lab wrote the table through the Hadoop catalog. From now on, write to it only through Polaris: a commit through `lab` would move the pointer of the Hadoop catalog and leave Polaris on the old version.

The metadata file must be in an allowed location of your catalog. If the registration fails with a message about locations, ask the instructor.

Questions:

- What did `register_table` write, in Polaris and in the bucket? Compare the number of objects under `warehouse/silver/orders/` before and after.
- Why is it dangerous to keep writing to this table through the `lab` catalog once it is registered in `polaris`?

## Create tables as `dev`

This section requires `POLARIS_PRINCIPAL = "dev"`. Check the table through the catalog:

```python
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)
```

Create a second table, managed by Polaris this time, from the users table of the Spark lab:

```python
stg_users = spark.table("lab.silver.users")
stg_users.writeTo("polaris.silver.users").createOrReplace()

spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
```

Compare where the two tables live in the bucket:

```bash
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/warehouse/silver/" --recursive | awk '{print $4}'
```

Questions:

- Where did Polaris store the files for `silver.users`, and why?
- Both tables are in your bucket. What is the difference between "registered" and "created through Polaris" for the data files?

## Schema change as `dev`

This section requires `POLARIS_PRINCIPAL = "dev"`. Add a column to a table managed by Polaris:

```python
spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
spark.sql("DESCRIBE polaris.silver.users").show()
```

Questions:

- Which metadata file records the change? Is it the same file that `register_table` pointed to?
- Which privilege does Polaris check for this statement?

## Read as the analyst

This section requires `POLARIS_PRINCIPAL = "analyst"`. The `analyst` principal accesses the same catalog as `dev`, but has only the privileges granted through `catalog_reader`.

1. Restart the notebook kernel to close the existing Spark session
2. Change the value of `POLARIS_PRINCIPAL` in the session cell to `"analyst"`, and run the session cell again
3. Check that the printed role is `analyst`, then run the cells below

```python
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT product, count(*) FROM polaris.silver.orders GROUP BY product").show()
```

Then try to write:

```python
spark.sql("INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
```

The write should fail. Record the error message. The error should indicate that Polaris returned a `Forbidden` response, meaning that the operation was denied before the write could be committed.

```python
spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")
```

The schema change should fail for the same reason.

Questions:

- Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?
- Check the snapshots of `silver.orders` as `dev`, after restarting the kernel with `POLARIS_PRINCIPAL = "dev"`. Did the failed `INSERT` create one?

When the checks are done, set `POLARIS_PRINCIPAL` back to `"dev"` and restart the kernel for the next sections.

## Exercises

1. Remove the `country` column from `polaris.silver.users` and check the change in the snapshots. Explain why the change is a metadata operation.
2. The `catalog_reader` role is intended to provide read access at the catalog level. Create a more restrictive catalog role named `silver_reader`, granting the required listing and read privileges only on the `silver` namespace. Then create a `gold` namespace as `dev` and test whether a principal assigned `silver_reader` can read tables in `silver` but not in `gold`. You may need to create a separate principal and principal role to test this cleanly.

## Cleanup

Drop the tables created in your Polaris catalog, as `dev`. The tables of the bronze layer are kept for the next modules, and the Silver table is kept because the Trino lab reads it.

```python
spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
```

Do not unregister `silver.orders`. Unregistering removes the pointer only, not the data, but the Trino lab expects the table to exist in Polaris. Stop the Spark session:

```python
spark.stop()
exit()
```

---

_The content of this document, including all text, images, and associated materials, is the exclusive property of
Adaltas and is protected by applicable copyright laws. Unauthorized distribution, reproduction, or sharing of this
content, in whole or in part, is strictly prohibited without the express written consent of the author(s). Any
violation of this restriction may result in legal action and the imposition of penalties as prescribed by law._
