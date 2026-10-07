---
duration: 1h30
category:
  - name: LAB
components:
  - name: SPARK
  - name: S3
platforms:
  - name: LINUX
resources:
  - title: RDD Programming Guide (official documentation)
    url: https://spark.apache.org/docs/latest/rdd-programming-guide.html
  - title: PySpark RDD API reference
    url: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html
revisions:
  - date: 2026-09-25
    comment: Initial page
    author: mori@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: Introduction to Spark's RDD API

## Objectives

- Build RDDs from the bronze datasets and observe how Spark partitions them
- Observe lazy evaluation: transformations build a lineage, only actions run it
- Compare `reduceByKey` and `groupByKey` on the same aggregation, and read the difference in the Spark UI
- Deduplicate `orders` by hand with `reduceByKey`, the RDD equivalent of the window function used in the DataFrame lab
- Join `users` and `orders` as key-value RDDs, and see why a join is Spark's most expensive wide transformation
- Understand what the DataFrame API and Spark SQL give you for free, by first doing without them

## Prerequisites

- The `vscode-pyspark` Onyxia service and the project of the [uv lab](../03.object-storage/lab-1-uv.md)
- The `bronze/users.csv` and `bronze/orders.csv` objects uploaded at the end of the
  [S3 lab](../03.object-storage/lab-2-s3.md)

## Environment

The lab is run from a Jupyter notebook in VS Code. Create a notebook named `lab-pyspark-rdd.ipynb` in your project. Each `python` block of this page is a cell of the notebook. Each `bash` block is a command to run in a terminal.

RDDs operate on files, not on an object store: there is no `RDD` reader for S3A. Download the two bronze datasets once, and work on the local copies for the rest of the lab.

```bash
GIT_REPO_NAME=<git-repo-name>
cd /home/onyxia/work/$GIT_REPO_NAME
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"

aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" users.csv
aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/orders.csv" orders.csv
ls -la users.csv orders.csv
```

Launch PySpark shell.

```bash
pyspark --master local[*]
```

## SparkContext

RDDs are created and manipulated through a `SparkContext`, the object a `SparkSession` wraps internally. Unlike the DataFrame lab, no S3A configuration and no Iceberg catalog are needed: everything here runs against local files.

```python
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
```

The session still runs in `local[*]` mode: driver and executors share one JVM. Partitions and shuffles are real, only the network between executors is not.

## RDD creation and partitions

```python
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")

print(users_lines.getNumPartitions())
print(orders_lines.getNumPartitions())
```

`textFile()` reads the file **line by line**: each element of the RDD is one physical line, with no notion of a header, a column, or a type.

```python
orders_repartitioned = orders_lines.repartition(8)
print(orders_repartitioned.getNumPartitions())
```

Questions:

- What decides the number of partitions `textFile()` picks by default, for a file this size?
- `repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?

## Lazy evaluation and lineage

Remove the header line from `orders_lines`, but do not run anything yet.

```python
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
```

The debug string shows a lineage, not a result: no line has been read or filtered yet. Trigger it with an action, and open the Spark UI at `http://localhost:4040` to see the stage it created.

```python
print(orders_data.count())
```

Questions:

- Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?
- At which point in this notebook would `.cache()` change that answer?

## The cost of no schema: the multiline address

The DataFrame lab reads `users.csv` with `multiLine=True`, because each address spans two physical lines: the street on one line, the city, state and zip code on the next. `textFile()` has no such option. Look at what it actually read:

```python
for line in users_lines.take(6):
    print(line)
```

Every other element is not a user record: it is the tail end of the previous line's address, with no `uuid` and no way on its own to tell which user it belongs to.

```python
print(users_lines.count())
```

This count is higher than the number of users in the file, for exactly that reason.

Questions:

- Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?
- In general terms, what has to be true of a file format for a line-oriented reader like `textFile()` to be safe to
  use on it?

Work around it by keeping only the lines that start a genuine record, recognisable by their leading UUID:

```python
import re

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print(users_records.count())
```

This count should match the number of users you saw in the DataFrame lab. Keep in mind this filter only discards the broken continuation lines; it does not reattach them to the record they belong to, so any field taken from the address itself is still unusable from this RDD.

## Manual parsing of orders

`orders.csv` has no embedded newlines, so a naive split per line is safe here.

```python
def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }

orders_rdd = orders_data.map(parse_order).cache()
orders_rdd.take(3)
```

Nothing here validates the number of fields, the format of `date`, or that `quantity` actually parses as an int. A malformed row fails the job at the action that first touches it, not at read time. It is cached because the next three sections all reuse it.

Questions:

- What would happen to this cell if one line in `orders.csv` had an extra comma inside `product`?
- Where does the DataFrame API perform the equivalent check, and when?

## Key-value RDDs: total quantity per user

```python
from operator import add

user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
user_quantity.take(5)
```

The same result, with `groupByKey`:

```python
user_quantity_grouped = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)
user_quantity_grouped.take(5)
```

Open the Spark UI and compare the shuffle read/write size of the two stages.

Questions:

- `reduceByKey` combines values on each partition before shuffling. What does `groupByKey` shuffle instead, and why is that more expensive?
- For which aggregations would `groupByKey` be unavoidable, `reduceByKey` not being able to express them?

## Manual deduplication

The DataFrame lab deduplicates `orders` on `order_id`, keeping the most recent `ordered_at`, with a window function:
`row_number()` over `PARTITION BY order_id ORDER BY ordered_at DESC`. Do the same with `reduceByKey`:

```python
def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b

orders_dedup = (
    orders_rdd
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)

print(orders_rdd.count(), orders_dedup.count())
```

Questions:

- `partitionBy()` in the window function is described as a wide transformation, shuffling rows so every row of a
  given `order_id` lands on the same partition. What plays that role here?
- Which version states the intent more directly: the window function, or the `reduceByKey` with a custom combiner?

## Joining orders with users

Build a key-value RDD of users from the filtered lines, keeping only what the join needs:

```python
def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username

users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))

enriched = orders_kv.join(users_kv)
enriched.take(3)
```

Each element of `enriched` is `(user_id, (order, username))`. Nothing in this notebook joined `users` and `orders` as DataFrames: this is the one operation here that the DataFrame lab does not already cover.

Questions:

- `join()` is a wide transformation on **both** RDDs. What would need to be true of how `orders_kv` and `users_kv`
  are partitioned for Spark to skip the shuffle?
- Replace `join()` with `leftOuterJoin()`. What changes in the result if a `user_id` in `orders` has no matching user?
- `enriched` is not yet filtered or aggregated. What is the risk of calling `.collect()` on it directly, on a dataset
  much larger than this one?

## Exercises

1. Rewrite `user_quantity` with `aggregateByKey`, producing `(order_count, total_quantity)` per user in a single
   pass, then compute the average order quantity per user from that.
2. Inject one fake `user_id` into a copy of `orders_rdd` (one not present in `users_kv`) and redo the
   `leftOuterJoin()`. Filter for the orders with no matching user and count them.
3. Reimplement `user_quantity` with the DataFrame API in a new cell: `groupBy("user_id").sum("quantity")`. Compare
   the two cells for line count, and compare `.explain()` on the DataFrame version with the RDD's `.toDebugString()`.
4. Time `orders_rdd.count()` and `user_quantity.count()` before and after removing `.cache()` from the parsing cell,
   with `time.perf_counter()`. Check the **Storage** tab of the Spark UI in both cases.

## Cleanup

```python
spark.stop()
exit()
```

```bash
rm -f users.csv orders.csv
```

---

_The content of this document, including all text, images, and associated materials, is the exclusive property of
Adaltas and is protected by applicable copyright laws. Unauthorized distribution, reproduction, or sharing of this
content, in whole or in part, is strictly prohibited without the express written consent of the author(s). Any
violation of this restriction may result in legal action and the imposition of penalties as prescribed by law._
