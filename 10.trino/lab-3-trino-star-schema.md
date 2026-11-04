---
duration: 2h
category:
  - name: LAB
components:
  - name: TRINO
  - name: ICEBERG
  - name: POLARIS
  - name: S3
platforms:
  - name: LINUX
resources:
  - title: Trino documentation
    url: https://trino.io/docs/current/
  - title: Trino, Iceberg connector
    url: https://trino.io/docs/current/connector/iceberg.html
  - title: Trino, EXPLAIN ANALYZE
    url: https://trino.io/docs/current/sql/explain-analyze.html
  - title: Trino Python client
    url: https://github.com/trinodb/trino-python-client
  - title: Apache Iceberg, REST catalog specification
    url: https://iceberg.apache.org/rest-catalog-spec/
revisions:
  - date: 2026-10-08
    comment: Initial page
    author: mori@adaltas.com
  - date: 2026-10-25
    comment: Add the star schema section
    author: mori@adaltas.com
  - date: 2026-11-03
    comment: Update
    author: elodine@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: star schema

Model facts and dimensions to illustrate the usage of star schema.

## Objectives

- Connect to a Trino cluster from Python
- Model the gold layer as a **star schema**: a fact table and its dimensions
- Compare the star schema with the previous method to perform silver to gold transformation

## Prerequisites

- The project of the [uv lab](../03.object-storage/lab-1-uv.md)
- The [Spark lab](./07.sparksql-and-dataframes/lab-pyspark-silver.md), which builds the silver layer as Iceberg tables
- The [Polaris lab](./09.polaris/lab-polaris.md), which exposes the silver layer through a Polaris catalog
- The [Trino lab](./10.trino/lab-2-trino-gold.md).

## Environment

The Python shell from the previous lab should be open and ready for the following lab, if it's closed, you need to checkout the enviornment setup.

## Gold layer with star schema

In the previous lab, the gold tables each answer one question. Another way to shape the gold layer is a **star schema**: a "fact" table which records the events to measure, surrounded by "dimension" tables which describe them. A new question is answered by joining the fact table to the dimensions it needs, without building a new table.

| Table         | One row represents | Content                                               |
| ------------- | ------------------ | ----------------------------------------------------- |
| `fact_orders` | one order          | the keys of the dimensions, `order_id` and `quantity` |
| `dim_user`    | one user           | state, city, birthdate                                |
| `dim_product` | one product        | the product name                                      |
| `dim_date`    | one calendar day   | year, quarter, month, day of the week                 |

The grain of the fact table is decided first. Here it is the grain of `silver.orders`: one row per order. The dimensions are built before the fact table, which looks up their keys.

### Dimensions

`dim_user` and `dim_product` use generated surrogate keys, an integer generated in the gold layer and independent of the identifiers of the source. Whereas `dim_date` uses an integer derived from the calendar date. The silver identifier is kept in the dimension as an attribute. Only the attributes useful to analyze the orders are copied: `email` and `street` stay in the silver layer.

`dim_user` and `dim_product` are created.

```python
sql("""
    CREATE TABLE lakehouse.gold.dim_user AS
    SELECT
        row_number() OVER (ORDER BY user_id) AS user_key,
        user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users
""")
sql("""
    CREATE TABLE lakehouse.gold.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)
""")
```

The date dimension is not derived from a silver table. It is generated, with one row per day between the first and the last order:

```python
first_day, last_day = sql("""
    SELECT CAST(min(ordered_at) AS date) AS first_day,
           CAST(max(ordered_at) AS date) AS last_day
    FROM lakehouse.silver.orders
""").iloc[0]

sql(f"""
    CREATE TABLE lakehouse.gold.dim_date AS
    SELECT
        CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
        d                                       AS full_date,
        year(d)                                 AS year,
        quarter(d)                              AS quarter,
        month(d)                                AS month,
        date_format(CAST(d AS timestamp), '%M') AS month_name,
        day_of_week(d)                          AS day_of_week,
        day_of_week(d) IN (6, 7)                AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)
""")
```

`sequence()` generates an array of dates and `UNNEST` turns it into rows. The key of this dimension is the integer `YYYYMMDD`, which is easy to build from any timestamp.

### Fact table

The fact table keeps one row per order, with the keys of the dimensions instead of the silver identifiers:

```python
sql("""
    CREATE TABLE lakehouse.gold.fact_orders AS
    SELECT
        o.order_id,
        u.user_key,
        p.product_key,
        CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
        o.ordered_at,
        o.quantity
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_id = o.user_id
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product = o.product
""")

sql("""
    SELECT 'dim_user' AS table_name, count(*) AS row_count FROM lakehouse.gold.dim_user
    UNION ALL SELECT 'dim_product', count(*) FROM lakehouse.gold.dim_product
    UNION ALL SELECT 'dim_date', count(*) FROM lakehouse.gold.dim_date
    UNION ALL SELECT 'fact_orders', count(*) FROM lakehouse.gold.fact_orders
""")
```

With the dataset of the lab, `dim_user` has 50 rows, `dim_product` has 6, and `fact_orders` has 5000, like `silver.orders`.

Notes on the fact table:

- The lookups are `LEFT JOIN`. An order whose user or product is not found is kept, with a `NULL` key, instead of silently disappearing.
- `order_id` stays in the fact table. An identifier which has no dimension of its own is called a **degenerate dimension**.
- `ordered_at` is kept for analyses at the time of the day. `date_key` is the link to `dim_date`.

## Query the star

Join the fact table to the dimensions required by the question, and only those:

```python
sql("""
    SELECT d.year, u.state, p.product,
           count(*)        AS orders,
           sum(f.quantity) AS units
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    GROUP BY d.year, u.state, p.product
    ORDER BY units DESC
    LIMIT 10
""")
```

The aggregate `orders_by_state_month` can be rebuilt from the star, without reading the silver layer:

```python
sql("""
    SELECT u.state, d.year, d.month,
           count(*)                   AS orders,
           sum(f.quantity)            AS units,
           count(DISTINCT f.user_key) AS buyers
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key = f.user_key
    GROUP BY u.state, d.year, d.month
    ORDER BY d.year, d.month, u.state
    LIMIT 10
""")
```

The numbers must match the first rows of `orders_by_state_month`.

## Validation

As for the other gold tables, compare the star with the silver layer. No output means that every assertion passed.

```python
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_orders = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_units = sql("SELECT sum(quantity) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]

# No order is lost or duplicated by the lookups of the keys
assert silver_orders == fact_orders, f"{silver_orders} != {fact_orders}"
assert silver_units == fact_units, f"{silver_units} != {fact_units}"

# Every order finds its three dimension rows
unmatched = sql("""
    SELECT count(*) AS v
    FROM lakehouse.gold.fact_orders AS f
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    LEFT JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL
""").iloc[0, 0]
assert unmatched == 0, f"{unmatched} orders without a complete dimension row"
```

Questions:

- Are these checks sufficient to guarantee that every order is correctly represented in fact_orders? Give an example of a data defect that could go undetected, and identify additional checks needed to verify the correctness of the transformation.
- What is the grain of `fact_orders`? Of `dim_date`?
- Which assertion fails if `silver.users` contains the same `user_id` twice? Which one fails if an order has no product (`NULL`)?
- The surrogate keys are generated with `row_number()`. What happens to `user_key` if `dim_user` is rebuilt after a new user is added? What does it imply for `fact_orders`?
- `dim_user` overwrites the attributes of a user. If a user moves to another state, which state do the past orders show? What would a dimension that keeps the history of the changes need?
- Compare `orders_by_state_month` and the star query which rebuilds it. Which questions does each one answer without a new table, and what does the star cost at query time?

## Exercises

1. Create a table `gold.orders_by_state_month_part` with `WITH (partitioning = ARRAY['month(month)'])`. List its files and compare with the unpartitioned table. Run a query filtering on one month and compare the input rows in `EXPLAIN ANALYZE`.
2. Write a gold table `gold.cohorts` giving, for each decade of birth, the number of users, the number of orders and the average number of units per order.
3. Append one order to `silver.orders` with `INSERT INTO`, refresh `gold.orders_by_state_month` with a `MERGE`, and read the previous state of the gold table with `FOR VERSION AS OF`.
4. Run `ALTER TABLE lakehouse.gold.top_products EXECUTE optimize`. List the snapshots and the files before and after, and explain what changed.
5. Run the same aggregation of `orders_by_state_month` from the Spark session of the Spark lab, connected to Polaris, and compare the row counts of both results.

## Close the connection:

```python
conn.close()
```

## Commit

Open another terminal and prepare the metadata file to commit.

```bash
cd /home/onyxia/work/$GIT_REPO_NAME
mkdir lab_trino
aws s3 sync \
  "s3://${LAB_BUCKET_NAME}/warehouse/gold" \
  "./lab_trino/gold/" \
  --exclude "*/data/*"
```

Changes are committed to Git.

```bash
git add \
  lab_trino \
git commit -m "feat(trino-lab): trino gold metadata"
git push
```

---

_The content of this document, including all text, images, and associated materials, is the exclusive property of
Adaltas and is protected by applicable copyright laws. Unauthorized distribution, reproduction, or sharing of this
content, in whole or in part, is strictly prohibited without the express written consent of the author(s). Any
violation of this restriction may result in legal action and the imposition of penalties as prescribed by law._
