---
duration: 2h
category:
  - name: LAB
components:
  - name: DBT
  - name: DUCKDB
  - name: S3
platforms:
  - name: LINUX
resources:
  - title: dbt (official documentation)
    url: https://docs.getdbt.com/docs/introduction
  - title: dbt-duckdb adapter
    url: https://github.com/duckdb/dbt-duckdb
  - title: dbt data tests (official documentation)
    url: https://docs.getdbt.com/docs/build/data-tests
  - title: dbt incremental models (official documentation)
    url: https://docs.getdbt.com/docs/build/incremental-models
  - title: dbt node selection syntax (official documentation)
    url: https://docs.getdbt.com/reference/node-selection/syntax
revisions:
  - date: 2026-09-25
    comment: Update
    author: mori@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: Bronze to Silver transformation with dbt and DuckDB

## Objectives

- Create a dbt project using DuckDB as the execution engine
- Declare the CSV datasets stored on S3 as the sources of the bronze layer
- Build the silver layer: typed, cleaned and deduplicated models
- Validate the data with generic and singular tests, and investigate the failures
- Build the gold layer: an incremental fact table and an aggregate exported to S3 in Parquet
- Run the whole pipeline with `dbt build` and explore its lineage

## Prerequisites

- The `vscode-pyspark` Onyxia service and the project of the [uv lab](../03.object-storage/lab-1-uv.md)
- The `bronze/users.csv` and `bronze/orders.csv` objects uploaded at the end of the
  [S3 lab](../03.object-storage/lab-2-s3.md)
- The [DuckDB lab](../04.sql-analytics/lab-duckdb.md), in particular the S3 configuration with the
  `s3_onyxia_connection` secret

## Architecture

In this lab, the raw datasets already exist in the **bronze** layer:

```text
S3
└── bronze/
    ├── users.csv
    └── orders.csv
```

dbt does not ingest or modify these files. It declares them as sources and transforms them into the **silver** layer:

```text
S3 bronze
    │
    ├── users.csv ──→ stg_users
    │
    └── orders.csv ─→ stg_orders
                         │
                         ▼
                    Silver layer
```

The bronze files remain unchanged. The silver models contain typed, cleaned and deduplicated data suitable for downstream analytics.

## Environment

Move into the project and define the bucket name:

```bash
GIT_REPO_NAME=<git-repo-name>

cd /home/onyxia/work/$GIT_REPO_NAME

export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"

echo "$LAB_BUCKET_NAME"
#> user-gollum
```

Check that the bronze datasets are present:

```bash
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/"
#> 2026-09-14 11:02:10     331568 orders.csv
#> 2026-09-14 11:02:09       7351 users.csv
```

If the files are missing, generate and upload them again:

```bash
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv

aws s3 --profile default cp users.csv \
  "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3 --profile default cp orders.csv \
  "s3://$LAB_BUCKET_NAME/bronze/orders.csv"
```

The bucket name is read from the `LAB_BUCKET_NAME` environment variable by dbt.

## Installation

dbt Core is a Python package. Each database is supported by an adapter, a separate package which depends on `dbt-core`.
[dbt-duckdb](https://github.com/duckdb/dbt-duckdb) runs the models inside an embedded DuckDB database.

Add it to the existing uv project:

```bash
uv add dbt-duckdb
uv run dbt --version
#> Core:
#>   - installed: 1.12.5
#>   - latest:    1.12.5 - Up to date!
#>
#> Plugins:
#>   - duckdb: 1.11.0 - Up to date!
```

The versions may differ.

dbt does not process the data itself. It compiles SQL and sends the resulting statements to DuckDB.

The architecture is therefore:

```text
dbt
 │
 │ compiles SQL / executes models
 ▼
DuckDB
 │
 │ reads
 ▼
S3 bronze/*.csv
```

## dbt project

### Initialization

Create the project in a `lab_medallion` directory. The `--skip-profile-setup` flag disables the interactive prompts,
the connection is configured below.

```bash
uv run dbt init lab_medallion --skip-profile-setup

cd lab_medallion

rm -rf models/example

find . -type f | sort
#> ./analyses/.gitkeep
#> ./dbt_project.yml
#> ./.gitignore
#> ./macros/.gitkeep
#> ./README.md
#> ./seeds/.gitkeep
#> ./snapshots/.gitkeep
#> ./tests/.gitkeep
```

The remaining commands of the lab are executed from the `lab_medallion` directory. `uv run` looks for the
`pyproject.toml` file in the parent directories and uses the environment of the project.

- `dbt_project.yml`: the name of the project, the location of its files, and the configuration of its resources
- `models/`: the SQL models, and the YAML files documenting and testing them
- `seeds/`: small CSV files loaded as tables, used for reference data
- `tests/`: singular tests, SQL queries returning the rows which violate an assertion
- `macros/`: reusable Jinja functions
- `snapshots/`, `analyses/`: slowly changing dimensions and ad-hoc queries, not used in this lab

```text
lab_medallion/
├── models/
├── tests/
├── macros/
├── seeds/
├── snapshots/
├── analyses/
└── dbt_project.yml
```

For this lab, we mainly use:

- `models/`: SQL models and their documentation/tests
- `tests/`: singular SQL tests
- `dbt_project.yml`: project configuration

We will not use seeds, snapshots or analyses.

### Connection profile

The connection is defined in a profile. dbt looks for the `profiles.yml` file in the current directory first, then in
`~/.dbt/`. The profile of this lab contains no credential and is stored with the project:

```bash
cat > profiles.yml <<'YAML'
lab_medallion:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: lab_medallion.duckdb
      threads: 4
      settings:
        TimeZone: UTC
YAML
```

The profile defines how dbt connects to DuckDB.

- `target`: the default target
- `path`: the DuckDB database file
- `threads`: number of models that can run in parallel
- `settings`: DuckDB configuration

In the Onyxia environment, the S3 credentials are already available to DuckDB through the persistent
`s3_onyxia_connection` secret created in the previous DuckDB lab.

Validate the configuration and the connection:

```bash
uv run dbt debug
#> ...
#> Configuration:
#>   profiles.yml file [OK found and valid]
#>   dbt_project.yml file [OK found and valid]
#> Required dependencies:
#>  - git [OK found]
#>
#> Connection:
#>   database: lab_medallion
#>   schema: main
#>   path: lab_medallion.duckdb
#> ...
#>   Connection test: [OK connection ok]
#>
#> All checks passed!
```

### Project configuration

Replace `dbt_project.yml` with:

```bash
cat > dbt_project.yml <<'YAML'
name: lab_medallion
version: "1.0.0"
profile: lab_medallion

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - target
  - dbt_packages

models:
  lab_medallion:
    silver:
      +materialized: view
      +schema: silver
YAML
```

Create the silver directory:

```bash
mkdir -p models/bronze models/silver
```

The `+` prefix marks a configuration inherited by all the resources of the directory, and each model can override it.
By default, dbt concatenates the schema of the target, `main`, and the custom schema: the silver models are created in
the `main_silver` schema. This prevents developers sharing a warehouse from overwriting each other's tables.

dbt generates files which are not source code. The `.gitignore` file created by `dbt init` is itself ignored by the
`.*` rule of the project, add the patterns to the `.gitignore` file at the root of the project instead:

```bash
cat >> ../.gitignore <<'INI'
target/
dbt_packages/
logs/
*.duckdb
INI
```

## Bronze layer

The bronze layer is not built by dbt: the raw files are loaded by the ingestion process, here the Kubernetes Job of the
S3 lab. dbt declares them as sources.

```bash
cat > models/bronze/sources.yml <<'YAML'
sources:
  - name: bronze
    description: Raw datasets uploaded to the S3 bucket by the dataset generators.
    config:
      meta:
        external_location: "read_csv('s3://{{ env_var('LAB_BUCKET_NAME') }}/bronze/{name}.csv', strict_mode = false)"
    tables:
      - name: users
        description: Users generated by `dataset-users`, one row per user.
      - name: orders
        description: Orders generated by `dataset-orders`, one row per order.
YAML
```

A source is referenced in a model with `{{ source('bronze', 'users') }}`. By default, dbt replaces it with the name of
a table in the database. The `external_location` property is specific to dbt-duckdb: the reference is replaced by a
`read_csv` call on the S3 object, `{name}` being substituted with the name of the table. The `env_var` Jinja function
reads the bucket name from the environment, which keeps it out of the code.

`dbt show` compiles and executes a query, and prints a preview of the result:

```bash
uv run dbt show -q --limit 3 \
  --inline "select uuid, user_uuid, date, quantity, product from {{ source('bronze', 'orders') }}"
#> | uuid                 | user_uuid            |                 date | quantity | product |
#> | -------------------- | -------------------- | -------------------- | -------- | ------- |
#> | 2339ba19-2563-4cc... | bdd640fb-0667-4ad... | 2020-01-01 00:02:... |        4 | drink   |
#> | b49e04cc-c243-49e... | bdd640fb-0667-4ad... | 2020-01-01 01:28:... |        5 | bread   |
#> | 41843b03-04dd-405... | bdd640fb-0667-4ad... | 2020-01-01 02:12:... |        2 | donut   |
```

Questions:

- The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months
  later?
- The source reads the whole file on every query. Which other format and layout would you choose if the orders were
  ingested every hour for years?

## Silver layer

The silver models clean the raw records, with one model per source table. By convention, they are prefixed with `stg_`
for staging.

### Users

The `address` column spans 2 lines: the street, then the city, the state and the zip code. Look at the raw addresses:
some of them, such as `DPO AP 09617`, are military addresses without a city.

```bash
uv run dbt show -q --limit 3 --inline "select address from {{ source('bronze', 'users') }}" --output json
#> {
#>   "show": [
#>     {
#>       "address": "908 Jennifer Squares\nRobinsonshire, KY 01352"
#>     },
#>     {
#>       "address": "Unit 6184 Box 9593\nDPO AP 09617"
#>     },
#>     {
#>       "address": "283 Steven Groves\nLake Mark, WI 07832"
#>     }
#>   ]
#> }
```

```bash
cat > models/silver/stg_users.sql <<'SQL'
with source as (
    select * from {{ source('bronze', 'users') }}
),

typed as (
    select
        cast(uuid as uuid) as user_id,
        trim(username) as username,
        trim(name) as name,
        upper(trim(sex)) as sex,
        lower(trim(mail)) as email,
        cast(birthdate as date) as birthdate,
        -- The address spans 2 lines: the street, then the city, the state and the zip code
        split_part(address, chr(10), 1) as street,
        split_part(address, chr(10), 2) as address_line_2
    from source
)

select
    user_id,
    username,
    name,
    sex,
    email,
    birthdate,
    street,
    -- Military addresses, such as "DPO AE 12345", have no comma
    nullif(regexp_extract(address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from typed
SQL
```

The model is a `SELECT` statement split into common table expressions: one step reads the source, the next ones
transform it. The columns are renamed with consistent conventions (`uuid` becomes `user_id`, `mail` becomes `email`),
cast to their types, and standardized.

### Orders

```bash
cat > models/silver/stg_orders.sql <<'SQL'
with source as (
    select * from {{ source('bronze', 'orders') }}
),

typed as (
    select
        cast(uuid as uuid) as order_id,
        cast(user_uuid as uuid) as user_id,
        cast(date as timestamptz) as ordered_at,
        cast(quantity as integer) as quantity,
        lower(trim(product)) as product
    from source
)

select *
from typed
-- Keep a single row per order, the most recent one if the source delivers duplicates
qualify row_number() over (partition by order_id order by ordered_at desc) = 1
SQL
```

The generator does not produce duplicates, but ingestion processes do: retried extractions, overlapping incremental
loads. The deduplication makes the model robust to a replay of the bronze layer.

## Build

`dbt run` builds the models. The `--select` option restricts the execution to the models of the `silver` directory:

```bash
uv run dbt run --select silver
#> ...
#> 1 of 2 START sql view model main_silver.stg_orders ............................. [RUN]
#> 2 of 2 START sql view model main_silver.stg_users .............................. [RUN]
#> 1 of 2 OK created sql view model main_silver.stg_orders ........................ [OK in 0.07s]
#> 2 of 2 OK created sql view model main_silver.stg_users ......................... [OK in 0.07s]
#>
#> Finished running 2 view models in 0 hours 0 minutes and 0.13 seconds (0.13s).
#>
#> Completed successfully
#>
#> Done. PASS=2 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=2
```

dbt writes the compiled queries in `target/compiled/`, where the Jinja references are resolved, and the statements sent
to DuckDB in `target/run/`. Compare both versions of `stg_users.sql`:

```bash
cat target/compiled/lab_medallion/models/silver/stg_users.sql
cat target/run/lab_medallion/models/silver/stg_users.sql
```

Open the database with the DuckDB CLI in read-only mode to inspect the result. A DuckDB file is opened by a single
process in read-write mode: close the CLI before running dbt again.

```bash
duckdb -readonly lab_medallion.duckdb
```

```sql
SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY ALL;
-- ┌──────────────┬────────────┬────────────┐
-- │ table_schema │ table_name │ table_type │
-- │   varchar    │  varchar   │  varchar   │
-- ├──────────────┼────────────┼────────────┤
-- │ main_silver  │ stg_orders │ VIEW       │
-- │ main_silver  │ stg_users  │ VIEW       │
-- └──────────────┴────────────┴────────────┘
SELECT username, street, city, state, zip_code FROM main_silver.stg_users LIMIT 3;
-- ┌────────────────┬──────────────────────┬───────────────┬─────────┬──────────┐
-- │    username    │        street        │     city      │  state  │ zip_code │
-- │    varchar     │       varchar        │    varchar    │ varchar │ varchar  │
-- ├────────────────┼──────────────────────┼───────────────┼─────────┼──────────┤
-- │ garzaanthony   │ 908 Jennifer Squares │ Robinsonshire │ KY      │ 01352    │
-- │ blairamanda    │ Unit 6184 Box 9593   │ NULL          │ AP      │ 09617    │
-- │ elizabethmiles │ 283 Steven Groves    │ Lake Mark     │ WI      │ 07832    │
-- └────────────────┴──────────────────────┴───────────────┴─────────┴──────────┘
DESCRIBE main_silver.stg_orders;
-- ┌─────────────┬──────────────────────────┬─────────┬─────────┬─────────┬─────────┐
-- │ column_name │       column_type        │  null   │   key   │ default │  extra  │
-- │   varchar   │         varchar          │ varchar │ varchar │ varchar │ varchar │
-- ├─────────────┼──────────────────────────┼─────────┼─────────┼─────────┼─────────┤
-- │ order_id    │ UUID                     │ YES     │ NULL    │ NULL    │ NULL    │
-- │ user_id     │ UUID                     │ YES     │ NULL    │ NULL    │ NULL    │
-- │ ordered_at  │ TIMESTAMP WITH TIME ZONE │ YES     │ NULL    │ NULL    │ NULL    │
-- │ quantity    │ INTEGER                  │ YES     │ NULL    │ NULL    │ NULL    │
-- │ product     │ VARCHAR                  │ YES     │ NULL    │ NULL    │ NULL    │
-- └─────────────┴──────────────────────────┴─────────┴─────────┴─────────┴─────────┘
.quit
```

Questions:

- The silver models are views. What happens when a view is queried, and when is it preferable to a table?
- `UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?

## Data tests

### Generic tests

Generic tests are declared in YAML, next to the models, along with their documentation. Each test compiles to a query
returning the rows which violate the assertion: the test passes when the query returns no row.

```bash
cat > models/silver/silver.yml <<'YAML'
models:
  - name: stg_users
    description: One row per user, typed, with the address split into its components.
    columns:
      - name: user_id
        description: Identifier of the user.
        data_tests:
          - unique
          - not_null
      - name: email
        data_tests:
          - unique
          - not_null
      - name: sex
        data_tests:
          - accepted_values:
              arguments:
                values: ["F", "M"]
      - name: state
        description: Two letters code of the state, territory or military region.
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('states')
                field: code

  - name: stg_orders
    description: One row per order, typed and deduplicated.
    columns:
      - name: order_id
        data_tests:
          - unique
          - not_null
      - name: user_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('stg_users')
                field: user_id
      - name: quantity
        data_tests:
          - accepted_values:
              arguments:
                values: [1, 2, 3, 4, 5]
                quote: false
      - name: product
        data_tests:
          - accepted_values:
              arguments:
                values: ["bread", "brioche", "cookie", "croissant", "donut", "drink"]
YAML
```

- `unique` and `not_null` validate a primary key.
- `accepted_values` validates a closed list of values.
- `relationships` validates a foreign key: every value must exist in the referenced model.

Run the tests of the silver layer:

```bash
uv run dbt test --select silver
```

### Investigate a failing test

Add a singular test to verify that every order references an existing user.

```bash
cat > tests/assert_orders_have_users.sql <<'SQL'
select
    o.order_id,
    o.user_id
from {{ ref('stg_orders') }} o
left join {{ ref('stg_users') }} u
    on o.user_id = u.user_id
where u.user_id is null
SQL
```

A singular test fails when its query returns rows.

Run:

```bash
uv run dbt test --select silver
```

If the test fails, inspect the compiled query:

```bash
cat target/compiled/lab_medallion/tests/assert_orders_have_users.sql
```

Store the failing rows:

```bash
uv run dbt test --select silver --store-failures
```

Check the message printed and then inspect the failure table using DuckDB.

```bash
duckdb -readonly lab_medallion.duckdb
```

```sql
SELECT * FROM "lab_medallion"."main_dbt_test__audit"."<failure_table_name>";
-- ┌─────────────┬───────────┐
-- │ value_field │ n_records │
-- │   varchar   │   int64   │
-- ├─────────────┼───────────┤
-- │ donuts      │       824 │
-- │ dring       │       824 │
-- └─────────────┴───────────┘
.quit
```

The important idea is:

```text
test query
    │
    ├── returns 0 rows → PASS
    │
    └── returns rows    → FAIL
```

### Singular tests

A singular test is a SQL query stored in the `tests/` directory, for assertions which do not fit a generic test. A user
cannot place an order before being born:

```bash
cat > tests/assert_orders_after_user_birth.sql <<'SQL'
-- A user cannot order before being born: the test fails if this query returns rows
select
    o.order_id,
    o.ordered_at,
    u.user_id,
    u.birthdate
from {{ ref('stg_orders') }} o
join {{ ref('stg_users') }} u on o.user_id = u.user_id
where o.ordered_at < u.birthdate
SQL
uv run dbt test --select silver
#> ...
#> 4 of 14 FAIL 288 assert_orders_after_user_birth ................................ [FAIL 288 in 0.13s]
#> ...
#> Done. PASS=13 WARN=0 ERROR=1 SKIP=0 NO-OP=0 REUSED=0 TOTAL=14
```

The number of failing rows depends on the day the dataset was generated. The orders are dated in 2020, while Faker
generates birthdates relative to the current date: some users are born years after their first order. This time, the
source data is wrong, and the bronze layer cannot be corrected.

Several strategies exist: reject the invalid records in the silver layer, replace the invalid birthdates with `NULL`,
ask the producer of the data to fix the generator, or accept the issue temporarily and monitor it. The last option is
implemented with the `severity` configuration, which turns the failure into a warning. Add the configuration at the top
of the test:

```bash
sed -i "1i {{ config(severity='warn') }}\n" tests/assert_orders_after_user_birth.sql
head -3 tests/assert_orders_after_user_birth.sql
#> {{ config(severity='warn') }}
#>
#> -- A user cannot order before being born: the test fails if this query returns rows
uv run dbt test --select silver
#> ...
#> 4 of 14 WARN 288 assert_orders_after_user_birth ................................ [WARN 288 in 0.07s]
#> ...
#> Done. PASS=13 WARN=1 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=14
```

Questions:

- Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the
  sales per region?
- The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which
  reference data would it require?

## Full silver pipeline

`dbt build` combines model execution and testing.

Remove the database:

```bash
rm -f lab_medallion.duckdb
```

Then:

```bash
uv run dbt build --select silver
```

dbt builds the models and executes their tests according to the dependency graph.

The pipeline is now:

```text
                 ┌── stg_users ── tests
S3 bronze ───────┤
                 └── stg_orders ─ tests
```

### Lineage

dbt knows dependencies through `source()` and `ref()`.

List the models:

```bash
uv run dbt ls -q --resource-type model
```

List the complete graph leading to `stg_orders`:

```bash
uv run dbt ls -q --resource-type source model \
  --select +stg_orders
```

You should see the bronze source and the silver model.

The important distinction is:

```jinja
{{ source('bronze', 'orders') }}
```

means:

```text
external/raw dataset
```

while:

```jinja
{{ ref('stg_users') }}
```

means:

```text
another dbt resource
```

The dependency graph is therefore:

```text
source: bronze.users
        │
        ▼
   stg_users


source: bronze.orders
        │
        ▼
   stg_orders
```

### Documentation

`dbt docs generate` collects the descriptions of the YAML files and the schemas of the relations in `target/`. With
`--static`, the documentation website is written into a single HTML file:

```bash
uv run dbt docs generate --static
ls target/static_index.html
```

Download `target/static_index.html` from the VSCode explorer (right-click, "Download") and open it in your browser.
Browse the models, their columns and tests, and click the lineage button in the bottom right corner to display the
graph of the project.

## Export the Silver Layer to Parquet

So far, the silver models are materialized as DuckDB views. You can also export the result of a silver transformation as a Parquet file in the S3 bucket.

### Create a Parquet export

After building the `stg_users` model, open DuckDB:

```bash
uv run duckdb lab_medallion.duckdb
```

Then export the model to S3:

```bash
uv run duckdb lab_medallion.duckdb -c "
COPY (
    SELECT *
    FROM main_silver.stg_users
)
TO 's3://$LAB_BUCKET_NAME/silver/dbt/users.parquet'
(FORMAT parquet);
"
```

Exit DuckDB:

```sql
.exit
```

You can verify that the file exists:

```bash
aws s3 ls s3://$LAB_BUCKET_NAME/silver/dbt/
```

You should see:

```text
silver/dbt/users.parquet
```

### Query the Parquet file directly

DuckDB can query the Parquet file without importing it into the database:

```bash
uv run duckdb lab_medallion.duckdb -c "
SELECT *
FROM read_parquet('s3://$LAB_BUCKET_NAME/silver/dbt/users.parquet')
LIMIT 10;
"
```

You can also run an aggregation directly on the file:

```bash
uv run duckdb lab_medallion.duckdb -c "
SELECT sex, count(*) AS users
FROM read_parquet('s3://$LAB_BUCKET_NAME/silver/dbt/users.parquet')
GROUP BY sex;
"
```

### Compare the two approaches

The same silver data can now be accessed in two ways:

```text
S3 Bronze CSV
      │
      ▼
  dbt / DuckDB
      │
      ├──► DuckDB view
      │
      └──► S3 Silver Parquet
```

A **view** stores the SQL query. The transformation is executed when the view is queried.

A **Parquet file** stores the transformed data. The transformation is performed once during the export, and subsequent queries read the materialized Parquet data.

This illustrates the difference between **logical materialization** (view) and **physical materialization** (Parquet).

### Exercise

Export `stg_orders` as:

```text
s3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet
```

Then query the Parquet file with DuckDB and compare its result with:

```sql
SELECT *
FROM main_silver.stg_orders;
```

## Commit

```bash
cd /home/onyxia/work/$GIT_REPO_NAME
git add .gitignore pyproject.toml uv.lock lab_medallion
git status --short
git commit -m "feat: medallion pipeline with dbt"
git push
```

Check that the `target/`, `logs/` directories and the `lab_medallion.duckdb` file are not staged.

## Exercises

### 1. Add a cleaned column

Add a `username_normalized` column to `stg_users`.

It should:

- remove leading/trailing whitespace
- convert the username to lowercase

Document the column and add an appropriate test.

### 2. Improve order validation

Add tests for `stg_orders` to verify:

- `quantity` is positive
- `product` belongs to the expected list
- every `user_id` exists in `stg_users`

Use generic tests where possible and a singular test where necessary.

### 3. Clean invalid birthdates

Modify `stg_users` so that a birthdate later than the user's first order is replaced with `NULL`.

Add:

```text
birthdate_is_valid
```

as a boolean column.

The column should indicate whether the original birthdate passed the validation.

Then modify:

```text
assert_orders_after_user_birth
```

so that the test passes.

### 4. Detect invalid ZIP codes

The generated addresses associate a state with a ZIP code.

Create a data-quality test that detects inconsistent state/ZIP combinations.

You will need reference data containing valid ZIP-code ranges or mappings.

Consider:

- Where should this reference data come from?
- Should it be stored as a dbt seed?
- Should the validation happen in the silver layer?
- What happens when the reference data changes?

### 5. Materialization experiment

The silver models currently use views.

Change the configuration temporarily:

```yaml
silver:
  +materialized: table
```

Run:

```bash
uv run dbt build --select silver
```

Compare the two approaches.

Consider:

- Where is the data stored?
- When is the transformation executed?
- What happens when the underlying CSV changes?
- Which approach requires more storage?
- Which approach avoids re-reading the source files for every query?

Restore the original configuration when finished.

## Cleanup

Remove the objects of the gold layer. The objects of the bronze layer are kept for the next modules.

```bash
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/gold/" --recursive
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
#> 2026-09-14 11:02:10     331568 bronze/orders.csv
#> 2026-09-14 11:02:09       7351 bronze/users.csv
```

The `lab_medallion.duckdb` database file can be deleted as well: `dbt build` recreates all the models from the bronze
layer and the seeds.

---

_The content of this document, including all text, images, and associated materials, is the exclusive property of Adaltas and is protected by applicable copyright laws. Unauthorized distribution, reproduction, or sharing of this content, in whole or in part, is strictly prohibited without the express written consent of the author(s). Any violation of this restriction may result in legal action and the imposition of penalties as prescribed by law._
