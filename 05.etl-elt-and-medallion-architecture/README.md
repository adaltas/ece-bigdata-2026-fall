---
duration: 1 hour
---

# ETL/ELT & the Medallion Architecture

Before data can be analyzed, reported on, or used to train models, it must move from its source systems into a
centralized store (data warehouse or data lake). How that movement happens, and where transformation occurs, define the
two dominant paradigms: ETL and ELT.

Moving data is only half of the problem. Once in the platform, the data must be organized so that consumers know which
datasets are raw, which ones are trustworthy, and which ones answer business questions. The medallion architecture is
the most common answer in data lakes and lakehouses, and analytics engineering, with tools such as dbt, provides the
practices to build and maintain it.

## Data pipelines

A data pipeline is a sequence of steps moving data from one or more sources to one or more destinations, transforming it
along the way. It typically covers:

- **Sources**  
  Operational databases (PostgreSQL, MySQL), SaaS applications exposed through APIs (CRM, payment, marketing), files
  dropped by partners, application logs, events from devices and message queues.
- **Ingestion**  
  Extracting the data from the sources and landing it in the platform.
- **Storage**  
  Object storage, a data warehouse or a lakehouse.
- **Transformation**  
  Cleaning, integrating and modeling the data.
- **Serving**  
  Exposing the result to dashboards, notebooks, machine learning models, APIs, or back to operational applications.
- **Orchestration and monitoring**  
  Scheduling the steps, managing their dependencies, retrying failures, alerting.

Pipelines run in two modes:

- **Batch**: data is processed in bounded chunks, on a schedule (every hour, every night) or when a file arrives. Simple
  to reason about, reprocess and test. Latency ranges from minutes to a day.
- **Streaming**: data is processed continuously, record by record or in micro-batches, as events happen. Latency ranges
  from milliseconds to seconds, at the cost of more complex operations (state, ordering, late events).

This module focuses on batch pipelines, which remain the majority of analytics workloads.

## Extract, Transform, Load

Both ETL and ELT are built from the same three steps. They differ in where and when transformation happens relative to
loading.

![](./assets/etl-vs-elt.png)

### Extract

Extraction pulls the data from its source, with as little impact as possible on the source system.

- **Full extraction**  
  The complete dataset is read on every run. Simple and always consistent, but the cost grows with the volume. Suited to
  small reference tables (products, countries).
- **Incremental extraction**  
  Only the records created or modified since the previous run are read, using a high watermark: a monotonic column such
  as an `updated_at` timestamp or an auto-incremented identifier. The value of the last run is stored and used as a
  filter on the next run. Deleted records are not detected, and a record updated without changing the watermark column
  is missed.
- **Change Data Capture (CDC)**  
  The changes (inserts, updates, deletes) are read from the transaction log of the database, for example with Debezium.
  It captures every change with a low impact on the source.

Extraction must deal with the constraints of the sources: API rate limits and pagination, credentials rotation, time
zones, and schema drift, when a source adds, removes or renames a column without notice.

### Transform

Transformation reshapes the data into a form fit for consumption:

- **Cleaning**: remove corrupted or invalid records, handle missing values, trim strings
- **Standardization**: consolidate date formats, units, currencies, codes (`M`/`F` vs. `male`/`female`), time zones
- **Typing**: cast text into integers, decimals, dates and timestamps
- **Deduplication**: keep a single version of each entity
- **Integration**: join data across sources, reconcile identifiers of the same entity
- **Enrichment**: add derived attributes (age from a birthdate, geographic region from a zip code)
- **Business rules**: compute metrics such as revenue, active customers, churn
- **Aggregation and modeling**: build fact and dimension tables, summaries per day, product, customer
- **Privacy**: mask, hash, or remove Personally Identifiable Information (PII)

### Load

Loading writes the result into the target system. Several strategies exist:

| Strategy          | Description                                                 | Typical use                      |
| ----------------- | ----------------------------------------------------------- | -------------------------------- |
| Full overwrite    | Replace the whole table                                     | Small tables, full recomputation |
| Append            | Insert new records, keep the existing ones                  | Immutable events, logs, orders   |
| Upsert (merge)    | Insert new records, update the existing ones matched by key | Entities which change: users     |
| Partition replace | Overwrite only the partitions touched by the run            | Daily batches, backfills         |

Appending without care creates duplicates when a run is retried. Upserts and partition replacements are preferred
because they are idempotent.

## ETL and ELT

### ETL (Extract, Transform, Load)

Transformation happens before loading, typically in a dedicated staging/processing layer, using a defined schema the
target expects. ETL dominated from the 1990s: storage and compute in a data warehouse were expensive, so only clean,
integrated and useful data was loaded. Dedicated tools such as Informatica PowerCenter, IBM DataStage, Talend or Apache
Hop run the transformations on their own servers.

Benefits:

- Data lands in the warehouse already clean, validated, and query-ready
- Enforces schema-on-write: consumers get consistent, trustworthy structure
- Well-suited to regulated environments (finance, healthcare) where PII must be filtered/masked before reaching storage
- Reduces the storage and compute consumed in the target system
- Mature, well-understood tooling and governance patterns

Issues:

- Significant upfront engineering effort to define transformation logic before you know all downstream use cases
- Wasted work if the data is never exploited
- Poor fit for unstructured or semi-structured data (logs, JSON blobs, images) since the schema must be fixed in advance
- Raw/original data is often discarded after transformation, making it hard to reprocess if requirements change
- The ETL server becomes a bottleneck when the volume grows, and the logic is often locked in a proprietary graphical
  tool, hard to version and test

### ELT (Extract, Load, Transform)

Raw data is loaded into the target first (schema-on-read), and transformation happens afterward, inside the target
system itself.

ELT rose in the 2010s with the data lake and the cloud data warehouses. Object storage made storing everything cheap,
the separation of storage and compute made processing power elastic, and massively parallel engines (BigQuery,
Snowflake, Spark, Trino) made SQL fast on large volumes. It became cheaper to load first and decide later.

Benefits:

- Raw data is preserved as-is: simplified archiving, easy to reprocess or backfill if transformation logic changes
- Faster time-to-load: you don't have to fully define transformations upfront
- Better fit for unstructured/semi-structured/high-volume data, since structure can be imposed later, only for the parts
  you actually need
- Enables a medallion architecture (Bronze → raw, Silver → cleaned/conformed, Gold → business-level aggregates), letting
  multiple transformation stages coexist

Issues:

- Raw, unvalidated data sits in the warehouse — governance and access control must compensate (who can query Bronze vs.
  Gold?)
- Transformation logic is decentralized (SQL/dbt models, notebooks), which can lead to inconsistent business logic
  across teams if not governed
- Requires a target system powerful/cheap enough to do transformation at scale (this is why ELT rose with cloud data
  warehouses, not before)
- Storing everything has a cost, and sensitive data stored raw must still comply with regulations such as GDPR (right to
  erasure, retention limits)

### Comparison

| Characteristic | ETL                                   | ELT                                       |
| -------------- | ------------------------------------- | ----------------------------------------- |
| Transformation | Before loading, in a dedicated engine | After loading, inside the target          |
| Schema         | On write                              | On read, then enforced in later layers    |
| Raw data       | Usually discarded                     | Preserved                                 |
| Data types     | Structured                            | Structured, semi-structured, unstructured |
| Time to load   | Slower, logic defined upfront         | Fast                                      |
| Reprocessing   | Re-extract from sources               | Replay from raw data                      |
| Compute        | ETL server                            | Warehouse, lakehouse, Spark, DuckDB       |
| Typical tools  | Informatica, DataStage, Talend, Hop   | dbt, Spark SQL, SQLMesh                   |

### Hybrid patterns

In practice, the boundary is blurry:

- **EtLT**: a light transformation ("t") is applied during ingestion, such as dropping PII columns, flattening JSON or
  converting to Parquet, followed by the business transformations (the "T") in the platform.
- **Reverse ETL**: modeled data is pushed back from the warehouse into operational tools, such as customer segments sent
  to a CRM or a marketing platform.
- **Zero ETL**: managed services replicate an operational database into an analytical store without a user-built
  pipeline, for example between Amazon Aurora and Redshift.

### Tooling landscape

| Category        | Role                                  | Examples                                             |
| --------------- | ------------------------------------- | ---------------------------------------------------- |
| Ingestion       | Extract and load from many connectors | Airbyte, Fivetran, dlt, Apache NiFi, Debezium, Kafka |
| Transformation  | Transform inside the platform         | dbt, SQLMesh, Spark, DuckDB                          |
| Orchestration   | Schedule and chain the steps          | Apache Airflow, Dagster, Argo Workflows, Kestra      |
| Quality         | Validate the data                     | dbt tests, Great Expectations, Soda                  |
| Catalog/lineage | Document and trace the datasets       | OpenMetadata, DataHub, Apache Polaris                |

## The Medallion Architecture

A layered design pattern for organizing a lake/lakehouse by progressive data quality, commonly used with ELT. The term
was popularized by Databricks, but the idea is older: data warehouses already distinguished staging areas, integration
layers and data marts.

Data flows from left to right, and each layer increases the structure, quality and business value of the data:

```text
 Sources ──► Bronze ──────────► Silver ──────────────► Gold ──────────────► Consumers
             raw, as-is         cleaned, conformed     business-ready       BI, ML, APIs
```

### Bronze

The Bronze layer contains the raw data, as extracted from the sources.

- Full fidelity: records are kept as received, including duplicates and errors
- Original format (CSV, JSON, Avro) or a lossless conversion to Parquet
- Append-only, with ingestion metadata: load timestamp, source file, batch identifier
- Long retention: it is the history which allows reprocessing when a bug is fixed or a new use case appears
- Access restricted to data engineers

Bronze is the "single source of truth" of the platform.
Keeping the Bronze layer untouched decouples ingestion from transformation: a source can be ingested before anybody
knows how it will be used, and the extraction does not have to be replayed when the transformation changes.

### Silver

The Silver layer contains cleaned and conformed data, at the grain of the source entities.

- Schema enforced: columns renamed with consistent conventions and cast to the correct types
- Invalid records filtered or quarantined, missing values handled
- Deduplicated: one row per entity or per event
- Standardized values: dates, units, codes, time zones
- Integrated: entities from several sources are joined and share common identifiers
- PII handled here: hashing, masking, access restriction
- Stored in a columnar, transactional format (Parquet, Iceberg, Delta Lake)

Data scientists and analysts explore the data created in silver layer, and all Gold datasets
are built from it.

### Gold

The Gold layer contains business-level datasets, designed for consumption.

- Organized by use case or business domain: sales, marketing, finance
- Modeled for analytics: dimensional models (facts and dimensions), wide denormalized tables, aggregates
- Business rules and KPIs computed once, in a single place: revenue, active customers, basket size
- Optimized for reading: materialized as tables, partitioned, sorted
- Consumed by BI tools, dashboards, reports, ML feature tables and APIs

### Summary

| Characteristic  | Bronze                 | Silver                      | Gold                           |
| --------------- | ---------------------- | --------------------------- | ------------------------------ |
| Content         | Raw data               | Cleaned, conformed entities | Business aggregates and models |
| Schema          | Source schema, or none | Enforced, typed             | Modeled for a use case         |
| Quality         | Unvalidated            | Validated, deduplicated     | Tested business rules          |
| Grain           | As ingested            | One row per entity or event | Aggregated or dimensional      |
| Load pattern    | Append                 | Upsert, incremental         | Overwrite, incremental         |
| Users           | Data engineers         | Data engineers, scientists  | Analysts, business users, apps |
| Materialization | Files                  | Tables or views             | Tables                         |

### Example

The datasets generated in the previous labs follow this path:

- **Bronze**: `bronze/users.csv` and `bronze/orders.csv` in the S3 bucket, as produced by the generator. The `address`
  column of the users is a multiline text, every column of the CSV files is text before type detection.
- **Silver**: `stg_users` casts `uuid` to a UUID and `birthdate` to a date, renames `mail` to `email`, and splits the
  address into `street`, `state` and `zip_code`. `stg_orders` casts `date` to a timestamp and `quantity` to an integer,
  and removes orders referencing unknown users.
- **Gold**: `fct_orders` joins orders with users, and `sales_by_state_month` aggregates the quantities sold per state,
  product and month for a sales dashboard.

### Variations and limits

The three layers are a convention, not a standard:

- Some platforms add a **landing** zone before Bronze, where files are dropped before being validated and registered.
- Silver is often split into several steps, for example staging (one model per source table) and intermediate (joins
  and business logic reused by several Gold models).
- Gold can hold a platform-wide dimensional model, then department-specific data marts, or a **semantic layer** defining
  metrics once for all BI tools.

The medallion names do not tell anything about the quality of what is inside. The value comes from explicit contracts:
what each layer guarantees, who owns it, who may read it, and how it is tested. Copying every dataset through three
layers without a purpose increases storage costs, latency and maintenance.

## Designing reliable transformations

### Idempotency

A transformation is idempotent when running it several times with the same input produces the same result. Pipelines
fail and are retried, and data is reprocessed after a bug fix: without idempotency, retries create duplicates or corrupt
aggregates.

- Prefer overwrites, merges on a key and partition replacements to blind appends
- Parametrize a run by its logical period (`2020-01-01`), never by the current time (`now()`)
- Make the transformations deterministic: a stable ordering when deduplicating, no random values

### Full refresh vs. incremental processing

- **Full refresh**: the target is rebuilt from the whole input on each run. Simple and always correct, but the cost
  grows with the history.
- **Incremental**: only the new or changed records are processed, then merged into the target. Faster and cheaper, but
  the logic must handle late-arriving data, updates and deletions, and an occasional full refresh is required to fix
  drift.

Start with full refreshes, and switch a model to incremental when its runtime or cost becomes a problem.

### Deduplication

Sources deliver duplicates: retried API calls, overlapping incremental extractions, CDC replays. A window function keeps
the most recent version of each entity:

```sql
SELECT *
FROM bronze_users
QUALIFY row_number() OVER (PARTITION BY uuid ORDER BY _loaded_at DESC) = 1;
```

### Backfills and history

A backfill reprocesses a past period, for example after a bug fix or when a new column is added. It is only possible
when the raw data has been kept in Bronze and the transformations are idempotent.

For dimensions which change over time, snapshots implement slowly changing dimensions of type 2: each change creates a
new row with `valid_from` and `valid_to` columns, preserving the history of the attributes.

## Data quality

Moving data between layers is only useful if its quality improves. Quality is measured along several dimensions:

- **Completeness**: mandatory values are present, no record is lost between layers
- **Uniqueness**: no duplicated entity or event
- **Validity**: values respect the expected type, format and range (a quantity between 1 and 5, an email with an `@`)
- **Consistency**: references between datasets are respected, every order references an existing user
- **Accuracy**: values reflect reality, totals match the source system
- **Timeliness**: data is available and fresh when consumers need it

Quality checks are automated and executed as part of the pipeline:

- **Tests** on the models: not null, unique, accepted values, relationships, custom SQL assertions
- **Row count and freshness checks** between layers and against the sources
- **Quarantine**: invalid records are written aside with the reason of the rejection, instead of failing the whole run
  or silently disappearing
- **Data contracts**: producers and consumers agree on the schema, semantics and quality guarantees of a dataset, and
  breaking changes are detected before they reach production

A failing test on Silver should block the build of the Gold models depending on it: publishing a wrong KPI is often
worse than publishing a late one.

## Analytics engineering

Analytics engineering applies software engineering practices to analytics code. The analytics engineer sits between the
data engineer, who builds the ingestion and the platform, and the data analyst, who answers business questions. Their
role is to turn raw data into clean, tested and documented datasets.

The practices are those of software development:

- Transformations are code, stored in Git and reviewed through merge requests
- Each dataset is defined once, as a modular query referencing other datasets
- Tests and documentation live next to the code
- Separate environments (development, staging, production) and continuous integration
- Dependencies between datasets are explicit, providing lineage

## dbt

[dbt](https://www.getdbt.com/) (data build tool) is the reference tool of analytics engineering. It handles the "T" of
ELT: it does not extract nor load data, and does not process data itself. It compiles SQL models and runs them in the
target platform: DuckDB, PostgreSQL, Snowflake, BigQuery, Databricks, Spark, Trino. Each platform is supported by an
adapter, such as `dbt-duckdb` used in the lab.

dbt Core is an open-source command line tool, and dbt Labs provides a managed cloud offering.

### Models

A model is a `SELECT` statement stored in a `.sql` file under `models/`. dbt wraps it in the DDL required to create a
view or a table named after the file. Models reference each other with the Jinja functions `ref()` and `source()`:

```sql
-- models/staging/stg_orders.sql
SELECT
  cast(uuid AS uuid) AS order_id,
  cast(user_uuid AS uuid) AS user_id,
  cast(date AS timestamp) AS ordered_at,
  cast(quantity AS integer) AS quantity,
  product
FROM {{ source('bronze', 'raw_orders') }}
```

```sql
-- models/marts/fct_orders.sql
SELECT o.order_id, o.ordered_at, o.product, o.quantity, u.state
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_users') }} u ON o.user_id = u.user_id
```

- `source()` references a raw table declared in a YAML file, outside of dbt
- `ref()` references another model

From these references, dbt builds a directed acyclic graph (DAG) of the models. It runs them in the right order, in
parallel when possible, and resolves the schema and name of each relation according to the target environment.

### Materializations

The materialization defines how a model is persisted in the platform:

| Materialization | Behavior                                                      | Use                         |
| --------------- | ------------------------------------------------------------- | --------------------------- |
| `view`          | Creates a view, the query runs when the view is read          | Light Silver models         |
| `table`         | Rebuilds a table on every run                                 | Gold models read frequently |
| `incremental`   | Inserts or merges only the new records into an existing table | Large event tables          |
| `ephemeral`     | Not persisted, inlined as a CTE in the models referencing it  | Reusable intermediate logic |

Materializations are configured per folder in `dbt_project.yml`, or per model with a `config()` block. An incremental
model filters its input when the target table already exists:

```sql
{{ config(materialized='incremental', unique_key='order_id') }}

SELECT * FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
WHERE ordered_at > (SELECT max(ordered_at) FROM {{ this }})
{% endif %}
```

### Tests and documentation

Data tests are declared in YAML next to the models. Generic tests are provided for the common checks, and singular tests
are SQL queries returning the failing rows:

```yaml
models:
  - name: stg_orders
    description: One row per order, typed and cleaned.
    columns:
      - name: order_id
        data_tests:
          - unique
          - not_null
      - name: user_id
        data_tests:
          - relationships:
              to: ref('stg_users')
              field: user_id
      - name: product
        data_tests:
          - accepted_values:
              values: ["bread", "brioche", "cookie", "croissant", "donut", "drink"]
```

The descriptions written in YAML are compiled with the DAG into a documentation website, generated by
`dbt docs generate`, which displays the lineage of every model. Other features include seeds (small CSV reference files
loaded as tables), snapshots (slowly changing dimensions of type 2), macros (reusable Jinja functions) and packages
(shared macros and tests, such as `dbt_utils`).

### Project structure

A common layout maps the dbt folders onto the medallion layers:

```text
lab_medallion/
├── dbt_project.yml       # project configuration, materializations per folder
├── profiles.yml          # connection to the target platform (often in ~/.dbt/)
├── models/
│   ├── staging/          # Silver: one model per source table, cleaned and typed
│   │   ├── sources.yml   # Bronze tables declared as sources
│   │   ├── staging.yml   # tests and documentation
│   │   ├── stg_users.sql
│   │   └── stg_orders.sql
│   ├── intermediate/     # Silver: reusable joins and business logic
│   └── marts/            # Gold: facts, dimensions and aggregates
├── seeds/
├── snapshots/
├── macros/
└── tests/                # singular tests
```

### Commands

| Command             | Description                                            |
| ------------------- | ------------------------------------------------------ |
| `dbt init`          | Create a new project                                   |
| `dbt debug`         | Validate the configuration and the connection          |
| `dbt run`           | Build the models                                       |
| `dbt test`          | Run the data tests                                     |
| `dbt build`         | Run seeds, snapshots, models and tests in DAG order    |
| `dbt compile`       | Render the Jinja into plain SQL, in `target/compiled/` |
| `dbt docs generate` | Generate the documentation and the lineage graph       |

The `--select` option restricts the execution to a subset of the DAG: `--select stg_users` builds a single model,
`--select staging` a folder, `--select +fct_orders` a model and all its parents, `--select stg_users+` a model and all
its children. `dbt build` stops the downstream models when a test fails, which prevents invalid data from reaching Gold.

## References

- [What is Medallion Architecture?](https://www.databricks.com/blog/what-is-medallion-architecture)
- [What’s the Difference Between ETL and ELT?](https://aws.amazon.com/compare/the-difference-between-etl-and-elt/)
- [What is analytics engineering?](https://www.getdbt.com/what-is-analytics-engineering)
- [dbt documentation](https://docs.getdbt.com/docs/introduction)
- [How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
- [dbt-duckdb adapter](https://github.com/duckdb/dbt-duckdb)
- [Functional Data Engineering, a modern paradigm for batch data processing](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a)
- Joe Reis and Matt Housley, _Fundamentals of Data Engineering_, O'Reilly, 2022

---

_The content of this document, including all text, images, and associated materials, is the exclusive property of
Adaltas and is protected by applicable copyright laws. Unauthorized distribution, reproduction, or sharing of this
content, in whole or in part, is strictly prohibited without the express written consent of the author(s). Any violation
of this restriction may result in legal action and the imposition of penalties as prescribed by law._
