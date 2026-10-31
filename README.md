# Big Data Framework Syllabus - ECE 2026 fall

## Introduction

This course introduces modern cloud-native big data architecture built entirely on Kubernetes. Students will gain end-to-end practical experience with distributed system, cloud-native data storage and processing mechanism, medallion data lakehouse architecture, ETL/ELT data pipeline, processing engine, and data governance.

## Modules

### Module 1: Big Data Fundamentals

Course:

- Distributed systems concepts
- Cloud-native paradigm shift
- Kubernetes control plane, Pods, Deployments, Services, and ConfigMaps

Lab:

- Set up a distributed local cluster using minikube
- Execute core kubectl commands
- Deploy a containerized test application
- Expose application via a Kubernetes Service

### Module 2: Containerization and Kubernetes

Course:

- Distributed systems concepts
- Cloud-native paradigm shift
- Kubernetes control plane, Pods, Deployments, Services, and ConfigMaps

Lab:

- Set up a distributed local cluster using minikube
- Execute core kubectl commands
- Deploy a containerized test application
- Expose application via a Kubernetes Service

### Module 3: Cloud Storage Essentials & Ceph Object Storage

Course:

- Block vs. File vs. Object storage paradigms
- Ceph architecture
- RADOS Gateway (RGW) and S3 API compatibility

Lab:

- Provision S3-compatible storage buckets on Ceph RGW
- Configure access keys
- Verify endpoint CRUD operations

### Module 4: SQL analytics

Course:

- Warehouse, lake and lakehouse architectures
- OLTP vs. OLAP, row vs. columnar storage
- Dimensional modeling and analytical SQL
- Parquet and DuckDB

Lab:

- Query S3 CSV datasets with DuckDB
- Write aggregations, joins, window functions and pivots
- Export to partitioned Parquet and compare with CSV
- Run DuckDB from Python

### Module 5: Medallion Architecture & ETL/ELT Dataflow (dbt)

Course:

- ETL vs. ELT
- Medallion architecture (Bronze, Silver, Gold)
- Idempotency, incremental processing and data quality
- Analytics engineering with dbt

Lab:

- Set up a dbt project on DuckDB
- Declare S3 CSV files as Bronze sources
- Build cleaned Silver models
- Add data tests and fix the failures
- Export Silver to Parquet

### Module 6: Spark introduction

Course:

- Spark architecture: driver, executors, cluster managers
- RDDs, DataFrames and Datasets
- Lazy evaluation, DAG and stages
- Narrow vs. wide transformations and shuffles

Lab:

- Build and partition RDDs from the bronze datasets
- Observe lazy evaluation in the Spark UI
- Compare `reduceByKey` and `groupByKey`
- Deduplicate and join key-value RDDs

### Module 7: Spark SQL & DataFrames

Course:

- RDD limits and DataFrame advantages
- Catalyst optimizer
- Schema management: inferred vs. explicit
- PySpark internals, UDFs and tuning

Lab:

- Read bronze CSVs from S3 with PySpark
- Build the silver layer with the DataFrame API and Spark SQL
- Write the silver layer as Parquet and as Iceberg tables
- Compare results with the dbt lab

### Module 8: Iceberg introduction

Course:

- Iceberg table format and metadata hierarchy
- Partitioning and hidden partitioning
- Schema evolution, time travel and row-level operations
- Table maintenance

Lab:

- Inspect an Iceberg table's files and metadata
- Append data and explore snapshots
- Query previous versions with time travel
- Evolve the schema
- Compare Iceberg with a plain Parquet directory

### Module 9: Apache Polaris

Course:

- Iceberg REST catalog and Polaris APIs
- Catalogs, namespaces, principals and roles
- RBAC and credential vending
- Data governance pillars

Lab:

- Connect Spark to Polaris through the REST API
- Register an existing Iceberg table in Polaris
- Create and alter tables as the `dev` principal
- Check read-only access as the `analyst` principal

### Module 10: Trino

Course:

- Trino architecture: coordinator, workers, connectors
- Trino vs. Spark
- Query execution and fault tolerance
- Trino on Iceberg through Polaris

Lab:

- Query the silver layer from Python through Polaris
- Inspect Iceberg metadata and query plans
- Build and validate gold tables with CTAS
- Model the gold layer as a star schema

### Module 11: Apache Hop

Course:

- Hop concepts: transforms, actions, hops
- Pipelines vs. workflows
- Projects, environments and variables
- Hop tools and essential transforms

Lab:

- Run Hop Web in Docker and create a project
- Build the silver users pipeline visually
- Route invalid rows to a quarantine file
- Run the provided silver orders pipeline

### Module 12: Apache Hop advanced

Course:

- Error handling and quarantine
- Workflows and orchestration
- Variables, parameters and run configurations
- Running and tuning Hop jobs

Lab:

- Build the gold table with a Hop pipeline
- Chain the pipelines in a workflow
- Quarantine orders from unknown users
- Run the workflow on S3 as a Kubernetes Job
- Check idempotency on rerun

### Module 13: Cloud Migration

Course:

- on-prem vs. cloud
- Cloud service models
- Migration strategies (7 Rs)
