# Big Data Framework Syllabus - ECE 2026 fall

## Introduction

This course introduces modern cloud-native big data architecture built entirely on Kubernetes. Students will gain end-to-end practical experience with distributed system, cloud-native data storage and processing mechanism, real-time streaming ingestion, medallion data lakehouse architecture, ETL/ELT data pipeline, and security.

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

### Module 4: Medallion Architecture & ETL/ELT Dataflow (dbt)

Course:

- The Medallion Data Architecture (Bronze, Silver, Gold layers)
- ELT vs. ETL paradigms
- Analytics Engineering concepts

Lab:

- Initialize a dbt (Data Build Tool) project
- Write custom transformation models
- Build aggregation logic
- Implement data quality tests

### Module 5: Distributed Computation Intro (Spark & Spark Operator)

Course:

- Apache Spark core architecture (Driver vs. Executors)
- Kubernetes Custom Resource Definitions (CRDs)

Lab:

- Submit a Kubeflow Spark Operator SparkApplication manifest
- Manage executor lifecycles
- Configure automated Spark CronJobs

### Module 6: Event Streaming (Strimzi Kafka Operator & Debezium)

Course:

- Event-driven architectures
- Kafka broker mechanics and Topic partitioning
- Change Data Capture (CDC) via write-ahead log parsing

Lab:

- Deploy a Kafka cluster via Strimzi CRDs
- Provision a PostgreSQL connector via Debezium CDC
- Produce and consume events
- Verify stream offsets

### Module 7: Real-Time Stream Processing with Structured Streaming

Course:

- Unbounded DataFrame abstractions
- Micro-batching vs. continuous processing
- Event-time semantics, watermarking, and fault recovery

Lab:

- Build and deploy a Spark Structured Streaming application
- Consume live events from Kafka topics
- Apply stateful transformations
- Output results to a sink

### Module 8: Data Orchestration Fundamentals (Apache NiFi/Hop)

Course:

- Flow-based programming concepts
- Data provenance
- Backpressure handling and connection routing strategies

Lab:

- Navigate the Apache NiFi/Hop web UI
- Configure source and sink processors
- Construct a basic ingestion pipeline

### Module 9: Advanced Data Orchestration & Pipeline Integration

Course:

- Enterprise ETL/ELT design patterns
- Multi-stage routing and automated error recovery
- Task scheduling and queue capacity monitoring

Lab:

- Construct an advanced NiFi/Hop workflow integrating Spark compute and Kafka streams
- Feature automated retry paths and routing logic
- Implement alert triggers

### Module 10: Lakehouse Architecture Foundations & Query Engines

Course:

- Introduction to the open lakehouse architecture
- Apache Polaris REST Catalog specification
- Federated querying over object stores with Trino

Lab:

- Interact with Apache Polaris via REST APIs
- Manage catalog entities (namespaces, tables)
- Execute basic SQL queries in Trino

### Module 11: Advanced Lakehouse Integration (Spark, Polaris, & Trino)

Course:

- Apache Iceberg table format mechanics (ACID transactions, snapshot isolation, time travel)
- Cross-engine catalog integration

Lab:

- Write a PySpark job to ingest data into Iceberg tables via the Polaris REST catalog
- Run concurrent analytical queries via Spark SQL
- Execute queries via Trino

### Module 12: Cloud Security

Course:

- Security pillars: identification, authentication, authorization
- Role-Based Access Control (RBAC) and Identity & Access Management (IAM)
- OAuth2/OIDC standards using Keycloak
- Lightweight LDAP authentication using GLAuth
- OPAL and Kafka

Lab:

- ServiceAccounts & token management
- RBAC (Roles, RoleBindings, ClusterRoles)
- NetworkPolicies and pod-to-pod isolation
- Secrets management

### Module 13: Cloud Migration

Course:

- on-prem vs. cloud
- Cloud service models
- Migration strategies (7 Rs)
