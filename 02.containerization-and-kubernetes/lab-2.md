# Lab

Explore Kubernetes ConfigMap & Secret for storage of configuration and credentials.

## Objectives

By the end of this lab, you will be able to:

- Understand the difference between ConfigMap and Secret
- Create ConfigMaps and Secrets using `kubectl` and YAML manifests
- Inject ConfigMaps and Secrets into pod environment variables and volumes
- Store real-world credentials (S3 access keys, Ceph credentials, database passwords)
- Apply security best practices for sensitive data

## Prerequisites

- A running Kubernetes cluster (minikube)
- Familiarity with basic `kubectl` commands (from lab-1)

## What is a ConfigMap?

A ConfigMap stores non-sensitive configuration data as key-value pairs or entire files.

- Use ConfigMap for:

  - Application settings (database host, log level, timeouts)
  - Feature flags
  - Non-secret configuration files (application.yaml, app.properties)
  - Data pipeline parameters (batch size, retention days)

- Do NOT use ConfigMap for:

  - Passwords, API keys, tokens
  - Private credentials of any kind
  - Anything you'd be uncomfortable seeing in plain text in git

## What is a Secret?

A Secret stores sensitive data (encoded in base64, optionally encrypted at rest).

- Use Secret for:

  - Passwords and access keys
  - OAuth tokens, API keys
  - SSH keys
  - Database credentials
  - Cloud provider credentials (AWS, Azure, Ceph S3 access)

Important: Base64 encoding is NOT encryption. Secrets are base64-encoded by default, but:

- Anyone with cluster access can read them
- For production, enable encryption at rest
- Never commit secrets to git

## 1. Setup

Create lab namespace and directory.

```bash
kubectl create namespace lab-configmap-secret
mkdir lab-configmap-secret && cd lab-configmap-secret
```

## 2. Creating ConfigMaps

### Method 1

Create ConfigMap from a file.

```bash
# Create a config file locally
cat > pipeline-config.properties << 'EOF'
# Data Pipeline Configuration
bronze.path=/mnt/data/bronze
silver.path=/mnt/data/silver
gold.path=/mnt/data/gold
batch.size=1000
retention.days=90
log.level=INFO
spark.cores=4
kafka.brokers=kafka-0.kafka.svc.cluster.local:9092
EOF

# Create ConfigMap from the file
kubectl -n lab-configmap-secret create configmap pipeline-config --from-file=pipeline-config.properties

# View the ConfigMap
kubectl -n lab-configmap-secret get configmap pipeline-config
kubectl -n lab-configmap-secret describe configmap pipeline-config
kubectl -n lab-configmap-secret get configmap pipeline-config -o yaml
```

### Method 2

Create ConfigMap from literal key-value pairs.

```bash
# Quick creation with direct values
kubectl -n lab-configmap-secret \
  create configmap data-paths \
  --from-literal=bronze=/mnt/data/bronze \
  --from-literal=silver=/mnt/data/silver \
  --from-literal=gold=/mnt/data/gold

# View it
kubectl -n lab-configmap-secret get configmap data-paths -o yaml
```

### Method 3

Create ConfigMap from YAML manifest. This is the preferred method for version control and GitOps.

```bash
# Create the YAML manifest
cat > configmap-data-platform.yaml << 'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: data-platform-config
  namespace: lab-configmap-secret
data:
  # Simple key-value pairs
  log_level: INFO
  spark_workers: "4"
  batch_size: "1000"

  # Multi-line config (entire file as a value)
  application.yaml: |
    # Data Platform Application Config
    pipeline:
      name: bronze-to-silver-etl
      schedule: "0 2 * * *"
      timeout_minutes: 120
    storage:
      type: s3
      endpoint: ceph-rgw.ceph-storage.svc.cluster.local:7480
      region: us-east-1
    kafka:
      bootstrap_servers: kafka-0.kafka.svc.cluster.local:9092
      consumer_group: data-platform-consumers
      topic_prefix: data.bronze

  # CSV mapping example
  table_mappings.csv: |
    source_table,bronze_path,silver_layer
    customers,/data/bronze/customers,deduplicated_customers
    orders,/data/bronze/orders,aggregated_orders
    products,/data/bronze/products,product_catalog
EOF

# Apply the ConfigMap
kubectl apply -f configmap-data-platform.yaml
```

### Inspect ConfigMap content

```bash
# View the entire ConfigMap
kubectl -n lab-configmap-secret get configmap data-platform-config -o yaml

# View specific key
kubectl -n lab-configmap-secret get configmap data-platform-config -o jsonpath='{.data.log_level}'

# View a file inside ConfigMap
kubectl -n lab-configmap-secret get configmap data-platform-config -o jsonpath='{.data.application\.yaml}'
```

## 3. Creating Secrets

### Method 1

Create Secret from literal values.

```bash
# Scenario: Store Ceph S3 credentials
kubectl -n lab-configmap-secret \
  create secret generic ceph-credentials \
  --from-literal=access-key=AKIAIOSFODNN7EXAMPLE \
  --from-literal=secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# View it (note: data is encoded)
kubectl -n lab-configmap-secret get secret ceph-credentials -o yaml
```

### Method 2

Create Secret from a file.

```bash
# Create a credentials file locally (simulating a kubeconfig or cert)
cat > s3-credentials.txt << 'EOF'
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
EOF

# Create Secret from file
kubectl -n lab-configmap-secret \
  create secret generic s3-credentials \
  --from-file=s3-credentials.txt

# View it
kubectl -n lab-configmap-secret get secret s3-credentials -o yaml
```

### Method 3

Create Secret from YAML (best practice for GitOps)

**⚠️ WARNING:** In a real project, encode secrets or use a tool like [Sealed Secrets](https://github.com/bitnami/sealed-secrets). Never commit unencrypted secrets to git!

```bash
# For this lab, we'll create it manually, but in production use:
# - Sealed Secrets (encrypts before committing)
# - External Secrets Operator (fetches from vault)
# - HashiCorp Vault
# - Cloud provider secret managers (Azure Key Vault, AWS Secrets Manager, GCP Secret Manager)

cat > secret-ceph-credentials.yaml << 'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: ceph-s3-credentials
  namespace: lab-configmap-secret
type: Opaque
stringData:  # Use stringData for readable YAML (automatically base64-encoded when applied)
  access-key: AKIAIOSFODNN7EXAMPLE
  secret-key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  endpoint: ceph-rgw.ceph-storage.svc.cluster.local:7480
  bucket: data-platform-bronze
EOF

# Apply the Secret
kubectl apply -f secret-ceph-credentials.yaml
```

### Decode Secret values

```bash
# Get the entire secret as YAML
kubectl -n lab-configmap-secret get secret ceph-s3-credentials -o yaml

# Manually decode one field
kubectl -n lab-configmap-secret get secret ceph-s3-credentials -o jsonpath='{.data.secret-key}' | base64 -d

# Decode all fields (shell script)
kubectl -n lab-configmap-secret \
  get secret ceph-s3-credentials -o json | \
  jq -r '.data | to_entries[] | "\(.key)=\(.value | @base64d)"'
```

## 4. Using ConfigMap & Secret in Pods

### Method 1

Inject as Environment Variables.

From ConfigMap

```bash
cat > pod-with-configmap.yaml << 'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: spark-app
  namespace: lab-configmap-secret
spec:
  containers:
  - name: spark
    image: apache/spark:latest
    command: ["sleep", "600"]
    env:
    # #1 ConfigMap key as env var
    - name: LOG_LEVEL
      valueFrom:
        configMapKeyRef:
          name: data-platform-config
          key: log_level

    # #2 ConfigMap key
    - name: SPARK_WORKERS
      valueFrom:
        configMapKeyRef:
          name: data-platform-config
          key: spark_workers

    # Inject all ConfigMap keys as env vars
    envFrom:
    - configMapRef:
        name: data-platform-config
EOF

kubectl apply -f pod-with-configmap.yaml

# Verify env vars inside pod
kubectl -n lab-configmap-secret exec -it spark-app -- env | grep -E "LOG_LEVEL|SPARK"
```

From Secret:

```bash
cat > pod-with-secret.yaml << 'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: ceph-connector
  namespace: lab-configmap-secret
spec:
  containers:
  - name: ceph-app
    image: python:3.9
    command: ["python", "-c", "import os; print(f'Access Key: {os.environ[\"S3_ACCESS_KEY\"]}')"]
    env:
    # Single Secret key as env var
    - name: S3_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: ceph-s3-credentials
          key: access-key

    - name: S3_SECRET_KEY
      valueFrom:
        secretKeyRef:
          name: ceph-s3-credentials
          key: secret-key

    - name: S3_ENDPOINT
      valueFrom:
        secretKeyRef:
          name: ceph-s3-credentials
          key: endpoint

    # Inject all Secret keys as env vars
    envFrom:
    - secretRef:
        name: ceph-s3-credentials
EOF

kubectl apply -f pod-with-secret.yaml

# Wait sometimes and view pod logs to see the access key (be careful in production!)
kubectl -n lab-configmap-secret logs ceph-connector
```

### Method 2

Mount as Volumes (Files)

From ConfigMap:

```bash
cat > pod-with-configmap-volume.yaml << 'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: spark-config-reader
  namespace: lab-configmap-secret
spec:
  containers:
  - name: spark
    image: apache/spark:latest
    command: ["sleep", "600"]
    volumeMounts:
    # Mount entire ConfigMap as a directory
    - name: config-volume
      mountPath: /etc/config
  volumes:
  - name: config-volume
    configMap:
      name: data-platform-config
      # Optional: mount specific keys
      items:
      - key: application.yaml
        path: application.yaml  # File name inside /etc/config
      - key: table_mappings.csv
        path: tables.csv
EOF

kubectl apply -f pod-with-configmap-volume.yaml

# Wait and exec into pod and view files
kubectl -n lab-configmap-secret exec -it spark-config-reader -- ls -la /etc/config/
kubectl -n lab-configmap-secret exec -it spark-config-reader -- cat /etc/config/application.yaml
kubectl -n lab-configmap-secret exec -it spark-config-reader -- cat /etc/config/tables.csv
```

From Secret:

```bash
cat > pod-with-secret-volume.yaml << 'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: ceph-connector-secure
  namespace: lab-configmap-secret
spec:
  containers:
  - name: ceph-app
    image: python:3.9
    command: ["sh", "-c", "cat /etc/ceph-credentials/s3-credentials.txt && sleep 1000"]
    volumeMounts:
    # Mount Secret as a volume (files)
    - name: ceph-creds
      mountPath: /etc/ceph-credentials
      readOnly: true  # Good practice: mount as read-only
  volumes:
  - name: ceph-creds
    secret:
      secretName: s3-credentials
      defaultMode: 0600  # File permissions (read/write for owner only)
EOF

kubectl apply -f pod-with-secret-volume.yaml

# Wait and view the mounted credentials
kubectl -n lab-configmap-secret exec -it ceph-connector-secure -- cat /etc/ceph-credentials/s3-credentials.txt
```

## Teardown

Remove every workloads deployed in this lab.

```bash
kubectl delete namespace lab-configmap-secret
cd .. && rm -r lab-configmap-secret
```
