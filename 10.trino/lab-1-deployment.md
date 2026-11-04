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
  - date: 2026-10-08
    comment: Initial draft, Polaris configuration to be adapted once the cluster is deployed
    author: mori@adaltas.com
  - date: 2026-11-03
    comment: Update
    author: elodine@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: deploy Trino using Helm chart (k8s)

This lab is a prerequisite for 2 Trino labs. It describes how to deploy Trino on the Adaltas Kubernetes platform.

## Prerequisites

- A Polaris service deployed with [previous module](../09.polaris)
- The `vscode-pyspark` Onyxia service with limits of `cpu: 2000m`, `memory 2000Mi`.

```bash
STATEFULSET_NAME="$(kubectl get statefulsets.apps -o=jsonpath='{.items[0].metadata.name}' -l app.kubernetes.io/name=vscode-pyspark)"
kubectl patch statefulset $STATEFULSET_NAME \
  -n $KUBERNETES_NAMESPACE \
  --type='strategic' \
  -p='{
    "spec": {
      "template": {
        "spec": {
          "containers": [{
            "name": "vscode-pyspark",
            "resources": {
              "requests": {
                "cpu": "200m",
                "memory": "400Mi"
              },
              "limits": {
                "cpu": "2000m"
                ,"memory": "2000Mi"
              }
            }
          }]
        }
      }
    }
  }'
kubectl rollout status statefulset/$STATEFULSET_NAME -n $KUBERNETES_NAMESPACE
```

- The required configuration files are created for Trino deployment.

```bash
mkdir -p trino/deployment
cat <<'YAML' >trino/deployment/Chart.yaml
apiVersion: v2
name: trino
version: 1.45.0
dependencies:
  - name: trino
    version: 1.45.0
    repository: https://trinodb.github.io/charts
YAML
cat <<'YAML' >trino/deployment/values.yaml
trino:
  image:
    tag: "480"

  server:
    workers: 0 # Coordinator-only; enables node-scheduler.include-coordinator.
    config:
      query:
        maxMemory: "400MB"

  coordinator:
    jvm:
      maxHeapSize: "700M"
    resources:
      requests:
        cpu: 250m
        memory: 224Mi
      limits:
        cpu: 1000m
        memory: 1Gi
    config:
      nodeScheduler:
        includeCoordinator: true
      query:
        maxMemoryPerNode: "250MB"

  envFrom:
    - secretRef:
        name: trino-polaris-oauth

  env:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: polaris-s3
          key: AWS_ACCESS_KEY_ID
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: polaris-s3
          key: AWS_SECRET_ACCESS_KEY
    - name: AWS_SESSION_TOKEN
      valueFrom:
        secretKeyRef:
          name: polaris-s3
          key: AWS_SESSION_TOKEN

    - name: POLARIS_CATALOG
      valueFrom:
        configMapKeyRef:
          name: polaris-config
          key: POLARIS_CATALOG
    - name: POLARIS_URI
      valueFrom:
        configMapKeyRef:
          name: polaris-config
          key: POLARIS_URI
    - name: S3_ENDPOINT
      valueFrom:
        configMapKeyRef:
          name: polaris-config
          key: S3_ENDPOINT

  additionalCatalogs:
    lakehouse: |-
      connector.name=iceberg
      iceberg.catalog.type=rest
      iceberg.rest-catalog.uri=${ENV:POLARIS_URI}/api/catalog
      iceberg.rest-catalog.warehouse=${ENV:POLARIS_CATALOG}
      iceberg.rest-catalog.security=OAUTH2
      iceberg.rest-catalog.oauth2.server-uri=${ENV:POLARIS_URI}/api/catalog/v1/oauth/tokens
      iceberg.rest-catalog.oauth2.credential=${ENV:POLARIS_DEV_CREDENTIAL}
      iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
      fs.native-s3.enabled=true
      s3.endpoint=${ENV:S3_ENDPOINT}
      s3.region=us-east-1
      s3.path-style-access=true

  service:
    type: ClusterIP

  ingress:
    enabled: false # Trino is unauthenticated here; use port-forward.
YAML
```

## Environment

```bash
GIT_REPO_NAME=<git-repo-name>
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_MGMT_URI="http://polaris-mgmt:8182"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export API="$POLARIS_URI/api/management/v1"
echo "$LAB_BUCKET_NAME"
#> user-gollum
```

If the `AWS_*` environment variables are missing or outdated, refresh the `default` profile configuration and export them to the current shell. Refers to lab of [Object storage with S3](../03.object-storage/lab-2-s3.md#environment). Then update the credentials in Polaris pod too by following the related section in "[Lab: update the s3 credential in Polaris pod (k8s)](../09.polaris/lab-3.md#update-the-credentials-in-the-polaris-deployment)".

Check that the s3 bucket can be accessed.

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
#> 2026-09-14 11:02:10     331568 orders.csv
#> 2026-09-14 11:02:09       7351 users.csv
```

Ensure that `/home/onyxia/.polaris-credentials.env` still exists and contents principals's credentials.

```bash
cat /home/onyxia/.polaris-credentials.env
```

If the file still exists and contains polaris' credentials, run the command below.

```bash
source /home/onyxia/.polaris-credentials.env
```

If they are lost, get new credentials.

```bash
# root principal access token
TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials \
  -d client_id=root \
  -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL \
| jq -r .access_token)

set -a
source <(
  for p in dev analyst; do
    curl -s -X POST \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      "${API}/principals/${p}/reset" \
    | jq -r --arg p "$p" \
        '"POLARIS_\($p | ascii_upcase)_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""'
  done
)
set +a
echo "$POLARIS_DEV_CREDENTIAL"
echo "$POLARIS_ANALYST_CREDENTIAL"
```

Create secret containing credentials for Polaris `dev` principal.

```bash
kubectl create secret generic trino-polaris-oauth \
  --from-literal=POLARIS_DEV_CREDENTIAL="$POLARIS_DEV_CREDENTIAL" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Update the secret and the configmap created from the previous lab.

```bash
kubectl create secret generic polaris-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trino-polaris-config \
  --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Deployment

The Trino module contains a Helm chart. It is deployed using `helm upgrade --install`. The whole deployment process will take about 2-3 minutes.

```bash
helm repo add trino https://trinodb.github.io/charts
helm repo update
helm dependency update ./trino/deployment
helm upgrade --install trino ./trino/deployment --wait
```

Verify.

```bash
curl -i http://trino:8080/v1/info
#> HTTP/1.1 200 OK
#> ...
```

## Teardown

When Trino is no longer needed, remove the Trino and its resources.

```bash
helm uninstall trino
helm repo remove trino
kubectl delete secret trino-polaris-oauth
kubectl delete configmap trino-polaris-config
```
