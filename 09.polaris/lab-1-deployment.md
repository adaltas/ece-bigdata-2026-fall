---
duration: 0.5h
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
  - date: 2026-10-28
    comment: Update
    author: elodine@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: deploy Polaris using Helm chart (k8s)

This lab is a prerequisite for the [Polaris lab](./lab.md). It describes how to deploy Polaris on the Adaltas Kubernetes platform.

## Prerequisites

- A `vscode-pyspark` service is running, and its terminal is open.
- The needed configuration files are created for Polaris deployment.

```bash
mkdir -p polaris/deployment/templates
# Chart.yaml
cat <<'YAML' >polaris/deployment/Chart.yaml
apiVersion: v2
name: polaris
version: 1.8.0
dependencies:
  - name: polaris
    version: 1.8.0
    repository: https://downloads.apache.org/polaris/helm-chart
  - name: postgresql
    version: 18.12.15
    repository: oci://registry-1.docker.io/bitnamicharts
YAML
# values.yaml
cat <<'YAML' >polaris/deployment/values.yaml
postgresql:
  auth:
    username: polaris
    database: polaris
    existingSecret: polaris-db
  primary:
    initdb:
      scripts:
        01-create-polaris-schema.sql: |
          CREATE SCHEMA IF NOT EXISTS polaris_schema AUTHORIZATION polaris;
    persistence:
      size: 1Gi
    resources:
      requests:
        cpu: 50m
        memory: 128Mi
      limits:
        cpu: 250m
        memory: 256Mi

polaris:
  persistence:
    type: relational-jdbc
    relationalJdbc:
      secret:
        name: polaris-db
        username: username
        password: password
        jdbcUrl: jdbcUrl

  authentication:
    type: internal

  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 512Mi

  # Load Polaris configuration from the existing ConfigMap.
  extraEnvFrom:
    - configMapRef:
        name: polaris-config

  # Load S3 credentials from the existing Secret.
  extraEnv:
    - name: POLARIS_BOOTSTRAP_CREDENTIALS
      value: "POLARIS,root,s3cr3t"

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

    - name: AWS_REGION
      value: us-east-1

  extraInitContainers:
    - name: polaris-bootstrap
      image: apache/polaris-admin-tool:1.8.0
      resources:
        requests:
          cpu: 50m
          memory: 128Mi
        limits:
          cpu: 500m
          memory: 512Mi

      env:
        - name: QUARKUS_DATASOURCE_USERNAME
          valueFrom:
            secretKeyRef:
              name: polaris-db
              key: username
        - name: QUARKUS_DATASOURCE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: polaris-db
              key: password
        - name: QUARKUS_DATASOURCE_JDBC_URL
          valueFrom:
            secretKeyRef:
              name: polaris-db
              key: jdbcUrl
        - name: POLARIS_PERSISTENCE_TYPE
          value: relational-jdbc
        - name: ROOT_PASSWORD
          valueFrom:
            secretKeyRef:
              name: polaris-credential
              key: POLARIS_ROOT_SECRET

      command: ["/bin/sh", "-c"]
      args:
        - |
          until (echo > /dev/tcp/polaris-postgresql/5432) 2>/dev/null; do
            echo "Waiting for PostgreSQL..."
            sleep 3
          done
          echo "PostgreSQL is ready."
          out="$(java -jar /deployments/polaris-admin-tool.jar bootstrap \
            -r POLARIS \
            -c "POLARIS,root,$ROOT_PASSWORD" 2>&1)"; rc=$?
          echo "$out"
          [ "$rc" -eq 0 ] && exit 0
          echo "$out" | grep -q "already been bootstrapped" && exit 0
          exit 1

  livenessProbe:
    initialDelaySeconds: 90
    failureThreshold: 6
    periodSeconds: 10
    timeoutSeconds: 10
    successThreshold: 1
    terminationGracePeriodSeconds: 30
YAML
# bootstrap-job.yaml
cat <<'YAML' >polaris/deployment/templates/bootstrap-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: polaris-bootstrap
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/name: polaris-bootstrap
    app.kubernetes.io/instance: {{ .Release.Name }}
  annotations:
    helm.sh/hook: post-install,post-upgrade
    helm.sh/hook-weight: "10"
    helm.sh/hook-delete-policy: before-hook-creation
spec:
  backoffLimit: 3
  activeDeadlineSeconds: 600

  template:
    metadata:
      labels:
        app.kubernetes.io/name: polaris-bootstrap
        app.kubernetes.io/instance: {{ .Release.Name }}

    spec:
      restartPolicy: Never

      containers:
        - name: bootstrap
          image: alpine:3.21
          imagePullPolicy: IfNotPresent

          resources:
            requests:
              cpu: 50m
              memory: 32Mi
            limits:
              cpu: 200m
              memory: 56Mi

          envFrom:
            - configMapRef:
                name: polaris-config

          env:
            - name: POLARIS_ROOT_SECRET
              valueFrom:
                secretKeyRef:
                  name: polaris-credential
                  key: POLARIS_ROOT_SECRET

            - name: AWS_REGION
              value: us-east-1

            - name: KUBERNETES_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace

            - name: LAB_BUCKET_NAME
              value: "$(KUBERNETES_NAMESPACE)"

            - name: POLARIS_BASE_LOCATION
              value: "s3://$(LAB_BUCKET_NAME)/warehouse/"

            - name: S3_ENDPOINT
              value: "https://s3.seaweedfs.adm.adaltas.cloud"

          command:
            - /bin/sh
            - -ec
            - |
              apk add --no-cache curl jq

              POLARIS_URI="${POLARIS_URI%/}"
              API="${POLARIS_URI}/api/management/v1"
              TOKEN_URI="${POLARIS_URI}/api/catalog/v1/oauth/tokens"

              echo "Waiting for Polaris..."

              until curl -fsS \
                "${POLARIS_MGMT_URI}/q/health/ready" >/dev/null; do
                echo "Polaris is not ready; retrying in 5 seconds..."
                sleep 5
              done

              echo "Polaris is ready."
              echo "Realm: ${POLARIS_REALM}"
              echo "Catalog: ${POLARIS_CATALOG}"

              # --------------------------------------------------
              # Authentication
              # --------------------------------------------------

              echo "Obtaining administrative access token..."

              TOKEN_RESPONSE=$(curl -fsS \
                -u "${POLARIS_CLIENT_ID}:${POLARIS_ROOT_SECRET}" \
                -H "Polaris-Realm: ${POLARIS_REALM}" \
                -H "Content-Type: application/x-www-form-urlencoded" \
                -d "grant_type=client_credentials" \
                -d "scope=PRINCIPAL_ROLE:ALL" \
                "${TOKEN_URI}")

              TOKEN=$(echo "${TOKEN_RESPONSE}" |
                jq -er '.access_token')

              # --------------------------------------------------
              # API helpers
              # --------------------------------------------------

              api_get() {
                curl -fsS \
                  -H "Authorization: Bearer ${TOKEN}" \
                  -H "Polaris-Realm: ${POLARIS_REALM}" \
                  -H "Accept: application/json" \
                  "$1"
              }

              api_post() {
                curl -fsS \
                  -X POST \
                  -H "Authorization: Bearer ${TOKEN}" \
                  -H "Polaris-Realm: ${POLARIS_REALM}" \
                  -H "Content-Type: application/json" \
                  -H "Accept: application/json" \
                  -d "$2" \
                  "$1"
              }

              api_put() {
                curl -fsS \
                  -X PUT \
                  -H "Authorization: Bearer ${TOKEN}" \
                  -H "Polaris-Realm: ${POLARIS_REALM}" \
                  -H "Content-Type: application/json" \
                  -H "Accept: application/json" \
                  -d "$2" \
                  "$1"
              }

              # Return HTTP status without hiding unexpected API errors.
              http_status() {
                curl -sS \
                  -o /dev/null \
                  -w '%{http_code}' \
                  -H "Authorization: Bearer ${TOKEN}" \
                  -H "Polaris-Realm: ${POLARIS_REALM}" \
                  "$1"
              }

              # --------------------------------------------------
              # Create principals
              # --------------------------------------------------

              ensure_principal() {
                name="$1"
                endpoint="${API}/principals/${name}"
                status=$(http_status "${endpoint}")

                case "${status}" in
                  200)
                    echo "Principal '${name}' already exists."
                    ;;
                  404)
                    echo "Creating principal '${name}'..."

                    response=$(api_post "${API}/principals" \
                      "$(jq -nc --arg name "${name}" \
                        '{
                          principal: {
                            name: $name,
                            properties: {}
                          },
                          credentialRotationRequired: false
                        }')")

                    echo "Principal '${name}' created."

                    # Credentials are returned only when the principal
                    # is created. Store them securely for later use.
                    echo "${response}" | jq -c '{
                      principal: .principal.name,
                      credentials: .credentials
                    }'
                    ;;
                  *)
                    echo "Unexpected HTTP ${status} checking principal '${name}'." >&2
                    exit 1
                    ;;
                esac
              }

              ensure_principal dev
              ensure_principal analyst

              # --------------------------------------------------
              # Create principal roles
              # --------------------------------------------------

              ensure_principal_role() {
                name="$1"
                endpoint="${API}/principal-roles/${name}"
                status=$(http_status "${endpoint}")

                case "${status}" in
                  200)
                    echo "Principal role '${name}' already exists."
                    ;;
                  404)
                    echo "Creating principal role '${name}'..."

                    api_post "${API}/principal-roles" \
                      "$(jq -nc --arg name "${name}" \
                        '{
                          principalRole: {
                            name: $name,
                            properties: {}
                          }
                        }')" >/dev/null

                    echo "Principal role '${name}' created."
                    ;;
                  *)
                    echo "Unexpected HTTP ${status} checking principal role '${name}'." >&2
                    exit 1
                    ;;
                esac
              }

              ensure_principal_role data_engineer
              ensure_principal_role data_analyst

              # --------------------------------------------------
              # Create catalog
              # --------------------------------------------------

              endpoint="${API}/catalogs/${POLARIS_CATALOG}"
              status=$(http_status "${endpoint}")

              case "${status}" in
                200)
                  echo "Catalog '${POLARIS_CATALOG}' already exists."
                  ;;
                404)
                  echo "Creating catalog '${POLARIS_CATALOG}'..."

                  CATALOG_PAYLOAD=$(jq -nc \
                    --arg name "${POLARIS_CATALOG}" \
                    --arg location "${POLARIS_BASE_LOCATION}" \
                    --arg endpoint "${S3_ENDPOINT}" \
                    --arg region "${AWS_REGION}" \
                    '{
                      catalog: {
                        name: $name,
                        type: "INTERNAL",
                        properties: {
                          "default-base-location": $location
                        },
                        storageConfigInfo: {
                          storageType: "S3",
                          allowedLocations: [$location],
                          endpoint: $endpoint,
                          endpointInternal: $endpoint,
                          pathStyleAccess: true,
                          region: $region,
                          stsUnavailable: true
                        }
                      }
                    }')

                  api_post "${API}/catalogs" \
                    "${CATALOG_PAYLOAD}" >/dev/null

                  echo "Catalog '${POLARIS_CATALOG}' created."
                  ;;
                *)
                  echo "Unexpected HTTP ${status} checking catalog." >&2
                  exit 1
                  ;;
              esac

              # --------------------------------------------------
              # Create catalog roles
              # --------------------------------------------------

              ensure_catalog_role() {
                name="$1"
                endpoint="${API}/catalogs/${POLARIS_CATALOG}/catalog-roles/${name}"
                status=$(http_status "${endpoint}")

                case "${status}" in
                  200)
                    echo "Catalog role '${name}' already exists."
                    ;;
                  404)
                    echo "Creating catalog role '${name}'..."

                    api_post \
                      "${API}/catalogs/${POLARIS_CATALOG}/catalog-roles" \
                      "$(jq -nc --arg name "${name}" \
                        '{
                          catalogRole: {
                            name: $name,
                            properties: {}
                          }
                        }')" >/dev/null

                    echo "Catalog role '${name}' created."
                    ;;
                  *)
                    echo "Unexpected HTTP ${status} checking catalog role '${name}'." >&2
                    exit 1
                    ;;
                esac
              }

              ensure_catalog_role catalog_contributor
              ensure_catalog_role catalog_reader

              # --------------------------------------------------
              # Grant privileges to catalog roles
              # --------------------------------------------------

              grant_privilege() {
                role="$1"
                privilege="$2"

                endpoint="${API}/catalogs/${POLARIS_CATALOG}/catalog-roles/${role}/grants"

                # Avoid submitting duplicate grants.
                grants=$(api_get "${endpoint}")

                if echo "${grants}" | jq -e \
                  --arg privilege "${privilege}" \
                  '.grants[]? |
                    select(
                      .type == "catalog" and
                      .privilege == $privilege
                    )' >/dev/null; then
                  echo "${privilege} is already granted to '${role}'."
                  return
                fi

                echo "Granting ${privilege} to '${role}'..."

                payload=$(jq -nc --arg privilege "${privilege}" \
                  '{
                    grant: {
                      type: "catalog",
                      privilege: $privilege
                    }
                  }')

                api_put "${endpoint}" "${payload}" >/dev/null

                echo "Granted ${privilege} to '${role}'."
              }

              # Contributor privileges.
              grant_privilege catalog_contributor CATALOG_MANAGE_ACCESS
              grant_privilege catalog_contributor CATALOG_MANAGE_METADATA
              grant_privilege catalog_contributor CATALOG_MANAGE_CONTENT

              # Reader privileges.
              grant_privilege catalog_reader TABLE_READ_DATA
              grant_privilege catalog_reader TABLE_READ_PROPERTIES
              grant_privilege catalog_reader CATALOG_READ_PROPERTIES

              # --------------------------------------------------
              # Assign principal roles to principals
              # --------------------------------------------------

              assign_principal_role() {
                principal="$1"
                role="$2"

                endpoint="${API}/principals/${principal}/principal-roles"

                roles=$(api_get "${endpoint}")

                if echo "${roles}" | jq -e \
                  --arg role "${role}" \
                  '.roles[]? | select(.name == $role)' \
                  >/dev/null; then
                  echo "Principal role '${role}' is already assigned to '${principal}'."
                  return
                fi

                echo "Assigning '${role}' to '${principal}'..."

                api_put "${endpoint}" \
                  "$(jq -nc --arg role "${role}" \
                    '{
                      principalRole: {
                        name: $role
                      }
                    }')" >/dev/null

                echo "Assigned '${role}' to '${principal}'."
              }

              assign_principal_role dev data_engineer
              assign_principal_role analyst data_analyst

              # --------------------------------------------------
              # Assign catalog roles to principal roles
              # --------------------------------------------------

              assign_catalog_role() {
                principal_role="$1"
                catalog_role="$2"

                endpoint="${API}/principal-roles/${principal_role}/catalog-roles/${POLARIS_CATALOG}"

                roles=$(api_get "${endpoint}")

                if echo "${roles}" | jq -e \
                  --arg role "${catalog_role}" \
                  '.roles[]? | select(.name == $role)' \
                  >/dev/null; then
                  echo "Catalog role '${catalog_role}' is already assigned to '${principal_role}'."
                  return
                fi

                echo "Assigning '${catalog_role}' to '${principal_role}'..."

                api_put "${endpoint}" \
                  "$(jq -nc --arg role "${catalog_role}" \
                    '{
                      catalogRole: {
                        name: $role
                      }
                    }')" >/dev/null

                echo "Assigned '${catalog_role}' to '${principal_role}'."
              }

              assign_catalog_role data_engineer catalog_contributor
              assign_catalog_role data_analyst catalog_reader

              echo "========================================"
              echo "Polaris RBAC bootstrap completed."
              echo "Catalog: ${POLARIS_CATALOG}"
              echo "dev      -> data_engineer -> catalog_contributor"
              echo "analyst  -> data_analyst -> catalog_reader"
              echo "========================================"
YAML
# polaris-db-secret.yaml
cat <<'YAML' >polaris/deployment/templates/polaris-db-secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: polaris-db
  annotations:
    secret-generator.v1.mittwald.de/autogenerate: password
type: Opaque
stringData:
  username: polaris
  jdbcUrl: jdbc:postgresql://{{ .Release.Name }}-postgresql:5432/polaris
YAML
```

## Define environment variables

```bash
GIT_REPO_NAME=<git-repo-name>
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_MGMT_URI="http://polaris-mgmt:8182"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export API="$POLARIS_URI/api/management/v1"
echo "$LAB_BUCKET_NAME"
```

If the `AWS_*` environment variables are missing or outdated, refresh the `default` profile configuration and export them to the current shell. Refers to lab of [Object storage with S3](../03.object-storage/lab-2-s3.md#environment).

Check that the datasets are present in the bronze layer:

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
#> 2026-09-14 11:02:10     331568 orders.csv
#> 2026-09-14 11:02:09       7351 users.csv
```

Create a ConfigMap containing the environment variables.

```bash
kubectl create configmap polaris-config \
  --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" \
  --from-literal=POLARIS_URI="$POLARIS_URI" \
  --from-literal=POLARIS_MGMT_URI="$POLARIS_MGMT_URI" \
  --from-literal=POLARIS_REALM="POLARIS" \
  --from-literal=POLARIS_CLIENT_ID="root" \
  --from-literal=POLARIS_BASE_LOCATION="s3://$LAB_BUCKET_NAME/warehouse/" \
  --from-literal=S3_ENDPOINT="https://s3.seaweedfs.adm.adaltas.cloud/" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Create secret `polaris-credential` for Polaris root credential. The `root` principal is the initial principal in Polaris. A principal represents a user or service. Its secret is defined as `s3cr3t` for demo usage, you may change it.

```bash
kubectl create secret generic polaris-credential \
  --from-literal=POLARIS_ROOT_SECRET="s3cr3t" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Create secret `polaris-s3` for AWS credentials. These credentials are required for Polaris to access the S3 bucket.

```bash
kubectl create secret generic polaris-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Deployment

The Polaris module contains a Helm chart and a bootstrap Job that creates the required Polaris entities. Polaris is deployed using `helm upgrade --install`. The whole deployment process will take around 2-3 minutes.

```bash
helm repo add polaris https://downloads.apache.org/polaris/helm-chart
helm repo update
helm dependency update ./deployment
helm upgrade --install polaris ./deployment --wait
```

Verify that Polaris is running.

```bash
curl -s http://polaris-mgmt:8182/q/health
#> {
#>     "status": "UP",
```

## Environment variable configuration

The credentials for the two principals are stored in environment variables.

```bash
kubectl logs job/polaris-bootstrap \
  | grep '"credentials"' \
  | jq -r 'select(.principal == "dev" or .principal == "analyst")
      | "POLARIS_\(.principal | ascii_upcase)_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' \
  > /home/onyxia/.polaris-credentials.env
set -a; . /home/onyxia/.polaris-credentials.env; set +a
echo $POLARIS_DEV_CREDENTIAL
#> ccfc...:c72bfea3e8619...
echo $POLARIS_ANALYST_CREDENTIAL
#> e9c2...:48db20d1d282a...
```

In a production workflow, credentials should not be committed to the Git repository. Since the current working filesystem is ephemeral, copy the credentials to your local filesystem for safekeeping. They cannot be retrieved later, so losing them will require resetting the corresponding principals to generate new credentials.

## Advanced

This section demonstrates the entities created by the bootstrap Job and is optional. You may skip this section.

### Retrieve root level token

Retrieve an access token for the root principal to perform the following REST API checks.

```bash
TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials \
  -d client_id=root \
  -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL \
| jq -r .access_token)
```

### List catalog

```bash
curl -s "$API/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
| jq
#> {
#>   "catalogs": [
#>     {
#>       "type": "INTERNAL",
#>       "name": "$POLARIS_CATALOG",
#>       "properties": {
#>         "default-base-location": "s3://$POLARIS_CATALOG/warehouse/"
#>       },
#>       ...
```

### List the principals

```bash
curl -s "$API/principals" \
  -H "Authorization: Bearer $TOKEN" \
| jq
#> {
#>   "principals": [
#>     {
#>       "name": "root",
#>       ...
#>       "name": "dev",
#>       ...
#>       "name": "analyst",
#>       ...
```

### List the principal roles

```bash
curl -s "$API/principal-roles" \
  -H "Authorization: Bearer $TOKEN" \
| jq
#> {
#>   "roles": [
#>     {
#>       "name": "service_admin",
#>       ...
#>       "name": "data_engineer",
#>       ...
#>       "name": "data_scientist",
#>       ...
```

### List the catalog roles

```bash
curl -s "$API/catalogs/$POLARIS_CATALOG/catalog-roles" \
  -H "Authorization: Bearer $TOKEN" \
| jq
#> {
#>   "roles": [
#>     {
#>       "name": "catalog_admin",
#>       ...
#>       "name": "catalog_contributor",
#>       ...
#>       "name": "catalog_reader",
#>       ...
```

## Teardown

The aim of this section is to show how to remove Polaris when it is no longer needed. For the next module, keep it.

```bash
helm uninstall polaris
kubectl delete job polaris-bootstrap
kubectl delete secret polaris-s3 polaris-credential
kubectl delete configmap polaris-config
kubectl delete pvc data-polaris-postgresql-0
```
