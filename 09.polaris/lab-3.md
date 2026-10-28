---
duration: 0.5h
category:
  - name: LAB
components:
  - name: POLARIS
  - name: S3
platforms:
  - name: LINUX
resources:
  - title: Apache Polaris documentation
    url: https://polaris.apache.org/
revisions:
  - date: 2026-10-26
    comment: Initial draft, Polaris configuration to be adapted once the cluster is deployed
    author: mori@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: update the s3 credential in Polaris pod (k8s)

Polaris reads its S3 credentials from environment variables, which are fixed when the pod starts. This lab shows how to refresh them when they expire, without losing the catalog.

## Prerequisites

- A `vscode-pyspark` service is running, and its terminal is open.
- The course repository is cloned in the vscode filesystem.
  ```bash
  cd ~/work/courses/courses/bigdata/modules/09.polaris
  ```
- [Lab 1](./lab-1-deployment.md) is finished.

## Renew the S3 credentials locally

Follow the credential renewal in the [S3 lab](../03.object-storage/lab-2-s3.md) of module 3 to update the `~/.aws/` directory.

Export the new credentials as environment variables.

```bash
eval "$(aws configure export-credentials --profile default --format env)"
```

Verify that the bucket is accessible with the new credentials.

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
#> 2026-09-14 11:02:10     331568 orders.csv
#> 2026-09-14 11:02:09       7351 users.csv
```

## Update the credentials in the Polaris deployment

Update the `polaris-s3` secret with the new AWS credentials.

```bash
kubectl create secret generic polaris-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Updating a secret does not change the environment of a running pod. Restart the Polaris deployment so the new pod reads the new values.

```bash
kubectl rollout restart deployment/polaris
kubectl rollout status deployment/polaris
#> deployment "polaris" successfully rolled out
```

### Resource constraint

If the new pod is stuck with resource constraint, try scale down first.

```bash
kubectl scale deployment/polaris --replicas=0
kubectl scale deployment/polaris --replicas=1
kubectl rollout status deployment/polaris
```

The Polaris entities (catalog, principals, roles and grants) are stored in PostgreSQL, so they survive the restart. The credentials of the `dev` and `analyst` principals are unchanged.

## Verify

Check that the new pod uses the new access key. Compare only the key ID, never print the secret.

```bash
kubectl exec deployment/polaris -- sh -c 'tr "\0" "\n" < /proc/1/environ | grep AWS_ACCESS_KEY_ID'
echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"
```
