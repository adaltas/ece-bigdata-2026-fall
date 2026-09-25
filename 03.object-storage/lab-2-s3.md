---
duration: 2h
category:
  - name: LAB
components:
  - name: S3
  - name: KUBERNETES
platforms:
  - name: LINUX
resources:
  - title: AWS CLI S3 commands (official documentation)
    url: https://docs.aws.amazon.com/cli/latest/reference/s3/
  - title: AWS CLI S3 API commands (official documentation)
    url: https://docs.aws.amazon.com/cli/latest/reference/s3api/
  - title: s5cmd (official repository)
    url: https://github.com/peak/s5cmd
revisions:
  - date: 2026-09-13
    comment: Initial page
    author: david@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: Object storage with S3

## Objectives

- Familiarize with the usage and management of an S3 bucket
- Upload, list, download, and delete objects
- Use metadata, presigned URLs, multipart uploads, versioning, lifecycle policies, and access control
- Upload the datasets to the bronze layer from a Kubernetes Job

## Prerequisites

- The `vscode-pyspark` Onyxia service and the project of the [previous lab](./lab-1-uv.md)
- The [ConfigMap and Secret](../02.containerization-and-kubernetes/lab-2.md) and
  [Job](../02.containerization-and-kubernetes/lab-3.md) labs

Some commands of this lab configure the bucket itself (versioning, lifecycle, policies, ACLs). Depending on the S3
backend of the platform and on your permissions, they may return an `AccessDenied` or `NotImplemented` error. In such a
case, read the explanation, note the error, and continue with the next section.

## Environment

Onyxia configures the S3 access of the service in two places:

- the `default` profile of the AWS configuration files, `~/.aws/credentials` and `~/.aws/config`
- `AWS_*` environment variables

To configure the AWS configurations and credentials, please copy the script from the S3 Profile Details in Data Storage tab,
and execute the copied script in the terminal.

![](./assets/onyxia-aws-config.png)

The credentials are temporary. The profile is the reference: the environment variables may be missing or outdated and
lead to authentication errors. All the commands of this lab explicitly use the profile with `--profile 'default'`.

Display the profile configuration and where each value comes from, the secrets are masked:

```bash
aws configure list --profile 'default'
```

s5cmd and the Python SDK need the endpoint URL. It is read from the profile, or built from the `AWS_S3_ENDPOINT`
variable which contains the host name without the scheme:

```bash
export S3_ENDPOINT_URL=$(
  aws configure get endpoint_url --profile 'default' \
  || echo "https://$AWS_S3_ENDPOINT"
)
echo "$S3_ENDPOINT_URL"
```

In Onyxia, the bucket name is `user-<username>` and equals your namespace name.

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
echo "$LAB_BUCKET_NAME"
```

If a command fails with an `ExpiredToken` error, restart the service to renew the credentials.

## Bucket verification

By default, Onyxia creates a bucket dedicated to each user. Check that it exists:

```bash
aws s3 --profile 'default' ls
#> 2026-09-01 10:00:00 user-gollum
```

If the bucket does not exist, the following error occurs when writing into it:

```
upload failed: ./users.csv to s3://user-gollum/bronze/users.csv
An error occurred (AccessDenied) when calling the PutObject operation: Access Denied.
```

In this case, open the file explorer page of the Onyxia portal, which creates the personal bucket.

## CLI utilities

`aws s3` is the official CLI tool recommended by AWS. It provides high-level file commands (`cp`, `mv`, `ls`, `rm`,
`sync`). `aws s3api` exposes every operation of the S3 API (`head-object`, `put-bucket-versioning`, ...).

`s5cmd` is a faster drop-in alternative to the `aws s3` commands. Key differences:

- **Speed**  
  Runs operations in parallel by default, significantly faster for bulk uploads, downloads, and deletes.
- **Syntax**  
  Slightly shorter, no `s3` subcommand (eg `aws s3 cp` is replaced by `s5cmd cp`).
- **Batch operations**  
  Accepts a list of commands from a file, enabling massive parallel execution (eg `s5cmd run commands.txt`)
- **Scope**  
  Covers mostly object operations (`cp`, `mv`, `rm`, `ls`, `sync`, `cat`). For API-level operations
  (`put-bucket-policy`, `head-object`, `list-object-versions`…) you still need `aws s3api`.
- **Configuration**  
  Reuses the AWS credentials files and profiles (`--profile`), but has no `configure` command. The endpoint is set with
  `--endpoint-url` or the `S3_ENDPOINT_URL` variable.

Use `s5cmd` for data transfer at scale, `aws s3api` for everything else.

## s5cmd utility installation

User binaries are commonly installed in `~/.local/bin`. The folder is added to the `$PATH` environment variable.

```bash
CMD='export PATH="$HOME/.local/bin:$PATH"'
grep -qxF "$CMD" "$HOME/.bashrc" || echo "$CMD" >> "$HOME/.bashrc"
source "$HOME/.bashrc"
```

[s5cmd](https://github.com/peak/s5cmd) is downloaded from its GitHub releases and its checksum is verified before
installation. The installation runs in a subshell, `( ... )`, so that an error does not close your terminal.

```bash
(
  set -e
  S5CMD_VERSION=$(
    curl -s https://api.github.com/repos/peak/s5cmd/releases/latest \
    | jq -r '.tag_name | .[1:]'
  )
  BIN_DIR=$([[ "$USER" == "root" ]] && echo /usr/local/bin || echo ~/.local/bin)
  mkdir -p "$BIN_DIR"
  # Architecture discovery
  case "$(uname -m)" in
    x86_64) S5CMD_ARCH="64bit" ;;
    aarch64|arm64) S5CMD_ARCH="arm64" ;;
    *) echo "System architecture $(uname -m) not supported."; exit 1 ;;
  esac
  # Binary and checksums download
  S5CMD_FILE="s5cmd_${S5CMD_VERSION}_Linux-${S5CMD_ARCH}.tar.gz"
  S5CMD_BASE_URL="https://github.com/peak/s5cmd/releases/download/v${S5CMD_VERSION}"
  TMP_DIR=$(mktemp -d)
  cd "$TMP_DIR"
  curl -fsSLO "$S5CMD_BASE_URL/$S5CMD_FILE"
  curl -fsSLO "$S5CMD_BASE_URL/s5cmd_checksums.txt"
  # Checksum validation and installation
  grep " ${S5CMD_FILE}$" s5cmd_checksums.txt | sha256sum -c
  tar -xf "$S5CMD_FILE" -C "$BIN_DIR" s5cmd
  chmod +x "$BIN_DIR/s5cmd"
  # Cleanup
  rm -rf "$TMP_DIR"
)
```

The `s5cmd` command is now available.

```bash
command -v s5cmd
#> /home/onyxia/.local/bin/s5cmd
s5cmd version
#> v2.3.0-991c9fb
```

## Upload (PUT)

Move into the project of the previous lab. The user dataset is generated in the CSV format and uploaded to the bucket.
Under the hood this is a single HTTP `PUT` request. The object is written atomically: no other client sees a partial
state during the upload.

```bash
# Define the name of your repo/directory accordinly
GIT_REPO_NAME=<git-repo-name>
cd /home/onyxia/work/$GIT_REPO_NAME
uv run dataset-users -o csv > users.csv
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
# Equivalent with s5cmd:
# s5cmd --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
```

The generated CSV files are data, not source code: add `*.csv` to the `.gitignore` file of the project.

## List objects

Listing is an HTTP `GET` request on the bucket which returns all keys starting with a given prefix. It looks like
browsing a directory, but the bucket actually contains a flat list of keys.

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
#> 2026-09-14 00:38:01       7351 users.csv
s5cmd --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
#> 2026/09/14 00:38:01              7351  users.csv
```

The `--recursive` argument lists every object under the prefix, regardless of the `/` characters in their keys.

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
```

## Download (GET)

`GET` retrieves an object by its exact key:

```bash
aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" ./users_downloaded.csv
diff users.csv users_downloaded.csv && echo "identical"
```

## Moving and renaming

There is no atomic rename. It is achieved with two separate operations: copying the source object to its destination key
and deleting the source object. Pipelines must tolerate seeing both keys for a short period, and a partial state if the
operation fails between the two steps. `aws s3 mv` performs both operations:

```bash
aws s3 --profile 'default' mv "s3://$LAB_BUCKET_NAME/bronze/users.csv" "s3://$LAB_BUCKET_NAME/bronze/users_renamed.csv"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
#> 2026-09-14 00:40:12       7351 users_renamed.csv
```

## Delete

`DELETE` removes a single object by key:

```bash
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/bronze/users_renamed.csv"
```

There is no directory to delete. Deleting everything under a prefix requires deleting each object individually. The
`--recursive` flag loops over all matching keys:

```bash
# For illustration only, the cleanup is done at the end of the lab
# aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/bronze/" --recursive
```

## Metadata

Every object carries two kinds of metadata.

System metadata is set automatically: size, content type, last modification date, and an ETag. For an object uploaded in
a single `PUT`, the ETag is the MD5 hash of its content. For a multipart upload, it is computed from the hashes of the
parts and followed by `-<number of parts>`. It is used to detect changes.

```bash
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3api --profile 'default' head-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv
md5sum users.csv
```

```json
{
  "LastModified": "2026-09-14T10:23:00+00:00",
  "ContentLength": 7351,
  "ETag": "\"<md5 hash of users.csv>\"",
  "ContentType": "text/csv",
  "Metadata": {}
}
```

Compare the ETag with the output of `md5sum`.

User-defined metadata attaches arbitrary key-value pairs to an object at upload time:

```bash
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv" \
  --metadata "source=dataset_users.py,version=1,rows=50"

aws s3api --profile 'default' head-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv \
  | jq '.Metadata'
#> { "source": "dataset_users.py", "version": "1", "rows": "50" }
```

Because objects are immutable, metadata is immutable too. To update it, the object must be re-uploaded or copied onto
itself with new metadata.

Metadata is retrieved object by object with `HeadObject`: S3 provides no request to search objects by metadata. It is
suited to describe an object (lineage, schema version) but not to replace an index or a catalog.

## Presigned URLs

A presigned URL lets anyone download an object without credentials. The URL embeds the authentication signature and an
expiration time:

```bash
URL=$(aws s3 --profile 'default' presign "s3://$LAB_BUCKET_NAME/bronze/users.csv" --expires-in 300)
echo "$URL"
#> https://<endpoint>/user-gollum/bronze/users.csv?X-Amz-Algorithm=...&X-Amz-Expires=300&X-Amz-Signature=...
```

Any HTTP client can use it directly, with no configuration:

```bash
curl -s "$URL" | head -n 3
```

After the expiration time, the URL returns a `403 Forbidden`.

Presigned URLs can also authorize a `PUT` request, letting external clients upload a file without exposing your
credentials and without routing the data through your own backend. The `aws s3 presign` command only generates `GET`
URLs, `PUT` URLs are generated with an SDK such as boto3:

```bash
uv run --with boto3 python - <<'PY'
import os
import boto3

session = boto3.Session(profile_name="default")
s3 = session.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])
print(s3.generate_presigned_url(
    "put_object",
    Params={"Bucket": os.environ["LAB_BUCKET_NAME"], "Key": "bronze/upload.csv"},
    ExpiresIn=300,
))
PY
```

## Multipart Upload

For large files, the S3 protocol splits the upload into numbered parts sent in parallel and assembled by the server. The
AWS CLI does this automatically above a configurable threshold, 8 MB by default:

```bash
dd if=/dev/urandom of=large_dataset.bin bs=1M count=200
aws s3 --profile 'default' cp large_dataset.bin "s3://$LAB_BUCKET_NAME/large/dataset.bin"
aws s3api --profile 'default' head-object --bucket "$LAB_BUCKET_NAME" --key large/dataset.bin | jq -r '.ETag'
#> "a1b2c3...-25"
```

The `-25` suffix of the ETag is the number of parts.

The benefits are:

- **Resilience**: if a part fails, only that part is retried.
- **Speed**: parts are uploaded in parallel, saturating available bandwidth.
- **Size**: a single `PUT` is capped at 5 GB, multipart handles up to 5 TB.

Incomplete multipart uploads are invisible to normal listing but consume storage. List them with:

```bash
aws s3api --profile 'default' list-multipart-uploads --bucket "$LAB_BUCKET_NAME"
```

A lifecycle rule, presented below, aborts them automatically.

## Versioning

When versioning is enabled, uploading to an existing key creates a new version instead of overwriting the previous one.
All versions are retained and individually addressable:

```bash
aws s3api --profile 'default' put-bucket-versioning \
  --bucket "$LAB_BUCKET_NAME" \
  --versioning-configuration Status=Enabled

# Version 1: 50 users
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"

# Version 2: 40 users
uv run dataset-users -c 40 -o csv > users_v2.csv
aws s3 --profile 'default' cp users_v2.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"

aws s3api --profile 'default' list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix bronze/users.csv \
  | jq '.Versions[] | {VersionId, IsLatest, Size, LastModified}'
```

```json
{
  "VersionId": "abc123",
  "IsLatest": true,
  "Size": 5855,
  "LastModified": "2026-09-14T10:30:00+00:00"
}
{
  "VersionId": "def456",
  "IsLatest": false,
  "Size": 7351,
  "LastModified": "2026-09-14T10:23:00+00:00"
}
```

Retrieve any specific version by its ID, replacing `<version-id>` with the ID of the oldest version:

```bash
aws s3api --profile 'default' get-object \
  --bucket "$LAB_BUCKET_NAME" \
  --key bronze/users.csv \
  --version-id <version-id> \
  users_v1.csv
diff -q users.csv users_v1.csv && echo "version 1 restored"
```

Deleting a versioned object does not remove any data — it creates a **delete marker**. The object disappears from normal
`GET` requests but all versions remain recoverable:

```bash
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/bronze/users.csv"
# The object seems gone...
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
# ...but all versions are still there, with a delete marker
aws s3api --profile 'default' list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix bronze/users.csv
```

## Lifecycle Policies

Lifecycle rules let the storage backend automatically expire objects or abort stale uploads, without any external cron
job or script.

The following policy:

- expires the objects under `large/` after 30 days
- permanently deletes the non-current versions 7 days after they were replaced
- aborts any multipart upload that has not been completed within 7 days

```bash
cat > lifecycle.json <<'EOF'
{
  "Rules": [
    {
      "ID": "expire-large-after-30-days",
      "Status": "Enabled",
      "Filter": { "Prefix": "large/" },
      "Expiration": { "Days": 30 }
    },
    {
      "ID": "delete-noncurrent-versions",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "NoncurrentVersionExpiration": { "NoncurrentDays": 7 }
    },
    {
      "ID": "abort-incomplete-multipart",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    }
  ]
}
EOF

aws s3api --profile 'default' put-bucket-lifecycle-configuration \
  --bucket "$LAB_BUCKET_NAME" \
  --lifecycle-configuration file://lifecycle.json
aws s3api --profile 'default' get-bucket-lifecycle-configuration --bucket "$LAB_BUCKET_NAME"
```

Versioning and lifecycle rules are complementary. Versioning protects against accidental overwrites and deletions. On a
versioned bucket, an `Expiration` rule only adds a delete marker and the data is still stored: a
`NoncurrentVersionExpiration` rule is required to control the cost of retaining old versions.

## Access Control

Access control has two levels.

**Bucket policies** are JSON documents attached to a bucket. They define who (principal) can or cannot do what (actions)
on which objects (resources). The following policy protects the raw data of the bronze layer against deletion:

```bash
cat > policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ProtectBronzeLayer",
      "Effect": "Deny",
      "Principal": "*",
      "Action": ["s3:DeleteObject"],
      "Resource": "arn:aws:s3:::$LAB_BUCKET_NAME/bronze/*"
    }
  ]
}
EOF

aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/protected.csv"
aws s3api --profile 'default' put-bucket-policy --bucket "$LAB_BUCKET_NAME" --policy file://policy.json
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/bronze/protected.csv"
#> delete failed: ... An error occurred (AccessDenied) when calling the DeleteObject operation: Access Denied
```

Is the deletion denied? Support for bucket policies differs between S3-compatible backends such as SeaweedFS, used by
the platform, Ceph RGW or MinIO: some only apply them to anonymous users and manage the permissions of authenticated
users with IAM policies instead.

Remove the policy:

```bash
aws s3api --profile 'default' delete-bucket-policy --bucket "$LAB_BUCKET_NAME"
```

**Object ACLs** apply to a single object. They are a legacy mechanism, disabled by default on new AWS buckets and not
supported by every S3-compatible backend. The most common use was making an object publicly readable with the
`public-read` canned ACL. Only read the ACL of your object, do not publish data on a shared platform:

```bash
aws s3api --profile 'default' get-object-acl --bucket "$LAB_BUCKET_NAME" --key bronze/protected.csv
```

Presigned URLs are the preferred way to share a single object.

## Consistency

AWS S3, since December 2020, and most S3-compatible backends provide **strong read-after-write consistency**: as soon
as a `PUT` returns successfully, any subsequent `GET` on that key returns the new object. There is no delay, no
propagation window to wait for:

```bash
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" /dev/null && echo "immediately available"
```

Some object stores, especially multi-region setups, historically provided only eventual consistency. Always verify this
for your specific backend before building pipelines that depend on it.

## Cleanup

Remove the objects, versions and bucket configurations created during this lab. On a versioned bucket, `aws s3 rm` only
adds delete markers: every version and delete marker must be deleted explicitly.

```bash
# Delete all versions and delete markers under the lab prefixes
for prefix in bronze/ large/; do
  aws s3api --profile 'default' list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix "$prefix" --output json \
  | jq -c '{Objects: ([.Versions[]?, .DeleteMarkers[]?] | map({Key, VersionId})), Quiet: true}' \
  > delete.json
  if [ "$(jq '.Objects | length' delete.json)" -gt 0 ]; then
    aws s3api --profile 'default' delete-objects --bucket "$LAB_BUCKET_NAME" --delete file://delete.json
  fi
done
# Versioning cannot be disabled once enabled, only suspended
aws s3api --profile 'default' put-bucket-versioning \
  --bucket "$LAB_BUCKET_NAME" --versioning-configuration Status=Suspended
aws s3api --profile 'default' delete-bucket-lifecycle --bucket "$LAB_BUCKET_NAME"
# Local files
rm -f large_dataset.bin users_downloaded.csv users_v1.csv users_v2.csv delete.json lifecycle.json policy.json
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
```

## Upload the datasets from a Kubernetes Job

In a data platform, ingestion runs inside the cluster, not from a developer workstation. This last exercise combines the
previous labs: the datasets are provided in a ConfigMap, the credentials in a Secret, and a Job uploads them to the
bronze layer. The resulting objects are used in the next modules.

The service account of the Onyxia service is usually granted the permissions to manage these objects in your namespace.
Verify it:

```bash
kubectl auth can-i create jobs
#> yes
kubectl auth can-i create secrets
#> yes
```

If the answer is `no`, ask your instructor to enable the Kubernetes admin role of the service. Generate the datasets:

```bash
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
ls -lh users.csv orders.csv
```

A ConfigMap is limited to 1 MiB. It is acceptable for these small datasets, a real ingestion reads its data from the
source system.

```bash
kubectl create configmap datasets --from-file=users.csv --from-file=orders.csv

kubectl create configmap s3-config \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_DEFAULT_REGION="$(aws configure get region --profile 'default')" \
  --from-literal=LAB_BUCKET_NAME="$LAB_BUCKET_NAME"

# Export the credentials of the profile into a temporary file, never print them
aws configure export-credentials --profile 'default' --format env-no-export \
  | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > s3.env
kubectl create secret generic s3-credentials --from-env-file=s3.env
rm s3.env
```

The Job runs the official AWS CLI image. The environment variables are injected from the ConfigMap and the Secret, and
the datasets are mounted as files.

```bash
cat > job-upload-bronze.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: upload-bronze
spec:
  backoffLimit: 2
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: upload
          image: amazon/aws-cli:latest
          command: ["sh", "-c"]
          args:
            - |
              set -e
              for dataset in users orders; do
                aws s3 cp "/data/$dataset.csv" "s3://$LAB_BUCKET_NAME/bronze/$dataset.csv"
              done
              aws s3 ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive
          envFrom:
            - configMapRef:
                name: s3-config
            - secretRef:
                name: s3-credentials
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              memory: 256Mi
          volumeMounts:
            - name: datasets
              mountPath: /data
              readOnly: true
      volumes:
        - name: datasets
          configMap:
            name: datasets
EOF

kubectl apply -f job-upload-bronze.yaml
kubectl wait --for=condition=complete job/upload-bronze --timeout=120s
kubectl logs job/upload-bronze
```

Verify the result from the terminal:

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive
#> 2026-09-14 11:02:10     331568 bronze/orders.csv
#> 2026-09-14 11:02:09       7351 bronze/users.csv
```

Questions:

- Why are the credentials stored in a Secret and not in the ConfigMap?
- The credentials of Onyxia are temporary. What happens if the Job is executed again tomorrow? How would a production
  platform provide credentials to a Job?
- How would you turn this Job into a daily ingestion?

Once verified, remove the Kubernetes objects. The objects of the bronze layer are kept for the next modules.

```bash
kubectl delete -f job-upload-bronze.yaml
kubectl delete configmap datasets s3-config
kubectl delete secret s3-credentials
```
