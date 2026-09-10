# Lab

Explore Kubernetes Job & CronJob for task/compute execution.

## Objectives

By the end of this lab, you will be able to:

- Understand the difference between Job and CronJob
- Create and monitor Kubernetes Jobs
- Schedule recurring tasks with CronJob
- Apply Jobs and CronJobs to data pipeline scenarios (batch ETL, backups)
- Handle job failures and retries

## Prerequisites

- A running Kubernetes cluster (minikube)
- Familiarity with Pods and basic `kubectl` commands

## Job vs CronJob

Job: Runs a container to completion, retries on failure, stops when done.  
CronJob: Schedules Jobs using cron expressions (`0 2 * * *` = 2 AM daily).

Check [crontab guru](https://crontab.guru/examples.html) for more other ways to manipulate cron.

## Setup

Create lab namespace and directory.

```bash
kubectl create namespace lab-job-cronjob
mkdir lab-job-cronjob && cd lab-job-cronjob
```

## 1. Creating a Simple Job

Simple Job that run few Python print commands.

```bash
cat > job-simple.yaml << 'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: data-import-job
  namespace: lab-job-cronjob
spec:
  template:
    spec:
      containers:
      - name: import
        image: python:3.9
        command: ["python", "-c"]
        args:
          - |
            import time
            print("Starting data import...")
            time.sleep(3)
            print("Processing 1000 records...")
            print("Import completed!")
      restartPolicy: Never
EOF

kubectl apply -f job-simple.yaml
kubectl -n lab-job-cronjob get job
kubectl logs -n lab-job-cronjob -l job-name=data-import-job
kubectl -n lab-job-cronjob wait --for=condition=complete job/data-import-job  --timeout=30s

# Job with retry logic
cat > job-with-retry.yaml << 'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: data-transform-job
  namespace: lab-job-cronjob
spec:
  backoffLimit: 3
  activeDeadlineSeconds: 60
  template:
    spec:
      containers:
      - name: transform
        image: python:3.9
        command: ["python", "-c"]
        args:
          - |
            import random
            if random.random() < 0.3:
              print("ERROR: Validation failed!")
              exit(1)
            print("Transformation completed!")
      restartPolicy: Never
EOF

kubectl apply -f job-with-retry.yaml
kubectl -n lab-job-cronjob get job data-transform-job -w
```

## 2. Job Parallelization

Launch a Job that run a single task in 4 different pods to verfiy the parallelism.

```bash
cat > job-parallel.yaml << 'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: batch-export-job
  namespace: lab-job-cronjob
spec:
  parallelism: 4      # Run 4 Pods in parallel
  completions: 4      # Need 4 successful completions
  backoffLimit: 2
  template:
    spec:
      containers:
      - name: export
        image: python:3.9
        command: ["python", "-c"]
        args:
          - |
            import os, time
            pod = os.getenv('HOSTNAME', 'unknown')
            print(f"Pod {pod}: Exporting shard...")
            time.sleep(5)
            print(f"Pod {pod}: Export completed!")
      restartPolicy: Never
EOF

kubectl apply -f job-parallel.yaml
kubectl -n lab-job-cronjob get pods -w  # Watch Pods run in parallel
```

## 3. Creating a CronJob

CronJob that runs every 2 minutes.

```bash
cat > cronjob-pipeline.yaml << 'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: bronze-to-silver-pipeline
  namespace: lab-job-cronjob
spec:
  schedule: "*/2 * * * *"
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: transform
            image: python:3.9
            command: ["python", "-c"]
            args:
              - |
                from datetime import datetime
                print(f"[{datetime.now()}] Bronze→Silver ETL started...")
                import time; time.sleep(2)
                print(f"[{datetime.now()}] Completed! 500 records processed")
          restartPolicy: Never
EOF

kubectl apply -f cronjob-pipeline.yaml
kubectl -n lab-job-cronjob get cronjob
kubectl -n lab-job-cronjob get jobs -w  # Watch Jobs created automatically
kubectl -n lab-job-cronjob logs -l job-name=<job-name> -f
```

Common cron schedules for data pipelines

```bash
# Cron format: minute hour day month weekday
# Example: Apply a nightly backup CronJob
cat > cronjob-backup.yaml << 'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: daily-backup
  namespace: lab-job-cronjob
spec:
  schedule: "0 2 * * *"  # 2 AM daily
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: python:3.9
            command: ["python", "-c", "print('Backup completed')"]
          restartPolicy: Never
EOF

kubectl apply -f cronjob-backup.yaml
```

## 4. Monitoring Jobs & CronJobs

```bash
# List all Jobs
kubectl -n lab-job-cronjob get jobs

# View Job status and events
kubectl -n lab-job-cronjob describe job data-import-job

# View logs from Job Pod
kubectl -n lab-job-cronjob logs -l job-name=data-import-job

# Suspend/resume a CronJob
kubectl -n lab-job-cronjob patch cronjob bronze-to-silver-pipeline -p '{"spec":{"suspend":true}}'
kubectl -n lab-job-cronjob patch cronjob bronze-to-silver-pipeline -p '{"spec":{"suspend":false}}'
```

## 5. Data Pipeline Scenario

```bash
cat > cronjob-etl.yaml << 'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: etl-bronze-to-silver
  namespace: lab-job-cronjob
spec:
  schedule: "*/5 * * * *"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      backoffLimit: 2
      template:
        spec:
          containers:
          - name: etl
            image: python:3.9
            env:
            - name: BATCH_SIZE
              value: "1000"
            command: ["python", "-c"]
            args:
              - |
                from datetime import datetime
                import os
                timestamp = datetime.now().isoformat()
                batch_size = os.getenv('BATCH_SIZE')
                print(f"[{timestamp}] ETL: Bronze→Silver")
                print(f"[{timestamp}] Batch size: {batch_size}")
                print(f"[{timestamp}] Reading, validating, deduplicating...")
                print(f"[{timestamp}] Writing to Silver layer...")
                print(f"[{timestamp}] Completed!")
          restartPolicy: Never
EOF

kubectl apply -f cronjob-etl.yaml
kubectl get cronjob -n lab-job-cronjob
kubectl get jobs -n lab-job-cronjob -w
```

## Teardown

```bash
# Delete all Jobs and CronJobs in the namespace
kubectl -n lab-job-cronjob delete job --all
kubectl -n lab-job-cronjob delete cronjob --all

# Delete the namespace
kubectl delete namespace lab-job-cronjob

# Verify deletion
kubectl get namespace lab-job-cronjob  # Should show "not found"

# Remove lab directory
cd .. && rm -r lab-job-cronjob
```
