# Lab: Essential Linux Commands for big data

## Objectives

By the end of this lab, you will be able to:

- Navigate and manage the filesystem (`mkdir`, `ls`, `cat`, `head`, `tail`, `cp`, `mv`)
- Search and filter content (`grep`)
- Apply these skills to Kubernetes cluster operations

## Environment

- Local terminal (Linux, macOS, or WSL on Windows)
- A working directory: `~/data-platform-lab/` (you may create this directory through Files, Finder, or File Explorer)

## `mkdir`: Create directories

- Syntax: `mkdir [options] directory_name`

- Common options:

  - `-p` : Create parent directories as needed (don't fail if they exist)

```bash
# Execute the command in the terminal
cd ~/data-platform-lab

# Create a simple directory
mkdir bronze

# Create nested directories in one command
mkdir -p data/silver/customers
mkdir -p data/gold/metrics
```

## `ls`: List directory contents

- Syntax: `ls [options] [path]`

- Common options:

  - `-l` : Long format (permissions, owner, size, date)
  - `-a` : Show hidden files (starting with `.`)
  - `-h` : Human-readable file sizes (K, M, G)
  - `-R` : Recursive (show subdirectories)

```bash
# Verify the directories created before
ls -lR .
ls -lha .
```

## `cat`: Display file contents & create files

- Syntax:
  - `cat [file]`
  - `cat > filename`

### Create files

```bash
# Create a sample data file
cat > data/bronze/customers.csv << 'EOF'
id,name,email,created_at
1,Alice Johnson,alice@example.com,2024-01-15
2,Bob Smith,bob@example.com,2024-02-20
3,Carol White,carol@example.com,2024-03-10
EOF
```

### Display files

```bash
cat data/bronze/customers.csv
```

## `head`: View the beginning of a file

- Syntax: `head [options] [file]`

- Common options:
  - `-n N` : Show first N lines (default: 10)

```bash
# Create a log file
cat > data/bronze/spark_log.txt << 'EOF'
2024-01-15T10:23:45 INFO Starting Spark job
2024-01-15T10:23:46 INFO Initializing RDD
2024-01-15T10:23:47 DEBUG Partition 1 assigned to worker-1
2024-01-15T10:23:48 DEBUG Partition 2 assigned to worker-2
2024-01-15T10:24:00 INFO Map phase complete
2024-01-15T10:24:15 INFO Shuffle phase complete
2024-01-15T10:24:30 INFO Reduce phase complete
2024-01-15T10:24:45 INFO Job succeeded
2024-01-15T10:24:46 INFO Output written to HDFS
2024-01-15T10:24:47 INFO Cleaning up
EOF

# View first 5 lines
head -n 5 data/bronze/spark_log.txt

# Default (first 10 lines)
head data/bronze/spark_log.txt
```

## `tail`: View the end of a file

- Syntax: `tail [options] [file]`

- Common options:

  - `-n N` : Show last N lines (default: 10)
  - `-f` : **Follow mode** (stream new lines as they're added—useful for live logs!)

```bash
# View last 3 lines
tail -n 3 data/bronze/spark_log.txt

# Simulate a live log (in one terminal, run in background)
tail -f data/bronze/spark_log.txt

# In another terminal, append logs
sleep 2 && echo "2024-01-15T10:25:00 INFO New event" >> data/bronze/spark_log.txt
sleep 1 && echo "2024-01-15T10:25:01 INFO Another event" >> data/bronze/spark_log.txt

# Kill the tail -f process (Ctrl+C)
```

## `cp`: Copy files and directories

- Syntax: `cp [options] source destination`

- Common options:

  - `-r` : Recursive (copy directories and their contents)
  - `-v` : Verbose (show what's being copied)

```bash
# Copy a single file
cp data/bronze/customers.csv data/bronze/customers_archive.csv
ls -l data/bronze/

# Copy an entire directory tree
cp -r data/bronze data/bronze_backup
ls -lR data/bronze_backup/

# Verbose copy
cp -v data/bronze/spark_log.txt data/silver/spark_log.txt
```

## `mv`: Move or rename files

- Syntax: `mv source destination`

```bash
# Rename a file
mv data/silver/spark_log.txt data/silver/spark_log_archive.txt
ls -l data/silver/

# Move a file to a different directory
mv data/bronze/customers_archive.csv data/silver/
ls -l data/silver/
```

## `grep`: Search for patterns (essential for logs!)

- Syntax: `grep [options] pattern [file]`

- Common options:

  - `-i` : Case-insensitive
  - `-n` : Show line numbers
  - `-c` : Count matching lines
  - `-A N` : Show N lines after match
  - `-B N` : Show N lines before match

```bash
# Count errors in a log
grep -c "ERROR" data/bronze/spark_log.txt

# Search with line numbers (case-insensitive)
grep -in "info" data/bronze/spark_log.txt

# Show context: 1 line before and after matches
grep -B1 -A1 "phase complete" data/bronze/spark_log.txt
```

## Bonus

The following exercises will require an active Kubernetes cluster up and running.

```bash
# Get pod logs and filter for errors
kubectl logs pod-name | grep "ERROR"

# Tail live pod logs (like tail -f)
kubectl logs -f pod-name

# Search all pod events
kubectl get events | grep "CrashLoopBackOff"

# Find all files in a pod's mounted volume
find /mnt/data -name "*.log" | head -20

# Count how many files in each directory
find data/ -type d | while read dir; do echo "$dir: $(ls $dir | wc -l)"; done
```

## Further practice

Check out this tutorial: [The Linux command line for beginners](https://ubuntu.com/tutorials/command-line-for-beginners#1-overview) for more information and practice.
