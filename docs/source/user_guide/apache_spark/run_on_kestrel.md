# How to Run dsgrid on Kestrel

This guide explains how to run dsgrid on NREL's Kestrel HPC system.

## Steps

### 1. Start a Screen Session

SSH to a login node and start a screen session (or similar tool like tmux):

```bash
screen -S dsgrid
```

This allows you to maintain your session even if you disconnect.

### 2. Install dsgrid

Follow the installation instructions at [Installation](../../getting_started/installation).

### 3. Create Runtime Config

Create a dsgrid runtime config file pointing to the shared registry:

```bash
dsgrid config create sqlite:////projects/dsgrid/standard-scenarios.db -N standard-scenarios
```

This configures dsgrid to use the NREL shared registry database.

### 4. Start a Spark Cluster

Start a Spark cluster with your desired number of compute nodes by following the instructions at [Start Spark Cluster on Kestrel](spark_cluster_on_kestrel).

### 5. Run dsgrid Commands

Run all CPU-intensive dsgrid commands from the first node in your HPC allocation using spark-submit:

```bash
spark-submit --master=spark://$(hostname):7077 $(which dsgrid-cli.py) [command] [options] [args]
```

**Examples:**

Register a dataset:
```bash
spark-submit --master=spark://$(hostname):7077 $(which dsgrid-cli.py) \
    registry datasets register dataset.json5 \
    -l "Register my dataset"
```

Run a query:
```bash
spark-submit --master=spark://$(hostname):7077 $(which dsgrid-cli.py) \
    query project run query.json5
```

### 6. Resume After Disconnect

Because you started a screen session at the beginning, if you disconnect from your SSH session for any reason, you can pick your work back up:

1. SSH to the same login node you used initially
2. Resume your screen session:

```bash
screen -r dsgrid
```

## Tips

- **Use screen or tmux**: Long-running jobs benefit from persistent sessions
- **Monitor jobs**: Use `squeue -u $USER` to check job status
- **Check logs**: Review Spark driver and executor logs for debugging
- **Adjust resources**: Scale your Spark cluster based on data size

## Common Commands

List available registries:
```bash
dsgrid registry list
```

Check project details:
```bash
dsgrid registry projects show <project-id>
```

Validate a config file:
```bash
dsgrid config validate dataset.json5
```

## Next Steps

- Learn how to [start a Spark cluster on Kestrel](spark_cluster_on_kestrel)
- Follow the [query project tutorial](../tutorials/query_project)
- Understand [Apache Spark configuration](spark_overview)
