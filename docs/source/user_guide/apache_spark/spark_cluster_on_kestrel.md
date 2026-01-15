# How to Start a Spark Cluster on Kestrel

This guide explains how to start an Apache Spark cluster on NREL's Kestrel HPC system for running dsgrid queries.

## Prerequisites

Install the Python package `sparkctl` - a tool for managing Spark clusters on HPC systems:

```bash
pip install "sparkctl[pyspark]"
```

Refer to the [sparkctl documentation](https://nrel.github.io/sparkctl/) for more details.

## Compute Node Types

Spark works best with fast local storage. The standard Kestrel nodes do not have any local storage. The best candidates are the **256 standard nodes (no GPUs) with 1.92 TB NVMe M.2 drives**.

Please refer to the [Kestrel system configuration page](https://www.nrel.gov/hpc/kestrel-system-configuration.html) for specific hardware information. The GPU nodes will work as well, but at a greater cost in AUs.

:::{tip}
If those nodes are not available, you may be able to complete your queries by using the standard nodes and specifying a path on the Lustre filesystem in the Spark configuration file `conf/spark-env.sh`. Change `SPARK_LOCAL_DIRS` and `SPARK_WORKER_DIR`.
:::

## Steps

### 1. Create a Work Directory

From the HPC login node, create a work directory somewhere in `/scratch/$USER`:

```bash
cd /scratch/$USER
mkdir dsgrid-work
cd dsgrid-work
```

### 2. Allocate Compute Nodes

Request one or more nodes from the SLURM scheduler. Adjust the parameters based on your needs:

```bash
salloc -t 01:00:00 -N1 --account=dsgrid --partition=debug --tmp=1600G --mem=240G
```

**Parameter guide:**
- `-t 01:00:00`: Time limit (1 hour in this example)
- `-N1`: Number of nodes (1 in this example; increase for larger datasets)
- `--account=dsgrid`: Your allocation account
- `--partition=debug`: Queue partition (use `standard` for longer jobs)
- `--tmp=1600G`: Local scratch space (use with NVMe nodes)
- `--mem=240G`: Memory per node

### 3. Configure and Start the Cluster

Configure the Spark settings and start the cluster:

```bash
sparkctl configure --start
```

Run `sparkctl --help` to see all available options.

### 4. Set Environment Variables

Set the Spark configuration and Java environment variables:

```bash
export SPARK_CONF_DIR=$(pwd)/conf
export JAVA_HOME=/datasets/images/apache_spark/jdk-21.0.7
```

### 5. Verify Cluster is Running

The Spark cluster is now ready to use at `spark://$(hostname):7077`.

You can verify it's running by checking the Spark master UI:
```bash
echo "Spark Master UI: http://$(hostname):8080"
```

Run all query scripts from this node using spark-submit as described in [Run dsgrid on Kestrel](run_on_kestrel).

## Example: Multi-Node Cluster

For larger datasets, allocate multiple nodes:

```bash
# Allocate 4 nodes for 2 hours
salloc -t 02:00:00 -N4 --account=dsgrid --partition=standard --tmp=1600G --mem=240G

# Configure and start (sparkctl detects all allocated nodes)
sparkctl configure --start

# Set environment
export SPARK_CONF_DIR=$(pwd)/conf
export JAVA_HOME=/datasets/images/apache_spark/jdk-21.0.7

# Run dsgrid query
spark-submit --master=spark://$(hostname):7077 $(which dsgrid-cli.py) query project run query.json5
```

## Configuration Tips

### Adjust Spark Partitions

For better performance with large datasets, set the number of shuffle partitions:

```bash
spark-submit --master=spark://$(hostname):7077 \
    --conf spark.sql.shuffle.partitions=2400 \
    $(which dsgrid-cli.py) query project run query.json5
```

Rule of thumb: Use 2-4x the number of CPU cores across your cluster.

### Memory Settings

If you encounter out-of-memory errors, adjust executor memory:

```bash
spark-submit --master=spark://$(hostname):7077 \
    --executor-memory 50g \
    --driver-memory 50g \
    $(which dsgrid-cli.py) query project run query.json5
```

## Troubleshooting

### Cluster Won't Start

- Check that your allocation is active: `squeue -u $USER`
- Verify Java is available: `java --version`
- Review logs in `./logs/` directory

### Out of Memory Errors

- Increase `--mem` when allocating nodes
- Add more nodes with `-N`
- Reduce data processed per partition

### Slow Performance

- Use nodes with local NVMe storage
- Increase shuffle partitions for better parallelism
- Review Spark UI (port 8080) for task distribution

## Cleaning Up

When finished, stop the Spark cluster and release your allocation:

```bash
sparkctl stop
exit  # Exit salloc session
```

## Next Steps

- Learn how to [run dsgrid on Kestrel](run_on_kestrel)
- Understand [Spark configuration options](spark_overview)
- Follow the [query project tutorial](../tutorials/query_project)
