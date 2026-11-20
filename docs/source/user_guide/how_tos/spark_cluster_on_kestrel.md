# How to Start a Spark Cluster on Kestrel

This guide explains how to start an Apache Spark cluster on NLR's Kestrel HPC system for running dsgrid queries.

## Prerequisites

Install the Python package `sparkctl` - a tool for managing Spark clusters on HPC systems - into the same Python environment (venv or conda) as dsgrid:

```bash
pip install "sparkctl[pyspark]"
```

Refer to the [sparkctl documentation](https://natlabrockies.github.io/sparkctl/) for more details.

### One-Time sparkctl Setup

Before sparkctl can start a cluster, it needs to know where the Spark and Java binaries are. Kestrel provides them in `/datasets/images/apache_spark/`. Run this once (it can be done on a login node):

```bash
sparkctl default-config \
    /datasets/images/apache_spark/spark-4.1.1-bin-hadoop3 \
    /datasets/images/apache_spark/jdk-21.0.7 \
    -e slurm
```

This command writes `~/.sparkctl.toml`, which every later `sparkctl` command reads. Use `-d <directory>` to write the file somewhere else (sparkctl also checks the current directory). Rerun the command if you change Spark versions.

:::{tip}
Choose the Spark directory whose version matches the `pyspark` package in your environment (`pip show pyspark`). Run `ls /datasets/images/apache_spark/` to see which versions are available.
:::

:::{note}
The `-e slurm` option (`--compute-environment`) tells sparkctl to find the allocated nodes and their CPU and memory through Slurm environment variables and to start workers with `srun`. It is the default, so you can leave it out on Kestrel. The other choice, `native`, is for machines without a scheduler, where you list the worker nodes yourself.
:::

If you skip this step, `sparkctl configure` fails with `No sparkctl binary paths are configured`.

## Compute Node Types

Spark works best with fast local storage. The standard Kestrel nodes do not have any local storage. The best candidates are the **256 standard nodes (no GPUs) with 1.92 TB NVMe M.2 drives**.

Please refer to the [Kestrel system configuration page](https://www.nlr.gov/hpc/kestrel-system-configuration.html) for specific hardware information. The GPU nodes will work as well, but at a greater cost in AUs.

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

`salloc` opens a new shell on the compute node, so activate your Python environment there first, then configure the Spark settings and start the cluster:

```bash
source /path/to/your/.venv/bin/activate   # or: conda activate <env>
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

The Spark cluster is now ready to use at `spark://$(hostname):7077`. The Spark master runs on this node (the first node of your allocation) and serves its web UI on port 8080. Get the node name:

```bash
hostname
```

Compute nodes are not reachable directly from your laptop, so open an SSH tunnel to that node by following the [SSH tunnel instructions](../apache_spark/overview.md#ssh-tunnel-for-remote-access), then browse to `http://localhost:8080`. In the master UI, check that **Alive Workers** equals the number of nodes you allocated (`-N`).

### 6. Run dsgrid Commands

Run dsgrid commands from this same node with `spark-submit`, in the shell where your Python environment is active and the Step 4 variables are set:

```bash
spark-submit --master=spark://$(hostname):7077 $(which dsgrid-cli.py) query project run query.json5
```

While a command is running, you can follow its progress in the application UI at `http://localhost:4040` (through the same SSH tunnel). See [Run dsgrid on Kestrel](run_on_kestrel) for more examples.

### 7. Stop the Cluster and Release the Allocation

When finished, stop the Spark cluster and release your allocation:

```bash
sparkctl stop
exit  # Exit salloc session
```

Also close the SSH tunnel session on your laptop.

## Example: Multi-Node Cluster

For larger datasets, allocate multiple nodes:

```bash
# Allocate 4 nodes for 2 hours
salloc -t 02:00:00 -N4 --account=dsgrid --partition=standard --tmp=1600G --mem=240G

# Activate your environment, then configure and start (sparkctl detects all allocated nodes)
source /path/to/your/.venv/bin/activate
sparkctl configure --start

# Set environment
export SPARK_CONF_DIR=$(pwd)/conf
export JAVA_HOME=/datasets/images/apache_spark/jdk-21.0.7

# Run dsgrid query
spark-submit --master=spark://$(hostname):7077 $(which dsgrid-cli.py) query project run query.json5

# Stop the cluster and release the allocation
sparkctl stop
exit
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

### `sparkctl: command not found`

Your Python environment is not active in the current shell. Activate the environment where you installed sparkctl (for example, `source /path/to/your/.venv/bin/activate`).

### `No sparkctl binary paths are configured`

sparkctl doesn't know where Spark and Java are installed. Run the [one-time setup](#one-time-sparkctl-setup) command `sparkctl default-config`.

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

## Next Steps

- Learn how to [run dsgrid on Kestrel](run_on_kestrel)
- Understand [Spark configuration options](../apache_spark/overview)
- Follow the [query project tutorial](../tutorials/query_project)
