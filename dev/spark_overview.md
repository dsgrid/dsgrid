# Apache Spark Overview
This page describes Spark concepts that are important to understand when using dsgrid.

[Spark Overview](#spark-overview) |
[Conventions](#conventions) |
[Installing a Spark Standalone Cluster on Your Laptop](#installing-a-spark-standalone-cluster-on-your-laptop) |
[Installing a Spark Standalone Cluster on an HPC](#installing-a-spark-standalone-cluster-on-an-hpc) |
[Run pyspark through IPython or Jupyter](#run-pyspark-through-ipython-or-jupyter) |
[Creating a SparkSession with dsgrid](#creating-a-sparksession-with-dsgrid) |
[Spark Configuration Problems](#spark-configuration-problems)

**Windows users**: Spark does not offer a great user experience in Windows. While `pyspark` and
`spark-submit` work in local mode, running a cluster requires manual configuration. The developers
provide cluster management scripts in bash, and so they do not work in Windows. When running a
cluster on your laptop we recommend that you use dsgrid in a Windows Subsystem for Linux (WSL2)
environment instead of a native Windows environment.

## Conventions
This page uses the UNIX conventions for environment variables and running commands in
a shell. If you do run any of these commands in a native Windows environment, you will have to
adjust depending on whether you are running PowerShell or the old Command shell.

Explanation of bash functionality used here:
- `$VARIABLE_NAME` or `${VARIABLE_NAME}`: This uses the environment variable `VARIABLE_NAME`. If
the variable isn't defined, bash will use an empty string.
- `export VARIABLE_NAME=x`: This sets the environment variable `VARIABLE_NAME` to the value `x` in
the current shell. If you want to set this variable for all future shells as well, add the
statement to your shell rc file (`$HOME/.bashrc` or `$HOME/.zshrc`).
- `export VARIABLE_NAME=$(hostname)`: This sets the environment variable `VARIABLE_NAME` to the
string returned by the command `hostname`.


## Spark Overview

### Cluster Mode
Apache provides an overview at https://spark.apache.org/docs/3.3.1/cluster-overview.html

The important parts to understand are how dsgrid uses different cluster modes in different
environments.

- Local computer in local mode: All Spark components run in a single process. This is
  great for testing and development but is not performant. It will not use all CPUs on
  your system. You run in this mode when you type `pyspark` in your terminal.

- Local computer with a standalone cluster: You must install Spark and then manually
  start the cluster. Refer to the [installation
  instructions](#installing-a-spark-standalone-cluster-on-your-laptop). This
  enables full performance on your system. It also allows you to debug your
  jobs in the Spark UI before or after they complete.

- HPC compute node in local mode: Use this only for quick checks. Same points above for local
  computer in local mode apply. Create a standalone cluster for real work.

- HPC compute node(s) with a standalone cluster: Create a cluster on any number of compute nodes
  and then use all CPUs for your jobs. Refer to the [installation
  instructions](#installing-a-spark-standalone-cluster-on-an-hpc).

- AWS EMR cluster: The EMR scripts in the dsgrid repo at `/emr` will create a Spark cluster on EC2
  compute nodes with a cluster manager. The cluster managers allow multiple users to access a
  single cluster and offer better job scheduling. Refer to the README.md in that directory.


### Run Spark Applications
There are three ways of running Spark applications in Python. `spark-submit` and `pyspark`,
provided by the Spark and pyspark installations, are recommended because they allow you to fully
customize the execution environment. More details follow
[below](#tuning-spark-configuration-settings).

1. `spark-submit`
This will create a SparkSession, make that session available to the Python process, and run your
code.
```
$ spark-submit [options] your_script.py [your_script_options]
```
2. `pyspark`
This will create a SparkSession, make that session available to the Python process, and leave you
in the Python interpreter for an interactive session.
```
$ pyspark [options]
```

3. Inside Python
```
$ python
>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder.appName("your_app_name").getOrCreate()
```

### Spark UI
The Spark master starts a web application at http://<master_hostname>:8080. The worker is available
at port 4040. You can monitor and debug all aspects of your jobs in this application. You can also
inspect all cluster configuration settings.

If your Spark cluster is running on a remote system, like an HPC, you may need to open an ssh tunnel to
the master node. Here is how to do that on NREL's Eagle cluster.

On your laptop:
```
$ export COMPUTE_NODE=<compute_node_name>
$ ssh -L 4040:$COMPUTE_NODE:4040 -L 8080:$COMPUTE_NODE:8080 $USER@eagle.hpc.nrel.gov
```


## Installing a Spark Standalone Cluster on your laptop
Download your desired version from https://spark.apache.org/downloads.html and extract it on
your system.

**Note**: Choose the version that is set in `dsgrid/setup.py` for `pyspark`. At least the minor
versions must match.

Here is an example:
```
$ cd <your-preferred-base-directory>  # directions below assume $HOME
$ wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
$ tar -xzf spark-3.3.1-bin-hadoop3.tgz && rm spark-3.3.1-bin-hadoop3.tgz
```

The full instructions to create a cluster are at
http://spark.apache.org/docs/latest/spark-standalone.html. The rest of this section documents a
limited set of instructions that should work on your system.

### Set environment variables

```
$ export SPARK_HOME=$HOME/spark-3.3.1-bin-hadoop3
$ export PATH=$PATH:$SPARK_HOME/sbin
```

Note that after doing this your system will have two versions of `pyspark`:
- In your Python virtual environment where you installed dsgrid (because dsgrid installs pyspark)
- In `$HOME/spark-3.3.1-bin/hadoop3/bin`

If you use a conda virtual environment, when that environment is activated, its `pyspark` will be
in your system path. Be sure not to add the spark bin directory to your path so that there are no
collisions.

**Warning**: Setting `SPARK_HOME` will affect operation of your Python `pyspark` installation in
local mode. That may not be what you want if you make settings specific to the standalone cluster.

### Customize Spark configuration settings
```
$ cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
$ cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
```
Set `spark.driver.memory` and `spark.driver.maxResultSize` in `spark-defaults.conf` to the maximum
data sizes that you expect to pull from Spark to Python, such as if you call `df.toPandas()`.
`1g` is probably reasonable.

Set `spark.sql.shuffle.partitions` to 1-4x the number of cores in your system.

Set `spark.executor.cores` and `spark.executor.memory` to numbers that allow creation of your
desired number of executors. Spark will try to create the most number of executors such that each
executor has those resources. For example, if your system has 16 cores and you assign 16g of memory
to the worker (see below), `spark.executor.cores 3` and `spark.executor.memory 5g` will result in 3
executors.

### Start the Spark processes
```
$SPARK_HOME/sbin/start-master.sh
```
Start a worker with this command. Give the worker as much memory as you can afford. Executor memory
comes from this pool. You can also configure this in `spark-env.sh`.
```
$SPARK_HOME/sbin/start-worker.sh -m 16g spark://$(hostname):7077
```

If you add the `sbin` to your `PATH` environment variable, here is a one-liner:
```
$ start-master.sh && start-worker.sh -m 24g spark://$(hostname):7077
```

### Stop the Spark processes
Stop all of the processes when you complete your work.
```
$ stop-worker.sh && stop-master.sh
```

## Installing a Spark Standalone Cluster on an HPC
This section describes how you can run Spark jobs on any number of HPC compute nodes.
The scripts and examples described here rely on the SLURM scheduling system and have been tested
on NREL's Eagle cluster.

NREL's HPC GitHub repository contains scripts that will create an ephemeral Spark cluster
on compute nodes that you allocate.

1. The repository has setup instructions that are not currently correct because the branch
   is still under review. Follow
   https://github.com/daniel-thom/HPC/blob/apache-spark-clusters/applications/spark/README.md
   except substitute step 1 with the steps below.

   Secondly, the README has generic instructions to run Spark in a variety of ways. The rest
   of this section gives specific guidance on how to apply those instructions to use dsgrid with
   a Spark cluster in an interactive session.
```
$ git clone https://github.com/daniel-thom/HPC.git
$ cd HPC
$ git fetch origin apache-spark-clusters
$ git checkout apache-spark-clusters
```

2. You probably want to choose compute node(s) with fast local storage. This example will work.
```
$ salloc -t 01:00:00 -N1 --account=dsgrid --partition=debug --mem=730G
```
Given that this example uses `salloc`, once SLURM allocates a node it will log you into the
compute node. If you choose more than one node, you will be logged into the node that will be used
as the Spark master node.

Notes:
- NREL's Eagle cluster will let you allocate two debug jobs each with two nodes. So, you can use
  these scripts to create a four-node cluster for one hour.
- If the debug partition is not too full, you can append `--qos=standby` to the command above
  and not be charged any AUs.

3. Add the setup scripts to your path.
```
$ export PATH=$PATH:<your-repo-path>/HPC/applications/spark/spark_scripts
```

4. Select a Spark container compatible with dsgrid, which currently requires Spark v3.3.x and
   Python 3.10. The team has validated the container below. It was created with this Dockerfile
   in dsgrid: `docker/spark/Dockerfile`. The container includes ipython, jupyter, pyspark, pandas,
   and pyarrow, but not dsgrid.
```
$ create_config.sh -c `/projects/dsgrid/containers/spark_py310.sif`
```

5. Customize the Spark configuration files in `./conf` as necessary per the HPC instructions.

6. Start the cluster.
```
$ start_spark_cluster.sh
```
The cluster URL will be `spark://$(hostname):7077`.

7. Activate your dsgrid conda environment. You will connect to the cluster with the `pyspark`
   package in that environment.


## Run pyspark through IPython or Jupyter
You can configure `pyspark` to start `IPython` or `Jupyter` instead of the standard Python
interpreter by setting the environment variables `PYSPARK_DRIVER_PYTHON` and
`PYSPARK_DRIVER_PYTHON_OPTS`.

### IPython
```
$ export PYSPARK_DRIVER_PYTHON=ipython
# local mode
$ pyspark
# cluster mode
$ pyspark --master=spark://$(hostname):7077
```
Now you are in IPython instead of the standard Python interpreter.

### Jupyter
```
$ export PYSPARK_DRIVER_PYTHON=jupyter
$ export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8889 --ip=0.0.0.0"
# local mode
$ pyspark
# cluster mode
$ pyspark --master=spark://$(hostname):7077
```
Pyspark will start a Jupyter notebook and you'll see the URL printed in the terminal. If you're on
a remote server, like in an HPC environment, you'll need to create an ssh tunnel in order to
connect in a browser.

Once you connect in a brower, enter the following in a cell in order to connect to this cluster:
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("your_app_name").getOrCreate()
```

## Tuning Spark Configuration Settings
In general you want to run Spark with as many executors as possible on each worker node. The
Amazon orchestration software along with the cluster manager *may* take care of that when
running on AWS (you will still have to adjust `spark.sql.shuffle.partitions`). You will have to
perform more customizations when running a standalone cluster on your laptop or an HPC.

There are multiple ways of setting parameters. These are listed in order of priority - later
methods will override the earlier methods when allowed.

1. Global Spark configuration directory: This is `$SPARK_HOME/conf` or `$SPARK_CONF_DIR`.
   You can customize settings in `spark-defaults.conf` and `spark-env.sh`. Make customizations
   here if you will use the same settings in all jobs.

2. Spark launch scripts: Use `spark-submit` to run scripts. Use `pyspark` to run interactively.
   Both scripts offer the same startup options. You can choose to run in local mode or attach to
   a cluster. You can override any setting from #1. Make changes here if you will use different
   settings across jobs. Note that some settings must be made before the Spark JVM starts, like
   `spark.driver.memory`, and so this is your last chance to customize those values.

3. SparkSession construction inside a Python process: You can customize things like executor
   settings when you construct the `SparkSession` in Python. For example, this code block will
   create a session where the job starts a single executor with a single core that uses all
   available memory.
```
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("my_app")
conf.set("spark.executor.cores", 1)
conf.set("spark.executor.memory", "16g")
conf.setMaster(cluster)
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

4. Dynamic changes: You can make changes to a limited number of settings at runtime.
   You can't change the number of executor cores because those have already been allocated. You
   can change the number of shuffle partitions that Spark will use. You may want to change that
   value if the sizes of the dataframes you're working on change dramatically.
```
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.shuffle.partitions", 500)
```

## Creating a SparkSession with dsgrid
Ensure that the dsgrid software uses the cluster with optimized settings. If you start the dsgrid
Python process with the Spark scripts `spark-submit` or `pyspark` and set the `--master` option,
those scripts will create a SparkSession attached to the cluster and pass it to the Python process.

You can optionally set the `SPARK_CLUSTER` environment variable to the cluster URL and then dsgrid
will connect to it.
```
$ export SPARK_CLUSTER=spark://$(hostname):7077
```

Using `SPARK_CLUSTER` is a bit simpler, but you cannot configure settings like
`spark.driver.memory`, which, as stated earlier, must be set before the JVM is created.

### spark-submit
Running dsgrid CLI commands through `spark-submit` requires cumbersome syntax because the tool
needs to

1. Detect that the script is Python (which is why this example uses dsgrid-cli.py instead of dsgrid).
2. Know the full path to the script (accomplished with the utility `which`).

Here's how to do that:

```
$ spark-submit --master spark://$(hostname):7077 \
    $(which dsgrid-cli.py) \
    query project run \
    --offline \
    --registry-path=/scratch/${USER}/.dsgrid-registry \
    query.json
```


## Spark Configuration Problems
Get used to monitoring Spark jobs in the Spark UI. The master is at `http://<master_hostname>:8080`
and the worker is at `http://<master_hostname>:4040`. If a job seems stuck or slow, explore why.
Then kill the job, make config changes, and retry. A misconfigured job will take too long or never
finish.

This section explains some common problems.

### spark.sql.shuffle.partitions
The most common performance issue we encounter when running complex queries is due to a non-ideal
setting for `spark.sql.shuffle.partitions`. The default Spark value is 200. Some online sources
recommend setting it to 1-4x the total number of CPUs in your cluster. This
[video](https://www.youtube.com/watch?v=daXEp4HmS-E&t=4251s) by a Spark developer offers a
recommendation that has worked out better.

Use this formula:
```
num_partitions = max_shuffle_write_size / target_partition_size
```
You will have to run your job once to determine `max_shuffle_write_size`. You can find it on the
Spark UI Stages tab in the Shuffle Write column. Your `target_partition_size` should be between
128 - 200 MB.

The minimum partitions value should be the total number of cores in the cluster unless you want to
leave some cores available for other jobs that may be running simultaneously.

### Dynamic allocation
This feature is disabled by default on standalone clusters. It is described
[here](https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation).
This section does not necessarily line up with the Spark documentation and will change as we learn
more.

We have observed two cases where enabling dynamic allocation on a standalone cluster significantly
improves performance:

- When a Spark job produces a huge number of tasks. This can happen when checking dimension
associations as well as queries that map a dataset to project dimensions.
- When there is really only enough work for one executor but Spark distributes it to all executors
anyway. The inter-process communication adds tons of overhead. The executors also appear to do a
lot more work. With dynamic allocation Spark gradually adds executors and these problems don't
always occur.

The second issue sometimes occurs when submitting a dataset to a project in the registry.
We recommend enabling dynamic allocation when doing this. If that is enabled then dsgrid code will
reconfigure the SparkSession to use a single executor with one core.

Set these values in your `spark-defaults.conf`. `spark.shuffle.service.enabled` must be set before
you start the workers.
```
spark.dynamicAllocation.enabled true
spark.dynamicAllocation.shuffleTracking.enabled true
spark.shuffle.service.enabled true
```

We have not observed any downside to having this feature enabled.

### Slow local storage
Spark will write lots of temporary data to local storage during shuffle operations. If your joins
cause lots of shuffling, it is very important that your local storage be fast. If you use an Eagle
node with a slow spinning disk (130 MB/s), your job will likely fail. Use a `bigmem` or `gpu`
node. They have SSDs that can write data at 2 GB/s.

### Too many executors are involved in a job
Some of our Spark jobs, particularly those that create large, in-memory tables from dimension
records and mappings, perform much better when there is only one executor and only one CPU. If
you think this is happening then set these values in your `spark-default.confg`:
```
spark.executor.cores 1
# This value should be greater than half of the memory allocated to the Spark workers, which
# will ensure that Spark can only create one executor.
spark.executor.memory 15g
```
One common symptom of this type of problem is that a job run in local mode works better than in a
standalone cluster. The likely reason is that the standalone cluster has many executors and local
mode only has one.

### A table has partitions that are too small or too large.
Spark documentation recommends that Parquet partition files be in the range of 100-200 MiB, with
128 MiB being ideal. A very high compression ratio may change that. This affects how much data
each task reads from storage into memory.

Check your partition sizes with this command:
```
$ find <path_to_top_level_data.parquet> -name "*.parquet" -exec ls -lh {} +
```

Check the total size with this command:
```
$ du -sh <path_to_top_level_data.parquet>
```

Divide the total size by the target partition size to get the desired number of partitions.

If there are too few partitions currently:
```
df.repartition(target_partitions).write.parquet("data.parquet")
```

If there are too many partitions currently:
```
df.coalesce(target_partitions).write.parquet("data.parquet")
```

If you are partitioning by a column and find that there are many very small partition files,
repartition like this:
```
df.repartition(column_name).write.partitionBy(column_name).parquet("data.parquet")
```

### Skew
We have not yet experienced problems with data skew, but expect to. It is covered by many online
sources.

If your query will produce high data skew, such as can happen with a query that produces results
from large and small counties, you can use a salting technique to balance the data. For example,

```
df.withColumn("salt_column", F.lit(F.rand() * (num_partitions - 1))) \
    .groupBy("county", "salt_column") \
    .agg(F.sum("county")) \
    .drop("salt_column")
```
