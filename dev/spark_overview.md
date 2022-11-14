# Apache Spark Overview
This page describes Spark concepts that are important to understand when using dsgrid.

[Cluster Mode Overview](#cluster-mode-overview) |
[Run pyspark through iPython or Jupyter](#run-pyspark-through-ipython-or-jupyter) |
[Creating a SparkSession with dsgrid](#creating-a-sparksession-with-dsgrid) |
[Installing a Spark Standalone Cluster on Your Laptop](#installing-a-spark-standalong-cluster-on-your-laptop) |
[Installing a Spark Standalone Cluster on an HPC](#installing-a-spark-standalong-cluster-on-an-hpc) |
[Spark Configuration Problems](#spark-configuration-problems)

**Windows users**: Spark does not offer a great user experience in Windows.
The developers provide cluster management scripts in bash, and so they do not work in Windows.
We have experienced other installation headaches as well. We recommend that you use dsgrid in
a Windows Subsystem for Linux (WSL2) environment instead of a native Windows environment.
Furthermore, this page uses the UNIX conventions for environment variables and running
commands in a shell. If you do run any of these commands in a native Windows environment,
you will have to adjust depending on whether you are running PowerShell or the old Command
shell.


## Cluster Mode Overview
Apache provides an overview at https://spark.apache.org/docs/3.3.1/cluster-overview.html

The important parts to understand are how dsgrid uses different cluster modes in different
environments.

- Local computer in local mode: All Spark components run in a single process. This is
  great for testing and development but is not performant. It will not use all CPUs on
  your system. You run in this mode when you type `pyspark` in your terminal.

- Local computer with a standalone cluster: You must install Spark and then manually
  start the cluster. Refer to the installation instructions below. This enables full
  performance on your system. It also allows you to debug your jobs in the Spark UI before or after
  they complete.

- HPC compute node in local mode: In general this is not recommended. It's easy to create a
  standalone cluster. But, for some quick checks, it is fine. Run `pyspark` in the terminal
  and do your work. Just remember that you won't get to use all CPUs.

- HPC compute node(s) with a standalone cluster: Use the HPC repository scripts to create a
  cluster on any number of compute nodes and then use all CPUs for your jobs. Refer to the
  installation instructions.

- AWS EMR cluster: The scripts in the dsgrid repo will create a Spark cluster with a Yarn cluster
  manager. The YARN cluster manager allows multiple users to access a single cluster and offers
  better job scheduling.

## Run pyspark through iPython or Jupyter
Here is how to run all pyspark sessions through iPython or Jupyter.

### iPython
```
$ export PYSPARK_DRIVER_PYTHON=ipython
$ pyspark --master=spark://$(hostname):7077
```
Now you are in ipython instead of the standard Python interpreter.

### Jupyter
```
$ export PYSPARK_DRIVER_PYTHON=jupyter
$ export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8889 --ip=0.0.0.0"
$ pyspark --master=spark://$(hostname):7077
```
Pyspark will start a Jupyter notebook. Enter the following in a cell in order to attach to
this cluster:
```
from IPython.core.display import display, HTML
from pyspark.sql import SparkSession
display(HTML("<style>.container { width:100% !important; }</style>"))

spark = SparkSession.builder.appName("my_app").getOrCreate()
```


## Tuning Spark Configuration Settings
In general you want to run Spark with as many executors as possible on each worker node. The
Amazon orchestration software along with the YARN cluster manager *may* take care of that when
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

3. SparkSession construction: You can customize things like executor settings when you construct
   the `SparkSession` in Python. This example will ensure that the job will start a single executor
   with a single core and use all available memory.
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
   can change the number of shuffle partitions that Spark will use. You may want ot change that
   value if the size of the dataframes you're working on change dramatically.
```
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.shuffle.partitions", 500)
```

## Creating a SparkSession with dsgrid
Ensure that the dsgrid software uses the cluster with optimized settings. Most importantly,
don't create a session in local mode, bypassing the cluster. You can verify that you are
using the correct settings in the Spark UI.

### Through Spark scripts
This is the recommended procedure because you can customize any setting.

Suppose that you want to run a query with a CLI command and use custom settings.
This example assumes that you are on the HPC compute node that is the Spark master.

**Note**: `spark-submit` needs to be able to detect that the script is Python, and so you must
substitute `dsgrid-cli.py` for the usual `dsgrid`. You also need to specify its full path.

```
$ spark-submit --master spark://$(hostname):7077 \
    --conf spark.sql.shuffle.partitions=500 \
    $(which dsgrid-cli.py) \
    query project run \
    --offline \
    --registry-path=/scratch/${USER}/.dsgrid-registry \
    query.json
```

Here is a similar example if you want to run code interactively in ipython or jupyter.

```
$ pyspark --master spark://$(hostname):7077 --conf spark.sql.shuffle.partitions=500
```

In both cases the Spark scripts will create a session and make it available to the Python
process. dsgrid will connect to it.

### At dsgrid runtime
dsgrid will attempt to connect to an existing cluster through the `SPARK_CLUSTER` environment
variable. If you want to choose this route:
```
$ export SPARK_CLUSTER=spark://$(hostname):7077
$ dsgrid query project run --offline --registry-path=/scratch/${USER}/.dsgrid-registry query.json
```

The upside of this approach is that it is a bit simpler. The downside is that you cannot configure
settings like `spark.driver.memory`, which, as stated earlier, must be set before the JVM is
created.


## Installing a Spark Standalone Cluster on Your Laptop
Download your desired version from https://spark.apache.org/downloads.html and extract it on
your system.

**Note**: Choose the version that is set in `dsgrid/setup.py` for `pyspark`. At least the minor
versions must match.

Here is an example:
```
$ cd
$ wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
$ tar -xzf spark-3.3.1-bin-hadoop3.tgz && rm spark-3.3.1-bin-hadoop3.tgz
# Consider adding this to your shell rc file.
$ export SPARK_HOME=~/spark-3.3.1-bin-hadoop3
```

The full instructions to create a cluster are at
http://spark.apache.org/docs/latest/spark-standalone.html. The rest of this section documents a
limited set that should work on your system.

1. Ensure that the environment variable `SPARK_HOME` is set to your installation directory.
2. Customize values in `$SPARK_HOME/conf/spark-defaults.conf` and `$SPARK_HOME/conf/spark-env.sh`.

3. Start the master with this command:
```
$SPARK_HOME/sbin/start-master.sh
```

4. Open http://localhost:8080/ in your browser and copy the cluster URL and port. It will be
something like `spark://$(hostname):7077`.

5. Start a worker with this command. Give the worker as much memory as you can afford. You can also
configure this in step #2.
```
$SPARK_HOME/sbin/start-worker.sh -m 16g spark://$(hostname):7077
```

If you add the `sbin` to your `PATH` environment variable, here is a one-liner:
```
$ start-master.sh && start-worker.sh -m 24g spark://$(hostname):7077
```

Monitor cluster tasks in your browser.

6. Stop all of the processes when you complete your work.
```
$ stop-worker.sh && stop-master.sh
```

## Installing a Spark Standalone Cluster on an HPC
This section describes how you can run Spark jobs on any number of HPC compute nodes.

The HPC GitHub repository contains scripts that will create an ephemeral Spark cluster
on compute nodes that you allocate.

1. The repository as setup instructions that are not currently correct because the branch
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
Notes:
- Eagle will let you allocate two debug jobs each with two nodes. So, you can use these scripts
  to create a four-node cluster for one hour.
- If the debug partition is not too full, you can append `--qos=standby` to the command above
  and not be charged any AUs.

3. Setup the environment in a directory in `/scratch/$USER`.
```
$ export PATH=$PATH:<your-repo-path>/HPC/applications/spark/spark_scripts
```

4. Select a container valid for use with dsgrid, which currently requires Spark v3.3.x and
   Python 3.10. The team has validated the container below. It was created with this Dockerfile
   in dsgrid: `docker/spark/Dockerfile`
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

**Note**: The container includes ipython, pyspark, pandas, and pyarrow, but not dsgrid.
That may change when we have stable versions of dsgrid software.

8. Use your preferred method to connect to the cluster in dsgrid software. Your options are as
   follows:

- Run dsgrid CLI commands through `spark-submit` or set the `SPARK_CLUSTER` environment variable
  and run the commands directly.
```
$ spark-submit --master=spark://$(hostname):7077 \
	$(which dsgrid-cli.py) \
	registry \
	--offline \
	--path=<your-regisry-path> \
	--help
```
```
$ export SPARK_CLUSTER=spark://$(hostname):7077
$ dsgrid registry --offline --path=<your-registry-path> --help
```

- Run dsgrid software in an ipython session.
```
$ export PYSPARK_DRIVER_PYTHON=ipython
$ pyspark --master=spark://$(hostname):7077
```

- Run dsgrid software in a Jupyter notebook.
```
$ export PYSPARK_DRIVER_PYTHON=jupyter
$ export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8889 --ip=0.0.0.0"
$ pyspark --master=spark://$(hostname):7077
```
Once the notebook starts, open an ssh tunnel with port 8889. Open up ports for the Spark UI
at the same time. Identify the hostname of this compute node with this command:
```
$ hostname
```

On your laptop:
```
$ export COMPUTE_NODE=<compute_node_name>
$ ssh -L 4040:$COMPUTE_NODE:4040 -L 8080:$COMPUTE_NODE:8080 -L 8889:$COMPUTE_NODE:8889 $USER@eagle.hpc.nrel.gov
```
Connect to the Jupyter notebook in your browser.


## Spark Configuration Problems
Get used to monitoring Spark jobs in the Spark UI. The master is at http://hostname:8080 and the
worker is at http://hostname:4040. If a job seems stuck or slow, explore why.
Then kill the job, make config changes, and retry. A misconfigured job will take too long or never
finish.

This section explains some common problems.

### spark.sql.shuffle.partitions
The most common performance issue we encounter when running complex queries is due to a non-ideal
setting for `spark.sql.shuffle.partitions`. The default Spark value is 200. Some online sources
recommend setting it to 1-4x the total number of CPUs in your cluster. For example, if you have two
Eagle nodes, 72 - 288. This [video](https://www.youtube.com/watch?v=daXEp4HmS-E&t=4251s) by a Spark
developer offers a recommendation that has worked out better.

Use this formula:
```
num_partitions = max_shufffle_write_size / target_partition_size
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

### Slow local storage
Spark will write lots of temporary data to local storage during shuffle operations. If your joins
cause lots of shuffling, it is very important that your local storage be fast. If you use an Eagle
node with a slow spinning disk (130 MB/s), your job will likelly fail. Use a `bigmem` or `gpu`
node. They have SSDs that can write data at 2 GB/s.

We have not observed any downside to having this feature enabled.

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

If you are not partitioning by column and know the target number of partitions:

If there are too few partitions currently:
```
df.repartition(X).write.parquet("data.parquet")
```

If there are too many partitions currently:
```
df.coalesce(X).write.parquet("data.parquet")
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
