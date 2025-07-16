.. _how-to-start-spark-cluster-kestrel:

***************************************
How to Start a Spark Cluster on Kestrel
***************************************
This section assumes that you have pip-installed the Python package ``sparkctl`` and are familiar
with its [documentation](https://nrel.github.io/sparkctl/).

Compute Node Types
==================
Spark works best with fast local storage. The standard Kestrel nodes do not have any local storage.
The best candidates are the 256 standard nodes (no GPUs) with 1.92 TB NVMe M.2 drives. Please refer
to the `Kestrel system configuration page
<https://www.nrel.gov/hpc/kestrel-system-configuration.html>`_ for specific hardware information.
The GPU nodes will work as well, but at a greater cost in AUs.

If those nodes are not available, you may be able to complete your queries by using the standard
nodes and specifying a path on the Lustre filesystem in the Spark configuration file
``conf/spark-env.sh``. Change ``SPARK_LOCAL_DIRS`` and ``SPARK_WORKER_DIR``.

Steps
=====
1. From the HPC login node, create a work directory somewhere in ``/scratch/$USER``.

2. Allocate one or more nodes.

.. code-block:: console

    $ salloc -t 01:00:00 -N1 --account=dsgrid --partition=debug --tmp=1600G --mem=240G

3. Configure the Spark settings and start the cluster. Run -h to see the available options.

.. code-block:: console

    $ sparkctl configure --start

5. Set the Spark configuration and Java environment variables.

.. code-block:: console

    $ export SPARK_CONF_DIR=$(pwd)/conf
    $ export JAVA_HOME=/datasets/images/apache_spark/jdk-21.0.7

The Spark cluster is ready to query at ``spark://$(hostname):7077``. Run all query scripts from
this node.
