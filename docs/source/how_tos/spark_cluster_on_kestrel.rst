.. _how-to-start-spark-cluster-kestrel:

***************************************
How to Start a Spark Cluster on Kestrel
***************************************
This section assumes that you have cloned the HPC Spark setup scripts from this `repo
<https://github.com/NREL/HPC.git>`_. If you are unfamiliar with that, please read the full details
at :ref:`spark-on-hpc`.

Compute Node Types
==================
Spark works best with fast local storage. The standard Kestrel nodes do not have any local storage.
The best candidates are the 256 standard nodes (no GPUs) with 1.92 TB NVMe M.2 drives. Please refer
to the `Kestrel system configuration page
<https://www.nrel.gov/hpc/kestrel-system-configuration.html>`_ for specific hardware information.
The GPU nodes will work well, but at a greater cost in AUs.

If those nodes are not available, you may be able to complete your queries by using the standard
nodes and specifying a path on the Lustre filesystem in the Spark configuration file
``conf/spark-env.sh``. Change ``SPARK_LOCAL_DIRS`` and ``SPARK_WORKER_DIR``.

Steps
=====
1. From the HPC login node, create a work directory somewhere in ``/scratch/$USER``.

2. Add the ``spark_scripts`` to your PATH if you haven't already:

.. code-block:: console

   $ export PATH=$PATH:~/repos/HPC/applications/spark/spark_scripts

3. Allocate one or more nodes.

.. code-block:: console

    $ salloc -t 01:00:00 -N1 --account=dsgrid --partition=debug --tmp=1600G

4. Configure the Spark settings and start the cluster. Run -h to see the available options.

.. code-block:: console

    $ configure_and_start_spark.sh -c /datasets/images/apache_spark/spark351_py312.sif

5. Set the Spark configuration environment variable.

.. code-block:: console

    $ export SPARK_CONF_DIR=$(pwd)/conf

The Spark cluster is ready to query at ``spark://$(hostname):7077``. Run all query scripts from
this node.
