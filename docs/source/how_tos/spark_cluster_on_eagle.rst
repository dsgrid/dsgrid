.. _how-to-start-spark-cluster-eagle:

*************************************
How to start a Spark cluster on Eagle
*************************************
This section assumes that you have cloned the HPC Spark setup scripts from this `repo
<https://github.com/NREL/HPC.git>`_. If you are unfamiliar with that, please read the full details
at :ref:`spark-on-hpc`.

This example will acquire one compute node from the ``debug`` partition. If you need more than two
nodes or more than one hour of runtime, use the ``bigmem`` or ``gpu`` partition.

.. warning:: Do not use compute nodes from the standard partition on Eagle if your queries will
   shuffle data. The local storage on those nodes are too slow and will cause timeouts. All bigmem
   and gpu nodes have fast local storage.

Steps
=====
1. From the HPC login node, create a work directory somewhere in ``/scratch/$USER``.

2. Add the ``spark_scripts`` to your PATH if you haven't already:

.. code-block:: console

   $ export PATH=$PATH:~/repos/HPC/applications/spark/spark_scripts

3. Allocate one or more nodes.

.. code-block:: console

    $ salloc -t 01:00:00 -N1 --account=dsgrid --partition=debug --mem=730G

4. Create the initial config file.

.. code-block:: console

   $ create_config.sh -c /datasets/images/apache_spark/spark341_py311.sif

5. Configure the Spark settings. Run -h to see the available options.

.. code-block:: console

    $ configure_spark.sh

6. Start the cluster.

.. code-block:: console

    $ start_spark_cluster.sh

7. Set the Spark configuration environment variable.

.. code-block:: console

    $ export SPARK_CONF_DIR=$(pwd)/conf

The Spark cluster is ready to query at ``spark://$(hostname):7077``. Run all query scripts from
this node.
