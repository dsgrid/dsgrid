.. _how-to-run-dsgrid-eagle:

**************************
How to run dsgrid on Eagle
**************************

1. ssh to a login node and start a screen session (or similar, e.g., tmux):

.. code-block:: console

    $ screen -S dsgrid

2. Follow the installation instructions at :ref:`installation`.

3. Create a dsgrid runtime config file:

.. code-block:: console

    $ dsgrid config create -u http://dsgrid-registry.hpc.nrel.gov:8529 -N standard-scenarios --offline

4. Start a Spark cluster with your desired number of compute nodes by following the instructions at
   :ref:`how-to-start-spark-cluster-eagle`.

5. Run all CPU-intensive dsgrid commands from the first node in your HPC allocation like this:

.. code-block:: console

    $ spark-submit --master=spark://$(hostname):7077 $(which dsgrid-cli.py) [command] [options] [args]

6. Because you started a screen session at the beginning, if you disconnect from your ssh session
   for any reason you can pick your work back up by ssh'ing to the same login node you used the
   first time and resuming your screen session:

.. code-block:: console

    $ screen -r dsgrid
