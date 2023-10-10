.. _set_up_standalone_registry:

***********************************
How to Set Up a Standalone Registry
***********************************

dsgrid stores registry information in an ArangoDB database. If you want to work with a completely 
local version of dsgrid, you must set up and connect to a local ArangoDB instance. There are two 
ways to install ArangoDB on your computer: `Native Installation`_ or `Docker Container`_. You might 
also want to set up a standalone registry on `NREL HPC`_ for development or testing purposes.

Once installed, the easiest way to mange the database manually is through Arango's web UI,
available at http://localhost:8529.

Native Installation
===================

1. Install **ArangoDB Community Edition** locally by following the instructions at
   https://www.arangodb.com/download-major/. If asked and you plan to run dsgrid tests, set the root 
   password to openSesame to match the defaults.

   Add the ``bin`` directory to your system path and customize the configuration files, 
   particularly regarding authentication, as desired. 

    .. tabs::

        .. tab:: Mac

            The ``bin`` directory will be in a location like:

            .. code-block:: bash

                $HOME/Applications/ArangoDB3-CLI.app/Contents/Resources/opt/arangodb/bin

            though you may have chosen to install to ``/Applications``. 

            The configuration files will be in a directory like:

            .. code-block:: bash

                $HOME/Applications/ArangoDB3-CLI.app/Contents/Resources/opt/arangodb/etc/arangodb3

        .. tab:: Windows

            The ``bin`` directory will be in a location like:

            .. code-block:: pwsh
                
                C:\Users\$USER\AppData\Local\ArangoDB3 3.10.5\usr\bin

            and the executable installer will have already added it to your path (User variables, 
            Path).

            The configuration files will be in a directory like:

            .. code-block:: pwsh

                C:\Users\$USER\AppData\Local\ArangoDB3 3.10.5\etc\arangodb3

2. Start the database.

    .. tabs::

        .. tab:: Mac

            Start the database by running ``arangodb``. 

            If you get the error ``cannot find configuration file`` then make this directory and copy 
            the configuration files:

            .. code-block:: console

                $ mkdir ~/.arangodb
                $ cp ~/Applications/ArangoDB3-CLI.app/Contents/Resources/opt/arangodb/etc/arangodb3/*conf ~/.arangodb

            If you don't copy the files, you can specify the config file with 
            ``arangodb --conf <your-path>/arangod.conf``

        .. tab:: Windows

            Start the database by running ``arangod``. It is preferable to run ``arangod`` from the 
            path like ``C:\Users\$USER\AppData\Local\ArangoDB3 3.10.5``. Alternatively, you can 
            directly use the ArangoDB Server shortcut on your Windows desktop.


Docker Container
================

Run the ArangoDB Docker container by following instructions at
https://www.arangodb.com/download-major/docker/. Note the details about data persistence.

For example:

.. code-block:: console
    
    $ docker create --name arangodb-persist arangodb true

    $ docker run --name=arango-container --volumes-from arangodb-persist -p 8529:8529 -e ARANGO_ROOT_PASSWORD=openSesame arangodb/arangodb:3.10.4

Once the docker container is running, Arango commands will need to be preceded with `docker exec`. 
For example, using the container name from above:

.. code-block:: console
    
    $ docker exec arango-container arangorestore ...


NREL HPC
========

The dsgrid repository includes ``scripts/start_arangodb_on_eagle.sh``. It will start an ArangoDB
on a compute node using the debug partition. It stores Arango files in ``/scratch/${USER}/arangodb3``
and ``/scratch/${USER}/arangodb3-apps``. If you would like to use a completely new database,
delete those directories before running the script.

Note that Slurm will log stdout/stderr from ``arangod`` into ``./arango_<job-id>.o/e``.

The repository also includes ``scripts/start_spark_and_arango_on_eagle.sh``. It starts Spark as well
as ArangoDB, but you must have cloned ``https://github.com/NREL/HPC.git``. It looks for the repo at
``~/repos/HPC``, but you can set a custom value on the command line, such as the example below.

You may want to adjust the number and type of nodes in the script based on your Spark requirements.

.. code-block:: console

    $ sbatch scripts/start_spark_and_arango_on_eagle.sh ~/HPC

Note that Slurm will log stdout/stderr from into ``./dsgrid_infra<job-id>.o/e``. Look at the .o
file to see the URL for the Spark cluster and the Spark configuration directory.

It is advised to gracefully shut down the database if you want to ensure that all updates have
been persisted to files. To do that:

1. ssh to the compute node running the database.
   
2. Identify the process ID of ``arangod``. In this example the PID is ``195412``.
   
    .. code-block:: console

        $ ps -ef | grep arango
        dthom    195412 195392  0 09:31 ?        00:00:06 arangod --server.authentication=true --config /tmp/arangod.conf

3. Send ``SIGTERM`` to the process.

    .. code-block:: console

        $ kill -s TERM 195412

4. ``arangod`` will detect the signal and gracefully shutdown.

Modify the HPC parameters as needed. Or run the commands manually. **Note that you should never run
ArangoDB on a login node.**

If you need to start a Spark cluster, you can do that on the same compute node running the database.