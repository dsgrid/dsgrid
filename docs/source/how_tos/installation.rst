.. _installation:

************
Installation
************

Python Environment
==================
dsgrid requires python=3.11 or later. If you do not already have a python environment with
python>=3.11, we recommend using `Conda <https://conda.io/projects/conda/en/latest/index.html>`_ to
help manage your python packages and environments.

Steps to make a dsgrid Conda environment:

1. `Download and install Conda <https://conda.io/projects/conda/en/latest/user-guide/install>`_ if
   it is not already installed. We recommend Miniconda over Anaconda because it has a smaller
   installation size.
2. Create a suitable environment.

.. code-block:: console

    $ conda create -n dsgrid python=3.11

3. Activate the environment:

.. code-block:: console

    $ conda activate dsgrid

dsgrid's key dependency is `Apache Spark`_. Apache Spark requires Java, so check if you have it.
Both of these commands must work:

.. tabs::

    .. code-tab:: console Mac/Linux

        $ java --version
        openjdk 11.0.12 2021-07-20
        $ echo $JAVA_HOME
        /Users/dthom/brew/Cellar/openjdk@11/11.0.12

        $ # If you don't have java installed:
        $ conda install openjdk

    .. code-tab:: pwsh-session Windows

        > java --version
        openjdk 11.0.13 2021-10-19
        OpenJDK Runtime Environment JBR-11.0.13.7-1751.21-jcef (build 11.0.13+7-b1751.21)
        OpenJDK 64-Bit Server VM JBR-11.0.13.7-1751.21-jcef (build 11.0.13+7-b1751.21, mixed mode)
        > echo %JAVA_HOME%
        C:\Users\ehale\Anaconda3\envs\dsgrid\Library

        > # If you don't have java installed:
        > conda install openjdk

Package Installation
=====================
To use DuckDB as the backend:

.. code-block:: console

    $ pip install dsgrid-toolkit

To use Apache Spark as the backend:

.. code-block:: console

    $ pip install "dsgrid-toolkit[spark]" --group=pyhive


Registry
========

NREL Shared Registry
--------------------
The current dsgrid registries are stored in per-project SQLite database files.
All configuration information is stored in the database(s) and all dataset files are stored on
the NREL HPC shared filesystem.

Standalone Registry
-------------------
To use dsgrid in your own computational environment, you will need to initialize your own
registry with this CLI command:

.. code-block:: bash

   dsgrid create-registry --help

Apache Spark
============

- NREL High Performance Computing: :ref:`how-to-start-spark-cluster-kestrel`
- Standalone resources: [TODO: Provide link]


Test your installation
======================

If you're running dsgrid at NREL and using the shared registry, you can test your installation
with this command:

.. code-block:: console

    $ dsgrid -u sqlite:///<your-db-path> registry projects list

.. _configure_dsgrid:

Save your configuration
=======================

Running ``dsgrid config create`` stores key information for working with dsgrid in a config file at
``~/.dsgrid.json5``. Currently, dsgrid only supports offline mode, and the other key information to
store is the registry URL. The parameters in the config file are the default values used by the
command-line interface.

The appropriate configuration for using the shared registry at NREL is:

.. code-block:: console

    $ dsgrid config create sqlite:////projects/dsgrid/standard-scenarios.db

.. todo:: Access from AWS

.. AWS Cloud
  =========
  dsgrid uses Amazon Web Services (AWS) cloud. The dsgrid registry of datasets and configurations are stored on S3. dsgrid also uses EMR spark clusters for big data ETLs and queries.

  Currently, the dsgrid registry is only accessible through the internal NREL dsgrid sandbox account (``nrel-aws-dsgrid``). To get set up on the sandbox account, please reach out to the dsgrid team.

  Setup sandbox account
  ---------------------
  Once the NREL Stratus Cloud Team has set you up with a dsgrid sandbox account (``nrel-aws-dsgrid``), you will recieve an email with your temporay password and instructions on how to setup your account. Follow the instructions in the email to complete the following:

      1. Log in and set up your password
      2. Set up Multi-Factor Authentication (MFA)

  Configure named profile
  -----------------------

  .. todo:: Named profile requirement is temporary and will be replaced with work in dsrig PR #56

  Configure named profile for nrel-aws-dsgrid. See `these directions <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html>`_ for how to configure your named profile for the aws-cli. Or alternatively, follow these directions:

  Then add the following text to the ``~/.aws/credentials`` file (replacing XXXX with your creditentials):

  .. code-block:: bash

      [nrel-aws-dsgrid]
      aws_access_key_id = XXXX
      aws_secret_access_key = XXXX

  You can find your `AWS security credentials <https://console.aws.amazon.com/iam/home?#/security_credentials>`_ in your profile.

  To save your changes in vi, type ``ESC`` then ``:x``.


  Finally, check that you can view contents in the registry:

  .. code-block:: bash

      aws s3 ls s3://nrel-dsgrid-registry
