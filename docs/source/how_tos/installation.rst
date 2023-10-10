.. _installation:

************
Installation
************

Python Environment
==================
dsgrid requires python=3.10 or later. If you do not already have a python environment with
python>=3.10, we recommend using `Conda <https://conda.io/projects/conda/en/latest/index.html>`_ to
help manage your python packages and environments.

Steps to make a dsgrid Conda environment:

1. `Download and install Conda <https://conda.io/projects/conda/en/latest/user-guide/install>`_ if
   it is not already installed. We recommend Miniconda over Anaconda because it has a smaller
   installation size.
2. Create a suitable environment.

.. code-block:: console

    $ conda create -n dsgrid python=3.10

3. Activate the environment:

.. code-block:: console

    $ conda activate dsgrid

dsgrid's key dependencies are an `ArangoDB registry <#registry>`_, which can be 
`shared <#nrel-shared-registry>`_ or `standalone <#standalone-registry>`_, and 
`Apache Spark`_. Apache Spark requires Java, so check if you have it. Both of these 
commands must work:

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

With ssh keys:

.. code-block:: bash

    pip install git+ssh://git@github.com/dsgrid/dsgrid.git@main

Or from http:

.. code-block:: bash

    pip install git+https://github.com/dsgrid/dsgrid.git@main

.. todo:: pipy.org/pip installation not available yet.


Registry
========

NREL Shared Registry
--------------------
The current dsgrid registry is hosted on a VM in the NREL data center. The database URL is
http://dsgrid-registry.hpc.nrel.gov:8529. All configuration information is stored in the database
and all dataset files are stored on the the NREL HPC.

You can list projects, datasets, and dimensions with the dsgrid CLI tool from any system within
NREL. To query the data you must be on the NREL HPC.

.. todo:: Steps to replicate the database to another system

Standalone Registry
-------------------
To use dsgrid in your own computational environment, you will need to run ArangoDB and either set
up a new dsgrid registry or load a dsgrid registry that has been written to disk. Please see the 
:ref:`how-to guide on setting up a standalone dsgrid registry <set_up_standalone_registry>`.


Apache Spark
============

- NREL High Performance Computing: :ref:`how-to-start-spark-cluster-eagle`
- Standalone resources: [TODO: Provide link]


Test your installation
======================

If you're running dsgrid at NREL and using the shared registry, you can test your installation
with this command:

.. code-block:: console

    $ dsgrid -u http://dsgrid-registry.hpc.nrel.gov:8529 -N standard-scenarios registry projects list

You can test your installation similarly if you are using a different registry, just change the
ArangoDB URL (-u) and database name (-N) arguments to match your set-up.

.. _configure_dsgrid:

Save your configuration
=======================

Running ``dsgrid config create`` stores key information for working with dsgrid in a config file at
``~/.dsgrid.json5``. Currently, dsgrid only supports offline mode, and the other key information to
store is the ArangoDB URL and the name of the dsgrid registry. The parameters in the config file
are the default values used by the command-line interface.

The appropriate configuration for using the shared registry at NREL is:

.. code-block:: console

    $ dsgrid config create -u http://dsgrid-registry.hpc.nrel.gov:8529 -N standard-scenarios --offline

Similar to testing your installation, you can save the correct configurations for other set-ups
by changing the ArangoDB URL (-u) and database name (-N) arguments of the above command.

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
