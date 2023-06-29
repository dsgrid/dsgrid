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

.. code-block:: bash

    conda create -n dsgrid python=3.10

3. Activate the environment:

.. code-block:: bash

    conda activate dsgrid


Package Installation
=====================

With ssh keys:

.. code-block:: bash

    pip install git+ssh://git@github.com/dsgrid/dsgrid.git@main

Or from http:

.. code-block:: bash

    pip install git+https://github.com/dsgrid/dsgrid.git@main

.. todo:: pipy.org/pip installation not available yet.

Windows
-------
To install Apache Spark on Windows, follow `these instructions <https://sparkbyexamples.com/pyspark-tutorial/#pyspark-installation>`_.


Registry
========
The current dsgrid registry is hosted on a VM in the NREL data center. The database URL is
http://dsgrid-registry.hpc.nrel.gov:8529. All configuration information is stored in the database
and all dataset files are stored on the the NREL HPC.

You can list projects, datasets, and dimensions with the dsgrid CLI tool from any system within
NREL. To query the data you must be on the NREL HPC.

.. todo:: Steps to replicate the database to another system

Test your installation
======================

.. code-block:: console

    $ dsgrid -u http://dsgrid-registry.hpc.nrel.gov:8529 -n standard-scenarios registry projects list


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
