#####
Setup
#####


Python Environment
==================
dsgrid requires python=3.8 or later. If you do not already have a python environment with python>=3.8, we recommend using `Conda <https://conda.io/projects/conda/en/latest/index.html>`_ to help manage your python packages and environments.

Steps to make a dsgrid Conda environment:

1. `Download and install Conda <https://conda.io/projects/conda/en/latest/user-guide/install/index.html#regular-installation>`_ if it is not already installed.
2. Create a suitable environment.

.. code-block:: bash

    conda create -n dsgrid python=3.8

3. Activate the environment:

.. code-block:: bash

    conda activate dsgrid


Installation
============
Clone the `dsgrid repository <https://github.com/dsgrid/dsgrid>`_.

With ssh keys:

.. code-block:: bash

    pip install git+ssh://git@github.com/dsgrid/dsgrid.git@main

Or from http:

.. code-block:: bash

    pip install git+https://github.com/dsgrid/dsgrid.git@main

.. todo:: PIPY/pip installation not available yet.

#### Windows

To install Apache Spark on Windows, follow `these instructions <https://sparkbyexamples.com/pyspark-tutorial/#pyspark-installation>`_.


AWS Cloud
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


Project Repository
==================
Every dsgrid project needs a github repository to store all configs and misc. scripts and to collaborate on project decisions. An example dsgrid project repository is the `dsgrid-project-StandardScenarios <https://github.com/dsgrid/dsgrid-project-StandardScenarios>`_ repo.

Project Repo Organization
-------------------------
We recommend that dsgrid project repositories use the following directory organization structure:

    .. code-block::

        .
        ├── dsgrid_project
            ├── datasets
            │   ├── benchmark
            │   ├── historical
            │   └── sector_models
            │       ├── comstock
            │       │   ├── dimension_mappings
            │       │   ├── dimensions
            │       │   ├── dataset.toml
            │       │   ├── dimension_mappings.toml
            │       └── ...
            ├── dimension_associations
            ├── dimension_mappings
            ├── dimensions
            └── project.toml

In the directory structure above, all project files are stored in the ``dsgrid_project`` root directory. And within this root directory we have:

    * ``datasets``: This is where we define input datasets, oranized first by type (i.e., ``datasets/historical``, ``datasets/benchmark``, ``datasets/sector_models``) and then by source (e.g., ``datasets/sector_models/comstock``) for the dsgrid project
    * ``dimension_mappings``: This where we store :under:`project-level` dimension mapping records (csv or json)
    * ``dimensons``: This is where we store :under:`project-level` dimension records (csv or json)
    * ``project.toml``: This is the main project configuration file. See ::ref

    Within each `datasets/{type}/{source}` sub-folder are the following files:

        * ``dimension_mappings``:  This where we store :under:`dataset-level` dimension mapping records (csv or json)
        * ``dimensions``: This is where we store :under:`dataset-level` dimension records (csv or json)
        * ``dataset.toml``: This is the dataset configuration file
        * ``dimension_mappings.toml``: :under:`Dataset-level`  dimension mappings configs for new dimension mappings that need to be registered
