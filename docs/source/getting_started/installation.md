# Installation

## Python Environment

dsgrid requires python=3.11 or later. If you do not already have a python environment with python>=3.11, we recommend using [Conda](https://conda.io/projects/conda/en/latest/index.html) to help manage your python packages and environments.

### Steps to Create a dsgrid Conda Environment

1. [Download and install Conda](https://conda.io/projects/conda/en/latest/user-guide/install) if it is not already installed. We recommend Miniconda over Anaconda because it has a smaller installation size.

2. Create a suitable environment:

```bash
conda create -n dsgrid python=3.11
```

3. Activate the environment:

```bash
conda activate dsgrid
```

### Install Java

dsgrid's key dependency is Apache Spark. Apache Spark requires Java, so check if you have it. Both of these commands must work:

::::{tab-set}

:::{tab-item} Mac/Linux
```bash
java --version
# openjdk 11.0.12 2021-07-20

echo $JAVA_HOME
# /Users/dthom/brew/Cellar/openjdk@11/11.0.12

# If you don't have java installed:
conda install openjdk
```
:::

:::{tab-item} Windows
```pwsh
java --version
# openjdk 11.0.13 2021-10-19
# OpenJDK Runtime Environment JBR-11.0.13.7-1751.21-jcef

echo %JAVA_HOME%
# C:\Users\ehale\Anaconda3\envs\dsgrid\Library

# If you don't have java installed:
conda install openjdk
```
:::

::::

## Package Installation

### To use DuckDB as the backend:

```bash
pip install dsgrid-toolkit
```

### To use Apache Spark as the backend:

```bash
pip install "dsgrid-toolkit[spark]"
```

## Registry

### NREL Shared Registry

The current dsgrid registries are stored in per-project SQLite database files. All configuration information is stored in the database(s) and all dataset files are stored on the NREL HPC shared filesystem.

### Standalone Registry

To use dsgrid in your own computational environment, you will need to initialize your own registry with this CLI command:

```bash
dsgrid create-registry --help
```

## Apache Spark

- **NREL High Performance Computing**: [How to Start Spark Cluster on Kestrel](../user_guide/apache_spark/spark_cluster_on_kestrel)
- **Standalone resources**: [TODO: Provide link]

## Test Your Installation

If you're running dsgrid at NREL and using the shared registry, you can test your installation with this command:

```bash
dsgrid -u sqlite:///<your-db-path> registry projects list
```

## Save Your Configuration

Running `dsgrid config create` stores key information for working with dsgrid in a config file at `~/.dsgrid.json5`. Currently, dsgrid only supports offline mode, and the other key information to store is the registry URL. The parameters in the config file are the default values used by the command-line interface.

The appropriate configuration for using the shared registry at NREL is:

```bash
dsgrid config create sqlite:////projects/dsgrid/standard-scenarios.db
```

:::{admonition} AWS Cloud Access
:class: note

Access from AWS is under development.

<!--
dsgrid uses Amazon Web Services (AWS) cloud. The dsgrid registry of datasets and configurations are stored on S3. dsgrid also uses EMR spark clusters for big data ETLs and queries.

Currently, the dsgrid registry is only accessible through the internal NREL dsgrid sandbox account (`nrel-aws-dsgrid`). To get set up on the sandbox account, please reach out to the dsgrid team.

### Setup sandbox account

Once the NREL Stratus Cloud Team has set you up with a dsgrid sandbox account (`nrel-aws-dsgrid`), you will receive an email with your temporary password and instructions on how to setup your account. Follow the instructions in the email to complete the following:

1. Log in and set up your password
2. Set up Multi-Factor Authentication (MFA)

### Configure named profile

Configure named profile for nrel-aws-dsgrid. See [these directions](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) for how to configure your named profile for the aws-cli.

Add the following text to the `~/.aws/credentials` file (replacing XXXX with your credentials):

```
[nrel-aws-dsgrid]
aws_access_key_id = XXXX
aws_secret_access_key = XXXX
```

You can find your [AWS security credentials](https://console.aws.amazon.com/iam/home?#/security_credentials) in your profile.

Finally, check that you can view contents in the registry:

```bash
aws s3 ls s3://nrel-dsgrid-registry
```
-->
:::

## Next Steps

- Learn about [browsing the registry](../user_guide/registry/browse_registry)
- Explore the [tutorials](../user_guide/tutorials/index) to get started with dsgrid
- Understand [data formats](../software_reference/dataset_formats) for preparing your data
