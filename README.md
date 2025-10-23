# dsgrid
[![Documentation](https://img.shields.io/badge/docs-ready-blue.svg)](https://dsgrid.github.io/dsgrid)
[![codecov](https://codecov.io/gh/dsgrid/dsgrid/branch/main/graph/badge.svg?token=W0441C9XAL)](https://codecov.io/gh/dsgrid/dsgrid)

Python API for contributing to and accessing demand-side grid model (dsgrid) projects and datasets.

⚠️ **dsgrid is under active development and does not yet have a formal package release.** Details listed here are subject to change. Please reach out to the dsgrid coordination team with any questions or other feedback. ⚠️

[Install](#install) | [Usage](#usage) | [Uninstall](#uninstall)

## Install

[Virtual environment](#virtual-environment) | [Dependencies](#dependencies) | [from PIPY/pip](#from-pipypip) | [from pip+git](#from-pipgit) | [from cloned repository](#from-cloned-repository)

### Virtual environment

Create a virtual environment in which to install dsgrid. Anaconda or miniconda is recommended.

```
conda create -n dsgrid python=3.11
conda activate dsgrid
```

### Dependencies

dsgrid uses [Apache Spark](#https://spark.apache.org/) to manage big data. There are no separate installation steps for Apache Spark beyond installing the dsgrid package and installing:

```
pip install "dsgrid-toolkit[spark]" --group=pyhive
```

Otherwise installing the pyspark Python dependency handles it.

However, you should be aware that Apache Spark's Microsoft Windows support is poor and essentially limited to local mode. That is, if you use dsgrid on a Windows machine you should not attempt to install a full version of Spark nor expect to run on a Spark cluster. As such, we recommend limiting dsgrid use on Windows to browsing the registry, registering and submitting small- to medium-sized datasets, or development work with small test projects. Full dsgrid functionality with large projects requires additional computational resources, e.g., high performance or cloud computing, typically on a Linux operating system.

#### Additional Notes
- If pyspark complains about not finding Python, you may need to locate your python executable file (python.exe on Windows), copy it, and rename the copy to python3 (python3.exe on Windows)

Spark requires Java 8 or later with the `JAVA_HOME` environment variable set to the Java installation directory.

On Linux you can install OpenJDK with conda:
```
conda install openjdk
```

Windows install instructions are below.

#### Windows

To install Apache Spark on Windows, follow [these instructions](https://towardsdatascience.com/installing-apache-pyspark-on-windows-10-f5f0c506bea1).

### From PIPY/pip

pip install dsgrid-toolkit

or

pip install "dsgrid-toolkit[spark]" --group=pyhive

### From pip+git

**With ssh keys:**
```
pip install git+ssh://git@github.com/dsgrid/dsgrid.git@main

# or

pip install git+ssh://git@github.com/dsgrid/dsgrid.git@develop
```

**From http:**
```
pip install git+https://github.com/dsgrid/dsgrid.git@main

# or

pip install git+https://github.com/dsgrid/dsgrid.git@develop
```

### From Cloned Repository

First, clone the repository and change into the `dsgrid` directory. For example:

```
cd ~                                       # or other directory where you put repositories
git clone git@github.com:dsgrid/dsgrid.git # or the http address
cd dsgrid
```

Then install the pacakge using the pip `-e` flag to directly use the files in the
cloned repository.

**Users:**
```
pip install -e . --group=pyhive
```

**Developers:**
```
pip install -e '.[dev,spark]' --group=pyhive
```

## Usage

dsgrid is primarily a command-line interface (CLI) tool. To see the available commands:
```
dsgrid --help
```

## Uninstall

```
pip uninstall dsgrid
```

If you are using a conda environment
```
conda deactivate
```

## Software Record

dsgrid is developed under NREL Software Record SWR-21-52, "demand-side grid model".
