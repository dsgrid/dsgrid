# dsgrid
[![Documentation](https://img.shields.io/badge/docs-ready-blue.svg)](https://dsgrid.github.io/dsgrid)

Python API for contributing to and accessing demand-side grid model (dsgrid) datasets.

⚠️ **dsgrid is under active development and does not yet have a formal package release.** Details listed here are subject to change. Please reach out to the dsgrid coordination team with any questions or other feedback. ⚠️

[Install](#install) | [Usage](#usage) | [Uninstall](#uninstall)

## Install

[Virtual environment](#virtual-environment) | [Dependencies](#dependencies) | [from PIPY/pip](#from-pipypip) | [from pip+git](#from-pipgit) | [from cloned repository](#from-cloned-repository)

### Virtual environment

Create a virtual environment in which to install dsgrid. Miniconda is recommended.

```
conda create -n dsgrid python=3.10
conda activate dsgrid
```

### Dependencies
dsgrid uses [Apache Spark](#https://spark.apache.org/) to manage big data.

#### Windows

To install Apache Spark on Windows, follow [these instructions](https://towardsdatascience.com/installing-apache-pyspark-on-windows-10-f5f0c506bea1).

#### Mac

### From PIPY/pip

*Not yet available*

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
pip install -e .
```

**Developers:**
```
pip install -e '.[dev]'
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
