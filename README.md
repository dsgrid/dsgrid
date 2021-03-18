# dsgrid
[![Documentation](https://img.shields.io/badge/docs-ready-blue.svg)](https://dsgrid.github.io/dsgrid)

Python API for accessing demand-side grid model (dsgrid) datasets. 

[Install](#install) | [Usage](#usage) | [Uninstall](#uninstall)

## Install

[Dependencies](#dependencies) | [from PIPY/pip](#from-pipypip) | [from pip+git](#from-pipgit) | [from cloned repository](#from-cloned-repository)

### Dependencies
dsgrid uses Spark. To be fully functional, you will also need to point to a dsgrid project.

#### Windows

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

**Conda:** 
```
conda env create -f environment.yml
# conda activate?

pip install -e .

# or

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

Or, if you are using a conda environment
```
conda deactivate ???
```