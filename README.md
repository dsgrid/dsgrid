# dsgrid
[![Documentation](https://img.shields.io/badge/docs-ready-blue.svg)](https://dsgrid.github.io/dsgrid)
[![codecov](https://codecov.io/gh/dsgrid/dsgrid/branch/main/graph/badge.svg?token=W0441C9XAL)](https://codecov.io/gh/dsgrid/dsgrid)

Python API for contributing to and accessing demand-side grid model (dsgrid) projects and datasets.

**dsgrid is under active development.** Please reach out to the dsgrid coordination team with any questions or other feedback.

[Install](#install) | [Usage](#usage) | [Uninstall](#uninstall) | [Documentation](https://dsgrid.github.io/dsgrid)

## Install

dsgrid requires Python 3.11 or later (prebuilt packages are available for 3.11–3.13). Create and
activate a virtual environment, then install dsgrid with pip.

Virtual environment:

**Mac/Linux:**
```
python3 -m venv .venv
source .venv/bin/activate
```

**Windows (PowerShell):**
```
py -m venv .venv
.venv\Scripts\Activate.ps1
```

Install dsgrid with the default DuckDB backend:
```
pip install dsgrid-toolkit
```

or with Apache Spark support (requires Java 17 or 21):
```
pip install "dsgrid-toolkit[spark]"
```

Spark's Windows support is limited to local mode, so large projects require Linux-based HPC or
cloud resources.

See the [Installation guide](https://dsgrid.github.io/dsgrid/getting_started/installation.html)
for full instructions, including Java setup and registry configuration.

**Developers:** to work on dsgrid itself (editable install, Rust extension, tests), see
[Development Setup](https://dsgrid.github.io/dsgrid/developer_guide/development_setup.html).

## Usage

dsgrid is primarily a command-line interface (CLI) tool. To see the available commands:
```
dsgrid --help
```

## Uninstall

```
pip uninstall dsgrid-toolkit
deactivate
```

You can then delete the `.venv` directory.

## Software Record

dsgrid is developed under National Laboratory of the Rockies (formerly NREL) Software Record SWR-21-52, "demand-side grid model".
