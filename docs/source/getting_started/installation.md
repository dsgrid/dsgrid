# Installation

:::{admonition} Contributing to dsgrid or installing from source?
:class: tip

This page covers installing the released dsgrid package. If you want to work on the dsgrid
code itself, follow [Development Setup](../developer_guide/development_setup) instead.
:::

## Python Environment

dsgrid requires Python 3.11 or later. Prebuilt packages are published for Python 3.11, 3.12, and
3.13 on Linux, macOS, and Windows, so we recommend one of those versions. We recommend installing
dsgrid into a virtual environment created with Python's built-in `venv` module.

:::{admonition} NLR Kestrel HPC
:class: note

Load a Python module before creating the environment, for example `module load python/3.13.12`
(run `module avail python` to see the available versions).
:::

### Create and Activate a Virtual Environment

Choose a working directory for your dsgrid work and create a virtual environment named `.venv`
inside it. You only need to create the environment once. Activate it in every new terminal
session where you want to use dsgrid.

::::{tab-set}

:::{tab-item} Mac/Linux
```bash
# Create the environment (once)
python3 -m venv .venv

# Activate the environment (each new terminal)
source .venv/bin/activate

# Upgrade pip (recommended)
python -m pip install --upgrade pip
```
:::

:::{tab-item} Windows (PowerShell)
```pwsh
# Create the environment (once)
py -m venv .venv

# Activate the environment (each new terminal)
.venv\Scripts\Activate.ps1

# Upgrade pip (recommended)
python -m pip install --upgrade pip
```

If PowerShell refuses to run the activation script, allow locally created scripts for your user
account and try again:

```pwsh
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```
:::

:::{tab-item} Windows (Command Prompt)
```bat
:: Create the environment (once)
py -m venv .venv

:: Activate the environment (each new terminal)
.venv\Scripts\activate.bat

:: Upgrade pip (recommended)
python -m pip install --upgrade pip
```
:::

::::

When an environment is active, your prompt shows its name (for example, `(.venv)`). To leave the
environment, run this command on any platform:

```bash
deactivate
```

## Package Installation

With your virtual environment active, install dsgrid with one of the following commands.

### To use DuckDB as the backend

```bash
pip install dsgrid-toolkit
```

### To use Apache Spark as the backend

```bash
pip install "dsgrid-toolkit[spark]"
```

:::{admonition} Spark on Windows
:class: warning

Apache Spark's Windows support is limited to local mode. On Windows, do not attempt to run a
Spark cluster; we recommend limiting dsgrid use to browsing the registry, registering and
submitting small- to medium-sized datasets, and development work with small test projects. Large
projects require Linux-based HPC or cloud resources. See also the Windows notes in
[Apache Spark](../user_guide/apache_spark/overview.md).
:::

#### Install Java

Java is only required when you use the Spark backend. dsgrid uses Apache Spark 4.0, which
requires **Java 17 or 21**. We recommend Java 21.

Spark uses the `JAVA_HOME` environment variable if it is set, and otherwise the `java` command on
your `PATH`. We recommend setting `JAVA_HOME`. Check your installation with:

::::{tab-set}

:::{tab-item} Mac/Linux
```bash
java --version
echo $JAVA_HOME
```
:::

:::{tab-item} Windows (PowerShell)
```pwsh
java --version
echo $env:JAVA_HOME
```
:::

:::{tab-item} Windows (Command Prompt)
```bat
java --version
echo %JAVA_HOME%
```
:::

::::

If Java 17 or 21 is not installed, install it as follows.

::::{tab-set}

:::{tab-item} Mac
Install Eclipse Temurin 21 with [Homebrew](https://brew.sh):

```bash
brew install --cask temurin@21
```

Then set `JAVA_HOME` by adding this line to your shell profile (e.g., `~/.zshrc`):

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```
:::

:::{tab-item} Linux
Install OpenJDK 21 with your distribution's package manager. If your distribution does not
package Java 21 (e.g., Debian 12), install Java 17 instead (`openjdk-17-jdk` or
`java-17-openjdk-devel`).

```bash
# Debian/Ubuntu
sudo apt install openjdk-21-jdk

# Fedora/RHEL
sudo dnf install java-21-openjdk-devel
```

Then set `JAVA_HOME` by adding this line to your shell profile (e.g., `~/.bashrc`):

```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```
:::

:::{tab-item} NLR Kestrel
A JDK is available on the shared filesystem. Add these lines to your shell profile or job
script:

```bash
export JAVA_HOME=/datasets/images/apache_spark/jdk-21.0.7
export PATH=$JAVA_HOME/bin:$PATH
```
:::

:::{tab-item} Windows
Download the Eclipse Temurin 21 JDK `.msi` installer from
[adoptium.net](https://adoptium.net/temurin/releases/?version=21). During installation, enable
the **Set JAVA_HOME variable** feature.

Alternatively, install it with winget:

```pwsh
winget install EclipseAdoptium.Temurin.21.JDK
```

Open a new terminal and run `echo $env:JAVA_HOME` (PowerShell) or `echo %JAVA_HOME%` (Command 
Prompt). If it prints nothing, set `JAVA_HOME` to the JDK installation directory (e.g., 
`C:\Program Files\Eclipse Adoptium\jdk-21...`) under
**System Properties → Environment Variables**.
:::

::::

## Registry

### NLR Shared Registry

The current dsgrid registries are stored in per-project SQLite database files. All configuration
information is stored in the database(s) and all dataset files are stored on the NLR HPC shared
filesystem.

### Standalone Registry

To use dsgrid in your own computational environment, you will need to initialize your own registry
with this CLI command:

```bash
dsgrid create-registry --help
```

## Runtime Backends

DuckDB is the default backend and is suitable for local work and moderate-sized datasets. Use the
Spark extra when you need a distributed backend for large datasets or HPC workflows.

- **NLR High Performance Computing**: [How to Start Spark Cluster on Kestrel](../user_guide/how_tos/spark_cluster_on_kestrel)
- **Standalone resources**: [Installing a Spark Standalone Cluster](../user_guide/apache_spark/overview.md#installing-a-spark-standalone-cluster)

## Test Your Installation

Confirm that the dsgrid command-line interface is installed:

```bash
dsgrid --help
```

If you're running dsgrid at NLR and using the shared registry, you can also test your access to
the registry with this command:

```bash
dsgrid -u sqlite:///<your-db-path> registry projects list
```

## Save Your Configuration

Running `dsgrid config create` stores key information for working with dsgrid in a config file at
`~/.dsgrid.json5`. Currently, dsgrid only supports offline mode, and the other key information to
store is the registry URL. The parameters in the config file are the default values used by the
command-line interface.

## Next Steps

- Learn about [browsing the registry](../user_guide/how_tos/browse_registry)
- Explore the [tutorials](../user_guide/tutorials/index) to get started with dsgrid
- Understand [data file formats](../user_guide/dataset_registration/data_file_formats) for preparing your data
