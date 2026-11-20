# Development Setup

This page describes how to set up an environment for developing dsgrid: cloning the repository,
installing the Rust toolchain, making an editable install with git hooks, and building the
documentation. If you
only want to use dsgrid, follow [Installation](../getting_started/installation) instead.

## Clone the Repository

dsgrid uses git submodules, including the `dsgrid-test-data` repository required by the tests.
Clone with submodules:

```bash
git clone --recurse-submodules https://github.com/dsgrid/dsgrid.git
cd dsgrid
```

If you already cloned without `--recurse-submodules`, initialize them with:

```bash
git submodule update --init --recursive
```

Refer to [Testing](testing.md) for how to keep the test data submodule up to date.

:::{note}
The repository uses git pre-commit hooks for formatting, linting, and type checking. They are
installed in the [editable install](#install-dsgrid-in-editable-mode) step below, once the
`pre-commit` tool is available. Don't skip that step.
:::

## Create a Virtual Environment

Create a virtual environment named `.venv` in the root of your clone (it is already ignored by
git) and activate it. The commands are the same as in
[Installation](../getting_started/installation.md#create-and-activate-a-virtual-environment):

::::{tab-set}

:::{tab-item} Mac/Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```
:::

:::{tab-item} Windows (PowerShell)
```pwsh
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```
:::

:::{tab-item} Windows (Command Prompt)
```bat
py -m venv .venv
.venv\Scripts\activate.bat
python -m pip install --upgrade pip
```
:::


::::

On NLR's Kestrel HPC, load a Python module first, e.g., `module load python/3.13.12`.

## Install Rust

dsgrid includes a Rust extension, `dsgrid.minimal_patterns`, which identifies minimal patterns of
missing dimension combinations during dataset registration and submission. Released packages
include it prebuilt, but building dsgrid from source requires the Rust toolchain (`cargo`). Install
it with [rustup](https://rustup.rs).

::::{tab-set}

:::{tab-item} Mac/Linux
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Accept the default installation, then open a new terminal or run `. "$HOME/.cargo/env"`.

Rust needs a C linker:

- **Mac:** install the Xcode Command Line Tools with `xcode-select --install` if you have not
  already.
- **Linux:** install your distribution's C toolchain if needed (e.g., `sudo apt install
  build-essential` or `sudo dnf install gcc`).
:::

:::{tab-item} Windows
Rust on Windows requires the Microsoft C++ build tools (the MSVC compiler and linker and the
Windows SDK). Install them yourself **before** installing Rust. `rustup-init.exe` offers to
install them, but it may offer an older Visual Studio version, and that route is not always
reliable.

1. Download and run the installer for the latest
   [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) (or
   Visual Studio Community).
2. On the **Individual components** tab, select:
   - **MSVC C++ x64/x86 build tools (Latest)**
   - The latest **Windows 11 SDK** (or Windows 10 SDK on Windows 10)

   Alternatively, select the **Desktop development with C++** workload, which includes these
   components and more.
3. Install rustup, either by downloading and running `rustup-init.exe` from
   [rustup.rs](https://rustup.rs) and accepting the defaults, or with winget:

   ```pwsh
   winget install --id Rustlang.Rustup -e
   ```

Open a new terminal after installation.
:::

:::{tab-item} NLR Kestrel
There is no Rust module on Kestrel. Install rustup into your home directory with the Mac/Linux
command. The toolchain uses roughly 1 GB; if your home directory quota is tight, set `RUSTUP_HOME`
and `CARGO_HOME` to a directory on `/projects` or `/scratch` before running the installer and in
your shell profile.
:::

::::

Verify the installation:

```bash
cargo --version
```

The Rust dependencies require a recent compiler (currently rustc 1.88 or later). If you installed
Rust previously and the build fails with a message like `requires rustc 1.88`, update the
toolchain:

```bash
rustup update
```

## Install dsgrid in Editable Mode

From the root of your clone, with the virtual environment active:

```bash
pip install -e ".[dev]"
```

Add extras as needed:

| Extra | Contents |
| --- | --- |
| `dev` | Testing, linting, type checking, and `maturin` |
| `spark` | Apache Spark backend (requires [Java 17 or 21](../getting_started/installation.md#install-java)) |
| `doc` | Sphinx and extensions for building the documentation |

For example, `pip install -e ".[dev,doc,spark]"` installs everything, which matches the
documentation CI job.

Then install the git pre-commit hooks (the `pre-commit` tool comes with the `dev` extra):

```bash
pre-commit install
```

The hooks run on every `git commit`. They include `ty check`, which runs against your installed
environment (matching CI), so keep your virtual environment active when you commit.

:::{important}
If `cargo` is not on your `PATH`, the install still succeeds but prints a warning and skips the
Rust extension. Features that depend on it, such as writing minimal-pattern files for missing
dimension associations, will be unavailable. Check that the extension is present with:

```bash
python -c "import dsgrid.minimal_patterns"
```
:::

### Rebuilding the Rust Extension

Editable installs pick up changes to Python files immediately, but changes to the Rust code in
`rust/` are only compiled at install time. After modifying Rust code, rebuild with either:

```bash
pip install -e ".[dev]"
# or
maturin develop --release
```

## Build the Documentation

Install the `doc` extra, then build the HTML documentation from the `docs` directory. The
repository includes a `Makefile` for Mac/Linux and an equivalent `make.bat` for Windows.

::::{tab-set}

:::{tab-item} Mac/Linux
```bash
cd docs
make html
```
:::

:::{tab-item} Windows (PowerShell)
```pwsh
cd docs
.\make.bat html
```
:::

:::{tab-item} Windows (Command Prompt)
```bat
cd docs
make.bat html
```
:::

::::

The output is written to `docs/_build/html`; open `docs/_build/html/index.html` in a browser to
view it. See `docs/README.md` for the documentation style guide. The published documentation is 
built automatically by a GitHub Action.

## Releasing to PyPI

1. Update the version in **both** of these files to the new version (e.g., `0.5.2`):

   - `__version__` in `dsgrid/__init__.py`
   - `version` under `[package]` in `rust/Cargo.toml`

   :::{warning}
   The wheels and source distribution are built with maturin, which takes the package version
   from `rust/Cargo.toml`, **not** from `dsgrid/__init__.py`. If you only update
   `dsgrid/__init__.py`, the release builds files with the previous version number and the PyPI
   upload fails with `400 File already exists`. This happened with both v0.4.0 and v0.5.0.
   :::

   Recommended: also update the version recorded in the tracked `rust/Cargo.lock`. The release
   build does this automatically if you forget, but the lock file would then be left out of date
   in git, and the next developer's first build would modify it. This command changes only
   dsgrid's own entry, without compiling anything or upgrading dependencies:

   ```bash
   cargo update --workspace --manifest-path rust/Cargo.toml
   ```

   Optional: confirm the version that maturin will use. This does not compile anything; the
   output file name (e.g., `dsgrid_toolkit-0.5.2.tar.gz`) should show the new version.

   ```bash
   maturin sdist --out /tmp/dsgrid-sdist
   ```

2. Commit the changes and merge them to `main`.
3. Create a GitHub release with a tag matching the new version (e.g., `v0.5.2`).
4. Publishing the release triggers `.github/workflows/publish_to_pypi.yml`, which builds wheels
   for Linux, macOS, and Windows (Python 3.11–3.13) plus a source distribution and uploads them
   to PyPI. Check that the workflow succeeds.

## Next Steps

- [Testing](testing.md): running the test suite and working with the test data submodule
- [Registry Database](registry_database.md): the registry schema and how to inspect it
- [Local Tools](how_tos/local_tools.md): the API server, project viewer, and other development
  conveniences
