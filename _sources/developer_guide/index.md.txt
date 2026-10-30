# Developer Guide

Resources for contributing to dsgrid development and understanding the codebase architecture.

For an overview of the system design, see [Software Architecture](../software_reference/architecture.md).

## How-Tos

```{toctree}
:maxdepth: 1

how_tos/how_to_convert_time_zone
```

## Planned Content

- **Development setup** — Installing development dependencies, pre-commit
  hooks, and building the documentation.
- **Testing** — Running the test suite, the test data submodule, and writing
  new tests.
- **Contributing** — Pull request workflow, coding conventions, and the
  release process.
- **API reference** — Reference documentation for the Python API.
- **More how-tos**, for example:
  - Building the Rust extension (`dsgrid.minimal_patterns`) with maturin.
  - Running the tests against the Spark backend.
  - Rebuilding the cached test registry and updating the `dsgrid-test-data`
    submodule.
  - Documenting a new data model in the generated model reference pages.
  - Adding a new time dimension type.
