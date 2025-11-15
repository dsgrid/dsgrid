# dsgrid AI Agent Instructions

## Project Overview
dsgrid is a Python framework for managing demand-side grid model datasets using a registry-based architecture with Apache Spark/DuckDB for big data processing. The project provides a CLI tool and Python API for registering, querying, and analyzing energy datasets across multiple dimensions (time, geography, sector, subsector, enduse, etc.).

## Architecture

### Core Components
- **Registry System** (`dsgrid/registry/`): Manages registration of projects, datasets, dimensions, and dimension mappings using SQLAlchemy + SQLite/PostgreSQL backend
  - `RegistryManager`: Top-level orchestrator (`registry/registry_manager.py`)
  - `ProjectRegistryManager`, `DatasetRegistryManager`, `DimensionRegistryManager`, `DimensionMappingRegistryManager`: Domain-specific managers
  - Registry operations are hierarchical: dimension → dimension_mapping → dataset → project

- **Config System** (`dsgrid/config/`): Pydantic-based configuration models
  - All configs inherit from `ConfigBase` or `DSGBaseModel`
  - Extensive use of field validators and model validators for cross-field validation
  - Configs serialize to/from JSON with support for relative file paths

- **Spark/DuckDB Abstraction** (`dsgrid/spark/`): Dual backend support
  - Check backend with `use_duckdb()` from `dsgrid.spark.types`
  - Import unified interfaces from `dsgrid.spark.types` (DataFrame, F, SparkSession, etc.)
  - Backend selected via `DSGRID_BACKEND_ENGINE` env var or `.dsgrid.json5` runtime config
  - DuckDB is default for local development; Spark for production/HPC

- **Dataset Handling** (`dsgrid/dataset/`): Schema handlers for StandardSchema vs OneTable formats
  - Load data from Parquet or CSV files
  - Handle pivoted/unpivoted table formats
  - Apply dimension mappings and unit conversions

- **Query System** (`dsgrid/query/`): Project-based querying with dimension aggregation/filtering

## Key Patterns & Conventions

### Configuration Models
- All config classes use Pydantic v2 with custom `DSGBaseModel` base class
- Use `@field_validator` for single-field validation, `@model_validator(mode="before")` for cross-field validation
- File references in configs are relative to config file location (validated via `in_other_dir` context manager)
- Config IDs must match pattern: lowercase with hyphens/underscores/numbers (validated via `check_config_id_strict`)

```python
from dsgrid.data_models import DSGBaseModel
from dsgrid.config.config_base import ConfigBase

class MyConfig(ConfigBase):  # Use ConfigBase for registry configs
    # Fields use snake_case
    dataset_id: str

    @field_validator("dataset_id")
    @classmethod
    def validate_id(cls, v):
        check_config_id_strict(v)
        return v
```

### Exception Handling
- Use domain-specific exceptions from `dsgrid.exceptions`
- Common: `DSGInvalidParameter`, `DSGInvalidQuery`, `DSGValueNotRegistered`, `DSGInvalidOperation`
- Don't catch exceptions unless you can handle them meaningfully; let them propagate

### Logging
- Get logger with `logger = logging.getLogger(__name__)`
- Use structured log levels: debug for detailed tracing, info for user actions, warning for deprecations
- CLI handles logging setup via `setup_logging` in `dsgrid.loggers`

### Spark/DuckDB Usage
- Always import from `dsgrid.spark.types` for backend-agnostic code
- Use `@pytest.mark.skipif(use_duckdb(), reason="...")` for Spark-only tests
- Prefer DuckDB SQL dialect when using `use_duckdb()` branch (e.g., `CAST(... AS BIGINT)` not `BIGINT(...)`)

### Performance Tracking
- Decorate performance-critical functions with `@track_timing(timer_stats_collector)`
- Enable via `--timings` CLI flag or `timings: true` in runtime config
- Import: `from dsgrid.utils.timing import track_timing, timer_stats_collector`

## Development Workflows

### Setup
```powershell
conda create -n dsgrid python=3.11
conda activate dsgrid
pip install -e '.[dev,spark]' --group=pyhive
```

### Testing
- Use `pytest` (configured in `pyproject.toml`)
- Test data in `dsgrid-test-data/` submodule (must run `git submodule init && git submodule update`)
- Key fixtures in `tests/conftest.py`:
  - `cached_registry`: Shared read-only test registry (session scope)
  - `mutable_cached_registry`: Writable registry copy (function scope)
  - Spark session automatically initialized per module

```powershell
pytest tests/test_queries.py -v
pytest -k "test_registry" --cov=dsgrid --cov-report=html
```

### CLI Structure
- Main CLI: `dsgrid` (defined in `dsgrid/cli/dsgrid.py`)
- Admin CLI: `dsgrid-admin` (for registry creation)
- Common options: `--offline`, `--url`, `--scratch-dir`, `--console-level`, `--timings`
- Commands organized as Click groups: `registry`, `query`, `config`, `download`

```powershell
# Registry operations
dsgrid --offline registry projects list
dsgrid registry datasets register path/to/config.json5

# Query operations
dsgrid query project run --project-id my-project --output results.parquet
```

### Registry Operations
- Always use `DatabaseConnection` for DB connections (handles SQLite URLs)
- Registry can run in offline mode (no sync with remote) or online mode
- Create registry: `dsgrid-admin create-registry <url> --data-path <path>`
- Bulk register: `dsgrid registry bulk-register registration.json5`

### Code Style
- Line length: 99 characters (configured in `pyproject.toml`)
- Use Ruff for linting/formatting
- Type hints required for function signatures
- Docstrings: NumPy style (numpydoc)

## Common Gotchas
- Windows Spark support is limited to local mode only; use DuckDB backend on Windows
- Java 8+ required with `JAVA_HOME` set for Spark
- Registry database migrations are NOT automatic; schema changes may require registry recreation
- Time dimensions require timezone-aware handling; use `chronify` library integration
- Dimension mappings must be registered before datasets that use them
- Dataset load data filenames are restricted: `load_data.parquet`, `load_data.csv`, or `table.parquet`

## File Locations
- Runtime config: `~/.dsgrid.json5` (created via `DsgridRuntimeConfig`)
- Default log file: `dsgrid.log` in current directory
- Test data registry: `tests/data/` (committed), `dsgrid-test-data/` (submodule)
- Scratch directories: Configurable via `--scratch-dir`, defaults to current directory
