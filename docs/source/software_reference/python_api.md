# Python API

Most users interact with dsgrid through the CLI, but you can also drive the
full lifecycle — registry creation, dataset registration, project registration,
dataset submittal, and project querying — from Python. This page documents the
key classes for each workflow.


## Connecting to a Registry

```{eval-rst}
.. autopydantic_model:: dsgrid.registry.registry_database.DatabaseConnection
```

```{eval-rst}
.. autoclass:: dsgrid.registry.registry_manager.RegistryManager
   :members: load, create, dispose, dataset_manager, dimension_manager, dimension_mapping_manager, project_manager
   :special-members: __enter__, __exit__
```

### Example — Load an existing registry

```python
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.registry.registry_database import DatabaseConnection

conn = DatabaseConnection(url="sqlite:///path/to/registry.db")
manager = RegistryManager.load(conn, offline_mode=True)
```

### Example — Create a new registry

```python
from pathlib import Path
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.registry.registry_database import DatabaseConnection

conn = DatabaseConnection(url="sqlite:///my_registry.db")
manager = RegistryManager.create(
    conn,
    data_path=Path("registry_data"),
    overwrite=True,
)
```


## Browsing the Registry

The four manager properties on `RegistryManager` provide access to everything
stored in the registry.

```{eval-rst}
.. autoclass:: dsgrid.registry.project_registry_manager.ProjectRegistryManager
   :members: get_by_id, load_project, show
   :noindex:
```

```{eval-rst}
.. autoclass:: dsgrid.registry.dataset_registry_manager.DatasetRegistryManager
   :members: get_by_id, show
   :noindex:
```

```{eval-rst}
.. autoclass:: dsgrid.registry.dimension_registry_manager.DimensionRegistryManager
   :members: get_by_id, show
   :noindex:
```

```{eval-rst}
.. autoclass:: dsgrid.registry.dimension_mapping_registry_manager.DimensionMappingRegistryManager
   :members: get_by_id, show
   :noindex:
```

### Example — Inspect project dimensions

```python
from dsgrid.dimension.base_models import DimensionType

project = manager.project_manager.load_project("dsgrid_conus_2022")
geo_dim = project.config.get_base_dimension(DimensionType.GEOGRAPHY)
geo_dim.get_records_dataframe().show()
print(geo_dim.get_unique_ids())

# Show the records for a supplemental dimension.
project.config.get_dimension_records("commercial_end_uses").show()
```


## Projects and Datasets

```{eval-rst}
.. autoclass:: dsgrid.project.Project
   :members: config, version, get_dataset, load_dataset, is_registered
```

```{eval-rst}
.. autoclass:: dsgrid.config.project_config.ProjectConfig
   :members: get_base_dimension, get_base_time_dimension, get_dimension, get_dimension_records, get_dimension_record_ids, list_registered_dataset_ids, list_unregistered_dataset_ids, list_base_dimensions, list_supplemental_dimensions, get_base_to_supplemental_mapping_records, get_base_to_supplemental_dimension_mappings_by_types, list_dimension_names
```

```{eval-rst}
.. autoclass:: dsgrid.config.dataset_config.DatasetConfig
   :members: load_from_user_path
```

```{eval-rst}
.. autoclass:: dsgrid.config.dimension_config.DimensionConfig
   :members: get_records_dataframe, get_unique_ids
```

```{eval-rst}
.. autoclass:: dsgrid.dimension.base_models.DimensionType
   :members:
   :undoc-members:
```


## Config Generation

These helper functions create dataset and project configuration dictionaries
programmatically, bypassing Pydantic validation. They are useful when building
configs in scripts or tests where file-path resolution is not needed.

```{eval-rst}
.. autofunction:: dsgrid.config.dataset_config.make_unvalidated_dataset_config
```

```{eval-rst}
.. autofunction:: dsgrid.config.project_config.make_unvalidated_project_config
```

### Example — Build a dataset config programmatically

```python
import json5
from pathlib import Path
from dsgrid.config.dataset_config import make_unvalidated_dataset_config

config_dict = make_unvalidated_dataset_config(
    dataset_id="my_dataset",
    metric_type="energy",
)

# Write to a JSON5 file for registration.
Path("my_dataset/dataset.json5").write_text(json5.dumps(config_dict, indent=2))
```

### Example — Build a project config programmatically

```python
import json5
from pathlib import Path
from dsgrid.config.project_config import make_unvalidated_project_config

config_dict = make_unvalidated_project_config(
    project_id="my_project",
    dataset_ids=["dataset_a", "dataset_b"],
    metric_types=["energy"],
    name="My Project",
    description="An example project.",
)

Path("my_project/project.json5").write_text(json5.dumps(config_dict, indent=2))
```


## Registration

### Registering dimensions

Dimensions are usually registered automatically as part of dataset or project
registration (via inline `dimensions` in the config). If you need to register
dimensions independently — for example, to share a dimension across multiple
datasets before registering any of them — use this method.

```{eval-rst}
.. automethod:: dsgrid.registry.dimension_registry_manager.DimensionRegistryManager.register
```

### Registering dimension mappings

Dimension mappings are usually registered as part of `submit_dataset`. If you
need to register mappings independently — for example, to reuse a mapping
across multiple dataset submissions, or to use with a standalone dataset
query — use this method.

```{eval-rst}
.. automethod:: dsgrid.registry.dimension_mapping_registry_manager.DimensionMappingRegistryManager.register
```

### Registering a dataset

```{eval-rst}
.. automethod:: dsgrid.registry.dataset_registry_manager.DatasetRegistryManager.register
```

### Registering a project

```{eval-rst}
.. automethod:: dsgrid.registry.project_registry_manager.ProjectRegistryManager.register
```

### Submitting a dataset to a project

```{eval-rst}
.. automethod:: dsgrid.registry.project_registry_manager.ProjectRegistryManager.submit_dataset
```

```{eval-rst}
.. automethod:: dsgrid.registry.project_registry_manager.ProjectRegistryManager.register_and_submit_dataset
```

### Example — Register and submit a dataset

```python
from pathlib import Path

# Register the dataset.
manager.dataset_manager.register(
    config_file=Path("my_dataset/dataset.json5"),
    submitter="Jane Doe",
    log_message="Initial registration of my_dataset",
)

# Submit it to a project with dimension mappings.
manager.project_manager.submit_dataset(
    project_id="dsgrid_conus_2022",
    dataset_id="my_dataset",
    submitter="Jane Doe",
    log_message="Submit my_dataset to dsgrid_conus_2022",
    dimension_mapping_file=Path("my_dataset/dimension_mappings.json5"),
)
```


## Queries

### Project query data models

```{eval-rst}
.. autopydantic_model:: dsgrid.query.models.ProjectQueryModel
.. autopydantic_model:: dsgrid.query.models.ProjectQueryParamsModel
.. autopydantic_model:: dsgrid.query.models.QueryResultParamsModel
.. autopydantic_model:: dsgrid.query.models.DatasetModel
.. autopydantic_model:: dsgrid.query.models.StandaloneDatasetModel
.. autopydantic_model:: dsgrid.query.models.ProjectionDatasetModel
.. autopydantic_model:: dsgrid.query.models.AggregationModel
.. autopydantic_model:: dsgrid.query.models.DimensionNamesModel
.. autopydantic_model:: dsgrid.query.models.ColumnModel
```

```{eval-rst}
.. autoclass:: dsgrid.query.models.ColumnType
.. autoclass:: dsgrid.query.models.DatasetType
```

### Dataset query data models

A *dataset query* remaps a registered dataset's dimensions without involving a
project. This is useful when you want to map a dataset to alternate dimensions
(e.g., county → state) as a standalone operation. The required dimension
mappings must already be registered in the registry.

```{eval-rst}
.. autopydantic_model:: dsgrid.query.models.DatasetQueryModel
```

```{eval-rst}
.. autofunction:: dsgrid.query.models.make_dataset_query
```

### Query submission

```{eval-rst}
.. autoclass:: dsgrid.query.query_submitter.ProjectQuerySubmitter
   :members: submit
```

```{eval-rst}
.. autoclass:: dsgrid.query.query_submitter.DatasetQuerySubmitter
   :members: submit
```

```{eval-rst}
.. autoclass:: dsgrid.query.query_submitter.CompositeDatasetQuerySubmitter
   :members: submit, create_dataset
```

### Example — Submit a project query

```python
from dsgrid.query.models import (
    AggregationModel,
    DatasetModel,
    DimensionNamesModel,
    ProjectQueryParamsModel,
    ProjectQueryModel,
    QueryResultParamsModel,
    StandaloneDatasetModel,
)
from dsgrid.query.query_submitter import ProjectQuerySubmitter

project = manager.project_manager.load_project("dsgrid_conus_2022")
submitter = ProjectQuerySubmitter(project, output_dir=Path("query_output"))

query = ProjectQueryModel(
    name="Total Electricity Use By State and Sector",
    project=ProjectQueryParamsModel(
        project_id="dsgrid_conus_2022",
        dataset=DatasetModel(
            dataset_id="electricity_use",
            source_datasets=[
                StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                StandaloneDatasetModel(dataset_id="resstock_conus_2022_projected"),
                StandaloneDatasetModel(dataset_id="tempo_conus_2022_mapped"),
            ],
        ),
    ),
    result=QueryResultParamsModel(
        aggregations=[
            AggregationModel(
                dimensions=DimensionNamesModel(
                    geography=["state"],
                    metric=["electricity_collapsed"],
                    model_year=[],
                    scenario=[],
                    sector=["sector"],
                    subsector=[],
                    time=[],
                    weather_year=[],
                ),
                aggregation_function="sum",
            ),
        ],
    ),
)

df = submitter.submit(query)
df.show()
```

### Example — Map a dataset to alternate dimensions

```python
from pathlib import Path
from dsgrid.config.dimensions import DimensionReferenceModel
from dsgrid.query.models import make_dataset_query
from dsgrid.query.query_submitter import DatasetQuerySubmitter

# Identify the target dimension (must already be registered).
to_dim_ref = DimensionReferenceModel(
    dimension_type="geography",
    dimension_id="<state-dimension-uuid>",
    version="1.0.0",
)

query = make_dataset_query(
    name="my_dataset_remapped_to_state",
    dataset_id="my_dataset",
    to_dimension_references=[to_dim_ref],
)

submitter = DatasetQuerySubmitter(output_dir=Path("dataset_query_output"))
df = submitter.submit(query, mgr=manager)
df.show()
```
