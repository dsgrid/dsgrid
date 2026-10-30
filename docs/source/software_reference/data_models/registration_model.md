# Bulk Registration

## RegistrationModel

*dsgrid.config.registration_models.RegistrationModel*

Defines a list of projects and datasets to be registered.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `projects` | list[[ProjectRegistrationModel](registration_model.md#projectregistrationmodel)] | *(required)* | List of projects to register. |
| `datasets` | list[[DatasetRegistrationModel](registration_model.md#datasetregistrationmodel)] | *(required)* | List of datasets to register. |
| `dataset_submissions` | list[[DatasetSubmissionModel](registration_model.md#datasetsubmissionmodel)] | *(required)* | List of datasets to be submitted to projects. |

</div>


---

## DatasetRegistrationModel

*dsgrid.config.registration_models.DatasetRegistrationModel*

Defines a dataset to be registered.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `dataset_id` | `str` | *(required)* | Dataset ID |
| `config_file` | `Path` | *(required)* | Path to dataset.json5 |
| `replace_dimension_names_with_ids` | `bool` | `False` | Replace the dimension entries with IDs of dimensions in the database with matching names. Typically only useful for tests. |
| `log_message` | `str` \| None | `None` | Log message to use when registering the dataset. Defaults to an auto-generated message. |

</div>

### Validators

<div class="model-validators-table">

| Name | Applies To | Description |
|------|------------|-------------|
| `fix_log_message` | `fix_log_message` | No description |
| `fix_paths` | `*(model)*` | No description |

</div>


---

## DatasetSubmissionModel

*dsgrid.config.registration_models.DatasetSubmissionModel*

Defines how a dataset should be submitted to a project.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `dataset_id` | `str` | *(required)* |  |
| `project_id` | `str` | *(required)* |  |
| `dimension_mapping_file` | `Path` \| None | `None` | Path to file containing mappings of dataset-to-project dimensions |
| `dimension_mapping_references_file` | `Path` \| None | `None` | Path to file containing references to mappings of dataset-to-project dimensions |
| `replace_dimension_mapping_names_with_ids` | `bool` | `False` | Replace the dimension mapping entries with IDs of dimension mappings in the database with matching names. Typically only useful for tests. |
| `autogen_reverse_supplemental_mappings` | `set` | `set()` | Dimensions on which to attempt create reverse mappings from supplemental dimensions. |
| `log_message` | `str` \| None | `None` | Log message to use when submitting the dataset. Defaults to an auto-generated message. |

</div>

### Validators

<div class="model-validators-table">

| Name | Applies To | Description |
|------|------------|-------------|
| `fix_log_message` | `fix_log_message` | No description |
| `fix_autogen_reverse_supplemental_mappings` | `*(model)*` | No description |

</div>


---

## ProjectRegistrationModel

*dsgrid.config.registration_models.ProjectRegistrationModel*

Defines a project to be registered.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project_id` | `str` | *(required)* | Project ID |
| `config_file` | `Path` | *(required)* | Path to project.json5 |
| `log_message` | `str` \| None | `None` | Log message to use when registering the project. Defaults to an auto-generated message. |

</div>

### Validators

<div class="model-validators-table">

| Name | Applies To | Description |
|------|------------|-------------|
| `fix_log_message` | `fix_log_message` | No description |
| `fix_paths` | `*(model)*` | No description |

</div>
