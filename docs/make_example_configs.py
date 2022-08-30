"""Copy example configs from the US Project Repo to use for docs.
"""

from dsgrid.filesystem.local_filesystem import LocalFilesystem
import os
from pathlib import Path


PROJECT_REPO = "https://github.com/dsgrid/dsgrid-project-StandardScenarios.git"

base_dir = PROJECT_REPO / "dsgrid_project"
dataset_dir = base_dir / "datasets" / "sector_models" / "comstock"

project_config = base_dir / "project.toml"
dataset_config = dataset_dir / "dataset.toml"
dataset_to_project_dimension_mapping_config = dataset_dir / "dimension_mappings.toml"

docs_dir = Path(__file__).resolve().parent / "_build" / "example_configs"

os.makedirs(docs_dir, exist_ok=True)

fs = LocalFilesystem()

for config in (
    project_config,
    dataset_config,
    dataset_to_project_dimension_mapping_config,
):
    filename = config.name
    docs_config = docs_dir / filename
    fs.copy_file(src=config, dst=docs_config)
