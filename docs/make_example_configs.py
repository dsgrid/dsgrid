"""Copy example configs from the US Project Repo to use for docs.
"""
from dsgrid.filesystem.local_filesystem import LocalFilesystem
import os
from pathlib import Path


# Specify source of configs
if os.environ.get("CI") is not None:
    username = os.environ["ACCESS_KEY"]
    token = os.environ["ACCESS_TOKEN"]
    # TODO git clone SS repo

PROJECT_REPO = Path(__file__).resolve().parents[2] / "dsgrid" / "dsgrid-project-StandardScenarios"

base_dir = PROJECT_REPO / "dsgrid_project"
dataset_dir = base_dir / "datasets" / "sector_models" / "comstock"

project_config = base_dir / "project.toml"
dataset_config = dataset_dir / "dataset.toml"
dataset_to_project_dimension_mapping_config = dataset_dir / "dimension_mappings.toml"

# Specify destination for configs
docs_dir = Path(__file__).resolve().parent / "_build" / "example_configs"
docs_dir.mkdir(parents=True, exist_ok=True)

# Copy the following:
fs = LocalFilesystem()
configs = [
    project_config,
    dataset_config,
    dataset_to_project_dimension_mapping_config,
]
for config in configs:
    filename = config.name
    docs_config = docs_dir / filename
    fs.copy_file(src=config, dst=docs_config)
