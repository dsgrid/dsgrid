"""Copy example configs from the US Project Repo to use for docs.
"""

from dsgrid.filesystem.local_filesystem import LocalFilesystem
import os
from pathlib import Path


PROJECT_REPO = Path(__file__).resolve().parent.parent / "dsgrid-test-data" / "test_efs"

base_dir = PROJECT_REPO / "dsgrid_project"
dataset_dir = base_dir / "datasets" / "sector_models" / "comstock"

project_config = base_dir / "project.toml"
dimensions_config = base_dir / "dimensions.toml"
dimension_mappings_config = base_dir / "dimension_mappings.toml"
dataset_config = dataset_dir / "dataset.toml"
dimension_mapping_ref_config = dataset_dir / "dimension_mapping_references.toml"

docs_dir = Path(__file__).resolve().parent / "_build" / "example_configs"

os.makedirs(docs_dir, exist_ok=True)

fs = LocalFilesystem()

for config in (
    project_config,
    dimensions_config,
    dimension_mappings_config,
    dataset_config,
    dimension_mapping_ref_config,
):
    filename = config.name
    docs_config = docs_dir / filename
    fs.copy_file(src=config, dst=docs_config)
