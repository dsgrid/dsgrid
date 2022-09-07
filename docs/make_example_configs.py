"""Copy example configs from the US Project Repo to use for docs.
"""
import os
from pathlib import Path

from dsgrid.filesystem.local_filesystem import LocalFilesystem
from dsgrid.utils.run_command import check_run_command


# Specify source of configs
if os.environ.get("CI") is not None:
    token = os.environ["ACCESS_TOKEN"]
    project_repo = Path(".") / "dsgrid-project-StandardScenarios"
    assert not project_repo.exists()
    cmd = f"git clone https://{token}/@github.com/dsgrid/dsgrid-project-StandardScenarios.git {project_repo}"
    check_run_command(cmd)
else:
    project_repo = Path(os.environ.get("DSGRID_PROJECT_STANDARD_SCENARIOS_PATH"))
    if project_repo is None:
        raise Exception(
            "The environment variable DSGRID_PROJECT_STANDARD_SCENARIOS_PATH must be defined when building docs locally."
        )

base_dir = project_repo / "dsgrid_project"
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
