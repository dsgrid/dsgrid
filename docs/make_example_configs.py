"""Copy example configs from the US Project Repo to use for docs.
"""
import os
import logging
from pathlib import Path
import requests


# Specify source of configs
PROJECT_REPO = "https://raw.githubusercontent.com/dsgrid/dsgrid-project-StandardScenarios/main"

base_dir = os.path.join(PROJECT_REPO, "dsgrid_project")
dataset_dir = os.path.join(base_dir, "datasets", "sector_models", "comstock")

project_config = os.path.join(base_dir, "project.toml")
project_config_token = "?token=GHSAT0AAAAAABJFWPWFKEUMTUQOBPWSR2DEYYXQ26A"
dataset_config = os.path.join(dataset_dir, "dataset.toml")
dataset_config_token = "?token=GHSAT0AAAAAABJFWPWEQW6WXQCEHTPOKODKYYXQROA"
dataset_to_project_dimension_mapping_config = os.path.join(dataset_dir, "dimension_mappings.toml")
dataset_to_project_dimension_mapping_config_token = (
    "?token=GHSAT0AAAAAABJFWPWFEXIIAKYYMVSXW4PQYYXQVCA"
)

# Specify destination for configs
docs_dir = Path(__file__).resolve().parent / "_build" / "example_configs"
docs_dir.mkdir(parents=True, exist_ok=True)

# Copy the following:
config_dict = {
    project_config: project_config_token,
    dataset_config: dataset_config_token,
    dataset_to_project_dimension_mapping_config: dataset_to_project_dimension_mapping_config_token,
}
for config, token in config_dict.items():
    url = config + token
    filename = os.path.basename(config)
    docs_config = docs_dir / filename
    print(f" Copying config\n\tfrom: {url}\n\tto: {docs_config}")
    res = requests.get(url)

    if not res:
        logging.exception(f"An error has occurred. Status_code={res.status_code}")

    with open(docs_config, "wb") as f:
        f.write(res.content)
