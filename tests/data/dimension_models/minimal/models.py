import os
import sys


data_repo = os.environ.get("US_DATA_REPO")
if data_repo is None:
    print(
        "You must define the environment US_DATA_REPO with the path to the dsgrid-data-UnitedStates repository"
    )
    sys.exit(1)


PROJECT_CONFIG_FILE = os.path.join(data_repo, "dsgrid_project", "project.toml")
DIMENSION_CONFIG_FILE = os.path.join(data_repo, "dsgrid_project", "dimensions.toml")
