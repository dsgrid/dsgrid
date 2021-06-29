import os
import sys


PROJECT_REPO = os.environ.get("TEST_PROJECT_REPO")
if PROJECT_REPO is None:
    print(
        "You must define the environment TEST_PROJECT_REPO with the path to the dsgrid-project-EFS repository"
    )
    sys.exit(1)


PROJECT_CONFIG_FILE = os.path.join(PROJECT_REPO, "dsgrid_project", "project.toml")
DIMENSION_CONFIG_FILE = os.path.join(PROJECT_REPO, "dsgrid_project", "dimensions.toml")
