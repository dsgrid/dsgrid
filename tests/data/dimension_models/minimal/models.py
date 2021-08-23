import os

from dsgrid.tests.common import TEST_PROJECT_REPO

if TEST_PROJECT_REPO is None:
    print(
        "You must define the environment TEST_PROJECT_REPO with the path to the dsgrid-project-EFS repository"
    )
    sys.exit(1)

print(f"\n>>> TEST_PROJECT_REPO:{TEST_PROJECT_REPO}\n")

dir_path = os.path.dirname(os.path.abspath(__file__))

PROJECT_CONFIG_FILE = os.path.join(TEST_PROJECT_REPO, "dsgrid_project", "project.toml")
DIMENSION_CONFIG_FILE = os.path.join(TEST_PROJECT_REPO, "dsgrid_project", "dimensions.toml")
DIMENSION_CONFIG_FILE_TIME = os.path.join(dir_path, "dimension_test_time.toml")
DIMENSION_MAPPINGS_CONFIG_FILE = os.path.join(
    TEST_PROJECT_REPO, "dsgrid_project", "dimension_mappings.toml"
)
