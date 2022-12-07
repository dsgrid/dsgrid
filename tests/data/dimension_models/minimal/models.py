import os

from dsgrid.tests.common import TEST_PROJECT_REPO

dir_path = os.path.dirname(os.path.abspath(__file__))

PROJECT_CONFIG_FILE = os.path.join(
    TEST_PROJECT_REPO, "dsgrid_project", "project_with_dimension_ids.json5"
)
DIMENSION_CONFIG_FILE = os.path.join(TEST_PROJECT_REPO, "dsgrid_project", "dimensions.json5")
DIMENSION_CONFIG_FILE_TIME = os.path.join(dir_path, "dimension_test_time.json5")
DIMENSION_MAPPINGS_CONFIG_FILE = os.path.join(
    TEST_PROJECT_REPO, "dsgrid_project", "dimension_mappings.json5"
)
