import os

from dsgrid.utils.run_command import check_run_command
from dsgrid.utils.spark import init_spark
from dsgrid.tests.common import (
    TEST_PROJECT_PATH,
    TEST_PROJECT_REPO,
    TEST_DATASET_DIRECTORY,
    TEST_REGISTRY,
)


def pytest_sessionstart(session):
    if not os.listdir(TEST_PROJECT_PATH):
        print("The {dsgrid-test-data} submodule has not been initialized. "
              "Please run these commands:")
        print("git submodule init")
        print("git submodule update")

    if TEST_REGISTRY.exists():
        print(f"Use existing test registry at {TEST_REGISTRY}.")
    else:
        check_run_command(
            f"python tests/make_us_data_registry.py {TEST_REGISTRY} -p {TEST_PROJECT_REPO} "
            f"-d {TEST_DATASET_DIRECTORY}"
        )

    init_spark("dsgrid-test")
