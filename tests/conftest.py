import os
import re
import shutil
import sys

import pytest

from dsgrid.utils.run_command import run_command, check_run_command
from dsgrid.utils.spark import init_spark
from dsgrid.tests.common import (
    TEST_PROJECT_PATH,
    TEST_PROJECT_REPO,
    TEST_DATASET_DIRECTORY,
    TEST_REGISTRY,
)


def pytest_sessionstart(session):
    if not os.listdir(TEST_PROJECT_PATH):
        print(
            "The dsgrid-test-data submodule has not been initialized. "
            "Please run these commands:"
        )
        print("git submodule init")
        print("git submodule update")

    commit_file = TEST_REGISTRY / "commit.txt"
    latest_commit = _get_latest_commit()
    if (
        TEST_REGISTRY.exists()
        and commit_file.exists()
        and commit_file.read_text().strip() == latest_commit
    ):
        print(f"Use existing test registry at {TEST_REGISTRY}.")
    else:
        if TEST_REGISTRY.exists():
            shutil.rmtree(TEST_REGISTRY)
        ret = run_command(
            f"python dsgrid/tests/make_us_data_registry.py {TEST_REGISTRY} -p {TEST_PROJECT_REPO} "
            f"-d {TEST_DATASET_DIRECTORY}"
        )
        if ret != 0:
            # Delete it because it is invalid.
            shutil.rmtree(TEST_REGISTRY)
            sys.exit(1)
        commit_file.write_text(latest_commit + "\n")


def _get_latest_commit():
    output = {}
    check_run_command("git log -n 1", output=output)
    match = re.search(r"^commit (\w+)", output["stdout"])
    assert match, output
    commit = match.group(1)
    return commit


@pytest.fixture
def spark_session():
    spark = init_spark("dsgrid_test")
    yield spark
    spark.stop()
