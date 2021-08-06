import os
from pathlib import Path

from dsgrid.utils.run_command import check_run_command
from dsgrid.utils.spark import init_spark
from dsgrid.tests.common import (
    TEST_PROJECT_PATH,
    TEST_PROJECT_REPO,
    TEST_DATASET_DIRECTORY,
    TEST_REGISTRY,
)


TEST_DATA_BRANCH = "main"


def pytest_sessionstart(session):
    if TEST_PROJECT_PATH.exists():
        set_current_branch(TEST_PROJECT_PATH, TEST_DATA_BRANCH)
    else:
        check_run_command(
            f"git clone https://github.com/dsgrid/dsgrid-test-data.git {TEST_PROJECT_PATH}"
        )
        set_current_branch(TEST_PROJECT_PATH, TEST_DATA_BRANCH)

    if not TEST_REGISTRY.exists():
        check_run_command(
            f"python tests/make_us_data_registry.py {TEST_REGISTRY} -p {TEST_PROJECT_REPO} -d {TEST_DATASET_DIRECTORY}"
        )

    init_spark("dsgrid-test")


def set_current_branch(repo_path, branch):
    orig = os.getcwd()
    os.chdir(repo_path)
    try:
        cur_branch = get_current_branch()
        if cur_branch != branch:
            check_run_command(f"git fetch origin {branch}")
            check_run_command(f"git checkout {branch}")
    finally:
        os.chdir(orig)


def get_current_branch():
    output = {}
    check_run_command("git rev-parse --abbrev-ref HEAD", output=output)
    return output["stdout"].strip()
