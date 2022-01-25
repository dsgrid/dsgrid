import copy
import getpass
import os
import shutil
import sys
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from tempfile import gettempdir

import pandas as pd
import pytest

from dsgrid.exceptions import (
    DSGInvalidDataset,
    DSGInvalidDimension,
)
from dsgrid.tests.common import make_test_project_dir, make_test_data_dir, TEST_DATASET_DIRECTORY
from dsgrid.utils.files import dump_line_delimited_json, load_line_delimited_json
from dsgrid.tests.make_us_data_registry import make_test_data_registry, replace_dataset_path


def test_aeo_datasets(make_test_data_dir):
    if "test_aeo_data" not in os.listdir(make_test_data_dir):
        print("test_invalid_datasets requires the dsgrid-test-data repository")
        sys.exit(1)

    datasets = os.listdir(make_test_data_dir / "test_aeo_data")
    for dataset in datasets:
        print(f">> {dataset}...")
        dataset_dir = make_test_data_dir / "test_aeo_data" / dataset
        assert dataset_dir.exists()
        dataset_config_file = dataset_dir / "dataset.toml"
        assert dataset_config_file.exists()

        with TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            print("---->")
            print(base_dir)
            print()
            manager = make_test_data_registry(
                base_dir,
                dataset_config_file,  # which is this?
                dataset_path=dataset_dir,
                include_datasets=False,
            )

            user = getpass.getuser()
            log_message = "test log message"

            try:
                # Create a new directory because there are collisions with cached
                # Spark load_data_lookup dataframes.
                test_dir = base_dir / f"test_data_dir"
                replace_dataset_path(dataset_config_file, dataset_path=dataset_dir)
                shutil.copytree(make_test_data_dir, test_dir)
                exc, match_msg = setup_test(test_dir)
                manager.dataset_manager.register(dataset_config_file, user, log_message)
            finally:
                shutil.rmtree(test_dir)
