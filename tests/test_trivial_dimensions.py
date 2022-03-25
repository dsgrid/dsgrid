import os
from pyspark.sql.functions import log
import pytest

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.exceptions import DSGInvalidDimension
from dsgrid.tests.common import create_local_test_registry, make_test_project_dir
from dsgrid.utils.files import load_data, dump_data
from dsgrid.tests.common import make_test_project_dir, make_test_data_dir, TEST_DATASET_DIRECTORY
from dsgrid.tests.make_us_data_registry import make_test_data_registry

from pathlib import Path
from tempfile import TemporaryDirectory


def test_trivial_dimension_bad(make_test_project_dir, make_test_data_dir):
    """Test bad trivial dimensions where county geography is set to trivial"""
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        manager = make_test_data_registry(
            base_dir,
            make_test_project_dir,
            dataset_path=make_test_data_dir,
            include_datasets=False,
        )
        dataset_dir = make_test_project_dir / "datasets" / "sector_models" / "comstock"
        assert dataset_dir.exists()
        dataset_config_file = dataset_dir / "dataset.toml"

        config = load_data(dataset_config_file)
        config["trivial_dimensions"] = config["trivial_dimensions"] + ["geography"]
        dump_data(config, dataset_config_file)
        dataset_path = make_test_data_dir / config["dataset_id"]

        with pytest.raises(DSGInvalidDimension):
            manager.dataset_manager.register(
                config_file=dataset_config_file,
                dataset_path=dataset_path,
                submitter="test",
                log_message="test",
                dimension_file=dataset_dir / "dimensions.toml",
            )
