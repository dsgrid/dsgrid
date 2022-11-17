import pytest

from dsgrid.exceptions import DSGInvalidDimension
from dsgrid.utils.files import load_data, dump_data
from dsgrid.tests.common import (
    replace_dimension_uuids_from_registry,
)
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
            include_projects=False,
            include_datasets=False,
        )
        project_config_file = make_test_project_dir / "project.json5"
        manager.project_manager.register(project_config_file, "user", "log")

        dataset_dir = make_test_project_dir / "datasets" / "modeled" / "comstock"
        assert dataset_dir.exists()
        dataset_config_file = dataset_dir / "dataset.json5"
        replace_dimension_uuids_from_registry(base_dir, (dataset_config_file,))

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
            )
