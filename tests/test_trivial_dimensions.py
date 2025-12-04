import pytest

from dsgrid.exceptions import DSGInvalidDimension
from dsgrid.registry.registry_database import RegistryDatabase, DatabaseConnection
from dsgrid.utils.files import load_data, dump_data
from dsgrid.utils.id_remappings import (
    map_dimension_names_to_ids,
    replace_dimension_names_with_current_ids,
)
from dsgrid.tests.make_us_data_registry import make_test_data_registry


def test_trivial_dimension_bad(make_test_project_dir, make_test_data_dir, tmp_path):
    """Test bad trivial dimensions where county geography is set to trivial"""
    conn = DatabaseConnection(url=f"sqlite:///{tmp_path}/tmp-dsgrid-reg.db")
    with make_test_data_registry(
        tmp_path,
        make_test_project_dir,
        include_projects=False,
        include_datasets=False,
        database_url=conn.url,
    ) as manager:
        try:
            project_config_file = make_test_project_dir / "project.json5"
            manager.project_manager.register(project_config_file, "user", "log")

            dataset_dir = make_test_project_dir / "datasets" / "modeled" / "comstock"
            assert dataset_dir.exists()
            dataset_config_file = dataset_dir / "dataset.json5"
            mappings = map_dimension_names_to_ids(manager.dimension_manager)
            replace_dimension_names_with_current_ids(dataset_config_file, mappings)

            config = load_data(dataset_config_file)
            config["trivial_dimensions"] = config["trivial_dimensions"] + ["geography"]
            dump_data(config, dataset_config_file)
            with pytest.raises(DSGInvalidDimension):
                manager.dataset_manager.register(
                    config_file=dataset_config_file,
                    submitter="test",
                    log_message="test",
                )
        finally:
            RegistryDatabase.delete(conn)
