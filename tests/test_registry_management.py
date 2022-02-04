import getpass
import os
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir

import pyspark
import pytest
from semver import VersionInfo

from dsgrid.exceptions import (
    DSGDuplicateValueRegistered,
    DSGInvalidDataset,
    DSGInvalidDimension,
    DSGInvalidDimensionMapping,
    DSGInvalidParameter,
    DSGInvalidOperation,
    DSGValueNotRegistered,
)
from dsgrid.registry.common import DatasetRegistryStatus, ProjectRegistryStatus, VersionUpdateType
from dsgrid.registry.dataset_registry_manager import DatasetRegistryManager
from dsgrid.registry.project_registry_manager import ProjectRegistryManager
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import (
    create_local_test_registry,
    make_test_project_dir,
    TEST_DATASET_DIRECTORY,
)
from dsgrid.utils.files import dump_data, load_data
from dsgrid.tests.common import (
    replace_dimension_mapping_uuids_from_registry,
    replace_dimension_uuids_from_registry,
)
from dsgrid.tests.make_us_data_registry import make_test_data_registry, replace_dataset_path


def test_register_project_and_dataset(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        manager = make_test_data_registry(base_dir, make_test_project_dir, TEST_DATASET_DIRECTORY)
        project_mgr = manager.project_manager
        dataset_mgr = manager.dataset_manager
        dimension_mgr = manager.dimension_manager
        dimension_mapping_mgr = manager.dimension_mapping_manager
        project_ids = project_mgr.list_ids()
        assert len(project_ids) == 1
        project_id = project_ids[0]
        dataset_ids = dataset_mgr.list_ids()
        dataset_id = dataset_ids[0]
        assert len(dataset_ids) == 1
        dimension_ids = dimension_mgr.list_ids()
        assert dimension_ids
        dimension_id = dimension_ids[0]
        dimension_mapping_ids = dimension_mapping_mgr.list_ids()
        assert dimension_mapping_ids
        dimension_mapping_id = dimension_mapping_ids[0]
        user = getpass.getuser()
        log_message = "intial registration"

        project_config = project_mgr.get_by_id(project_id, VersionInfo.parse("1.1.0"))
        assert project_config.model.status == ProjectRegistryStatus.COMPLETE
        dataset = project_config.get_dataset(dataset_id)
        assert dataset.status == DatasetRegistryStatus.REGISTERED

        # The project version from before dataset submission should still be there.
        project_config = project_mgr.get_by_id(project_id, VersionInfo.parse("1.0.0"))
        dataset = project_config.get_dataset(dataset_id)
        assert dataset.status == DatasetRegistryStatus.UNREGISTERED

        with pytest.raises(DSGDuplicateValueRegistered):
            project_config_file = make_test_project_dir / "project.toml"
            project_mgr.register(project_config_file, user, log_message)

        with pytest.raises(DSGDuplicateValueRegistered):
            dataset_config_file = (
                make_test_project_dir / "datasets/sector_models/comstock/dataset.toml"
            )
            dataset_mgr.register(dataset_config_file, user, log_message)

        with pytest.raises(DSGDuplicateValueRegistered):
            dim_config_file = make_test_project_dir / "dimensions.toml"
            dimension_mgr.register(dim_config_file, user, log_message)

        with pytest.raises(DSGDuplicateValueRegistered):
            # Time dimension doesn't have records and duplicates are only based on fields.
            dimension_models = load_data(make_test_project_dir / "dimensions.toml")["dimensions"]
            time_models = [x for x in dimension_models if x["type"] == "time"]
            assert len(time_models) == 1
            new_models = {"dimensions": time_models}
            new_file = make_test_project_dir / "time_dimension.toml"
            dump_data(new_models, new_file)
            dimension_mgr.register(new_file, user, log_message)

        with pytest.raises(DSGDuplicateValueRegistered):
            dset_dir = Path("datasets/sector_models/comstock")
            dimension_mapping_config = make_test_project_dir / dset_dir / "dimension_mappings.toml"
            dimension_mapping_mgr.register(dimension_mapping_config, user, log_message)

        # Test updates to all configs.
        check_config_update(base_dir, project_mgr, project_id, user, VersionInfo.parse("1.1.0"))
        check_config_update(base_dir, dataset_mgr, dataset_id, user, VersionInfo.parse("1.0.0"))
        dim_dir = base_dir / "dimensions"
        os.makedirs(dim_dir)
        check_config_update(dim_dir, dimension_mgr, dimension_id, user, VersionInfo.parse("1.0.0"))
        check_config_update(
            dim_dir, dimension_mapping_mgr, dimension_mapping_id, user, VersionInfo.parse("1.0.0")
        )

        # Test removals.
        check_config_remove(project_mgr, project_id)
        check_config_remove(dataset_mgr, dataset_id)
        check_config_remove(dimension_mgr, dimension_id)
        check_config_remove(dimension_mapping_mgr, dimension_mapping_id)


def test_auto_updates(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        mgr = make_test_data_registry(base_dir, make_test_project_dir, TEST_DATASET_DIRECTORY)
        project_mgr = mgr.project_manager
        dataset_mgr = mgr.dataset_manager
        dimension_mgr = mgr.dimension_manager
        dimension_mapping_mgr = mgr.dimension_mapping_manager
        project_id = project_mgr.list_ids()[0]
        dataset_id = dataset_mgr.list_ids()[0]
        dimension_id = [x for x in dimension_mgr.iter_ids() if x.startswith("us_counties")][0]
        dimension = dimension_mgr.get_by_id(dimension_id)

        # Test that we can convert records to a Spark DataFrame. Unrelated to the rest.
        assert isinstance(dimension.get_records_dataframe(), pyspark.sql.DataFrame)

        dimension.model.description += "; test update"
        update_type = VersionUpdateType.MINOR
        log_message = "test update"
        dimension_mgr.update(dimension, update_type, log_message)

        # Find a mapping that uses this dimension and verify that it gets updated.
        mapping = None
        for _mapping in dimension_mapping_mgr.iter_configs():
            fields = _mapping.config_id.split("__")
            if fields[0].startswith("us_counties") and fields[1].startswith("us_census_regions"):
                mapping = _mapping
                break
        assert mapping is not None
        orig_version = dimension_mapping_mgr.get_current_version(mapping.config_id)
        assert orig_version == VersionInfo.parse("1.0.0")

        mgr.update_dependent_configs(dimension, update_type, log_message)

        new_version = dimension_mapping_mgr.get_current_version(mapping.config_id)
        assert new_version == VersionInfo.parse("1.1.0")

        project = project_mgr.get_by_id(project_id)
        found = False
        for mapping_ref in project.model.dimension_mappings.base_to_supplemental:
            if mapping_ref.mapping_id == mapping.config_id:
                assert mapping_ref.version == new_version
                found = True
        assert found

        dataset = dataset_mgr.get_by_id(dataset_id)
        found = False
        for dimension_ref in dataset.model.dimensions:
            if dimension_ref.dimension_id == dimension.config_id:
                assert dimension_ref.version == VersionInfo.parse("1.1.0")
                found = True
        assert found

        assert project.model.datasets[0].version == VersionInfo.parse("1.1.0")
        assert project_mgr.get_current_version(project_id) == VersionInfo.parse("1.2.0")
        assert dataset_mgr.get_current_version(dataset_id) == VersionInfo.parse("1.1.0")

        # The project should get updated again if we update the dimension mapping.
        mapping.model.description += "test update"
        dimension_mapping_mgr.update(mapping, VersionUpdateType.PATCH, "test update")
        assert dimension_mapping_mgr.get_current_version(mapping.config_id) == VersionInfo.parse(
            "1.1.1"
        )
        mgr.update_dependent_configs(mapping, VersionUpdateType.PATCH, "test update")
        assert project_mgr.get_current_version(project_id) == VersionInfo.parse("1.2.1")

        # And again if we update the dataset.
        dataset.model.description += "test update"
        dataset_mgr.update(dataset, VersionUpdateType.PATCH, "test update")
        mgr.update_dependent_configs(dataset, VersionUpdateType.PATCH, "test update")
        assert project_mgr.get_current_version(project_id) == VersionInfo.parse("1.2.2")


def test_invalid_dimension_mapping(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        path = create_local_test_registry(Path(tmpdir))
        user = getpass.getuser()
        log_message = "Initial registration"
        manager = RegistryManager.load(path, offline_mode=True)

        dim_mgr = manager.dimension_manager
        dim_mgr.register(make_test_project_dir / "dimensions.toml", user, log_message)
        dim_mapping_mgr = manager.dimension_mapping_manager
        dimension_mapping_file = make_test_project_dir / "dimension_mappings.toml"
        replace_dimension_uuids_from_registry(path, [dimension_mapping_file])

        record_file = (
            make_test_project_dir
            / "dimension_mappings"
            / "base-to-supplemental"
            / "lookup_county_to_state.csv"
        )
        orig_text = record_file.read_text()

        # Invalid 'from' record
        record_file.write_text(orig_text + "invalid county,CO\n")
        with pytest.raises(DSGInvalidDimensionMapping):
            dim_mapping_mgr.register(dimension_mapping_file, user, log_message)

        # Invalid 'to' record
        record_file.write_text(orig_text.replace("CO", "Colorado"))
        with pytest.raises(DSGInvalidDimensionMapping):
            dim_mapping_mgr.register(dimension_mapping_file, user, log_message)

        # Duplicate 'from' record
        record_file.write_text(orig_text + "\n08031,CO")
        with pytest.raises(DSGInvalidDimensionMapping):
            dim_mapping_mgr.register(dimension_mapping_file, user, log_message)

        # Valid
        record_file.write_text(orig_text.replace("CO", ""))
        dim_mapping_mgr.register(dimension_mapping_file, user, log_message)


def register_project(project_mgr, config_file, project_id, user, log_message):
    project_mgr.register(config_file, user, log_message)
    assert project_mgr.list_ids() == [project_id]


def register_dataset(dataset_mgr, config_file, dataset_id, user, log_message):
    dataset_mgr.register(config_file, user, log_message)
    assert dataset_mgr.list_ids() == [dataset_id]


def submit_dataset(
    project_mgr, project_config, dataset_config, dimension_mapping_refs, user, log_message
):
    project_id = project_config.config_id
    dataset_id = dataset_config.config_id
    project_mgr.submit_dataset(
        project_id,
        dataset_id,
        dimension_mapping_refs,
        user,
        log_message,
    )


def check_config_update(tmpdir, mgr, config_id, user, version):
    """Runs basic positive and negative update tests for the config. Also tests dump."""
    config_file = Path(tmpdir) / mgr.registry_class().config_filename()
    assert not config_file.exists()
    try:
        mgr.dump(config_id, tmpdir)
        with pytest.raises(DSGInvalidOperation):
            mgr.dump(config_id, tmpdir)
        mgr.dump(config_id, tmpdir, force=True)
        assert config_file.exists()
        config_data = load_data(config_file)
        config_data["description"] += "; updated description"
        dump_data(config_data, config_file)
        with pytest.raises(DSGInvalidParameter):
            mgr.update_from_file(
                config_file,
                "invalid_config_id",
                user,
                VersionUpdateType.PATCH,
                "update to description",
                version,
            )
        with pytest.raises(DSGInvalidParameter):
            mgr.update_from_file(
                config_file,
                config_id,
                user,
                VersionUpdateType.PATCH,
                "update to description",
                version.bump_patch(),
            )

        mgr.update_from_file(
            config_file,
            config_id,
            user,
            VersionUpdateType.PATCH,
            "update to description",
            version,
        )
        assert mgr.get_current_version(config_id) == version.bump_patch()
    finally:
        if config_file.exists():
            os.remove(config_file)


def check_config_remove(mgr, config_id):
    """Runs removal tests for the config."""
    mgr.remove(config_id)
    with pytest.raises(DSGValueNotRegistered):
        mgr.get_by_id(config_id)
