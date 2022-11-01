import getpass
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import pyspark
import pytest
from semver import VersionInfo

from dsgrid.config.dimension_association_manager import _make_dimension_associations_table_name
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import (
    DSGDuplicateValueRegistered,
    DSGInvalidDataset,
    DSGInvalidDimension,
    DSGInvalidDimensionMapping,
    DSGValueNotRegistered,
)
from dsgrid.registry.common import DatasetRegistryStatus, ProjectRegistryStatus, VersionUpdateType
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import (
    check_configs_update,
    create_local_test_registry,
    replace_dimension_uuids_from_registry,
    replace_dimension_mapping_uuids_from_registry,
    TEST_DATASET_DIRECTORY,
)
from dsgrid.utils.files import dump_data, load_data
from dsgrid.utils.spark import is_table_stored
from dsgrid.tests.make_us_data_registry import make_test_data_registry


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
        log_message = "initial registration"
        dataset_path = TEST_DATASET_DIRECTORY / dataset_id

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
                make_test_project_dir / "datasets" / "modeled" / "comstock" / "dataset.toml"
            )
            dataset_mgr.register(dataset_config_file, dataset_path, user, log_message)

        # Duplicate mappings get re-used.
        mapping_ids = dimension_mapping_mgr.list_ids()
        dimension_mapping_mgr.dump(dimension_mapping_id, base_dir)
        dimension_mapping_config = base_dir / "dimension_mapping.toml"
        data = load_data(dimension_mapping_config)
        dump_data({"mappings": [data]}, dimension_mapping_config)
        dimension_mapping_mgr.register(dimension_mapping_config, user, log_message)
        assert len(dimension_mapping_mgr.list_ids()) == len(mapping_ids)

        table_name = _make_dimension_associations_table_name(
            project_id, dataset_id, DimensionType.METRIC
        )
        assert is_table_stored(table_name)
        check_configs_update(base_dir, manager)
        assert not is_table_stored(table_name)
        check_update_project_dimension(base_dir, manager, dataset_id)
        # Note that the dataset is now unregistered.

        # Test removals.
        check_config_remove(project_mgr, project_id)
        check_config_remove(dataset_mgr, dataset_id)
        check_config_remove(dimension_mgr, dimension_id)
        check_config_remove(dimension_mapping_mgr, dimension_mapping_id)


def test_duplicate_dimensions(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        path = create_local_test_registry(Path(tmpdir))
        user = getpass.getuser()
        log_message = "Initial registration"
        manager = RegistryManager.load(path, offline_mode=True)

        dimension_mgr = manager.dimension_manager
        dimension_mgr.register(make_test_project_dir / "dimensions.toml", user, log_message)

        # Registering duplicate dimensions and mappings are allowed.
        # If names are the same, they are replaced. Otherwise, new ones get registered.
        # Time dimension has more fields checked.
        dimension_ids = dimension_mgr.list_ids()
        dim_config_file = make_test_project_dir / "dimensions.toml"

        dimension_mgr.register(dim_config_file, user, log_message)
        assert len(dimension_mgr.list_ids()) == len(dimension_ids)

        data = load_data(dim_config_file)
        data["dimensions"][0]["name"] += " new"
        dump_data(data, dim_config_file)

        dimension_mgr.register(dim_config_file, user, log_message)
        assert len(dimension_mgr.list_ids()) == len(dimension_ids) + 1

        data = load_data(dim_config_file)
        for dim in data["dimensions"]:
            if dim["type"] == "time":
                assert dim["time_interval_type"] == "period_beginning"
                dim["time_interval_type"] = "period_ending"
        dump_data(data, dim_config_file)

        dimension_mgr.register(dim_config_file, user, log_message)
        assert len(dimension_mgr.list_ids()) == len(dimension_ids) + 2


def test_duplicate_project_dimension_display_names(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        path = create_local_test_registry(Path(tmpdir))
        user = getpass.getuser()
        log_message = "Initial registration"
        manager = RegistryManager.load(path, offline_mode=True)

        project_file = make_test_project_dir / "project.toml"
        data = load_data(project_file)
        for dim in data["dimensions"]["supplemental_dimensions"]:
            if dim["display_name"] == "State":
                dim["display_name"] = "County"
        dump_data(data, project_file)
        with pytest.raises(DSGInvalidDimension):
            manager.project_manager.register(project_file, user, log_message)


def test_register_duplicate_project_rollback_dimensions(make_test_project_dir):
    src_dir = make_test_project_dir
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        manager = make_test_data_registry(
            base_dir, src_dir, include_projects=False, include_datasets=False
        )
        project_file = src_dir / "project.toml"
        orig_dimension_ids = manager.dimension_manager.list_ids()

        data = load_data(project_file)
        # Inject an invalid project ID so that we can test rollback of dimensions.
        data["project_id"] = "project-with-dashes"
        dump_data(data, project_file)

        with pytest.raises(ValueError):
            manager.project_manager.register(
                project_file,
                getpass.getuser(),
                "register duplicate project",
            )

        # Dimensions and mappings should have been registered and then cleared.
        assert manager.dimension_manager.list_ids() == orig_dimension_ids
        assert not manager.dimension_mapping_manager.list_ids()


def test_register_and_submit_rollback_on_failure(make_test_project_dir):
    src_dir = make_test_project_dir
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        path = create_local_test_registry(base_dir)
        manager = RegistryManager.load(path, offline_mode=True)
        project_file = src_dir / "project.toml"
        project_id = load_data(project_file)["project_id"]
        dataset_dir = src_dir / "datasets" / "modeled" / "comstock"
        dataset_config_file = dataset_dir / "dataset.toml"
        dataset_id = load_data(dataset_config_file)["dataset_id"]
        dataset_mapping_file = dataset_dir / "dimension_mappings.toml"
        dataset_path = (
            Path(os.environ.get("DSGRID_LOCAL_DATA_DIRECTORY", TEST_DATASET_DIRECTORY))
            / "test_efs_comstock"
        )
        subsectors_file = (
            dataset_dir
            / "dimension_mappings"
            / "lookup_comstock_subsectors_to_project_subsectors.csv"
        )
        # Remove some records.
        data = subsectors_file.read_text().splitlines()[:-2]
        subsectors_file.write_text("\n".join(data))

        manager.project_manager.register(
            project_file,
            getpass.getuser(),
            "register project",
        )

        replace_dimension_uuids_from_registry(path, (dataset_config_file,))
        orig_dimension_ids = manager.dimension_manager.list_ids()
        orig_mapping_ids = manager.dimension_mapping_manager.list_ids()

        try:
            with pytest.raises(DSGInvalidDataset):
                manager.project_manager.register_and_submit_dataset(
                    dataset_config_file,
                    dataset_path,
                    project_id,
                    getpass.getuser(),
                    "register dataset and submit",
                    dimension_mapping_file=dataset_mapping_file,
                )
        finally:
            missing_record_file = Path(
                f"{dataset_id}__{project_id}__missing_dimension_record_combinations.csv"
            )
            if missing_record_file.exists():
                shutil.rmtree(missing_record_file)

        assert manager.dimension_manager.list_ids() == orig_dimension_ids
        assert manager.dimension_mapping_manager.list_ids() == orig_mapping_ids
        assert not manager.dataset_manager.list_ids()
        assert manager.project_manager.list_ids() == [project_id]


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
        for mapping_ref in project.model.dimension_mappings.base_to_supplemental_references:
            if mapping_ref.mapping_id == mapping.config_id:
                assert mapping_ref.version == new_version
                found = True
        assert found

        dataset = dataset_mgr.get_by_id(dataset_id)
        found = False
        for dimension_ref in dataset.model.dimension_references:
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
        dimension_mapping_file = make_test_project_dir / "dimension_mappings_with_ids.toml"
        replace_dimension_uuids_from_registry(path, [dimension_mapping_file])

        record_file = (
            make_test_project_dir
            / "dimension_mappings"
            / "base_to_supplemental"
            / "lookup_county_to_state.csv"
        )
        orig_text = record_file.read_text()

        # Invalid 'from' record
        record_file.write_text(orig_text + "invalid county,1,CO\n")
        with pytest.raises(DSGInvalidDimensionMapping):
            dim_mapping_mgr.register(dimension_mapping_file, user, log_message)

        # Invalid 'from' record - nulls aren't allowd
        record_file.write_text(orig_text + ",1.2,CO\n")
        with pytest.raises(DSGInvalidDimensionMapping):
            dim_mapping_mgr.register(dimension_mapping_file, user, log_message)

        # Invalid 'to' record
        record_file.write_text(orig_text.replace("CO", "Colorado"))
        with pytest.raises(DSGInvalidDimensionMapping):
            dim_mapping_mgr.register(dimension_mapping_file, user, log_message)

        # Duplicate "from" record, invalid as mapping_type = one_to_one_multiplication
        orig_text2 = orig_text.split(",")
        orig_text2 = ",".join(orig_text2[::2])
        record_file.write_text(orig_text2 + "\n08031,CO\n")
        msg = r"dimension_mapping.*has mapping_type.*, which does not allow duplicated.*records"
        with pytest.raises(DSGInvalidDimensionMapping, match=msg):
            dim_mapping_mgr.register(dimension_mapping_file, user, log_message)

        # Valid - null value in "to" record (Only one valid test allowed in this test func)
        record_file.write_text(orig_text.replace("CO", ""))
        dim_mapping_mgr.register(dimension_mapping_file, user, log_message)


def test_register_submit_dataset_long_workflow(make_test_project_dir):
    src_dir = make_test_project_dir
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        manager = make_test_data_registry(
            base_dir, src_dir, include_projects=False, include_datasets=False
        )
        dim_mapping_mgr = manager.dimension_mapping_manager
        project_config_file = src_dir / "project_with_dimension_ids.toml"
        project_id = load_data(project_config_file)["project_id"]
        project_dimension_mapping_config = src_dir / "dimension_mappings_with_ids.toml"
        project_dimension_file = src_dir / "dimensions.toml"
        dataset_dir = src_dir / "datasets" / "modeled" / "comstock"
        dataset_config_file = dataset_dir / "dataset_with_dimension_ids.toml"
        dataset_id = load_data(dataset_config_file)["dataset_id"]
        dataset_dimension_file = dataset_dir / "dimensions.toml"
        dimension_mapping_config = dataset_dir / "dimension_mapping_config_with_ids.toml"
        dimension_mapping_refs = dataset_dir / "dimension_mapping_references.toml"
        user = getpass.getuser()
        log_message = "register"

        manager.dimension_manager.register(project_dimension_file, user, log_message)
        manager.dimension_manager.register(dataset_dimension_file, user, log_message)

        needs_replacements = [project_dimension_mapping_config, dimension_mapping_config]
        replace_dimension_uuids_from_registry(base_dir, needs_replacements)

        dim_mapping_mgr.register(project_dimension_mapping_config, user, log_message)
        dim_mapping_mgr.register(dimension_mapping_config, user, log_message)

        needs_replacements = [project_config_file, dimension_mapping_refs]
        replace_dimension_mapping_uuids_from_registry(base_dir, needs_replacements)
        replace_dimension_uuids_from_registry(base_dir, (project_config_file, dataset_config_file))

        manager.project_manager.register(project_config_file, user, "register project")
        dataset_path = TEST_DATASET_DIRECTORY / dataset_id
        manager.dataset_manager.register(
            dataset_config_file, dataset_path, user, "register dataset"
        )
        manager.project_manager.submit_dataset(
            project_id,
            dataset_id,
            user,
            log_message,
            dimension_mapping_references_file=dimension_mapping_refs,
        )

        assert manager.dimension_manager.list_ids()
        assert manager.dimension_mapping_manager.list_ids()
        assert manager.project_manager.list_ids() == [project_id]
        assert manager.dataset_manager.list_ids() == [dataset_id]


def register_project(project_mgr, config_file, project_id, user, log_message):
    project_mgr.register(config_file, user, log_message)
    assert project_mgr.list_ids() == [project_id]


def register_dataset(dataset_mgr, config_file, dataset_id, user, log_message):
    dataset_mgr.register(config_file, user, log_message)
    assert dataset_mgr.list_ids() == [dataset_id]


def check_update_project_dimension(tmpdir, manager, dataset_id):
    """Verify that updating a project's dimension causes all datasets to go unregistered."""
    project_mgr = manager.project_manager
    project_id = project_mgr.list_ids()[0]
    dimension_mgr = manager.dimension_manager
    dimension_id = dimension_mgr.list_ids()[0]
    user = getpass.getuser()

    dim_dir = Path(tmpdir) / "new_dimension"
    dim_config_file = dim_dir / dimension_mgr.registry_class().config_filename()
    dimension_mgr.dump(dimension_id, dim_dir, force=True)
    dim_data = load_data(dim_config_file)
    dim_data["description"] += "; updated description"
    dump_data(dim_data, dim_config_file)
    dimension_mgr.update_from_file(
        dim_config_file,
        dimension_id,
        user,
        VersionUpdateType.PATCH,
        "update to description",
        dimension_mgr.get_current_version(dimension_id),
    )
    project_dir = Path(tmpdir) / "new_project"
    project_config_file = project_dir / project_mgr.registry_class().config_filename()
    project_mgr.dump(project_id, project_dir, force=True)
    project_data = load_data(project_config_file)
    new_version = dimension_mgr.get_current_version(dimension_id)
    for dim in project_data["dimensions"]["base_dimension_references"]:
        if dim["dimension_id"] == dimension_id:
            dim["version"] = str(new_version)
            break
    dump_data(project_data, project_config_file)
    project_mgr.update_from_file(
        project_config_file,
        project_id,
        user,
        VersionUpdateType.PATCH,
        "update dimension",
        project_mgr.get_current_version(project_id),
    )
    _check_dataset_statuses(project_mgr, project_id, DatasetRegistryStatus.UNREGISTERED)
    assert project_mgr.get_by_id(project_id).model.status == ProjectRegistryStatus.IN_PROGRESS


def _check_dataset_statuses(project_mgr, project_id, expected_status):
    config = project_mgr.get_by_id(project_id)
    assert config.model.datasets
    for dataset in config.model.datasets:
        assert dataset.status == expected_status


def check_config_remove(mgr, config_id):
    """Runs removal tests for the config."""
    mgr.remove(config_id)
    with pytest.raises(DSGValueNotRegistered):
        mgr.get_by_id(config_id)
