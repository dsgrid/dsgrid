import copy
import getpass
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from dsgrid.common import BackendEngine
from dsgrid.cli.dsgrid import cli
from dsgrid.config.input_dataset_requirements import (
    InputDatasetDimensionRequirementsModel,
    InputDatasetDimensionRequirementsListModel,
    InputDatasetListModel,
)
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.exceptions import (
    DSGDuplicateValueRegistered,
    DSGInvalidDataset,
    DSGInvalidDimensionMapping,
    DSGValueNotRegistered,
)
from dsgrid.registry.common import (
    DatasetRegistryStatus,
    ProjectRegistryStatus,
    RegistryTables,
    VersionUpdateType,
    RegistryType,
)
from dsgrid.registry.registry_auto_updater import RegistryAutoUpdater
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.spark.types import DataFrame
from dsgrid.tests.common import (
    check_configs_update,
    create_local_test_registry,
    TEST_DATASET_DIRECTORY,
)
from dsgrid.utils.files import dump_data, load_data
from dsgrid.utils.id_remappings import (
    map_dimension_names_to_ids,
    map_dimension_ids_to_names,
    map_dimension_mapping_names_to_ids,
    replace_dimension_names_with_current_ids,
    replace_dimension_mapping_names_with_current_ids,
)
from dsgrid.tests.make_us_data_registry import make_test_data_registry


def test_register_project_and_dataset(mutable_cached_registry, tmp_path):
    manager, test_project_dir = mutable_cached_registry
    project_mgr = manager.project_manager
    dataset_mgr = manager.dataset_manager
    dimension_mgr = manager.dimension_manager
    dimension_mapping_mgr = manager.dimension_mapping_manager
    project_ids = project_mgr.list_ids()
    assert len(project_ids) == 1
    project_id = project_ids[0]
    dataset_ids = dataset_mgr.list_ids()
    dataset_id = dataset_ids[0]
    assert len(dataset_ids) == 2
    dimension_ids = dimension_mgr.list_ids()
    assert dimension_ids
    dimension_id = dimension_ids[0]
    dimension_mapping_ids = dimension_mapping_mgr.list_ids()
    assert dimension_mapping_ids
    dimension_mapping_id = dimension_mapping_ids[0]
    user = getpass.getuser()
    log_message = "initial registration"

    project_config = project_mgr.get_by_id(project_id, "1.2.0")
    assert project_config.model.status == ProjectRegistryStatus.COMPLETE
    dataset = project_config.get_dataset(dataset_id)
    assert dataset.status == DatasetRegistryStatus.REGISTERED

    # The project version from before dataset submission should still be there.
    project_config = project_mgr.get_by_id(project_id, "1.0.0")
    dataset = project_config.get_dataset(dataset_id)
    assert dataset.status == DatasetRegistryStatus.UNREGISTERED

    with pytest.raises(DSGDuplicateValueRegistered):
        project_config_file = test_project_dir / "project.json5"
        project_mgr.register(project_config_file, user, log_message)

    with pytest.raises(DSGDuplicateValueRegistered):
        dataset_config_file = (
            test_project_dir / "datasets" / "modeled" / "comstock" / "dataset.json5"
        )
        dataset_mgr.register(dataset_config_file, user, log_message)

    # Duplicate mappings get re-used.
    mapping_ids = dimension_mapping_mgr.list_ids()
    dimension_mapping_mgr.dump(dimension_mapping_id, tmp_path)
    dimension_mapping_config = tmp_path / "dimension_mapping.json5"
    data = load_data(dimension_mapping_config)
    dump_data({"mappings": [data]}, dimension_mapping_config)
    dimension_mapping_mgr.register(dimension_mapping_config, user, log_message)
    assert len(dimension_mapping_mgr.list_ids()) == len(mapping_ids)

    check_configs_update(tmp_path, manager)
    check_update_project_dimension(tmp_path, manager)
    # Note that the dataset is now unregistered.

    initial_registration = project_mgr.db.get_initial_registration(None, project_id)
    assert initial_registration.log_message == "Initial registration"

    # Test removals.
    check_config_remove(project_mgr, project_id)
    check_config_remove(dataset_mgr, dataset_id)
    check_config_remove(dimension_mgr, dimension_id)
    check_config_remove(dimension_mapping_mgr, dimension_mapping_id)


def test_duplicate_dimensions(tmp_registry_db):
    test_project_dir, tmp_path, url = tmp_registry_db
    conn = DatabaseConnection(url=url)
    create_local_test_registry(tmp_path, conn=conn)
    user = getpass.getuser()
    log_message = "Initial registration"
    with RegistryManager.load(conn, offline_mode=True) as manager:
        dimension_mgr = manager.dimension_manager
        dimension_mgr.register(test_project_dir / "dimensions.json5", user, log_message)

        # Registering duplicate dimensions and mappings are allowed.
        # If names are the same, they are replaced. Otherwise, new ones get registered.
        dimension_ids = dimension_mgr.list_ids()
        dim_config_file = test_project_dir / "dimensions.json5"

        dimension_mgr.register(dim_config_file, user, log_message)
        assert len(dimension_mgr.list_ids()) == len(dimension_ids)

        data = load_data(dim_config_file)
        data["dimensions"][0]["name"] += " new"
        dump_data(data, dim_config_file)

        dimension_mgr.register(dim_config_file, user, log_message)
        assert len(dimension_mgr.list_ids()) == len(dimension_ids) + 1

        data = load_data(dim_config_file)
        found = False
        for dim in data["dimensions"]:
            if dim["type"] == "time":
                assert dim["time_interval_type"] == "period_beginning"
                dim["time_interval_type"] = "period_ending"
                found = True
        assert found
        dump_data(data, dim_config_file)

        dimension_mgr.register(dim_config_file, user, log_message)
        assert len(dimension_mgr.list_ids()) == len(dimension_ids) + 2


def test_duplicate_project_dimension_names(tmp_registry_db):
    test_project_dir, tmp_path, url = tmp_registry_db
    conn = DatabaseConnection(url=url)
    create_local_test_registry(tmp_path, conn=conn)
    user = getpass.getuser()
    log_message = "Initial registration"
    with RegistryManager.load(conn, offline_mode=True) as manager:
        project_file = test_project_dir / "project.json5"
        data = load_data(project_file)
        for dim in data["dimensions"]["supplemental_dimensions"]:
            if dim["name"] == "US States":
                dim["name"] = "US Counties 2010 - ComStock Only"
        dump_data(data, project_file)
        with pytest.raises(ValidationError):
            manager.project_manager.register(project_file, user, log_message)


def test_register_duplicate_project_rollback_dimensions(tmp_registry_db):
    test_project_dir, tmp_path, url = tmp_registry_db
    src_dir = test_project_dir
    with make_test_data_registry(
        tmp_path,
        test_project_dir,
        database_url=url,
        include_projects=False,
        include_datasets=False,
    ) as manager:
        project_file = src_dir / "project.json5"
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


def test_register_and_submit_rollback_on_failure(tmp_registry_db):
    test_project_dir, tmp_path, url = tmp_registry_db
    conn = DatabaseConnection(url=url)
    create_local_test_registry(tmp_path, conn=conn)
    with RegistryManager.load(conn, offline_mode=True) as manager:
        project_file = test_project_dir / "project.json5"
        project_id = load_data(project_file)["project_id"]
        dataset_dir = test_project_dir / "datasets" / "modeled" / "comstock"
        dataset_config_file = dataset_dir / "dataset.json5"
        dataset_id = load_data(dataset_config_file)["dataset_id"]
        dataset_mapping_file = dataset_dir / "dimension_mappings.json5"
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

        mappings = map_dimension_names_to_ids(manager.dimension_manager)
        replace_dimension_names_with_current_ids(dataset_config_file, mappings)
        orig_dimension_ids = manager.dimension_manager.list_ids()
        orig_mapping_ids = manager.dimension_mapping_manager.list_ids()

        try:
            with pytest.raises(DSGInvalidDataset):
                manager.project_manager.register_and_submit_dataset(
                    dataset_config_file,
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
                if missing_record_file.is_dir():
                    shutil.rmtree(missing_record_file)
                else:
                    missing_record_file.unlink()

        assert manager.dimension_manager.list_ids() == orig_dimension_ids
        assert manager.dimension_mapping_manager.list_ids() == orig_mapping_ids
        assert not manager.dataset_manager.list_ids()
        assert manager.project_manager.list_ids() == [project_id]


def test_add_subset_dimensions(mutable_cached_registry, tmp_path):
    mgr, _ = mutable_cached_registry
    project_mgr = mgr.project_manager
    project_id = project_mgr.list_ids()[0]
    project = project_mgr.get_by_id(project_id)
    subset_data_file = tmp_path / "data.csv"
    subset_dimensions_data = """
id,electricity_end_uses
cooling,x
fans,x
    """
    subset_dimensions_model = {
        "subset_dimensions": [
            {
                "name": "End Uses by Fuel Type",
                "description": "Provides selection of end uses by fuel type.",
                "type": "metric",
                "filename": str(subset_data_file),
                "create_supplemental_dimension": True,
                "selectors": [
                    {
                        "name": "electricity_end_uses",
                        "description": "All Electric End Uses",
                        "column_values": {"fuel_id": "electricity", "unit": "MWh"},
                    },
                ],
            },
        ],
    }
    model_file = tmp_path / "model.json5"
    dump_data(subset_dimensions_model, model_file)
    subset_data_file.write_text(subset_dimensions_data, encoding="utf-8")

    url = f"sqlite:///{project_mgr.db.engine.url.database}"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "projects",
            "register-subset-dimensions",
            project_id,
            str(model_file),
            "-l",
            "test register-subset-dimensions",
        ],
    )
    assert result.exit_code == 0
    found_new_dimension = False
    project = project_mgr.get_by_id(project_id)
    for dim in project.list_supplemental_dimensions(DimensionType.METRIC):
        if dim.model.name == "End Uses by Fuel Type":
            found_new_dimension = True
            break
    assert found_new_dimension


def test_add_supplemental_dimension(mutable_cached_registry, tmp_path):
    mgr, _ = mutable_cached_registry
    project_mgr = mgr.project_manager
    project_id = project_mgr.list_ids()[0]
    project = project_mgr.get_by_id(project_id)
    county_ids = list(project.get_base_dimension(DimensionType.GEOGRAPHY).get_unique_ids())

    num_regions = 5
    regions = {x: [] for x in range(num_regions)}
    for i in range(len(county_ids)):
        region_id = i % num_regions
        regions[region_id].append(county_ids[i])

    dimensions_file = tmp_path / "dimensions.json5"
    dim_records_file = tmp_path / "regions.csv"
    mapping_records_file = tmp_path / "mappings.csv"
    new_dimensions = {
        "supplemental_dimensions": [
            {
                "type": "geography",
                "class": "GeographyDimensionBaseModel",
                "name": "Random Region",
                "description": "Randomly-generated regions",
                "file": str(dim_records_file),
                "module": "dsgrid.dimension.standard",
                "mapping": {
                    "description": "Maps US Counties to regions",
                    "file": str(mapping_records_file),
                    "mapping_type": "many_to_one_aggregation",
                },
            },
        ],
    }

    dump_data(new_dimensions, dimensions_file)
    with open(dim_records_file, "w") as f:
        f.write("id,name\n")
        for region in regions:
            f.write(f"{region},{region}\n")

    with open(mapping_records_file, "w") as f:
        f.write("from_id,to_id\n")
        for region, county_ids in regions.items():
            for county_id in county_ids:
                f.write(f"{county_id},{region}\n")

    url = f"sqlite:///{project_mgr.db.engine.url.database}"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "projects",
            "register-supplemental-dimensions",
            project_id,
            str(dimensions_file),
            "-l",
            "test register-supplemental-dimensions",
        ],
    )
    assert result.exit_code == 0
    found_new_dimension = False
    project = project_mgr.get_by_id(project_id)
    for dim in project.list_supplemental_dimensions(DimensionType.GEOGRAPHY):
        if dim.model.name == "Random Region":
            found_new_dimension = True
            break
    assert found_new_dimension


@pytest.mark.skipif(
    DsgridRuntimeConfig.load().backend_engine == BackendEngine.SPARK,
    reason="Spark backend is not supported with a DuckDB data store",
)
def test_register_with_duckdb_store(registry_with_duckdb_store):
    conn = registry_with_duckdb_store
    with RegistryManager.load(conn, offline_mode=True) as manager:
        project_mgr = manager.project_manager
        dataset_mgr = manager.dataset_manager
        project_ids = project_mgr.list_ids()
        assert len(project_ids) == 1
        project_id = project_ids[0]
        dataset_ids = dataset_mgr.list_ids()
        assert len(dataset_ids) == 2
        project = project_mgr.load_project(project_id)
        found_lookup = False
        for dataset_id in dataset_ids:
            dataset = project.load_dataset(dataset_id)
            assert isinstance(dataset._handler._load_data, DataFrame)
            if dataset_id == "test_efs_comstock":
                assert isinstance(dataset._handler._load_data_lookup, DataFrame)
                found_lookup = True
        assert found_lookup


def test_remove_dataset(mutable_cached_registry):
    mgr, _ = mutable_cached_registry
    project_mgr = mgr.project_manager
    project_id = project_mgr.list_ids()[0]
    config = project_mgr.get_by_id(project_id)
    dataset_id = config.model.datasets[0].dataset_id
    assert config.model.datasets[0].status == DatasetRegistryStatus.REGISTERED

    url = f"sqlite:///{project_mgr.db.engine.url.database}"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "datasets",
            "remove",
            dataset_id,
        ],
    )
    assert result.exit_code == 0
    config = project_mgr.get_by_id(project_id)
    assert config.model.datasets[0].status == DatasetRegistryStatus.UNREGISTERED


def test_add_dataset_requirements(mutable_cached_registry, tmp_path):
    mgr, _ = mutable_cached_registry
    project_mgr = mgr.project_manager
    project_id = project_mgr.list_ids()[0]
    config = project_mgr.get_by_id(project_id)
    dataset = copy.deepcopy(config.model.datasets[0])
    dataset.dataset_id = "fake"
    dataset.status = DatasetRegistryStatus.UNREGISTERED
    dataset.mapping_references.clear()
    model = InputDatasetListModel(datasets=[dataset])
    dataset_file = tmp_path / "datasets.json5"
    with open(dataset_file, "w") as f:
        f.write(model.model_dump_json())

    url = f"sqlite:///{project_mgr.db.engine.url.database}"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "projects",
            "add-dataset-requirements",
            project_id,
            str(dataset_file),
            "-l",
            "replace dataset dimension requirements",
        ],
    )
    assert result.exit_code == 0
    config = project_mgr.get_by_id(project_id)
    assert config.model.datasets[0].status == DatasetRegistryStatus.REGISTERED
    assert config.model.datasets[1].status == DatasetRegistryStatus.REGISTERED
    assert config.model.datasets[2].dataset_id == "fake"


def test_replace_dataset_dimension_requirements(mutable_cached_registry, tmp_path):
    mgr, _ = mutable_cached_registry
    project_mgr = mgr.project_manager
    project_id = project_mgr.list_ids()[0]
    config = project_mgr.get_by_id(project_id)
    requirements_file = tmp_path / "requirements.json5"
    dataset = config.model.datasets[1]
    com_record_ids = [
        "FullServiceRestaurant",
        "Hospital",
        "LargeHotel",
        "LargeOffice",
        "MediumOffice",
        "Outpatient",
        "PrimarySchool",
        "QuickServiceRestaurant",
        "SmallHotel",
        "SmallOffice",
        "StandaloneRetail",
        "StripMall",
        "Warehouse",
    ]

    assert dataset.status == DatasetRegistryStatus.REGISTERED
    assert config.model.datasets[0].status == DatasetRegistryStatus.REGISTERED
    reqs = dataset.required_dimensions
    assert reqs.multi_dimensional[0].subsector.subset[0].selectors == ["commercial_subsectors2"]
    reqs.multi_dimensional[0].subsector.subset.clear()
    reqs.multi_dimensional[0].subsector.base.record_ids = com_record_ids
    model = InputDatasetDimensionRequirementsListModel(
        dataset_dimension_requirements=[
            InputDatasetDimensionRequirementsModel(
                dataset_id=dataset.dataset_id, required_dimensions=reqs
            ),
        ],
    )
    with open(requirements_file, "w") as f:
        f.write(model.model_dump_json())

    url = f"sqlite:///{project_mgr.db.engine.url.database}"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "projects",
            "replace-dataset-dimension-requirements",
            project_id,
            str(requirements_file),
            "-l",
            "replace dataset dimension requirements",
        ],
    )
    assert result.exit_code == 0
    config = project_mgr.get_by_id(project_id)
    assert config.model.datasets[0].status == DatasetRegistryStatus.REGISTERED
    assert config.model.datasets[1].status == DatasetRegistryStatus.UNREGISTERED
    assert reqs.multi_dimensional[0].subsector.base.record_ids == com_record_ids


def test_auto_updates(mutable_cached_registry: tuple[RegistryManager, Path]):
    mgr, _ = mutable_cached_registry
    project_mgr = mgr.project_manager
    dataset_mgr = mgr.dataset_manager
    dimension_mgr = mgr.dimension_manager
    dimension_mapping_mgr = mgr.dimension_mapping_manager
    project_id = project_mgr.list_ids()[0]
    dataset_id = dataset_mgr.list_ids()[0]
    dimension = [
        x
        for x in dimension_mgr.iter_configs()
        if x.model.name.startswith("US Counties 2010 - ComStock Only")
    ][0]
    orig_dim_version = dimension.model.version

    # Test that we can convert records to a Spark DataFrame. Unrelated to the rest.
    assert isinstance(dimension.get_records_dataframe(), DataFrame)

    dimension.model.description += "; test update"
    update_type = VersionUpdateType.MINOR
    log_message = "test update"
    new_dimension = dimension_mgr.update(dimension, update_type, log_message)

    # Find a mapping that uses this dimension and verify that it gets updated.
    mapping = None
    for _mapping in dimension_mapping_mgr.iter_configs():
        from_dim = dimension_mgr.get_by_id(_mapping.model.from_dimension.dimension_id).model
        to_dim = dimension_mgr.get_by_id(_mapping.model.to_dimension.dimension_id).model
        if from_dim.name.startswith("US Counties 2010 - ComStock Only") and to_dim.name.startswith(
            "US Census Regions"
        ):
            mapping = _mapping
            break
    assert mapping is not None
    orig_version = dimension_mapping_mgr.get_latest_version(mapping.model.mapping_id)
    assert orig_version == "1.0.0"

    project = project_mgr.get_by_id(project_id)
    orig_project_version = project.model.version
    assert orig_project_version == "1.2.0"

    updater = RegistryAutoUpdater(mgr)
    updater.update_dependent_configs(new_dimension, orig_dim_version, update_type, log_message)

    new_version = dimension_mapping_mgr.get_latest_version(mapping.model.mapping_id)
    assert new_version == "1.1.0"

    project = project_mgr.get_by_id(project_id)
    new_project_version = project.model.version
    assert new_project_version == "1.3.0"

    found = False
    for mapping_ref in project.model.dimension_mappings.base_to_supplemental_references:
        if mapping_ref.mapping_id == mapping.model.mapping_id:
            assert mapping_ref.version == new_version
            found = True
    assert found

    dataset = dataset_mgr.get_by_id(dataset_id)
    assert dataset.model.version == "1.1.0"
    found = False
    for dimension_ref in dataset.model.dimension_references:
        if dimension_ref.dimension_id == dimension.model.dimension_id:
            assert dimension_ref.version == "1.1.0"
            found = True
    assert found

    # The project should get updated again if we update the dimension mapping.
    mapping = dimension_mapping_mgr.get_by_id(mapping.model.mapping_id)
    mapping.model.description += "test update"
    original_version = mapping.model.version
    new_mapping = dimension_mapping_mgr.update(mapping, VersionUpdateType.PATCH, "test update")
    assert new_mapping.model.version == "1.1.1"
    updater.update_dependent_configs(
        new_mapping, original_version, VersionUpdateType.PATCH, "test update"
    )
    assert project_mgr.get_latest_version(project_id) == "1.3.1"

    dataset.model.description += "test update"
    original_version = dataset.model.version
    dataset_mgr.update(dataset, VersionUpdateType.PATCH, "test update")
    updater.update_dependent_configs(
        dataset, original_version, VersionUpdateType.PATCH, "test update"
    )
    assert project_mgr.get_latest_version(project_id) == "1.3.2"


def test_invalid_dimension_mapping(tmp_registry_db):
    test_project_dir, tmp_path, url = tmp_registry_db
    user = getpass.getuser()
    log_message = "Initial registration"
    conn = DatabaseConnection(url=url)
    create_local_test_registry(tmp_path, conn=conn)
    with RegistryManager.load(conn, offline_mode=True) as manager:
        dim_mgr = manager.dimension_manager
        dim_mgr.register(test_project_dir / "dimensions.json5", user, log_message)
        dim_mapping_mgr = manager.dimension_mapping_manager
        dimension_mapping_file = test_project_dir / "dimension_mappings_with_ids.json5"
        mappings = map_dimension_names_to_ids(manager.dimension_manager)
        replace_dimension_names_with_current_ids(dimension_mapping_file, mappings)

        record_file = (
            test_project_dir
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


def test_register_submit_dataset_long_workflow(tmp_registry_db):
    src_dir, tmp_path, url = tmp_registry_db
    with make_test_data_registry(
        tmp_path,
        src_dir,
        include_projects=False,
        include_datasets=False,
        database_url=url,
    ) as manager:
        dim_mapping_mgr = manager.dimension_mapping_manager
        project_config_file = src_dir / "project_with_dimension_ids.json5"
        project_id = load_data(project_config_file)["project_id"]
        project_dimension_mapping_config = src_dir / "dimension_mappings_with_ids.json5"
        project_dimension_file = src_dir / "dimensions.json5"
        dataset_dir = src_dir / "datasets" / "modeled" / "comstock"
        dataset_config_file = dataset_dir / "dataset_with_dimension_ids.json5"
        dataset_id = load_data(dataset_config_file)["dataset_id"]
        dataset_dimension_file = dataset_dir / "dimensions.json5"
        dimension_mapping_config = dataset_dir / "dimension_mapping_config_with_ids.json5"
        dimension_mapping_refs = dataset_dir / "dimension_mapping_references.json5"
        user = getpass.getuser()
        log_message = "register"

        manager.dimension_manager.register(project_dimension_file, user, log_message)
        manager.dimension_manager.register(dataset_dimension_file, user, log_message)

        dim_mappings = map_dimension_names_to_ids(manager.dimension_manager)
        for filename in (project_dimension_mapping_config, dimension_mapping_config):
            replace_dimension_names_with_current_ids(filename, dim_mappings)

        dim_mapping_mgr.register(project_dimension_mapping_config, user, log_message)
        dim_mapping_mgr.register(dimension_mapping_config, user, log_message)
        dim_id_to_name = map_dimension_ids_to_names(manager.dimension_manager)
        dim_mapping_mappings = map_dimension_mapping_names_to_ids(
            manager.dimension_mapping_manager, dim_id_to_name
        )

        for filename in (project_config_file, dataset_config_file):
            replace_dimension_names_with_current_ids(filename, dim_mappings)
        for filename in (project_config_file, dimension_mapping_refs):
            replace_dimension_mapping_names_with_current_ids(filename, dim_mapping_mappings)

        manager.project_manager.register(project_config_file, user, "register project")
        manager.dataset_manager.register(
            dataset_config_file,
            user,
            "register dataset",
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


def test_registry_contains(cached_registry):
    conn = cached_registry
    with RegistryManager.load(conn) as mgr:
        project_mgr = mgr.project_manager
        dimension_mgr = mgr.dimension_manager
        county_dim = None
        for config in dimension_mgr.iter_configs():
            if config.model.name == "US Counties 2010 - ComStock Only":
                county_dim = config.model
                break
        assert county_dim is not None
        containing_models = project_mgr.db.get_containing_models(None, county_dim)
        assert len(containing_models[RegistryType.PROJECT]) == 1
        assert len(containing_models[RegistryType.DATASET]) == 2
        assert len(containing_models[RegistryType.DIMENSION_MAPPING]) == 4
        assert len(containing_models[RegistryType.DIMENSION]) == 0


def test_sql(cached_registry):
    conn = cached_registry
    with RegistryManager.load(conn) as mgr:
        project_mgr = mgr.project_manager
        df = project_mgr.db.sql(f"SELECT * FROM {RegistryTables.REGISTRATIONS.value}")
        assert "timestamp" in df.columns


def test_register_dataset_with_data_base_dir(tmp_registry_db, tmp_path):
    """Test dataset registration with --data-base-dir CLI option."""
    src_dir, registry_tmp_path, url = tmp_registry_db
    with make_test_data_registry(
        registry_tmp_path,
        src_dir,
        include_projects=False,
        include_datasets=False,
        database_url=url,
    ) as manager:
        project_config_file = src_dir / "project_with_dimension_ids.json5"
        project_dimension_mapping_config = src_dir / "dimension_mappings_with_ids.json5"
        project_dimension_file = src_dir / "dimensions.json5"
        dataset_dir = src_dir / "datasets" / "modeled" / "comstock"
        dataset_config_file = dataset_dir / "dataset_with_dimension_ids.json5"
        dataset_dimension_file = dataset_dir / "dimensions.json5"
        user = getpass.getuser()
        log_message = "register"

        manager.dimension_manager.register(project_dimension_file, user, log_message)
        manager.dimension_manager.register(dataset_dimension_file, user, log_message)

        dim_mappings = map_dimension_names_to_ids(manager.dimension_manager)
        replace_dimension_names_with_current_ids(project_dimension_mapping_config, dim_mappings)
        replace_dimension_names_with_current_ids(project_config_file, dim_mappings)
        replace_dimension_names_with_current_ids(dataset_config_file, dim_mappings)

        manager.dimension_mapping_manager.register(
            project_dimension_mapping_config, user, log_message
        )
        dim_id_to_name = map_dimension_ids_to_names(manager.dimension_manager)
        dim_mapping_mappings = map_dimension_mapping_names_to_ids(
            manager.dimension_mapping_manager, dim_id_to_name
        )
        replace_dimension_mapping_names_with_current_ids(project_config_file, dim_mapping_mappings)
        manager.project_manager.register(project_config_file, user, "register project")

        # Create a modified dataset config with relative paths that will be resolved
        # against a different base directory (the actual data files location)
        data_base_dir = TEST_DATASET_DIRECTORY / "test_efs_comstock"
        modified_config_file = tmp_path / "dataset_modified.json5"
        data = load_data(dataset_config_file)

        # Change paths to be relative to the data_base_dir
        data["table_schema"]["data_file"]["path"] = "load_data.csv"
        data["table_schema"]["lookup_data_file"]["path"] = "load_data_lookup.json"
        data["table_schema"]["missing_associations"] = ["missing_associations"]
        dump_data(data, modified_config_file)

        # Use CLI runner to test the --data-base-dir option
        db_url = f"sqlite:///{manager.dataset_manager.db.engine.url.database}"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--url",
                db_url,
                "registry",
                "datasets",
                "register",
                str(modified_config_file),
                "-l",
                "test register with data-base-dir",
                "-D",
                str(data_base_dir),
                "-M",
                str(data_base_dir),
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert manager.dataset_manager.list_ids() == ["test_efs_comstock"]


def test_register_and_submit_dataset_with_data_base_dir(tmp_registry_db, tmp_path):
    """Test register-and-submit-dataset CLI command with --data-base-dir option."""
    src_dir, registry_tmp_path, url = tmp_registry_db
    with make_test_data_registry(
        registry_tmp_path,
        src_dir,
        include_projects=False,
        include_datasets=False,
        database_url=url,
    ) as manager:
        project_config_file = src_dir / "project_with_dimension_ids.json5"
        project_dimension_mapping_config = src_dir / "dimension_mappings_with_ids.json5"
        project_dimension_file = src_dir / "dimensions.json5"
        dataset_dir = src_dir / "datasets" / "modeled" / "comstock"
        dataset_config_file = dataset_dir / "dataset_with_dimension_ids.json5"
        dataset_dimension_file = dataset_dir / "dimensions.json5"
        dimension_mapping_config = dataset_dir / "dimension_mapping_config_with_ids.json5"
        dimension_mapping_refs = dataset_dir / "dimension_mapping_references.json5"
        user = getpass.getuser()
        log_message = "register"

        manager.dimension_manager.register(project_dimension_file, user, log_message)
        manager.dimension_manager.register(dataset_dimension_file, user, log_message)

        dim_mappings = map_dimension_names_to_ids(manager.dimension_manager)
        replace_dimension_names_with_current_ids(project_dimension_mapping_config, dim_mappings)
        replace_dimension_names_with_current_ids(project_config_file, dim_mappings)
        replace_dimension_names_with_current_ids(dataset_config_file, dim_mappings)
        replace_dimension_names_with_current_ids(dimension_mapping_config, dim_mappings)

        manager.dimension_mapping_manager.register(
            project_dimension_mapping_config, user, log_message
        )
        manager.dimension_mapping_manager.register(dimension_mapping_config, user, log_message)

        dim_id_to_name = map_dimension_ids_to_names(manager.dimension_manager)
        dim_mapping_mappings = map_dimension_mapping_names_to_ids(
            manager.dimension_mapping_manager, dim_id_to_name
        )
        replace_dimension_mapping_names_with_current_ids(project_config_file, dim_mapping_mappings)
        replace_dimension_mapping_names_with_current_ids(
            dimension_mapping_refs, dim_mapping_mappings
        )
        manager.project_manager.register(project_config_file, user, "register project")
        project_id = load_data(project_config_file)["project_id"]

        # Create a modified dataset config with relative paths that will be resolved
        # against a different base directory (the actual data files location)
        data_base_dir = TEST_DATASET_DIRECTORY / "test_efs_comstock"
        modified_config_file = tmp_path / "dataset_modified.json5"
        data = load_data(dataset_config_file)

        # Change paths to be relative to the data_base_dir
        data["table_schema"]["data_file"]["path"] = "load_data.csv"
        data["table_schema"]["lookup_data_file"]["path"] = "load_data_lookup.json"
        data["table_schema"]["missing_associations"] = ["missing_associations"]
        dump_data(data, modified_config_file)

        # Use CLI runner to test register-and-submit-dataset with --data-base-dir
        db_url = f"sqlite:///{manager.dataset_manager.db.engine.url.database}"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--url",
                db_url,
                "registry",
                "projects",
                "register-and-submit-dataset",
                "-c",
                str(modified_config_file),
                "-p",
                project_id,
                "-r",
                str(dimension_mapping_refs),
                "-l",
                "test register-and-submit with data-base-dir",
                "-D",
                str(data_base_dir),
                "-M",
                str(data_base_dir),
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert manager.dataset_manager.list_ids() == ["test_efs_comstock"]

        # Verify dataset was submitted to project
        project = manager.project_manager.get_by_id(project_id)
        dataset = project.get_dataset("test_efs_comstock")
        assert dataset.status == DatasetRegistryStatus.REGISTERED


def register_project(project_mgr, config_file, project_id, user, log_message):
    project_mgr.register(config_file, user, log_message)
    assert project_mgr.list_ids() == [project_id]


def register_dataset(dataset_mgr, config_file, dataset_id, user, log_message):
    dataset_mgr.register(config_file, user, log_message)
    assert dataset_mgr.list_ids() == [dataset_id]


def check_update_project_dimension(tmpdir, manager):
    """Verify that updating a project's dimension causes all datasets to go unregistered."""
    project_mgr = manager.project_manager
    dimension_mgr = manager.dimension_manager
    project_id = project_mgr.list_ids()[0]
    project_config = project_mgr.get_by_id(project_id)
    dimension_id = project_config.model.dimensions.base_dimension_references[0].dimension_id
    user = getpass.getuser()

    dim_dir = tmpdir / "new_dimension"
    dim_config_file = dim_dir / dimension_mgr.config_class().config_filename()
    dimension_mgr.dump(dimension_id, dim_dir, force=True)
    dim_data = load_data(dim_config_file)
    orig_version = dim_data["version"]
    dim_data["description"] += "; updated description"
    dump_data(dim_data, dim_config_file)
    dimension_mgr.update_from_file(
        dim_config_file,
        dimension_id,
        user,
        VersionUpdateType.PATCH,
        "update to description",
        dimension_mgr.get_latest_version(dimension_id),
    )
    new_version = dimension_mgr.get_latest_version(dimension_id)
    assert new_version != orig_version
    project_dir = tmpdir / "new_project"
    project_config_file = project_dir / project_mgr.config_class().config_filename()
    project_mgr.dump(project_id, project_dir, force=True)
    project_data = load_data(project_config_file)
    found = False
    for dim in project_data["dimensions"]["base_dimension_references"]:
        if dim["dimension_id"] == dimension_id:
            dim["version"] = str(new_version)
            found = True
            break
    assert found
    dump_data(project_data, project_config_file)
    project_mgr.update_from_file(
        project_config_file,
        project_id,
        user,
        VersionUpdateType.PATCH,
        "update dimension",
        project_mgr.get_latest_version(project_id),
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
