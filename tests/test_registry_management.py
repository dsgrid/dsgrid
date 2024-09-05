import copy
import getpass
import os
import shutil
from pathlib import Path

import pyspark
import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from dsgrid.cli.dsgrid import cli
from dsgrid.cli.dsgrid_admin import cli as cli_admin
from dsgrid.config.input_dataset_requirements import (
    InputDatasetDimensionRequirementsModel,
    InputDatasetDimensionRequirementsListModel,
    InputDatasetListModel,
)
from dsgrid.common import DEFAULT_DB_PASSWORD
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import (
    DSGDuplicateValueRegistered,
    DSGInvalidDataset,
    DSGInvalidDimensionMapping,
    DSGValueNotRegistered,
)
from dsgrid.registry.common import DatasetRegistryStatus, ProjectRegistryStatus, VersionUpdateType
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
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


def test_register_project_and_dataset(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    manager = make_test_data_registry(
        tmp_path, test_project_dir, dataset_path=TEST_DATASET_DIRECTORY, database_name=db_name
    )
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
    dataset_path = TEST_DATASET_DIRECTORY / dataset_id

    project_config = project_mgr.get_by_id(project_id, "1.1.0")
    assert project_config.model.status == ProjectRegistryStatus.IN_PROGRESS
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
        dataset_mgr.register(dataset_config_file, dataset_path, user, log_message)

    # Duplicate mappings get re-used.
    mapping_ids = dimension_mapping_mgr.list_ids()
    dimension_mapping_mgr.dump(dimension_mapping_id, tmp_path)
    dimension_mapping_config = tmp_path / "dimension_mapping.json5"
    data = load_data(dimension_mapping_config)
    for field in ("_id", "_key", "_rev"):
        data.pop(field)
    dump_data({"mappings": [data]}, dimension_mapping_config)
    dimension_mapping_mgr.register(dimension_mapping_config, user, log_message)
    assert len(dimension_mapping_mgr.list_ids()) == len(mapping_ids)

    check_configs_update(tmp_path, manager)
    check_update_project_dimension(tmp_path, manager)
    # Note that the dataset is now unregistered.

    # Test removals.
    check_config_remove(project_mgr, project_id)
    check_config_remove(dataset_mgr, dataset_id)
    check_config_remove(dimension_mgr, dimension_id)
    check_config_remove(dimension_mapping_mgr, dimension_mapping_id)


def test_duplicate_dimensions(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    conn = DatabaseConnection(database=db_name)
    create_local_test_registry(Path(tmp_path), conn=conn)
    user = getpass.getuser()
    log_message = "Initial registration"
    manager = RegistryManager.load(conn, offline_mode=True)

    dimension_mgr = manager.dimension_manager
    dimension_mgr.register(test_project_dir / "dimensions.json5", user, log_message)

    # Registering duplicate dimensions and mappings are allowed.
    # If names are the same, they are replaced. Otherwise, new ones get registered.
    # Time dimension has more fields checked.
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
    for dim in data["dimensions"]:
        if dim["type"] == "time":
            assert dim["time_interval_type"] == "period_beginning"
            dim["time_interval_type"] = "period_ending"
    dump_data(data, dim_config_file)

    dimension_mgr.register(dim_config_file, user, log_message)
    assert len(dimension_mgr.list_ids()) == len(dimension_ids) + 2


def test_duplicate_project_dimension_display_names(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    conn = DatabaseConnection(database=db_name)
    create_local_test_registry(tmp_path, conn=conn)
    user = getpass.getuser()
    log_message = "Initial registration"
    manager = RegistryManager.load(conn, offline_mode=True)

    project_file = test_project_dir / "project.json5"
    data = load_data(project_file)
    for dim in data["dimensions"]["supplemental_dimensions"]:
        if dim["display_name"] == "State":
            dim["display_name"] = "County"
    dump_data(data, project_file)
    with pytest.raises(ValidationError):
        manager.project_manager.register(project_file, user, log_message)


def test_register_duplicate_project_rollback_dimensions(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    src_dir = test_project_dir
    manager = make_test_data_registry(
        tmp_path,
        test_project_dir,
        dataset_path=TEST_DATASET_DIRECTORY,
        database_name=db_name,
        include_projects=False,
        include_datasets=False,
    )
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
    test_project_dir, tmp_path, db_name = tmp_registry_db
    conn = DatabaseConnection(database=db_name)
    create_local_test_registry(tmp_path, conn=conn)
    manager = RegistryManager.load(conn, offline_mode=True)
    project_file = test_project_dir / "project.json5"
    project_id = load_data(project_file)["project_id"]
    dataset_dir = test_project_dir / "datasets" / "modeled" / "comstock"
    dataset_config_file = dataset_dir / "dataset.json5"
    dataset_id = load_data(dataset_config_file)["dataset_id"]
    dataset_mapping_file = dataset_dir / "dimension_mappings.json5"
    dataset_path = (
        Path(os.environ.get("DSGRID_LOCAL_DATA_DIRECTORY", TEST_DATASET_DIRECTORY))
        / "test_efs_comstock"
    )
    subsectors_file = (
        dataset_dir / "dimension_mappings" / "lookup_comstock_subsectors_to_project_subsectors.csv"
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


def test_add_subset_dimensions(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    mgr = make_test_data_registry(
        tmp_path, test_project_dir, dataset_path=TEST_DATASET_DIRECTORY, database_name=db_name
    )
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
                "display_name": "end_uses_by_fuel_type",
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

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "--username",
            "root",
            "--password",
            DEFAULT_DB_PASSWORD,
            "--database-name",
            db_name,
            "--offline",
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


def test_add_supplemental_dimension(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    mgr = make_test_data_registry(
        tmp_path, test_project_dir, dataset_path=TEST_DATASET_DIRECTORY, database_name=db_name
    )
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
                "display_name": "Random Region",
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

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "--username",
            "root",
            "--password",
            DEFAULT_DB_PASSWORD,
            "--database-name",
            db_name,
            "--offline",
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


def test_remove_dataset(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    mgr = make_test_data_registry(
        tmp_path, test_project_dir, dataset_path=TEST_DATASET_DIRECTORY, database_name=db_name
    )
    project_mgr = mgr.project_manager
    project_id = project_mgr.list_ids()[0]
    config = project_mgr.get_by_id(project_id)
    dataset_id = config.model.datasets[0].dataset_id
    assert config.model.datasets[0].status == DatasetRegistryStatus.REGISTERED

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli_admin,
        [
            "--username",
            "root",
            "--password",
            DEFAULT_DB_PASSWORD,
            "--database-name",
            db_name,
            "--offline",
            "registry",
            "datasets",
            "remove",
            dataset_id,
        ],
    )
    assert result.exit_code == 0
    config = project_mgr.get_by_id(project_id)
    assert config.model.datasets[0].status == DatasetRegistryStatus.UNREGISTERED


def test_add_dataset_requirements(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    mgr = make_test_data_registry(
        tmp_path, test_project_dir, dataset_path=TEST_DATASET_DIRECTORY, database_name=db_name
    )
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

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "--username",
            "root",
            "--password",
            DEFAULT_DB_PASSWORD,
            "--database-name",
            db_name,
            "--offline",
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


def test_replace_dataset_dimension_requirements(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    mgr = make_test_data_registry(
        tmp_path, test_project_dir, dataset_path=TEST_DATASET_DIRECTORY, database_name=db_name
    )
    project_mgr = mgr.project_manager
    project_id = project_mgr.list_ids()[0]
    config = project_mgr.get_by_id(project_id)
    requirements_file = tmp_path / "requirements.json5"
    dataset = config.model.datasets[1]
    assert dataset.status == DatasetRegistryStatus.REGISTERED
    assert config.model.datasets[0].status == DatasetRegistryStatus.REGISTERED
    reqs = dataset.required_dimensions
    assert reqs.multi_dimensional[0].subsector.supplemental[0].record_ids == [
        "commercial_subsectors"
    ]
    reqs.multi_dimensional[0].subsector.supplemental[0].record_ids = ["residential_subsectors"]
    reqs.multi_dimensional[0].subsector.supplemental[0].name = "Residential Subsector"
    model = InputDatasetDimensionRequirementsListModel(
        dataset_dimension_requirements=[
            InputDatasetDimensionRequirementsModel(
                dataset_id=dataset.dataset_id, required_dimensions=reqs
            ),
        ],
    )
    with open(requirements_file, "w") as f:
        f.write(model.model_dump_json())

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "--username",
            "root",
            "--password",
            DEFAULT_DB_PASSWORD,
            "--database-name",
            db_name,
            "--offline",
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
    assert reqs.multi_dimensional[0].subsector.supplemental[0].record_ids == [
        "residential_subsectors"
    ]


def test_auto_updates(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    mgr = make_test_data_registry(
        tmp_path, test_project_dir, dataset_path=TEST_DATASET_DIRECTORY, database_name=db_name
    )
    project_mgr = mgr.project_manager
    dataset_mgr = mgr.dataset_manager
    dimension_mgr = mgr.dimension_manager
    dimension_mapping_mgr = mgr.dimension_mapping_manager
    project_id = project_mgr.list_ids()[0]
    dataset_id = dataset_mgr.list_ids()[0]
    dimension = [
        x for x in dimension_mgr.iter_configs() if x.model.name.startswith("US Counties 2010")
    ][0]

    # Test that we can convert records to a Spark DataFrame. Unrelated to the rest.
    assert isinstance(dimension.get_records_dataframe(), pyspark.sql.DataFrame)

    dimension.model.description += "; test update"
    update_type = VersionUpdateType.MINOR
    log_message = "test update"
    dimension_mgr.update(dimension, update_type, log_message)

    # Find a mapping that uses this dimension and verify that it gets updated.
    mapping = None
    for _mapping in dimension_mapping_mgr.iter_configs():
        from_dim = dimension_mgr.get_by_id(_mapping.model.from_dimension.dimension_id).model
        to_dim = dimension_mgr.get_by_id(_mapping.model.to_dimension.dimension_id).model
        if from_dim.name.startswith("US Counties") and to_dim.name.startswith("US Census Region"):
            mapping = _mapping
            break
    assert mapping is not None
    orig_version = dimension_mapping_mgr.get_latest_version(mapping.model.mapping_id)
    assert orig_version == "1.0.0"

    mgr.update_dependent_configs(dimension, update_type, log_message)

    new_version = dimension_mapping_mgr.get_latest_version(mapping.model.mapping_id)
    assert new_version == "1.1.0"

    project = project_mgr.get_by_id(project_id)
    found = False
    for mapping_ref in project.model.dimension_mappings.base_to_supplemental_references:
        if mapping_ref.mapping_id == mapping.model.mapping_id:
            assert mapping_ref.version == new_version
            found = True
    assert found

    dataset = dataset_mgr.get_by_id(dataset_id)
    found = False
    for dimension_ref in dataset.model.dimension_references:
        if dimension_ref.dimension_id == dimension.model.dimension_id:
            assert dimension_ref.version == "1.1.0"
            found = True
    assert found

    assert project.model.datasets[0].version == "1.1.0"
    assert project_mgr.get_latest_version(project_id) == "1.3.0"
    assert dataset_mgr.get_latest_version(dataset_id) == "1.1.0"

    # The project should get updated again if we update the dimension mapping.
    mapping.model.description += "test update"
    dimension_mapping_mgr.update(mapping, VersionUpdateType.PATCH, "test update")
    assert dimension_mapping_mgr.get_latest_version(mapping.config_id) == "1.1.1"
    mgr.update_dependent_configs(mapping, VersionUpdateType.PATCH, "test update")
    assert project_mgr.get_latest_version(project_id) == "1.3.1"

    # And again if we update the dataset.
    dataset.model.description += "test update"
    dataset_mgr.update(dataset, VersionUpdateType.PATCH, "test update")
    mgr.update_dependent_configs(dataset, VersionUpdateType.PATCH, "test update")
    assert project_mgr.get_latest_version(project_id) == "1.3.2"


def test_invalid_dimension_mapping(tmp_registry_db):
    test_project_dir, tmp_path, db_name = tmp_registry_db
    user = getpass.getuser()
    log_message = "Initial registration"
    conn = DatabaseConnection(database=db_name)
    create_local_test_registry(tmp_path, conn=conn)
    manager = RegistryManager.load(conn, offline_mode=True)

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
    src_dir, tmp_path, db_name = tmp_registry_db
    manager = make_test_data_registry(
        tmp_path, src_dir, include_projects=False, include_datasets=False, database_name=db_name
    )
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
    dataset_path = TEST_DATASET_DIRECTORY / dataset_id
    manager.dataset_manager.register(dataset_config_file, dataset_path, user, "register dataset")
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


def check_update_project_dimension(tmpdir, manager):
    """Verify that updating a project's dimension causes all datasets to go unregistered."""
    project_mgr = manager.project_manager
    project_id = project_mgr.list_ids()[0]
    dimension_mgr = manager.dimension_manager
    dimension_id = dimension_mgr.list_ids()[0]
    user = getpass.getuser()

    dim_dir = tmpdir / "new_dimension"
    dim_config_file = dim_dir / dimension_mgr.config_class().config_filename()
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
        dimension_mgr.get_latest_version(dimension_id),
    )
    project_dir = tmpdir / "new_project"
    project_config_file = project_dir / project_mgr.config_class().config_filename()
    project_mgr.dump(project_id, project_dir, force=True)
    project_data = load_data(project_config_file)
    new_version = dimension_mgr.get_latest_version(dimension_id)
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
