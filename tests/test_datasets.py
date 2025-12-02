import copy
import fileinput
import getpass
import json
import logging
import os
import shutil
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest
from chronify import InvalidTable

from dsgrid.config.dataset_config import DatasetConfigModel
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension, DSGInvalidField
from dsgrid.utils.id_remappings import (
    map_dimension_names_to_ids,
    replace_dimension_names_with_current_ids,
)
from dsgrid.utils.files import (
    dump_json_file,
    dump_line_delimited_json,
    load_json_file,
    load_line_delimited_json,
    load_data,
    delete_if_exists,
)
from dsgrid.tests.make_us_data_registry import make_test_data_registry
from dsgrid.tests.common import (
    STANDARD_SCENARIOS_PROJECT_REPO,
)

logger = logging.getLogger()

PROJECT_ID = "test_efs"
DATASET_ID = "test_efs_comstock"


@pytest.fixture(scope="module")
def setup_registry(tmp_path_factory, make_test_project_dir_module, make_test_data_dir_module):
    """Tests that don't successfully register the dataset can share this fixture."""
    base_dir = tmp_path_factory.mktemp("dsgrid")
    test_project_dir = make_test_project_dir_module
    test_data_dir = make_test_data_dir_module

    if DATASET_ID not in os.listdir(test_data_dir):
        logger.error("test_invalid_datasets requires the dsgrid-test-data repository")
        sys.exit(1)

    with make_test_data_registry(
        base_dir,
        test_project_dir,
        include_datasets=False,
        database_url=f"sqlite:///{base_dir}/registry.db",
    ) as manager:
        dataset_config_path = test_project_dir / "datasets" / "modeled" / "comstock"
        assert dataset_config_path.exists()
        dataset_config_file = dataset_config_path / "dataset.json5"
        mappings = map_dimension_names_to_ids(manager.dimension_manager)
        replace_dimension_names_with_current_ids(dataset_config_file, mappings)
        yield manager, base_dir, dataset_config_path, test_data_dir


@pytest.fixture(scope="function")
def setup_registry_single(tmp_path_factory, make_test_project_dir, make_test_data_dir):
    """Tests that successfully register the dataset must use this fixture."""
    base_dir = tmp_path_factory.mktemp("dsgrid")
    test_project_dir = make_test_project_dir
    test_data_dir = make_test_data_dir

    if DATASET_ID not in os.listdir(test_data_dir):
        logger.error("test_invalid_datasets requires the dsgrid-test-data repository")
        sys.exit(1)

    with make_test_data_registry(
        base_dir,
        test_project_dir,
        include_datasets=False,
        database_url=f"sqlite:///{base_dir}/registry.db",
    ) as manager:
        dataset_config_path = test_project_dir / "datasets" / "modeled" / "comstock"
        assert dataset_config_path.exists()
        dataset_config_file = dataset_config_path / "dataset.json5"
        mappings = map_dimension_names_to_ids(manager.dimension_manager)
        replace_dimension_names_with_current_ids(dataset_config_file, mappings)
        yield manager, base_dir, dataset_config_path, test_data_dir


@pytest.fixture
def register_dataset(setup_registry):
    manager, base_dir, dataset_config_path, src_dataset_path = setup_registry
    test_dir = base_dir / "test_data_dir"
    shutil.copytree(src_dataset_path, test_dir)
    dataset_config_file = dataset_config_path / "dataset.json5"
    data = load_data(dataset_config_file)
    dataset_id = data["dataset_id"]
    dataset_path = test_dir / dataset_id

    # Create a copy of the entire dataset config directory (including dimensions)
    test_config_dir = test_dir / "config"
    shutil.copytree(dataset_config_path, test_config_dir)
    test_config_file = test_config_dir / "dataset.json5"

    # Update paths in the config to point to the copied test data
    test_config = load_data(test_config_file)
    if "table_schema" in test_config and test_config["table_schema"] is not None:
        ts = test_config["table_schema"]
        if "data_file" in ts:
            ts["data_file"]["path"] = str(dataset_path / "load_data.csv")
        if "lookup_data_file" in ts and ts["lookup_data_file"] is not None:
            ts["lookup_data_file"]["path"] = str(dataset_path / "load_data_lookup.json")
        if "missing_associations" in ts and ts["missing_associations"]:
            ts["missing_associations"] = [str(dataset_path / "missing_associations")]
    from dsgrid.utils.files import dump_data

    dump_data(test_config, test_config_file)

    # This dict must get filled in by each test.
    expected_errors = {"exception": None, "match_msg": None}
    yield test_config_file, dataset_path, expected_errors
    try:
        with pytest.raises(expected_errors["exception"], match=expected_errors["match_msg"]):
            manager.dataset_manager.register(
                test_config_file,
                getpass.getuser(),
                "register invalid dataset",
            )
            expected_errors.clear()
    finally:
        shutil.rmtree(test_dir)


# TODO: This is now unused because the checks are primarily done when registering the dataset.
# We should be able to find a way to test a failure in submit-dataset in the future.
# @pytest.fixture
# def register_and_submit_dataset(setup_registry_single):
#     manager, base_dir, dataset_config_path, dataset_path = setup_registry_single
#     test_dir = base_dir / "test_data_dir"
#     shutil.copytree(dataset_path, test_dir)
#     dataset_config_file = dataset_config_path / "dataset.json5"
#     dataset_id = load_data(dataset_config_file)["dataset_id"]
#     dataset_path = test_dir / dataset_id
#     missing_assoc_file = dataset_path / "missing_associations.csv"
#     # This dict must get filled in by each test.
#     expected_errors = {"exception": None, "match_msg": None}
#     yield dataset_config_path, dataset_path, expected_errors
#     try:
#         manager.dataset_manager.register(
#             dataset_config_file,
#             dataset_path,
#             getpass.getuser(),
#             "register bad dataset",
#         )
#         project = manager.project_manager.load_project(PROJECT_ID)
#         assert not project.is_registered(dataset_id)
#         with pytest.raises(expected_errors["exception"], match=expected_errors["match_msg"]):
#             manager.project_manager.submit_dataset(
#                 PROJECT_ID,
#                 dataset_id,
#                 getpass.getuser(),
#                 "submit invalid dataset",
#                 dimension_mapping_file=dataset_config_path / "dimension_mappings.json5",
#             )
#             expected_errors.clear()
#         assert project.is_registered(dataset_id) == (expected_errors["exception"] is None)
#     finally:
#         shutil.rmtree(test_dir)
#         missing_record_file = Path(
#             f"{dataset_id}__{PROJECT_ID}__missing_dimension_record_combinations.parquet"
#         )
#         delete_if_exists(missing_record_file)


def test_invalid_load_data_lookup_column_name(register_dataset):
    config_file, dataset_path, expected_errors = register_dataset
    config = load_json_file(config_file)
    for column in config["table_schema"]["lookup_data_file"]["columns"]:
        if column["name"] == "subsector":
            column["name"] = "invalid_dimension"
    dump_json_file(config, config_file)
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    for item in data:
        item["invalid_dimension"] = item.pop("subsector")
    dump_line_delimited_json(data, lookup_file)
    expected_errors["exception"] = DSGInvalidDimension
    expected_errors["match_msg"] = r"column=.*invalid_dimension.*is not expected"


def test_invalid_load_data_lookup_integer_column(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    # Convert geography values from strings to integers to trigger the type validation error
    for item in data:
        item["geography"] = int(item["geography"])
    dump_line_delimited_json(data, lookup_file)
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"geography.*must have data type.*StringType"


def test_invalid_load_data_lookup_no_id(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    for item in data:
        if "id" in item:
            item.pop("id")
    dump_line_delimited_json(data, lookup_file)
    # Error is now raised during file reading due to schema validation
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"Expected columns.*id.*are not in"


def test_invalid_load_data_lookup_mismatched_records(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    first_id = data[0]["id"]
    dump_line_delimited_json(data[1:], lookup_file)

    data_file = dataset_path / "load_data.csv"
    with fileinput.input(files=[data_file], inplace=True) as f:
        for line in f:
            if not line.startswith(f"{first_id},"):
                print(line, end="")
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"is missing required dimension records"


def test_invalid_load_data_missing_timestamp(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    data_file = dataset_path / "load_data.csv"
    # Remove one row/timestamp from all load data arrays.
    timestamp = "2012-01-01T10:00:00"
    lines = data_file.read_text().splitlines()
    with open(data_file, "w") as f_out:
        for line in lines:
            if timestamp not in line:
                f_out.write(line)
                f_out.write("\n")

    expected_errors["exception"] = InvalidTable
    expected_errors["match_msg"] = r"Actual timestamps do not match expected timestamps"


def test_invalid_load_data_id_missing_timestamp(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    data_file = dataset_path / "load_data.csv"
    # Remove one row/timestamp for one load data array.
    text = "\n".join(data_file.read_text().splitlines()[:-1])
    data_file.write_text(text)
    expected_errors["exception"] = InvalidTable
    expected_errors["match_msg"] = r"The count of time values in each time array must be"


def test_invalid_load_data_id_extra_timestamp(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    data_file = dataset_path / "load_data.csv"
    lines = data_file.read_text().splitlines()
    header = lines[0].split(",")
    index = header.index("timestamp")
    row = lines[-1].split(",")
    timestamp = pd.to_datetime(row[index])
    new_row = copy.copy(row)
    new_row[index] = str(timestamp + timedelta(hours=1)).replace(" ", "T")
    with open(data_file, "a") as f_out:
        f_out.write(",".join([str(x) for x in new_row]))
        f_out.write("\n")
    expected_errors["exception"] = InvalidTable
    expected_errors["match_msg"] = r"Actual timestamps do not match expected timestamps"


def test_invalid_load_data_null_id(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    data_file = dataset_path / "load_data.csv"
    lines = data_file.read_text().splitlines()
    header = lines[0]
    assert header.split(",")[0] == "id"
    new_line_fields = lines[-1].split(",")
    new_line_fields[0] = ""
    lines.append(",".join(new_line_fields))
    data_file.write_text("\n".join(lines))
    expected_errors["exception"] = DSGInvalidField
    expected_errors["match_msg"] = r"DataFrame contains NULL value.*id"


def test_invalid_load_data_lookup_mismatched_ids(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    new_line = copy.deepcopy(data[0])
    new_line["id"] = 999999999
    data.append(new_line)
    dump_line_delimited_json(data, lookup_file)
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"Data IDs for .*data.lookup are inconsistent"


def test_invalid_load_data_lookup_null_id(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    item = copy.deepcopy(data[0])
    item["geography"] = None
    data.append(item)
    dump_line_delimited_json(data, lookup_file)
    expected_errors["exception"] = DSGInvalidField
    expected_errors["match_msg"] = r"DataFrame contains NULL value.*geography"


def test_invalid_load_data_extra_column(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    data_file = dataset_path / "load_data.csv"
    lines = data_file.read_text().splitlines()
    with open(data_file, "w") as f_out:
        f_out.write(lines[0])
        f_out.write(",extra\n")
        for line in lines[1:]:
            f_out.write(line)
            f_out.write(",0\n")

    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"column.*is not expected in load_data"


def test_invalid_load_data_lookup_missing_records(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    associations_file = dataset_path / "missing_associations" / "geography__subsector.csv"
    associations_file.unlink()
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"missing required dimension records"


def test_recovery_dataset_registration_failure_recovery(setup_registry_single):
    manager, base_dir, dataset_config_path, dataset_path = setup_registry_single
    test_dir = base_dir / "test_data_dir"
    shutil.copytree(dataset_path, test_dir)
    dataset_config_file = dataset_config_path / "dataset.json5"
    dim_mapping_file = dataset_config_path / "dimension_mappings.json5"
    dataset_id = load_data(dataset_config_file)["dataset_id"]
    dataset_path = test_dir / dataset_id

    try:
        with pytest.raises(DSGInvalidDataset):
            manager.project_manager.register_and_submit_dataset(
                dataset_config_file,
                PROJECT_ID,
                getpass.getuser(),
                "register and submit",
                dimension_mapping_file=None,
                dimension_mapping_references_file=None,
                autogen_reverse_supplemental_mappings=None,
            )

        manager.project_manager.register_and_submit_dataset(
            dataset_config_file,
            PROJECT_ID,
            getpass.getuser(),
            "register and submit",
            dimension_mapping_file=dim_mapping_file,
        )

    finally:
        shutil.rmtree(test_dir)
        missing_record_file = Path(
            f"{dataset_id}__{PROJECT_ID}__missing_dimension_record_combinations.parquet"
        )
        delete_if_exists(missing_record_file)


def test_invalid_dataset_id(tmp_path):
    config_file = (
        STANDARD_SCENARIOS_PROJECT_REPO
        / "dsgrid_project"
        / "datasets"
        / "modeled"
        / "comstock"
        / "dataset.json5"
    )
    DatasetConfigModel.load(config_file)

    data = load_data(config_file)
    data["dataset_id"] = "123invalid"
    filename = tmp_path / "dataset.json5"
    filename.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError):
        DatasetConfigModel.load(filename)
