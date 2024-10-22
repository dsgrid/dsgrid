import copy
import getpass
import logging
import os
import shutil
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension
from dsgrid.utils.id_remappings import (
    map_dimension_names_to_ids,
    replace_dimension_names_with_current_ids,
)
from dsgrid.utils.files import dump_line_delimited_json, load_line_delimited_json, load_data
from dsgrid.tests.make_us_data_registry import make_test_data_registry

logger = logging.getLogger()

PROJECT_ID = "test_efs"
DATASET_ID = "test_efs_comstock"


def make_registry(base_dir, test_project_dir, test_data_dir):
    if DATASET_ID not in os.listdir(test_data_dir):
        logger.error("test_invalid_datasets requires the dsgrid-test-data repository")
        sys.exit(1)

    manager = make_test_data_registry(
        base_dir,
        test_project_dir,
        dataset_path=test_data_dir,
        include_datasets=False,
        database_name="tmp-dsgrid",
    )
    dataset_config_path = test_project_dir / "datasets" / "modeled" / "comstock"
    assert dataset_config_path.exists()
    dataset_config_file = dataset_config_path / "dataset.json5"
    mappings = map_dimension_names_to_ids(manager.dimension_manager)
    replace_dimension_names_with_current_ids(dataset_config_file, mappings)
    return manager, dataset_config_path


@pytest.fixture(scope="module")
def setup_registry(tmp_path_factory, make_test_project_dir_module, make_test_data_dir_module):
    """Tests that don't successfully register the dataset can share this fixture."""
    base_dir = tmp_path_factory.mktemp("dsgrid")
    test_project_dir = make_test_project_dir_module
    test_data_dir = make_test_data_dir_module
    manager, dataset_config_path = make_registry(base_dir, test_project_dir, test_data_dir)
    yield manager, base_dir, dataset_config_path, test_data_dir


@pytest.fixture(scope="function")
def setup_registry_single(tmp_path_factory, make_test_project_dir, make_test_data_dir):
    """Tests that successfully register the dataset must use this fixture."""
    base_dir = tmp_path_factory.mktemp("dsgrid")
    test_project_dir = make_test_project_dir
    test_data_dir = make_test_data_dir
    manager, dataset_config_path = make_registry(base_dir, test_project_dir, test_data_dir)
    yield manager, base_dir, dataset_config_path, test_data_dir


@pytest.fixture
def register_dataset(setup_registry):
    manager, base_dir, dataset_config_path, dataset_path = setup_registry
    test_dir = base_dir / "test_data_dir"
    shutil.copytree(dataset_path, test_dir)
    dataset_config_file = dataset_config_path / "dataset.json5"
    dataset_id = load_data(dataset_config_file)["dataset_id"]
    dataset_path = test_dir / dataset_id
    # This dict must get filled in by each test.
    expected_errors = {"exception": None, "match_msg": None}
    yield dataset_config_path, dataset_path, expected_errors
    try:
        with pytest.raises(expected_errors["exception"], match=expected_errors["match_msg"]):
            manager.dataset_manager.register(
                dataset_config_file,
                dataset_path,
                getpass.getuser(),
                "register invalid dataset",
            )
            expected_errors.clear()
    finally:
        shutil.rmtree(test_dir)


@pytest.fixture
def register_and_submit_dataset(setup_registry_single):
    manager, base_dir, dataset_config_path, dataset_path = setup_registry_single
    test_dir = base_dir / "test_data_dir"
    shutil.copytree(dataset_path, test_dir)
    dataset_config_file = dataset_config_path / "dataset.json5"
    dataset_id = load_data(dataset_config_file)["dataset_id"]
    dataset_path = test_dir / dataset_id
    # This dict must get filled in by each test.
    expected_errors = {"exception": None, "match_msg": None}
    yield dataset_config_path, dataset_path, expected_errors
    try:
        manager.dataset_manager.register(
            dataset_config_file,
            dataset_path,
            getpass.getuser(),
            "register bad dataset",
        )
        project = manager.project_manager.load_project(PROJECT_ID)
        assert not project.is_registered(dataset_id)
        with pytest.raises(expected_errors["exception"], match=expected_errors["match_msg"]):
            manager.project_manager.submit_dataset(
                PROJECT_ID,
                dataset_id,
                getpass.getuser(),
                "submit invalid dataset",
                dimension_mapping_file=dataset_config_path / "dimension_mappings.json5",
            )
            expected_errors.clear()
        assert project.is_registered(dataset_id) == (expected_errors["exception"] is None)
    finally:
        shutil.rmtree(test_dir)
        missing_record_file = Path(
            f"{dataset_id}__{PROJECT_ID}__missing_dimension_record_combinations.csv"
        )
        if missing_record_file.exists():
            shutil.rmtree(missing_record_file)


def test_invalid_load_data_lookup_column_name(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    for item in data:
        item["invalid_dimension"] = item.pop("subsector")
    dump_line_delimited_json(data, lookup_file)
    expected_errors["exception"] = DSGInvalidDimension
    expected_errors["match_msg"] = r"column.*is not expected or of a known dimension type"


def test_invalid_load_data_lookup_integer_column(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
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
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"load_data_lookup does not include an .id. column"


def test_invalid_load_data_lookup_mismatched_records(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    dump_line_delimited_json(data[:1], lookup_file)
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"load_data_lookup records do not match dimension records"


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

    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"load_data timestamps do not match expected times"


def test_invalid_load_data_id_missing_timestamp(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    data_file = dataset_path / "load_data.csv"
    # Remove one row/timestamp for one load data array.
    text = "\n".join(data_file.read_text().splitlines()[:-1])
    data_file.write_text(text)
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors[
        "match_msg"
    ] = r"All time arrays must be repeated the same number of times: unique timestamp repeats =.*"


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
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"load_data timestamps do not match expected times"


def test_invalid_load_data_null_id(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    data_file = dataset_path / "load_data.csv"
    lines = data_file.read_text().splitlines()
    header = lines[0]
    assert header.split(",")[0] == "id"
    first_id = lines[1].split(",")[0]

    def make_matching_id_null(line_number, id_val):
        row = lines[line_number]
        fields = row.split(",")
        if fields[0] == id_val:
            fields[0] = ""
            lines[line_number] = ",".join(fields)

    for i in range(len(lines)):
        make_matching_id_null(i, first_id)

    data_file.write_text("\n".join(lines))
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"load_data .*has a null ID"


def test_invalid_load_data_lookup_mismatched_ids(register_dataset):
    _, dataset_path, expected_errors = register_dataset
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    data[0]["id"] += 999999999
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
    expected_errors["exception"] = DSGInvalidDataset
    expected_errors["match_msg"] = r"has a NULL value"


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


def test_invalid_load_data_lookup_missing_records(register_and_submit_dataset):
    _, dataset_path, expected_errors = register_and_submit_dataset
    lookup_file = dataset_path / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    bad_data = [x for x in data if x["id"] is not None]
    dump_line_delimited_json(bad_data, lookup_file)
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
    lookup_file = dataset_path / "load_data_lookup.json"
    good_data = load_line_delimited_json(lookup_file)
    bad_data = [x for x in good_data if x["id"] is not None]
    dump_line_delimited_json(bad_data, lookup_file)

    try:
        with pytest.raises(DSGInvalidDataset):
            manager.project_manager.register_and_submit_dataset(
                dataset_config_file,
                dataset_path,
                PROJECT_ID,
                getpass.getuser(),
                "register and submit",
                dimension_mapping_file=None,
                dimension_mapping_references_file=None,
                autogen_reverse_supplemental_mappings=None,
            )

        dump_line_delimited_json(good_data, lookup_file)
        manager.project_manager.register_and_submit_dataset(
            dataset_config_file,
            dataset_path,
            PROJECT_ID,
            getpass.getuser(),
            "register and submit",
            dimension_mapping_file=dim_mapping_file,
        )

    finally:
        shutil.rmtree(test_dir)
        missing_record_file = Path(
            f"{dataset_id}__{PROJECT_ID}__missing_dimension_record_combinations.csv"
        )
        if missing_record_file.exists():
            shutil.rmtree(missing_record_file)
