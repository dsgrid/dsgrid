import copy
import getpass
import os
import shutil
import sys
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension
from dsgrid.tests.common import make_test_project_dir, make_test_data_dir, TEST_DATASET_DIRECTORY
from dsgrid.utils.files import dump_line_delimited_json, load_line_delimited_json
from dsgrid.tests.make_us_data_registry import make_test_data_registry, replace_dataset_path


def test_invalid_datasets(make_test_project_dir, make_test_data_dir):
    if "test_efs_comstock" not in os.listdir(make_test_data_dir):
        print("test_invalid_datasets requires the dsgrid-test-data repository")
        sys.exit(1)

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
        dimension_mapping_refs = dataset_dir / "dimension_mapping_references.toml"
        assert dimension_mapping_refs.exists()
        dataset_config_file = dataset_dir / "dataset.toml"

        user = getpass.getuser()
        log_message = "test log message"
        tests = (
            _setup_invalid_load_data_lookup_column_name,
            _setup_invalid_load_data_lookup_no_id,
            _setup_invalid_load_data_lookup_mismatched_records,
            _setup_invalid_load_data_missing_timestamp,
            _setup_invalid_load_data_id_missing_timestamp,
            _setup_invalid_load_data_id_extra_timestamp,
            _setup_invalid_load_data_lookup_mismatched_ids,
            _setup_invalid_load_data_extra_column,
            _setup_invalid_load_data_lookup_missing_dimension_combo,
        )
        # This is arranged in this way to avoid having to re-create the registry every time,
        # which is quite slow. There is one downside: if one test is able to register the
        # dataset (which would be a bug), later tests will fail even if they should pass.
        for i, setup_test in enumerate(tests):
            try:
                # Create a new directory because there are collisions with cached
                # Spark load_data_lookup dataframes.
                print(f"> test {i}...")
                test_dir = base_dir / f"test_data_dir_{i}"
                replace_dataset_path(dataset_config_file, dataset_path=test_dir)
                shutil.copytree(make_test_data_dir, test_dir)
                exc, match_msg = setup_test(test_dir)
                with pytest.raises(exc, match=match_msg):
                    manager.dataset_manager.register(dataset_config_file, user, log_message)
            finally:
                shutil.rmtree(test_dir)


def _setup_invalid_load_data_lookup_column_name(data_dir):
    lookup_file = data_dir / "test_efs_comstock" / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    for item in data:
        item["invalid_dimension"] = item.pop("subsector")
    dump_line_delimited_json(data, lookup_file)
    return DSGInvalidDimension, r"column.*is not expected or of a known dimension type"


def _setup_invalid_load_data_lookup_no_id(data_dir):
    lookup_file = data_dir / "test_efs_comstock" / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    for item in data:
        if "id" in item:
            item.pop("id")
    dump_line_delimited_json(data, lookup_file)
    return DSGInvalidDataset, r"load_data_lookup does not include an .id. column"


def _setup_invalid_load_data_lookup_mismatched_records(data_dir):
    lookup_file = data_dir / "test_efs_comstock" / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    dump_line_delimited_json(data[:1], lookup_file)
    return DSGInvalidDataset, r"load_data_lookup records do not match dimension records"


def _setup_invalid_load_data_missing_timestamp(data_dir):
    data_file = data_dir / "test_efs_comstock" / "load_data.csv"
    # Remove one row/timestamp from all load data arrays.
    timestamp = "2012-01-01T10:00:00"
    lines = data_file.read_text().splitlines()
    with open(data_file, "w") as f_out:
        for line in lines:
            if timestamp not in line:
                f_out.write(line)
                f_out.write("\n")

    return DSGInvalidDataset, r"load_data timestamps do not match expected times"


def _setup_invalid_load_data_id_missing_timestamp(data_dir):
    data_file = data_dir / "test_efs_comstock" / "load_data.csv"
    # Remove one row/timestamp for one load data array.
    text = "\n".join(data_file.read_text().splitlines()[:-1])
    data_file.write_text(text)
    return DSGInvalidDataset, r"One or more arrays do not have.*timestamps"


def _setup_invalid_load_data_id_extra_timestamp(data_dir):
    data_file = data_dir / "test_efs_comstock" / "load_data.csv"
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
    return DSGInvalidDataset, r"load_data timestamps do not match expected times"


def _setup_invalid_load_data_null_id(data_dir):
    data_file = data_dir / "test_efs_comstock" / "load_data.csv"
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
    return DSGInvalidDataset, r"load_data .*has a null ID"


def _setup_invalid_load_data_lookup_mismatched_ids(data_dir):
    lookup_file = data_dir / "test_efs_comstock" / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    data[0]["id"] += 999999999
    dump_line_delimited_json(data, lookup_file)
    return DSGInvalidDataset, r"Data IDs for .*data.lookup are inconsistent"


def _setup_invalid_load_data_extra_column(data_dir):
    data_file = data_dir / "test_efs_comstock" / "load_data.csv"
    lines = data_file.read_text().splitlines()
    with open(data_file, "w") as f_out:
        f_out.write(lines[0])
        f_out.write(",extra\n")
        for line in lines[1:]:
            f_out.write(line)
            f_out.write(",0\n")

    return DSGInvalidDataset, r"column.*is not expected in load_data"


def _setup_invalid_load_data_lookup_missing_dimension_combo(data_dir):
    lookup_file = data_dir / "test_efs_comstock" / "load_data_lookup.json"
    data = load_line_delimited_json(lookup_file)
    dump_line_delimited_json(data[:-1], lookup_file)
    return (
        DSGInvalidDataset,
        r"load_data_lookup records do not match dimension records for dimension combinations",
    )
