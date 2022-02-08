import getpass
import logging
import os
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir

import pandas as pd
import pytest

from dsgrid.tests.common import (
    TEST_PROJECT_PATH,
    make_test_data_dir,
    create_local_test_registry,
    replace_dimension_uuids_from_registry,
)
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.files import load_data, dump_data
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension


logger = logging.getLogger()

### set up make_test_project_dir ###
TEST_PROJECT_REPO = TEST_PROJECT_PATH / "test_aeo"


@pytest.fixture
def make_test_project_dir():
    tmpdir = _make_project_dir(TEST_PROJECT_REPO)
    yield tmpdir / "dsgrid_project"
    shutil.rmtree(tmpdir)


def _make_project_dir(project):
    tmpdir = Path(gettempdir()) / "test_project"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    shutil.copytree(project / "dsgrid_project", tmpdir / "dsgrid_project")
    return tmpdir


### set up registry for AEO data ###
def make_registry_for_aeo(
    registry_path, src_dir, dataset_name: str, dataset_path=None, include_datasets=False
) -> RegistryManager:
    """Creates a local registry to test registration of AEO dimensions and dataset.

    Parameters
    ----------
    registry_path : Path
        Path in which the registry will be created.
    src_dir : Path
        Path containing source config files
    dataset_path : Path | None
        If None, use "DSGRID_LOCAL_DATA_DIRECTORY" env variable.

    """
    if dataset_path is None:
        dataset_path = os.environ["DSGRID_LOCAL_DATA_DIRECTORY"]
    path = create_local_test_registry(registry_path)
    dataset_dir = Path(f"datasets/benchmark/{dataset_name}")
    user = getpass.getuser()
    log_message = "Initial registration"
    manager = RegistryManager.load(path, offline_mode=True)
    dim_mgr = manager.dimension_manager
    dim_mgr.register(src_dir / dataset_dir / "dimensions.toml", user, log_message)

    dataset_config_file = src_dir / dataset_dir / "dataset.toml"
    dataset_id = load_data(dataset_config_file)["dataset_id"]
    replace_dataset_path(dataset_config_file, dataset_path=dataset_path)
    replace_dimension_uuids_from_registry(path, (dataset_config_file,))

    manager.dataset_manager.register(dataset_config_file, user, log_message)
    logger.info(f"dataset={dataset_name} registered successfully!\n")
    return manager


def replace_dataset_path(dataset_config_file, dataset_path):
    config = load_data(dataset_config_file)
    src_data = Path(dataset_path)
    config["path"] = str(src_data)
    dump_data(config, dataset_config_file)
    logger.info("Replaced dataset path in %s with %s", dataset_config_file, src_data)


### set up tests for AEO data ###
def test_aeo_datasets_registration(make_test_project_dir, make_test_data_dir):
    if "test_aeo_data" not in os.listdir(make_test_data_dir):
        logger.info("test_invalid_datasets requires the dsgrid-test-data repository")
        sys.exit(1)

    datasets = os.listdir(make_test_data_dir / "test_aeo_data")
    for i, dataset in enumerate(datasets, 1):
        logger.info(f">> Registering: {i}. {dataset}...")
        data_dir = make_test_data_dir / "test_aeo_data" / dataset

        shutil.copyfile(data_dir / "load_data.csv", data_dir / "load_data_original.csv")

        logger.info("1. normal registration: ")
        _test_dataset_registration(make_test_project_dir, data_dir, dataset)

        logger.info("2. with unexpected col: ")
        _modify_data_file(data_dir, export_index=True)
        with pytest.raises(
            DSGInvalidDimension, match=r"column.*is not expected or of a known dimension type"
        ):
            _test_dataset_registration(make_test_project_dir, data_dir, dataset)

        logger.info("3. with a duplicated dimension: ")
        _modify_data_file(data_dir, duplicate_col="subsector")
        with pytest.raises((ValueError, DSGInvalidDimension)):
            # (ValueError,  match=r"*is not a valid DimensionType"),
            # (DSGInvalidDimension,  match=r"column.*is not expected or of a known dimension type")
            _test_dataset_registration(make_test_project_dir, data_dir, dataset)

        logger.info("4. with a duplicated pivot col: ")
        _modify_data_file(data_dir, duplicate_col="elec_heating")
        with pytest.raises(
            DSGInvalidDimension, match=r"column.*is not expected or of a known dimension type"
        ):
            _test_dataset_registration(make_test_project_dir, data_dir, dataset)

        logger.info("5. End Uses dataset only - missing time ")
        if "End_Uses" in dataset:
            _modify_data_file(data_dir, drop_first_row=True)
            with pytest.raises(
                DSGInvalidDataset, match=r"One or more arrays do not have.*timestamps"
            ):
                _test_dataset_registration(make_test_project_dir, data_dir, dataset)


def _test_dataset_registration(make_test_project_dir, data_dir, dataset):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        logger.info(f"temp_registry created: {base_dir}")
        manager = make_registry_for_aeo(
            base_dir,
            make_test_project_dir,
            dataset,
            dataset_path=data_dir,
            include_datasets=False,
        )


def _modify_data_file(
    data_dir,
    duplicate_col=None,
    drop_first_row=False,
    remove_geography_subsector=None,
    export_index=False,
):
    df_data = pd.read_csv(data_dir / "load_data_original.csv")

    if duplicate_col is not None:
        df_data[f"{duplicate_col}_dup"] = df_data[duplicate_col]
        df_data = df_data.rename(columns={f"{duplicate_col}_dup": duplicate_col})
    if remove_geography_subsector is not None:
        (geography, subsector) = remove_geography_subsector
        to_drop = df_data[
            (df_data["geography"] == geography) & (df_data["subsector"] == subsector)
        ].index
        df_data = df_data.drop(to_drop).reset_index(drop=True)
    if drop_first_row:
        df_data = df_data.iloc[1:]
    if export_index:
        df_data.reset_index(inplace=True)
    logger.info(df_data)
    df_data.to_csv(data_dir / "load_data.csv", index=False)
