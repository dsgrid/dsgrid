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
    create_local_test_registry,
)
from dsgrid.config.simple_models import RegistrySimpleModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension
from dsgrid.registry.common import DatabaseConnection
from dsgrid.registry.filter_registry_manager import FilterRegistryManager
from dsgrid.registry.registry_database import RegistryDatabase
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.files import load_json_file, dump_json_file
from dsgrid.utils.spark import get_unique_values


logger = logging.getLogger()

# set up make_test_project_dir
TEST_PROJECT_REPO = TEST_PROJECT_PATH / "test_aeo"


@pytest.fixture(scope="module")
def make_test_project_dir(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp("registry")
    url = f"sqlite:///{base_dir}/registry.db"
    conn = DatabaseConnection(url=url)
    RegistryDatabase.delete(conn)
    tmpdir = _make_project_dir(TEST_PROJECT_REPO)
    src_dir = tmpdir / "dsgrid_project"
    registry_dir = tmp_path_factory.mktemp("registry")
    yield src_dir, registry_dir, conn
    shutil.rmtree(tmpdir)
    RegistryDatabase.delete(conn)


def _make_project_dir(project):
    tmpdir = Path(gettempdir()) / "test_project"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    shutil.copytree(project / "dsgrid_project", tmpdir / "dsgrid_project")
    return tmpdir


# set up registry for AEO data
def make_registry_for_aeo(
    src_dir, registry_path, conn, dataset_name: str, data_dir: Path
) -> RegistryManager:
    """Creates a local registry to test registration of AEO dimensions and dataset.

    Parameters
    ----------
    registry_path : Path
        Path in which the registry will be created.
    src_dir : Path
        Path containing source config files
    dataset_name : str
        Name of the dataset to register.
    data_dir : Path
        Path to the data files for this dataset.

    """

    create_local_test_registry(registry_path, conn=conn)
    dataset_dir = Path(f"datasets/benchmark/{dataset_name}")
    user = getpass.getuser()
    log_message = "Initial registration"
    with RegistryManager.load(conn, offline_mode=True) as manager:
        dataset_config_file = src_dir / dataset_dir / "dataset.json5"
        # Update the data file path to point to the actual data location
        config = load_json_file(dataset_config_file)
        config["table_schema"]["data_file"]["path"] = str(data_dir / "load_data.csv")
        dump_json_file(config, dataset_config_file)
        manager.dataset_manager.register(dataset_config_file, user, log_message)
        logger.info(f"dataset={dataset_name} registered successfully!\n")
    return manager


# set up tests for AEO data
def test_aeo_datasets_registration(make_test_project_dir, make_test_data_dir_module):
    if "test_aeo_data" not in os.listdir(make_test_data_dir_module):
        logger.info("test_invalid_datasets requires the dsgrid-test-data repository")
        sys.exit(1)

    src_dir, registry_dir, conn = make_test_project_dir
    datasets = os.listdir(make_test_data_dir_module / "test_aeo_data")
    for i, dataset in enumerate(datasets, 1):
        logger.info(f">> Registering: {i}. {dataset}...")
        data_dir = make_test_data_dir_module / "test_aeo_data" / dataset

        shutil.copyfile(data_dir / "load_data.csv", data_dir / "load_data_original.csv")

        try:
            logger.info("1. normal registration: ")
            _test_dataset_registration(src_dir, registry_dir, conn, data_dir, dataset)

            logger.info("2. with unexpected col: ")
            _modify_data_file(data_dir, export_index=True)
            with pytest.raises(DSGInvalidDataset, match=r"column.*is not expected"):
                _test_dataset_registration(src_dir, registry_dir, conn, data_dir, dataset)

            logger.info("3. with a duplicated dimension: ")
            _modify_data_file(data_dir, duplicate_col="subsector")
            # This is not really a valuable test any more.
            # Spark doesn't allow duplicate columns in CSV files. Maybe previous versions did.
            # It appends the column index to each duplicate column name.
            # DuckDB appends "_1" suffix to the duplicate, and our code catches that as unexpected.
            with pytest.raises((ValueError, DSGInvalidDataset, DSGInvalidDimension)):
                _test_dataset_registration(src_dir, registry_dir, conn, data_dir, dataset)

            logger.info("4. End Uses dataset only - missing time ")
            if "End_Uses" in dataset:
                _modify_data_file(data_dir, drop_first_row=True)
                with pytest.raises(
                    DSGInvalidDataset,
                    match=r"All time arrays must be repeated the same number of times: unique timestamp repeats =.*",
                ):
                    _test_dataset_registration(src_dir, registry_dir, conn, data_dir, dataset)
        finally:
            RegistryDatabase.delete(conn)
            shutil.copyfile(data_dir / "load_data_original.csv", data_dir / "load_data.csv")


def _test_dataset_registration(src_dir, registry_dir, conn, data_dir, dataset):
    logger.info(f"temp_registry created: {registry_dir}")
    return make_registry_for_aeo(
        src_dir,
        registry_dir,
        conn,
        dataset,
        data_dir,
    )


def test_filter_aeo_dataset(make_test_project_dir, make_test_data_dir_module):
    src_dir, _, conn = make_test_project_dir
    dataset_id = "aeo2021_ref_com_energy_end_use_growth_factors"
    geography_record = "mountain"
    filter_config = {
        "projects": [],
        "datasets": [
            {
                "dataset_id": dataset_id,
                "dimensions": [{"dimension_type": "geography", "record_ids": [geography_record]}],
            }
        ],
    }
    simple_model = RegistrySimpleModel(**filter_config)
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        dataset = "Commercial_End_Use_Growth_Factors"
        data_dir = make_test_data_dir_module / "test_aeo_data" / dataset
        make_registry_for_aeo(
            src_dir,
            base_dir,
            conn,
            dataset,
            data_dir,
        )
        try:
            with FilterRegistryManager.load(conn, offline_mode=True) as filter_mgr:
                filter_mgr.filter(simple_model)
            with RegistryManager.load(conn, offline_mode=True) as mgr:
                config = mgr.dataset_manager.get_by_id(dataset_id)
                geo = config.get_dimension(DimensionType.GEOGRAPHY).get_records_dataframe()
                assert get_unique_values(geo, "id") == {geography_record}
        finally:
            RegistryDatabase.delete(conn)


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
