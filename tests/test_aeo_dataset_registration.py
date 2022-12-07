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
from dsgrid.registry.filter_registry_manager import FilterRegistryManager
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension
from dsgrid.utils.spark import get_unique_values


logger = logging.getLogger()

# set up make_test_project_dir
TEST_PROJECT_REPO = TEST_PROJECT_PATH / "test_aeo"


@pytest.fixture(scope="module")
def make_test_project_dir():
    tmpdir = _make_project_dir(TEST_PROJECT_REPO)
    yield tmpdir / "dsgrid_project"
    shutil.rmtree(tmpdir)

    # These can cause failures in later tests.
    for name in ("spark-warehouse", "metastore_db"):
        path = Path(name)
        if path.exists():
            shutil.rmtree(path)


def _make_project_dir(project):
    tmpdir = Path(gettempdir()) / "test_project"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    shutil.copytree(project / "dsgrid_project", tmpdir / "dsgrid_project")
    return tmpdir


# set up registry for AEO data
def make_registry_for_aeo(
    registry_path, src_dir, dataset_name: str, dataset_path=None
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

    dataset_config_file = src_dir / dataset_dir / "dataset.json5"
    manager.dataset_manager.register(dataset_config_file, dataset_path, user, log_message)
    logger.info(f"dataset={dataset_name} registered successfully!\n")
    return manager


# set up tests for AEO data
def test_aeo_datasets_registration(make_test_project_dir, make_test_data_dir_module):
    if "test_aeo_data" not in os.listdir(make_test_data_dir_module):
        logger.info("test_invalid_datasets requires the dsgrid-test-data repository")
        sys.exit(1)

    datasets = os.listdir(make_test_data_dir_module / "test_aeo_data")
    for i, dataset in enumerate(datasets, 1):
        logger.info(f">> Registering: {i}. {dataset}...")
        data_dir = make_test_data_dir_module / "test_aeo_data" / dataset

        shutil.copyfile(data_dir / "load_data.csv", data_dir / "load_data_original.csv")

        try:
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
                    DSGInvalidDataset,
                    match=r"All time arrays must be repeated the same number of times: unique timestamp repeats =.*",
                ):
                    _test_dataset_registration(make_test_project_dir, data_dir, dataset)
        finally:
            shutil.copyfile(data_dir / "load_data_original.csv", data_dir / "load_data.csv")


def _test_dataset_registration(make_test_project_dir, data_dir, dataset):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        logger.info(f"temp_registry created: {base_dir}")
        return make_registry_for_aeo(
            base_dir,
            make_test_project_dir,
            dataset,
            dataset_path=data_dir,
        )


def test_filter_aeo_dataset(make_test_project_dir, make_test_data_dir_module):
    dataset_id = "aeo2021_ref_com_energy_end_use_growth_factors"
    geography_record = "mountain"
    filter_config = {
        "name": "test-registry",
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
        data_dir = Path(make_test_data_dir_module) / "test_aeo_data" / dataset
        make_registry_for_aeo(
            base_dir,
            make_test_project_dir,
            dataset,
            dataset_path=data_dir,
        )
        FilterRegistryManager.load(base_dir, offline_mode=True).filter(simple_model)
        mgr = RegistryManager.load(base_dir, offline_mode=True)
        config = mgr.dataset_manager.get_by_id(dataset_id)
        geo = config.get_dimension(DimensionType.GEOGRAPHY).get_records_dataframe()
        assert get_unique_values(geo, "id") == {geography_record}


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
