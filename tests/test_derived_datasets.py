import logging
import shutil
from collections import namedtuple
from pathlib import Path

import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.query.derived_dataset import (
    create_derived_dataset_config_from_query,
    does_query_support_a_derived_dataset,
)
from dsgrid.query.models import (
    ColumnType,
    DatasetModel,
    ProjectQueryDatasetParamsModel,
    ProjectQueryParamsModel,
    ProjectQueryModel,
    QueryResultParamsModel,
    ExponentialGrowthDatasetModel,
)
from dsgrid.query.query_submitter import QuerySubmitterBase
from dsgrid.registry.dataset_registry import DatasetRegistry
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.run_command import check_run_command
from dsgrid.utils.spark import read_dataframe


REGISTRY_PATH = (
    Path(__file__).absolute().parent.parent
    / "dsgrid-test-data"
    / "filtered_registries"
    / "simple_standard_scenarios"
)

RESSTOCK_PROJECTION_QUERY = Path("tests") / "data" / "resstock_conus_2022_projected.json5"

Datasets = namedtuple("Datasets", ["comstock", "resstock", "tempo"])

logger = logging.getLogger(__name__)


@pytest.fixture
def valid_query():
    yield ProjectQueryModel(
        name="resstock_conus_2022_projected",
        project=ProjectQueryParamsModel(
            project_id="dsgrid_conus_2022",
            include_dsgrid_dataset_components=False,
            dataset=DatasetModel(
                dataset_id="resstock_conus_2022_projected",
                source_datasets=[
                    ExponentialGrowthDatasetModel(
                        dataset_id="resstock_conus_2022_projected",
                        initial_value_dataset_id="resstock_conus_2022_reference",
                        growth_rate_dataset_id="aeo2021_reference_residential_energy_use_growth_factors",
                        construction_method="formula123",
                    ),
                ],
                params=ProjectQueryDatasetParamsModel(),
            ),
        ),
        result=QueryResultParamsModel(column_type=ColumnType.DIMENSION_TYPES),
    )


def test_resstock_projection_valid_query(valid_query):
    assert does_query_support_a_derived_dataset(valid_query)


def test_resstock_projection_invalid_query_column_type(valid_query):
    query = valid_query
    query.result.column_type = ColumnType.DIMENSION_QUERY_NAMES
    assert not does_query_support_a_derived_dataset(query)


def test_resstock_projection_invalid_query_supplemental_columns(valid_query):
    query = valid_query
    query.result.supplemental_columns = ["state"]
    assert not does_query_support_a_derived_dataset(query)


def test_resstock_projection_invalid_query_replace_ids_with_names(valid_query):
    query = valid_query
    query.result.replace_ids_with_names = True
    assert not does_query_support_a_derived_dataset(query)


def test_create_derived_dataset_config(tmp_path):

    registry_manager = RegistryManager.load(REGISTRY_PATH, offline_mode=True)
    project_id = "dsgrid_conus_2022"
    project = registry_manager.project_manager.load_project(project_id)

    dataset_id = "resstock_conus_2022_projected"
    query_output_base = tmp_path / "query_output"
    check_run_command(
        "dsgrid query project run --offline "
        f"--registry-path={REGISTRY_PATH} "
        f"{RESSTOCK_PROJECTION_QUERY} -o {query_output_base} --force"
    )
    query_output = query_output_base / dataset_id
    assert query_output.exists()
    table_file = QuerySubmitterBase.table_filename(query_output)
    assert table_file.exists()

    # Ensure that this derived dataset matches the one in the registry.
    orig_df = read_dataframe(REGISTRY_PATH / "data" / dataset_id / "1.0.0" / "table.parquet")
    new_df = read_dataframe(query_output / "table.parquet")
    assert sorted(new_df.columns) == sorted(orig_df.columns)

    # orig_df does not have time-wrapping, so need to load project_time to convert
    project_time_dim = project.config.get_base_dimension(DimensionType.TIME)
    orig_df = project_time_dim.convert_dataframe(orig_df, project_time_dim)
    assert new_df.sort(*orig_df.columns).collect() == orig_df.sort(*orig_df.columns).collect()

    # Create the config in the CLI and Python API to get test coverage in both places.
    dataset_dir = tmp_path / dataset_id
    dataset_config_file = dataset_dir / DatasetRegistry.config_filename()
    dataset_dir.mkdir()
    assert create_derived_dataset_config_from_query(query_output, dataset_dir, registry_manager)
    assert dataset_config_file.exists()

    check_run_command(
        f"dsgrid query project create-derived-dataset-config --offline "
        f"--registry-path={REGISTRY_PATH} {query_output} {dataset_dir} --force"
    )
    assert dataset_config_file.exists()
    shutil.rmtree(dataset_dir)
