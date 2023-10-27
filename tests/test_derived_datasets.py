import logging
import shutil
from collections import namedtuple
from pathlib import Path

import pytest
from click.testing import CliRunner

from dsgrid.common import DEFAULT_DB_PASSWORD
from dsgrid.cli.dsgrid import cli
from dsgrid.config.dataset_config import DatasetConfig
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
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
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
    query.result.column_type = ColumnType.DIMENSION_QUERY_NAMES
    query.result.supplemental_columns = ["state"]
    assert not does_query_support_a_derived_dataset(query)


def test_resstock_projection_invalid_query_replace_ids_with_names(valid_query):
    query = valid_query
    query.result.replace_ids_with_names = True
    assert not does_query_support_a_derived_dataset(query)


def test_create_derived_dataset_config(tmp_path):
    dataset_id = "resstock_conus_2022_projected"
    query_output_base = tmp_path / "query_output"
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "--username",
            "root",
            "--password",
            DEFAULT_DB_PASSWORD,
            "--offline",
            "--database-name",
            "simple-standard-scenarios",
            "query",
            "project",
            "run",
            str(RESSTOCK_PROJECTION_QUERY),
            "-o",
            str(query_output_base),
            "--force",
        ],
    )
    assert result.exit_code == 0
    query_output = query_output_base / dataset_id
    assert query_output.exists()
    table_file = QuerySubmitterBase.table_filename(query_output)
    assert table_file.exists()

    # Ensure that this derived dataset matches the one in the registry.
    orig_df = read_dataframe(REGISTRY_PATH / "data" / dataset_id / "1.0.0" / "table.parquet")
    new_df = read_dataframe(query_output / "table.parquet")
    assert sorted(new_df.columns) == sorted(orig_df.columns)
    assert new_df.sort(*orig_df.columns).collect() == orig_df.sort(*orig_df.columns).collect()

    # Create the config in the CLI and Python API to get test coverage in both places.
    dataset_dir = tmp_path / dataset_id
    dataset_config_file = dataset_dir / DatasetConfig.config_filename()

    conn = DatabaseConnection(database="simple-standard-scenarios")
    registry_manager = RegistryManager.load(conn, offline_mode=True)
    dataset_dir.mkdir()
    assert create_derived_dataset_config_from_query(query_output, dataset_dir, registry_manager)
    assert dataset_config_file.exists()

    result = runner.invoke(
        cli,
        [
            "--username",
            "root",
            "--password",
            DEFAULT_DB_PASSWORD,
            "--offline",
            "--database-name",
            "simple-standard-scenarios",
            "query",
            "project",
            "create-derived-dataset-config",
            str(query_output),
            str(dataset_dir),
            "--force",
        ],
    )
    assert result.exit_code == 0
    assert dataset_config_file.exists()
    shutil.rmtree(dataset_dir)
