import logging
import shutil
from collections import namedtuple
from pathlib import Path

import pytest
from click.testing import CliRunner
from pandas.testing import assert_frame_equal

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
    ProjectionDatasetModel,
    DatasetConstructionMethod,
)
from dsgrid.query.query_submitter import QuerySubmitterBase
from dsgrid.registry.common import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB
from dsgrid.ibis.io import read_dataframe
from dsgrid.ibis.table_utils import table_to_pandas

from tests._helpers import order_by as _order_by


REGISTRY_PATH = (
    Path(__file__).absolute().parents[1]
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
                    ProjectionDatasetModel(
                        dataset_id="resstock_conus_2022_projected",
                        initial_value_dataset_id="resstock_conus_2022_reference",
                        growth_rate_dataset_id="aeo2021_reference_residential_energy_use_growth_factors",
                        construction_method=DatasetConstructionMethod.EXPONENTIAL_GROWTH,
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
    query.result.column_type = ColumnType.DIMENSION_NAMES
    assert not does_query_support_a_derived_dataset(query)


def test_resstock_projection_invalid_query_replace_ids_with_names(valid_query):
    query = valid_query
    query.result.replace_ids_with_names = True
    assert not does_query_support_a_derived_dataset(query)


def test_create_derived_dataset_config(tmp_path):
    conn = DatabaseConnection(url=SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB)
    dataset_id = "resstock_conus_2022_projected"
    query_output_base = tmp_path / "query_output"
    runner = CliRunner()
    cmd = [
        "--url",
        conn.url,
        "query",
        "project",
        "run",
        str(RESSTOCK_PROJECTION_QUERY),
        "-o",
        str(query_output_base),
        "--overwrite",
    ]
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    query_output = query_output_base / dataset_id
    assert query_output.exists()
    table_file = QuerySubmitterBase.table_filename(query_output)
    assert table_file.exists()

    # Ensure that this derived dataset matches the one in the registry.
    orig_df = read_dataframe(REGISTRY_PATH / "data" / dataset_id / "1.0.0" / "table.parquet")
    new_df = read_dataframe(query_output / "table.parquet")
    assert sorted(new_df.columns) == sorted(orig_df.columns)
    orig_data = table_to_pandas(_order_by(orig_df, *orig_df.columns))
    new_data = table_to_pandas(_order_by(new_df.select(*orig_df.columns), *orig_df.columns))
    # Compare the value column with a relative tolerance rather than exact
    # equality: unit conversion multiplies/divides in float64, so results differ
    # from the stored golden values by up to one ULP (~2e-16) depending on the
    # backend's arithmetic order. Key columns are non-numeric and still match
    # exactly. atol=0 keeps the check purely relative.
    assert_frame_equal(new_data, orig_data, rtol=1e-15, atol=0)

    # Create the config in the CLI and Python API to get test coverage in both places.
    dataset_dir = tmp_path / dataset_id
    dataset_config_file = dataset_dir / DatasetConfig.config_filename()

    with RegistryManager.load(conn, offline_mode=True) as registry_manager:
        dataset_dir.mkdir()
        assert create_derived_dataset_config_from_query(
            query_output, dataset_dir, registry_manager
        )
    assert dataset_config_file.exists()

    result = runner.invoke(
        cli,
        [
            "--url",
            conn.url,
            "query",
            "project",
            "create-derived-dataset-config",
            str(query_output),
            str(dataset_dir),
            "--overwrite",
        ],
    )
    assert result.exit_code == 0
    assert dataset_config_file.exists()
    shutil.rmtree(dataset_dir)
