import logging
import shutil
from collections import namedtuple
from pathlib import Path

import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.dimension_filters import (
    DimensionFilterExpressionModel,
)
from dsgrid.query.derived_dataset import does_query_support_a_derived_dataset
from dsgrid.query.models import (
    DatasetModel,
    ProjectQueryDatasetParamsModel,
    ProjectQueryParamsModel,
    ProjectQueryModel,
    QueryResultParamsModel,
    ExponentialGrowthDatasetModel,
)
from dsgrid.query.query_submitter import QuerySubmitterBase
from dsgrid.registry.dataset_registry import DatasetRegistry
from dsgrid.utils.run_command import check_run_command


REGISTRY_PATH = (
    Path(__file__).absolute().parent.parent
    / "dsgrid-test-data"
    / "filtered_registries"
    / "simple_standard_scenarios"
)

RESSTOCK_PROJECTION_QUERY = Path("tests") / "data" / "resstock_projected_conus_2022.json5"

Datasets = namedtuple("Datasets", ["comstock", "resstock", "tempo"])

logger = logging.getLogger(__name__)


@pytest.fixture
def valid_query():
    yield ProjectQueryModel(
        name="resstock_projected_conus_2022",
        project=ProjectQueryParamsModel(
            project_id="dsgrid_conus_2022",
            include_dsgrid_dataset_components=False,
            dataset=DatasetModel(
                dataset_id="resstock_projected_conus_2022",
                source_datasets=[
                    ExponentialGrowthDatasetModel(
                        dataset_id="resstock_projected_conus_2022",
                        initial_value_dataset_id="resstock_conus_2022_reference",
                        growth_rate_dataset_id="aeo2021_reference_residential_energy_use_growth_factors",
                        construction_method="formula123",
                    ),
                ],
                params=ProjectQueryDatasetParamsModel(),
            ),
        ),
        result=QueryResultParamsModel(),
    )


def test_resstock_projection_valid_query(valid_query):
    assert does_query_support_a_derived_dataset(valid_query)


def test_resstock_projection_invalid_query_filtered_dataset(valid_query):
    query = valid_query
    query.project.dataset.params = ProjectQueryDatasetParamsModel(
        dimension_filters=[
            DimensionFilterExpressionModel(
                dimension_type=DimensionType.GEOGRAPHY,
                dimension_query_name="county",
                operator="==",
                value="06037",
            ),
        ],
    )
    assert not does_query_support_a_derived_dataset(query)


def test_resstock_projection_invalid_query_filtered_result(valid_query):
    query = valid_query
    query.result.dimension_filters = [
        DimensionFilterExpressionModel(
            dimension_type=DimensionType.GEOGRAPHY,
            dimension_query_name="county",
            operator="==",
            value="06037",
        ),
    ]
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
    dataset_id = "resstock_projected_conus_2022"
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

    dataset_dir = tmp_path / dataset_id
    check_run_command(
        f"dsgrid query project create-derived-dataset-config --offline "
        f"--registry-path={REGISTRY_PATH} {query_output} {dataset_dir} --force"
    )
    dataset_config_file = dataset_dir / DatasetRegistry.config_filename()
    assert dataset_config_file.exists()

    tmp_registry = tmp_path / "registry"
    shutil.copytree(REGISTRY_PATH, tmp_registry)
    check_run_command(
        f"dsgrid registry --offline --path {tmp_registry} projects register-and-submit-dataset "
        f"-c {dataset_config_file} -p dsgrid_conus_2022 -l 'Submit resstock projection' "
        f"-d {query_output}"
    )
    # TODO: load dataset and check values
