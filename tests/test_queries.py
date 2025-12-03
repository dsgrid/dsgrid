import abc
import copy
import logging
import math
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from click.testing import CliRunner
from pandas.testing import assert_frame_equal
import pandas as pd

import dsgrid
from dsgrid.common import VALUE_COLUMN, BackendEngine
from dsgrid.cli.dsgrid import cli
from dsgrid.dataset.models import (
    PivotedTableFormatModel,
    UnpivotedTableFormatModel,
)
from dsgrid.dimension.base_models import DimensionType, DimensionCategory
from dsgrid.dimension.dimension_filters import (
    DimensionFilterExpressionModel,
    DimensionFilterColumnOperatorModel,
    SubsetDimensionFilterModel,
    SupplementalDimensionFilterColumnOperatorModel,
)
from dsgrid.exceptions import DSGInvalidQuery
from dsgrid.loggers import setup_logging
from dsgrid.project import Project
from dsgrid.query.dataset_mapping_plan import MapOperation
from dsgrid.query.models import (
    AggregationModel,
    ColumnModel,
    ColumnType,
    CompositeDatasetQueryModel,
    CreateCompositeDatasetQueryModel,
    DatasetModel,
    DimensionNamesModel,
    ProjectQueryDatasetParamsModel,
    ProjectQueryParamsModel,
    ProjectQueryModel,
    QueryResultParamsModel,
    ReportInputModel,
    ReportType,
    ProjectionDatasetModel,
    StandaloneDatasetModel,
    DatasetConstructionMethod,
)
from dsgrid.query.dataset_mapping_plan import (
    DatasetMappingPlan,
    MapOperationCheckpoint,
)
from dsgrid.query.query_submitter import ProjectQuerySubmitter, CompositeDatasetQuerySubmitter
from dsgrid.query.report_peak_load import PeakLoadInputModel, PeakLoadReport
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.spark.functions import (
    aggregate_single_value,
    read_csv,
)
from dsgrid.spark.types import (
    DataFrame,
    DoubleType,
    F,
    SparkSession,
    StructField,
    StringType,
    StructType,
    use_duckdb,
)
from dsgrid.tests.common import (
    CACHED_TEST_REGISTRY_DB,
    SIMPLE_STANDARD_SCENARIOS,
    SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB,
    TEST_PROJECT_PATH,
)
from dsgrid.tests.utils import read_parquet
from dsgrid.utils.files import load_data, dump_data
from dsgrid.utils.spark import custom_time_zone
from .simple_standard_scenarios_datasets import REGISTRY_PATH, load_dataset_stats
from dsgrid.dimension.time import TimeZone


DIMENSION_MAPPING_SCHEMA = StructType(
    [
        StructField("from_id", StringType(), False),
        StructField("to_id", StringType()),
        StructField("from_fraction", DoubleType()),
    ]
)


logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def la_expected_electricity_hour_16(tmp_path_factory):
    output_dir = tmp_path_factory.mktemp("diurnal_queries")
    project = get_project(
        DatabaseConnection(url=SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB),
        "dsgrid_conus_2022",
    )
    query = ProjectQueryModel(
        name="projected_dg_conus_2022",
        project=ProjectQueryParamsModel(
            project_id="dsgrid_conus_2022",
            include_dsgrid_dataset_components=False,
            dataset=DatasetModel(
                dataset_id="projected_dg_conus_2022",
                source_datasets=[
                    StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                    StandaloneDatasetModel(dataset_id="resstock_conus_2022_projected"),
                ],
            ),
        ),
        result=QueryResultParamsModel(),
    )
    ProjectQuerySubmitter(project, output_dir).submit(
        query,
        persist_intermediate_table=False,
        load_cached_table=False,
    )
    df = read_parquet(str(output_dir / query.name / "table.parquet")).filter("county == '06037'")
    end_uses = ["electricity_cooling", "electricity_heating"]
    gcols = [x for x in df.columns if x not in {"end_use", "value"}]
    df = (
        df.filter(F.col("end_use").isin(end_uses))
        .groupBy(*gcols)
        .agg(F.sum(VALUE_COLUMN).alias(VALUE_COLUMN))
    )
    tz = project.config.get_base_dimension(DimensionType.TIME).get_time_zone()
    with custom_time_zone(tz.tz_name):
        expected = (
            df.groupBy("county", F.hour("time_est").alias("hour"))
            .agg(F.mean(VALUE_COLUMN).alias(VALUE_COLUMN))
            .filter("hour == 16")
            .collect()[0][VALUE_COLUMN]
        )
    yield {
        "la_electricity_hour_16": expected,
    }


@pytest.mark.parametrize("category", list(DimensionCategory))
@pytest.mark.parametrize("to_time_zone", [None, TimeZone.PPT, "geography"])
def test_electricity_values(category, to_time_zone):
    run_query_test(QueryTestElectricityValues, category, to_time_zone=to_time_zone)


@pytest.mark.parametrize(
    "inputs",
    [
        ("county", "sum"),
        ("county", "max"),
        ("state", "sum"),
        ("state", "max"),
    ],
)
def test_electricity_use(inputs):
    geo, op = inputs
    run_query_test(QueryTestElectricityUse, geo, op)


@pytest.mark.parametrize(
    "inputs",
    [
        ("county", "sum", DimensionCategory.BASE),
        ("county", "sum", DimensionCategory.SUBSET),
    ],
)
def test_electricity_use_with_results_filter(inputs):
    geo, op, category = inputs
    run_query_test(QueryTestElectricityUseFilterResults, geo, op, category)


def test_total_electricity_use_with_filter():
    run_query_test(QueryTestTotalElectricityUseWithFilter)


@pytest.mark.parametrize(
    "column_inputs",
    [
        (ColumnType.DIMENSION_NAMES, ["state", "reeds_pca", "census_region"], True, True),
        (ColumnType.DIMENSION_TYPES, ["reeds_pca"], False, True),
        (ColumnType.DIMENSION_TYPES, ["state"], False, True),
        (ColumnType.DIMENSION_TYPES, ["state", "reeds_pca", "census_region"], True, False),
    ],
)
def test_total_electricity_use_by_state_and_pca(column_inputs):
    column_type, columns, aggregate_each_dataset, is_valid = column_inputs
    if is_valid:
        run_query_test(
            QueryTestElectricityUseByStateAndPCA, column_type, columns, aggregate_each_dataset
        )
    else:
        with pytest.raises(ValueError):
            run_query_test(
                QueryTestElectricityUseByStateAndPCA, column_type, columns, aggregate_each_dataset
            )


def test_annual_electricity_by_state():
    run_query_test(QueryTestAnnualElectricityUseByState)


def test_diurnal_electricity_use_by_county_chained(la_expected_electricity_hour_16):
    run_query_test(
        QueryTestDiurnalElectricityUseByCountyChained,
        expected_values=la_expected_electricity_hour_16,
    )


def test_peak_load():
    run_query_test(QueryTestPeakLoadByStateSubsector)


def test_map_annual_time():
    run_query_test(QueryTestMapAnnualTime)


def test_unit_mapping(cached_registry):
    run_query_test(QueryTestUnitMapping)


@pytest.mark.parametrize(
    "inputs",
    [
        ("True", "True"),
        ("True", "False"),
        ("False", "True"),
        ("False", "False"),
    ],
)
def test_dataset_mapping_plan_order(inputs):
    handle_data_skew, persist = inputs
    run_query_test(QueryTestDatasetMappingPlan, handle_data_skew, persist)


def test_invalid_drop_metric_dimension():
    with pytest.raises(ValueError):
        AggregationModel(
            dimensions=DimensionNamesModel(
                geography=["county"],
                metric=[],
                model_year=["model_year"],
                scenario=["scenario"],
                sector=["sector"],
                subsector=["subsector"],
                time=["time_est"],
                weather_year=["weather_2012"],
            ),
            aggregation_function="sum",
        )


def test_invalid_pivoted_dimension_aggregations():
    with pytest.raises(ValueError):
        QueryResultParamsModel(
            aggregations=[
                AggregationModel(
                    dimensions=DimensionNamesModel(
                        geography=["county"],
                        metric=["electricity_end_uses", "natural_gas_end_uses"],
                        model_year=["model_year"],
                        scenario=["scenario"],
                        sector=["sector"],
                        subsector=["subsector"],
                        time=["time_est"],
                        weather_year=["weather_2012"],
                    ),
                    aggregation_function="sum",
                ),
            ],
            table_format=PivotedTableFormatModel(pivoted_dimension_type=DimensionType.METRIC),
        )

    with pytest.raises(ValueError):
        QueryResultParamsModel(
            aggregations=[
                AggregationModel(
                    dimensions=DimensionNamesModel(
                        geography=["county"],
                        metric=["end_uses_by_fuel_type"],
                        model_year=["model_year"],
                        scenario=["scenario"],
                        sector=["sector"],
                        subsector=["subsector"],
                        time=["time_est"],
                        weather_year=["weather_2012"],
                    ),
                    aggregation_function="sum",
                ),
                AggregationModel(
                    dimensions=DimensionNamesModel(
                        geography=["county"],
                        metric=[],
                        model_year=[],
                        scenario=[],
                        sector=[],
                        subsector=[],
                        time=[
                            ColumnModel(dimension_name="time_est", function="hour", alias="hour")
                        ],
                        weather_year=[],
                    ),
                    aggregation_function="mean",
                ),
            ],
            table_format=PivotedTableFormatModel(pivoted_dimension_type=DimensionType.METRIC),
        )


def test_invalid_aggregation_subset_dimension():
    with pytest.raises(DSGInvalidQuery):
        run_query_test(QueryTestInvalidAggregation)


def test_create_composite_dataset_query(tmp_path):
    output_dir = tmp_path / "queries"
    project = get_project(
        DatabaseConnection(url=SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB),
        "dsgrid_conus_2022",
    )
    query = QueryTestElectricityValuesCompositeDataset(
        REGISTRY_PATH, project, output_dir=output_dir
    )
    CompositeDatasetQuerySubmitter(project, output_dir).create_dataset(query.make_query())
    assert query.validate()

    query2 = QueryTestElectricityValuesCompositeDatasetAgg(
        REGISTRY_PATH, project, output_dir=output_dir, geography="county"
    )
    CompositeDatasetQuerySubmitter(project, output_dir).submit(query2.make_query())

    query3 = QueryTestElectricityValuesCompositeDatasetAgg(
        REGISTRY_PATH,
        project,
        output_dir=output_dir,
        geography="state",
    )
    CompositeDatasetQuerySubmitter(project, output_dir).submit(query3.make_query())


def test_query_cli_create_validate(tmp_path):
    filename = tmp_path / "query.json5"
    cmd = [
        "--url",
        SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB,
        "query",
        "project",
        "create",
        "-r",
        "-f",
        str(filename),
        "-F",
        "expression",
        "-F",
        "column_operator",
        "-F",
        "supplemental_column_operator",
        "-F",
        "expression_raw",
        "--overwrite",
        "my_query",
        "dsgrid_conus_2022",
        "projected_dg_conus_2022",
    ]
    shutdown_project()
    runner = CliRunner()
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    query = ProjectQueryModel.from_file(filename)
    assert query.name == "my_query"
    assert query.result.aggregations
    result = runner.invoke(cli, ["query", "project", "validate", str(filename)])
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "table_format",
    [
        {
            "format_type": "pivoted",
            "pivoted_dimension_type": "metric",
        },
        {
            "format_type": "unpivoted",
        },
    ],
)
def test_query_cli_run(tmp_path, cached_registry, table_format):
    conn: DatabaseConnection = cached_registry
    shutdown_project()
    output_dir = tmp_path / "queries"
    baseline_query = TEST_PROJECT_PATH / "test_efs" / "queries" / "baseline.json5"
    five_year_query = TEST_PROJECT_PATH / "test_efs" / "queries" / "five_year_intervals.json5"
    for filename in (baseline_query, five_year_query):
        data = load_data(filename)
        data["result"]["table_format"] = table_format
        dst = tmp_path / filename.name
        dump_data(data, dst)
        cmd = [
            "--url",
            conn.url,
            "query",
            "project",
            "run",
            "--output",
            str(output_dir),
            str(dst),
        ]
        runner = CliRunner()
        result = runner.invoke(cli, cmd)
        assert result.exit_code == 0

    baseline_df = read_parquet(output_dir / "baseline" / "table.parquet")
    baseline_years = sorted(
        (
            int(x["Model Years 2010 to 2050"])
            for x in baseline_df.select("Model Years 2010 to 2050").distinct().collect()
        )
    )
    assert baseline_years == [2010, 2020, 2030, 2040, 2050]
    five_year_df = read_parquet(output_dir / "five_year_intervals" / "table.parquet")
    five_year_years = sorted(
        (
            int(x["Five Year Intervals"])
            for x in five_year_df.select("Five Year Intervals").distinct().collect()
        )
    )
    assert five_year_years == [2010, 2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050]

    def get_pivoted_value_sum(df):
        return aggregate_single_value(df, "sum", "cooling") + aggregate_single_value(
            df, "sum", "fans"
        )

    def get_unpivoted_value_sum(df):
        return aggregate_single_value(df, "sum", VALUE_COLUMN)

    get_value_sum = (
        get_pivoted_value_sum
        if table_format["format_type"] == "pivoted"
        else get_unpivoted_value_sum
    )
    baseline_sum = get_value_sum(baseline_df)
    baseline_years_str = [str(x) for x in baseline_years]
    five_year_sum = get_value_sum(
        five_year_df.filter(F.col("Five Year Intervals").isin(baseline_years_str))
    )
    assert math.isclose(five_year_sum, baseline_sum)

    val1 = get_value_sum(five_year_df.filter(F.col("Five Year Intervals") == "2020"))
    val2 = get_value_sum(five_year_df.filter(F.col("Five Year Intervals") == "2030"))
    interpolated_val = get_value_sum(five_year_df.filter(F.col("Five Year Intervals") == "2025"))
    assert math.isclose(interpolated_val, val1 / 2 + val2 / 2)


def test_dimension_names_model():
    # Test that this model is defined with all dimension types.
    diff = {x.value for x in DimensionType}.symmetric_difference(
        set(DimensionNamesModel.model_fields)
    )
    assert not diff


def test_map_dataset(tmp_path):
    dataset_id = "comstock_conus_2022_reference"
    mapping_plan = DatasetMappingPlan(
        dataset_id=dataset_id,
        mappings=[
            MapOperation(
                name="scenario",
            ),
            MapOperation(
                name="county",
                persist=True,
            ),
        ],
        apply_fraction_op=MapOperation(
            name="apply_fraction_op",
            persist=True,
        ),
        keep_intermediate_files=True,
    )
    plan_file = tmp_path / "mapping_plan.json5"
    mapping_plan.to_file(plan_file)
    output_dir = tmp_path / "queries"
    scratch_dir = tmp_path / "scratch"
    cmd = [
        "--scratch-dir",
        str(scratch_dir),
        "--url",
        SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB,
        "query",
        "project",
        "map-dataset",
        "--mapping-plan",
        str(plan_file),
        "dsgrid_conus_2022",
        dataset_id,
        "--output",
        str(output_dir),
    ]
    runner = CliRunner()
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    df1 = read_parquet(output_dir / dataset_id / "table.parquet")
    columns = [
        "time_est",
        "model_year",
        "scenario",
        "county",
        "sector",
        "subsector",
        "end_use",
    ]
    dfp1 = df1.sort(*columns).toPandas()
    checkpoint_files = [x for x in scratch_dir.iterdir() if x.suffix == ".json"]
    assert len(checkpoint_files) == 2
    checkpoint_files.sort(key=lambda x: x.stat().st_mtime)
    checkpoint = MapOperationCheckpoint.from_file(checkpoint_files[-1])
    assert checkpoint.completed_operation_names == ["scenario", "county", "apply_fraction_op"]

    for i, checkpoint_file in enumerate(checkpoint_files):
        out_dir = cmd[-1] + f"from_checkpoint_{i}"
        cmd2 = copy.copy(cmd)
        cmd2[-1] = out_dir
        cmd2.append("--checkpoint-file")
        cmd2.append(str(checkpoint_file))
        result2 = runner.invoke(cli, cmd2)
        assert result2.exit_code == 0
        df2 = read_parquet(Path(out_dir) / dataset_id / "table.parquet")
        dfp2 = df2.sort(*columns).toPandas()
        assert_frame_equal(dfp1, dfp2)


def test_dataset_queries(tmp_path):
    output_dir = tmp_path / "queries"
    scratch_dir = tmp_path / "scratch"
    for query in ("resstock_county", "resstock_project_county", "resstock_state"):
        cmd = [
            "--scratch-dir",
            str(scratch_dir),
            "--url",
            SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB,
            "query",
            "dataset",
            "run",
            str(SIMPLE_STANDARD_SCENARIOS / "queries" / f"{query}.json5"),
            "--output",
            str(output_dir),
        ]
        runner = CliRunner()
        result = runner.invoke(cli, cmd)
        assert result.exit_code == 0

    rcounty = read_parquet(output_dir / "resstock_county" / "table.parquet")
    rcounty_renamed = rcounty.withColumn(
        "geography",
        F.when(F.col("geography") == "G0600370", "06037")
        .when(F.col("geography") == "G0600730", "06073")
        .when(F.col("geography") == "G3600470", "36047")
        .when(F.col("geography") == "G3600810", "36081"),
    )

    def add_state(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "state",
            F.when(F.col("geography") == "06037", "CA")
            .when(F.col("geography") == "06073", "CA")
            .when(F.col("geography") == "36047", "NY")
            .when(F.col("geography") == "36081", "NY"),
        )

    rpcounty = read_parquet(output_dir / "resstock_project_county" / "table.parquet")
    rstate = read_parquet(output_dir / "resstock_state" / "table.parquet")

    rcounty_renamed = add_state(rcounty_renamed)
    rpcounty = add_state(rpcounty)

    res_rcounty = (
        rcounty_renamed.groupBy("geography", "metric")
        .agg(F.sum("value").alias("value"))
        .sort("geography", "metric")
        .toPandas()
    )
    res_rpcounty = (
        rpcounty.groupBy("geography", "metric")
        .agg(F.sum("value").alias("value"))
        .sort("geography", "metric")
        .toPandas()
    )
    assert_frame_equal(res_rcounty, res_rpcounty)

    res_rpcounty_state = (
        rpcounty.drop("geography")
        .withColumnRenamed("state", "geography")
        .groupBy("geography", "metric")
        .agg(F.sum("value").alias("value"))
        .sort("geography", "metric")
        .toPandas()
    )
    res_state = (
        rstate.groupBy("geography", "metric")
        .agg(F.sum("value").alias("value"))
        .sort("geography", "metric")
        .toPandas()
    )
    assert_frame_equal(res_rpcounty_state, res_state)


_projects = {}
_managers = {}


def get_project(conn: DatabaseConnection, project_id: str):
    """Load a Project and cache it for future calls.
    Loading is slow and the Project isn't being changed by these tests.

    Note: This function uses manual dispose() via shutdown_project() instead of
    context managers because the managers need to persist across multiple test calls.
    The cached managers are cleaned up in shutdown_project().
    """
    key = (conn.url, project_id)
    if key in _projects:
        return _projects[key]
    mgr = RegistryManager.load(
        conn,
        offline_mode=True,
    )
    _managers[key] = mgr
    _projects[key] = mgr.project_manager.load_project(project_id)
    return _projects[key]


def shutdown_project():
    """Shutdown a project and stop the SparkSession so that another process can create one."""
    _projects.clear()
    for mgr in _managers.values():
        mgr.dispose()
    _managers.clear()
    if not use_duckdb():
        spark = SparkSession.getActiveSession()
        if spark is not None:
            spark.stop()


def run_query_test(test_query_cls, *args, expected_values=None, to_time_zone=None):
    output_dir = Path(tempfile.gettempdir()) / "queries"
    if output_dir.exists():
        shutil.rmtree(output_dir)

    project = get_project(test_query_cls.get_db_connection(), test_query_cls.get_project_id())
    try:
        query = test_query_cls(
            *args, REGISTRY_PATH, project, output_dir=output_dir, to_time_zone=to_time_zone
        )
        for load_cached_table in (False, True):
            ProjectQuerySubmitter(project, output_dir).submit(
                query.make_query(),
                persist_intermediate_table=True,
                load_cached_table=load_cached_table,
                overwrite=True,
            )
            assert query.validate(expected_values=expected_values)
            metadata_file = output_dir / query.name / "metadata.json"
            subprocess.run(
                [sys.executable, "scripts/table_metadata.py", str(metadata_file)], check=True
            )
    finally:
        if output_dir.exists():
            shutil.rmtree(output_dir)


class QueryTestBase(abc.ABC):
    """Base class for all test queries"""

    def __init__(
        self,
        registry_path,
        project,
        output_dir=Path("queries"),
        to_time_zone=None,
    ):
        self._registry_path = Path(registry_path)
        self._project = project
        self._output_dir = Path(output_dir)
        self._model = None
        self._cached_stats = None
        self._to_time_zone = to_time_zone

    @staticmethod
    def get_db_connection() -> DatabaseConnection:
        return DatabaseConnection(url=SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB)

    @staticmethod
    def get_project_id() -> str:
        return "dsgrid_conus_2022"

    @property
    def name(self) -> str:
        """Return the name of the query.

        Returns
        -------
        str

        """
        return self.make_query().name

    @property
    def output_dir(self):
        """Return the output directory for the query results.

        Returns
        -------
        Path

        """
        return self._output_dir

    def get_raw_stats(self):
        """Return the raw stats for the data tables.

        These stats assume that the query model years are ["2018", "2040"].

        Returns
        -------
        dict
        """
        if self._cached_stats is None:
            self._cached_stats = load_dataset_stats()
        return self._cached_stats

    @abc.abstractmethod
    def make_query(self):
        """Return the query model"""

    @abc.abstractmethod
    def validate(self, expected_values=None):
        """Validate the results

        Parameters
        ----------
        expected_values : dict | None
            Optional dictionary containing expected values from a pytest fixture.

        Returns
        -------
        bool
            Return True when the validation is successful.

        """

    def get_filtered_county_id(self):
        filters = self._model.project.dataset_params.dimension_filters
        counties = [x.value for x in filters if x.name == "county"]
        assert len(counties) == 1, f"Unexpected length of filtered counties: {len(counties)}"
        return counties[0]


class QueryTestElectricityValues(QueryTestBase):
    NAME = "electricity-values"

    def __init__(self, category: DimensionCategory, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._category = category

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="projected_dg_conus_2022",
                    source_datasets=[
                        ProjectionDatasetModel(
                            dataset_id="comstock_conus_2022_projected",
                            initial_value_dataset_id="comstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_commercial_energy_use_growth_factors",
                            construction_method=DatasetConstructionMethod.EXPONENTIAL_GROWTH,
                        ),
                        ProjectionDatasetModel(
                            dataset_id="resstock_conus_2022_projected",
                            initial_value_dataset_id="resstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_residential_energy_use_growth_factors",
                            construction_method=DatasetConstructionMethod.EXPONENTIAL_GROWTH,
                        ),
                        # StandaloneDatasetModel(dataset_id="tempo_conus_2022"),
                    ],
                    expression="comstock_conus_2022_projected | resstock_conus_2022_projected",
                    # expression="comstock_conus_2022_projected | resstock_conus_2022_projected | tempo_conus_2022",
                    params=ProjectQueryDatasetParamsModel(
                        dimension_filters=[
                            # This is a nonsensical way to filter down to county 06037, but
                            # it tests the code with combinations of base and supplemental
                            # dimension filters.
                            DimensionFilterColumnOperatorModel(
                                dimension_type=DimensionType.GEOGRAPHY,
                                dimension_name="county",
                                operator="isin",
                                value=["06037", "36047"],
                            ),
                            DimensionFilterExpressionModel(
                                dimension_type=DimensionType.GEOGRAPHY,
                                dimension_name="state",
                                operator="==",
                                column="name",
                                value="California",
                            ),
                        ],
                    ),
                ),
            ),
            result=QueryResultParamsModel(
                replace_ids_with_names=True,
                table_format=UnpivotedTableFormatModel(),
                time_zone=self._to_time_zone,
            ),
        )
        match self._category:
            case DimensionCategory.BASE:
                filter_model = DimensionFilterExpressionModel(
                    dimension_type=DimensionType.METRIC,
                    dimension_name="end_use",
                    operator="==",
                    column="fuel_id",
                    value="electricity",
                )
            case DimensionCategory.SUBSET:
                filter_model = SubsetDimensionFilterModel(
                    dimension_type=DimensionType.METRIC,
                    dimension_names=["electricity_end_uses"],
                )
            case DimensionCategory.SUPPLEMENTAL:
                filter_model = SupplementalDimensionFilterColumnOperatorModel(
                    dimension_type=DimensionType.METRIC,
                    dimension_name="end_uses_by_fuel_type",
                    operator="isin",
                    column="fuel_id",
                    value=["electricity"],
                )
            case _:
                assert False, f"{self._category=}"

        self._model.project.dataset.params.dimension_filters.append(filter_model)
        return self._model

    def validate(self, expected_values=None):
        county = "06037"
        county_name = (
            self._project.config.get_dimension_records("county")
            .filter(f"id == {county}")
            .collect()[0]
            .name
        )
        df = read_parquet(self.output_dir / self.name / "table.parquet")
        assert "natural_gas_heating" not in df.columns
        non_value_columns = set(
            self._project.config.list_dimension_names(category=DimensionCategory.BASE)
        )
        non_value_columns.update({"id", "timestamp"})
        value_columns = sorted((x for x in df.columns if x not in non_value_columns))
        expected = [VALUE_COLUMN]

        pdf = df.toPandas()
        # Check time zone conversion
        if self._model.result.time_zone:
            expected.append("time_zone")
            if isinstance(self._model.result.time_zone, TimeZone):
                assert set(pdf["time_zone"]) == {self._model.result.time_zone.tz_name}
                assert pdf.loc[0, "time_est"].tz is None
                expected_min = (
                    pd.Timestamp("2012-01-01", tz=ZoneInfo("EST"))
                    .tz_convert(self._to_time_zone.tz)
                    .tz_localize(None)
                )
                assert pdf["time_est"].min() == expected_min
                expected_max = (
                    pd.Timestamp("2012-12-31 23:00", tz=ZoneInfo("EST"))
                    .tz_convert(self._to_time_zone.tz)
                    .tz_localize(None)
                )
                assert pdf["time_est"].max() == expected_max
        else:
            config = dsgrid.runtime_config
            if config.backend_engine != BackendEngine.SPARK:
                assert pdf.loc[0, "time_est"].tz is not None

        # expected = ["electricity_cooling", "electricity_ev_l1l2", "electricity_heating"]
        success = set(value_columns) == set(expected)
        if not success:
            logger.error("Mismatch in columns: actual=%s expected=%s", value_columns, expected)
        if not df.select("county").distinct().filter(f"county == '{county_name}'").collect():
            logger.error("County name = %s is not present", county_name)
            success = False
        if success:
            total_cooling = aggregate_single_value(
                df.filter("end_use == 'Cooling'"), "sum", VALUE_COLUMN
            )
            total_heating = aggregate_single_value(
                df.filter("end_use == 'Heating'"), "sum", VALUE_COLUMN
            )
            expected = self.get_raw_stats()["by_county"][county]["comstock_resstock"]["sum"]
            assert math.isclose(total_cooling, expected["electricity_cooling"])
            assert math.isclose(total_heating, expected["electricity_heating"])

        return success


class QueryTestElectricityUse(QueryTestBase):
    NAME = "total_electricity_use"

    def __init__(self, geography, op, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._geography = geography
        self._op = op

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="projected_dg_conus_2022",
                    source_datasets=[
                        StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                        StandaloneDatasetModel(dataset_id="resstock_conus_2022_projected"),
                    ],
                    params=ProjectQueryDatasetParamsModel(
                        dimension_filters=[
                            SupplementalDimensionFilterColumnOperatorModel(
                                dimension_type=DimensionType.METRIC,
                                dimension_name="end_uses_by_fuel_type",
                                operator="isin",
                                column="fuel_id",
                                value=["electricity"],
                            ),
                        ],
                    ),
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=[self._geography],
                            metric=["end_uses_by_fuel_type"],
                            model_year=[],
                            scenario=[],
                            sector=[],
                            subsector=[],
                            time=[],
                            weather_year=[],
                        ),
                        aggregation_function=self._op,
                    ),
                ],
                output_format="parquet",
                table_format=UnpivotedTableFormatModel(),
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        if self._geography == "county":
            validate_electricity_use_by_county(
                self._op,
                self.output_dir / self.name / "table.parquet",
                self.get_raw_stats(),
                "comstock_resstock",
                4,
            )
        elif self._geography == "state":
            validate_electricity_use_by_state(
                self._op,
                self.output_dir / self.name / "table.parquet",
                self.get_raw_stats(),
                "comstock_resstock",
            )
        else:
            assert False, self._geography

        return True


class QueryTestDatasetMappingPlan(QueryTestBase):
    NAME = "dataset_mapping_plan"

    def __init__(self, handle_data_skew, persist, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._handle_data_skew = handle_data_skew
        self._persist = persist

    def make_query(self) -> None:
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="tempo_mapped",
                    source_datasets=[
                        StandaloneDatasetModel(dataset_id="tempo_conus_2022"),
                    ],
                ),
                mapping_plans=[
                    DatasetMappingPlan(
                        dataset_id="tempo_conus_2022",
                        mappings=[
                            MapOperation(
                                name="county",
                                handle_data_skew=self._handle_data_skew,
                                persist=self._persist,
                            ),
                            MapOperation(
                                name="model_year",
                                handle_data_skew=self._handle_data_skew,
                                persist=self._persist,
                            ),
                            MapOperation(
                                name="end_use",
                                handle_data_skew=self._handle_data_skew,
                                persist=self._persist,
                            ),
                            MapOperation(
                                name="subsector",
                                handle_data_skew=self._handle_data_skew,
                                persist=self._persist,
                            ),
                        ],
                    ),
                ],
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=["state"],
                            metric=["end_uses_by_fuel_type"],
                            model_year=[],
                            scenario=[],
                            sector=[],
                            subsector=[],
                            time=[],
                            weather_year=[],
                        ),
                        aggregation_function="sum",
                    ),
                ],
                output_format="parquet",
                table_format=UnpivotedTableFormatModel(),
            ),
        )
        return self._model

    def validate(self, expected_values=None) -> bool:
        return True


class QueryTestElectricityUseFilterResults(QueryTestBase):
    NAME = "total_electricity_use"

    def __init__(self, geography, op, metric_dimension_category, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert geography in ("county", "state"), geography
        self._geography = geography
        self._op = op
        self._metric_dimension_category = metric_dimension_category

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="projected_dg_conus_2022",
                    source_datasets=[
                        StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                        StandaloneDatasetModel(dataset_id="resstock_conus_2022_projected"),
                    ],
                    params=ProjectQueryDatasetParamsModel(
                        dimension_filters=[
                            SupplementalDimensionFilterColumnOperatorModel(
                                dimension_type=DimensionType.METRIC,
                                dimension_name="end_uses_by_fuel_type",
                                operator="isin",
                                column="fuel_id",
                                value=["electricity", "natural_gas"],
                            ),
                        ],
                    ),
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=[self._geography],
                            metric=["end_uses_by_fuel_type"],
                            model_year=[],
                            scenario=[],
                            sector=[],
                            subsector=[],
                            time=[],
                            weather_year=[],
                        ),
                        aggregation_function=self._op,
                    ),
                ],
                dimension_filters=[
                    DimensionFilterColumnOperatorModel(
                        dimension_type=DimensionType.GEOGRAPHY,
                        dimension_name=self._geography,
                        operator="isin",
                        value=["06037", "36047"] if self._geography == "county" else ["CA", "NY"],
                    ),
                    SubsetDimensionFilterModel(
                        dimension_type=DimensionType.SUBSECTOR,
                        dimension_names=["commercial_subsectors"],
                    ),
                ],
                output_format="parquet",
                table_format=UnpivotedTableFormatModel(),
            ),
        )

        match self._metric_dimension_category:
            case DimensionCategory.BASE:
                self._model.result.dimension_filters.append(
                    DimensionFilterColumnOperatorModel(
                        dimension_type=DimensionType.METRIC,
                        dimension_name="end_use",
                        operator="isin",
                        value=["electricity_cooling", "electricity_heating"],
                    ),
                )
            case DimensionCategory.SUBSET:
                self._model.result.dimension_filters.append(
                    SubsetDimensionFilterModel(
                        dimension_type=DimensionType.METRIC,
                        dimension_names=["electricity_end_uses"],
                    ),
                )
            case _:
                assert False, self._metric_dimension_category
        return self._model

    def validate(self, expected_values=None):
        if self._geography == "county":
            validate_electricity_use_by_county(
                self._op,
                self.output_dir / self.name / "table.parquet",
                self.get_raw_stats(),
                "comstock",
                2,
            )
        elif self._geography == "state":
            validate_electricity_use_by_state(
                self._op,
                self.output_dir / self.name / "table.parquet",
                self.get_raw_stats(),
                "comstock",
            )
        else:
            assert False, self._geography

        return True


class QueryTestTotalElectricityUseWithFilter(QueryTestBase):
    NAME = "total_electricity_use"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="projected_dg_conus_2022",
                    source_datasets=[
                        StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                        StandaloneDatasetModel(dataset_id="resstock_conus_2022_projected"),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                dimension_filters=[
                    DimensionFilterExpressionModel(
                        dimension_type=DimensionType.GEOGRAPHY,
                        dimension_name="county",
                        operator="==",
                        value="06037",
                        column="county",
                    ),
                ],
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=["county"],
                            metric=["end_uses_by_fuel_type"],
                            model_year=[],
                            scenario=[],
                            sector=[],
                            subsector=[],
                            time=[],
                            weather_year=[],
                        ),
                        aggregation_function="sum",
                    ),
                ],
                output_format="parquet",
                table_format=UnpivotedTableFormatModel(),
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        validate_electricity_use_by_county(
            "sum",
            self.output_dir / self.name / "table.parquet",
            self.get_raw_stats(),
            "comstock_resstock",
            1,
        )
        return True


class QueryTestDiurnalElectricityUseByCountyChained(QueryTestBase):
    NAME = "diurnal_electricity_use_by_county"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="projected_dg_conus_2022",
                    source_datasets=[
                        StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                        StandaloneDatasetModel(dataset_id="resstock_conus_2022_projected"),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=["county"],
                            metric=["end_uses_by_fuel_type"],
                            model_year=["model_year"],
                            scenario=["scenario"],
                            sector=["sector"],
                            subsector=["subsector"],
                            time=["time_est"],
                            weather_year=["weather_2012"],
                        ),
                        aggregation_function="sum",
                    ),
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=["county"],
                            metric=["end_uses_by_fuel_type"],
                            model_year=[],
                            scenario=[],
                            sector=[],
                            subsector=[],
                            time=[
                                ColumnModel(
                                    dimension_name="time_est", function="hour", alias="hour"
                                )
                            ],
                            weather_year=[],
                        ),
                        aggregation_function="mean",
                    ),
                ],
                sort_columns=["county", "hour"],
                output_format="parquet",
                table_format=UnpivotedTableFormatModel(),
            ),
        )
        return self._model

    def validate(self, expected_values):
        filename = self.output_dir / self.name / "table.parquet"
        df = read_parquet(filename)
        assert not {"end_uses_by_fuel_type", "county", "hour"}.difference(df.columns)
        hour = 16
        county = "06037"
        end_use = "electricity_end_uses"
        assert (
            df.filter(f"county == '{county}' and end_uses_by_fuel_type == '{end_use}'")
            .select("hour")
            .distinct()
            .count()
            == 24
        )
        filtered_values = (
            df.filter(f"county == '{county}'")
            .filter(f"hour == {hour}")
            .filter(f"end_uses_by_fuel_type == '{end_use}'")
            .collect()
        )

        df.filter(f"county == '{county}' and end_uses_by_fuel_type == '{end_use}'").show()
        assert len(filtered_values) == 1
        assert math.isclose(filtered_values[0].value, expected_values["la_electricity_hour_16"])
        return True


class QueryTestElectricityUseByStateAndPCA(QueryTestBase):
    NAME = "total_electricity_use_by_state_and_pca"

    def __init__(
        self,
        column_type: ColumnType,
        geography_columns: list[str],
        aggregate_each_dataset: bool,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._column_type = column_type
        self._geography_columns = geography_columns
        self._aggregate_each_dataset = aggregate_each_dataset

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="projected_dg_conus_2022",
                    source_datasets=[
                        StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                        StandaloneDatasetModel(dataset_id="resstock_conus_2022_projected"),
                        StandaloneDatasetModel(dataset_id="tempo_conus_2022_mapped"),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                column_type=self._column_type,
                aggregate_each_dataset=self._aggregate_each_dataset,
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=self._geography_columns,
                            metric=["end_uses_by_fuel_type"],
                            model_year=["model_year"],
                            scenario=["scenario"],
                            sector=["sector"],
                            subsector=[],
                            time=["time_est"],
                            weather_year=["weather_2012"],
                        ),
                        aggregation_function="sum",
                    ),
                ],
                output_format="parquet",
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        df = read_parquet(self.output_dir / self.name / "table.parquet")
        match self._column_type:
            case ColumnType.DIMENSION_NAMES:
                assert "time_est" in df.columns
                for column in self._geography_columns:
                    assert column.dimension_name in df.columns
            case ColumnType.DIMENSION_TYPES:
                assert "timestamp" in df.columns
                assert "geography" in df.columns
                for column in self._geography_columns:
                    assert column.dimension_name not in df.columns
            case _:
                assert False, f"Bug: add support for {self._column_type}"
        return True


class QueryTestAnnualElectricityUseByState(QueryTestBase):
    NAME = "annual_electricity_use_by_state"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="projected_dg_conus_2022",
                    source_datasets=[
                        StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                        StandaloneDatasetModel(dataset_id="resstock_conus_2022_projected"),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=["state"],
                            metric=["end_uses_by_fuel_type"],
                            model_year=["model_year"],
                            scenario=["scenario"],
                            sector=["sector"],
                            subsector=["subsector"],
                            time=[
                                ColumnModel(
                                    dimension_name="time_est", function="year", alias="year"
                                )
                            ],
                            weather_year=["weather_2012"],
                        ),
                        aggregation_function="sum",
                    ),
                ],
                sort_columns=["state"],
                output_format="csv",
                table_format=UnpivotedTableFormatModel(),
            ),
        )
        return self._model

    def validate(self, expected_values):
        filename = self.output_dir / self.name / "table.csv"
        df = read_csv(filename)
        years = df.select("year").distinct().collect()
        assert len(years) == 1
        assert years[0].year == 2012
        validate_electricity_use_by_state(
            "sum",
            df.groupBy("state", "end_uses_by_fuel_type", "year").agg(
                F.sum(VALUE_COLUMN).alias(VALUE_COLUMN)
            ),
            self.get_raw_stats(),
            "comstock_resstock",
        )
        return True


class QueryTestPeakLoadByStateSubsector(QueryTestBase):
    NAME = "peak-load-by-state-subsector"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="projected_dg_conus_2022",
                    source_datasets=[
                        StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                        StandaloneDatasetModel(dataset_id="resstock_conus_2022_projected"),
                    ],
                    params=ProjectQueryDatasetParamsModel(
                        dimension_filters=[
                            SupplementalDimensionFilterColumnOperatorModel(
                                dimension_type=DimensionType.METRIC,
                                dimension_name="end_uses_by_fuel_type",
                                operator="isin",
                                column="fuel_id",
                                value=["electricity"],
                            ),
                        ],
                    ),
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=["state"],
                            metric=["end_uses_by_fuel_type"],
                            model_year=["model_year"],
                            scenario=["scenario"],
                            sector=["sector"],
                            subsector=["subsector"],
                            time=["time_est"],
                            weather_year=["weather_2012"],
                        ),
                        aggregation_function="sum",
                    ),
                ],
                reports=[
                    ReportInputModel(
                        report_type=ReportType.PEAK_LOAD,
                        inputs=PeakLoadInputModel(
                            group_by_columns=["state", "subsector", "scenario", "model_year"]
                        ),
                    ),
                ],
                output_format="parquet",
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        df = read_parquet(self.output_dir / self.name / "table.parquet")
        peak_load = read_parquet(self.output_dir / self.name / PeakLoadReport.REPORT_FILENAME)
        model_year = "2020"
        scenario = "reference"
        state = "CA"
        subsector = "hospital"

        def make_expr(tdf):
            return (
                (tdf.state == state)
                & (tdf.subsector == subsector)
                & (tdf.model_year == model_year)
                & (tdf.scenario == scenario)
                & (tdf.end_uses_by_fuel_type == "electricity_end_uses")
            )

        expected = aggregate_single_value(df.filter(make_expr(df)), "max", VALUE_COLUMN)
        actual = peak_load.filter(make_expr(peak_load)).collect()[0][VALUE_COLUMN]
        assert math.isclose(actual, expected)
        return True


class QueryTestMapAnnualTime(QueryTestBase):
    NAME = "map-annual-time"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="eia_861_annual_energy_use_state_sector_mapped",
                    source_datasets=[
                        StandaloneDatasetModel(
                            dataset_id="eia_861_annual_energy_use_state_sector"
                        ),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=["state"],
                            metric=["end_use"],
                            model_year=["model_year"],
                            scenario=[],
                            sector=["sector"],
                            subsector=[],
                            time=["time_est"],
                            weather_year=["weather_2012"],
                        ),
                        aggregation_function="sum",
                    ),
                ],
                output_format="parquet",
                table_format=PivotedTableFormatModel(pivoted_dimension_type=DimensionType.METRIC),
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        df = read_parquet(self.output_dir / self.name / "table.parquet")
        distinct_model_years = df.select(DimensionType.MODEL_YEAR.value).distinct().collect()
        assert len(distinct_model_years) == 1
        assert distinct_model_years[0][DimensionType.MODEL_YEAR.value] == "2020"
        expected_ca_res = calc_expected_eia_861_ca_res_load_value()
        actual_ca_res = aggregate_single_value(
            df.filter("state == 'CA' and sector == 'res'"),
            "sum",
            "electricity_unspecified",
        )
        assert math.isclose(actual_ca_res, expected_ca_res)
        return True


class QueryTestInvalidAggregation(QueryTestBase):
    NAME = "invalid_aggregation"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="projected_dg_conus_2022",
                    source_datasets=[
                        StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=["county"],
                            metric=["electricity_end_uses"],
                            model_year=[],
                            scenario=[],
                            sector=[],
                            subsector=[],
                            time=[],
                            weather_year=[],
                        ),
                        aggregation_function="sum",
                    ),
                ],
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        assert False


class QueryTestElectricityValuesCompositeDataset(QueryTestBase):
    NAME = "electricity-values"

    def make_query(self):
        self._model = CreateCompositeDatasetQueryModel(
            name=self.NAME,
            dataset_id="com_res",
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
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
                    params=ProjectQueryDatasetParamsModel(
                        dimension_filters=[
                            SupplementalDimensionFilterColumnOperatorModel(
                                dimension_type=DimensionType.METRIC,
                                dimension_name="end_uses_by_fuel_type",
                                operator="isin",
                                column="fuel_id",
                                value=["electricity"],
                            ),
                        ],
                    ),
                ),
            ),
            result=QueryResultParamsModel(),
        )
        return self._model

    def validate(self, expected_values=None):
        df = read_parquet(
            str(self.output_dir / "composite_datasets" / self._model.dataset_id / "table.parquet")
        )
        assert sorted([x.end_use for x in df.select("end_use").distinct().collect()]) == [
            "electricity_cooling",
            "electricity_heating",
        ]
        summary = (
            df.select("end_use", VALUE_COLUMN)
            .groupBy("end_use")
            .agg(F.sum(VALUE_COLUMN).alias(VALUE_COLUMN))
            .collect()
        )
        totals = {x.end_use: x[VALUE_COLUMN] for x in summary}
        expected = self.get_raw_stats()["overall"]["resstock"]["sum"]
        assert math.isclose(totals["electricity_cooling"], expected["electricity_cooling"])
        assert math.isclose(totals["electricity_heating"], expected["electricity_heating"])
        return True


class QueryTestElectricityValuesCompositeDatasetAgg(QueryTestBase):
    NAME = "electricity-values-agg-from-composite-dataset"

    def __init__(self, *args, geography="county", **kwargs):
        super().__init__(*args, **kwargs)
        self._geography = geography

    def make_query(self):
        self._model = CompositeDatasetQueryModel(
            name=self.NAME,
            dataset_id="com_res",
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionNamesModel(
                            geography=[self._geography],
                            metric=["end_uses_by_fuel_type"],
                            model_year=[],
                            scenario=[],
                            sector=[],
                            subsector=[],
                            time=[],
                            weather_year=[],
                        ),
                        aggregation_function="sum",
                    ),
                ],
                output_format="parquet",
                table_format=UnpivotedTableFormatModel(),
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        if self._geography == "county":
            validate_electricity_use_by_county(
                "sum",
                self.output_dir / self.name / "table.parquet",
                self.get_raw_stats(),
                "comstock_resstock",
                4,
            )
        elif self._geography == "state":
            validate_electricity_use_by_state(
                "sum",
                self.output_dir / self.name / "table.parquet",
                self.get_raw_stats(),
                "comstock_resstock",
            )
        logger.error(
            "Validation is not supported with geography=%s",
            self._geography,
        )
        assert False


class QueryTestUnitMapping(QueryTestBase):
    NAME = "test_efs_comstock_query"

    @staticmethod
    def get_db_connection() -> DatabaseConnection:
        return DatabaseConnection(url=CACHED_TEST_REGISTRY_DB)

    @staticmethod
    def get_project_id():
        return "test_efs"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id=self.get_project_id(),
                include_dsgrid_dataset_components=False,
                dataset=DatasetModel(
                    dataset_id="efs_comstock",
                    source_datasets=[
                        StandaloneDatasetModel(dataset_id="test_efs_comstock"),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                output_format="parquet",
                table_format=PivotedTableFormatModel(pivoted_dimension_type=DimensionType.METRIC),
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        filename = self.output_dir / self.name / "table.parquet"
        df = read_parquet(filename)
        project = get_project(self.get_db_connection(), self.get_project_id())
        project.load_dataset("test_efs_comstock")
        dataset = project.get_dataset("test_efs_comstock")
        ld = dataset._handler._load_data
        lk = dataset._handler._load_data_lookup
        raw_ld = ld.join(lk, on="id").drop("id")
        # This test dataset has some fractional mapping values included.
        # subsector = hospital and model_year = 2020 are 1.0, fans are 1.0
        expected_cooling = (
            raw_ld.sort("timestamp")
            .filter("subsector == 'com__Hospital' and metric = 'com_cooling'")
            .limit(1)
            .collect()[0]
        )
        expected_fans = (
            raw_ld.sort("timestamp")
            .filter("subsector == 'com__Hospital' and metric = 'com_fans'")
            .limit(1)
            .collect()[0]
        )
        subsector = expected_cooling.subsector.replace("com__", "")
        actual = (
            df.filter(F.col("ComStock Subsectors EFS") == subsector)
            .filter(F.col("US Counties 2010 - ComStock Only") == expected_cooling.geography)
            .filter(F.col("Model Years 2010 to 2050") == "2020")
            .sort("Time-2012-EST-hourly-periodBeginning-noDST-noLeapDayAdjustment-total")
            .limit(1)
            .collect()[0]
        )
        assert actual.fans == expected_fans[VALUE_COLUMN] * 0.9
        assert actual.cooling == expected_cooling[VALUE_COLUMN] * 1000
        return True


def validate_electricity_use_by_county(
    op, results_path, raw_stats, datasets, expected_county_count
):
    results = read_parquet(results_path)
    counties = [str(x.county) for x in results.select("county").distinct().collect()]
    assert len(counties) == expected_county_count, counties
    stats = raw_stats["by_county"]
    col = "end_uses_by_fuel_type"
    for county in counties:
        actual = results.filter(
            f"county == '{county}' and {col} == 'electricity_end_uses'"
        ).collect()[0][VALUE_COLUMN]
        expected = stats[county][datasets][op]["electricity"]
        assert math.isclose(actual, expected)


def validate_electricity_use_by_state(op, results_path: DataFrame | Path, raw_stats, datasets):
    if isinstance(results_path, DataFrame):
        results = results_path
    else:
        results = read_parquet(results_path)
    if op == "sum":
        exp_ca = get_expected_ca_sum_electricity(raw_stats, datasets)
        exp_ny = get_expected_ny_sum_electricity(raw_stats, datasets)
    else:
        assert op == "max", op
        exp_ca = get_expected_ca_max_electricity(raw_stats, datasets)
        exp_ny = get_expected_ny_max_electricity(raw_stats, datasets)

    col = "end_uses_by_fuel_type"
    actual_ca = results.filter(f"state == 'CA' and {col} == 'electricity_end_uses'").collect()[0][
        VALUE_COLUMN
    ]
    actual_ny = results.filter(f"state == 'NY' and {col} == 'electricity_end_uses'").collect()[0][
        VALUE_COLUMN
    ]
    assert math.isclose(actual_ca, exp_ca)
    assert math.isclose(actual_ny, exp_ny)


def get_expected_ca_max_electricity(raw_stats, datasets):
    by_county = raw_stats["by_county"]
    return max(
        (
            by_county["06037"][datasets]["max"]["electricity"],
            by_county["06073"][datasets]["max"]["electricity"],
        )
    )


def get_expected_ny_max_electricity(raw_stats, datasets):
    by_county = raw_stats["by_county"]
    return max(
        (
            by_county["36047"][datasets]["max"]["electricity"],
            by_county["36081"][datasets]["max"]["electricity"],
        )
    )


def get_expected_ca_sum_electricity(raw_stats, datasets):
    by_county = raw_stats["by_county"]
    return (
        by_county["06037"][datasets]["sum"]["electricity"]
        + by_county["06073"][datasets]["sum"]["electricity"]
    )


def get_expected_ny_sum_electricity(raw_stats, datasets):
    by_county = raw_stats["by_county"]
    return (
        by_county["36047"][datasets]["sum"]["electricity"]
        + by_county["36081"][datasets]["sum"]["electricity"]
    )


def calc_expected_eia_861_ca_res_load_value():
    project = get_project(
        DatabaseConnection(url=SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB),
        "dsgrid_conus_2022",
    )
    dataset_id = dataset_id = "eia_861_annual_energy_use_state_sector"
    project.load_dataset(dataset_id)
    mapping_id = None
    for dataset in project.config.model.datasets:
        if dataset.dataset_id == "eia_861_annual_energy_use_state_sector":
            for ref in dataset.mapping_references:
                if ref.from_dimension_type == DimensionType.GEOGRAPHY:
                    mapping_id = ref.mapping_id
                    break
        if mapping_id is not None:
            break
    assert mapping_id is not None
    records = project.dimension_mapping_manager.get_by_id(mapping_id).get_records_dataframe()

    fraction_06037 = records.filter("to_id == '06037'").collect()[0].from_fraction
    fraction_06073 = records.filter("to_id == '06073'").collect()[0].from_fraction
    dataset = project.get_dataset(dataset_id)
    raw = dataset._handler._load_data.filter("geography == 'CA' and sector == 'res'").collect()
    assert len(raw) == 1
    num_scenarios = 2
    elec_mwh_state = raw[0][VALUE_COLUMN] * num_scenarios
    elec_mwh_selected_counties = elec_mwh_state * fraction_06037 + elec_mwh_state * fraction_06073
    return elec_mwh_selected_counties


# The next two functions are for ad hoc testing.


def run_query(
    dimension_name,
    registry_path=REGISTRY_PATH,
    operation="sum",
    output_dir=Path("queries"),
    persist_intermediate_table=True,
    load_cached_table=True,
):
    setup_logging(
        "dsgrid", "query.log", console_level=logging.INFO, file_level=logging.INFO, mode="w"
    )
    project = Project.load(
        "dsgrid_conus_2022",
        offline_mode=True,
        registry_path=registry_path,
    )
    if dimension_name == QueryTestElectricityValues.NAME:
        query = QueryTestElectricityValues(True, registry_path, project, output_dir=output_dir)
    else:
        msg = f"no query for {dimension_name}"
        raise Exception(msg)

    ProjectQuerySubmitter(project, output_dir).submit(
        query.make_query(),
        persist_intermediate_table=persist_intermediate_table,
        load_cached_table=load_cached_table,
        force=True,
    )
    assert query.validate()


def run_composite_dataset(
    registry_path=REGISTRY_PATH,
    output_dir=Path("queries"),
    persist_intermediate_table=False,
    load_cached_table=True,
):
    setup_logging(
        "dsgrid", "query.log", console_level=logging.INFO, file_level=logging.INFO, mode="w"
    )
    conn = DatabaseConnection(url=SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB)
    with RegistryManager.load(
        conn,
        offline_mode=True,
    ) as mgr:
        project = mgr.project_manager.load_project("dsgrid_conus_2022")
        query = QueryTestElectricityValuesCompositeDataset(
            registry_path, project, output_dir=output_dir
        )
        CompositeDatasetQuerySubmitter(project, output_dir).create_dataset(
            query.make_query(),
            persist_intermediate_table=persist_intermediate_table,
            load_cached_table=load_cached_table,
        )
        assert query.validate()

        query2 = QueryTestElectricityValuesCompositeDatasetAgg(
            registry_path, project, output_dir=output_dir, geography="county"
        )
        CompositeDatasetQuerySubmitter(project, output_dir).submit(query2.make_query())
        assert query2.validate()

        query3 = QueryTestElectricityValuesCompositeDatasetAgg(
            registry_path, project, output_dir=output_dir, geography="state"
        )
        CompositeDatasetQuerySubmitter(project, output_dir).submit(query3.make_query())
        assert query3.validate()
