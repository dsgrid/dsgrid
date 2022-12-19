import abc
import logging
import math
import shutil
import tempfile
from collections import defaultdict, namedtuple
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
import pytest
from pyspark.sql import SparkSession

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.dimension_filters import (
    DimensionFilterExpressionModel,
    DimensionFilterColumnOperatorModel,
    SupplementalDimensionFilterColumnOperatorModel,
)
from dsgrid.exceptions import DSGInvalidQuery
from dsgrid.loggers import setup_logging
from dsgrid.project import Project
from dsgrid.query.models import (
    AggregationModel,
    ColumnModel,
    CompositeDatasetQueryModel,
    CreateCompositeDatasetQueryModel,
    DatasetsModel,
    DimensionQueryNamesModel,
    ProjectQueryDatasetParamsModel,
    ProjectQueryParamsModel,
    ProjectQueryModel,
    QueryResultParamsModel,
    ReportInputModel,
    ReportType,
    ExponentialGrowthDatasetModel,
    StandaloneDatasetModel,
)
from dsgrid.query.query_submitter import ProjectQuerySubmitter, CompositeDatasetQuerySubmitter
from dsgrid.query.report_peak_load import PeakLoadInputModel, PeakLoadReport
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.run_command import check_run_command


REGISTRY_PATH = (
    Path(__file__).absolute().parent.parent
    / "dsgrid-test-data"
    / "filtered_registries"
    / "simple_standard_scenarios"
)

DIMENSION_MAPPING_SCHEMA = StructType(
    [
        StructField("from_id", StringType(), False),
        StructField("to_id", StringType()),
        StructField("from_fraction", DoubleType()),
    ]
)

Datasets = namedtuple("Datasets", ["comstock", "resstock", "tempo"])

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def la_expected_electricity_hour_16(tmp_path_factory):
    output_dir = tmp_path_factory.mktemp("diurnal_queries")
    project = get_project()
    query = QueryTestElectricityValues(False, REGISTRY_PATH, project, output_dir=output_dir)
    ProjectQuerySubmitter(project, output_dir).submit(
        query.make_query(),
        persist_intermediate_table=False,
        load_cached_table=False,
    )
    df = read_parquet(str(output_dir / query.name / "table.parquet"))
    df = df.withColumn("elec", df.electricity_cooling + df.electricity_heating).drop(
        "electricity_cooling", "electricity_heating"
    )
    expected = (
        df.groupBy("county", F.hour("time_est").alias("hour"))
        .agg(F.mean("elec"))
        .filter("hour == 16")
        .collect()[0]["avg(elec)"]
    )
    yield {
        "la_electricity_hour_16": expected,
    }


def test_electricity_values():
    run_query_test(QueryTestElectricityValues, True)
    run_query_test(QueryTestElectricityValues, False)


def test_electricity_use_by_county():
    run_query_test(QueryTestElectricityUse, "county", "sum")
    run_query_test(QueryTestElectricityUse, "county", "max")


def test_electricity_use_by_state():
    run_query_test(QueryTestElectricityUse, "state", "sum")
    run_query_test(QueryTestElectricityUse, "state", "max")


def test_total_electricity_use_with_filter():
    run_query_test(QueryTestTotalElectricityUseWithFilter)


def test_total_electricity_use_by_state_and_pca():
    run_query_test(QueryTestElectricityUseByStateAndPCA)


def test_diurnal_electricity_use_by_county_chained(la_expected_electricity_hour_16):
    run_query_test(
        QueryTestDiurnalElectricityUseByCountyChained,
        expected_values=la_expected_electricity_hour_16,
    )


def test_peak_load():
    run_query_test(QueryTestPeakLoadByStateSubsector)


def test_invalid_drop_pivoted_dimension(tmp_path):
    invalid_agg = AggregationModel(
        dimensions=DimensionQueryNamesModel(
            data_source=["data_source"],
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
    query = ProjectQueryModel(
        name="test",
        project=ProjectQueryParamsModel(
            project_id="dsgrid_conus_2022",
            include_dsgrid_dataset_components=False,
            datasets=DatasetsModel(
                datasets=[
                    StandaloneDatasetModel(
                        dataset_id="comstock_conus_2022_reference",
                    ),
                    StandaloneDatasetModel(
                        dataset_id="resstock_conus_2022_reference",
                    ),
                ],
            ),
        ),
        result=QueryResultParamsModel(
            output_format="parquet",
        ),
    )
    project = get_project()
    output_dir = tmp_path / "queries"

    query.result.aggregations = [invalid_agg]
    with pytest.raises(DSGInvalidQuery):
        ProjectQuerySubmitter(project, output_dir).submit(query)


def test_create_composite_dataset_query(tmp_path):
    output_dir = tmp_path / "queries"
    project = get_project()
    query = QueryTestElectricityValuesCompositeDataset(
        REGISTRY_PATH, project, output_dir=output_dir
    )
    CompositeDatasetQuerySubmitter(project, output_dir).create_dataset(query.make_query())

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
    cmd = (
        f"dsgrid query project create --offline --registry-path={REGISTRY_PATH} "
        f"-d -r -f {filename} -F expression -F column_operator "
        "-F supplemental_column_operator -F raw --force my_query dsgrid_conus_2022"
    )
    shutdown_project()
    check_run_command(cmd)
    query = ProjectQueryModel.from_file(filename)
    assert query.name == "my_query"
    assert query.result.aggregations
    check_run_command(f"dsgrid query project validate {filename}")


def test_query_cli_run(tmp_path):
    output_dir = tmp_path / "queries"
    project = get_project()
    query = QueryTestElectricityValues(True, REGISTRY_PATH, project, output_dir=output_dir)
    filename = tmp_path / "query.json"
    filename.write_text(query.make_query().json(indent=2))
    cmd = (
        f"dsgrid query project run --offline --registry-path={REGISTRY_PATH} "
        f"--output={output_dir} {filename}"
    )
    shutdown_project()
    check_run_command(cmd)
    query.validate()


def test_dimension_query_names_model():
    # Test that this model is defined with all dimension types.
    diff = {x.value for x in DimensionType}.symmetric_difference(
        set(DimensionQueryNamesModel.__fields__)
    )
    assert not diff


_project = None


def get_project():
    """Load a Project and cache it for future calls.
    Loading is slow and the Project isn't being changed by these tests.
    """
    global _project
    if _project is not None:
        return _project
    mgr = RegistryManager.load(
        REGISTRY_PATH,
        offline_mode=True,
    )
    return mgr.project_manager.load_project("dsgrid_conus_2022")


def shutdown_project():
    """Shutdown a project and stop the SparkSession so that another process can create one."""
    global _project
    if _project is not None:
        _project = None

    spark = SparkSession.getActiveSession()
    if spark is not None:
        spark.stop()

    # There could be collisions with the Hive metastore.
    for path in ("metastore_db", "spark-warehouse"):
        if Path(path).exists():
            shutil.rmtree(path)


def run_query_test(test_query_cls, *args, expected_values=None):
    output_dir = Path(tempfile.gettempdir()) / "queries"
    if output_dir.exists():
        shutil.rmtree(output_dir)

    project = get_project()
    try:
        query = test_query_cls(*args, REGISTRY_PATH, project, output_dir=output_dir)
        for load_cached_table in (False, True):
            ProjectQuerySubmitter(project, output_dir).submit(
                query.make_query(),
                persist_intermediate_table=True,
                load_cached_table=load_cached_table,
                force=True,
            )
            query.validate(expected_values=expected_values)
    finally:
        if output_dir.exists():
            shutil.rmtree(output_dir)


class QueryTestBase(abc.ABC):
    """Base class for all test queries"""

    stats = None

    def __init__(self, registry_path, project, output_dir=Path("queries")):
        self._registry_path = Path(registry_path)
        self._project = project
        self._output_dir = Path(output_dir)
        self._model = None

    @property
    def name(self):
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
        if QueryTestBase.stats is None:
            logger.info("Generate raw stats")
            QueryTestBase.stats = generate_raw_stats(self._registry_path)
        return QueryTestBase.stats

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
        counties = [x.value for x in filters if x.dimension_query_name == "county"]
        assert len(counties) == 1, f"Unexpected length of filtered counties: {len(counties)}"
        return counties[0]


class QueryTestElectricityValues(QueryTestBase):

    NAME = "electricity-values"

    def __init__(self, use_supplemental_dimension, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._use_supplemental_dimension = use_supplemental_dimension

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                datasets=DatasetsModel(
                    datasets=[
                        ExponentialGrowthDatasetModel(
                            dataset_id="comstock_projected_conus_2022",
                            initial_value_dataset_id="comstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_commercial_energy_use_growth_factors",
                            construction_method="formula123",
                        ),
                        ExponentialGrowthDatasetModel(
                            dataset_id="resstock_projected_conus_2022",
                            initial_value_dataset_id="resstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_residential_energy_use_growth_factors",
                            construction_method="formula123",
                        ),
                        # StandaloneDatasetModel(dataset_id="tempo_conus_2022"),
                    ],
                    expression="comstock_projected_conus_2022 | resstock_projected_conus_2022",
                    # expression="comstock_projected_conus_2022 | resstock_projected_conus_2022 | tempo_conus_2022",
                    params=ProjectQueryDatasetParamsModel(
                        dimension_filters=[
                            # This is a nonsensical way to filter down to county 06037, but
                            # it tests the code with combinations of base and supplemental
                            # dimension filters.
                            DimensionFilterColumnOperatorModel(
                                dimension_type=DimensionType.GEOGRAPHY,
                                dimension_query_name="county",
                                operator="isin",
                                value=["06037", "36047"],
                            ),
                            DimensionFilterExpressionModel(
                                dimension_type=DimensionType.GEOGRAPHY,
                                dimension_query_name="state",
                                operator="==",
                                column="name",
                                value="California",
                            ),
                        ],
                    ),
                ),
            ),
            result=QueryResultParamsModel(
                supplemental_columns=["state"],
                replace_ids_with_names=True,
            ),
        )
        if self._use_supplemental_dimension:
            self._model.project.datasets.params.dimension_filters.append(
                SupplementalDimensionFilterColumnOperatorModel(
                    dimension_type=DimensionType.METRIC,
                    dimension_query_name="electricity",
                )
            )
        else:
            self._model.project.datasets.params.dimension_filters.append(
                DimensionFilterExpressionModel(
                    dimension_type=DimensionType.METRIC,
                    dimension_query_name="end_use",
                    operator="==",
                    column="fuel_id",
                    value="electricity",
                )
            )
        return self._model

    def validate(self, expected_values=None):
        county = "06037"
        county_name = (
            self._project.config.get_dimension_records("county")
            .filter(f"id == {county}")
            .collect()[0]
            .name
        )
        df = read_parquet(str(self.output_dir / self.name / "table.parquet"))
        assert "natural_gas_heating" not in df.columns
        non_value_columns = self._project.config.get_base_dimension_query_names()
        non_value_columns.update({"id", "timestamp"})
        supp_columns = {x.get_column_name() for x in self._model.result.supplemental_columns}
        non_value_columns.update(supp_columns)
        value_columns = sorted((x for x in df.columns if x not in non_value_columns))
        expected = ["electricity_cooling", "electricity_heating"]
        # expected = ["electricity_cooling", "electricity_ev_l1l2", "electricity_heating", "fraction"]
        success = value_columns == expected
        if not success:
            logger.error("Mismatch in columns: actual=%s expected=%s", value_columns, expected)
        if supp_columns.difference(df.columns):
            logger.error("supplemental_columns=%s are not present in table", supp_columns)
            success = False
        if not df.select("county").distinct().filter(f"county == '{county_name}'").collect():
            logger.error("County name = %s is not present", county_name)
            success = False
        if success:
            total_cooling = df.agg(F.sum("electricity_cooling").alias("sum")).collect()[0].sum
            total_heating = df.agg(F.sum("electricity_heating").alias("sum")).collect()[0].sum
            expected = self.get_raw_stats()["by_county"][county]["comstock_resstock"]["sum"]
            assert math.isclose(total_cooling, expected["electricity_cooling"])
            assert math.isclose(total_heating, expected["electricity_heating"])


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
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                datasets=DatasetsModel(
                    datasets=[
                        ExponentialGrowthDatasetModel(
                            dataset_id="comstock_projected_conus_2022",
                            initial_value_dataset_id="comstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_commercial_energy_use_growth_factors",
                            construction_method="formula123",
                        ),
                        ExponentialGrowthDatasetModel(
                            dataset_id="resstock_projected_conus_2022",
                            initial_value_dataset_id="resstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_residential_energy_use_growth_factors",
                            construction_method="formula123",
                        ),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionQueryNamesModel(
                            data_source=[],
                            geography=[self._geography],
                            metric=["electricity"],
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
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        if self._geography == "county":
            validate_electricity_use_by_county(
                self._op,
                self.output_dir / self.name / "table.parquet",
                self.get_raw_stats(),
                4,
            )
        elif self._geography == "state":
            validate_electricity_use_by_state(
                self._op,
                self.output_dir / self.name / "table.parquet",
                self.get_raw_stats(),
            )
        else:
            assert False, self._geography


class QueryTestTotalElectricityUseWithFilter(QueryTestBase):

    NAME = "total_electricity_use"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                datasets=DatasetsModel(
                    datasets=[
                        ExponentialGrowthDatasetModel(
                            dataset_id="comstock_projected_conus_2022",
                            initial_value_dataset_id="comstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_commercial_energy_use_growth_factors",
                            construction_method="formula123",
                        ),
                        ExponentialGrowthDatasetModel(
                            dataset_id="resstock_projected_conus_2022",
                            initial_value_dataset_id="resstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_residential_energy_use_growth_factors",
                            construction_method="formula123",
                        ),
                    ],
                    params=ProjectQueryDatasetParamsModel(
                        dimension_filters=[
                            DimensionFilterExpressionModel(
                                dimension_type=DimensionType.GEOGRAPHY,
                                dimension_query_name="county",
                                operator="==",
                                value="06037",
                            ),
                        ],
                    ),
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionQueryNamesModel(
                            data_source=[],
                            geography=["county"],
                            metric=["electricity"],
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
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        validate_electricity_use_by_county(
            "sum",
            self.output_dir / self.name / "table.parquet",
            self.get_raw_stats(),
            1,
        )


class QueryTestDiurnalElectricityUseByCountyChained(QueryTestBase):

    NAME = "diurnal_electricity_use_by_county"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                datasets=DatasetsModel(
                    datasets=[
                        ExponentialGrowthDatasetModel(
                            dataset_id="comstock_projected_conus_2022",
                            initial_value_dataset_id="comstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_commercial_energy_use_growth_factors",
                            construction_method="formula123",
                        ),
                        ExponentialGrowthDatasetModel(
                            dataset_id="resstock_projected_conus_2022",
                            initial_value_dataset_id="resstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_residential_energy_use_growth_factors",
                            construction_method="formula123",
                        ),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionQueryNamesModel(
                            data_source=["data_source"],
                            geography=["county"],
                            metric=["electricity"],
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
                        dimensions=DimensionQueryNamesModel(
                            data_source=[],
                            geography=["county"],
                            metric=["electricity"],
                            model_year=[],
                            scenario=[],
                            sector=[],
                            subsector=[],
                            time=[
                                ColumnModel(
                                    dimension_query_name="time_est", function="hour", alias="hour"
                                )
                            ],
                            weather_year=[],
                        ),
                        aggregation_function="mean",
                    ),
                ],
                sort_columns=["county", "hour"],
                output_format="parquet",
            ),
        )
        return self._model

    def validate(self, expected_values):
        filename = self.output_dir / self.name / "table.parquet"
        df = read_parquet(str(filename))
        assert not {"all_electricity_sum", "county", "hour"}.difference(df.columns)
        hour = 16
        val = (
            df.filter("county == '06037'")
            .filter(f"hour == {hour}")
            .collect()[0]
            .all_electricity_sum
        )
        assert math.isclose(val, expected_values["la_electricity_hour_16"])


class QueryTestElectricityUseByStateAndPCA(QueryTestBase):

    NAME = "total_electricity_use_by_state_and_pca"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                datasets=DatasetsModel(
                    datasets=[
                        ExponentialGrowthDatasetModel(
                            dataset_id="comstock_projected_conus_2022",
                            initial_value_dataset_id="comstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_commercial_energy_use_growth_factors",
                            construction_method="formula123",
                        ),
                        ExponentialGrowthDatasetModel(
                            dataset_id="resstock_projected_conus_2022",
                            initial_value_dataset_id="resstock_conus_2022_reference",
                            growth_rate_dataset_id="aeo2021_reference_residential_energy_use_growth_factors",
                            construction_method="formula123",
                        ),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionQueryNamesModel(
                            data_source=["data_source"],
                            geography=["state", "reeds_pca", "census_region"],
                            metric=["electricity"],
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
        assert not {"all_electricity_sum", "reeds_pca", "state", "census_region"}.difference(
            df.columns
        )


class QueryTestPeakLoadByStateSubsector(QueryTestBase):

    NAME = "peak-load-by-state-subsector"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                datasets=DatasetsModel(
                    datasets=[
                        StandaloneDatasetModel(
                            dataset_id="comstock_conus_2022_reference",
                        ),
                        StandaloneDatasetModel(
                            dataset_id="resstock_conus_2022_reference",
                        ),
                    ],
                ),
            ),
            result=QueryResultParamsModel(
                aggregations=[
                    AggregationModel(
                        dimensions=DimensionQueryNamesModel(
                            data_source=["data_source"],
                            geography=["state"],
                            metric=["electricity"],
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
        model_year = "2018"
        scenario = "reference"
        state = "CA"
        subsector = "hospital"

        def make_expr(tdf):
            return (
                (tdf.state == state)
                & (tdf.subsector == subsector)
                & (tdf.model_year == model_year)
                & (tdf.scenario == scenario)
            )

        expected = (
            df.filter(make_expr(df))
            .agg(F.max("all_electricity_sum").alias("max_val"))
            .collect()[0]
            .max_val
        )
        actual = peak_load.filter(make_expr(peak_load)).collect()[0].all_electricity_sum
        assert math.isclose(actual, expected)


class QueryTestElectricityValuesCompositeDataset(QueryTestBase):

    NAME = "electricity-values"

    def make_query(self):
        self._model = CreateCompositeDatasetQueryModel(
            name=self.NAME,
            dataset_id="com_res",
            project=ProjectQueryParamsModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                datasets=DatasetsModel(
                    datasets=[
                        StandaloneDatasetModel(
                            dataset_id="comstock_conus_2022_reference",
                        ),
                        StandaloneDatasetModel(
                            dataset_id="resstock_conus_2022_reference",
                        ),
                    ],
                    params=ProjectQueryDatasetParamsModel(
                        dimension_filters=[
                            SupplementalDimensionFilterColumnOperatorModel(
                                dimension_type=DimensionType.METRIC,
                                dimension_query_name="electricity",
                            ),
                        ],
                    ),
                ),
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        df = read_parquet(
            str(self.output_dir / "composite_datasets" / self._model.dataset_id / "table.parquet")
        )
        assert "natural_gas_heating" not in df.columns
        non_value_columns = self._project.config.get_base_dimension_query_names()
        non_value_columns.update({"id", "timestamp"})
        non_value_columns.update(self._model.result.supplemental_columns)
        value_columns = sorted((x for x in df.columns if x not in non_value_columns))
        # TODO: fraction will be removed eventually
        expected = ["electricity_cooling", "electricity_heating", "fraction"]
        # expected = ["electricity_cooling", "electricity_ev_l1l2", "electricity_heating", "fraction"]
        assert value_columns == expected
        assert not set(self._model.result.supplemental_columns).difference(df.columns)

        total_cooling = df.agg(F.sum("electricity_cooling").alias("sum")).collect()[0].sum
        total_heating = df.agg(F.sum("electricity_heating").alias("sum")).collect()[0].sum
        expected = self.get_raw_stats()["overall"]["comstock_resstock"]["sum"]
        assert math.isclose(total_cooling, expected["electricity_cooling"])
        assert math.isclose(total_heating, expected["electricity_heating"])


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
                        dimensions=DimensionQueryNamesModel(
                            data_source=[],
                            geography=[self._geography],
                            metric=["electricity"],
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
            ),
        )
        return self._model

    def validate(self, expected_values=None):
        if self._geography == "county":
            validate_electricity_use_by_county(
                "sum",
                self.output_dir / self.name / "table.parquet",
                self.get_raw_stats(),
                4,
            )
        elif self._geography == "state":
            validate_electricity_use_by_state(
                "sum",
                self.output_dir / self.name / "table.parquet",
                self.get_raw_stats(),
            )
        logger.error(
            "Validation is not supported with geography=%s",
            self._geography,
        )
        assert False


def perform_op(df, column, operation):
    return df.select(column).agg(operation(column).alias("tmp_col")).collect()[0].tmp_col


def validate_electricity_use_by_county(op, results_path, raw_stats, expected_county_count):
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    results = spark.read.parquet(
        str(results_path),
    )
    counties = [str(x.county) for x in results.select("county").distinct().collect()]
    assert len(counties) == expected_county_count, counties
    stats = raw_stats["by_county"]
    for county in counties:
        col = f"all_electricity_{op}"
        actual = results.filter(f"county == '{county}'").collect()[0][col]
        expected = stats[county]["comstock_resstock"][op]["electricity"]
        assert math.isclose(actual, expected)


def validate_electricity_use_by_state(op, results_path, raw_stats):
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    results = spark.read.parquet(str(results_path))
    if op == "sum":
        exp_ca = get_expected_ca_sum_electricity(raw_stats)
        exp_ny = get_expected_ny_sum_electricity(raw_stats)
    else:
        assert op == "max", op
        exp_ca = get_expected_ca_max_electricity(raw_stats)
        exp_ny = get_expected_ny_max_electricity(raw_stats)
    col = f"all_electricity_{op}"
    actual_ca = results.filter("state == 'CA'").collect()[0][col]
    actual_ny = results.filter("state == 'NY'").collect()[0][col]
    assert math.isclose(actual_ca, exp_ca)
    assert math.isclose(actual_ny, exp_ny)


def get_expected_ca_max_electricity(raw_stats):
    by_county = raw_stats["by_county"]
    return max(
        (
            by_county["06037"]["comstock_resstock"]["max"]["electricity"],
            by_county["06073"]["comstock_resstock"]["max"]["electricity"],
        )
    )


def get_expected_ny_max_electricity(raw_stats):
    by_county = raw_stats["by_county"]
    return max(
        (
            by_county["36047"]["comstock_resstock"]["max"]["electricity"],
            by_county["36081"]["comstock_resstock"]["max"]["electricity"],
        )
    )


def get_expected_ca_sum_electricity(raw_stats):
    by_county = raw_stats["by_county"]
    return (
        by_county["06037"]["comstock_resstock"]["sum"]["electricity"]
        + by_county["06073"]["comstock_resstock"]["sum"]["electricity"]
    )


def get_expected_ny_sum_electricity(raw_stats):
    by_county = raw_stats["by_county"]
    return (
        by_county["36047"]["comstock_resstock"]["sum"]["electricity"]
        + by_county["36081"]["comstock_resstock"]["sum"]["electricity"]
    )


BUILDING_COUNTY_MAPPING = {
    "06037": "G0600370",
    "06073": "G0600730",
    "36047": "G3600470",
    "36081": "G3600810",
}


def generate_raw_stats(path):
    datasets = read_datasets(path)
    stats = {"overall": defaultdict(dict), "by_county": {}}
    for project_county in BUILDING_COUNTY_MAPPING:
        stats["by_county"][project_county] = defaultdict(dict)

    operations = (F.sum, F.max, F.mean)
    for name in Datasets._fields:
        for op in operations:
            table = getattr(datasets, name)
            perform_op_by_electricity(stats["overall"], table, name, op)
            for project_county in stats["by_county"]:
                if name == "tempo":
                    dataset_county = project_county
                else:
                    dataset_county = BUILDING_COUNTY_MAPPING[project_county]
                _table = table.filter(f"geography='{dataset_county}'")
                perform_op_by_electricity(stats["by_county"][project_county], _table, name, op)

    accumulate_stats(stats["overall"])
    for county_stats in stats["by_county"].values():
        accumulate_stats(county_stats)
    return stats


def accumulate_stats(stats):
    com = stats["comstock"]
    res = stats["resstock"]
    tem = stats["tempo"]
    com["sum"]["electricity"] = (
        com["sum"]["electricity_cooling"] + com["sum"]["electricity_heating"]
    )
    res["sum"]["electricity"] = (
        res["sum"]["electricity_cooling"] + res["sum"]["electricity_heating"]
    )
    tem["sum"]["electricity"] = tem["sum"]["L1andL2"]
    com["max"]["electricity"] = max(
        (com["max"]["electricity_cooling"], com["max"]["electricity_heating"])
    )
    res["max"]["electricity"] = max(
        (res["max"]["electricity_cooling"], res["max"]["electricity_heating"])
    )
    tem["max"]["electricity"] = tem["max"]["L1andL2"]
    stats["comstock_resstock"] = {
        "sum": {
            "electricity_cooling": com["sum"]["electricity_cooling"]
            + res["sum"]["electricity_cooling"],
            "electricity_heating": com["sum"]["electricity_heating"]
            + res["sum"]["electricity_heating"],
            "electricity": com["sum"]["electricity"] + res["sum"]["electricity"],
        },
        "max": {
            "electricity_cooling": max(
                (com["max"]["electricity_cooling"], res["max"]["electricity_cooling"])
            ),
            "electricity_heating": max(
                (com["max"]["electricity_heating"], res["max"]["electricity_heating"])
            ),
            "electricity": max((com["max"]["electricity"], res["max"]["electricity"])),
        },
    }
    stats["total"] = {
        "sum": {
            "electricity": com["sum"]["electricity"]
            + res["sum"]["electricity"]
            + tem["sum"]["electricity"],
        },
        "max": {
            "electricity": max(
                (com["max"]["electricity"], res["max"]["electricity"], tem["max"]["electricity"])
            ),
        },
    }


def read_datasets(path):
    aeo_com = map_aeo_com_subsectors(
        map_aeo_com_county_to_comstock_county(
            duplicate_aeo_com_census_division_to_county(
                apply_load_mapping_aeo_com(
                    read_csv_single_table(
                        path
                        / "data"
                        / "aeo2021_reference_commercial_energy_use_growth_factors"
                        / "1.0.0"
                        / "load_data.csv"
                    )
                )
            )
        )
    )
    aeo_res = apply_load_mapping_aeo_res(
        read_csv_single_table(
            path
            / "data"
            / "aeo2021_reference_residential_energy_use_growth_factors"
            / "1.0.0"
            / "load_data.csv"
        ).drop("sector")
    )
    comstock = make_projection_df(
        aeo_com,
        read_table(path / "data" / "comstock_conus_2022_reference" / "1.0.0"),
        ["geography", "subsector", "model_year"],
    )
    resstock = make_projection_df(
        aeo_res,
        read_table(path / "data" / "resstock_conus_2022_reference" / "1.0.0"),
        ["model_year"],
    )
    datasets = Datasets(
        comstock=comstock,
        resstock=resstock,
        tempo=read_dataset_tempo(),
    )
    return datasets


def apply_load_mapping_aeo_com(aeo_com):
    return (
        aeo_com.withColumn("electricity_cooling", F.col("elec_cooling") * 1.0)
        .withColumn("electricity_heating", F.col("elec_heating") * 1.0)
        .drop("elec_cooling", "elec_heating")
    )


def _load_dimension_mapping_records(path):
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    return spark.read.csv(str(path), header=True, schema=DIMENSION_MAPPING_SCHEMA).cache()


def duplicate_aeo_com_census_division_to_county(aeo_com):
    path = REGISTRY_PATH / "configs" / "dimension_mappings"
    paths = [x for x in path.iterdir() if x.name.startswith("us_census_divisions__us_counties")]
    assert len(paths) == 1, paths
    dm_path = paths[0] / "1.0.0"
    csv_files = [x for x in dm_path.iterdir() if x.suffix == ".csv"]
    assert len(csv_files) == 1, csv_files
    records = _load_dimension_mapping_records(csv_files[0])
    assert records.select("from_fraction").distinct().collect()[0].from_fraction == 1.0
    records = records.drop("from_fraction")
    mapped = aeo_com.join(records, on=aeo_com.geography == records.from_id)
    # Make sure no census division got dropped in the join.
    orig_count = aeo_com.select("geography").distinct().count()
    new_count = mapped.select("geography").distinct().count()
    assert orig_count == new_count, f"{orig_count} {new_count}"
    return mapped.drop("from_id", "geography").withColumnRenamed("to_id", "geography")


def map_aeo_com_county_to_comstock_county(aeo_com):
    path = REGISTRY_PATH / "configs" / "dimension_mappings"
    paths = [
        x
        for x in path.iterdir()
        if x.name.startswith("conus_2022-comstock_us_county_fip__us_counties_2020_l48")
    ]
    assert len(paths) == 1, paths
    dm_path = paths[0] / "1.0.0"
    csv_files = [x for x in dm_path.iterdir() if x.suffix == ".csv"]
    assert len(csv_files) == 1, csv_files
    records = _load_dimension_mapping_records(csv_files[0])
    assert records.select("from_fraction").distinct().collect()[0].from_fraction == 1.0
    records = records.drop("from_fraction")
    mapped = aeo_com.join(records, on=aeo_com.geography == records.to_id)
    # Make sure no entries were dropped.
    orig_count = aeo_com.count()
    new_count = mapped.count()
    assert orig_count == new_count, f"{orig_count} {new_count}"
    return mapped.drop("to_id", "geography").withColumnRenamed("from_id", "geography")


def map_aeo_com_subsectors(aeo_com):
    path = REGISTRY_PATH / "configs" / "dimension_mappings"
    paths = [
        x
        for x in path.iterdir()
        if x.name.startswith("aeo2021-commercial-building-types__conus-2022-detailed-subsectors")
    ]
    assert len(paths) == 1, paths
    dm_path = paths[0] / "1.0.0"
    csv_files = [x for x in dm_path.iterdir() if x.suffix == ".csv"]
    assert len(csv_files) == 1, csv_files
    records = _load_dimension_mapping_records(csv_files[0])
    mapped = aeo_com.join(records, on=aeo_com.subsector == records.from_id)
    # Make sure no subsector got dropped in the join.
    orig_count = aeo_com.select("subsector").distinct().count()
    new_count = mapped.select("subsector").distinct().count()
    assert orig_count == new_count, f"{orig_count} {new_count}"
    mapped = mapped.drop("from_id", "subsector").withColumnRenamed("to_id", "subsector")
    for column in ("electricity_cooling", "electricity_heating"):
        mapped = (
            mapped.withColumn("tmp", F.col(column) * F.col("from_fraction"))
            .drop(column)
            .withColumnRenamed("tmp", column)
        )
    return (
        mapped.drop("from_fraction")
        .groupBy("subsector", "geography")
        .agg(
            F.sum("electricity_cooling").alias("electricity_cooling"),
            F.sum("electricity_heating").alias("electricity_heating"),
            F.sum("ng_heating").alias("ng_heating"),
        )
    )


def apply_load_mapping_aeo_res(aeo_res):
    return (
        aeo_res.withColumn("electricity_cooling", F.col("elec_heat_cool") * 1.0)
        .withColumn("electricity_heating", F.col("elec_heat_cool") * 1.0)
        .drop("elec_heat_cool")
    )


def make_projection_df(aeo, ld_df, join_columns):
    # comstock and resstock have a single year of data for model_year 2018
    # Apply the growth rate for 2018 and 2040, the years in the filtered registry.
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    years_df = spark.createDataFrame([{"model_year": "2018"}, {"model_year": "2040"}])
    aeo = aeo.crossJoin(years_df)
    ld_df = ld_df.crossJoin(years_df)
    base_year = 2018
    gr_df = aeo
    pivoted_columns = ("electricity_cooling", "electricity_heating")
    for column in pivoted_columns:
        gr_col = column + "__gr"
        gr_df = gr_df.withColumn(
            gr_col,
            F.pow((1 + F.col(column)), F.col("model_year").cast(IntegerType()) - base_year),
        ).drop(column)

    df = ld_df.join(gr_df, on=join_columns)
    for column in pivoted_columns:
        tmp_col = column + "_tmp"
        gr_col = column + "__gr"
        df = (
            df.withColumn(tmp_col, F.col(column) * F.col(gr_col))
            .drop(column, gr_col)
            .withColumnRenamed(tmp_col, column)
        )

    df.cache()
    return df


def read_dataset_tempo():
    project = get_project()
    dataset_id = dataset_id = "tempo_conus_2022"
    project.load_dataset(dataset_id)
    tempo = project.get_dataset(dataset_id)
    lookup = tempo._handler._load_data_lookup
    load_data = tempo._handler._load_data
    tempo_data_mapped_time = tempo._handler._convert_time_dimension(
        load_data.join(lookup, on="id").drop("id"), project.config
    )
    return tempo_data_mapped_time.cache()


def read_csv_single_table(path):
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    return spark.read.csv(str(path), header=True, inferSchema=True)


def read_table(path):
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    load_data = spark.read.parquet(str(path / "load_data.parquet")).cache()
    lookup = spark.read.parquet(str(path / "load_data_lookup.parquet")).cache()
    table = load_data.join(lookup, on="id").drop("id").cache()
    return table


def perform_op_by_electricity(stats, table, name, operation):
    if name in ("comstock", "resstock"):
        columns = ["electricity_cooling", "electricity_heating"]
    elif name == "tempo":
        columns = ["L1andL2"]
    else:
        assert False, name
    for col in columns:
        op = operation.__name__
        col_name = f"{op}_{col}"
        if op not in stats[name]:
            stats[name][op] = {}
        val = getattr(
            table.agg(operation(col).alias(col_name)).collect()[0],
            col_name,
        )
        if op == "sum":
            # 2 scenarios
            val *= 2
        stats[name][op][col] = val
    stats[name]["count"] = table.count()


def read_parquet(filename):
    """Read a Parquet file and load it into cache. This helps debugging with pytest --pdb.
    If you don't use this, the parquet file will get deleted on a failure and you won't be able
    to inspect the dataframe.
    """
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    df = spark.read.parquet(str(filename)).cache()
    df.count()
    return df


# The next two functions are for ad hoc testing.


def run_query(
    dimension_query_name,
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
    if dimension_query_name == QueryTestElectricityValues.NAME:
        query = QueryTestElectricityValues(True, registry_path, project, output_dir=output_dir)
    else:
        raise Exception(f"no query for {dimension_query_name}")

    ProjectQuerySubmitter(project, output_dir).submit(
        query.make_query(),
        persist_intermediate_table=persist_intermediate_table,
        load_cached_table=load_cached_table,
        force=True,
    )
    result = query.validate()
    print(f"Result of query {query.name} = {result}")


def run_composite_dataset(
    registry_path=REGISTRY_PATH,
    output_dir=Path("queries"),
    persist_intermediate_table=False,
    load_cached_table=True,
):
    setup_logging(
        "dsgrid", "query.log", console_level=logging.INFO, file_level=logging.INFO, mode="w"
    )
    mgr = RegistryManager.load(
        offline_mode=True,
        registry_path=registry_path,
    )
    project = mgr.project_manager.load_project("dsgrid_conus_2022")
    query = QueryTestElectricityValuesCompositeDataset(
        registry_path, project, output_dir=output_dir
    )
    CompositeDatasetQuerySubmitter(project, output_dir).create_dataset(
        query.make_query(),
        persist_intermediate_table=persist_intermediate_table,
        load_cached_table=load_cached_table,
    )
    result = query.validate()
    print(f"Result of query {query.name} = {result}")

    query2 = QueryTestElectricityValuesCompositeDatasetAgg(
        registry_path, project, output_dir=output_dir, geography="county"
    )
    CompositeDatasetQuerySubmitter(project, output_dir).submit(query2.make_query())
    result = query2.validate()
    print(f"Result of query {query2.name} = {result}")

    query3 = QueryTestElectricityValuesCompositeDatasetAgg(
        registry_path, project, output_dir=output_dir, geography="state"
    )
    CompositeDatasetQuerySubmitter(project, output_dir).submit(query3.make_query())
    result = query3.validate()
    print(f"Result of query {query3.name} = {result}")
