import abc
import logging
import math
import shutil
import tempfile
from collections import defaultdict, namedtuple
from pathlib import Path

import pyspark.sql.functions as F
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
    DimensionQueryNamesModel,
    ProjectQueryParamsModel,
    ProjectQueryModel,
    QueryResultParamsModel,
    ReportInputModel,
    ReportType,
)
from dsgrid.query.query_submitter import ProjectQuerySubmitter, CompositeDatasetQuerySubmitter
from dsgrid.query.peak_load_report import PeakLoadInputModel, PeakLoadReport
from dsgrid.utils.run_command import check_run_command


REGISTRY_PATH = (
    Path(__file__).absolute().parent.parent
    / "dsgrid-test-data"
    / "filtered_registries"
    / "simple_standard_scenarios"
)

Datasets = namedtuple("Datasets", ["comstock", "resstock", "tempo"])
Tables = namedtuple("Tables", ["load_data", "lookup", "table"])

logger = logging.getLogger(__name__)


def test_electricity_values():
    run_query_test(QueryTestElectricityValues, True)
    run_query_test(QueryTestElectricityValues, False)


def test_electricity_use_by_county():
    run_query_test(QueryTestElectricityUse, "county", "sum")
    run_query_test(QueryTestElectricityUse, "county", "max")


def test_electricity_use_by_state():
    run_query_test(QueryTestElectricityUse, "state", "sum")
    run_query_test(QueryTestElectricityUse, "state", "max")


def test_total_electrictity_use_with_filter():
    run_query_test(QueryTestTotalElectricityUseWithFilter)


def test_total_electrictity_use_by_state_and_pca():
    run_query_test(QueryTestElectricityUseByStateAndPCA)


def test_diurnal_electrictity_use_by_pca_pre_post_concat():
    run_query_test(QueryTestDiurnalElectricityUseByCountyPrePostConcat)


def test_diurnal_electrictity_use_by_county_chained():
    run_query_test(QueryTestDiurnalElectricityUseByCountyChained)


def test_peak_load():
    run_query_test(QueryTestPeakLoadByStateSubsector)


def test_invalid_drop_pivoted_dimension():
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
            dataset_ids=[
                "conus_2022_reference_comstock",
                "conus_2022_reference_resstock",
            ],
            per_dataset_aggregations=[],
        ),
        result=QueryResultParamsModel(
            output_format="parquet",
        ),
    )
    project = get_project()
    output_dir = Path(tempfile.gettempdir()) / "queries"

    query.project.per_dataset_aggregations = [invalid_agg]
    with pytest.raises(DSGInvalidQuery):
        ProjectQuerySubmitter(project, output_dir).submit(query)

    query.project.per_dataset_aggregations = []
    query.result.aggregations = [invalid_agg]
    with pytest.raises(DSGInvalidQuery):
        ProjectQuerySubmitter(project, output_dir).submit(query)


def test_create_composite_dataset_query():
    output_dir = Path(tempfile.gettempdir()) / "queries"
    if output_dir.exists():
        shutil.rmtree(output_dir)

    project = get_project()
    try:
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
    finally:
        if output_dir.exists():
            shutil.rmtree(output_dir)


def test_query_cli_create_validate():
    filename = Path(tempfile.gettempdir()) / "query.json"
    try:
        cmd = (
            f"dsgrid query project create --offline --registry-path={REGISTRY_PATH} "
            f"-d -r -f {filename} -F expression -F column_operator "
            "-F supplemental_column_operator -F raw --force my_query dsgrid_conus_2022"
        )
        check_run_command(cmd)
        query = ProjectQueryModel.from_file(filename)
        assert query.name == "my_query"
        assert query.project.per_dataset_aggregations
        assert query.result.aggregations
        check_run_command(f"dsgrid query project validate {filename}")
    finally:
        if filename.exists():
            filename.unlink()


def test_query_cli_run():
    output_dir = Path(tempfile.gettempdir()) / "queries"
    if output_dir.exists():
        shutil.rmtree(output_dir)

    project = get_project()
    try:
        query = QueryTestElectricityValues(True, REGISTRY_PATH, project, output_dir=output_dir)
        filename = Path(tempfile.gettempdir()) / "query.json"
        filename.write_text(query.make_query().json())
        cmd = (
            f"dsgrid query project run --offline --registry-path={REGISTRY_PATH} "
            f"--output={output_dir} {filename}"
        )
        check_run_command(cmd)
        query.validate()
    finally:
        if output_dir.exists():
            shutil.rmtree(output_dir)


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
    _project = Project.load(
        "dsgrid_conus_2022",
        offline_mode=True,
        registry_path=REGISTRY_PATH,
    )
    return _project


def run_query_test(test_query_cls, *args):
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
            )
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
    def validate(self):
        """Validate the results

        Returns
        -------
        bool
            Return True when the validation is successful.

        """

    def get_filtered_county_id(self):
        filters = self._model.project.dimension_filters
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
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                    # "tempo_conus_2022",
                ],
                dimension_filters=[
                    # This is a nonsensical way to filter down to county 06037, but it tests
                    # the code with combinations of base and supplemental dimension filters.
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
                # filtered_datasets=[],
            ),
            result=QueryResultParamsModel(
                supplemental_columns=["state"],
                replace_ids_with_names=True,
            ),
        )
        if self._use_supplemental_dimension:
            self._model.project.dimension_filters.append(
                SupplementalDimensionFilterColumnOperatorModel(
                    dimension_type=DimensionType.METRIC,
                    dimension_query_name="electricity",
                )
            )
        else:
            self._model.project.dimension_filters.append(
                DimensionFilterExpressionModel(
                    dimension_type=DimensionType.METRIC,
                    dimension_query_name="end_use",
                    operator="==",
                    column="fuel_id",
                    value="electricity",
                )
            )
        return self._model

    def validate(self):
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
        # TODO: fraction will be removed eventually
        expected = ["electricity_cooling", "electricity_heating", "fraction"]
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
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                ],
                per_dataset_aggregations=[
                    AggregationModel(
                        dimensions=DimensionQueryNamesModel(
                            data_source=["data_source"],
                            geography=["county"],
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

    def validate(self):
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
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                ],
                dimension_filters=[
                    DimensionFilterExpressionModel(
                        dimension_type=DimensionType.GEOGRAPHY,
                        dimension_query_name="county",
                        operator="==",
                        value="06037",
                    ),
                ],
                per_dataset_aggregations=[
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
                ],
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

    def validate(self):
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
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                ],
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

    def validate(self):
        validate_county_diurnal_hourly(self.output_dir / self.name / "table.parquet")


class QueryTestDiurnalElectricityUseByCountyPrePostConcat(QueryTestBase):

    NAME = "diurnal_electricity_use_by_county_pre_post"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                ],
                per_dataset_aggregations=[
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
                ],
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
                output_format="parquet",
            ),
        )
        return self._model

    def validate(self):
        validate_county_diurnal_hourly(self.output_dir / self.name / "table.parquet")


class QueryTestElectricityUseByStateAndPCA(QueryTestBase):

    NAME = "total_electricity_use_by_state_and_pca"

    def make_query(self):
        self._model = ProjectQueryModel(
            name=self.NAME,
            project=ProjectQueryParamsModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                ],
                per_dataset_aggregations=[
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
            ),
            result=QueryResultParamsModel(
                output_format="parquet",
            ),
        )
        return self._model

    def validate(self):
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
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                ],
                per_dataset_aggregations=[
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
            ),
            result=QueryResultParamsModel(
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

    def validate(self):
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
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                    # "tempo_conus_2022",
                ],
                dimension_filters=[
                    SupplementalDimensionFilterColumnOperatorModel(
                        dimension_type=DimensionType.METRIC,
                        dimension_query_name="electricity",
                    ),
                ],
            ),
        )
        return self._model

    def validate(self):
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

    def validate(self):
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
        col = "all_electricity_sum"
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
    col = "all_electricity_sum"
    actual_ca = results.filter("state == 'CA'").collect()[0][col]
    actual_ny = results.filter("state == 'NY'").collect()[0][col]
    assert math.isclose(actual_ca, exp_ca)
    assert math.isclose(actual_ny, exp_ny)


def validate_county_diurnal_hourly(filename):
    df = read_parquet(filename)
    assert not {"all_electricity_sum", "county", "hour"}.difference(df.columns)

    val = df.filter("county == '06037'").filter("hour == 16").collect()[0].all_electricity_sum
    # Computed this value manually by reading a composite dataframe and running this
    # df = df.withColumn("elec", df.electricity_cooling + df.electricity_heating).drop("electricity_cooling", "electricity_heating")
    # df.groupBy("county", F.hour("time_est").alias("hour")).agg(F.mean(".elec")).sort("county", "hour").show()
    expected = 223597.40584404464
    assert math.isclose(val, expected)


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
            table = getattr(datasets, name).table
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
                (com["max"]["electricity"], res["max"]["electricity"], tem["sum"]["electricity"])
            ),
        },
    }


def read_datasets(path):
    datasets = Datasets(
        comstock=read_table(path / "data" / "conus_2022_reference_comstock" / "1.0.0"),
        resstock=read_table(path / "data" / "conus_2022_reference_resstock" / "1.0.0"),
        tempo=read_table(path / "data" / "tempo_conus_2022" / "1.0.0"),
    )
    return datasets


def read_table(path):
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    load_data = spark.read.parquet(str(path / "load_data.parquet")).cache()
    lookup = spark.read.parquet(str(path / "load_data_lookup.parquet")).cache()
    table = load_data.join(lookup, on="id").drop("id").cache()
    return Tables(load_data, lookup, table)


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
            # 2 scenarios x 2 model years
            val *= 4
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
    project = Project.load(
        "dsgrid_conus_2022",
        offline_mode=True,
        registry_path=registry_path,
    )
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
