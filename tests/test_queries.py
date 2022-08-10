import abc
import logging
import math
import shutil
import tempfile
from collections import defaultdict, namedtuple
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DoubleType
import pyspark.sql.functions as F

from dsgrid.dimension.base_models import DimensionType
from dsgrid.loggers import setup_logging
from dsgrid.project import Project
from dsgrid.query.models import (
    AggregationModel,
    ProjectQueryModel,
    ProjectQueryResultModel,
    CreateDerivedDatasetQueryModel,
    DerivedDatasetQueryModel,
    MetricReductionModel,
)
from dsgrid.dataset.dimension_filters import (
    DimensionFilterValueModel,
    DimensionFilterExpressionModel,
    DimensionFilterColumnOperatorModel,
)
from dsgrid.query.query_submitter import ProjectQuerySubmitter, DerivedDatasetQuerySubmitter


REGISTRY_PATH = (
    Path(__file__).absolute().parent.parent
    / "dsgrid-test-data"
    / "filtered_registries"
    / "simple_standard_scenarios"
)

Datasets = namedtuple("Datasets", ["comstock", "resstock", "tempo"])
Tables = namedtuple("Tables", ["load_data", "lookup", "table"])

logger = logging.getLogger(__name__)


def test_electricity_queries():
    output_dir = Path(tempfile.gettempdir()) / "queries"
    if output_dir.exists():
        shutil.rmtree(output_dir)

    project = Project.load(
        "dsgrid_conus_2022",
        offline_mode=True,
        registry_path=REGISTRY_PATH,
    )
    try:
        queries = [
            QueryTestElectricityAgg("sum", REGISTRY_PATH, project, output_dir=output_dir),
            QueryTestElectricityAgg("max", REGISTRY_PATH, project, output_dir=output_dir),
            QueryTestElectricityValues(REGISTRY_PATH, project, output_dir=output_dir),
            QueryTestElectricityUseByCounty(REGISTRY_PATH, project, output_dir=output_dir),
            QueryTestElectricityUseByState(REGISTRY_PATH, project, output_dir=output_dir),
        ]
        for query in queries:
            for load_cached_table in (False, True):
                ProjectQuerySubmitter(project, output_dir).submit(
                    query.make_query(),
                    persist_intermediate_table=True,
                    load_cached_table=load_cached_table,
                )
                assert query.validate()
    finally:
        shutil.rmtree(output_dir)


def test_derived_dataset_queries():
    output_dir = Path(tempfile.gettempdir()) / "queries"
    if output_dir.exists():
        shutil.rmtree(output_dir)

    project = Project.load(
        "dsgrid_conus_2022",
        offline_mode=True,
        registry_path=REGISTRY_PATH,
    )
    try:
        query = QueryTestElectricityValuesDerivedDataset(
            REGISTRY_PATH, project, output_dir=output_dir
        )
        DerivedDatasetQuerySubmitter(project, output_dir).create_dataset(
            query.make_query(),
            persist_intermediate_table=True,
        )
        assert query.validate()

        query2 = QueryTestElectricityValuesDerivedDatasetAgg(
            REGISTRY_PATH, project, output_dir=output_dir, group_by_columns=["county"]
        )
        DerivedDatasetQuerySubmitter(project, output_dir).submit(query2.make_query())
        assert query2.validate()

        query3 = QueryTestElectricityValuesDerivedDatasetAgg(
            REGISTRY_PATH, project, output_dir=output_dir, group_by_columns=["state"]
        )
        DerivedDatasetQuerySubmitter(project, output_dir).submit(query3.make_query())
        assert query3.validate()
    finally:
        shutil.rmtree(output_dir)


class QueryTestBase(abc.ABC):
    """Base class for all test queries"""

    stats = None

    def __init__(self, registry_path, project, output_dir=Path("queries")):
        self._registry_path = Path(registry_path)
        self._project = project
        self._output_dir = Path(output_dir)
        self._model = None
        self._spark = SparkSession.builder.appName("dgrid").getOrCreate()

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

    def get_expected_ca_max_electricity(self):
        by_county = self.get_raw_stats()["by_county"]
        return max(
            (
                by_county["06037"]["comstock_resstock"]["max"]["electricity"],
                by_county["06073"]["comstock_resstock"]["max"]["electricity"],
            )
        )

    def get_expected_ny_max_electricity(self):
        by_county = self.get_raw_stats()["by_county"]
        return max(
            (
                by_county["36047"]["comstock_resstock"]["max"]["electricity"],
                by_county["36081"]["comstock_resstock"]["max"]["electricity"],
            )
        )

    def get_expected_ca_sum_electricity(self):
        by_county = self.get_raw_stats()["by_county"]
        return (
            by_county["06037"]["comstock_resstock"]["sum"]["electricity"]
            + by_county["06073"]["comstock_resstock"]["sum"]["electricity"]
        )

    def get_expected_ny_sum_electricity(self):
        by_county = self.get_raw_stats()["by_county"]
        return (
            by_county["36047"]["comstock_resstock"]["sum"]["electricity"]
            + by_county["36081"]["comstock_resstock"]["sum"]["electricity"]
        )


class QueryTestElectricityAgg(QueryTestBase):

    NAME = "electricity-agg-06037"

    def __init__(self, operation, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._operation = operation

    def make_query(self):
        name = self.NAME
        self._model = ProjectQueryResultModel(
            name=name,
            project=ProjectQueryModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                    # "tempo_conus_2022",
                ],
                # TODO: need to allow min/max/sum of metric on other supplemental dimensions
                dimension_filters=[
                    DimensionFilterExpressionModel(
                        dimension_type=DimensionType.GEOGRAPHY,
                        dimension_query_name="county",
                        operator="==",
                        value="06037",
                    ),
                ],
                metric_reductions=[
                    MetricReductionModel(
                        dimension_query_name="electricity",
                        operation=self._operation,
                    ),
                ],
                # filtered_datasets=[],
            ),
        )
        return self._model

    def get_output_electricity_op(self, operation):
        df = self._spark.read.parquet(str(self.output_dir / self.name / "table.parquet"))
        value = perform_op(df, f"all_electricity_{operation.__name__}", operation)
        return value

    def validate(self):
        county = self.get_filtered_county_id()
        expected_base = self.get_raw_stats()["by_county"][county]["comstock_resstock"]
        if self._operation == "sum":
            expected = expected_base["sum"]["electricity"]
            actual = self.get_output_electricity_op(F.sum)
        elif self._operation == "max":
            expected = expected_base["max"]["electricity"]
            actual = self.get_output_electricity_op(F.max)
        else:
            logger.warning("Validation is not yet supported for %s", self._operation)
            return True

        result = math.isclose(expected, actual)
        if not result:
            ratio = actual / expected
            logger.error(
                "Mismatch in values. actual=%s expected=%s ratio=%s", actual, expected, ratio
            )
        return result


class QueryTestElectricityValues(QueryTestBase):

    NAME = "electricity-values"

    def make_query(self):
        self._model = ProjectQueryResultModel(
            name=self.NAME,
            project=ProjectQueryModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                    # "tempo_conus_2022",
                ],
                dimension_filters=[
                    DimensionFilterColumnOperatorModel(
                        dimension_type=DimensionType.METRIC,
                        dimension_query_name="electricity",
                        operator="like",
                        value="%",
                    ),
                    DimensionFilterValueModel(
                        dimension_type=DimensionType.GEOGRAPHY,
                        dimension_query_name="county",
                        value="06037",
                    ),
                ],
                # drop_dimensions=[DimensionType.SUBSECTOR],
                # filtered_datasets=[],
            ),
            supplemental_columns=["state"],
            replace_ids_with_names=True,
        )
        return self._model

    def validate(self):
        county = self.get_filtered_county_id()
        county_name = (
            self._project.config.get_dimension_records("county")
            .filter(f"id == {county}")
            .collect()[0]
            .name
        )
        df = self._spark.read.parquet(str(self.output_dir / self.name / "table.parquet"))
        assert "natural_gas_heating" not in df.columns
        non_value_columns = set(self._project.config.get_base_dimension_query_names().values())
        non_value_columns.update({"id", "timestamp"})
        non_value_columns.update(self._model.supplemental_columns)
        value_columns = sorted((x for x in df.columns if x not in non_value_columns))
        # TODO: fraction will be removed eventually
        expected = ["electricity_cooling", "electricity_heating", "fraction"]
        success = value_columns == expected
        if not success:
            logger.error("Mismatch in columns: actual=%s expected=%s", value_columns, expected)
        if set(self._model.supplemental_columns).difference(df.columns):
            logger.error(
                "supplemental_columns=%s are not present in table",
                self._model.supplemental_columns,
            )
            success = False
        if not df.select("county").distinct().filter(f"county == '{county_name}'").collect():
            logger.error("County name = %s is not present", county_name)
            success = False
        if success:
            total_cooling = df.agg(F.sum("electricity_cooling").alias("sum")).collect()[0].sum
            total_heating = df.agg(F.sum("electricity_heating").alias("sum")).collect()[0].sum
            expected = self.get_raw_stats()["by_county"][county]["comstock_resstock"]["sum"]
            if not math.isclose(total_cooling, expected["electricity_cooling"]):
                logger.error(
                    "Mismatch in electricity_cooling: actual=%s expected=%s",
                    total_cooling,
                    expected["electricity_cooling"],
                )
                success = False
            if not math.isclose(total_heating, expected["electricity_heating"]):
                logger.error(
                    "Mismatch in electricity_heating: actual=%s expected=%s",
                    total_heating,
                    expected["electricity_heating"],
                )
                success = False
        return success


class QueryTestElectricityUseByCounty(QueryTestBase):

    NAME = "electricity-use-by-county"

    def make_query(self):
        self._model = ProjectQueryResultModel(
            name=self.NAME,
            project=ProjectQueryModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                    # "tempo_conus_2022",
                ],
                # TODO: Should this filter out non-electricity columns? Is that what we want?
                # We should leave non-electicity columns in the table.
                metric_reductions=[
                    MetricReductionModel(dimension_query_name="electricity", operation="sum")
                ],
            ),
            aggregations=[
                AggregationModel(
                    group_by_columns=["county"],
                    aggregation_function=F.sum,
                    name="sum",
                ),
                AggregationModel(
                    group_by_columns=["county"],
                    aggregation_function=F.max,
                    name="max",
                ),
            ],
            output_format="csv",
        )
        return self._model

    def validate(self):
        return validate_electricity_use_by_county(
            self.output_dir / self.name / "sum" / "table.csv",
            self.output_dir / self.name / "max" / "table.csv",
            self.get_raw_stats(),
        )


class QueryTestElectricityUseByState(QueryTestBase):

    NAME = "max-electricity-use-by-state"

    def make_query(self):
        self._model = ProjectQueryResultModel(
            name=self.NAME,
            project=ProjectQueryModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                    # "tempo_conus_2022",
                ],
                metric_reductions=[
                    MetricReductionModel(dimension_query_name="electricity", operation="max")
                ],
            ),
            aggregations=[
                AggregationModel(
                    group_by_columns=["state"],
                    aggregation_function=F.max,
                    name="max",
                ),
            ],
            replace_ids_with_names=True,
            output_format="csv",
        )
        return self._model

    def validate(self):
        results_max = self._spark.read.csv(
            str(self.output_dir / self.name / "max" / "table.csv"),
            header=True,
            schema=StructType(
                [
                    StructField("max(all_electricity_max)", DoubleType()),
                    StructField("state", StringType()),
                ]
            ),
        )
        actual_ca = results_max.filter("state == 'California'").collect()[0][
            "max(all_electricity_max)"
        ]
        actual_ny = results_max.filter("state == 'New York'").collect()[0][
            "max(all_electricity_max)"
        ]
        exp_ca_max = self.get_expected_ca_max_electricity()
        exp_ny_max = self.get_expected_ny_max_electricity()
        success = True
        if not math.isclose(actual_ca, exp_ca_max):
            logger.error("Mismatch in CA max values actual=%s expected=%s", actual_ca, exp_ca_max)
            success = False
        if not math.isclose(actual_ny, exp_ny_max):
            logger.error("Mismatch in ny max values actual=%s expected=%s", actual_ny, exp_ny_max)
            success = False

        return success


class QueryTestElectricityValuesDerivedDataset(QueryTestBase):

    NAME = "electricity-values"

    def make_query(self):
        self._model = CreateDerivedDatasetQueryModel(
            name=self.NAME,
            dataset_id="com_res",
            project=ProjectQueryModel(
                project_id="dsgrid_conus_2022",
                include_dsgrid_dataset_components=False,
                dataset_ids=[
                    "conus_2022_reference_comstock",
                    "conus_2022_reference_resstock",
                    # "tempo_conus_2022",
                ],
                dimension_filters=[
                    DimensionFilterColumnOperatorModel(
                        dimension_type=DimensionType.METRIC,
                        dimension_query_name="electricity",
                        operator="like",
                        value="%",
                    ),
                ],
            ),
        )
        return self._model

    def validate(self):
        df = self._spark.read.parquet(
            str(self.output_dir / "derived_datasets" / self._model.dataset_id / "dataset.parquet")
        )
        assert "natural_gas_heating" not in df.columns
        non_value_columns = set(self._project.config.get_base_dimension_query_names().values())
        non_value_columns.update({"id", "timestamp"})
        non_value_columns.update(self._model.supplemental_columns)
        value_columns = sorted((x for x in df.columns if x not in non_value_columns))
        # TODO: fraction will be removed eventually
        expected = ["electricity_cooling", "electricity_heating", "fraction"]
        success = value_columns == expected
        if not success:
            logger.error("Mismatch in columns: actual=%s expected=%s", value_columns, expected)
        if set(self._model.supplemental_columns).difference(df.columns):
            logger.error(
                "supplemental_columns=%s are not present in table",
                self._model.supplemental_columns,
            )
            success = False

        if success:
            total_cooling = df.agg(F.sum("electricity_cooling").alias("sum")).collect()[0].sum
            total_heating = df.agg(F.sum("electricity_heating").alias("sum")).collect()[0].sum
            expected = self.get_raw_stats()["overall"]["comstock_resstock"]["sum"]
            if not math.isclose(total_cooling, expected["electricity_cooling"]):
                logger.error(
                    "Mismatch in electricity_cooling: actual=%s expected=%s",
                    total_cooling,
                    expected["electricity_cooling"],
                )
                success = False
            if not math.isclose(total_heating, expected["electricity_heating"]):
                logger.error(
                    "Mismatch in electricity_heating: actual=%s expected=%s",
                    total_heating,
                    expected["electricity_heating"],
                )
                success = False
        return success


class QueryTestElectricityValuesDerivedDatasetAgg(QueryTestBase):

    NAME = "electricity-values-agg-from-derived-dataset"

    def __init__(self, *args, group_by_columns=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._group_by_columns = group_by_columns or ["county"]

    def make_query(self):
        self._model = DerivedDatasetQueryModel(
            name=self.NAME,
            dataset_id="com_res",
            metric_reductions=[
                MetricReductionModel(
                    dimension_query_name="electricity",
                    operation="sum",
                ),
            ],
            aggregations=[
                AggregationModel(
                    group_by_columns=self._group_by_columns,
                    aggregation_function=F.sum,
                    name="sum",
                ),
                AggregationModel(
                    group_by_columns=self._group_by_columns,
                    aggregation_function=F.max,
                    name="max",
                ),
            ],
            output_format="csv",
        )
        return self._model

    def validate(self):
        if self._group_by_columns == ["county"]:
            return validate_electricity_use_by_county(
                self.output_dir / self.name / "sum" / "table.csv",
                self.output_dir / self.name / "max" / "table.csv",
                self.get_raw_stats(),
            )
        elif self._group_by_columns == ["state"]:
            exp_ca_max = self.get_expected_ca_max_electricity()
            exp_ny_max = self.get_expected_ny_max_electricity()
            exp_ca_sum = self.get_expected_ca_sum_electricity()
            exp_ny_sum = self.get_expected_ny_sum_electricity()
            results_sum = self._spark.read.csv(
                str(self.output_dir / self.name / "sum" / "table.csv"),
                header=True,
                schema=StructType(
                    [
                        StructField("state", StringType()),
                        StructField("sum(all_electricity_sum)", DoubleType()),
                    ]
                ),
            )
            results_max = self._spark.read.csv(
                str(self.output_dir / self.name / "max" / "table.csv"),
                header=True,
                schema=StructType(
                    [
                        StructField("state", StringType()),
                        StructField("max(all_electricity_sum)", DoubleType()),
                    ]
                ),
            )
            actual_ca_sum = results_sum.filter("state == 'CA'").collect()[0][
                "sum(all_electricity_sum)"
            ]
            actual_ny_sum = results_sum.filter("state == 'NY'").collect()[0][
                "sum(all_electricity_sum)"
            ]
            actual_ca_max = results_max.filter("state == 'CA'").collect()[0][
                "max(all_electricity_sum)"
            ]
            actual_ny_max = results_max.filter("state == 'NY'").collect()[0][
                "max(all_electricity_sum)"
            ]
            success = True
            if not math.isclose(actual_ca_max, exp_ca_max):
                logger.error(
                    "Mismatch in CA max electricity actual=%s expected=%s",
                    actual_ca_max,
                    exp_ca_max,
                )
                success = False
            if not math.isclose(actual_ny_max, exp_ny_max):
                logger.error(
                    "Mismatch in NY max electricity actual=%s expected=%s",
                    actual_ny_max,
                    exp_ny_max,
                )
                success = False
            if not math.isclose(actual_ca_sum, exp_ca_sum):
                logger.error(
                    "Mismatch in CA sum electricity actual=%s expected=%s",
                    actual_ca_sum,
                    exp_ca_sum,
                )
                success = False
            if not math.isclose(actual_ny_sum, exp_ny_sum):
                logger.error(
                    "Mismatch in NY sum electricity actual=%s expected=%s",
                    actual_ny_sum,
                    exp_ny_sum,
                )
                success = False
            return success
        else:
            logger.warning(
                "Validation of is not supported with group_by_columns=%s", self._group_by_columns
            )
        return True


def perform_op(df, column, operation):
    return df.select(column).agg(operation(column).alias("tmp_col")).collect()[0].tmp_col


def validate_electricity_use_by_county(results_sum_path, results_max_path, raw_stats):
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    results_sum = spark.read.csv(
        str(results_sum_path),
        header=True,
        schema=StructType(
            [
                StructField("county", StringType()),
                StructField("sum(all_electricity_sum)", DoubleType()),
            ],
        ),
    )
    results_max = spark.read.csv(
        str(results_max_path),
        header=True,
        schema=StructType(
            [
                StructField("county", StringType()),
                StructField("max(all_electricity_sum)", DoubleType()),
            ]
        ),
    )
    counties = [str(x.county) for x in results_sum.select("county").collect()]
    assert len(counties) == 4, counties
    success = True
    stats = raw_stats["by_county"]
    for op, results in zip(("sum", "max"), (results_sum, results_max)):
        for county in counties:
            col = f"{op}(all_electricity_sum)"
            actual = results.filter(f"county == '{county}'").collect()[0][col]
            expected = stats[county]["comstock_resstock"][op]["electricity"]
            if not math.isclose(actual, expected):
                logger.error(
                    "Mismatch in operation=%s actual=%s expected=%s", op, actual, expected
                )
                success = False

    return success


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
        query = QueryTestElectricityValues(registry_path, project, output_dir=output_dir)
    elif dimension_query_name == QueryTestElectricityAgg.NAME:
        query = QueryTestElectricityAgg(operation, registry_path, project, output_dir=output_dir)
    elif dimension_query_name == QueryTestElectricityUseByCounty.NAME:
        query = QueryTestElectricityUseByCounty(registry_path, project, output_dir=output_dir)
    elif dimension_query_name == QueryTestElectricityUseByState.NAME:
        query = QueryTestElectricityUseByState(registry_path, project, output_dir=output_dir)
    else:
        raise Exception(f"no query for {dimension_query_name}")

    ProjectQuerySubmitter(project, output_dir).submit(
        query.make_query(),
        persist_intermediate_table=persist_intermediate_table,
        load_cached_table=load_cached_table,
    )
    result = query.validate()
    print(f"Result of query {query.name} = {result}")


def run_derived_dataset(
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
    query = QueryTestElectricityValuesDerivedDataset(registry_path, project, output_dir=output_dir)
    DerivedDatasetQuerySubmitter(project, output_dir).create_dataset(
        query.make_query(),
        persist_intermediate_table=persist_intermediate_table,
        load_cached_table=load_cached_table,
    )
    result = query.validate()
    print(f"Result of query {query.name} = {result}")

    query2 = QueryTestElectricityValuesDerivedDatasetAgg(
        registry_path, project, output_dir=output_dir, group_by_columns=["county"]
    )
    DerivedDatasetQuerySubmitter(project, output_dir).submit(query2.make_query())
    result = query2.validate()
    print(f"Result of query {query2.name} = {result}")

    query3 = QueryTestElectricityValuesDerivedDatasetAgg(
        registry_path, project, output_dir=output_dir, group_by_columns=["state"]
    )
    DerivedDatasetQuerySubmitter(project, output_dir).submit(query3.make_query())
    result = query3.validate()
    print(f"Result of query {query3.name} = {result}")
