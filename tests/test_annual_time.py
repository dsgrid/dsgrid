import pyspark.sql.functions as F
import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.config.annual_time_dimension_config import (
    AnnualTimeDimensionConfig,
    AnnualTimeDimensionModel,
)
from dsgrid.config.date_time_dimension_config import (
    DateTimeDimensionConfig,
    DateTimeDimensionModel,
)
from dsgrid.config.dimensions import TimeRangeModel
from dsgrid.dimension.time import (
    MeasurementType,
    TimeIntervalType,
    TimeZone,
)
from dsgrid.utils.spark import get_spark_session


@pytest.fixture
def annual_dataframe():
    spark = get_spark_session()
    df = spark.createDataFrame(
        [
            {
                "time_year": "2019",
                "geography": "CO",
                "electricity_sales": 602872.1,
            },
            {
                "time_year": "2020",
                "geography": "CO",
                "electricity_sales": 702872.1,
            },
            {
                "time_year": "2021",
                "geography": "CO",
                "electricity_sales": 802872.1,
            },
            {
                "time_year": "2022",
                "geography": "CO",
                "electricity_sales": 902872.1,
            },
        ]
    )
    yield df


@pytest.fixture
def annual_time_dimension():
    yield AnnualTimeDimensionConfig(
        AnnualTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="AnnualTime",
            module="dsgrid.dimension.standard",
            name="annual_time",
            description="test annual time",
            display_name="Annual",
            ranges=[
                TimeRangeModel(start="2010", end="2020"),
            ],
            str_format="%Y",
            measurement_type=MeasurementType.TOTAL,
            include_leap_day=True,
        )
    )


@pytest.fixture
def date_time_dimension():
    yield DateTimeDimensionConfig(
        DateTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="Time",
            module="dsgrid.dimension.standard",
            frequency="P0DT1H",
            timezone=TimeZone.EST,
            name="datetime",
            description="example date time",
            display_name="time_est",
            ranges=[
                TimeRangeModel(start="2012-02-01 00:00:00", end="2012-02-07 23:00:00"),
            ],
            str_format="%Y-%m-%d %H:%M:%S",
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
            measurement_type=MeasurementType.TOTAL,
        )
    )


def test_map_annual_time_measured_to_datetime(
    annual_dataframe, annual_time_dimension, date_time_dimension
):
    annual_time_dimension.model.measurement_type = MeasurementType.MEASURED
    model_years = [2020, 2024]
    df = annual_time_dimension.map_annual_time_measured_to_datetime(
        annual_dataframe, date_time_dimension, model_years
    )
    assert df.count() == 168
    values = df.select("electricity_sales").distinct().collect()
    assert len(values) == 1
    assert (
        values[0].electricity_sales
        == annual_dataframe.filter("time_year == 2020").collect()[0].electricity_sales
    )
    mapped_model_years = (
        df.withColumn("year", F.year(df.timestamp)).select("year").distinct().collect()
    )
    assert len(mapped_model_years) == 1
    assert mapped_model_years[0].year == 2020


def test_map_annual_time_total_to_datetime(
    annual_dataframe, annual_time_dimension, date_time_dimension
):
    annual_time_dimension.model.measurement_type = MeasurementType.TOTAL
    model_years = [2020, 2024]
    value_columns = ["electricity_sales"]
    df = annual_time_dimension.map_annual_total_to_datetime(
        annual_dataframe, date_time_dimension, model_years, value_columns
    )
    assert df.count() == 168
    values = df.select("electricity_sales").distinct().collect()
    assert len(values) == 1
    num_intervals_2020 = (
        366 * 24 / (date_time_dimension.model.frequency.total_seconds() / (60 * 60))
    )
    assert (
        values[0].electricity_sales
        == annual_dataframe.filter("time_year == 2020").collect()[0].electricity_sales
        / num_intervals_2020
    )
    mapped_model_years = (
        df.withColumn("year", F.year(df.timestamp)).select("year").distinct().collect()
    )
    assert len(mapped_model_years) == 1
    assert mapped_model_years[0].year == 2020
