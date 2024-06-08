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
from dsgrid.utils.spark import create_dataframe_from_dicts


@pytest.fixture(scope="module")
def annual_dataframe():
    data = [
        {
            "time_year": 2019,
            "geography": "CO",
            "electricity_sales": 602872.1,
        },
        {
            "time_year": 2020,
            "geography": "CO",
            "electricity_sales": 702872.1,
        },
        {
            "time_year": 2021,
            "geography": "CO",
            "electricity_sales": 802872.1,
        },
        {
            "time_year": 2022,
            "geography": "CO",
            "electricity_sales": 902872.1,
        },
    ]
    yield create_dataframe_from_dicts(data).cache()


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
    df = annual_time_dimension.map_annual_time_measured_to_datetime(
        annual_dataframe, date_time_dimension
    )
    expected_by_year = {x.time_year: x.electricity_sales for x in annual_dataframe.collect()}
    _check_values(annual_dataframe, date_time_dimension, df, expected_by_year)


def test_map_annual_time_total_to_datetime(
    annual_dataframe, annual_time_dimension, date_time_dimension
):
    annual_time_dimension.model.measurement_type = MeasurementType.TOTAL
    value_columns = ["electricity_sales"]
    df = annual_time_dimension.map_annual_total_to_datetime(
        annual_dataframe, date_time_dimension, value_columns
    )
    expected_by_year = {
        x.time_year: x.electricity_sales / (366 * 24) for x in annual_dataframe.collect()
    }
    _check_values(annual_dataframe, date_time_dimension, df, expected_by_year)


def _check_values(annual_dataframe, date_time_dimension, df, expected_by_year):
    num_rows = annual_dataframe.count()
    num_timestamps = 24 * 7
    assert df.count() == num_rows * num_timestamps
    values = df.select("time_year", "electricity_sales").distinct().collect()
    assert len(values) == num_rows
    by_year = {x.time_year: x.electricity_sales for x in values}
    assert len(by_year) == len(expected_by_year)
    for year in by_year:
        assert by_year[year] == expected_by_year[year]

    time_col = date_time_dimension.get_load_data_time_columns()[0]
    timestamps = (
        df.groupBy("time_year")
        .agg(F.count(time_col).alias("count_timestamps"))
        .select("count_timestamps")
        .distinct()
        .collect()
    )
    assert len(timestamps) == 1
    assert timestamps[0]["count_timestamps"] == num_timestamps
