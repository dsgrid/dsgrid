import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.config.annual_time_dimension_config import (
    AnnualTimeDimensionConfig,
    AnnualTimeDimensionModel,
    map_annual_time_to_date_time,
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
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.utils.dataset import check_historical_annual_time_model_year_consistency
from dsgrid.spark.types import F, use_duckdb
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

    df = create_dataframe_from_dicts(data)
    if not use_duckdb():
        df.cache()
        df.count()
    yield df


@pytest.fixture
def annual_dataframe_with_model_year_values():
    yield [
        {
            "time_year": 2019,
            "model_year": 2019,
            "geography": "CO",
            "electricity_sales": 602872.1,
        },
        {
            "time_year": 2020,
            "model_year": 2020,
            "geography": "CO",
            "electricity_sales": 702872.1,
        },
        {
            "time_year": 2021,
            "model_year": 2021,
            "geography": "CO",
            "electricity_sales": 802872.1,
        },
        {
            "time_year": None,
            "model_year": 2022,
            "geography": "CO",
            "electricity_sales": 902872.1,
        },
    ]


@pytest.fixture
def annual_dataframe_with_model_year_valid(annual_dataframe_with_model_year_values):
    data = annual_dataframe_with_model_year_values
    df = create_dataframe_from_dicts(data)
    if not use_duckdb():
        df.cache()
    yield df, "time_year", "model_year"


@pytest.fixture
def annual_dataframe_with_model_year_invalid(annual_dataframe_with_model_year_values):
    data = annual_dataframe_with_model_year_values
    data.append(
        {
            "time_year": 2023,
            "model_year": 2019,
            "geography": "CO",
            "electricity_sales": 702872.1,
        },
    )
    df = create_dataframe_from_dicts(data)
    if not use_duckdb():
        df.cache()
    yield df, "time_year", "model_year"


@pytest.fixture
def annual_time_dimension():
    yield AnnualTimeDimensionConfig(
        AnnualTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="AnnualTime",
            module="dsgrid.dimension.standard",
            name="annual_time",
            description="test annual time",
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
            ranges=[
                TimeRangeModel(start="2012-02-01 00:00:00", end="2012-02-07 23:00:00"),
            ],
            str_format="%Y-%m-%d %H:%M:%S",
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
            measurement_type=MeasurementType.TOTAL,
        )
    )


def test_map_annual_time_total_to_datetime(
    annual_dataframe, annual_time_dimension, date_time_dimension
):
    annual_time_dimension.model.measurement_type = MeasurementType.TOTAL
    value_columns = {"electricity_sales"}
    df = map_annual_time_to_date_time(
        annual_dataframe,
        annual_time_dimension,
        date_time_dimension,
        value_columns,
    )
    expected_by_year = {
        x.time_year: x.electricity_sales / (366 * 24) for x in annual_dataframe.collect()
    }
    num_rows = annual_dataframe.count()
    num_timestamps = 24 * 7
    assert df.count() == num_rows * num_timestamps
    values = df.select("model_year", "electricity_sales").distinct().collect()
    assert len(values) == num_rows
    by_year = {x.model_year: x.electricity_sales for x in values}
    assert len(by_year) == len(expected_by_year)
    for year in by_year:
        assert by_year[year] == expected_by_year[int(year)]

    time_col = date_time_dimension.get_load_data_time_columns()[0]
    count_timestamps_per_model_year = (
        df.groupBy("model_year")
        .agg(F.count(time_col).alias("count_timestamps"))
        .select("count_timestamps")
        .distinct()
        .collect()
    )
    assert len(count_timestamps_per_model_year) == 1
    assert count_timestamps_per_model_year[0]["count_timestamps"] == num_timestamps


def test_historical_annual_model_year_consistency_valid(annual_dataframe_with_model_year_valid):
    df, time_col, model_year_col = annual_dataframe_with_model_year_valid
    check_historical_annual_time_model_year_consistency(df, time_col, model_year_col)


def test_historical_annual_model_year_consistency_invalid(
    annual_dataframe_with_model_year_invalid,
):
    df, time_col, model_year_col = annual_dataframe_with_model_year_invalid
    with pytest.raises(DSGInvalidDataset):
        check_historical_annual_time_model_year_consistency(df, time_col, model_year_col)
