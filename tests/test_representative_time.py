import math
import ibis
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import TimeZoneFormat, TimeDimensionType
from dsgrid.common import VALUE_COLUMN
from dsgrid.config.dimensions import (
    AlignedTimeSingleTimeZone,
    RepresentativePeriodTimeDimensionModel,
    MonthRangeModel,
    TimeRangeModel,
)
from dsgrid.config.date_time_dimension_config import (
    DateTimeDimensionConfig,
    DateTimeDimensionModel,
)
from dsgrid.config.representative_period_time_dimension_config import (
    RepresentativePeriodTimeDimensionConfig,
)
from dsgrid.dimension.time import (
    MeasurementType,
    TimeIntervalType,
    RepresentativePeriodFormat,
)
from dsgrid.ibis_api import read_csv
from dsgrid.tests.utils import use_duckdb
from dsgrid.utils.dataset import map_time_dimension_with_chronify


ONE_WEEKDAY_DAY_AND_ONE_WEEKEND_DAY_PER_MONTH_BY_HOUR_FILE = (
    Path("dsgrid-test-data")
    / "datasets"
    / "representative_time"
    / "OneWeekdayDayAndOneWeekendDayPerMonthByHour.csv"
)


@pytest.fixture(scope="module")
def one_weekday_day_and_one_weekend_day_per_month_by_hour_table():
    return read_csv(str(ONE_WEEKDAY_DAY_AND_ONE_WEEKEND_DAY_PER_MONTH_BY_HOUR_FILE))


def make_date_time_config():
    return DateTimeDimensionConfig(
        DateTimeDimensionModel(
            name="Time EST",
            description="test",
            class_name="Time",
            type=DimensionType.TIME,
            time_type=TimeDimensionType.DATETIME,
            measurement_type=MeasurementType.TOTAL,
            ranges=[
                TimeRangeModel(
                    start="2018-01-01 00:00:00",
                    end="2018-12-31 23:00:00",
                    str_format="%Y-%m-%d %H:%M:%S",
                    frequency=timedelta(hours=1),
                ),
            ],
            time_zone_format=AlignedTimeSingleTimeZone(
                format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
                time_zone="Etc/GMT+5",
            ),
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
        )
    )


def make_one_weekday_day_and_one_weekend_day_per_month_by_hour_config():
    return RepresentativePeriodTimeDimensionConfig(
        RepresentativePeriodTimeDimensionModel(
            name="one_weekday_day_and_one_weekend_day_per_month_by_hour",
            description="test",
            class_name="Time",
            type=DimensionType.TIME,
            time_type=TimeDimensionType.REPRESENTATIVE_PERIOD,
            measurement_type=MeasurementType.TOTAL,
            format=RepresentativePeriodFormat.ONE_WEEKDAY_DAY_AND_ONE_WEEKEND_DAY_PER_MONTH_BY_HOUR,
            ranges=[MonthRangeModel(start=1, end=12)],
            time_interval_type=TimeIntervalType.PERIOD_ENDING,
        )
    )


@pytest.mark.parametrize("spark_time_zone", ["America/Los_Angeles"], indirect=True)
def test_time_mapping(
    one_weekday_day_and_one_weekend_day_per_month_by_hour_table,
    spark_time_zone,
    scratch_dir_context,
):
    # This test sets the Spark session time zone so that it can check times consistently
    # across computers.
    # It uses Pacific Prevailing Time to make the checks consistent with the dataset.
    df = one_weekday_day_and_one_weekend_day_per_month_by_hour_table
    # This dataset has only California counties.
    df = df.mutate(time_zone=ibis.literal("America/Los_Angeles"))
    config = make_one_weekday_day_and_one_weekend_day_per_month_by_hour_config()
    project_time_config = make_date_time_config()
    mapped_df = map_time_dimension_with_chronify(
        df,
        VALUE_COLUMN,
        config,
        project_time_config,
        scratch_dir_context,
    )
    timestamps = (
        mapped_df.select("timestamp").distinct().order_by("timestamp").to_pyarrow().to_pylist()
    )
    zi = ZoneInfo("Etc/GMT+5")
    est_timestamps = [x["timestamp"].astimezone(zi) for x in timestamps]
    start = datetime(year=2018, month=1, day=1, tzinfo=zi)
    resolution = timedelta(hours=1)
    expected_timestamps = [start + i * resolution for i in range(8760)]
    assert est_timestamps == expected_timestamps
    assert mapped_df.filter(mapped_df[VALUE_COLUMN].isnull()).count().execute() == 0

    max_value = df.select(VALUE_COLUMN).to_pandas()[VALUE_COLUMN].max()
    max_row = df.filter(df["value"] == max_value).to_pyarrow().to_pylist()[0]
    # Expected max is determined by manually inspecting the file.
    expected_max = 0.9995036580360138
    assert math.isclose(max_value, expected_max)
    expected_scenario = "scenario1"
    expected_month = 3
    expected_hour = 3
    # -1 hour for period ending to beginning
    expected_hour_pst = expected_hour - 1
    expected_geo = "06073"
    expected_model_year = "2030"
    num_weekdays_in_march_2018 = 22
    assert max_row["month"] == expected_month
    assert max_row["hour"] == expected_hour
    assert max_row["is_weekday"]
    assert str(max_row["geography"]) == expected_geo
    assert str(max_row["model_year"]) == expected_model_year
    assert max_row["scenario"] == expected_scenario
    mapped_df_at_max = mapped_df.filter(mapped_df["value"] == max_value)
    from dsgrid.ibis_api import get_ibis_connection

    get_ibis_connection().create_view("tmp_view", mapped_df, overwrite=True)
    if use_duckdb():
        func = "ISODOW"
        saturday = 6
    else:
        func = "WEEKDAY"
        saturday = 5

    query = f"""
        SELECT *
        FROM tmp_view
        WHERE (
            model_year = '{max_row["model_year"]}'
            AND geography = '{max_row["geography"]}'
            AND scenario = '{max_row["scenario"]}'
            AND MONTH(timestamp) = {max_row["month"]}
            AND HOUR(timestamp) = {expected_hour_pst}
            AND {func}(timestamp) < {saturday}
        )
    """
    from dsgrid.ibis_api import get_ibis_connection

    filtered_mapped_df = get_ibis_connection().sql(query)
    assert mapped_df_at_max.count().execute() == num_weekdays_in_march_2018
    assert filtered_mapped_df.count().execute() == num_weekdays_in_march_2018
    assert filtered_mapped_df.select("value").distinct().count().execute() == 1
    assert math.isclose(
        filtered_mapped_df.select("value").distinct().to_pyarrow().to_pylist()[0]["value"],
        expected_max,
    )
    assert (
        mapped_df_at_max.order_by("timestamp").to_pyarrow().to_pylist()
        == filtered_mapped_df.order_by("timestamp").to_pyarrow().to_pylist()
    )
