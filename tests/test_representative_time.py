import math
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import TimeDimensionType, TimeZone
from dsgrid.common import VALUE_COLUMN
from dsgrid.config.dimensions import (
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
from dsgrid.spark.functions import (
    aggregate_single_value,
    init_spark,
    is_dataframe_empty,
)
from dsgrid.spark.types import (
    BooleanType,
    ByteType,
    DoubleType,
    F,
    StringType,
    StructType,
    StructField,
    use_duckdb,
)
from dsgrid.utils.dataset import (
    map_time_dimension_with_chronify_duckdb,
    map_time_dimension_with_chronify_spark_path,
)
from dsgrid.utils.spark import (
    get_spark_session,
    persist_table,
    read_dataframe,
)


ONE_WEEKDAY_DAY_AND_ONE_WEEKEND_DAY_PER_MONTH_BY_HOUR_FILE = (
    Path("dsgrid-test-data")
    / "datasets"
    / "representative_time"
    / "OneWeekdayDayAndOneWeekendDayPerMonthByHour.csv"
)


@pytest.fixture(scope="module")
def one_weekday_day_and_one_weekend_day_per_month_by_hour_table():
    spark = init_spark()
    schema = StructType(
        [
            StructField("scenario", StringType(), False),
            StructField("model_year", StringType(), False),
            StructField("geography", StringType(), False),
            StructField("subsector", StringType(), False),
            StructField("metric", StringType(), False),
            StructField("month", ByteType(), False),
            StructField("hour", ByteType(), False),
            StructField("is_weekday", BooleanType(), False),
            StructField("value", DoubleType(), False),
        ]
    )
    return spark.read.csv(
        str(ONE_WEEKDAY_DAY_AND_ONE_WEEKEND_DAY_PER_MONTH_BY_HOUR_FILE), schema=schema, header=True
    )


def make_date_time_config():
    return DateTimeDimensionConfig(
        DateTimeDimensionModel(
            name="Time EST",
            description="test",
            class_name="Time",
            type=DimensionType.TIME,
            time_type=TimeDimensionType.DATETIME,
            measurement_type=MeasurementType.TOTAL,
            str_format="%Y-%m-%d %H:%M:%S",
            ranges=[
                TimeRangeModel(
                    start="2018-01-01 00:00:00",
                    end="2018-12-31 23:00:00",
                ),
            ],
            frequency=timedelta(hours=1),
            timezone=TimeZone.EST,
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
    df = df.withColumn("time_zone", F.lit("America/Los_Angeles"))
    config = make_one_weekday_day_and_one_weekend_day_per_month_by_hour_config()
    project_time_config = make_date_time_config()
    if use_duckdb():
        mapped_df = map_time_dimension_with_chronify_duckdb(
            df,
            VALUE_COLUMN,
            config,
            project_time_config,
            scratch_dir_context,
        )
    else:
        filename = persist_table(
            df,
            scratch_dir_context,
            tag="tmp query",
        )
        mapped_df = map_time_dimension_with_chronify_spark_path(
            df=read_dataframe(filename),
            filename=filename,
            value_column=VALUE_COLUMN,
            from_time_dim=config,
            to_time_dim=project_time_config,
            scratch_dir_context=scratch_dir_context,
        )
    timestamps = mapped_df.select("timestamp").distinct().sort("timestamp").collect()
    zi = ZoneInfo("EST")
    est_timestamps = [x.timestamp.astimezone(zi) for x in timestamps]
    start = datetime(year=2018, month=1, day=1, tzinfo=zi)
    resolution = timedelta(hours=1)
    expected_timestamps = [start + i * resolution for i in range(8760)]
    assert est_timestamps == expected_timestamps
    assert is_dataframe_empty(mapped_df.filter(f"{VALUE_COLUMN} IS NULL"))

    max_value = aggregate_single_value(df.select(VALUE_COLUMN), "max", VALUE_COLUMN)
    max_row = df.filter(f"value == {max_value}").collect()[0]
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
    assert max_row.month == expected_month
    assert max_row.hour == expected_hour
    assert max_row.is_weekday
    assert max_row.geography == expected_geo
    assert max_row.model_year == expected_model_year
    assert max_row.scenario == expected_scenario
    mapped_df_at_max = mapped_df.filter(f"value == {max_value}")
    mapped_df.createOrReplaceTempView("tmp_view")
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
            model_year = '{max_row.model_year}'
            AND geography = '{max_row.geography}'
            AND scenario = '{max_row.scenario}'
            AND MONTH(timestamp) = {max_row.month}
            AND HOUR(timestamp) = {expected_hour_pst}
            AND {func}(timestamp) < {saturday}
        )
    """
    spark = get_spark_session()
    filtered_mapped_df = spark.sql(query)
    assert mapped_df_at_max.count() == num_weekdays_in_march_2018
    assert filtered_mapped_df.count() == num_weekdays_in_march_2018
    assert filtered_mapped_df.select("value").distinct().count() == 1
    assert math.isclose(
        filtered_mapped_df.select("value").distinct().collect()[0]["value"], expected_max
    )
    assert (
        mapped_df_at_max.sort("timestamp").collect()
        == filtered_mapped_df.sort("timestamp").collect()
    )
