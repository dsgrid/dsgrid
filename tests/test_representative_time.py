import math
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DoubleType,
    StringType,
    StructType,
    StructField,
)

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import TimeDimensionType, TimeZone
from dsgrid.exceptions import DSGInvalidDataset
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
from dsgrid.dimension.time import MeasurementType, TimeIntervalType, RepresentativePeriodFormat
from dsgrid.utils.spark import get_spark_session


ONE_WEEKDAY_DAY_AND_ONE_WEEKEND_DAY_PER_MONTH_BY_HOUR_FILE = (
    Path("dsgrid-test-data")
    / "datasets"
    / "representative_time"
    / "OneWeekdayDayAndOneWeekendDayPerMonthByHour.csv"
)


@pytest.fixture(scope="module")
def one_weekday_day_and_one_weekend_day_per_month_by_hour_table():
    spark = get_spark_session()
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
            display_name="Time EST",
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
            display_name="Hourly for representative days",
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


def test_time_consistency(one_weekday_day_and_one_weekend_day_per_month_by_hour_table):
    df = one_weekday_day_and_one_weekend_day_per_month_by_hour_table
    config = make_one_weekday_day_and_one_weekend_day_per_month_by_hour_config()
    time_columns = config.get_load_data_time_columns()
    assert time_columns == ["month", "is_weekday", "hour"]
    with pytest.raises(DSGInvalidDataset):
        config.check_dataset_time_consistency(df.filter("hour != 3"), time_columns)
    config.check_dataset_time_consistency(df, time_columns)


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
    df = df.withColumn("time_zone", F.lit("PacificPrevailing"))
    config = make_one_weekday_day_and_one_weekend_day_per_month_by_hour_config()
    project_time_config = make_date_time_config()
    value_columns = {VALUE_COLUMN}
    mapped_df = config.convert_dataframe(
        df, project_time_config, value_columns, scratch_dir_context
    )
    timestamps = mapped_df.select("timestamp").distinct().sort("timestamp").collect()
    zi = ZoneInfo("EST")
    est_timestamps = [x.timestamp.astimezone(zi) for x in timestamps]
    start = datetime(year=2018, month=1, day=1, tzinfo=zi)
    resolution = timedelta(hours=1)
    expected_timestamps = [start + i * resolution for i in range(8760)]
    assert est_timestamps == expected_timestamps
    assert mapped_df.filter(f"{VALUE_COLUMN} IS NULL").rdd.isEmpty()

    max_values = df.select("value").agg(F.max("value").alias("max")).collect()
    assert len(max_values) == 1
    max_value = max_values[0].max
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
    mapped_df_at_max = mapped_df.filter(f"value == {max_value}").cache()
    filtered_mapped_df = mapped_df.filter(
        (F.col("model_year") == max_row.model_year)
        & (F.col("geography") == max_row.geography)
        & (F.col("scenario") == max_row.scenario)
        & (F.month("timestamp") == max_row.month)
        & (F.hour("timestamp") == expected_hour_pst)
        & (F.weekday("timestamp") < 5)
    ).cache()
    try:
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
    finally:
        mapped_df_at_max.unpersist()
        filtered_mapped_df.unpersist()
