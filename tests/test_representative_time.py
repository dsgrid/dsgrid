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


def test_time_mapping(one_weekday_day_and_one_weekend_day_per_month_by_hour_table):
    df = one_weekday_day_and_one_weekend_day_per_month_by_hour_table
    # This dataset has only California counties.
    df = df.withColumn("time_zone", F.lit("PacificStandard"))
    config = make_one_weekday_day_and_one_weekend_day_per_month_by_hour_config()
    project_time_config = make_date_time_config()
    mapped_df = config.convert_dataframe(df, project_time_config)
    timestamps = mapped_df.select("timestamp").distinct().sort("timestamp").collect()
    zi = ZoneInfo("EST")
    est_timestamps = [x.timestamp.astimezone(zi) for x in timestamps]
    start = datetime(year=2018, month=1, day=1, tzinfo=zi)
    resolution = timedelta(hours=1)
    expected_timestamps = [start + i * resolution for i in range(8760)]
    assert est_timestamps == expected_timestamps
    assert mapped_df.filter(f"{VALUE_COLUMN} IS NULL").rdd.isEmpty()
