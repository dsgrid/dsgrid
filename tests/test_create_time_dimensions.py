import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import pytest
from pydantic import ValidationError
import logging
import pyspark.sql.functions as F
import numpy as np
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from dsgrid.config.dimensions_config import DimensionsConfigModel
from dsgrid.utils.files import load_data
from tests.data.dimension_models.minimal.models import DIMENSION_CONFIG_FILE_TIME
from dsgrid.config.dimensions import DateTimeDimensionModel
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.config.representative_period_time_dimension_config import (
    RepresentativePeriodTimeDimensionConfig,
)
from dsgrid.config.indexed_time_dimension_config import IndexedTimeDimensionConfig
from dsgrid.dimension.time import (
    LeapDayAdjustmentType,
    DaylightSavingSpringForwardType,
    DaylightSavingFallBackType,
    TimeZone,
    DataAdjustmentModel,
    get_dls_springforward_time_change_by_year,
    get_dls_springforward_time_change_by_time_range,
    get_dls_fallback_time_change_by_year,
    get_dls_fallback_time_change_by_time_range,
)
from dsgrid.utils.spark import get_spark_session

logger = logging.getLogger(__name__)


@pytest.fixture
def time_dimension_model0():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[0]  # DateTimeDimensionModel (8760 period-beginning)


@pytest.fixture
def time_dimension_model1():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[1]  # DateTimeDimensionModel (daily time)


@pytest.fixture
def time_dimension_model2():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[2]  # DateTimeDimensionModel (8760 period-ending, 6-h freq)


@pytest.fixture
def time_dimension_model3():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[
        3
    ]  # DateTimeDimensionModel (8760 local standard, for daylight adjustment)


@pytest.fixture
def annual_time_dimension_model():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[4]  # AnnualTimeDimensionModel (annual time, correct format)


@pytest.fixture
def representative_time_dimension_model():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[5]  # RepresentativeTimeDimensionModel


@pytest.fixture
def indexed_time_dimension_model():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[6]  # IndexedTimeDimensionModel


def check_date_range_creation(time_dimension_model, data_adjustment=None):
    if data_adjustment is None:
        data_adjustment = DataAdjustmentModel()
    config = DateTimeDimensionConfig(time_dimension_model)  # TimeDimensionConfig
    time_range = config.get_time_ranges(data_adjustment=data_adjustment)
    tz = config.get_tzinfo()

    # create date range for time dimension
    assert len(time_range) == 1, "there are more than one time range."
    df = pd.DataFrame()
    df["dim_dt"] = time_range[0].list_time_range()
    if tz is None:
        # if timezone naive, Spark will create timestamps in local system time
        tz = df.loc[0, "dim_dt"].tzinfo
        df["dim_dt"] = df["dim_dt"].apply(lambda x: x.astimezone(tz))

    logger.info("Date range created: ", time_range[0].show_range(5))  # show first and last 5

    # create date range using pandas
    start = datetime.datetime.strptime(
        time_dimension_model.ranges[0].start, time_dimension_model.str_format
    )
    end = datetime.datetime.strptime(
        time_dimension_model.ranges[0].end, time_dimension_model.str_format
    )
    hours = time_dimension_model.frequency / datetime.timedelta(hours=1)

    if hours == 365 * 24:
        freq = "AS"
    else:
        freq = f"{int(hours)}h"
    ts = pd.date_range(start, end, freq=freq, tz=tz)

    # make necessary data adjustments
    ld_adj = data_adjustment.leap_day_adjustment
    sf_adj = data_adjustment.daylight_saving_adjustment.spring_forward_hour
    fb_adj = data_adjustment.daylight_saving_adjustment.fall_back_hour

    ts_to_drop, ts_to_add = [], []
    if sf_adj == DaylightSavingSpringForwardType.DROP:
        sf_times = get_dls_springforward_time_change_by_time_range(
            start.replace(tzinfo=tz),
            end.replace(tzinfo=tz),
            frequency=time_dimension_model.frequency,
        )
        if not time_dimension_model.timezone.is_standard():
            assert sf_times != [], "No spring forward time change found."
        ts_to_drop += sf_times
    if (
        fb_adj == DaylightSavingFallBackType.DUPLICATE
        or fb_adj == DaylightSavingFallBackType.INTERPOLATE
    ):
        fb_times = get_dls_fallback_time_change_by_time_range(
            start.replace(tzinfo=tz),
            end.replace(tzinfo=tz),
            frequency=time_dimension_model.frequency,
        )
        if not time_dimension_model.timezone.is_standard():
            assert fb_times != [], "No fall back time change found."
        ts_to_add += fb_times

    years = set(ts.year)
    for yr in years:
        if ld_adj == LeapDayAdjustmentType.NONE:
            pass
        elif ld_adj == LeapDayAdjustmentType.DROP_JAN1:
            ts_to_drop += pd.date_range(
                start=f"{yr}-01-01", freq=freq, periods=24 / hours, tz=tz
            ).to_list()
        elif ld_adj == LeapDayAdjustmentType.DROP_DEC31:
            ts_to_drop += pd.date_range(
                start=f"{yr}-12-31", freq=freq, periods=24 / hours, tz=tz
            ).to_list()
        elif ld_adj == LeapDayAdjustmentType.DROP_FEB29:
            if yr % 4 == 0:
                ts_to_drop += pd.date_range(
                    start=f"{yr}-02-29", freq=freq, periods=24 / hours, tz=tz
                ).to_list()
            else:
                logger.info(f" {yr} is not a leap year, no Feb 29 to drop")
        else:
            assert False

    ts = ts.drop(ts_to_drop)
    ts = ts.append(ts_to_add)
    df["pd_dt"] = sorted(ts)
    # compare two date range creation
    df["delta"] = df["pd_dt"] - df["dim_dt"]
    ts_diff = df.loc[df["delta"] != datetime.timedelta(0)]
    assert len(ts_diff) == 0, f"ts_diff: {ts_diff}"


def check_validation_error_365_days(time_dimension_model):
    with pytest.raises(ValidationError):
        data = time_dimension_model.model_dump()
        data["ranges"][0]["start"] = "2018"
        data["ranges"][0]["end"] = "2050"
        data["str_format"] = "%Y"
        data["frequency"] = datetime.timedelta(days=365)
        DateTimeDimensionModel.model_validate(data)


def check_register_annual_time(annual_time_dimension_model):
    print(annual_time_dimension_model)


def to_utc(time_change):
    return [x.astimezone(ZoneInfo("UTC")) for x in time_change]


# Test funcs:
def test_time_dimension_model0(time_dimension_model0):
    check_date_range_creation(time_dimension_model0)


def test_time_dimension_model1(time_dimension_model1):
    check_date_range_creation(time_dimension_model1)
    check_validation_error_365_days(time_dimension_model1)


def test_time_dimension_model2(time_dimension_model2):
    check_date_range_creation(time_dimension_model2)


def test_time_dimension_model3(time_dimension_model3):
    check_date_range_creation(time_dimension_model3)


def test_time_dimension_model4(annual_time_dimension_model):
    check_register_annual_time(annual_time_dimension_model)


def test_time_dimension_model5(representative_time_dimension_model):
    config = RepresentativePeriodTimeDimensionConfig(representative_time_dimension_model)
    if config.model.format.value == "one_week_per_month_by_hour":
        n_times = len(config.list_expected_dataset_timestamps())
        assert n_times == 24 * 7 * 12, n_times
        assert config.get_frequency() == datetime.timedelta(hours=1)

    config.get_time_ranges()  # TODO: this is not correct yet in terms of year, maybe this functionality should exist in project instead


def test_time_dimension_model_lead_day_adjustment(time_dimension_model0):
    daylight_saving_adjustment = {
        "spring_forward_hour": "none",
        "fall_back_hour": "none",
    }
    data_adjustment = DataAdjustmentModel(
        leap_day_adjustment=LeapDayAdjustmentType.DROP_DEC31,
        daylight_saving_adjustment=daylight_saving_adjustment,
    )
    check_date_range_creation(time_dimension_model0, data_adjustment=data_adjustment)

    data_adjustment = DataAdjustmentModel(
        leap_day_adjustment=LeapDayAdjustmentType.DROP_JAN1,
        daylight_saving_adjustment=daylight_saving_adjustment,
    )
    check_date_range_creation(time_dimension_model0, data_adjustment=data_adjustment)

    data_adjustment = DataAdjustmentModel(
        leap_day_adjustment=LeapDayAdjustmentType.DROP_FEB29,
        daylight_saving_adjustment=daylight_saving_adjustment,
    )
    check_date_range_creation(time_dimension_model0, data_adjustment=data_adjustment)


def test_time_dimension_model_daylight_saving_adjustment(time_dimension_model3):
    data_adjustment = DataAdjustmentModel(
        leap_day_adjustment=LeapDayAdjustmentType.NONE,
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "none",
        },
    )
    check_date_range_creation(time_dimension_model3, data_adjustment=data_adjustment)

    data_adjustment = DataAdjustmentModel(
        leap_day_adjustment=LeapDayAdjustmentType.NONE,
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "duplicate",
        },
    )
    check_date_range_creation(time_dimension_model3, data_adjustment=data_adjustment)

    data_adjustment = DataAdjustmentModel(
        leap_day_adjustment=LeapDayAdjustmentType.NONE,
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "interpolate",
        },
    )
    check_date_range_creation(time_dimension_model3, data_adjustment=data_adjustment)


def test_daylight_saving_time_changes():
    # Spring forward
    truth = [datetime.datetime(2018, 3, 11, 2, 0, tzinfo=ZoneInfo(key="US/Eastern"))]
    time_change = get_dls_springforward_time_change_by_year(2018, TimeZone.EPT)
    assert to_utc(time_change) == to_utc(truth)

    from_ts = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    to_ts = datetime.datetime(2018, 12, 31, 0, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    time_change = get_dls_springforward_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(truth)

    from_ts = datetime.datetime(2018, 3, 11, 1, 55, tzinfo=ZoneInfo(key="US/Eastern"))
    time_change = get_dls_springforward_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(truth)

    from_ts = datetime.datetime(2018, 3, 11, 3, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    time_change = get_dls_springforward_time_change_by_time_range(from_ts, to_ts)
    assert time_change == []

    # multiple years
    truth = [
        datetime.datetime(2018, 3, 11, 2, 0, tzinfo=ZoneInfo(key="US/Mountain")),
        datetime.datetime(2019, 3, 10, 2, 0, tzinfo=ZoneInfo(key="US/Mountain")),
        datetime.datetime(2020, 3, 8, 2, 0, tzinfo=ZoneInfo(key="US/Mountain")),
    ]
    time_change = get_dls_springforward_time_change_by_year([2018, 2019, 2020], TimeZone.MPT)
    assert to_utc(time_change) == to_utc(truth)

    from_ts = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=ZoneInfo(key="US/Mountain"))
    to_ts = datetime.datetime(2020, 12, 31, 0, 0, tzinfo=ZoneInfo(key="US/Mountain"))
    time_change = get_dls_springforward_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(truth)

    # Fall back
    time_change = get_dls_fallback_time_change_by_year(2018, TimeZone.EPT)
    truth = [datetime.datetime(2018, 11, 4, 1, 0, tzinfo=ZoneInfo(key="EST"))]
    assert to_utc(time_change) == to_utc(truth)

    from_ts = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    to_ts = datetime.datetime(2018, 12, 31, 0, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    time_change = get_dls_fallback_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(truth)

    from_ts = datetime.datetime(2018, 11, 4, 1, 55, tzinfo=ZoneInfo(key="US/Eastern"))
    time_change = get_dls_fallback_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(truth)

    from_ts = datetime.datetime(2018, 11, 4, 3, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    time_change = get_dls_fallback_time_change_by_time_range(from_ts, to_ts)
    assert time_change == []

    # multiple years
    time_change = get_dls_fallback_time_change_by_year([2018, 2019, 2020], TimeZone.MPT)
    truth = [
        datetime.datetime(2018, 11, 4, 1, 0, tzinfo=ZoneInfo(key="MST")),
        datetime.datetime(2019, 11, 3, 1, 0, tzinfo=ZoneInfo(key="MST")),
        datetime.datetime(2020, 11, 1, 1, 0, tzinfo=ZoneInfo(key="MST")),
    ]
    assert to_utc(time_change) == to_utc(truth)

    from_ts = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=ZoneInfo(key="US/Mountain"))
    to_ts = datetime.datetime(2020, 12, 31, 0, 0, tzinfo=ZoneInfo(key="US/Mountain"))
    time_change = get_dls_fallback_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(truth)

    # Standard Time returns nothing
    assert get_dls_springforward_time_change_by_year(2020, TimeZone.ARIZONA) == []
    assert get_dls_fallback_time_change_by_year(2020, TimeZone.ARIZONA) == []
    assert get_dls_springforward_time_change_by_year([2018, 2024], TimeZone.MST) == []
    assert get_dls_fallback_time_change_by_year([2018, 2024], TimeZone.MST) == []

    from_ts = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=ZoneInfo(key="EST"))
    to_ts = datetime.datetime(2024, 12, 31, 0, 0, tzinfo=ZoneInfo(key="EST"))
    assert get_dls_springforward_time_change_by_time_range(from_ts, to_ts) == []
    assert get_dls_fallback_time_change_by_time_range(from_ts, to_ts) == []

    to_ts = datetime.datetime(2024, 12, 31, 0, 0, tzinfo=ZoneInfo(key="Etc/GMT+8"))
    with pytest.raises(ValueError, match=r"do not have the same time zone"):
        get_dls_springforward_time_change_by_time_range(from_ts, to_ts)

    with pytest.raises(ValueError, match=r"do not have the same time zone"):
        get_dls_fallback_time_change_by_time_range(from_ts, to_ts)


def test_time_dimension_model6(indexed_time_dimension_model):
    """Test data_adjustment mapping tables"""
    time_zone = TimeZone.MST
    config = IndexedTimeDimensionConfig(indexed_time_dimension_model)

    # [1] Duplicating fallback 1AM
    data_adjustment = DataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "duplicate",
        }
    )

    # index-time mapping table
    table1 = config.build_time_dataframe(
        timezone=time_zone.tz, data_adjustment=data_adjustment
    ).withColumn("time_zone", F.lit(time_zone.value))
    # data_adjustment mapping table
    table2 = config._create_adjustment_map_from_model_time(data_adjustment, time_zone)
    joined_table = table1.selectExpr("time_index", "timestamp as model_time").join(
        table2, ["model_time"], "right"
    )
    # joined_table.sort([F.col("time_index"), F.col("timestamp")]).show()
    # joined_table.sort([F.col("time_index").desc(), F.col("timestamp").desc()]).show()

    # check joined_table
    res = joined_table.select("time_index", "multiplier").collect()
    indices = [x.time_index for x in res]
    assert 1682 not in indices, "time_index 1682 is found, expecting it missing."
    indices_count = {x: indices.count(x) for x in indices}
    indices_dup = {x: v for x, v in indices_count.items() if v > 1}
    assert indices_dup == {7393: 2}, f"Unexpected duplicated time_index found, {indices_dup}."

    multipliers = [x.multiplier for x in res]
    assert multipliers == [1 for x in multipliers], "multiplier column is not all 1."

    timestamps = joined_table.sort(table1.time_index).select("timestamp").toPandas()
    missing_ts = pd.Timestamp("2012-03-11 02:00:00")
    assert (
        missing_ts not in timestamps["timestamp"]
    ), f"timestamp {missing_ts} is found, expecting it missing."
    duplicated_ts = pd.Timestamp("2012-11-04 01:00:00")
    timestamps_count = timestamps["timestamp"].value_counts()
    timestamps_dup = timestamps_count[timestamps_count > 1]
    assert timestamps_dup.index.to_list() == [
        duplicated_ts
    ], f"Unexpected duplicated timestamp found, {timestamps_dup.index.to_list()}"
    assert timestamps_dup.to_list() == [
        2
    ], f"timestamp {duplicated_ts} is duplicated more than twice."

    # [2] Interpolating fallback between 1 and 2AM
    data_adjustment = DataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "interpolate",
        }
    )

    # index-time mapping table
    table1 = config.build_time_dataframe(
        timezone=time_zone.tz, data_adjustment=data_adjustment
    ).withColumn("time_zone", F.lit(time_zone.tz_name))
    # data_adjustment mapping table
    table2 = config._create_adjustment_map_from_model_time(data_adjustment, time_zone)
    joined_table = table1.selectExpr("time_index", "timestamp as model_time").join(
        table2, ["model_time"], "right"
    )
    joined_table = joined_table.withColumn(
        "standard_time",
        F.from_utc_timestamp(
            F.to_utc_timestamp(F.col("timestamp"), TimeZone.MPT.tz_name),
            time_zone.tz_name,
        ),
    )
    # joined_table.sort([F.col("time_index"), F.col("timestamp")]).show()
    # joined_table.sort([F.col("time_index").desc(), F.col("timestamp").desc()]).show()

    # check joined_table
    res = joined_table.select("time_index", "multiplier").collect()
    indices = [x.time_index for x in res]
    assert 1682 not in indices, "time_index 1682 is found, expecting it missing."
    indices_count = {x: indices.count(x) for x in indices}
    indices_dup = {x: v for x, v in indices_count.items() if v > 1}
    assert indices_dup == {
        7393: 2,
        7394: 2,
    }, f"Unexpected duplicated time_index found, {indices_dup}."

    res2 = joined_table.select("time_index", "multiplier").where("multiplier < 1").collect()
    multipliers = [x.multiplier for x in res2]
    assert multipliers == [0.5, 0.5], "multiplier column does not have exactly two 0.5."
    indices2 = [x.time_index for x in res2]
    itpl_indices = [7393, 7394]
    assert (
        sorted(indices2) == itpl_indices
    ), f"Expecting interpolated indices: {itpl_indices} but found {indices2}"

    timestamps = joined_table.sort(table1.time_index).select("timestamp").toPandas()
    missing_ts = pd.Timestamp("2012-03-11 02:00:00")
    assert (
        missing_ts not in timestamps["timestamp"]
    ), f"timestamp {missing_ts} is found, expecting it missing."
    duplicated_ts = pd.Timestamp("2012-11-04 01:00:00")
    timestamps_count = timestamps["timestamp"].value_counts()
    timestamps_dup = timestamps_count[timestamps_count > 1]
    assert timestamps_dup.index.to_list() == [
        duplicated_ts
    ], f"Unexpected duplicated timestamp found, {timestamps_dup.index.to_list()}"
    assert timestamps_dup.to_list() == [
        3
    ], f"timestamp {duplicated_ts} is duplicated more than twice."


def test_indexed_time_conversion(time_dimension_model0, indexed_time_dimension_model):
    # mock project dataframe
    schema = StructType(
        [
            StructField("geography", StringType(), False),
            StructField("time_index", IntegerType(), False),
            StructField("value", DoubleType(), False),
            StructField("time_zone", StringType(), False),
        ]
    )
    df = get_spark_session().createDataFrame([], schema=schema)
    geography = ["Colorado", "Wyoming", "Arizona"]
    time_zones = ["MountainPrevailing", "MountainPrevailing", "USArizona"]
    indices = np.arange(1680, 7396).tolist()
    values = np.arange(1680, 7396).tolist()
    df_tz = get_spark_session().createDataFrame(zip(indices, values), ["time_index", "value"])
    for geo, tz in zip(geography, time_zones):
        df = df.union(
            df_tz.withColumn("geography", F.lit(geo))
            .withColumn("time_zone", F.lit(tz))
            .select(schema.names)
        )

    n_df = df.count()

    project_time_dim = DateTimeDimensionConfig(
        time_dimension_model0
    )  # fake, any will do to return time column name
    config = IndexedTimeDimensionConfig(indexed_time_dimension_model)

    # [1] Duplicating fallback 1AM
    data_adjustment = DataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "duplicate",
        }
    )
    df2 = config.convert_dataframe(
        df,
        project_time_dim,
        model_years=None,
        value_columns=None,
        wrap_time_allowed=False,
        data_adjustment=data_adjustment,
    )
    # df2.sort(F.col("timestamp"), F.col("geography")).show()
    # df2.sort(F.col("geography"), F.col("timestamp").desc()).show()
    # df2.sort(F.col("geography").desc(), F.col("timestamp").desc()).show()

    f2 = df2.sort(F.col("geography"), F.col("timestamp")).toPandas()
    assert (
        len(f2) == n_df
    ), f"convert_dataframe() did not return the same row count. before={n_df} vs. after={len(f2)}"
    # for AZ, no missing or interpolation
    f2_filtered = f2.loc[f2["geography"] == "Arizona", "value"].to_list()
    assert f2_filtered == values, "f2 for AZ has missing or interpolated values."
    f2_filtered = f2.loc[f2["geography"] == "Colorado", "value"].to_list()
    assert 1682 not in f2_filtered, "value 1682 is found for CO, expecting it missing."
    dup_val = 7393
    assert dup_val in f2_filtered, f"Expecting duplicated value {dup_val} for CO, but not found."

    # [2] Interpolating fallback between 1 and 2AM
    data_adjustment = DataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "interpolate",
        }
    )
    df2 = config.convert_dataframe(
        df,
        project_time_dim,
        model_years=None,
        value_columns=None,
        wrap_time_allowed=False,
        data_adjustment=data_adjustment,
    )
    # df2.sort(F.col("timestamp"), F.col("geography")).show()
    # df2.sort(F.col("geography"), F.col("timestamp").desc()).show()
    # df2.sort(F.col("geography").desc(), F.col("timestamp").desc()).show()

    f2 = df2.sort(F.col("geography"), F.col("timestamp")).toPandas()
    assert (
        len(f2) == n_df
    ), f"convert_dataframe() did not return the same row count. before={n_df} vs. after={len(f2)}"
    # for AZ, no missing or interpolation
    f2_filtered = f2.loc[f2["geography"] == "Arizona", "value"].to_list()
    assert f2_filtered == values, "f2 for AZ has missing or interpolated values."
    f2_filtered = f2.loc[f2["geography"] == "Colorado", "value"].to_list()
    assert 1682 not in f2_filtered, "value 1682 is found for CO, expecting it missing."
    itpl_val = (7393 + 7394) / 2
    assert (
        itpl_val in f2_filtered
    ), f"Expecting interpolated value {itpl_val} for CO, but not found."
