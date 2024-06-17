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
from dsgrid.config.dimensions import DateTimeDimensionModel, IndexRangeModel
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.config.representative_period_time_dimension_config import (
    RepresentativePeriodTimeDimensionConfig,
)
from dsgrid.config.index_time_dimension_config import IndexTimeDimensionConfig
from dsgrid.dimension.time import (
    LeapDayAdjustmentType,
    TimeZone,
    TimeBasedDataAdjustmentModel,
)
from dsgrid.dimension.time_utils import (
    get_dls_springforward_time_change,
    get_dls_springforward_time_change_by_year,
    get_dls_springforward_time_change_by_time_range,
    get_dls_fallback_time_change,
    get_dls_fallback_time_change_by_year,
    get_dls_fallback_time_change_by_time_range,
    create_adjustment_map_from_model_time,
    build_index_time_map,
    get_time_ranges,
)
from dsgrid.utils.spark import get_spark_session

logger = logging.getLogger(__name__)


@pytest.fixture
def time_dimension_model0():
    config_as_dict = load_data(DIMENSION_CONFIG_FILE_TIME)
    config_as_dict["dimensions"] = [config_as_dict["dimensions"][0]]
    model = DimensionsConfigModel(**config_as_dict).dimensions[0]
    yield model  # DateTimeDimensionModel (8760 period-beginning)


@pytest.fixture
def time_dimension_model1():
    config_as_dict = load_data(DIMENSION_CONFIG_FILE_TIME)
    config_as_dict["dimensions"] = [config_as_dict["dimensions"][1]]
    model = DimensionsConfigModel(**config_as_dict).dimensions[0]
    yield model  # DateTimeDimensionModel (annual)


@pytest.fixture
def time_dimension_model2():
    config_as_dict = load_data(DIMENSION_CONFIG_FILE_TIME)
    config_as_dict["dimensions"] = [config_as_dict["dimensions"][2]]
    model = DimensionsConfigModel(**config_as_dict).dimensions[0]
    yield model  # DateTimeDimensionModel (8760 period-ending, 6-h freq)


@pytest.fixture
def time_dimension_model3():
    config_as_dict = load_data(DIMENSION_CONFIG_FILE_TIME)
    config_as_dict["dimensions"] = [config_as_dict["dimensions"][3]]
    model = DimensionsConfigModel(**config_as_dict).dimensions[0]
    yield model  # DateTimeDimensionModel (8760 UTC, 15-min)


@pytest.fixture
def annual_time_dimension_model():
    config_as_dict = load_data(DIMENSION_CONFIG_FILE_TIME)
    config_as_dict["dimensions"] = [config_as_dict["dimensions"][4]]
    model = DimensionsConfigModel(**config_as_dict).dimensions[0]
    yield model  # AnnualTimeDimensionModel (annual time, correct format)


@pytest.fixture
def representative_time_dimension_model():
    config_as_dict = load_data(DIMENSION_CONFIG_FILE_TIME)
    config_as_dict["dimensions"] = [config_as_dict["dimensions"][5]]
    model = DimensionsConfigModel(**config_as_dict).dimensions[0]
    yield model  # RepresentativeTimeDimensionModel


@pytest.fixture
def index_time_dimension_model():
    config_as_dict = load_data(DIMENSION_CONFIG_FILE_TIME)
    config_as_dict["dimensions"] = [config_as_dict["dimensions"][6]]
    model = DimensionsConfigModel(**config_as_dict).dimensions[0]
    yield model  # IndexTimeDimensionModel (industrial time)


@pytest.fixture
def datetime_eq_index_time_model():
    config_as_dict = load_data(DIMENSION_CONFIG_FILE_TIME)
    config_as_dict["dimensions"] = [config_as_dict["dimensions"][7]]
    model = DimensionsConfigModel(**config_as_dict).dimensions[0]
    yield model  # DateTime version of index_time model 1


@pytest.fixture
def index_time_dimension_model_subhourly():
    config_as_dict = load_data(DIMENSION_CONFIG_FILE_TIME)
    config_as_dict["dimensions"] = [config_as_dict["dimensions"][6]]
    model = DimensionsConfigModel(**config_as_dict).dimensions[0]
    model.frequency = datetime.timedelta(minutes=30)
    model.ranges = [IndexRangeModel(start=0, end=8784 * 2 - 1)]
    yield model


@pytest.fixture
def datetime_eq_index_time_model_subhourly():
    config_as_dict = load_data(DIMENSION_CONFIG_FILE_TIME)
    config_as_dict["dimensions"] = [config_as_dict["dimensions"][0]]
    model = DimensionsConfigModel(**config_as_dict).dimensions[0]
    model.frequency = datetime.timedelta(minutes=30)
    yield model  # 30-min freq version of time model 0


def create_index_time_dataframe(interval="1h"):
    # mock index time dataframe
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
    if interval == "1h":
        indices = np.arange(0, 8784).tolist()
        values = np.arange(0.0, 8784.0).tolist()
    elif interval == "30min":
        indices = np.arange(0, 8784 * 2 - 1).tolist()
        values = np.arange(0.0, 8783.5, 0.5).tolist()
    else:
        raise ValueError("Unsupported {interval=}")
    df_tz = get_spark_session().createDataFrame(zip(indices, values), ["time_index", "value"])
    for geo, tz in zip(geography, time_zones):
        df = df.union(
            df_tz.withColumn("geography", F.lit(geo))
            .withColumn("time_zone", F.lit(tz))
            .select(schema.names)
        )
    return df


@pytest.fixture
def df_index_time():
    yield create_index_time_dataframe(interval="1h")


@pytest.fixture
def df_index_time_subhourly():
    yield create_index_time_dataframe(interval="30min")


@pytest.fixture
def df_date_time():
    # datetime version of df_index_time
    schema = StructType(
        [
            StructField("geography", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("value", DoubleType(), False),
            StructField("time_zone", StringType(), False),
        ]
    )
    df = get_spark_session().createDataFrame([], schema=schema)
    geography = ["Colorado", "Wyoming", "Arizona"]
    time_zones = ["MountainPrevailing", "MountainPrevailing", "USArizona"]
    ts_pt = pd.date_range(
        "2012-01-01", "2012-12-31 23:00:00", freq="1h", tz=ZoneInfo("US/Mountain")
    )
    ts_pt = [str(ts) for ts in ts_pt]
    ts_st = pd.date_range(
        "2012-01-01", "2012-12-31 23:00:00", freq="1h", tz=ZoneInfo("US/Mountain")
    )
    ts_st = [str(ts) for ts in ts_st]
    timestamps = [ts_pt, ts_pt, ts_st]
    values = np.arange(0.0, 8784.0).tolist()
    sch = StructType(
        [
            StructField("timestamp", StringType(), False),
            StructField("value", DoubleType(), False),
        ]
    )
    for geo, tz, ts in zip(geography, time_zones, timestamps):
        df_tz = get_spark_session().createDataFrame(zip(ts, values), schema=sch)
        df = df.union(
            df_tz.withColumn("geography", F.lit(geo))
            .withColumn("time_zone", F.lit(tz))
            .select(schema.names)
        )
    yield df


def check_date_range_creation(time_dimension_model, time_based_data_adjustment=None):
    if time_based_data_adjustment is None:
        time_based_data_adjustment = TimeBasedDataAdjustmentModel()
    config = DateTimeDimensionConfig(time_dimension_model)  # TimeDimensionConfig
    time_range = get_time_ranges(config, time_based_data_adjustment=time_based_data_adjustment)
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
        freq = time_dimension_model.frequency
    ts = pd.date_range(start, end, freq=freq, tz=tz).to_list()

    # make necessary data adjustments
    ld_adj = time_based_data_adjustment.leap_day_adjustment
    ts_to_drop, ts_to_add = [], []

    years = set([t.year for t in ts])
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

    ts = [t for t in ts if t not in ts_to_drop]
    ts = ts + ts_to_add
    df["pd_dt"] = sorted(ts)
    # compare two date range creation
    df["delta"] = df["pd_dt"] - df["dim_dt"]
    ts_diff = df.loc[df["delta"] != datetime.timedelta(0)]
    assert len(ts_diff) == 0, f"ts_diff: {ts_diff}"


def check_validation_error_365_days(time_dimension_model):
    with pytest.raises(
        ValidationError, match="use class=AnnualTime, time_type=annual to specify a year series"
    ):
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


def industrial_model_time_conversion_tests(config, project_time_dim, df):
    values = np.arange(0.0, 8784.0).tolist()
    n_df = df.count()

    # [1] Duplicating fallback 1AM
    # This has the same behavior as time_based_data_adjustment = None
    time_based_data_adjustment = TimeBasedDataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "duplicate",
        }
    )
    df2 = config.convert_dataframe(
        df,
        project_time_dim,
        model_years=None,
        value_columns=["value"],
        wrap_time_allowed=True,
        time_based_data_adjustment=time_based_data_adjustment,
    )
    # df2.sort(F.col("timestamp"), F.col("geography")).show()

    f2 = df2.sort(F.col("geography"), F.col("timestamp")).toPandas()
    print(f2.loc[1680:7396])  # daylight saving transition
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

    df3 = config.convert_dataframe(
        df,
        project_time_dim,
        model_years=None,
        value_columns=["value"],
        wrap_time_allowed=True,
        time_based_data_adjustment=None,
    )
    f3 = df3.sort(F.col("geography"), F.col("timestamp")).toPandas()
    print(f3.loc[1680:7396])  # daylight saving transition
    assert (
        len(f2.compare(f3)) == 0
    ), f"LocalModel_time.convert_dataframe() with time_based_data_adjustment=None should have the same behavior as with {time_based_data_adjustment=}"

    # [2] Interpolating fallback between 1 and 2AM
    time_based_data_adjustment = TimeBasedDataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "interpolate",
        }
    )
    df2 = config.convert_dataframe(
        df,
        project_time_dim,
        model_years=None,
        value_columns=["value"],
        wrap_time_allowed=True,
        time_based_data_adjustment=time_based_data_adjustment,
    )
    # df2.sort(F.col("timestamp"), F.col("geography")).show()

    f2 = df2.sort(F.col("geography"), F.col("timestamp")).toPandas()
    print(f2.loc[1680:7396])  # daylight saving transition
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


def local_time_conversion_tests(config, project_time_dim, df):
    values = np.arange(0.0, 8784.0).tolist()

    # [1] Duplicating fallback 1AM
    # This has the same behavior as time_based_data_adjustment = None
    time_based_data_adjustment = TimeBasedDataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "duplicate",
        }
    )
    df2 = config.convert_dataframe(
        df,
        project_time_dim,
        model_years=None,
        value_columns=["value"],
        wrap_time_allowed=True,
        time_based_data_adjustment=time_based_data_adjustment,
    )
    f2 = df2.sort(F.col("geography"), F.col("timestamp")).toPandas()
    print(f2.loc[1680:7396])  # daylight saving transition
    for geo in sorted(f2["geography"].unique()):
        assert (
            f2.loc[f2["geography"] == geo, "value"].to_list() == values
        ), f"Expecting no change in 'value' column for {geo=}"
    df3 = config.convert_dataframe(
        df,
        project_time_dim,
        model_years=None,
        value_columns=["value"],
        wrap_time_allowed=True,
        time_based_data_adjustment=None,
    )
    f3 = df3.sort(F.col("geography"), F.col("timestamp")).toPandas()
    print(f3.loc[1680:7396])  # daylight saving transition
    assert (
        len(f2.compare(f3)) == 0
    ), f"Local_time.convert_dataframe() with time_based_data_adjustment=None should have the same behavior as with {time_based_data_adjustment=}"


# -- Test funcs --
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


def test_daylight_saving_time_changes():
    # Spring forward
    truth = [datetime.datetime(2018, 3, 11, 3, 0, tzinfo=ZoneInfo(key="US/Eastern"))]
    time_change = get_dls_springforward_time_change(2018, TimeZone.EPT)
    assert to_utc([time_change]) == to_utc(truth)

    time_change = get_dls_springforward_time_change(2018, None)
    assert time_change == truth[0].replace(tzinfo=None)

    from_ts = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    to_ts = datetime.datetime(2018, 12, 31, 0, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    time_change = get_dls_springforward_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(truth)

    time_change = get_dls_springforward_time_change_by_time_range(
        from_ts.replace(tzinfo=None), to_ts.replace(tzinfo=None)
    )
    assert time_change == [truth[0].replace(tzinfo=None)]

    time_change = get_dls_springforward_time_change_by_time_range(
        from_ts, to_ts, frequency=datetime.timedelta(hours=6)
    )
    assert time_change == []

    time_change = get_dls_springforward_time_change_by_time_range(
        from_ts, to_ts, frequency=datetime.timedelta(minutes=15)
    )
    truth2 = [truth[0] + datetime.timedelta(minutes=15) * x for x in range(4)]
    assert to_utc(time_change) == to_utc(truth2)

    from_ts = datetime.datetime(2018, 3, 11, 3, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    time_change = get_dls_springforward_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(
        [from_ts]
    )  # In prevailing time, 2018-03-11 03:00 == 2018-03-11 02:00

    # multiple years
    truth = [
        datetime.datetime(2018, 3, 11, 3, 0, tzinfo=ZoneInfo(key="US/Mountain")),
        datetime.datetime(2019, 3, 10, 3, 0, tzinfo=ZoneInfo(key="US/Mountain")),
        datetime.datetime(2020, 3, 8, 3, 0, tzinfo=ZoneInfo(key="US/Mountain")),
    ]
    time_change = get_dls_springforward_time_change_by_year([2018, 2019, 2020], TimeZone.MPT)
    assert to_utc(time_change.values()) == to_utc(truth)

    from_ts = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=ZoneInfo(key="US/Mountain"))
    to_ts = datetime.datetime(2020, 12, 31, 0, 0, tzinfo=ZoneInfo(key="US/Mountain"))
    time_change = get_dls_springforward_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(truth)

    # Fall back
    truth = [datetime.datetime(2018, 11, 4, 1, 0, tzinfo=ZoneInfo(key="EST"))]
    time_change = get_dls_fallback_time_change(2018, TimeZone.EPT)
    assert to_utc([time_change]) == to_utc(truth)

    time_change = get_dls_fallback_time_change(2018, None)
    assert time_change == truth[0].replace(tzinfo=None)

    from_ts = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    to_ts = datetime.datetime(2018, 12, 31, 0, 0, tzinfo=ZoneInfo(key="US/Eastern"))
    time_change = get_dls_fallback_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(truth)

    time_change = get_dls_fallback_time_change_by_time_range(
        from_ts.replace(tzinfo=None), to_ts.replace(tzinfo=None)
    )
    assert time_change == [truth[0].replace(tzinfo=None)]

    time_change = get_dls_fallback_time_change_by_time_range(
        from_ts, to_ts, frequency=datetime.timedelta(hours=6)
    )
    assert time_change == []

    time_change = get_dls_fallback_time_change_by_time_range(
        from_ts, to_ts, frequency=datetime.timedelta(minutes=15)
    )
    truth2 = [truth[0] + datetime.timedelta(minutes=15) * x for x in range(4)]
    assert to_utc(time_change) == to_utc(truth2)

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
    assert to_utc(time_change.values()) == to_utc(truth)

    from_ts = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=ZoneInfo(key="US/Mountain"))
    to_ts = datetime.datetime(2020, 12, 31, 0, 0, tzinfo=ZoneInfo(key="US/Mountain"))
    time_change = get_dls_fallback_time_change_by_time_range(from_ts, to_ts)
    assert to_utc(time_change) == to_utc(truth)

    # Standard Time returns nothing
    assert get_dls_springforward_time_change(2020, TimeZone.ARIZONA) is None
    assert get_dls_fallback_time_change(2020, TimeZone.ARIZONA) is None
    assert get_dls_springforward_time_change_by_year([2018, 2024], TimeZone.MST) == {}
    assert get_dls_fallback_time_change_by_year([2018, 2024], TimeZone.MST) == {}

    from_ts = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=ZoneInfo(key="EST"))
    to_ts = datetime.datetime(2024, 12, 31, 0, 0, tzinfo=ZoneInfo(key="EST"))
    assert get_dls_springforward_time_change_by_time_range(from_ts, to_ts) == []
    assert get_dls_fallback_time_change_by_time_range(from_ts, to_ts) == []

    to_ts = datetime.datetime(2024, 12, 31, 0, 0, tzinfo=ZoneInfo(key="Etc/GMT+8"))
    with pytest.raises(ValueError, match=r"do not have the same time zone"):
        get_dls_springforward_time_change_by_time_range(from_ts, to_ts)

    with pytest.raises(ValueError, match=r"do not have the same time zone"):
        get_dls_fallback_time_change_by_time_range(from_ts, to_ts)


def test_time_dimension_model_lead_day_adjustment(time_dimension_model0):
    daylight_saving_adjustment = {
        "spring_forward_hour": "none",
        "fall_back_hour": "none",
    }
    time_based_data_adjustment = TimeBasedDataAdjustmentModel(
        leap_day_adjustment=LeapDayAdjustmentType.DROP_DEC31,
        daylight_saving_adjustment=daylight_saving_adjustment,
    )
    check_date_range_creation(
        time_dimension_model0, time_based_data_adjustment=time_based_data_adjustment
    )

    time_based_data_adjustment = TimeBasedDataAdjustmentModel(
        leap_day_adjustment=LeapDayAdjustmentType.DROP_JAN1,
        daylight_saving_adjustment=daylight_saving_adjustment,
    )
    check_date_range_creation(
        time_dimension_model0, time_based_data_adjustment=time_based_data_adjustment
    )

    time_based_data_adjustment = TimeBasedDataAdjustmentModel(
        leap_day_adjustment=LeapDayAdjustmentType.DROP_FEB29,
        daylight_saving_adjustment=daylight_saving_adjustment,
    )
    check_date_range_creation(
        time_dimension_model0, time_based_data_adjustment=time_based_data_adjustment
    )


def test_data_adjustment_mapping_table(index_time_dimension_model):
    """Test time_based_data_adjustment mapping tables"""
    time_zone = TimeZone.MST
    config = IndexTimeDimensionConfig(index_time_dimension_model)

    # [1] Duplicating fallback 1AM
    time_based_data_adjustment = TimeBasedDataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "duplicate",
        }
    )

    # index-time mapping table
    table1 = build_index_time_map(
        config, timezone=time_zone.tz, time_based_data_adjustment=time_based_data_adjustment
    ).withColumn("time_zone", F.lit(time_zone.value))
    # time_based_data_adjustment mapping table
    table2 = create_adjustment_map_from_model_time(
        config, time_based_data_adjustment=time_based_data_adjustment, time_zone=time_zone
    )
    joined_table = table1.selectExpr("time_index", "timestamp as model_time").join(
        table2, ["model_time"], "right"
    )
    # joined_table.sort([F.col("time_index"), F.col("timestamp")]).show()

    # check joined_table
    res = joined_table.select("time_index", "multiplier").collect()
    indices = [x.time_index for x in res]
    assert 1682 not in indices, "time_index 1682 is found, expecting it missing."
    indices_count = {x: indices.count(x) for x in indices}
    indices_dup = {x: v for x, v in indices_count.items() if v > 1}
    assert indices_dup == {7393: 2}, f"Unexpected duplicated time_index found, {indices_dup}."

    multipliers = [x.multiplier for x in res]
    assert multipliers == [1 for x in multipliers], "multiplier column is not all 1."

    # [2] Interpolating fallback between 1 and 2AM
    time_based_data_adjustment = TimeBasedDataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "interpolate",
        }
    )

    # index-time mapping table
    table1 = build_index_time_map(
        config, timezone=time_zone.tz, time_based_data_adjustment=time_based_data_adjustment
    ).withColumn("time_zone", F.lit(time_zone.tz_name))
    # time_based_data_adjustment mapping table
    table2 = create_adjustment_map_from_model_time(
        config, time_based_data_adjustment=time_based_data_adjustment, time_zone=time_zone
    )
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


def test_index_time_conversion(index_time_dimension_model, time_dimension_model0, df_index_time):
    """test industrial time with time_based_data_adjustment"""
    df = df_index_time
    project_time_dim = DateTimeDimensionConfig(time_dimension_model0)
    config = IndexTimeDimensionConfig(index_time_dimension_model)
    time_cols = config.get_load_data_time_columns()
    config.check_dataset_time_consistency(df, time_cols)
    industrial_model_time_conversion_tests(config, project_time_dim, df)


@pytest.mark.skip
def test_datetime_conversion(datetime_eq_index_time_model, time_dimension_model0, df_date_time):
    """test datetime_format.LOCAL_AS_STRINGS"""
    df = df_date_time
    project_time_dim = DateTimeDimensionConfig(time_dimension_model0)
    config = DateTimeDimensionConfig(datetime_eq_index_time_model)
    df = config.convert_time_format(df)
    time_cols = config.get_load_data_time_columns()
    config.check_dataset_time_consistency(df, time_cols)
    local_time_conversion_tests(config, project_time_dim, df)


def test_index_time_conversion_subhourly(
    index_time_dimension_model_subhourly,
    datetime_eq_index_time_model_subhourly,
    df_index_time_subhourly,
):
    """test subhourly industrial time with time_based_data_adjustment"""
    df = df_index_time_subhourly
    project_time_dim = DateTimeDimensionConfig(datetime_eq_index_time_model_subhourly)
    config = IndexTimeDimensionConfig(index_time_dimension_model_subhourly)

    # -- test --
    values = np.arange(0.0, 8783.5, 0.5).tolist()
    n_df = df.count()

    # [1] Duplicating fallback 1AM
    # This has the same behavior as time_based_data_adjustment = None
    time_based_data_adjustment = TimeBasedDataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "duplicate",
        }
    )
    df2 = config.convert_dataframe(
        df,
        project_time_dim,
        model_years=None,
        value_columns=["value"],
        wrap_time_allowed=True,
        time_based_data_adjustment=time_based_data_adjustment,
    )
    # df2.sort(F.col("timestamp"), F.col("geography")).show()

    f2 = df2.sort(F.col("geography"), F.col("timestamp")).toPandas()
    assert (
        len(f2) == n_df
    ), f"convert_dataframe() did not return the same row count. before={n_df} vs. after={len(f2)}"
    # for AZ, no missing or interpolation
    f2_filtered = f2.loc[f2["geography"] == "Arizona", "value"].to_list()
    assert f2_filtered == values, "f2 for AZ has missing or interpolated values."
    f2_filtered = f2.loc[f2["geography"] == "Colorado", "value"].to_list()
    for val in [1682, 1682.5]:
        assert val not in f2_filtered, f"value {val} is found for CO, expecting it missing."
    for dup_val in [7393, 7393.5]:
        assert (
            dup_val in f2_filtered
        ), f"Expecting duplicated value {dup_val} for CO, but not found."

    df3 = config.convert_dataframe(
        df,
        project_time_dim,
        model_years=None,
        value_columns=["value"],
        wrap_time_allowed=True,
        time_based_data_adjustment=None,
    )
    f3 = df3.sort(F.col("geography"), F.col("timestamp")).toPandas()
    assert (
        len(f2.compare(f3)) == 0
    ), f"LocalModel_time.convert_dataframe() with time_based_data_adjustment=None should have the same behavior as with {time_based_data_adjustment=}"

    # [2] Interpolating fallback between 1 and 2AM
    time_based_data_adjustment = TimeBasedDataAdjustmentModel(
        daylight_saving_adjustment={
            "spring_forward_hour": "drop",
            "fall_back_hour": "interpolate",
        }
    )
    df2 = config.convert_dataframe(
        df,
        project_time_dim,
        model_years=None,
        value_columns=["value"],
        wrap_time_allowed=True,
        time_based_data_adjustment=time_based_data_adjustment,
    )
    # df2.sort(F.col("timestamp"), F.col("geography")).show()

    f2 = df2.sort(F.col("geography"), F.col("timestamp")).toPandas()
    assert (
        len(f2) == n_df
    ), f"convert_dataframe() did not return the same row count. before={n_df} vs. after={len(f2)}"
    # for AZ, no missing or interpolation
    f2_filtered = f2.loc[f2["geography"] == "Arizona", "value"].to_list()
    assert f2_filtered == values, "f2 for AZ has missing or interpolated values."
    f2_filtered = f2.loc[f2["geography"] == "Colorado", "value"].to_list()
    for val in [1682, 1682.5]:
        assert val not in f2_filtered, f"value {val} is found for CO, expecting it missing."
    itpl_val = (7393 + 7394) / 2
    for val in [itpl_val, itpl_val + 0.5]:
        assert val in f2_filtered, f"Expecting interpolated value {val} for CO, but not found."
