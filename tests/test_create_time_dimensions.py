import datetime
import logging
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
import numpy as np
from pydantic import ValidationError

from dsgrid.config.dimensions_config import DimensionsConfigModel
from dsgrid.utils.files import load_data
from tests.data.dimension_models.minimal.models import DIMENSION_CONFIG_FILE_TIME
from dsgrid.config.dimensions import (
    DateTimeDimensionModel,
    IndexRangeModel,
)
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.config.annual_time_dimension_config import AnnualTimeDimensionConfig

from dsgrid.dimension.time import (
    LeapDayAdjustmentType,
    TimeBasedDataAdjustmentModel,
)
from dsgrid.dimension.time_utils import (
    get_time_ranges,
)

from dsgrid.spark.types import (
    DoubleType,
    F,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from dsgrid.utils.spark import (
    get_spark_session,
)

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
    geography = ["Colorado", "California", "Arizona"]
    time_zones = ["MountainPrevailing", "PacificPrevailing", "USArizona"]
    if interval == "1h":
        indices = np.arange(0, 8784).tolist()
        values = np.arange(0.0, 8784.0).tolist()
    elif interval == "30min":
        indices = np.arange(0, 8784 * 2 - 1).tolist()
        values = np.arange(0.0, 8783.5, 0.5).tolist()
    else:
        msg = f"Unsupported {interval=}"
        raise ValueError(msg)
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
    geography = ["Colorado", "California", "Arizona"]
    time_zones = ["MountainPrevailing", "PacificPrevailing", "USArizona"]
    ts_pt = pd.date_range(
        "2012-01-01", "2012-12-31 23:00:00", freq="1h", tz=ZoneInfo("US/Mountain")
    )
    ts_pt = [str(ts) for ts in ts_pt]
    ts_st = pd.date_range(
        "2012-01-01", "2012-12-31 23:00:00", freq="1h", tz=ZoneInfo("US/Mountain")
    )
    ts_st = [str(ts) for ts in ts_st]
    timestamps = [ts_pt, ts_pt, ts_st]
    values = np.arange(0.0, 8784.0).tolist()  # daylight saving transition [1680:7396]
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


def check_start_time_and_length(time_dimension_model):
    config = DateTimeDimensionConfig(time_dimension_model)  # TimeDimensionConfig
    ts1 = pd.date_range(
        start=config.model.ranges[0].start,
        end=config.model.ranges[0].end,
        freq=config.get_frequency(),
        tz=config.get_tzinfo(),
    )
    ts2 = pd.date_range(
        start=config.get_start_times()[0],
        periods=config.get_lengths()[0],
        freq=config.get_frequency(),
    )
    assert ts1.equals(ts2)


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


def to_utc(time_change):
    return [x.astimezone(ZoneInfo("UTC")) for x in time_change]


# -- Test funcs --
def test_time_dimension_model0(time_dimension_model0):
    check_date_range_creation(time_dimension_model0)
    check_start_time_and_length(time_dimension_model0)


def test_time_dimension_model1(time_dimension_model1):
    check_date_range_creation(time_dimension_model1)
    check_validation_error_365_days(time_dimension_model1)
    check_start_time_and_length(time_dimension_model1)


def test_time_dimension_model2(time_dimension_model2):
    check_date_range_creation(time_dimension_model2)
    check_start_time_and_length(time_dimension_model2)


def test_time_dimension_model3(time_dimension_model3):
    check_date_range_creation(time_dimension_model3)
    check_start_time_and_length(time_dimension_model3)


def test_annual_time_dimension_model(annual_time_dimension_model):
    config = AnnualTimeDimensionConfig(annual_time_dimension_model)
    for st, length, time_range in zip(
        config.get_start_times(), config.get_lengths(), config.model.ranges
    ):
        start, end = int(time_range.start), int(time_range.end)
        assert st == datetime.datetime(year=start, month=1, day=1)
        assert length == len(range(start, end + 1))


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
