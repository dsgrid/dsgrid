import datetime
import pandas as pd
import pytest
from pydantic import ValidationError
import logging

from dsgrid.config.dimensions_config import DimensionsConfigModel
from dsgrid.utils.files import load_data
from tests.data.dimension_models.minimal.models import DIMENSION_CONFIG_FILE_TIME
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.config.representative_period_time_dimension_config import (
    RepresentativePeriodTimeDimensionConfig,
)
from dsgrid.dimension.time import LeapDayAdjustmentType


logger = logging.getLogger(__name__)


@pytest.fixture
def time_dimension_model1():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[0]  # DateTimeDimensionModel (8760 period-beginning)


@pytest.fixture
def time_dimension_model2():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[1]  # DateTimeDimensionModel (daily time)


@pytest.fixture
def time_dimension_model3():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[2]  # DateTimeDimensionModel (8760 period-ending)


@pytest.fixture
def annual_time_dimension_model():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[3]  # AnnualTimeDimensionModel (annual time, correct format)


@pytest.fixture
def representative_time_dimension_model():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[4]  # RepresentativeTimeDimensionModel


def check_date_range_creation(time_dimension_model):
    config = DateTimeDimensionConfig(time_dimension_model)  # TimeDimensionConfig
    time_range = config.get_time_ranges()

    # create date range for time dimension
    df = pd.DataFrame()
    df["dim_dt"] = time_range[0].list_time_range()

    str(time_range)
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
    tz = config.get_tzinfo()
    ts = pd.date_range(start, end, freq=freq, tz=tz)

    # make necessary adjustments for leap_day_adjustment
    years = set(ts.year)
    ts_to_drop = []
    for yr in years:
        if time_dimension_model.leap_day_adjustment == LeapDayAdjustmentType.NONE:
            pass
        elif time_dimension_model.leap_day_adjustment == LeapDayAdjustmentType.DROP_JAN1:
            ts_to_drop = (
                ts_to_drop
                + pd.date_range(
                    start=f"{yr}-01-01", freq=freq, periods=24 / hours, tz=tz
                ).to_list()
            )
        elif time_dimension_model.leap_day_adjustment == LeapDayAdjustmentType.DROP_DEC31:
            ts_to_drop = (
                ts_to_drop
                + pd.date_range(
                    start=f"{yr}-12-31", freq=freq, periods=24 / hours, tz=tz
                ).to_list()
            )
        elif time_dimension_model.leap_day_adjustment == LeapDayAdjustmentType.DROP_FEB29:
            if yr % 4 == 0:
                ts_to_drop = (
                    ts_to_drop
                    + pd.date_range(
                        start=f"{yr}-02-29", freq=freq, periods=24 / hours, tz=tz
                    ).to_list()
                )
            else:
                logger.info(f" {yr} is not a leap year, no Feb 29 to drop")
        else:
            assert False

    df["pd_dt"] = ts.drop(ts_to_drop)

    # compare two date range creation
    df["delta"] = df["pd_dt"] - df["dim_dt"]
    num_ts_diff = (df["delta"] != datetime.timedelta(0)).sum()

    assert num_ts_diff == 0


def check_validation_error_365_days(time_dimension_model):
    with pytest.raises(ValidationError):
        time_dimension_model.ranges[0].start = "2018"
        time_dimension_model.ranges[0].end = "2050"
        time_dimension_model.str_format = "%Y"
        time_dimension_model.frequency = datetime.timedelta(days=365)


def check_register_annual_time(annual_time_dimension_model):
    print(annual_time_dimension_model)


# Test funcs:
def test_time_dimension_model1(time_dimension_model1):
    check_date_range_creation(time_dimension_model1)


def test_time_dimension_model2(time_dimension_model2):
    check_date_range_creation(time_dimension_model2)
    check_validation_error_365_days(time_dimension_model2)


def test_time_dimension_model3(time_dimension_model3):
    check_date_range_creation(time_dimension_model3)


def test_time_dimension_model4(annual_time_dimension_model):
    check_register_annual_time(annual_time_dimension_model)


def test_time_dimension_model5(representative_time_dimension_model):
    config = RepresentativePeriodTimeDimensionConfig(representative_time_dimension_model)
    config.list_expected_dataset_timestamps()
    config.get_time_ranges()


def test_time_dimension_model_lead_day_adj(time_dimension_model1):
    time_dimension_model1.leap_day_adjustment = LeapDayAdjustmentType.DROP_DEC31
    check_date_range_creation(time_dimension_model1)
    time_dimension_model1.leap_day_adjustment = LeapDayAdjustmentType.DROP_JAN1
    check_date_range_creation(time_dimension_model1)
    time_dimension_model1.leap_day_adjustment = LeapDayAdjustmentType.DROP_FEB29
    check_date_range_creation(time_dimension_model1)
