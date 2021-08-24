import datetime
import pandas as pd
import pytest
from pydantic import ValidationError
import logging

from dsgrid.config.dimensions_config import DimensionsConfigModel
from dsgrid.utils.files import load_data
from tests.data.dimension_models.minimal.models import DIMENSION_CONFIG_FILE_TIME
from dsgrid.config.dimension_config import TimeDimensionConfig
import os


logger = logging.getLogger(__name__)


@pytest.fixture
def time_dimension_model1():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[0]  # TimeDimensionModel (8760 period-beginning)


@pytest.fixture
def time_dimension_model2():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[1]  # TimeDimensionModel (annual)


@pytest.fixture
def time_dimension_model3():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[2]  # TimeDimensionModel (8760 period-ending)


@pytest.fixture
def annual_time_dimension_model():
    file = DIMENSION_CONFIG_FILE_TIME
    config_as_dict = load_data(file)
    model = DimensionsConfigModel(**config_as_dict)
    yield model.dimensions[3]  # AnnualTimeDimensionModel


def check_date_range_creation(time_dimension_model):
    config = TimeDimensionConfig(time_dimension_model)  # TimeDimensionConfig
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
    df["pd_dt"] = pd.date_range(start, end, freq=freq, tz=tz)

    # compare two date range creation
    df["delta"] = df["pd_dt"] - df["dim_dt"]
    num_ts_diff = (df["delta"] != datetime.timedelta(0)).sum()

    assert num_ts_diff == 0


def check_validation_error_366_days(time_dimension_model):
    with pytest.raises(ValidationError):
        time_dimension_model.frequency = datetime.timedelta(days=366)


def check_register_annual_time(annual_time_dimension_model):
    print(annual_time_dimension_model)


# Test funcs:
def test_time_dimension_model1(time_dimension_model1):
    check_date_range_creation(time_dimension_model1)


def test_time_dimension_model2(time_dimension_model2):
    check_date_range_creation(time_dimension_model2)
    check_validation_error_366_days(time_dimension_model2)


def test_time_dimension_model3(time_dimension_model3):
    check_date_range_creation(time_dimension_model3)


def test_time_dimension_model4(annual_time_dimension_model):
    check_register_annual_time(annual_time_dimension_model)
