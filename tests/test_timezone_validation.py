"""Tests for timezone validation improvements.

Covers three layers of defense for the case where geography records are missing
valid ``time_zone`` values but the time dimension requires localization:

- Option 1: ``DatasetConfigModel.check_time_zone`` validator catches the problem
  at config load time, including when ``use_project_geography_time_zone=True``.
- Option 2: ``localize_timestamps_if_necessary`` runtime null check.
- Option 3: ``check_timezone_in_geography`` detects all-null ``time_zone`` values.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock

from dsgrid.common import TIME_ZONE_COLUMN, TIME_COLUMN, VALUE_COLUMN
from dsgrid.config.dataset_config import DatasetConfigModel, DataClassificationType
from dsgrid.config.dimensions import (
    AlignedTimeSingleTimeZone,
    DateTimeDimensionModel,
    DimensionModel,
    TimeFormatDateTimeNTZModel,
    TimeFormatDateTimeTZModel,
    LocalTimeMultipleTimeZones,
    TimeRangeModel,
)
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.dimension.base_models import (
    DimensionType,
    check_timezone_in_geography,
)
from dsgrid.dimension.time import (
    TimeIntervalType,
    MeasurementType,
    TimeZoneFormat,
)
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension, DSGInvalidOperation
from dsgrid.registry.project_registry_manager import ProjectRegistryManager
from dsgrid.spark.functions import get_spark_session
from dsgrid.spark.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)
from dsgrid.utils.dataset import localize_timestamps_if_necessary
from dsgrid.utils.scratch_dir_context import ScratchDirContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_geo_csv(tmp_path, *, with_time_zone: bool, filename="geography.csv"):
    """Create a geography CSV with or without a time_zone column."""
    path = tmp_path / filename
    if with_time_zone:
        path.write_text("id,name,time_zone\ng1,Geo 1,Etc/GMT+5\n")
    else:
        path.write_text("id,name\ng1,Geo 1\n")
    return str(path)


def _make_geography_dimension(tmp_path, *, with_time_zone: bool):
    """Build a DimensionModel for geography."""
    geo_file = _make_geo_csv(tmp_path, with_time_zone=with_time_zone)
    return DimensionModel(
        name="geography",
        type=DimensionType.GEOGRAPHY,
        module="dsgrid.dimension.standard",
        **{"class": "Geography"},
        file=geo_file,
    )


def _make_time_dimension_ntz_multi_tz():
    """Build a DateTimeDimensionModel with aligned_in_std_clock_time + timestamp_ntz."""
    return DateTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        **{"class": "Time"},
        column_format=TimeFormatDateTimeNTZModel(),
        time_zone_format=LocalTimeMultipleTimeZones(
            format_type=TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME,
            time_zones=["Etc/GMT+5"],
        ),
        measurement_type=MeasurementType.TOTAL,
        ranges=[
            TimeRangeModel(
                start="2018-01-01 00:00:00",
                end="2018-01-01 01:00:00",
                frequency=pd.Timedelta(hours=1),
            )
        ],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )


def _make_time_dimension_tz_multi_tz():
    """Build a DateTimeDimensionModel with aligned_in_std_clock_time + timestamp_tz."""
    return DateTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        **{"class": "Time"},
        column_format=TimeFormatDateTimeTZModel(),
        time_zone_format=LocalTimeMultipleTimeZones(
            format_type=TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME,
            time_zones=["Etc/GMT+5"],
        ),
        measurement_type=MeasurementType.TOTAL,
        ranges=[
            TimeRangeModel(
                start="2018-01-01 00:00:00",
                end="2018-01-01 01:00:00",
                frequency=pd.Timedelta(hours=1),
            )
        ],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )


def _build_dataset_config_model(
    tmp_path,
    *,
    use_project_geography_time_zone: bool,
    geo_has_tz: bool,
    time_dim_model,
):
    """Build a DatasetConfigModel with minimal required fields."""
    geo_dim = _make_geography_dimension(tmp_path, with_time_zone=geo_has_tz)
    return DatasetConfigModel(
        dataset_id="test_dataset",
        data_classification=DataClassificationType.LOW,
        use_project_geography_time_zone=use_project_geography_time_zone,
        dimensions=[time_dim_model, geo_dim],
    )


# ===========================================================================
# Option 3 tests: check_timezone_in_geography
# ===========================================================================


class TestCheckTimezoneInGeography:
    """Tests for the hardened check_timezone_in_geography function."""

    def test_all_null_time_zones_raises(self, tmp_path):
        """Geography records with all-null time_zone should raise DSGInvalidDimension."""
        geo_dim = _make_geography_dimension(tmp_path, with_time_zone=False)
        # Records are GeographyDimensionBaseModel instances with time_zone=None
        assert all(rec.time_zone is None for rec in geo_dim.records)
        with pytest.raises(DSGInvalidDimension, match="null"):
            check_timezone_in_geography(geo_dim)

    def test_valid_time_zones_passes(self, tmp_path):
        """Geography records with valid time_zone should pass."""
        geo_dim = _make_geography_dimension(tmp_path, with_time_zone=True)
        check_timezone_in_geography(geo_dim)

    def test_custom_err_msg(self, tmp_path):
        """Custom error message should be used when all time zones are null."""
        geo_dim = _make_geography_dimension(tmp_path, with_time_zone=False)
        custom_msg = "Custom error about missing time zones"
        with pytest.raises(DSGInvalidDimension, match="Custom error"):
            check_timezone_in_geography(geo_dim, err_msg=custom_msg)


# ===========================================================================
# Option 1 tests: DatasetConfigModel.check_time_zone validator
# ===========================================================================


class TestCheckTimeZoneValidator:
    """Tests for the check_time_zone model_validator on DatasetConfigModel."""

    def test_default_flag_no_tz_in_geography_raises(self, tmp_path):
        """use_project_geography_time_zone=False + NTZ + no time_zone → should raise."""
        time_dim = _make_time_dimension_ntz_multi_tz()
        with pytest.raises((ValueError, DSGInvalidDimension)):
            _build_dataset_config_model(
                tmp_path,
                use_project_geography_time_zone=False,
                geo_has_tz=False,
                time_dim_model=time_dim,
            )

    def test_default_flag_with_tz_in_geography_passes(self, tmp_path):
        """use_project_geography_time_zone=False + NTZ + valid time_zone → should pass."""
        time_dim = _make_time_dimension_ntz_multi_tz()
        config = _build_dataset_config_model(
            tmp_path,
            use_project_geography_time_zone=False,
            geo_has_tz=True,
            time_dim_model=time_dim,
        )
        assert config is not None

    def test_project_geo_flag_ntz_no_tz_raises(self, tmp_path):
        """use_project_geography_time_zone=True + NTZ + no time_zone → should raise.

        This is the key case: even with the flag set, registration-time localization
        needs time_zone from the dataset's geography.
        """
        time_dim = _make_time_dimension_ntz_multi_tz()
        with pytest.raises((ValueError, DSGInvalidDimension), match="registration"):
            _build_dataset_config_model(
                tmp_path,
                use_project_geography_time_zone=True,
                geo_has_tz=False,
                time_dim_model=time_dim,
            )

    def test_project_geo_flag_ntz_with_tz_passes(self, tmp_path):
        """use_project_geography_time_zone=True + NTZ + valid time_zone → should pass."""
        time_dim = _make_time_dimension_ntz_multi_tz()
        config = _build_dataset_config_model(
            tmp_path,
            use_project_geography_time_zone=True,
            geo_has_tz=True,
            time_dim_model=time_dim,
        )
        assert config is not None

    def test_project_geo_flag_tz_aware_no_tz_passes(self, tmp_path):
        """use_project_geography_time_zone=True + timestamp_tz + no time_zone → should pass.

        When timestamps are already timezone-aware, no localization is needed during
        registration, so geography time_zone is not required.
        """
        time_dim = _make_time_dimension_tz_multi_tz()
        config = _build_dataset_config_model(
            tmp_path,
            use_project_geography_time_zone=True,
            geo_has_tz=False,
            time_dim_model=time_dim,
        )
        assert config is not None


# ===========================================================================
# Option 2 tests: localize_timestamps_if_necessary null check
# ===========================================================================


@pytest.fixture(scope="module")
def spark():
    return get_spark_session()


class DummyDatasetConfig:
    """Minimal stub for DatasetConfig used by localize_timestamps_if_necessary."""

    def __init__(self, time_dim, geography_dim=None):
        self._time_dim = time_dim
        self._geo_dim = geography_dim

    def get_dimension(self, dimension_type):
        if dimension_type == DimensionType.TIME:
            return self._time_dim
        if dimension_type == DimensionType.GEOGRAPHY:
            return self._geo_dim
        return None

    def get_value_columns(self):
        return [VALUE_COLUMN]


class NullTzGeoDim:
    """Geography dimension stub that returns records with all-null time_zone."""

    def __init__(self, spark_session):
        self._spark = spark_session

    def get_records_dataframe(self):
        pdf = pd.DataFrame({"id": ["g1"], "time_zone": [None]})
        return self._spark.createDataFrame(pdf)


class TestLocalizeTimestampsNullCheck:
    """Tests for the runtime null-check in localize_timestamps_if_necessary."""

    def test_all_null_time_zone_raises(self, spark, tmp_path):
        """When add_time_zone produces all-null time_zone, raise DSGInvalidOperation."""
        time_model = DateTimeDimensionModel(
            name="time",
            type=DimensionType.TIME,
            module="dsgrid.dimension.standard",
            **{"class": "Time"},
            column_format=TimeFormatDateTimeNTZModel(),
            time_zone_format=LocalTimeMultipleTimeZones(
                format_type=TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME,
                time_zones=["Etc/GMT+5"],
            ),
            measurement_type=MeasurementType.TOTAL,
            ranges=[
                TimeRangeModel(
                    start="2018-01-01 00:00:00",
                    end="2018-01-01 01:00:00",
                    frequency=pd.Timedelta(hours=1),
                )
            ],
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
        )
        time_dim = DateTimeDimensionConfig.load_from_model(time_model)
        geo_dim = NullTzGeoDim(spark)
        config = DummyDatasetConfig(time_dim, geography_dim=geo_dim)

        pdf = pd.DataFrame(
            {
                TIME_COLUMN: [
                    pd.Timestamp("2018-01-01 00:00:00"),
                    pd.Timestamp("2018-01-01 01:00:00"),
                ],
                "geography": ["g1", "g1"],
                VALUE_COLUMN: [1.0, 2.0],
            }
        )
        schema = StructType(
            [
                StructField(TIME_COLUMN, TimestampNTZType(), True),
                StructField("geography", StringType(), True),
                StructField(VALUE_COLUMN, DoubleType(), True),
            ]
        )
        sdf = spark.createDataFrame(pdf, schema=schema)

        with pytest.raises(DSGInvalidOperation, match="all null"):
            localize_timestamps_if_necessary(
                sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
            )


# ===========================================================================
# Option 4 tests: _check_time_zone_for_mapping at project submission
# ===========================================================================


def _make_time_dimension_utc():
    """Build a DateTimeDimensionModel with aligned_in_absolute_time (UTC-like).

    ``is_time_zone_required_in_geography()`` returns False for this type.
    """
    return DateTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        **{"class": "Time"},
        column_format=TimeFormatDateTimeNTZModel(),
        time_zone_format=AlignedTimeSingleTimeZone(
            format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
            time_zone="Etc/GMT+5",
        ),
        measurement_type=MeasurementType.TOTAL,
        ranges=[
            TimeRangeModel(
                start="2018-01-01 00:00:00",
                end="2018-01-01 01:00:00",
                frequency=pd.Timedelta(hours=1),
            )
        ],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )


def _make_mock_dataset_config(
    tmp_path,
    *,
    time_dim_model,
    use_project_geography_time_zone,
    geo_has_tz,
):
    """Build a mock DatasetConfig for submission-time validation tests."""
    from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig

    time_dim = DateTimeDimensionConfig.load_from_model(time_dim_model)

    geo_dim_model = _make_geography_dimension(tmp_path, with_time_zone=geo_has_tz)
    geo_config = MagicMock()
    geo_config.model = geo_dim_model

    dataset_model = MagicMock()
    dataset_model.use_project_geography_time_zone = use_project_geography_time_zone

    config = MagicMock()
    config.get_time_dimension.return_value = time_dim
    config.get_dimension.return_value = geo_config
    config.model = dataset_model
    config.config_id = "test_dataset"
    return config


def _make_mock_project_config(time_dim_model):
    """Build a mock ProjectConfig for submission-time validation tests."""
    from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig

    time_dim = DateTimeDimensionConfig.load_from_model(time_dim_model)
    config = MagicMock()
    config.get_base_time_dimension.return_value = time_dim
    return config


def test_submit_dataset_utc_to_local_no_tz_raises(tmp_path):
    """Dataset in UTC, project in local time, no time_zone on geo → should raise."""
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_utc(),
        use_project_geography_time_zone=False,
        geo_has_tz=False,
    )
    project_config = _make_mock_project_config(_make_time_dimension_ntz_multi_tz())

    with pytest.raises(DSGInvalidDataset, match="time zone information"):
        ProjectRegistryManager._check_time_zone_for_mapping(project_config, dataset_config)


def test_submit_dataset_utc_to_local_with_tz_passes(tmp_path):
    """Dataset in UTC, project in local time, time_zone present → should pass."""
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_utc(),
        use_project_geography_time_zone=False,
        geo_has_tz=True,
    )
    project_config = _make_mock_project_config(_make_time_dimension_ntz_multi_tz())

    ProjectRegistryManager._check_time_zone_for_mapping(project_config, dataset_config)


def test_submit_dataset_utc_to_local_project_geo_flag_passes(tmp_path):
    """Dataset in UTC, project in local time, use_project_geography_time_zone=True → pass."""
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_utc(),
        use_project_geography_time_zone=True,
        geo_has_tz=False,
    )
    project_config = _make_mock_project_config(_make_time_dimension_ntz_multi_tz())

    ProjectRegistryManager._check_time_zone_for_mapping(project_config, dataset_config)


def test_submit_dataset_utc_to_utc_no_tz_passes(tmp_path):
    """Both dataset and project in UTC → no time_zone needed, should pass."""
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_utc(),
        use_project_geography_time_zone=False,
        geo_has_tz=False,
    )
    project_config = _make_mock_project_config(_make_time_dimension_utc())

    ProjectRegistryManager._check_time_zone_for_mapping(project_config, dataset_config)


# ===========================================================================
# Index time time_zone_format tests
# ===========================================================================


def _make_index_time_single_tz():
    """Build an IndexTimeDimensionModel with aligned_in_absolute_time (single tz)."""
    from dsgrid.config.dimensions import IndexTimeDimensionModel

    return IndexTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        **{"class": "Time"},
        time_zone_format=AlignedTimeSingleTimeZone(
            format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
            time_zone="Etc/GMT+5",
        ),
        measurement_type=MeasurementType.TOTAL,
        ranges=[{"start": 0, "end": 8783, "starting_timestamp": "2012-01-01 00:00:00", "frequency": "01:00:00"}],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )


def _make_index_time_multi_tz():
    """Build an IndexTimeDimensionModel with aligned_in_std_clock_time (per-geography tz)."""
    from dsgrid.config.dimensions import IndexTimeDimensionModel

    return IndexTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        **{"class": "Time"},
        time_zone_format=LocalTimeMultipleTimeZones(
            format_type=TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME,
            time_zones=["Etc/GMT+5", "Etc/GMT+6"],
        ),
        measurement_type=MeasurementType.TOTAL,
        ranges=[{"start": 0, "end": 8783, "starting_timestamp": "2012-01-01 00:00:00", "frequency": "01:00:00"}],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )


def test_index_time_single_tz_does_not_require_geography_tz():
    """IndexTime with aligned_in_absolute_time should not require time_zone in geography."""
    model = _make_index_time_single_tz()
    assert not model.is_time_zone_required_in_geography()


def test_index_time_multi_tz_requires_geography_tz():
    """IndexTime with aligned_in_std_clock_time should require time_zone in geography."""
    model = _make_index_time_multi_tz()
    assert model.is_time_zone_required_in_geography()


def test_index_time_single_tz_config_accessors():
    """IndexTimeDimensionConfig with single tz should return tz from config."""
    from dsgrid.config.index_time_dimension_config import IndexTimeDimensionConfig

    model = _make_index_time_single_tz()
    config = IndexTimeDimensionConfig.load_from_model(model)
    assert config.get_time_zone() == "Etc/GMT+5"
    assert config.get_time_zones() == ["Etc/GMT+5"]
    assert config.get_tzinfo() is not None


def test_index_time_multi_tz_config_accessors():
    """IndexTimeDimensionConfig with multi tz should return None for get_time_zone."""
    from dsgrid.config.index_time_dimension_config import IndexTimeDimensionConfig

    model = _make_index_time_multi_tz()
    config = IndexTimeDimensionConfig.load_from_model(model)
    assert config.get_time_zone() is None
    assert config.get_time_zones() == ["Etc/GMT+5", "Etc/GMT+6"]
    assert config.get_tzinfo() is None


def test_index_time_single_tz_chronify_type():
    """IndexTimeDimensionConfig with single tz should produce IndexTimeRange."""
    import chronify
    from dsgrid.config.index_time_dimension_config import IndexTimeDimensionConfig

    model = _make_index_time_single_tz()
    config = IndexTimeDimensionConfig.load_from_model(model)
    result = config.to_chronify()
    assert isinstance(result, chronify.IndexTimeRange)


def test_index_time_multi_tz_chronify_type():
    """IndexTimeDimensionConfig with multi tz should produce IndexTimeRangeWithTZColumn."""
    import chronify
    from dsgrid.config.index_time_dimension_config import IndexTimeDimensionConfig

    model = _make_index_time_multi_tz()
    config = IndexTimeDimensionConfig.load_from_model(model)
    result = config.to_chronify()
    assert isinstance(result, chronify.IndexTimeRangeWithTZColumn)


def test_index_time_legacy_config_defaults_to_multi_tz():
    """An index time config without time_zone_format should default to aligned_in_std_clock_time."""
    from dsgrid.config.dimensions import IndexTimeDimensionModel

    model = IndexTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        **{"class": "Time"},
        # No time_zone_format — legacy config
        measurement_type=MeasurementType.TOTAL,
        ranges=[{"start": 0, "end": 8783, "starting_timestamp": "2012-01-01 00:00:00", "frequency": "01:00:00"}],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )
    assert model.is_time_zone_required_in_geography()
    assert model.time_zone_format.format_type == TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME


def test_index_time_single_tz_dataset_config_no_geo_tz_passes(tmp_path):
    """Dataset with single-tz IndexTime should not require time_zone in geography."""
    model = _make_index_time_single_tz()
    config = _build_dataset_config_model(
        tmp_path,
        use_project_geography_time_zone=False,
        geo_has_tz=False,
        time_dim_model=model,
    )
    assert config is not None


def test_index_time_multi_tz_dataset_config_no_geo_tz_raises(tmp_path):
    """Dataset with multi-tz IndexTime should require time_zone in geography."""
    model = _make_index_time_multi_tz()
    with pytest.raises((ValueError, DSGInvalidDimension)):
        _build_dataset_config_model(
            tmp_path,
            use_project_geography_time_zone=False,
            geo_has_tz=False,
            time_dim_model=model,
        )
