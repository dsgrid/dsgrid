"""Tests for timezone validation improvements.

Covers the layers of defense for the case where geography records are missing
valid ``time_zone`` values but time mapping requires them:

- ``DatasetConfigModel.check_time_zone`` validator catches the problem at
  config load time, including when ``use_project_geography_time_zone=True``.
- ``ProjectRegistryManager._check_time_zone_for_mapping`` catches it at
  dataset-submission time.
- ``localize_timestamps_if_necessary`` raises on an all-null runtime join.
- ``check_timezone_in_geography`` detects all-null ``time_zone`` values.
"""

from unittest.mock import MagicMock

import chronify
import pandas as pd
import pytest

from dsgrid.common import TIME_COLUMN, VALUE_COLUMN
from dsgrid.config.dataset_config import DataClassificationType, DatasetConfigModel
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.config.dimensions import (
    AlignedTimeSingleTimeZone,
    DateTimeDimensionModel,
    DimensionModel,
    IndexTimeDimensionModel,
    LocalTimeMultipleTimeZones,
    TimeFormatDateTimeNTZModel,
    TimeFormatDateTimeTZModel,
    TimeRangeModel,
)
from dsgrid.config.index_time_dimension_config import IndexTimeDimensionConfig
from dsgrid.dimension.base_models import DimensionType, check_timezone_in_geography
from dsgrid.dimension.time import MeasurementType, TimeIntervalType, TimeZoneFormat
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension, DSGInvalidOperation
from dsgrid.ibis.session import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)
from dsgrid.registry.project_registry_manager import ProjectRegistryManager
from dsgrid.utils.dataset import localize_timestamps_if_necessary
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from tests._helpers import DummyDatasetConfig, DummyGeoDim


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


def _make_time_dimension(column_format, time_zone_format) -> DateTimeDimensionModel:
    return DateTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        **{"class": "Time"},
        column_format=column_format,
        time_zone_format=time_zone_format,
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


def _make_time_dimension_ntz_multi_tz():
    """aligned_in_std_clock_time + timestamp_ntz: requires registration localization."""
    return _make_time_dimension(
        TimeFormatDateTimeNTZModel(),
        LocalTimeMultipleTimeZones(
            format_type=TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME,
            time_zones=["Etc/GMT+5"],
        ),
    )


def _make_time_dimension_tz_multi_tz():
    """aligned_in_std_clock_time + timestamp_tz: no registration localization needed."""
    return _make_time_dimension(
        TimeFormatDateTimeTZModel(),
        LocalTimeMultipleTimeZones(
            format_type=TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME,
            time_zones=["Etc/GMT+5"],
        ),
    )


def _make_time_dimension_absolute():
    """aligned_in_absolute_time: is_time_zone_required_in_geography() is False."""
    return _make_time_dimension(
        TimeFormatDateTimeNTZModel(),
        AlignedTimeSingleTimeZone(
            format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
            time_zone="Etc/GMT+5",
        ),
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


# ---------------------------------------------------------------------------
# check_timezone_in_geography
# ---------------------------------------------------------------------------


def test_check_timezone_in_geography_all_null_raises(tmp_path):
    geo_dim = _make_geography_dimension(tmp_path, with_time_zone=False)
    assert all(rec.time_zone is None for rec in geo_dim.records)
    with pytest.raises(DSGInvalidDimension, match="null"):
        check_timezone_in_geography(geo_dim)


def test_check_timezone_in_geography_valid_passes(tmp_path):
    geo_dim = _make_geography_dimension(tmp_path, with_time_zone=True)
    check_timezone_in_geography(geo_dim)


def test_check_timezone_in_geography_err_msg_override(tmp_path):
    """A caller-supplied err_msg replaces the default all-null message."""
    geo_dim = _make_geography_dimension(tmp_path, with_time_zone=False)
    caller_msg = "Dataset xyz requires geography time zones for time mapping"
    with pytest.raises(DSGInvalidDimension, match=caller_msg) as exc:
        check_timezone_in_geography(geo_dim, err_msg=caller_msg)
    assert "all values" not in str(exc.value)


# ---------------------------------------------------------------------------
# DatasetConfigModel.check_time_zone validator
# ---------------------------------------------------------------------------


def test_dataset_config_no_tz_in_geography_raises(tmp_path):
    """use_project_geography_time_zone=False + NTZ + no time_zone raises."""
    with pytest.raises((ValueError, DSGInvalidDimension)):
        _build_dataset_config_model(
            tmp_path,
            use_project_geography_time_zone=False,
            geo_has_tz=False,
            time_dim_model=_make_time_dimension_ntz_multi_tz(),
        )


def test_dataset_config_with_tz_in_geography_passes(tmp_path):
    """use_project_geography_time_zone=False + NTZ + valid time_zone passes."""
    config = _build_dataset_config_model(
        tmp_path,
        use_project_geography_time_zone=False,
        geo_has_tz=True,
        time_dim_model=_make_time_dimension_ntz_multi_tz(),
    )
    assert config is not None


def test_dataset_config_project_geo_flag_ntz_no_tz_raises(tmp_path):
    """use_project_geography_time_zone=True + NTZ + no time_zone raises.

    This is the key case: even with the flag set, registration-time localization
    needs time_zone from the dataset's geography.
    """
    with pytest.raises((ValueError, DSGInvalidDimension), match="registration"):
        _build_dataset_config_model(
            tmp_path,
            use_project_geography_time_zone=True,
            geo_has_tz=False,
            time_dim_model=_make_time_dimension_ntz_multi_tz(),
        )


def test_dataset_config_project_geo_flag_ntz_with_tz_passes(tmp_path):
    """use_project_geography_time_zone=True + NTZ + valid time_zone passes."""
    config = _build_dataset_config_model(
        tmp_path,
        use_project_geography_time_zone=True,
        geo_has_tz=True,
        time_dim_model=_make_time_dimension_ntz_multi_tz(),
    )
    assert config is not None


def test_dataset_config_project_geo_flag_tz_aware_no_tz_passes(tmp_path):
    """use_project_geography_time_zone=True + timestamp_tz + no time_zone passes.

    When timestamps are already timezone-aware, no localization is needed during
    registration, so geography time_zone is not required.
    """
    config = _build_dataset_config_model(
        tmp_path,
        use_project_geography_time_zone=True,
        geo_has_tz=False,
        time_dim_model=_make_time_dimension_tz_multi_tz(),
    )
    assert config is not None


# ---------------------------------------------------------------------------
# localize_timestamps_if_necessary null check
# ---------------------------------------------------------------------------


def test_localize_all_null_time_zone_raises(spark, tmp_path):
    """When add_time_zone produces all-null time_zone, raise DSGInvalidOperation."""
    time_dim = DateTimeDimensionConfig.load_from_model(_make_time_dimension_ntz_multi_tz())
    config = DummyDatasetConfig(time_dim, geography_dim=DummyGeoDim(spark, time_zone=None))

    geography_column = DimensionType.GEOGRAPHY.value
    pdf = pd.DataFrame(
        {
            TIME_COLUMN: [
                pd.Timestamp("2018-01-01 00:00:00"),
                pd.Timestamp("2018-01-01 01:00:00"),
            ],
            geography_column: ["g1", "g1"],
            VALUE_COLUMN: [1.0, 2.0],
        }
    )
    schema = StructType(
        [
            StructField(TIME_COLUMN, TimestampNTZType(), True),
            StructField(geography_column, StringType(), True),
            StructField(VALUE_COLUMN, DoubleType(), True),
        ]
    )
    df = spark.createDataFrame(pdf, schema=schema)

    with pytest.raises(DSGInvalidOperation, match="all null"):
        localize_timestamps_if_necessary(
            df, config, scratch_dir_context=ScratchDirContext(tmp_path)
        )


# ---------------------------------------------------------------------------
# _check_time_zone_for_mapping at project submission
# ---------------------------------------------------------------------------


def _make_mock_dataset_config(
    tmp_path,
    *,
    time_dim_model,
    use_project_geography_time_zone,
    geo_has_tz,
):
    """Build a mock DatasetConfig for submission-time validation tests."""
    time_dim = DateTimeDimensionConfig.load_from_model(time_dim_model)

    geo_config = MagicMock()
    geo_config.model = _make_geography_dimension(tmp_path, with_time_zone=geo_has_tz)

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
    config = MagicMock()
    config.get_base_time_dimension.return_value = DateTimeDimensionConfig.load_from_model(
        time_dim_model
    )
    return config


def test_submit_dataset_absolute_to_local_no_tz_raises(tmp_path):
    """Dataset in absolute time, project in local time, no time_zone on geo raises."""
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_absolute(),
        use_project_geography_time_zone=False,
        geo_has_tz=False,
    )
    project_config = _make_mock_project_config(_make_time_dimension_ntz_multi_tz())

    with pytest.raises(DSGInvalidDataset, match="time zone information"):
        ProjectRegistryManager._check_time_zone_for_mapping(project_config, dataset_config)


def test_submit_dataset_absolute_to_local_with_tz_passes(tmp_path):
    """Dataset in absolute time, project in local time, time_zone present passes."""
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_absolute(),
        use_project_geography_time_zone=False,
        geo_has_tz=True,
    )
    project_config = _make_mock_project_config(_make_time_dimension_ntz_multi_tz())

    ProjectRegistryManager._check_time_zone_for_mapping(project_config, dataset_config)


def test_submit_dataset_absolute_to_local_project_geo_flag_passes(tmp_path):
    """use_project_geography_time_zone=True defers to project geography at query time."""
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_absolute(),
        use_project_geography_time_zone=True,
        geo_has_tz=False,
    )
    project_config = _make_mock_project_config(_make_time_dimension_ntz_multi_tz())

    ProjectRegistryManager._check_time_zone_for_mapping(project_config, dataset_config)


def test_submit_dataset_absolute_to_absolute_no_tz_passes(tmp_path):
    """Both dataset and project in absolute time: no time_zone needed."""
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_absolute(),
        use_project_geography_time_zone=False,
        geo_has_tz=False,
    )
    project_config = _make_mock_project_config(_make_time_dimension_absolute())

    ProjectRegistryManager._check_time_zone_for_mapping(project_config, dataset_config)


# ---------------------------------------------------------------------------
# Index time time_zone_format
# ---------------------------------------------------------------------------


_INDEX_RANGE = {
    "start": 0,
    "end": 8783,
    "starting_timestamp": "2012-01-01 00:00:00",
    "frequency": "01:00:00",
}


def _make_index_time(time_zone_format) -> IndexTimeDimensionModel:
    return IndexTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        **{"class": "Time"},
        time_zone_format=time_zone_format,
        measurement_type=MeasurementType.TOTAL,
        ranges=[dict(_INDEX_RANGE)],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )


def _make_index_time_single_tz():
    """Build an IndexTimeDimensionModel with aligned_in_absolute_time (single tz)."""
    return _make_index_time(
        AlignedTimeSingleTimeZone(
            format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
            time_zone="Etc/GMT+5",
        )
    )


def _make_index_time_multi_tz(time_zones=None):
    """Build an IndexTimeDimensionModel with aligned_in_std_clock_time (per-geography tz)."""
    return _make_index_time(
        LocalTimeMultipleTimeZones(
            format_type=TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME,
            time_zones=time_zones or ["Etc/GMT+5", "Etc/GMT+6"],
        )
    )


def test_index_time_single_tz_does_not_require_geography_tz():
    assert not _make_index_time_single_tz().is_time_zone_required_in_geography()


def test_index_time_multi_tz_requires_geography_tz():
    assert _make_index_time_multi_tz().is_time_zone_required_in_geography()


def test_index_time_single_tz_config_accessors():
    config = IndexTimeDimensionConfig.load_from_model(_make_index_time_single_tz())
    assert config.get_time_zone() == "Etc/GMT+5"
    assert config.get_time_zones() == ["Etc/GMT+5"]
    assert config.get_tzinfo() is not None


def test_index_time_multi_tz_config_accessors():
    config = IndexTimeDimensionConfig.load_from_model(_make_index_time_multi_tz())
    assert config.get_time_zone() is None
    assert config.get_time_zones() == ["Etc/GMT+5", "Etc/GMT+6"]
    assert config.get_tzinfo() is None


def test_index_time_single_entry_multi_tz_stays_naive():
    """A one-entry multi-tz list must not behave like the single-tz format.

    get_time_zone() is format-based rather than count-based so that
    get_start_times() stays naive for IndexTimeRangeWithTZColumn.
    """
    config = IndexTimeDimensionConfig.load_from_model(
        _make_index_time_multi_tz(time_zones=["Etc/GMT+5"])
    )
    assert config.get_time_zone() is None
    assert config.get_tzinfo() is None
    assert config.get_start_times()[0].tzinfo is None


def test_index_time_single_tz_chronify_type():
    config = IndexTimeDimensionConfig.load_from_model(_make_index_time_single_tz())
    assert isinstance(config.to_chronify(), chronify.IndexTimeRange)


def test_index_time_multi_tz_chronify_type():
    config = IndexTimeDimensionConfig.load_from_model(_make_index_time_multi_tz())
    assert isinstance(config.to_chronify(), chronify.IndexTimeRangeWithTZColumn)


def test_index_time_legacy_config_defaults_to_multi_tz():
    """An index time config without time_zone_format defaults to aligned_in_std_clock_time."""
    model = IndexTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        **{"class": "Time"},
        # No time_zone_format — legacy config
        measurement_type=MeasurementType.TOTAL,
        ranges=[dict(_INDEX_RANGE)],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )
    assert model.is_time_zone_required_in_geography()
    assert model.time_zone_format.format_type == TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME


def test_index_time_single_tz_dataset_config_no_geo_tz_passes(tmp_path):
    """Dataset with single-tz IndexTime does not require time_zone in geography."""
    config = _build_dataset_config_model(
        tmp_path,
        use_project_geography_time_zone=False,
        geo_has_tz=False,
        time_dim_model=_make_index_time_single_tz(),
    )
    assert config is not None


def test_index_time_multi_tz_dataset_config_no_geo_tz_raises(tmp_path):
    """Dataset with multi-tz IndexTime requires time_zone in geography."""
    with pytest.raises((ValueError, DSGInvalidDimension)):
        _build_dataset_config_model(
            tmp_path,
            use_project_geography_time_zone=False,
            geo_has_tz=False,
            time_dim_model=_make_index_time_multi_tz(),
        )
