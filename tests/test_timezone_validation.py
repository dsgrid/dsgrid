"""Tests for timezone validation improvements.

dsgrid takes geographic time zones from the geography dimension whose record IDs the
data carries at that point in the pipeline: the dataset's own geography during
registration, and the mapping target during query-time mapping when geography is mapped.
These tests cover the layers of defense for the case where the selected geography records
are missing valid ``time_zone`` values but time mapping requires them:

- ``DatasetConfigModel.check_time_zone`` catches at config load time what registration
  will need.
- ``ProjectRegistryManager._check_time_zone_for_mapping`` catches at dataset-submission
  time what query-time mapping will need.
- ``DatasetSchemaHandlerBase._get_time_zone_geography_dimension`` selects the geography
  at query time and raises when its records cannot supply time zones.
- ``localize_timestamps_if_necessary`` raises on an all-null runtime join.
- ``check_timezone_in_geography`` detects all-null ``time_zone`` values.
"""

from unittest.mock import MagicMock, patch

import chronify
import pandas as pd
import pytest

from dsgrid.common import TIME_COLUMN, TIME_ZONE_COLUMN, VALUE_COLUMN
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
from dsgrid.config.dimension_config import DimensionConfig
from dsgrid.config.index_time_dimension_config import IndexTimeDimensionConfig
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dataset.dataset_schema_handler_two_table import TwoTableDatasetSchemaHandler
from dsgrid.dataset.models import ValueFormat
from dsgrid.dimension.base_models import DimensionType, check_timezone_in_geography
from dsgrid.dimension.time import (
    MeasurementType,
    TimeBasedDataAdjustmentModel,
    TimeIntervalType,
    TimeZoneFormat,
)
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension, DSGInvalidOperation
from dsgrid.ibis.operations import drop_columns
from dsgrid.ibis.session import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)
from dsgrid.registry.project_registry_manager import ProjectRegistryManager
from dsgrid.utils.dataset import add_time_zone, localize_timestamps_if_necessary
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from tests._helpers import (
    DummyDatasetConfig,
    DummyGeoDim,
    make_table,
    skip_unless_duckdb,
)
from tests.test_localize_timestamps_if_necessary import _make_multi_tz_dataframe


def _make_geo_csv(tmp_path, *, with_time_zone: bool, filename="geography.csv"):
    """Create a geography CSV with or without a time_zone column."""
    path = tmp_path / filename
    if with_time_zone:
        path.write_text("id,name,time_zone\ng1,Geo 1,Etc/GMT+5\n")
    else:
        path.write_text("id,name\ng1,Geo 1\n")
    return str(path)


def _make_geography_dimension(tmp_path, *, with_time_zone: bool, filename="geography.csv"):
    """Build a DimensionModel for geography."""
    geo_file = _make_geo_csv(tmp_path, with_time_zone=with_time_zone, filename=filename)
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


def _build_dataset_config_model(tmp_path, *, geo_has_tz: bool, time_dim_model, **extra):
    """Build a DatasetConfigModel with minimal required fields."""
    geo_dim = _make_geography_dimension(tmp_path, with_time_zone=geo_has_tz)
    return DatasetConfigModel(
        dataset_id="test_dataset",
        data_classification=DataClassificationType.LOW,
        dimensions=[time_dim_model, geo_dim],
        **extra,
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
    """Naive timestamps are localized at registration, so geography time_zone is required."""
    with pytest.raises((ValueError, DSGInvalidDimension), match="registration"):
        _build_dataset_config_model(
            tmp_path,
            geo_has_tz=False,
            time_dim_model=_make_time_dimension_ntz_multi_tz(),
        )


def test_dataset_config_with_tz_in_geography_passes(tmp_path):
    """Naive timestamps plus a valid time_zone column passes."""
    config = _build_dataset_config_model(
        tmp_path,
        geo_has_tz=True,
        time_dim_model=_make_time_dimension_ntz_multi_tz(),
    )
    assert config is not None


def test_dataset_config_tz_aware_no_tz_passes(tmp_path):
    """timestamp_tz needs no registration localization, so geography time_zone is optional.

    Whether query-time mapping can find time zones is decided at submission, once the
    geography mapping is known.
    """
    config = _build_dataset_config_model(
        tmp_path,
        geo_has_tz=False,
        time_dim_model=_make_time_dimension_tz_multi_tz(),
    )
    assert config is not None


def test_dataset_config_drops_deprecated_time_zone_flag(tmp_path, caplog):
    """A config carrying the removed use_project_geography_time_zone field still loads."""
    with caplog.at_level("WARNING"):
        config = _build_dataset_config_model(
            tmp_path,
            geo_has_tz=True,
            time_dim_model=_make_time_dimension_ntz_multi_tz(),
            use_project_geography_time_zone=True,
        )
    assert not hasattr(config, "use_project_geography_time_zone")
    assert "use_project_geography_time_zone" in caplog.text


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


def _make_mock_dataset_config(tmp_path, *, time_dim_model, geo_has_tz):
    """Build a mock DatasetConfig for submission-time validation tests."""
    time_dim = DateTimeDimensionConfig.load_from_model(time_dim_model)

    geo_config = MagicMock()
    geo_config.model = _make_geography_dimension(
        tmp_path, with_time_zone=geo_has_tz, filename="dataset_geography.csv"
    )

    config = MagicMock()
    config.get_time_dimension.return_value = time_dim
    config.get_dimension.return_value = geo_config
    config.config_id = "test_dataset"
    return config


def _make_mock_project_config(tmp_path, time_dim_model, *, geo_has_tz=True):
    """Build a mock ProjectConfig for submission-time validation tests."""
    geo_config = MagicMock()
    geo_config.model = _make_geography_dimension(
        tmp_path, with_time_zone=geo_has_tz, filename="project_geography.csv"
    )

    config = MagicMock()
    config.get_base_time_dimension.return_value = DateTimeDimensionConfig.load_from_model(
        time_dim_model
    )
    config.get_base_dimension.return_value = geo_config
    return config


def test_submit_dataset_project_geography_supplies_time_zones(tmp_path):
    """A project query always reads time zones from the project's base geography.

    The dataset's own geography has no time_zone column, which does not matter.
    """
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_absolute(),
        geo_has_tz=False,
    )
    project_config = _make_mock_project_config(
        tmp_path, _make_time_dimension_ntz_multi_tz(), geo_has_tz=True
    )

    ProjectRegistryManager._check_time_zone_for_mapping(project_config, dataset_config)


def test_submit_dataset_project_geography_without_time_zone_raises(tmp_path):
    """The dataset's time zones cannot stand in for the project's at mapping time."""
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_absolute(),
        geo_has_tz=True,
    )
    project_config = _make_mock_project_config(
        tmp_path, _make_time_dimension_ntz_multi_tz(), geo_has_tz=False
    )

    with pytest.raises(DSGInvalidDataset, match="project's base geography dimension"):
        ProjectRegistryManager._check_time_zone_for_mapping(project_config, dataset_config)


def test_submit_dataset_absolute_to_absolute_no_tz_passes(tmp_path):
    """Both dataset and project in absolute time: no time_zone needed."""
    dataset_config = _make_mock_dataset_config(
        tmp_path,
        time_dim_model=_make_time_dimension_absolute(),
        geo_has_tz=False,
    )
    project_config = _make_mock_project_config(
        tmp_path, _make_time_dimension_absolute(), geo_has_tz=False
    )

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
        geo_has_tz=False,
        time_dim_model=_make_index_time_single_tz(),
    )
    assert config is not None


def test_index_time_multi_tz_dataset_config_no_geo_tz_passes(tmp_path):
    """Index time is never localized at registration, so config load does not require tz.

    Multi-tz index time does need geography time zones, but only at mapping time, where
    the geography that supplies them depends on the dataset's geography mapping.
    """
    config = _build_dataset_config_model(
        tmp_path,
        geo_has_tz=False,
        time_dim_model=_make_index_time_multi_tz(),
    )
    assert config is not None


# ---------------------------------------------------------------------------
# Query-time geography selection
# ---------------------------------------------------------------------------


def _make_geography_config(tmp_path, *, with_time_zone: bool, filename):
    return DimensionConfig.load_from_model(
        _make_geography_dimension(tmp_path, with_time_zone=with_time_zone, filename=filename)
    )


def _make_handler(dataset_geo_dim):
    """Build a stand-in handler exposing what _get_time_zone_geography_dimension reads."""
    handler = MagicMock()
    handler._config.get_dimension_with_records.return_value = dataset_geo_dim
    handler.dataset_id = "test_dataset"
    return handler


def test_select_geography_uses_mapping_target_when_geography_is_mapped(tmp_path):
    """When geography was mapped, the target's records own the ids in the column."""
    dataset_geo = _make_geography_config(tmp_path, with_time_zone=True, filename="ds.csv")
    to_geo = _make_geography_config(tmp_path, with_time_zone=True, filename="project.csv")

    selected = DatasetSchemaHandlerBase._get_time_zone_geography_dimension(
        _make_handler(dataset_geo), to_geo
    )
    assert selected is to_geo


def test_select_geography_uses_dataset_when_geography_is_not_mapped(tmp_path):
    """When geography was not mapped, the dataset's own records own the ids."""
    dataset_geo = _make_geography_config(tmp_path, with_time_zone=True, filename="ds.csv")

    selected = DatasetSchemaHandlerBase._get_time_zone_geography_dimension(
        _make_handler(dataset_geo), None
    )
    assert selected is dataset_geo


def test_select_geography_raises_when_mapping_target_has_no_time_zone(tmp_path):
    """A dataset geography with time zones does not rescue a target without them."""
    dataset_geo = _make_geography_config(tmp_path, with_time_zone=True, filename="ds.csv")
    to_geo = _make_geography_config(tmp_path, with_time_zone=False, filename="project.csv")

    with pytest.raises(DSGInvalidDataset, match="mapped into"):
        DatasetSchemaHandlerBase._get_time_zone_geography_dimension(
            _make_handler(dataset_geo), to_geo
        )


def test_select_geography_raises_when_dataset_has_no_time_zone(tmp_path):
    """Unmapped geography without time zones names the dataset's own dimension."""
    dataset_geo = _make_geography_config(tmp_path, with_time_zone=False, filename="ds.csv")

    with pytest.raises(DSGInvalidDataset, match="dataset's own geography dimension"):
        DatasetSchemaHandlerBase._get_time_zone_geography_dimension(
            _make_handler(dataset_geo), None
        )


def test_select_geography_raises_when_dataset_has_no_geography_records(tmp_path):
    """No geography dimension with records is a dataset error, not an assertion."""
    with pytest.raises(DSGInvalidDataset, match="no geography dimension with records"):
        DatasetSchemaHandlerBase._get_time_zone_geography_dimension(_make_handler(None), None)


def _make_mapping_manager(scratch_dir_context):
    """Build a mapping manager stand-in that runs the operation and does not persist."""
    manager = MagicMock()
    manager.has_completed_operation.return_value = False
    manager.plan.map_time_op.persist = False
    manager.scratch_dir_context = scratch_dir_context
    return manager


def _run_convert_time_dimension(df, geo_dim, scratch_dir_context):
    """Run _convert_time_dimension with chronify stubbed out.

    Returns the patched ``add_time_zone`` so the caller can assert whether the geography
    join happened. chronify cannot map ``DatetimeRangeWithTZColumn`` at all, so the real
    mapper cannot stand in here.

    Only the DuckDB dispatcher is stubbed, hence the ``skip_unless_duckdb`` on the callers.
    The guard under test sits before the backend dispatch and is itself backend-agnostic.
    """
    from_time_dim = DateTimeDimensionConfig.load_from_model(_make_time_dimension_ntz_multi_tz())
    to_time_dim = DateTimeDimensionConfig.load_from_model(_make_time_dimension_absolute())
    handler = _make_handler(geo_dim)
    handler._config.get_time_dimension.return_value = from_time_dim
    # Keep the real selection logic; only the mapping_manager and chronify are stand-ins.
    handler._get_time_zone_geography_dimension.side_effect = lambda to_geo: (
        DatasetSchemaHandlerBase._get_time_zone_geography_dimension(handler, to_geo)
    )

    module = "dsgrid.dataset.dataset_schema_handler_base"
    with (
        patch(f"{module}.add_time_zone", side_effect=add_time_zone) as mock_add_tz,
        patch(f"{module}.map_time_dimension_with_chronify_duckdb", side_effect=lambda df, **_: df),
    ):
        result = DatasetSchemaHandlerBase._convert_time_dimension(
            handler,
            load_data_df=df,
            to_time_dim=to_time_dim,
            value_column=VALUE_COLUMN,
            mapping_manager=_make_mapping_manager(scratch_dir_context),
            wrap_time_allowed=False,
            time_based_data_adjustment=TimeBasedDataAdjustmentModel(),
            to_geo_dim=None,
        )
    return mock_add_tz, result


@skip_unless_duckdb
def test_convert_time_skips_join_when_time_zone_column_present(
    spark, scratch_dir_context, tmp_path
):
    """A table already carrying time_zone must not be joined with geography records again.

    Registration-time localization leaves the column on the registered data, and a
    one-table dataset may supply it directly. Joining again collides on the column name.
    """
    df = _make_multi_tz_dataframe(spark, time_zones=("Etc/GMT+5",))
    assert TIME_ZONE_COLUMN in df.columns

    geo_dim = _make_geography_config(tmp_path, with_time_zone=True, filename="geo.csv")
    # Without the guard, this is the join that _convert_time_dimension would perform.
    with pytest.raises(DSGInvalidOperation, match="collide"):
        add_time_zone(df, geo_dim)

    mock_add_tz, result = _run_convert_time_dimension(df, geo_dim, scratch_dir_context)
    mock_add_tz.assert_not_called()
    assert TIME_ZONE_COLUMN not in result.columns


@skip_unless_duckdb
def test_convert_time_joins_when_time_zone_column_absent(spark, scratch_dir_context, tmp_path):
    """Without the column, the selected geography records still supply it."""
    df = drop_columns(_make_multi_tz_dataframe(spark, time_zones=("Etc/GMT+5",)), TIME_ZONE_COLUMN)
    assert TIME_ZONE_COLUMN not in df.columns

    geo_dim = _make_geography_config(tmp_path, with_time_zone=True, filename="geo.csv")
    mock_add_tz, result = _run_convert_time_dimension(df, geo_dim, scratch_dir_context)
    mock_add_tz.assert_called_once()
    assert TIME_ZONE_COLUMN not in result.columns


# ---------------------------------------------------------------------------
# time_zone as a data column in the two-table format
# ---------------------------------------------------------------------------


def _make_two_table_handler(load_data, lookup):
    """Build a two-table handler exposing only what the column checks read."""
    handler = object.__new__(TwoTableDatasetSchemaHandler)
    handler._load_data = load_data
    handler._load_data_lookup = lookup
    handler._config = MagicMock()
    handler._config.get_value_format.return_value = ValueFormat.STACKED
    time_dim = MagicMock()
    time_dim.get_load_data_time_columns.return_value = ["timestamp"]
    handler._config.get_time_dimension.return_value = time_dim
    return handler


def _two_table_frames(*, lookup_has_tz: bool, load_data_has_tz: bool):
    load_columns = ["id", "timestamp", VALUE_COLUMN]
    load_rows = [("1", "2018-01-01 00:00:00", 1.0), ("1", "2018-01-01 01:00:00", 2.0)]
    if load_data_has_tz:
        load_columns.append(TIME_ZONE_COLUMN)
        load_rows = [row + ("Etc/GMT+5",) for row in load_rows]

    lookup_columns = ["id", "geography", "sector", "subsector", "metric", "model_year", "scenario"]
    lookup_row = ("1", "g1", "com", "hospital", "electricity", "2018", "reference")
    if lookup_has_tz:
        lookup_columns.append(TIME_ZONE_COLUMN)
        lookup_row = lookup_row + ("Etc/GMT+5",)

    return make_table(load_columns, *load_rows), make_table(lookup_columns, lookup_row)


def test_two_table_lookup_accepts_time_zone_column(spark):
    """The lookup may carry time_zone, matching what one-table already allows."""
    load_data, lookup = _two_table_frames(lookup_has_tz=True, load_data_has_tz=False)
    handler = _make_two_table_handler(load_data, lookup)
    handler._check_lookup_data_consistency()


def test_two_table_load_data_accepts_time_zone_column(spark):
    """load_data may carry time_zone, matching what one-table already allows."""
    load_data, lookup = _two_table_frames(lookup_has_tz=False, load_data_has_tz=True)
    handler = _make_two_table_handler(load_data, lookup)
    handler._check_dataset_internal_consistency()


def test_two_table_still_rejects_unknown_columns(spark):
    """Allowing time_zone must not open the door to arbitrary columns."""
    load_data = make_table(
        ["id", "timestamp", VALUE_COLUMN, "not_a_dimension"],
        ("1", "2018-01-01 00:00:00", 1.0, "x"),
    )
    _, lookup = _two_table_frames(lookup_has_tz=False, load_data_has_tz=False)
    handler = _make_two_table_handler(load_data, lookup)
    with pytest.raises(DSGInvalidDataset, match="not_a_dimension"):
        handler._check_dataset_internal_consistency()
