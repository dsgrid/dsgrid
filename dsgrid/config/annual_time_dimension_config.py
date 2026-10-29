import ibis
import logging
from datetime import timedelta
from typing import Type
from dateutil.relativedelta import relativedelta

import pandas as pd
from chronify.time_range_generator_factory import make_time_range_generator

from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import AnnualTimeRange
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import AnnualTimestampType
from dsgrid.dimension.time_utils import is_leap_year, build_annual_ranges
from dsgrid.ibis.operations import cross_join, filter_sql
from dsgrid.ibis.table_utils import is_table_empty, table_column_to_list, table_to_records
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.ibis.session import get_runtime_session
from dsgrid.ibis.tz import custom_time_zone
from .dimensions import AnnualTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class AnnualTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an AnnualTimeDimensionModel.

    Note: Annual time does not currently support Chronify conversion because the annual time
    to datetime mapping is not yet available in Chronify.
    """

    @staticmethod
    def model_class() -> Type[AnnualTimeDimensionModel]:
        return AnnualTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns) -> None:
        logger.info("Check AnnualTimeDimensionConfig dataset time consistency.")
        if len(time_columns) > 1:
            msg = (
                "AnnualTimeDimensionConfig expects only one column from "
                f"get_load_data_time_columns, but has {time_columns}"
            )
            raise ValueError(msg)
        time_col = time_columns[0]
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()
        actual_timestamps_df = filter_sql(
            load_data_df.select(time_col).distinct(), f"{time_col} IS NOT NULL"
        ).order_by(time_col)
        actual_timestamps = [
            pd.Timestamp(str(value), tz=self.get_tzinfo()).to_pydatetime()
            for value in table_column_to_list(actual_timestamps_df, time_col)
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            msg = f"load_data {time_col}s do not match expected times. mismatch={mismatch}"
            raise DSGInvalidDataset(msg)

    def build_time_dataframe(self) -> ibis.Table:
        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        model_time = self.list_expected_dataset_timestamps()
        df_time = get_runtime_session().createDataFrame(model_time, schema=[time_col])
        return df_time

    def get_frequency(self) -> relativedelta:
        freqs = [trange.frequency for trange in self.model.ranges]
        if len(set(freqs)) > 1:
            msg = f"AnnualTimeDimensionConfig.get_frequency found multiple frequencies: {freqs}"
            raise ValueError(msg)
        return relativedelta(years=freqs[0])

    def get_time_ranges(self) -> list[AnnualTimeRange]:
        ranges = []
        for start, end, freq in build_annual_ranges(self.model.ranges, tz=self.get_tzinfo()):
            ranges.append(
                AnnualTimeRange(
                    start=start,
                    end=end,
                    frequency=freq,
                )
            )

        return ranges

    def get_start_times(self) -> list[pd.Timestamp]:
        start_times = []
        for start, _, _ in build_annual_ranges(self.model.ranges, tz=self.get_tzinfo()):
            start_times.append(start)

        return start_times

    def get_lengths(self) -> list[int]:
        lengths = []
        for start, end, freq in build_annual_ranges(self.model.ranges, tz=self.get_tzinfo()):
            if (end.year - start.year) % freq == 0:
                length = (end.year - start.year) // freq + 1
            else:
                # In case where end year is not inclusive
                length = (end.year - start.year) // freq
            lengths.append(length)
        return lengths

    def get_load_data_time_columns(self) -> list[str]:
        return list(AnnualTimestampType._fields)

    def get_time_zone(self) -> None:
        return None

    def get_tzinfo(self) -> None:
        return None

    def get_time_interval_type(self) -> None:
        return None

    def list_expected_dataset_timestamps(
        self, time_based_data_adjustment=None
    ) -> list[AnnualTimestampType]:
        timestamps = []
        for start, end, freq in build_annual_ranges(self.model.ranges, tz=self.get_tzinfo()):
            year = start.year
            while year <= end.year:
                timestamps.append(AnnualTimestampType(year))
                year += freq
        return timestamps


def map_annual_time_to_date_time(
    df: ibis.Table,
    annual_dim: AnnualTimeDimensionConfig,
    dt_dim: DateTimeDimensionConfig,
    value_columns: set[str],
) -> ibis.Table:
    """Map an Ibis table with an annual time dimension to a DateTime time dimension.

    Raises
    ------
    DSGInvalidDataset
        If ``df`` already has a model_year column whose values do not match the
        annual time column.
    """
    annual_col = annual_dim.get_load_data_time_columns()[0]
    myear_column = DimensionType.MODEL_YEAR.value
    if myear_column in df.columns:
        _check_model_year_matches_annual_time(df, annual_col, myear_column)
    timestamps = make_time_range_generator(dt_dim.to_chronify()).list_timestamps()
    time_cols = dt_dim.get_load_data_time_columns()
    assert len(time_cols) == 1, time_cols
    time_col = time_cols[0]
    dt_df = get_runtime_session().createDataFrame(
        [(x.to_pydatetime(),) for x in timestamps], schema=[time_col]
    )

    # Note that MeasurementType.TOTAL has already been verified, i.e.,
    # each value associated with an annual time represents the total over that year.
    #
    # The custom_time_zone context manager makes .year() resolve in the
    # target TZ on Spark by swapping spark.sql.session.timeZone. On DuckDB it
    # sets the connection TimeZone, which only affects TIMESTAMPTZ columns —
    # which dt_df[time_col] is, because chronify's list_timestamps() returns
    # TZ-aware pandas Timestamps and createDataFrame maps those to
    # ``timestamp('UTC')`` (TIMESTAMPTZ). See dsgrid.ibis.tz for the broader
    # cross-backend contract.
    # Pass dt_df[time_col] so custom_time_zone fails loudly if it is ever a naive DuckDB
    # timestamp: .year() would otherwise silently resolve in UTC and pick the wrong divisor.
    with custom_time_zone(dt_dim.model.time_zone_format.time_zone, dt_df[time_col]):
        years = table_column_to_list(
            dt_df.select(year=dt_df[time_col].year()).distinct(),
            "year",
        )
        if len(years) != 1:
            msg = f"DateTime dimension has more than one year: {years=}"
            raise NotImplementedError(msg)
        if annual_dim.model.include_leap_day and is_leap_year(years[0]):
            measured_duration = timedelta(days=366)
        else:
            measured_duration = timedelta(days=365)

    df2 = cross_join(df, dt_df)
    frequency: timedelta = dt_dim.get_frequency()
    value_divisor = measured_duration / frequency
    exprs: dict[str, ibis.Expr] = {}
    for column in df2.columns:
        if column == annual_col:
            continue
        if column in value_columns:
            exprs[column] = df2[column] / value_divisor
        else:
            exprs[column] = df2[column]
    if myear_column not in df.columns:
        exprs[myear_column] = df2[annual_col].cast("string")
    return df2.select(**exprs)  # ty: ignore[invalid-argument-type]


def _check_model_year_matches_annual_time(
    df: ibis.Table, annual_col: str, myear_column: str
) -> None:
    """Verify that existing model_year values match the annual time column.

    Raises
    ------
    DSGInvalidDataset
        If any (annual time, model_year) pair disagrees, including a NULL on
        one side only.
    """
    pairs = df.select(annual_col, myear_column).distinct()
    mismatches = pairs.filter(~pairs[annual_col].cast("string").identical_to(pairs[myear_column]))
    if not is_table_empty(mismatches):
        invalid = table_to_records(mismatches.limit(100))
        msg = (
            f"The existing {myear_column} column must match the annual time column "
            f"{annual_col} when mapping annual time to datetime. mismatches={invalid}"
        )
        raise DSGInvalidDataset(msg)
