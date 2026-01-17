import logging
from datetime import timedelta
from dateutil.relativedelta import relativedelta

import ibis
import ibis.expr.types as ir
import pandas as pd
from chronify.time_range_generator_factory import make_time_range_generator

from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import AnnualTimeRange
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import AnnualTimestampType
from dsgrid.dimension.time_utils import build_annual_ranges, is_leap_year
from dsgrid.ibis_api import (
    set_session_time_zone,
    handle_column_spaces,
    select_expr,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from .dimensions import AnnualTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class AnnualTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an AnnualTimeDimensionModel.

    Note: Annual time does not currently support Chronify conversion because the annual time
    to datetime mapping is not yet available in Chronify.
    """

    @staticmethod
    def model_class() -> AnnualTimeDimensionModel:
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
        # Ibis execution
        # Ensure time_col is selected properly
        actual_data = (
            load_data_df.select(time_col)
            .distinct()
            .filter(load_data_df[time_col].notnull())
            .order_by(time_col)
            .to_pyarrow()
            .to_pylist()
        )
        actual_timestamps = [
            pd.Timestamp(str(x[time_col]), tz=self.get_tzinfo()).to_pydatetime()
            for x in actual_data
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            msg = f"load_data {time_col}s do not match expected times. mismatch={mismatch}"
            raise DSGInvalidDataset(msg)

    def build_time_dataframe(self) -> ir.Table:
        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        model_time = self.list_expected_dataset_timestamps()
        # model_time is list of namedtuples.
        df_time = ibis.memtable(pd.DataFrame(model_time))
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

    def list_expected_dataset_timestamps(self) -> list[AnnualTimestampType]:
        timestamps = []
        for start, end, freq in build_annual_ranges(self.model.ranges, tz=self.get_tzinfo()):
            year = start.year
            while year <= end.year:
                timestamps.append(AnnualTimestampType(year))
                year += freq
        return timestamps


def map_annual_time_to_date_time(
    df: ir.Table,
    annual_dim: AnnualTimeDimensionConfig,
    dt_dim: DateTimeDimensionConfig,
    value_columns: set[str],
) -> ir.Table:
    """Map a DataFrame with an annual time dimension to a DateTime time dimension."""
    annual_col = annual_dim.get_load_data_time_columns()[0]
    myear_column = DimensionType.MODEL_YEAR.value
    timestamps = make_time_range_generator(dt_dim.to_chronify()).list_timestamps()
    time_cols = dt_dim.get_load_data_time_columns()
    assert len(time_cols) == 1, time_cols
    time_col = time_cols[0]

    dt_df = ibis.memtable(
        pd.DataFrame([(x.to_pydatetime(),) for x in timestamps], columns=[time_col])
    )

    # Get the year from the datetime profile to determine if leap year
    with set_session_time_zone(dt_dim.model.time_zone_format.time_zone):
        years = (
            select_expr(dt_df, [f"YEAR({handle_column_spaces(time_col)}) AS year"])
            .distinct()
            .to_pyarrow()
            .to_pylist()
        )
        if len(years) != 1:
            msg = f"DateTime dimension has more than one year: {years=}"
            raise NotImplementedError(msg)

        if annual_dim.model.include_leap_day and is_leap_year(years[0]["year"]):
            measured_duration = timedelta(days=366)
        else:
            measured_duration = timedelta(days=365)

    # Cross join annual data with datetime timestamps
    df2 = df.cross_join(dt_df)

    # Calculate scale factor as measured_duration / frequency
    # This gives the number of time periods in a year (e.g., 8760 hours for non-leap year)
    frequency: timedelta = dt_dim.get_frequency()
    scale = measured_duration / frequency

    mutations = {myear_column: df2[annual_col].cast("string")}
    for column in value_columns:
        mutations[column] = df2[column] / scale

    df2 = df2.mutate(**mutations)
    # Use explicit select instead of drop to avoid DuckDB execution issues
    cols_to_keep = [c for c in df2.columns if c != annual_col]
    df2 = df2.select(cols_to_keep)

    return df2
