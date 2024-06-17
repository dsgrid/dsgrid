import logging
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    IntegerType,
    DoubleType,
    StringType,
)
import pyspark.sql.functions as F

from dsgrid.dimension.time import (
    IndexTimeRange,
    DatetimeRange,
    TimeZone,
    TimeBasedDataAdjustmentModel,
)
from dsgrid.dimension.time_utils import (
    create_adjustment_map_from_model_time,
    build_index_time_map,
)
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidParameter
from dsgrid.time.types import DatetimeTimestampType, IndexTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import get_spark_session
from .dimensions import IndexTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.config.dimensions import TimeRangeModel


logger = logging.getLogger(__name__)


class IndexTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a IndexTimeDimensionModel."""

    @staticmethod
    def model_class():
        return IndexTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns):
        logger.info("Check IndexTimeDimensionConfig dataset time consistency.")
        if len(time_columns) > 1:
            msg = f"IndexTimeDimensionConfig expects only one time column, but has {time_columns=}"
            raise DSGInvalidParameter(msg)
        time_col = time_columns[0]

        # check indices are consistent with ranges
        index_ranges = self.get_time_ranges()
        assert len(index_ranges) == 1, len(index_ranges)
        index_range = index_ranges[0]

        expected_indices = set(index_range.list_time_range())
        actual_indices = set(
            [
                x[time_col]
                for x in load_data_df.filter(f"{time_col} is not null")
                .select(time_col)
                .distinct()
                .collect()
            ]
        )

        if expected_indices != actual_indices:
            mismatch = sorted(expected_indices.symmetric_difference(actual_indices))
            raise DSGInvalidDataset(
                f"load_data {time_col}s do not match expected times. mismatch={mismatch}"
            )

    def build_time_dataframe(self, model_years=None):
        # shows time as indices

        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        model_time = self.list_expected_dataset_timestamps(model_years=model_years)
        schema = StructType(
            [
                StructField(time_col, IntegerType(), False),
            ]
        )
        df_time = get_spark_session().createDataFrame(model_time, schema=schema)

        return df_time

    def convert_dataframe(
        self,
        df,
        project_time_dim,
        value_columns,
        model_years=None,
        wrap_time_allowed=False,
        time_based_data_adjustment=None,
    ):
        df = self._map_index_time_to_datetime(
            df,
            project_time_dim,
            value_columns,
            time_based_data_adjustment=time_based_data_adjustment,
        )
        df = self._convert_time_to_project_time(
            df,
            project_time_dim=project_time_dim,
            wrap_time=wrap_time_allowed,
        )

        return df

    def _map_index_time_to_datetime(
        self, df, project_time_dim, value_columns, time_based_data_adjustment=None
    ):
        if time_based_data_adjustment is None:
            time_based_data_adjustment = TimeBasedDataAdjustmentModel()
        idx_col = self.get_load_data_time_columns()
        assert len(idx_col) == 1, idx_col
        idx_col = idx_col[0]

        time_col = DatetimeTimestampType._fields[0]

        ptime_col = project_time_dim.get_load_data_time_columns()
        assert len(ptime_col) == 1, ptime_col
        ptime_col = ptime_col[0]

        # local time zones, create time zone map to covert
        assert "time_zone" in df.columns, df.columns
        geo_tz = [TimeZone(row.time_zone) for row in df.select("time_zone").distinct().collect()]
        assert geo_tz
        # indices correspond to clock time laid out like Standard Time
        geo_tz2 = [tz.get_standard_time() for tz in geo_tz]

        schema = StructType(
            [
                StructField(idx_col, IntegerType(), False),
                StructField(time_col, TimestampType(), False),
                StructField("multiplier", DoubleType(), False),
                StructField("time_zone", StringType(), False),
            ]
        )
        time_map = get_spark_session().createDataFrame([], schema=schema)
        for tz, tz2 in zip(geo_tz, geo_tz2):
            # table is built in standard time but listed as prevailing
            index_map = build_index_time_map(
                self, timezone=tz2.tz, time_based_data_adjustment=time_based_data_adjustment
            )
            index_map = index_map.withColumn("time_zone", F.lit(tz.value))

            # time_based_data_adjustment mapping table
            table = create_adjustment_map_from_model_time(self, time_based_data_adjustment, tz)
            index_map = (
                index_map.selectExpr(idx_col, "time_zone", f"{time_col} AS model_time")
                .join(table, ["model_time"], "right")
                .drop("model_time")
            )
            time_map = time_map.union(index_map.select(schema.names))
        df = df.join(time_map, on=[idx_col, "time_zone"], how="inner").drop(idx_col, "time_zone")
        groupby = [x for x in df.columns if x not in value_columns + ["multiplier"]]
        df = df.groupBy(*groupby).agg(
            *[F.sum(F.col(col) * F.col("multiplier")).alias(col) for col in value_columns]
        )
        return df

    def get_frequency(self):
        return self.model.frequency

    def get_time_ranges(self, model_years=None):
        dt_ranges = self._create_represented_time_ranges()
        ranges = []
        time_ranges = self._build_time_ranges(
            dt_ranges, self.model.str_format, model_years=model_years, tz=self.get_tzinfo()
        )
        for index_range, time_range in zip(self.model.ranges, time_ranges):
            ranges.append(
                IndexTimeRange(
                    start=time_range[0],
                    end=time_range[1],
                    frequency=self.model.frequency,
                    start_index=index_range.start,
                )
            )

        return ranges

    def _get_represented_time_ranges(self, model_years=None):
        dt_ranges = self._create_represented_time_ranges()
        ranges = []
        for start, end in self._build_time_ranges(
            dt_ranges, self.model.str_format, model_years=model_years, tz=self.get_tzinfo()
        ):
            ranges.append(
                DatetimeRange(
                    start=start,
                    end=end,
                    frequency=self.model.frequency,
                )
            )

        return ranges

    def get_load_data_time_columns(self):
        return list(IndexTimestampType._fields)

    def get_tzinfo(self):
        return None

    def get_time_interval_type(self):
        return self.model.time_interval_type

    def list_expected_dataset_timestamps(self, model_years=None):
        # list timestamps as indices
        indices = []
        for index_range in self.get_time_ranges(model_years=model_years):
            indices += [IndexTimestampType(x) for x in index_range.list_time_range()]
        return indices

    def _list_represented_dataset_timestamps(self, model_years=None):
        timestamps = []
        for time_range in self.get_time_ranges(model_years=model_years):
            timestamps += [DatetimeTimestampType(x) for x in time_range.list_time_range()]
        return timestamps

    def _create_represented_time_ranges(self):
        """create datetime ranges from index ranges."""
        ranges = []
        for ts, range in zip(self.model.starting_timestamps, self.model.ranges):
            start = datetime.strptime(ts, self.model.str_format)
            steps = range.end - range.start
            end = start + self.model.frequency * steps
            ts_range = TimeRangeModel(start=str(start), end=str(end))
            ranges.append(ts_range)
        return ranges
