import logging
from pyspark.sql.types import StructType, StructField, TimestampType

from dsgrid.dimension.time import make_time_range
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import DatetimeTimestampType, IndexedTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import get_spark_session
from .dimensions import IndexedTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class IndexedTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a IndexedTimeDimensionModel."""

    @staticmethod
    def model_class():
        return IndexedTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns):
        logger.info("Check IndexedTimeDimensionConfig dataset time consistency.")
        if len(time_columns) > 1:
            raise ValueError(
                "IndexedTimeDimensionConfig expects only one column from "
                f"get_load_data_time_columns, but has {time_columns}"
            )
        time_col = time_columns[0]

        # check indices are consistent with index_ranges
        index_ranges = self.model.index_ranges
        assert len(index_ranges) == 1, len(index_ranges)
        index_range = index_ranges[0]

        expected_indices = [
            x
            for x in range(
                index_range.start, index_range.end + index_range.interval, index_range.interval
            )
        ]
        actual_indices = [
            x[time_col]
            for x in load_data_df.select(time_col)
            .distinct()
            .filter(f"{time_col} is not null")
            .sort(time_col)
            .collect()
        ]

        if expected_indices != actual_indices:
            mismatch = sorted(set(expected_indices).symmetric_difference(set(actual_indices)))
            raise DSGInvalidDataset(
                f"load_data {time_col}s do not match expected times. mismatch={mismatch}"
            )

    def build_time_dataframe(self, model_years=None):
        # Note: DF.show() displays time in session time, which may be confusing.
        # But timestamps are stored correctly here

        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        schema = StructType([StructField(time_col, TimestampType(), False)])
        model_time = self.list_expected_dataset_timestamps(model_years=model_years)
        df_time = get_spark_session().createDataFrame(model_time, schema=schema)

        return df_time

    def convert_dataframe(
        self, df, project_time_dim, model_years=None, value_columns=None, wrap_time_allowed=False
    ):
        # convert from index to timestamps

        df = self._convert_time_to_project_time_interval(
            df=df, project_time_dim=project_time_dim, wrap_time=wrap_time_allowed
        )
        return df

    def get_frequency(self):
        return self.model.frequency

    def get_time_ranges(self, model_years=None):
        ranges = []
        for start, end in self._build_time_ranges(
            self.model.ranges, self.model.str_format, model_years=model_years, tz=self.get_tzinfo()
        ):
            ranges.append(
                make_time_range(
                    start=start,
                    end=end,
                    frequency=self.model.frequency,
                    leap_day_adjustment=self.model.data_adjustment.leap_day_adjustment,
                    time_interval_type=self.model.time_interval_type,
                )
            )

        return ranges

    def get_load_data_time_columns(self):
        return list(IndexedTimestampType._fields)

    def get_tzinfo(self):
        return self.model.timezone.tz

    def get_time_interval_type(self):
        return self.model.time_interval_type

    def list_expected_dataset_timestamps(self, model_years=None):
        # this shows timestamps in local system tz since input tz is none
        timestamps = []
        for time_range in self.get_time_ranges(model_years=model_years):
            timestamps += [DatetimeTimestampType(x) for x in time_range.list_time_range()]
        return timestamps
