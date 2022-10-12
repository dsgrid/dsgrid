import logging
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, IntegerType

import pandas as pd

from dsgrid.dimension.time import make_time_range
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import AnnualTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import _get_spark_session
from .dimensions import AnnualTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class AnnualTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an AnnualTimeDimensionModel."""

    @staticmethod
    def model_class():
        return AnnualTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df):
        logger.info("Check AnnualTimeDimensionConfig dataset time consistency.")
        time_col = self.get_timestamp_load_data_columns()
        if len(time_col) > 1:
            raise ValueError(
                "AnnualTimeDimensionConfig expects only one column from "
                f"get_timestamp_load_data_columns, but has {time_col}"
            )
        time_col = time_col[0]
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()
        actual_timestamps = [
            pd.Timestamp(str(x[time_col]), tz=self.get_tzinfo()).to_pydatetime()
            for x in load_data_df.select(time_col).distinct().sort(time_col).collect()
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            raise DSGInvalidDataset(
                f"load_data {time_col}s do not match expected times. mismatch={mismatch}"
            )

    def build_time_dataframe(self):
        time_col = self.get_timestamp_load_data_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        schema = StructType([StructField(time_col, IntegerType(), False)])

        model_time = self.list_expected_dataset_timestamps()
        df_time = _get_spark_session.createDataFrame(model_time, schema=schema)
        return df_time

    # def build_time_dataframe_with_time_zone(self):
    #     return self.build_time_dataframe()

    def convert_dataframe(self, df=None, project_time_dim=None, time_zone_mapping=None):
        return df

    def get_frequency(self):
        return timedelta(days=365)

    def get_time_ranges(self):
        ranges = []
        frequency = self.get_frequency()
        for time_range in self.model.ranges:
            start = datetime.strptime(time_range.start, self.model.str_format)
            start = pd.Timestamp(start)
            end = datetime.strptime(time_range.end, self.model.str_format)
            end = pd.Timestamp(end)
            ranges.append(
                make_time_range(
                    start=start,
                    end=end,
                    frequency=frequency,
                    leap_day_adjustment=None,
                )
            )

        return ranges

    def get_timestamp_load_data_columns(self):
        return list(AnnualTimestampType._fields)

    def get_tzinfo(self):
        return None

    def list_expected_dataset_timestamps(self):
        # TODO: need to support validation of multiple time ranges: DSGRID-173
        assert len(self.model.ranges) == 1, self.model.ranges
        start, end = (int(self.model.ranges[0].start), int(self.model.ranges[0].end))
        return [AnnualTimestampType(x) for x in range(start, end + 1)]
