import logging
from datetime import datetime, timedelta

import pandas as pd

from dsgrid.dimension.time import make_time_range
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import AnnualTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
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
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173
        expected_timestamps = time_range.list_time_range()
        actual_timestamps = [
            pd.Timestamp(str(x.year)).to_pydatetime()
            for x in load_data_df.select("year").distinct().sort("year").collect()
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            raise DSGInvalidDataset(
                f"load_data timestamps do not match expected times. mismatch={mismatch}"
            )

    def convert_dataframe(self, df, project_time_dim):
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
        return ["year"]

    def get_tzinfo(self):
        return None

    def list_expected_dataset_timestamps(self):
        # TODO: need to support validation of multiple time ranges: DSGRID-173
        assert len(self.model.ranges) == 1, self.model.ranges
        start, end = (int(self.model.ranges[0].start), int(self.model.ranges[0].end))
        return [AnnualTimestampType(x) for x in range(start, end + 1)]
