import logging
from datetime import datetime

import pandas as pd
import pyspark.sql.functions as F

from dsgrid.dimension.time import TimeZone
from dsgrid.dimension.time import TimeZone, make_time_range
from dsgrid.exceptions import DSGInvalidDataset
from .dimensions import DateTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class DateTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a DateTimeDimensionModel."""

    @staticmethod
    def model_class():
        return DateTimeDimensionModel

    def check_dataset_time_consistency(self, load_data_df):
        tz = self.get_tzinfo()
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()
        actual_timestamps = [
            x.timestamp.astimezone().astimezone(tz)
            for x in load_data_df.select("timestamp").distinct().sort("timestamp").collect()
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            raise DSGInvalidDataset(
                f"load_data timestamps do not match expected times. mismatch={mismatch}"
            )

        timestamps_by_id = (
            load_data_df.select("timestamp", "id")
            .groupby("id")
            .agg(F.countDistinct("timestamp").alias("distinct_timestamps"))
        )
        distinct_counts = timestamps_by_id.select("distinct_timestamps").distinct()
        expected_count = len(expected_timestamps)
        if distinct_counts.count() != 1:
            for row in timestamps_by_id.collect():
                if row.distinct_timestamps != len(expected_timestamps):
                    logger.error(
                        "load_data ID=%s does not have %s timestamps: actual=%s",
                        row.id,
                        len(expected_timestamps),
                        row.distinct_timestamps,
                    )

            raise DSGInvalidDataset(
                f"One or more arrays do not have {len(expected_timestamps)} timestamps"
            )

        val = distinct_counts.collect()[0].distinct_timestamps
        if val != expected_count:
            raise DSGInvalidDataset(
                f"load_data arrays do not have {len(expected_timestamps)} "  \
                "timestamps: actual={row.distinct_timestamps}"
            )


    def convert_dataframe(self, df):
        return df

    def get_frequency(self):
        return self.model.frequency

    def get_time_ranges(self):
        ranges = []
        tz = self.get_tzinfo()
        for time_range in self.model.ranges:
            start = datetime.strptime(time_range.start, self.model.str_format)
            start = pd.Timestamp(start, tz=tz)
            end = datetime.strptime(time_range.end, self.model.str_format)
            end = pd.Timestamp(end, tz=tz)
            ranges.append(
                make_time_range(
                    start=start,
                    end=end,
                    frequency=self.model.frequency,
                    leap_day_adjustment=self.model.leap_day_adjustment,
                )
            )

        return ranges

    def get_timestamp_load_data_columns(self):
        return ["timestamp"]

    def get_tzinfo(self):
        assert self.model.timezone is not TimeZone.LOCAL, self.model.timezone
        return self.model.timezone.tz
