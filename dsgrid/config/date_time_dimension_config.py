from datetime import datetime

import pyspark.sql.functions as F
import pandas as pd

from dsgrid.dimension.time import TimeZone, make_time_range
from dsgrid.exceptions import DSGInvalidDataset
from .dimensions import DateTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.utils.spark import read_dataframe


class DateTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a DateTimeDimensionModel."""

    @staticmethod
    def model_class():
        return DateTimeDimensionModel

    def get_tzmap(self, time_ranges_TZ, load_data_for_time_check):
        if time_ranges_TZ == TimeZone.LOCAL:
            # load map as spark df and raise error if not found
            tz_map = read_dataframe(filename, cache=False)  # FIXME
            # LL-TODO: if time_ranges_tz==None (default), apply pydantic checks
            # 1. tz_map file exist
            # 2. from_id/geography in tz_map file matches geography dimension
            tz_map = tz_map.select(
                F.col("from_id").alias("geography"),
                F.udf(lambda x: str(TimeZone(x).tz))(F.col("to_id")).alias("convert_to_timezone"),
            ).cache()

        else:
            # create a map based on time_ranges_tz
            tz_map = load_data_for_time_check.select(
                "geography", F.lit(str(time_ranges_TZ.tz)).alias("convert_to_timezone")
            )

        return tz_map

    def check_dataset_time_consistency(self, load_data_for_time_check):
        """
        all time is stored on disk in UTC, but when loaded, displayed in pyspark session tz

        Returns
        -------
        load_data_for_time_check2 : pyspark.sql.DataFrame
            a copy of "load_data_for_time_check" with timestamp col converted based on tz_map

        """
        data_TZ = self.get_tzinfo()
        time_ranges = self.get_time_ranges()
        time_ranges_TZ = self.get_time_ranges_tzinfo()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()

        if data_TZ != time_ranges_TZ:
            # convert data timestamps to time_ranges_tz and compare to expected_timestamps
            tz_map = self.get_tzmap(time_ranges_TZ)
            orig_cols = load_data_for_time_check.columns
            # Join time with tz_map
            load_data_for_time_check2 = load_data_for_time_check.join(
                tz_map,
                "geography",
                "left",
            )

            # convert time based on map
            load_data_for_time_check2 = load_data_for_time_check2.withColumn(
                "timestamp_converted",
                F.from_utc_timestamp("timestamp", F.col("convert_to_timezone")),
            ).drop(
                "convert_to_timezone"
            )  # tz=UTC

            # Note: x.timestamp.astimezone().astimezone(data_tz): converts to pyspark session time!!!, which is another setting from spark.sql.session.timeZone

            actual_timestamps = [
                x.timestamp.astimezone(TimeZone.UTC.tz).replace(tzinfo=None)
                for x in load_data_for_time_check2.select(
                    F.col("timestamp_converted").alias("timestamp")
                )
                .distinct()
                .sort("timestamp_converted")
                .collect()
            ]  # check this against EFS comstock test data.

        else:
            load_data_for_time_check2 = load_data_for_time_check.withColumn(
                "timestamp_converted", F.col("timestamp")
            )
            actual_timestamps = [
                x.timestamp.astimezone().astimezone(data_tz)
                for x in load_data_for_time_check2.select("timestamp")
                .distinct()
                .sort("timestamp")
                .collect()
            ]

        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            logger.info(
                load_data_for_time_check2[["geography", "timestamp", "timestamp_converted"]].show()
            )
            raise DSGInvalidDataset(
                f"dataset timestamps do not match expected timestamps. mismatch={mismatch}"
            )
        else:
            load_data_for_time_check2 = load_data_for_time_check2.drop(
                "timestamp"
            ).withColumnRenamed("timestamp_converted", "timestamp")

        return load_data_for_time_check2

    def convert_dataframe(self, df):
        return df

    def get_frequency(self):
        return self.model.frequency

    def get_time_ranges(self):
        ranges = []
        tz = self.get_tzinfo().tz
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
        return self.model.timezone

    def get_time_ranges_tzinfo(self):
        return self.model.ranges_timezone
