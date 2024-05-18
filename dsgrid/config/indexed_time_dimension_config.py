import logging
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    IntegerType,
)
import pyspark.sql.functions as F
from zoneinfo import ZoneInfo
from datetime import timedelta

from dsgrid.dimension.time import make_time_range, TimeZone
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import DatetimeTimestampType, IndexedTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import get_spark_session
from .dimensions import IndexedTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.dimension.time import (
    DataAdjustmentModel,
    LeapDayAdjustmentType,
    DaylightSavingFallBackType,
    get_dls_fallback_time_change_by_time_range,
)


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
        index_ranges = self._get_index_ranges()
        assert len(index_ranges) == 1, len(index_ranges)
        index_range = index_ranges[0]

        expected_indices = index_range.list_time_range()
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

    def build_time_dataframe(self, model_years=None, timezone=None, data_adjustment=None):
        # shows time as indices

        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        model_time = self._list_expected_dataset_time_indices(
            model_years=model_years, timezone=timezone, data_adjustment=data_adjustment
        )
        rep_time = self.list_expected_dataset_timestamps(
            model_years=model_years, timezone=timezone, data_adjustment=data_adjustment
        )
        ts_time_col = rep_time[0]._fields[0]
        schema = StructType(
            [
                StructField(time_col, IntegerType(), False),
                StructField(ts_time_col, TimestampType(), False),
            ]
        )
        # TODO: temp fix
        data = []
        for a, b in zip(model_time, rep_time):
            data.append((a[0], b[0]))
        df_time = get_spark_session().createDataFrame(data, schema=schema)

        return df_time

    def convert_dataframe(
        self,
        df,
        project_time_dim,
        model_years=None,
        value_columns=None,
        wrap_time_allowed=False,
        data_adjustment=None,
    ):
        if data_adjustment is None:
            data_adjustment = DataAdjustmentModel()

        # convert from index to timestamps -- TODO
        if self.model.timezone in [TimeZone.LOCAL_PREVAILING, TimeZone.LOCAL_STANDARD]:
            time_col = self.get_load_data_time_columns()
            assert len(time_col) == 1, time_col
            time_col = time_col[0]
            ptime_col = project_time_dim.get_load_data_time_columns()
            assert len(ptime_col) == 1, ptime_col
            ptime_col = ptime_col[0]

            assert "time_zone" in df.columns, df.columns
            geo_tz = [
                TimeZone(row.time_zone) for row in df.select("time_zone").distinct().collect()
            ]
            assert geo_tz
            assert (
                self.model.timezone == TimeZone.LOCAL_STANDARD
            ), f"{self.model.timezone} is not LOCAL_STANDARD"
            geo_tz_from = [tz.get_standard_time() for tz in geo_tz]

            tzf = geo_tz_from[0]  # TODO convert to for-loop
            index_map = self.build_time_dataframe(model_years=model_years, timezone=tzf.tz)
            index_map = index_map.withColumn("timezone", F.lit(tzf.tz_name))

            time_indices = [
                x[0]
                for x in self._list_expected_dataset_time_indices(
                    model_years=model_years, timezone=tzf.tz
                )
            ]
            timestamps = [
                x[0]
                for x in self.list_expected_dataset_timestamps(
                    model_years=model_years, timezone=tzf.tz
                )
            ]  # model_time
            from_timezones = [tzf.tz_name for x in timestamps]

            schema = StructType(
                [
                    StructField(time_col, IntegerType(), False),
                    StructField(timestamps[0]._fields[0], TimestampType(), False),
                    StructField("time_zone", TimestampType(), False),
                ]
            )

            from_tz_map = get_spark_session().createDataFrame(
                zip(time_indices, timestamps, from_timezones), schema=schema
            )
            from_tz_map

            #     to_tz_map = self._convert_time_zone(from_tz_map.withColumn("to_time_zone", F.lit(tzt.tz_name)), ptime_col, "time_zone", "to_time_zone")
            #     # create map from index to correct clock time
            #     tz_map = from_tz_map.select(time_col, ptime_col).join(to_tz_map.select(ptime_col, ("to_time_zone").alias("time_zone")), on=ptime_col, how="right")
            #     time_map = time_map.union(tz_map)

            # df = df.join(time_map, on=[time_col, "time_zone"], how="inner")
            # TODO: handle interpolation

            return df

    def _create_adjustment_map_from_model_time(
        self, data_adjustment: DataAdjustmentModel, time_zone: TimeZone, model_years=None
    ):
        """Create data adjustment mapping from model_time to prevailing time of input time_zone."""
        ld_adj = data_adjustment.leap_day_adjustment
        fb_adj = data_adjustment.daylight_saving_adjustment.fall_back_hour

        TZ_st, TZ_pt = time_zone.get_standard_time(), time_zone.get_prevailing_time()
        ranges = self.get_time_ranges(
            model_years=model_years, timezone=TZ_pt.tz, data_adjustment=data_adjustment
        )
        freq = self.model.frequency
        model_time, prevailing_time, multipliers = [], [], []
        for range in ranges:
            cur_pt = range.start.to_pydatetime()
            end_pt = range.end.to_pydatetime()

            if fb_adj == DaylightSavingFallBackType.INTERPOLATE:
                fb_times = get_dls_fallback_time_change_by_time_range(
                    cur_pt, end_pt, frequency=freq
                )  # in PT
                fb_repeats = [0 for x in fb_times]
                print(fb_times)

            cur = range.start.to_pydatetime().astimezone(ZoneInfo("UTC"))
            end = range.end.to_pydatetime().astimezone(ZoneInfo("UTC")) + freq

            while cur < end:
                multiplier = 1.0
                frequency = freq
                cur_pt = cur.astimezone(TZ_pt.tz)
                model_ts = cur_pt.replace(tzinfo=TZ_st.tz)
                month = cur_pt.month
                day = cur_pt.day
                if ld_adj == LeapDayAdjustmentType.DROP_FEB29 and month == 2 and day == 29:
                    cur += frequency
                    pass
                if ld_adj == LeapDayAdjustmentType.DROP_DEC31 and month == 12 and day == 31:
                    cur += frequency
                    pass
                if ld_adj == LeapDayAdjustmentType.DROP_JAN1 and month == 1 and day == 1:
                    cur += frequency
                    pass

                if fb_adj == DaylightSavingFallBackType.INTERPOLATE:
                    for i, ts in enumerate(fb_times):
                        if cur == ts.astimezone(ZoneInfo("UTC")):
                            if fb_repeats[i] == 0:
                                frequency = timedelta(0)
                                multiplier = 0.5
                            if fb_repeats[i] == 1:
                                model_ts = (
                                    (cur + frequency).astimezone(TZ_pt.tz).replace(tzinfo=TZ_st.tz)
                                )
                                multiplier = 0.5
                            fb_repeats[i] += 1

                model_time.append(model_ts)
                prevailing_time.append(cur_pt)
                multipliers.append(multiplier)
                cur += frequency

        table = get_spark_session().createDataFrame(
            zip(model_time, prevailing_time, multipliers),
            ["model_time", "prevailing_time", "multiplier"],
        )
        return table

    @staticmethod
    def _convert_time_zone(df, time_col: str, from_tz, to_tz):
        """convert dataframe from one single time zone to another"""
        nontime_cols = [col for col in df.columns if col != time_col]
        df2 = df.select(
            F.from_utc_timestamp(F.to_utc_timestamp(F.col(time_col), from_tz), to_tz).alias(
                time_col
            ),
            *nontime_cols,
        )
        return df2

    def get_frequency(self):
        return self.model.frequency

    def get_time_ranges(self, model_years=None, timezone=None, data_adjustment=None):
        if timezone is None:
            timezone = self.get_tzinfo()
        if data_adjustment is None:
            data_adjustment = DataAdjustmentModel()
        ranges = []
        for start, end in self._build_time_ranges(
            self.model.ranges, self.model.str_format, model_years=model_years, tz=timezone
        ):
            ranges.append(
                make_time_range(
                    start=start,
                    end=end,
                    frequency=self.model.frequency,
                    data_adjustment=data_adjustment,
                    time_interval_type=self.model.time_interval_type,
                )
            )

        return ranges

    def _get_index_ranges(self, model_years=None, timezone=None, data_adjustment=None):
        if timezone is None:
            timezone = self.get_tzinfo()
        if data_adjustment is None:
            data_adjustment = DataAdjustmentModel()
        ranges = []
        time_ranges = self._build_time_ranges(
            self.model.ranges, self.model.str_format, model_years=model_years, tz=timezone
        )
        for index_range, range in zip(self.model.index_ranges, time_ranges):
            ranges.append(
                make_time_range(
                    start=range[0],
                    end=range[1],
                    frequency=self.model.frequency,
                    data_adjustment=data_adjustment,
                    time_interval_type=self.model.time_interval_type,
                    start_index=index_range.start,
                    step=index_range.interval,
                )
            )

        return ranges

    def get_load_data_time_columns(self):
        return list(IndexedTimestampType._fields)

    def get_tzinfo(self):
        return self.model.timezone.tz

    def get_time_interval_type(self):
        return self.model.time_interval_type

    def list_expected_dataset_timestamps(
        self, model_years=None, timezone=None, data_adjustment=None
    ):
        timestamps = []
        for time_range in self.get_time_ranges(
            model_years=model_years, timezone=timezone, data_adjustment=data_adjustment
        ):
            timestamps += [DatetimeTimestampType(x) for x in time_range.list_time_range()]
        return timestamps

    def _list_expected_dataset_time_indices(
        self, model_years=None, timezone=None, data_adjustment=None
    ):
        # this shows the timestamps as indices
        indices = []
        for index_range in self._get_index_ranges(
            model_years=model_years, timezone=timezone, data_adjustment=data_adjustment
        ):
            indices += [IndexedTimestampType(x) for x in index_range.list_time_range()]
        return indices
