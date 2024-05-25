import logging
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    IntegerType,
    DoubleType,
    StringType,
)
import pyspark.sql.functions as F

from dsgrid.dimension.time import make_time_range, TimeZone, DataAdjustmentModel
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import DatetimeTimestampType, IndexedTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import get_spark_session
from .dimensions import IndexedTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.common import VALUE_COLUMN


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
            for x in load_data_df.filter(f"{time_col} is not null")
            .select(time_col)
            .distinct()
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

        idx_col = self.get_load_data_time_columns()
        assert len(idx_col) == 1, idx_col
        idx_col = idx_col[0]

        time_col = DatetimeTimestampType._fields[0]

        ptime_col = project_time_dim.get_load_data_time_columns()
        assert len(ptime_col) == 1, ptime_col
        ptime_col = ptime_col[0]

        # if single real time zone, convert from index to timestamps
        if self.model.timezone not in [
            TimeZone.LOCAL,
            TimeZone.LOCAL_MODEL,
        ]:
            index_map = self.build_time_dataframe(
                model_years=model_years,
                timezone=self.model.timezone.tz,
                data_adjustment=data_adjustment,
            )
            df = df.join(index_map, idx_col, "left").drop(idx_col)
        else:
            # if local time zones, create time zone map to covert
            assert "time_zone" in df.columns, df.columns
            geo_tz = [
                TimeZone(row.time_zone) for row in df.select("time_zone").distinct().collect()
            ]
            assert geo_tz
            if self.model.timezone == TimeZone.LOCAL_MODEL:
                # for LocalModel time, indices correspond to clock time laid out like Standard Time
                geo_tz2 = [tz.get_standard_time() for tz in geo_tz]
            else:
                geo_tz2 = geo_tz

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
                # for LocalModel time, table is built in standard time but listed as prevailing
                index_map = self.build_time_dataframe(
                    model_years=model_years, timezone=tz2.tz, data_adjustment=data_adjustment
                )
                index_map = index_map.withColumn("time_zone", F.lit(tz.value))

                if self.model.timezone == TimeZone.LOCAL_MODEL:
                    # data_adjustment mapping table
                    table = self._create_adjustment_map_from_model_time(data_adjustment, tz)
                    index_map = (
                        index_map.selectExpr(idx_col, "time_zone", f"{time_col} AS model_time")
                        .join(table, ["model_time"], "right")
                        .drop("model_time")
                    )
                else:
                    index_map = index_map.withColumn("multiplier", F.lit(1.0))
                time_map = time_map.union(index_map.select(schema.names))

            df = df.join(time_map, on=[idx_col, "time_zone"], how="inner").drop(
                *[idx_col, "time_zone"]
            )
            groupby = [x for x in df.columns if x not in [VALUE_COLUMN, "multiplier"]]
            df = df.groupBy(*groupby).agg(
                F.sum(F.col(VALUE_COLUMN) * F.col("multiplier")).alias(VALUE_COLUMN)
            )
        df = self._convert_time_to_project_time_interval(
            df=df, project_time_dim=project_time_dim, wrap_time=wrap_time_allowed
        )

        return df

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
