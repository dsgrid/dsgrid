from datetime import timedelta

from .dimensions import AnnualTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


class AnnualTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an AnnualTimeDimensionModel."""

    @staticmethod
    def model_class():
        return AnnualTimeDimensionModel

    def check_dataset_time_consistency(self, load_data_df):
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()
        actual_timestamps = [
            pd.Timestamp(str(x)).to_pydatetime()
            for x in load_data_df.select("timestamp").distinct().sort("timestamp").collect()
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            raise DSGInvalidDataset(
                f"load_data timestamps do not match expected times. mismatch={mismatch}"
            )

    def convert_dataframe(self, df):
        return df

    def get_frequency(self):
        return timedelta(days=365)

    def get_time_ranges(self):
        ranges = []
        for time_range in self.model.ranges:
            start = datetime.strptime(time_range.start, self.model.str_format)
            start = pd.Timestamp(start)
            end = datetime.strptime(time_range.end, self.model.str_format)
            end = pd.Timestamp(end)
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
        return ["year"]

    def get_tzinfo(self):
        return None
