import logging
from pathlib import Path

import pyspark.sql.functions as F

from dsgrid.common import VALUE_COLUMN
from dsgrid.data_models import DSGBaseModel
from dsgrid.dataset.models import TableFormatType
from dsgrid.dimension.base_models import DimensionType
from dsgrid.utils.dataset import ordered_subset_columns
from dsgrid.utils.spark import read_dataframe
from .query_context import QueryContext
from .reports_base import ReportsBase


logger = logging.getLogger(__name__)


class PeakLoadInputModel(DSGBaseModel):

    group_by_columns: list[str]


class PeakLoadReport(ReportsBase):
    """Find peak load in a derived dataset."""

    REPORT_FILENAME = "peak_load.parquet"

    def generate(
        self,
        filename: Path,
        output_dir: Path,
        context: QueryContext,
        inputs: PeakLoadInputModel,
    ):
        match context.get_table_format_type():
            case TableFormatType.PIVOTED:
                value_columns = list(context.get_pivoted_columns())
                group_by_columns = inputs.group_by_columns
            case TableFormatType.UNPIVOTED:
                value_columns = [VALUE_COLUMN]
                metric_columns = context.get_dimension_column_names(DimensionType.METRIC)
                if len(metric_columns) > 1:
                    raise Exception(f"Bug: {metric_columns=}")
                metric_column = next(iter(metric_columns))
                group_by_columns = inputs.group_by_columns[:]
                if metric_column not in group_by_columns:
                    group_by_columns.append(metric_column)

        df = read_dataframe(filename)
        expr = [F.max(x).alias(x) for x in value_columns]
        peak_load = df.groupBy(*group_by_columns).agg(*expr)
        join_cols = group_by_columns + value_columns
        time_columns = context.get_dimension_column_names(DimensionType.TIME)
        diff = time_columns.difference(df.columns)
        if diff:
            raise Exception(f"BUG: expected time column(s) {diff} are not present in table")
        columns = ordered_subset_columns(df, time_columns) + join_cols
        with_time = peak_load.join(df.select(*columns), on=join_cols).sort(*group_by_columns)
        output_file = output_dir / PeakLoadReport.REPORT_FILENAME
        with_time.write.mode("overwrite").parquet(str(output_file))
        logger.info("Wrote Peak Load Report to %s", output_file)
        return output_file
