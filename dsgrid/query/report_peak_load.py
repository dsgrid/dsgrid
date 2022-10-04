import logging
from pathlib import Path
from typing import List

import pyspark.sql.functions as F

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.utils.spark import read_dataframe
from .models import TableFormatType
from .query_context import QueryContext
from .reports_base import ReportsBase


logger = logging.getLogger(__name__)


class PeakLoadInputModel(DSGBaseModel):

    group_by_columns: List[str]


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
        if context.get_table_format_type() == TableFormatType.PIVOTED:
            value_columns = list(context.get_pivoted_columns())
        else:
            # TODO: this is TBD
            # value_columns = ["value"]
            raise Exception(f"Unsupported table format {context.get_table_format_type()}")

        df = read_dataframe(filename)
        expr = [F.max(x).alias(x) for x in value_columns]
        peak_load = df.groupBy(*inputs.group_by_columns).agg(*expr)
        join_cols = inputs.group_by_columns + value_columns
        time_columns = context.get_dimension_query_names(DimensionType.TIME)
        diff = time_columns.difference(df.columns)
        if diff:
            raise Exception(f"BUG: expected time column(s) {diff} are not present in table")
        with_time = peak_load.join(df.select(*time_columns, *join_cols), on=join_cols).sort(
            *inputs.group_by_columns
        )
        output_file = output_dir / PeakLoadReport.REPORT_FILENAME
        with_time.write.mode("overwrite").parquet(str(output_file))
        logger.info("Wrote Peak Load Report to %s", output_file)
        return output_file
