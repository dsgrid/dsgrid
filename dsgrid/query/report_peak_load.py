import logging
from pathlib import Path

from dsgrid.common import VALUE_COLUMN
from dsgrid.data_models import DSGBaseModel
from dsgrid.dataset.models import TableFormatType
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidQuery
from dsgrid.query.models import ProjectQueryModel
from dsgrid.spark.functions import join_multiple_columns
from dsgrid.spark.types import F
from dsgrid.utils.dataset import ordered_subset_columns
from dsgrid.utils.files import delete_if_exists
from dsgrid.utils.spark import read_dataframe
from .query_context import QueryContext
from .reports_base import ReportsBase


logger = logging.getLogger(__name__)


class PeakLoadInputModel(DSGBaseModel):
    group_by_columns: list[str]


class PeakLoadReport(ReportsBase):
    """Find peak load in a derived dataset."""

    REPORT_FILENAME = "peak_load.parquet"

    def check_query(self, query: ProjectQueryModel) -> None:
        if query.result.table_format.format_type != TableFormatType.UNPIVOTED:
            msg = "The PeakLoadReport requires the table format type to be unpivoted."
            raise DSGInvalidQuery(msg)

    def generate(
        self,
        filename: Path,
        output_dir: Path,
        context: QueryContext,
        inputs: PeakLoadInputModel,
    ) -> Path:
        value_columns = [VALUE_COLUMN]
        metric_columns = context.get_dimension_column_names(DimensionType.METRIC)
        if len(metric_columns) > 1:
            msg = f"Bug: {metric_columns=}"
            raise Exception(msg)
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
            msg = f"BUG: expected time column(s) {diff} are not present in table"
            raise Exception(msg)
        columns = ordered_subset_columns(df, time_columns) + join_cols
        with_time = join_multiple_columns(peak_load, df.select(*columns), join_cols).sort(
            *group_by_columns
        )
        output_file = output_dir / PeakLoadReport.REPORT_FILENAME
        delete_if_exists(output_file)
        with_time.write.parquet(str(output_file))
        logger.info("Wrote Peak Load Report to %s", output_file)
        return output_file
