import logging
from pathlib import Path

from dsgrid.common import VALUE_COLUMN
from dsgrid.data_models import DSGBaseModel
from dsgrid.dataset.models import ValueFormat
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidQuery
from dsgrid.query.models import ProjectQueryModel
from dsgrid.utils.dataset import ordered_subset_columns
from dsgrid.utils.files import delete_if_exists
from dsgrid.ibis_api import read_dataframe
from .query_context import QueryContext
from .reports_base import ReportsBase


logger = logging.getLogger(__name__)


class PeakLoadInputModel(DSGBaseModel):
    group_by_columns: list[str]


class PeakLoadReport(ReportsBase):
    """Find peak load in a derived dataset."""

    REPORT_FILENAME = "peak_load.parquet"

    def check_query(self, query: ProjectQueryModel) -> None:
        if query.result.table_format.format_type != ValueFormat.STACKED:
            msg = "The PeakLoadReport requires the value format to be stacked."
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
        # expr = [F.max(x).alias(x) for x in value_columns]
        # Ibis syntax
        # Note: join_cols requires the values column to be present in peak_load to join back on value?
        # That logic seems to find rows matching the peak load.

        aggregations = {x: df[x].max() for x in value_columns}
        peak_load = df.group_by(*group_by_columns).aggregate(**aggregations)

        join_cols = group_by_columns + value_columns
        time_columns = context.get_dimension_column_names(DimensionType.TIME)
        diff = time_columns.difference(df.columns)
        if diff:
            msg = f"BUG: expected time column(s) {diff} are not present in table"
            raise Exception(msg)
        columns = ordered_subset_columns(df, time_columns) + join_cols

        right_df = df.select(*columns)
        select_cols = list(peak_load.columns) + list(time_columns)
        with_time = (
            peak_load.inner_join(right_df, join_cols)
            .select(select_cols)
            .order_by(*group_by_columns)
        )
        output_file = output_dir / PeakLoadReport.REPORT_FILENAME
        delete_if_exists(output_file)

        # with_time is an Ibis Table
        from dsgrid.ibis_api import get_ibis_connection

        get_ibis_connection().to_parquet(with_time, output_file)
        logger.info("Wrote Peak Load Report to %s", output_file)
        return output_file
