from dsgrid.dataset.models import ValueFormat
from .table_format_handler_base import TableFormatHandlerBase

from .unpivoted_table import UnpivotedTableHandler


def make_table_format_handler(
    value_format: ValueFormat, project_config, dataset_id=None
) -> TableFormatHandlerBase:
    """Return a format handler for the passed value format."""
    match value_format:
        case ValueFormat.STACKED:
            handler = UnpivotedTableHandler(project_config, dataset_id=dataset_id)
        case _:
            raise NotImplementedError(str(value_format))

    return handler
