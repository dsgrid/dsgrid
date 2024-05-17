from dsgrid.dataset.models import TableFormatType
from .table_format_handler_base import TableFormatHandlerBase

from .unpivoted_table import UnpivotedTableHandler


def make_table_format_handler(
    format_type: TableFormatType, project_config, dataset_id=None
) -> TableFormatHandlerBase:
    """Return a format handler for the passed type."""
    match format_type:
        case TableFormatType.UNPIVOTED:
            handler = UnpivotedTableHandler(project_config, dataset_id=dataset_id)
        case _:
            raise NotImplementedError(str(format_type))

    return handler
