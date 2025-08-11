import abc
from pathlib import Path
from typing import Any

from dsgrid.query.models import ProjectQueryModel
from dsgrid.query.query_context import QueryContext


class ReportsBase(abc.ABC):
    """Base class for pre-defined reports"""

    @abc.abstractmethod
    def check_query(self, query: ProjectQueryModel) -> None:
        """Check compatibility of the user query with the report."""

    @abc.abstractmethod
    def generate(
        self, filename: Path, output_dir: Path, context: QueryContext, inputs: Any
    ) -> Path:
        """Generate the report on df into output_dir."""
