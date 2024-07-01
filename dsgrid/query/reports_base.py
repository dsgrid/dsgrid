import abc
from pathlib import Path

from dsgrid.query.models import ProjectQueryModel


class ReportsBase(abc.ABC):
    """Base class for pre-defined reports"""

    @abc.abstractmethod
    def check_query(self, model: ProjectQueryModel) -> None:
        """Check compatibility of the user query with the report."""

    @abc.abstractmethod
    def generate(self, filename: Path, output_dir: Path, inputs) -> None:
        """Generate the report on df into output_dir."""
