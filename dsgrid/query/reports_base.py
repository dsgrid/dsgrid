import abc
from pathlib import Path


class ReportsBase(abc.ABC):
    """Base class for pre-defined reports"""

    @abc.abstractmethod
    def generate(self, filename: Path, output_dir: Path, inputs):
        """Generate the report on df into output_dir."""
