import logging
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from dsgrid.utils.files import delete_if_exists


logger = logging.getLogger(__name__)


class ScratchDirContext:
    """Manages the lifetime of files in a scratch directory."""

    def __init__(self, scratch_dir: Path):
        self._scratch_dir = scratch_dir
        self._paths: set[Path] = set()
        if not self._scratch_dir.exists():
            self._scratch_dir.mkdir()

    @property
    def scratch_dir(self) -> Path:
        """Return the scratch directory."""
        return self._scratch_dir

    def add_tracked_path(self, path: Path) -> None:
        """Add tracking of a path in the scratch directory."""
        self._paths.add(path)

    def list_tracked_paths(self) -> list[Path]:
        """Return a list of paths being tracked."""
        return list(self._paths)

    def get_temp_filename(self, prefix=None, suffix=None, add_tracked_path=True) -> Path:
        """Return a temporary filename based in the scratch directory.

        Parameters
        ----------
        prefix : str | None
            Forwarded to NamedTemporaryFile.
        suffix : str | None
            Forwarded to NamedTemporaryFile.
        add_tracked_path : bool
            If True, add tracking of the path
        """
        with NamedTemporaryFile(dir=self._scratch_dir, prefix=prefix, suffix=suffix) as f:
            path = Path(f.name)
            if add_tracked_path:
                self._paths.add(path)
            return Path(f.name)

    def finalize(self) -> None:
        """Remove all tracked paths once use of them is complete."""
        for path in self._paths:
            delete_if_exists(path)
            logger.info("Deleted temporary path %s", path)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.finalize()
        if not list(self._scratch_dir.iterdir()):
            os.rmdir(self._scratch_dir)
