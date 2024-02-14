import os
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile


class ScratchDirContext:
    """Manages the lifetime of files in a scratch directory."""

    def __init__(self, scratch_dir: Path):
        self._scratch_dir = scratch_dir
        self._paths: list[Path] = []
        if not self._scratch_dir.exists():
            self._scratch_dir.mkdir()

    @property
    def scratch_dir(self) -> Path:
        """Return the scratch directory."""
        return self._scratch_dir

    def add_tracked_path(self, path: Path) -> None:
        """Add tracking of a path in the scratch directory."""
        self._paths.append(path)

    def list_tracked_paths(self) -> list[Path]:
        """Return a list of paths being tracked."""
        return self._paths[:]

    def get_temp_filename(self, prefix=None, suffix=None) -> Path:
        """Return a temporary filename based in the scratch directory.

        Parameters
        ----------
        prefix : str | None
            Forwarded to NamedTemporaryFile.
        suffix : str | None
            Forwarded to NamedTemporaryFile.
        """
        with NamedTemporaryFile(dir=self._scratch_dir, prefix=prefix, suffix=suffix) as f:
            return Path(f.name)

    def finalize(self) -> None:
        """Remove all tracked paths once use of them is complete."""
        for path in self._paths:
            if path.exists():
                if path.is_file():
                    path.unlink()
                else:
                    shutil.rmtree(path)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.finalize()
        if not list(self._scratch_dir.iterdir()):
            os.rmdir(self._scratch_dir)
