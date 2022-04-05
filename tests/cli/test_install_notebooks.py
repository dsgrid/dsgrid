from pathlib import Path
from tempfile import TemporaryDirectory

from dsgrid.utils.run_command import check_run_command


def test_install_notebooks():
    with TemporaryDirectory() as tmpdir:
        check_run_command(f"dsgrid install-notebooks --path={tmpdir}")
        assert list((Path(tmpdir) / "dsgrid-notebooks").iterdir())
