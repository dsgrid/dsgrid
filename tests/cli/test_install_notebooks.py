from pathlib import Path
from tempfile import TemporaryDirectory

from dsgrid.utils.run_command import check_run_command, run_command


def test_install_notebooks():
    with TemporaryDirectory() as tmpdir:
        expected_filename = "registration.ipynb"
        check_run_command(f"dsgrid install-notebooks --path={tmpdir}")
        files = list((Path(tmpdir) / "dsgrid-notebooks").iterdir())
        assert files
        assert files[0].name == expected_filename
        assert run_command(f"dsgrid install-notebooks --path={tmpdir}") != 0
        assert run_command(f"dsgrid install-notebooks --path={tmpdir} --force ") == 0
        files = list((Path(tmpdir) / "dsgrid-notebooks").iterdir())
        assert files
        assert files[0].name == expected_filename
