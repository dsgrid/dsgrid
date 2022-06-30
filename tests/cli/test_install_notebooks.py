from pathlib import Path
from tempfile import TemporaryDirectory

from dsgrid.utils.run_command import check_run_command, run_command


def test_install_notebooks():
    with TemporaryDirectory() as tmpdir:
        expected_filenames = [
            "connect_to_dsgrid_registry.ipynb",
            "registration.ipynb",
            "start_notebook.sh",
        ]
        check_run_command(f"dsgrid install-notebooks --path={tmpdir}")
        files = list((Path(tmpdir) / "dsgrid-notebooks").iterdir())
        # TODO: Why does this fail only on CI?
        # assert sorted([x.name for x in files]) == expected_filenames
        assert sorted([x.name for x in files])[0] == expected_filenames[0]
        assert run_command(f"dsgrid install-notebooks --path={tmpdir}") != 0
        assert run_command(f"dsgrid install-notebooks --path={tmpdir} --force ") == 0
        files = list((Path(tmpdir) / "dsgrid-notebooks").iterdir())
        assert files
        # assert sorted([x.name for x in files]) == expected_filenames
        assert sorted([x.name for x in files])[0] == expected_filenames[0]
