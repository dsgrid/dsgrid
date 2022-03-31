import getpass
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from dsgrid.exceptions import DSGInvalidDimensionAssociation
from dsgrid.tests.common import make_test_project_dir
from dsgrid.tests.make_us_data_registry import make_test_data_registry


def test_invalid_projects(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        manager = make_test_data_registry(
            base_dir,
            make_test_project_dir,
            include_projects=False,
            include_datasets=False,
        )

        user = getpass.getuser()
        log_message = "test log message"
        register_tests = (_setup_invalid_dimension_associations,)

        # This is arranged in this way to avoid having to re-create the registry every time,
        # which is quite slow. There is one downside: if one test is able to register the
        # project (which would be a bug), later tests will fail even if they should pass.
        for i, setup_test in enumerate(register_tests):
            test_dir = base_dir / f"test_data_dir_{i}"
            try:
                shutil.copytree(make_test_project_dir, test_dir)
                project_config_file = test_dir / "project.toml"
                exc, match_msg = setup_test(test_dir)
                with pytest.raises(exc, match=match_msg):
                    manager.project_manager.register_from_file(
                        project_config_file, user, log_message
                    )
            finally:
                if test_dir.exists():
                    shutil.rmtree(test_dir)


def _setup_invalid_dimension_associations(project_dir):
    association_file = project_dir / "dimension_associations" / "sector__subsector.csv"
    with open(association_file, "a") as f_out:
        f_out.write("invalid,invalid\n")
    return DSGInvalidDimensionAssociation, r"Dropped records when joining by data_source"
