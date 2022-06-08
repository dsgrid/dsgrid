import pytest

from dsgrid.exceptions import DSGRuntimeError
from dsgrid.utils.run_command import check_run_command


def test_check_run_command():
    good_cmd = "dsgrid registry --help"
    bad_cmd = "dsgrid invalid"
    check_run_command(good_cmd)

    with pytest.raises(DSGRuntimeError):
        check_run_command(bad_cmd)
