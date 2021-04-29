import pytest

from dsgrid.exceptions import DSGRuntimeError
from dsgrid.utils.run_command import check_run_command


def test_check_run_command():
    good_cmd = "dsgrid registry --offline list"
    bad_cmd = "dsgrid registry invalid"
    check_run_command(good_cmd)

    with pytest.raises(DSGRuntimeError):
        check_run_command(bad_cmd)
