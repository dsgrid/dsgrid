from dsgrid.dimension.time_utils import is_leap_year


def test_is_leap_year():
    assert is_leap_year(2020)
    assert not is_leap_year(2021)
    assert not is_leap_year(2100)
    assert is_leap_year(2400)
