import pytest

from dsgrid.utils.spark import init_spark


def pytest_sessionstart(session):
    init_spark("dsgrid-test")
