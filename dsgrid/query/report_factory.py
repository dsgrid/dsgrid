"""Constructs a report class from a type."""

from dsgrid.query.models import ReportType
from dsgrid.query.report_peak_load import PeakLoadReport
from dsgrid.query.reports_base import ReportsBase


_TYPE_TO_CLASS = {
    ReportType.PEAK_LOAD: PeakLoadReport,
}


def make_report(report_type: ReportType) -> ReportsBase:
    """Make a report class from a report_type."""
    cls = _TYPE_TO_CLASS.get(report_type)
    if cls is None:
        msg = str(report_type)
        raise NotImplementedError(msg)
    return cls()
