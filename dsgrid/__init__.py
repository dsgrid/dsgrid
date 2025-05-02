import warnings

from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.utils.timing import timer_stats_collector  # noqa: F401

__version__ = "0.1.0"
__copyright__ = "Copyright 2023, The Alliance for Sustainable Energy, LLC"
__author__ = "Elaine Hale"

warnings.filterwarnings("ignore", module="duckdb_engine")

runtime_config = DsgridRuntimeConfig.load()
