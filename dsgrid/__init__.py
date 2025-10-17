import datetime as dt
import warnings

from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.utils.timing import timer_stats_collector  # noqa: F401

__title__ = "dsgrid"
__description__ = (
    "Python API for registring and accessing demand-side grid model (dsgrid) datasets"
)
__url__ = "https://github.com/dsgrid/dsgrid"
__version__ = "0.2.0"
__author__ = "NREL"
__maintainer_email__ = "elaine.hale@nrel.gov"
__license__ = "BSD-3"
__copyright__ = "Copyright {}, The Alliance for Sustainable Energy, LLC".format(
    dt.date.today().year
)

warnings.filterwarnings("ignore", module="duckdb_engine")

runtime_config = DsgridRuntimeConfig.load()
