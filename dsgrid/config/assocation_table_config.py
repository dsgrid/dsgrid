from typing import List, Optional, Union, Dict
import logging

from pydantic import Field
from pydantic import validator

from dsgrid.config.assocation_tables import AssociationTableModel
from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.utilities import check_uniqueness


logger = logging.getLogger(__name__)

"""
VALIDATE:
1. 8 unique types of dimensions (all except time) but each type can have more than one entry.
2. dimension `ids' and records are all unique.
"""
