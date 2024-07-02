import csv
import logging
import os
from typing import List, Optional, Union

from pydantic import field_validator, Field, ValidationInfo, field_serializer
from typing_extensions import Annotated

from dsgrid.config.common import compute_hash_from_model_records
from dsgrid.config.dimension_mapping_base import (
    DimensionMappingBaseModel,
    DimensionMappingDatasetToProjectBaseModel,
    DimensionMappingPreRegisteredBaseModel,
)
from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.utilities import convert_record_dicts_to_classes
from .config_base import ConfigWithRecordFileBase


logger = logging.getLogger(__name__)


class MappingTableRecordModel(DSGBaseModel):
    """Represents one record in dimension mapping record files. Maps one dimension to another."""

    from_id: Annotated[
        str,
        Field(
            title="from_id",
            description="Source mapping",
        ),
    ]
    to_id: Annotated[
        Union[str, None],
        Field(
            None,
            title="to_id",
            description="Destination mapping",
        ),
    ]
    from_fraction: Annotated[
        float,
        Field(
            title="from_fraction",
            description="Fraction of from_id to map to to_id",
            default=1.0,
        ),
    ]

    @field_validator("from_id", "to_id")
    @classmethod
    def check_to_id(cls, val):
        if val == "":
            return None
        return val


class MappingTableByNameModel(DimensionMappingPreRegisteredBaseModel):
    """Attributes for a dimension mapping table for soon-to-be registered dimensions by name.
    This will be converted to a MappingTableModel as soon as the dimensions are registered.
    """

    filename: Annotated[
        Optional[str],
        Field(
            default=None,
            title="filename",
            alias="file",
            description="Filename containing association table records.",
        ),
    ]
    records: Annotated[
        List,
        Field(
            title="records",
            description="Dimension mapping records in filename that get loaded at runtime",
            default=[],
        ),
    ]


class DatasetBaseToProjectMappingTableModel(DimensionMappingDatasetToProjectBaseModel):
    """Attributes for a dimension mapping table to map soon-to-be-registered dataset base
    dimensions to a project's dimensions. This will be converted to a MappingTableModel as soon as
    the dimensions are registered.
    """

    filename: Annotated[
        str,
        Field(
            title="filename",
            alias="file",
            description="Filename containing association table records.",
        ),
    ]
    records: Annotated[
        List,
        Field(
            title="records",
            description="Dimension mapping records in filename that get loaded at runtime",
            default=[],
        ),
    ]


class DatasetBaseToProjectMappingTableListModel(DSGBaseModel):
    """Represents the config file passed to register-and-submit-dataset command."""

    mappings: List[DatasetBaseToProjectMappingTableModel]


class MappingTableModel(DimensionMappingBaseModel):
    """Attributes for a dimension mapping table"""

    filename: Annotated[
        Optional[str],
        Field(
            title="filename",
            alias="file",
            default=None,
            description="Filename containing association table records. Only assigned for user input "
            "and output purposes. The registry database stores records in the mapping JSON document.",
        ),
    ]
    records: Annotated[
        List,
        Field(
            title="records",
            description="dimension mapping records in filename that get loaded at runtime",
            json_schema_extra={
                "dsgrid_internal": True,
            },
            default=[],
        ),
    ]
    file_hash: Annotated[
        Optional[str],
        Field(
            title="file_hash",
            description="Hash of the contents of the file, computed by dsgrid.",
            json_schema_extra={
                "dsgrid_internal": True,
            },
            default=None,
        ),
    ]

    @field_validator("filename")
    @classmethod
    def check_filename(cls, filename):
        """Validate record file"""
        if filename is not None:
            if not os.path.isfile(filename):
                raise ValueError(f"{filename} does not exist")
            if not filename.endswith(".csv"):
                raise ValueError(f"only CSV is supported: {filename}")
        return filename

    @field_validator("records")
    @classmethod
    def add_records(cls, records, info: ValidationInfo):
        """Add records from the file."""
        if records:
            if isinstance(records[0], dict):
                records = convert_record_dicts_to_classes(records, MappingTableRecordModel)
            return records

        if "filename" not in info.data:
            msg = "Bug: filename can't be None if records is empty."
            raise ValueError(msg)

        with open(info.data["filename"], encoding="utf8") as f_in:
            return convert_record_dicts_to_classes(csv.DictReader(f_in), MappingTableRecordModel)

    @field_validator("file_hash")
    @classmethod
    def compute_file_hash(cls, file_hash, info: ValidationInfo):
        """Compute file hash."""
        return compute_hash_from_model_records(file_hash, info)

    @field_serializer("filename")
    def serialize_cls(self, val, _):
        return None

    @classmethod
    def from_pre_registered_model(
        cls, model: DimensionMappingPreRegisteredBaseModel, from_dimension, to_dimension
    ):
        return MappingTableModel(
            mapping_type=model.mapping_type,
            archetype=model.archetype,
            from_dimension=from_dimension,
            to_dimension=to_dimension,
            description=model.description,
            filename=model.filename,
            records=model.records,
            from_fraction_tolerance=model.from_fraction_tolerance,
            to_fraction_tolerance=model.to_fraction_tolerance,
        )


class MappingTableConfig(ConfigWithRecordFileBase):
    """Provides an interface to an MappingTableModel"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dataframe = None

    @staticmethod
    def config_filename():
        return "dimension_mapping.json5"

    @property
    def config_id(self):
        return self.model.mapping_id

    @staticmethod
    def model_class():
        return MappingTableModel

    def get_unique_from_ids(self):
        """Return the unique from IDs in an association table's records.

        Returns
        -------
        set
            set of str

        """
        return {x.from_id for x in self.model.records}

    def get_unique_to_ids(self):
        """Return the unique to IDs in an association table's records.

        Returns
        -------
        set
            set of str

        """
        return {x.to_id for x in self.model.records}
