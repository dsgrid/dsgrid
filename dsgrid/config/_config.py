"""
Shared configuration models and enums shared between ProjectConfig and
DatasetConfig.
"""

import os
from typing import Optional, Union

from pydantic.dataclasses import dataclass
from pydantic import Field
from pydantic import validator

from dsgrid.data_models import DSGBaseModel


@dataclass
class DimensionMap:
    # TODO: this needs QAQC checks
    from_dimension: str
    to_dimension: str
    from_key: str
    to_key: str


class DimensionDirectMapping(DSGBaseModel):
    field: str = Field(
        title="field",
        description="Field in from_dimension containing foreign_key",
    )
    to_dimension: Union[str, type] = Field(
        title="to_dimension",
        description="Target Dimension for mapping, initially a str",
    )
    foreign_key: str = Field(
        title="foreign_key",
        description="Key in to_dimension",
    )


class MappingBaseModel(DSGBaseModel):
    from_dimension: str = Field(
        title="from_dimension",
        description="ORM class name that defines the from dimension",
    )
    to_dimension: str = Field(
        title="to_dimension",
        description="ORM class name that defines the to dimension",
    )
    from_dimension_cls: Optional[type] = Field(
        title="from_dimension_cls",
        description="ORM class that defines the from dimension",
    )
    to_dimension_cls: Optional[type] = Field(
        title="to_dimension_cls",
        description="ORM class that defines the to dimension",
    )


class OneToManyMapping(MappingBaseModel):
    """Defines mapping of one to many."""


class ManyToOneMapping(MappingBaseModel):
    """Defines mapping of many to one."""


class ManyToManyMapping(MappingBaseModel):
    """Defines mapping of many to many."""

    filename: str = Field(
        title="file",
        alias="file",
        description="File that defines the associations",
    )

    @validator("filename")
    def check_file(cls, filename):
        """Check that association file exists."""
        if not os.path.isfile(filename):
            raise ValueError(f"file {filename} does not exist")

        return filename
