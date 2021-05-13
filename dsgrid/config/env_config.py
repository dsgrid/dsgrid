"""
Running List of TODO's:


Questions:
- Do diemsnion names need to be distinct from project's? What if the dataset
is using the same dimension as the project?
"""
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union, Dict
import os
import logging

# import toml

# from pydantic.fields import Field
# from pydantic.class_validators import root_validator, validator

# from dsgrid.common import LOCAL_REGISTRY_DATA
# from dsgrid.exceptions import DSGBaseException
# # from dsgrid.dimension.base import DimensionType, DSGBaseModel

# import boto3

# # from dsgrid.config._config import TimeDimensionModel, DimensionModel, ConfigRegistrationModel


# logger = logging.getLogger(__name__)


# class EnvRegistryMode(Enum):
#     LOCAL = "local"
#     S3 = "s3"


# class EnvSparkMode(Enum):
#     LOCAL = "local"
#     EMR = "emr"


# class EnvRegistryModel(DSGBaseModel):
#     """Config model for registry environment settings."""
#     mode: EnvRegistryMode = Field(
#         title="registry_mode", 
#         alias="mode", 
#         description="Registry environment mode",
#     )
#     profile: Optional[str] = Field(
#         alias="profile", 
#         description="AWS profile for dsgrid Registry",
#         default='default',
#     )

#     @validator("profile")
#     def check_profile(cls, profile):
#         """Check that the profile exists in aws credentials"""
#         available_profiles = list_profiles()
#         if profile not in available_profiles:
#             raise ValueError(
#                 f"AWS profile '{profile}' does not exist. You may need to set aws configure."
#             )
#         return profile


# class EnvSparkModel(DSGBaseModel):
#     """Config model for spark environment settings""" 
#     mode: EnvSparkMode = Field(
#         title="spark_mode", 
#         alias="mode", 
#         description="Registry spark mode",
#     )
#     profile: Optional[str] = Field(
#         alias="profile", 
#         description="AWS profile for running spark",
#         default='default',
#     )

#     # TODO: this is a duplicate validator.
#     @validator("profile")
#     def check_profile(cls, profile):
#         """Check that the profile exists in aws credentials"""
#         available_profiles = list_profiles()
#         if profile not in available_profiles:
#             raise ValueError(
#                 f"AWS profile '{profile}' does not exist. You may need to set aws configure."
#             )
#         return profile

    
#     # TODO: Remove this once EMR is supported.
#     @validator("mode")
#     def fail_if_emr_mode(cls, mode):
#         """Fail if spark mode = 'emr'
#         """
#         print(mode)
#         if mode == EnvSparkMode.EMR:
#             raise ValueError("emr mode for spark is currently not supported by dsgrid.")
#         return mode


# class EnvEMRModel(DSGBaseModel):
#     """AWS EMR environment settings."""
#     # TODO: what are these emr configs?
#     instance_type = str = Field(
#         default="TODO"
#     )
    

# class EnvConfigModel(DSGBaseModel):
#     """Base model for environment settings."""

#     registry: EnvRegistryModel = Field(
#         alias="registry-config", 
#         description="Registry environment settings",
#     )
#     spark: EnvSparkModel = Field(
#         alias="spark-config", 
#         description="Spark environment settings",
#     )
#     # emr: Optional[EnvEMRModel] = Field(
#     #     alias="emr-config",
#     #     default=None,
#     #     description="AWS EMR configurations",
#     # )

#     # @validator("emr", always=True)
#     # def check_emr(cls, emr):
#     #     print(cls)
#     #     if cls["spark"]["mode"] == EnvSparkMode.EMR.value:
#     #         if emr is None:
#     #             raise ValueError("EMR configurations are required if the the spark mode = emr.")
#     #     return emr


# class EnvConfig:
#     """Provides an interface to a EnvConfigModel."""

#     PROJECT_REPO = os.environ.get("TEST_PROJECT_REPO")
#     ENV_CONFIG_FILE = os.path.join(PROJECT_REPO, 'dsgrid_project', Path("env.toml"))
    
#     def __init__(self, model):
#         self._model = model

#     @classmethod
#     def load(cls, config_file=ENV_CONFIG_FILE):
#         model = EnvConfigModel.load(config_file)
#         return cls(model)
