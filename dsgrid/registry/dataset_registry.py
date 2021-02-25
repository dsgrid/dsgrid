import os
from enum import Enum
from typing import List, Optional
import toml

from pydantic.fields import Field, Required
from pydantic.class_validators import root_validator, validator

from dsgrid.dimension.base import DSGBaseModel

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config._config import ConfigRegistrationDetails

from dsgrid.registry.versioning import versioning, DATASET_REGISTRY_PATH

# TODO: I think we need to also register the pydantic models associated with
#   the configuration OR we need to set the dataset_config type to
#   DatasetCOnfig instead of dict of the loaded toml (not the cls.dict()).
#   Due to parsing issues we are currently setting type as dict. However,
#   pydantic may allow for some orm_mode or parse_json encoder settings that
#   we could use instead. Ideally, I would like each registry to be just a
#   single .toml file instead of a .toml + a bunch of other files.
#   @dthom - please help!**

# TODO: consider checking file paths and update status to apply versioning #


class DatasetConfigRegistry(DatasetConfig):
    """When registering a dataset, require that registration details be in
    the dataset configuraiton."""
    # require registration context when registerying
    registration: ConfigRegistrationDetails = Field(
        title="registration",
        description="registration details"
    )


class DatasetRegistry(DSGBaseModel):
    """Dataset registration class"""
    dataset_config: dict = Field(
        title='dataset_config',
        description="dataset configuration",
    )
    # TODO: type should be DataVersion or something like that; TBD
    dataset_version: str = Field(
        title='dataset_version',
        description="full dataset version (dataset id + version)",
        default="",
    )

    def register(cls):
        """Create Dataset Registration TOML file."""
        # TODO: ATM this is just a local registration; need a central
        #       cloud-version next

        # remove registration settings when registering
        #   TODO: a way to exclude fields on export would be ideal.
        #       Open pydanic PR suggests it will be implemented soon.
        #       https://github.com/samuelcolvin/pydantic/pull/2231
        #       exclude registration details from being export
        cls_dict = cls.dict()
        del cls_dict['dataset_config']['registration']

        registry_file = f'{DATASET_REGISTRY_PATH}/{cls.dataset_version}.toml'
        with open(registry_file, 'w') as j:
            toml.dump(cls_dict, j)


def register_dataset(config_toml):
    """
    Register a dataset with a registered dsgrid project
     given datast configuration toml.
    """
    config_dict = toml.load(config_toml)
    
    # validate dataset config for registration
    DatasetConfigRegistry(**config_dict)

    # set dataset version
    dataset_version = versioning(
        registry_type='dataset',
        id_handle=config_dict['dataset_id'],
        update=config_dict['registration']['update'])

    # register dataset
    DatasetRegistry(
        dataset_config=config_dict, dataset_version=dataset_version).register()

    print('Done Registering Dataset')
