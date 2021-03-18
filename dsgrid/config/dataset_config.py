import datetime
from typing import List
from pydantic.dataclasses import dataclass
import logging
import os

from dsgrid.config.base_config import BaseConfig
from dsgrid.config.project_config import ProjectConfig
from dsgrid.exceptions import ConfigError

logger = logging.getLogger(__name__)
PROJECTDIR = os.path.dirname(os.path.dirname(
    os.path.realpath(__file__)))  # TODO: check proper usage


class DatasetConfig(BaseConfig):
    """Dataset configuration class
    """

    # TODO: if dataset is not sector model, we may not require all these dims
    PROJECT_REQUIREMENTS = ('id', 'model_name', 'model_sector', 'sectors')
    REQUIRED_DIMENSIONS = (
        'geography', 'sector', 'subsector', 'enduse', 'time')

    def __init__(self, config):
        super().__init__(config)

        # get project config
        self._project_config_toml = self.init_project_config_toml()

        self._dataset_id = self.get("dataset_id")
        self._dataset_type = self.get("dataset_type")
        self._model_name = self.get("model_name")
        self._model_sector = self.get("model_sector")
        self._sectors = self.get("sectors")
        self._version = self.get("version")
        self._dimensions = self.init_dimensions()
        self._metadata = self.get("metadata")

        # run preflight checks
        self.dataset_preflight_checks()

    def init_project_config_toml(self):
        """Init the project config toml file from the dataset's config_dir.

        Assumes relative path of /dsgrid_project/project/project.toml
        """
        relpath = "dsgrid_project/project/project.toml"
        return self.config_dir.split("dsgrid_project")[0]+relpath

    @property
    # TODO: make the ID from the version, filename, sector_model, dataset_type;
    #       set this at the baseconfig level?
    def dataset_id(self):
        self._dataset_id = self.get("dataset_id")
        return self._dataset_id

    @property
    def dataset_type(self):
        self._dataset_type = self.get("dataset_type")
        return self._dataset_type

    @property
    def model_name(self):
        self._model_name = self.get("model_name")
        return self._model_name

    @property
    def model_sector(self):
        self._model_sector = self.get("model_sector")
        return self._model_sector

    @property
    def sectors(self):
        self._sectors = self.get("sectors")
        return self._sectors

    @property
    def version(self):
        # TODO: enforce versioning standards
        self._version = self.get("version")
        return self._version

    @property
    def dimensions(self):
        """
        Get the dataset dimensions and return as a dictionary of datatset
        dimension data classes.
        """
        return self._dimensions

    @property
    def metadata(self):
        """
        Get dataset metadata and return as user-defined dict.
        """
        self._metadata = self.get("metadata")
        return self._metadata

    def init_dimensions(self):
        project_config_dim = ProjectConfig(
            self._project_config_toml)['dimensions']['project']
        dimension = {}
        for dim, dim_dict in self.get("dimensions").items():
            # init all non-time dimensions
            if dim != 'time':
                # check that dataset dimension name != project's
                if dim_dict['use_project_dimension'] is False:
                    if dim_dict['name'] == project_config_dim[dim]['name']:
                        raise ConfigError(
                            f"dataset dimension name {dim_dict['name']}",
                            "cannot be the same as project dimension name",
                            f"{project_config_dim[dim]['name']}")
                # if use_project_dimension, set dim name and file to project's
                if dim_dict['use_project_dimension']:
                    dim_dict['name'] = project_config_dim[dim]['name']
                    dim_dict['file'] = project_config_dim[dim]['file']
                    dim_dict['project_dimension_mapping'] = {
                        'from_key': "id",
                        'to_key': "id"
                        }
                # add dimension names to mapping
                dim_dict['project_dimension_mapping'] = {
                    'From': dim_dict['name'],
                    'From_key': dim_dict['project_dimension_mapping']
                                    ['from_key'],
                    'To': project_config_dim[dim]['name'],
                    'To_key': dim_dict['project_dimension_mapping']['to_key']
                    }
                # set dimclass to dataclass
                dimclass = DatasetGeneralDimensionConfig(**dim_dict)

            # init time dimension
            else:
                dimclass = TimeDimensionConfig(**dim_dict)
            dimension[dim] = dimclass
        return dimension

    def dataset_preflight_checks(self):
        """Run dataset-specific preflight checks on Dataset Config"""

        PROJECT_CONFIG = ProjectConfig(self._project_config_toml)

        # check that dataset_id is expected by the project
        for i in PROJECT_CONFIG.input_datasets[self.dataset_type]:
            if i["id"] == self.dataset_id:
                project_dataset_id = i
        if project_dataset_id is None:
            raise ConfigError(
                f"Dataset_id {self.dataset_id} is not defined in Project ",
                "dataset config")

        # check that dataset has all other project requirements
        for i in PROJECT_CONFIG.input_datasets[self.dataset_type]:
            if i["id"] == self.dataset_id:
                for req in self.PROJECT_REQUIREMENTS:
                    if req != 'id':
                        if i[req] != self[req]:
                            raise ConfigError(
                                f"Dataset {req} config is {i[req]} but ",
                                f"project expects it to be {self[req]}")

        # check all required dimensions exist
        for required_dimension in self.REQUIRED_DIMENSIONS:
            if required_dimension not in self.get("dimensions").keys():
                raise ConfigError(
                    f"Required dimension {required_dimension} not in Dataset ",
                    "dimension config")

        # TODO: check filter clauses


# TODO: dthom to figure out how to enforce data types (including special types)
#       with python standard lib instead of pydantic
@dataclass
class FromToDimensionMappingConfig:
    """A dataclass to handle the from dataset to project dimension mapping
       configuration.
    """
    From: str  # TODO: lowercase from not valid
    From_key: str
    To: str
    To_key: str


@dataclass
class DatasetGeneralDimensionConfig:
    """A datacass to handle the Dataset configurations for general dimensions.
    """
    use_project_dimension: bool = True
    name: str = ""
    filter_project_dimension: bool = False
    filter_where_clause: str = ""
    file: str = ""
    project_dimension_mapping: FromToDimensionMappingConfig = \
        FromToDimensionMappingConfig(
            From="", To="", From_key="id", To_key="id")

    def __post_init__(self):
        # file str replacement
        if self.file is not None:
            self.file = self.file.replace('PROJECTDIR', PROJECTDIR)

        self._run_checks()

    def _run_checks(self):
        # check config specs against logic
        if self.use_project_dimension is False:
            if self.file == "":
                raise ConfigError(
                    "file must be specified if use_project_dimension is False")
            if self.name == "":
                raise ConfigError(
                    "must name dataset dimension if use_project_dimension is",
                    "False")
        if self.filter_project_dimension is True:
            if self.filter_where_clause == "":
                raise ConfigError(
                    "filter_where_clause must be specified if ",
                    "filter_project_dimension is True")

        # check that file exists
        if not os.path.exists(self.file):
            raise ConfigError(f"Dataset {self.file} does not exist")


# TODO: where does this go? in a seperate file?
@dataclass
class TimeDimensionConfig:
    """A datacass to handle the Dataset configurations for time dimensions."""
    start: datetime.datetime
    end: datetime.datetime
    frequency_unit: str
    frequency_number: int
    includes_dst: bool
    includes_leap_day: bool
    interval_unit: str
    interval_number: int
    leap_day_adjustment: str
    model_years: List[int]
    period: str
    str_format: str
    timezone: str
    value_representation: str

    def __post_init__(self):
        # check leap day configs
        if self.includes_leap_day is False:
            if self.leap_day_adjustment == "":
                raise ConfigError(
                    "leap_day_adjustment should be defined if ",
                    "includes_leap_day is False")

        # check config settings against available options
        run_check_dict = {
            self.period: TimeConfigOptions.PERIODS,
            self.timezone: TimeConfigOptions.TIMEZONES,
            self.interval_unit: TimeConfigOptions.INTERVAL_UNITS,
            self.frequency_unit: TimeConfigOptions.FREQUENCY_UNITS,
            self.leap_day_adjustment: TimeConfigOptions.LEAP_DAY_ADJUSTMENTS,
            self.value_representation: TimeConfigOptions.VALUE_REPRESENTATION
            }
        self._run_checks(run_check_dict)

        # TODO: Check that there is no tz of datetime or that tz == timezone
        # TODO check model year is appropriate year
        # TODO check str_format

    def _run_checks(self, run_check_dict):
        for key, value in run_check_dict.items():
            # TODO: make this config msg clearer by specifying the attribute
            if key not in value:
                raise ConfigError(
                    f"config {key} is not in possible values of{value}")


# TODO: where does this go? in a new file?
class TimeConfigOptions():
    FREQUENCY_UNITS = ['min', 'hour', 'year']
    INTERVAL_UNITS = ['year']
    LEAP_DAY_ADJUSTMENTS = ["", "drop_dec31", "drop_feb29", "drop_jan1"]
    PERIODS = ['period-ending', 'period-beginning', 'instantaneous']
    TIMEZONES = ['EST', 'UTC']  # TODO: this should come from some master class
    VALUE_REPRESENTATION = ['mean', 'min', 'max', 'measured']
