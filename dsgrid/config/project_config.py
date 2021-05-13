import itertools
import logging
from typing import Dict, List, Optional, Union

from pydantic import Field
from pydantic import root_validator, validator
from semver import VersionInfo

from .config_base import ConfigBase
from .dataset_config import InputDatasetType
from .dimension_mapping_base import DimensionMappingReferenceModel
from .dimensions import (
    DimensionReferenceModel,
    DimensionType,
)
from dsgrid.exceptions import DSGInvalidField, DSGInvalidDimensionMapping
from dsgrid.data_models import DSGBaseModel
from dsgrid.registry.common import (
    ProjectRegistryStatus,
    DatasetRegistryStatus,
    check_config_id_strict,
)

from dsgrid.utils.utilities import check_uniqueness, format_enum_for_docs
from dsgrid.utils.versioning import handle_version_or_str

# from dsgrid.dimension.time import (
#     LeapDayAdjustmentType, Period, TimeValueMeasurement, TimeFrequency,
#     TimezoneType
#     )

LOAD_DATA_FILENAME = "load_data.parquet"
LOAD_DATA_LOOKUP_FILENAME = "load_data_lookup.parquet"

logger = logging.getLogger(__name__)


class DimensionsModel(DSGBaseModel):
    """Contains dimensions defined by a project"""

    base_dimensions: List[DimensionReferenceModel] = Field(
        title="base_dimensions",
        description="List of registry references (``DimensionReferenceModel``) for a project's "
        "base dimensions (or core dimensions).",
        requirements=(
            "All base ``DimensionType`` must be defined and only one dimension reference per type "
            "is allowed.",
        ),
    )
    supplemental_dimensions: Optional[List[DimensionReferenceModel]] = Field(
        title="supplemental_dimensions",
        description="List of registry references for a project's supplemental dimensions.",
        requirements=(
            "Dimensions references of the same ``DimensionType`` are allowed for supplemental "
            "dimension refrences (i.e., multiple `Geography` types are allowed).",
        ),
        notes=(
            "Supplemental dimensions are used to support additional querying and transformations",
            "(e.g., aggregations, disgaggregations, filtering, scaling, etc.) of the project's ",
            "base data.",
        ),
        default=[],
        optional=True,
    )

    @validator("base_dimensions")
    def check_project_dimension(cls, val):
        """Validate base_dimensions types"""
        dimension_types = [i.dimension_type for i in val]
        required_dim_types = list(DimensionType)
        # validate required dimensions for base_dimensions
        for i in required_dim_types:
            if i not in dimension_types:
                raise ValueError(
                    f"Required project dimension {i} is not in project ",
                    "config project.base_dimensions",
                )
        check_uniqueness(dimension_types, "project_dimension")
        return val

    @root_validator
    def check_dimension_mappings(cls, values: dict) -> dict:
        """validates that a
        check that keys exist in both jsons
        check that all from_keys have a match in the to_keys json

        """
        # supplemental_mapping = {
        #   x.name: x.cls for x in values["supplemental_dimensions"]}
        # Should already have been checked.
        # assert len(supplemental_mapping) == \
        #   len(values["supplemental_dimensions"])
        # for dim in values["base_dimensions"]:
        #    mappings = getattr(dim, "mappings", [])
        #    # TODO: other mapping types
        #    for mapping in (
        #       x for x in mappings if isinstance(x, DimensionDirectMapping)):
        #        to_dim = supplemental_mapping.get(mapping.to_dimension)
        #        if to_dim is None:
        #            raise ValueError(
        #               f"dimension {mapping.to_dimension} is not stored in"
        #               f"supplemental_dimensions"
        #            )
        #        mapping.to_dimension = to_dim

        return values


class InputDatasetModel(DSGBaseModel):
    """Defines an input dataset for the project config."""

    dataset_id: str = Field(
        title="dataset_id",
        description="Unique dataset identifier.",
    )
    dataset_type: InputDatasetType = Field(
        title="dataset_type",
        description="Dataset type.",
        options=format_enum_for_docs(InputDatasetType),
    )
    version: Union[str, VersionInfo] = Field(
        title="version",
        description="Version of the registered dataset",
        default="1.0.0",  # TODO: convert to VersionInfo type?
        requirements=(
            "The version string must be in semver format (e.g., '1.0.0') and it must be a valid/"
            "existing version in the registry.",
        ),
        # TODO: add notes about warnings for outdated versions?
        # TODO: Maybe version needs to be Optional at first. Is
    )
    status: Optional[DatasetRegistryStatus] = Field(
        title="status",
        description="Registration status of the dataset, added by dsgrid.",
        default=DatasetRegistryStatus.UNREGISTERED,
        dsg_internal=True,
        notes=("status is "),
    )
    # TODO this model_sector must be validated in the dataset_config
    # TODO: this is only necessary for input_model types
    model_sector: str = Field(  # TODO: should this be data_source instead?
        # TODO: need to discuss with team why this is needed. One potential reason is because it is
        # helpful to query at some point which datasets have not yet been registered. Dataset_id may
        # not be all that descriptive, but the data_source is. We may also want the metric_type here too.
        title="model_sector",
        description="Model sector ID, required only if dataset type is ``sector_model``.",  # TODO: add validator
        optional=True,
    )
    # TODO: is this needed?
    # sectors: List[str] = Field(
    #    title="sectors",
    #    description="sectors used in the project",
    # )
    @validator("version")
    def check_version(cls, version):
        return handle_version_or_str(version)


class InputDatasetsModel(DSGBaseModel):
    """Defines all input datasets for a project"""

    datasets: List[InputDatasetModel] = Field(
        title="datasets",
        description="List of project input datasets",
    )

    # TODO:
    #   - Check for unique dataset IDs
    #   - check model_name, model_sector, sectors are all expected and align
    #   with the dimension records


class DimensionMappingsModel(DSGBaseModel):
    """Defines all dimension mappings associated with a dsgrid project, including base-to-base, base-to-supplemental, and dataset-to-project mappings."""

    base_to_base: Optional[List[DimensionMappingReferenceModel]] = Field(
        title="base_to_base",
        description="Base dimension to base dimension mappings (e.g., sector to subsector) that "
        "define the project dimension expectations for input datasets and allowable queries.",
        default=[],
        optional=True,
        notes=(
            "At the project-level, base-to-base mappings are optional. If no base-to-base"
            " dimension mapping is provided, dsgrid assumes a full-join of all base dimension"
            " records. For example, if a full-join is assumed for the sector dimension, then all"
            " sectors will map to all subsectors, they will also map to all geographies, all model"
            " years, and so forth.",
            # TODO: have we implemented this full join assumption?
        ),
    )
    base_to_supplemental: Optional[List[DimensionMappingReferenceModel]] = Field(
        title="base_to_supplemental",
        description="Base dimension to supplemental dimension mappings (e.g., county-to-state)"
        " used to support various queries and dimension transformations.",
        default=[],
        optional=True,
    )
    dataset_to_project: Optional[Dict[str, List[DimensionMappingReferenceModel]]] = Field(
        title="dataset_to_project",
        description="Dataset-to-project mappings map dataset dimensions to project dimensions.",
        default={},
        dsg_internal=True,
        optional=True,
        notes=(
            "Once a dataset is submitted to a project, dsgrid adds the dataset-to-project mappings"
            " to the project config",
            "Some projects may not have any dataset-to-project mappings. Dataset-to-project"
            " mappings are only supplied if a dataset's dimensions do not match the project's"
            " dimension. ",
        ),
        # TODO: need to document missing dimensoin records, fill values, etc.
    )


class ProjectConfigModel(DSGBaseModel):
    """Represents project configurations"""

    project_id: str = Field(
        title="project_id",
        description="A unique project identifier that is project-specific (e.g., "
        "'standard-scenarios-2021').",
        requirements=("must not contain any dashes (`-`)",),
        # @DT - I can also point directly to the validator, however, not all validation takes place
        #   in these pydantic validators, so maybe we just list them here in the requirements
        #   instead of pointing directly to the validators
    )
    name: str = Field(
        title="name",
        description="A project name to accompany the ID.",
        # TODO: do project names also need to be unique?
    )
    description: str = Field(
        title="description",
        description="Detailed project description.",
        notes=(
            "The description will get stored in the project registry and may be used for searching",
        ),  # TODO: is this true about all fields here?
    )
    status: Optional[ProjectRegistryStatus] = Field(
        title="status",
        description="project registry status",
        default="Initial Registration",
        dsg_internal=True,
    )
    datasets: List[InputDatasetModel] = Field(
        title="datasets",
        description="List of input datasets for the project.",
        # TODO: can include
    )
    dimensions: DimensionsModel = Field(
        title="dimensions",
        description="List of `base` and `supplemental` dimensions.",
    )
    dimension_mappings: Optional[DimensionMappingsModel] = Field(
        title="dimension_mappings",
        description="List of base-to-base and base-to-supplemental mappings.",  # TODO: technically also includes dataset-to-project mappings
        default=[],
        optional=True,
        notes=("`[dimension_mappings]` are optional at the project level.",),
    )
    registration: Optional[Dict] = Field(  # TODO: Is this still being used?
        title="registration",
        description="Registration information",
        dsg_internal=True,
    )

    @validator("project_id")
    def check_project_id_handle(cls, project_id):
        """Check for valid characters in project id"""
        # TODO: may want to check for pre-existing project_id --> are we doing this?
        #       (e.g., LA100 Run 1 vs. LA100 Run 0 kind of thing)
        if "-" in project_id:
            raise ValueError('invalid character "-" in project id')

        check_config_id_strict(project_id, "Project")
        return project_id

    # TODO: validate that datasets listed are listed by the project -- are we doing this?


class ProjectConfig(ConfigBase):
    """Provides an interface to a ProjectConfigModel."""

    def __init__(self, model):
        super().__init__(model)
        self._base_dimensions = {}
        self._supplemental_dimensions = {}

    @staticmethod
    def model_class():
        return ProjectConfigModel

    @staticmethod
    def config_filename():
        return "project.toml"

    @classmethod
    def load(cls, config_file, dimension_manager):
        config = cls._load(config_file)
        config.load_dimensions(dimension_manager)
        return config

    def load_dimensions(self, dimension_manager):
        """Load all Base Dimensions.

        Parameters
        ----------
        dimension_manager : DimensionRegistryManager

        """
        base_dimensions = dimension_manager.load_dimensions(self.model.dimensions.base_dimensions)
        supplemental_dimensions = dimension_manager.load_dimensions(
            self.model.dimensions.supplemental_dimensions
        )
        dims = itertools.chain(base_dimensions.values(), supplemental_dimensions.values())
        check_uniqueness((x.model.name for x in dims), "dimension name")
        check_uniqueness((getattr(x, "cls") for x in dims), "dimension cls")

        self._base_dimensions.update(base_dimensions)
        self._supplemental_dimensions.update(supplemental_dimensions)

    def add_dataset_dimension_mappings(self, dataset_config, references):
        """Add a dataset's dimension mappings to the project.

        Parameters
        ----------
        dataset_config : DatasetConfig
        references : list
            list of DimensionMappingReferenceModel

        Raises
        ------
        DSGInvalidDimensionMapping
            Raised if a requirement is violated.

        """
        self.check_dataset_dimension_mappings(dataset_config, references)
        if dataset_config.model.dataset_id not in self.model.dimension_mappings.dataset_to_project:
            self.model.dimension_mappings.dataset_to_project[dataset_config.model.dataset_id] = []
        mappings = self.model.dimension_mappings.dataset_to_project[
            dataset_config.model.dataset_id
        ]
        existing_ids = set((x.mapping_id for x in mappings))
        for reference in references:
            if reference.mapping_id not in existing_ids:
                mappings.append(reference)
                logger.info(
                    "Added dimension mapping for dataset=%s: %s",
                    dataset_config.model.dataset_id,
                    reference.mapping_id,
                )

    def check_dataset_dimension_mappings(
        self, dataset_config, references: DimensionMappingReferenceModel
    ):
        """Check that a dataset provides required mappings to the project.

        Parameters
        ----------
        dataset_config : DatasetConfig
        references : list
            list of DimensionMappingReferenceModel

        Raises
        ------
        DSGInvalidDimensionMapping
            Raised if a requirement is violated.

        """
        # The dataset has to have each project dimension or provide a mapping.
        project_keys = set(self.base_dimensions.keys())
        dataset_keys = set(dataset_config.dimensions)
        requires_mapping = project_keys.difference(dataset_keys)
        for dimension in requires_mapping:
            if dimension.type.value == "time":
                pass  # TODO: need to programmatically generate a time dimension mapping
            else:
                if dimension.type not in [i.to_dimension_type for i in references]:
                    raise DSGInvalidDimensionMapping(
                        f"dataset {dataset_config.model.dataset_id} has missing dimension mappings: {dimension}"
                    )

                # TODO: check that all expected dimension IDs are present
                # project_dimension_ids = [i["id"] for i in self.base_dimensions[dimension].records]
                # dataset_dimension_id = [i["id"] for i in dataset_config.dimensions[dimension].records] # TODO: how do I get dimension mapping records?
                # TODO: if there are proejct dimension IDs that are missing from the dataset, log a warning
                # TODO: consider throwing an error if a dataset dimension_id maps to too many project dimension ids (unless aggregation is specified)

    @property
    def config_id(self):
        return self._model.project_id

    def get_dataset(self, dataset_id):
        """Return a dataset by ID."""
        for dataset in self.model.input_datasets.datasets:
            if dataset.dataset_id == dataset_id:
                return dataset

        raise DSGInvalidField(
            f"project_id={self._model.project_id} does not have dataset_id={dataset_id}"
        )

    def has_dataset(self, dataset_id, status=None):
        """Return True if the dataset_id is present in the configuration.

        Parameters
        ----------
        dataset_id : str
        status : None | DatasetRegistryStatus
            If set, only return True if the status matches.
        """
        for dataset in self.iter_datasets():
            if dataset.dataset_id == dataset_id:
                if status is None or dataset.status == status:
                    return True
                return False

        # TODO DT: what about benchmark and historical?
        return False

    def iter_datasets(self):
        for dataset in self.model.input_datasets.datasets:
            yield dataset

    def iter_dataset_ids(self):
        for dataset in self.model.input_datasets.datasets:
            yield dataset.dataset_id

    def list_registered_dataset_ids(self):
        """List registered datasets associated with the project.

        Returns
        -------
        list
            list of dataset IDs

        """
        status = DatasetRegistryStatus.REGISTERED
        return [x.dataset_id for x in self._iter_datasets_by_status(status)]

    def list_unregistered_dataset_ids(self):
        """Get unregistered datasets associated with project registry.

        Returns
        -------
        list
            list of dataset IDs

        """
        status = DatasetRegistryStatus.UNREGISTERED
        return [x.dataset_id for x in self._iter_datasets_by_status(status)]

    def _iter_datasets_by_status(self, status: DatasetRegistryStatus) -> InputDatasetModel:
        for dataset in self.iter_datasets():
            if dataset.status == status:
                yield dataset

    def set_status(self, status):
        """Set the project status to the given value.

        Parameters
        ----------
        status : ProjectRegistryStatus

        """
        self.model.status = status
        logger.info("Set project_id=%s status=%s", self.config_id, status)

    def set_dataset_status(self, dataset_id, status):
        """Set the dataset status to the given value.

        Parameters
        ----------
        dataset_id : str
        status : DatasetRegistryStatus

        Raises
        ------
        ValueError
            Raised if dataset_id is not stored.

        """
        dataset = self.get_dataset(dataset_id)
        dataset.status = status
        logger.info(
            "Set dataset_id=%s status=%s for project_id=%s",
            dataset_id,
            status,
            self._model.project_id,
        )

    @property
    def base_dimensions(self):
        """Return the Base Dimensions.

        Returns
        -------
        dict
            dict of DimensionBaseModel keyed by DimensionKey

        """
        return self._base_dimensions

    @property
    def supplemental_dimensions(self):
        """Return the supplemental dimensions.

        Returns
        -------
        dict
            dict of DimensionBaseModel keyed by DimensionKey

        """
        return self._supplemental_dimensions
