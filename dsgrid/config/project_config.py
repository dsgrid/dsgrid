import itertools
import logging
import os
from typing import Dict, List, Set

from pydantic import Field
from pydantic import root_validator, validator

from dsgrid.data_models import DSGBaseModel, serialize_model_data
from dsgrid.dimension.base_models import check_required_dimensions
from dsgrid.exceptions import DSGInvalidField, DSGInvalidDimension, DSGInvalidParameter
from dsgrid.registry.common import (
    ProjectRegistryStatus,
    DatasetRegistryStatus,
    check_config_id_strict,
)
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.registry.dimension_mapping_registry_manager import DimensionMappingRegistryManager
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.utilities import check_uniqueness
from dsgrid.utils.versioning import handle_version_or_str
from .config_base import ConfigWithDataFilesBase
from .dataset_config import InputDatasetType
from .dimension_associations import DimensionAssociations
from .dimension_mapping_base import DimensionMappingReferenceModel
from .mapping_tables import MappingTableByNameModel
from .dimensions import (
    DimensionReferenceModel,
    DimensionType,
    handle_dimension_union,
    DimensionModel,
)


logger = logging.getLogger(__name__)


class DimensionsModel(DSGBaseModel):
    """Contains dimensions defined by a project"""

    base_dimensions: List = Field(
        title="base_dimensions",
        description="List of dimensions for a project's base dimensions. They will be "
        "automatically registered during project registration and then converted to "
        "base_dimension_references.",
        requirements=(
            "All base :class:`dsgrid.dimensions.base_model.DimensionType` must be defined and only"
            " one dimension reference per type is allowed.",
        ),
        default=[],
    )
    base_dimension_references: List[DimensionReferenceModel] = Field(
        title="base_dimensions",
        description="List of registry references (``DimensionReferenceModel``) for a project's "
        "base dimensions.",
        requirements=(
            "All base :class:`dsgrid.dimensions.base_model.DimensionType` must be defined and only"
            " one dimension reference per type is allowed.",
        ),
        default=[],
    )
    supplemental_dimensions: List = Field(
        title="supplemental_dimensions",
        description="List of supplemental dimensions. They will be automatically registered "
        "during project registration and then converted to supplemental_dimension_references.",
        notes=(
            "Supplemental dimensions are used to support additional querying and transformations",
            "(e.g., aggregations, disgaggregations, filtering, scaling, etc.) of the project's ",
            "base data.",
        ),
        default=[],
    )
    supplemental_dimension_references: List[DimensionReferenceModel] = Field(
        title="supplemental_dimensions",
        description="List of registry references for a project's supplemental dimensions.",
        requirements=(
            "Dimensions references of the same :class:`dsgrid.dimensions.base_model.DimensionType`"
            " are allowed for supplemental dimension references (i.e., multiple `Geography` types"
            " are allowed).",
        ),
        notes=(
            "Supplemental dimensions are used to support additional querying and transformations",
            "(e.g., aggregations, disgaggregations, filtering, scaling, etc.) of the project's ",
            "base data.",
        ),
        default=[],
    )
    all_in_one_supplemental_dimensions: List[DimensionType] = Field(
        title="all_in_one_supplemental_dimensions",
        description="dsgrid will auto-generate dimensions that aggregate across all records for "
        "the base dimensions of these types.",
        default=[],
    )

    @root_validator(pre=False)
    def check_dimensions(cls, values):
        """Validate that the dimensions are complete and consistent."""
        dimensions = list(
            itertools.chain(
                values.get("base_dimensions", []), values.get("base_dimension_references", [])
            )
        )
        check_required_dimensions(dimensions, "project base dimensions")

        return values

    @root_validator(pre=True)
    def pre_check_values(cls, values: dict) -> dict:
        """Checks that base dimensions are defined."""
        if not values.get("base_dimensions", []) and not values.get(
            "base_dimension_references", []
        ):
            raise ValueError("Either base_dimensions or base_dimension_references must be defined")

        return values

    @validator("base_dimensions")
    def check_files(cls, values: dict) -> dict:
        """Validate dimension files are unique across all dimensions"""
        check_uniqueness(
            (x.filename for x in values if isinstance(x, DimensionModel)),
            "dimension record filename",
        )
        return values

    @validator("base_dimensions")
    def check_names(cls, values: dict) -> dict:
        """Validate dimension names are unique across all dimensions."""
        check_uniqueness(
            [dim.name for dim in values],
            "dimension record name",
        )
        return values

    @validator("base_dimensions", "supplemental_dimensions", pre=True, each_item=True, always=True)
    def handle_dimension_union(cls, values):
        return handle_dimension_union(values)


class InputDatasetModel(DSGBaseModel):
    """Defines an input dataset for the project config."""

    dataset_id: str = Field(
        title="dataset_id",
        description="Unique dataset identifier.",
        updateable=False,
    )
    dataset_type: InputDatasetType = Field(
        title="dataset_type",
        description="Dataset type.",
        options=InputDatasetType.format_for_docs(),
        updateable=False,
    )
    version: str = Field(
        title="version",
        description="Version of the registered dataset",
        default=None,
        requirements=(
            # TODO: add notes about warnings for outdated versions DSGRID-189 & DSGRID-148
            # TODO: need to assume the latest version. DSGRID-190
            "The version specification is optional. If no version is supplied, then the latest"
            " version in the registry is assumed.",
            "The version string must be in semver format (e.g., '1.0.0') and it must be a valid/"
            "existing version in the registry.",
        ),
        updateable=False,
        # TODO: add notes about warnings for outdated versions? DSGRID-189.
    )
    mapping_references: List[DimensionMappingReferenceModel] = Field(
        title="mapping_references",
        description="Defines how to map the dataset dimensions to the project.",
        default=[],
    )
    status: DatasetRegistryStatus = Field(
        title="status",
        description="Registration status of the dataset, added by dsgrid.",
        default=DatasetRegistryStatus.UNREGISTERED,
        dsg_internal=True,
        notes=("status is "),
        updateable=False,
    )

    @validator("version")
    def check_version(cls, version):
        return handle_version_or_str(version)


class DimensionMappingsModel(DSGBaseModel):
    """Defines all dimension mappings associated with a dsgrid project,
    including dimension associations, base-to-supplemental mappings, and dataset-to-project mappings."""

    # This may eventually need to be a Union of types.
    base_to_supplemental: List[MappingTableByNameModel] = Field(
        title="base_to_supplemental",
        description="Base dimension to supplemental dimension mappings (e.g., county-to-state)"
        " used to support various queries and dimension transformations. They will be "
        "automatically registered during project registration and then converted to "
        "base_to_supplemental_references.",
        default=[],
    )
    base_to_supplemental_references: List[DimensionMappingReferenceModel] = Field(
        title="base_to_supplemental_references",
        description="Base dimension to supplemental dimension mappings (e.g., county-to-state)"
        " used to support various queries and dimension transformations.",
        default=[],
    )
    dataset_to_project: Dict[str, List[DimensionMappingReferenceModel]] = Field(
        title="dataset_to_project",
        description="Dataset-to-project mappings map dataset dimensions to project dimensions.",
        default={},
        dsg_internal=True,
        notes=(
            "Once a dataset is submitted to a project, dsgrid adds the dataset-to-project mappings"
            " to the project config",
            "Some projects may not have any dataset-to-project mappings. Dataset-to-project"
            " mappings are only supplied if a dataset's dimensions do not match the project's"
            " dimension. ",
        ),
        updateable=False,
        # TODO: need to document missing dimension records, fill values, etc. DSGRID-191.
    )


class ProjectConfigModel(DSGBaseModel):
    """Represents project configurations"""

    project_id: str = Field(
        title="project_id",
        description="A unique project identifier that is project-specific (e.g., "
        "'standard-scenarios-2021').",
        requirements=("Must not contain any dashes (`-`)",),
        updateable=False,
    )
    name: str = Field(
        title="name",
        description="A project name to accompany the ID.",
    )
    description: str = Field(
        title="description",
        description="Detailed project description.",
        notes=(
            "The description will get stored in the project registry and may be used for"
            " searching",
        ),
    )
    status: ProjectRegistryStatus = Field(
        title="status",
        description="project registry status",
        default=ProjectRegistryStatus.INITIAL_REGISTRATION,
        dsg_internal=True,
        updateable=False,
    )
    datasets: List[InputDatasetModel] = Field(
        title="datasets",
        description="List of input datasets for the project.",
    )
    dimensions: DimensionsModel = Field(
        title="dimensions",
        description="List of `base` and `supplemental` dimensions.",
    )
    dimension_mappings: DimensionMappingsModel = Field(
        title="dimension_mappings",
        description="List of project mappings. Initialized with base-to-base and"
        " base-to-supplemental mappings. dataset-to-project mappings are added by dsgrid as"
        " datasets get registered with the project.",
        default=DimensionMappingsModel(),
        notes=("`[dimension_mappings]` are optional at the project level.",),
    )
    dimension_associations: List = Field(
        title="dimension_associations",
        description="List of tabular files that specify required dimension associations. The "
        "files will be joined in the given order to produce a single table.",
        default=[],
    )

    @root_validator(pre=False, skip_on_failure=True)
    def check_mappings_with_dimensions(cls, values):
        """Check that dimension mappings refer to dimensions listed in the model."""
        dimension_names = {
            (x.name, x.dimension_type)
            for x in itertools.chain(
                values["dimensions"].base_dimensions,
                values["dimensions"].supplemental_dimensions,
            )
        }
        mapping_names = set()
        for mapping in values["dimension_mappings"].base_to_supplemental:
            mapping_names.add((mapping.from_dimension.name, mapping.from_dimension.dimension_type))
            mapping_names.add((mapping.to_dimension.name, mapping.to_dimension.dimension_type))

        diff = mapping_names.difference(dimension_names)
        if diff:
            raise ValueError(f"base_to_supplemental mappings contain unknown dimensions: {diff}")

        return values

    @validator("project_id")
    def check_project_id_handle(cls, project_id):
        """Check for valid characters in project id"""
        if "-" in project_id:
            raise ValueError('invalid character "-" in project id')

        check_config_id_strict(project_id, "Project")
        return project_id

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        return serialize_model_data(data)


class _DimensionQueryNamesModel(DSGBaseModel):
    base: str
    supplemental: List[str]


class ProjectDimensionQueryNamesModel(DSGBaseModel):
    """Defines the query names for all base and supplemental dimensions in the project."""

    data_source: _DimensionQueryNamesModel
    geography: _DimensionQueryNamesModel
    metric: _DimensionQueryNamesModel
    model_year: _DimensionQueryNamesModel
    scenario: _DimensionQueryNamesModel
    sector: _DimensionQueryNamesModel
    subsector: _DimensionQueryNamesModel
    time: _DimensionQueryNamesModel
    weather_year: _DimensionQueryNamesModel


# TODO DT: make a test that verifies all dimensions are covered


class ProjectConfig(ConfigWithDataFilesBase):
    """Provides an interface to a ProjectConfigModel."""

    def __init__(self, model):
        super().__init__(model)
        self._base_dimensions = {}  # DimensionKey to DimensionConfig
        self._supplemental_dimensions = {}  # DimensionKey to DimensionConfig
        self._base_to_supplemental_mappings = {}
        self._dimension_associations = None
        self._dimensions_by_query_name = {}

    @staticmethod
    def model_class():
        return ProjectConfigModel

    @staticmethod
    def config_filename():
        return "project.toml"

    @staticmethod
    def data_file_fields():
        return []

    @staticmethod
    def data_files_fields():
        return ["dimension_associations"]

    @classmethod
    def load(cls, config_file, dimension_manager, dimension_mapping_manager):
        config = super().load(config_file)
        config.src_dir = os.path.dirname(config_file)
        config.load_dimensions_and_mappings(dimension_manager, dimension_mapping_manager)
        return config

    def load_dimensions_and_mappings(self, dimension_manager, dimension_mapping_manager):
        self.load_dimension_associations()
        self.load_dimensions(dimension_manager)
        self.load_dimension_mappings(dimension_mapping_manager)

    @property
    def dimension_associations(self):
        return self._dimension_associations

    def get_base_dimension(self, dimension_type: DimensionType):
        """Return the base dimension matching dimension_type.

        Parameters
        ----------
        dimension_type : DimensionType

        Returns
        -------
        DimensionConfig

        """
        for key, dim_config in self.base_dimensions.items():
            if key.type == dimension_type:
                return dim_config
        assert False, dimension_type

    def get_dimension(self, dimension_query_name: str):
        """Return an instance of DimensionBaseConfig.

        Parameters
        ----------
        dimension_query_name : str

        Returns
        -------
        DimensionBaseConfig

        """
        dim = self._dimensions_by_query_name.get(dimension_query_name)
        if dim is None:
            raise DSGInvalidDimension(f"dimension_query_name={dimension_query_name} is not stored")
        return dim

    def get_dimension_records(self, dimension_query_name: str):
        """Return a DataFrame containing the records for a dimension.

        Parameters
        ----------
        dimension_query_name : str

        Returns
        -------
        DimensionBaseConfig

        """
        return self.get_dimension(dimension_query_name).get_records_dataframe()

    def get_supplemental_dimensions(self, dimension_type: DimensionType):
        """Return the supplemental dimensions matching dimension (if any).

        Parameters
        ----------
        dimension_type : DimensionType

        Returns
        -------
        List[DimensionConfig]

        """
        return [v for k, v in self.supplemental_dimensions.items() if k.type == dimension_type]

    def get_base_to_supplemental_dimension_mappings_by_types(self, dimension_type: DimensionType):
        """Return the base-to-supplemental dimension mappings for the dimension (if any).

        Parameters
        ----------
        dimension : DimensionType

        Returns
        -------
        list
            List of DimensionMappingConfig

        """
        return [
            x
            for x in self._base_to_supplemental_mappings.values()
            if x.model.from_dimension.dimension_type == dimension_type
        ]

    def get_base_to_supplemental_mapping_records(self, dimension_query_name: str):
        """Return the project's base-to-supplemental dimension mapping records.

        Parameters
        ----------
        dimension_query_name : str

        Returns
        -------
        pyspark.sql.DataFrame

        """
        dim = self.get_dimension(dimension_query_name)
        dimension_type = dim.model.dimension_type
        base_dim = self.get_base_dimension(dimension_type)
        if dim.model.dimension_id == base_dim.model.dimension_id:
            raise DSGInvalidParameter(
                f"Cannot pass base dimension: {dimension_type}/{dimension_query_name}"
            )

        for mapping in self._base_to_supplemental_mappings.values():
            if mapping.model.to_dimension.dimension_id == dim.model.dimension_id:
                return mapping.get_records_dataframe().filter("to_id is not NULL")

        raise DSGInvalidParameter(
            f"No mapping is stored for {dimension_type}/{dimension_query_name}"
        )

    def has_base_to_supplemental_dimension_mapping_types(self, dimension_type):
        """Return True if the config has these base-to-supplemental mappings."""
        return self._has_mapping(
            dimension_type,
            dimension_type,
            self._base_to_supplemental_mappings,
        )

    @staticmethod
    def _has_mapping(from_dimension_type, to_dimension_type, mapping):
        for config in mapping.values():
            if (
                config.model.from_dimension.dimension_type == from_dimension_type
                and config.model.to_dimension.dimension_type == to_dimension_type
            ):
                return True
        return False

    def list_dimension_query_names(self):
        """Return query names for all dimensions in the project.

        Returns
        -------
        list
            Sorted list of strings

        """
        return sorted(self._dimensions_by_query_name.keys())

    def get_base_dimension_query_names(self) -> Set[str]:
        """Return the query names for the base dimensions."""
        return set(self.get_base_dimension_to_query_name_mapping().values())

    def get_base_dimension_to_query_name_mapping(self) -> Dict[DimensionType, str]:
        """Return a mapping of DimensionType to query name for base dimensions.

        Returns
        -------
        dict

        """
        query_names = {}
        for dimension_type in DimensionType:
            dim = self.get_base_dimension(dimension_type)
            query_names[dimension_type] = dim.model.dimension_query_name
        return query_names

    def get_supplemental_dimension_to_query_name_mapping(self) -> Dict[DimensionType, List[str]]:
        """Return a mapping of DimensionType to query name for supplemental dimensions.

        Returns
        -------
        dict

        """
        query_names = {}
        for dimension_type in DimensionType:
            query_names[dimension_type] = sorted(
                (
                    x.model.dimension_query_name
                    for x in self.get_supplemental_dimensions(dimension_type)
                )
            )
        return query_names

    def get_dimension_query_names_model(self):
        """Return an instance of ProjectDimensionQueryNamesModel for the project."""
        base_query_names_by_type = self.get_base_dimension_to_query_name_mapping()
        supp_query_names_by_type = self.get_supplemental_dimension_to_query_name_mapping()
        model = {}
        for dimension_type in DimensionType:
            model[dimension_type.value] = {
                "base": base_query_names_by_type[dimension_type],
                "supplemental": supp_query_names_by_type[dimension_type],
            }
        return ProjectDimensionQueryNamesModel(**model)

    def load_dimension_associations(self):
        """Load all dimension associations."""
        # Find out what dimension have no associations which means that all dimension recrods from
        # the project must be used
        self._dimension_associations = DimensionAssociations.load(
            self.src_dir,
            self.model.dimension_associations,
        )

    def load_dimensions(self, dimension_manager: DimensionRegistryManager):
        """Load all Base Dimensions.

        Parameters
        ----------
        dimension_manager : DimensionRegistryManager

        """
        base_dimensions = dimension_manager.load_dimensions(
            self.model.dimensions.base_dimension_references
        )
        supplemental_dimensions = dimension_manager.load_dimensions(
            self.model.dimensions.supplemental_dimension_references
        )
        self._base_dimensions.update(base_dimensions)
        self._supplemental_dimensions.update(supplemental_dimensions)
        self._dimensions_by_query_name.clear()
        for dim in self.iter_dimensions():
            if dim.model.dimension_query_name in self._dimensions_by_query_name:
                raise DSGInvalidDimension(
                    f"dimension_query_name={dim.model.dimension_query_name} exists multiple times in project "
                    f"{self.config_id}"
                )
            self._dimensions_by_query_name[dim.model.dimension_query_name] = dim

    def load_dimension_mappings(self, dimension_mapping_manager: DimensionMappingRegistryManager):
        """Load all dimension mappings.

        Parameters
        ----------
        dimension_mapping_manager: DimensionMappingRegistryManager

        """
        base_to_supp = dimension_mapping_manager.load_dimension_mappings(
            self.model.dimension_mappings.base_to_supplemental_references
        )

        self._base_to_supplemental_mappings.update(base_to_supp)
        # TODO: Once we start using these we may need to store by (from, to) as key instead.

    @track_timing(timer_stats_collector)
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

    @property
    def config_id(self):
        return self._model.project_id

    def get_dataset(self, dataset_id):
        """Return a dataset by ID."""
        for dataset in self.model.datasets:
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

        # TODO: what about benchmark and historical?
        return False

    def iter_datasets(self):
        for dataset in self.model.datasets:
            yield dataset

    def iter_dataset_ids(self):
        for dataset in self.model.datasets:
            yield dataset.dataset_id

    def iter_dimensions(self):
        """Return an iterator over all dimensions of the project.

        Yields
        ------
        DimensionConfig

        """
        return itertools.chain(
            self.base_dimensions.values(), self.supplemental_dimensions.values()
        )

    def list_dimension_query_names_by_type(self, dimension_type: DimensionType):
        """List the query names available for a dimension type."""
        return [
            x.model.dimension_query_name
            for x in self.iter_dimensions()
            if x.model.dimension_type == dimension_type
        ]

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

    @track_timing(timer_stats_collector)
    def make_dimension_association_table(self, data_source=None):
        """Return a table with associations across all dimensions.

        Parameters
        ----------
        data_source : DimensionType, optional
            Filter table by data_source, by default None.

        Returns
        -------
        pyspark.sql.DataFrame

        """
        ds = DimensionType.DATA_SOURCE.value
        table = self.dimension_associations.table
        table_columns = set()
        # table is None when the project doesn't define any dimension associations.
        if table is not None:
            if ds in table.columns and data_source is not None:
                table = table.filter(f"{ds} = '{data_source}'").drop(ds)
            table_columns.update(table.columns)

        all_dimensions = {d.value for d in DimensionType if d != DimensionType.TIME}
        missing_dimensions = all_dimensions.difference(table_columns)
        for dim in missing_dimensions:
            table_count = 0 if table is None else table.count()
            other = (
                self.get_base_dimension(DimensionType(dim))
                .get_records_dataframe()
                .selectExpr(f"id as {dim}")
            )
            if table is None:
                table = other
            else:
                table = table.crossJoin(other)
                assert table.count() == other.count() * table_count

        return table

    def are_all_datasets_submitted(self):
        """Return True if all datasets have been submitted.

        Returns
        -------
        bool

        """
        return not self.list_unregistered_dataset_ids()

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
            dict of DimensionConfig keyed by DimensionKey

        """
        return self._base_dimensions

    @property
    def supplemental_dimensions(self):
        """Return the supplemental dimensions.

        Returns
        -------
        dict
            dict of DimensionConfig keyed by DimensionKey

        """
        return self._supplemental_dimensions
