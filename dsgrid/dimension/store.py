import logging


from dsgrid.dimension.dimension_records import DimensionRecords
from dsgrid.exceptions import (
    DSGInvalidDimension,
    # DSGInvalidDimensionMapping,
)
from dsgrid.utils.timing import timed_debug


logger = logging.getLogger(__name__)


class DimensionStore:
    """Provides mapping functionality for project or dataset dimensions."""

    REQUIRED_FIELDS = ("id", "name")

    def __init__(self, record_store):
        self._record_store = record_store
        self._store = {}  # {class type: DimensionRecordBaseModel}
        self._dimension_direct_mappings = {}

    @classmethod
    @timed_debug
    def load(cls, dimensions):
        """Load dimension records.

        Parameters
        ----------
        dimensions : sequence
            list or iterable of DimensionConfig

        spark : SparkSession

        Returns
        -------
        DimensionStore

        """
        records = DimensionRecords()
        store = cls(records)
        for dimension in dimensions:
            store.add_dimension(dimension.model)
        return store

    def add_dimension(self, dimension):
        """Add a dimension to the store.

        Parameters
        ----------
        dimension : DimensionRecordBaseModel

        """
        assert dimension.cls not in self._store
        self._store[dimension.cls] = dimension

        if getattr(dimension, "mappings", None):
            for mapping in dimension.mappings:
                self.add_dimension_mapping(dimension, mapping)

        if getattr(dimension, "records", None):
            self._record_store.add_dataframe(dimension)

    def get_dimension(self, dimension_class):
        """Return a dimension from the store.

        Parameters
        ----------
        dimension_class : type
            subclass of DimensionRecordBaseModel

        Returns
        -------
        DimensionRecordBaseModel
            instance of DimensionRecordBaseModel

        """
        self._raise_if_dimension_not_stored(dimension_class)
        return self._store[dimension_class]

    def iter_dimension_classes(self, base_class=None):
        """Return an iterator over the stored dimension classes.

        Parameters
        ----------
        base_class : type | None
            If set, return subclasses of this abstract dimension class

        Returns
        -------
        iterator
            model classes representing each dimension

        """
        if base_class is None:
            return self._store.keys()
        return (x for x in self._store if issubclass(x, base_class))

    def list_dimension_classes(self, base_class=None):
        """Return the stored dimension classes.

        Parameters
        ----------
        base_class : type | None
            If set, return subclasses of this abstract dimension class

        Returns
        -------
        list
            list of classes representing each dimension type

        """
        return sorted(
            list(self.iter_dimension_classes(base_class=base_class)),
            key=lambda x: x.__name__,
        )

    @property
    def record_store(self):
        """Return the DimensionRecords."""
        return self._record_store

    @property
    def spark(self):
        """Return the SparkSession instance."""
        return self._record_store.spark

    def _raise_if_dimension_not_stored(self, dimension_class):
        if dimension_class not in self._store:
            raise DSGInvalidDimension(f"{dimension_class} is not stored")

    # def add_dimension_mapping(self, dimension, mapping):
    #    """Add a dimension mapping to the store.

    #    Parameters
    #    ----------
    #    dimension : DimensionRecordBaseModel
    #    mapping : DimensionDirectMapping

    #    """
    #    # TODO: other mapping types
    #    assert isinstance(mapping, DimensionDirectMapping), mapping.__name__
    #    key = (dimension.cls, mapping.to_dimension)
    #    assert key not in self._dimension_direct_mappings
    #    self._dimension_direct_mappings[key] = mapping
    #    logger.debug("Added dimension mapping %s-%s", dimension.cls, mapping)

    # def get_dimension_direct_mapping(self, from_dimension, to_dimension):
    #    """Return the mapping to perform a join.

    #    Parameters
    #    ----------
    #    from_dimension : type
    #    to_dimension : type

    #    Returns
    #    -------
    #    str

    #    Raises
    #    ------
    #    DSGInvalidDimensionMapping
    #        Raised if the mapping is not stored.

    #    """
    #    key = (from_dimension, to_dimension)
    #    self._raise_if_mapping_not_stored(key)
    #    return self._dimension_direct_mappings[key]

    # def _raise_if_mapping_not_stored(self, key):
    #    if key not in self._dimension_direct_mappings:
    #        raise DSGInvalidDimensionMapping(f"{key} is not stored")
