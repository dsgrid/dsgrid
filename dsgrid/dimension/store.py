
from collections import defaultdict
from dataclasses import fields
import logging
import sqlite3

from dsgrid.dimension.base import DSGBaseDimension
from dsgrid.exceptions import DSGInvalidField, DSGInvalidEnumeration
from dsgrid.utils.files import load_data
from dsgrid.utils.timing import timed_debug


logger = logging.getLogger(__name__)


class DimensionStore:
    """Provides mapping functionality for project-defined dimensions."""

    REQUIRED_FIELDS = ("id", "name")

    def __init__(self):
        # Two-level dict: {DimensionType: {id: Record}}
        # Example
        #     {State: {'TX': {'id': 'TX', 'name', 'Texas'}}}
        self._store = defaultdict(dict)
        # Allows deserialization of records.
        self._type_mapping = {}
        self._one_to_many_mapping = defaultdict(dict)
        self._one_to_one_mapping = defaultdict(dict)
        self._programmatic_one_to_one_mapping = {}

    @classmethod
    @timed_debug
    def load(cls, model_mappings, one_to_many=None, one_to_one=None, programmatic_one_to_one=None):
        """Load a project's dimension dataset records.

        Parameters
        ----------
        model_mappings : dict
            Maps a dataclass to a JSON file defining records for that dataclass

        Returns
        -------
        DimensionStore

        """
        # TODO: develop data structure to accept all inputs
        store = cls()

        for dimension_type, filename in model_mappings.items():
            if not issubclass(dimension_type, DSGBaseDimension):
                raise DSGInvalidEnumeration(
                    f"{dimension_type.__name__} is not derived from DSGBaseDimension"
                )

            ids = set()
            for record_dict in load_data(filename):
                try:
                    record = dimension_type(**record_dict)
                except TypeError:
                    logger.exception("Error: construct %s from %s", dimension_type.__name__, record_dict)
                    continue

                for field in cls.REQUIRED_FIELDS:
                    if not hasattr(record, field):
                        raise DSGInvalidField(f"{dimension_type.__name__} must define {field}")

                record_id = getattr(record, "id")
                if not isinstance(record_id, str):
                    raise DSGInvalidField(f"id must be a string: {dimension_type.__name__} {record}")
                if record_id in ids:
                    raise DSGInvalidField(f"{dimension_type.__name__}{record_id} is not unique")
                ids.add(record_id)
                store.add_record(dimension_type, record)

        # TODO: this will cache all possible mappings in memory. Current
        # expectation is that the size will not be too large. If that changes
        # then this could easily be adjusted to run queries on demand.

        if one_to_many:
            conn = store.to_sqlite3()
            store.add_one_to_many(one_to_many, conn)

        if one_to_one is not None:
            store.add_one_to_one(one_to_one)

        if programmatic_one_to_one:
            store.add_programmatic_one_to_one(programmatic_one_to_one)

        return store

    @timed_debug
    def add_one_to_many(self, one_to_many_mapping, conn):
        for mapping in one_to_many_mapping:
            key = (mapping.from_type, mapping.to_type)
            for from_record in self.iter_records(mapping.from_type):
                table = mapping.to_type.__name__
                query = f"select {table}.id from {table} where {table}.{mapping.to_field} = '{from_record.id}'"
                self._one_to_many_mapping[key][from_record.id] = [
                    self.get_record(mapping.to_type, x[0]) for x in conn.execute(query)
                ]

        logger.debug("size of one_to_many = %s",
            sum((len(x) for x in self._one_to_many_mapping.values())),
        )

    @timed_debug
    def add_one_to_one(self, one_to_one_mapping):
        for mapping in one_to_one_mapping:
            key = (mapping.from_type, mapping.to_type)
            for record in self.iter_records(mapping.from_type):
                dest_id = getattr(record, mapping.from_field)
                # TODO: is this correct for missing mappings?
                dest = self.get_record_or_default(mapping.to_type, dest_id, None)
                self._one_to_one_mapping[key][record.id] = dest

        logger.debug("size of one_to_one = %s",
            sum((len(x) for x in self._one_to_one_mapping.values())),
        )

    @timed_debug
    def add_programmatic_one_to_one(self, programmatic_one_to_one_mapping):
        for mapping in programmatic_one_to_one_mapping:
            key = (mapping.from_type, mapping.to_type)
            if key in self._programmatic_one_to_one_mapping:
                assert mapping.convert_func == self._programmatic_one_to_one_mapping[key]
            self._programmatic_one_to_one_mapping[key] = mapping.convert_func

        logger.debug("size of programmatic_one_to_one = %s",
            len(self._programmatic_one_to_one_mapping.values()),
        )

    def add_record(self, dimension_type, record):
        """Add a record to the store.

        Parameters
        ----------
        dimension_type : class
        record : dataclass

        """
        if dimension_type not in self._store:
            self._type_mapping[dimension_type.__name__] = dimension_type

        self._store[dimension_type][record.id] = record
        # too noisy
        #logger.debug("Added %s %s", dimension_type.__name__, record.name)

    def get_record(self, dimension_type, record_id, default=None):
        """Get a record from the store.

        Parameters
        ----------
        dimension_type : class
        record_id : id

        """
        self._raise_if_not_stored(dimension_type)

        record = self._store[dimension_type].get(record_id)
        if record is None:
            raise DSGInvalidEnumeration(f"{dimension_type.__name__} {record_id} is not stored")

        return record

    def get_record_or_default(self, dimension_type, record_id, default):
        """Get a record from the store.

        Parameters
        ----------
        dimension_type : class
        record_id : id

        """
        if dimension_type not in self._store:
            return default

        record = self._store[dimension_type].get(record_id)
        return record or default

    def has_record(self, dimension_type, record_id):
        """Return true if the record is stored.

        Parameters
        ----------
        dimension_type : class
        record_id : id

        """
        if dimension_type not in self._store:
            return False

        return record_id in self._store[dimension_type]

    def iter_dimension_types(self, base_class=None):
        """Return an iterator over the stored dimension types.

        Parameters
        ----------
        base_class : class | None
            If set, return subclasses of this abstract dimension class

        Returns
        -------
        iterator
            dataclasses representing each dimension type

        """
        if base_class is None:
            return self._store.keys()
        return (x for x in self._store.keys() if issubclass(x, base_class))

    def iter_records(self, dimension_type):
        """Return an iterator over the records for dimension_type.

        Parameters
        ----------
        dimension_type : class
            dataclass

        Returns
        -------
        iterator

        """
        self._raise_if_not_stored(dimension_type)
        return self._store[dimension_type].values()

    def list_dimension_types(self, base_class=None):
        """Return the stored dimension types.

        Parameters
        ----------
        base_class : class | None
            If set, return subclasses of this abstract dimension class

        Returns
        -------
        list
            list of dataclasses representing each dimension type

        """
        return sorted(list(self.iter_dimension_types(base_class=base_class)),
                      key=lambda x: x.__name__)

    def list_records(self, dimension_type):
        """Return the records for the dimension_type.

        Returns
        -------
        list
            list of dataclass instances

        """
        return sorted(list(self.iter_records(dimension_type)), key=lambda x: x.id)

    def map_one_to_many(self, from_type, to_type, val):
        return self._map_val(self._one_to_many_mapping, from_type, to_type, val)

    def map_one_to_one(self, from_type, to_type, val):
        return self._map_val(self._one_to_one_mapping, from_type, to_type, val)

    def _map_val(self, data_dict, from_type, to_type, val):
        key = (from_type, to_type)
        mapping = data_dict.get(key)
        if mapping is None:
            raise Exception(f"no mapping for {from_type} to {to_type}")

        result = mapping.get(val)
        if result is None:
            raise Exception(f"no mapping for {from_type} to {to_type} {val}") 

        return result

    def map_programmatic_one_to_one(self, from_type, to_type, val):
        key = (from_type, to_type)
        convert_func = self._programmatic_one_to_one_mapping.get(key)
        if convert_func is None:
            raise Exception(f"no mapping for {from_type} to {to_type}")

        return self.get_record(to_type, convert_func(val))

    def to_sqlite3(self):
        """Convert the store to sqlite3 for queries."""
        # TODO: could make an option to store in a file.
        conn = sqlite3.connect(":memory:")
        for dimension_type, records in self._store.items():
            table = dimension_type.__name__
            names = []
            types = []
            for field in fields(dimension_type):
                names.append(field.name)
                types.append(_SQL_TYPES.get(field.type, field.type.__name__))
            schema = ", ".join((f"{n} {t}" for n, t in zip(names, types)))
            conn.execute(f"create table {table}({schema})")
            items = []
            for record in records.values():
                items.append(tuple(getattr(record, name) for name in names))
            placeholder = ", ".join(["?"] * len(names))
            conn.executemany(f"insert into {table} values ({placeholder})", items)

        return conn

    def _raise_if_not_stored(self, dimension_type):
        if dimension_type not in self._store:
            raise DSGInvalidEnumeration(f"{dimension_type.__name__} is not stored")


# This may not be required for numbers. Strings that are numbers are converted
# to numbers unless we do this.
_SQL_TYPES = {
    None: "NULL",
    int: "INTEGER",
    float: "REAL",
    str: "text",
    bytes: "BLOB",
}


def deserialize_record(dimension_type, record_as_tuple):
    """Return an instance of the dimension_type deserialized from a tuple.

    Parameters
    ----------
    dimension_type : class
    record_as_tuple : tuple
        record read from SQL db

    Returns
    -------
    dataclass

    """
    return dimension_type(*record_as_tuple)
