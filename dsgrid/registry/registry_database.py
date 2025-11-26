import getpass
import logging
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Any, Generator

from chronify.utils.path_utils import check_overwrite
from sqlalchemy import (
    Column,
    Connection,
    Engine,
    Integer,
    ForeignKey,
    JSON,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    delete,
    insert,
    select,
    update,
)

from dsgrid.exceptions import (
    DSGValueNotRegistered,
    DSGInvalidOperation,
    DSGValueNotStored,
    DSGDuplicateValueRegistered,
)
from dsgrid.registry.common import (
    DataStoreType,
    DatabaseConnection,
    RegistrationModel,
    RegistryTables,
    RegistryType,
    MODEL_TYPE_TO_ID_FIELD_MAPPING,
)
from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.registry.data_store_factory import make_data_store
from dsgrid.utils.files import dump_data


logger = logging.getLogger(__name__)


class RegistryDatabase:
    """Database containing a dsgrid registry"""

    def __init__(self, engine: Engine, data_store: DataStoreInterface | None = None) -> None:
        """Construct the database."""
        self._metadata = MetaData()
        self._engine = engine
        self._data_store = data_store

    def dispose(self) -> None:
        """Dispose the database engine and release all connections."""
        self._engine.dispose()

    @classmethod
    def create(
        cls,
        conn: DatabaseConnection,
        data_path: Path,
        data_store_type: DataStoreType = DataStoreType.FILESYSTEM,
        overwrite: bool = False,
        **connect_kwargs: Any,
    ) -> "RegistryDatabase":
        """Create a new registry database."""
        filename = conn.get_filename()
        for path in (filename, data_path):
            check_overwrite(path, overwrite=overwrite)
        data_path.mkdir()
        data_store = make_data_store(data_path, data_store_type, initialize=True)
        db = cls(create_engine(conn.url, **connect_kwargs), data_store)
        db.initialize_db(data_path, data_store_type)
        return db

    @classmethod
    def create_with_existing_data(
        cls,
        conn: DatabaseConnection,
        data_path: Path,
        data_store_type: DataStoreType = DataStoreType.FILESYSTEM,
        overwrite: bool = False,
        **connect_kwargs: Any,
    ) -> "RegistryDatabase":
        """Create a new registry database with existing registry data."""
        filename = conn.get_filename()
        check_overwrite(filename, overwrite=overwrite)
        store = make_data_store(data_path, data_store_type, initialize=False)
        db = RegistryDatabase(create_engine(conn.url, **connect_kwargs), store)
        db.initialize_db(data_path, data_store_type)
        return db

    @classmethod
    def connect(
        cls,
        conn: DatabaseConnection,
        **connect_kwargs: Any,
    ) -> "RegistryDatabase":
        """Load an existing registry database."""
        # This tests the connection.
        conn.get_filename()
        engine = create_engine(conn.url, **connect_kwargs)
        db = RegistryDatabase(engine)
        db.update_sqlalchemy_metadata()
        base_path = db.get_data_path()
        data_store_type = db.get_data_store_type()
        db.data_store = make_data_store(base_path, data_store_type, initialize=False)
        return db

    def update_sqlalchemy_metadata(self) -> None:
        """Update the sqlalchemy metadata for table schema. Call this method if you add tables
        in the sqlalchemy engine outside of this class.
        """
        self._metadata.reflect(self._engine, views=True)

    @property
    def engine(self) -> Engine:
        """Return the sqlalchemy engine."""
        return self._engine

    @property
    def data_store(self) -> DataStoreInterface:
        """Return the data store."""
        if self._data_store is None:
            msg = "Data store is not initialized. Use create() or connect() to initialize."
            raise DSGInvalidOperation(msg)
        return self._data_store

    @data_store.setter
    def data_store(self, store: DataStoreInterface) -> None:
        """Set the data store."""
        self._data_store = store

    def initialize_db(self, data_path: Path, data_store_type: DataStoreType) -> None:
        """Initialize the database to store a dsgrid registry."""
        create_tables(self._engine, self._metadata)
        created_by = getpass.getuser()
        created_on = str(datetime.now())
        registry_data_path = str(data_path)
        with self._engine.begin() as conn:
            kv_table = self.get_table(RegistryTables.KEY_VALUE)
            conn.execute(insert(kv_table).values(key="created_by", value=created_by))
            conn.execute(insert(kv_table).values(key="created_on", value=created_on))
            conn.execute(insert(kv_table).values(key="data_path", value=registry_data_path))
            conn.execute(
                insert(kv_table).values(key="data_store_type", value=data_store_type.value)
            )

        record = {
            "created_by": created_by,
            "created_on": created_on,
            "data_path": registry_data_path,
            "data_store_type": data_store_type.value,
        }
        dump_data(record, data_path / "registry.json5")

    def get_table(self, name: RegistryTables) -> Table:
        """Return the sqlalchemy Table object."""
        if not self.has_table(name):
            msg = f"{name=}"
            raise DSGValueNotStored(msg)
        return Table(name.value, self._metadata)

    def has_table(self, name: RegistryTables) -> bool:
        """Return True if the database has a table with the given name."""
        return name in self._metadata.tables

    def try_get_table(self, name: RegistryTables) -> Table | None:
        """Return the sqlalchemy Table object or None if it is not stored."""
        if not self.has_table(name):
            return None
        return Table(name.value, self._metadata)

    def list_tables(self) -> list[str]:
        """Return a list of tables in the database."""
        return [RegistryTables(x) for x in self._metadata.tables]

    @classmethod
    def copy(
        cls,
        src_conn: DatabaseConnection,
        dst_conn: DatabaseConnection,
        dst_data_path: Path,
    ) -> "RegistryDatabase":
        """Copy the contents of a source database to a destination and return the destination.
        Currently, only supports SQLite backends.
        """
        sqlite_base = "sqlite:///"
        if sqlite_base not in src_conn.url or sqlite_base not in dst_conn.url:
            # If/when we need postgres, we can use postgres tools or copy the tables through
            # sqlalchemy.
            msg = "Both src and destination databases must be sqlite. {src_conn=} {dst_conn=}"
            raise NotImplementedError(msg)

        cls.delete(dst_conn)
        dst = cls.create(dst_conn, dst_data_path)
        with sqlite3.connect(src_conn.url.replace("sqlite:///", "")) as src:
            with dst.engine.begin() as dst_conn_:
                # The backup below will overwrite the data_path value.
                table = dst.get_table(RegistryTables.KEY_VALUE)
                stmt = select(table.c.value).where(table.c.key == "data_path")
                row = dst_conn_.execute(stmt).fetchone()
                assert row is not None
                orig_data_path = row.value
                assert dst_conn_._dbapi_connection is not None
                assert isinstance(
                    dst_conn_._dbapi_connection.driver_connection, sqlite3.Connection
                )
                src.backup(dst_conn_._dbapi_connection.driver_connection)
                stmt = update(table).where(table.c.key == "data_path").values(value=orig_data_path)
                dst_conn_.execute(stmt)

        logger.info("Copied database %s to %s", src_conn.url, dst_conn.url)
        dst.dispose()
        return dst

    @staticmethod
    def delete(conn: DatabaseConnection) -> None:
        """Delete the dsgrid database."""
        filename = conn.get_filename()
        if filename.exists():
            filename.unlink()

    @staticmethod
    def has_database(conn: DatabaseConnection) -> bool:
        """Return True if the database exists."""
        filename = conn.get_filename()
        return filename.exists()

    def _get_model_id(self, model_type: RegistryType, model: dict[str, Any]) -> str:
        return model[self._get_model_id_field(model_type)]

    @staticmethod
    def _get_model_id_field(model_type: RegistryType) -> str:
        return MODEL_TYPE_TO_ID_FIELD_MAPPING[model_type]

    def insert_model(
        self,
        conn: Connection,
        model_type: RegistryType,
        model: dict[str, Any],
        registration: RegistrationModel,
    ) -> dict[str, Any]:
        """Add a model to the database. Sets the id field of the model with the database value."""
        table = self.get_table(RegistryTables.MODELS)
        model_id = self._get_model_id(model_type, model)
        if self.has(conn, model_type, model_id):
            msg = f"{model_type=} {model_id}"
            raise DSGDuplicateValueRegistered(msg)

        res = conn.execute(
            insert(table).values(
                registration_id=registration.id,
                model_type=model_type.value,
                model_id=model_id,
                version=model["version"],
                model=model,
            )
        )
        db_id = res.lastrowid
        new_model = self._add_database_id(conn, table, db_id)
        self._set_current_version(conn, model_type, model_id, db_id)
        logger.debug("Inserted model_type=%s model_id=%s id=%s", model_type, model_id, db_id)
        return new_model

    def update_model(
        self,
        conn: Connection,
        model_type: RegistryType,
        model: dict[str, Any],
        registration: RegistrationModel,
    ) -> dict[str, Any]:
        """Add a model to the database. Sets the id field of the model with the database value."""
        table = self.get_table(RegistryTables.MODELS)
        model_id = self._get_model_id(model_type, model)
        res = conn.execute(
            insert(table).values(
                registration_id=registration.id,
                model_type=model_type.value,
                model_id=model_id,
                version=model["version"],
                model=model,
            )
        )
        db_id = res.lastrowid
        new_model = self._add_database_id(conn, table, db_id)
        self._update_current_version(conn, model_type, model_id, db_id)
        logger.error("Updated model_type=%s model_id=%s id=%s", model_type, model_id, db_id)
        return new_model

    def _add_database_id(self, conn: Connection, table: Table, db_id: int) -> dict[str, Any]:
        """Add the newly-generated ID to the model's JSON blob and update the db."""
        stmt = select(table.c.model).where(table.c.id == db_id)
        row = conn.execute(stmt).fetchone()
        assert row
        data = row.model
        data["id"] = db_id
        conn.execute(update(table).where(table.c.id == db_id).values(model=data))
        return data

    def _set_current_version(
        self, conn: Connection, model_type: RegistryType, model_id: str, db_id: int
    ) -> None:
        table = self.get_table(RegistryTables.CURRENT_VERSIONS)
        conn.execute(
            insert(table).values(
                model_type=model_type.value,
                model_id=model_id,
                current_id=db_id,
                update_timestamp=str(datetime.now()),
            )
        )
        logger.debug("Set the current version of %s %s to %s", model_type, model_id, db_id)

    def _update_current_version(
        self, conn: Connection, model_type: RegistryType, model_id: str, db_id: int
    ) -> None:
        table = self.get_table(RegistryTables.CURRENT_VERSIONS)
        stmt = (
            update(table)
            .where(table.c.model_type == model_type)
            .where(table.c.model_id == model_id)
            .values(current_id=db_id, update_timestamp=str(datetime.now()))
        )
        conn.execute(stmt)
        logger.debug("Set the current version of %s %s to %s", model_type, model_id, db_id)

    def get_containing_models_by_db_id(
        self,
        conn: Connection,
        db_id: int,
        parent_model_type: RegistryType | None = None,
    ) -> list[tuple[RegistryType, dict[str, Any]]]:
        table1 = self.get_table(RegistryTables.CONTAINS)
        table2 = self.get_table(RegistryTables.MODELS)
        table3 = self.get_table(RegistryTables.CURRENT_VERSIONS)
        stmt = (
            select(table2.c.model_type, table2.c.model)
            .join(table2, table1.c.parent_id == table2.c.id)
            .join(table3, table2.c.id == table3.c.current_id)
            .where(table1.c.child_id == db_id)
        )
        if parent_model_type is not None:
            stmt = stmt.where(table2.c.model_type == parent_model_type)
        return [(RegistryType(x.model_type), x.model) for x in conn.execute(stmt).fetchall()]

    def get_containing_models(
        self,
        conn: Connection,
        child_model_type: RegistryType,
        model_id: str,
        version: str,
        parent_model_type: RegistryType | None = None,
    ):
        db_id = self._get_db_id(conn, child_model_type, model_id, version)
        return self.get_containing_models_by_db_id(
            conn, db_id, parent_model_type=parent_model_type
        )

    def _get_db_id(
        self, conn: Connection, model_type: RegistryType, model_id: str, version: str
    ) -> int:
        table = self.get_table(RegistryTables.MODELS)
        stmt = (
            select(table.c.id)
            .where(table.c.model_type == model_type)
            .where(table.c.model_id == model_id)
            .where(table.c.version == version)
        )
        row = conn.execute(stmt).fetchone()
        assert row
        return row.id

    def delete_models(self, conn: Connection, model_type: RegistryType, model_id: str) -> None:
        """Delete all documents of model_type with the model_id."""
        for table in (RegistryTables.MODELS, RegistryTables.CURRENT_VERSIONS):
            table = self._get_table(table)
            stmt = (
                delete(table)
                .where(table.c.model_type == model_type.value)
                .where(table.c.model_id == model_id)
            )

            conn.execute(stmt)

        logger.info("Deleted all documents with model_id=%s", model_id)

    def get_data_path(self) -> Path:
        """Return the path where dataset data is stored."""
        table = self._get_table(RegistryTables.KEY_VALUE)
        with self._engine.connect() as conn:
            row = conn.execute(select(table.c.value).where(table.c.key == "data_path")).fetchone()
            if row is None:
                msg = "Bug: received no result in query for data_path"
                raise Exception(msg)
            return Path(row.value)

    def get_data_store_type(self) -> DataStoreType:
        """Return the path where dataset data is stored."""
        table = self._get_table(RegistryTables.KEY_VALUE)
        with self._engine.connect() as conn:
            row = conn.execute(
                select(table.c.value).where(table.c.key == "data_store_type")
            ).fetchone()
            if row is None:
                # Allow legacy registries to keep working.
                return DataStoreType.FILESYSTEM
            return DataStoreType(row.value)

    def _get_table(self, table_type: RegistryTables) -> Table:
        return Table(table_type.value, self._metadata)

    def get_latest(self, conn: Connection, model_type: RegistryType, model_id: str):
        return self._get_latest_column(conn, model_type, model_id, "model")

    def get_latest_version(self, conn: Connection, model_type: RegistryType, model_id: str) -> str:
        return self._get_latest_column(conn, model_type, model_id, "version")

    def _get_latest_column(
        self, conn: Connection, model_type: RegistryType, model_id: str, column: str
    ) -> Any:
        table1 = self._get_table(RegistryTables.MODELS)
        table2 = self._get_table(RegistryTables.CURRENT_VERSIONS)
        stmt = (
            select(
                table1.c[column],
            )
            .join_from(table1, table2, table1.c.id == table2.c.current_id)
            .where(table2.c.model_type == model_type)
            .where(table2.c.model_id == model_id)
        )
        rows = conn.execute(stmt).fetchall()
        if not rows:
            msg = f"{model_type=} {model_id=} is not registered"
            raise DSGValueNotRegistered(msg)
        if len(rows) != 1:
            msg = "Bug: received more than one model set to latest: {rows}"
            raise Exception(msg)
        return getattr(rows[0], column)

    def list_model_ids(self, conn: Connection, model_type: RegistryType) -> list[str]:
        table = self.get_table(RegistryTables.MODELS)
        stmt = select(table.c.model_id).where(table.c.model_type == model_type).distinct()
        return [x.model_id for x in conn.execute(stmt).fetchall()]

    def iter_models(
        self, conn: Connection, model_type: RegistryType, all_versions: bool = False
    ) -> Generator[dict[str, Any], None, None]:
        table = self.get_table(RegistryTables.MODELS)
        if all_versions:
            stmt = select(table.c.model).where(table.c.model_type == model_type)
        else:
            table2 = self._get_table(RegistryTables.CURRENT_VERSIONS)
            stmt = (
                select(
                    table.c.model,
                )
                .join_from(table, table2, table.c.id == table2.c.current_id)
                .where(table2.c.model_type == model_type)
            )
        for item in conn.execute(stmt).fetchall():
            yield item.model

    def _get_by_version(
        self, conn: Connection, model_type: RegistryType, model_id: str, version: str
    ):
        table = self.get_table(RegistryTables.MODELS)
        stmt = (
            select(table.c.model)
            .where(table.c.model_type == model_type)
            .where(table.c.model_id == model_id)
            .where(table.c.version == str(version))
        )
        rows = conn.execute(stmt).fetchall()
        if not rows:
            msg = f"{model_type=} {model_id}"
            raise DSGValueNotRegistered(msg)
        if len(rows) > 1:
            msg = f"Bug: found more than one row. {model_type=} {model_id=} {version=}"
            raise Exception(msg)
        return rows[0].model

    def insert_registration(
        self,
        conn: Connection,
        registration: RegistrationModel,
    ) -> RegistrationModel:
        """Insert a registration entry to the database.

        Returns
        -------
        RegistrationModel
            Will be identical to the input except that id will be assigned from the database.
        """
        table = self.get_table(RegistryTables.REGISTRATIONS)
        res = conn.execute(
            insert(table).values(
                timestamp=str(registration.timestamp),
                submitter=registration.submitter,
                update_type=registration.update_type,
                log_message=registration.log_message,
            )
        )
        data = registration.model_dump(mode="json")
        data["id"] = res.lastrowid
        return RegistrationModel(**data)

    def get_registration(self, conn: Connection, db_id: int) -> RegistrationModel:
        """Return the registration information for the database ID."""
        table1 = self.get_table(RegistryTables.MODELS)
        table2 = self.get_table(RegistryTables.REGISTRATIONS)
        stmt = (
            select(
                table2.c.id,
                table2.c.timestamp,
                table2.c.submitter,
                table2.c.update_type,
                table2.c.log_message,
            )
            .join_from(table1, table2, table1.c.registration_id == table2.c.id)
            .where(table1.c.id == db_id)
        )
        rows = conn.execute(stmt).fetchall()
        if not rows:
            msg = f"{db_id=}"
            raise DSGValueNotRegistered(msg)
        if len(rows) > 1:
            msg = f"Bug: found more than one row matching {db_id=}"
            raise Exception(msg)
        row = rows[0]
        return RegistrationModel(
            id=row.id,
            timestamp=row.timestamp,
            submitter=row.submitter,
            update_type=row.update_type,
            log_message=row.log_message,
        )

    def get_initial_registration(
        self, conn: Connection, model_type: RegistryType, model_id: str
    ) -> RegistrationModel:
        """Return the initial registration information for the ID."""
        table1 = self.get_table(RegistryTables.MODELS)
        table2 = self.get_table(RegistryTables.REGISTRATIONS)
        stmt = (
            select(
                table2.c.id,
                table2.c.timestamp,
                table2.c.submitter,
                table2.c.update_type,
                table2.c.log_message,
            )
            .join_from(table1, table2, table1.c.registration_id == table2.c.id)
            .where(table1.c.model_type == model_type.value)
            .where(table1.c.model_id == model_id)
            .order_by(table1.c.id)
            .limit(1)
        )
        row = conn.execute(stmt).fetchone()
        if not row:
            msg = f"{model_type=} {model_id=}"
            raise DSGValueNotRegistered(msg)
        assert row
        return RegistrationModel(
            id=row.id,
            timestamp=row.timestamp,
            submitter=row.submitter,
            update_type=row.update_type,
            log_message=row.log_message,
        )

    def has(
        self,
        conn: Connection,
        model_type: RegistryType,
        model_id: str,
        version: str | None = None,
    ) -> bool:
        """Return True if the database has a document matching the inputs."""
        table = self.get_table(RegistryTables.MODELS)
        stmt = (
            select(table.c.id)
            .where(table.c.model_type == model_type)
            .where(table.c.model_id == model_id)
        )
        if version is not None:
            stmt = stmt.where(table.c.version == version)
        stmt = stmt.limit(1)
        res = conn.execute(stmt).fetchone()
        return bool(res)

    def insert_contains_edge(
        self,
        conn: Connection,
        parent_id: int,
        child_id: int,
    ) -> None:
        table = self.get_table(RegistryTables.CONTAINS)
        conn.execute(insert(table).values(parent_id=parent_id, child_id=child_id))

    def replace_model(self, conn: Connection, model: dict[str, Any]):
        """Replace the model in the database."""
        table = self.get_table(RegistryTables.MODELS)
        conn.execute(update(table).where(table.c.id == model["id"]).values(model=model))


def create_tables(engine: Engine, metadata: MetaData) -> None:
    """Create the registry tables in the database."""
    # Note to devs: Please update dev/registry_database.md if you change the schema.
    Table(
        RegistryTables.KEY_VALUE.value,
        metadata,
        Column("key", String(), unique=True, primary_key=True),
        Column("value", String(), nullable=False),
    )
    reg_table = Table(
        RegistryTables.REGISTRATIONS.value,
        metadata,
        Column("id", Integer, unique=True, primary_key=True),
        Column("timestamp", String(), nullable=False),
        Column("submitter", String(), nullable=False),
        Column("update_type", String(), nullable=False),
        Column("log_message", String(), nullable=False),
    )
    models_table = Table(
        RegistryTables.MODELS.value,
        metadata,
        Column("id", Integer(), unique=True, primary_key=True),
        Column("registration_id", Integer(), ForeignKey(reg_table.c.id), nullable=False),
        # project, dataset, dimension, dimension_mapping
        Column("model_type", String(), nullable=False),
        # project_id, dataset_id, dimension_id, mapping_id
        Column("model_id", String(), nullable=False),
        Column("version", String(), nullable=False),
        # This is project_config, dataset_config, etc.
        Column("model", JSON(), nullable=False),
        UniqueConstraint("model_type", "model_id", "version"),
    )
    Table(
        RegistryTables.CURRENT_VERSIONS.value,
        metadata,
        Column("id", Integer(), unique=True, primary_key=True),
        Column("model_type", String(), nullable=False),
        Column("model_id", String(), nullable=False),
        Column("current_id", Integer(), ForeignKey(models_table.c.id), nullable=False),
        Column("update_timestamp", String(), nullable=False),
        UniqueConstraint("model_type", "model_id"),
    )
    # This table manages associations between
    # projects and datasets,
    # projects and dimensions,
    # datasets and dimensions,
    # dimension mappings and dimensions,
    # and possibly derived datasets and datasets.
    Table(
        RegistryTables.CONTAINS,
        metadata,
        Column("id", Integer, primary_key=True, unique=True),
        Column("parent_id", Integer(), ForeignKey(models_table.c.id), nullable=False),
        Column("child_id", Integer(), ForeignKey(models_table.c.id), nullable=False),
    )
    metadata.create_all(engine)
