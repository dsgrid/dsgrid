import json
import logging
import re
from pathlib import Path

from arango import ArangoClient

from dsgrid.common import DEFAULT_DB_PASSWORD
from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGValueNotRegistered, DSGInvalidParameter
from dsgrid.registry.common import Collection, RegistrationModel, Edge


GRAPH = "registry"
# This number limits graph traversals along, for example, a project_root for a single project
# config to all of its versions (1.0.0, 1.0.1, 1.1.0, 2.0, etc.).
MAX_CONFIG_VERSIONS = 1000

logger = logging.getLogger(__name__)


class DatabaseConnection(DSGBaseModel):
    """Input information to connect to a registry database"""

    database: str = "dsgrid"
    hostname: str = "localhost"
    port: int = 8529
    username: str = "root"
    password: str = DEFAULT_DB_PASSWORD

    @classmethod
    def from_url(cls, url, **kwargs):
        """Create a connection from a URL."""
        regex = re.compile(r"http://(.*):(\d+)")
        match = regex.search(url)
        if match is None:
            raise DSGInvalidParameter(f"Invalid URL format: {url}")
        hostname = match.group(1)
        port = match.group(2)
        return cls(hostname=hostname, port=port, **kwargs)

    def url(self):
        return f"http://{self.hostname}:{self.port}"


class RegistryDatabase:
    """Database containing a dsgrid registry"""

    def __init__(self, client):
        self._client = client
        self._graph = client.graph(GRAPH)

    @classmethod
    def create(cls, conn: DatabaseConnection, data_path: Path):
        """Create a database to store a dsgrid registry."""
        client = ArangoClient(hosts=f"http://{conn.hostname}:{conn.port}")
        sys_db = client.db("_system", username=conn.username, password=conn.password)
        if not sys_db.create_database(conn.database):
            raise Exception(f"Failed to create the database {conn.database}")
        client = client.db(conn.database, username=conn.username, password=conn.password)
        client.create_collection("data_path")
        # For AWS there will be other fields.
        client.collection("data_path").insert({"data_path": str(data_path)})
        data_path.mkdir(exist_ok=True)
        (data_path / "data").mkdir(exist_ok=True)
        registry_file = data_path / "registry.json5"
        data = conn.model_dump()
        data.pop("password")
        registry_file.write_text(json.dumps(data, indent=2))

        graph = client.create_graph(GRAPH)
        graph.create_vertex_collection(Collection.PROJECT_ROOTS.value)
        graph.create_vertex_collection(Collection.DATASET_ROOTS.value)
        graph.create_vertex_collection(Collection.DATASET_DATA_ROOTS.value)
        graph.create_vertex_collection(Collection.DIMENSION_ROOTS.value)
        graph.create_vertex_collection(Collection.DIMENSION_MAPPING_ROOTS.value)
        graph.create_vertex_collection(Collection.PROJECTS.value)
        graph.create_vertex_collection(Collection.DATASETS.value)
        graph.create_vertex_collection(Collection.DATASET_DATA.value)
        graph.create_vertex_collection(Collection.DIMENSIONS.value)
        graph.create_vertex_collection(Collection.DIMENSION_MAPPINGS.value)
        dimension_types = graph.create_vertex_collection(Collection.DIMENSION_TYPES.value)
        for dim_type in DimensionType:
            dimension_types.insert({"_key": dim_type.value})

        # TODO: Figure out to specify exact from/to relationships.
        # We should be able to be limit what can be created.
        graph.create_edge_definition(
            edge_collection=Edge.CONTAINS.value,
            from_vertex_collections=[
                Collection.PROJECTS.value,
            ],
            to_vertex_collections=[
                Collection.DIMENSIONS.value,
            ],
        )
        graph.create_edge_definition(
            edge_collection=Edge.DERIVES.value,
            from_vertex_collections=[Collection.DATASETS.value],
            to_vertex_collections=[Collection.DATASETS.value],
        )
        graph.create_edge_definition(
            edge_collection=Edge.UPDATED_TO.value,
            from_vertex_collections=[
                Collection.DIMENSION_ROOTS.value,
            ],
            to_vertex_collections=[Collection.DIMENSIONS.value],
        )
        graph.create_edge_definition(
            edge_collection=Edge.LATEST.value,
            from_vertex_collections=[
                Collection.DIMENSION_ROOTS.value,
            ],
            to_vertex_collections=[
                Collection.DIMENSIONS.value,
            ],
        )
        graph.create_edge_definition(
            edge_collection=Edge.OF_TYPE.value,
            from_vertex_collections=[Collection.DIMENSIONS.value],
            to_vertex_collections=[Collection.DIMENSION_TYPES.value],
        )

        return cls(client)

    @classmethod
    def connect(cls, conn: DatabaseConnection):
        """Connect to a dsgrid registry database."""
        client = ArangoClient(hosts=f"http://{conn.hostname}:{conn.port}")
        client = client.db(conn.database, username=conn.username, password=conn.password)
        logger.info("Connected to database=%s at URL=%s", conn.database, conn.url())
        return cls(client)

    @classmethod
    def copy(cls, src_conn: DatabaseConnection, dst_conn: DatabaseConnection, dst_data_path):
        """Copy the contents of a source database to a destination and return the destination."""
        src = cls.connect(src_conn)
        cls.delete(dst_conn)
        dst = cls.create(dst_conn, dst_data_path)
        edge_names = set()
        for collection in src.client.collections():
            name = collection["name"]
            # If we keep anything besides data_path here, we need to import it.
            if name in {"dimension_types", "data_path"}:
                continue
            if not name.startswith("_"):
                if collection["type"] == "edge":
                    edge_names.add(name)
                    continue
                for item in src.client.collection(name):
                    dst.client.collection(name).insert(item)
        for name in edge_names:
            for item in src.client.collection(name):
                dst.client.collection(name).insert(item)

        logger.info(
            "Copied database %s / %s to %s / %s",
            src_conn.url(),
            src_conn.database,
            dst_conn.url(),
            dst_conn.database,
        )
        return dst

    @staticmethod
    def delete(conn: DatabaseConnection):
        """Delete the dsgrid database."""
        client = ArangoClient(hosts=f"http://{conn.hostname}:{conn.port}")
        sys_db = client.db("_system", username=conn.username, password=conn.password)
        if sys_db.has_database(conn.database):
            sys_db.delete_database(conn.database)
            logger.info("Deleted the database %s", conn.database)

    def insert_edge(self, from_id, to_id, data, edge: Edge):
        self._graph.edge_collection(edge.value).insert(
            {
                "_from": from_id,
                "_to": to_id,
                **data,
            }
        )

    def insert_updated_to_edge(self, from_, to, registration: RegistrationModel):
        self.insert_edge(from_.id, to.id, registration.serialize(), Edge.UPDATED_TO)

    def collection(self, name):
        return self._client.collection(name)

    @property
    def client(self):
        return self._client

    def delete_document(self, collection_name, db_id):
        self._client.collection(collection_name).delete(db_id)

    def delete_latest_edge(self, root):
        collection = self._graph.edge_collection(Edge.LATEST.value)
        edges = list(collection.find({"_from": root.id}))
        assert len(edges) == 1, edges
        self._graph.edge_collection(Edge.LATEST.value).delete(edges[0])

    def get_data_path(self) -> Path:
        """Return the path where dataset data is stored."""
        vals = [x for x in self._client.collection("data_path")]
        assert len(vals) == 1, vals
        return Path(vals[0]["data_path"])

    def get_latest(self, root_db_id):
        cursor = self._client.aql.execute(
            f"""
            FOR v in 1
                OUTBOUND "{root_db_id}"
                GRAPH "{GRAPH}"
                OPTIONS {{edgeCollections: ["{Edge.LATEST.value}"]}}
                RETURN v
        """,
            count=True,
        )
        count = cursor.count()
        assert count <= 1, f"{root_db_id=} {count=}"
        if count == 0:
            raise DSGValueNotRegistered(f"{root_db_id=} is not registered")
        doc = cursor.next()
        if doc is None:
            raise DSGValueNotRegistered(f"{root_db_id=} is not registered")
        return doc

    def get_latest_version(self, root_db_id):
        cursor = self._client.aql.execute(
            f"""
            FOR v, e, p in 1
                OUTBOUND "{root_db_id}"
                GRAPH "{GRAPH}"
                OPTIONS {{edgeCollections: ["{Edge.LATEST.value}"]}}
                RETURN p.vertices[1]._id
        """,
            count=True,
        )
        count = cursor.count()
        assert count <= 1, f"{root_db_id=} {count=}"
        if count == 0:
            raise DSGValueNotRegistered(f"{root_db_id=} is not registered")
        model_db_id = cursor.next()
        if model_db_id is None:
            raise DSGValueNotRegistered(f"{root_db_id=} is not registered")
        return self.get_version(model_db_id)

    def _get_by_version(self, db_id, version):
        cursor = self._client.aql.execute(
            f"""
            FOR v, e, p in 0..{MAX_CONFIG_VERSIONS}
                OUTBOUND "{db_id}"
                GRAPH "{GRAPH}"
                OPTIONS {{edgeCollections: ["{Edge.UPDATED_TO.value}"]}}
                FILTER e.version == "{version}"
                RETURN p.vertices[-1]
        """,
            count=True,
        )
        count = cursor.count()
        assert count <= 1, f"{db_id=} {count=}"
        if count == 0:
            raise DSGValueNotRegistered(f"{db_id=} {version=} is not registered")
        return cursor.next()

    def get_registration(self, db_id) -> RegistrationModel:
        """Return the registration information for the id."""
        cursor = self._client.aql.execute(
            f"""
            FOR v, e in 1
                INBOUND "{db_id}"
                GRAPH "{GRAPH}"
                OPTIONS {{edgeCollections: ["{Edge.UPDATED_TO.value}"]}}
                RETURN {{version: e.version, submitter: e.submitter, date: e.date,
                        log_message: e.log_message}}
        """,
            count=True,
        )
        count = cursor.count()
        assert count <= 1, f"{db_id=} {count=}"
        if count == 0:
            raise DSGValueNotRegistered(f"{db_id=} is not registered")

        return RegistrationModel(**cursor.next())

    def get_initial_registration(self, db_id) -> RegistrationModel:
        """Return the initial registration information for the ID."""
        cursor = self._client.aql.execute(
            f"""
            FOR v, e in 1
                OUTBOUND "{db_id}"
                GRAPH "{GRAPH}"
                OPTIONS {{edgeCollections: ["{Edge.UPDATED_TO.value}"]}}
                RETURN {{version: e.version, submitter: e.submitter, date: e.date,
                        log_message: e.log_message}}
        """,
            count=True,
        )
        count = cursor.count()
        assert count <= 1, f"{db_id=} {count=}"
        if count == 0:
            raise DSGValueNotRegistered(f"{db_id=} is not registered")

        return RegistrationModel(**cursor.next())

    def get_version(self, db_id):
        """Return the dsgrid version of the database document.

        Parameters
        ----------
        db_id : str
            Database _id field of the document.

        Returns
        -------
        str
        """
        cursor = self._client.aql.execute(
            f"""
            FOR v, e in 1
                INBOUND "{db_id}"
                GRAPH "{GRAPH}"
                OPTIONS {{edgeCollections: ["{Edge.UPDATED_TO.value}"]}}
                RETURN e.version
        """,
            count=True,
        )
        count = cursor.count()
        assert count <= 1, f"{db_id=} {count=}"
        if count == 0:
            raise DSGValueNotRegistered(f"{db_id=} is not registered")
        return cursor.next()

    def has(self, db_id, version=None):
        collection_name, key = db_id.split("/")
        if version is None:
            return self._client.collection(collection_name).has(key)

        cursor = self._client.aql.execute(
            f"""
            FOR v, e, p in 0..{MAX_CONFIG_VERSIONS}
                OUTBOUND "{db_id}"
                GRAPH "{GRAPH}"
                OPTIONS {{edgeCollections: ["{Edge.UPDATED_TO.value}"]}}
                FILTER e.version == "{version}"
                RETURN p.vertices[1]
        """,
            count=True,
        )
        return cursor.count() > 0

    def insert_contains_edge(self, db_id1, db_id2):
        self._graph.edge_collection(Edge.CONTAINS.value).insert(
            {
                "_from": db_id1,
                "_to": db_id2,
            }
        )

    def insert_latest_edge(self, root, model):
        self._graph.edge_collection(Edge.LATEST.value).insert(
            {
                "_from": root.id,
                "_to": model.id,
            }
        )

    def iter_filtered_documents(self, collection, filter_config: dict):
        """Return a generator of documents from a collection based on a filter."""
        for doc in self._client.collection(collection).find(filter_config):
            yield doc

    def iter_updated_to_documents(self, db_id):
        """Return a generator of database documents connected by the 'updated_to' edge."""
        cursor = self._client.aql.execute(
            f"""
            FOR v, e, p in 1..{MAX_CONFIG_VERSIONS}
                OUTBOUND "{db_id}"
                GRAPH "{GRAPH}"
                OPTIONS {{edgeCollections: ["{Edge.UPDATED_TO.value}"]}}
                RETURN p.vertices[-1]
        """,
            count=True,
        )
        count = cursor.count()
        if count == 0:
            raise DSGValueNotRegistered(f"{db_id=} is not registered")

        for doc in cursor:
            yield doc

    def list_connected_ids(self, db_id, edge: Edge, depth):
        """Return a list of database objects linked to db_id by edge."""
        cursor = self._client.aql.execute(
            f"""
            FOR v in 1..{depth}
                OUTBOUND "{db_id}"
                GRAPH "{GRAPH}"
                OPTIONS {{edgeCollections: ["{edge.value}"]}}
                RETURN v._id
        """,
            count=True,
        )
        count = cursor.count()
        if count == 0:
            raise DSGValueNotRegistered(f"{db_id=} is not registered")
        return list(cursor)

    def replace_document(self, doc):
        return self._client.replace_document(doc)


if __name__ == "__main__":
    conn = DatabaseConnection(database="test-dsgrid")
    # RegistryDatabase.delete(conn)
    # client = RegistryDatabase.create(conn, Path("."))
    client = RegistryDatabase.connect(conn)
    # src_conn = DatabaseConnection(database="test-dsgrid")
    # dst_conn = DatabaseConnection(database="test-dsgrid2")
    # RegistryDatabase.delete(dst_conn)
    # client = RegistryDatabase.copy(src_conn, dst_conn, Path("reg2"))
