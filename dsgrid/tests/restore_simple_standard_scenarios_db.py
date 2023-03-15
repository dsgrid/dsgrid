"""Workaround for restoring the simple-standard-scenarios database in GitHub Actions"""

import json
import re
from pathlib import Path

from dsgrid.registry.common import Edge
from dsgrid.registry.registry_database import RegistryDatabase, DatabaseConnection


def restore_simple_standard_scenarios(conn=None):
    dump_path = (
        Path("dsgrid-test-data") / "filtered_registries" / "simple_standard_scenarios" / "dump"
    )
    conn = conn or DatabaseConnection(database="simple-standard-scenarios")
    data_path = Path("dsgrid-test-data") / "filtered_registries" / "simple_standard_scenarios"
    RegistryDatabase.delete(conn)
    regex = re.compile(r"([a-z_]+)_[a-z0-9]+\.data.json")
    client = RegistryDatabase.create(conn, data_path)
    skip = {"ENCRYPTION", "dump.json"}
    edge_names = {x.value for x in Edge}
    edge_files = []
    for path in dump_path.iterdir():
        if path.name.startswith("_") or path.name in skip or path.name.endswith("structure.json"):
            continue
        match = regex.search(path.name)
        collection = match.group(1)
        if collection in edge_names:
            edge_files.append((path, collection))
            continue
        if collection in ("dimension_types", "data_path"):
            continue
        assert match
        _insert_collection(client, path, collection)

    for path, collection in edge_files:
        _insert_collection(client, path, collection)


def _insert_collection(client, path: Path, collection: str):
    with open(path) as f:
        for line in f:
            doc = json.loads(line.strip())
            client.collection(collection).insert(doc)


if __name__ == "__main__":
    restore_simple_standard_scenarios()
