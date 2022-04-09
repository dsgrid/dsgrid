from pathlib import Path
import os

AWS_PROFILE_NAME = "nrel-aws-dsgrid"
REMOTE_REGISTRY = "s3://nrel-dsgrid-registry"

if os.environ.get("NREL_CLUSTER") is not None:
    LOCAL_REGISTRY = Path("/scratch") / os.environ["USER"] / ".dsgrid-registry"
else:
    LOCAL_REGISTRY = Path.home() / ".dsgrid-registry"

LOCAL_REGISTRY_DATA = LOCAL_REGISTRY / "data"
PROJECT_FILENAME = "project.toml"
REGISTRY_FILENAME = "registry.toml"
DATASET_FILENAME = "dataset.toml"
DIMENSIONS_FILENAME = "dimensions.toml"

SYNC_EXCLUDE_LIST = ["*.DS_Store", "**/*.lock"]
