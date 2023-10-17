from pathlib import Path
import os

AWS_PROFILE_NAME = "nrel-aws-dsgrid"
REMOTE_REGISTRY = "s3://nrel-dsgrid-registry"


def on_hpc():
    # NREL_CLUSTER is not set when you ssh into a compute node.
    return "NREL_CLUSTER" in os.environ or "SLURM_JOB_ID" in os.environ


if on_hpc():
    LOCAL_REGISTRY = Path("/scratch") / os.environ["USER"] / ".dsgrid-registry"
else:
    LOCAL_REGISTRY = Path.home() / ".dsgrid-registry"

LOCAL_REGISTRY_DATA = LOCAL_REGISTRY / "data"
PROJECT_FILENAME = "project.json5"
REGISTRY_FILENAME = "registry.json5"
DATASET_FILENAME = "dataset.json5"
DIMENSIONS_FILENAME = "dimensions.json5"
DEFAULT_DB_PASSWORD = "openSesame"

SYNC_EXCLUDE_LIST = ["*.DS_Store", "**/*.lock"]
