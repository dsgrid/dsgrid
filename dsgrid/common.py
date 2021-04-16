from pathlib import Path

AWS_PROFILE_NAME = "nrel-aws-dsgrid"
S3_REGISTRY = "s3://nrel-dsgrid-registry"
LOCAL_REGISTRY = Path.home() / ".dsgrid-registry"
LOCAL_REGISTRY_DATA = Path.home() / ".dsgrid-registry" / "data"
PROJECT_FILENAME = "project.toml"
REGISTRY_FILENAME = "registry.toml"
DATASET_FILENAME = "dataset.toml"
DIMENSIONS_FILENAME = "dimensions.toml"
