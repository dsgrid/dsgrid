from pathlib import Path

S3_REGISTRY = "s3://nrel-dsgrid-scratch/dsgrid_v2.0.0/registry"
LOCAL_REGISTRY = Path.home() / ".dsgrid-registry"
LOCAL_REGISTRY_DATA = Path.home() / ".dsgrid-registry" / "data"
PROJECT_FILENAME = "project.toml"
REGISTRY_FILENAME = "registry.toml"
DATASET_FILENAME = "dataset.toml"
DIMENSION_FILENAME = "dimension.toml"
