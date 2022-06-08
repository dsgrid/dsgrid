from dsgrid.dimension.base_models import DimensionType


def create_empty_remote_registry(s3_filesystem):
    """Create fresh remote registry for testing that includes all base dirs"""
    assert (
        s3_filesystem._bucket == "nrel-dsgrid-registry-test"
    ), "Remote registry tests cannot be performed on a non-test bucket"
    contents = s3_filesystem.rglob(s3_filesystem._bucket)
    assert len(contents) == 0, "remote registry is not empty"
    for folder in (
        "configs/datasets",
        "configs/projects",
        "configs/dimensions",
        "configs/dimension_mappings",
        "data",
    ):
        s3_filesystem.mkdir(f"{folder}")
    for dim_type in DimensionType:
        s3_filesystem.mkdir(f"configs/dimensions/{dim_type.value}")
    assert s3_filesystem.listdir() == ["configs", "data"]


def clean_remote_registry(s3_filesystem, prefix=""):
    """Hard delete all object versions and markers in the versioned s3 test registry bucket; add root folders"""
    assert (
        s3_filesystem._bucket == "nrel-dsgrid-registry-test"
    ), "Remote registry tests cannot be performed on a non-test bucket"
    s3 = s3_filesystem._client
    bucket = s3_filesystem._bucket
    for i in range(2):
        versions = s3.list_object_versions(Bucket=bucket, Prefix=prefix)
        keys = [
            k for k in versions.keys() if k in ("Versions", "VersionIdMarker", "DeleteMarkers")
        ]
        for key in keys:
            for v in versions[key]:
                if key != prefix:
                    s3.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
    assert s3_filesystem.listdir() == []
