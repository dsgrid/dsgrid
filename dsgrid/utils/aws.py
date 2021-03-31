"""Contains utility functions to interact with AWS."""

import logging
import time

import boto3

from dsgrid.utils.run_command import check_run_command


logger = logging.getLogger(__name__)


def exists(path):
    # TODO S3: implement with boto3
    return False


def sync(src, dst):
    """Syncs a path on AWS with a local path or vice versa.

    Parameters
    ----------
    src : str
        AWS or local path
    dst : str
        AWS or local path

    Raises
    ------
    DSGRuntimeError
        Raised if the aws sync command returns an error.

    """
    start = time.time()
    sync_command = f"aws s3 sync {src} {dst}"
    logger.info("Running %s", sync_command)
    try:
        check_run_command(sync_command)
        logger.info("Command took %s seconds", time.time() - start)
    except:
        logger.error(
            "Syncing with AWS failed. You may need to run 'aws configure' " "to point to sdi."
        )
        raise


def list_dir(path):
    # TODO: split bucket name and path from the input path
    bucket = "nrel-dsgrid-scratch"
    session = boto3.session.Session()
    client = session.client("s3")
    result = client.list_objects_v2(Bucket=bucket, Prefix=path)
    if result["IsTruncated"]:
        raise Exception(f"Received truncated result when listing {path}. Need to improve logic.")
    return [x["Key"] for x in result["Contents"]]


def list_dir_in_bucket(bucket, path):
    # TODO: split bucket name and path from the input path
    session = boto3.session.Session()
    client = session.client("s3")
    result = client.list_objects_v2(Bucket=bucket, Prefix=path)
    if result["IsTruncated"]:
        raise Exception(f"Received truncated result when listing {path}. Need to improve logic.")
    return [x["Key"] for x in result["Contents"]]


def make_dirs(path, exist_ok=True):
    # TODO make dirs with boto3
    pass
