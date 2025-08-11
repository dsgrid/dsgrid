import boto3
from pathlib import Path
import getpass
import os
import pysftp
import s3fs
import shutil
import sshtunnel
import time
import webbrowser
import yaml
from datetime import datetime
import tempfile
import subprocess
import sys
import argparse
import logging


def build_package():
    """Build a distributable package of the library defined by the setup.py file in the parent directory
    :return: path to package
    :rtype: pathlib.Path
    """
    pkgdir = Path(__file__).resolve().parents[1]

    subprocess.run(
        [sys.executable, "setup.py", "sdist"],
        cwd=pkgdir,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return sorted((pkgdir / "dist").glob("*.tar.gz"), key=os.path.getmtime, reverse=True)[0]


def launchemr(dir_to_sync=None, name=None):
    if name is None:
        name = f"dsgrid-SparkEMR ({getpass.getuser()})"

    here = Path(__file__).resolve().parent
    with open(here / "emr_config.yml", "r") as f:
        cfg = yaml.safe_load(f)

    cluster_id_filename = here / "running_cluster_id.txt"

    # Get AWS credentials
    profile_name = cfg.get("profile", "default")
    region = cfg.get("region", "us-west-2")  # incompatible with existing subnet_id
    print(f"Using AWS profile:  {profile_name}  with region:  {region}  ")
    session = boto3.Session(profile_name=profile_name, region_name=region)
    credentials = session.get_credentials()

    # Get AWS ssh key
    if cfg.get("ssh_keys") is not None and "key_name" in cfg["ssh_keys"]:
        key_name = cfg["ssh_keys"]["key_name"]
    else:
        key_name = getpass.getuser() + "-dsgrid"

    # Set user scratch folder for cluster
    fs = s3fs.S3FileSystem(key=credentials.access_key, secret=credentials.secret_key)
    s3_scratch = cfg["s3_scratch"].strip().rstrip("/")
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H:%M")

    emr = boto3.client(
        "emr",
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token,
    )

    if cluster_id_filename.exists():
        with open(cluster_id_filename, "rt") as f:
            text = f.read()
            [job_flow_id, s3_scratch_user] = [x.strip() for x in text.split("\n")]
        print(f"Found previously running EMR cluster: {job_flow_id}")
        try:
            resp = emr.describe_cluster(ClusterId=job_flow_id)
            state = resp["Cluster"]["Status"]["State"]
            message = resp["Cluster"]["Status"]["StateChangeReason"].get("Message", "(no message)")
            if state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
                print(f"  EMR cluster is {state}: {message}, REMOVING...")
                os.remove(cluster_id_filename)
                resp = input(
                    f" >> Delete S3 scratch directory for the TERMINATED cluster: {s3_scratch_user} : [y/n]? "
                )
                if resp.lower().endswith("y") and fs.exists(s3_scratch_user):
                    print("Deleting S3 scratch directory...")
                    fs.rm(s3_scratch_user, recursive=True)
            else:
                print(f"  Reconnecting to cluster: {job_flow_id}")

        except (
            emr.exceptions.InternalServerException,
            emr.exceptions.InvalidRequestException,
        ) as e:
            print(f"  CANNOT read EMR cluster {e}, REMOVING...")
            os.remove(cluster_id_filename)
            resp = input(
                f" >> Delete S3 scratch directory for the UNKNOWN cluster: {s3_scratch_user} : [y/n]? "
            )
            if resp.lower().endswith("y") and fs.exists(s3_scratch_user):
                print("Deleting S3 scratch directory...")
                fs.rm(s3_scratch_user, recursive=True)

    if not cluster_id_filename.exists():
        s3_scratch_user = f"{s3_scratch}/{getpass.getuser()}_{timestamp}"
        # Upload bootstrap script
        bootstrap_script_loc = f"{s3_scratch_user}/bootstrap-pyspark"
        local_bootstrap_pyspark = str(here / "bootstrap-pyspark")
        fs.put(local_bootstrap_pyspark, bootstrap_script_loc)

        # Upload dsgrid package
        pkg_to_upload = build_package()
        fs.put(str(pkg_to_upload), f"{s3_scratch_user}/pkg.tar.gz", recursive=False)

        # Run EMR job flow
        # resource: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html
        idle_time_out = min(cfg.get("idle_time_out", 3600), 3600 * 3)  # sec, enforcing max of 3hrs
        resp = emr.run_job_flow(
            Name=f"{getpass.getuser()}-dsgrid",
            LogUri=f"{s3_scratch_user}/emrlogs/",
            ReleaseLabel="emr-6.6.0",
            Instances={
                "InstanceGroups": [
                    {
                        "Market": cfg.get("master_instance", {}).get("market", "ON_DEMAND"),
                        "InstanceRole": "MASTER",
                        "InstanceType": cfg.get("master_instance", {}).get("type", "m5.2xlarge"),
                        "InstanceCount": 1,
                    },
                    {
                        "Market": cfg.get("core_instance", {}).get("market", "SPOT"),
                        "InstanceRole": "CORE",
                        "InstanceType": cfg.get("core_instances", {}).get("type", "c5d.12xlarge"),
                        "InstanceCount": cfg.get("core_instances", {}).get("count", 2),
                    },
                ],
                "Ec2KeyName": key_name,
                "KeepJobFlowAliveWhenNoSteps": True,
                "Ec2SubnetId": cfg.get("subnet_id"),
                "TerminationProtected": False,
                # "AutoTerminate": True,
            },
            Configurations=[
                {
                    "Classification": "yarn-site",
                    "Properties": {
                        "yarn.log-aggregation-enable": "true",
                        "yarn.log-aggregation.retain-seconds": "-1",
                        "yarn.nodemanager.remote-app-log-dir": f"{s3_scratch_user}/yarnlogs",
                    },
                }
            ],
            Applications=[
                {
                    "Name": "Hadoop",
                },
                {
                    "Name": "Spark",
                },
                {
                    "Name": "Livy",
                },
                {
                    "Name": "Hive",
                },
                {
                    "Name": "JupyterEnterpriseGateway",
                },
            ],
            BootstrapActions=[
                {
                    "Name": "launchFromS3",
                    "ScriptBootstrapAction": {
                        "Path": bootstrap_script_loc,
                        "Args": ["--s3scratch", s3_scratch_user],
                    },
                },
            ],
            VisibleToAllUsers=True,
            AutoTerminationPolicy={
                "IdleTimeout": idle_time_out,  # sec
            },
            EbsRootVolumeSize=80,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            Tags=[
                {"Key": "billingId", "Value": str(cfg.get("billing_id"))},
                {"Key": "project", "Value": "dsgrid"},
            ],
        )
        job_flow_id = resp["JobFlowId"]
        with open(cluster_id_filename, "wt") as f:
            f.write(job_flow_id + "\n" + s3_scratch_user)
        time.sleep(5)
        print(f"Started a new cluster: {job_flow_id}")

    while True:
        resp = emr.describe_cluster(ClusterId=job_flow_id)
        state = resp["Cluster"]["Status"]["State"]
        message = resp["Cluster"]["Status"]["StateChangeReason"].get("Message", "(no message)")
        print(f"Cluster Status: {state} - {message}")
        if state == "WAITING":
            break
        elif state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
            print(f"EMR Cluster is {state}", message)
            msg = f"EMR Cluster is {state}: {message}"
            raise RuntimeError(msg)
        time.sleep(30)

    master_instance = emr.list_instances(ClusterId=job_flow_id, InstanceGroupTypes=["MASTER"])
    ip_address = master_instance.get("Instances")[0].get("PublicIpAddress")
    master_address = resp["Cluster"]["MasterPublicDnsName"]
    print(f"Connecting to {master_address} at {ip_address}")

    if cfg.get("ssh_keys") is not None and "pkey_location" in cfg["ssh_keys"]:
        mypkey = os.path.abspath(os.path.expanduser(cfg["ssh_keys"]["pkey_location"]))
    else:
        mypkey = str(Path.home() / ".ssh" / f"{getpass.getuser()}-dsgrid.pem")

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    print("Copying AWS config files")
    aws_credentials = cfg.get("aws_credentials_location", str(Path.home() / ".aws"))
    with pysftp.Connection(
        master_address, username="hadoop", private_key=mypkey, cnopts=cnopts
    ) as sftp:
        sftp.put_r(aws_credentials, "/home/hadoop/.aws")

    if dir_to_sync is None:
        dir_to_sync = here.parent / "dsgrid" / "notebooks"
    else:
        dir_to_sync = Path(dir_to_sync)
    print(f"Copying directory to master node: {dir_to_sync}...")
    with pysftp.Connection(
        master_address, username="hadoop", private_key=mypkey, cnopts=cnopts
    ) as sftp:
        if sftp.exists(dir_to_sync.name):
            sftp.rmdir(dir_to_sync.name)
        else:
            sftp.makedirs(dir_to_sync.name)
        sftp.put_r(str(dir_to_sync), dir_to_sync.name)

    print("Opening tunnel to jupyter notebook server")
    tunnel = sshtunnel.SSHTunnelForwarder(
        ssh_address_or_host=master_address,
        ssh_username="hadoop",
        ssh_pkey=mypkey,
        remote_bind_address=("127.0.0.1", 8888),
    )
    tunnel.daemon_forward_servers = True
    tunnel.start()

    jupyter_url = f"Jupyter Notebook URL: http://localhost:{tunnel.local_bind_port}"
    print(f"\n{jupyter_url}")
    print("  Password is dsgrid")
    print("  Press Ctrl+C to quit\n")
    print(f"  To ssh into the master node: ssh -i {mypkey} hadoop@{ip_address}\n")
    try:
        webbrowser.open_new_tab(jupyter_url)
    except Exception:
        logging.exception(
            "Failed to open a web brower tab. "
            "Try copying the Jupyter Notebook URL directly into a web browser."
        )

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Caught Ctrl+C, shutting down tunnel, please wait")

    tunnel.stop()

    print(f"Copying directory back to local machine: {dir_to_sync}...")
    with tempfile.TemporaryDirectory() as tmpdir:
        with pysftp.Connection(
            master_address, username="hadoop", private_key=mypkey, cnopts=cnopts
        ) as sftp:
            sftp.get_r(dir_to_sync.name, tmpdir)
        shutil.rmtree(dir_to_sync)
        shutil.copytree(os.path.join(tmpdir, dir_to_sync.name), str(dir_to_sync))

    resp = input(" >> Terminate cluster [y/n]? ")
    if resp.lower().startswith("y"):
        print(f"Terminating cluster {job_flow_id} ...")
        emr.terminate_job_flows(JobFlowIds=[job_flow_id])
        wait = True

        # Give time for cluster to shut down fully
        while wait:
            state = sleep_loop_for_cluster_shutdown(
                emr, job_flow_id, n_attempts=12, sleep_interval=10
            )
            if state != "TERMINATED":
                resp = input(
                    f" >> Cluster: {job_flow_id} could not be terminated after 2 min, "
                    "wait some more? [y/n]? "
                )
                if resp.lower().startswith("y"):
                    continue
                else:
                    print("Try manually shut down the cluster on the AWS portal.")
            wait = False

        # Delete s3 scratch
        if state == "TERMINATED":
            os.remove(cluster_id_filename)
            resp2 = input(f" >> Delete S3 scratch directory: {s3_scratch_user} : [y/n]? ")
            if resp2.lower().endswith("y"):
                print("Deleting S3 scratch directory...")
                fs.rm(s3_scratch_user, recursive=True)


def sleep_loop_for_cluster_shutdown(emr, job_flow_id, n_attempts=18, sleep_interval=10):
    attempt = 0
    state = emr.describe_cluster(ClusterId=job_flow_id)["Cluster"]["Status"]["State"]

    while state != "TERMINATED":
        attempt += 1
        time.sleep(sleep_interval)
        state = emr.describe_cluster(ClusterId=job_flow_id)["Cluster"]["Status"]["State"]
        print(f"Cluster Status: {state}")
        if attempt == n_attempts:
            break

    return state


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--dir_to_sync",
        help="Path of directory to sync to AWS Hadoop filesystem for Jupyter, defaults to: ./dsgrid/notebooks",
    )
    parser.add_argument(
        "-n",
        "--name",
        help="Name of AWS EMR session to display, defaults to: dsgrid-SparkEMR <USER>",
    )
    args = parser.parse_args()
    launchemr(dir_to_sync=args.dir_to_sync, name=args.name)
