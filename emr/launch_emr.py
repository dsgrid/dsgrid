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

# import git
# import paramiko
import tempfile
import subprocess
import sys


def build_package():
    """Build a distributable package of the library defined by the setup.py file in the parent directory
    :return: path to package
    :rtype: pathlib.Path
    """
    pkgdir = Path(__file__).resolve().parent.parent

    subprocess.run([sys.executable, "setup.py", "sdist"], cwd=pkgdir, check=True)

    return sorted(
        (pkgdir / "dist").glob("*.tar.gz"), key=os.path.getmtime, reverse=True
    )[0]


def launchemr(name=None):

    if name is None:
        name = f"dsgrid-SparkEMR ({getpass.getuser()})"

    here = Path(__file__).resolve().parent
    with open(here / "emr_config.yml", "r") as f:
        cfg = yaml.safe_load(f)

    cluster_id_filename = here / "running_cluster_id.txt"

    # this is moving the bootstrap-dask file to S3
    profile_name = cfg.get("profile", "default")
    region = cfg.get("region", "us-west-2")  # incompatible with existing subnet_id
    session = boto3.Session(profile_name=profile_name, region_name=region)
    credentials = session.get_credentials()

    fs = s3fs.S3FileSystem(key=credentials.access_key, secret=credentials.secret_key)
    s3_scratch = cfg["s3_scratch"].strip().rstrip("/")
    bootstrap_script_loc = f"{s3_scratch}/bootstrap-pyspark"
    local_bootstrap_pyspark = str(here / "bootstrap-pyspark")
    fs.put(local_bootstrap_pyspark, bootstrap_script_loc)

    # Upload parent directory package
    pkg_to_upload = build_package()
    fs.put(str(pkg_to_upload), f"{s3_scratch}/pkg.tar.gz", recursive=False)

    emr = boto3.client("emr")

    if cluster_id_filename.exists():
        with open(cluster_id_filename, "rt") as f:
            job_flow_id = f.read()
        print(f"Found previously running EMR cluster: {job_flow_id}")
        try:
            resp = emr.describe_cluster(ClusterId=job_flow_id)
            state = resp["Cluster"]["Status"]["State"]
            message = resp["Cluster"]["Status"]["StateChangeReason"].get(
                "Message", "(no message)"
            )
            if state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
                print(f"  EMR cluster is {state}: {message}, REMOVING...")
                os.remove(cluster_id_filename)
            else:
                print(f"  Reconnecting to cluster: {job_flow_id}")
        except Exception as e:
            print(f"  CANNOT read EMR cluster: {e}, REMOVING...")
            os.remove(cluster_id_filename)

    if cfg.get("ssh_keys") is not None and "key_name" in cfg["ssh_keys"]:
        key_name = cfg["ssh_keys"]["key_name"]
    else:
        key_name = os.environ["USER"] + "-dsgrid"
    if not cluster_id_filename.exists():
        # resource: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html
        resp = emr.run_job_flow(
            Name=f"{getpass.getuser()}-dsgrid",
            LogUri=f"{s3_scratch}/emrlogs/",
            ReleaseLabel="emr-6.6.0",
            Instances={
                "InstanceGroups": [
                    {
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": cfg.get("master_instance", {}).get(
                            "type", "m5.2xlarge"
                        ),
                        "InstanceCount": 1,
                    },
                    {
                        "Market": "ON_DEMAND",  # <-- can be changed to "SPOT"
                        "InstanceRole": "CORE",
                        "InstanceType": cfg.get("core_instances", {}).get(
                            "type", "r5.12xlarge"
                        ),
                        "InstanceCount": cfg.get("core_instances", {}).get("count", 2),
                    },
                ],
                "Ec2KeyName": key_name,
                "KeepJobFlowAliveWhenNoSteps": True,
                "Ec2SubnetId": cfg.get("subnet_id"),
                "TerminationProtected": False,
                # "AutoTerminate": True,
            },
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
                        "Args": ["--s3scratch", s3_scratch],
                    },
                },
            ],
            VisibleToAllUsers=True,
            AutoTerminationPolicy={
                "IdleTimeout": 7200,  # sec
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
            f.write(job_flow_id)
        time.sleep(5)
        print(f"Started a new cluster: {job_flow_id}")

    while True:
        resp = emr.describe_cluster(ClusterId=job_flow_id)
        state = resp["Cluster"]["Status"]["State"]
        message = resp["Cluster"]["Status"]["StateChangeReason"].get(
            "Message", "(no message)"
        )
        print(f"Cluster Status: {state} - {message}")
        if state == "WAITING":
            break
        elif state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
            print(f"EMR Cluster is {state}", message)
            os.remove(cluster_id_filename)
            raise RuntimeError(f"EMR Cluster is {state}: {message}")
        time.sleep(30)

    master_instance = emr.list_instances(
        ClusterId=job_flow_id, InstanceGroupTypes=["MASTER"]
    )
    ip_address = master_instance.get("Instances")[0].get("PublicIpAddress")
    master_address = resp["Cluster"]["MasterPublicDnsName"]
    print(f"Connecting to {master_address} at {ip_address}")

    if cfg.get("ssh_keys") is not None and "pkey_location" in cfg["ssh_keys"]:
        mypkey = os.path.abspath(os.path.expanduser(cfg["ssh_keys"]["pkey_location"]))
    else:
        mypkey = str(Path.home() / ".ssh" / f"{os.environ['USER']}-dsgrid.pem")

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    print("Copying AWS config files")
    aws_credentials = here / "credentials"
    with pysftp.Connection(
        master_address, username="hadoop", private_key=mypkey, cnopts=cnopts
    ) as sftp:
        sftp.put_r(aws_credentials, "/home/hadoop/.aws")

    local_notebooks_dir = here.parent / "dsgrid" / "notebooks"
    print(f"Copying notebooks to master node, from: {local_notebooks_dir}...")
    with pysftp.Connection(
        master_address, username="hadoop", private_key=mypkey, cnopts=cnopts
    ) as sftp:
        sftp.makedirs("dsgrid_notebooks")
        sftp.put_r(str(local_notebooks_dir), "dsgrid_notebooks")

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
    webbrowser.open_new_tab(jupyter_url)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Caught Ctrl+C, shutting down tunnel, please wait")

    tunnel.stop()

    print(f"Copying notebooks back to local machine: {local_notebooks_dir}...")
    with tempfile.TemporaryDirectory() as tmpdir:
        with pysftp.Connection(
            master_address, username="hadoop", private_key=mypkey, cnopts=cnopts
        ) as sftp:
            sftp.get_r("dsgrid_notebooks", tmpdir)
        shutil.rmtree(local_notebooks_dir)
        shutil.copytree(
            os.path.join(tmpdir, "dsgrid_notebooks"), str(local_notebooks_dir)
        )

    resp = input("Terminate cluster [y/n]? ")
    if resp.lower().startswith("y"):
        print(f"Terminating cluster {job_flow_id}")
        emr.terminate_job_flows(JobFlowIds=[job_flow_id])
        os.remove(cluster_id_filename)
        if not resp.lower().endswith("k"):
            fs.rm(s3_scratch, recursive=True)


if __name__ == "__main__":
    launchemr()
