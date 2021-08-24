import boto3
import getpass
import os
import pysftp
import s3fs
import shutil
import sshtunnel
import time
import webbrowser
import yaml
import git
import paramiko


def launchemr(name=None):

    if name is None:
        name = f"dsgrid-SparkEMR ({getpass.getuser()})"

    here = os.path.dirname(os.path.abspath(__file__))

    with open(os.path.join(here, "emr_config.yml"), "r") as f:
        cfg = yaml.safe_load(f)

    cluster_id_filename = os.path.join(here, "running_cluster_id.txt")

    # this is moving the bootstrap-dask file to S3
    fs = s3fs.S3FileSystem()
    s3_scratch = cfg["s3_scratch"].strip().rstrip("/")
    bootstrap_script_loc = f"{s3_scratch}/bootstrap-pyspark"
    local_bootstrap_pyspark = os.path.join(here, "bootstrap-pyspark")
    fs.put(local_bootstrap_pyspark, bootstrap_script_loc)

    emr = boto3.client("emr")

    if os.path.exists(cluster_id_filename):
        with open(cluster_id_filename, "rt") as f:
            job_flow_id = f.read()
    else:
        resp = emr.run_job_flow(
            Name=f"{getpass.getuser()}-dsgrid",
            LogUri=f"{s3_scratch}/emrlogs/",
            ReleaseLabel="emr-5.29.0",
            Instances={
                "InstanceGroups": [
                    {
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": cfg.get("master_instance", {}).get(
                            "type"
                        ),
                        "InstanceCount": 1,
                    },
                    {
                        "Market": "ON_DEMAND",
                        "InstanceRole": "CORE",
                        "InstanceType": cfg.get("core_instances", {}).get(
                            "type"
                        ),
                        "InstanceCount": cfg.get("core_instances", {}).get(
                            "count"
                        ),
                    },
                ],
                "Ec2KeyName": cfg["ssh_keys"]["key_name"],
                "KeepJobFlowAliveWhenNoSteps": True,
                "Ec2SubnetId": cfg.get("subnet_id"),
            },
            Applications=[{"Name": "Spark"}],
            BootstrapActions=[
                {
                    "Name": "launchFromS3",
                    "ScriptBootstrapAction": {
                        "Path": bootstrap_script_loc,
                        "Args": [],
                    },
                },
            ],
            VisibleToAllUsers=True,
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
    print(f"Job Flow ID: {job_flow_id}")

    while True:
        resp = emr.describe_cluster(ClusterId=job_flow_id)
        state = resp["Cluster"]["Status"]["State"]
        message = resp["Cluster"]["Status"]["StateChangeReason"].get(
            "Message", "(no message)"
        )
        print(f"Cluster Status: {state} - {message}")
        if state in ["WAITING", "TERMINATED", "TERMINATED_WITH_ERRORS"]:
            break
        time.sleep(30)

    master_instance = emr.list_instances(
        ClusterId=job_flow_id, InstanceGroupTypes=["MASTER"]
    )
    ip_address = master_instance.get("Instances")[0].get("PrivateIpAddress")
    print(f"Connecting to {ip_address}")

    mypkey = os.path.abspath(
        os.path.expanduser(cfg["ssh_keys"]["pkey_location"])
    )

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    print("Copying AWS config files")
    aws_credentials = os.path.join(here, "credentials")
    with pysftp.Connection(
        ip_address, username="hadoop", private_key=mypkey, cnopts=cnopts
    ) as sftp:
        sftp.makedirs("code")
        sftp.put_r(aws_credentials, "/home/hadoop/.aws")

    print("Cloning latest ds-grid code")
    ds_grid_repo_path = os.path.join(here, "code/dsgrid")
    if os.path.exists(ds_grid_repo_path):
        shutil.rmtree(ds_grid_repo_path)
        os.makedirs(ds_grid_repo_path)

    dsgrid_repo = git.Repo.clone_from(
        "https://github.com/dsgrid/dsgrid.git",
        ds_grid_repo_path,
        branch=str(cfg.get("repo_branch")),
    )

    print("Copying notebooks and code to master node")
    with pysftp.Connection(
        ip_address, username="hadoop", private_key=mypkey, cnopts=cnopts
    ) as sftp:
        sftp.makedirs("code")
        sftp.put_r(os.path.join(here, "code"), "code")

    print("Installing DS-Grid")
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(ip_address, username="hadoop", key_filename=mypkey)
    command = "cd ~/code/dsgrid && pip install -e ."
    stdin, stdout, stderr = ssh_client.exec_command(command)
    print(stdout.readlines())
    ssh_client.close()

    print("Opening tunnel to jupyter notebook server")
    tunnel = sshtunnel.SSHTunnelForwarder(
        ssh_address_or_host=ip_address,
        ssh_username="hadoop",
        ssh_pkey=mypkey,
        remote_bind_address=("127.0.0.1", 8888),
    )
    tunnel.daemon_forward_servers = True
    tunnel.start()

    jupyter_url = (
        f"Jupyter Notebook URL: http://localhost:{tunnel.local_bind_port}"
    )
    print(jupyter_url)
    print("Password is pyspark-user")
    print("press Ctrl+C to quit")
    webbrowser.open_new_tab(jupyter_url)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Caught Ctrl+C, shutting down tunnel, please wait")

    tunnel.stop()

    print("Copying notebooks back to local machine")
    shutil.rmtree(os.path.join(here, "code"))
    with pysftp.Connection(
        ip_address, username="hadoop", private_key=mypkey, cnopts=cnopts
    ) as sftp:
        sftp.get_r("code", here)

    resp = input("Terminate cluster [y/n]? ")
    if resp.lower().startswith("y"):
        print(f"Terminating cluster {job_flow_id}")
        emr.terminate_job_flows(JobFlowIds=[job_flow_id])
        os.remove(cluster_id_filename)
        if not resp.lower().endswith("k"):
            fs.rm(s3_scratch, recursive=True)

if __name__ == "__main__":
    launchemr()
