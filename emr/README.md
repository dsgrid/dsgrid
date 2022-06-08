# EMR for ds-grid

## Setup
create a conda environment for launching AWS Elastic Map Reduce Spark cluster.
```
cd path_to/dsgrid/emr/
conda env create -f environment.yml
conda activate daskemr
```

The `launch_emr.py` script can be used launch an EMR cluster under the ds-grid account.
It should take about 10 minutes to initialize the cluster for the first time.
The `emr_config.yml` file holds all the required configurations to launch new clusters. The most notable option is the `ssh_keys` which is required to open a ssh-tunnel between EMR and localhost. This key can be obtained [here](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#KeyPairs:).

## AWS Credentials
You will also need a `credentials` folder containing a `config` and `credentials` file with the proper aws keys under a `nrel-aws-dsgrid` profile.

## Usage

### Config file
All relevant config info is stored on the `emr_config.yml` file.
The only parts that need to be edited by the user are:

Cluster Size:
```
master_instance:
  type: m5.2xlarge
core_instances:
  count: 2
  type: r5.2xlarge
```

Private SSH keys to ensure connectivity
```
ssh_keys:
  pkey_location: /Users/roliveir/.ssh/dsgrid-key.pem # The path to your private key file used with AWS.
  key_name: dsgrid-key # The name of the permission key file on AWS.
```

DS-Grid branch to clone and install
```
repo_branch: main
```

Once the cluster is running and ssh-tunnel is set-up you can access the system through Jupyter (available on http://localhost:53644) or sshing into the Master node. The IP for the master node gets printed during the initialization process. For example: `ssh -i "dsgrid-key.pem" hadoop@172.18.27.112`
