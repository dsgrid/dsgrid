# EMR for dsgrid

## Setup

Create a conda environment for launching AWS Elastic Map Reduce Spark cluster:
```bash
cd path_to/dsgrid/emr/
conda env create -f environment.yml
conda activate dsgrid-emr
```

The `launch_emr.py` script can be used launch an EMR cluster under the dsgrid account. It should take about 10 minutes to initialize the cluster for the first time. The `emr_config.yml` file holds all the configurations required to launch new clusters. For the purpose of getting started, the most notable option is `ssh_keys` which is required to open a ssh-tunnel between EMR and localhost. This key can be obtained [here](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#KeyPairs:). If you name your key `<USER>-dsgrid`, choose an RSA key pair type, create a .pem private key file, and place the .pem file in the `~/.ssh` folder then you should not have to change the `emr_config.yml` file to spin up a basic Spark cluster.

## AWS Credentials
You will also need a `credentials` folder containing a `config` and `credentials` file with the proper aws keys under a `nrel-aws-dsgrid` profile.

## Usage

### Config file
All relevant config info is stored on the `emr_config.yml` file.

The only parts that need to be edited by the user are:

Cluster Size:
```yml
master_instance:
  type: m5.2xlarge
  market: ON_DEMAND
core_instances:
  count: 2
  type: r5.2xlarge #c5d.12xlarge
  market: SPOT
```
You can find more information about EC2 instance types at [EC2Instances.info] (https://ec2instances.info/).

Private SSH keys to ensure connectivity. The default option is to leave ssh_keys blank:
```yml
ssh_keys:
  # The default behavior uses ~/.ssh/<USER>-dsgrid.pem and <USER>-dsgrid.
  # Change these values if you use different settings.
  # pkey_location: ~/.ssh/<USER>-dsgrid.pem # The path to your private key file used with AWS.
  # key_name: <USER>-dsgrid # The name of the permission key file on AWS.
```

If you choose to specify a different name and/or file location follow this pattern:
```yml
ssh_keys:
  pkey_location: /Users/roliveir/.ssh/dsgrid-key.pem # The path to your private key file used with AWS.
  key_name: dsgrid-key # The name of the permission key file on AWS.
```

dsgrid branch to clone and install
```yml
repo_branch: main
```

### Running

To start up the cluster, run the `launch_emr.py` script from the `dsgrid/emr` folder using the `dsgrid-emr` conda environment you created:
```bash
(dsgrid-emr) [$USER@COMPUTER ~/dsgrid/emr]$ python launch_emr.py
```

Once the cluster is running and the ssh-tunnel is set-up you can access the system through Jupyter or by ssh-ing into the Master node. The Jupyter address and the IP for the master node get printed during the initialization process. For example the text printed during start up looks like: 
```bash
copying tests_aws/test_registry/test_data_sync.py -> dsgrid-0.1.0/tests_aws/test_registry
copying tests_aws/test_registry/test_push_pull_online_mode.py -> dsgrid-0.1.0/tests_aws/test_registry
copying tests_aws/test_registry/tests_s3_locks.py -> dsgrid-0.1.0/tests_aws/test_registry
Writing dsgrid-0.1.0/setup.cfg
Creating tar archive
removing 'dsgrid-0.1.0' (and everything under it)
Started a new cluster: j-2Y17R69QY2BKZ
Cluster Status: STARTING - (no message)
Cluster Status: STARTING - (no message)
Cluster Status: STARTING - (no message)
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: BOOTSTRAPPING - Running bootstrap actions
Cluster Status: WAITING - Cluster ready to run steps.
Connecting to ec2-54-184-112-221.us-west-2.compute.amazonaws.com at 54.184.112.221
Copying AWS config files
Copying notebooks to master node, from: /home/ehale/dsgrid/dsgrid/notebooks...
Opening tunnel to jupyter notebook server

Jupyter Notebook URL: http://localhost:43431
  Password is dsgrid
  Press Ctrl+C to quit

  To ssh into the master node: ssh -i /home/ehale/.ssh/ehale-dsgrid.pem hadoop@54.184.112.221
```
and based on this the user can login to Jupyter by putting `http://localhost:43431` in the address bar of her web browser or can ssh to the Master node via:
```bash
ssh -i /home/ehale/.ssh/ehale-dsgrid.pem hadoop@54.184.112.221
```

In either case, create a SparkSession attached to the cluster by entering this code block in a Jupyter cell or by running these lines in an ipython session or by including them in a script:
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("app").getOrCreate()
```

### Shutting down

If you are not working in the default Jupyter dsgrid_notebooks folder you might want to save your work back to your local machine before shutting the cluster down. 

To shut the cluster down, simply Ctrl-C out of the local python process you used to start the cluster. You will see text like:
```
Caught Ctrl+C, shutting down tunnel, please wait
Copying notebooks back to local machine: /home/ehale/dsgrid/dsgrid/notebooks...
Terminate cluster [y/n]? y
Terminating cluster j-2Y17R69QY2BKZ
```

You can also see what instances you have running (and manually shut them down, if so desired) by going to the [EC2 management](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#Home) page of the AWS console. The EC2 instances will also shut down automatically after 1 hour of idle time.