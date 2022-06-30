# EMR for dsgrid

## Setup
create a conda environment for launching AWS Elastic Map Reduce (EMR) Spark cluster.
```
cd path_to/dsgrid/emr/
conda env create -f environment.yml
conda activate dsgrid-emr
```

### Overview
The `emr_config.yml` file holds all the required configurations to launch new clusters. An ssh-key is required to open a ssh-tunnel between EMR and localhost. This key can be obtained on the [AWS console](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#KeyPairs:). If you choose an RSA key pair type, create a .pem private key file with the name `<USER>-dsgrid`, and place the .pem file in the `~/.ssh` folder then you should not have to change the `emr_config.yml` file to spin up a basic Spark cluster.

The bootstrap actions are defined in the `bootstrap-pyspark` file and include installing dsgrid and other packages on both master and core/worker instances.

The `launch_emr.py` script can be used to launch an EMR cluster under the `nrel-aws-dsgrid` account.
It should take about 10 minutes to initialize the cluster for the first time. The script copies the `./dsgrid/notebooks` folder or a user-specified folder onto the AWS Hadoop filesystem (see the Running section on how to specify that).


### Config file
The `emr_config.yml` file manages the EC2 instances request in addition to the location of your AWS credentials and ssh key.
To create this file, make a copy of the `emr_config_example.yml` file. The default location and name of the pem key is as follows. Edit the config if your pem key is stored differently.
```
ssh_keys:
  pkey_location: ~/.ssh/<USER>-dsgrid.pem # The path to your private key file used with AWS.
  key_name: <USER>-dsgrid.pem # The name of the permission key file on AWS.
```

As part of the bootstrapping process, your AWS `config` and `credentials` files will be copied to the S3 filesystem to enable certain dsgrid operations, such as syncing the remote dsgrid registry. By default, the launching script will look for those credentials in `~/.aws` of your local system. If different, specify the location in the config:
```
aws_credentials_location: path_to_your_credentials
```

Below is the default cluster instance configuration. We recommend using only ON_DEMAND for `master_instance` to maintain continuity of the cluster. For `core_instances`, choose between ON_DEMAND or SPOT at this time. SPOT pricing can be up to 90% less than ON_DEMAND but instances under this pricing scheme can be reclaimed by EC2 intermittently, which may affect your running processes. You can learn more about EC2 instance types at [EC2Instances.info] (https://ec2instances.info/) or [AWS EC2 pricing] (https://aws.amazon.com/ec2/pricing/?trk=36c6da98-7b20-48fa-8225-4784bced9843&sc_channel=ps&sc_campaign=acquisition&sc_medium=ACQ-P%7CPS-GO%7CBrand%7CDesktop%7CSU%7CCompute%7CEC2%7CUS%7CEN%7CText&s_kwcid=AL!4422!3!488982705735!p!!g!!aws%20ec2%20pricing&ef_id=EAIaIQobChMIs-bhp6-_-AIV4w-tBh2MHw9VEAAYASABEgKzyPD_BwE:G:s&s_kwcid=AL!4422!3!488982705735!p!!g!!aws%20ec2%20pricing).
```
master_instance:
  type: m5.2xlarge
  market: ON_DEMAND
core_instances:
  count: 2
  type: c5d.12xlarge
  market: SPOT
```
`idle_time_out` controlls the length of time for which the EC2 instances could sit idling before they get shut down automatically. If unspecified, `idle_time_out` is default to 1 hour. dsgrid enforces a maximum idle time of 3 hours.

### Running

To start up the cluster, run the `launch_emr.py` script from the `dsgrid/emr` folder using the `dsgrid-emr` conda environment you created:
```
python launch_emr.py [--dir_to_sync <path_to_dir>] [--name <name_your_emr_session>]
```

By default, the `./dsgrid/notebooks` folder is copied to the Hadoop filesystem. If you want to sync a different folder, use the `--dir_to_sync` or `-d` flag.

Once the cluster is running and the ssh-tunnel is set-up you can access the system through Jupyter or by ssh-ing into the Master node. The Jupyter address and the IP for the master node get printed during the initialization process. For example the stdout printed during start up looks like:
```
copying tests_aws/test_registry/test_data_sync.py -> dsgrid-0.1.0/tests_aws/test_registry
copying tests_aws/test_registry/test_push_pull_online_mode.py -> dsgrid-0.1.0/tests_aws/test_registry
copying tests_aws/test_registry/tests_s3_locks.py -> dsgrid-0.1.0/tests_aws/test_registry
Writing dsgrid-0.1.0/setup.cfg
Creating tar archive
removing 'dsgrid-0.1.0' (and everything under it)
Started a new cluster: j-2Y17R69QY2BKZ
Cluster Status: STARTING - (no message)
Cluster Status: STARTING - (no message)
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

  To ssh into the master node: ssh -i ~/.ssh/<USER>-dsgrid.pem hadoop@54.184.112.221
```
Login to Jupyter by opening the URL in a web browser. You can also ssh to the Master node as instructed above.

In either case, create a SparkSession attached to the cluster by entering this code block in a Jupyter cell or by running these lines in an ipython session or by including them in a script:
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("AWS-Cluster").getOrCreate()
```

### Shutting down
When shutting down the cluster, the `dir_to_sync` folder gets copied back to your local machine.

To shut the cluster down, simply Ctrl+C out of the local python process you used to start the cluster and follow the prompts. You will see stdout like:
```
Caught Ctrl+C, shutting down tunnel, please wait
Copying notebooks back to local machine: /home/ehale/dsgrid/dsgrid/notebooks...
Terminate cluster [y/n]? y
Terminating cluster j-2Y17R69QY2BKZ
```

After terminating your cluster, you will be prompted to delete your S3 scratch folder, which contains files for bootstrapping as well as your EMR logs. dsgrid will periodically clear out the scratch folder to maintain storage cost and EMR performance. You can also see what instances you have running (and manually shut them down, if so desired, but doing it this way will not preserve any changes made to `dir_to_sync`) by going to the [EC2 management](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#Home) page of the AWS console. The cluster will also shut down after the `idle_time_out` is reached.
