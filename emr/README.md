# EMR for ds-grid

The `launch_emr.py` script can be used launch an EMR cluster under the ds-grid account.
It should take about 10 minutes to initialize the cluster for the first time.
The `emr_config.yml` file holds all the required configurations to launch new clusters. The most notable option is the `ssh_keys` which is required to open a ssh-tunnel between EMR and localhost. This key can be obtained [here](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#KeyPairs:).

## AWS Credentials
You will also need a `credentials` folder containing a `config` and `credentials` file with the proper aws keys under a `nrel-aws-dsgrid` profile.

## Usage
Once the cluster is running and ssh-tunnel is set-up you can access the system through Jupyter (available on http://localhost:53644) or sshing into the Master node. The IP for the master node gets printed during the initialization process. For example: `ssh -i "dsgrid-key.pem" hadoop@172.18.27.112`



