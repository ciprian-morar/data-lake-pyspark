# Summary of the project, how to run the Python scripts, and an explanation of the files in the repository

A music streaming startup, Sparkify, has grown their user base and 
song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, 
as well as a directory with JSON metadata on the songs in their app.

This project is an example of building an ETL pipeline for a data lake hosted on S3. 

For this project I load datasets from two s3 locations: 

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data

With the **Song data** loaded in spark dataframes I create song and artists Spark analytics 
tables. After loading and transforming **Log Data** I create users, time and songplays tables.

The new created analytics tables are writed in partitions on S3 buckets.

## 1. Configure environment locally to be able to interact programatically with AWS services 

1. install AWS CLI 2
1. Configure CLI
  * Access key - Generate an Access key from the AWS IAM Service 
  * Default AWS Region - Specifies the AWS Region where you want to send your 
    requests by default
  * Default Output format - Specifies how the results are formatted 
    It can either be a json, yaml, text or a table
  * Profile - A collection of settings is called a profile. The default profile name is default, 
    however, you can create a new profile using the aws configure --profile new_name command.
    
    Configure the credentials and the environment variables:
        * Navigate the home directory and check the current configuration:
            aws configure list
        * Store the access key in a default file ~/.aws/credentials 
        and store the profile in the ~/.aws/config file using the following command.
            aws configure --profile default
       * Upon prompt, paste the copied access key (access key id and secret access key). 
         Enter the default region as us-east-1 and output format as json.
       * Let the system know that your sensitive information is residing in the .aws folder
            export AWS_CONFIG_FILE=~/.aws/config
            export AWS_SHARED_CREDENTIALS_FILE=~/.aws/credentials 
       * Check the successful configuration of the AWS CLI, by running an AWS command:
            aws iam list-users

## 2. AWS CLI - Create EMR Cluster.

1. Create default roles in IAM - Before you run the aws emr create-cluster command, 
   make sure to have the necessary roles created in your account. 
   Use the following command.
        aws emr create-default-roles
   
1. Generate EC2 key pairs AWS_EC2_Demo.pem file.

1. Launch your cluster:

    aws emr create-cluster --name py_cluster --use-default-roles --release-label emr-5.28.0 
    --instance-count 3 --instance-type m5.xlarge --applications Name=Spark Name=Hadoop Name=Livy 
    Name=Hive --ec2-attributes KeyName=AWS_EC2_Demo,SubnetId=subnet-c***** 
    --log-uri s3://aws-emr-*********
   
1. Change Security groups - Enable the Security Groups 
   setting of the master EC2 instance to accept incoming SSH protocol 
   from your local computer
   * Edit the security group to authorize inbound SSH traffic (port 22) from your local computer
    
    

## 3. Copy the etl.py file through SSH Tunnel on the Master Node

1. Connect using the SSH protocol
    ssh -i AWS_EC2_Demo.pem hadoop@ec2-3-139-93-181.us-east-2.compute.amazonaws.com
1. Set Up an SSH Tunnel to the Master Node Using Dynamic Port Forwarding
   ssh -i AWS_EC2_Demo.pem -N -D 8157 hadoop@ec2-3-139-93-181.us-east-2.compute.amazonaws.com
1. Copy etl.py script from locally 
   scp -i AWS_EC2_Demo.pem etl.py hadoop@ec2-3-139-93-181.us-east-2.compute.amazonaws.com:/home/hadoop/
1. Configure Proxy Settings in your Local Computer
    * Add ProxySwitchOmega extension to your Chrome browser
    * Create a new profile with name emr-socks-proxy and select PAC profile type
    * Save the following profile script in your new profile:
        function FindProxyForURL(url, host) {
         if (shExpMatch(url, "*ec2*.amazonaws.com*")) return 'SOCKS5 localhost:8157';
         if (shExpMatch(url, "*ec2*.compute*")) return 'SOCKS5 localhost:8157';
         if (shExpMatch(url, "http://10.*")) return 'SOCKS5 localhost:8157';
         if (shExpMatch(url, "*10*.compute*")) return 'SOCKS5 localhost:8157';
         if (shExpMatch(url, "*10*.amazonaws.com*")) return 'SOCKS5 localhost:8157';
         if (shExpMatch(url, "*.compute.internal*")) return 'SOCKS5 localhost:8157';
         if (shExpMatch(url, "*ec2.internal*")) return 'SOCKS5 localhost:8157';
         return 'DIRECT';
        }
    * Enable the emr-socks-proxy profile  
    * Once, you have configured the proxy, you can access the Spark UI using 
      the command (replace the master node public DNS for you):
        http://ec2-3-139-93-181.us-east-2.compute.amazonaws.com:18080/
    

## 4. Submit Spark Script
1. From the location /home/hadoop on the master node run the following command:
    usr/bin/spark-submit --master yarn .etl.py
1. Verify the results in the we browser using the Spark UI configure earlier.   