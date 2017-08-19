# Final Project Submission

This is the submissison for the final project for Team 1 created on Youtube Social Network Analysis.

The PPT has been shared on the following location
https://docs.google.com/presentation/d/1ys6_IB59gYB2g_Bh3azn2tyK3cT0FDGVPFj2gdw__Y4/edit?usp=sharing

# Data Ingestion Docker Execution

## Pull the image

```
docker pull vishalsatam1988/finalprojdataingestion
```

## Create a container

```
docker create --name="finalprojingestion" vishalsatam1988/finalprojdataingestion
```

## Copy your config file into your container

```
docker cp <local file path> <containername>:/src/finalprojingestion/config/
docker cp <path>/config.json finalprojingestion:/src/finalprojingestion/config/
```

## Start the container

Please note that this image performs computations over a large file and may result in causing memory issues
If you encounter them, execute on EC2

```
docker start -i <container name>
docker start -i finalprojingestion
```

# Database Setup

The below section explains how you can setup the database environment on the cloud

## Docker Execution

### Pull the Docker Image
```
docker pull vishalsatam1988/finaldbaasrds
```

### Create Container
Create the docker Container and copy the config file. Sample file is given in this repository
```
docker create --name="finalprojdbaas" vishalsatam1988/finaldbaasrds 
```

### Copy Config File
```
docker cp <local file path> <containername>:/src/finalproj/config/
docker cp <path>/config.txt finalprojdbaas:/src/finalproj/config/
```

### Start the Container - Sets up RDS db on AWS

This step will execute the shell script getFromS3AndLoadRDS.sh and setup an your RDS instance. Please make sure you provide the correct endpoint.
```
docker start <containername>
docker start -i finalprojdbaas
```


## After the Database Setup


### Commit the docker container 
```
docker commit finalprojdbaas vishalsatam1988/finaldbaasrds
```

Enter the Docker container using
```
docker run -it vishalsatam1988/finaldbaasrds /bin/bash
```

Execute the following script using the command 
```
sh /src/finalproj/SetupNeo4JAndSparkEC2Environment.sh <positional arguments described below>
```
This will **setup the Neo4J and SPARK** cluster on **AWS**

SetupNeo4JAndSparkEC2Environment.sh

positional arguments
1 - Number of Slave Nodes
2 - Size of the Volume
3 - EC2 Instance type - (Recommended - m4.xlarge)
4 -  Region
5 - EBS volume type
6 - Elastic IP to associate the Neo4J endpoint
7 - EC2 Instance Type - (Recommended m4.large)


### Setting up the Neo4J cluster on EC2

Once the EC2 instance that has the Neo4J is up and running, you can SSH into the instance using the command
```
ssh -i <private key file> <instance-id>
```
And then copy paste the coe given in the file ExecuteInNeo4jEC2Instance.sh for setting up the Database and importing Neo4J database on EC2. The script file downloads data from your S3 and creates the Neo4J database. Please provide a positional arument - 
* 1 bucket name
*The file contains an SSH command*

### Executing the Spark job

For running the Spark Job, you will have to SSH (ssh -i <private key file> ubuntu@<ec2 instance url>) into the new Spark EC2 instance which was created in your accunt and execute the  pageRankForSpark.py by executing the following command. The page rank file pageRankForSpark.py has been provided in this repository.
```
spark-submit ~/spark/bin/spark-submit pageRankForSpark.py
```