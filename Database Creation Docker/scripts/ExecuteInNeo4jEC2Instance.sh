#SSH into your EC2 instance and then execute the below code.
#ssh -i $KEY_NAME ubuntu@$GROUPNAME

sudo apt install awscli

sudo wget -O /var/lib/neo4j/plugins/apoc-3.2.0.4-all.jar https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/3.2.0.4/apoc-3.2.0.4-all.jar
sudo chmod 777 /var/lib/neo4j/plugins/apoc-3.2.0.4-all.jar
sudo wget -O /var/lib/neo4j/plugins/graph-algorithms-algo-3.2.2.1.jar https://github.com/neo4j-contrib/neo4j-graph-algorithms/releases/download/3.2.2.1/graph-algorithms-algo-3.2.2.1.jar
sudo chmod 777 /var/lib/neo4j/plugins/graph-algorithms-algo-3.2.2.1.jar


sudo chmod 777 /etc/neo4j/neo4j.conf
echo "dbms.security.procedures.unrestricted=algo.*,apoc.*" >> /etc/neo4j/neo4j.conf

sudo chmod 777 /var/lib/neo4j/data/databases/
sudo mv /var/lib/neo4j/data/databases/graph.db /var/lib/neo4j/data/databases/graph.db_bkp

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_DEFAULT_REGION=

aws s3 cp s3://$1/MainFileCombined_clean.csv /var/lib/neo4j/data/databases/
aws s3 cp s3://$1/MainFileCombined_clean_header.csv /var/lib/neo4j/data/databases/
aws s3 cp s3://$1/UserFileCombined_clean.csv /var/lib/neo4j/data/databases/
aws s3 cp s3://$1/UserFileCombined_clean_header.csv /var/lib/neo4j/data/databases/
aws s3 cp s3://$1/UserUploads.csv /var/lib/neo4j/data/databases/
aws s3 cp s3://$1/UserUploads_header.csv /var/lib/neo4j/data/databases/
aws s3 cp s3://$1/VideoRelationships.csv /var/lib/neo4j/data/databases/
aws s3 cp s3://$1/VideoRelationships_header.csv /var/lib/neo4j/data/databases/

neo4j-import --into /var/lib/neo4j/data/databases/graph.db --skip-bad-relationships true --skip-bad-nodes true --bad-tolerance 999999999 --nodes:userID "/var/lib/neo4j/data/databases/UserFileCombined_clean_header.csv,/var/lib/neo4j/data/databases/UserFileCombined_clean.csv" --nodes:videoID "/var/lib/neo4j/data/databases/MainFileCombined_clean_header.csv,/var/lib/neo4j/data/databases/MainFileCombined_clean.csv"  --relationships:UPLOADED "/var/lib/neo4j/data/databases/UserUploads_header.csv,/var/lib/neo4j/data/databases/UserUploads.csv" --relationships:RELATEDTO "/var/lib/neo4j/data/databases/VideoRelationships_header.csv,/var/lib/neo4j/data/databases/VideoRelationships.csv" --stacktrace true --input-encoding ISO-8859-1 --id-type STRING

sudo mv /var/lib/neo4j/data/databases/graph.db /var/lib/neo4j/data/databases/graph.db.bkp


sudo mkdir /var/run/neo4j
sudo chmod 777 /var/run/neo4j
sudo neo4j restart
