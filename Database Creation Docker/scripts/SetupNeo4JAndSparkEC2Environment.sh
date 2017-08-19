#Common Code for Creating Seurity Profile, Access Keys and Grouop
filename=$LOGPATH/$(date "+%d%m%Y%H%m%s").log
echo $filename
touch $filename
echo "copying db config files data from config.txt"
echo "copying db config files data from config.txt" >> $filename
head $CONFIGPATH/config.txt --lines=4 >> /etc/mysql/my.cnf
echo "Setting AWS Access Keys from config.txt"
echo "Setting AWS Access Keys from config.txt" >> $filename
awskey=$(sed '5!d' $CONFIGPATH/config.txt)
awssecret=$(sed '6!d' $CONFIGPATH/config.txt)
export "$awskey"
export "$awssecret"


export KEY_NAME="finalproj"
aws ec2 create-key-pair --key-name $KEY_NAME --output text > $KEY_NAME.pem

export GROUP="finalproject-sg"
aws ec2 create-security-group --group-name $GROUP --description "Final Project Security Group" | jq ."GroupId" | export GROUPNAME

for port in 22 7474 7473 7687; do
  aws ec2 authorize-security-group-ingress --group-name $GROUPNAME --protocol tcp --port $port --cidr 0.0.0.0/0
done

chmod 600 $KEY_NAME.pem

aws ec2 run-instances --image-id ami-f03c4fe6 --count 1 --instance-type $7 --key-name $KEY_NAME --security-groups $GROUP --query "Instances[*].InstanceId" | jq .[] | export INSTANCENAME

aws ec2 associate-address --instance-id $INSTANCENAME --public-ip $6

#Create Spark EC2 cluster
#Download Spark EC2
wget -O ~ https://github.com/amplab/spark-ec2/archive/branch-2.0.zip
unzip ~ -d ~/

#Launch Cluster
sudo ~/spark-test/spark-ec2-branch-2.0/spark-ec2 -k $GROUPNAME -i $KEY_NAME.pem -s $1 -z $4 --hadoop-major-version=yarn --ebs-vol-num=1 --ebs-vol-size=$2 -t $3 --ebs-vol-type $5 launch youtubegraph
