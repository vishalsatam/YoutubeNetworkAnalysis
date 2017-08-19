#export DATAPATH=/home/vishalsatam/finalprojectDockerRDS/data
#export CONFIGPATH=/home/vishalsatam/finalprojectDockerRDS
#export LOGPATH=/home/vishalsatam/finalprojectDockerRDS/logs
#export SCRIPTSPATH=/home/vishalsatam/finalprojectDockerRDS/sql

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

if [ -e $DATAPATH/MainFile_W_PR_DEG.csv ]
then
	echo "File Exists. Will not download"
	echo "File Exists. Will not download" >> $filename
else
	echo "Downloading MainFile_W_PR_DEG.csv from S3"
	echo "Downloading from S3" >> $filename
	aws s3 cp s3://Team1FinalProject/MainFile_W_PR_DEG.csv $DATAPATH/
fi

if [ -e $DATAPATH/userstatistics.csv ]
then
	echo "File Exists. Will not download"
	echo "File Exists. Will not download" >> $filename
else
	echo "Downloading from userstatistics S3"
	echo "Downloading from userstatistics S3" >> $filename
	aws s3 cp s3://Team1FinalProject/userstatistics.csv $DATAPATH/
fi


chmod 777 $DATAPATH/*
sed -i '1d' $DATAPATH/MainFile_W_PR_DEG.csv
sed -i '1d' $DATAPATH/userstatistics.csv
echo "Creating and Youtube Videos tables"
echo "Creating and uploading Youtube Videos tables" >> $filename
mysql youtubedb < $SCRIPTSPATH/loadYoutubeVideos.sql
echo "Creating and Youtube Users tables"
echo "Creating and uploading Youtube Users tables" >> $filename
mysql youtubedb < $SCRIPTSPATH/loadYoutubeUsers.sql
echo "All tables created"
echo "All tables created" >> $filename
