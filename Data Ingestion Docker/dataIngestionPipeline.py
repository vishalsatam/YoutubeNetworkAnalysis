import json
import datetime
import time
import pandas as pd
import glob
import logging
import os
import requests
import shutil
import luigi
from pathlib import Path
import zipfile
from bs4 import BeautifulSoup
import boto3
from botocore.client import Config

LOGPATH = os.environ['LOGPATH']+"/"
NOW = datetime.datetime.now()
TODAYSDATE = str(NOW.day).zfill(2)+str(NOW.month).zfill(2)+str(NOW.year)
TODAYSDATESTRING = str(NOW.day).zfill(2)+"/"+str(NOW.month).zfill(2)+"/"+str(NOW.year)

#MAINPATH="C:/Users/visha/Desktop/MSIS/Advanced Data Science/Assignments/Final Assignments/Docker"
MAINPATH=os.environ['MAINPATH']+"/"

#LOGPATH = "C:/Users/visha/Desktop/MSIS/Advanced Data Science/Assignments/Assignment3/logs"+"/"
LOGPATH = MAINPATH+"/logs/"
LOGFILENAME = LOGPATH+"/"+TODAYSDATE+str(NOW.hour).zfill(2)+str(NOW.minute).zfill(2)+str(NOW.second).zfill(2)+".log"
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',filename=LOGFILENAME,datefmt='%Y-%m-%d %H:%M:%S',level=logging.INFO)

CONFIGPATH=os.environ['CONFIGPATH']+"/"

TEMPPATH=MAINPATH+"/temp/"
SUCCESSOUTPUTPATH=TEMPPATH+"/Success.out"
YOUTUBEDOWNPATH=MAINPATH+"/data/"
YOUTUBELINK="http://netsg.cs.sfu.ca/youtubedata/"
mainFileDownPathRaw=YOUTUBEDOWNPATH+"/MainFileCombined_raw.csv"
mainFileDownPathClean=YOUTUBEDOWNPATH+"/MainFileCombined_clean.csv"

userFileDownPathRaw=YOUTUBEDOWNPATH+"/UserFileCombined_raw.csv"
userFileDownPathClean=YOUTUBEDOWNPATH+"/UserFileCombined_clean.csv"

videoRelationshipsFile=YOUTUBEDOWNPATH+"/VideoRelationships.csv"
uploadsRelationshipsFile=YOUTUBEDOWNPATH+"/UserUploads.csv"

processRelationship=TEMPPATH+"/processRelationships.json"
headerFilesTempPath=TEMPPATH+"/headerFilesTemp.json"

headerUploaderProfile=YOUTUBEDOWNPATH+"/UserUploads_header.csv"
headerVideoNodes=YOUTUBEDOWNPATH+"/MainFileCombined_clean_header.csv"
headerRelatedVideos=YOUTUBEDOWNPATH+"/VideoRelationships_header.csv"
headerUploaders=YOUTUBEDOWNPATH+"/UserFileCombined_clean_header.csv"

def downloadExtractRemove(filetype,link,extractpath,filename):
    filepath=extractpath+"/"+filename
    downfile = requests.get(link)
    filelist = []
    with open(filepath, "wb") as fil:
        fil.write(downfile.content)
    with open(filepath, "rb") as file:
        zip_ref = zipfile.ZipFile(file)
        zip_ref.extractall(extractpath)
        #z = zip_ref.filelist
        #print(filename.split(".")[0])
        folderPath=extractpath+"/"+filename.split(".")[0]
        for f in glob.iglob(folderPath+"/*.txt", recursive=True):
            if "log.txt" not in f:
                if filetype=="MainCrawl":
                    filelist.append(r""+f)
                if filetype=="UserCrawl":
                    if "id" not in f:
                        filelist.append(r""+f)
        zip_ref.close()
    os.remove(filepath)
    return filelist

def clean_text(row):
    # return the list of decoded cell in the Series instead 
    return [r.decode('unicode_escape').encode('ascii', 'ignore') for r in row]

def clean_columns(df):
    for column in df:
        df['column'] = df.apply(clean_text)
    return df

# Function gets all sample links
def getDownloadLinksFrom(filetype,downPath,downloadurl):
    #params = {"username":username,"password":password,"action":"acceptTandC","accept":"Yes","acceptSubmit":"Continue"}
    page = requests.get(downloadurl)
    soup = BeautifulSoup(page.content, 'html.parser')
    downloadlinks=soup.find_all('a')
    downfilesDict=[]
    for link in downloadlinks:
        if filetype=="MainCrawl":
            if ".zip" in link.text:
                if "u" in link.text:
                    break
                if link.text=="080707.zip":
                    break
                downfilesDict.append(link.text)
        elif filetype=="UpdateCrawl":
            if "u.zip" in link.text:
                downfilesDict.append(link.text)
        elif filetype=="SizeAndBitrateCrawl":
            sizeRateList = ['0523.zip','0628.zip','080908sizerate.zip']
            if link.text in sizeRateList:
                downfilesDict.append(link.text)
        elif filetype=="UserCrawl":
            userList=['0528.zip','080903user.zip']
            if link.text in userList:
                #downloadExtractRemove(downloadurl+link['href'],downPath,link.text)
                downfilesDict.append(link.text)
    return downfilesDict

def processMainfileCrawl(filelist,filepath):
    todays_file = Path(filepath)
    if not todays_file.is_file():
        headerFlag=True
    else:
        headerFlag=False
    videoscollist=['videoID','userID','videoAge','category','length','views','rating','numberRatings','comments','relVideoID1','relVideoID2','relVideoID3','relVideoID4','relVideoID5','relVideoID6','relVideoID7','relVideoID8','relVideoID9','relVideoID10','relVideoID11','relVideoID12','relVideoID13','relVideoID14','relVideoID15','relVideoID16','relVideoID17','relVideoID18','relVideoID19','relVideoID20']
    concat_df = []
    for fil in filelist:
        with open(fil,"r"):
            concat_df.append(pd.read_csv(fil,delimiter="\t",names=videoscollist))
        os.remove(fil)
    concat_df=pd.concat(concat_df)
    concat_df=cleanMainFile(concat_df)
    with open(filepath, 'a') as f:
        concat_df=clean_columns(concat_df)
        concat_df.to_csv(f, header=headerFlag,index=False)

def processUserCrawl(filelist,filepath):
    todays_file = Path(filepath)
    if not todays_file.is_file():
        headerFlag=True
    else:
        headerFlag=False
    usercollisttokeep=['userID','uploads','friends']
    usercollistold=['userID','uploads','watches','friends']
    concat_df = []
    for fil in filelist:
        if "0528" in fil:
            concat_df.append(pd.read_csv(fil,delimiter="\t",names=usercollistold))
        elif "080903" in fil:
            concat_df.append(pd.read_csv(fil,delimiter="\t",names=usercollisttokeep))
        os.remove(fil)
        concat_df=pd.concat(concat_df)
        concat_df=cleanUserFile(concat_df)
        with open(filepath, 'a') as f:
            concat_df=clean_columns(concat_df)
            concat_df.to_csv(f, header=headerFlag,index=False)     

def cleanMainFile(to_be_cleaned_df):
    to_be_cleaned_df.update(to_be_cleaned_df[['videoAge','length','views','rating','numberRatings','comments']].fillna(0))
    to_be_cleaned_df=to_be_cleaned_df[to_be_cleaned_df['videoID'].notnull()]
    to_be_cleaned_df=to_be_cleaned_df[to_be_cleaned_df['userID'].notnull()]
    to_be_cleaned_df.drop_duplicates(subset='videoID',inplace=True,keep="last")
    
    to_be_cleaned_df.category.replace('Nonprofits &amp; Activism','Nonprofits & Activism',inplace=True)
    to_be_cleaned_df.category.replace('News &amp; Politics','News & Politics',inplace=True)
    to_be_cleaned_df.category.replace('People &amp; Blogs','People & Blogs',inplace=True)
    to_be_cleaned_df.category.replace('Film &amp; Animation','Film & Animation',inplace=True)
    to_be_cleaned_df.category.replace('Travel &amp; Events','Travel & Events',inplace=True)
    to_be_cleaned_df.category.replace('Howto &amp; Style','Howto & Style',inplace=True)
    to_be_cleaned_df.category.replace('Pets &amp; Animals','Pets & Animals',inplace=True)
    to_be_cleaned_df.category.replace('Autos &amp; Vehicles','Autos & Vehicles',inplace=True)
    to_be_cleaned_df.category.replace('Science &amp; Technology','Science & Technology',inplace=True)
    
    return to_be_cleaned_df

def cleanUserFile(to_be_cleaned_df):
    to_be_cleaned_df=to_be_cleaned_df[['userID','uploads','friends']]
    to_be_cleaned_df=to_be_cleaned_df[to_be_cleaned_df['userID'].notnull()]
    to_be_cleaned_df.drop_duplicates(subset='userID',inplace=True,keep="last")
    to_be_cleaned_df.update(to_be_cleaned_df[['uploads','friends']].fillna(0))
    return to_be_cleaned_df

def readJson(path):
    fil = open(path,'r')
    conf = json.load(fil)
    fil.close()
    return conf
def transformVideosToRelations(inputDF,relfilename):
    counter=1
    colstokeep=['videoID','relVideoID1', 'relVideoID2',
           'relVideoID3', 'relVideoID4', 'relVideoID5', 'relVideoID6',
           'relVideoID7', 'relVideoID8', 'relVideoID9', 'relVideoID10',
           'relVideoID11', 'relVideoID12', 'relVideoID13', 'relVideoID14',
           'relVideoID15', 'relVideoID16', 'relVideoID17', 'relVideoID18',
           'relVideoID19', 'relVideoID20']
    inc=500000
    startindex=0
    endindex=inc
    rangeinc=int(inputDF.videoID.count()/inc)
    print("BEGIN Process -- Converting Videos File to Relationships")
    logging.info("BEGIN Process -- Converting Videos File to Relationships")
    with open(relfilename,'a') as fil:
        for i in range(1,rangeinc+1):
            df2 = inputDF.iloc[startindex:endindex][colstokeep].set_index('videoID').stack().reset_index(level=1, drop=True).reset_index(name='relatedVideoID')
            df2.dropna(subset=['relatedVideoID'])
            df2=df2[df2['videoID']!=df2['relatedVideoID']]
            df2.dropna(inplace=True)
            df2=clean_columns(df2)
            df2.to_csv(fil,index=False)
            startindex=endindex
            endindex=endindex+inc
            print("Processing "+str(counter/rangeinc*100)+"% completed")
            logging.info("Processing "+str(counter/rangeinc*100)+"% completed")
            counter+=1
    print("END Process -- Converting Videos File to Relationships")
    logging.info("END Process -- Converting Videos File to Relationships")

def transformVideosToUserUploads(inputDF,relfilename):
    with open(relfilename,'a') as fil:
        inputDF=clean_columns(inputDF)
        inputDF[['videoID','userID']].to_csv(fil,index=False)
        inputDF.dropna(inplace=True)


class ScrapeLinks(luigi.Task):
    def output(self):
        return luigi.LocalTarget(TEMPPATH+"/scrapedLinks.txt")
    def run(self):
        dictCrawl={}
        dictCrawl["MainCrawl"]=getDownloadLinksFrom("MainCrawl",YOUTUBEDOWNPATH,YOUTUBELINK)
        dictCrawl["UpdateCrawl"]=getDownloadLinksFrom("UpdateCrawl",YOUTUBEDOWNPATH,YOUTUBELINK)
        dictCrawl["UserCrawl"]=getDownloadLinksFrom("UserCrawl",YOUTUBEDOWNPATH,YOUTUBELINK)
        dictCrawl["SizeAndBitrateCrawl"]=getDownloadLinksFrom("SizeAndBitrateCrawl",YOUTUBEDOWNPATH,YOUTUBELINK)

        with open(self.output().path,'w') as f:
            f.write(json.dumps(dictCrawl))

class DownloadAndWrangleUserFiles(luigi.Task):
    def requires(self):
        return ScrapeLinks()
    def run(self):
        logging.info(str(datetime.datetime.now()) +" :: "+" Starting process of usercrawl files")
        print(str(datetime.datetime.now()) +" :: "+" Starting process of usercrawl files")
        scrapeLinks=readJson(self.input().path)
        
        for filename in scrapeLinks["UserCrawl"]:
            logging.info(str(datetime.datetime.now()) +" :: "+" Downloading and Processing "+filename.split(".")[0])
            print(str(datetime.datetime.now()) +" :: "+" Downloading and Processing "+filename.split(".")[0])
            filelist=downloadExtractRemove("UserCrawl",YOUTUBELINK+filename,YOUTUBEDOWNPATH,filename)
            
            processUserCrawl(filelist,userFileDownPathRaw)
            #ad code to log
            
            print("Processing "+filename.split(".")[0])
            shutil.rmtree(YOUTUBEDOWNPATH+"/"+filename.split(".")[0])
            logging.info(str(datetime.datetime.now()) +" :: "+" End of Processing "+filename.split(".")[0])
        
        df=pd.read_csv(userFileDownPathRaw)
        df.drop_duplicates(subset='userID',inplace=True,keep="last")
        df=clean_columns(df)
        df.to_csv(self.output().path,index=False)
        logging.info(str(datetime.datetime.now()) +" :: "+" Ending process of usercrawl files")
        print(str(datetime.datetime.now()) +" :: "+" Ending process of usercrawl files")
        os.remove(userFileDownPathRaw)
            
    def output(self):
        return luigi.LocalTarget(userFileDownPathClean)

def UploadToS3Function(S3,BUCKET_NAME,filepath,text):
    data = open(filepath,'rb')
    keyName = filepath.split("/")[-1]
    logging.info("Uploading "+text+" File To S3")
    print("Uploading "+text+" File To S3")
    S3.Bucket(BUCKET_NAME).put_object(Key=keyName, Body=data)
    print("File "+keyName+" Uploaded successfully to S3 bucket " + BUCKET_NAME)
    logging.info("File "+keyName+" Uploaded successfully to S3 bucket " + BUCKET_NAME)
    data.close()

class UploadProcessedInformationToS3(luigi.Task):
    def output(self):
        return luigi.LocalTarget(SUCCESSOUTPUTPATH)
    def requires(self):
        yield ProcessRelationships()
        yield CreateHeaderFiles()
    def run(self):
        configFile=readJson(CONFIGPATH+"config.json")
        
        BUCKET_NAME = configFile['BucketName']
        S3 = boto3.resource('s3',
            aws_access_key_id= configFile['AWSAccess'],
            aws_secret_access_key=configFile['AWSSecret'],
            config=Config(signature_version='s3v4')
            )
        try:
            S3.create_bucket(Bucket=BUCKET_NAME)
        except:
            logging.error("Error while creating / accessing new Bucket")
            raise
        logging.info("Bucket "+BUCKET_NAME+"created / already exists")

        allPaths=readJson(self.input()[0].path)

        mainVideosFileInputInputPath = allPaths['mainVideosFile']
        uploaderRelationshipsInputPath = allPaths['uploaderRelationships']
        videoRelationshipsInputPath = allPaths['videoRelationships']
        userVideos = allPaths['usersFile']
        
        headerPaths=readJson(self.input()[1].path)
        headerUploaderProfile=headerPaths['headerUploaderProfile']
        headerVideoNodes=headerPaths['headerVideoNodes']
        headerRelatedVideos=headerPaths['headerRelatedVideos']
        headerUploaders=headerPaths['headerUploaders']
        
        UploadToS3Function(S3,BUCKET_NAME,mainVideosFileInputInputPath,"Main Videos")
        UploadToS3Function(S3,BUCKET_NAME,userVideos,"Users")
        UploadToS3Function(S3,BUCKET_NAME,uploaderRelationshipsInputPath,"Uploader Relationships")
        UploadToS3Function(S3,BUCKET_NAME,videoRelationshipsInputPath,"Video Relationships")
        UploadToS3Function(S3,BUCKET_NAME,headerUploaderProfile,"Uploaders Header")
        UploadToS3Function(S3,BUCKET_NAME,headerVideoNodes,"Videos Header")
        UploadToS3Function(S3,BUCKET_NAME,headerRelatedVideos,"Related Videos Header")
        UploadToS3Function(S3,BUCKET_NAME,headerUploaders,"Video Uploaders Header")

        with open(self.output().path,"w") as outFile:
            outFile.write("SUCCESS")

class CreateHeaderFiles(luigi.Task):
    def run(self):
        with open(headerUploaderProfile,"w") as f:
            f.write("videoID:END_ID,userID:START_ID")
        with open(headerVideoNodes,"w") as f:
            f.write("videoID:ID,userID,videoAge,category,length,views,rating,numberRatings,comments")
        with open(headerRelatedVideos,"w") as f:
            f.write("videoID:START_ID,relatedVideoID:END_ID")
        with open(headerUploaders,"w") as f:
            f.write("userID:ID,uploads,friends")

        dictHeaders={'headerUploaderProfile':headerUploaderProfile,'headerVideoNodes':headerVideoNodes,'headerRelatedVideos':headerRelatedVideos,'headerUploaders':headerUploaders}
        
        with open(self.output().path,"w") as f:
            f.write(json.dumps(dictHeaders))
    def output(self):
        return luigi.LocalTarget(headerFilesTempPath)

class ProcessRelationships(luigi.Task):
    def requires(self):
        yield DownloadAndWrangleMainFiles()
        yield DownloadAndWrangleUserFiles()
    def run(self):
        mainVideos = Path(self.input()[0].path)
        userFile = Path(self.input()[1].path)
        relationship = Path(videoRelationshipsFile)
        uploads = Path(uploadsRelationshipsFile)
        
        if mainVideos.is_file() :
            if not relationship.is_file():
                logging.error(str(datetime.datetime.now()) +" :: "+"Video Relationships File not generated")
                raise CustomDataIngestionException(str(datetime.datetime.now()) +" :: "+"Video Relationships File not generated")
            if not uploads.is_file():
                logging.error(str(datetime.datetime.now()) +" :: "+"Uploader Relationships File not generated")
                raise CustomDataIngestionException(str(datetime.datetime.now()) +" :: "+"Uploader Relationships File not generated")
        else:
            logging.error(str(datetime.datetime.now()) +" :: "+"Main Videos File not generated")
            raise CustomDataIngestionException(str(datetime.datetime.now()) +" :: "+"Main Videos File not generated")
        
        if not userFile.is_file():
            logging.error(str(datetime.datetime.now()) +" :: "+"Users File not generated")
            raise CustomDataIngestionException(str(datetime.datetime.now()) +" :: "+"Users File not generated")
        
        with open(self.output().path,"w") as f:
            procRelDict={'videoRelationships' : videoRelationshipsFile, 'uploaderRelationships' : uploadsRelationshipsFile ,'mainVideosFile' : self.input()[0].path,'usersFile': self.input()[1].path}
            f.write(json.dumps(procRelDict))
        
    def output(self):
        return luigi.LocalTarget(processRelationship)

class DownloadAndWrangleMainFiles(luigi.Task):
    def output(self):
        return luigi.LocalTarget(mainFileDownPathClean)
    def run(self):
        logging.info(str(datetime.datetime.now()) +" :: "+" Starting process of maincrawl files")
        print(str(datetime.datetime.now()) +" :: "+" Starting process of maincrawl files")
        cleanpath=readJson(self.input().path)
        counter = 0
        for filename in cleanpath["MainCrawl"]:
            logging.info(str(datetime.datetime.now()) +" :: "+" Downloading and Processing "+filename.split(".")[0])
            filelist=downloadExtractRemove("MainCrawl",YOUTUBELINK+filename,YOUTUBEDOWNPATH,filename)

            if counter == 10:
                time.sleep(60)
                counter=0
            counter+=1
            processMainfileCrawl(filelist,mainFileDownPathRaw)
            #ad code to log
            
            print("Processing "+filename.split(".")[0])
            shutil.rmtree(YOUTUBEDOWNPATH+"/"+filename.split(".")[0])
            logging.info(str(datetime.datetime.now()) +" :: "+" End of Processing "+filename.split(".")[0])
        
        df=pd.read_csv(mainFileDownPathRaw,encode('ascii', 'replace'))
        df.drop_duplicates(subset='videoID',inplace=True,keep="last")
        
        transformVideosToRelations(df,videoRelationshipsFile)
        
        finalColumns=['videoID','userID','videoAge','category','length','views','rating','numberRatings','comments']
        df=df[finalColumns]
        
        transformVideosToUserUploads(df,uploadsRelationshipsFile)
        df=clean_columns(df)
        df.to_csv(self.output().path,index=False)
        logging.info(str(datetime.datetime.now()) +" :: "+" Ending process of maincrawl files")
        print(str(datetime.datetime.now()) +" :: "+" Ending process of maincrawl files")
        os.remove(mainFileDownPathRaw)
        
    def requires(self):
        return ScrapeLinks()

class CustomDataIngestionException(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)