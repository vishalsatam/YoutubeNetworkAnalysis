from flaskext.mysql import MySQL
from flask import Flask, render_template, request, jsonify,session,redirect
import atexit
import cf_deployment_tracker
import os
import json
import requests
import random
from py2neo import Graph

# Emit Bluemix deployment event
cf_deployment_tracker.track()

app = Flask(__name__)

db_name = 'mydb'
client = None
db = None

mysql = MySQL()
app.config['MYSQL_DATABASE_USER'] = 'youtubeuser'
app.config['MYSQL_DATABASE_PASSWORD'] = 'youtubepassword'
app.config['MYSQL_DATABASE_DB'] = 'youtubedb'
app.config['MYSQL_DATABASE_HOST'] = 'youtubedb.ciw9xte70vcm.us-east-1.rds.amazonaws.com'
mysql.init_app(app)
conn = mysql.connect()

#graph = Graph("http://34.232.196.81:7474")
'''
if 'VCAP_SERVICES' in os.environ:
    vcap = json.loads(os.getenv('VCAP_SERVICES'))
    print('Found VCAP_SERVICES')
    if 'cloudantNoSQLDB' in vcap:
        creds = vcap['cloudantNoSQLDB'][0]['credentials']
        user = creds['username']
        password = creds['password']
        url = 'https://' + creds['host']
        client = Cloudant(user, password, url=url, connect=True)
        db = client.create_database(db_name, throw_on_exists=False)
elif os.path.isfile('vcap-local.json'):
    with open('vcap-local.json') as f:
        vcap = json.load(f)
        print('Found local VCAP_SERVICES')
        creds = vcap['services']['cloudantNoSQLDB'][0]['credentials']
        user = creds['username']
        password = creds['password']
        url = 'https://' + creds['host']
        client = Cloudant(user, password, url=url, connect=True)
        db = client.create_database(db_name, throw_on_exists=False)
'''
# On Bluemix, get the port number from the environment variable PORT
# When running this app on the local machine, default the port to 8080
port = int(os.getenv('PORT', 8080))

def loadApiKeys(mltype):
    with open('apikeys.json') as data_file:    
        apikeys = json.load(data_file)
    if mltype=="clustering":
        return apikeys['clustering']
    elif mltype=="prediction":
        return apikeys['prediction']
    else:
        print("Algorithm doesn't exist")


@app.route('/') 
def indexpage():
    return render_template('index.html')
    #return render_template('UserAccount.html')

@app.route('/home')
def home():
    try:
        if session['username'] == "":
            return redirect('/')
    except:
        return redirect('/')
    
    print("Time: {0} / Used Cache: {1}")
    loremcategories=['abstract','animals','business','cats','city','food','nightlife','fashionpeople','nature','sports','technics','transport']
    
    loremcategories[random.randint(0,len(loremcategories)-1)]
    
    #dictJ={"categories":[{"name":"Entertainment","values":[{"videoId":"1","rating":"1","imgCategory":"&lt;img src=http://lorempixel.com/140/100/&gt;"},{"videoId":"2","rating":"2","imgCategory":loremcategories[random.randint(0,len(loremcategories)-1)]},{"videoId":"2","rating":"2","imgCategory":loremcategories[random.randint(0,len(loremcategories)-1)]},{"videoId":"2","rating":"2","imgCategory":loremcategories[random.randint(0,len(loremcategories)-1)]},{"videoId":"2","rating":"2","imgCategory":loremcategories[random.randint(0,len(loremcategories)-1)]},{"videoId":"2","rating":"2","imgCategory":loremcategories[random.randint(0,len(loremcategories)-1)]}]},{"name":"Science","values":[{"videoId":"3","rating":"4"},{"videoId":"4","rating":"4"}]}]}
    dictJ={}
    cursor =conn.cursor()
    query="SELECT COUNT(category) AS total,category FROM videos GROUP BY category ORDER BY total DESC"
    cursor.execute(query)
    data = cursor.fetchall()
    counter=0
    dictJ['categories']=[]
    categoryList=[]
    countsList=[]
    for row in data:
        categoryList.append(row[1])
        countsList.append(row[0])

        subJ={"name":row[1],"values":[]}
        
        cursor2 =conn.cursor()
        subquery="select videoID,views,comments,PageRank FROM  videos WHERE category = '"+row[1]+"' ORDER BY views DESC limit 6"
        #print(subquery)
        cursor2.execute(subquery)
        dataVids = cursor2.fetchall()
        counter+=1
        valueList=[]
        for subrow in dataVids:
            valueList.append({"videoID":subrow[0],"views":subrow[1],"comments":subrow[2],"PageRank":subrow[3]})
        subJ['values']=valueList
        dictJ['categories'].append(subJ)
        #print(dictJ)
        if counter>5:
            break
    cursor.close()
    return render_template('home.html',my_string=dictJ)

@app.route('/logout', methods=['GET'])
def logout():
    session.clear()
    return render_template('index.html')

@app.route('/login', methods=['POST'])
def login():
    try:
        if request.form['username']=="":
            return redirect('/')
    except:
        return redirect('/')
    username= request.form['username']
    
    subquery="select count(1) as count FROM users WHERE userID = '"+username+"'"
    cursor2 =conn.cursor()
    cursor2.execute(subquery)
    usercount = cursor2.fetchall()
    usercount=usercount[0][0]
    if usercount==1:
        session['username'] = username
        cursor2.close()
        return redirect('/home')
    else:
        session.clear()
        cursor2.close()
        return redirect('/')

@app.route('/MyAccount', methods=['GET'])
def renderMyAccount():
    try:
        if session['username'] == 'BUSINESSUSER':
            return render_template('UserAccount.html')
        elif session['username'] == "":
            return redirect('/')
        else:
            
            username= session['username']
            subquery="select TotalVideos,TotalViews,MaxViewedVideo,MaxViews,MinViewedVideo,MinViews,TotalLengthInMinutes,AvgRating,TotalInDegree,MaxAgeInDays,MinAgeInDays,PopularityScore,friends as count FROM users WHERE userID = '"+username+"'"
            cursor2 =conn.cursor()
            cursor2.execute(subquery)
            data = cursor2.fetchall()
            if len(data)==1:
                user={"totVid":data[0][0],"totViews":data[0][1],"mostPopular":str(data[0][2])+" views "+str(data[0][3]),"leastPopular":str(data[0][4])+" views "+str(data[0][5]),"totMinutes":data[0][6],"avgRating":data[0][7],"totInDeg":data[0][8],"accountSince":data[0][9],"mostRecent":data[0][10],"popScore":data[0][11],"friends":data[0][12]}
    
            
            cursor2.close()
            return render_template('UserAccount.html',user=user)
    except:
        return redirect('/')

@app.route('/Analytics',methods=['GET'])
def renderanalyticsPage():
    try:
        if session['username'] == "":
            return redirect('/')
    except:
        return redirect('/')
    username= session['username']
    subquery="select kmeanlabel FROM users WHERE userID = '"+username+"'"
    cursor2 =conn.cursor()
    cursor2.execute(subquery)
    data = cursor2.fetchall()
    cluster=data[0][0]
    print(cluster)
    if cluster == 0:
        cluster = "an Irregular"
    elif cluster == 1:
        cluster = "an Occasional"
    elif cluster == 2:
        cluster = "a Regular"
    else:
        cluster="a Non"
    cursor2.close()
    return render_template('Analytics.html',cluster=cluster)

@app.route('/GetEstimation',methods=['POST'])
def estimateViews():
    
    try:
        if session['username'] == "":
            return redirect('/')
    except:
        return redirect('/')
    apikeys=loadApiKeys("prediction")
    api_key=apikeys['api_key']
    url=apikeys['url']
    if apikeys == None:
            print("Api Keys file has some issue")
            prediction="Some Error occured with api keys file"
            return_dict = {"prediction":prediction}
            return json.dumps(return_dict)
    else:
        frm_comments=request.json['frm_comments']
        frm_ratings=request.json['frm_ratings']
        frm_page_rank=request.json['frm_page_rank']
        frm_category=request.json['frm_category']
        
        data =  {

                "Inputs": {
        
                        "input1":
                        {
                            "ColumnNames": ["category", "numberRatings", "comments", "PageRank"],
                            "Values": [ [ frm_category, frm_ratings, frm_comments, frm_page_rank ] ]
                        },        },
                    "GlobalParameters": {
        }
            }
        
        body = str.encode(json.dumps(data))
        headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
        
        response = requests.post(url, data=body,headers=headers)
        #print(response.content)
            
        response_json=json.loads(response.content)
        #print(response_json)
        estimated_views=response_json['Results']['output1']['value']['Values'][0][4]
        
        if estimated_views == "":
            estimated_views = "Some error occured"
        print(estimated_views)
        return_dict = {"estimated_views":estimated_views}
        return json.dumps(return_dict)
        


@app.route('/graphDemo',methods=['GET'])
def renderGraphDemo():
    try:
        if session['username'] == "":
            return redirect('/')
    except:
        return redirect('/')
    return render_template('graphdemo.html')

@atexit.register
def shutdown():
    if conn:
        conn.close()
    if client:
        client.disconnect()

if __name__ == '__main__':
    app.secret_key = 'You Will Never Guess'
    app.run(host='0.0.0.0', port=port, debug=True)
