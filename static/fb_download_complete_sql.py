import facebook
import requests
import time
from time import gmtime, strftime
import datetime
import json  

from guess_language import guess_language
from textblob import TextBlob
import mysql.connector
start_time = time.time()
mysqlUser='root'
mysqlPass='XXX;' #please change to your own password

access_token='170909756591306|0Crp0iDzlsPF-z_cIo0eGMFH2ek'
delta_days=1 #after inital load, every time this run is to load up to 5 days of data
#it is to get total posts of a business
#it returns total count as well as all the post contents
def get_totalpost(business):

    user = business
    pastday = datetime.date.today() - datetime.timedelta(delta_days)
    unix_time= pastday.strftime("%s")
    
    
    print unix_time, pastday.strftime("%Y-%m-%d")
    
    
    
    graph = facebook.GraphAPI(access_token)
    #graph = facebook.GraphAPI(access_token,version='2.5')
    profile = graph.get_object(user)
    posts = graph.get_connections(profile['id'], 'posts?since='+str(unix_time)) #get the post for up to 30 days
    
    totalCount=0
    text=[]
# Wrap this block in a while loop so we can keep paginating requests until
# finished.
    while True:
        try:
        # Perform some action on each post in the collection we receive from
        # Facebook.
       #[some_action(post=post) for post in posts['data']]
        # Attempt to make a request to the next page of data, if it exists.
            for post in posts['data']:
                text.append(post)
            totalCount=totalCount+ len(posts['data'])
            with requests.Session() as s:
                posts = s.get(posts['paging']['next']).json()
        
       # totalCount=totalCount+1
        except KeyError:
        # When there are no more pages (['paging']['next']), break from the
        # loop and end the script.
            break

    return totalCount, text


#this is the function to get the total likes of a post
def get_total_post_likes(post):


    base_url = 'https://graph.facebook.com/'+post

    fields = 'likes?summary=true'

    url = '%s/%s&access_token=%s' % \
        (base_url, fields, access_token,)

# This API is HTTP-based and could be requested in the browser,
# with a command line utlity like curl, or using just about
# any programming language by making a request to the URL.
# Click the hyperlink that appears in your notebook output
# when you execute this code cell to see for yourself...
    #print url

# Interpret the response as JSON and convert back
# to Python data structures
    with requests.Session() as s:
        content = s.get(url).json()
    if content is not None:
        return content['summary']['total_count']

#it is to get total number of a sharedpost for a particular post
#it returns total count 
def get_shared_posts(post):

# You'll need an access token here to do anything.  You can get a temporary one
# here: https://developers.facebook.com/tools/explorer/
    
    user = post

    graph = facebook.GraphAPI(access_token,version='2.5')
    profile = graph.get_object(user)
    posts = graph.get_connections(profile['id'], 'sharedposts')
    totalCount=0
    text=[]
# Wrap this block in a while loop so we can keep paginating requests until
# finished.
    while True:
        try:
        # Perform some action on each post in the collection we receive from
        # Facebook.
       #[some_action(post=post) for post in posts['data']]
        # Attempt to make a request to the next page of data, if it exists.
            for post in posts['data']:
                text.append(post)
            totalCount=totalCount+ len(posts['data'])
            with requests.Session() as s:
                posts = s.get(posts['paging']['next']).json()
            
       # totalCount=totalCount+1
        except KeyError:
        # When there are no more pages (['paging']['next']), break from the
        # loop and end the script.
            break
    #remove all the non-english comments for furture sentiment analysis
   
    return totalCount


#it is to get all comments of a post
#it returns total count as well as all the comments contents

def get_post_comments(post):
    user = post
    pastday = datetime.date.today() - datetime.timedelta(delta_days)
    unix_time= pastday.strftime("%s")
    
    graph = facebook.GraphAPI(access_token,version='2.5')
    profile = graph.get_object(user)
    posts = graph.get_connections(profile['id'], 'comments')
    comments=[]
# Wrap this block in a while loop so we can keep paginating requests until
# finished.
    while True:
        try:
        # Perform some action on each post in the collection we receive from
        # Facebook.
       #[some_action(post=post) for post in posts['data']]
        # Attempt to make a request to the next page of data, if it exists.
            for post in posts['data']:
                comments.append(post)
            with requests.Session() as s:
                posts = s.get(posts['paging']['next']).json()
        
       # totalCount=totalCount+1
        except KeyError:
        # When there are no more pages (['paging']['next']), break from the
        # loop and end the script.
            break
    return comments

#get sentiment of a text
def get_sentiment(text):
    nlpblob = TextBlob(text)
    nlpblob.sentiment
    return nlpblob.sentiment.polarity

#get overall sentiment of comments
#comments is a list of dictionary    
def get_overall_sentiment(comments):
    if (len(comments)==0):
        return 0
    totalSentiment=0
    for comment in comments:
        value=comment.get('message') #get the comment content
        lan=guess_language.guessLanguage(value)
        #only do the sentiment analysis for English comments
        if (lan=='en' or lan =='UNKNOWN'): 
            totalSentiment=get_sentiment(value)+totalSentiment
        #average sentiment for this post
    return totalSentiment/len(comments)           

#get total likes of a business
def get_total_likes(business):


    base_url = 'https://graph.facebook.com/'+business

    fields = 'likes'

    url = '%s?fields=%s&access_token=%s' % \
        (base_url, fields, access_token,)

# This API is HTTP-based and could be requested in the browser,
# with a command line utlity like curl, or using just about
# any programming language by making a request to the URL.
# Click the hyperlink that appears in your notebook output
# when you execute this code cell to see for yourself...
    
# Interpret the response as JSON and convert back
# to Python data structures
    content = requests.get(url).json()

# Pretty-print the JSON and display it
    #print json.dumps(content, indent=1)
    return content['likes']

#get all the business
def get_business_list():

    business_list=[]
#do all the analysis with files
    cnx = mysql.connector.connect(user='root', password='Libo1922;', database='mydb')
    cursor = cnx.cursor()

    query = ("select name from fb_business")


    cursor.execute(query)


    for name in cursor:
        business_list.append(name)

    cursor.close()
    cnx.close()
    return business_list

#delete the existing business like for a particular date
def delete_business_like(time):

  
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()

    query = "delete from fb_business_total_like where date= '" + time + "'"
    cursor.execute(query,time)
    cursor.close()
    cnx.commit()
    cnx.close()

#insert the total business like        
def insert_business_like(business, count,time):

  
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
 
    cursor = cnx.cursor()

    query = "insert into fb_business_total_like(business, like_count,date) values (%s,%s,%s)"
    cursor.execute(query,(business,count,time))
    cursor.close()
    cnx.commit()
    cnx.close()

#load a json data            
def load_data(file_name):
    return json.loads(open(file_name).read())

#insert business posts for a date
def insert_business_post(business, post_id,time,post):
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
       
    cursor = cnx.cursor()
    try:
    
        query = "insert into fb_posts(business, post_id,created_dt,posts) values (%s,%s,%s,%s)"
        cursor.execute(query,(business,post_id,time,json.dumps(post)))
        cursor.close()
        cnx.commit()
        cnx.close()
    except Exception:
        cnx.close()
        cursor.close()

#insert a post's like counts
def insert_post_like(business, post_id,time,like):
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()
    try:
        query = "insert into fb_post_like(business, post_id,date,like_count) values (%s,%s,%s,%s)"
        cursor.execute(query,(business,post_id,time,like))
        cursor.close()
        cnx.commit()
        cnx.close()
    except Exception:
        cnx.close()
        cursor.close()

#insert a facebook's shared counts
def insert_post_shared(business, post_id,time,shared):
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()
    try:
        query = "insert into fb_post_shared(business, post_id,date,shared_count) values (%s,%s,%s,%s)"
        cursor.execute(query,(business,post_id,time,shared))
        cursor.close()
        cnx.commit()
        cnx.close()
    except Exception:
        cnx.commit()
        cnx.close()

#insert a facebook post's comments
def insert_post_comments(business, post_id,comment_id, comment,created_dt):
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()
    try:
        query = "insert into fb_post_comments(business, post_id,comment_id, comment,created_dt) values (%s,%s,%s,%s,%s)"
        cursor.execute(query,(business,post_id,comment_id, comment,created_dt))
        cursor.close()
        cnx.commit()
        cnx.close()
    except Exception:
        cnx.commit()
        cnx.close()


  
allBusiness=get_business_list()
print allBusiness

import datetime
today = datetime.date.today()
format='%Y-%m-%d'
todayString= today.strftime(format)
delete_business_like(todayString)
for business in allBusiness:
    print 'Starting to populate summary data for' + business[0]
    count=get_total_likes(business[0])
    print business[0],count,todayString
    insert_business_like(business[0],count,todayString)

#business_list=['macys']
for business in allBusiness:
    business_id=business[0]
    print 'Starting to download data for:' + business[0]
    post_likes={}
    post_message={}
    post_comments={}
    post_commentscount={}
    post_sharedposts={}
    post_commentsentiment={}
    start_time = time.time()
    total, posts=get_totalpost(business_id)
    print 'Downloaded' + str(total) +' for' +business_id
    end_time = time.time()
    print 'total time to take to get post:' + str(end_time-start_time) + ' seconds'
    
    start_time = time.time()
    print 'Saving post data to file...'
    
    with open('fb'+business_id+todayString+'posts.json', 'w') as outfile:
                json.dump(posts, outfile)
                     
    print 'Populating post data to database...'
    for post in posts:
        created_tm=post.get('created_time')
        created_tm=created_tm[:10]
        post_id=post.get('id')
        insert_business_post(business_id,post_id, created_tm, post) 

    #construct a dictionary with post_id and the message of the post
    start_time = time.time()
    #construct a dictionary with post_id and the likes of the post, shared posts
    for text in posts:
            value=text.get('message')
            post_id=text.get('id')
         #   likes=get_total_post_likes(post_id)
         #   post_likes.update({post_id:likes})
    end_time = time.time()
    print post_likes
    
    with open('fb' + business_id+todayString+'posts_likes.json', 'w') as outfile:
                json.dump(post_likes, outfile)
    print 'Populating post like data to database...'
       
    for post_id in post_likes:
        like=post_likes.get(post_id)
        insert_post_like(business_id, post_id, todayString, like)
                        
    print 'total time to take to get post likes:' + str(end_time-start_time) + ' seconds'  
    
    start_time = time.time()
    #construct a dictionary with post_id and the shared posts
    for text in posts:
        try:
            value=text.get('message')
            post_id=text.get('id')
            shared=get_shared_posts(post_id)
            post_sharedposts.update({post_id:shared})
        except Exception:
            print 'Error to get shared post for' + post_id
            continue    
    
        
    with open('fb' + business_id+todayString+'posts_shared.json', 'w') as outfile:
                json.dump(post_sharedposts, outfile)
                
    print 'Populating post shared data to database...'
   
    for post_id in post_sharedposts:
        shared=post_sharedposts.get(post_id)
        insert_post_shared(business_id, post_id, todayString, shared)
                    
    end_time = time.time()
    print 'total time to take to get shared post:' + str(end_time-start_time) + ' seconds'              
    start_time = time.time()
    counter=0
    faileCounter=0
    for text in posts:   
            try:
                value=text.get('message')
                post_id=text.get('id')
                print 'processing: '+ post_id
                print counter, post_id
                comments=get_post_comments(post_id)
            #shared_posts=get_shared_posts(post_id)
            #post_sharedposts.update({post_id:shared_posts})
                post_comments.update({post_id:comments})
        #    sentimentScore=get_overall_sentiment(comments)
        #    post_commentsentiment.update({post_id:sentimentScore})
                counter=counter+1
            except Exception:
                print 'Exception on processing' + post_id
                faileCounter=faileCounter+1
                continue #just continue
    print len(post_comments)       
    end_time = time.time()
    print 'total time to take to get post comments' + str(end_time-start_time) + ' seconds'        
    
    with open('fb' + business_id+todayString+'comments.json', 'w') as outfile:
        json.dump(post_comments, outfile)   
    
    print 'Populating post comments data to database...'
    
    for text in posts:
        post_id=text.get('id')
        comments=post_comments.get(post_id)
        if comments is not None:
            for comment in comments:
                created_tm=comment.get('created_time')
                created_tm=created_tm[:10]
                comment_id=comment.get('id')
                insert_post_comments(business_id, post_id, comment_id, json.dumps(comment),created_tm)  
                                            
    print 'Completed the download for' + business_id
