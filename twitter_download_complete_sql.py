#download twitter data with Twitter API
#save result into mySQL database
import time
from time import gmtime, strftime
import datetime
import io
import json  
import twitter

import matplotlib.pylab as plt
import sys
from urllib import unquote
from urllib2 import URLError
from httplib import BadStatusLine
from dateutil import parser
import mysql.connector
start_time = time.time()
mysqlUser='root'
mysqlPass='XXXX' #please change to your own password

delta_days=7 #after inital load, every time this run is to load up to 5 days of data

currentDate = datetime.date.today()
#print curtime 
currentDate=currentDate.strftime("%Y-%m-%d")
def oauth_login():
    # Go to http://twitter.com/apps/new to create an app and get values
    # for these credentials that you'll need to provide in place of these
    # empty string values that are defined as placeholders.
    # See https://dev.twitter.com/docs/auth/oauth for more information 
    # on Twitter's OAuth implementation.
    
    CONSUMER_KEY = 'sLBvZAxlLdXSGKl7kFXIu5S60'
    CONSUMER_SECRET ='xWzWUd5Xh3rKpqNvUvBCByyq02nbzjwH7fJItRkV43mNnoJv85'
    OAUTH_TOKEN = '3713549777-jLQQTUCybfnZMTcKSEhmfavSNdmDPmZDW8HKwaX'
    OAUTH_TOKEN_SECRET = 'mO6p8QklRNe5ToapEAGuRSwnBsZ2NrWhxP3Zj5oMmXnC6'

    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET,
                           CONSUMER_KEY, CONSUMER_SECRET)

    twitter_api = twitter.Twitter(auth=auth)
    
   
    
    return twitter_api
    
twitter_api=oauth_login()
print twitter_api
#from Mining the Social Web, 2nd Edition
def make_twitter_request(twitter_api_func, max_errors=10, *args, **kw): 
    # A nested helper function that handles common HTTPErrors. Return an updated
    # value for wait_period if the problem is a 500 level error. Block until the
    # rate limit is reset if it's a rate limiting issue (429 error). Returns None
    # for 401 and 404 errors, which requires special handling by the caller.
    def handle_twitter_http_error(e, wait_period=2, sleep_when_rate_limited=True):
    
        if wait_period > 3600: # Seconds
            print >> sys.stderr, 'Too many retries. Quitting.'
            raise e
    
        # See https://dev.twitter.com/docs/error-codes-responses for common codes
    
        if e.e.code == 401:
            print >> sys.stderr, 'Encountered 401 Error (Not Authorized)'
            return None
        elif e.e.code == 404:
            print >> sys.stderr, 'Encountered 404 Error (Not Found)'
            return None
        elif e.e.code == 429: 
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
            if sleep_when_rate_limited:
                print >> sys.stderr, "Retrying in 16 minutes...ZzZ..."
                sys.stderr.flush()
                time.sleep(60*16 + 5)
                print >> sys.stderr, '...ZzZ...Awake now and trying again.'
                return 2
            else:
                raise e # Caller must handle the rate limiting issue
        elif e.e.code in (500, 502, 503, 504):
            print >> sys.stderr, 'Encountered %i Error. Retrying in %i seconds' % \
                (e.e.code, wait_period)
            time.sleep(wait_period)
            wait_period *= 1.5
            return wait_period
        else:
            raise e

    # End of nested helper function
    
    wait_period = 2 
    error_count = 0 

    while True:
        try:
            return twitter_api_func(*args, **kw)
        except twitter.api.TwitterHTTPError, e:
            error_count = 0 
            wait_period = handle_twitter_http_error(e, wait_period)
            if wait_period is None:
                return
        except URLError, e:
            error_count += 1
            time.sleep(wait_period)
            wait_period *= 1.5
            print >> sys.stderr, "URLError encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise
        except BadStatusLine, e:
            error_count += 1
            time.sleep(wait_period)
            wait_period *= 1.5
            print >> sys.stderr, "BadStatusLine encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise

def save_json(filename, data):
    with io.open('twitter_{0}.json'.format(filename), 
                 'w', encoding='utf-8') as f:
        f.write(unicode(json.dumps(data, ensure_ascii=False)))
        f.close()

def get_twitter_summary(target): 
    print 'Retreiving Twitter Summary Information For:' + target
    response = make_twitter_request(twitter_api.users.lookup, 
                                    screen_name=target)
    #print json.dumps(response, indent=1)
    totalTweets=response[0]['statuses_count'] 
    totalFollowers=response[0]['followers_count'],
    totalFriends=response[0]['friends_count']
    print 'Total Tweets:', totalTweets, 'Total Followers:', totalFollowers[0], 'Total Friends:', totalFriends
    return totalTweets, totalFollowers, totalFriends
                       
def insert_business_tweets(business, tweet_id,time,tweet):
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
       
    cursor = cnx.cursor()
    try:
    
        query = "insert into twitter_tweets(business, tweet_id,created_dt,tweets) values (%s,%s,%s,%s)"
        cursor.execute(query,(business,tweet_id,time,json.dumps(tweet)))
        cursor.close()
        cnx.commit()
        cnx.close()
    except Exception:
        cnx.close()
        cursor.close()
#to get all the business' original tweets
def get_tweets_from_business(business):
    pastday = datetime.date.today() - datetime.timedelta(delta_days)
        
    #print curtime 
    pastDate=pastday.strftime("%Y-%m-%d")
    
    print 'Retreiving Tweets Information From:' + business  
    
    #construct a query
    q = 'from:' + business+' since:'+ pastDate

    #q = '@Macys since:2014-11-21' # business 
    print >> sys.stderr, 'Filtering the public timeline for track="%s"' % (q,)
    sys.stderr.flush()
    
    
    
    #specify tweets requested per search request and the total number of searches
    count = 100
    twitter_search_limit = 180
    try:
        search_results = twitter_api.search.tweets(q=q, count=count)
        statuses = search_results['statuses']
    except twitter.api.TwitterHTTPError, e:
            error_count = 0 
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
            print >> sys.stderr, "Retrying in 5 minutes...ZzZ..."
            sys.stderr.flush()
            time.sleep(60*16 + 5)
            search_results = twitter_api.search.tweets(q=q, count=count)
            statuses = search_results['statuses']

         
    #print status and statistics
    print 'initial search for',q,"completed"
    statuses = search_results['statuses']
    print "Length of statuses", len(statuses)
    
    #gather additional tweets 
    
    for _ in range(twitter_search_limit):
        print "Length of statuses", len(statuses)
        try:
            next_results = search_results['search_metadata']['next_results']
        except twitter.api.TwitterHTTPError, e:
            error_count = 0 
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
            print >> sys.stderr, "Retrying in 5 minutes...ZzZ..."
            sys.stderr.flush()
            time.sleep(60*16 + 5)
            
        except KeyError, e: # No more results when next_results doesn't exist
            break
            
        # Create a dictionary of statuses
        kwargs = dict([ kv.split('=') for kv in unquote(next_results[1:]).split("&") ])
        try:
            search_results = twitter_api.search.tweets(**kwargs)
        
        except twitter.api.TwitterHTTPError, e:
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
            print >> sys.stderr, "Retrying in 5 minutes...ZzZ..."
            sys.stderr.flush()
            time.sleep(60*16 + 5)
            search_results = twitter_api.search.tweets(**kwargs)
        
            
        #Append statuses from new search to existing dictionary
        statuses += search_results['statuses']
    
        
    #create another dictionary of statuses while JSON file reading issues are debugged
    backup = statuses
    
    
    #print statuses
    #report total number of tweets
    number_of_tweets = len(statuses)    
    
    #show status and statistics for debugging
    print "Twitter Search Completed"
    print "Length of statuses", number_of_tweets
    #Show one sample search result by slicing the list...
    #print json.dumps(statuses[0], indent=1)
    #below is the function to get the tweets directly from the business itself
    count=0
    realResult=set([])
    tweetsFromBusiness={}
    for status in statuses:
        screenName=status['user']['screen_name']
        replyto=status['in_reply_to_screen_name']
        #repplyto=status['in_reply_to_status_id_str']
        if (screenName==business) and (replyto is None):
            #print status['id_str']
            realResult.add(status['id_str'])
            tweetsFromBusiness.update({status['id_str']:status})
            #print status['text']
            count=count+1
    
    print 'Number of Tweets from Business:' + str(count)
    
    print len(tweetsFromBusiness), len(realResult)
        
    
        
    return tweetsFromBusiness


#it is to get all the orgininal business tweets' replies
def get_tweets_with_replies(business, tweetsfrombusiness):
    print 'Retreiving Tweets mentioning:' + business
    pastday = datetime.date.today() - datetime.timedelta(delta_days)
        
    #print curtime 
    pastDate=pastday.strftime("%Y-%m-%d")
    
    #get all teh tweets that mention the business 
    q = '@' + business+' since:'+ pastDate
    
    print q
    count = 100
    twitter_search_limit = 180
    try:
        search_results = twitter_api.search.tweets(q=q, count=count)
        statuses = search_results['statuses']
    except twitter.api.TwitterHTTPError, e:
        print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
        print >> sys.stderr, "Retrying in 5 minutes...ZzZ..."
        sys.stderr.flush()
        time.sleep(60*16 + 5)
        search_results = twitter_api.search.tweets(q=q, count=count)
        statuses = search_results['statuses']

       
    #print status and statistics
    print 'initial search for',q,"completed"
    statuses = search_results['statuses']
    print "Length of statuses", len(statuses)
    
            
    for _ in range(twitter_search_limit):
        print "Length of statuses", len(statuses)
        try:
            next_results = search_results['search_metadata']['next_results']
        except twitter.api.TwitterHTTPError, e:
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
            print >> sys.stderr, "Retrying in 5 minutes...ZzZ..."
            sys.stderr.flush()
            time.sleep(60*16 + 5)
        except KeyError, e: # No more results when next_results doesn't exist
            break
            
        # Create a dictionary of statuses
        kwargs = dict([ kv.split('=') for kv in unquote(next_results[1:]).split("&") ])
        try: 
            search_results = twitter_api.search.tweets(**kwargs)
        #Append statuses from new search to existing dictionary
            statuses += search_results['statuses']
        except twitter.api.TwitterHTTPError, e:
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
            print >> sys.stderr, "Retrying in 5 minutes...ZzZ..."
            sys.stderr.flush()
            time.sleep(60*16 + 5)
            search_results = twitter_api.search.tweets(**kwargs)
        #Append statuses from new search to existing dictionary
            statuses += search_results['statuses']
        
        
    #create another dictionary of statuses while JSON file reading issues are debugged
    backup = statuses
    
    
    #print statuses
    #report total number of tweets
    number_of_tweets = len(statuses)    
    
    #show status and statistics for debugging
    print "Twitter Search Completed"
    print "Length of statuses", number_of_tweets
    
    #Here is to get all the tweets directly reply to the business's orginal tweets
    count=0
    replyResult=set([])
    allReplies={}
    
    for status in statuses:
        screenName=status['user']['screen_name']
        #replyto=status['in_reply_to_screen_name']
        reply_to_id=status['in_reply_to_status_id_str']
        #if (screenName=="Macys") and (replyto is None):
        if (screenName <>business) and (reply_to_id is not None) and (reply_to_id <>'null'):
            if (allReplies.get(reply_to_id) is not None):
                oldList=allReplies.get(reply_to_id)
                #print oldList
                oldList.append(status)
                allReplies.update({reply_to_id:oldList})
            else:
                newList=[]
                newList.append(status)
                #print newList
                allReplies.update({reply_to_id:newList})   
            replyResult.add(status['in_reply_to_status_id_str'])
            count=count+1
    
    print count
    
    #print statuses
    print replyResult, len(replyResult)
    
    #get the interesection of the oginal tweets and reply tweets
    #those are the tweets that are replied directly to the original tweets
    tweetsWithReply=replyResult.intersection(tweetsfrombusiness)
    
    print tweetsWithReply, len(tweetsWithReply)
    return allReplies
        
def insert_tweet_like(business, post_id,time,like):
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()
    try:
        query = "insert into twitter_tweets_like(business, tweet_id,date,like_count) values (%s,%s,%s,%s)"
        cursor.execute(query,(business,post_id,time,like))
        cursor.close()
        cnx.commit()
        cnx.close()
    except Exception:
        cnx.close()
        cursor.close()

from datetime import datetime    


def insert_post_shared(business, post_id,time,shared):
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()
    try:
        query = "insert into twitter_tweets_shared(business, tweet_id,date,shared_count) values (%s,%s,%s,%s)"
        cursor.execute(query,(business,post_id,time,shared))
        cursor.close()
        cnx.commit()
        cnx.close()
    except Exception:
        cnx.commit()
        cnx.close()
        

#this is the function to get the total likes of a post
def get_total_post_likes(post_id):
    totalCount=post_likes.get(post_id)
    return totalCount

def get_shared_posts(post_id):
    totalCount=post_sharedposts.get(post_id)
    return totalCount


def get_post_comments(post_id):
    comments=post_comments.get(post_id)
    return comments
#it is to get all comments of a post
#it returns total count as well as all the comments contents


#get sentiment of a string

def insert_post_comments(business, post_id,comment_id, comment,created_dt):
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()
    try:
        query = "insert into twitter_tweets_comments(business, tweet_id,comment_id, comment,created_dt) values (%s,%s,%s,%s,%s)"
        cursor.execute(query,(business,post_id,comment_id, comment,created_dt))
        cursor.close()
        cnx.commit()
        cnx.close()
    except Exception:
        cnx.commit()
        cnx.close()

            
#get all the business
def get_business_list():

    business_list=[]
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()

    query = ("select twitter_account from fb_business where twitter_account<>''")


    cursor.execute(query)


    for name in cursor:
        business_list.append(name)

    cursor.close()
    cnx.close()
    return business_list

#delete the existing business like for a particular date
def delete_business_summary(time):

  
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()

    query = "delete from twitter_business_total where date= '" + time + "'"
    cursor.execute(query,time)
    cursor.close()
    cnx.commit()
    cnx.close()

#insert the total business like        
def insert_business_summary(business, tweetscount,followingcount,followerscount,time):

  
#do all the analysis with files
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
 
    cursor = cnx.cursor()

    query = "insert into twitter_business_total(business, tweets_count,following_count, followers_count,date) values (%s,%s,%s,%s,%s)"
    cursor.execute(query,(business,tweetscount,followingcount,followerscount,time))
    cursor.close()
    cnx.commit()
    cnx.close()

  
allBusiness=get_business_list()
print allBusiness

import datetime
today = datetime.date.today()
format='%Y-%m-%d'
todayString= today.strftime(format)
delete_business_summary(todayString)
for business in allBusiness:
    print 'Starting to populate twitter summary data for' + business[0]
    tweetscount, followerscount, followingcount =get_twitter_summary(business[0])
    print business[0],tweetscount,followingcount, followerscount, todayString
    insert_business_summary(business[0],tweetscount,followingcount[0], followerscount, todayString)

#business_list=['macys']

#allBusiness=['Target', 'Walmart', 'Macys', 'jcpenney']
for business_str in allBusiness:
    business=business_str[0]
    print 'Starting to download data for:' + business_str[0]
    post_likes={}
    post_message={}
    post_comments={}
    post_commentscount={}
    post_sharedposts={}
    post_commentsentiment={}
    start_time = time.time()
    tweetsFromBusiness=get_tweets_from_business(business)
    for tweet_id in tweetsFromBusiness:
        #print post
        tweet=tweetsFromBusiness.get(tweet_id)
        created_tm=tweet['created_at']
        date = parser.parse(created_tm)
        created_tm=date.strftime("%Y-%m-%d")
        insert_business_tweets(business,tweet_id, created_tm, tweet) 
    print 'Downloaded: ' + str(len(tweetsFromBusiness)) +' business tweets: ' ' for ' +business
    
    end_time = time.time()
    
    print 'total time to take to get tweets:' + str(end_time-start_time) + ' seconds'
    
    start_time = time.time()
    
    allReplies=get_tweets_with_replies(business, tweetsFromBusiness)
    
    end_time = time.time()
    
    print 'total time to take to get tweets replies:' + str(end_time-start_time) + ' seconds'
    start_time = time.time()
    for tweet_id in tweetsFromBusiness:
        tweet=tweetsFromBusiness.get(tweet_id)
        post_likes.update({tweet['id_str']:tweet['favorite_count']})
        post_sharedposts.update({tweet['id_str']:tweet['retweet_count']})
        tweet_replies=allReplies.get(tweet['id_str'])
        if tweet_replies is not None:
            post_comments.update({tweet_id:tweet_replies})
          
    print 'Populating tweet like data to database...'
    for tweet_id in post_likes:
        like=post_likes.get(tweet_id)
        insert_tweet_like(business, tweet_id, todayString, like)

    print 'Populating tweet shared data to database...'
    for tweet_id in post_sharedposts:
        shared=post_sharedposts.get(tweet_id)
        insert_post_shared(business, tweet_id, todayString, shared)


    print 'Populating tweets replies data to database...'
    for tweet_id in post_comments:
        comments=post_comments.get(tweet_id)
        if comments is not None:
            for comment in comments:
                created_tm=comment['created_at']
                date = parser.parse(created_tm)
                created_tm=date.strftime("%Y-%m-%d")
                comment_id=comment['id_str']
                post_id=comment['in_reply_to_status_id_str']
                insert_post_comments(business, post_id, comment_id, json.dumps(comment),created_tm)  

    end_time = time.time()
    print 'total time to take to populating post data into database:' + str(end_time-start_time) + ' seconds'
    
    print 'saving data into files...'
    
    #save tweets from business
    save_json('data/' + business+'_tweets'+todayString, tweetsFromBusiness)
    save_json('data/' + business+'_replies'+ todayString, post_comments)
    save_json('data/' + business+'_likes'+ todayString, post_likes)
    save_json('data/' + business+'_shared'+ todayString, post_sharedposts)
                                                                
    print 'Completed the download for:' + business
