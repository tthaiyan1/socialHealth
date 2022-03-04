

# Social Network Analysis for Small Business
#
#Demonstration Web Application
#Illustrates output of social media analsyis 
#and proposed layout of web site
#the backend analsysis was done with python, mysql 
# Flask Web Framework
# Boostrap CSS template
# Static plots generated using matplotlib
#



from flask import Flask, render_template, session, redirect, url_for, flash, request
from flask.ext.script import Manager
from flask.ext.bootstrap import Bootstrap
from flask.ext.moment import Moment
from flask.ext.wtf import Form
from wtforms import StringField, IntegerField, RadioField, SubmitField, SelectField
from wtforms.validators import Required, NumberRange



#modules for data collecton and analysis 
import facebook
import requests
import time
from time import gmtime, strftime
import datetime
from guess_language import guess_language
from textblob import TextBlob
import json  
import collections
import pandas as pd

import mysql.connector
from guess_language import guess_language
from textblob import TextBlob



access_token='170909756591306|0Crp0iDzlsPF-z_cIo0eGMFH2ek'



#=========================================================================================
app = Flask(__name__)
app.config['SECRET_KEY'] = 'dhdhggdsjhgfjgyterewbejkdkdndhdhdgsbddddddd31415'
app.debug = True
manager = Manager(app)
bootstrap = Bootstrap(app)
moment = Moment(app)

default_image_width = "120px"

image_width_small = "120px"
image_height_small = "200px"

image_width_large = "600px"
image_height_large= "600px"

#=========================================================================================

            

            
def load_posts_data(business):
    posts=[]
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()
    query = "SELECT posts from fb_posts where business = '" + business + "'"
    cursor.execute(query)
    for post in cursor:
        posts.append(post)
    cursor.close()
    cnx.close()    
    return posts

def load_posts_like(business):
    post_likes={}
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()
    query = "SELECT post_id,like_count from fb_post_like where business = '" + business + "'"
    cursor.execute(query)
    for post_id, like_count in cursor:
        post_likes.update({post_id:like_count})
    cursor.close()
    cnx.close()    
    return post_likes    

def load_posts_shared(business):
    post_shared={}
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()
    query = "SELECT post_id,shared_count from fb_post_shared where business = '" + business + "'"
    cursor.execute(query)
    for post_id, like_count in cursor:
        post_shared.update({post_id:like_count})
    cursor.close()
    cnx.close()    
    return post_shared 

def load_posts_comments(business):
    posts={}
    comments=[]
    cnx = mysql.connector.connect(user=mysqlUser, password=mysqlPass, database='mydb')
    cursor = cnx.cursor()
    query = "SELECT post_Id,comment from fb_post_comments where business = '" + business + "' order by post_id"
    cursor.execute(query)
    oldpost_id=''
    for post_id, comment in cursor:
        if (post_id==oldpost_id):
            comments=posts.get(post_id)
            comments.append(comment)
            posts.update({post_id:comments})
        else:
            #create a dictionary
            #key is the post_Id, value is all the comments of the list
            #so update the dictionary only if the post_id changed
            comments=[]
            comments.append(comment)
            oldpost_id=post_id
            if (oldpost_id<>''):
                posts.update({oldpost_id:comments})
            
    cursor.close()
    cnx.close()    
    return posts
posts = load_posts_data(business_id)




posts_shared=load_posts_shared(business_id)


posts_comments =load_posts_comments(business_id)


#this is the function to get the total likes of a post
def get_total_post_likes(post_id):
    totalCount=posts_likes.get(post_id)
    return totalCount

def get_shared_posts(post_id):
    totalCount=posts_shared.get(post_id)
    return totalCount


def get_post_comments(post_id):
    comments=posts_comments.get(post_id)
    return comments
#it is to get all comments of a post
#it returns total count as well as all the comments contents



def get_sentiment(text): #get sentiment of a string
    nlpblob = TextBlob(text)
    nlpblob.sentiment
    return nlpblob.sentiment.polarity

def get_overall_sentiment(comments):
    #get overall sentiment of comments
    #comments is a list of dictionary    
    if (len(comments)==0):
        return 0
    totalSentiment=0
    for comment in comments:
        jsontext=json.loads(comment)
        value=jsontext.get('message') #get the comment content
        lan=guess_language.guessLanguage(value)
        #only do the sentiment analysis for English comments
        if (lan=='en' or lan =='UNKNOWN'): 
            totalSentiment=get_sentiment(value)+totalSentiment    
    #average sentiment for this post
    return totalSentiment/len(comments)           

def construct_post_dict():
    # construct a dictionary with post_id and the message of the post
    counter=0
    for post in posts:
        jsontext=json.loads(post[0])
        value=jsontext.get('message')
        post_id=jsontext.get('id')
        post_message.update({post_id:value})
        counter=counter+1

def construct_comment_count_dict():
    #construct a dictionary with post_id and the total count of the post's comments
    #returns of list with commentcounts and sentinment dictionaries
    for post in posts:
        jsontext=json.loads(post[0])
        value=jsontext.get('message')
        post_id=jsontext.get('id')
        comments=posts_comments.get(post_id)
        if comments is not None:
            post_commentscount.update({post_id:len(comments)})
            if(len(comments)>0):
                sentimentScore=get_overall_sentiment(comments)
                post_commentsentiment.update({post_id:sentimentScore})
    
    list_of_dicts = []           
    list_of_dicts = [post_commentscount,post_commentsentiment]
    return list_of_dicts     
        
def most_liked_posts(posts_likes):
    #print the top ten most liked posts
    popular_posts = collections.Counter(posts_likes)
    counter=0
    df = pd.DataFrame(columns=['Most Popular Posts', 'Count']) 
    pd.set_option('max_colwidth',65)
    for post_id, count in popular_posts.most_common(10):
        #print post_id, count
        df.loc[counter]=[post_message.get(post_id),count]
        counter=counter+1
    ten_most_liked_posts = df
    return ten_most_liked_posts  


def most_shared_posts(posts_shared):
    #print the top ten most shared posts
    shared_most_posts = collections.Counter(posts_shared)
    df = pd.DataFrame(columns=['Most Shared Posts', 'Count']) 
    counter=0
    for post_id, count in shared_most_posts.most_common(10):
        df.loc[counter]=[post_message.get(post_id),count]
        counter=counter+1
    ten_most_shared_posts = df
    return ten_most_shared_posts

def most_commented_posts(post_commentscount):
    #print the top ten most commented posts
    commented_most_posts = collections.Counter(post_commentscount)
    ten_most_commented_posts = {}
    for post_id, count in commented_most_posts.most_common(10):
        ten_most_commented_posts['post_message'] = [post_message.get(post_id),count]
        counter=counter+1
    return ten_most_commented_posts

def most_positive_posts(post_commentsentiment):
    #print the top ten most postive comments posts
    postive_most_posts = collections.Counter(post_commentsentiment)
    df = pd.DataFrame(columns=['Most Positive Posts', 'Score']) 
    counter=0
    for post_id, count in postive_most_posts.most_common(10):
        df.loc[counter]=[post_message.get(post_id),count]
        counter=counter+1
    ten_most_positive_posts = df
    return ten_most_positive_posts


def most_negative_posts(post_commentsentiment):        
    #print the top ten least postive comments posts
    postive_most_posts = collections.Counter(post_commentsentiment)
    df = pd.DataFrame(columns=['Most Negative Posts', 'Score']) 
    counter=0
    for post_id, count in postive_most_posts.most_common()[:-11:-1]:
        #print post_id, count
        df.loc[counter]=[post_message.get(post_id),count]
        counter=counter+1
    
    ten_most_negative_posts = df
    return ten_most_negative_posts










#=========================================================================================
#FORMS
#=========================================================================================

main_menu = [('one','Analyze a Single Business'),
    ('market_fb','Market Analysis - Luxury Cars - Facebook'),
    ('market_tw','Market Analysis - Retail - Twitter'),
    ('all_fb','Analysis of Multiple Markets - Facebook'),
    ('all_tw','Analysis of Multiple Markets - Twitter'),
    ('networks','Relative Impact of Different Social Networks')]

business_list = [('bmw','BMW')]
#business_list = [('audi', 'Audi'), ('bmw','BMW'), ('mb','Mercedes-Benz'), ('lexus','Lexus'), ('jcp',"JC Penney's"),('target','Target'), ('visa','Visa'), ('mc','MasterCard'), ('amex','American Express')]

social_network_list = [('facebook','Facebook'),
    ('twitter','Twitter')]

analysis_list = [('overview','Overview'),
    ('ml','Most Liked'),
    ('ms','Most Shared'),
    ('mp','Most Positive'),
    ('mn','Most Negative'),
    ('test','TEST')]


class MainMenu(Form):
    scope = SelectField('Select Scope', choices=main_menu, validators=[Required()])
    submit = SubmitField('Next')

class NameForm(Form):
    name = SelectField('Business name?', choices=business_list, validators=[Required()])
    submit = SubmitField('Next')

class StartForm(Form):
    social_network = SelectField('Social Network?', choices =social_network_list, validators=[Required()])
    days = IntegerField('How far back do you want to look (days)?', 
        validators=[Required(), NumberRange(min=1, max=7, message='Please enter a integer between 1 and 7')])
    submit = SubmitField('Next')

class SelectAnalysis(Form):
    analysis_type = SelectField('Analysis', choices = analysis_list, validators=[Required()])
    submit = SubmitField('Next')

#=========================================================================================
#View Functions
#=========================================================================================

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500

@app.route('/', methods=['GET', 'POST'])
def main_menu():
    form = MainMenu()
    session['scope'] = form.scope.data
    #NOTE TO SELF - it would be simpler to align names in list with view function names and 
    #then return redirect(url_for(session['scope'])) 
    if form.validate_on_submit():
        if session['scope'] == 'one':
            return redirect(url_for('one'))
        elif session['scope'] == 'market_fb':
            return redirect(url_for('markets'))
        elif session['scope'] == 'market_tw':
            return redirect(url_for('markets'))
        elif session['scope'] == 'networks':
            return redirect(url_for('networks'))
        else:
            return redirect(url_for('charts'))
    return render_template('main_menu.html', form=form)

@app.route('/one', methods=['GET', 'POST'])
def one():
    form = NameForm()
    if form.validate_on_submit():
        session['name'] = form.name.data
        return redirect(url_for('start'))
    return render_template('one.html', form=form, name=session.get('name'), days=session.get('days'))


@app.route('/start', methods=['GET','POST'])
def start():
    name = session['name']
    #days = session['days']
    #social_network = session['social_network']
    b_n = [item[1] for item in business_list if item[0] == name]
    b_n = str(b_n[0]).strip('[]')
    form = StartForm()
    if form.validate_on_submit():
        session['days'] = form.days.data
        session['social_network'] = form.social_network.data
        return redirect(url_for('download'))   
    return render_template('start.html', form=form, name=b_n)


@app.route('/download', methods=['GET','POST'])
def download():
    name = session['name']
    days = session['days']
    #get_totalpost(name,days)
    social_network = session['social_network'] 
    s_n = [item[1] for item in social_network_list if item[0] == social_network]
    s_n = str(s_n[0]).strip('[]')
    b_n = [item[1] for item in business_list if item[0] == name]
    b_n = str(b_n[0]).strip('[]')
    form = SelectAnalysis()
    if request.method == 'POST':
        session['analysis_type'] = form.analysis_type.data
        #session['fb_posts'] = get_totalpost(name,days)
        
        return redirect(url_for('analysis'))
    elif request.method == 'GET':
        return render_template('download.html', 
            form=form, 
            name = b_n, 
            days = days, 
            social_network = s_n)

@app.route('/analysis')
def analysis():
    analysis_type = session['analysis_type'] 
    
    s_n = [item[1] for item in social_network_list if item[0] == session['social_network']]
    s_n = str(s_n[0]).strip('[]')
    b_n = [item[1] for item in business_list if item[0] == session['name']]
    b_n = str(b_n[0]).strip('[]')
    a_t = [item[1] for item in analysis_list if item[0] == session['analysis_type']]
    a_t = str(a_t[0]).strip('[]')
    a_t = a_t.lower()
    analysis_result = "TEST RESULTS"
    if session['analysis_type'] == 'overview':
        return render_template('charts_fb_bmw.html')
    if session['analysis_type'] == 'test':
        return render_template('analysis_results.html', 
        name = b_n,
        days = session['days'], 
        social_network = s_n,
        analysis_type = a_t,
        analysis_result = session['fb_posts'])
    return render_template('analysis_results.html', 
        name = b_n,
        days = session['days'], 
        social_network = s_n,
        analysis_type = a_t,
        analysis_result = analysis_result)

@app.route('/charts')
def charts():
    scope = session['scope']     
    if scope == 'all_fb':
        return render_template('charts_fb.html',
            image_width_small = image_width_small,
            image_height_small = image_height_small,
            image_width_large = image_width_large,
            image_height_large= image_height_large,)
    if scope == 'all_tw':
        return render_template('charts_tw.html', 
            image_width_small = image_width_small,
            image_height_small = image_height_small,
            image_width_large = image_width_large,
            image_height_large= image_height_large,)
    return redirect(url_for('main_menu'))

@app.route('/markets')
def markets():
    scope = session['scope']     
    if scope == 'market_fb':
        return render_template('charts_market_fb.html',
            image_width_small = image_width_small,
            image_height_small = image_height_small,
            image_width_large = image_width_large,
            image_height_large= image_height_large,)
    if scope == 'market_tw':
        return render_template('charts_market_tw.html', 
            image_width_small = image_width_small,
            image_height_small = image_height_small,
            image_width_large = image_width_large,
            image_height_large= image_height_large,)
    return redirect(url_for('main_menu'))

@app.route('/networks')
def networks():
    return render_template('charts_fb_v_tw.html')
 
#=========================================================================================
#MAIN
#=========================================================================================
    
if __name__ == '__main__':
    manager.run()
