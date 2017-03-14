flog = open('/home/pi/zack/send_scheduled_tweet_log', 'w')
flog.write("Start\n")

import time, os, datetime, sys, psutil, random
import re
import MySQLdb
import subprocess
import telegram
import glob
from bs4 import BeautifulSoup
#import requests
#import httplib2
from BeautifulSoup import BeautifulSoup, SoupStrainer
reload(sys)
sys.setdefaultencoding("utf-8")

from twython import Twython
from twython import TwythonAuthError, TwythonError

import errno
from socket import error as socket_error

from subprocess import Popen, PIPE

import clipboard
sys.path.insert(0, "../ZackInc")
import zack_inc as zc


bot_token = "308120049:AAFBSyovjvhlYAe1xeTO2HAvYO4GBY3xudc"

def mysql_connect():
    cnx = None
    try:
        
        # Connecting from an external network.
        # Make sure your network is whitelisted
        #logging.info("Connecting via remote %s..." % app.config['DBNAME'])
        client_cert_pem = "instance/client_cert_pem"
        client_key_pem = "instance/client_key_pem"
        ssl = {'cert': client_cert_pem, 'key': client_key_pem}
         
        host = "169.254.184.34"
        local_or_remote = open('/home/pi/zack/local_or_remote', 'r').read()
        if local_or_remote == "remote":
            host = "127.0.0.1"
                    
        print("Connect on %s" % host)
        cnx = MySQLdb.connect(
            host=host,
            port=3306,
            user='root', passwd='password', db='monoprice', charset="utf8", use_unicode=True)
        
        #logging.info("Success = %s" % str(res[0]))
        response = "Success!"
    except Exception as err:
        response = "Failed."
        print("Connection error: %s" % err)
        
    return cnx, response



def keypress(sequence):
    p = Popen(['xte'], stdin=PIPE)
    p.communicate(input=sequence)


def authenticate_me(whoami):
    if whoami == "zack":
        key_data = open('/home/pi/zack/zackcapozzi_twitter_keys', 'r').read()
        r1 = re.compile(r'CONSUMER_KEY \= \'(.*?)\'')
        r2 = re.compile(r'CONSUMER_SECRET \= \'(.*?)\'')
        r3 = re.compile(r'ACCESS_KEY \= \'(.*?)\'')
        r4 = re.compile(r'ACCESS_SECRET \= \'(.*?)\'')
        CONSUMER_KEY = r1.search(key_data).group(1)
        CONSUMER_SECRET = r2.search(key_data).group(1)
        ACCESS_KEY = r3.search(key_data).group(1)
        ACCESS_SECRET = r4.search(key_data).group(1)
        key_data = None
    elif whoami == "lacrosse_reference":
        key_data = open('/home/pi/zack/lacrossereference_twitter_keys', 'r').read()
        r1 = re.compile(r'CONSUMER_KEY \= \'(.*?)\'')
        r2 = re.compile(r'CONSUMER_SECRET \= \'(.*?)\'')
        r3 = re.compile(r'ACCESS_KEY \= \'(.*?)\'')
        r4 = re.compile(r'ACCESS_SECRET \= \'(.*?)\'')
        CONSUMER_KEY = r1.search(key_data).group(1)
        CONSUMER_SECRET = r2.search(key_data).group(1)
        ACCESS_KEY = r3.search(key_data).group(1)
        ACCESS_SECRET = r4.search(key_data).group(1)
        key_data = None
    else:
        return None
    return Twython(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET)

flog.write("\n\nSearch for scheduled tweets\n------------------------------\n")

mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()
now = datetime.datetime.now()
query = "Select ID, send_time, send_as, msg, image_path, in_reply_to_ID from Scheduled_Tweets where sent != 1 and active=1 and status='active'"
flog.write("Query %s\n" % (query))
cursor.execute(query)
res = cursor.fetchall()
to_send = []
within_one_hour = []
within_two_hour = []
for r in res:
    flog.write("\tCompare now %s against %s (%s)\n" % (now, r[1], r[3]))
    delta = now - r[1]
    flog.write("\t\tDays apart: %d\n" % delta.days)
    flog.write("\t\tSeconds apart: %d\n" % delta.seconds)
    agg = delta.days * 3600*24 + delta.seconds
    flog.write("\t\tAggregate seconds: %d\n" % agg)
    if agg < 60 and agg >= 0:
        d = {'send_as': r[2], 'tweet': r[3], 'image_path': r[4], 'ID': r[0], 'in_reply_to_ID': None if r[5] == '' else r[5]}
        flog.write("\t\tAdd a tweet to be sent at %s\n" % r[1])
        flog.write("\t\t\tAs %s\n" % d['send_as'])
        flog.write("\t\t\tMsg: %s\n" % d['tweet'])
        to_send.append(d)
    elif agg > -3600 and agg < 0:
        within_one_hour.append(r[3])
    elif agg > -7200 and agg < 0:
        within_two_hour.append(r[3])
        
cursor.close(); mysql_conn.close()
if len(to_send) == 0:
    if len(within_one_hour) > 0:
        flog.write("\n\nTweets Scheduled in Next Hour\n------------------------------\n")
        for t in within_one_hour:
            flog.write("\t%s\n" % t)
    elif len(within_two_hour) > 0:
        flog.write("\n\nTweets Scheduled in Next Two Hours\n------------------------------\n")
        for t in within_two_hour:
            flog.write("\t%s\n" % t)
    else:
        flog.write("\n\nNo Scheduled Tweets to send within the next two hours...\n")
    
else:
    flog.write("\n\nSend %d scheduled tweet(s)\n------------------------------\n" % (len(to_send)))
for tweet_rec in to_send:
    

    #1 Confirm that the arguments were all accounted for 
    #python send_tweet.py [as who] [text] [optional photo path]
    flog.write("#1\n")
    send_as = tweet_rec['send_as']
    tweet = tweet_rec['tweet']
    image_path = tweet_rec['image_path']
    image_yes_no = 1
    if image_path is None or image_path == "n/a":
        image_path = None
        image_yes_no = 0
    

    #2 Use [send_as] to authenticate above
    flog.write("#2\n")
    api = authenticate_me(send_as)
    if api is None:
        print("Authentication failed\n--------------------------\n\tsend_tweet.py [send as] [tweet text] [optional image path]")
        sys.exit()
        
        
    #3 Attach image (if necessary)
    flog.write("#3\n")
    image_ids = None
    if image_path is not None:
        image_open = open(image_path, 'rb')
        image_ids = api.upload_media(media=image_open)


    #4 Send status/tweet
    flog.write("#4\n")
    if True:
        if image_ids is not None:
            if tweet_rec['in_reply_to_ID'] is not None:
                flog.write("Send tweet w/ image and w/ reply to ID: %s\n" % (tweet))
                api.update_status(status=tweet, in_reply_to_status_id=tweet_rec['in_reply_to_ID'], media_ids=image_ids['media_id'])
            else:
                flog.write("Send tweet w/ image: %s\n" % (tweet))
                api.update_status(status=tweet, media_ids=image_ids['media_id'])
        else:
            
            if tweet_rec['in_reply_to_ID'] is not None:
                flog.write("Send tweet w/ reply to ID: %s\n" % (tweet))
                api.update_status(status=tweet, in_reply_to_status_id=tweet_rec['in_reply_to_ID'])
            else:
                flog.write("Send tweet: %s\n" % (tweet))
                api.update_status(status=tweet)
    api = None

    #5 Create record in the database
    flog.write("#5\n")
    mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()
    query = "INSERT INTO Tweets_Sent (datestamp, sent_as, content, image_yes_no, image_path, active) VALUES (%s, %s, %s, %s, %s, 1)"
    param = [datetime.datetime.today(), send_as, tweet, image_yes_no, image_path]
    print("Query %s w/ %s" % (query, param))
    cursor.execute(query, param)
    query = "UPDATE Scheduled_Tweets set sent=1 where ID=%s"
    param = [tweet_rec['ID']]
    print("Query %s w/ %s" % (query, param))
    cursor.execute(query, param)
    if True:
        mysql_conn.commit();
    else:
        flog.write("\n\n\tREMINDER: nothing is being committed...\n")
    cursor.close(); mysql_conn.close()
    flog.close()
