flog = open('/home/pi/zack/send_tweet_log', 'w')
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

    


#1 Confirm that the arguments were all accounted for 
#python send_tweet.py [as who] [text] [optional photo path]
args = sys.argv
flog.write("#1\n")

send_as = ""
tweet = ""
image_path = ""
image_yes_no = 0
if len(args) == 3:
	image_path = None
	send_as = args[1]
	tweet = args[2]
elif len(args) == 4:
	image_path = args[3]
	send_as = args[1]
	tweet = args[2]
	image_yes_no = 1
	if not os.path.isfile(image_path):
		print("The image you want to tweet was not a valid file name\n---------------------------------------\n\t%s" % image_path)
		sys.exit()
else:
	print("Invalid arguments Error\n--------------------------\n\tsend_tweet.py [send as] [tweet text] [optional image path]")
	sys.exit()


#2 Use [send_as] to authenticate above
flog.write("#2\n")
api = authenticate_me(send_as)
if api is None:
	print("Authentication failed\n--------------------------\n\tsend_tweet.py [send as] [tweet text] [optional image path]")
	sys.exit()
	
	
#3 Attach image if necessary
flog.write("#3\n")
image_ids = None
if image_path is not None:
	image_open = open(image_path, 'rb')
	image_ids = api.upload_media(media=image_open)

#4 Send status/tweet
flog.write("#4\n")
for i in range(30):
	print("Sending %s in %d..." % (tweet, (30-i))); time.sleep(1)
	
if image_ids is not None:
	api.update_status(status=tweet, media_ids=image_ids['media_id'])
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
mysql_conn.commit();
cursor.close(); mysql_conn.close()
flog.close()
