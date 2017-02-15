import time, os, datetime, sys, psutil, random
import re
import MySQLdb
import subprocess
import telegram
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
		 
		host = "192.168.1.149"
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
        log_msg("Connection error: %s" % err)
        
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

#2 Use [as who] to authenticate above

#3 Send status/tweet

#4 Create record in the database

mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()
query = ""
param = []
cursor.execute(query, param)
mysql_conn.commit();
cursor.close(); mysql_conn.close()
