import time, os, datetime, sys, psutil, random
import re
import MySQLdb
import subprocess
import telegram; bot_token = "308120049:AAFBSyovjvhlYAe1xeTO2HAvYO4GBY3xudc"
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
					
		#print("Connect on %s" % host)
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

log_file = '/home/pi/zack/Logs/download_tweets_log_%s.txt' % (datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
log_ = open(log_file, 'w')
log_.close()
def log_msg(s):
    print(s)
    log = open(log_file, 'a')
    log.write("%s  %s\n" % (datetime.datetime.today().strftime("%H:%M:%S"),s))
    log.close()
    
# Create a connection to the database
   
mysql_conn, response = mysql_connect(); cursor = mysql_conn.cursor()



scriptname = "download_tweets"
query = "SELECT local_or_remote from Capozzi_Scripts where name=%s"
param = [scriptname]
cursor.execute(query, param)
local_or_remote = open('/home/pi/zack/local_or_remote', 'r').read().strip()
row = cursor.fetchone()
if local_or_remote != row[0]:
    print("Do not run %s because this host isn't the one that's supposed to be running it ( %s vs %s )" % (scriptname, local_or_remote, row[0]))
    sys.exit()
    
query = "SELECT ID, twitter_handle from Twitter_Accounts where active=1 order by last_twitter_download asc, ID desc"
cursor.execute(query)
res = cursor.fetchall()
cursor.close(); mysql_conn.close()  
#f = open('/home/pi/zack/tweets.csv', 'w')

date_regex = re.compile(r'[A-Za-z]{3}\s([A-Za-z]{3})\s([0-9]{2})\s([0-9]{2})\:([0-9]{2})\:([0-9]{2})\s\+[0-9]{4}\s([0-9]{4})')
header = ""
scan_auto_sourced_accounts = 1
session_total_tweets_found = 0
session_total_new_tweets = 0
session_accounts_scanned = 0
session_start_time = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
current_milli_time = lambda: int(round(time.time() * 1000))
start_ms = current_milli_time()
total_calls = float(0.0)
first_call = True
for handle in res:
    twitter_handle = handle[1]
    per_call_delay = int(re.compile(r'per_call_delay\: ([0-9]+)').search(open('/home/pi/zack/download_tweets_parameters', 'r').read()).group(1))

    mysql_conn, response = mysql_connect(); cursor = mysql_conn.cursor()
    query = "Select created_at from Tweets_Captured where created_by=%s and active=1"
    param = [handle[1]]
    print("Query %s w/ %s" % (query, param))
    cursor.execute(query, param)
    tweets_for_this_user_ = cursor.fetchall()
    tweets_for_this_user = []
    for t in tweets_for_this_user_:
		tweets_for_this_user.append(t[0])
    cursor.close(); mysql_conn.close()  
    #print(tweets_for_this_user)
                    
    handle_tweets_added = 0
    handle_tweets_already_added = 0
    download_start = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S") 
    session_accounts_scanned += 1
    api = authenticate_me("zack")
    try:
        if not first_call:
            log_msg("Wait for %d." % (per_call_delay/2))
            for i in range(per_call_delay/2):
                time.sleep(1)
        first_call = False
        log_msg("Read timeline for %s (%d)" % (handle[1], handle[0]))
            
        total_calls+= 1.0; user_timeline = api.get_user_timeline(screen_name=handle[1], count=1)
        log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
        latest = user_timeline[0]["id"]
        print ("Last ID: %s" % latest)
        points=[latest]
        log_msg("Wait for %d." % (per_call_delay/2))
        for i in range(per_call_delay/2):
            time.sleep(1)
    except IndexError as e:
        handle = None
    except TwythonError as e:
        log_msg("1) TwythonError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
        log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
        log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
        log_msg("Error text: %s" % e)
        handle = None
        api = None
        
        
    except TwythonAuthError as e:
        if "Twitter API returned a 401 (Unauthorized)" in str(e):
            log_msg("2) TwythonAuthError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
        else:
            log_msg("3) TwythonAuthError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
            sys.exit()
        
    if handle is not None:
        for i in range(0, 16):
            per_call_delay = int(re.compile(r'per_call_delay\: ([0-9]+)').search(open('/home/pi/zack/download_tweets_parameters', 'r').read()).group(1))

            log_msg("Read timeline for %s (%d) %d/16" % (handle[1], handle[0], i+1))
            if i > 0:
                api = authenticate_me("zack")
            try:
            
                total_calls+= 1.0; user_timeline = api.get_user_timeline(screen_name=handle[1], count=200, include_retweets=True, max_id=points[-1])
                log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
            except TwythonAuthError as e:
                if "Twitter API returned a 401 (Unauthorized)" in str(e):
                    log_msg("4) TwythonAuthError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
                    log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
                    log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
                    log_msg("Error text: %s" % e)
                    handle = None
                    api = None
                else:
                    log_msg("5) TwythonAuthError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
                    log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
                    log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
                    log_msg("Error text: %s" % e)
                    handle = None
                    api = None
                    sys.exit()
            except TwythonError as e:
                if "Twitter API returned a 503 (Service Unavailable), Over capacity" in str(e):
                    log_msg("6) TwythonError Error (503 (Service Unavailable), Over capacity) occurred when trying to capture the tweets for handle: %s" % handle[1])
                    log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
                    log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
                    log_msg("Error text: %s" % e)
                    handle = None
                    api = None
                    time.sleep(600)
        
                else:    
                    log_msg("7) TwythonError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
                    log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
                    log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
                    log_msg("Error text: %s" % e)
                    handle = None
                    api = None
        
            api = None
            if handle is not None:
            
            
                mysql_conn, r = mysql_connect();  mysql_attempts = 0
                while mysql_conn is None and mysql_attempts < 10:
                    time.sleep(15)
                    mysql_conn, r = mysql_connect();  mysql_attempts += 1
                if mysql_conn is None:
                    log_msg("Could not connect to MySQL after 10 tries...exiting.")
                    sys.exit()
                cursor = mysql_conn.cursor()
                #cursor.execute('SET NAMES utf8mb4;')
                #cursor.execute('SET CHARACTER SET utf8;')
                #cursor.execute('SET character_set_connection=utf8;')
                
                new_tweets = 0
                already_tweets = 0
                count_of_tweets = len(user_timeline)
                session_total_tweets_found += count_of_tweets
                log_msg("\t%d tweet(s) pulled..." % (count_of_tweets))
                
                for i, tweet in enumerate(user_timeline):                    
                        
                    text = str(tweet['text']).decode('utf-8', 'ignore').encode("utf-8")
                    text = ''.join([j if ord(j) < 128 else ' ' for j in text])
                    
                    m = date_regex.search(tweet['created_at'])
                    if m is not None:
                        dt = datetime.datetime.strptime("%s-%s-%s %s:%s:%s" % (m.group(6), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)), "%Y-%b-%d %H:%M:%S")
                        #log_msg(dt)
                        #dt = dt.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        log_msg("Regex fail: %s" % tweet['created_at'])
                        sys.exit()
                    
                    if header == "":
                        header = ",".join(tweet.keys())
                        
                    tweet_values = []
                    for t in map(str, tweet.values()):
                         
                        tweet_values.append(str(t).replace(",", ";").replace(chr(13), "").replace(chr(10), ""))
                    points.append(tweet['id'])
                    
                    check = dt
                    if check in tweets_for_this_user:
                        #print("Tweet already added to the database...")
                        handle_tweets_already_added += 1
                        already_tweets += 1
                    else:
                        #if r[0] == 0:
                        entities = str(tweet['entities']).decode('utf-8', 'ignore').encode("utf-8")
                        lang = ''
                        if 'lang' in tweet:
							lang = tweet['lang']
                        if 'retweeted_status' in tweet:
                            query = "INSERT INTO Tweets_Captured (active, created_at, created_by, text , id, is_quote_status, retweeted, retweet_count , favorited, favorite_count , source , in_reply_to_screen_name, lang, entities, retweeted_tweet) VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            param = [dt, handle[1], text, tweet['id'], tweet['is_quote_status'], tweet['retweeted'], tweet['retweet_count'], tweet['favorited'], tweet['favorite_count'], tweet['source'], tweet['in_reply_to_screen_name'], lang, entities, tweet['retweeted_status']['id']]
                        else:
                            query = "INSERT INTO Tweets_Captured (active, created_at, created_by, text , id, is_quote_status, retweeted, retweet_count , favorited, favorite_count , source , in_reply_to_screen_name, lang, entities) VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            param = [dt, handle[1], text, tweet['id'], tweet['is_quote_status'], tweet['retweeted'], tweet['retweet_count'], tweet['favorited'], tweet['favorite_count'], tweet['source'], tweet['in_reply_to_screen_name'], lang, entities]
                        cursor.execute(query, param)
                        new_tweets += 1
                        handle_tweets_added += 1
                        session_total_new_tweets += 1

                        
                log_msg("\t\tnew tweets added:     %d" % (new_tweets))
                log_msg("\t\ttweets already stored: %d" % (already_tweets))
                
                if True:
                    mysql_conn.commit(); 
                else:
                    print("\n\n\tReminder - nothing is being committed...\n\n")
                cursor.close(); mysql_conn.close() 
                    
                log_msg("Wait for %d seconds..." % per_call_delay)
                for i in range(per_call_delay):
                    time.sleep(1)
                if count_of_tweets == 1:
                    log_msg("Only one tweet was pulled, must have reached the end of the stream, exit pull for %s" % handle[1])
                    break
                if new_tweets == 0:
                    log_msg("No new tweets found, must have captured all the new stuff, exit pull for %s" % handle[1])
                    break
            else:
                break
        if handle is not None:
            mysql_conn, r = mysql_connect();  mysql_attempts = 0
            while mysql_conn is None and mysql_attempts < 10:
                time.sleep(15)
                mysql_conn, r = mysql_connect();  mysql_attempts += 1
            if mysql_conn is None:
                log_msg("Could not connect to MySQL after 10 tries...exiting.")
                sys.exit()
            cursor = mysql_conn.cursor()
            log_msg("Done reading %s - %d tweets added; %d tweets already stored" % (handle[1], handle_tweets_added, handle_tweets_already_added))   
            query = "UPDATE Twitter_Accounts set last_twitter_download=%s, total_twitter_downloads=total_twitter_downloads+1 where twitter_handle=%s"
            param = [download_start, handle[1]]
            cursor.execute(query, param)
            mysql_conn.commit(); cursor.close(); mysql_conn.close()     
    else:
        mysql_conn, r = mysql_connect();  mysql_attempts = 0
        while mysql_conn is None and mysql_attempts < 10:
            time.sleep(15)
            mysql_conn, r = mysql_connect();  mysql_attempts += 1
        if mysql_conn is None:
            log_msg("Could not connect to MySQL after 10 tries...exiting.")
            sys.exit()
        cursor = mysql_conn.cursor()
        log_msg("Error reading %s - %d tweets added; %d tweets already stored" % (twitter_handle, handle_tweets_added, handle_tweets_already_added))   
        query = "UPDATE Twitter_Accounts set last_twitter_download=%s, total_twitter_downloads=total_twitter_downloads+1 where twitter_handle=%s"
        param = [download_start, twitter_handle]
        cursor.execute(query, param)
        mysql_conn.commit(); cursor.close(); mysql_conn.close()  
#f.close()
session_end_time = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")

mysql_conn, r = mysql_connect();  mysql_attempts = 0
while mysql_conn is None and mysql_attempts < 10:
    time.sleep(15)
    mysql_conn, r = mysql_connect();  mysql_attempts += 1
if mysql_conn is None:
    log_msg("Could not connect to MySQL after 10 tries...exiting.")
    sys.exit()
cursor = mysql_conn.cursor()
query = "INSERT INTO Twitter_Pull_Sessions (start_time, end_time, accounts_scanned, total_tweets_found, total_new_tweets, scan_auto_sourced_accounts) VALUES (%s, %s, %s, %s, %s, %s)"
param = [session_start_time, session_end_time, session_accounts_scanned, session_total_tweets_found, session_total_new_tweets, scan_auto_sourced_accounts]
cursor.execute(query, param)
mysql_conn.commit(); cursor.close(); mysql_conn.close() 

log_msg("DONE!!!")

bot = telegram.Bot(token=bot_token)

# Waits for the first incoming message
updates=[]
while not updates:
    updates = bot.getUpdates()
    
# Gets the id for the active chat

chat_id=updates[-1].message.chat_id

# Sends a message to the chat
msg = "DL Tweets done: %s tweets read; %s new tweets from %s accounts." % ("{:,}".format(session_total_tweets_found), "{:,}".format(session_total_new_tweets), "{:,}".format(session_accounts_scanned))
bot.sendMessage(chat_id=chat_id, text=msg)
print("Telegram %s" % msg)

