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
import telegram; bot_token = "308120049:AAFBSyovjvhlYAe1xeTO2HAvYO4GBY3xudc"

import errno
from socket import error as socket_error

from subprocess import Popen, PIPE

import clipboard
sys.path.insert(0, "../ZackInc")
import zack_inc as zc


def mysql_connect():
    cnx = None
    try:
        env = os.getenv('SERVER_SOFTWARE')
        if (env and env.startswith('Google App Engine/')):
              #logging.info("Connecting via local...")
              # Connecting from App Engine
            cnx = MySQLdb.connect(
            unix_socket='/cloudsql/pg-us-n-app-242036:us-central1:doormandb',
            user=app.config['DBUSER'], passwd=app.config['DBPASS'], db=app.config['DBNAME'])
            response = "Success!"
        else:
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


##########################
commit_results = True  #
##########################
 
# Create a connection to the database
mysql_conn, response = mysql_connect(); cursor = mysql_conn.cursor()

scriptname = "update_twitter_accounts"
query = "SELECT local_or_remote from Capozzi_Scripts where name=%s"
param = [scriptname]
cursor.execute(query, param)
local_or_remote = open('/home/pi/zack/local_or_remote', 'r').read().strip()
row = cursor.fetchone()
if local_or_remote != row[0]:
    print("Do not run %s because this host isn't the one that's supposed to be running it ( %s vs %s )" % (scriptname, local_or_remote, row[0]))
    sys.exit()

query = "SELECT ID, twitter_handle from Twitter_Accounts where active=1 order by ID desc"
cursor.execute(query)
res = cursor.fetchall()
cursor.close(); mysql_conn.close()  

date_regex = re.compile(r'[A-Za-z]{3}\s([A-Za-z]{3})\s([0-9]{2})\s([0-9]{2})\:([0-9]{2})\:([0-9]{2})\s\+[0-9]{4}\s([0-9]{4})')
header = ""
log_file = '/home/pi/zack/Logs/update_twitter_accounts_log_%s.txt' % (datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
log_ = open(log_file, 'w')
log_.close()
def log_msg(s, no_print=False):
    if not no_print:
        print(s)
    log = open(log_file, 'a')
    log.write("%s  %s\n" % (datetime.datetime.today().strftime("%H:%M:%S"),s))
    log.close()

total_calls = 1.0
start_ms = zc.current_milli_time()

weekday = datetime.datetime.today().weekday()
hr_period = int(int((datetime.datetime.today() - datetime.timedelta(hours=6)).hour) / 8)

# 3 8-hr periods
updated_accounts = 0
for handle in res:
    
    
    log_msg("Refreshing profile snapshot for %s" % handle[1])
    api = authenticate_me("zack")
    try:
        total_calls += 1
        tu = api.show_user(screen_name=handle[1])
    except TwythonAuthError as e:
        if "Twitter API returned a 401 (Unauthorized)" in str(e):
            log_msg("1) TwythonAuthError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (zc.current_milli_time() - start_ms)/1000, float(total_calls)/(float(zc.current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
        elif "Twitter API returned a 403 (Forbidden), User has been suspended." in str(e):
            log_msg("1) TwythonAuthError Error occurred when trying to capture the tweets for suspended handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (zc.current_milli_time() - start_ms)/1000, float(total_calls)/(float(zc.current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
        else:
            log_msg("1) TwythonAuthError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (zc.current_milli_time() - start_ms)/1000, float(total_calls)/(float(zc.current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
    except TwythonError as e:
        if "Twitter API returned a 401 (Unauthorized)" in str(e):
            log_msg("1) TwythonError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (zc.current_milli_time() - start_ms)/1000, float(total_calls)/(float(zc.current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
        elif "Twitter API returned a 404 (Not Found), User not found." in str(e):
            log_msg("1) TwythonError Error occurred when trying to capture the tweets for suspended handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (zc.current_milli_time() - start_ms)/1000, float(total_calls)/(float(zc.current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
        elif "Twitter API returned a 403 (Forbidden), User has been suspended." in str(e):
            log_msg("1) TwythonError Error occurred when trying to capture the tweets for suspended handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (zc.current_milli_time() - start_ms)/1000, float(total_calls)/(float(zc.current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
        else:
            log_msg("1) TwythonError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (zc.current_milli_time() - start_ms)/1000, float(total_calls)/(float(zc.current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
            sys.exit()
    api = None
    mysql_conn, r = mysql_connect();  mysql_attempts = 0
    while mysql_conn is None and mysql_attempts < 10:
        time.sleep(15)
        mysql_conn, r = mysql_connect();  mysql_attempts += 1
    if mysql_conn is None:
        log_msg("Could not connect to MySQL after 10 tries...exiting.")
        sys.exit()
    
    if handle is not None:
        log_msg("%s has %d followers..." % (handle[1], tu['followers_count']))
    
    

    
        cursor = mysql_conn.cursor()
        query = "UPDATE Twitter_Account_Snapshot set most_recent=0 where twitter_account_ID=%s"
        param = [handle[0]]
        cursor.execute(query, param)
        
        query = "INSERT INTO Twitter_Account_Snapshot (datestamp, twitter_account_ID, twitter_handle, num_followers, verified, twitter_name, num_tweets, num_following, url, most_recent)"
        query += " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)"
        param = [datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), handle[0], handle[1], tu['followers_count'], 1 if tu['verified'] else 0, tu['name'], tu['statuses_count'], tu['friends_count'], tu['url']]
        cursor.execute(query, param)
        log_msg("Query %s w/ %s" % (query, param))
        query = "UPDATE Twitter_Accounts set name=%s where ID=%s"
        param = [tu['name'], handle[0]]
        cursor.execute(query, param)
        log_msg("Query %s w/ %s" % (query, param))
        tu = None
        updated_accounts += 1
    
    if commit_results:
        mysql_conn.commit()
    else:
        print("\n\n\tREMINDER, nothing is being committed!!!!\n\n")
    cursor.close(); mysql_conn.close()     
    
    per_call_delay = int(re.compile(r'per_call_delay\: ([0-9]+)').search(open('/home/pi/zack/update_twitter_profile_parameters', 'r').read()).group(1))
    log_msg("Wait for %d seconds..." % per_call_delay)
    time.sleep(per_call_delay)
    coming_period = int(int((datetime.datetime.today() - datetime.timedelta(hours=6) + datetime.timedelta(minutes=10)).hour) / 8)
    if hr_period != coming_period:
        log_msg("End of session at %s" % (datetime.datetime.today().strftime("%H:%M:%S")))
        break


api = authenticate_me("zack")
follower_count = 0
next_cursor = -1
last_cursor = -1
my_followers = []
followers_added_to_capture = 0
while next_cursor:
    
    followers = api.get_followers_list(screen_name = "laxreference", count=200)
    for follower in followers['users']:
        follower_count += 1
        print "ID #%s - %s\t\t(%d total)" % (follower['id'], follower['screen_name'], follower_count)
        d = {'id': follower['id'], 'handle': "@%s" % (follower['screen_name'])}
        my_followers.append(d)
    next_cursor = followers['next_cursor']
    print("Next Cursor: %s (%s)" % (str(next_cursor), next_cursor.__class__))
    if last_cursor == next_cursor or True:
        break
    last_cursor = next_cursor
    time.sleep(60)
    


check_followed_query1 = "SELECT count(1) from Twitter_Accounts where twitter_handle = %s"
check_followed_query2 = "SELECT count(1) from Twitter_Accounts where twitter_handle = %s and active=1"

mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()
for m in my_followers:
    check_followed_param = [m['handle']]
    
    cursor.execute(check_followed_query1, check_followed_param)
    count = cursor.fetchone()[0]
    query = None
    if count == 0:
        query = "INSERT INTO Twitter_Accounts (ID, twitter_handle, twitter_ID, created_on, active, auto_sourced_as_follower) VALUES ((SELECT count(1)+1 from Twitter_Accounts fds), %s, %s, %s, 1, 1)"
        param = [m['handle'], m['id'], datetime.datetime.today()]
        followers_added_to_capture += 1
    else:
        cursor.execute(check_followed_query2, check_followed_param)
        count = cursor.fetchone()[0]
        if count == 0:
            query = "UPDATE Twitter_Accounts set active=1 where twitter_handle=%s"
            param = [m['handle']]
    if query is not None:
        print("Query %s w/ %s" % (query, param))
        cursor.execute(query, param)
    
    query = "INSERT INTO Twitter_Follower_Snapshot (datestamp, twitter_ID) VALUES(%s, %s)"
    param = [datetime.datetime.today(), m['handle']]
    print("Query %s /w %s" % (query, param))
    cursor.execute(query, param)
print("\n\n\tFound %d total followers.\n\n" % follower_count)

if commit_results:
    mysql_conn.commit()
else:
    print("\n\n\tREMINDER, nothing is being committed!!!!\n\n")
cursor.close(); mysql_conn.close()
log_msg("DONE!!!")

bot = telegram.Bot(token=bot_token)

# Waits for the first incoming message
updates=[]
while not updates:
    updates = bot.getUpdates()
    
# Gets the id for the active chat

chat_id=updates[-1].message.chat_id

# Sends a message to the chat
msg = "Done with update_twitter_accounts\n  %s accounts updated\n  %s followers added to tweet capture" % ("{:,}".format(updated_accounts), "{:,}".format(followers_added_to_capture))
bot.sendMessage(chat_id=chat_id, text=msg)
print("Telegram %s" % msg)

