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
from twython import TwythonAuthError

import errno
from socket import error as socket_error

from subprocess import Popen, PIPE

import clipboard

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
                        
            cnx = MySQLdb.connect(
                host='127.0.0.1',
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
    
# Create a connection to the database
mysql_conn, response = mysql_connect(); cursor = mysql_conn.cursor()

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

for handle in res:
    
    log_msg("Refreshing profile snapshot for %s" % handle[1])
    api = authenticate_me("zack")
    try:
        total_calls += 1
        tu = api.show_user(screen_name=handle[1])
    except TwythonAuthError as e:
        if "Twitter API returned a 401 (Unauthorized)" in str(e):
            log_msg("1) TwythonAuthError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
        if "Twitter API returned a 403 (Forbidden), User has been suspended." in str(e):
            log_msg("1) TwythonAuthError Error occurred when trying to capture the tweets for suspended handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
        else:
            log_msg("1) TwythonAuthError Error occurred when trying to capture the tweets for handle: %s" % handle[1])
            log_msg("Total Calls: %d\tOver %d seconds\t%.1f calls/min." % (total_calls, (current_milli_time() - start_ms)/1000, float(total_calls)/(float(current_milli_time() - start_ms)/60/1000)))
            log_msg("@ error, Per Call Delay: %d" % (per_call_delay))
            log_msg("Error text: %s" % e)
            handle = None
            api = None
            sys.exit()
        
    log_msg("%s has %d followers..." % (handle[1], tu['followers_count']))
    
    api = None

    
    mysql_conn, r = mysql_connect();  mysql_attempts = 0
    while mysql_conn is None and mysql_attempts < 10:
        time.sleep(15)
        mysql_conn, r = mysql_connect();  mysql_attempts += 1
    if mysql_conn is None:
        log_msg("Could not connect to MySQL after 10 tries...exiting.")
        sys.exit()
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
    
    mysql_conn.commit(); 
    cursor.close(); mysql_conn.close()     
    
    per_call_delay = int(re.compile(r'per_call_delay\: ([0-9]+)').search(open('/home/pi/zack/update_twitter_profile_parameters', 'r').read()).group(1))
    log_msg("Wait for %d seconds..." % per_call_delay)
    time.sleep(per_call_delay)
