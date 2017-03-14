import time, os, datetime, sys, psutil, random
import re
import MySQLdb
import subprocess
import telegram
import requests
import httplib2, httplib
from BeautifulSoup import BeautifulSoup, SoupStrainer
reload(sys)
sys.setdefaultencoding("utf-8")

import errno
from socket import error as socket_error
import ssl
from subprocess import Popen, PIPE

import clipboard
sys.path.insert(0, "../ZackInc")
import zack_inc as zc


bot_token = "308120049:AAFBSyovjvhlYAe1xeTO2HAvYO4GBY3xudc"

def setup_telegram():
    # Connect to our bot
    bot = telegram.Bot(token=bot_token)

    # Waits for the first incoming message
    updates=[]
    while not updates:
        updates = bot.getUpdates()
        
    # Gets the id for the active chat
    #print updates[-1].message.text
    chat_id=updates[-1].message.chat_id


    return bot, chat_id
    
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
        log_msg("Connection error: %s" % err)
        
    return cnx, response

weekday = datetime.datetime.today().weekday()
hr_period = int(int((datetime.datetime.today() - datetime.timedelta(hours=6)).hour) / 8)

def keypress(sequence):
    p = Popen(['xte'], stdin=PIPE)
    p.communicate(input=sequence)

log_file = '/home/pi/zack/Logs/yelp_ext_url_log_%s.txt' % (datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
log_ = open(log_file, 'w')
log_.close()
def log_msg(s, no_print=False):
    if not no_print:
        print(s)
    log = open(log_file, 'a')
    log.write("%s  %s\n" % (datetime.datetime.today().strftime("%H:%M:%S"),s))
    log.close()
    
# Create a connection to the database
mysql_conn, r = mysql_connect();  mysql_attempts = 0
while mysql_conn is None and mysql_attempts < 10:
    time.sleep(15)
    mysql_conn, r = mysql_connect();  mysql_attempts += 1
if mysql_conn is None:
    log_msg("Could not connect to MySQL after 10 tries...exiting.")
    sys.exit()
cursor = mysql_conn.cursor()



scriptname = "yelp_ext_url_scan"
query = "SELECT local_or_remote from Capozzi_Scripts where name=%s"
param = [scriptname]
cursor.execute(query, param)
local_or_remote = open('/home/pi/zack/local_or_remote', 'r').read().strip()
row = cursor.fetchone()
if local_or_remote != row[0]:
    print("Do not run %s because this host isn't the one that's supposed to be running it ( %s vs %s )" % (scriptname, local_or_remote, row[0]))
    sys.exit()
    
query = "SELECT yelp_url, yelp_ID from Yelp_Listings where country_code ='US' and active=1 and manually_scanned=0"
cursor.execute(query)
res1 = cursor.fetchall()
query = "SELECT yelp_url, yelp_ID from Yelp_Listings where country_code!='US' and active=1 and manually_scanned=0"
cursor.execute(query)
res2 = cursor.fetchall()
res = []
for r in res1:
    res.append(r)
for r in res2:
    res.append(r)
res1 = None
res2 = None

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
email_regex = re.compile(r'([\(\)a-z0-9\_\-]+@[a-z0-9\_\-\(\)]+\.[a-z0-9\_\-\(\)]+)', re.IGNORECASE)
url_regex = re.compile(r'biz_redir\?url\=(.*?)\&website_link', re.IGNORECASE)
requests_made = 0.0

update_query_found = "UPDATE Yelp_Listings set external_url=%s, manually_scanned = 1 where yelp_ID=%s"
update_query_not_found = "UPDATE Yelp_Listings set manually_scanned = 1 where yelp_ID=%s"


cnt_link_not_found = 0
cnt_link_found = 0
consec_no_link = 0

for k, url in enumerate(res):
    per_call_delay = int(re.compile(r'per_call_delay\: ([0-9]+)').search(open('/home/pi/zack/yelp_ext_url_scan_parameters', 'r').read()).group(1))
    if k > 0:
        log_msg("Being a good internet citizen and waiting for %d seconds before searching url %d out of %d" % (per_call_delay, k+1, len(res)))
        for j in range(per_call_delay):
            time.sleep(1)
    elif k == 10:
        break
    http = httplib2.Http(timeout=20, disable_ssl_certificate_validation=True)
    try:
        requests_made += 1
        status, response = http.request(url[0])
        log_msg("Returned a response of length %d" % len(response), no_print=True)
        
    except httplib.InvalidURL: 
        log_msg("Found an invalid url, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except httplib2.SSLHandshakeError: 
        log_msg("SSL Issue with link:\n\t%s\n" % url[0])
        status = None
    except httplib.ResponseNotReady: 
        log_msg("Found an ResponseNotReady Error, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except httplib2.ServerNotFoundError: 
        log_msg("ServerNotFound Error with link:\n\t%s\n" % url[0])
        status = None
    except OverflowError: 
        log_msg("OverflowError, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except httplib.IncompleteRead: 
        log_msg("Incomplete read, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except ssl.SSLEOFError: 
        log_msg("SSL Issue with link:\n\t%s\n" % url[0])
        status = None
    except socket_error: 
        log_msg("Socket Error with link:\n\t%s\n" % url[0])
        status = None
    except UnicodeError: 
        log_msg("UnicodeError with link:\n\t%s\n" % url[0])
        status = None
    except httplib2.RedirectLimit:
        log_msg("RedirectLimit with link:\n\t%s\n" % url[0])
        status = None
        
    if status is not None:
        log_msg("Status: %s" % status, no_print = True)
        #f.write("Status: %s\n" % status)
        if 'content-type' in status:
            
            if "text/html" in status['content-type'].lower():
            
                new_links_found = 0
                found = False
                mysql_conn, r = mysql_connect();  mysql_attempts = 0
                while mysql_conn is None and mysql_attempts < 10:
                    time.sleep(15)
                    mysql_conn, r = mysql_connect();  mysql_attempts += 1
                if mysql_conn is None:
                    log_msg("Could not connect to MySQL after 10 tries...exiting.")
                    sys.exit()
                cursor = mysql_conn.cursor()
                for i, link in enumerate(BeautifulSoup(response, parseOnlyThese=SoupStrainer('a'))):
                    
                    if link.has_key("href"):
                        log_msg("%d\t%s" % (i, link['href']),no_print=True)
                        #print("\t%s" % link)
                        link = link['href']
                        m = url_regex.search(link)
                        if m is not None:
                            link = m.group(1)
                            link = link.replace("%3A", ":").replace("%2F", "/")
                            log_msg("\tFound a link: %s" % link)
                            param = [link, url[1]]
                            #print("Query %s w/ %s" % (update_query_found, param))
                            cursor.execute(update_query_found, param)
                            found = True
                            break
                log_msg("Finished read of %s" % url[0], no_print=True)
                if not found:
                    param = [url[1]]
                    log_msg("\tNo link found...")
                    #print("Query %s w/ %s" % (update_query_not_found, param))
                    cursor.execute(update_query_not_found, param)
                    if 'content-length' in status:
                        cnt_link_not_found += 1
                        consec_no_link += 1
                    
                        f_ = open('/home/pi/zack/yelp_ext_url_scans_response_lengths', 'a')
                        f_.write("%s,no link\n" % (status['content-length'])); f_.close()
                        if consec_no_link == 50:
                            log_msg("\n\n\t50 consecutive links didn't have an external URL; that's fishy, we're exiting")
                            sys.exit()
                else:
                    cnt_link_found += 1
                    consec_no_link = 0
                    f_ = open('/home/pi/zack/yelp_ext_url_scans_response_lengths', 'a')
                    f_.write("%s,link\n" % (status['content-length'])); f_.close()
                if True:
                    mysql_conn.commit(); 
                else:
                    print("Reminder: nothing is being committed right now.")
                cursor.close(); mysql_conn.close()     
                log_msg("\t\t%s link(s) processed  --  %s with links  -- %s without links...\n----------------------------------------------" % ("{:,}".format(cnt_link_found + cnt_link_not_found), "{:,}".format(cnt_link_found), "{:,}".format(cnt_link_not_found)))
    coming_period = int(int((datetime.datetime.today() - datetime.timedelta(hours=6) + datetime.timedelta(minutes=2)).hour) / 8)
    if hr_period != coming_period:
        log_msg("End of session at %s" % (datetime.datetime.today().strftime("%H:%M:%S")))
        
        #bot, chat_id = setup_telegram()
        #msg = "End of yelp_ext_url_scan session at %s" % (datetime.datetime.today().strftime("%H:%M:%S"))
        #bot.sendMessage(chat_id=chat_id, text=msg)
        log_msg("Done!!!")
        sys.exit()   
log_msg("Done!!!")
