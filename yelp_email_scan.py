import time, os, datetime, sys, psutil, random
import re
import MySQLdb
import subprocess
import telegram
import requests
import httplib2, httplib, zlib
from BeautifulSoup import BeautifulSoup, SoupStrainer
reload(sys)
sys.setdefaultencoding("utf-8")

import errno
from socket import error as socket_error
import ssl
from subprocess import Popen, PIPE

from random import shuffle

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

log_file = '/home/pi/zack/Logs/yelp_email_log_%s.txt' % (datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
log_ = open(log_file, 'w')
log_.close()
def log_msg(s, no_print=False):
    if not no_print:
        print(s)
    log = open(log_file, 'a')
    log.write("%s  %s\n" % (datetime.datetime.today().strftime("%H:%M:%S"),s))
    log.close()
    
# Create a connection to the database
mysql_conn, response = mysql_connect(); cursor = mysql_conn.cursor()

query  = "SELECT a.url, b.ID, c.ID, a.yelp_listing_ID, a.time_stored, a.capture_sequence from Data_Capture_Links a, Data_Capture_Campaigns b, Email_Scrapes c where "
query += "c.active=1 and c.complete = 0 and a.scanned_for_emails=0 and ISNULL(crawl_errors) and IFNULL(a.content_type, '')='' and "
query += "c.data_capture_campaign_ID=a.data_capture_campaign_ID and  b.ID=a.data_capture_campaign_ID"
cursor.execute(query)
res = cursor.fetchall()
cursor.close(); mysql_conn.close()

log_msg("There are %d links to be scanned for emails..." % len(res))

domain_regex = re.compile(r"^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?")
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
requests_made = 0.0

weekday = datetime.datetime.today().weekday()
hr_period = int(int((datetime.datetime.today() - datetime.timedelta(hours=6)).hour) / 8)

update_query_error = "UPDATE Data_Capture_Links set crawl_errors = IFNULL(crawl_errors, 0)+1 where time_stored=%s and capture_sequence=%s"
update_query_found = "UPDATE Data_Capture_Links set scanned_for_emails=1 where time_stored=%s and capture_sequence=%s"
select_query = "Select count(1) from Emails where email_address=%s and email_scrape_ID=%s"
insert_query = "INSERT INTO Emails (company_ID, person_ID, email_address, email_domain, active, total_emails, last_send, total_responses, removed, usercode, email_scrape_ID, found_on, source_url, yelp_listing_ID) VALUES (-1, -1, %s, '', 1, 0, '1900-01-01 01:00:00', 0, 0, '', %s, %s, %s, %s)"
per_call_delay = 30
last_domain = ""

# Randomly shuffle the pages to be searched so that no one host gets pinged repeatedly and consecutively
rng = range(0, len(res))
shuffle(rng)
res2 = []
for i in rng:
    res2.append(res[i])

for k, url in enumerate(res2):
    m = domain_regex.search(url[0])
    if m is not None:
        domain = "%s%s" % (m.group(1), m.group(3))
    else:
        domain = ''
    
    log_msg("%s out of %s" % ("{:,}".format(k+1), "{:,}".format(len(res))))
    if last_domain != domain:
        #log_msg("It's a different domain from before, so searching %s (%d out of %d)\n\t%s vs %s" % (url[0], k+1, len(res), domain, last_domain))
        #log_msg("\tIt's a different domain from before, so searching %s (%d out of %d)" % (url[0], k+1, len(res)))
        time.sleep(.3)
    elif k > 0:
        log_msg("\tBeing a good internet citizen and waiting for %d seconds before searching %s (%d out of %d)" % (per_call_delay, url[0], k+1, len(res)))
        for j in range(per_call_delay):
            time.sleep(1)
    else:
        log_msg("\tBecause it's the first, we are going to go ahead with  searching %s (%d out of %d)" % (url[0], k+1, len(res)))
    
    start_ms = current_milli_time()
    http = httplib2.Http(timeout=20)
    try:
        requests_made += 1
        status, response = http.request(url[0])
        log_msg("Returned a response of length %d" % len(response), no_print=True)
    except httplib.BadStatusLine: 
        log_msg("Found a bad status line, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except httplib.IncompleteRead: 
        log_msg("Incomplete read, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except httplib.InvalidURL: 
        log_msg("Found an invalid url, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except zlib.error: 
        log_msg("zlib decompression error, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except httplib2.FailedToDecompressContent: 
        log_msg("httplib2.FailedToDecompressContent error, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except httplib2.RelativeURIError: 
        log_msg("RelativeURIError, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except httplib2.RedirectMissingLocation: 
        log_msg("RedirectMissingLocation, not sure why this was grabbed as a link\n\t%s\n" % url[0])
        status = None
    except httplib2.SSLHandshakeError: 
        log_msg("SSL Issue with link:\n\t%s\n" % url[0])
        status = None
    except httplib2.ServerNotFoundError: 
        log_msg("ServerNotFound Error with link:\n\t%s\n" % url[0])
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
    end_ms = current_milli_time()
    print("\tConnecting to site took %s ms" % "{:,}".format(end_ms - start_ms))
    start_ms = current_milli_time()
    mysql_conn, r = mysql_connect();  mysql_attempts = 0
    while mysql_conn is None and mysql_attempts < 10:
        log_msg("Attempting to reconnect to MySQL in 15 sec...")
        time.sleep(15)
        mysql_conn, r = mysql_connect();  mysql_attempts += 1
    if mysql_conn is None:
        log_msg("Could not connect to MySQL after 10 tries...exiting.")
        sys.exit()
    cursor = mysql_conn.cursor()
    end_ms = current_milli_time()
    print("\tConnecting to MySQL DB took %d ms" % (end_ms - start_ms))
    if status is not None:
        log_msg("Status: %s" % status, no_print = True)
        
        if 'content-type' in status:
            
            if "text/html" in status['content-type'].lower():
            
                matches = re.findall(email_regex, response)
                if len(matches) > 0:
                    log_msg("\tWe found %d matches" % len(matches), no_print = True)
                    for m in matches:
                        if not m.endswith(".png") and not m.endswith(".pdf") and not m.endswith(".bmp") and not m.endswith(".jpg"):
                            param = [m, url[2]]
                            log_msg("Query %s w\ %s" % (select_query, param), no_print = True)
                            cursor.execute(select_query, param)
                            r = cursor.fetchone()
                            if r[0] == 0:
                                log_msg("\t\tstore %s" % m)
                                param = [m, url[2], datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), url[0], url[3]]
                                log_msg("Query %s w\ %s" % (insert_query, param), no_print = True)
                                cursor.execute(insert_query, param)
                            
                        
        if len(response) > 0:
            param = [url[4], url[5]]
            log_msg("Query %s w\ %s" % (update_query_found, param), no_print = True)
            cursor.execute(update_query_found, param)
             
                
    else:
        param = [url[4], url[5]]
        log_msg("Query %s w\ %s" % (update_query_error, param), no_print = True)
        cursor.execute(update_query_error, param)
    if True:
        mysql_conn.commit()
    else:
        print("\n\nReminder, nothing is being committed!!!!\n")
    cursor.close(); mysql_conn.close()     
    last_domain = domain
    coming_period = int(int((datetime.datetime.today() - datetime.timedelta(hours=6) + datetime.timedelta(minutes=10)).hour) / 8)
    if hr_period != coming_period:
        log_msg("End of session at %s" % (datetime.datetime.today().strftime("%H:%M:%S")))
        
        #bot, chat_id = setup_telegram()
        #msg = "End of yelp_email_scan session at %s" % (datetime.datetime.today().strftime("%H:%M:%S"))
        #bot.sendMessage(chat_id=chat_id, text=msg)
        log_msg("Done!!!")
        sys.exit()
log_msg("DONE!!!")
