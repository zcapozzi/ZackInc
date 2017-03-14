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

import telegram

import zlib
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

domain_regex = re.compile(r"^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?")

def get_links(url, layer, offset, parent, tag_type):
    global all_links
    global domain_regex
    global all_links_detail
    global requests_made
    new_links_found = -1
        
    m = domain_regex.search(url)
    url_domain = None
    if m is not None:
        url_domain = m.group(4)
        
    
    if layer <= layer_limit and len(all_links) < 500 and "lepainquotidien" not in url and "aubonpain" not in url and "nothingbundtcakes" not in url:
        processed_links = 0
        #print("create http for %s" % url)
        log_msg("Create http for %s\n" % url, no_print=True)
        time.sleep(1)
        http = httplib2.Http(timeout=20, disable_ssl_certificate_validation=True)
        #print("make request")
        try:
            requests_made += 1
            status, response = http.request(url)
            log_msg("Returned a response of length %d" % len(response), no_print=True)
        except httplib.BadStatusLine: 
            log_msg("Found a bad status line, not sure why this was grabbed as a link\n\t%s\n" % url)
            status = None
        except httplib.IncompleteRead: 
            log_msg("IncompleteRead error, not sure why this was grabbed as a link\n\t%s\n" % url)
            status = None
        except httplib.InvalidURL: 
            log_msg("Found an invalid url, not sure why this was grabbed as a link\n\t%s\n" % url)
            status = None
        except zlib.error: 
            log_msg("zlib decompression error, not sure why this was grabbed as a link\n\t%s\n" % url)
            status = None
        except httplib2.MalformedHeader: 
            log_msg("MalformedHeader, not sure why this was grabbed as a link\n\t%s\n" % url)
            status = None
        except httplib2.RelativeURIError: 
            log_msg("RelativeURIError, not sure why this was grabbed as a link\n\t%s\n" % url)
            status = None
        except httplib2.FailedToDecompressContent: 
            log_msg("FailedToDecompressContent, not sure why this was grabbed as a link\n\t%s\n" % url)
            status = None
        except httplib.ResponseNotReady: 
            log_msg("Found an ResponseNotReady Error, not sure why this was grabbed as a link\n\t%s\n" % url)
            status = None
        except httplib2.RedirectMissingLocation: 
            log_msg("RedirectMissingLocation, not sure why this was grabbed as a link\n\t%s\n" % url)
            status = None
        except httplib2.SSLHandshakeError: 
            log_msg("SSL Issue with link:\n\t%s\n" % url)
            status = None
        except httplib2.ServerNotFoundError: 
            log_msg("ServerNotFound Error with link:\n\t%s\n" % url)
            status = None
        except ssl.SSLEOFError: 
            log_msg("SSL Issue with link:\n\t%s\n" % url)
            status = None
        except socket_error: 
            log_msg("Socket Error with link:\n\t%s\n" % url)
            status = None
        except UnicodeError: 
            log_msg("UnicodeError with link:\n\t%s\n" % url)
            status = None
        except httplib2.RedirectLimit:
            log_msg("RedirectLimit with link:\n\t%s\n" % url)
            status = None
            
        if status is not None:
            log_msg("Status: %s" % status, no_print = True)
            if 'content-type' in status:
                if "text/html" in status['content-type'].lower():
                
                    new_links_found = 0
                    try:
                        for i, link in enumerate(BeautifulSoup(response, parseOnlyThese=SoupStrainer('a'))):
                        
                            if link is not None:
                            
                                if link.has_key("href"):
                                    
                                    if not link['href'].startswith("javascript") and not link['href'].startswith("mailto"):
                                        if not link['href'].startswith("http"):
                                            #print("Add prefix to %s" % link['href'])
                                            m = domain_regex.search(url)
                                            if m is not None:
                                                #print(m.groups())
                                                top_domain = "%s%s" % (m.group(1), m.group(3))
                                                #print("\tAdd %s to %s to make %s" % (top_domain, link['href'], "%s%s" % (top_domain, link['href'])))
                                                if top_domain.endswith("?") or top_domain.endswith("/") or link['href'].startswith("?") or link['href'].startswith("/"):
                                                    log_msg("\tAdd %s to %s to make %s\n" % (top_domain, link['href'], "%s%s" % (top_domain, link['href'])), no_print=True)
                                                    link_url = "%s%s" % (top_domain, link['href'])
                                                else:
                                                    log_msg("\tAdd %s to '/' and %s to make %s\n" % (top_domain, link['href'], "%s%s" % (top_domain, link['href'])), no_print=True)
                                                    link_url = "%s/%s" % (top_domain, link['href'])
                                                
                                            else:
                                                link_url = ""   
                                        else:
                                                link_url = link['href']
                                        
                                        m = domain_regex.search(link_url)
                                        link_domain = None
                                        if m is not None:
                                            link_domain = m.group(4)
                                            #print("%s led to %s\n\t%s vs %s\n\n%s\n\n" % (url, link_url, url_domain, link_domain, m.groups()))
                                            # Check if the domains match
                                            domain_match = False
                                            if link_domain is not None:
                                                url_domain = url_domain.replace("www.", "")
                                                link_domain = link_domain.replace("www.", "")
                                                if url_domain in link_domain or link_domain in url_domain:
                                                    domain_match = True
                                                #print("%s led to %s\n\t%s vs %s\n\n%s\n\n" % (url, link_url, url_domain, link_domain, m.groups()))
                                                    
                                                if link_url != "" and link_url != url  and new_links_found < 100 and domain_match:
                                                    if link_url.endswith("#"):
                                                        log_msg("\t\tI have removed the trailing number sign from %s" % (link_url), no_print=True)
                                                        link_url = link_url[0:-1]
                                                    if link_url not in all_links:
                                                        log_msg("\tFound %s" % (link_url), no_print=True)
                                                        all_links.append(link_url)
                                                        if len(all_links) % 100 == 0 or (len(all_links) < 100 and len(all_links) % 25 == 0):
                                                            log_msg("  %04d links found so far\t\t%04d requests already made..." % (len(all_links), requests_made))
                                                        processed_links += 1
                                                        new_links_found += 1
                                                        if not link_url.endswith(".jpg") and not link_url.endswith(".png") and not link_url.endswith(".pdf"):
                                                            get_links(link_url, layer+1, offset + ">>", url, 'a')
                                            else:
                                                log_msg("\tNo domain found for %s" % (link_url))
                                        else:
                                            log_msg("\tNo domain found for %s" % (link_url))
                    except TypeError:
                        log_msg("\tType Error when parsing %s" % (url))   
    content_type = ''
    if not url.startswith("http"):
        #print("Add prefix to %s" % link['href'])
        m = domain_regex.search(url)
        if m is not None:
            #print(m.groups())
            top_domain = "%s%s" % (m.group(1), m.group(3))
            #print("\tAdd %s to %s to make %s" % (top_domain, link['href'], "%s%s" % (top_domain, link['href'])))
            
            url = "%s%s" % (top_domain, url)
        else:
            url = url   
    if url.lower().endswith(".jpg"):
        content_type = "jpg"
    elif url.lower().endswith(".png"):
        content_type = "png"
    elif url.lower().endswith(".pdf"):
        content_type = "pdf"
    elif url.lower().endswith(".bmp"):
        content_type = "bmp"
    elif url.lower().endswith(".xlsx"):
        content_type = "xlsx"
        
    d = {'url': url, 'found_at_layer': layer, 'found_on': parent, 'new_links_found': new_links_found, 'content_type': content_type, 'tag_type': tag_type}
    if d not in all_links_detail:
        all_links_detail.append(d)

weekday = datetime.datetime.today().weekday()
hr_period = int(int((datetime.datetime.today() - datetime.timedelta(hours=6)).hour) / 8)

# 3 8-hr periods
# 6 A - 2 P
# 2 P - 10P
# 10P - 6 A

log_file = '/home/pi/zack/Logs/yelp_ext_site_crawler_log_%s.txt' % (datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
log_ = open(log_file, 'w')
log_.close()
def log_msg(s, no_print=False):
    if not no_print:
        print(s)
    log = open(log_file, 'a')
    log.write("%s  %s\n" % (datetime.datetime.today().strftime("%H:%M:%S"), s))
    log.close()
    
# Create a connection to the database
mysql_conn, response = mysql_connect(); cursor = mysql_conn.cursor()



scriptname = "yelp_ext_site_crawler"
query = "SELECT local_or_remote from Capozzi_Scripts where name=%s"
param = [scriptname]
cursor.execute(query, param)
local_or_remote = open('/home/pi/zack/local_or_remote', 'r').read().strip()
row = cursor.fetchone()
if local_or_remote != row[0]:
    print("Do not run %s because this host isn't the one that's supposed to be running it ( %s vs %s )" % (scriptname, local_or_remote, row[0]))
    sys.exit()
    
query = "SELECT a.yelp_url, a.yelp_ID, a.external_url, b.ID from Yelp_Listings a, Data_Capture_Campaigns b where a.country_code='US' and b.ID=a.data_capture_campaign_ID and IFNULL(a.external_url, '')!='' and a.active=1 and a.scanned_ext_for_emails=0"
cursor.execute(query)
res1 = cursor.fetchall()
query = "SELECT a.yelp_url, a.yelp_ID, a.external_url, b.ID from Yelp_Listings a, Data_Capture_Campaigns b where a.country_code!='US' and b.ID=a.data_capture_campaign_ID and IFNULL(a.external_url, '')!='' and a.active=1 and a.scanned_ext_for_emails=0"
cursor.execute(query)
res2 = cursor.fetchall()
res = []
for r in res1:
    res.append(r)
for r in res2:
    res.append(r)
cursor.close(); mysql_conn.close()

sites = []
for r in res:
    sites.append({'yelp_listing_ID': r[1], 'url': r[2], 'data_capture_campaign_ID': r[3]})
log_msg("There are %d links to be crawled for other links..." % len(sites))

layer_limit = 4

for site in sites:
    log_msg("***********************************\n   Processing %s" % site['url'])
    
    requests_made = 0
    all_links = []
    all_links_detail = []
    


    parse_begin = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    success = False
    get_links(site['url'], 0, "", ".", ".")
    parse_end = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    #print("Links received at %s" % parse_end)


    mysql_conn, r = mysql_connect();  mysql_attempts = 0
    while mysql_conn is None and mysql_attempts < 10:
        time.sleep(15)
        mysql_conn, r = mysql_connect();  mysql_attempts += 1
    if mysql_conn is None:
        log_msg("Could not connect to MySQL after 10 tries...exiting.")

    cursor = mysql_conn.cursor()
    mysql_conn.set_character_set('utf8')
    cursor = mysql_conn.cursor()
        
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')
    
    current_milli_time = lambda: int(round(time.time() * 1000))
    log_msg("There were %d links found in total..." % len(all_links))
    for i, (link, detail) in enumerate(zip(all_links, all_links_detail)):
        log_msg("\t%04d  -  %s" % (i,link))
        #print("\t\t%s" % (detail['tag_type']))
        #print("\t\tFound at layer: %d" % detail['found_at_layer'])
        #print("\t\tUnique links contained: %d" % detail['new_links_found'])
        #print("\t\tFound on: %s" % detail['found_on'])
        capture_manually = 0
        if "contact" in link.lower():
            capture_manually = 1
        upload_link = link.decode('utf-8', 'ignore').encode('utf-8')
        query2 = "INSERT INTO Data_Capture_Links (yelp_listing_ID, data_capture_campaign_ID, time_stored, url, capture_sequence, layer, found_on, new_links_found, content_type, tag_type, capture_manually, scanned_for_emails) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        param2 = [site['yelp_listing_ID'], site['data_capture_campaign_ID'], current_milli_time(), upload_link, i, detail['found_at_layer'], detail['found_on'], detail['new_links_found'], detail['content_type'], detail['tag_type'], capture_manually, 0]
        #print("Query: %s \n\tw/ %s\n" % (query2, param2))
        cursor.execute(query2, param2)

    log_msg("Final Stats:\n---------------------------------")
    record = {'url': site['url'], 'start_date': parse_begin, 'end_date': parse_end, 'total_links_found': len(all_links_detail)}
    log_msg(record)

    query  = "UPDATE Yelp_Listings set scanned_ext_for_emails=1 where yelp_ID=%s"
    param = [site['yelp_listing_ID']]
    log_msg("Query: %s \n\tw/ %s" % (query, param))
    cursor.execute(query, param)
    if True:
        mysql_conn.commit()
    else:
        print("\n\n\tReminder, results were not committed to the DB!!!!\n\n")
    cursor.close()
    mysql_conn.close()
    
    coming_period = int(int((datetime.datetime.today() - datetime.timedelta(hours=6) + datetime.timedelta(minutes=5)).hour) / 8)
    if hr_period != coming_period:
        log_msg("End of session at %s" % (datetime.datetime.today().strftime("%H:%M:%S")))
        
        bot, chat_id = setup_telegram()
        msg = "End of yelp_ext_site_crawler session at %s" % (datetime.datetime.today().strftime("%H:%M:%S"))
        bot.sendMessage(chat_id=chat_id, text=msg)
        log_msg("Done!!!")
        sys.exit()
log_msg("\n\nDONE!!!")
