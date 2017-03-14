import time, os, datetime, sys, psutil
import re
import MySQLdb
import subprocess
import telegram

from subprocess import Popen, PIPE

from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator
import yelp

import urllib2

bot_token = "308120049:AAFBSyovjvhlYAe1xeTO2HAvYO4GBY3xudc"
sys.path.insert(0, "../ZackInc")
import zack_inc as zc


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
        print("Connection error: %s" % err)
        
    return cnx, response

initial_loop = True

key_data = open('/home/pi/zack/yelp_api_keys', 'r').read()
r1 = re.compile(r'YOUR_CONSUMER_KEY \= \"(.*?)\"')
r2 = re.compile(r'YOUR_CONSUMER_SECRET \= \"(.*?)\"')
r3 = re.compile(r'YOUR_TOKEN \= \"(.*?)\"')
r4 = re.compile(r'YOUR_TOKEN_SECRET \= \"(.*?)\"')
YOUR_CONSUMER_KEY = r1.search(key_data).group(1)
YOUR_CONSUMER_SECRET = r2.search(key_data).group(1)
YOUR_TOKEN = r3.search(key_data).group(1)
YOUR_TOKEN_SECRET = r4.search(key_data).group(1)
key_data = None

def get_zips():
    if os.path.isfile('/home/pi/zack/zip_codes.csv'):
        zips = open('/home/pi/zack/zip_codes.csv', 'r').read().split("\n")  
    elif os.path.isfile('/home/pi/zack/14zpallnoagi.csv'):
        zips = []
        if True:
            print("Open file 2...")
            data = open('/home/pi/zack/14zpallagi.csv', 'r').read().split("\n")
            print("Process file 2...")
            for i, d in enumerate(data[1:]):
                if i % 10000 == 0:
                    print("\t%d / %d @ %s" % (i, len(data), datetime.datetime.today()))
                #print(d)
                tokens = d.split(',')
                if len(tokens) > 2:
                    #print(tokens[2])
                    if tokens[2] not in zips:
                        zips.append(tokens[2])
        
        if True:
            print("Open file 1...")
            data = open('/home/pi/zack/14zpallnoagi.csv', 'r').read().split("\n")
            print("Process file 1...")
            
            for i, d in enumerate(data[1:]):
                if i % 3000 == 0:
                    print("\t%d / %d @ %s" % (i, len(data), datetime.datetime.today()))
                #print(d)
                tokens = d.split(',')
                if len(tokens) > 2:
                    #print(tokens[2])
                    if tokens[2] not in zips:
                        zips.append(tokens[2])
        if True:
            print("Open file 3...")
            data = open('/home/pi/zack/rows.csv', 'r').read().split("\n")
            print("Process file 3...")
            for i, d in enumerate(data[1:]):
                if i % 25000 == 0:
                    print("\t%d / %d @ %s" % (i, len(data), datetime.datetime.today()))
                #print(d)
                tokens = d.split(',')
                if len(tokens) > 2:
                    #print(tokens[2])
                    if tokens[2] not in zips:
                        zips.append(tokens[2])
        print("Found %d total zips..." % (len(zips)))
        f = open('/home/pi/zack/zip_codes.csv', 'w')
        for z in zips:
            f.write("%s\n" % z)
        f.close()
        sys.exit()
    return zips
category_filter = "bakeries"
params = {
    'term': 'food',
    'category_filter': category_filter
}
params2 = {
    'term': 'food',
    'category_filter': category_filter,
    'offset': 20
}

zips = get_zips()
print("There are %d zips that need to be searched..." % len(zips))


auth = Oauth1Authenticator(
    consumer_key=YOUR_CONSUMER_KEY,
    consumer_secret=YOUR_CONSUMER_SECRET,
    token=YOUR_TOKEN,
    token_secret=YOUR_TOKEN_SECRET
)
client = Client(auth)

log_file = '/home/pi/zack/Logs/scan_yelp_log_%s.txt' % (datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
log_ = open(log_file, 'w')
log_.close()
def log_msg(s, no_print=False):
    if not no_print:
        print(s)
    log = open(log_file, 'a')
    log.write("%s  %s\n" % (datetime.datetime.today().strftime("%H:%M:%S"),s))
    log.close()

weekday = datetime.datetime.today().weekday()
hr_period = int(int((datetime.datetime.today() - datetime.timedelta(hours=6)).hour) / 8)

# 3 8-hr periods
# 6 A - 2 P
# 2 P - 10P
# 10P - 6 A
mysql_conn, r = mysql_connect();  mysql_attempts = 0
while mysql_conn is None and mysql_attempts < 10:
    time.sleep(15)
    mysql_conn, r = mysql_connect();  mysql_attempts += 1
if mysql_conn is None:
    log_msg("Could not connect to MySQL after 10 tries...exiting.")

cursor = mysql_conn.cursor()



scriptname = "scan_yelp"
query = "SELECT local_or_remote from Capozzi_Scripts where name=%s"
param = [scriptname]
cursor.execute(query, param)
local_or_remote = open('/home/pi/zack/local_or_remote', 'r').read().strip()
row = cursor.fetchone()
if local_or_remote != row[0]:
    print("Do not run %s because this host isn't the one that's supposed to be running it ( %s vs %s )" % (scriptname, local_or_remote, row[0]))
    sys.exit()
    
query = "SELECT ID from Data_Capture_Campaigns where name like '%Yelp%' and active=1"
cursor.execute(query)
data_campaign_IDs = cursor.fetchall()
cursor.close(); mysql_conn.close()
time.sleep(.2)

print("hr period: %d" % hr_period)

for data_campaign_ID in data_campaign_IDs:
    data_campaign_ID = data_campaign_ID[0]
    mysql_conn, r = mysql_connect();  mysql_attempts = 0
    while mysql_conn is None and mysql_attempts < 10:
        time.sleep(15)
        mysql_conn, r = mysql_connect();  mysql_attempts += 1
    if mysql_conn is None:
        log_msg("Could not connect to MySQL after 10 tries...exiting.")

    cursor = mysql_conn.cursor()
    query = "SELECT repository from Data_Capture_Campaigns where ID=%s"
    param = [data_campaign_ID]
    cursor.execute(query, param)
    r = cursor.fetchone()
    repo = r[0]
    log_msg("Use the repository at %s" % repo)
    group_by_query = "SELECT search_zip, count(1) from Yelp_Listings where active=1 group by search_zip"
    cursor.execute(group_by_query)
    count_by_zips = cursor.fetchall()
    gb_zips = []
    gb_counts = []
    log_msg("Creating list of listings by zip from db...")
    for c in count_by_zips:
        gb_zips.append(c[0])
        gb_counts.append(c[1])

    cursor.close(); mysql_conn.close()
    count_query = "SELECT count(1) from Yelp_Listings where active=1 and search_zip=%s"
    search_query = "SELECT count(1) from Yelp_Listings where active=1 and yelp_ID=%s and data_capture_campaign_ID=%s"
    insert_query = "INSERT INTO Yelp_Listings (time_captured, search_zip, yelp_ID, data_capture_campaign_ID, yelp_name, yelp_url, external_url, yelp_category, yelp_phone, is_closed, yelp_city, yelp_state, yelp_zip, country_code, manually_scanned, scanned_ext_for_emails, active) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    log_msg("Adding zips with no listings...")
    for z_count, z in enumerate(zips):
        if z_count % 10000 == 0:
            log_msg("Processing %d / %d" % (z_count, len(zips)))
        if z not in gb_zips:
            gb_zips.append(z)
            gb_counts.append(0)

    log_msg("Start processing zip codes...")
    for z_count, zip_code in enumerate(zips):

        if zip_code != "00000":

            file_exists = os.path.isfile(os.path.join(repo, "auto_yelp_zip_code_%s.txt" % zip_code))
            if gb_counts[gb_zips.index(zip_code)] == 0 and not file_exists:
                per_call_delay = int(re.compile(r'per_call_delay\: ([0-9]+)').search(open('/home/pi/zack/scan_yelp_parameters', 'r').read()).group(1))

                mysql_conn, r = mysql_connect();  mysql_attempts = 0
                while mysql_conn is None and mysql_attempts < 10:
                        time.sleep(15)
                        mysql_conn, r = mysql_connect();  mysql_attempts += 1
                if mysql_conn is None:
                        log_msg("Could not connect to MySQL after 10 tries...exiting.")
                        sys.exit()
                        
                cursor = mysql_conn.cursor()
                cursor.execute("SET collation_connection='utf8_general_ci'")
                cursor.execute("ALTER DATABASE monoprice CHARACTER SET utf8 COLLATE utf8_general_ci")
                cursor.execute("ALTER TABLE Yelp_Listings CONVERT TO CHARACTER SET utf8 COLLATE utf8_general_ci")
            
                print("Being a good internet citizen and waiting for %d seconds before searching %s" % (per_call_delay, zip_code))
                time.sleep(per_call_delay)
                try:
                    s = client.search(zip_code, **params)
                    log_msg("Being a good internet citizen and waiting for %d seconds before searching %s" % (per_call_delay, zip_code))
                    time.sleep(per_call_delay)
                    s2 = client.search(zip_code, **params2)
                    log_msg("\tReturned %d businesses" % (s2.total + s.total))
                    f = open(os.path.join(repo, "auto_yelp_zip_code_%s.txt" % zip_code), 'w')
                    f.write("\tReturned %d businesses\n" % (s2.total + s.total))
                    f.close()
                except yelp.errors.InternalError as e:
                    s = None
                    log_msg("\t500 Internal Server error for location: %s (%s)" % (zip_code, e))
                    
                except ValueError as e:
                    s = None
                    log_msg("\tValue Error error for location: %s (%s)" % (zip_code, e))
                    
                except yelp.errors.InvalidSignature as e:
                    s = None
                    log_msg("\tInvalidSignature error for location: %s (%s)" % (zip_code, e))
                    
                except urllib2.URLError as e:
                    s = None
                    log_msg("\turllib2.URLError error for location: %s (%s)" % (zip_code, e))
                    
                except yelp.errors.UnavailableForLocation:
                    s = None
                    log_msg("\tNo API access for location: %s" % (zip_code))
                    f = open(os.path.join(repo, "auto_yelp_zip_code_%s.txt" % zip_code), 'w')
                    f.write("\tNo API access for location\n")
                    f.close()
                if s is not None:
                    new_listings = 0
                    for i, l in enumerate(s.businesses):
                        log_msg(l, no_print=True)
                        try:
                            bname = l.name.encode("utf-8")
                            l_id = l.id.encode("utf-8")
                        except UnicodeDecodeError as e:
                            log_msg("Error encoding %s with utf-8" % (l.name))
                            bname = l.name.encode("latin-1")
                            l_id = l.id.encode("latin-1")
                            log_msg("Try encoding %s with latin-1" % (bname))
                        print("\tBusiness Name: %s" % (bname))
                        print("\tBusiness ID: %s" % (l_id))
                        print("\tBusiness Phone: %s" % (l.phone))
                        search_param = [l_id, data_campaign_ID]
                        insert_param = [datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), zip_code, l_id, data_campaign_ID, bname, l.url, '', category_filter, l.phone, l.is_closed, l.location.city, l.location.state_code, l.location.postal_code, l.location.country_code, 0, 0, 1]
                        try:
                            cursor.execute(search_query, search_param)
                        except MySQLdb.OperationalError as e:
                                if "MySQL server has gone away" in str(e):
                                    log_msg("Attempting to restart MySQL connection... %s (r8k34cb)" % e)
                                    time.sleep(15)
                                    mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()
                                    cursor.execute(search_query, search_param)
                                else:
                                    log_msg("Unknown MySQL error...  %s (e5kpqcb)" % e)
                                    cursor.execute(search_query, search_param)
                        r = cursor.fetchone()
                        if int(r[0]) == 0:
                            #print("\t\tExecute %s w/ %s" % (insert_query, insert_param))
                            cursor.execute(insert_query, insert_param)
                            new_listings += 1
                        else:
                            print("\t\tAlready stored in the DB...")    
                    for i, l in enumerate(s2.businesses):
                        log_msg(l, no_print=True)
                        try:
                            bname = l.name.encode("utf-8")
                            l_id = l.id.encode("utf-8")
                        except UnicodeDecodeError as e:
                            log_msg("Error encoding %s with utf-8" % (l.name))
                            bname = l.name.encode("latin-1")
                            l_id = l.id.encode("latin-1")
                            log_msg("Try encoding %s with latin-1" % (bname))
                        print("\tBusiness Name: %s" % (bname))
                        print("\tBusiness ID: %s" % (l_id))
                        print("\tBusiness Phone: %s" % (l.phone))
                        search_param = [l_id, data_campaign_ID]
                        insert_param = [datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), zip_code, l_id, data_campaign_ID, bname, l.url, '', category_filter, l.phone, l.is_closed, l.location.city, l.location.state_code, l.location.postal_code, l.location.country_code, 0, 0, 1]
                        cursor.execute(search_query, search_param)
                        r = cursor.fetchone()
                        if int(r[0]) == 0:
                            #print("\t\tExecute %s w/ %s" % (insert_query, insert_param))
                            cursor.execute(insert_query, insert_param)
                            new_listings += 1
                            mysql_conn.commit(); 
                        else:
                            print("\t\tAlready stored in the DB...")
                            
                    print("Added %d new listing(s)..." % new_listings)
                cursor.close(); mysql_conn.close()
        coming_period = int(int((datetime.datetime.today() - datetime.timedelta(hours=6) + datetime.timedelta(minutes=15)).hour) / 8)
        if hr_period != coming_period:
            log_msg("End of session at %s" % (datetime.datetime.today().strftime("%H:%M:%S")))
            
            #bot, chat_id = setup_telegram()
            #msg = "End of scan_yelp session at %s" % (datetime.datetime.today().strftime("%H:%M:%S"))
            #bot.sendMessage(chat_id=chat_id, text=msg)
            log_msg("Done!!!")
            sys.exit()
log_msg("Done!!!")
