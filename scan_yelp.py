import time, os, datetime, sys, psutil
import re
import MySQLdb
import subprocess
import telegram

from subprocess import Popen, PIPE

from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator
import yelp

def send_telegram(s):
    # Connect to our bot
    bot = telegram.Bot(token="308120049:AAFBSyovjvhlYAe1xeTO2HAvYO4GBY3xudc")

    # Waits for the first incoming message
    updates=[]
    while not updates:
        updates = bot.getUpdates()
        
    # Gets the id for the active chat
    #print updates[-1].message.text
    chat_id=updates[-1].message.chat_id

    # Sends a message to the chat
    
    for i, msg in enumerate(msgs):
        bot.sendMessage(chat_id=chat_id, text=s)


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

zips = open('/home/pi/zack/zip_codes.csv', 'r').read().split("\n")  
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
    log.write("%s\n" % s)
    log.close()

data_campaign_ID = 3
query = "SELECT repository from Data_Capture_Campaigns where ID=%s"
param = [data_campaign_ID]
mysql_conn, r = mysql_connect();  mysql_attempts = 0
while mysql_conn is None and mysql_attempts < 10:
    time.sleep(15)
    mysql_conn, r = mysql_connect();  mysql_attempts += 1
if mysql_conn is None:
    log_msg("Could not connect to MySQL after 10 tries...exiting.")
    sys.exit()
cursor = mysql_conn.cursor()
cursor.execute(query, param)
r = cursor.fetchone()
repo = r[0]
print("Use the repository at %s" % repo)
cursor.close(); mysql_conn.close()
count_query = "SELECT count(1) from Yelp_Listings where active=1 and search_zip=%s"
search_query = "SELECT count(1) from Yelp_Listings where active=1 and yelp_ID=%s and data_capture_campaign_ID=%s"
insert_query = "INSERT INTO Yelp_Listings (time_captured, search_zip, yelp_ID, data_capture_campaign_ID, yelp_name, yelp_url, external_url, yelp_category, yelp_phone, is_closed, yelp_city, yelp_state, yelp_zip, country_code, manually_scanned, scanned_ext_for_emails, active) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

per_call_delay = 15
for z_count, zip_code in enumerate(zips):
    if zip_code != "00000":
        count_param = [zip_code]
        
        mysql_conn, r = mysql_connect();  mysql_attempts = 0
        while mysql_conn is None and mysql_attempts < 10:
            time.sleep(15)
            mysql_conn, r = mysql_connect();  mysql_attempts += 1
        if mysql_conn is None:
            log_msg("Could not connect to MySQL after 10 tries...exiting.")
            sys.exit()
        cursor = mysql_conn.cursor()
        cursor.execute(count_query, count_param)
        r = cursor.fetchone()
        file_exists = os.path.isfile(os.path.join(repo, "auto_yelp_zip_code_%s.txt" % zip_code))
        if int(r[0]) == 0 and not file_exists:
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
            except yelp.errors.UnavailableForLocation:
                s = None
                print("\tNo API access for location: %s" % (zip_code))
                f = open(os.path.join(repo, "auto_yelp_zip_code_%s.txt" % zip_code), 'w')
                f.write("\tNo API access for location\n")
                f.close()
            if s is not None:
                new_listings = 0
                for i, l in enumerate(s.businesses):
                    print("\tBusiness Name/ID/Phone: %s\t%s\t%s" % (l.name, l.id, l.phone))
                    search_param = [l.id, data_campaign_ID]
                    insert_param = [datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), zip_code, l.id, data_campaign_ID, l.name, l.url, '', category_filter, l.phone, l.is_closed, l.location.city, l.location.state_code, l.location.postal_code, l.location.country_code, 0, 0, 1]
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
                                sys.exit()
                    r = cursor.fetchone()
                    if int(r[0]) == 0:
                        #print("\t\tExecute %s w/ %s" % (insert_query, insert_param))
                        cursor.execute(insert_query, insert_param)
                        new_listings += 1
                    else:
                        print("\t\tAlready stored in the DB...")
                        
                for i, l in enumerate(s2.businesses):
                    print("\tBusiness Name/ID/Phone: %s\t%s\t%s" % (l.name, l.id, l.phone))
                    search_param = [l.id, data_campaign_ID]
                    insert_param = [datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), zip_code, l.id, data_campaign_ID, l.name, l.url, '', category_filter, l.phone, l.is_closed, l.location.city, l.location.state_code, l.location.postal_code, l.location.country_code, 0, 0, 1]
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
        
