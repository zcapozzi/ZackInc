import time, os, datetime, sys, psutil, random
import re
import MySQLdb
import subprocess
import telepot
from bs4 import BeautifulSoup
import requests
#import httplib2
from BeautifulSoup import BeautifulSoup, SoupStrainer
reload(sys)
sys.setdefaultencoding("utf-8")

from twython import Twython
from twython import TwythonAuthError, TwythonError
import telepot; bot_token = "308120049:AAFBSyovjvhlYAe1xeTO2HAvYO4GBY3xudc"

import errno
from socket import error as socket_error

from subprocess import Popen, PIPE

import clipboard
sys.path.insert(0, "/home/pi/zack/ZackInc")
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



##########################
commit_results = True  #
##########################

# Create a connection to the database
mysql_conn, response = mysql_connect(); cursor = mysql_conn.cursor()
query = "SELECT email_campaign_descriptor, twitter_handle from GA_Tracking_Tags where campaign_source like '%Forum%'"

cursor.execute(query)
res = cursor.fetchall()

regex = []
regex.append(re.compile(r'([0-9,]+) post', re.IGNORECASE))
regex.append(re.compile(r'Read ([0-9,]+) time', re.IGNORECASE))
for r in res:
    url = r[1]
    forum = r[0]
    print("Read from %s" % url)
    response = requests.get(url, timeout=20, headers={'user-agent': 'trololololololol 900'})
    print(response.status_code)
    if response.status_code == 200:
        data = response.content
        uniques = []
        for r in regex:
            matches = re.findall(r, data)

            for m in matches:
                print("\tFound a match: %s" % str(m))
                if int(m.replace(",", "")) not in uniques:
                    uniques.append(int(m.replace(",", "")))
        for m in uniques[0:1]:
            count = m
            query = "INSERT INTO Forum_Post_Counts(datestamp, forum, post_count) VALUES (%s, %s, %s)"
            param = [datetime.datetime.now(), forum, count]
            print ("\t\tQuery %s w/ %s" % (query, param))
            cursor.execute(query, param)
        if len(uniques) > 1:
            print("Error: more than one match....")
if commit_results:
    mysql_conn.commit()
else:
    print("\n\n\tREMINDER, nothing is being committed!!!!\n\n")
cursor.close(); mysql_conn.close()

if False:
    bot = telepot.Bot(token=bot_token)

    # Waits for the first incoming message
    updates=[]
    while not updates:
        updates = bot.getUpdates()

    # Gets the id for the active chat

    chat_id=updates[-1]['message']['chat']['id']

    # Sends a message to the chat
    msg = "Done with update_twitter_accounts\n  %s accounts updated\n  %s followers added to tweet capture" % ("{:,}".format(updated_accounts), "{:,}".format(followers_added_to_capture))
    bot.sendMessage(chat_id=chat_id, text=msg)
    print("telegram %s" % msg)

