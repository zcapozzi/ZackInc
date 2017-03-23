# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 14:10:05 2015

@author: zcapozzi002
"""

import time, os, datetime, sys, psutil, random, statistics
import re
import MySQLdb
import subprocess
import telegram; bot_token = open("/home/pi/zack/telegram_bot_token", 'r').read().strip()
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
import telegram.error

import zlib
sys.path.insert(0, "../ZackInc")
import zack_inc as zc


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

bot = telegram.Bot(token=bot_token)

# Waits for the first incoming message
print("Start")
updates=[]
start_hour = datetime.datetime.now().hour
chat_id = None
ids = []

while len(updates) == 0 and start_hour == datetime.datetime.now().hour:        
    updates = bot.getUpdates()
    time.sleep(1)

if len(updates) > 0:
    for u in updates:
        
        msg = u['message']
        if msg['message_id'] not in ids:
            ids.append(msg['message_id'])
            
    if chat_id is None:
            chat_id=updates[-1].message.chat_id

    last_text = None
    while start_hour == datetime.datetime.now().hour:
        
        try:
            updates = bot.getUpdates()
        except telegram.error.TimeOut as e:
            print("Bot timed out, waiting 10 seconds...")
            time.sleep(10)
            updates = []
        for u in updates:
            
            msg = u['message']
            if msg['message_id'] not in ids:
                ids.append(msg['message_id'])
                text = msg['text'].strip()
                if text.lower().strip() == "last":
                    text = last_text
                    print("Re-process the last request...")
                sent_at = msg['date']
                sent_by = msg['from_user']['first_name']
                print("\n\n>> %s\n\n" % sent_by)
                print("At %s, %s sent '%s'" % (sent_at, sent_by, text))
                return_msg = None
                if sent_by == 'Zack':
                    if text.lower().startswith('backup'):
                        if "lacrosse" in text.lower()[6:].strip():
                            args = ['/home/pi/zack/backup_lacrosse_reference.sh', '&']
                            print(args)
                            proc = subprocess.Popen(args, stdout=subprocess.PIPE)
                    elif text.lower().startswith("add twitter"):
                        mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor();
                        tokens = text.split(" ")
                        twitter_handle = tokens[2]
                        if not twitter_handle.startswith("@"):
                            twitter_handle = "@" + twitter_handle
                        
                        query = "SELECT count(1) from Twitter_Accounts where twitter_handle=%s and active=1"
                        param = [twitter_handle]
                        cursor.execute(query, param)
                        r = cursor.fetchone()
                        if r[0] > 0:
                            return_msg = "Twitter handle already followed."
                        else:
                            classification1 = ""
                            if len(tokens) > 3:
                                classification1 = tokens[3]
                            
                            follow_mentioned_accounts = 0
                            query = "INSERT INTO Twitter_Accounts (ID, name, probable_twitter_handle, twitter_handle, twitter_ID, twitter_verified, created_on, last_twitter_download, daily_tweets, last_twitter_search, last_twitter_peer_search, active, total_twitter_downloads, auto_sourced_via_mention, auto_sourced_via_mention_account, follow_mentioned_accounts, classification1, yelp_listing_ID) "
                            query += "VALUES ((SELECT count(1) + 1 from Twitter_Accounts fd), '', '', %s, '', 0, %s, '1900-01-01 01:00:00', 0, '1900-01-01 01:00:00', '1900-01-01 01:00:00', 1, 0, 0, '', %s, %s, '')"
                            param = [twitter_handle, datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), follow_mentioned_accounts, classification1]
                            cursor.execute(query, param)
                            return_msg = "%s has been added w/ classification='%s'." % (twitter_handle, classification1)
                            mysql_conn.commit()    
                        cursor.close(); mysql_conn.close()
                if return_msg is not None:
                    bot.sendMessage(chat_id=chat_id, text=return_msg)
                last_text = text
                            
        time.sleep(3)    
