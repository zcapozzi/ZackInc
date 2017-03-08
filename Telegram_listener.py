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

import telegram;

import zlib

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

        
updates = bot.getUpdates()
    
for u in updates:
    
    msg = u['message']
    if msg['message_id'] not in ids:
        ids.append(msg['message_id'])
        
if chat_id is None:
        chat_id=updates[-1].message.chat_id

last_text = None
while start_hour == datetime.datetime.now().hour:
    
    updates = bot.getUpdates()
    
    for u in updates:
        
        msg = u['message']
        if msg['message_id'] not in ids:
            ids.append(msg['message_id'])
            text = msg['text']
            if text.lower().strip() == "last":
                text = last_text
                print("Re-process the last request...")
            sent_at = msg['date']
            sent_by = msg['from_user']['first_name']
            print("\n\n>> %s\n\n" % sent_by)
            print("At %s, %s sent '%s'" % (sent_at, sent_by, text))
            
            if sent_by == 'Zack':
                if text.lower().startswith('backup'):
                    if "lacrosse" in text.lower()[6:].strip():
                        args = ['/home/pi/zack/backup_lacrosse_reference.sh', '&']
                        print(args)
                        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
            last_text = text
                        
    time.sleep(5)    
        
    #sys.exit()

if False:
    # Gets the id for the active chat
    chat_id=updates[-1].message.chat_id

    # Sends a message to the chat
    bot.sendMessage(chat_id=chat_id, text=msg)
