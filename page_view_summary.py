import time, datetime, sys, os
from time import strptime
from datetime import date
import calendar
import mysql.connector
import gc
from mysql.connector import errorcode
import requests
from bs4 import BeautifulSoup
import smtplib
import re
import httplib2, httplib, zlib, ssl
from socket import error as socket_error
from BeautifulSoup import BeautifulSoup, SoupStrainer
import telegram; bot_token = "308120049:AAFBSyovjvhlYAe1xeTO2HAvYO4GBY3xudc"
import random
import subprocess
import statistics, numpy
import MySQLdb
from StringIO import StringIO
from cycler import cycler

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

sys.path.insert(0, "../ZackInc")
import zack_inc as zc
import Image

from matplotlib.font_manager import FontProperties


def mysql_connect():
    cnx = None
    try:
        
        # Connecting from an external network.
        # Make sure your network is whitelisted
        #logging.info("Connecting via remote %s..." % app.config['DBNAME'])
        #client_cert_pem = "instance/client_cert_pem"
        #client_key_pem = "instance/client_key_pem"
        #ssl = {'cert': client_cert_pem, 'key': client_key_pem}
         
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
        #log_msg("Connection error: %s" % err)
        
    return cnx, response

send_images = True
if len(sys.argv) > 1:
    for i in range(1, len(sys.argv)):
        if sys.argv[i] == '-no-send':
            send_images = False
            
            
metrics = ['pageviews', 'newUsers', 'Users', 'timeOnPage', 'organicSearches']
weeks_back = 3
for metric in metrics:
    mysql_conn, response = mysql_connect()
    cursor = mysql_conn.cursor()
    query = "SELECT dimension_description, data, timestamp from GA_Dimension_Metrics where dimension_description like '%%<.>%%' and description=%s and metric=%s and dimension_description not like '%%wp-%%' and timestamp>STR_TO_DATE(%s, '%%Y-%%m-%%d %%H:%%i:%%s') order by timestamp asc, dimension_description asc"
    param = [metric, "hostname, ga:pagePath", (datetime.datetime.today()-datetime.timedelta(days=7*weeks_back)).strftime("%Y-%m-%d %H:%M:%S")]
    cursor.execute(query, param)
    res = cursor.fetchall()
    urls = []
    url_code_combos = []
    dates = []
    y_posts = []
    y_pages = []
    x = []
    x_dates = []
    num_dates = 0
    
    if datetime.datetime.today().strftime("%Y-%m-%d") == "2017-03-14":
        dates.append("03-13")
        x.append("03-13")
        x_dates.append(13)
        num_dates += 1
    
    for r in res:
        #print(r)
        dt = r[2].strftime("%m-%d")
        url = r[0].split(r"<.>")[1].strip()
        code = r[0].split(r"<.>")[0].strip()
        d = {'url': url, 'code': code}
        
        if url not in urls:
            urls.append(url)
            
            
        if d not in url_code_combos:
            url_code_combos.append(d)
        if dt not in dates:
            dates.append(dt)
            x.append(dt)
            x_dates.append(r[2].day)
            num_dates += 1
    for u in urls:
        y_posts.append([0] * num_dates)
        y_pages.append([0] * num_dates)
    #print("X: %s" % str(x))
    #print(y)
    #sys.exit()
    for i, u in enumerate(urls):
        rec = {'url': zc.get_url_title(u, cursor), 'json': '', 'ID': i}

        for uc in url_code_combos:
            if uc['url'] == u:
                for r in res:
                    url = r[0].split(r"<.>")[1].strip()
                    code = r[0].split(r"<.>")[0].strip()
                    c = {'url': url, 'code': code }
                    dt = r[2].strftime("%m-%d")
                    
                    if c == uc:
                        print("Write to %d" % urls.index(u))
                        print("\t%s" % str(c))
                        print("\t%s" % str(u))
                        
                        if u.startswith("/201"):
                            y_posts[urls.index(u)][x.index(dt)] = r[1]
                        else:
                            y_pages[urls.index(u)][x.index(dt)] = r[1]
                        
    ys = [{'vals': y_posts, 'title': "Posts"}, {'vals': y_pages, 'title': "Pages"}]
    for ys_ in ys:
                                
        fig = plt.figure()
        ax1 = fig.add_axes((.1, .2, .8, .7))
        ax1.set_title("%s - %s" % (metric.title(), ys_['title']), loc='left')
        ax1.set_ylabel("# of %s" % metric.title())
        ax1.set_prop_cycle(cycler('color', ['r', 'g', 'b', 'y', 'c', 'm', 'k']))
        
        fontP = FontProperties()
        fontP.set_size('small')
        series = 0
        for y_, u in zip(ys_['vals'], urls):
            if sum(y_) > 0:
                print("For %s \n\tPlot %s vs \n\t%s" % (zc.get_url_title(u, cursor), str(x_dates), str(y_)))
                ax1.plot(x_dates, y_, label=zc.get_url_title(u, cursor), linewidth=2.0)
                series += 1
        if series > 0:
            plt.xticks(x_dates, dates)
            #ax1.legend(loc='best')
            #print("Shift it
            offset = -.3 - .04*series
            # -.50 is good for 5
            # -.70 is good for 10
            lgd = ax1.legend(bbox_to_anchor=(0,offset), loc='lower left', ncol=1)
            plt.savefig('/home/pi/zack/%sSummary (%s).png' % (metric.title(), ys_['title']), bbox_extra_artists=(lgd,), bbox_inches='tight'); 
            
            if send_images:
                bot = telegram.Bot(token=bot_token)

                # Waits for the first incoming message
                updates=[]
                while not updates:
                    updates = bot.getUpdates()
                    
                # Gets the id for the active chat

                chat_id=updates[-1].message.chat_id

                # Sends a message to the chat
                #output = StringIO(open(.read())
                #img = Image.open()
                bot.sendPhoto(chat_id=chat_id, photo=open('/home/pi/zack/%sSummary (%s).png' % (metric.title(), ys_['title']), 'rb'))
            

        plt.close()     
        
   
        
cursor.close(); mysql_conn.close()  
