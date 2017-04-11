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
bot_token = "308120049:AAFBSyovjvhlYAe1xeTO2HAvYO4GBY3xudc"
import telepot

import random
import subprocess
import statistics, numpy
import MySQLdb
from StringIO import StringIO
from cycler import cycler

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

sys.path.insert(0, "/home/pi/zack/ZackInc")
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



mysql_conn, response = mysql_connect()
cursor = mysql_conn.cursor()
query = "SELECT SUBSTRING(dimension_description, LOCATE('>', dimension_description)+1), avg(data), DATE(timestamp)  from GA_Dimension_Metrics where property_ID=2 and dimension_description like '%%<.>%%' and description=%s and metric=%s and dimension_description not like '%%wp-%%' group by SUBSTRING(dimension_description, LOCATE('>', dimension_description)+1), DATE(timestamp) "
#query += " limit 50"
param = ['pageviews', "hostname, ga:pagePath"]
start_query_ms = zc.current_milli_time()
cursor.execute(query, param)
res1 = cursor.fetchall()
end_query_ms = zc.current_milli_time()
elapsed_ms = end_query_ms - start_query_ms

if False: # If we wanted to try to capture all data, we'd use this because starting in Mar (14th), we started to capture the referrer and the url
    query = "SELECT dimension_description, avg(data), DATE(timestamp) from GA_Dimension_Metrics where property_ID=2 and dimension_description not like '%%<.>%%' and description=%s and metric=%s and dimension_description not like '%%wp-%%' group by dimension_description, DATE(timestamp) order by 3 asc"
    #query += " limit 50"
    param = ['pageviews', "hostname, ga:pagePath"]
    cursor.execute(query, param)
    res2 = cursor.fetchall()

    res = res1 + res2
else:
    res = res1

urls = []
for r in res:
    #print(r)
    url = r[0].strip()

    label = zc.get_url_title(url, cursor)
    dt = datetime.datetime.strptime(r[2].strftime("%Y-%m-%d"), "%Y-%m-%d")
    if label not in [u['url'] for u in urls]:
        url_type = ""

        if "/201" in r[0]:
            url_type = "Post"
        elif "/category" in r[0]:
            url_type = "Category"
        elif "/?s" in r[0]:
            url_type = "Search"
        else:
            url_type = "Page"

        urls.append({'url': label, 'total_views': 0, 'min_date': dt, 'max_date': dt, 'type': url_type})
    else:
        if dt < urls[[t['url'] for t in urls].index(label)]['min_date']:
            urls[[t['url'] for t in urls].index(label)]['min_date'] = dt
        if dt > urls[[t['url'] for t in urls].index(label)]['max_date']:
            urls[[t['url'] for t in urls].index(label)]['max_date'] = dt

display_posts_urls = []
display_pages_urls = []
display_search_urls = []
display_category_urls = []

for target_url in urls:
    #print("Process views for %s, which has data from %s to %s" % (target_url['url'], target_url['min_date'], target_url['max_date']))
    total_cnt = 0.0
    total_views = 0.0
    res_cnt = 0
    for i, r in enumerate(res):


        url = r[0].strip()
        label = zc.get_url_title(url, cursor)
        if target_url['url'] == label:
            res_cnt += 1

    processed_cnt = 0
    for r in res:


        url = r[0].strip()
        label = zc.get_url_title(url, cursor)
        if target_url['url'] == label:
            processed_cnt += 1
            if "Ohio" in label:
                print(r)

            dt = datetime.datetime.strptime(r[2].strftime("%Y-%m-%d"), "%Y-%m-%d")
            num_views = r[1]
            if "Ohio" in label:
                print ("\tURL: %s\t%s - %d" % (label, dt.strftime("%Y-%m-%d"), num_views))

            total_views += num_views
            total_cnt += 1.0

    if total_views > 35:
        if target_url['type'] == "Post":
            display_posts_urls.append({'url': target_url['url'][0:28], 'est_views': total_views/7.0})
        elif target_url['type'] == "Page":
            display_pages_urls.append({'url': target_url['url'][0:28], 'est_views': total_views/7.0})
        elif target_url['type'] == "Category":
            display_category_urls.append({'url': target_url['url'][0:28], 'est_views': total_views/7.0})
        elif target_url['type'] == "Search":
            display_search_urls.append({'url': target_url['url'][0:28], 'est_views': total_views/7.0})
    else:
        if target_url['type'] == "Category":
            display_category_urls.append({'url': target_url['url'][0:28], 'est_views': total_views/7.0})
        elif target_url['type'] == "Search":
            display_search_urls.append({'url': target_url['url'][0:28], 'est_views': total_views/7.0})

display_posts_urls = sorted(display_posts_urls, key=lambda x: x['est_views'], reverse=True)
display_pages_urls = sorted(display_pages_urls, key=lambda x: x['est_views'], reverse=True)
display_category_urls = sorted(display_category_urls, key=lambda x: x['est_views'], reverse=True)
display_search_urls = sorted(display_search_urls, key=lambda x: x['est_views'], reverse=True)

print("For display\n----------------------------------")
loop = 0
fig = plt.figure()

send_images = True
for i, d in enumerate(display_posts_urls):
    ax1 = fig.add_axes((.1, .2, .8, .7))
    if i % 7 == 0:
        x = [0]*7; x_off = [0]*7; y = [0]*7; labels = ['']*7
        loop += 1
    print("%s%.1f" % ("{:<150}".format(d['url']), d['est_views']))

    x[i%7] = (i % 7); x_off[i%7] = (i % 7) - .25
    y[i%7] = d['est_views']; labels[i%7] = d['url']

    if i % 7 == 6 or i + 1 == len(display_posts_urls):
        title_str = "PageViews for Posts (All Time)"; ax1.set_title(title_str, loc='left')

        opacity = 0.4; plt.bar(x, y, alpha=opacity, color='b'); ax1.set_ylabel('PageViews')

        plt.xticks(x_off,labels, rotation=35, size=7)
        l = ax1.yaxis.get_ticklocs().tolist()
        plt.yticks(l, ["%s" % "{:,}".format(int(z)) if z > 0 and i + 1 < len(l) else "" for i, z in enumerate(l)])

        ax1.legend(loc='best'); plt.gcf().subplots_adjust(bottom=0.30)
        plt.savefig(os.path.join('/home/pi/zack/LacrosseReference', "Images", "View Counts - Posts (%d).png" % (loop))); plt.clf()
        if send_images and loop == 1:
            bot = telepot.Bot(token=bot_token)

            # Waits for the first incoming message
            updates=[]
            while not updates:
                updates = bot.getUpdates()

            # Gets the id for the active chat
            chat_id=updates[-1]['message']['chat']['id']

            # Sends a message to the chat
            bot.sendPhoto(chat_id=chat_id, photo=open(os.path.join('/home/pi/zack/LacrosseReference', "Images", "View Counts - Posts (%d).png" % (loop)), 'rb'))

loop = 0
for i, d in enumerate(display_pages_urls):
    ax1 = fig.add_axes((.1, .2, .8, .7))
    if i % 7 == 0:
        x = [0]*7; x_off = [0]*7; y = [0]*7; labels = ['']*7
        loop += 1
    print("%s%.1f" % ("{:<150}".format(d['url']), d['est_views']))

    x[i%7] = (i % 7); x_off[i%7] = (i % 7) - .25
    y[i%7] = d['est_views']; labels[i%7] = d['url']

    if i % 7 == 6 or i + 1 == len(display_pages_urls):
        title_str = "PageViews for Pages (All Time)"; ax1.set_title(title_str, loc='left')

        opacity = 0.4; plt.bar(x, y, alpha=opacity, color='b'); ax1.set_ylabel('PageViews')

        plt.xticks(x_off,labels, rotation=35, size=7)
        l = ax1.yaxis.get_ticklocs().tolist()
        plt.yticks(l, ["%s" % "{:,}".format(int(z)) if z > 0 and i + 1 < len(l) else "" for i, z in enumerate(l)])

        ax1.legend(loc='best'); plt.gcf().subplots_adjust(bottom=0.30)
        plt.savefig(os.path.join('/home/pi/zack/LacrosseReference', "Images", "View Counts - Pages (%d).png" % (loop))); plt.clf()
        if send_images and loop == 1:
            bot = telepot.Bot(token=bot_token)

            # Waits for the first incoming message
            updates=[]
            while not updates:
                updates = bot.getUpdates()

            # Gets the id for the active chat
            chat_id=updates[-1]['message']['chat']['id']

            # Sends a message to the chat
            bot.sendPhoto(chat_id=chat_id, photo=open(os.path.join('/home/pi/zack/LacrosseReference', "Images", "View Counts - Pages (%d).png" % (loop)), 'rb'))

loop = 0
for i, d in enumerate(display_category_urls):
    ax1 = fig.add_axes((.1, .2, .8, .7))
    if i % 7 == 0:
        x = [0]*7; x_off = [0]*7; y = [0]*7; labels = ['']*7
        loop += 1
    print("%s%.1f" % ("{:<150}".format(d['url']), d['est_views']))

    x[i%7] = (i % 7); x_off[i%7] = (i % 7) - .25
    y[i%7] = d['est_views']; labels[i%7] = d['url']

    if i % 7 == 6 or i + 1 == len(display_category_urls):
        title_str = "PageViews for Categories (All Time)"; ax1.set_title(title_str, loc='left')

        opacity = 0.4; plt.bar(x, y, alpha=opacity, color='b'); ax1.set_ylabel('PageViews')

        plt.xticks(x_off,labels, rotation=35, size=7)
        ax1.set_ylim([0, max(y)+5])
        l = ax1.yaxis.get_ticklocs().tolist()
        plt.yticks(l, ["%s" % "{:,}".format(int(z)) if z > 0 and i + 1 < len(l) else "" for i, z in enumerate(l)])

        ax1.legend(loc='best'); plt.gcf().subplots_adjust(bottom=0.30)
        plt.savefig(os.path.join('/home/pi/zack/LacrosseReference', "Images", "View Counts - Categories (%d).png" % (loop))); plt.clf()

        if send_images and loop == 1:
            bot = telepot.Bot(token=bot_token)

            # Waits for the first incoming message
            updates=[]
            while not updates:
                updates = bot.getUpdates()

            # Gets the id for the active chat
            chat_id=updates[-1]['message']['chat']['id']

            # Sends a message to the chat
            bot.sendPhoto(chat_id=chat_id, photo=open(os.path.join('/home/pi/zack/LacrosseReference', "Images", "View Counts - Categories (%d).png" % (loop)), 'rb'))


loop = 0
for i, d in enumerate(display_search_urls):
    ax1 = fig.add_axes((.1, .2, .8, .7))
    if i % 7 == 0:
        x = [0]*7; x_off = [0]*7; y = [0]*7; labels = ['']*7
        loop += 1
    print("%s%.1f" % ("{:<150}".format(d['url']), d['est_views']))

    x[i%7] = (i % 7); x_off[i%7] = (i % 7) - .25
    y[i%7] = d['est_views']; labels[i%7] = d['url']

    if i % 7 == 6 or i + 1 == len(display_search_urls):
        title_str = "PageViews for Searches (All Time)"; ax1.set_title(title_str, loc='left')

        opacity = 0.4; plt.bar(x, y, alpha=opacity, color='b'); ax1.set_ylabel('PageViews')

        plt.xticks(x_off,labels, rotation=35, size=7)
        l = ax1.yaxis.get_ticklocs().tolist()
        plt.yticks(l, ["%s" % "{:,}".format(int(z)) if z > 0 and i + 1 < len(l) else "" for i, z in enumerate(l)])

        ax1.legend(loc='best'); plt.gcf().subplots_adjust(bottom=0.30)
        plt.savefig(os.path.join('/home/pi/zack/LacrosseReference', "Images", "View Counts - Searches (%d).png" % (loop))); plt.clf()

        if send_images and loop == 1:
            bot = telepot.Bot(token=bot_token)

            # Waits for the first incoming message
            updates=[]
            while not updates:
                updates = bot.getUpdates()

            # Gets the id for the active chat
            chat_id=updates[-1]['message']['chat']['id']

            # Sends a message to the chat
            bot.sendPhoto(chat_id=chat_id, photo=open(os.path.join('/home/pi/zack/LacrosseReference', "Images", "View Counts - Searches (%d).png" % (loop)), 'rb'))




cursor.close(); mysql_conn.close()
print("\n\tInitial query took %s ms" % ("{:,}".format(elapsed_ms)))
