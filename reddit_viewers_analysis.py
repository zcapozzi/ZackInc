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

mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()

query = "SELECT forum, year(datestamp), month(datestamp), DAYOFWEEK(datestamp), hour(datestamp), minute(datestamp), avg(post_count) from Forum_Post_Counts where forum like '%Reddit%' group by forum, year(datestamp), month(datestamp), DAYOFWEEK(datestamp), hour(datestamp), minute(datestamp)"
#query += " limit 5"
cursor.execute(query)
res = cursor.fetchall()

ys = []
times = []
l = []
for r in res:
    print r
    if r[0] not in [d_['title'] for d_ in ys]:
        d = {'title': r[0], 'months': []}
        ys.append(d)
    d = ys[[d_['title'] for d_ in ys].index(r[0])]
    month = "%04d-%02d" % (r[1], r[2])
    if month not in [d_['month'] for d_ in d['months']]:
        d['months'].append({'month': month, 'times': [[], [], [], [], [], [], []]})

    day_of_week = r[3]-1

    display_time = "%02d:%02d" % (r[4], r[5])
    actual_time = r[4]*60 + r[5]
    count = r[6]
    user_counts = d['months'][[d_['month'] for d_ in d['months']].index(month)]['times'][day_of_week].append({'display_time': display_time, 'actual_time': actual_time, 'count': count})

    # Collect a list of the display times
    if display_time not in [d_['display_time'] for d_ in times]:
        times.append({'display_time': display_time, 'actual_time': actual_time})

if False:
    for ys_ in ys:
        print "Processing %s" % ys_['title']
        for month in ys_['months']:
            print (" For month: %s" % (month['month']))

            for i, weekday in enumerate(month['times']):

                for wd in weekday:

                    print("  %s had %f on day %d" % (wd['display_time'], wd['count'], i))


for ys_ in ys:
    print "Processing %s" % ys_['title']
    for month in ys_['months']:
        print (" For month: %s" % (month['month']))
        fig = plt.figure()
        ax1 = fig.add_axes((.1, .2, .8, .7))
        ax1.set_title("%s - %s" % (ys_['title'], month['month']), loc='left')
        ax1.set_ylabel("Day of Week")
        ax1.set_ylim(0, 8)
        #ax1.set_xlim(-.1, 1.1)
        for i, weekday in enumerate(month['times']):

            if len(weekday) > 0:
                max_count = float(max([d_['count'] for d_ in weekday]))
                #print max_count
                y = []
                x = []
                x_labels = []
                for wd in weekday:
                    #print("  %s had %f on day %d" % (wd['display_time'], wd['count'], i))
                    y.append((float(7 - i) + float(wd['count'])/max_count))
                    #y.append(float(wd['count'])/max_count)
                    #x.append(float(wd['actual_time'])/1440.0)
                    x.append(float(wd['actual_time'])/1440.0)
                    x_labels.append(wd['display_time'])
                #print(x)
                #print(y)
                #print(x_labels)
                print ""
                ax1.plot(x, y, linewidth=2.0)

        l = ax1.xaxis.get_ticklocs().tolist()
        new_labels = ["" if zc < 0 else "%d%%" % (zc*100) for zc in l]
        #print l
        #print new_labels
        #print x
        #print x_labels
        #plt.xticks(x, x_labels)

        plt.yticks([1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5], ['Sat', 'Fri', 'Thu', 'Wed', 'Tue', 'Mon', 'Sun'])
        ax1.set_yticks([1, 2, 3, 4, 5, 6, 7], minor=True)
        ax1.yaxis.grid(True, which='minor')

        locs = []
        for i in range(1,24):
            locs.append(float(i)/24.0)
        ax1.set_xticks(locs, minor=True)
        ax1.xaxis.grid(True, which='minor')

        plt.xticks([float(d_['actual_time'])/1440.0 for i, d_ in enumerate(times)], ["" if i%12 > 0 else d_['display_time'] for i, d_ in enumerate(times)])
        plt.savefig('/home/pi/zack/LacrosseReference/Images/%s RedditSummary (%s).png' % (ys_['title'], month['month']));
        n = datetime.datetime.today()

        if True:
            bot = telepot.Bot(token=bot_token)

            # Waits for the first incoming message
            updates=[]
            while not updates:
                updates = bot.getUpdates()

            # Gets the id for the active chat
            chat_id=updates[-1]['message']['chat']['id']

            # Sends a message to the chat
            bot.sendPhoto(chat_id=chat_id, photo=open('/home/pi/zack/LacrosseReference/Images/%s RedditSummary (%s).png' % (ys_['title'], month['month']), 'rb'))


cursor.close(); mysql_conn.close()
