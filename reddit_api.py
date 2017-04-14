

# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 14:10:05 2015

@author: zcapozzi002
"""

import time, datetime, sys, os
from time import strptime
from datetime import datetime, date
import calendar
import mysql.connector
import gc
from mysql.connector import errorcode
import requests
from bs4 import BeautifulSoup
import smtplib
import re

import statistics, numpy as np
from operator import truediv
import subprocess

import matplotlib.pyplot as plt
sys.path.insert(0, "../ZackInc")
import zack_inc as zc

import praw

def authenticate_me(whoami):
    if whoami == "zack":
        key_data = open('/home/pi/zack/lacrosse_reference_reddit_api_keys', 'r').read()
        r1 = re.compile(r'secret\: (.*?)\n')
        r2 = re.compile(r'token: (.*?)\n')
        r3 = re.compile(r'username: (.*?)\n')
        r4 = re.compile(r'password: (.*?)\n')
        client_secret = r1.search(key_data).group(1)
        client_id = r2.search(key_data).group(1)
        username = r3.search(key_data).group(1)
        password = r4.search(key_data).group(1)
        key_data = None
    elif whoami == "lacrosse_reference":
        key_data = open('/home/pi/zack/lacrosse_reference_reddit_api_keys', 'r').read()
        r1 = re.compile(r'secret: (.*?)\n')
        r2 = re.compile(r'token: (.*?)\n')
        r3 = re.compile(r'username: (.*?)\n')
        r4 = re.compile(r'password: (.*?)\n')
        client_secret = r1.search(key_data).group(1)
        client_id = r2.search(key_data).group(1)
        username = r3.search(key_data).group(1)
        password = r4.search(key_data).group(1)
        key_data = None
    else:
        return None
    return praw.Reddit(client_id=client_id, client_secret=client_secret, password=password, user_agent='testscript by /u/%s' % username, username=username)


reddit = authenticate_me('lacrosse_reference')
top = reddit.subreddit('lacrosse').traffic()


reddit = None
