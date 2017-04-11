

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
