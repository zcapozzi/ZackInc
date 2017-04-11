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
import statistics, numpy
from operator import truediv
import subprocess
import matplotlib.pyplot as plt
sys.path.insert(0, "../ZackInc")
import zack_inc as zc
import math

import Image, ImageDraw, ImageFont, textwrap
from ast import literal_eval

user_width = 641
user_height = 33
font = None
font_size = 16
font_style = "Regular"
font_color = (0, 0, 0, 175)
fill_color = (255, 255, 255, 255)
text = 'Hello World!'
if len(sys.argv) > 1:
    for i in range(1, len(sys.argv)):
        #print("Arg: %s" % sys.argv[i])
        if sys.argv[i] == "-x":
            user_width = int(sys.argv[1+i])
        elif sys.argv[i] == "-y":
            user_height = int(sys.argv[1+i])
        elif sys.argv[i] == "-font":
            font = sys.argv[1+i]
        elif sys.argv[i] == "-fontsize":
            font_size = int(sys.argv[1+i])
        elif sys.argv[i] == "-fontcolor":
            font_color = literal_eval(sys.argv[1+i])
        elif sys.argv[i] == "-fillcolor":
            fill_color = literal_eval(sys.argv[1+i])
        elif sys.argv[i] == "-text":
            text = sys.argv[1+i]
        elif sys.argv[i] == "-h" or sys.argv[i] == "-help":
            print ("Options:\n------------------------------")
            print ("\t-x [integer] - to set the width of the image")
            print ("\t-y [integer] - to set the height of the image")
            sys.exit()

text = text.replace("<br>", "\n").replace("\\n", "\n")
base = Image.open('/home/pi/zack/blank_header_label.png').convert('RGBA')
im = Image.new('RGBA', base.size, fill_color)
im = im.resize((user_width, user_height), Image.ANTIALIAS)
if font is not None:
    font = ImageFont.truetype('/home/pi/fonts/ofl/%s/%s-%s.ttf' % (font.lower(), font, font_style), font_size)

draw = ImageDraw.Draw(im)
if "\n" in text:
    row_height = 20 # 20 is good for font-size=16
    lines = text.split("\n")
    for i, l in enumerate(lines):
        draw.text((10, row_height * i + 2), l, font=font, fill=font_color)
else:
    draw.text((10, 2), text, font=font, fill=font_color)
im.save('/home/pi/zack/draw_text_header.png', 'PNG')
del draw

