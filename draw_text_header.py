# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 14:10:05 2015

@author: zcapozzi002
"""

import time, sys, os
#from time import strptime
#from datetime import datetime, date
import calendar
#import mysql.connector
import gc
#from mysql.connector import errorcode
#import requests
#from bs4 import BeautifulSoup
#import smtplib
#import re
#import statistics, numpy
#from operator import truediv
import subprocess
#import matplotlib.pyplot as plt
#sys.path.insert(0, "../ZackInc")
#import zack_inc as zc
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
output_file = '/home/pi/zack/draw_text_header.png'


row_height = 20 # 20 is good for font-size=16
row_height = 16 # 16 is good for font-size=11
row_heights = None
dont_trim = False
if len(sys.argv) > 1:
    for i in range(1, len(sys.argv)):
        #print("Arg: %s" % sys.argv[i])
        if sys.argv[i] == "-x":
            user_width = int(sys.argv[1+i])
        elif sys.argv[i] == "-y":
            user_height = int(sys.argv[1+i])
        elif sys.argv[i] == "-o":
            output_file = sys.argv[i+1]
        elif sys.argv[i] == "-notrim":
            dont_trim = True
        elif sys.argv[i] == "-font":
            font = sys.argv[1+i]
        elif sys.argv[i] == "-rowheights":
            row_heights = map(int, (sys.argv[1+i].split(",")))
        elif sys.argv[i] == "-rowheight":
            row_height = int(sys.argv[1+i])
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

max_length = 0

if "\n" in text:
    lines = text.split("\n")
    rows = 0
    if row_heights is None:
        for i, l in enumerate(lines):
            rows += 1
        im = im.resize((user_width, max(user_height, row_height * rows + 2 + row_height)), Image.ANTIALIAS)
        draw = ImageDraw.Draw(im)
        for i, l in enumerate(lines):
            draw.text((6, 2 + row_height*i), l, font=font, fill=font_color)
            #print l
    else:
        print("Printing " + text)
        im = im.resize((user_width, max(user_height, sum(row_heights))), Image.ANTIALIAS)
        draw = ImageDraw.Draw(im)
        row_height = 0
        for i, (l, rh) in enumerate(lines, row_heights):
            row_height += rh
            draw.text((6, 2 + row_height), l, font=font, fill=font_color)
            print "  %s" % l
            if max_length < draw.textsize(l, font=font)[0]:
                print("    Change max length to %d" % (draw.textsize(l, font=font)[0]))
                max_length = draw.textsize(l, font=font)[0]


else:
    draw = ImageDraw.Draw(im)
    length, height = draw.textsize(text, font=font)


    if length > user_width:
        tokens = text.split(" ")
        num = len(tokens)
        line = ""
        start_id = 0
        rows = 0
        for i in range (num):
            #print ("%d vs %d" % (draw.textsize(" ".join(tokens[start_id:i+1]), font=font)[0], user_width-10))
            if draw.textsize(" ".join(tokens[start_id:i]), font=font)[0] > user_width-10 or i == num - 1:
                im = im.resize((user_width, max(user_height, row_height * rows + 2 + row_height)), Image.ANTIALIAS)
                #print("Resize to (%d, %d)" % (user_width, max(user_height, row_height * rows + 2 + row_height)))
                rows += 1
                start_id = i
        rows = 0
        start_id = 0
        draw2 = ImageDraw.Draw(im)
        for i in range (num):
            if start_id != i or i == num - 1:
                #print ("%s" % (" ".join(tokens[start_id:i])))
                #print ("\t%d vs %d" % (draw.textsize(" ".join(tokens[start_id:i+1]), font=font)[0], user_width-10))
                #print ("\t\t%d vs %d" % (i, num-1))

                if draw2.textsize(" ".join(tokens[start_id:i+1]), font=font)[0] > user_width-10:
                    draw2.text((10, row_height * rows + 2), " ".join(tokens[start_id:i]), font=font, fill=font_color)
                    #print ("write %s to row %d" % (" ".join(tokens[start_id:i]), rows))
                    start_id = i
                    rows += 1
        #print ("write %s to row %d" % (" ".join(tokens[start_id:]), rows))
        draw2.text((6, row_height * rows + 2), " ".join(tokens[start_id:]), font=font, fill=font_color)

    else:

        if not dont_trim:
            #print "Resize to %s" % str((draw.textsize(text, font=font)[0]+10, draw.textsize(text, font=font)[1]+4))
            im = im.resize((draw.textsize(text, font=font)[0]+5, draw.textsize(text, font=font)[1]+4), Image.ANTIALIAS)
            draw = ImageDraw.Draw(im)
        draw.text((6,2), text, font=font, fill=font_color)

im.save(output_file, 'PNG')
del draw

