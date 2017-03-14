#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
sys.path.insert(0, "../ZackInc")
import zack_inc as zc


import requests
import json
def goo_shorten_url(url):
    api_key = open('/home/pi/zack/zackcapozzi_google_api_key', 'r').read().strip()
    post_url = 'https://www.googleapis.com/urlshortener/v1/url?key=%s' % api_key
    payload = {'longUrl': url}
    headers = {'content-type': 'application/json'}
    r = requests.post(post_url, data=json.dumps(payload), headers=headers)
    #print(r.text)
    d = json.loads(r.text)
    return d['id']
print("Result: %s" % goo_shorten_url("www.example.com")    )
