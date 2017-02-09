#!/usr/bin/python

import httplib2, json

from apiclient import errors
from apiclient.discovery import build
from oauth2client.client import OAuth2WebServerFlow
import sys, time, datetime
from datetime import datetime
import MySQLdb

from googleapiclient import sample_tools

from oauth2client.service_account import ServiceAccountCredentials

def mysql_connect():
    cnx = None
    try:
        # Connecting from an external network.
        # Make sure your network is whitelisted
        client_cert_pem = "instance/client_cert_pem"
        client_key_pem = "instance/client_key_pem"
        ssl = {'cert': client_cert_pem, 'key': client_key_pem}
                    
        cnx = MySQLdb.connect(
            host='127.0.0.1',
            port=3306,
            user='root', passwd='password', db='monoprice')
        response = "Success!"
    except Exception as err:
        response = "Failed: %s" % err
        print(response)
        
    return cnx, response
    
def execute_request(service, site_url, request):
  """Executes a searchAnalytics.query request.
  Args:
    service: The webmasters service to use when executing the query.
    site_url: The site or app URI to request data for.
    request: The request to be executed.
  Returns:
    An array of response rows.
  """
  return service.searchanalytics().query(
      siteUrl=site_url, body=request).execute()


def print_table(response, title):
  """Prints out a response table.
  Each row contains key(s), clicks, impressions, CTR, and average position.
  Args:
    response: The server response to be printed as a table.
    title: The title of the table.
  """
  print title + ':'

  if 'rows' not in response:
    print 'Empty response'
    return

  rows = response['rows']
  row_format = '{:<20}' + '{:>20}' * 4
  print row_format.format('Keys', 'Clicks', 'Impressions', 'CTR', 'Position')
  for row in rows:
    keys = ''
    # Keys are returned only if one or more dimensions are requested.
    if 'keys' in row:
      keys = u','.join(row['keys']).encode('utf-8')
    print row_format.format(
        keys, row['clicks'], row['impressions'], row['ctr'], row['position'])
        
        
# Copy your credentials from the console
CLIENT_ID = 'YOUR_CLIENT_ID'
CLIENT_SECRET = 'YOUR_CLIENT_SECRET'

# Check https://developers.google.com/webmaster-tools/search-console-api-original/v3/ for all available scopes
OAUTH_SCOPE = 'https://www.googleapis.com/auth/webmasters.readonly'

# Redirect URI for installed apps
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'

# Run through the OAuth flow and retrieve credentials
#flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, REDIRECT_URI)
#authorize_url = flow.step1_get_authorize_url()
#print 'Go to the following link in your browser: ' + authorize_url
#code = raw_input('Enter verification code: ').strip()
#credentials = flow.step2_exchange(code)

service_account_email = open('/home/pi/zack/zackcapozzi_google_api_service_email', 'r').read().strip()
key_file_location = '/home/pi/zack/capozziinc-4af3fcf485ca.json'
  
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    key_file_location, scopes=OAUTH_SCOPE)
    
# Create an httplib2.Http object and authorize it with our credentials
http = httplib2.Http()
http = credentials.authorize(http)

service = build('webmasters', 'v3', http=http)

# Retrieve list of properties in account
site_list = service.sites().list().execute()

# Filter for verified websites
verified_sites_urls = [s['siteUrl'] for s in site_list['siteEntry']
                       if s['permissionLevel'] != 'siteUnverifiedUser'
                          and s['siteUrl'][:4] == 'http']

# Printing the URLs of all websites you are verified for.
for site_url in verified_sites_urls:
    if True:
        if site_url == "http://lacrossereference.com/":
            print "************************************\n%s\n-------------------------------------------" % site_url
            start_date = "2017-02-01"
            end_date = "2017-02-08"
            request = {
              'startDate': start_date,
              'endDate': end_date,
              'dimensions': ['date']
            }
            time.sleep(5); response = execute_request(service, site_url, request)
            print_table(response, 'Available dates'); print("")
            
            # Get totals for the date range.
            request = {
              'startDate': start_date,
              'endDate': end_date
            }
            time.sleep(5); response = execute_request(service, site_url, request)
            print_table(response, 'Totals'); print("")

            # Get top 10 queries for the date range, sorted by click count, descending.
            request = {
              'startDate': start_date,
              'endDate': end_date,
              'dimensions': ['query'],
              'rowLimit': 10
            }
            time.sleep(5); response = execute_request(service, site_url, request)
            print_table(response, 'Top Queries'); print("")

            # Get top 11-20 mobile queries for the date range, sorted by click count, descending.
            request = {
              'startDate': start_date,
              'endDate': end_date,
              'dimensions': ['query'],
              'dimensionFilterGroups': [{
                  'filters': [{
                      'dimension': 'device',
                      'expression': 'mobile'
                  }]
              }],
              'rowLimit': 10,
              'startRow': 10
            }
            time.sleep(5); response = execute_request(service, site_url, request)
            print_table(response, 'Top 11-20 Mobile Queries'); print("")

            # Get top 10 pages for the date range, sorted by click count, descending.
            request = {
              'startDate': start_date,
              'endDate': end_date,
              'dimensions': ['page'],
              'rowLimit': 10
            }
            time.sleep(5); response = execute_request(service, site_url, request)
            print_table(response, 'Top Pages'); print("")

            # Get the top 10 queries in India, sorted by click count, descending.
            request = {
              'startDate': start_date,
              'endDate': end_date,
              'dimensions': ['query'],
              'dimensionFilterGroups': [{
                  'filters': [{
                      'dimension': 'country',
                      'expression': 'ind'
                  }]
              }],
              'rowLimit': 10
            }
            time.sleep(5); response = execute_request(service, site_url, request)
            print_table(response, 'Top queries in India'); print("")

            # Group by both country and device.
            request = {
              'startDate': start_date,
              'endDate': end_date,
              'dimensions': ['country', 'device'],
              'rowLimit': 10
            }
            time.sleep(5); response = execute_request(service, site_url, request)
            print_table(response, 'Group by country and device'); print("")
    else:
        print site_url
        # Retrieve list of sitemaps submitted
        sitemaps = service.sitemaps().list(siteUrl=site_url).execute()
        if 'sitemap' in sitemaps:
            sitemap_urls = [s['path'] for s in sitemaps['sitemap']]
            #print "  " + "\n  ".join(sitemap_urls)

        print("Retrieve Search Analytics...")
        # Retrieve list of search analytics
        body = ""
        searchAnalytics = service.searchanalytics().query()
        
        print(searchAnalytics.__class__)
        print(searchAnalytics)
