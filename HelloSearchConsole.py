#!/usr/bin/python

import httplib2, json

from apiclient import errors
from apiclient.discovery import build
from oauth2client.client import OAuth2WebServerFlow
import sys, time, datetime
from datetime import datetime, timedelta

import MySQLdb

from googleapiclient import sample_tools

from oauth2client.service_account import ServiceAccountCredentials
sys.path.insert(0, "/home/pi/zack/ZackInc")
import zack_inc as zc


def mysql_connect():
    cnx = None
    try:
        # Connecting from an external network.
        # Make sure your network is whitelisted
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
key_file_location = '/home/pi/zack/capozziinc-a528d09ee730.json'

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

time.sleep(60)

wait_delay = 2
# Printing the URLs of all websites you are verified for.

mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor();


scriptname = "HelloSearchConsole"
query = "SELECT local_or_remote from Capozzi_Scripts where name=%s"
param = [scriptname]
cursor.execute(query, param)
local_or_remote = open('/home/pi/zack/local_or_remote', 'r').read().strip()
row = cursor.fetchone()
if local_or_remote != row[0]:
    print("Do not run %s because this host isn't the one that's supposed to be running it ( %s vs %s )" % (scriptname, local_or_remote, row[0]))
    sys.exit()

query = "UPDATE GS_Metrics set most_recent=0"
cursor.execute(query)
query = "UPDATE GS_Dimension_Metrics set most_recent=0"
cursor.execute(query)
mysql_conn.commit(); cursor.close(); mysql_conn.close();
metrics_insert = "INSERT INTO GS_Metrics (timestamp, property_ID, description, data, most_recent) VALUES (%s, %s, %s, %s, 1)"
dimension_metrics_insert = "INSERT INTO GS_Dimension_Metrics (timestamp, property_ID, metric, description, dimension_description, data, most_recent) VALUES (%s, %s, %s, %s, %s, %s, 1)"
for site_url in verified_sites_urls:

    if True:
        if True or site_url == "http://lacrossereference.com/":
            mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor();
            query = "SELECT ID from GA_Properties where url=%s"
            param = [site_url]
            cursor.execute(query, param)
            property_ID = cursor.fetchone()[0]
            print "************************************\n%s (%d)\n-------------------------------------------" % (site_url, property_ID)
            start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
            end_date = datetime.today().strftime("%Y-%m-%d")
            print("Search %s to %s" % (start_date, end_date))


            # Get totals for the date range.
            request = {
              'startDate': start_date,
              'endDate': end_date
            }
            time.sleep(wait_delay); response = execute_request(service, site_url, request);
            print_table(response, 'Totals'); print("")

            if 'rows' in response and response['rows'] is not None:
                for row in response['rows']:
                    param = [datetime.today(), property_ID, 'clicks', row['clicks']]
                    print("%s w/ %s" % (metrics_insert, param))
                    cursor.execute(metrics_insert, param)

                    param = [datetime.today(), property_ID, 'impressions', row['impressions']]
                    print("%s w/ %s" % (metrics_insert, param))
                    cursor.execute(metrics_insert, param)


                    f_ = open('/home/pi/zack/HelloSearchConsole_row_position_log.txt', 'a')
                    param = [datetime.today(), property_ID, 'position', row['position']]
                    print("%s w/ %s" % (metrics_insert, param))
                    f_.write("%s w/ %s\n" % (metrics_insert, param))
                    cursor.execute(metrics_insert, param)
                    f_.close()


            # Get top 10 queries for the date range, sorted by click count, descending.
            request = {
              'startDate': start_date,
              'endDate': end_date,
              'dimensions': ['query'],
              'rowLimit': 15
            }
            time.sleep(wait_delay); response = execute_request(service, site_url, request)
            print_table(response, 'Top Queries'); print("")
            if 'rows' in response and response['rows'] is not None:
                for row in response['rows']:
                    param = [datetime.today(), property_ID, 'clicks', 'top queries', u','.join(row['keys']).encode('utf-8'), row['clicks']]
                    print("%s w/ %s" % (dimension_metrics_insert, param))
                    cursor.execute(dimension_metrics_insert, param)

                    param = [datetime.today(), property_ID, 'impressions', 'top queries', u','.join(row['keys']).encode('utf-8'), row['impressions']]
                    print("%s w/ %s" % (dimension_metrics_insert, param))
                    cursor.execute(dimension_metrics_insert, param)

                    param = [datetime.today(), property_ID, 'position', 'top queries', u','.join(row['keys']).encode('utf-8'), row['position']]
                    print("%s w/ %s" % (dimension_metrics_insert, param))
                    cursor.execute(dimension_metrics_insert, param)

            # Get top 10 pages for the date range, sorted by click count, descending.
            request = {
              'startDate': start_date,
              'endDate': end_date,
              'dimensions': ['page'],
              'rowLimit': 10
            }
            time.sleep(wait_delay); response = execute_request(service, site_url, request)
            print_table(response, 'Top Pages'); print("")
            if 'rows' in response and response['rows'] is not None:
                for row in response['rows']:
                    param = [datetime.today(), property_ID, 'clicks', 'top pages', u','.join(row['keys']).encode('utf-8'), row['clicks']]
                    print("%s w/ %s" % (dimension_metrics_insert, param))
                    cursor.execute(dimension_metrics_insert, param)

                    param = [datetime.today(), property_ID, 'impressions', 'top pages', u','.join(row['keys']).encode('utf-8'), row['impressions']]
                    print("%s w/ %s" % (dimension_metrics_insert, param))
                    cursor.execute(dimension_metrics_insert, param)

                    param = [datetime.today(), property_ID, 'position', 'top pages', u','.join(row['keys']).encode('utf-8'), row['position']]
                    print("%s w/ %s" % (dimension_metrics_insert, param))
                    cursor.execute(dimension_metrics_insert, param)

            # Group by both country and device.
            request = {
              'startDate': start_date,
              'endDate': end_date,
              'dimensions': ['country', 'device'],
              'rowLimit': 10
            }
            time.sleep(wait_delay); response = execute_request(service, site_url, request)
            print_table(response, 'Group by country and device'); print("")

            # Group by both country and device.
            request = {
              'startDate': start_date,
              'endDate': end_date,
              'dimensions': ['country'],
              'rowLimit': 10
            }
            time.sleep(wait_delay); response = execute_request(service, site_url, request)
            print_table(response, 'Group by country'); print("")
            if 'rows' in response and response['rows'] is not None:
                for row in response['rows']:
                    param = [datetime.today(), property_ID, 'clicks', 'top countries', u','.join(row['keys']).encode('utf-8'), row['clicks']]
                    print("%s w/ %s" % (dimension_metrics_insert, param))
                    cursor.execute(dimension_metrics_insert, param)

                    param = [datetime.today(), property_ID, 'impressions', 'top countries', u','.join(row['keys']).encode('utf-8'), row['impressions']]
                    print("%s w/ %s" % (dimension_metrics_insert, param))
                    cursor.execute(dimension_metrics_insert, param)

                    param = [datetime.today(), property_ID, 'position', 'top countries', u','.join(row['keys']).encode('utf-8'), row['position']]
                    print("%s w/ %s" % (dimension_metrics_insert, param))
                    cursor.execute(dimension_metrics_insert, param)


            if True:
                mysql_conn.commit()
            else:
                print("\n\n\tReminder, nothing is being committed...")
            mysql_conn.close(); cursor.close()
    else:
        print site_url
        # Retrieve list of sitemaps submitted
        sitemaps = service.sitemaps().list(siteUrl=site_url).execute()
        if 'sitemap' in sitemaps:
            sitemap_urls = [s['path'] for s in sitemaps['sitemap']]
            #print "  " + "\n  ".join(sitemap_urls)

