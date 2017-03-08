"""A simple example of how to access the Google Analytics API."""

import argparse

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

import sys
import MySQLdb
import datetime
from datetime import datetime

def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):
  """Get a service that communicates to a Google API.

  Args:
    api_name: The name of the api to connect to.
    api_version: The api version to connect to.
    scope: A list auth scopes to authorize for the application.
    key_file_location: The path to a valid service account p12 key file.
    service_account_email: The service account email address.

  Returns:
    A service that is connected to the specified API.
  """

  #credentials = ServiceAccountCredentials.from_p12_keyfile(
  #  service_account_email, key_file_location, scopes=scope)
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
    key_file_location, scopes=scope)

  http = credentials.authorize(httplib2.Http())

  # Build the service object.
  service = build(api_name, api_version, http=http)

  return service

"""
def get_first_profile_id(service):
  # Use the Analytics service object to get the first profile id.

  # Get a list of all Google Analytics accounts for this user
  accounts = service.management().accounts().list().execute()

  if accounts.get('items'):
    # Get the first Google Analytics account.
    account = accounts.get('items')[0].get('id')

    # Get a list of all the properties for the first account.
    properties = service.management().webproperties().list(
        accountId=account).execute()

    if properties.get('items'):
      # Get the first property id.
      property = properties.get('items')[0].get('id')

      # Get a list of all views (profiles) for the first property.
      profiles = service.management().profiles().list(
          accountId=account,
          webPropertyId=property).execute()

      if profiles.get('items'):
        # return the first view (profile) id.
        return profiles.get('items')[0].get('id')

  return None
"""

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


def check_GA_records(account_obj):
    query = "SELECT count(1) from GA_Properties where GA_Code=%s and website=%s"
    param = [account_obj.get('id'), account_obj.get('name')]
    mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()
    cursor.execute(query, param)
    r = cursor.fetchone()
    if r[0] == 0: # Need to add the record
        query = "INSERT INTO GA_Properties (ID, website, GA_code, created_on, active) VALUES ((SELECT count(1) + 1 from GA_Properties fds), %s, %s, %s, 1)"
        param = [account_obj.get('name'), account_obj.get('id'), account_obj.get('created')]
        print("Query %s w/ %s" % (query, param))
        cursor.execute(query, param)
        mysql_conn.commit()
    cursor.close(); mysql_conn.close()
def get_all_profiles(service):
  # Use the Analytics service object to get the first profile id.

  # Get a list of all Google Analytics accounts for this user
  accounts = service.management().accounts().list().execute()

  if accounts.get('items'):
    account_ids = []
    ids = []
    names = []
    for i in range(len(accounts.get('items'))):
        account = accounts.get('items')[i].get('id')
        account_obj = accounts.get('items')[i]
        check_GA_records(account_obj)
        #print("Accounts: %s\n\n" % accounts)
        #sys.exit()
        # Get the first Google Analytics account.
        #account = accounts.get('items')[0].get('id')
        names.append(accounts.get('items')[i].get('name'))
        account_ids.append(accounts.get('items')[i].get('id'))
        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(
            accountId=account).execute()

        if properties.get('items'):
          # Get the first property id.
          #print("Properties: %s\n\n" % properties.get('items'))
          for j in range(len(properties.get('items'))):
              property = properties.get('items')[j].get('id')

              # Get a list of all views (profiles) for the first property.
              profiles = service.management().profiles().list(
                  accountId=account,
                  webPropertyId=property).execute()

              if profiles.get('items'):
                # return the first view (profile) id.
                ids.append(profiles.get('items')[0].get('id'))

  return ids, names, account_ids


def get_results(service, profile_id, name, account_ID):
    # Use the Analytics Service Object to query the Core Reporting API
    # for the number of sessions within the past seven days.
    #print("\n\nUse profile id: %s" % profile_id)
    record = {'Service': 'zack'}
    single_keys = ['sessions', 'bounces', 'avgSessionDuration', 'users', 'newUsers']
    multiple_keys = [['date', '7dayUsers'], ['date', '14dayUsers'], ['date', '30dayUsers'], 
    ['sourceMedium', 'timeOnPage'], ['sourceMedium', 'newUsers'], ['sourceMedium', 'Users'], ['sourceMedium', 'pageviews'], ['sourceMedium', 'organicSearches'],
    ['adContent', 'timeOnPage'], ['adContent', 'newUsers'], ['adContent', 'Users'], ['adContent', 'pageviews'], ['adContent', 'organicSearches'],
    ['searchKeyword', 'timeOnPage'], ['searchKeyword', 'newUsers'], ['searchKeyword', 'Users'], ['searchKeyword', 'pageviews'], ['searchKeyword', 'organicSearches'],
    ['searchStartPage', 'timeOnPage'], ['searchStartPage', 'newUsers'], ['searchStartPage', 'Users'], ['searchStartPage', 'pageviews'], ['searchStartPage', 'organicSearches'],
    ['hostname, ga:pagePath', 'timeOnPage'], ['hostname, ga:pagePath', 'newUsers'], ['hostname, ga:pagePath', 'Users'], ['hostname, ga:pagePath', 'pageviews'], ['hostname, ga:pagePath', 'organicSearches']]
   
    
    print("\n\nProcessing %s..." % name)
    mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()
    query = "SELECT ID from GA_Properties where website=%s and GA_Code=%s"
    param = [name, account_ID]
    print("Query %s w/ %s" % (query, param))
    cursor.execute(query, param)
    ID = cursor.fetchone()[0]
    
    
    insert_metric_query = "INSERT INTO GA_Metrics (timestamp, property_ID, description, data, most_recent) VALUES (%s, %s, %s, %s, 1)"
    insert_dimension_metric_query = "INSERT INTO GA_Dimension_Metrics (timestamp, property_ID, description, dimension_description, data, most_recent, metric) VALUES (%s, %s, %s, %s, %s, 1, %s)"
   
    insert_metric_query_no_data = "INSERT INTO GA_Metrics (timestamp, property_ID, description, most_recent) VALUES (%s, %s, %s, 1)"
    insert_dimension_metric_query_no_data = "INSERT INTO GA_Dimension_Metrics (timestamp, property_ID, description, dimension_description, most_recent, metric) VALUES (%s, %s, %s, %s, 1, %s)"
   
    for key in single_keys:
        #print("\tGrab key: %s" % key)  
        
        results = service.data().ga().get(ids='ga:' + profile_id, start_date="7daysAgo", end_date='today', metrics='ga:%s' % key).execute()
        
        if results.get('rows'):
            print("\t%s: %s" % (key, results.get('rows')[0][0]))
            record[key] = results.get('rows')[0][0]
            param = [datetime.today(), ID, key, results.get('rows')[0][0]]
            print("Query %s w/ %s" % (insert_metric_query, param))
            cursor.execute(insert_metric_query, param)
        else:
            print("\t%s: No data was returned" % key)
            param = [datetime.today(), ID, key]
            print("Query %s w/ %s" % (insert_metric_query_no_data, param))
            cursor.execute(insert_metric_query_no_data, param)
    
    for dim, metric in multiple_keys:
            
        if "dayUsers" in metric:
            results = service.data().ga().get(ids='ga:' + profile_id, start_date='today', end_date='today', dimensions='ga:%s' % dim, metrics='ga:%s' % metric).execute()
            if results.get('rows'):
                for d in results.get('rows'):

                    if len(d) == 2:
                        dim = d[0]
                    else:
                        dim = d[0] + d[1]
                    cnt = d[-1]
                    param = [datetime.today(), ID, metric, cnt]
                    print("Query %s w/ %s" % (insert_metric_query, param))
                    cursor.execute(insert_metric_query, param)
            else:
                print("\t%s: No data was returned" % key)
                param = [datetime.today(), ID, metric]
                print("Query %s w/ %s" % (insert_metric_query_no_data, param))
                cursor.execute(insert_metric_query_no_data, param)
            
        else:
            results = service.data().ga().get(ids='ga:' + profile_id, start_date='7daysAgo', end_date='today', dimensions='ga:%s' % dim, metrics='ga:%s' % metric).execute()
            print("\t%s\t%s" % (dim, metric))
            #print("For metric %s: %s" % (metric, data.get('rows')))
            
            if results.get('rows'):
                for d in results.get('rows'):
                    #print(d)
                    if len(d) == 2:
                        dim_ = d[0]
                    else:
                        dim_ = d[0] + d[1]
                    cnt = d[-1]
                    print("\t\t%s produced %s %s" % (dim_, cnt, metric))
                    param = [datetime.today(), ID, metric, dim_, cnt, dim]
                    print("Query %s w/ %s" % (insert_dimension_metric_query, param))
                    cursor.execute(insert_dimension_metric_query, param)
            else:
                print("\t\tNo data was returned for %s/%s" % (dim, metric))
                param = [datetime.today(), ID, metric, dim, None]
                print("Query %s w/ %s" % (insert_dimension_metric_query_no_data, param))
                cursor.execute(insert_dimension_metric_query_no_data, param)
    
    if commit_stuff:
        mysql_conn.commit()
    else:
        print("\n\nReminder, nothing is being committed...") 
    cursor.close(); mysql_conn.close()
"""
def print_results(results):
  # Print data nicely for the user.
  if results:
    total_sessions = results.get('rows')[0][0]
    print 'View (Profile): %s' % results.get('profileInfo').get('profileName')
    print 'Total Sessions: %s' % total_sessions
    
    
    
    print(results.__class__)
    print(results)

  else:
    print 'No results found'
  print("\n\n")
"""

def main():
    # Define the auth scopes to request.
    scope = ['https://www.googleapis.com/auth/analytics.readonly']

    # Use the developer console and replace the values with your
    # service account email and relative location of your key file.
    service_account_email = open('/home/pi/zack/zackcapozzi_google_api_service_email', 'r').read().strip()
    key_file_location = '/home/pi/zack/capozziinc-a528d09ee730.json'
    mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()


    scriptname = "HelloAnalytics"
    query = "SELECT local_or_remote from Capozzi_Scripts where name=%s"
    param = [scriptname]
    cursor.execute(query, param)
    local_or_remote = open('/home/pi/zack/local_or_remote', 'r').read().strip()
    row = cursor.fetchone()
    if local_or_remote != row[0]:
        print("Do not run %s because this host isn't the one that's supposed to be running it ( %s vs %s )" % (scriptname, local_or_remote, row[0]))
        sys.exit()
    if False: # if True, this will clear the tables prior to running
      
        cursor.execute("TRUNCATE TABLE GA_Metrics")
        cursor.execute("TRUNCATE TABLE GA_Dimension_Metrics")
      

    reset_query = "UPDATE GA_Metrics set most_recent=0"
    reset_query_2 = "UPDATE GA_Dimension_Metrics set most_recent=0"
    cursor.execute(reset_query)
    cursor.execute(reset_query_2)
    if commit_stuff:
        mysql_conn.commit(); 
    cursor.close(); mysql_conn.close();

    # Authenticate and construct service.
    service = get_service('analytics', 'v3', scope, key_file_location,
    service_account_email)
    #profile = get_first_profile_id(service)
    #print("Would have used %s" % profile)
    profiles, names, account_ids = get_all_profiles(service)
    for p, n, a in zip(profiles, names, account_ids):

        get_results(service, p, n, a)
commit_stuff = True    
if __name__ == '__main__':
  main()
  print("\t\n\n\tAll Done!!!")
"""
 def get_results(service, profile_id):
  # Use the Analytics Service Object to query the Core Reporting API
  # for the number of sessions within the past seven days.
  return service.data().ga().get(
      ids='ga:' + profile_id,
      start_date='7daysAgo',
      end_date='today',
      metrics='ga:sessions').execute()
 """ 
