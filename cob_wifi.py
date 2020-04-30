#-------------------------------------------------------------------------------
# Name:        Meraki WAP to AGOL
# Purpose:  This script loads data stored in Meraki to ArcGIS Online.
#
# Author:      John Spence
#
# Created:  4/24/2020
# Modified:  4/29/2020
# Modification Purpose:  Code cleanup.
#-------------------------------------------------------------------------------

# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# Pretty simple setup.  Just change your settings/configuration below.  Do not
# go below the "DO NOT UPDATE...." line.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# ArcGIS Online Portal
AGOL_Portal = ''  #If blank, it will search for the active portal from your ArcGIS//Pro install

# ArcGIS Online Credentials
AGOL_User = 'UserName'
AGOL_Pass = 'Password'

# Targeted Service & layer for Data
service_URL = 'https://services1.arcgis.com/EnterYoursHere'

# Initial Loading
initial_load = 0

# PyODBC confifguration
conn_params = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=ServerName;'
                      'Database=DatabaseName;'
                      'Trusted_Connection=yes;'  #Only if you are using a AD account.
#                      r'UID=YourUserName;'  # Comment out if you are using AD authentication.
#                      r'PWD=YourPassword'     # Comment out if you are using AD authentication.
                      )

# Database Cache Table
db_table = '[ITD].[Wi-Fi_AP]'

# Reserved MAC Addresses (Can be used to ID 1st responder focused APs)

Reserved = ['00:00:00:00:00:00']

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

import urllib
import urllib.request
import datetime
import json
import pyodbc

#-------------------------------------------------------------------------------
#
#
#                                 Functions
#
#
#-------------------------------------------------------------------------------

def insert_AGOL(upload_payload, edit_token):

    FS_service = service_URL + 'addFeatures/?token={}'.format(edit_token)

    data = urllib.parse.urlencode({'f': 'json', 'features': upload_payload}).encode('utf-8')

    req = urllib.request.Request(FS_service)
    response = urllib.request.urlopen(req,data=data)
    the_page = response.read()

    print ('    Record inserted.')

    return

def update_AGOL(upload_payload, edit_token, where_statement):

    FS_service = service_URL + 'updateFeatures/?token={}'.format(edit_token)

    data = urllib.parse.urlencode({'f': 'json', 'features': upload_payload}).encode('utf-8')

    req = urllib.request.Request(FS_service)
    response = urllib.request.urlopen(req,data=data)
    the_page = response.read()

    print ('    Record updated.')

    return

def get_token():

    url = 'https://www.arcgis.com/sharing/rest/generateToken'
    values = {'f': 'json',
              'username': AGOL_User,
              'password': AGOL_Pass,
              'referer' : 'https://www.arcgis.com',
              'expiration' : '10'}

    data = urllib.parse.urlencode(values).encode("utf-8")
    req = urllib.request.Request(url)

    response = None
    while response is None:
        try:
            response = urllib.request.urlopen(req,data=data)
        except:
            pass
    the_page = response.read()

    #Garbage Collection with some house building
    payload_json = the_page.decode('utf8')
    payload_json = json.loads(payload_json)

    edit_token = payload_json['token']

    return (edit_token)

def pushToAGOL(edit_token, APOwner, Street, City, Zip, LocationType, SSID, ID1, ID2, Latitude, Longitude, Created):

    if ID1 in Reserved:
        RES =  True
    else:
        RES = False

    ID1 = ID1.replace(':', '')
    ID1 = '{}'.format(ID1)
    print ('    Replaced my ID:  {}'.format(ID1))

    FS_service = service_URL + 'query/?token={}'.format(edit_token)

    where_statement = 'ID1=\'{}\''.format(ID1)

    data = urllib.parse.urlencode({'f': 'json', 'where': where_statement, 'returnCountOnly': 'true'}).encode('utf-8')

    item_count = None
    try:
        req = urllib.request.Request(FS_service)
        response = urllib.request.urlopen(req,data=data)
        response_payload = response.read()
        response_payload = json.loads(response_payload)
        item_count = response_payload['count']
    except:
        pass

    if item_count > 0:

        data = urllib.parse.urlencode({'f': 'json', 'where': where_statement, 'outFields':'OBJECTID'}).encode('utf-8')

        req = urllib.request.Request(FS_service)
        response = urllib.request.urlopen(req,data=data)
        response_payload = response.read()
        response_payload = json.loads(response_payload)
        for oid_item in response_payload['features']:
            objectID = oid_item['attributes']['OBJECTID']

    if item_count > 0:
        if RES == True:
            upload_payload = [{
                    'geometry' : {'x' : float(Longitude), 'y' : float(Latitude)},
                    'attributes' : {
                        'OBJECTID': objectID,
                        'APOwner' : APOwner,
                        'Street' : Street,
                        'City': City,
                        'Zip': '',
                        'LocationType': LocationType,
                        'SSID': SSID,
                        'ID1': ID1,
                        'ID2': ID2,
                        'Reserved' : 'Yes'
                    }
                    }]
        else:
            upload_payload = [{
                    'geometry' : {'x' : float(Longitude), 'y' : float(Latitude)},
                    'attributes' : {
                        'OBJECTID': objectID,
                        'APOwner' : APOwner,
                        'Street' : Street,
                        'City': City,
                        'Zip': '',
                        'LocationType': LocationType,
                        'SSID': SSID,
                        'ID1': ID1,
                        'ID2': ID2,
                        'Reserved' : 'No'
                    }
                    }]
        if initial_load == 0:
            update_AGOL(upload_payload, edit_token, where_statement)
        else:
            print ('    ---Initial Upload bypass!---')
    else:
        if RES == True:
            upload_payload = [{
                    'geometry' : {'x' : float(Longitude), 'y' : float(Latitude)},
                    'attributes' : {
                        'APOwner' : APOwner,
                        'Street' : Street,
                        'City': City,
                        'Zip': '',
                        'LocationType': LocationType,
                        'SSID': SSID,
                        'ID1': ID1,
                        'ID2': ID2,
                        'Reserved' : 'Yes'
                    }
                    }]
        else:
            upload_payload = [{
                    'geometry' : {'x' : float(Longitude), 'y' : float(Latitude)},
                    'attributes' : {
                        'APOwner' : APOwner,
                        'Street' : Street,
                        'City': City,
                        'Zip': '',
                        'LocationType': LocationType,
                        'SSID': SSID,
                        'ID1': ID1,
                        'ID2': ID2,
                        'Reserved' : 'No'
                    }
                    }]
        insert_AGOL(upload_payload, edit_token)

    return ()

def getData():

    edit_token = get_token()

    query_string =('''
            SELECT
                \'AP Owner Name\' as [APOwner]
                ,\'Not Available\' as [Street],\'Bellevue\' as [City]
                ,\'Not Available\' as [Zip],\'\' as [LocationType]
                ,\' \' as [SSID],[MAC_Address] as [ID1]
                ,[Name] as [ID2],[Latitude],[Longitude]
                ,[SysCreateDate] as [Created] FROM {}
                where active = \'true\'
            ''').format(db_table)

    query_conn = pyodbc.connect(conn_params)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    data = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    for ap in data:
        APOwner = ap[0]
        Street = ap[1]
        City = ap[2]
        Zip = ap[3]
        LocationType = ap[4]
        SSID = ap[5]
        ID1 = ap[6]
        ID2 = ap[7]
        Latitude = ap[8]
        Longitude = ap[9]
        Created = ap[10]

        print ('Sending {}'.format(ID2))

        task_complete = 0
        while task_complete != 1:
            try:
                pushToAGOL(edit_token, APOwner, Street, City, Zip, LocationType, SSID, ID1, ID2, Latitude, Longitude, Created)
                task_complete = 1
            except:
                pass

    return

def aisle6Cleanup():

    dt = datetime.date.today()
    dt = dt - datetime.timedelta(days=1)
    APOwner = 'AP Owner Name'
    #APOwner = ''

    print ('\n\nPurging Prior to:  {}'.format(dt))

    edit_token = get_token()

    FS_service = service_URL + 'deleteFeatures/?token={}'.format(edit_token)

    if APOwner == '':
        where_statement = 'EditDate < \'{} 00:00:01\' '''.format(dt, APOwner)
    else:
        where_statement = 'EditDate < \'{} 00:00:01\' and APOwner = \'{}\' '''.format(dt, APOwner)
    data = urllib.parse.urlencode({'f': 'json', 'where': where_statement}).encode('utf-8')

    req = urllib.request.Request(FS_service)
    response = urllib.request.urlopen(req,data=data)
    the_page = response.read()

    print ('    Records Purged.')

    return

#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

# Get some data.

getData()
aisle6Cleanup()
