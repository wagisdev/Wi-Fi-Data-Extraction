#-------------------------------------------------------------------------------
# Name:        Open Comcast Xfinity WAP Points
# Purpose:  This script scrapes out from the Xfinity website a full list of
#           open Wi-Fi access points.  It will require a listing of known
#           addresses that can act as a seed value when performing the extraction.
#
# Author:      John Spence, Spatial Data Administrator, City of Bellevue
#
# Created:  4/9/2020
# Modified:  5/14/2020
# Modification Purpose:  Code cleanup.
#
#
#-------------------------------------------------------------------------------

# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# Pretty simple setup.  Just change your settings/configuration below.  Do not
# go below the "DO NOT UPDATE...." line.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Comcast Search Parameter:
global city_search
city_search = 'San Francisco'
filter_search = '' #leave this one blank
topLeftLat = '38.798559'  #Top left of your search
topLeftLong = '-121.830453' #Top left of your search
bottomRightLat = '38.368839' #Bottom right of your search
bottomLeftLong = '-121.260018' #Bottom right of your search

# ArcGIS Online Portal
AGOL_Portal = ''  #If blank, it will search for the active portal from your ArcGIS//Pro install

# ArcGIS Online Credentials
AGOL_User = ''
AGOL_Pass = ''

# Targeted Service & layer for Data
service_URL = 'https://services3.arcgis.com/uknczv4rpevve42E/arcgis/rest/services/Wireless_Access_Points/FeatureServer/0/'

# Use local address database
local_address_db = 1
city_add = 'San Francisco'
state_add = 'CA'

# Initial Loading
initial_load = 0

# PyODBC confifguration
conn_params = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=GISSQL2019SDE;'
                      'Database=CA_Addresses;'
                      #'Trusted_Connection=yes;'  #Only if you are using a AD account.
                      r'UID=YourUserName;'  # Comment out if you are using AD authentication.
                      r'PWD=YourPassword'     # Comment out if you are using AD authentication.
                      )

# Database Cache Table
db_table = '[dbo].[WiFi_Temp]'

# Reserved
Reserved = []

# Check Ride
global checkRide
checkRide = []

# Address Count
global checkCount
checkCount = 0

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

import urllib
import urllib.request
import json
import pyodbc
import concurrent.futures
import datetime

#-------------------------------------------------------------------------------
#
#
#                                 Functions
#
#
#-------------------------------------------------------------------------------

def get_Comcast(address):

    url = 'https://hotspots.wifi.comcast.com/ajax/map-search'
    values = {'txtSearch' : address,
              'typeFilter' : [],
              'tllat' : topLeftLat,
              'tllon' : topLeftLong,
              'brlat' : bottomRightLat,
              'brlon' : bottomLeftLong}

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

    return (payload_json)

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

def checkIn_DB(payload_json):

    insert_count = 1
    for item in payload_json['results']:
        update_conn = pyodbc.connect(conn_params)
        update_cursor = update_conn.cursor()
        APOwner = item[0]
        Street = item[1]
        City = item[2]
        Zip = item[4]
        LocationType = item[9]
        SSID = item[9]
        ID1 = item[10]
        ID2 = item[11]
        Latitude = item[6]
        Longitude = item[5]

        if ID1 not in checkRide:
            checkRide.append(ID1)
            #Insert the address and status into the database.
            update_string = ('''
                insert into {}(
                    [APOwner]
                    ,[Street]
                    ,[City]
                    ,[Zip]
                    ,[LocationType]
                    ,[SSID]
                    ,[ID1]
                    ,[ID2]
                    ,[Latitude]
                    ,[Longitude]
                    ,[Created])
                values (
                '{}'
                ,'{}'
                ,'{}'
                ,'{}'
                ,'{}'
                ,'{}'
                ,'{}'
                ,'{}'
                ,'{}'
                ,'{}'
                , getdate()
                )''').format(db_table, APOwner, Street, City, Zip, LocationType, SSID, ID1, ID2, Latitude, Longitude)
            update_cursor.execute(update_string)
            update_conn.commit()
            update_cursor.close()
            update_conn.close()
            print ('Added...{}'.format(insert_count))
            insert_count += 1
        #else:
        #    print ('Not Adding!')

    return

def pushToAGOL():

    count = 0

    update_conn = pyodbc.connect(conn_params)
    update_cursor = update_conn.cursor()

    # Clean Out Dupes
    update_string = (
        '''WITH cte AS (
            SELECT
                [ID1]
                ,ROW_NUMBER() OVER (
                    PARTITION BY
                        [ID1]
                    ORDER BY
                        [ID1]
                ) row_num
                FROM
                {}
                )
        DELETE FROM cte
        WHERE row_num > 1'''
    ).format(db_table)

    update_cursor.execute(update_string)
    update_conn.commit()
    update_cursor.close()
    update_conn.close()

    # Build Payloads
    query_string = ('''SELECT * from {}''').format(db_table)
    query_conn = pyodbc.connect(conn_params)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    dataDB = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    for row in dataDB:

        edit_token = get_token()

        FS_service = service_URL + 'query/?token={}'.format(edit_token)

        where_statement = 'ID1=\'{}\''.format(row[6])

        data = urllib.parse.urlencode({'f': 'json', 'where': where_statement, 'returnCountOnly': 'true'}).encode('utf-8')

        item_count = None
        while item_count is None:
            try:
                req = urllib.request.Request(FS_service)
                response = urllib.request.urlopen(req,data=data)
                response_payload = response.read()
                response_payload = json.loads(response_payload)

                item_count = response_payload['count']

            except:
                pass
        if item_count > 0:
            where_statement = 'ID1=\'{}\''.format(row[6])

            data = urllib.parse.urlencode({'f': 'json', 'where': where_statement, 'outFields':'OBJECTID'}).encode('utf-8')

            req = urllib.request.Request(FS_service)
            response = urllib.request.urlopen(req,data=data)
            response_payload = response.read()
            response_payload = json.loads(response_payload)
            #print (response_payload)
            for oid_item in response_payload['features']:
                objectID = oid_item['attributes']['OBJECTID']

        if item_count > 0:
            if row[7] in Reserved:
                upload_payload = [{
                        'geometry' : {'x' : row[8], 'y' : row[9]},
                        'attributes' : {
                            'OBJECTID': objectID,
                            'APOwner' : row[0],
                            'Street' : row[1],
                            'City': row[2],
                            'Zip': int(row[3]),
                            'LocationType': row[4],
                            'SSID': row[5],
                            'ID1': row[6],
                            'ID2': row[7],
                            'Reserved' : 'Yes'
                        }
                        }]
            else:
                upload_payload = [{
                        'geometry' : {'x' : row[8], 'y' : row[9]},
                        'attributes' : {
                            'OBJECTID': objectID,
                            'APOwner' : row[0],
                            'Street' : row[1],
                            'City': row[2],
                            'Zip': int(row[3]),
                            'LocationType': row[4],
                            'SSID': row[5],
                            'ID1': row[6],
                            'ID2': row[7],
                            'Reserved' : 'No'
                        }
                        }]
            if initial_load == 0:
                update_AGOL(upload_payload, edit_token, where_statement)
                count += 1
                print ('Updated {}'.format(count))
            else:
                print ('    ---Initial Upload bypass!---')
        else:
            if row[7] in Reserved:
                upload_payload = [{
                        'geometry' : {'x' : row[8], 'y' : row[9]},
                        'attributes' : {
                            'APOwner' : row[0],
                            'Street' : row[1],
                            'City': row[2],
                            'Zip': int(row[3]),
                            'LocationType': row[4],
                            'SSID': row[5],
                            'ID1': row[6],
                            'ID2': row[7],
                            'Reserved' : 'Yes'
                        }
                        }]
            else:
                upload_payload = [{
                        'geometry' : {'x' : row[8], 'y' : row[9]},
                        'attributes' : {
                            'APOwner' : row[0],
                            'Street' : row[1],
                            'City': row[2],
                            'Zip': int(row[3]),
                            'LocationType': row[4],
                            'SSID': row[5],
                            'ID1': row[6],
                            'ID2': row[7],
                            'Reserved' : 'No'
                        }
                        }]
            insert_AGOL(upload_payload, edit_token)
            count += 1
            print ('Inserted {}'.format(count))
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

def process_address(address):
    print ('Processing Address #:  {}'.format(address))
    payload_json = get_Comcast(address)
    # Uncomment out if you want to use a direct to AGOL approach.  It's slower than mollasses.
    checkIn_DB(payload_json)

    return

def prepareTable():

    createTable()

    update_conn = pyodbc.connect(conn_params)
    update_cursor = update_conn.cursor()
    # Truncate the table in preparation for new data.
    update_string = ('''truncate table {}''').format(db_table)
    update_cursor.execute(update_string)
    update_conn.commit()
    update_cursor.close()
    update_conn.close()

    return


def createTable():

    try:
        deleteTable()
    except:
        print ('Table does not exist.')

    update_conn = pyodbc.connect(conn_params)
    update_cursor = update_conn.cursor()
    update_string = ('''
    CREATE TABLE {}(
	[APOwner] [nvarchar](255) NULL,
	[Street] [nvarchar](255) NULL,
	[City] [nvarchar](255) NULL,
	[Zip] [nvarchar](255) NULL,
	[LocationType] [nvarchar](255) NULL,
	[SSID] [nvarchar](255) NULL,
	[ID1] [nvarchar](255) NULL,
	[ID2] [nvarchar](255) NULL,
	[Latitude] [nvarchar](255) NULL,
	[Longitude] [nvarchar](255) NULL,
	[Created] [datetime2](7) NULL
    )''').format(db_table)
    update_cursor.execute(update_string)
    update_conn.commit()
    update_cursor.close()
    update_conn.close()

    return

def deleteTable():

    update_conn = pyodbc.connect(conn_params)
    update_cursor = update_conn.cursor()
    update_string = ('''DROP TABLE {}''').format(db_table)
    update_cursor.execute(update_string)
    update_conn.commit()
    update_cursor.close()
    update_conn.close()

    return

def aisle6Cleanup():

    dt = datetime.date.today()
    dt = dt - datetime.timedelta(days=1)
    APOwner = 'Xfinity WiFi'
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

prepareTable()

 #Update this string to set for your address repository.  The case below uses the parcel data as the MSAG.
 #I searched for specifically Street and added on later in the script "Bellevue" to get it closer.  Not always
 #a perfect fit.  distinct(COBParcelAddress)

query_string = ('''SELECT address
                FROM [dbo].[Humbolt]''')
query_conn = pyodbc.connect(conn_params)
query_cursor = query_conn.cursor()
query_cursor.execute(query_string)
data = query_cursor.fetchall()
query_cursor.close()
query_conn.close()

re_count = 1

city_search = []
for row in data:
    target = row[0]
    #target = '{}, {}'.format(target, city_add)
    target = '{}'.format(target)
    city_search.append(target)

with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
    executor.map(process_address, city_search)

pushToAGOL()
aisle6Cleanup()
deleteTable()
