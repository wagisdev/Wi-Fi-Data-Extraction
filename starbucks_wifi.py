#-------------------------------------------------------------------------------
# Name:        Starbucks Open Wifi to ArcGIS Online
# Purpose:  This script lifts and loads store locations for Starbucks into
#           ArcGIS Online.  The purpose was to locate all open Wi-Fi hotspots
#           during the COVID-19 crisis.
#
# Author:      John Spence
#
# Created:  4/29/2020
# Modified:  4/30/2020
# Modification Purpose:  Added entry cleanup.
#-------------------------------------------------------------------------------

# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# Pretty simple setup.  Just change your settings/configuration below.  Do not
# go below the "DO NOT UPDATE...." line.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Starbucks Search Parameters:
# Center point of your search.  Will only return 50 results so for larger locations,
# you will need to run this script interatively with different center points.
Lat = '47.614404'
Long = '-122.1946242'

# ArcGIS Online Portal
AGOL_Portal = ''  #Leave blank.

# ArcGIS Online Credentials
AGOL_User = 'Username'
AGOL_Pass = 'Password'

# Targeted Service & layer for Data
service_URL = 'https://services1.arcgis.com/YourService'

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

import urllib
import urllib.parse
import urllib.request
import json
import datetime

#-------------------------------------------------------------------------------
#
#
#                                 Functions
#
#
#-------------------------------------------------------------------------------

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

def pushToAGOL(edit_token, APOwner, Street, City, Zip, LocationType, SSID, ID1, ID2, Latitude, Longitude):

    RES = False

    FS_service = service_URL + 'query/?token={}'.format(edit_token)

    where_statement = 'ID1=\'{}\''.format(ID1)

    data = urllib.parse.urlencode({'f': 'json', 'where': where_statement, 'returnCountOnly': 'true'}).encode('utf-8')

    item_count = None
    while item_count is None:
        try:
            req = urllib.request.Request(FS_service)
            response = None
            while response is None:
                try:
                    response = urllib.request.urlopen(req,data=data)
                except:
                    pass
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
                        'Zip': Zip,
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
                        'Zip': Zip,
                        'LocationType': LocationType,
                        'SSID': SSID,
                        'ID1': ID1,
                        'ID2': ID2,
                        'Reserved' : 'No'
                    }
                    }]

        update_AGOL(upload_payload, edit_token, where_statement)

    else:
        if RES == True:
            upload_payload = [{
                    'geometry' : {'x' : float(Longitude), 'y' : float(Latitude)},
                    'attributes' : {
                        'APOwner' : APOwner,
                        'Street' : Street,
                        'City': City,
                        'Zip': Zip,
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
                        'Zip': Zip,
                        'LocationType': LocationType,
                        'SSID': SSID,
                        'ID1': ID1,
                        'ID2': ID2,
                        'Reserved' : 'No'
                    }
                    }]
        insert_AGOL(upload_payload, edit_token)

    return ()

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

def getStarbucks():
    url = 'https://www.starbucks.com/bff/locations?lat={}&lng={}'.format(Lat, Long)
    values = {}
    headers = {'authority' : 'www.starbucks.com',
               'scheme' : 'https',
               'path' : '/bff/locations?lat=48.043815&lng=-122.1335532',
               'accept' : 'application/json',
               'x-requested-with' : 'XMLHttpRequest',
               'referrer' : 'https://www.starbucks.com/store-locator?map={},{},12z'.format(Lat, Long)}

    req = urllib.request.Request(url)
    req.add_header('authority', 'www.starbucks.com')
    req.add_header('scheme', 'https')
    req.add_header('path', '/bff/locations?lat=48.043815&lng=-122.1335532')
    req.add_header('accept', 'application/json')
    req.add_header('x-requested-with', 'XMLHttpRequest')
    req.add_header('referrer', 'https://www.starbucks.com/store-locator?map={},{},12z'.format(Lat, Long))
    response = urllib.request.urlopen(req)
    the_page = response.read()

    payload_json = the_page.decode('utf8')
    payload_json = json.loads(payload_json)

    return (payload_json)

def parseStarBucksData(starbucksLocations):
    edit_token = get_token()
    for aps in starbucksLocations['stores']:
        print ('\n')
        APOwner = 'Starbucks'
        print ('    AP Owner:  {}'.format(APOwner))
        if aps['address']['streetAddressLine2'] is not None:
            Street = '{} {}'.format(aps['address']['streetAddressLine1'], aps['address']['streetAddressLine2'])
        else:
            Street = '{}'.format(aps['address']['streetAddressLine1'])
        print ('    Street:  {}'.format(Street))
        City = aps['address']['city']
        print ('    City:  {}'.format(City))
        Zip = aps['address']['postalCode']
        if len(Zip) > 5:
            Zip = '-'.join(Zip[i:i + 5] for i in range(0, len(Zip), 5))
        print ('    Zip:  {}'.format(Zip))
        LocationType = aps['name']
        SSID = 'Google Starbucks'
        print ('    SSID:  {}'.format(SSID))
        ID1 = aps['name']
        ID1 = ID1.replace('\'', '')
        print ('    ID1:  {}'.format(ID1))
        ID2 = aps['id']
        print ('    ID2:  {}'.format(ID2))
        Latitude = aps['coordinates']['latitude']
        print ('    Latitude:  {}'.format(Latitude))
        Longitude = aps['coordinates']['longitude']
        print ('    Longitude:  {}'.format(Longitude))

        pushToAGOL(edit_token, APOwner, Street, City, Zip, LocationType, SSID, ID1, ID2, Latitude, Longitude)

    return

def aisle6Cleanup():

    dt = datetime.date.today()
    dt = dt - datetime.timedelta(days=1)
    APOwner = 'Starbucks'
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

starbucksLocations = getStarbucks()
parseStarBucksData(starbucksLocations)
aisle6Cleanup()