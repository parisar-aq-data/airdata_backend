# iudx_archive.py

import time
start = time.time()

import requests, json, os, sys, datetime, random
import pandas as pd

import commonfuncs as cf
import dbconnect

root = os.path.dirname(__file__)
url = 'https://rs.iudx.org.in/ngsi-ld/v1/entities/'

#################
# FUNCTIONS

#################
# MAIN PROGRAM
cf.logmessageIUDX("Starting IUDX AQM archiver")

# fetch locations
s1 = "select location_id, lat, lon, name, resourceid from locations where active!='N' and type='iudx'"
locations = dbconnect.makeQuery(s1, output='list')
if not len(locations):
    cf.logmessageIUDX("No locations!")
    sys.exit()

timestamp = datetime.datetime.utcnow().replace(microsecond=0) + datetime.timedelta(hours=5.5)
timestampStr = timestamp.strftime('%Y-%m-%d %H:%M:%S')

collector = []
for L in locations:
    fullUrl = url + L['resourceid']
    try:
        cf.logmessageIUDX(f"Fetching data for {L['location_id']}: {L['name']}: {L['resourceid']}")
        data = requests.get(fullUrl).json()['results'][0]
    except:
        cf.logmessageIUDX(f"Unable to fetch data for {L['name']}")
    row = {}
    row['location_id'] = L['location_id']
    row['lat'] = L['lat']
    row['lon'] = L['lon']

    row['pm25'] = data.get('pm2p5',{}).get('avgOverTime')
    row['so2'] = data.get('so2',{}).get('avgOverTime')
    row['uv'] = data.get('uv',{}).get('avgOverTime')
    row['illuminance'] = data.get('illuminance',{}).get('avgOverTime')
    row['airTemperature'] = data.get('airTemperature',{}).get('avgOverTime')
    row['co'] = data.get('co',{}).get('avgOverTime')
    row['ambientNoise'] = data.get('ambientNoise',{}).get('avgOverTime')
    row['atmosphericPressure'] = data.get('atmosphericPressure',{}).get('avgOverTime')
    row['airQualityIndex'] = data.get('airQualityIndex')
    row['co2'] = data.get('co2',{}).get('avgOverTime')
    row['o3'] = data.get('o3',{}).get('avgOverTime')
    row['relativeHumidity'] = data.get('relativeHumidity',{}).get('avgOverTime')
    row['pm10'] = data.get('pm10',{}).get('avgOverTime')
    row['no2'] = data.get('no2',{}).get('avgOverTime')
    
    row['airQualityLevel'] = data.get('airQualityLevel')
    row['aqiMajorPollutant'] = data.get('aqiMajorPollutant')
    row['deviceStatus'] = data.get('deviceStatus')

    # 2021-05-15T10:32:34+05:30
    ts = data.get('observationDateTime','').replace('T',' ')[:19]
    row['time1'] = ts
    row['date1'] = ts[:10]

    systimestamp = datetime.datetime.utcnow().replace(microsecond=0) + datetime.timedelta(hours=5.5)
    row['added_on'] = systimestamp

    collector.append(row)
    # do row by row insert - so we don't have to bother with few data points repeating and hitting the unique constraint
    status = dbconnect.addRow(row,tablename='aqdata2')
    if not status:
        cf.logmessageIUDX(f"Failed to save data for {L['location_id']} to DB, skipping")
    time.sleep(1)

df = pd.DataFrame(collector)
# preview
# df.to_csv('iudx_sample.csv',index=False)

end = time.time()
cf.logmessageIUDX(f"Script completed in {round(end-start,2)} secs")
