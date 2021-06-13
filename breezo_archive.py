# fetchAQdata.py
NODB = False
import time
start = time.time()

import requests, json, os, sys, datetime, random
import pandas as pd

import commonfuncs as cf
import dbconnect

root = os.path.dirname(__file__)

url = os.environ.get('BREEZO_URL')
apikey = os.environ.get('BREEZO_APIKEY')


#################
# FUNCTIONS

#################
# MAIN PROGRAM

cf.logmessageBreezo("Starting Breezo script")
# fetch locations
s1 = "select location_id, lat, lon from locations where active!='N' and type='ward'"
locations = dbconnect.makeQuery(s1, output='list')
if not len(locations):
    cf.logmessageBreezo("No locations!")
    sys.exit()

timestamp = datetime.datetime.utcnow().replace(microsecond=0) + datetime.timedelta(hours=5.5)
# date1 = timestamp.strftime('%Y-%m-%d')
timestampStr = timestamp.strftime('%Y-%m-%d %H:%M:%S')

insertCounter = 0
collector = []
for L in locations:
    params = {
        "api-key": apikey,
        "duration": "1d",
        "product": "pm25",
        "latitude": L['lat'],
        "longitude": L['lon']
    }

    try:
        r = requests.get(url, params=params)
        response = r.json()
    except:
        cf.logmessageBreezo("API call failed, pausing 10 secs before trying next one")
        time.sleep(10)
        continue
    
    data = response.get('data',[])
    if not isinstance(data,list):
        cf.logmessageBreezo(f"Warning: 'data' not array:")
        cf.logmessageBreezo(response)
        continue
    
    if not len(data):
        cf.logmessageBreezo(f"No data received for {L['lat']}, {L['lon']}")
        continue

    for row in data:
        L['pm25'] = row.get('pm25',None)
        if not L['pm25']:
            continue
        timeHolder = row.get('datetime')
        if timeHolder:
            L['time1'] = datetime.datetime.strptime(timeHolder[:19],'%Y-%m-%dT%H:%M:%S')
            time1 = timeHolder.replace('T',' ')
        else:
            L['time1'] = timestamp
            time1 = timestampStr

        L['date1'] = L['time1'].strftime('%Y-%m-%d')
        L['added_on'] = timestamp

        # insert into DB, and if unique constraint hit then move on
        i1 = f"""insert into aqdata1 (location_id,pm25,time1,date1,lat,lon,added_on)
        values ('{L['location_id']}',{L['pm25']},'{L['time1']}','{L['date1']}',{L['lat']},{L['lon']},'{timestampStr}')
        """
        status1 = dbconnect.execSQL(i1)
        if not status1:
            pass
        else:
            insertCounter += 1
        collector.append(L.copy())

    # break # trial run
    # pause before next API call
    time.sleep(1)


if not len(collector):
    cf.logmessageBreezo("No data received at all, exiting")
    sys.exit()
    
# df['date1'] = date1 
cf.logmessageBreezo(f"Fetched {len(collector)} rows data, inserted {insertCounter}")

if NODB:
    df = pd.DataFrame(collector) # should have new cols
    df.to_csv('preview_breezo.csv',index=False)

# else:
#     status = dbconnect.addTable(df, tablename='aqdata1')
#     if not status:
#         cf.logmessageBreezo("Failed to insert to DB, pls check")

end = time.time()
cf.logmessageBreezo(f"Script completed in {round(end-start,2)} secs")
