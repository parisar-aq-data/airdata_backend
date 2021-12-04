# safar_archive.py

import time
start = time.time()

import requests, json, os, sys, datetime, random
import pandas as pd
from bs4 import BeautifulSoup

import commonfuncs as cf
import dbconnect

root = os.path.dirname(__file__)
url = 'http://safar.tropmet.res.in/map_data.php?for=current&city_id=1'


def pm25Transform(x):
    try:
        x = float(x)
    except:
        # not a number? go back.
        cf.logmessageSafar(f"Note: This pm25 value is not a number: [{x}]")
        return x

    if x <= 100:
        return x * 0.6
    elif x <= 300:
        return 60 + (x-100)*0.3
    else:
        return 120 + (x-300)*1.3

#################
# MAIN PROGRAM
cf.logmessageSafar("Starting Safar AQM archiver")

r = requests.get(url, allow_redirects=True)
soup = BeautifulSoup( r.text , 'lxml')

# getting to the main data, then cleaning out quotes mess
holder1 = soup.select('script')[-3].string.replace('\r\n','').replace('\t','').strip()[14:-1]
holder2 = holder1.replace('"title"','X1X').replace('"lat"','X2X').replace('"lng"','X3X').replace('"description"','X4X')\
    .replace('"','').replace("'",'"')\
    .replace("X1X",'"title"').replace("X2X",'"lat"').replace("X3X",'"lng"').replace("X4X",'"description"')
holder3 = json.loads(holder2)

# got the main code, now have to loop through it

collector = []
for row1 in holder3:
    station = {'title':row1['title'], 'lat':row1['lat'], 'lon':row1['lng']}
    holder4 = BeautifulSoup(row1['description'] , 'lxml')
    for n,trow in enumerate(holder4.select('tr')[4:-1]):
        holder5 = trow.select('td')
        if holder5[1].text != 'NA':
            station[holder5[0].text] = holder5[1].text
    collector.append(station)

df = pd.DataFrame(collector)
df.rename(columns={"PM2.5":"pm25"}, inplace=True)
df.columns = [x.lower() for x in df.columns] # from https://stackoverflow.com/questions/19726029/how-can-i-make-pandas-dataframe-column-headers-all-lowercase

# mention places
cf.logmessageSafar(f"Got data for {len(df)} places: {', '.join(df['title'].tolist())}")

df['location_id']= df['title'].apply(lambda x: f"s_{x.lower()[:4]}")
del df['title']

systimestamp = datetime.datetime.utcnow().replace(microsecond=0) + datetime.timedelta(hours=5.5)
df['time1'] = df['added_on'] = systimestamp
df['date1'] = systimestamp.strftime('%Y-%m-%d')

# 2021-06-26 : transform pm25 values to convert from AQI to PPM
df['pm25'] = df['pm25'].apply(pm25Transform)

# preview:
# df.to_csv('safar_sample.csv', index=False)

status = dbconnect.addTable(df, tablename='aqdata2')
if not status:
    cf.logmessageSafar(f"Failed to save data to DB, skipping")

end = time.time()
cf.logmessageSafar(f"Script completed in {round(end-start,2)} secs")

