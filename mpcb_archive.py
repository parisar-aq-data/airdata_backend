# mpcb_archive.py

import time
start = time.time()

import requests, json, os, sys, datetime, random
import pandas as pd
from bs4 import BeautifulSoup
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

import commonfuncs as cf
import dbconnect

root = os.path.dirname(__file__)
url = 'https://mpcb.ecmpcb.in/envtdata/getwholestation.php'


#################
# FUNCTIONS

def getDates(diff=0, offset=5.5, dformat='%d-%m-%Y', startOffset=-1):
    d1 = datetime.datetime.utcnow() + datetime.timedelta(hours=offset) + datetime.timedelta(days=startOffset)
    if diff:
        d2 = d1 - datetime.timedelta(days=diff)
    else:
        d2 = d1
    return d2.strftime(dformat), d1.strftime(dformat)

def validateDate(text1, start_date, end_date):
    dstart = datetime.datetime.strptime(start_date, '%d-%m-%Y')
    dend = datetime.datetime.strptime(end_date, '%d-%m-%Y')
    # cf.logmessageMpcb(dstart,dend)
    try:
        d = datetime.datetime.strptime(text1, '%d-%m-%Y')
    except ValueError as e:
        # cf.logmessageMpcb(e)
        return False
    if dstart <= d <= dend:
        return True
    else:
        # cf.logmessageMpcb("Date out of bounds:",text1)
        return False

def shortname(text1, prefix='mpcb_',length=4):
    text2 = text1.replace(' ','').lower()[:4]
    return prefix+text2

def changeDateFormat(text1, informat='%d-%m-%Y', dformat='%Y-%m-%d'):
    d = datetime.datetime.strptime(text1, informat)
    return d.strftime(dformat)

#################
# MAIN PROGRAM

if len(sys.argv)==3:
    start_date = sys.argv[1]
    end_date = sys.argv[2]

    try:
        datetime.datetime.strptime(start_date, '%d-%m-%Y')
        datetime.datetime.strptime(end_date, '%d-%m-%Y')
    except ValueError as e:
        cf.cf.logmessageMpcb(r"Invalid start n end dates, give in format: %d-%m-%Y")
        sys.exit()
else:
    start_date, end_date = getDates()

payload = {
    "cId": "0000000077",
    "fdate": start_date,
    "tdate": end_date
}

r = requests.get(url, params=payload, allow_redirects=True, verify=False)
cf.logmessageMpcb(r.url)

soup = BeautifulSoup( r.text , 'lxml')

locations = [shortname(x.text) for x in soup.select('td.skyblue2')]
cf.logmessageMpcb("locations:",locations)

tablesHolder = soup.select('table.readtab')

assert len(tablesHolder) == len(locations), "Mismatch between locations and tables"

collector = []
for tN,table in enumerate(tablesHolder):
    rowsHolder = table.select('tr')
    cf.logmessageMpcb(tN, locations[tN])
    for row in rowsHolder:
        tdHolder = row.select('td')
        if len(tdHolder) < 6: 
            continue
        check = validateDate(tdHolder[1].text, start_date, end_date)
        # cf.logmessageMpcb(tdHolder[1].text, check)
        if check:
            cf.logmessageMpcb(tdHolder[1].text)
            try:
                so2 = float(tdHolder[2].text)
            except:
                so2 = None
            try:
                nox = float(tdHolder[3].text)
            except:
                nox = None

            try:
                rspm = float(tdHolder[4].text)
            except:
                rspm = None
             
            try:
                aqi = float(tdHolder[5].text)
            except:
                aqi = None
            
            if (so2 or nox or rspm or aqi):
                collector.append({
                    'location_id':locations[tN], 
                    'date1': changeDateFormat(tdHolder[1].text), 
                    'so2':so2, 'nox':nox, 'rspm':rspm, 'aqi':aqi
                })
            else:
                cf.logmessageMpcb(f"No data for {locations[tN]}: {tdHolder[1].text}")
                cf.logmessageMpcb(tdHolder)

            

df = pd.DataFrame(collector)

# get rid of dupes
l1 = len(df)
df = df.drop_duplicates(['location_id','date1'])
l2 = len(df)

if l1 > l2:
    cf.logmessageMpcb(f"{l1-l2} dupes dropped")

if not len(df):
    cf.logmessageMpcb(f"No records found between dates {start_date} and {end_date}")
    sys.exit()

cf.logmessageMpcb(f"Total {len(df)} records found between dates {start_date} and {end_date}")

# preview
# df.to_csv("mpcb_preview.csv", index=False)

status = dbconnect.addTable(df, tablename='aqdata3')
if not status:
    cf.logmessageMpcb(f"Failed to save data to DB, skipping")

end = time.time()
cf.logmessageMpcb(f"Script completed in {round(end-start,2)} secs")

