# api1.py

import tornado.web
from tornado import concurrent
from tornado import escape
import json, os, time, datetime
import pandas as pd

import commonfuncs as cf
import dbconnect

root = os.path.dirname(__file__) # needed for tornado
dataFolder = os.path.join(root,'data')

shapesFile = 'pune.2017.wards.geojson'

#########################
# API CALLS

# class fetchBreezoMap(cf.BaseHandler):
#     executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)
#     @tornado.gen.coroutine
#     def get(self):
#         status, result = yield self.get_func()
#         self.set_status(status)
#         self.write(result)
    
#     @tornado.concurrent.run_on_executor 
#     def get_func(self):
#         cf.logmessage("fetchBreezoMap GET api call")
#         # to do: fetch latest data for a ward regardless of date
#         date1 = self.get_argument('date1',default='2021-04-18')
#         returnD = {}

#         # fetch data from DB
#         s1 = f"select * from aqdata1 where date1='{date1}' order by time1 desc"
#         df = dbconnect.makeQuery(s1, output='df')
#         if not len(df):
#             return cf.makeError("No data")

#         df = df.drop_duplicates('location_id')
#         locations_list = df['location_id'].tolist()
#         df = df.set_index('location_id')
#         # print(df)
#         geo = json.load(open(os.path.join(dataFolder,shapesFile), 'r'))
#         for f in geo.get('features',[]):
#             properties = f.get('properties',{})
#             ward = f"ward_{properties.get('prabhag','')}"
#             if ward in locations_list:
#                 properties['pm25'] = round(float(df.at[ward,'pm25']),4)
#                 properties['time1'] = df.at[ward,'time1']
#                 properties['lat'] = df.at[ward,'lat']
#                 properties['lon'] = df.at[ward,'lon']
        
#         returnD['data'] = geo
#         return cf.makeSuccess(returnD)


class getLatestData(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)
    @tornado.gen.coroutine
    def post(self):
        status, result = yield self.post_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor 
    def post_func(self):
        cf.logmessage("getLatestData POST api call")
        start = time.time()
        payload = escape.json_decode(self.request.body)

        '''
        fetch latest data under a category uptill given date / time
        { "date1": "2021-06-06",
          "categories": ["iudx","safar","ward"],
          "time": "12:00:00",
          "idsList": []
        }
        '''
        date1 = payload.get('date1')
        categories = payload.get('categories')
        catSQL = cf.quoteNcomma(categories)
        # fetch location ids first

        s1 = f"""select location_id, lat,lon, name, type from locations 
        where type in ({catSQL}) and active != 'N'
        order by type, location_id"""
        data = dbconnect.makeQuery(s1, output='list')
        if not len(data):
            return cf.makeError("No data found in DB")

        timestr = payload.get('time','23:59:59')
        timestamp = f"{date1} {timestr}"

        # loop through locations and query for latest data up till there.
        for N,row in enumerate(data):
            if row['type'] == 'ward':
                s2 = f"""select time1, pm25 from aqdata1
                where location_id='{row['location_id']}'
                and date1 <= '{date1}'
                and time1 <= '{timestamp}'
                order by time1 desc
                limit 1
                """
            elif row['type'] == 'iudx':
                s2 = f"""select time1, pm25, so2, uv, illuminance, airTemperature, co, 
                ambientNoise, atmosphericPressure, airQualityIndex, co2, o3, relativeHumidity,
                pm10, no2, airQualityLevel, aqiMajorPollutant, deviceStatus
                from aqdata2
                where location_id='{row['location_id']}'
                and date1 <= '{date1}'
                and time1 <= '{timestamp}'
                order by time1 desc
                limit 1
                """
            elif row['type'] == 'safar':
                s2 = f"""select time1, pm25, co, o3, pm10, no2
                from aqdata2
                where location_id='{row['location_id']}'
                and date1 <= '{date1}'
                and time1 <= '{timestamp}'
                order by time1 desc
                limit 1
                """
            dataRow = dbconnect.makeQuery(s2, output='oneJson')
            if not len(dataRow):
                cf.logmessage(s2)
                cf.logmessage(f"No data for {row['location_id']}, skipping")
                continue
            data[N].update(dataRow)
        
        returnD = {"data":data}

        # include shapes if type includes "ward"
        if 'ward' in categories:
            geo = json.load(open(os.path.join(dataFolder,shapesFile), 'r'))
            for f in geo.get('features',[]):
                properties = f.get('properties',{})
                ward = f"ward_{properties.get('prabhag','')}"
                # filter data
                matchingData = [x for x in data if x['location_id'] == ward]
                if len(matchingData) > 0:
                    if 'pm25' in matchingData[0].keys():
                        properties['pm25'] = round(matchingData[0]['pm25'],4)
                        properties['time1'] = matchingData[0]['time1']
                        properties['lat'] = matchingData[0]['lat']
                        properties['lon'] = matchingData[0]['lon']
                        properties['type'] = matchingData[0]['type']
            
            returnD['shapes'] = geo
        else: 
            returnD['shapes'] = {}

        end = time.time()
        cf.logmessage(f"{len(data)} data points in {round(end-start,2)} secs")
        return cf.makeSuccess(returnD)
