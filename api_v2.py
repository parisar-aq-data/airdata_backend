#!/usr/bin/env python
##
# API Version 2
# The API is being fleshed out more to support the front end.

# author Shweta Purushe
##

import dbconnect
import commonfuncs as cf
import datetime
import time
import os
import json
from tornado import escape
from tornado import concurrent
import tornado.web

from datetime import datetime

SHAPEFILE = 'pune.2017.wards.geojson'
# TODO paramterize the pollutant info in an enum if possible
POLLUTANT = 'pm25'
POLLUTANT_METRIC = 'average_daily_pm25'


root = os.path.dirname(__file__)
dataFolder = os.path.join(root, 'data')


class getGeoMappedPollutant(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def post(self):
        status, result = yield self.post_func()
        self.set_status(status)
        self.write(result)

    def getlocations(self, categories):
        catSQL = cf.quoteNcomma(categories)

        location_query = f"""select l.location_id
                                , lat,lon
                                , case when type = 'ward' then wmnm.name else l.name end as name
                                , type 
                            from locations l 
                            left join WardMarathiNameMap wmnm on wmnm.location_id  = l.location_id 
                    where type in ({catSQL}) and active != 'N'
                    order by type, location_id"""

        try:
            start = time.time()
            data = dbconnect.makeQuery(location_query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            locations = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(locations)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)

    # will return specific pollutant data for all category locations before a certain timestamp
    def getPollutantData(self, pollutant, categories, date, timestamp):

        catSQL = cf.quoteNcomma(categories)

        pollutant_query = f"""
        with geo as (
            select distinct l.location_id
                , lat,lon
                , case when type = 'ward' then wmnm.name else l.name end as name
                , type 
            from locations l 
            left join WardMarathiNameMap wmnm on wmnm.location_id  = l.location_id 
            where type in ({catSQL}) and active != 'N'
            order by type, l.location_id
        )
        , wards as (
            select time1, {pollutant} as average_daily_pm25, location_id 
            , row_number() over (partition by location_id ORDER by time1 desc) = 1 as latest_value
            from aqdata1
            where 
            date1 <= '{date}'
                and time1 <= '{timestamp}'
            order by location_id , latest_value
        )
        , iudx_and_safar as (
            select 
                    location_id 
                    , date1 
                    , avg({pollutant}) average_daily_pm25
            from aqdata2
            where date1 <= '{date}'
                and time1 <= '{timestamp}'
            GROUP BY 1,2
            order by location_id
        )
        , latest_iudx_safar as (
            select * 
                    , row_number() over (partition by location_id ORDER by date1 desc) = 1 as latest_value
            from iudx_and_safar 
            order by date1 desc
        )
        , mpcb as (
            select location_id 
                    ,round(avg(rspm),2) as average_daily_pm25
                    , row_number() over (partition by location_id) = 1 as latest_value
            from aqdata3 a3 
            group by 1
)
    select g.location_id , lat, lon, name, type, average_daily_pm25, latest_value
    from geo g
    inner join wards w on g.location_id = w.location_id
    where w.latest_value
        union
    select g.location_id , lat, lon, name, type, average_daily_pm25, latest_value
    from geo g 
    inner join latest_iudx_safar lis on g.location_id = lis.location_id
    where lis.latest_value
        UNION 
    select g.location_id , lat, lon, name, type, average_daily_pm25, latest_value
    from geo g 
    inner join mpcb m on g.location_id = m.location_id
    where m.latest_value"""

        try:
            pollutant_geolocations = dbconnect.makeQuery(
                pollutant_query, output='list')

            if not len(pollutant_geolocations):
                return cf.makeError("No data found in DB")

            return pollutant_geolocations
        except Exception as e:
            return cf.makeError(e)

    def getWardPolygons(self, returnD, pollutant_metric, categories):
        start = time.time()
        location_objs_with_pollutant = returnD.get("data")

        # filter out only wards
        # only wards have polygon data
        wardsData = [
            x for x in location_objs_with_pollutant if x['type'] == 'ward']

        # include shapes if type includes "ward"
        if 'ward' in categories:
            geo = json.load(open(os.path.join(dataFolder, SHAPEFILE), 'r'))

            for f in geo.get('features', []):
                properties = f.get('properties', {})
                ward = f"ward_{properties.get('prabhag','')}"

                # filter data
                matched_ward = next(
                    (w for w in wardsData if w['location_id'] == ward), None)

                if len(wardsData) > 0:
                    # update the matched ward properties with lat, lon, type and the pollutant_metric
                    if pollutant_metric in matched_ward.keys():
                        properties[pollutant_metric] = round(
                            float(matched_ward[pollutant_metric]), 4)
                        properties['lat'] = matched_ward['lat']
                        properties['lon'] = matched_ward['lon']
                        properties['type'] = matched_ward['type']

            returnD['shapes'] = geo

        else:
            returnD['shapes'] = {}
        end = time.time()
        cf.logmessage(
            f"Updated {len(wardsData)} locations in {round(end-start,2)} secs")

        return cf.makeSuccess(returnD)

    #######
    #
    # THE ACTUAL POST FUNC
    #
    #######

    @tornado.concurrent.run_on_executor
    def post_func(self):

        cf.logmessage(
            "* * * Calling the getGeoMappedPollutant API method * * *")

        returnD = {}  # final return object

       # unpacking payload
        payload = escape.json_decode(self.request.body)
        date1 = payload.get('date1')
        categories = payload.get('categories')
        timestr = payload.get('time', '23:59:59')
        timestamp = f"{date1} {timestr}"

        try:
            start = time.time()
            # fetch pollutant data for the 3 category locations (iudx, safar , ward)
            # for e.g.
            # {'location_id': 'iudx_01',
            #   'lat': '18.500945',
            #   'lon': '73.938842',
            #   'name': 'Hadapsar_Gadital_01',
            #   'type': 'iudx',
            #   'time1': '2021-06-06 08:01:03',
            #   'pm25': '16.3500'}
            # pollutant may be parameterized in the future
            pollutant_geolocations = self.getPollutantData(
                POLLUTANT, categories,  date1, timestamp)
            # 1. POLLUTANT, LOCATION, LAT, LONG (avg pollutant metric mapped to a geolocation)
            returnD["data"] = pollutant_geolocations

            # 2. in addition to #1 result --> GEOJSON DATA for every ward
            # FINAL RESULT
            updated_geo_data = self.getWardPolygons(
                returnD, POLLUTANT_METRIC, categories)

            end = time.time()
            cf.logmessage(
                f"{len(updated_geo_data)} data points and polygons retrieved in {round(end-start,2)} secs")

            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)


class getWardsAndMonitors(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def get(self):
        status, result = yield self.get_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def get_func(self):

        # BUILD QUERY

        query = f"""select case when type = 'ward' THEN wmnm.name else l.name end as label
                            ,case when type = 'ward' THEN UPPER(wmnm.name) else upper(l.name) end as value
                            ,UPPER(type) as type 
                    from locations l
                    left join WardMarathiNameMap wmnm on wmnm .location_id  = l.location_id"""
        cf.logmessage("* * * Calling the getWards API method * * *")

        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)
#
# get the top 3 least pollution reporting wards and top3 most pollution reporting wards for the month selected by User
#


class getPm25Ranks(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def post(self):
        status, result = yield self.post_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def post_func(self):

        # PAYLOAD
        payload = escape.json_decode(self.request.body)

        selectedMode = payload.get('selectedMode')
        # getting the range of data for which to report the pollution
        startDate = payload.get('startDate')
        endDate = payload.get('endDate')

        # BUILD QUERY

        if selectedMode == 'WARD':
            query = f"""
                    with av as 
                    (
                        select 
                                l.location_id 
                                , w.name
                                ,round(avg(pm25), 2) as Average_pm25
                        from locations l 
                        inner join aqdata1 a on a.location_id  = l.location_id 
                        inner join WardMarathiNameMap w on w.location_id  = l.location_id
                        where date1 between '{startDate}' and '{endDate}'
                        GROUP BY location_id, name
                    )
                    , ranks as (
                        select *
                                , DENSE_RANK () over (order by Average_pm25 ASC) as best
                                , DENSE_RANK () over (order by Average_pm25 DESC) as worst
                        FROM av
                    )
                    select * from ranks where best in (1,2,3) OR worst in (1,2,3)
            """
        elif selectedMode == 'IUDX':
            query = f"""
                    with daily_pm as (
                        select DISTINCT 
                                date1 
                                , l.location_id 
                                , name
                                , pm25  
                        from locations l 
                        left join aqdata2 a 
                            on l.location_id  = a.location_id 
                        where left(l.location_id, 1) = 'i' -- IUDX
                        and pm25 is not NULL   
                        and date1 between '{startDate}' and '{endDate}'
                    )
                    , av as (
                        SELECT name 
                            , round(avg(pm25), 2) as Average_pm25 
                        from daily_pm
                        group by name
                        order by Average_pm25 DESC 
                    )
                    , ranks as (
                        select *
                                , RANK () over (order by Average_pm25 ASC) as best
                                , RANK () over (order by Average_pm25 DESC) as worst
                        FROM av
                    )
                    select *
                    from ranks where best in (1,2,3) OR worst in (1,2,3)
            """
        elif selectedMode == 'SAFAR':
            query = f"""
                     with daily_pm as (
                        select DISTINCT 
                                date1 
                                , l.location_id 
                                , name
                                , pm25  
                        from locations l 
                        left join aqdata2 a 
                            on l.location_id  = a.location_id 
                        where left(l.location_id, 1) = 's' -- SAFAR
                        and pm25 is not NULL   
                        and date1 between '{startDate}' and '{endDate}'
                    )
                    , av as (
                        SELECT name 
                            , round(avg(pm25), 2) as Average_pm25 -- average pm25 for the selected date range
                        from daily_pm
                        group by name
                        order by Average_pm25 DESC 
                    )
                    , ranks as (
                        select *
                                , RANK () over (order by Average_pm25 ASC) as best
                        FROM av
                    )
                    select * from ranks order by best -- rank all SAFAR monitors
            """
        else:  # RANKING MPCB
            query = f"""
                    with av as (
                        select a.location_id
                                , name
                                , round(avg(rspm), 2) as Average_pm25 
                        from aqdata3 a
                        inner join locations l on a.location_id  = l.location_id
                        where rspm is not null 
                        and date1 BETWEEN '{startDate}' and '{endDate}'
                        group by 1
                    )
                    select * , DENSE_RANK () over (order by Average_pm25 ASC) as best from av order by best
            """

        cf.logmessage(
            f"* * * Calling the get_pm25_ranks API method for {selectedMode} * * *")

        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)


#
# get the CITY WIDE pollutant history across all wards/monitors for a particular datasource
# for the past 18 months relative to current date
# TODO parametrize pollutant

class getPollutantHistory(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def post(self):
        status, result = yield self.post_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def post_func(self):

        # PAYLOAD
        payload = escape.json_decode(self.request.body)
        selectedMode = payload.get('selectedMode')

        # BUILD QUERY

        if selectedMode == 'WARD':
            query = f"""
            WITH RECURSIVE t as (
                select DATE_FORMAT(DATE_ADD(CURRENT_DATE(), INTERVAL -17 MONTH) ,'%%Y-%%m-01') as dt UNION 
                SELECT DATE_ADD(t.dt, INTERVAL 1 MONTH) FROM t WHERE DATE_ADD(t.dt, INTERVAL 1 MONTH) <= DATE_FORMAT(CURRENT_DATE() ,'%%Y-%%m-01'))
            ,all_locations as (
                SELECT DISTINCT location_id
                from locations
                where UPPER(type) = '{selectedMode}'
            )
            , wards as (
                select 
                        al.*
                        , DATE_FORMAT(date1, '%%Y-%%m-01') as Month_Year
                        , pm25
                FROM all_locations al 
                left join aqdata1 a1 on al.location_id  = a1.location_id
                where pm25 is not null
                order by date1
            )
            , ward_aggs as (
            select dt as month_year
                    , round(avg(pm25), 2) as monthly_average_pm25 -- average across all wards
            from t 
                left join wards on t.dt = wards.Month_Year
            group by 1
            )
            select 
                    UPPER(DATE_FORMAT(month_year, '%%b')) as Month
                    , YEAR(month_year) as Year
                    , month(month_year) as month_number 
                    , monthly_average_pm25
            from ward_aggs
            """
        elif selectedMode == 'MPCB':
            query = f"""
            WITH RECURSIVE t as (
                select DATE_FORMAT(DATE_ADD(CURRENT_DATE(), INTERVAL -18 MONTH) ,'%%Y-%%m-01') as dt 
                UNION SELECT DATE_ADD(t.dt, INTERVAL 1 MONTH) 
                FROM t WHERE DATE_ADD(t.dt, INTERVAL 1 MONTH) <= DATE_FORMAT(CURRENT_DATE() ,'%%Y-%%m-01'))
            ,all_locations as (SELECT DISTINCT location_id from locations where UPPER(type) = '{selectedMode}')
            , mpcb_m as (
                select 
                        al.*
                        , DATE_FORMAT(date1, '%%Y-%%m-01') as Month_Year
                        , rspm
                FROM all_locations al 
                left join aqdata3 a3 on al.location_id  = a3.location_id
                where rspm is not null
                order by date1)
            , mpcb_aggs as (
            select dt as month_year
                    , round(avg(rspm), 2) as monthly_average_pm25 -- average across all wards
            from t 
                left join mpcb_m on t.dt = mpcb_m.Month_Year
            group by 1
            )
            select 
                    UPPER(DATE_FORMAT(month_year, '%%b')) as Month
                    , YEAR(month_year) as Year
                    , month(month_year) as month_number 
                    , monthly_average_pm25
            from mpcb_aggs
            """
        else:  # SAFAR and IUDX
            query = f"""
            WITH RECURSIVE t as (
                select DATE_FORMAT(DATE_ADD(CURRENT_DATE(), INTERVAL -18 MONTH) ,'%%Y-%%m-01') as dt 
                UNION 
                SELECT DATE_ADD(t.dt, INTERVAL 1 MONTH) FROM t 
            WHERE DATE_ADD(t.dt, INTERVAL 1 MONTH) <= DATE_FORMAT(CURRENT_DATE() ,'%%Y-%%m-01')
            )
            ,all_locations as (SELECT DISTINCT location_id from locations where UPPER(type) = '{selectedMode}')
            , daily_monitors as ( -- several readings a day
                select 
                        al.*
                        , DATE_FORMAT(date1, '%%Y-%%m-01') as Month_Year
                        , pm25 FROM all_locations al 
                left join aqdata2 a2 on al.location_id  = a2.location_id where pm25 is not null order by date1)
            ,monthly_avg as (
                SELECT location_id, Month_Year
                        , round(avg(pm25),2) as avg_monitor_pm25 from daily_monitors group by 1, 2)
            , mon_aggs as (
            select dt as month_year
                    , round(avg(avg_monitor_pm25), 2) as monthly_average_pm25 -- average across all iudx monnitors
            from t left join monthly_avg on t.dt = monthly_avg.Month_Year group by 1)
            select 
                    UPPER(DATE_FORMAT(month_year, '%%b')) as Month
                    , YEAR(month_year) as Year
                    , month(month_year) as month_number 
                    , monthly_average_pm25
            from mon_aggs"""
        cf.logmessage(
            f"* * * Calling the get_pollutant_history API method for {selectedMode}  * * *")

        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)


class getWardOrMonitorPollutantHistory(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def post(self):
        status, result = yield self.post_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def post_func(self):

        # PAYLOAD
        payload = escape.json_decode(self.request.body)

        selectedMode = payload.get('selectedMode')
        # getting the range of data for which to report the pollution
        startDate = payload.get('startDate')
        endDate = payload.get('endDate')
        wardOrMonitorName = payload.get("selectedWardOrMonitor")

        # BUILD QUERY

        if selectedMode == 'WARD':
            query = f"""
                        select  
                            UPPER(DATE_FORMAT(date1, '%%b')) as Month
                            , YEAR(date1) as Year
                            , month(date1) as Month_number
                            , round(avg(pm25), 2) as monthly_average_pm25
                        from locations l 
                        left join aqdata1 a1 on l.location_id  = a1.location_id
                        left join WardMarathiNameMap w on w.location_id  = l.location_id
                        where pm25 is not null
                        and w.name = '{wardOrMonitorName}'
                        and date1 between '{startDate}' and '{endDate}'
                        group by 1, 2, 3
                        ORDER BY Month_number
            """
        elif selectedMode == 'MPCB':
            query = f"""
                        SELECT UPPER(DATE_FORMAT(date1, '%%b')) as Month
                        , YEAR(date1) as Year
                        , month(date1) as Month_number
                        , round(avg(rspm), 2) as monthly_average_pm25
                        FROM locations l 
                            inner join aqdata3 mpcb on l.location_id = mpcb.location_id 
                        where rspm is not NULL and name ='{wardOrMonitorName}' and date1 between '{startDate}' and '{endDate}' 
                        GROUP by 1,2,3 order by year, Month_number"""
        else:  # SAFAR and IUDX
            query = f"""
                        select  
                            UPPER(DATE_FORMAT(date1, '%%b')) as Month
                            , YEAR(date1) as Year
                            , month(date1) as Month_number
                            , round(avg(pm25), 2) as monthly_average_pm25
                        from locations l 
                        left join aqdata2 a1 on l.location_id  = a1.location_id
                        where pm25 is not null
                        and name = '{wardOrMonitorName}'
                        and date1 between '{startDate}' and '{endDate}'
                        group by 1, 2, 3
                        ORDER BY Month_number
            """
        cf.logmessage(
            f"* * * Calling the getWardOrMonitorPollutantHistory API method for {selectedMode} : {wardOrMonitorName} * * *")

        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)


class getWardCentroids(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def get(self):
        status, result = yield self.get_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def get_func(self):

        # BUILD QUERY

        query = f"""select name, lat, lon from locations l where location_id like "%%ward%%" and active = 'Y'"""
        cf.logmessage("* * * Calling the getWardCentroids API method * * *")

        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)


class getWardOrMonitorSummary(cf.BaseHandler):
    executor = concurrent.futures.ThreadPoolExecutor(cf.maxThreads)

    @tornado.gen.coroutine
    def post(self):
        status, result = yield self.post_func()
        self.set_status(status)
        self.write(result)

    @tornado.concurrent.run_on_executor
    def post_func(self):

        # PAYLOAD
        payload = escape.json_decode(self.request.body)
        selectedMode = payload.get('selectedMode')
        wardOrMonitorName = payload.get("selectedWardOrMonitor")
        # getting the range of data for which to report the metadata
        startDate = payload.get('startDate')
        endDate = payload.get('endDate')

        # BUILD QUERY

        if selectedMode == 'WARD':
            query = f"""
            WITH RECURSIVE t as (select '{startDate}' as dt UNION SELECT DATE_ADD(t.dt, INTERVAL 1 DAY) FROM t WHERE DATE_ADD(t.dt, INTERVAL 1 DAY) <= '{endDate}')
            , aq_dates as (
                select distinct w.name, date1 , pm25
                from aqdata1 a1
                    inner join locations l on l.location_id  = a1.location_id
                    inner join WardMarathiNameMap w on w.location_id  = l.location_id 
                where w.name = '{wardOrMonitorName}' and date1 between '{startDate}' and '{endDate}'
            )
            , missing_days as (select ifnull(sum(case when date1 is null then 1 end),0) as num_missing_days from t left join aq_dates a on t.dt = a.date1 )
            , daily_pm as (
                select DISTINCT date1 , l.location_id , w.name, pm25 from locations l 
                inner join aqdata1 a on l.location_id  = a.location_id inner join WardMarathiNameMap w on w.location_id  = l.location_id  
                where pm25 is not NULL and date1 between '{startDate}' and '{endDate}'
            )
            , av as (SELECT name , round(avg(pm25), 2) as Average_pm25, count(*) over () as num_units from daily_pm group by name order by Average_pm25 DESC )
            , ranks as (select * , DENSE_RANK () over (order by Average_pm25 ASC) as pollution_rank FROM av)
            , threshold_days as (select  name , ifnull(sum(case when pm25 > 30 then 1 end),0) as count_exceeds_threshold from daily_pm where name = '{wardOrMonitorName}' group by 1)
            select ranks.* , threshold_days.count_exceeds_threshold , missing_days.num_missing_days from ranks 
            inner join threshold_days on ranks.name = threshold_days.name cross join missing_days
            """
        elif selectedMode == 'IUDX' or selectedMode == 'SAFAR':
            query = f"""
            WITH RECURSIVE t as (select '{startDate}' as dt UNION SELECT DATE_ADD(t.dt, INTERVAL 1 DAY) FROM t WHERE DATE_ADD(t.dt, INTERVAL 1 DAY) <= '{endDate}')
            , aq_dates as (select distinct name, date1 , pm25 from aqdata2 a2 
            inner join locations l on l.location_id  = a2.location_id where name = '{wardOrMonitorName}' and date1 between '{startDate}' and '{endDate}')
            , missing_days as (select ifnull(sum(case when date1 is null then 1 end),0) as num_missing_days from t left join aq_dates a on t.dt = a.date1 )
            , daily_pm as (select DISTINCT date1 ,time1, l.location_id , name, pm25 from locations l 
                left join aqdata2 a on l.location_id  = a.location_id 
                where left(l.location_id, 1) = (case when '{selectedMode}' = 'IUDX' then 'i' else 's' end) and pm25 is not NULL and date1 between '{startDate}' and '{endDate}')
            , av as (SELECT name , round(avg(pm25), 2) as Average_pm25, count(*) over () as num_units from daily_pm group by name order by Average_pm25 DESC )
            , ranks as (select *, DENSE_RANK () over (order by Average_pm25 ASC) as pollution_rank FROM av)
            ,several_daily as (select DISTINCT l.location_id, date1, time1, pm25 , name from locations l inner join aqdata2 a 
                on l.location_id  = a.location_id where date1 between '{startDate}' and '{endDate}' and name = '{wardOrMonitorName}' order by l.location_id , time1)
            ,avg_daily as (select name, date1, avg(pm25) as Average_pm25 from several_daily group by 1, 2)
            , threshold_days as (select  name , ifnull(sum(case when Average_pm25 > 30 then 1 end),0) as count_exceeds_threshold from avg_daily group by 1)
            select ranks.* , threshold_days.count_exceeds_threshold , missing_days.num_missing_days from ranks
            inner join threshold_days on ranks.name = threshold_days.name cross join missing_days
            """
        else:  # RANKING MPCB
            query = f"""
            WITH RECURSIVE t as (select '{startDate}' as dt UNION SELECT DATE_ADD(t.dt, INTERVAL 1 DAY) FROM t WHERE DATE_ADD(t.dt, INTERVAL 1 DAY) <= '{endDate}')
            , aq_dates as (
                select distinct name, date1 , rspm
                from aqdata3 a1 inner join locations l on l.location_id  = a1.location_id 
                where name = '{wardOrMonitorName}' and date1 between '{startDate}' and '{endDate}'
            )
            , missing_days as (select ifnull(sum(case when date1 is null then 1 end),0) as num_missing_days from t left join aq_dates a on t.dt = a.date1 )
            , daily_pm as (select DISTINCT date1 , l.location_id , name, rspm from locations l inner join aqdata3 a on l.location_id  = a.location_id 
                where rspm is not NULL and date1 between '{startDate}' and '{endDate}')
            , av as (SELECT name, count(*) over () as num_units, round(avg(rspm), 2) as Average_pm25 from daily_pm group by name order by Average_pm25 DESC)
            , ranks as (select *, DENSE_RANK () over (order by Average_pm25 ASC) as pollution_rank FROM av)
            , threshold_days as (select  name , ifnull(sum(case when rspm > 30 then 1 end),0) as count_exceeds_threshold from daily_pm where name = '{wardOrMonitorName}' group by 1)
            select ranks.* , threshold_days.count_exceeds_threshold, missing_days.num_missing_days from ranks
            inner join threshold_days on ranks.name = threshold_days.name cross join missing_days
            """

        cf.logmessage(
            f"* * * Calling the getWardOrMonitorSummary API method for {selectedMode} : {wardOrMonitorName} * * *")

        try:
            start = time.time()

            # MAKE QUERY
            data = dbconnect.makeQuery(query, output='list')
            if not len(data):
                return cf.makeError("No data found in DB")

            # FINAL RESULT
            returnD = {"data": data}

            end = time.time()
            cf.logmessage(
                f"{len(data)} data points in {round(end-start,2)} secs")
            return cf.makeSuccess(returnD)
        except TypeError as e:
            cf.logmessage(e)
            return cf.makeError(e)
