use airqual;

show tables in airqual;

select * 
from WardMarathiNameMap wm
inner join locations l on wm.location_id  = l.location_id 
;

show columns in aqdata1;

-- aqdata1 has ward data 
-- aqdata2 has iudx and safar data

-- earliest data is from march of 2021 

-- NOT all months have data from all sources 
with data_consistency as (
    select 
              EXTRACT(YEAR_MONTH from date1) month_year
            , case when left(location_id, 1) = 'i' then 'IUDX' else 'SAFAR' END as datasource
    from aqdata2 a 
    group by EXTRACT(YEAR_MONTH from date1), DATASOURCE
    order by 1, 2
)
select 
        concat(LEFT(month_year , 4) , '-', right(month_year , 2)) as month_year 
        ,datasource
--         , sum(case when datasource ='SAFAR' then 1 end) over (partition by datasource)
--         , sum(case when datasource ='IUDX' then 1 end) over (partition by datasource)
from data_consistency
group by 1, 2
;


-- DESCRIBE TABLE
select column_name,data_type 
from information_schema.columns 
where table_schema = 'aqdata1' -- and table_name = 'aqdata2' and table_name = 'aqdata3';
;

-- DATA RELATIONSHIPS
-- 1 -- 1 between location_id and name
SELECT location_id, name, count(distinct name)
from locations l 
group by location_id 
-- having count(DISTINCT name) > 1
;


-- API

-- GET WARDS AND MONITORS
select case when type = 'ward' THEN UPPER(wmnm.name) else upper(l.name) end label
        ,case when type = 'ward' THEN UPPER(wmnm.name) else upper(l.name) end as value
        ,UPPER(type) as type 
from locations l
left join WardMarathiNameMap wmnm on wmnm .location_id  = l.location_id 
;





/*
 * 
 * 
 * NEW RANKING April 19 2022
 * 
 * 
 * */
-- RANKING WARDS
with av as 
(
    select 
            l.location_id 
            , w.name
            ,round(avg(pm25), 2) as Average_pm25
    from locations l 
    inner join aqdata1 a on a.location_id  = l.location_id 
    inner join WardMarathiNameMap w on w.location_id  = l.location_id 
    where date1 between '2021/04/23' and '2021/11/16'
    GROUP BY location_id, name
)
, ranks as (
    select *
            , DENSE_RANK () over (order by Average_pm25 ASC) as best
            , DENSE_RANK () over (order by Average_pm25 DESC) as worst
    FROM av
)
select * from ranks where best in (1,2,3) OR worst in (1,2,3)
;

-- GET TOP 5 polluting IUDX and SAFAR monitors in a particular date range
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
    and date1 between '2021/04/23' and '2021/11/16'
    -- (2-3 daily readings per location)
    -- and l.location_id = 'iudx_38'
    ORDER by date1
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
select *
from ranks order by best
-- where best in (1,2,3) OR worst in (1,2,3)
;
-- RANKING MPCB
with av as (
    select a.location_id
            , name
            , round(avg(rspm), 2) as Average_pm25 
    from aqdata3 a
    inner join locations l on a.location_id  = l.location_id
    where rspm is not null
    and date1 BETWEEN '2007-08-04' and '2010-08-20'
    group by 1
)
select * , DENSE_RANK () over (order by Average_pm25 ASC) as best from av order by best;


-- POLLUTANT HISTORY
-- V2
-- wards

select  
        UPPER(DATE_FORMAT(date1, '%b')) as Month
        , YEAR(date1) as Year
        , month(date1) as Month_number
        , round(avg(pm25), 2) as monthly_average_pm25
from locations l 
left join aqdata1 a1 on l.location_id  = a1.location_id
where pm25 is not null and active = 'Y'
and name = 'कळस - धानोरी'
and date1 between '2021/04/23' and '2021/11/16'
group by 1,2, 3
-- ORDER BY str_to_date(MONTH,'%M')
order by month_number
;

-- IUDX and SAFAR


select  
         UPPER(DATE_FORMAT(date1, '%b')) as Month
        , YEAR(date1) as Year
        , month(date1) as Month_number
        , round(avg(pm25), 2) as monthly_average_pm25
from locations l 
left join aqdata2 a1 on l.location_id  = a1.location_id
where pm25 is not null
and name = 'LOHGAON'
and date1 between '2021/04/23' and '2022/09/29'
group by 1, 2, 3
ORDER BY Month_number
;





-- 50 iudx , 10 Safar, 41 wards

-- refactoring Nikhil's API work which took 2 minutes to run
with geo as (
    select distinct location_id, lat,lon, name, type 
    from locations 
    where type in ("iudx","safar","ward") 
        and active != 'N'
)
, wards as (
    select time1, pm25 as average_daily_pm25, location_id 
            , row_number() over (partition by location_id ORDER by time1 desc) = 1 as latest_value
    from aqdata1
    where 
     -- location_id  in ('ward1', 'ward2') 
         date1 <= '2021-6-6'
        and time1 <= '2021-6-6 12:00:00'
    order by location_id , latest_value DESC
)
, iudx_and_safar as (
    select 
            location_id 
            , date1 
            , avg(pm25) average_daily_pm25
    from aqdata2
    where
    date1 <= '2021-6-6'
        and time1 <= '2021-6-6 12:00:00'
    GROUP BY 1,2
    order by location_id
)
-- to get latest value
, latest_iudx_safar as (
    select * 
            , row_number() over (partition by location_id ORDER by date1 desc) = 1 as latest_value
    from iudx_and_safar 
    order by date1 desc
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
;






 

-- FINDING THE EUCLIDEAN DISTANCE FOR THE WARD A PARTICULAR MONITOR BELONGS TO 
with m as (
select lat as monitor_lat
      ,lon as monitor_lon
      , location_id 
      , name
from locations l 
where name like '%GOLF CLUB%'
)
, w as (
select distinct l2.location_id , name , l2.lat, l2.lon 
    from aqdata1 a1 
    inner join locations l2 
        on a1.location_id  = l2.location_id 
    where l2.location_id like '%ward%'
)
select 
        distinct 
        w.location_id as ward_location_id
        , lat as ward_lat
        , lon as ward_lon
        , monitor_lat
        , monitor_lon
        , m.location_id as monitor_locations_id
        , m.name as monitor_name
        , w.name as closest_ward_name
        , SQRT(power((monitor_lat - lat), 2) + power((monitor_lon - lon), 2)) as distance
from m 
cross join w  
order by distance asc
limit 5
;





-- HADAPSAR_GADITAL_01
-- NO DATA 2021/11/25 -- 2022/05/08







-- IUDX smartcity 50 raw monitors in random locations
-- SAFAR
-- WARDS




-- 3. Questions that should be answered by Dashboard 
-- a. How did this ward do compared to same time last year? -- NOT ENOUGH DATA 
-- b. Comparing all wards' performance over the last month?
-- c. Three best performing, worst performing wards 
-- d. For a ward, how many time did it exceed threshold by x%? 



-- average city pm2.5 in the last month. use map 
-- c. Three best performing, worst performing wards in the last month



/**DataSource > WARD > PM25 > dahsboard 

/*
 * 
 * 72 wards centroid ward No and name are unique : use this for identification for ward in UI dropdown 
 * 
 * DataSources : Safar (S_X) scrapping, iUDX - SmartCIty(IUDX_N) API: ward >> breezo (lat long) > pollution value 
 * 
 * 
 */

 

/**41 wards -- 0, N monitors (IUDX_, S_) -- show these in layers (map)

 -- pollution level of a ward > centroid > breezo (1 km2 resolution) numbers by tiles 
-- shivaji nagar pollution might be spread over 2 or more wards 



-- Top 5 
Safr > 
SmartC > 

marathi and English names 