DROP TABLE IF EXISTS aqdata1;
CREATE TABLE aqdata1 (
    location_id VARCHAR(100) NULL,
    pm25 DECIMAL(20,16) NULL,
    time1 DATETIME NULL,
    date1 DATE NULL,
    lat DECIMAL(9,6) NULL,
    lon DECIMAL(9,6) NULL,
    added_on DATETIME NULL,
    CONSTRAINT aqdata1_c1 UNIQUE (location_id, time1)
);


DROP TABLE IF EXISTS locations;
CREATE TABLE locations (
    location_id VARCHAR(100) NOT NULL PRIMARY KEY,
    lat DECIMAL(9,6) NOT NULL,
    lon DECIMAL(9,6) NOT NULL,
    name VARCHAR(255) NULL,
    type VARCHAR(100) NULL,
    resourceid varchar(200) NULL,
    active VARCHAR(1) NULL
);


DROP TABLE IF EXISTS aqdata2;
CREATE TABLE aqdata2 (
    location_id VARCHAR(100) NULL,
    time1 DATETIME NULL,
    date1 DATE NULL,
    lat DECIMAL(9,6) NULL,
    lon DECIMAL(9,6) NULL,
    pm25 DECIMAL(9,4) NULL,
    so2 DECIMAL(9,4) NULL,
    uv DECIMAL(9,4) NULL,
    illuminance DECIMAL(9,4) NULL,
    airTemperature DECIMAL(9,4) NULL,
    co DECIMAL(9,4) NULL,
    ambientNoise DECIMAL(9,4) NULL,
    atmosphericPressure DECIMAL(9,4) NULL,
    airQualityIndex DECIMAL(9,4) NULL,
    co2 DECIMAL(9,4) NULL,
    o3 DECIMAL(9,4) NULL,
    relativeHumidity DECIMAL(9,4) NULL,
    pm10 DECIMAL(9,4) NULL,
    no2 DECIMAL(9,4) NULL,

    airQualityLevel VARCHAR(50) NULL,
    aqiMajorPollutant VARCHAR(20) NULL,
    deviceStatus VARCHAR(20) NULL,
    added_on DATETIME NULL,
    CONSTRAINT aqdata2_c1 UNIQUE (location_id, time1)
);
