#! /usr/bin/env python3

from databaseConnection import get_connection_by_config
from pyspark.sql import SparkSession
from pyspark.sql import dataframe
from pyspark.sql.functions import to_date
from pyspark.sql.types import (StructField, StringType, IntegerType, StructType, DoubleType, DateType)
from psycopg2.extras import execute_values


def select_noaa_cols(row): 
    return (row.stationid, row.obsdate, row.element, row.dataval, row.mflag, row.sflag, row.latitude, row.longitude)

def get_noaa_simple_array(dataframe): 
    return dataframe.rdd.map(select_noaa_cols).collect()

# a function to pull the columns I'm interested in from the epa dataframe
def select_epa_cols(row): 
    return (row.statecode, row.countycode, row.sitecode, row.obsdate, row.pollutantname, row.unit, row.aqi, row.measurement, row.latitude, row.longitude)

# uses select_epa_cols to pull columns of interest, producing an rdd, then returns 
# the rdd converted to an array 
def get_epa_simple_array(dataframe): 
    return dataframe.rdd.map(select_epa_cols).collect()

# takes epa data from S3 to postgres database
def epa_s3_to_postgres(): 
    epa_data_schema = StructType([StructField('statecode', StringType(), True),\
                                  StructField('countycode', StringType(), True),\
                                  StructField('sitecode', StringType(), True),\
                                  StructField('parameter_code', IntegerType(), True),\
                                  StructField('poc', IntegerType(), True),\
                                  StructField('latitude', DoubleType(), True),\
                                  StructField('longitude', DoubleType(), True),\
                                  StructField('datum', StringType(), True),\
                                  StructField('pollutantname', StringType(), True),\
                                  StructField('sample_duration', StringType(), True),\
                                  StructField('pollutant_standard', StringType(), True),\
                                  StructField('obsdate', DateType(), True), \
                                  StructField('unit', StringType(), True),\
                                  StructField('event_type', StringType(), True),\
                                  StructField('observation_count', IntegerType(), True),\
                                  StructField('observation_percent', DoubleType(), True),\
                                  StructField('measurement', DoubleType(), True),\
                                  StructField('first_max_value', DoubleType(), True),\
                                  StructField('first_max_hour', IntegerType(), True),\
                                  StructField('aqi', IntegerType(), True),\
                                  StructField('method_code', StringType(), True),\
                                  StructField('method_name', StringType(), True),\
                                  StructField('local_site_name', StringType(), True),\
                                  StructField('address', StringType(), True),\
                                  StructField('state_name', StringType(), True),\
                                  StructField('county_name', StringType(), True),\
                                  StructField('city_name', StringType(), True),\
                                  StructField('cbsa_name', StringType(), True),\
                                  StructField('date_of_last_change', DateType(), True)])

    spark = SparkSession.builder.appName("Spark").config("spark.driver.extraClassPath", "/home/ubuntu").config('spark.executor.extraClassPath', '/home/ubuntu').getOrCreate()
    # first, load file from S3 
    epa_data = spark.read.csv("s3a://epa-aq-data/daily_44201_2000.csv", header=True, schema=epa_data_schema)
    print('got the epa spark data frame!')
    # now load epa data to PostgreSQL 
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    cursor = newConnection.cursor()
    pollutantTable = '''
                    DROP TABLE IF EXISTS ozone_2000;
                    CREATE TABLE ozone_2000 (
                        id serial PRIMARY KEY,
                        statecode CHAR(2), 
                        countycode CHAR(3), 
                        sitecode CHAR(4), 
                        obsdate DATE, 
                        pollutantname VARCHAR(10), 
                        unit VARCHAR(25),
                        aqi SMALLINT, 
                        measurement REAL, 
                        latitude REAL,
                        longitude REAL 
                    );
                    '''
    cursor.execute(pollutantTable)
    print("made pollutant table in epageo database")
    # add selected cols from dataframe to database 
    insert_command = '''
        INSERT INTO ozone_2000(statecode, countycode, sitecode, obsdate, pollutantname, unit, aqi, measurement, latitude, longitude) VALUES %s
                    '''
    epa_arr = get_epa_simple_array(epa_data) 
    execute_values(cursor, insert_command, epa_arr, page_size=500)
    print('finished inserting values!') 
    # add column with postgis point 
    collComand = 'ALTER TABLE ozone_2000 ADD COLUMN geogcol geography(Point, 4326);'
    cursor.execute(collComand)
    updateComand = 'UPDATE ozone_2000 SET geogcol = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);'
    cursor.execute(updateComand)
    # add a unique identifier for each station in the country 
    addCombined = 'ALTER TABLE ozone_2000 ADD COLUMN uniquesite VARCHAR(10)'
    cursor.execute(addCombined)
    concatCommand = 'UPDATE ozone_2000 SET uniquesite = concat(statecode, countycode, sitecode)'
    cursor.execute(concatCommand)
    # commit changes to database, close connection 
    newConnection.commit()
    cursor.close()
    newConnection.close()
    print('exiting') 

# noaa weather data + station file from s3 to postgres 
def noaa_s3_to_postgres(): 
    station_schema = StructType([StructField('stationid', StringType(), True),\
                         StructField('latitude', DoubleType(), True),\
                         StructField('longitude', DoubleType(), True),\
                         StructField('elevation', DoubleType(), True),\
                         StructField('state', StringType(), True)])
    spark = SparkSession.builder.appName("Spark").config("spark.driver.extraClassPath", "/home/ubuntu").config('spark.executor.extraClassPath', '/home/ubuntu').getOrCreate()
    station_data = spark.read.csv("s3a://yearly-weather-data/ghcnd-stations.csv", header=False, schema=station_schema)
    print('got the noaa station data frame!')
    noaa_data_schema = StructType([StructField('stationid', StringType(), True),\
                                  StructField('obsdate', StringType(), True),\
                                  StructField('element', StringType(), True),\
                                  StructField('dataval', StringType(), True),\
                                  StructField('mflag', StringType(), True),\
                                  StructField('qflag', StringType(), True),\
                                  StructField('sflag', StringType(), True),\
                                  StructField('obstime', StringType(), True)])
    # first, load file from S3 
    noaa_data = spark.read.csv("s3a://yearly-weather-data/2000.csv", header=False, schema=noaa_data_schema)
    print('got the noaa spark data frame!')
    # clean data 
    noaa_data = noaa_data.filter(noaa_data['element'].contains('TMAX'))
    noaa_data = noaa_data.filter(noaa_data['stationid'].contains('US'))
    noaa_data = noaa_data.filter(noaa_data['qflag'].isNull())
    noaa_data = noaa_data.withColumn('obsdate', to_date(noaa_data.obsdate, 'yyyymmdd'))
    # join to station data
    noaa_data = noaa_data.join(station_data, 'stationid', 'inner').drop('elevation', 'state', 'qflag', 'obstime')
    print('sample noaa data')
    print(noaa_data.take(5))
    #noaa_data = noaa_data.filter(noaa_data.qflag.isNull()).collect()
    print('filtered data! :) ')
    # now load noaa data to PostgreSQL 
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    cursor = newConnection.cursor()
    noaa_table = '''
                    DROP TABLE IF EXISTS noaa_2000;
                    CREATE TABLE noaa_2000 (
                        id serial PRIMARY KEY,
                        stationid CHAR(11),
                        obsdate DATE, 
                        element CHAR(4), 
                        dataval INTEGER, 
                        mflag CHAR(1), 
                        sflag CHAR(1), 
                        latitude REAL,
                        longitude REAL
                    );
                    '''
    cursor.execute(noaa_table)
    print("made noaa_2000 table in epageo database")
# add selected cols from dataframe to database 
    insert_command = '''
        INSERT INTO noaa_2000(stationid, obsdate, element, dataval, mflag, sflag, latitude, longitude) VALUES %s
                    '''
    noaa_arr = get_noaa_simple_array(noaa_data)
    execute_values(cursor, insert_command, noaa_arr, page_size=500)
    print('finished inserting values!')
    # add column with postgis point
    collComand = 'ALTER TABLE noaa_2000 ADD COLUMN geogcol geography(Point, 4326);'
    cursor.execute(collComand)
    updateComand = 'UPDATE noaa_2000 SET geogcol = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);'
    cursor.execute(updateComand)
    # commit changes to database, close connection 
    newConnection.commit()
    cursor.close()
    newConnection.close()
    print('exiting')

def main(): 
#    epa_s3_to_postgres()
#    noaa_stations_s3_to_postgres()
    noaa_s3_to_postgres()

main()
