#! /usr/bin/env python3

from databaseConnection import get_connection_by_config
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import substring, col, avg, to_date
from pyspark.sql.types import (StructField, StringType, IntegerType, StructType, DoubleType, DateType)
from psycopg2.extras import execute_values

# return a list of years to loop over 
def get_year_list(): 
    year_list = list()
    first_year = 2000
    last_year = 2018
    for year in range(first_year, last_year+1):
        year_list.append(str(year))
    return year_list

# Below: functions related to converting imported dataframes into 
# python lists of lists (arrays) containing only the columns I want to 
# insert into the PostGIS tables

# pull the columns I'm storing from a noaa dataframe 
def select_noaa_cols(row): 
    return (row.month, row.dataval, row.latitude, row.longitude)

def get_noaa_simple_array(dataframe): 
    return dataframe.rdd.map(select_noaa_cols).collect()

# pull the columns I'm storing from an epa dataframe
def select_epa_cols(row): 
    return (row.month, row.dataval, row.latitude, row.longitude)

# uses select_epa_cols to pull columns of interest, producing an rdd, then returns 
# the rdd converted to an array 
def get_epa_simple_array(dataframe): 
    return dataframe.rdd.map(select_epa_cols).collect()

# takes epa data from S3 to postgres database for a given year, pollutant
def epa_s3_to_postgres(pollutant_name, year): 
    pollutant_name_to_code = {'ozone':44201, 'pm25':88101, 'no2':42602} 
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
                                  StructField('obsdate', StringType(), True), \
                                  StructField('unit', StringType(), True),\
                                  StructField('event_type', StringType(), True),\
                                  StructField('observation_count', IntegerType(), True),\
                                  StructField('observation_percent', DoubleType(), True),\
                                  StructField('dataval', DoubleType(), True),\
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
    file_path = 's3a://epa-aq-data/daily_{0}_{1}.csv'.format(pollutant_name_to_code[pollutant_name], year)
    epa_data = spark.read.csv(file_path, header=True, schema=epa_data_schema)
    print('got the epa spark data frame!')
    # add month, do averaging by month
    epa_data = epa_data.withColumn('month', substring('obsdate', 6, 2).cast(IntegerType()))
    monthly_epa_data = epa_data.groupBy('month', 'longitude', 'latitude').agg(avg(col('dataval')).alias('dataval'))
    # now load epa data to PostgreSQL 
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    cursor = newConnection.cursor()
    pollutantTable = '''
                    DROP TABLE IF EXISTS {0}_{1}_avg;
                    CREATE TABLE {0}_{1}_avg (
                        month INTEGER, 
                        dataval REAL, 
                        latitude REAL,
                        longitude REAL 
                    );
                    '''.format(pollutant_name, year)
    cursor.execute(pollutantTable)
    print("made pollutant table in epageo database")
    # add selected cols from dataframe to database 
    insert_command = '''
        INSERT INTO {0}_{1}_avg(month, dataval, latitude, longitude) VALUES %s
                    '''.format(pollutant_name, year)
    epa_arr = get_epa_simple_array(monthly_epa_data) 
    execute_values(cursor, insert_command, epa_arr, page_size=500)
    # add column with postgis point 
    collComand = 'ALTER TABLE {0}_{1}_avg ADD COLUMN geogcol geography(Point, 4326);'.format(pollutant_name, year)
    cursor.execute(collComand)
    updateComand = 'UPDATE {0}_{1}_avg SET geogcol = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);'.format(pollutant_name, year)
    cursor.execute(updateComand)
    # add index
    indexComand = 'CREATE INDEX {0}_{1}_geog_index ON {0}_{1}_avg (geogcol) ;'.format(pollutant_name, year)
    cursor.execute(indexComand)
    # commit changes to database, close connection 
    newConnection.commit()
    cursor.close()
    newConnection.close()
    print('Finished processing epa data pollutant {0} year {1}'.format(pollutant_name, year))

# handle all epa data files for chosen years/pollutants 
def proc_all_epa(): 
    year_list = get_year_list()
    pollutant_list = ['no2', 'pm25', 'ozone']
    # loop over all combinations
    for pollutant_name in pollutant_list: 
        for year in year_list: 
            epa_s3_to_postgres(pollutant_name, year)
    print('finished processing epa data for all pollutants!')

# noaa weather data + station file from s3 to postgres 
def noaa_s3_to_postgres(year): 
    # first, handle the station info file 
    station_schema = StructType([StructField('stationid', StringType(), True),\
                         StructField('latitude', DoubleType(), True),\
                         StructField('longitude', DoubleType(), True),\
                         StructField('elevation', DoubleType(), True),\
                         StructField('state', StringType(), True)])
    spark = SparkSession.builder.appName("Spark").config("spark.driver.extraClassPath", "/home/ubuntu").config('spark.executor.extraClassPath', '/home/ubuntu').getOrCreate()
    station_data = spark.read.csv("s3a://yearly-weather-data/ghcnd-stations.csv", header=False, schema=station_schema)
    # now, deal with the annual file
    noaa_data_schema = StructType([StructField('stationid', StringType(), True),\
                                  StructField('obsdate', StringType(), True),\
                                  StructField('element', StringType(), True),\
                                  StructField('dataval', StringType(), True),\
                                  StructField('mflag', StringType(), True),\
                                  StructField('qflag', StringType(), True),\
                                  StructField('sflag', StringType(), True),\
                                  StructField('obstime', StringType(), True)])
    # first, load file from S3 
    file_path = "s3a://yearly-weather-data/{}.csv".format(year)
    noaa_data = spark.read.csv(file_path, header=False, schema=noaa_data_schema)
    # clean data 
    noaa_data = noaa_data.filter(noaa_data['element'].contains('TMAX'))
    noaa_data = noaa_data.filter(noaa_data['stationid'].contains('US'))
    noaa_data = noaa_data.filter(noaa_data['qflag'].isNull())
    # add month column
    noaa_data = noaa_data.withColumn('month', substring('obsdate', 5, 2).cast(IntegerType()))
    # join to station data
    noaa_data = noaa_data.join(station_data, 'stationid', 'inner').drop('elevation', 'state', 'qflag', 'obstime')
    # group data by month, longitude, latitude. then do average
    monthly_noaa_data = noaa_data.groupBy('month', 'longitude', 'latitude').agg(avg(col('dataval')).alias('dataval'))
    # now load noaa data to PostgreSQL 
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    cursor = newConnection.cursor()
    noaa_table = '''
                    DROP TABLE IF EXISTS noaa_{0}_avg;
                    CREATE TABLE noaa_{0}_avg (
                        month INTEGER, 
                        dataval INTEGER, 
                        latitude REAL,
                        longitude REAL
                    );
                    '''.format(year)
    cursor.execute(noaa_table)
    # add selected cols from dataframe to database 
    insert_command = '''
        INSERT INTO noaa_{}_avg(month, dataval, latitude, longitude) VALUES %s
                    '''.format(year)
    noaa_arr = get_noaa_simple_array(monthly_noaa_data)
    execute_values(cursor, insert_command, noaa_arr, page_size=500)
    # add column with postgis point
    collComand = 'ALTER TABLE noaa_{}_avg ADD COLUMN geogcol geography(Point, 4326);'.format(year)
    cursor.execute(collComand)
    updateComand = 'UPDATE noaa_{}_avg SET geogcol = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);'.format(year)
    cursor.execute(updateComand)
    # add index 
    indexComand = 'CREATE INDEX noaa_{0}_geog_index ON noaa_{0}_avg (geogcol) ;'.format(year)
    cursor.execute(indexComand)
    # commit changes to database, close connection 
    newConnection.commit()
    cursor.close()
    newConnection.close()
    print('Finished processing NOAA data year '+year)

def proc_all_noaa(): 
    year_list = get_year_list()
    for year in year_list: 
        noaa_s3_to_postgres(year)
    print('Finished processing all NOAA data!')

def main(): 
    proc_all_epa()
    proc_all_noaa()

main()
