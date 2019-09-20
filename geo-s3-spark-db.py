#! /usr/bin/env python3

from databaseConnection import get_connection_by_config
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StringType, IntegerType, StructType, DoubleType, DateType)
from psycopg2.extras import execute_values

def main(): 
    # first, load file from S3 
    spark = SparkSession.builder.appName("Spark").config("spark.driver.extraClassPath", "/home/ubuntu").config('spark.executor.extraClassPath', '/home/ubuntu').getOrCreate()
    epa_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("s3a://epa-aq-data/daily_44201_2000.csv")
    print('got the data frame! printing first token on the second line:')

# now try to connect to PostgreSQL 
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    cursor = newConnection.cursor()
    # make new table 
    # can use NOT NULL before comma at end of lines below 
    pollutantTable = '''
                    DROP TABLE IF EXISTS pollutant_geo_new;
                    CREATE TABLE pollutant_geo_new (
                        id serial PRIMARY KEY,
                        obsdate DATE, 
                        pollutantname VARCHAR(10), 
                        unit VARCHAR(25),
                        poc SMALLINT, 
                        aqi SMALLINT, 
                        meanmeasurement REAL, 
                        latitude REAL,
                        longitude REAL 
                    );
                    '''
    cursor.execute(pollutantTable)
    print("made pollutant table in epageo database")
    # write data from s3 file to table 
    epa_lines = epa_data.collect()
    val_list = []
    database_row = '''
                    INSERT INTO pollutant_geo_new(obsdate, pollutantname, unit, poc, aqi, meanmeasurement, latitude, longitude) VALUES %s
                    '''
    for row in epa_lines: 
        insert_vals = (row[11],row[8],row[12],int(row[4]),int(row[19]),float(row[16]), float(row[5]), float(row[6]))
        val_list.append(insert_vals)
    execute_values(cursor, database_row, val_list, page_size=100)
    print('finished inserting values!') 
    # add column with postgis point 
    collComand = 'ALTER TABLE pollutant_geo_new ADD COLUMN geom geometry(Point, 4326);'
    cursor.execute(collComand)
    updateComand = 'UPDATE pollutant_geo_new SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);'
    cursor.execute(updateComand)
    # commit changes to database, close connection 
    newConnection.commit()
    cursor.close()
    newConnection.close()
    print('exiting') 


main()
