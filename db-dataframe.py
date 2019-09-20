#! /usr/bin/env python3

#import psycopg2 as ps2
import pandas as pd
from databaseConnection import get_connection_by_config

def main(): 
    # make connection to database 
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    # set up query, make data frame from 
    nlines = 5
    query = """
            SELECT * FROM pollutant_table_new LIMIT {};
            """.format(nlines) 
    query_df = pd.read_sql(query, newConnection)
    newConnection.close()
    print(query_df.to_string())

main()
