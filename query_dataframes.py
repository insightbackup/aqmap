import pandas as pd
from database_connection import get_connection_by_config

# return a dataframe with monthly averages for a chosen pollutant for the specified geolocation
def get_pollutant_annual_df(year, pollutant, latitude, longitude):
    print('in get_pollutant_df func')
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    query = """
        SELECT distinct on ({1}_{0}_avg.month) {1}_{0}_avg.month, {1}_{0}_avg.dataval as conc
        FROM
        {1}_{0}_avg
        ORDER BY
        {1}_{0}_avg.month, {1}_{0}_avg.geogcol <->
        ST_MakePoint({3},{2})::geography ;
        """.format(year, pollutant, latitude, longitude)
    pollutant_year_df = pd.read_sql(query, newConnection)
    newConnection.close()
    return pollutant_year_df

# return a dataframe with monthly averages for temperature for the specified geolocation
def get_temp_annual_df(year, latitude, longitude):
    print('in get temp function')
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    query = """
    SELECT distinct on (noaa_{0}_avg.month) noaa_{0}_avg.month as month, noaa_{0}_avg.dataval/10 as tmax
    FROM noaa_{0}_avg
    ORDER BY noaa_{0}_avg.month, noaa_{0}_avg.geogcol  <->
    ST_MakePoint({2},{1})::geography ;
    """.format(year, latitude, longitude)
    temp_year_df = pd.read_sql(query, newConnection)
    newConnection.close()
    return temp_year_df

# return a dataframe with annual average temperatures
def get_temp_annual_avg_df(year, latitude, longitude, n_neighbors):
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    query = """
    SELECT AVG(dataval)/10 as tmax, ST_X(geogcol::geometry), ST_Y(geogcol::geometry)
    FROM noaa_{0}_avg GROUP BY noaa_{0}_avg.geogcol
    ORDER BY noaa_{0}_avg.geogcol <-> ST_MakePoint({2},{1})::geography
    LIMIT {3} ;
    """.format(year, latitude, longitude, n_neighbors)
    avg_df = pd.read_sql(query, newConnection)
    newConnection.close()
    return avg_df

# return a dataframe with annual averages for a pollutant at n_neighbors
# nearest neigbors of the latitude and longitude given
def get_pollutant_annual_avg_df(year, pollutant, latitude, longitude, n_neighbors):
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    query = """
    SELECT AVG(dataval) as conc, ST_X(geogcol::geometry), ST_Y(geogcol::geometry)
    FROM {1}_{0}_avg GROUP BY {1}_{0}_avg.geogcol
    ORDER BY {1}_{0}_avg.geogcol <-> ST_MakePoint({3},{2})::geography
    LIMIT {4} ;
    """.format(year, pollutant, latitude, longitude, n_neighbors)
    avg_df = pd.read_sql(query, newConnection)
    newConnection.close()
    return avg_df

