import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from geopy.geocoders import OpenCage

import pandas as pd
import plotly.graph_objs as go

from databaseConnection import get_connection_by_config

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# what data is available? 
years_available = list()
for year in range(2000, 2019): 
    years_available.append(str(year))


# return a dataframe with monthly averages for a chosen pollutant for the specified geolocation
def get_pollutant_annual_df(year, pollutant, latitude, longitude):
    print('in get_pollutant_df func')
    newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')
    query = """
        SELECT distinct on ({1}_{0}_avg.month) {1}_{0}_avg.month, {1}_{0}_avg.avg
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
    SELECT distinct on (noaa_{0}_avg.month) noaa_{0}_avg.month as month, noaa_{0}_avg.avg/10 as tmax
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
    SELECT AVG(avg)/10 as tmax, ST_X(geogcol::geometry), ST_Y(geogcol::geometry)
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
    SELECT AVG(avg), ST_X(geogcol::geometry), ST_Y(geogcol::geometry)
    FROM {1}_{0}_avg GROUP BY {1}_{0}_avg.geogcol 
    ORDER BY {1}_{0}_avg.geogcol <-> ST_MakePoint({3},{2})::geography 
    LIMIT {4} ;
    """.format(year, pollutant, latitude, longitude, n_neighbors)
    avg_df = pd.read_sql(query, newConnection)
    newConnection.close()
    return avg_df

def make_scatter(df, marker_color, xlabel, ylabel, plot_title, xname, yname): 
    ticknums = [1,2,3,4,5,6,7,8,9,10,11,12]
    tickmonths = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=df[xname],y=df[yname],
            mode='markers',
            marker={
                'size': 15,
                'opacity': 0.8,
                'color': marker_color,
                'line': {'width': 0.5, 'color': 'white'}
              }
        )
    )
    fig.update_layout(xaxis = go.layout.XAxis(tickmode = 'array', tickvals = ticknums, ticktext = tickmonths, title = xlabel), yaxis = go.layout.YAxis(title = ylabel), title=go.layout.Title(text=plot_title), hovermode='closest')
    return fig

def make_geo(df, chart_title, lonname, latname, textname, user_lon, user_lat):
    fig = go.Figure()
    # add database points
    fig.add_trace(
        go.Scattergeo(
               lon = df[lonname],
               lat = df[latname],
               text = df[textname],
               name = '',
               showlegend = False,
               mode='markers',
               marker = dict(
                   color = df[textname],
                   size = 12,
                   line = dict(
                        width = 1,
                        color = 'gray'
                       ),
                   showscale = True,
                   #colorscale = 'Parula',
                   autocolorscale = False,
                   symbol = 'circle',
                   colorbar_title = '' 
                )
        )
    )
    # add user point 
    fig.add_trace(
        go.Scattergeo(
               lon = [user_lon],
               lat = [user_lat],
               text = ['Input Address'],
               name = '',
               showlegend = False,
               mode='markers',
               marker = dict(
                        showscale = False,
                        color = 'black',
                        size = 14,
                        symbol = 'star',
                        colorbar_title = ''
                   )
        )
    )
    margin = 0.1
    min_lat = df[latname].min()-margin
    max_lat = df[latname].max()+margin
    min_lon = df[lonname].min()-margin
    max_lon = df[lonname].max()+margin
    fig.update_layout(title=go.layout.Title(text=chart_title), hovermode='closest', geo = dict(
       scope = 'usa',
       showland = True,
       showrivers = True,
       showlakes = True,
       showocean = True,
       showcoastlines = True,
       center = dict(lon = user_lon, lat = user_lat),
       lonaxis = dict(range = [min_lon, max_lon]),
       lataxis = dict(range = [min_lat, max_lat]),
    )
    )
    return fig


app.layout = html.Div([
    html.Div([
        # a text input box for address 
        html.Div(
            [dcc.Input(
                id = 'address-input', 
                type = 'text',
                placeholder = 'Address',
                debounce = True
            )]
        ),
        # a drop-down menu with years 
        html.Div(
            [dcc.Dropdown(
                id = 'yaxis-column', 
                options=[{'label':i, 'value':i} for i in years_available],
                value='2018'
            )]
        ),
        # the month-temperature scatter plot 
        html.Div( 
            [dcc.Graph(id='temp-graph')]
        ),
        # the temperature geo plot 
        html.Div( 
            [dcc.Graph(id='temp-geo-scatter')]
        ),
        # the ozone-temperature scatter plot 
        html.Div(
            [dcc.Graph(id='ozone-graph')],
        ),
        # the ozone annual geographic scatter plot 
        html.Div(
            [dcc.Graph(id='ozone-geo-scatter')],
        ),
        # the pm 2.5 scatter plot 
        html.Div(
            [dcc.Graph(id='pm25-graph')],
        ),
        # the pm 2.5 geo plot 
        html.Div(
            [dcc.Graph(id='pm25-geo-scatter')],
        ),
        # the no2 scatter plot 
        html.Div(
            [dcc.Graph(id='no2-graph')],
        ),
        # the no2 geo plot 
        html.Div(
            [dcc.Graph(id='no2-geo-scatter')],
        ),
            ],
        ),
    ],
    id="mainContainer",
)

user_lat = 0
user_lon = 0

# update fig on year/temp selection 

@app.callback(
    [Output('temp-graph', 'figure'),Output('temp-geo-scatter', 'figure'),Output('ozone-graph', 'figure'), Output('ozone-geo-scatter', 'figure'), Output('pm25-graph', 'figure'), Output('pm25-geo-scatter', 'figure'), Output('no2-graph', 'figure'), Output('no2-geo-scatter', 'figure')],
    [Input('yaxis-column', 'value'), Input('address-input', 'value')])

def make_main_figure(year, address): 
    if year is None:
        raise PreventUpdate
    if address is None: 
        raise PreventUpdate

    # process address input by user, get latitude and longitude 
    print('user address was '+address)
    api_key = '32d83f5a52324a0895dada1099b0f2a5'
    geolocator = OpenCage(api_key, domain='api.opencagedata.com', scheme=None, user_agent='AQMap', format_string=None, timeout=4)
    location = geolocator.geocode(address)

    user_lat = location.latitude
    user_lon = location.longitude

    # get  necessary dataframes from database 
    ozone_df = get_pollutant_annual_df(year, 'ozone', location.latitude, location.longitude)
    ozone_geo_df = get_pollutant_annual_avg_df(year, 'ozone', location.latitude, location.longitude, 10) 
    print('got ozone df')
    pm25_df = get_pollutant_annual_df(year, 'pm25', location.latitude, location.longitude)
    pm25_geo_df = get_pollutant_annual_avg_df(year, 'pm25', location.latitude, location.longitude, 10)
    print('got pm25 df')
    no2_df = get_pollutant_annual_df(year, 'no2', location.latitude, location.longitude)
    no2_geo_df = get_pollutant_annual_avg_df(year, 'no2', location.latitude, location.longitude, 10)
    print('got no2 df')
    temp_df = get_temp_annual_df(year, location.latitude, location.longitude)
    temp_geo_df = get_temp_annual_avg_df(year, location.latitude, location.longitude, 10)
    print('got temp df')

    # making the ozone geo plots
    ozonefig = make_scatter(ozone_df, 'slateblue', 'Month', 'Concentration (ppm)', 'Ozone', 'month', 'avg')
    ozonegeo = make_geo(ozone_geo_df, 'Mean Annual Ozone', 'st_x', 'st_y', 'avg', user_lon, user_lat)
    # making the pm2.5 plots 
    pm25fig = make_scatter(pm25_df, 'seagreen', 'Month', 'Concentration (μg/m^3)', 'PM2.5','month','avg')
    pm25geo = make_geo(pm25_geo_df, 'Mean Annual PM2.5', 'st_x', 'st_y', 'avg', user_lon, user_lat)
    # making the no2 plots
    no2fig = make_scatter(no2_df, 'goldenrod', 'Month', 'Concentration (ppb)', 'Nitrogen Dioxide','month','avg')
    no2geo = make_geo(no2_geo_df, 'Mean Annual Nitrogen Dioxide', 'st_x', 'st_y', 'avg', user_lon, user_lat)
    # making the temp plots 
    tempfig = make_scatter(temp_df, 'tomato', 'Month', '˚C', 'Maximum Temperature', 'month', 'tmax')
    tempgeo = make_geo(temp_geo_df, 'Mean Annual Temperature', 'st_x', 'st_y', 'tmax', user_lon, user_lat)

    return tempfig, tempgeo, ozonefig, ozonegeo, pm25fig, pm25geo, no2fig, no2geo

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port='8080', debug=True)
