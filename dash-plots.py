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
                value='2000'
            )]
        ),
        # the month-temperature scatter plot 
        html.Div( 
            [dcc.Graph(id='temp-graph')]
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
        # the no2 scatter plot 
        html.Div(
            [dcc.Graph(id='no2-graph')],
        ),
            ],
        ),
    ],
    id="mainContainer",
)

@app.callback(
    [Output('temp-graph', 'figure'),Output('ozone-graph', 'figure'), Output('ozone-geo-scatter', 'figure'), Output('pm25-graph', 'figure'), Output('no2-graph', 'figure')],
    [Input('yaxis-column', 'value'), Input('address-input', 'value')])

def make_main_figure(year, address): 
    if year is None:
        raise PreventUpdate
    if address is None: 
        raise PreventUpdate

    # process address input by user, get latitude and longitude 
    print('user address was '+address)
    api_key = '32d83f5a52324a0895dada1099b0f2a5'
    geolocator = OpenCage(api_key, domain='api.opencagedata.com', scheme=None, user_agent='AQMap', format_string=None)
    location = geolocator.geocode(address)

    # get  necessary dataframes from database 
    ozone_df = get_pollutant_annual_df(year, 'ozone', location.latitude, location.longitude)
    ozone_geo_df = get_pollutant_annual_avg_df(year, 'ozone', location.latitude, location.longitude, 10) 
    print('got ozone df')
    pm25_df = get_pollutant_annual_df(year, 'pm25', location.latitude, location.longitude)
    print('got pm25 df')
    no2_df = get_pollutant_annual_df(year, 'no2', location.latitude, location.longitude)
    print('got no2 df')
    temp_df = get_temp_annual_df(year, location.latitude, location.longitude)
    print('got temp df')
    ticknums = [1,2,3,4,5,6,7,8,9,10,11,12]
    tickmonths = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

    # making the ozone fig
    ozonefig = go.Figure()
    ozonefig.add_trace(
        go.Scatter(x=ozone_df['month'],y=ozone_df['avg'],
            mode='markers',
            marker={
                'size': 15, 
                'opacity': 0.8,
                'color': 'slateblue',
                'line': {'width': 0.5, 'color': 'white'}
              }
        )
    )
    ozonefig.update_layout(xaxis = go.layout.XAxis(tickmode = 'array', tickvals = ticknums, ticktext = tickmonths, title = 'Month'), yaxis = go.layout.YAxis(title = 'Concentration (ppm)'), title=go.layout.Title(text='Ozone'), hovermode='closest')

    # making the ozone geo scatter
    ozonegeo = go.Figure() 
    ozonegeo.add_trace(
        go.Scattergeo(
               lon = ozone_geo_df['st_x'],
               lat = ozone_geo_df['st_y'],
               text = ozone_geo_df['avg'], 
               mode='markers',
               marker_color = ozone_geo_df['avg']
        )
    )
    ozonegeo.update_layout(title=go.layout.Title(text='Mean Annual Ozone'), hovermode='closest', geo = dict(
       scope = 'usa', 
       center = dict(lon = location.longitude, lat = location.latitude),
    )
    )
    # making the pm2.5 fig 
    pm25fig = go.Figure()
    pm25fig.add_trace(
        go.Scatter(x=pm25_df['month'],y=pm25_df['avg'],
            mode='markers',
            marker={
                'size': 15, 
                'opacity': 0.8,
                'color': 'seagreen',
                'line': {'width': 0.5, 'color': 'white'}
              }
        )
    )
    pm25fig.update_layout(xaxis = go.layout.XAxis(tickmode = 'array', tickvals = ticknums, ticktext = tickmonths, title = 'Month'), yaxis = go.layout.YAxis(title = 'Concentration (μg/m^3)'), title=go.layout.Title(text='PM2.5'), hovermode='closest')
    # making the no2 fig
    no2fig = go.Figure()
    no2fig.add_trace(
        go.Scatter(x=no2_df['month'],y=no2_df['avg'],
            mode='markers',
            marker={
                'size': 15, 
                'opacity': 0.8,
                'color': 'goldenrod',
                'line': {'width': 0.5, 'color': 'white'}
              }
        )
    )
    no2fig.update_layout(xaxis = go.layout.XAxis(tickmode = 'array', tickvals = ticknums, ticktext = tickmonths, title = 'Month'), yaxis = go.layout.YAxis(title = 'Concentration (ppb)'), title=go.layout.Title(text='Nitrogen Dioxide'), hovermode='closest')
    # making the temp fig 
    tempfig = go.Figure()
    tempfig.add_trace(
            go.Scatter(x=temp_df['month'],y=temp_df['tmax'],
            mode='markers',
            marker={
                'size': 15,                    
                'opacity': 0.8,
                'color': 'tomato',
                'line': {'width': 0.5, 'color': 'white'}
              }
        )
    )
    #tempfig.update_layout(title=go.layout.Title(text='Maximum Temperature'),xaxis={'title': 'Month'},yaxis={'title': '˚C'},hovermode='closest')
    tempfig.update_layout(xaxis = go.layout.XAxis(tickmode = 'array', tickvals = ticknums, ticktext = tickmonths, title = 'Month'), yaxis = go.layout.YAxis(title = '˚C'), title=go.layout.Title(text='Maximum Temperature'), hovermode='closest')
    return tempfig, ozonefig, ozonegeo, pm25fig, no2fig

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port='8080', debug=True)
