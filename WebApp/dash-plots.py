import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from geopy.geocoders import OpenCage

import pandas as pd
import plotly.graph_objs as go

from geocode_access import get_geocode_keys
import query_dataframes as qd 

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# get API keys for OpenCage and MapBox
opencage_key, mapbox_key = get_geocode_keys('geocode.ini', 'geocode_keys')

# initialize cache for user addresses 
ADDRESS_CACHE = {}

# what data is available? 
years_available = list()
for year in range(2000, 2019): 
    years_available.append(str(year))

# make a scatter plot with month on the x axis and concentration/temp on the y axis 
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

# make a Mapbox scatter plot based on (lat, long, value)
def make_geo_mapbox(df, chart_title, lonname, latname, textname, user_lon, user_lat):
    fig = go.Figure()
    # add database points
    fig.add_trace(
        go.Scattermapbox(
               lon = df[lonname],
               lat = df[latname],
               text = df[textname],
               name = '',
               showlegend = False,
               mode='markers',
               marker = go.scattermapbox.Marker(
                   color = df[textname],
                   size = 12,
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
        go.Scattermapbox(
               lon = [user_lon],
               lat = [user_lat],
               text = ['Input Address'],
               name = '',
               showlegend = False,
               mode='markers',
               marker = go.scattermapbox.Marker(
                        showscale = False,
                        # color is only available for circle markers
                        size = 13,
                        symbol = 'star',
                        colorbar_title = ''
                   )
        )
    )
    fig.update_layout(title=go.layout.Title(text=chart_title), hovermode='closest', mapbox = dict(
       accesstoken=mapbox_key,
       center = go.layout.mapbox.Center(lon = user_lon, lat = user_lat),
       zoom=7
        )
    )
    return fig

# set layout for webpage here. Must include all elements 
app.layout = html.Div([
    html.Div([
        # the header 
        html.H1(
            '''
            AQMap: Explore Your Atmosphere
            '''
        ),
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
        html.Div([
            html.Div( 
                [dcc.Graph(id='temp-graph')],
                className = 'six columns'

            ),
            # the temperature geo plot 
            html.Div( 
                [dcc.Graph(id='temp-geo-scatter')],
                className = 'six columns'
            ),
        ], className = "row"),
        html.Div([
        # the ozone scatter plot 
            html.Div(
                [dcc.Graph(id='ozone-graph')],
                className = 'six columns'
            ),
            # the ozone annual geographic scatter plot 
            html.Div(
                [dcc.Graph(id='ozone-geo-scatter')],
                className = 'six columns'
            ),
        ], className = "row"),
        html.Div([
            # the pm 2.5 scatter plot 
            html.Div(
                [dcc.Graph(id='pm25-graph')],
                className = 'six columns'
            ),
            # the pm 2.5 geo plot 
            html.Div(
                [dcc.Graph(id='pm25-geo-scatter')],
                className = 'six columns'
            ),
        ], className = "row"),
        html.Div([
            # the no2 scatter plot 
            html.Div(
                [dcc.Graph(id='no2-graph')],
                className = 'six columns'
            ),
            # the no2 geo plot 
            html.Div(
                [dcc.Graph(id='no2-geo-scatter')],
                className = 'six columns'
            ),
        ], className = "row"),
        ],
        ),
    ],
    id="mainContainer",
)

user_lat = 0
user_lon = 0

# update figs on year/temp selection 

@app.callback(
    [Output('temp-graph', 'figure'),Output('temp-geo-scatter', 'figure'),Output('ozone-graph', 'figure'), Output('ozone-geo-scatter', 'figure'), Output('pm25-graph', 'figure'), Output('pm25-geo-scatter', 'figure'), Output('no2-graph', 'figure'), Output('no2-geo-scatter', 'figure')],
    [Input('yaxis-column', 'value'), Input('address-input', 'value')])

def make_main_figure(year, address): 

    if year is None:
        raise PreventUpdate
    if address is None: 
        raise PreventUpdate

    # process address input by user, get latitude and longitude 
    if address not in ADDRESS_CACHE:
        print('new address, making call to geolocator')
        api_key = opencage_key
        geolocator = OpenCage(api_key, domain='api.opencagedata.com', scheme=None, user_agent='AQMap', format_string=None, timeout=4)
        ADDRESS_CACHE[address] = geolocator.geocode(address)
    location = ADDRESS_CACHE[address]
    user_lat = location.latitude
    user_lon = location.longitude
    # get  necessary dataframes from PostGIS database 
    ozone_df = qd.get_pollutant_annual_df(year, 'ozone', location.latitude, location.longitude)
    ozone_geo_df = qd.get_pollutant_annual_avg_df(year, 'ozone', location.latitude, location.longitude, 10) 
    print('got ozone df')
    pm25_df = qd.get_pollutant_annual_df(year, 'pm25', location.latitude, location.longitude)
    pm25_geo_df = qd.get_pollutant_annual_avg_df(year, 'pm25', location.latitude, location.longitude, 10)
    print('got pm25 df')
    no2_df = qd.get_pollutant_annual_df(year, 'no2', location.latitude, location.longitude)
    no2_geo_df = qd.get_pollutant_annual_avg_df(year, 'no2', location.latitude, location.longitude, 10)
    print('got no2 df')
    temp_df = qd.get_temp_annual_df(year, location.latitude, location.longitude)
    temp_geo_df = qd.get_temp_annual_avg_df(year, location.latitude, location.longitude, 10)
    print('got temp df')

    # making the ozone geo plots
    ozonefig = make_scatter(ozone_df, 'slateblue', 'Month', 'Concentration (ppm)', 'Ozone', 'month', 'conc')
    ozonegeo = make_geo_mapbox(ozone_geo_df, 'Mean Annual Ozone', 'st_x', 'st_y', 'conc', user_lon, user_lat)
    # making the pm2.5 plots 
    pm25fig = make_scatter(pm25_df, 'seagreen', 'Month', 'Concentration (μg/m^3)', 'PM2.5','month','conc')
    pm25geo = make_geo_mapbox(pm25_geo_df, 'Mean Annual PM2.5', 'st_x', 'st_y', 'conc', user_lon, user_lat)
    # making the no2 plots
    no2fig = make_scatter(no2_df, 'goldenrod', 'Month', 'Concentration (ppb)', 'Nitrogen Dioxide','month','conc')
    no2geo = make_geo_mapbox(no2_geo_df, 'Mean Annual Nitrogen Dioxide', 'st_x', 'st_y', 'conc', user_lon, user_lat)
    # making the temp plots 
    tempfig = make_scatter(temp_df, 'tomato', 'Month', '˚C', 'Maximum Temperature', 'month', 'tmax')
    tempgeo = make_geo_mapbox(temp_geo_df, 'Mean Annual Temperature', 'st_x', 'st_y', 'tmax', user_lon, user_lat)

    return tempfig, tempgeo, ozonefig, ozonegeo, pm25fig, pm25geo, no2fig, no2geo

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port='8080', debug=True)
