import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from databaseConnection import get_connection_by_config

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

#df = pd.read_csv(
#    'https://gist.githubusercontent.com/chriddyp/' +
#    '5d1ea79569ed194d432e56108a04d188/raw/' +
#    'a9f9e8076b837d541398e999dcbac2b2826a81f8/'+
#    'gdp-life-exp-2007.csv')

newConnection = get_connection_by_config('database.ini', 'postgresql_conn_data')

query = """
    SELECT distinct on (ozone_2000.obsdate) ozone_2000.obsdate, ozone_2000.measurement,
    ST_Distance(
    ozone_2000.geogcol,
    ST_MakePoint(-73.9442,40.6782)::geography
    ) AS distance
    FROM
    ozone_2000
    ORDER BY
    ozone_2000.obsdate, ozone_2000.geogcol <->
    ST_MakePoint(-73.9442,40.6782)::geography ;
    """
ozone_df = pd.read_sql(query, newConnection)

app.layout = html.Div([
    dcc.Graph(
        id='life-exp-vs-gdp',
        figure={
            'data': [
                go.Scatter(
                    x=ozone_df['obsdate'],
                    y=ozone_df['measurement'],
                    #text=ozone_df['measurement'],
                    mode='markers',
                    opacity=0.7,
                    marker={
                        'size': 15,
                        'line': {'width': 0.5, 'color': 'white'}
                    }
                ) 
            ],
            'layout': go.Layout(
                xaxis={'title': 'Date'},
                yaxis={'title': 'Ozone Concentration (ppm)'},
              #  margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
              #  legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }
    )
])

newConnection.close()

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port='8080')
