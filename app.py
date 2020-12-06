import os
import pandas as pd
import etl
import time
import psycopg2
import RDS_config
import features
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import predict
from google.cloud import automl_v1beta1 as automl
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

connection = psycopg2.connect(host = RDS_config.host,
                                  port = RDS_config.port,
                                  user = RDS_config.user,
                                  password = RDS_config.password,
                                  dbname= RDS_config.dbname)

client = automl.TablesClient(project='msds-434-final-project')

df = etl.getPushShiftData(subreddit = 'borrow', size = '200')
df['created_utc'] = df.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x['created_utc'])), axis=1)
table = df.loc[(df['title'].str.contains("[REQ]")) & (df['selftext'] != '[removed]'), ['author', 'title', 'full_link', 'created_utc']]
table = table.head(15)
table['Default Likelihood'] = None
features = features.get_feats(connection = connection, df = table)

app = dash.Dash(__name__)
app.title = "BorrowerAnalysis"

app.layout = html.Div([
    html.H1("Reddit Borrower Analysis", style = {'text-align': 'center'}),
    html.Br(),
    html.Button('Generate Default Likelihood', 
                id='submit-val', 
                n_clicks = 0,
                style = {'text-align': 'center', 'verticalAlign': 'middle', 'display': 'inline'}),
    html.Br(),html.Br(),
    html.Div(
        dcc.Loading(id = 'results_table_load', 
                    type = 'circle',
                    children = [
                                dash_table.DataTable(
                                    id='results_table', 
                                    data = table.to_dict('records'),
                                    columns = [{"name": 'Default Likelihood', "id": 'Default Likelihood'},
                                            {"name": 'Borrower', "id": 'author'},
                                            {"name": 'Post Title', "id": 'title'},
                                            {"name": 'Link', "id": 'full_link', 'presentation':'markdown'},
                                            {"name": 'Create Date UTC', "id": 'created_utc'}],
                                    style_cell = {'whitespace': 'normal',
                                                'height': 'auto'}
                                )
                    ])
    )

])

@app.callback(
    [Output(component_id = 'results_table', component_property = 'data'),
    Output(component_id = 'results_table', component_property = 'columns')],
    [Input(component_id = 'submit-val', component_property = 'n_clicks')])
def update_table(n_clicks):
    if n_clicks == 0:
        raise PreventUpdate
    else:
        predictions = predict.get_predictions(client = client, features_df = features)
        results_table = pd.merge(predictions, table, on='author')
        results_table = results_table[['Default Likelihood_x', 'author', 'title', 'full_link', 'created_utc']]
        columns = [ {"name": 'Default Likelihood', "id": 'Default Likelihood_x'},
                    {"name": 'Borrower', "id": 'author'},
                    {"name": 'Post Title', "id": 'title'},
                    {"name": 'Link', "id": 'full_link', 'presentation':'markdown'},
                    {"name": 'Create Date UTC', "id": 'created_utc'}]
        return(results_table.to_dict('records'), columns)

if __name__ == '__main__':
    app.run_server(debug = True)
    