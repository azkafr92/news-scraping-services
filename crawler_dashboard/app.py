# run scrapyrt on web_crawler folder (port 5000)  ->  scrapyrt -p 5000
# this is to run scrapy by requests.get on this script
# url format : http://localhost:<port>/crawl.json?spider_name=<spider name>&url=<url>
# also, on crawler_dashboard folder, run  ->  python getDataset.py
# this is the API to get dataset from database
# then, run the app on the same folder
# python app.py
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import io
import csv
import xml.etree.ElementTree as ET
from flask import Flask,jsonify,Response,send_file,request
import requests
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import datetime as dt
import urllib.parse
from get_dataset import get_dataset

server = Flask(__name__)
app = dash.Dash(__name__,
                meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
                external_stylesheets=[dbc.themes.BOOTSTRAP],
                server=server)
app.config['suppress_callback_exceptions']=True

website_and_channel_options = {
    'detik':['news','finance','inet','oto','sport','hot','wolipop','health','food','travel'],
    'kompas':['news','tren','hype','money','bola','tekno','sains','otomatif','lifestyle','properti','travel','edukasi','jeo','health','skola']
}
base_url_format = {
    'detik': 'https://{channel}.detik.com/indeks?date={month}/{day}/{year}',
    'kompas': 'https://{channel}.kompas.com/search/{year}-{month}-{day}/1'
}
#=================#
# config database #
#=================#
database = 'crawler_db'
user = 'postgres'
password = 'password'
host = 'localhost'
port = 5434

#--------------------#
# build display      #
# for main dashboard #
#--------------------#
def website_options():
    _options = dbc.InputGroup([
        dbc.InputGroupAddon('Website',addon_type='prepend',style={'width':'100%'}),
        dbc.Select(
            id='website',
            options=[{'label':i+'.com','value':i} for i in website_and_channel_options],
            value='detik'
        )
    ],size='lg')
    return _options

def channel_options():
    _options = dbc.InputGroup([
        dbc.InputGroupAddon('Channel',addon_type='prepend',style={'width':'100%'}),
        dbc.Select(
            id='channel',
            options=[]
        )
    ],size='lg')
    return _options

def date_picker():
    max_date_allowed = dt.datetime.now().strftime('%Y-%m-%d')
    _calendar = dbc.InputGroup([
        dbc.InputGroupAddon('Date',addon_type='prepend',style={'width':'100%'}),
        html.Br(),
        dcc.DatePickerSingle(
            id='calendar',
            min_date_allowed=dt.datetime(2020,1,1),
            max_date_allowed=max_date_allowed,
            initial_visible_month=dt.datetime.now(),
            display_format='DD MMMM YYYY',
            placeholder='Select date...'
        )
    ],size='lg')
    return _calendar

def token_box():
    box = dbc.InputGroup([
        dbc.InputGroupAddon('Token',addon_type='prepend',style={'width':'100%'}),
        html.Br(),
        dbc.Input(id='token',placeholder='input your token here...',type='text')
    ])
    return box

def cutted_text(text):
    if '</p>' in text:
        try:
            first_paragraph = text.index('</p>')+4
            text = text[:first_paragraph]
            text = text.replace('<br>',' ')
            text = ''.join(ET.fromstring(text).itertext())
        except:
            pass

    return text+'..'

def display_dataset(dataset,channel,website,_date):
    param = urllib.parse.urlencode({'channel':channel,'website':website,'date':_date})
    href = 'http://127.0.0.1:5002/csv?{}'.format(param)
    card_content = [
        dbc.Card(
        ([
            dbc.CardHeader(html.A(html.H5(data['title'],className='card-title'),href=data['link'])),
            dbc.CardBody(
                [
                    html.P(cutted_text(data['text'])),
                    html.Small(data['published'])
                ]
        )]))for data in dataset]
    return [dbc.CardColumns(card_content),
            html.Br(),
            html.A(id='download',children='Download Result',href=href)]

def check_token(token):
    if token == 'netmarks':
        return True
    return False
#-----------------#
# main app layout #
#-----------------#
app.layout = html.Div(
    children=[
        html.Div(
            style={ 'margin':'0 auto','width':'30em'},
            children=[
                dbc.Card(id='main',children=[
                    dbc.CardBody([
                        html.H1('Web Scraping Service',className='card-title'),
                        html.P('Select website you want to scrape'),
                        website_options(),html.Br(),
                        channel_options(),html.Br(),
                        date_picker(),html.Br(),
                        token_box(),html.Br(),
                        dbc.Spinner(id='loader',children=[dbc.Button(id='submit',children='Start scraping')],className='inline'),html.Br(),
                        html.Small('Process may take a few minutes, depending on how many url(s) are found. Do not refresh your browser until scraping done.')
                    ])],className='w-100 visible'),
                html.Div(id='result')]),
        html.Div(id='display-text',className='hidden'),
        html.Div(id='download-status',className='none')
    ]
)

#----------------#
# callback build #
#----------------#
@app.callback(
    Output('channel','options'),
    [Input('website','value')]
)
def set_channel_options(selected_website):
    return [{'label':i,'value':i} for i in website_and_channel_options[selected_website]]

@app.callback(
    [Output('result','children'),
     Output('result','className'),
     Output('loader','children')],
    [Input('submit','n_clicks')],
    [State('website','value'),
     State('channel','value'),
     State('calendar','date'),
     State('token','value')]
)
def display_result(n1,website,channel,_date,token):
    is_token_correct = check_token(token)
    if ((channel!=None)&(is_token_correct)):
        year,month,day = _date.split('-')
        _date_ = '/'.join([month,day,year])
        
        base_url = base_url_format[website].format(channel=channel,year=year,month=month,day=day)
        
        requests.get('http://localhost:9080/crawl.json?spider_name={spider_name}&url={url}'.format(spider_name=website,url=base_url))
        
        return [
            [html.P(children=[
                'You are choosing to scraping ',
                html.B('{channel}.{website}.com'.format(channel=channel,website=website)),
                ' on ',
                html.B('{_date}. '.format(_date=_date_)),
                'Scraping process is success. ',
                n1, ' click(s)'
            ]),
             dbc.Spinner(children=[dbc.Button(id='display',children='Display result')],className='inline'),html.Br()],
            'visible',
            dbc.Button(id='submit',children='Start scraping')
        ]
        
    return [
        None,
        'visible',
        dbc.Button(id='submit',children='Start scraping')]

# build script: check if html.Div children is not null,
# then call getDataset function, display the result on same/different page
# truncate the long text string (using textwrap.shorten)

@app.callback(
    [Output('display-text','children'),
     Output('display-text','className'),
     Output('display','className')],
    [Input('display','n_clicks')],
    [State('website','value'),
     State('channel','value'),
     State('calendar','date')]
)
def display_text(n1,website,channel,_date):
    dataset = get_dataset(channel,website,_date)
    return [
        display_dataset(dataset,channel,website,_date),
        'visible',
        'none'
    ]

if __name__ == "__main__":
    app.run_server(debug=True,port=5001)
