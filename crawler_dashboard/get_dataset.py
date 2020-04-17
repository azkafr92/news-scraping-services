import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request, send_file
import json
import io
import csv

app = Flask(__name__)

DEBUG = False
#=================#
# config database #
#=================#
database = 'crawler_db'
user = 'postgres'
password = 'password'
host = 'localhost'
port = 5434
conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)

def datetime_to_string(data):
    
    data['published'] = data['published'].strftime('%A, %d %B %Y %H:%M')
    return data

def get_dataset(channel,website,_date):
    #year,month,day = _date.split('-')
    #_date = '-'.join([year,month,day])
    start = _date + ' 00:00:00+07'
    end = _date + ' 23:59:59+07'
    #channel = '%'+channel+'%'
    
    with conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)    
        
        cur.execute(
            sql.SQL('''
                SELECT title,link,text,published FROM {}
                WHERE channel LIKE %s
                AND published BETWEEN %s AND %s
                ORDER BY published DESC LIMIT 100;
            ''').format(sql.Identifier(website)),
            (channel,start,end))
        result = cur.fetchall()
        result = list(map(datetime_to_string,result))
        #result = json.dumps(result)

        conn.commit()
        cur.close()
        
        return result
@app.route('/')
def home():
    channel = request.args.get('channel')
    website = request.args.get('website')
    _date = request.args.get('date')
    
    if not any([channel,website,_date]):
        return jsonify({'status':'failed','message':'Please check your param(s)'}),400
    
    result = get_dataset(channel=channel,website=website,_date=_date)
    
    return jsonify(result)

@app.route('/csv')
# there is some issue when downloading csv from chrome browser
# you must clean histories,cookies,cache,etc to get updated result
# otherwise, you only get what you downloaded at first time
# use newest version of mozilla firefox to get the best result
def download_result():
    channel = request.args.get('channel')
    website = request.args.get('website')
    _date = request.args.get('date')
    
    result = get_dataset(channel=channel,website=website,_date=_date)
    
    str_io = io.StringIO()
    writer = csv.writer(str_io)
    mem = io.BytesIO()
    
    line = ['title','link','text','published']
    writer.writerow(line)
    
    for data in result:
        line = [str(data['title']),str(data['link']),str(data['text']),str(data['published'])]
        writer.writerow(line)
    
    mem.write(str_io.getvalue().encode('utf-8'))
    mem.seek(0)
    
    str_io.close()
    
    return send_file(mem,
                     mimetype='text/csv',
                     as_attachment=True,
                     attachment_filename='result.csv') 
    

if __name__ == "__main__":
    #print(get_dataset('finance','detik','04/03/2020'))
    app.run(host='0.0.0.0',port=5002,debug=True)