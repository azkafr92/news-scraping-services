# run python .\web_crawler\spiders\crawlers.py on folder D:\netmarks-project\rss-parser\web_crawler
# use rss-reader-parser-dev environment
import traceback
import datetime as dt
import hashlib
import logging
logging.basicConfig(filename='info.log',filemode='a',format='%(name)s - %(asctime)s - %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
import sys
sys.path.append('./')

import scrapy
#==============================================#
# Running multiple spiders in the same process #
#==============================================#
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings
import urllib.parse
from web_crawler.items import WebCrawlerItem


#=================#
# config database #
#=================#
import psycopg2
from psycopg2 import sql
database = 'crawler_db'
user = 'postgres'
password = 'password'
host = 'localhost'
port = 5434
conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
create_table_if_not_exists = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
            id_ VARCHAR (255),
            title VARCHAR (255) NOT NULL,
            link VARCHAR (255) PRIMARY KEY,
            channel VARCHAR (255) NOT NULL,
            published TIMESTAMPTZ NOT NULL,
            text TEXT
            );
        """)
#=================#
# helper function #
#=================#
def get_channel_website_and_date(url,month,day,year):
    current_url = urllib.parse.unquote(url)
    _date = '-'.join([year,month,day])
    start = _date + ' 00:00:00+07'
    end = _date + ' 23:59:59+07'
    current_url = current_url.split('/')
    current_url = [url.split('.') for url in current_url if '.' in url][0][:2]
    channel,website = current_url
    
    return channel,website,start,end

def check_if_url_exist(channel,website,start,end,url_list):
    with conn:
        cur = conn.cursor()
        cur.execute(
            sql.SQL('''
                SELECT link FROM {}
                WHERE channel LIKE %s
                AND published BETWEEN %s AND %s;
            ''').format(sql.Identifier(website)),
            (channel,start,end))
        db_url_list = cur.fetchall()
        db_url_list = set([url[0] for url in db_url_list])
        #print(db_url_list)
        url_list = url_list-db_url_list
    return url_list

def parse_helper(response,table_name,body_path,published_date,channel):
    titleText = response.css('h1::text').get().strip()

    bodyText = response.css(body_path).getall()
    bodyText = ' '.join(bodyText).strip()
    
    md5 = hashlib.md5()
    md5.update(titleText.encode('utf-8'))
    id_ = md5.hexdigest()

    item = WebCrawlerItem(
            title=titleText,
            text=bodyText,
            table_name=table_name,
            link=response.request.url,
            published=published_date,
            id_=id_,
            channel=channel
        )
    
    return item

#===============#
# build crawler #
#===============#
detik_base_url = 'https://{}.detik.com/indeks?{}'
detik_channels = ['news','finance','inet','oto','sport','hot','wolipop','health','food','travel']
date_= urllib.parse.urlencode({'date':'03/29/2020'}) # month/day/year
class DetikSpider(scrapy.Spider):
    try:
        name = 'detik'
        allowed_domain = ['detik.com']
        
        #channel_to_scrape = 'sport'
        #index_to_scrape = detik_channels.index(channel_to_scrape)
        #start_urls = [detik_base_url.format(channel,date_) for channel in detik_channels][detik_channels.index(channel_to_scrape):detik_channels.index(channel_to_scrape)+1]
        
        body_path = 'p'
        published_date_path_list = ['div.detail__date::text','div.date::text', 'span.date::text']
        published_date_format = '%d %b %Y %H:%M %z'
    except:
        traceback.print_exc()

    def parse(self,response):
        # follow links to NEWS page
        current_url = urllib.parse.unquote(response.url)
        
        _date = current_url[current_url.index('=')+1:]
        month,day,year = _date.split('/')
        channel,website,start,end = get_channel_website_and_date(current_url,month=month,day=day,year=year)
        self.channel = channel
        
        href_list = response.css('a::attr(href)').getall()

        with conn:
            cur = conn.cursor()
            cur.execute(create_table_if_not_exists.format(sql.Identifier(website)))
            cur.close()

        href_list = set([href for href in href_list if (('https://' in href)&('/d-' in href))])
        initial_href_list_number = len(href_list)
        
        href_list = list(check_if_url_exist(channel=channel,website=website,start=start,end=end,url_list=href_list))
        current_href_list_number = len(href_list)
        info = {
            'DATETIME': dt.datetime.now(),
            'CURRENT URL' : current_url,
            'TOTAL URL FOUND': initial_href_list_number,
            'ALREADY SCRAPED URL(S)': initial_href_list_number-current_href_list_number,
            'URL(S) TO SCRAPED': current_href_list_number
        }
        logging.info(f'URL INFO : {info}')

        for href in href_list:
            yield response.follow(href,self.parse_news)

        # follow pagination link
        check_if_next = 'Next' in response.css('a.pagination__item::text').getall()
        if check_if_next:
            href = response.css('a.pagination__item::attr(href)').getall()[-1]
            yield response.follow(href, self.parse)

    def parse_news(self,response):
        published_date = None
        for path in self.published_date_path_list:
            if not published_date:
                published_date = response.css(path).getall()
            else:
                break
        published_date = ''.join(published_date).title().strip()
        published_date = published_date[published_date.index(',')+1:].strip()
        published_date = published_date.replace('Wib','+0700')
        published_date = dt.datetime.strptime(published_date,self.published_date_format)
        item = parse_helper(
            response=response,
            table_name=self.name,
            body_path=self.body_path,
            published_date=published_date,
            channel=self.channel
        )
        yield item

class KompasSpider(scrapy.Spider):
    name = 'kompas'
    allowed_domain = ['kompas.com']
    body_path = 'p'

    def parse(self,response):
        # follow link to NEWS page
        # note: kompas.com has variety of sub-channel inside news channel -> nasional, megapolitan, etc
        # so, for example instead of news.kompas.com, the link will be
        # example: https://megapolitan.kompas.com/read/2020/04/12/17302811/ridwan-kamil-psbb-di-bogor-depok-bekasi-diterapkan-mulai-rabu-15-april?_ga=2.49263654.145853088.1586699202-1273254806.1586395079
        # we will keep using the main channel

        current_url = response.url
        
        _date = current_url.rsplit('/',maxsplit=2)[-2]
        start = _date + ' 00:00:00+07'
        end = _date + ' 23:59:59+07'
        
        channel,website = current_url.split('/')[2].split('.')[:2]
        
        self.channel = channel
        
        href_list =  set(response.css('a.article__link::attr(href)').getall())
        
        with conn:
            cur = conn.cursor()
            cur.execute(create_table_if_not_exists.format(sql.Identifier(website)))
            cur.close()

        href_list = list(check_if_url_exist(channel=channel,website=website,start=start,end=end,url_list=href_list))
        for href in href_list:
            yield response.follow(href,self.parse_news)
        
        # follow pagination link
        check_if_next = 'Next' in response.css('a.paging__link--next::text').getall()
        if check_if_next:
            href = response.css('a.paging__link--next::attr(href)').getall()[-1]
            yield response.follow(href, self.parse)

    def parse_news(self,response):
        published_date_format = '%d/%m/%Y, %H:%M WIB'
        published_date = response.css('.read__time::text').get().split(' - ')[-1]
        published_date = dt.datetime.strptime(published_date,published_date_format)
        
        item = parse_helper(
            response=response,
            table_name=self.name,
            body_path=self.body_path,
            published_date=published_date,
            channel=self.channel
        )
        yield item

if __name__ == "__main__":
    settings =  get_project_settings()
    settings['LOG_LEVEL'] = 'DEBUG'
    settings['ITEM_PIPELINES'] = {'web_crawler.pipelines.WebCrawlerPipeline': 300,}
    settings['LOG_FILE'] = 'monitoring.log'

    process = CrawlerProcess(settings=settings)
    
    process.crawl(DetikSpider)
    
    process.start()
