import psycopg2
from psycopg2 import sql
import hashlib

#=================#
# helper function #
#=================#
def create_table_if_not_exists(conn,table_name):
    with conn:
        cur = conn.cursor()

        query = """
            CREATE TABLE IF NOT EXISTS {} (
            id_ VARCHAR (255) PRIMARY KEY,
            title VARCHAR (255) NOT NULL,
            summary TEXT,
            link VARCHAR (255) NOT NULL,
            published TIMESTAMPTZ NOT NULL,
            text TEXT
            );
        """.format(table_name)
        cur.execute(query)

        conn.commit()
    
    #print('creating {} table success'.format(table_name))

class WebCrawlerPipeline(object):
    def open_spider(self,spider):
        database = 'crawler_db'
        user = 'pi'
        password = 'password'
        host = 'localhost'
        port = 5434
        self.connection = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        self.cur = self.connection.cursor()

    def close_spider(self,spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        create_table_if_not_exists = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
            id_ VARCHAR (255),
            title VARCHAR (255) NOT NULL,
            link VARCHAR (255) PRIMARY KEY,
            channel VARCHAR (255) NOT NULL,
            published TIMESTAMPTZ NOT NULL,
            text TEXT
            );
        """).format(sql.Identifier(item['table_name']))

        self.cur.execute(create_table_if_not_exists)
        
        self.cur.execute(
            sql.SQL("""INSERT INTO {}(id_,title,link,channel,published,text) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;""").format(sql.Identifier(item['table_name'])),
            (item['id_'],item['title'],item['link'],item['channel'],item['published'],item['text'])
            )
        self.connection.commit()
        return item