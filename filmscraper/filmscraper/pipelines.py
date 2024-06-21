# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re
import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()


class FilmscraperPipeline:
    def process_item(self, item, spider):
        item = self.clean_title(item) # clean title
        item = self.clean_langues(item)
        # item = self.clean_actors(item)
        item = self.clean_duree(item)
        return item
    
    def clean_title(self, item):
        adapter = ItemAdapter(item)
        title = adapter.get('title')
        adapter['title'] = title.strip()
        return item
    
    def clean_langues(self, item):
        adapter = ItemAdapter(item)
        langues = adapter.get('langues')
        adapter['langues'] = langues.strip().replace('-', 'Null')
        return item
    
    # def clean_actors(self, item):
    #     adapter = ItemAdapter(item)
    #     actors = adapter.get('casting')
    #     adapter['casting'] = actors[1:]
    #     return item
    
    # def clean_real(self, item):
    #     adapter = ItemAdapter(item)
    #     real = adapter.get('real', 'Null')
    #     else:
            
    #         adapter['real'] = "No Actors"
    
    
    def clean_duree(self, item):
        adapter = ItemAdapter(item)
        duree = adapter.get('duree')
        duree_cleaned = duree.strip().replace(' ', '')
        adapter['duree'] = duree_cleaned
        return item

class MysqlDemoPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host = os.getenv("HOST"),
            port=os.getenv("PORT"),
            user = os.getenv("USER_NAME"),
            password = os.getenv("MYSAL_PASSWORD"),
            database = os.getenv("DATA_BASE_NAME")
        )
        self.cur = self.conn.cursor()
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS films(
            id int NOT NULL auto_increment, 
            title text,
            date text,
            PRIMARY KEY (id)
        )
        """)

    def process_item(self, item, spider):

        self.cur.execute(""" insert into films (title, date) values (%s,%s)""", (
            item["title"],
            str(item["date"])
        ))

        self.conn.commit()
        
        return item


    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
     