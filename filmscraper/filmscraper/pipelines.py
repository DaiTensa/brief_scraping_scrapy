# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re
import mysql.connector
import os
import datetime
from dotenv import load_dotenv
load_dotenv()


class FilmscraperPipeline:
    def process_item(self, item, spider):
        item = self.clean_langues(item)
        # item = self.clean_actors(item)
        # item = self.clean_duree(item)
        item = self.clean_time(item)
        return item
    
    def clean_langues(self, item):
        adapter = ItemAdapter(item)
        langues = adapter.get('Langues')
        adapter['Langues'] = langues.strip().replace('-', 'Null')
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
        duree = adapter.get('Duree')
        duree_cleaned = duree.strip().replace(' ', '')
        adapter['Duree'] = duree_cleaned
        return item
    
    def clean_time(self, item):
        adapter = ItemAdapter(item)
        time = adapter.get("Time")
        time = datetime.datetime.fromtimestamp(time)
        adapter["Time"] = time.strftime('%Y-%m-%d %H:%M:%S')
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
        CREATE TABLE IF NOT EXISTS Films(
            IdFilm int(10) UNSIGNED NOT NULL AUTO_INCREMENT, 
            Title varchar(255),
            TitleOrigine varchar(255),
            Synopsis text,
            Date text,
            Duree text,
            Directors text,
            Actors text,
            Public text,
            Langues text,
            Annee text,
            Type text,
            Genre text,
            NoteUser text,
            UserNoteCount text,
            NotePress text,
            PressNoteCount text,
            CumulBoxOffice TIMESTAMP NOT NULL,
            Time text,
            PRIMARY KEY (`IdFilm`) 
        ) AUTO_INCREMENT=100 DEFAULT CHARSET=utf8;
        """)

    def process_item(self, item, spider):

        self.cur.execute(""" insert into Films (
                        Title,
                        TitleOrigine,
                        Synopsis,
                        Date,
                        Duree,
                        Directors,
                        Actors,
                        Public,
                        Langues,
                        Annee,
                        Type,
                        Genre,
                        NoteUser,
                        UserNoteCount,
                        NotePress,
                        PressNoteCount,
                        CumulBoxOffice,
                        Time
                         ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
            item["Title"],
            item["TitleOrigine"],
            item["Synopsis"],
            item["Date"],
            item["Duree"],
            item["Directors"],
            item["Actors"],
            item["Public"],
            item["Langues"],
            item["Annee"],
            item["Type"],
            item["Genre"],
            item["NoteUser"],
            item["UserNoteCount"],
            item["NotePress"],
            item["PressNoteCount"],
            item["CumulBoxOffice"],
            item["Time"]
        ))

        self.conn.commit()

        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
     