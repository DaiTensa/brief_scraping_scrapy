# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import psycopg2
from itemadapter import ItemAdapter
import re
import mysql.connector
import os
import datetime
import urllib.parse
from dotenv import load_dotenv
from scrapy.exceptions import DropItem
load_dotenv()


class FilmscraperPipeline:
    def process_item(self, item, spider):
        item = self.clean_title(item, spider)
        item = self.clean_original_title(item, spider)
        item = self.clean_synopsis(item, spider)
        item = self.clean_duree(item, spider)
        item = self.clean_directors(item, spider)
        item = self.clean_actors(item, spider)
        item = self.clean_public(item, spider)
        item = self.clean_langues(item, spider)
        item = self.clean_time(item, spider)
        item = self.clean_note_presse_count(item, spider)
        item = self.clean_note_users_count(item, spider)
        item = self.clean_cumul_box_office(item, spider)
        item = self.clean_genre(item, spider)
        item = self.clean_annee(item, spider)
        item = self.clean_note_presse(item, spider)
        item = self.clean_note_users(item, spider)
        return item
    
    def clean_title(self, item, spider):
        adapter = ItemAdapter(item)
        title = adapter.get("Title")
        if title:
            adapter["Title"] = title.strip()
        return item

    def clean_original_title(self, item, spider):
        adapter = ItemAdapter(item)
        originl_title = adapter.get("TitleOrigine")
        if originl_title:
            adapter["TitleOrigine"] = originl_title.strip()
        return item

    def clean_synopsis(self, item, spider):
        adapter = ItemAdapter(item)
        synopsis = adapter.get("Synopsis")
        if synopsis:
            adapter["Synopsis"]= synopsis.strip()
        return item  

    def clean_date(self, item, spider):
        adapter = ItemAdapter(item)
        date = adapter.get("DateSortie")
        if date:
            adapter["DateSortie"] = date.strip()
        return item
    
    def clean_annee(self, item, spider):
        adapter = ItemAdapter(item)
        annee = adapter.get("Annee")
        if annee:
            adapter["Annee"] = int(annee)
        return item

    def clean_duree(self, item, spider):
        adapter = ItemAdapter(item)
        duree = adapter.get('Duree')
        duree_en_minute = 0
        if duree:
            duree = duree.strip()
            match = re.match(r'(?:(\d+)h)?\s*(\d+)?mn?', duree)
            if match:
                heures = match.group(1)
                minutes = match.group(2)
                if heures:
                    duree_en_minute += int(heures) * 60
                if minutes:
                    duree_en_minute += int(minutes)
            adapter["Duree"] = duree_en_minute
        return item

    def clean_directors(self, item, spider):
        adapter = ItemAdapter(item)
        directors = adapter.get("Directors")
        if directors:
            adapter["Directors"] = directors.strip()
        return item

    def clean_actors(self, item, spider):
        adapter = ItemAdapter(item)
        actors = adapter.get("Actors")
        if actors:
            cleaned_actors = [actor.strip() for actor in actors]
            adapter["Actors"] = ",".join(map(str, cleaned_actors))
        return item

    def clean_genre(self, item, spider):
        adapter = ItemAdapter(item)
        genres = adapter.get("Genre")
        if genres:
            cleaned_genres = [genre.strip() for genre in genres]
            adapter["Genre"] = ",".join(map(str, cleaned_genres))
        return item

    def clean_public(self, item, spider):
        adapter = ItemAdapter(item)
        public = adapter.get("Public")
        if public:
            adapter["Public"] = public.strip()
        return item

    def clean_langues(self, item, spider):
        adapter = ItemAdapter(item)
        langues = adapter.get('Langues')
        if langues:
            adapter['Langues'] = langues.strip().replace('-', 'Null')
        return item
    
    def clean_time(self, item, spider):
        adapter = ItemAdapter(item)
        time = adapter.get("TimeItem")
        if time:
            time = datetime.datetime.fromtimestamp(time)
            adapter["TimeItem"] = time.strftime('%Y-%m-%d %H:%M:%S')
        return item
    
    def clean_note_presse_count(self, item, spider):
        adapter = ItemAdapter(item)
        count_note_presse = adapter.get("PressNoteCount")
        if count_note_presse:
            match = re.search(r'\d+', count_note_presse)
            if match:
                adapter["PressNoteCount"] = match.group() 
        return item
    
    def clean_note_presse(self, item, spider):
        adapter = ItemAdapter(item)
        note_press = adapter.get("NotePress")
        if note_press:
            note_press_flaot = re.sub(r'(?<=\d),(?=\d)', '.', note_press)
            if note_press_flaot:
                adapter["NotePress"] = float(note_press_flaot) 
        return item
    
    def clean_note_users_count(self, item, spider):
        adapter = ItemAdapter(item)
        count_note_users = adapter.get("UserNoteCount")
        if count_note_users:
            match = re.search(r'\d+', count_note_users)
            if match:
                adapter["UserNoteCount"] = match.group()
        return item
    
    def clean_note_users(self, item, spider):
        adapter = ItemAdapter(item)
        note_users = adapter.get("NoteUser")
        if note_users:
            note_users_flaot = re.sub(r'(?<=\d),(?=\d)', '.', note_users)
            if note_users_flaot:
                adapter["NoteUser"] = float(note_users_flaot) 
        return item

    def clean_cumul_box_office(self, item, spider):
        adapter = ItemAdapter(item)
        box_office = adapter.get("CumulBoxOffice")
        if box_office:
            box_office = box_office.replace(" ", '')
            adapter["CumulBoxOffice"] = int(box_office.strip())
        return item

class PostgresPipeline:

    def __init__(self):
        self.conn = psycopg2.connect(
            host = os.getenv('PGHOST'),
            user = urllib.parse.quote(os.getenv('PGUSER')),
            password = urllib.parse.quote(os.getenv('PGPASSWORD')),
            database = os.getenv('PGDATABASE'),
            port = os.getenv('PGPORT')
        )
        self.cur = self.conn.cursor()
        self.cur.execute("""       
        CREATE TABLE IF NOT EXISTS film (
            IdFilm INTEGER PRIMARY KEY,
            Title VARCHAR (100),
            TitleOrigine VARCHAR (100),
            Genre VARCHAR (100),
            Public VARCHAR (100),
            Synopsis VARCHAR NOT NULL,
            Langues VARCHAR (50),
            Type VARCHAR (50),
            DateSortie VARCHAR NOT NULL,
            Annee INTEGER NOT NULL,
            Duree INTEGER NOT NULL,
            Directors VARCHAR NOT NULL,
            Actors VARCHAR NOT NULL,
            NoteUser SERIAL NOT NULL ,
            UserNoteCount INTEGER NOT NULL,
            NotePress SERIAL NOT NULL,
            PressNoteCount INTEGER NOT NULL,
            CumulBoxOffice INTEGER NOT NULL,
            TimeItem TIMESTAMP NOT NULL
            )
        """)
        self.conn.commit()

    def process_item(self, item, spider):
        try:
            # vérifier si le film existe dans la table film
            self.cur.execute("SELECT * FROM film WHERE IdFilm = %s", (item["IdFilm"],))
            film = self.cur.fetchone()

            # Si le film existe
            if film:
                spider.logger.warn("Item already in database: %s" % item['IdFilm'])
            
            # Insertion des nouvelles données
            else:
                self.cur.execute(""" INSERT INTO film (
                                IdFilm,
                                Title,
                                TitleOrigine,
                                Genre,
                                Public,
                                Synopsis,
                                Langues,
                                Type,
                                DateSortie,
                                Annee,
                                Duree,
                                Directors,
                                Actors,
                                NoteUser,
                                UserNoteCount,
                                NotePress,
                                PressNoteCount,
                                CumulBoxOffice,
                                TimeItem
                                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                    int(item["IdFilm"]),
                    item["Title"],
                    item["TitleOrigine"],
                    item["Genre"],
                    item["Public"],
                    item["Synopsis"],
                    item["Langues"],
                    item["Type"],
                    item["DateSortie"],
                    item["Annee"],
                    item["Duree"],
                    item["Directors"],
                    item["Actors"],
                    item["NoteUser"],
                    item["UserNoteCount"],
                    item["NotePress"],
                    item["PressNoteCount"],
                    item["CumulBoxOffice"],
                    item["TimeItem"]
                ))
                self.conn.commit()

        except psycopg2.Error as e:
            self.conn.rollback()
            spider.logger.error(f"Erreur lors de l'insertion dans la base de données: {e}")
            raise DropItem(f"Erreur lors de l'insertion dans la base de données: {e}")

        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()

class MysqlPipeline:

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
