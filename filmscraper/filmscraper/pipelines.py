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

#Processing
class FilmscraperPipeline:

    def process_item(self, item, spider):
        item = self.clean_title(item, spider)
        item = self.clean_original_title(item, spider)
        item = self.clean_synopsis(item, spider)
        item = self.clean_duree(item, spider)
        item = self.clean_casting(item, spider)
        item = self.clean_public(item, spider)
        item = self.clean_langues(item, spider)
        item = self.clean_date(item, spider)
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

    def clean_casting(self, item, spider):
        adapter = ItemAdapter(item)

        casting = adapter.get("Casting")
        directors = adapter.get("Directors")
        liste_acteurs = []
        liste_directors = []

        if casting:

            # si len(casting) = 9
            # acteur 8 derniers, director 1 premier
            if len(casting) >= 9:
                liste_acteurs = casting[-8:]
                liste_directors = casting[:-8]
           
            elif len(casting) <= 8:
                liste_acteurs = casting[0:]
                liste_directors = casting[0]
  
        # Nettoyer actors
        cleaned_actors = [actor.strip() for actor in liste_acteurs]
        adapter["Casting"] = ",".join(map(str, cleaned_actors))

        # Nettoyer directors
        if isinstance(liste_directors, list):
            directors=  [director.strip() for director in liste_directors]
            adapter["Directors"] =  ",".join(map(str, directors)) 
        else:
            directors = liste_directors.strip()
            adapter["Directors"] = directors

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

# MySQL
class FilmPostgresPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host = os.getenv("HOST"),
            port=os.getenv("PORT"),
            user = os.getenv("USER_NAME"),
            password = os.getenv("MYSAL_PASSWORD"),
            database = os.getenv("DATA_BASE_NAME")
        )
        self.cur = self.conn.cursor()
        
    def create_film_table(self):
        self.cur.execute("""       
       CREATE TABLE IF NOT EXISTS film (
            IdFilm INT PRIMARY KEY,
            Title VARCHAR(100),
            TitleOrigine VARCHAR(100) NULL,
            Genre VARCHAR(100),
            Public VARCHAR(100),
            Synopsis TEXT,
            Langues VARCHAR(50),
            Type VARCHAR(50),
            DateSortie VARCHAR(100) NOT NULL,
            Annee INT NOT NULL,
            Duree INT NOT NULL,
            Directors VARCHAR(255) NOT NULL,
            Casting VARCHAR(255) NOT NULL,
            NoteUser FLOAT NOT NULL,
            UserNoteCount INT NOT NULL,
            NotePress FLOAT NOT NULL,
            PressNoteCount INT NOT NULL,
            CumulBoxOffice INT NOT NULL,
            TimeItem TIMESTAMP NOT NULL
        );
        """)
        self.conn.commit()

    def insert_films_into_film_table(self, item, spider):
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
                                Casting,
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
                    item["Casting"],
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

    def process_item(self, item, spider):
        self.create_film_table()
        self.insert_films_into_film_table(item, spider)
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()

# MySQL
class PersonnesPostgresPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host = os.getenv("HOST"),
            port=os.getenv("PORT"),
            user = os.getenv("USER_NAME"),
            password = os.getenv("MYSAL_PASSWORD"),
            database = os.getenv("DATA_BASE_NAME")
        )
        self.cur = self.conn.cursor()
        
    def create_personne_table(self):
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS personnes (
            IdPersonne INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
            Nom_Prenom VARCHAR(100) NOT NULL
        )
        """)
        self.conn.commit()

    def insert_personne_into_personne_table(self,item,spider):
        try:
            casting = item.get("Casting", "").split(',')
            directors = item.get("Directors", "").split(',')
            all_personnes = set(casting + directors)

            for personne in all_personnes:
                if personne:
                    self.cur.execute("SELECT IdPersonne FROM personnes WHERE Nom_Prenom = %s", (personne.strip(),))
                    result = self.cur.fetchone()

                    if not result:
                        self.cur.execute("INSERT INTO personnes (Nom_Prenom) VALUES (%s)", (personne.strip(),))
                        self.conn.commit()

        except psycopg2.Error as e:
            self.conn.rollback()
            spider.logger.error(f"Erreur lors de l'insertion dans la base de données: {e}")
            raise DropItem(f"Erreur lors de l'insertion dans la base de données: {e}")

        return item
    
    def process_item(self, item, spider):
        self.create_personne_table()
        self.insert_personne_into_personne_table(item, spider)
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()



# MySQL
class CastingFilmPostgresPipeline:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host = os.getenv("HOST"),
            port=os.getenv("PORT"),
            user = os.getenv("USER_NAME"),
            password = os.getenv("MYSAL_PASSWORD"),
            database = os.getenv("DATA_BASE_NAME")
        )
        self.cur = self.conn.cursor()

    def create_casting_film_table(self):
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS filmcasting (
            IdFilm INTEGER,
            IdPersonne INTEGER,
            Role VARCHAR(10),
            PRIMARY KEY (IdFilm, IdPersonne),
            FOREIGN KEY (IdFilm) REFERENCES film (IdFilm),
            FOREIGN KEY (IdPersonne) REFERENCES personnes (IdPersonne)
        )
        """)
        self.conn.commit()

    def insert_personne_role_into_casting_film(self, item, spider):
        try:
            casting = item.get("Casting", "").split(',')
            directors = item.get("Directors", "").split(',')

            for actor in casting:
                if actor:
                    self.cur.execute("SELECT IdPersonne FROM personnes WHERE Nom = %s", (actor.strip(),))
                    result = self.cur.fetchone()
                    if result:
                        self.cur.execute("INSERT INTO filmcasting (IdFilm, IdPersonne, Role) VALUES (%s, %s, %s)",
                                         (item["IdFilm"], result[0], "Acteur"))
                        self.conn.commit()

            for director in directors:
                if director:
                    self.cur.execute("SELECT IdPersonne FROM personnes WHERE Nom = %s", (director.strip(),))
                    result = self.cur.fetchone()
                    if result:
                        self.cur.execute("INSERT INTO filmcasting (IdFilm, IdPersonne, Role) VALUES (%s, %s, %s)",
                                         (item["IdFilm"], result[0], "Realisateur"))
                        self.conn.commit()

        except psycopg2.Error as e:
            self.conn.rollback()
            spider.logger.error(f"Erreur lors de l'insertion dans la base de données: {e}")
            raise DropItem(f"Erreur lors de l'insertion dans la base de données: {e}")

        return item
    
    def process_item(self, item, spider):
        self.create_casting_film_table()
        self.insert_personne_role_into_casting_film(item, spider)
        return item
    
    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()

# Test Mysql
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
            Casting text,
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
                        Casting,
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
            item["Casting"],
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
