# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class FilmscraperItem(scrapy.Item):
    # define the fields for your item here like:
    Title = scrapy.Field()
    TitleOrigine = scrapy.Field()
    Synopsis = scrapy.Field()
    Date = scrapy.Field()
    Duree = scrapy.Field()
    Directors = scrapy.Field()
    Actors = scrapy.Field()
    Public = scrapy.Field()
    Langues = scrapy.Field()
    Annee = scrapy.Field()
    Type = scrapy.Field()
    Genre = scrapy.Field()
    Time = scrapy.Field()
    NoteUser = scrapy.Field()
    UserNoteCount = scrapy.Field()
    NotePress = scrapy.Field()
    PressNoteCount = scrapy.Field()
    CumulBoxOffice = scrapy.Field()
