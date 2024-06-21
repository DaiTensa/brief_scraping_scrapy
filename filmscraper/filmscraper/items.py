# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class FilmscraperItem(scrapy.Item):
    # define the fields for your item here like:
    Title = scrapy.Field()
    Synopsis = scrapy.Field()
    Date = scrapy.Field()
    Duree = scrapy.Field()
    Public = scrapy.Field()
    Langues = scrapy.Field()
    Annee = scrapy.Field()
    Type = scrapy.Field()
    Genre = scrapy.Field()
    Casting = scrapy.Field()
    CritiquesSpec = scrapy.Field()
