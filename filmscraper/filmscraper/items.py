# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class FilmscraperItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    synopsis = scrapy.Field()
    date = scrapy.Field()
    duree = scrapy.Field()
    public = scrapy.Field()
    langues = scrapy.Field()
    annee = scrapy.Field()
    Type = scrapy.Field()
    genre = scrapy.Field()
    casting = scrapy.Field()
    critiques_spec = scrapy.Field()
