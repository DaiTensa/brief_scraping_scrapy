# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SeriesScraperItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    synopsis = scrapy.Field()
    date = scrapy.Field()
    genre = scrapy.Field()
    saison = scrapy.Field()
    nationality = scrapy.Field()
    casting = scrapy.Field()
    SaisonCount = scrapy.Field()
    EpisodeCount = scrapy.Field()
