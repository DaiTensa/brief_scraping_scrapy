# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SeriesScraperItem(scrapy.Item):
    # define the fields for your item here like:
    Title = scrapy.Field()
    Synopsis = scrapy.Field()
    Date = scrapy.Field()
    Genre = scrapy.Field()
    Saison = scrapy.Field()
    Nationality = scrapy.Field()
    Casting = scrapy.Field()
    SaisonCount = scrapy.Field()
    EpisodeCount = scrapy.Field()
    Time = scrapy.Field()
