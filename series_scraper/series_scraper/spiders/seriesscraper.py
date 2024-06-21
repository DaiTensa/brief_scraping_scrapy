import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from series_scraper.items import SeriesScraperItem
import json
import re


class SeriesscraperSpider(CrawlSpider):
    name = "seriesscraper"
    allowed_domains = ["www.allocine.fr"]
    start_urls = [f"https://www.allocine.fr/series-tv/?page={i}" for i in range(1,2)]

    le_serie = LinkExtractor(allow= r'/series/ficheserie_gen_cserie=\d+.html')
    le_saison_episodes = LinkExtractor(allow=r'/series/ficheserie-\d+/saisons/')


    rule_serie_details = Rule(le_serie, callback="parse_item",follow=False)
    rule_serie_casting = Rule(le_saison_episodes, callback="parse_item",follow=False)

    rules = (
        rule_serie_details,
        rule_serie_casting,
        )
    
    @classmethod
    def get_js_str(cls, response):
        js_str = response.xpath('//script[@type="text/javascript"][position() > 7][1]/text()').get()
        js_str = js_str.replace('var dataLayer = dataLayer || ', '')
        js_str = js_str.replace(';', '').strip()
        data_dict = json.loads(js_str)
        return data_dict
        
    
    @classmethod
    def series_id(cls, response):
        data_dict = cls.get_js_str(response=response)
        id_serie = data_dict[0]["series_id"]
        return id_serie
    
    @classmethod
    def serie_nationality(cls, response):
        data_dict = cls.get_js_str(response=response)
        nationality = data_dict[0]["countries"]
        return nationality


    def parse_item(self, response):
        item = SeriesScraperItem()
        item['title']= response.xpath("//div[@class='titlebar-title titlebar-title-xl']/span/text()").get()
        item["synopsis"] = response.xpath("//div[@class='content-txt ']/*/text()").get()
        item["genre"] = response.xpath('//span[@class="spacer"][2]/following-sibling::*/text()').getall()
        item["date"] = response.xpath('//div[@class="meta-body-item meta-body-info"][1]/text()').get()
        item["nationality"] = self.serie_nationality(response=response)
        item["SaisonCount"] = response.xpath("//div[@class='stats-numbers-row-item']/div/text()").get()
        item["EpisodeCount"] = response.xpath("//div[@class='stats-numbers-row stats-numbers-seriespage']//div[2]/div/text()").get()
        id_serie = self.series_id(response=response)
        # print("l'id de la serie est ", id_serie)
        saisons_episodes_url = f'/series/ficheserie-{id_serie}/saisons/'
        
        if saisons_episodes_url:
            # print("saison episdoe url ok")
            yield response.follow(saisons_episodes_url, self.parse_saisons_episodes, meta={"item": item})

    def parse_saisons_episodes(self, response):
        print("parse episode saison ok")
        item = response.meta["item"]
        liste_saisons = response.xpath('//h2')
        print(liste_saisons)
        for saison in liste_saisons:
            print(saison)
            url_saison_details = saison.xpath('.//a[contains(@href, "saison")]/@href').get()
            if url_saison_details:
                print(url_saison_details)
                item['saison'] = saison.xpath('//h2/a/text()').get()
                yield response.follow(url_saison_details, self.parse_casting, meta={"item": item})

    def parse_casting(self, response):
        item = response.meta["item"]
        casting_xpath = "//div[@class='card person-card person-card-col']//div[@class='meta-title']//a/text() | //div[@class='card person-card person-card-col']//div[@class='meta-title']//span/text()"
        item["casting"] = response.xpath(casting_xpath).getall()
        yield item