from typing import Iterable
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from filmscraper.items import FilmscraperItem
from time import time
# from datetime import datetime


class FilmCrawlSpider(CrawlSpider):
    name = "film_crawl"
    allowed_domains = ["www.allocine.fr"]
    start_urls = ["https://www.allocine.fr/films/?page=1"] # for i in range(1,2)

    le_films = LinkExtractor(allow= r'/film/fichefilm_gen_cfilm=\d+.html')
    # le_casting = LinkExtractor(allow=r'/film/fichefilm-\d+/casting/')
    # le_critique_spec = LinkExtractor(allow=r'/film/fichefilm-\d+/critiques/spectateurs/')

    rule_films_details = Rule(le_films, callback="parse_accueil",follow=False)
    # rule_casting = Rule(le_casting, callback="parse_item", follow=False)
    # rule_critique_spec = Rule(le_critique_spec, callback="parse_item", follow=False)


    rules = (
        rule_films_details,
        # rule_casting,
        # rule_critique_spec
        )

    def parse_accueil(self, response):
        item = FilmscraperItem()

        item["Title"] = response.xpath('//div[@class="titlebar-title titlebar-title-xl"]/text()').get()
        item["Date"] = response.xpath('//div[@class="meta-body-item meta-body-info"]/span/text()').get()
        item["Duree"] = response.xpath('//div[@class="meta-body-item meta-body-info"]//span[@class="spacer"][1]/following-sibling::text()[1]').get()
        item["Genre"] = response.xpath('//div[@class="meta-body-item meta-body-info"]//span[@class="spacer"][2]/following-sibling::*/text()').getall()
        item["Directors"] = response.xpath('//div[@class="meta-body-item meta-body-direction meta-body-oneline"]/span[2]/text()').get()
        item["TitleOrigine"] = response.xpath('//div[@class="meta-body-item"]/span[2]/text()').get()
        item["Public"] = response.xpath("//div[@class='certificate']/*/text()").get()
        item["Synopsis"] = response.xpath("//div[@class='content-txt ']/*/text()").get()
        item["Langues"] = response.xpath('//span[text()="Langues"]/following-sibling::span/text()').get() 
        item["Annee"] = response.xpath('//span[text()="Année de production"]/following-sibling::span/text()').get() 
        item["Type"] = response.xpath('//span[text()="Type de film"]/following-sibling::span/text()').get()
        
        
        # parse casting and 
        casting_url = response.xpath('//a[contains(@href, "casting")]/@href').get()
        if casting_url:
            yield response.follow(casting_url, self.parse_casting, meta={"item": item})

    def parse_casting(self, response):
        item = response.meta["item"]
        casting_xpath = "//div[@class='card person-card person-card-col']//div[@class='meta-title']//a/text() | //div[@class='card person-card person-card-col']//div[@class='meta-title']//span/text()"
        item["Actors"] = response.xpath(casting_xpath).getall()

        # parse user critiques
        critique_url = response.url.replace("casting/", "critiques/spectateurs/")
        if critique_url:
            yield response.follow(critique_url, self.parse_critiques_user, meta={"item": item})
    
    def parse_critiques_user(self, response):
        item = response.meta['item']
        critiques_xpath = '//section[@class="section mdl"]//span[@class="note"]/text()'
        count_rating_spec = '//section[@class="section mdl"]//span[@class="user-note-count"]/text()'
        item["NoteUser"] = response.xpath(critiques_xpath).get()
        item["UserNoteCount"] = response.xpath(count_rating_spec).get()
        item["Time"] = time()
        # item["Time"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # parse press critiques
        critique_press_url = response.url.replace("spectateurs/", "presse/")
        if critique_press_url:
            yield response.follow(critique_press_url, self.parse_critiques_press, meta={"item": item})

    def parse_critiques_press(self, response):
        item = response.meta['item']
        critiques_xpath = '//div[@class="reviews-press-intro"]//span[@class="note"]/text()'
        count_rating_press = '//div[@class="big-note"]//span[@class="user-note-count"]/text()'
        item["NotePress"] = response.xpath(critiques_xpath).get()
        item["PressNoteCount"] = response.xpath(count_rating_press).get()

        # parse box office
        box_office_url = response.url.replace("critiques/presse/", "box-office/")
        if box_office_url:
            yield response.follow(box_office_url, self.parse_box_office, meta={"item": item})
        
        yield item

    def parse_box_office(self, response):
        item = response.meta['item']



# //div[@class='gd gd-gap-15 gd-xs-2 gd-s-4 ']/div[@class='card person-card person-card-col']
# //section[@class="section ovw ovw-technical"]//following-sibling::div[@class="item"][1]/span[@class="that"]/text()
# casting_url = response.xpath('//a[contains(@href, "casting")]/@href').get()
# critique_url = response.xpath('//a[contains(@href, "critiques/spectateurs")]/@href').get()