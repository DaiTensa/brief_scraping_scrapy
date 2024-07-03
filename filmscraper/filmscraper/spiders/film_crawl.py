from typing import Iterable
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from filmscraper.items import FilmscraperItem
from time import time
import json
# from datetime import datetime


class FilmCrawlSpider(CrawlSpider):
    name = "film_crawl"
    allowed_domains = ["www.allocine.fr"]
    start_urls = [f"https://www.allocine.fr/films/?page={i}"  for i in range(1,3)] 

    le_films = LinkExtractor(allow= r'/film/fichefilm_gen_cfilm=\d+.html')
    le_casting = LinkExtractor(allow=r'/film/fichefilm-\d+/casting/')
    le_critique_spec = LinkExtractor(allow=r'/film/fichefilm-\d+/critiques/spectateurs/')
    
    
    rule_films_details = Rule(le_films, callback="parse_accueil",follow=False)
    rule_casting = Rule(le_casting, callback="parse_casting", follow=False)
    rule_critique_spec = Rule(le_critique_spec, callback="parse_item", follow=False)
    
    rules = (
        rule_films_details,
        rule_casting,
        rule_critique_spec,
        )

    @classmethod
    def get_java_script_text_type(cls, response):
        java_script_text_type = response.xpath('//script[@type="text/javascript"][contains(text(),"var dataLayer = dataLayer")]/text()').get()
        if isinstance(java_script_text_type, str):
            java_script_text_type = java_script_text_type.replace('var dataLayer = dataLayer || ', '')
            java_script_text_type = java_script_text_type.replace(';', '').strip()
            data_dict = json.loads(java_script_text_type)
        return data_dict
    
    @classmethod
    def film_id(cls, response):
        data_dict = cls.get_java_script_text_type(response=response)
        id_film = data_dict[0]["movie_id"]
        return id_film


    # parse accueil page
    def parse_accueil(self, response):
        # Instanciation de l'object item
        item = FilmscraperItem()

        # Récupération de l'id du film : id unique pour chaque film
        film_id = self.film_id(response=response)

        # Parse des données de la page : https://www.allocine.fr/films/?page={numero_de_page}
        # Infos sur le film
        item["IdFilm"] = film_id
        item["Title"] = response.xpath('//div[@class="titlebar-title titlebar-title-xl"]/text()').get()
        item["TitleOrigine"] = response.xpath('//div[@class="meta-body-item"]/span[2]/text()').get()
        item["Genre"] = response.xpath('//div[@class="meta-body-item meta-body-info"]//span[@class="spacer"][2]/following-sibling::*/text()').getall()
        item["Public"] = response.xpath("//div[@class='certificate']/*/text()").get()
        item["Synopsis"] = response.xpath("//div[@class='content-txt ']/*/text()").get()
        item["Langues"] = response.xpath('//span[text()="Langues"]/following-sibling::span/text()').get() 
        item["Type"] = response.xpath('//span[text()="Type de film"]/following-sibling::span/text()').get()
        item["DateSortie"] = response.xpath('//div[@class="meta-body-item meta-body-info"]/span/text()').get()
        item["Annee"] = response.xpath('//span[text()="Année de production"]/following-sibling::span/text()').get() 
        item["Duree"] = response.xpath('//div[@class="meta-body-item meta-body-info"]//span[@class="spacer"][1]/following-sibling::text()[1]').get()
        # item["Directors"] = response.xpath('//div[@class="meta-body-item meta-body-direction meta-body-oneline"]/span[2]/text()').get()
        item["Directors"] = "vide"
        
        # Recherche l'url de la page casting afin de scraper la liste d'acteurs
        casting_url = response.xpath('//a[contains(@href, "casting")]/@href').get()
        if casting_url:
            yield response.follow(casting_url, self.parse_casting, meta={"item": item})

    # parse_cating permet de récupérer la liste de tous les acteurs pour chaque film
    def parse_casting(self, response):
        item = response.meta["item"]

        #Liste des acteurs par film
        actors = response.xpath("//div[@class='card person-card person-card-col']//div[@class='meta-title']//a | //div[@class='card person-card person-card-col']//div[@class='meta-title']//span")
        item["Casting"] = actors.xpath('.//text()').getall()

        # naviguer vers la page critiques et récuperer les notes spectateurs
        critique_url = response.url.replace("casting/", "critiques/spectateurs/")
        if critique_url:
            yield response.follow(critique_url, self.parse_critiques_user, meta={"item": item})
            
    # parse_critiques_users : récupérer la note moyenne des spectaterus
    def parse_critiques_user(self, response):
        item = response.meta['item']
        critiques_xpath = '//section[@class="section mdl"]//span[@class="note"]/text()'
        count_rating_spec = '//section[@class="section mdl"]//span[@class="user-note-count"]/text()'
        item["NoteUser"] = response.xpath(critiques_xpath).get()
        item["UserNoteCount"] = response.xpath(count_rating_spec).get()
        
        # naviguer vers la page critiques presse et récuperer les notes presse
        critique_press_url = response.url.replace("spectateurs/", "presse/")
        if critique_press_url:
            yield response.follow(critique_press_url, self.parse_critiques_press, meta={"item": item})

    # def parse_critiques_press(self, response): : récupérer la note moyenne de la presse
    def parse_critiques_press(self, response):
        item = response.meta['item']
        critiques_xpath = '//div[@class="reviews-press-intro"]//span[@class="note"]/text()'
        count_rating_press = '//div[@class="big-note"]//span[@class="user-note-count"]/text()'
        item["NotePress"] = response.xpath(critiques_xpath).get()
        item["PressNoteCount"] = response.xpath(count_rating_press).get()

        # naviguer vers la page box office
        box_office_url = response.url.replace("critiques/presse/", "box-office/")
        if box_office_url:
            yield response.follow(box_office_url, self.parse_box_office, meta={"item": item})
        
    # parse_box_office : récupérer le nombre cumulé de spectateurs depuis la sortie du film
    def parse_box_office(self, response):
        item = response.meta['item']
        boxoffices= response.xpath('//table[@class="box-office-table table-3-cell responsive-table responsive-table-lined"]/tbody/tr')
        lastweek = boxoffices[-1]
        item["CumulBoxOffice"] = lastweek.xpath('.//td[@class="responsive-table-column third-col"]/text()').get()
        item["TimeItem"] = time()
        yield item
