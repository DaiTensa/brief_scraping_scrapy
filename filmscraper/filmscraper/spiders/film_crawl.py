import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from filmscraper.items import FilmscraperItem


class FilmCrawlSpider(CrawlSpider):
    name = "film_crawl"
    allowed_domains = ["www.allocine.fr"]
    start_urls = [f"https://www.allocine.fr/films/?page={i}" for i in range(1,2)]

    le_films = LinkExtractor(allow= r'/film/fichefilm_gen_cfilm=\d+.html')
    le_casting = LinkExtractor(allow=r'/film/fichefilm-\d+/casting/')
    le_critique_spec = LinkExtractor(allow=r'/film/fichefilm-\d+/critiques/spectateurs/')

    rule_films_details = Rule(le_films, callback="parse_item",follow=False)
    rule_casting = Rule(le_casting, callback="parse_item", follow=True)
    rule_critique_spec = Rule(le_critique_spec, callback="parse_item", follow=True)


    rules = (
        rule_films_details,
        rule_casting,
        rule_critique_spec
        )

    def parse_item(self, response):
        item = FilmscraperItem()
        item["Title"] = response.xpath("//div[@class='titlebar-title titlebar-title-xl']/text()").get()
        item["Date"] = response.xpath('//div[@class="meta-body-item meta-body-info"]/*/text()').get()
        item["Duree"] = response.xpath('//div[@class="meta-body-item meta-body-info"]/span[@class="spacer"][1]/following-sibling::text()[1]').get()
        item["Public"] = response.xpath("//div[@class='certificate']/*/text()").get()
        item["Synopsis"] = response.xpath("//div[@class='content-txt ']/*/text()").get()
        item["Langues"] = response.xpath('//span[text()="Langues"]/following-sibling::span/text()').get() # //section[@class="section ovw ovw-technical"]
        item["Annee"] = response.xpath('/span[text()="Année de production"]/following-sibling::span/text()').get() #//section[@class="section ovw ovw-technical"]/
        item["Type"] = response.xpath('//span[text()="Type de film"]/following-sibling::span/text()').get() #//section[@class="section ovw ovw-technical"]
        item["Genre"] = response.xpath('//span[@class="spacer"][2]/following-sibling::*/text()').getall()
        

        casting_url = response.xpath('//a[contains(@href, "casting")]/@href').get()
        if casting_url:
            yield response.follow(casting_url, self.parse_casting, meta={"item": item})

    def parse_casting(self, response):
        item = response.meta["item"]
        casting_xpath = "//div[@class='card person-card person-card-col']//div[@class='meta-title']//a/text() | //div[@class='card person-card person-card-col']//div[@class='meta-title']//span/text()"
        item["Casting"] = response.xpath(casting_xpath).getall()

        critique_url = response.url.replace("casting/", "critiques/spectateurs/")
        if critique_url:
            yield response.follow(critique_url, self.parse_critiques, meta={"item": item})
    
    def parse_critiques(self, response):
        item = response.meta['item']
        critiques_xpath = '//section[@class="section mdl"]//span[@class="note"]/text()'
        item["CritiquesSpec"] = response.xpath(critiques_xpath).get()
        yield item



# //div[@class='gd gd-gap-15 gd-xs-2 gd-s-4 ']/div[@class='card person-card person-card-col']
# //section[@class="section ovw ovw-technical"]//following-sibling::div[@class="item"][1]/span[@class="that"]/text()
# casting_url = response.xpath('//a[contains(@href, "casting")]/@href').get()
# critique_url = response.xpath('//a[contains(@href, "critiques/spectateurs")]/@href').get()