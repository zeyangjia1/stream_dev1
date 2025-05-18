import scrapy

class ItcastSpider(scrapy.Spider):
    name = "itcast"
    allowed_domains = ["itcast.cn"]
    start_urls = ("http://www.itcast.cn/channel/teacher.shtml",)

    def parse(self, response):
        with open('itcast.html','wb') as f:
            f.write(response.body)
