# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class DynCrawlerSpider(CrawlSpider):
    name = 'syn_crawler'
    allowed_domains = ['syntell.com']
    start_urls = ['http://syntell.com/']

    rules = (
        Rule(LinkExtractor(allow=('carrieres',)), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        #i = {}
        filename = str(self.name)+'_'+ str(response.url).replace('/', '_') + '.html'
        
        with open(filename, 'wb') as f:
            f.write(response.body)
            
        #return i
