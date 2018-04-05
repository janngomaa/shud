# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from shudamazon.items import ShudCrawlerItem, GrpDealItem
import hashlib
from urllib.parse import urlparse



class GrpCrawlerSpider(CrawlSpider):
    name = 'groupon_crawler'
    allowed_domains = ['groupon.com']
    start_urls = ['http://groupon.com/']

    rules = (
        Rule(
            LinkExtractor(
                allow=(['\/deals\/']),
                deny=(['\/login\/']),
                canonicalize=False,
                unique=True,
            ),
            callback='parse_item', 
            follow=True
        ),
    )
    

    def parse_item(self, response):
        page = ShudCrawlerItem()
        deal = GrpDealItem()
        
        deal['id'] = hashlib.md5(bytes(str(response.url),"ascii")).hexdigest()
        deal['url']=response.url
        deal['referer']= str(response.request.headers.get('Referer', None))
        deal['domain'] = str(urlparse(response.url).netloc)
        deal['title'] = response.xpath('//*[@id="deal-title"]/text()').extract_first()
        
        deal['merchant'] = response.xpath('//*[@id="deal-subtitle-container"]/h2/span/span/text()').extract_first()
        deal['merchantLocation'] = response.xpath(\
                                                  '//*[@id="deal-subtitle-container"]/h2/span/div/a/text()'\
                                                 ).extract_first()        
        deal['dealOptTitles'] = response.xpath('//*[@id="purchase-cluster"]/div/div[position() < 5]/ul/li[1]/div/label/div/h3/text()').extract()
        deal['dealOptMessages'] = response.xpath(\
                                                 '//*[@id="purchase-cluster"]/div/div[position() < 5]'\
                                                 '/ul/li[position() < 5]/div/label/div/div/div[position() < 5]/div/text()'\
                                                ).extract()
        deal['dealOptInitPrices'] = response.xpath(\
                                                 '//*[@id="purchase-cluster"]/div/div[2]/ul/li[1]/div/'\
                                                 'label/div/div/div[2]/div[1]/div[1]/div/text()'\
                                                ).extract()
        deal['dealOptFinalPrices'] = response.xpath(\
                                                 '//*[@id="purchase-cluster"]/div/div[2]/ul/li[1]/div'\
                                                  '/label/div/div/div[2]/div[1]/div[2]/div/text()'\
                                                ).extract()
        
        
        yield deal
