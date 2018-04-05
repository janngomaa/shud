# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ShudamazonItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class ShudCrawlerItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    crawled = scrapy.Field()
    parsed = scrapy.Field()
    url = scrapy.Field()
    referer = scrapy.Field()
    
    
    
    
class GrpDealItem(scrapy.Item):
    id = scrapy.Field()    
    url = scrapy.Field()
    referer = scrapy.Field()
    domain = scrapy.Field()
    title = scrapy.Field()
    
    merchant = scrapy.Field()
    merchantLocation = scrapy.Field()
    dealOptTitles = scrapy.Field()
    dealOptMessages = scrapy.Field()
    dealOptInitPrices = scrapy.Field()
    dealOptFinalPrices = scrapy.Field()

