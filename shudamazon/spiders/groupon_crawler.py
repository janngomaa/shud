# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from shudamazon.items import ShudCrawlerItem
import hashlib
from urllib.parse import urlparse



class GrouponCrawlerSpider(CrawlSpider):
    name = 'groupon_crawler'
    allowed_domains = ['groupon.com']
    start_urls = ['http://groupon.com/']

    rules = (
        Rule(
            LinkExtractor(
                allow=(['\/deals\/']),
                deny=(['\/login']),
                canonicalize=False,
                unique=True,
            ),
            callback='parse_item', 
            follow=True
        ),
    )
    

    def parse_item(self, response):
        page = ShudCrawlerItem()
        fileid = hashlib.md5(bytes(str(response.url),"ascii")).hexdigest()
        filename = str(urlparse(response.url).netloc) +"__"+ fileid
        with open(filename, 'wb') as f:
            f.write(response.body)
                
        page['name']=filename
        page['crawled']=True
        page['parsed']=False
        page['url']=response.url
        page['id']= fileid 
        page['referer']= str(response.request.headers.get('Referer', None))
        
        yield page
