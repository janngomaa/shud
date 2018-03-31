# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from shudamazon.items import ShudCrawlerItem, GrouponDealItem
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
        deal = GrouponDealItem()
        fileid = hashlib.md5(bytes(str(response.url),"ascii")).hexdigest()
        filename = str(urlparse(response.url).netloc) +"__"+ fileid
        #with open(filename, 'wb') as f:
        #    f.write(response.body)
                
        page['name']=filename
        page['crawled']=True
        page['parsed']=False
        page['url']=response.url
        page['id']= fileid 
        page['referer']= str(response.request.headers.get('Referer', None))
        
        deal['header'] = page
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
