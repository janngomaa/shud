# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from shudamazon.items import ShudCrawlerItem, GrpDealItem
from shudamazon.helper import ShudHelper

import hashlib
from urllib.parse import urlparse




class GrpCrawlerSpider(CrawlSpider):
    name = 'grp_crawler'
    config = ShudHelper().getConfig()
    allowed_domains = [config.get('crawling', 'grp_domain')]
    start_urls = [config.get('crawling', 'grp_str_url')] 

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
        
        deal['dealOptTitles'] = response.xpath('//*[@id="purchase-cluster"]/div/div[position() < 5]'\
                                               '/ul/li[position() < 5]/div/label/div/h3/text()').extract()
        
        deal['dealOptMessages'] = response.xpath(\
                                                 '//*[@id="purchase-cluster"]/div/div[position() < 5]'\
                                                 '/ul/li[position() < 5]/div/label/div/div/div[position() < 5]/div/text()'\
                                                ).extract()
        
        deal['dealOptPrices'] = response.xpath(\
                                                 '//*[@id="purchase-cluster"]/div/div[position() < 5]/ul/li[position() < 5]/div/'\
                                                 'label/div/div/div[position() < 5]/div[position() < 5]/div[position() < 5]/div/text()'\
                                                ).extract()
        
        '''
        deal['dealOptFinalPrices'] = response.xpath(\
                                                 '//*[@id="purchase-cluster"]/div/div[position() < 5]/ul/li[position() < 5]/div'\
                                                  '/label/div/div/div[position() < 5]/div[position() < 5]/div[position() < 5]/div/text()'\
                                                ).extract()
        '''
        
        deal['dealTiming'] = response.xpath('//*[@id="purchase-cluster"]/div/div[1]/div/div[1]/div[2]/text()').extract_first()
        
        deal['dealRatingCount'] = response.xpath('//*[@itemprop="ratingCount"]/@content').extract_first()
        deal['dealRatingValue'] = response.xpath('//*[@itemprop="ratingValue"]/@content').extract_first()
        deal['dealViews'] = response.xpath('//*[@id="purchase-cluster"]/div/div[1]/div/div[2]/div[2]/text()').extract_first()
        deal['image_urls'] = response.xpath('//*[@id="overflow-container"]/div/img/@src').extract()
        
        
        
        yield deal
