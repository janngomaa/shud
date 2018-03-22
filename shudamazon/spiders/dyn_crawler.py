# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from shudamazon.items import ShudCrawlerItem
import hashlib
from urllib.parse import urlparse


class DynCrawlerSpider(CrawlSpider):
    name = 'dyn_crawler'
    allowed_domains = ['amazon.ca']
    start_urls = ['https://www.amazon.ca/gp/goldbox/']
    
    '''start_urls = ['https://www.amazon.ca/Philips-470013-PAR16-Glass-White/dp/B071W8TCLW/ref=gbps_tit_s-3_aa60_51d35bca?smid=A3DWYIK6Y9EEQB&pf_rd_p=b013a60b-0b5a-4a66-8ff5-66b6cedeaa60&pf_rd_s=slot-3&pf_rd_t=701&pf_rd_i=gb_main&pf_rd_m=A3DWYIK6Y9EEQB&pf_rd_r=ZSCJAMYP83HVJ706GQ37']
    '''

    rules = (
        Rule(
            LinkExtractor(
                #allow=(['www\.amazon\.ca\/.+\/dp\/']),
                allow=(),
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
        
        #if response.xpath('//*[@id="priceblock_dealprice_lbl"]'):
        #    with open(fileid, 'wb') as f:
        #        f.write(response.body)
                
        #page['name']=filename
        #page['crawled']=True
        #page['parsed']=False
        page['url']=response.url
        page['id']= fileid            
        
        yield page