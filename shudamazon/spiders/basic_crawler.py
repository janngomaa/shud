# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import BaseSpider
from scrapy.selector import Selector
from scrapy.http import Request
import re


class BasicCrawlerSpider(BaseSpider):
    name = 'basic_crawler'
    allowed_domains = ['syntell.com']
    start_urls = ['http://syntell.com/']

    def parse(self, response):
        
        visited_links=[]
        
        hxs = Selector(response)
        links = hxs.xpath('//a/@href').extract()
        
        for link in links:
            try:                
                if not link in visited_links:
                    visited_links.append(link)
                    filename = str(self.name)+'_'+ str(response.url).replace('/', '_') + '.html'
                    with open(filename, 'wb') as f:
                        f.write(response.body)
                    yield Request(link, self.parse)

                else:
                    full_url=response.urljoin(link)
                    visited_links.append(full_url)
                    yield Request(full_url, self.parse)
            except:
                print("Exception at %s" %str(link))
                pass
                
