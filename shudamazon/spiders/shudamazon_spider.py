import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import re
import hashlib
#from sets import Set
import inspect
from scrapy.http import HtmlResponse
#from selenium import webdriver
import time
#from datetime import datetime, timedelta
#import socket
#from selenium import webdriver
from bs4 import BeautifulSoup
#import sys
import requests
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
#from couchbase.bucket import Bucket
#from couchbase.cluster import Cluster, PasswordAuthenticator
#from couchbase.n1ql import N1QLQuery
import time

#import couchbase._libcouchbase as LCB
#import couchbase.exceptions as E
#from couchbase.user_constants import FMT_JSON
#from couchbase._pyport import ulp
from cryptography.fernet import Fernet
import random
import uuid
#import validators

class ShudScraperItem(scrapy.Item):
    # The source URL
    url_from = scrapy.Field()
    # The destination URL
    url_to = scrapy.Field()

    #spidername = "amazon", allowed_domain= "amazon.com", start_url="https://www.amazon.com/gp/goldbox"
def Parameters(spidername = "amazon", allowed_domain= "amazon.com", start_url="https://www.amazon.com/gp/goldbox"):
      # Spider Name  
    name =spidername   
     # The domains that are allowed (links to other domains are skipped)
    allowed_domains = allowed_domain
     # The URLs to start with
    start_urls = start_url
    para=[name, allowed_domains, start_urls]
    return para
    
#crawlerName="amazon", DomainNameAllowed=["amazon.com"], StartUrls=["https://www.amazon.com/gp/goldbox"], URLList=['https://www.amazon.com/gp/goldbox']
class AmazonSpider(scrapy.Spider):
              
    parametre=Parameters()
    
     # Spider Name 
    name =parametre[0]
    
    # The domains that are allowed (links to other domains are skipped)
    allowed_domains = parametre[1]
    
    # The URLs to start with
    start_urls =parametre[2]
    
    # This spider has one rule: extract all (unique and canonicalized) links, follow them and parse them using the parse_items method
    rules = [
        Rule(
            LinkExtractor(canonicalize=False,
                unique=True
            ),
            follow=True,
            callback="parse_items"
        )
    ]    
        
    def start_requests(self):
        urls = [self.start_urls]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # The list of items that are found on the particular page
        items = []
        URLListe=[]
        # Only extract canonicalized and unique links (with respect to the current page)
        links = LinkExtractor(canonicalize=False, unique=True).extract_links(response)
        # Now go through all the found links
        for link in links:
            
            # Check whether the domain of the URL of the link is allowed; so whether it is in one of the allowed domains
            is_allowed = False
            for allowed_domain in self.allowed_domains:
                if allowed_domain in link.url:
                    is_allowed = True
            # If it is allowed, create a new item and add it to the list of found items
            if is_allowed:
                item = ShudScraperItem()
                item['url_from'] = response.url
                item['url_to'] = link.url
                items.append(item)
                URLListe.append(response.url)
                URLListe.append(link.url)
      #  print("*****************DEBUT*********************************************" )
      #   print(URLListe)        
      #  print("*******************FIN*******************************************" )
        # Return all the found items    
        page = response.url.split("/")[-2]
        m=hashlib.md5(bytes(str(link.url),"ascii"))   # python 3                
        filename = str(self.name)+'_'+ m.hexdigest() + '-%s.html' % page
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)
        return items

#Function to update the list of urls to crawl
def Updatedlist(Listes1=None, Listes2=None):
    mergeliste = Set(Listes1) | Set(Listes2)
    return mergeliste



#process = CrawlerProcess(get_project_settings())
#process.crawl(AmazonSpider)
#, name = str("amazon"), allowed_domains= str("amazon.com"), start_urls=str("https://www.amazon.com/gp/goldbox")
#process.start()