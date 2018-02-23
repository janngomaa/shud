import scrapy
import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import re
import hashlib
import inspect
from scrapy.http import HtmlResponse
import time
from bs4 import BeautifulSoup
import requests
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
import time
from cryptography.fernet import Fernet
import random
import uuid
from scrapy.conf import settings
from scrapy.crawler import CrawlerProcess
from scrapy.xlib.pydispatch import dispatcher
from multiprocessing.queues import Queue
import multiprocessing
from scrapy import signals, log
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy.settings import Settings
import pandas as pd

class ShudScraperItem(scrapy.Item):
    # The source URL
    url_from = scrapy.Field()
    # The destination URL
    url_to = scrapy.Field()
    
def Parameters(spidername = "amazon", allowed_domain= "amazon.com", start_url="https://www.amazon.com/gp/goldbox"):
      # Spider Name  
    name =spidername   
     # The domains that are allowed (links to other domains are skipped)
    allowed_domains = allowed_domain
     # The URLs to start with
    start_urls = start_url
    para=[name, allowed_domains, start_urls]
    return para

    
def MyCrawler(self) :
    
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    process = CrawlerProcess({
     'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'   
    })
    #urlListe=[]
    Parame=Parameters()
    #urlListe = spark.read.csv("URLListe.csv")  
    urlListe = sqlContext.sql("SELECT url, parsed from UrlList where parsed = 'false'")
    for url in urlListe.rdd.collect():
        if 'amazon' in url[0]:
            print("JAUNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN")
            print(url[0])
            print("FINNNNNNNNNNNNNNNNNNNNNNNNNNNNNN")
            Parameters('Amazon', 'Amazon.com', url[0]) #Parame=
            process.crawl(AmazonSpider)  
            #df = spark.read.csv("URLListe.csv")
            df = sqlContext.sql("SELECT url, parsed from UrlList where parsed = 'false'")
            urlListe.union(df)
            urlListe = urlListe.dropDuplicates()


class ShudCrawler(scrapy.Spider):
    
    def ___init__(self):
        self.spkSession = SparkSession \
        .builder \
        .appName("ShudCrawler") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    
    parametre=Parameters()
    #  create the context spark
    #conf = SparkConf()
    #sc = pyspark.SparkContext(conf=conf)  

     # Spider Name 
    name =parametre[0]
    
    # The domains that are allowed (links to other domains are skipped)
    allowed_domains = parametre[1]
    URLListe=[]
    # The URLs to start with
    start_urls =parametre[2]
    #initialise list of url to crawl
    CrawList=parametre[2]
    
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
            #spark configuration spark session
        
        
        initList = [("", "")]
        newUrls = spark.createDataFrame(initList, schema=["url", "parsed"])
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
                #self.URLListe.append(response.url)
                self.URLListe.append(link.url)  
                
        newUrls = newUrls.union(spark.createDataFrame([(self.URLListe, "false")])
                                       ).union(spark.createDataFrame([(response.url, "true")]))
                
      #  print("*****************DEBUT*********************************************" )
      #   print(URLListe) 
        
        
        newUrls = newUrls.dropDuplicates()
        
        
        #MyCrawler
        
      #  print("*******************FIN*******************************************" )
        # Return all the found items    
        page = response.url.split("/")[-2]
        m=hashlib.md5(bytes(str(response.url),"ascii"))   # python 3                
        filename = str(self.name)+'_'+ m.hexdigest() + '-%s.html' % page
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)
        
        '''
        
        print('XXXXXXXXXXXXXXX DEBUT XXXXXXXXXXXXXXXXXXXXXX')
        MyCrawler(self)
        print('XXXXXXXXXXXXXXX FIN XXXXXXXXXXXXXXXXXXXXXX')
        '''
        # TODO
        # Enregistrer les nouvelles urls dans la table sans écraser son contenu
        #df.write.csv('URLListe.csv', mode="overwrite")
        urlListe = sqlContext.sql("SELECT url, parsed from UrlList where parsed = 'false' and url <> '%s'" %response.url)
        
        df = urlListe.union(newUrls)
        
        sqlContext.registerDataFrameAsTable(df, "UrlList")
        
        return items        