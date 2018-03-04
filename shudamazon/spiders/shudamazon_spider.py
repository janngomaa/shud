import scrapy
import configparser
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

class ShudCrawler(scrapy.Spider):
    name = "amazon"
    config = configparser.ConfigParser()
    config.read('../shud.ini')
    
    sparkSession = SparkSession \
            .builder \
            .appName(config.get('spark', 'appname')) \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
            
    sqlContext = SQLContext(sparkSession)

     # Spider Name 
        #self.config.get('crawling', 'spidername')
    
    # The domains that are allowed (links to other domains are skipped)
    allowed_domains = config.get('crawling', 'allowedDomain')
    # The URLs to start with
    start_urls = config.get('crawling', 'startUrl')
    
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
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        #TODO
        #Parcourir la liste de start urls à crawler et la mettre en mémore avec parsed=false
        initUrlList = [(self.config.get('crawling', 'startUrl'), "false")]
        df = self.sparkSession.createDataFrame(initUrlList, schema=["url", "crawled"])
        self.sqlContext.registerDataFrameAsTable(df, "WorkTable")
        
        '''
        urls = [self.start_urls]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
        '''
        indx = 0
        urlListe = self.sqlContext.sql("SELECT url from WorkTable where crawled = 'false'")
        while len(urlListe.rdd.collect()) > 0:
            print("####################### Current step = %s " %str(indx))
            
            for url in urlListe.rdd.collect():
                print("************************** Current url = %s " %str(url))
                #TODO
                #Vérifier que l'url contient au moins un des allowed domain
                if self.config.get('crawling', 'allowedDomain') in url[0]:
                    print("&&&&&&&&&&&&&&&&&&&&&& Allowed url = %s " %str(url[0]))
                    yield scrapy.Request(url=url[0], callback=self.parse)

            urlListe = self.sqlContext.sql("SELECT url from WorkTable where crawled = 'false'")
            if indx > 2:
                break
                
            indx += 1
            
            

    def parse(self, response):
        
        print("%%%%%%% Current url = %s " %response.url)
        print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")

        newUrls = []

        # Only extract canonicalized and unique links (with respect to the current page)
        links = LinkExtractor(canonicalize=False, unique=True).extract_links(response)
        # Now go through all the found links
        for link in links:            
            # Check whether the domain of the URL of the link is allowed; so whether it is in one of the allowed domains
            is_allowed = False
            for allowed_domain in self.allowed_domains:
                if allowed_domain in link.url:
                    is_allowed = True
            # If it is allowed, append the url to the list
            if is_allowed:
                newUrls.append((link.url, "false")) 
            #Get all urls to synchronize and update
            df = self.sqlContext\
                .sql("SELECT url, crawled from WorkTable where url <>'%s'" % response.url)\
                .union(self.sparkSession.createDataFrame(newUrls))\
                .union(self.sparkSession.createDataFrame([(response.url, "true")]))\
                .dropDuplicates(['url'])
        
        self.sqlContext.registerDataFrameAsTable(df, "WorkTable")
        print(df.show())
        
        # TODO
        #Put response body's content into RDDs
        #page = response.url.split("/")[-2]
        #page = response.url
        m=hashlib.md5(bytes(str(response.url),"ascii"))   # python 3                
        filename = str(self.name)+'_'+ m.hexdigest() + '.html'
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename) 
        
        return df