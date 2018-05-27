# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import configparser
import base64 
import time
from cryptography.fernet import Fernet
import random
import uuid
#import validators
#couchabse packages import
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster, PasswordAuthenticator
from couchbase.n1ql import N1QLQuery
import couchbase._libcouchbase as LCB
import couchbase.exceptions as E
from couchbase.user_constants import FMT_JSON
from couchbase._pyport import ulp

class ShudHelper():
    def getConfig():
        config = configparser.ConfigParser()
       # config.read(path)
        config.read('/home/jovyan/work/shud.ini')
        return config
    
    def cleanHtml(html):
        return str(html).replace('\n', '').strip()
    
    def getJsonFiles(datadirectory):
        #list of json-line files
        json_files = [pos_json for pos_json in os.listdir(datadirectory) if pos_json.endswith('.jl')]
        return json_files
    def getImageBase64(self, imagePath):        
        image = open(imagePath, 'rb') 
        image_read = image.read() 
        imagebase64 = base64.encodestring(image_read)
        return imagebase64
    
    def InsertData(DBName,BucketName, Data):
        #Database config
        config = getConfig() 
        cluster=Cluster(config.get('couchbase', 'cluster'))
        userName=config.get('couchbase', 'DAG')
        Pwd=config.get('couchbase', 'Pwd')
        #couchbase authentification
        cluster.authenticate(PasswordAuthenticator(userName, Pwd))  
        #open the database
        cb = cluster.open_bucket(DBName)
        #open the bucket name to load data
        dbkey=cluster.open_bucket(BucketName)
        #insert data to the bucket
        dbkey.upsert_multi(Data)