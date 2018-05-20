# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import configparser

class ShudHelper():
    def getConfig(self):
        config = configparser.ConfigParser()
        config.read('../shud.ini')
        return config
    
    def cleanHtml(self, html):
        return str(html).replace('\n', '').strip()
    
    def getJsonFiles(self, datadirectory):
        #list of json-line files
        json_files = [pos_json for pos_json in os.listdir(datadirectory) if pos_json.endswith('.jl')]
        return json_files
    
        

