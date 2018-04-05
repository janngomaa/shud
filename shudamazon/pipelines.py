# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
import json
import configparser
import datetime

class GrpDealItemPipeline(object):
    config = configparser.ConfigParser()
    config.read('../shud.ini')
    startTime =datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f_%z")
    batch_id = "grp_" + startTime
    fileName = config.get('layers', 'staging_repo') + batch_id + '.jl'
    
    def open_spider(self, spider):
        self.file = open(self.fileName, 'w')

    def close_spider(self, spider):
        self.file.close()
        #Insert into param table
        
    def process_item(self, item, spider):
        if not item['title'] is None:
            line = json.dumps(dict(item)) + "\n"
            self.file.write(line)
            return item
        else:
            raise DropItem("Missing title in %s" % item)
