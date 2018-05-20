# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
import json
import datetime
from shudamazon.helper import ShudHelper
from importlib.machinery import SourceFileLoader

class GrpDealItemPipeline(object):
    helper = SourceFileLoader("ShudHelper", "/home/jovyan/work/shud/helper/helper.py").load_module()
    config = helper.ShudHelper.getConfig()
    startTime =datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    batch_id = "grp_" + startTime
    fileName = config.get('directory', 'datadirectory') + batch_id + '.jl'
    
    def open_spider(self, spider):
        self.file = open(self.fileName, 'w')

    def close_spider(self, spider):
        self.file.close()
        #Insert into param table
        
    def process_item(self, item, spider):
        if not item['title'] is None:
            item['title'] = self.helper.ShudHelper.cleanHtml(item['title'])
            item['merchant'] = self.helper.ShudHelper.cleanHtml(item['merchant'])
            item['merchantLocation'] = self.helper.ShudHelper.cleanHtml(item['merchantLocation'])
            
            item['dealOptMessages'] = list(filter(lambda x: len(x)>0, \
                                                  map(self.helper.ShudHelper.cleanHtml, item['dealOptMessages'])))
            item['dealOptPrices'] = list(filter(lambda x: len(x)>0, \
                                                    map(self.helper.ShudHelper.cleanHtml, item['dealOptPrices'])))

            
            item['dealTiming'] = self.helper.ShudHelper.cleanHtml(item['dealTiming'])
            item['dealRatingValue'] = self.helper.ShudHelper.cleanHtml(item['dealRatingValue']) + '%'
            
            
            line = json.dumps(dict(item)) + "\n"
            self.file.write(line)
            return item
        else:
            raise DropItem("Missing title in %s" % item)
