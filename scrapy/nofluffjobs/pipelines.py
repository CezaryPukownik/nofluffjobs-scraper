# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from typing import Collection
import pymongo
from envyaml import EnvYAML
from itemadapter import ItemAdapter

class NofluffjobsPipeline:
    def open_spider(self, spider):
        # parse config file
        config = EnvYAML('configuration.yml')
        host = config['mongodb']['host']
        port = config['mongodb']['port']
        database = config['mongodb']['database']
        collection = config['mongodb']['collection']
        
        # connect to target mongodb collection
        mongodb = pymongo.MongoClient(f"mongodb://{host}:{port}/")
        self.collection = mongodb[database][collection]

    def process_item(self, item, spider):
        line = ItemAdapter(item).asdict()        
        self.collection.insert_one(line)
        return item

