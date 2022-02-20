# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from elasticsearch import Elasticsearch
import uuid

class NofluffjobsPipeline:
    def open_spider(self, spider):
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    def process_item(self, item, spider):
        # add item to elasticsearch database
        line = ItemAdapter(item).asdict()
        self.es.index(
            index = 'nofluffjobs',
            doc_type = 'job',
            id = uuid.uuid4(),
            body = line
        )
        return item

