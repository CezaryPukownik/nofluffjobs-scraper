# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class NofluffjobsItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    company = scrapy.Field()
    job_level = scrapy.Field()
    city = scrapy.Field()
    locations = scrapy.Field()
    salary = scrapy.Field()
    category = scrapy.Field()
    skills = scrapy.Field()
    methodology = scrapy.Field()
    timestamp = scrapy.Field()
