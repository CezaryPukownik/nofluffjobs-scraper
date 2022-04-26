from datetime import datetime

import scrapy
from ..items import NofluffjobsItem
from scrapy_selenium import SeleniumRequest

class JobsSpider(scrapy.Spider):
    name = "Jobs"
    start_urls = [
       'https://nofluffjobs.com/pl/jobs/artificial-intelligence?criteria=category%3Dbig-data,business-intelligence,business-analyst'
    ]

    def parse(self, response):
        # redirect response to selenium request with wait_time
        for url in self.start_urls:
            yield SeleniumRequest(url=url, callback=self.parse_result, wait_time=2)

    def parse_result(self, response):
        '''Parse list of jobs'''
        #get all jobs in list
        jobs = response.css('a.posting-list-item::attr(href)').getall()
        for job in jobs:
            # redirect job page to parse_job function
            yield scrapy.Request(url=response.urljoin(job), callback=self.parse_job, meta={'dont_merge_cookies': True})

        # go to next page if exist
        next_page = response.css('a[aria-label="Next"]::attr(href)').get()
        if next_page:
            next_page = response.urljoin(next_page)
            print(next_page)
            yield SeleniumRequest(url=next_page, callback=self.parse_result, wait_time=2)

    def parse_job(self, response):
        '''Parse job offer'''
        item = NofluffjobsItem()
        item['url'] = response.url
        
        # title: str
        item['title'] = response.css('h1::text').get()
        
        #company: str
        try:
            item['company'] = response.css('common-posting-header ::text').getall()[1].strip()
        except:
            item['company'] = None
            
        # job_level: List(str)
        item['job_level'] = [y.strip()  for x in response.css('common-posting-seniority ::text').getall() for y in x.split(',')]
        item['city'] = response.css('[id="backToCity"] ::text').get().strip()
        item['locations'] = [x.strip() for x in response.css('ul.locations-compact ::text').getall()]

        # salary: List(Dict(str, int, int))
        # salary = [{ 
        #   type: brutto
        #   min: xxxx
        #   max: xxxx 
        # }, ... ]
        
        try:
            item['salary'] = [ dict(type=a, min=b, max=c) for a,b,c in [(salary.css('.type::text').get().strip(), *[int(x.replace('\xa0','')) for x in salary.css('.mb-0::text').get().split('-')])  for salary in response.css('div.salary')]]
        except:
            item['salary'] = [ dict(type=a, max=b) for a,b in [(salary.css('.type::text').get().strip(), *[int(x.replace('\xa0','')) for x in salary.css('.mb-0::text').get().split('-')])  for salary in response.css('div.salary')]]
        
        # category: list
        try:
            item['category'] = response.css('common-posting-cat-tech span.font-weight-semi-bold ::text').get().split(', ')
        except:
            item['category'] = None
        
        # skills: List(Dict(str, str))
        # skills = [ {
            # name: xxxxxx
            # importance: nice-to-have
        # }, ... ]

        skills_require = [dict(type='require', name=x.strip()) for x in response.css('[id="posting-requirements"] common-posting-item-tag ::text').getall()]
        skills_nice_to_have = [dict(type='nice-to-have', name=x.strip()) for x in response.css('[id="posting-nice-to-have"] common-posting-item-tag ::text').getall()] 
        item['skills'] = skills_require + skills_nice_to_have

        # methodology: List(Dict(str, str))
        # methodology = [{
        #   type: Agile
        #   value: JIRA
        # }, ... ]
        
        try:
            item['methodology'] =  [dict(type=x.css('span::text').get().strip(), values=[y.strip() for y in x.css('dd::text').get().split(',')] ) for x in response.css('#posting-environment .row')]
        except:
            item['methodology'] = None

        
        item['timestamp'] = datetime.now()
        
        yield item