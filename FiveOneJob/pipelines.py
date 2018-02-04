# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from scrapy.conf import settings


class FiveonejobPipeline(object):
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(host=settings['MONGO_HOST'], port=settings['MONGO_PORT'])
        self.client.admin.authenticate(settings['MONGO_USER'], settings['MONGO_PSW'])
        self.FiveOne = self.client[settings['MONGO_DB']]
        self.FiveOneData = self.FiveOne[settings['MONGO_COLL']]
        pass

    def process_item(self, item, spider):
        data = {
            'from_website': item['from_website'],
            'min_salary': item['min_salary'],
            'max_salary': item['max_salary'],
            'location': item['location'],
            'publish_date': item['publish_date'],
            'work_experience': item['work_experience'],
            'limit_degree': item['limit_degree'],
            'people_count': item['people_count'],
            'career_type': item['career_type'],
            'work_duty': item['work_duty'],
            'work_need': item['work_need'],
            'work_type': item['work_type'],
            'work_duty_content': item['work_duty_content'],
            'work_info_url': item['work_info_url'],
            'business_name': item['business_name'],
            'business_type': item['business_type'],
            'business_count': item['business_count'],
            'business_website': item['business_website'],
            'business_industry': item['business_industry'],
            'business_location': item['business_location'],
            'business_info': item['business_info'],
        }
        self.FiveOneData.insert_one(data)
        return item

    def close_spider(self, spider):
        pass
