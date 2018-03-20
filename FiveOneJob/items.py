# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class FiveOneJobItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    _id = Field()
    from_website = Field()
    min_salary = Field()
    max_salary = Field()
    location = Field()
    publish_date = Field()
    limit_degree = Field()
    work_experience = Field()
    people_count = Field()
    career_type = Field()
    business_name = Field()
    business_website = Field()
    business_type = Field()
    business_location = Field()
    business_count = Field()
    business_industry = Field()
    business_info = Field()
    work_type = Field()
    work_duty = Field()
    work_need = Field()
    work_duty_content = Field()
    work_info_url = Field()
