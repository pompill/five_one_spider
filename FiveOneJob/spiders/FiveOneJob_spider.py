# -*- coding:utf-8 -*-

# Python内置库
from urllib import parse
# import re

# 第三方库
import scrapy
from scrapy.spiders import Spider
from lxml import etree
import re
from bs4 import BeautifulSoup as Bs

# 项目内部库
from FiveOneJob.items import FiveOneJobItem
from FiveOneJob.utils import changeK
from FiveOneJob.utils import changeMs
from FiveOneJob.utils import select_data


class FiveOneJobSpider(Spider):
    name = 'five_one'
    key = parse.quote('大数据')
    start_urls = ['http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea={}&keyword={}'
                  '&keywordtype=2&lang=c&stype=2&postchannel=0000&fromType=1&confirmdate=9']
    extra = '&curr_page={}'

    def start_requests(self):
        data = select_data.parse()
        for i in data:
            area = i['city']
            yield scrapy.Request(
                self.start_urls[0].format(area, self.key), callback=self.get_info_url, meta={'area': area})

    def get_info_url(self, response):
        html = response.body.decode('gbk', 'ignore')
        selector = etree.HTML(html)
        area = response.meta['area']
        if selector.xpath('//div[@class="el"]/p/span/a/@href'):
            info_url = selector.xpath('//div[@class="el"]/p/span/a/@href')
            for url in info_url:
                yield scrapy.Request(url, callback=self.get_info, meta={'url': url})
            page_num = int(self.get_page_num(response))
            if page_num > 1:
                for num in (2, page_num):
                    yield scrapy.Request(
                        self.start_urls[0].format(
                            area, self.key) + self.extra.format(num), callback=self.get_next_info_url)

    def get_next_info_url(self, response):
        html = response.body
        selector = etree.HTML(html)
        if selector.xpath('//div[@class="el"]/p/span/a/@href'):
            info_url = selector.xpath('//div[@class="el"]/p/span/a/@href')
            for url in info_url:
                yield scrapy.Request(url, callback=self.get_info, meta={'url': url})

    @staticmethod
    def get_page_num(response):
        html = response.body
        selector = etree.HTML(html)
        page_num = int(selector.xpath('string(//div[@class="p_in"]/span[1])').replace('共', '').replace('页，到第', ''))
        return page_num

    def get_info(self, response):
        try:
            html = response.body.decode('gbk', 'ignore')
            selector = etree.HTML(html)
            soup = Bs(html, 'lxml')
            item = FiveOneJobItem()
            salary = selector.xpath('string(//div[@class="cn"]/strong)')
            location = re.sub('\s+', '', selector.xpath('string(//div[@class="bmsg inbox"]/p)').replace('上班地址：', ''))
            work_experience = selector.xpath('string(//div[@class="t1"]/span[1])')
            date_id = len(soup.select('div[class="t1"] span[class="sp4"]'))
            people_count = selector.xpath(
                'string(//div[@class="t1"]/span[{}])'.format(date_id - 1)).replace(
                '招', '').replace(
                '人', '')
            date = selector.xpath('string(//div[@class="t1"]/span[{}])'.format(date_id)).replace('发布', '')
            career_type = selector.xpath('string(//div[@class="mt10"]/p[1]/span[@class="el"])').replace('其他', '')
            work_info_url = response.meta['url']
            business_name = selector.xpath('//p[@class="cname"]/a/@title')[0]
            if date_id > 3:
                limit_degree = selector.xpath('string(//div[@class="t1"]/span[{}])'.format(date_id - 2))
            else:
                limit_degree = ''
            if re.findall('万/月', salary):
                s = salary.replace('万/月', '').split('-')
                if len(s) == 2:
                    min_salary = changeK.change_to_k(float(s[0])*10000)
                    max_salary = changeK.change_to_k(float(s[1])*10000)
                else:
                    min_salary = s[0]
                    max_salary = s[0]
                item['min_salary'] = min_salary
                item['max_salary'] = max_salary
            elif re.findall('千/月', salary):
                s = salary.replace('千/月', '').split('-')
                if len(s) == 2:
                    min_salary = changeK.change_to_k(float(s[0]) * 10000)
                    max_salary = changeK.change_to_k(float(s[1]) * 10000)
                else:
                    min_salary = s[0]
                    max_salary = s[0]
                item['min_salary'] = min_salary
                item['max_salary'] = max_salary
            info = selector.xpath('string(//div[@class="bmsg job_msg inbox"])')
            work_info_content = re.sub('\s+', '', info)
            try:
                if re.findall('要求：(.*?)职责', work_info_content):
                    work_duty = re.findall('要求：(.*?)职责', work_info_content)[0][:-2]
                    work_need = re.findall('职责：(.*?)职能类别', work_info_content)[0]
                    if work_duty == '':
                        work_duty_content = work_info_content
                    else:
                        work_duty_content = ''
                else:
                    work_duty = re.findall('要求：(.*?)职能类别', work_info_content)[0]
                    work_need = ''
                    if work_duty == '':
                        work_duty_content = work_info_content
                    else:
                        work_duty_content = ''
            except Exception as err:
                print(err)
                work_duty = ''
                work_need = ''
                work_duty_content = work_info_content
            publish_date = changeMs.change_ms(str(str(2018) + str('-') + date))
            business_website = selector.xpath('//p[@class="cname"]/a/@href')
            item['from_website'] = "51job"
            item['location'] = location
            item['work_experience'] = work_experience
            item['limit_degree'] = limit_degree
            item['people_count'] = people_count
            item['publish_date'] = publish_date
            item['career_type'] = career_type
            item['work_duty'] = work_duty
            item['work_need'] = work_need
            item['work_duty_content'] = work_duty_content
            item['work_info_url'] = work_info_url
            item['work_type'] = '全职'
            item['business_name'] = business_name
            item['business_website'] = business_website[0]
            print(item)
            yield scrapy.Request(
                url=business_website[0], callback=self.get_company_info,
                meta={'item': item, 'work_info_html': response.body})
        except Exception as err:
            print(err, response.meta['url'])
            pass

    @staticmethod
    def get_company_info(response):
        try:
            html = response.body.decode('gbk', 'ignore')
            selector = etree.HTML(html)
            item = response.meta['item']
            business_type = selector.xpath('string(//p[@class="ltype"])').split('|')[0].strip()
            business_count = selector.xpath('string(//p[@class="ltype"])').split('|')[1].strip()
            business_industry = selector.xpath('string(//p[@class="ltype"])').split('|')[2].strip()
            business_location = selector.xpath('string(//div[@class="inbox"]/p)').replace('公司地址：', '').strip()
            business_info = selector.xpath('string(//div[@class="con_msg"]/div[1]/p)').strip()
            item['business_type'] = business_type
            item['business_count'] = business_count
            item['business_industry'] = business_industry
            item['business_location'] = business_location
            item['business_info'] = business_info
            yield item
        except Exception as err:
            print(err)
            html = response.meta['work_info_html']
            item = response.meta['item']
            selector = etree.HTML(html)
            business_all_info = re.sub('\s+', '', selector.xpath('string(//div[@class="tmsg inbox"])'))
            business_type = re.sub('\s+', '', selector.xpath(
                'string(//p[@class="msg ltype"])').split('|')[0]).strip()
            business_count = re.sub('\s+', '', selector.xpath(
                'string(//p[@class="msg ltype"])').split('|')[1]).strip()
            business_industry = re.sub('\s+', '', selector.xpath(
                'string(//p[@class="msg ltype"])').split('|')[2]).strip()
            item['business_type'] = business_type
            item['business_count'] = business_count
            item['business_industry'] = business_industry
            if re.findall('工作地址:(.*?)', business_all_info):
                try:
                    item['business_location'] = re.sub('\s+', '', re.findall(
                        '工作地址:(.*?)', business_all_info)[0]).strip()
                    item['business_info'] = re.findall('(.*?)工作地址', business_all_info)[0]
                except Exception as err:
                    print(err)
                    item['business_location'] = ''
                    item['business_info'] = business_all_info
            else:
                item['business_location'] = ''
                item['business_info'] = business_all_info
            yield item
