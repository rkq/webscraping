# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class NoticeItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    security_code = scrapy.Field()
    security_name = scrapy.Field()
    notice_title = scrapy.Field()
    notice_url = scrapy.Field()
    notice_date = scrapy.Field()
