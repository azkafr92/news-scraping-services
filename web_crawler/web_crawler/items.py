# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class WebCrawlerItem(scrapy.Item):
    title = scrapy.Field()
    text = scrapy.Field()
    table_name = scrapy.Field()
    link = scrapy.Field()
    id_ = scrapy.Field()
    channel = scrapy.Field()
    published= scrapy.Field()
