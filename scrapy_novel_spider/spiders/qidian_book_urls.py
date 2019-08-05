# 说明：
#     此爬虫是一次性爬取整个起点中文网所有书籍
# 存在问题：
#     1、书籍数据存储重复
#     2、存储顺序不可控
#     3、书籍重复抓取
# -*- coding: utf-8 -*-
import json
from urllib.parse import urljoin, unquote, urlparse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
import scrapy
from scrapy_novel_spider.items import BookItem, CatalogItem, BookUrlItem
from scrapy_redis.spiders import RedisCrawlSpider


class QidianSpider(RedisCrawlSpider):
    name = 'qidian_book_urls'
    allowed_domains = ['qidian.com']
    # start_urls = ['https://www.qidian.com/all_pub?chanId=13100&orderId=&page=1&style=1&pageSize=20&siteid=1&pubflag=1&hiddenField=0']
    redis_key = "start_url"
    rules = (
        Rule(LinkExtractor(allow=r'.*/mm/all.*.*'), callback='parse_detail', follow=True),
        Rule(LinkExtractor(allow=r'.*/all.*'), callback='parse_detail', follow=True),
        Rule(LinkExtractor(allow=r'.*/all_pub.*'), callback='parse_detail', follow=True),
    )

    def parse_detail(self, response):
        books = response.xpath('//ul[@class="all-img-list cf"]//li')
        srcs = []
        for book in books:
            src = book.xpath('./div[@class="book-mid-info"]/h4/a/@href').get()
            if src:
                src = 'https:' + src
                srcs.append(src)
        books2 = response.xpath('//table[@class="rank-table-list all"]//tr')
        for book2 in books2:
            src2 = book2.xpath('./td/a[@class="name"]/@href').get()
            if src2:
                src2 = 'https:' + src2
                srcs.append(src2)
        yield BookUrlItem(srcs=srcs)
