# -*- coding: utf-8 -*-
from urllib.parse import urljoin,unquote
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import scrapy

from scrapy_novel_spider.items import QidianItem


class QidianSpider(CrawlSpider):
    name = 'qidian'
    allowed_domains = ['qidian.com']
    start_urls = ['https://www.qidian.com/all']
    rules = (
        Rule(LinkExtractor(allow=r'.*/all\?.*'), callback='parse_detail', follow=True),
    )
    def parse_detail(self, response):
        # books = response.xpath('//ul[@class="all-img-list cf"]//li')
        # for book in books:
        #     book_id = book.xpath('./div[@class="book-mid-info"]/h4/a/@data-bid').get()
        #     src = book.xpath('./div[@class="book-mid-info"]/h4/a/@href').get()
        #     title = book.xpath('./div[@class="book-mid-info"]/h4/a/text()').get()
        #     img_url = book.xpath('./div[@class="book-img-box"]/a/img/@src').get()
        #     state = book.xpath('./div[@class="book-mid-info"]/p[@class="author"]/span[1]/text()').get()
        #     author = book.xpath('./div[@class="book-mid-info"]/p[@class="author"]/a[1]/text()').get()
        #     chan_name = book.xpath('./div[@class="book-mid-info"]/p[@class="author"]/a[2]/text()').get()
        #     sub_name = book.xpath('./div[@class="book-mid-info"]/p[@class="author"]/a[3]/text()').get()
        #     synoptic = book.xpath('./div[@class="book-mid-info"]/p[@class="intro"]/text()').getall()
        #     synoptic = " ".join(synoptic).strip()
        #     item = QidianItem(book_id=book_id, src=src, title=title, img_url=img_url, state=state, author=author,
        #                       chan_name=chan_name, sub_name=sub_name, synoptic=synoptic)
        #     yield item

        print(unquote(response.url))
        # next_url = response.xpath('//ul[@class="lbf-pagination-item-list"]/li[last()]/a/@href').get()
        # if not next_url:
        #     return
        # else:
        #     yield scrapy.Request(urljoin(response.url, next_url), callback=self.parse)
