# -*- coding: utf-8 -*-
from urllib.parse import urlparse, urljoin

import scrapy
import json

from scrapy_novel_spider.items import BookItem, CatalogItem


class ItemQidianBookSpider(scrapy.Spider):
    name = 'item_qidian_book'
    allowed_domains = ['qidian.com']
    start_urls = ['https://book.qidian.com/info/1015755915', 'https://book.qidian.com/info/1015450933',
                  'https://book.qidian.com/info/1015755915',
                  'https://book.qidian.com/info/1015444718', 'https://book.qidian.com/info/1015504449',
                  'https://book.qidian.com/info/1014977522', 'https://book.qidian.com/info/1014977522#Catalog']
    # start_urls = ['https://book.qidian.com/info/1015755915']
    catalog_path = 'https://read.qidian.com/ajax/book/category?_csrfToken=&bookId={0}'

    def parse(self, response):
        book_id = response.url.split('/')[-1]
        src = response.url
        img_url = response.xpath('//div[@class="book-information cf"]//a[@id="bookImg"]/img/@src').get()[:-5]
        book_info = response.xpath('//div[@class="book-information cf"]/div[contains(@class,"book-info")]')
        title = book_info.xpath('//h1/em/text()').get()
        blue = book_info.xpath('//p[@class="tag"]/span/text()').getall()
        red = book_info.xpath('//p[@class="tag"]/a/text()').getall()
        state = blue[0]
        author = book_info.xpath('//h1/span/a/text()').get()
        chan_name = red[0]
        sub_name = red[1]
        synoptic = response.xpath('//div[@class="book-content-wrap cf"]//div[@class="book-intro"]//p/text()').getall()
        synoptic = " ".join(synoptic).strip()
        origin_url = response.url
        gender = 0
        platform = '起点中文网'
        platform_src = 'https://www.qidian.com'

        item = BookItem(book_id=book_id, src=src, title=title, img_url=img_url, state=state, author=author,
                        chan_name=chan_name, sub_name=sub_name, synoptic=synoptic, origin_url=origin_url,
                        gender=gender, platform=platform, platform_src=platform_src)
        yield item
        path = self.catalog_path.format(int(book_id))
        yield scrapy.Request(url=path, meta={'item_book': item}, callback=self.catalog_item)

    def catalog_item(self, response):
        item_book = response.meta['item_book']
        # print(item_book)
        book_id = item_book['book_id']
        book_title = item_book['title']
        platform = item_book['platform']
        platform_src = item_book['platform_src']
        uuid = 1
        vnid = 1

        apiData = json.loads(response.text)
        # self.log(apiData)
        if apiData['data']['vs']:
            vss = apiData['data']['vs']
            for vs in vss:
                # self.log(vs)
                vn = str(vnid) + '_' + vs['vN']
                vnid += 1
                vip = vs['vS']
                for cs in vs['cs']:
                    id = cs['id']
                    if vip == 0:
                        cU = urljoin(response.url, '/chapter/' + cs['cU'])
                    else:
                        cU = urljoin(response.url, '/chapter/' + str(book_id) + '/' + str(id))
                        vn = vs['vN'] + '_VIP'
                    cN = str(uuid) + '_' + cs['cN']
                    cnt = cs['cnt']
                    update_time = cs['uT']
                    catalog = dict(catalog_id=id, title=cN, src=cU, book_id=book_id, book_title=book_title, cnt=cnt,
                                   uuid=uuid, vs=vip, vn=vn, update_time=update_time, platform=platform,
                                   platform_src=platform_src)
                    uuid += 1
                    yield scrapy.Request(url=catalog['src'], meta={'catalog': catalog, 'item_book': item_book},
                                         callback=self.catalog_txt)

    def catalog_txt(self, response):
        content = response.xpath('//div[contains(@class,"read-content")]/p/text()').getall()
        content = "\n".join(content)
        catalog = response.meta['catalog']
        item_book = response.meta['item_book']
        yield CatalogItem(catalog_id=catalog['catalog_id'], title=catalog['title'], src=catalog['src'],
                          book_id=catalog['book_id'], book_title=catalog['book_title'], cnt=catalog['cnt'],
                          uuid=catalog['uuid'], vs=catalog['vs'], vn=catalog['vn'], update_time=catalog['update_time'],
                          platform=catalog['platform'], platform_src=catalog['platform_src'], article=content,
                          item_book=item_book)
