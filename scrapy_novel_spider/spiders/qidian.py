# -*- coding: utf-8 -*-
import json
from urllib.parse import urljoin, unquote, urlparse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
import scrapy
from scrapy_novel_spider.items import BookItem, CatalogItem
from scrapy_redis.spiders import RedisCrawlSpider


class QidianSpider(RedisCrawlSpider):
    name = 'qidian'
    allowed_domains = ['qidian.com']
    # start_urls = ['https://www.qidian.com/all_pub?chanId=13100&orderId=&page=1&style=1&pageSize=20&siteid=1&pubflag=1&hiddenField=0']
    redis_key = "start_url"
    rules = (
        Rule(LinkExtractor(allow=r'.*/mm/all.*'), callback='parse_detail', follow=True),
        Rule(LinkExtractor(allow=r'.*/all.*'), callback='parse_detail', follow=True),
        Rule(LinkExtractor(allow=r'.*/all_pub.*'), callback='parse_detail', follow=True),
    )
    catalog_path = 'https://read.qidian.com/ajax/book/category?_csrfToken=&bookId={0}'

    def parse_detail(self, response):
        books = response.xpath('//ul[@class="all-img-list cf"]//li')
        for book in books:
            book_id = book.xpath('./div[@class="book-mid-info"]/h4/a/@data-bid').get()
            src = book.xpath('./div[@class="book-mid-info"]/h4/a/@href').get()
            title = book.xpath('./div[@class="book-mid-info"]/h4/a/text()').get()
            img_url = book.xpath('./div[@class="book-img-box"]/a/img/@src').get()
            state = book.xpath('./div[@class="book-mid-info"]/p[@class="author"]/span[1]/text()').get()
            author = book.xpath('./div[@class="book-mid-info"]/p[@class="author"]/a[1]/text()').get()
            chan_name = book.xpath('./div[@class="book-mid-info"]/p[@class="author"]/a[2]/text()').get()
            sub_name = book.xpath('./div[@class="book-mid-info"]/p[@class="author"]/a[3]/text()').get()
            synoptic = book.xpath('./div[@class="book-mid-info"]/p[@class="intro"]/text()').getall()
            synoptic = " ".join(synoptic).strip()
            origin_url = response.url
            urldict = urlparse(response.url)
            origin_path = urldict.path
            gender = 0
            if origin_path == '/all_pub':
                gender = 3
            if origin_path == '/mm/all':
                gender = 2
            if origin_path == '/all':
                gender = 1
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
