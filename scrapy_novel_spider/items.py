# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class BookUrlItem(Item):
    srcs = Field()

class BookItem(Item):
    # bookInfo
    id = Field()
    book_id = Field()
    src = Field()
    title = Field()
    img_url = Field()
    state = Field()
    author = Field()
    chan_name = Field()
    sub_name = Field()
    synoptic = Field()
    origin_url = Field()
    gender = Field()
    platform = Field()
    platform_src = Field()


class CatalogItem(Item):
    # catalogInfo
    id = Field()
    catalog_id = Field()
    title = Field()
    src = Field()
    book_id = Field()
    book_title = Field()
    cnt = Field()
    uuid = Field()
    vs = Field()
    vn = Field()
    update_time = Field()
    platform = Field()
    platform_src = Field()
    article = Field()
    item_book = Field()
