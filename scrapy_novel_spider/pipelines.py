# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy_novel_spider.items import QidianItem
from scrapy.exporters import JsonLinesItemExporter


class ScrapyNovelSpiderPipeline(object):
    def __init__(self):
        self.qidian_fp = open('qidian_dev.json', 'wb')
        self.qidian_exporter = JsonLinesItemExporter(self.qidian_fp, ensure_ascii=False)

    def process_item(self, item, spider):
        if isinstance(item,QidianItem):
            self.qidian_exporter.export_item(item)
        return item

    def close_spider(self):
        self.qidian_fp.close();
