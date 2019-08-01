# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy_novel_spider.items import BookItem, CatalogItem
from scrapy.exporters import JsonLinesItemExporter
from twisted.enterprise import adbapi
from pymysql import cursors


# class ScrapyNovelSpiderPipeline(object):
#     def __init__(self):
#         self.qidian_fp = open('qidian_dev.json', 'wb')
#         self.qidian_exporter = JsonLinesItemExporter(self.qidian_fp, ensure_ascii=False)
#
#     def process_item(self, item, spider):
#         if isinstance(item,QidianItem):
#             self.qidian_exporter.export_item(item)
#         return item
#
#     def close_spider(self):
#         self.qidian_fp.close();

class ScrapyNovelSpiderTwistedPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset='utf8mb4',
            cursorclass=cursors.DictCursor
        )
        dbpool = adbapi.ConnectionPool('pymysql', **dbparms)
        return cls(dbpool=dbpool)

    def process_item(self, item, spider):
        # if isinstance(item,BookItem):
        print(item)
        #     print('书籍==========')
        # elif isinstance(item,CatalogItem):
        #     print(item)
        #     print('目录==========')

        return item

    # result = self.dbpool.runQuery(self.get_book_ids(item))
    # result.addCallback(self.insert_book_item, item, spider)

    # 获取数据ids sql
    def get_book_ids(self, item):
        return 'SELECT id FROM book WHERE book_id= "%s" AND platform = "%s" AND platform_src = "%s"' % (
            item['book_id'], item['platform'], item['platform_src'])

    # 新增或修改 书籍信息
    def insert_book_item_cursor(self, cursor, item):
        insert_sql = '''INSERT INTO `book` (`id`, `book_id`, `src`,`title`,`img_url`,`state`,`author`,`chan_name`,`sub_name`,`gender`,`synoptic`,`platform`,`platform_src`)  VALUES (%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s) ON DUPLICATE KEY UPDATE id = VALUES (id), book_id = VALUES (book_id), src = VALUES (src),title = VALUES (title),img_url = VALUES (img_url),state = VALUES (state),author = VALUES (author),chan_name = VALUES (chan_name),sub_name = VALUES (sub_name),gender = VALUES (gender),synoptic = VALUES (synoptic),platform = VALUES (platform),platform_src = VALUES (platform_src), nex = nex+1'''
        cursor.execute(insert_sql, (
            item['id'], item['book_id'], item['src'], item['title'], item['img_url'], item['state'], item['author'],
            item['chan_name'], item['sub_name'], item['gender'], item['synoptic'], item['platform'],
            item['platform_src']))

    # 判断书籍是否已存在
    def insert_book_item(self, id_tup, item, spider):
        if len(id_tup) > 0:
            item['id'] = id_tup[0]['id']
        else:
            item['id'] = 0

        print(item)
        print('*' * 30)

        result = self.dbpool.runInteraction(self.insert_book_item_cursor, item)
        # result.addCallback(self.insert_book_item, item, spider)
