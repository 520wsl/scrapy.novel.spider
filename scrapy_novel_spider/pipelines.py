# -*- coding: utf-8 -*-

from pymysql import cursors
from twisted.enterprise import adbapi

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy_novel_spider.items import BookItem, CatalogItem


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
        if isinstance(item, BookItem):
            result = self.dbpool.runQuery(self.get_book_ids(item))
            result.addCallback(self.insert_book_item, item, spider)
        elif isinstance(item, CatalogItem):
            result = self.dbpool.runQuery(self.get_book_ids(item))
            result.addCallback(self.catalog_item, item, spider)
        return item

    # 获取数据ids sql
    def get_book_ids(self, item):
        return 'SELECT id FROM book WHERE book_id= "%s" AND platform = "%s" AND platform_src = "%s"' % (
            item['book_id'], item['platform'], item['platform_src'])

    # 获取数据ids sql
    def get_catalog_ids(self, item):
        return 'SELECT id FROM catalogs WHERE book_id= %s AND catalog_id = "%s"' % (item['book_id'], item['catalog_id'])

    # 获取数据ids sql
    def get_catalog_txt_ids(self, item):
        return 'SELECT id FROM txt WHERE catalog_id= %s ' % (item['catalog_id'])

    # 新增或修改 书籍信息
    def insert_book_item_cursor(self, cursor, item):
        insert_sql = '''INSERT INTO `book` (`id`, `book_id`, `src`,`title`,`img_url`,`state`,`author`,`chan_name`,`sub_name`,`gender`,`synoptic`,`platform`,`platform_src`)  VALUES (%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s) ON DUPLICATE KEY UPDATE id = VALUES (id), book_id = VALUES (book_id), src = VALUES (src),title = VALUES (title),img_url = VALUES (img_url),state = VALUES (state),author = VALUES (author),chan_name = VALUES (chan_name),sub_name = VALUES (sub_name),gender = VALUES (gender),synoptic = VALUES (synoptic),platform = VALUES (platform),platform_src = VALUES (platform_src), nex = nex+1'''
        cursor.execute(insert_sql, (
            item['id'], item['book_id'], item['src'], item['title'], item['img_url'], item['state'], item['author'],
            item['chan_name'], item['sub_name'], item['gender'], item['synoptic'], item['platform'],
            item['platform_src']))

    # 新增或修改 目录信息
    def insert_catalog_item_cursor(self, cursor, item):
        insert_sql = '''INSERT INTO `catalogs` (`id`,`catalog_id`,`title`, `src`,`book_id`,`book_title`,`cnt`,`uuid`,`vs`,`vn`,`update_time`)  VALUES (%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s) ON DUPLICATE KEY UPDATE id = VALUES (id),catalog_id = VALUES (catalog_id),title = VALUES (title), src = VALUES (src),book_id = VALUES (book_id),book_title = VALUES (book_title),cnt = VALUES (cnt),uuid = VALUES (uuid),vs = VALUES (vs),vn = VALUES (vn),update_time = VALUES (update_time), nex = nex+1'''
        cursor.execute(insert_sql, (
            item['id'], item['catalog_id'], item['title'], item['src'], item['book_id'], item['book_title'],
            item['cnt'],
            item['uuid'], item['vs'], item['vn'], item['update_time']))

    # 新增或修改 内容信息
    def insert_catalog_txt_item_cursor(self, cursor, item):
        insert_sql = '''INSERT INTO `txt` (`id`, `catalog_id`, `catalog_title`,`book_id`,`book_title`,`article`)  VALUES (%s,%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id = VALUES (id), catalog_id = VALUES (catalog_id), catalog_title = VALUES (catalog_title), book_id = VALUES (book_id), book_title = VALUES (book_title),article = VALUES (article), nex = nex+1'''
        cursor.execute(insert_sql, (
            item['id'], item['catalog_id'], item['title'], item['book_id'], item['book_title'], item['article']))

    # 新增 或 更新 book 信息
    def insert_book_item(self, ids, item, spider):
        if len(ids) > 0:
            item['id'] = ids[0]['id']
        else:
            item['id'] = 0
        self.dbpool.runInteraction(self.insert_book_item_cursor, item)

    # 新增 或  更新 catalo 信息
    def insert_catalog_item(self, ids, item, spider):
        if len(ids) > 0:
            item['id'] = ids[0]['id']
        else:
            item['id'] = 0
        result = self.dbpool.runInteraction(self.insert_catalog_item_cursor, item)
        result.addCallback(self.catalog_txt_item, item, spider)

    # 新增 或  更新 txt 信息
    def insert_catalog_txt_item(self, ids, item, spider):
        item['catalog_id'] = item['id']
        if len(ids) > 0:
            item['id'] = ids[0]['id']
        else:
            item['id'] = 0
        self.dbpool.runInteraction(self.insert_catalog_txt_item_cursor, item)

    # 获取 catalog id
    def catalog_item(self, ids, item, spider):
        if len(ids) > 0:
            item['book_id'] = ids[0]['id']
        else:
            item['book_id'] = 0
            result1 = self.dbpool.runInteraction(self.insert_book_item_cursor, item['catalog_item'])
            result1.addCallback(self.process_item, item, spider)
            return
        result2 = self.dbpool.runQuery(self.get_catalog_ids(item))
        result2.addCallback(self.insert_catalog_item, item, spider)

    # 获取 txt id
    def catalog_txt_item(self, res, item, spider):
        if item['id'] <= 0:
            result1 = self.dbpool.runQuery(self.get_catalog_ids(item))
            result1.addCallback(self.insert_catalog_item, item, spider)
            return
        result2 = self.dbpool.runQuery(self.get_catalog_txt_ids(item))
        result2.addCallback(self.insert_catalog_txt_item, item, spider)


